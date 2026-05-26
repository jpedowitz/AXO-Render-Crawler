import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { createHash } from 'crypto';
import { URL } from 'url';

// ── Types ────────────────────────────────────────────────────────────

export interface CrawledPage {
  url: string;
  title: string;
  statusCode: number;
  contentType: string;
  wordCount: number;
  aeoSignal: number;
  signals: string[];
  excerpt: string;
  contentHash: string;
  changed?: boolean;
  type?: string;
  text?: string;
  classification?: {
    priority?: number;
    action?: string;
    type?: string;
    trustSignal?: number;
    commercialIntent?: number;
    aiRelevance?: number;
    reasons?: string[];
  };
}

export interface CrawlOptions {
  startUrl: string;
  domain: string;
  maxPages: number;
  depth: number;
  includeSubdomains: boolean;
  timeoutMs: number;
  concurrency: number;
  perHostConcurrency: number;
  batchSize?: number;
  maxBatches?: number;
}

// ── User-Agent rotation ───────────────────────────────────────────────
// Use Googlebot so well-behaved sites serve real content, not bot blocks

const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// ── Bot-block detector ────────────────────────────────────────────────

function isBlockedPage(title: string, html: string, statusCode: number): boolean {
  if (statusCode === 403 || statusCode === 401 || statusCode === 429) return true;
  const t = title.toLowerCase();
  if (
    t.includes('access denied') ||
    t.includes('access to this page has been denied') ||
    t.includes('403 forbidden') ||
    t.includes('just a moment') ||    // Cloudflare
    t.includes('attention required') || // Cloudflare
    t.includes('checking your browser') ||
    t.includes('enable javascript') ||
    t.includes('please wait') ||
    t === 'error' ||
    t === ''
  ) return true;
  // Too short to be real content
  if (html.length < 500) return true;
  return false;
}

// ── Priority scorer ──────────────────────────────────────────────────

function priorityScore(url: string): number {
  const u = url.toLowerCase().replace(/^https?:\/\/[^/]+/, '');
  if (/\/(solutions?|services?|products?|platform|features?|capabilities)/.test(u)) return 100;
  if (/\/(vs|compare|alternatives?|pricing|roi|calculator)/.test(u)) return 95;
  if (/\/(case-stud|customer|success|results?|testimonial|proof)/.test(u)) return 90;
  if (/\/(contact|demo|trial|get-started|request|free)/.test(u)) return 88;
  if (/\/(about|team|leadership|company|mission|why)/.test(u)) return 70;
  if (/\/(blog|resources?|insights?|guides?|whitepapers?)(\/?$|\?|#)/.test(u)) return 75;
  if (/\/(blog|resources?|insights?)\/[^/]+/.test(u)) return 50;
  if (/\/(legal|privacy|terms|careers?|jobs?|press|events?\/\d{4})/.test(u)) return 5;
  if (/\/(tag|category|author)\//.test(u)) return 3;
  if (/\/page\/\d+/.test(u)) return 2;
  return 40;
}

function classifyType(url: string): string {
  const u = url.toLowerCase().replace(/^https?:\/\/[^/]+/, '');
  if (/\/(contact|demo|trial|get-started|request|sign-?up|free)/.test(u)) return 'conversion';
  if (/\/(blog|resources?|insights?|guides?|news|articles?)\/[^/]+/.test(u)) return 'article';
  return 'page';
}

function commercialIntentScore(url: string): number {
  const u = url.toLowerCase();
  if (/\/(pricing|demo|contact|trial|buy|request|get-started)/.test(u)) return 3;
  if (/\/(solutions?|services?|products?|platform|compare|vs\.)/.test(u)) return 2;
  return 1;
}

// ── AEO signal scorer ────────────────────────────────────────────────

function scoreAeo(html: string, url: string): {
  score: number;
  signals: string[];
  excerpt: string;
  wordCount: number;
  text: string;
} {
  const signals: string[] = [];
  const lower = html.toLowerCase();

  if (
    lower.includes('"@type": "faqpage"') ||
    lower.includes('"@type":"faqpage"') ||
    lower.includes('faqpage')
  ) signals.push('faq_schema');

  if (/\b(best|top|leading|trusted|proven|certified|award|recognized)\b/.test(lower))
    signals.push('proof_language');

  if (/\b(vs\.|versus|compare|better than|alternative|difference between|compared to)\b/.test(lower))
    signals.push('comparison_language');

  if (/\b(how to|step.by.step|guide|tutorial|learn|understand|what is|why does)\b/.test(lower))
    signals.push('ai_language');

  if (/\b(contact|get started|demo|free trial|buy now|sign up|request|schedule)\b/.test(lower))
    signals.push('decision_language');

  if (/<h[1-6]/i.test(html)) signals.push('heading_structure');

  const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  const wordCount = text.split(' ').filter(Boolean).length;
  const excerpt = text.substring(0, 300);

  const score = Math.min(
    10,
    signals.length * 1.5 +
    (wordCount > 500 ? 0.5 : 0) +
    (wordCount > 1000 ? 0.5 : 0) +
    (wordCount > 2000 ? 0.5 : 0)
  );

  return { score, signals, excerpt, wordCount, text: text.substring(0, 5000) };
}

// ── Sitemap discovery ────────────────────────────────────────────────

async function discoverSitemap(domain: string, timeoutMs: number): Promise<string[]> {
  const candidates = [
    `https://${domain}/sitemap.xml`,
    `https://${domain}/sitemap_index.xml`,
    `https://${domain}/sitemap-index.xml`,
    `https://www.${domain}/sitemap.xml`,
    `https://www.${domain}/sitemap_index.xml`,
  ];

  const found = new Set<string>();
  const perReqTimeout = Math.min(10000, timeoutMs);

  for (const sitemapUrl of candidates) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), perReqTimeout);
      const res = await fetch(sitemapUrl, {
        signal: controller.signal as any,
        headers: { 'User-Agent': USER_AGENT },
      });
      clearTimeout(timer);
      if (!res.ok) continue;

      const xml = await res.text();

      // Recurse into child sitemaps
      const childMatches = [...xml.matchAll(/<loc>\s*(https?:\/\/[^<]+\.xml[^<]*)\s*<\/loc>/gi)];
      for (const m of childMatches.slice(0, 20)) {
        const childUrl = m[1].trim();
        try {
          const ctrl2 = new AbortController();
          const t2 = setTimeout(() => ctrl2.abort(), perReqTimeout);
          const childRes = await fetch(childUrl, {
            signal: ctrl2.signal as any,
            headers: { 'User-Agent': USER_AGENT },
          });
          clearTimeout(t2);
          if (!childRes.ok) continue;
          const childXml = await childRes.text();
          [...childXml.matchAll(/<loc>\s*(https?:\/\/[^<\s]+)\s*<\/loc>/gi)]
            .map(cm => cm[1].trim())
            .filter(u => !u.endsWith('.xml'))
            .forEach(u => found.add(u));
        } catch { /* continue */ }
      }

      // Direct URLs in this sitemap
      [...xml.matchAll(/<loc>\s*(https?:\/\/[^<\s]+)\s*<\/loc>/gi)]
        .map(m => m[1].trim())
        .filter(u => !u.endsWith('.xml'))
        .forEach(u => found.add(u));

      if (found.size > 0) break;
    } catch { /* try next */ }
  }

  return [...found];
}

// ── BFS seed discovery (fallback when no sitemap) ───────────────────

async function bfsDiscover(
  startUrl: string,
  domain: string,
  maxUrls: number,
  timeoutMs: number
): Promise<string[]> {
  const visited = new Set<string>([startUrl]);
  const queue: string[] = [startUrl];
  const found: string[] = [startUrl];
  const deadline = Date.now() + timeoutMs;

  while (queue.length > 0 && found.length < maxUrls && Date.now() < deadline) {
    const url = queue.shift()!;
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 6000);
      const res = await fetch(url, {
        signal: controller.signal as any,
        headers: { 'User-Agent': USER_AGENT, Accept: 'text/html' },
        redirect: 'follow',
      });
      clearTimeout(timer);

      const ct = res.headers.get('content-type') || '';
      if (!ct.includes('html')) continue;

      const html = await res.text();
      const $ = cheerio.load(html);
      const base = new URL(url);

      $('a[href]').each((_, el) => {
        try {
          const href = $(el).attr('href') || '';
          const resolved = new URL(href, base);
          resolved.hash = '';
          const sp = new URLSearchParams(resolved.search);
          [...sp.keys()].filter(k => k.startsWith('utm')).forEach(k => sp.delete(k));
          resolved.search = sp.toString();

          const h = resolved.hostname.replace(/^www\./, '');
          const d = domain.replace(/^www\./, '');
          if (h !== d) return;

          const clean = resolved.toString();
          if (visited.has(clean)) return;
          if (/\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|xml|json|zip|mp4|mp3|woff|woff2|ttf)(\?|$)/i.test(clean)) return;

          visited.add(clean);
          queue.push(clean);
          found.push(clean);
        } catch { /* skip bad href */ }
      });
    } catch { /* skip failed fetch */ }
  }

  return found;
}

// ── Fetch a single page ──────────────────────────────────────────────

async function fetchPage(url: string, timeoutMs = 8000): Promise<CrawledPage | null> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: controller.signal as any,
      headers: {
        'User-Agent': USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      redirect: 'follow',
    });
    clearTimeout(timer);

    const ct = res.headers.get('content-type') || '';
    if (!ct.includes('html')) return null;

    const html = await res.text();
    const $ = cheerio.load(html);
    const title = ($('title').first().text() || $('h1').first().text() || url).trim().substring(0, 200);

    // Skip bot-blocked or empty pages — these poison the score
    if (isBlockedPage(title, html, res.status)) {
      return null;
    }

    const { score, signals, excerpt, wordCount, text } = scoreAeo(html, url);
    const contentHash = createHash('md5').update(html.substring(0, 50000)).digest('hex');
    const type = classifyType(url);
    const priority = priorityScore(url);
    const ci = commercialIntentScore(url);
    const aiRel = score >= 6 ? 3 : score >= 4 ? 2 : 1;

    return {
      url,
      title,
      statusCode: res.status,
      contentType: ct.split(';')[0].trim(),
      wordCount,
      aeoSignal: score,
      signals,
      excerpt,
      contentHash,
      changed: true,
      type,
      text,
      classification: {
        priority,
        action: score >= 7 ? 'optimize' : 'build',
        type,
        trustSignal: score >= 7 ? 1 : 0,
        commercialIntent: ci,
        aiRelevance: aiRel,
        reasons: signals,
      },
    };
  } catch {
    return null;
  }
}

// ── MAIN: Parallel crawl ─────────────────────────────────────────────

export async function crawlSite(options: CrawlOptions): Promise<CrawledPage[]> {
  const {
    startUrl,
    domain,
    maxPages,
    timeoutMs,
    batchSize = 25,
    maxBatches = 50,
  } = options;

  const deadline = Date.now() + timeoutMs;
  const normDomain = domain.replace(/^www\./, '');

  // ── Step 1: Discover all URLs ──────────────────────────────────────
  console.log(`[crawler] Discovering URLs for ${domain}…`);
  let allUrls = await discoverSitemap(
    normDomain,
    Math.min(timeoutMs * 0.15, 15000)
  );
  console.log(`[crawler] Sitemap: ${allUrls.length} URLs`);

  // Fallback to BFS if sitemap empty
  if (allUrls.length < 10) {
    console.log(`[crawler] Sitemap thin — BFS from ${startUrl}…`);
    allUrls = await bfsDiscover(
      startUrl,
      domain,
      Math.min(maxPages * 2, 2000),
      Math.min(timeoutMs * 0.25, 30000)
    );
    console.log(`[crawler] BFS found ${allUrls.length} URLs`);
  }

  // ── Step 2: Filter + dedupe + priority sort ────────────────────────
  const normalize = (u: string): string => {
    try {
      const p = new URL(u);
      p.hash = '';
      const sp = new URLSearchParams(p.search);
      [...sp.keys()].filter(k => k.startsWith('utm')).forEach(k => sp.delete(k));
      p.search = sp.toString();
      return p.toString();
    } catch { return u; }
  };

  const inDomain = (u: string): boolean => {
    try {
      const h = new URL(u).hostname.replace(/^www\./, '');
      return h === normDomain || h.endsWith('.' + normDomain);
    } catch { return false; }
  };

  const isContent = (u: string): boolean =>
    !/\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|xml|json|zip|mp4|mp3|woff|woff2|ttf)(\?|$)/i.test(u);

  const deduped = [...new Set(allUrls.map(normalize))]
    .filter(u => inDomain(u) && isContent(u));

  // Sort highest priority first
  deduped.sort((a, b) => priorityScore(b) - priorityScore(a));

  // Cap at maxPages — always include start URL
  if (!deduped.includes(normalize(startUrl))) {
    deduped.unshift(normalize(startUrl));
  }
  const urlsToFetch = deduped.slice(0, maxPages);

  console.log(`[crawler] Fetching ${urlsToFetch.length} URLs in parallel (batch=${batchSize}, maxConcurrentBatches=${maxBatches})…`);

  // ── Step 3: Chunk into batches ─────────────────────────────────────
  const chunks: string[][] = [];
  for (let i = 0; i < urlsToFetch.length; i += batchSize) {
    chunks.push(urlsToFetch.slice(i, i + batchSize));
  }

  // ── Step 4: Fire waves of concurrent batches ───────────────────────
  const results: CrawledPage[] = [];

  for (let wave = 0; wave < chunks.length; wave += maxBatches) {
    if (Date.now() > deadline) {
      console.log(`[crawler] Deadline hit — stopping at ${results.length} pages`);
      break;
    }

    const waveBatches = chunks.slice(wave, wave + maxBatches);
    const waveResults = await Promise.all(
      waveBatches.map(batch =>
        Promise.all(batch.map(url => fetchPage(url, 8000)))
      )
    );

    const wavePages = waveResults.flat().filter((p): p is CrawledPage => p !== null);
    results.push(...wavePages);

    console.log(`[crawler] Wave ${Math.floor(wave / maxBatches) + 1}/${Math.ceil(chunks.length / maxBatches)}: ${results.length} pages total`);
  }

  const elapsed = Math.round((Date.now() - (deadline - timeoutMs)) / 1000);
  console.log(`[crawler] Done: ${results.length} pages in ${elapsed}s`);
  return results;
}
