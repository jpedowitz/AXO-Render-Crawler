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
  onProgress?: (pagesFetched: number, totalUrls: number) => void;
}

// ── User-Agent ────────────────────────────────────────────────────────
// Chrome UA — Googlebot caused bot-challenge pages on pedowitzgroup.com etc.

const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// ── Bot-block detector ────────────────────────────────────────────────

function isBlockedPage(title: string, html: string, statusCode: number): boolean {
  if (statusCode === 403 || statusCode === 401 || statusCode === 429) return true;
  const t = title.toLowerCase();
  if (
    t.includes('access denied') ||
    t.includes('access to this page has been denied') ||
    t.includes('403 forbidden') ||
    t.includes('just a moment') ||
    t.includes('attention required') ||
    t.includes('checking your browser') ||
    t.includes('enable javascript') ||
    t.includes('please wait') ||
    t === 'error' ||
    t === ''
  ) return true;
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
  score: number; signals: string[]; excerpt: string; wordCount: number; text: string;
} {
  const signals: string[] = [];
  const lower = html.toLowerCase();

  if (lower.includes('"@type": "faqpage"') || lower.includes('"@type":"faqpage"') || lower.includes('faqpage'))
    signals.push('faq_schema');
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

// ── Sitemap discovery ─────────────────────────────────────────────────
// FIX: was 15% of crawlTimeoutMs (as low as 9s on 60s timeout).
// Now has its own fixed 45s budget, independent of crawlTimeoutMs.
// Also checks robots.txt for Sitemap: directive before trying candidate paths.

async function discoverSitemap(domain: string): Promise<string[]> {
  const SITEMAP_BUDGET_MS = 45000;
  const PER_REQ_MS = 10000;
  const startTime = Date.now();

  const candidates = [
    `https://${domain}/sitemap.xml`,
    `https://${domain}/sitemap_index.xml`,
    `https://${domain}/sitemap-index.xml`,
    `https://www.${domain}/sitemap.xml`,
    `https://www.${domain}/sitemap_index.xml`,
    `https://${domain}/wp-sitemap.xml`,
    `https://${domain}/sitemap/sitemap.xml`,
    `https://${domain}/sitemaps/sitemap.xml`,
    `https://${domain}/page-sitemap.xml`,
    `https://${domain}/post-sitemap.xml`,
  ];

  // robots.txt often has a Sitemap: directive pointing to the real path
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), PER_REQ_MS);
    const res = await fetch(`https://${domain}/robots.txt`, {
      signal: ctrl.signal as any,
      headers: { 'User-Agent': USER_AGENT },
    });
    clearTimeout(t);
    if (res.ok) {
      const txt = await res.text();
      const matches = [...txt.matchAll(/^Sitemap:\s*(https?:\/\/[^\s]+)/gim)];
      for (const m of matches) {
        const url = m[1].trim();
        if (!candidates.includes(url)) candidates.unshift(url);
      }
    }
  } catch { /* robots.txt optional */ }

  const found = new Set<string>();

  for (const sitemapUrl of candidates) {
    if (Date.now() - startTime > SITEMAP_BUDGET_MS) break;
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), PER_REQ_MS);
      const res = await fetch(sitemapUrl, {
        signal: controller.signal as any,
        headers: { 'User-Agent': USER_AGENT },
      });
      clearTimeout(timer);
      if (!res.ok) continue;

      const xml = await res.text();

      // Recurse into child sitemaps (sitemap index files)
      const childMatches = [...xml.matchAll(/<loc>\s*(https?:\/\/[^<]+\.xml[^<]*)\s*<\/loc>/gi)];
      for (const m of childMatches.slice(0, 30)) {
        if (Date.now() - startTime > SITEMAP_BUDGET_MS) break;
        const childUrl = m[1].trim();
        try {
          const ctrl2 = new AbortController();
          const t2 = setTimeout(() => ctrl2.abort(), PER_REQ_MS);
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

      [...xml.matchAll(/<loc>\s*(https?:\/\/[^<\s]+)\s*<\/loc>/gi)]
        .map(m => m[1].trim())
        .filter(u => !u.endsWith('.xml'))
        .forEach(u => found.add(u));

      if (found.size > 0) {
        console.log(`[crawler] Sitemap hit: ${sitemapUrl} → ${found.size} URLs`);
        break;
      }
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
        } catch { /* skip */ }
      });
    } catch { /* skip */ }
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

    if (isBlockedPage(title, html, res.status)) return null;

    const { score, signals, excerpt, wordCount, text } = scoreAeo(html, url);
    const contentHash = createHash('md5').update(html.substring(0, 50000)).digest('hex');
    const type = classifyType(url);
    const priority = priorityScore(url);
    const ci = commercialIntentScore(url);
    const aiRel = score >= 6 ? 3 : score >= 4 ? 2 : 1;

    return {
      url, title, statusCode: res.status,
      contentType: ct.split(';')[0].trim(),
      wordCount, aeoSignal: score, signals, excerpt, contentHash, changed: true, type, text,
      classification: {
        priority, action: score >= 7 ? 'optimize' : 'build', type,
        trustSignal: score >= 7 ? 1 : 0, commercialIntent: ci, aiRelevance: aiRel,
        reasons: signals,
      },
    };
  } catch {
    return null;
  }
}

// ── MAIN: Parallel crawl ─────────────────────────────────────────────

export async function crawlSite(options: CrawlOptions): Promise<CrawledPage[]> {
  const { startUrl, domain, maxPages, timeoutMs, batchSize = 25, maxBatches = 50, onProgress } = options;
  const normDomain = domain.replace(/^www\./, '');

  // Step 1: Sitemap discovery — owns its own 45s budget, does NOT eat into crawl time
  console.log(`[crawler] Discovering URLs for ${domain}…`);
  let allUrls = await discoverSitemap(normDomain);
  console.log(`[crawler] Sitemap: ${allUrls.length} URLs`);

  // FIX: threshold raised from <10 to <30. Sitemaps returning 5-25 URLs are valid small sites.
  if (allUrls.length < 30) {
    console.log(`[crawler] Sitemap thin (${allUrls.length}) — augmenting with BFS from ${startUrl}…`);
    const bfsUrls = await bfsDiscover(
      startUrl, domain,
      Math.min(maxPages * 2, 2000),
      Math.min(timeoutMs * 0.3, 45000)
    );
    console.log(`[crawler] BFS found ${bfsUrls.length} URLs`);
    const merged = new Set([...allUrls, ...bfsUrls]);
    allUrls = [...merged];
  }

  // Crawl deadline starts NOW — after sitemap discovery completes
  const deadline = Date.now() + timeoutMs;

  // Step 2: Filter + dedupe + priority sort
  const normalize = (u: string): string => {
    try {
      const p = new URL(u); p.hash = '';
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

  const deduped = [...new Set(allUrls.map(normalize))].filter(u => inDomain(u) && isContent(u));
  deduped.sort((a, b) => priorityScore(b) - priorityScore(a));

  if (!deduped.includes(normalize(startUrl))) deduped.unshift(normalize(startUrl));
  const urlsToFetch = deduped.slice(0, maxPages);

  console.log(`[crawler] Fetching ${urlsToFetch.length}/${deduped.length} URLs (batch=${batchSize})…`);

  // Step 3: Chunk into batches
  const chunks: string[][] = [];
  for (let i = 0; i < urlsToFetch.length; i += batchSize) {
    chunks.push(urlsToFetch.slice(i, i + batchSize));
  }

  // Step 4: Fire waves of concurrent batches
  const results: CrawledPage[] = [];

  for (let wave = 0; wave < chunks.length; wave += maxBatches) {
    if (Date.now() > deadline) {
      console.log(`[crawler] Deadline hit — stopping at ${results.length}/${urlsToFetch.length} pages`);
      break;
    }
    const waveBatches = chunks.slice(wave, wave + maxBatches);
    const waveResults = await Promise.all(
      waveBatches.map(batch => Promise.all(batch.map(url => fetchPage(url, 8000))))
    );
    const wavePages = waveResults.flat().filter((p): p is CrawledPage => p !== null);
    results.push(...wavePages);
    console.log(`[crawler] Wave ${Math.floor(wave / maxBatches) + 1}/${Math.ceil(chunks.length / maxBatches)}: ${results.length} pages`);
  }

  const confidence = results.length >= 75 ? 'high' : results.length >= 30 ? 'medium' : results.length >= 10 ? 'low' : 'insufficient';
  console.log(`[crawler] Done: ${results.length} pages — confidence: ${confidence}`);
  return results;
}
