import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { createHash } from 'crypto';
import { URL } from 'url';

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
  classification?: {
    priority?: number;
    action?: string;
    type?: string;
  };
  type?: string;
}

interface CrawlOptions {
  startUrl: string;
  domain: string;
  maxPages: number;
  depth: number;
  includeSubdomains: boolean;
  timeoutMs: number;
  concurrency: number;
  perHostConcurrency: number;
  // New parallel options
  batchSize?: number;       // pages per parallel worker (default 25)
  maxBatches?: number;      // max concurrent batches (default 50)
  sitemapFirst?: boolean;   // discover all URLs via sitemap before crawling
}

// ── Priority scorer ──────────────────────────────────────────────────
function priorityScore(url: string): number {
  const u = url.toLowerCase().replace(/^https?:\/\/[^/]+/, '');
  if (/\/(solutions?|services?|products?|platform|features?|capabilities)/.test(u)) return 100;
  if (/\/(vs|compare|alternatives?|pricing|roi|calculator)/.test(u)) return 95;
  if (/\/(case-stud|customer|success|results?|testimonial|proof)/.test(u)) return 90;
  if (/\/(contact|demo|trial|get-started|request)/.test(u)) return 88;
  if (/\/(about|team|leadership|company|mission|why)/.test(u)) return 70;
  if (/\/(blog|resources?|insights?|guides?|whitepapers?)(\/?$|\?|#)/.test(u)) return 75;
  if (/\/(blog|resources?|insights?)\/[^/]+/.test(u)) return 50;
  if (/\/(legal|privacy|terms|careers?|jobs?|press|events?\/\d{4})/.test(u)) return 5;
  if (/\/(tag|category|author)\//.test(u)) return 3;
  if (/\/page\/\d+/.test(u)) return 2;
  return 40;
}

function classifyUrl(url: string): string {
  const u = url.toLowerCase().replace(/^https?:\/\/[^/]+/, '');
  if (/\/(contact|demo|trial|get-started|request|sign-?up|free)/.test(u)) return 'conversion';
  if (/\/(blog|resources?|insights?|guides?|news|articles?)\/[^/]+/.test(u)) return 'article';
  return 'page';
}

// ── AEO signal scorer ────────────────────────────────────────────────
function scoreAeo(html: string, url: string): { score: number; signals: string[]; excerpt: string; wordCount: number } {
  const signals: string[] = [];
  const lower = html.toLowerCase();

  if (lower.includes('"@type": "faqpage"') || lower.includes('"@type":"faqpage"') || lower.includes('faqpage')) signals.push('faq_schema');
  if (/\b(best|top|leading|trusted|proven|certified|award)\b/.test(lower)) signals.push('proof_language');
  if (/\b(vs\.|versus|compare|better than|alternative|difference between)\b/.test(lower)) signals.push('comparison_language');
  if (/\b(how to|step.by.step|guide|tutorial|learn|understand|what is)\b/.test(lower)) signals.push('ai_language');
  if (/\b(contact|get started|demo|free trial|buy now|sign up|request)\b/.test(lower)) signals.push('decision_language');
  if (/<h[1-6]/i.test(html)) signals.push('heading_structure');

  // Word count from stripped text
  const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  const wordCount = text.split(' ').length;

  // Excerpt — first 200 chars of meaningful text
  const excerpt = text.substring(0, 200);

  // AEO signal score: 0-10
  const score = Math.min(10, (signals.length * 1.5) + (wordCount > 500 ? 1 : 0) + (wordCount > 1500 ? 0.5 : 0));

  return { score, signals, excerpt, wordCount };
}

// ── Discover all URLs from sitemap ───────────────────────────────────
async function discoverSitemap(domain: string, timeoutMs: number): Promise<string[]> {
  const sitemapUrls = [
    `https://${domain}/sitemap.xml`,
    `https://${domain}/sitemap_index.xml`,
    `https://${domain}/sitemap-index.xml`,
    `https://www.${domain}/sitemap.xml`,
  ];

  const found = new Set<string>();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(timeoutMs, 15000));

  for (const sitemapUrl of sitemapUrls) {
    try {
      const res = await fetch(sitemapUrl, { signal: controller.signal as any, headers: { 'User-Agent': 'AXO-Diagnostic/2.0' } });
      if (!res.ok) continue;
      const xml = await res.text();

      // Parse sitemap index — recurse into child sitemaps
      const childSitemaps = [...xml.matchAll(/<loc>\s*(https?:\/\/[^<]+\.xml[^<]*)\s*<\/loc>/gi)].map(m => m[1].trim());
      for (const child of childSitemaps.slice(0, 20)) {
        try {
          const childRes = await fetch(child, { signal: controller.signal as any, headers: { 'User-Agent': 'AXO-Diagnostic/2.0' } });
          if (!childRes.ok) continue;
          const childXml = await childRes.text();
          const childUrls = [...childXml.matchAll(/<loc>\s*(https?:\/\/[^<\s]+)\s*<\/loc>/gi)].map(m => m[1].trim());
          childUrls.forEach(u => found.add(u));
        } catch {}
      }

      // Parse direct URLs
      const directUrls = [...xml.matchAll(/<loc>\s*(https?:\/\/[^<\s]+)\s*<\/loc>/gi)]
        .map(m => m[1].trim())
        .filter(u => !u.endsWith('.xml'));
      directUrls.forEach(u => found.add(u));

      if (found.size > 0) break; // got URLs from first working sitemap
    } catch {}
  }

  clearTimeout(timer);
  return [...found];
}

// ── Fetch a single page ──────────────────────────────────────────────
async function fetchPage(url: string, timeoutMs = 8000): Promise<CrawledPage | null> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: controller.signal as any,
      headers: {
        'User-Agent': 'AXO-Diagnostic/2.0 (compatible; site analysis)',
        'Accept': 'text/html,application/xhtml+xml',
      },
      redirect: 'follow',
    });
    clearTimeout(timer);

    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('html')) return null;

    const html = await res.text();
    const $ = cheerio.load(html);

    const title = $('title').first().text().trim() || $('h1').first().text().trim() || url;
    const { score, signals, excerpt, wordCount } = scoreAeo(html, url);
    const contentHash = createHash('md5').update(html.substring(0, 50000)).digest('hex');
    const priority = priorityScore(url);
    const type = classifyUrl(url);

    return {
      url,
      title: title.substring(0, 200),
      statusCode: res.status,
      contentType: contentType.split(';')[0].trim(),
      wordCount,
      aeoSignal: score,
      signals,
      excerpt,
      contentHash,
      changed: true,
      type,
      classification: { priority, action: score >= 7 ? 'optimize' : 'build', type },
    };
  } catch {
    return null;
  }
}

// ── Crawl one page and extract links (for fallback BFS) ──────────────
async function fetchPageWithLinks(url: string, domain: string, timeoutMs = 8000): Promise<{ page: CrawledPage | null; links: string[] }> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: controller.signal as any,
      headers: { 'User-Agent': 'AXO-Diagnostic/2.0', 'Accept': 'text/html' },
      redirect: 'follow',
    });
    clearTimeout(timer);

    const contentType = res.headers.get('content-type') || '';
    if (!contentType.includes('html')) return { page: null, links: [] };

    const html = await res.text();
    const $ = cheerio.load(html);

    const title = $('title').first().text().trim() || url;
    const { score, signals, excerpt, wordCount } = scoreAeo(html, url);
    const contentHash = createHash('md5').update(html.substring(0, 50000)).digest('hex');
    const type = classifyUrl(url);

    const page: CrawledPage = {
      url,
      title: title.substring(0, 200),
      statusCode: res.status,
      contentType: contentType.split(';')[0].trim(),
      wordCount,
      aeoSignal: score,
      signals,
      excerpt,
      contentHash,
      changed: true,
      type,
      classification: { priority: priorityScore(url), action: score >= 7 ? 'optimize' : 'build', type },
    };

    // Extract same-domain links
    const base = new URL(url);
    const links: string[] = [];
    $('a[href]').each((_, el) => {
      try {
        const href = $(el).attr('href') || '';
        const resolved = new URL(href, base);
        if (resolved.hostname === base.hostname || resolved.hostname === `www.${domain}` || resolved.hostname === domain) {
          resolved.hash = '';
          resolved.search = resolved.search.replace(/[?&]utm[^&]*/gi, '').replace(/^[?&]/, '');
          const clean = resolved.toString();
          if (!clean.match(/\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|xml|json|zip|mp4|mp3|woff|woff2|ttf)(\?|$)/i)) {
            links.push(clean);
          }
        }
      } catch {}
    });

    return { page, links };
  } catch {
    return { page: null, links: [] };
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

  // Step 1: Discover all URLs via sitemap (fast, 5-15s)
  console.log(`[crawler] Discovering sitemap for ${domain}…`);
  let allUrls = await discoverSitemap(domain, Math.min(timeoutMs * 0.2, 15000));
  console.log(`[crawler] Sitemap yielded ${allUrls.length} URLs`);

  // Step 2: If sitemap empty/failed, do a BFS seed crawl to discover URLs
  if (allUrls.length < 10) {
    console.log(`[crawler] Sitemap thin — BFS discovery from ${startUrl}…`);
    const seedResult = await fetchPageWithLinks(startUrl, domain, 10000);
    const queue = [startUrl, ...(seedResult.links || [])];
    const bfsVisited = new Set<string>([startUrl]);
    const bfsFound: string[] = [startUrl];

    // BFS up to 3 levels to discover URLs when no sitemap
    for (let i = 0; i < Math.min(queue.length, 200) && Date.now() < deadline; i++) {
      const url = queue[i];
      if (!bfsVisited.has(url)) {
        bfsVisited.add(url);
        try {
          const r = await fetchPageWithLinks(url, domain, 5000);
          if (r.page) bfsFound.push(url);
          r.links.filter(l => !bfsVisited.has(l)).forEach(l => { queue.push(l); });
        } catch {}
      }
    }
    allUrls = bfsFound;
    console.log(`[crawler] BFS found ${allUrls.length} URLs`);
  }

  // Step 3: Filter to same domain, dedupe, priority sort
  const normalize = (u: string) => {
    try {
      const parsed = new URL(u);
      parsed.hash = '';
      return parsed.toString();
    } catch { return u; }
  };

  const domainFilter = (u: string) => {
    try {
      const h = new URL(u).hostname.replace(/^www\./, '');
      const d = domain.replace(/^www\./, '');
      return h === d || h.endsWith('.' + d);
    } catch { return false; }
  };

  const deduped = [...new Set(allUrls.map(normalize))].filter(domainFilter);

  // Sort by priority — highest value pages first
  deduped.sort((a, b) => priorityScore(b) - priorityScore(a));

  // Cap at maxPages
  const urlsToFetch = deduped.slice(0, maxPages);
  console.log(`[crawler] Fetching ${urlsToFetch.length} URLs (priority sorted, capped at ${maxPages})…`);

  // Step 4: Parallel batch fetch — batchSize pages per batch, maxBatches concurrent
  const results: CrawledPage[] = [];
  const chunks: string[][] = [];
  for (let i = 0; i < urlsToFetch.length; i += batchSize) {
    chunks.push(urlsToFetch.slice(i, i + batchSize));
  }

  // Process chunks in waves of maxBatches concurrent batches
  for (let wave = 0; wave < chunks.length; wave += maxBatches) {
    if (Date.now() > deadline) {
      console.log(`[crawler] Deadline reached after ${results.length} pages`);
      break;
    }
    const waveBatches = chunks.slice(wave, wave + maxBatches);
    const waveResults = await Promise.all(
      waveBatches.map(batch =>
        Promise.all(batch.map(url => fetchPage(url, 8000)))
      )
    );
    waveResults.flat().forEach(page => { if (page) results.push(page); });
    console.log(`[crawler] Wave ${Math.floor(wave / maxBatches) + 1}: ${results.length} pages fetched so far`);
  }

  console.log(`[crawler] Complete: ${results.length} pages in ${Math.round((Date.now() - (deadline - timeoutMs)) / 1000)}s`);
  return results;
}
