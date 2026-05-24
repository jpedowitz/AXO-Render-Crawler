import * as cheerio from 'cheerio';
import { classifyFetchedPage, classifyUrlPriority, normalizedContentHash, type PageClassification } from './pageClassifier.js';

export type CrawlOptions = {
  startUrl: string;
  domain: string;
  maxPages: number;
  depth: number;
  includeSubdomains: boolean;
  timeoutMs: number;
  concurrency?: number;
  perHostConcurrency?: number;
};

export type CrawledPage = {
  url: string;
  statusCode: number;
  title: string;
  text: string;
  wordCount: number;
  contentType: string;
  signals: string[];
  aeoSignal: number;
  excerpt: string;
  contentHash: string;
  classification: PageClassification;
  changed?: boolean;
};

type QueueItem = { url: string; depth: number };

type HostLease = { release: () => void };

export async function crawlSite(opts: CrawlOptions): Promise<CrawledPage[]> {
  const concurrency = Math.max(1, Number(opts.concurrency || 20));
  const perHostConcurrency = Math.max(1, Number(opts.perHostConcurrency || 5));

  const seen = new Set<string>();
  const queued = new Set<string>();
  const queue: QueueItem[] = [];
  const pages: CrawledPage[] = [];
  const activeByHost = new Map<string, number>();

  const start = normalizeUrl(opts.startUrl);
  if (start) {
    queue.push({ url: start, depth: 0 });
    queued.add(start);
  }

  async function acquireHost(url: string): Promise<HostLease> {
    const host = hostKey(url);
    while ((activeByHost.get(host) || 0) >= perHostConcurrency) {
      await sleep(20);
    }
    activeByHost.set(host, (activeByHost.get(host) || 0) + 1);
    return {
      release: () => {
        const next = Math.max(0, (activeByHost.get(host) || 1) - 1);
        if (next === 0) activeByHost.delete(host);
        else activeByHost.set(host, next);
      }
    };
  }

  async function processItem(item: QueueItem): Promise<void> {
    if (pages.length >= opts.maxPages) return;

    const normalized = normalizeUrl(item.url);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    if (!isAllowed(normalized, opts.domain, opts.includeSubdomains)) return;

    let lease: HostLease | null = null;
    try {
      lease = await acquireHost(normalized);
      const page = await fetchPage(normalized, opts.timeoutMs);
      if (pages.length < opts.maxPages) pages.push(page);

      if (item.depth < opts.depth && pages.length < opts.maxPages) {
        for (const link of extractLinks(page.text, normalized)) {
          if (queue.length + seen.size >= opts.maxPages * 8) break;
          if (!seen.has(link) && !queued.has(link) && isAllowed(link, opts.domain, opts.includeSubdomains)) {
            queued.add(link);
            queue.push({ url: link, depth: item.depth + 1 });
          }
        }
      }
    } catch {
      // Ignore individual page failures; the reducer handles partial crawls.
    } finally {
      if (lease) lease.release();
    }
  }

  while (queue.length && pages.length < opts.maxPages) {
    queue.sort((a, b) => classifyUrlPriority(b.url, b.depth).priority - classifyUrlPriority(a.url, a.depth).priority);
    const batch = queue.splice(0, concurrency);
    await Promise.allSettled(batch.map(processItem));
  }

  return pages.slice(0, opts.maxPages);
}

async function fetchPage(url: string, timeoutMs: number): Promise<CrawledPage> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const res = await fetch(url, {
    signal: controller.signal,
    redirect: 'follow',
    headers: { 'user-agent': 'TPG-AXO-Diagnostic/2.0 (+https://www.pedowitzgroup.com)' }
  });
  clearTimeout(timer);

  const contentTypeHeader = res.headers.get('content-type') || '';
  if (!contentTypeHeader.includes('text/html') && !contentTypeHeader.includes('application/xhtml')) {
    const emptyPage = {
      url,
      statusCode: res.status,
      title: url,
      text: '',
      wordCount: 0,
      contentType: 'non_html',
      signals: [],
      aeoSignal: 0,
      excerpt: '',
      contentHash: normalizedContentHash(url),
      classification: undefined as unknown as PageClassification
    };
    emptyPage.classification = classifyFetchedPage(emptyPage);
    return emptyPage;
  }

  const html = await res.text();
  const $ = cheerio.load(html);
  $('script, style, noscript, svg').remove();
  const title = $('title').text().trim() || $('h1').first().text().trim() || url;
  const bodyText = $('body').text().replace(/\s+/g, ' ').trim();
  const wordCount = bodyText ? bodyText.split(/\s+/).length : 0;
  const signals = detectSignals($, bodyText, html);
  const contentType = classifyContent(url, title, bodyText);
  const aeoSignal = scoreAeo(signals, wordCount, contentType);
  const excerpt = bodyText.slice(0, 600);
  const pageBase = { url, statusCode: res.status, title, text: html, wordCount, contentType, signals, aeoSignal, excerpt };
  const contentHash = normalizedContentHash(title + ' ' + bodyText.slice(0, 12000));
  const classification = classifyFetchedPage(pageBase);
  return { ...pageBase, contentHash, classification };
}

function detectSignals($: cheerio.CheerioAPI, text: string, html: string): string[] {
  const signals = new Set<string>();
  if ($('script[type="application/ld+json"]').length) signals.add('structured_data');
  if (/FAQPage|Question|Answer/i.test(html)) signals.add('faq_schema');
  if (/\b(vs\.?|compare|comparison|alternative|competitor)\b/i.test(text)) signals.add('comparison_language');
  if (/\b(pricing|cost|plans|roi|calculator)\b/i.test(text)) signals.add('decision_language');
  if (/\b(case study|customer story|results|outcomes)\b/i.test(text)) signals.add('proof_language');
  if (/\b(ai|automation|agent|generative|LLM|machine learning)\b/i.test(text)) signals.add('ai_language');
  if ($('h1').length && $('h2').length) signals.add('heading_structure');
  return [...signals];
}

function classifyContent(url: string, title: string, text: string): string {
  const s = `${url} ${title} ${text.slice(0, 300)}`.toLowerCase();
  if (/case-study|customer-story|case study/.test(s)) return 'case_study';
  if (/pricing|plans|cost/.test(s)) return 'pricing';
  if (/blog|article|insight|guide/.test(s)) return 'article';
  if (/faq|questions/.test(s)) return 'faq';
  if (/comparison|compare|vs|alternative/.test(s)) return 'comparison';
  if (/demo|contact|consultation/.test(s)) return 'conversion';
  return 'page';
}

function scoreAeo(signals: string[], wordCount: number, contentType: string): number {
  let score = 0;
  score += Math.min(4, signals.length * 0.75);
  if (wordCount > 600) score += 2;
  else if (wordCount > 250) score += 1;
  if (['faq', 'comparison', 'case_study', 'pricing'].includes(contentType)) score += 2;
  if (signals.includes('structured_data')) score += 1;
  if (signals.includes('faq_schema')) score += 1;
  return Math.max(0, Math.min(10, Number(score.toFixed(1))));
}

function normalizeUrl(url: string): string | null {
  try {
    const u = new URL(url);
    if (!['http:', 'https:'].includes(u.protocol)) return null;
    u.hash = '';
    // Remove common tracking params so the crawler does not waste budget.
    for (const key of [...u.searchParams.keys()]) {
      if (/^utm_|^fbclid$|^gclid$|^msclkid$/i.test(key)) u.searchParams.delete(key);
    }
    if (u.pathname.endsWith('/')) u.pathname = u.pathname.slice(0, -1) || '/';
    return u.toString();
  } catch { return null; }
}

function isAllowed(url: string, domain: string, includeSubdomains: boolean): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase().replace(/^www\./, '');
    return includeSubdomains ? host.endsWith(domain) : host === domain;
  } catch { return false; }
}

function extractLinks(html: string, baseUrl: string): string[] {
  const $ = cheerio.load(html);
  const links: string[] = [];
  $('a[href]').each((_, a) => {
    const href = $(a).attr('href');
    if (!href || href.startsWith('mailto:') || href.startsWith('tel:') || href.startsWith('#')) return;
    try {
      const parsed = new URL(href, baseUrl);
      if (!['http:', 'https:'].includes(parsed.protocol)) return;
      links.push(parsed.toString());
    } catch {}
  });
  return [...new Set(links)].slice(0, 2000);
}

function hostKey(url: string): string {
  try { return new URL(url).hostname.toLowerCase(); } catch { return 'unknown'; }
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
