import crypto from 'node:crypto';
import type { CrawledPage } from './crawler.js';

export type UrlPriority = {
  priority: number;
  urlType: string;
  reasons: string[];
};

export type PageClassification = UrlPriority & {
  action: 'score' | 'summarize' | 'ignore';
  commercialIntent: number;
  aiRelevance: number;
  trustSignal: number;
  uniquenessHint: number;
};

const LOW_VALUE_PATTERNS = [
  /\/privacy/i,
  /\/terms/i,
  /\/legal/i,
  /\/cookie/i,
  /\/careers?/i,
  /\/jobs?/i,
  /\/events?\/page\//i,
  /\/tag\//i,
  /\/author\//i,
  /\/category\//i,
  /\.(pdf|jpg|jpeg|png|gif|webp|svg|zip|docx?|xlsx?|pptx?)$/i
];

const HIGH_VALUE_PATTERNS = [
  { re: /pricing|plans|cost|roi|calculator/i, type: 'pricing', weight: 40 },
  { re: /compare|comparison|versus|\bvs\b|alternative|competitor/i, type: 'comparison', weight: 38 },
  { re: /case-stud|customer-story|customers|results|outcomes/i, type: 'proof', weight: 34 },
  { re: /solutions?|services?|platform|product/i, type: 'solution', weight: 30 },
  { re: /faq|questions|answers/i, type: 'faq', weight: 28 },
  { re: /demo|contact|consult|assessment|diagnostic/i, type: 'conversion', weight: 26 },
  { re: /blog|article|guide|resources|insights/i, type: 'educational', weight: 16 }
];

export function classifyUrlPriority(url: string, depth = 0): UrlPriority {
  const path = safePath(url);
  const reasons: string[] = [];

  if (LOW_VALUE_PATTERNS.some(re => re.test(path))) {
    return { priority: Math.max(1, 10 - depth), urlType: 'low_value', reasons: ['low-value url pattern'] };
  }

  let priority = Math.max(10, 80 - depth * 8);
  let urlType = 'page';
  for (const rule of HIGH_VALUE_PATTERNS) {
    if (rule.re.test(path)) {
      priority += rule.weight;
      urlType = rule.type;
      reasons.push(rule.type);
    }
  }

  if (depth === 0) {
    priority += 30;
    reasons.push('seed page');
  }
  if (path.split('/').filter(Boolean).length <= 1) {
    priority += 10;
    reasons.push('top-level page');
  }

  return { priority: clamp(priority, 1, 150), urlType, reasons };
}

export function classifyFetchedPage(page: Pick<CrawledPage, 'url' | 'title' | 'excerpt' | 'wordCount' | 'signals' | 'contentType' | 'text'>): PageClassification {
  const base = classifyUrlPriority(page.url, 0);
  const body = `${page.url} ${page.title} ${page.excerpt}`.toLowerCase();
  const reasons = [...base.reasons];

  const commercialIntent = scoreTerms(body, [
    'pricing', 'cost', 'roi', 'demo', 'contact sales', 'implementation', 'platform', 'services', 'solution', 'assessment', 'diagnostic'
  ]);
  const aiRelevance = scoreTerms(body, [
    'ai', 'artificial intelligence', 'agent', 'automation', 'generative', 'llm', 'answer engine', 'seo', 'aeo', 'axo'
  ]);
  const trustSignal = scoreTerms(body, [
    'case study', 'customer', 'results', 'outcomes', 'security', 'certified', 'gartner', 'forrester', 'award', 'testimonial'
  ]);

  let priority = base.priority;
  priority += commercialIntent * 8;
  priority += aiRelevance * 5;
  priority += trustSignal * 5;
  priority += Math.min(15, (page.signals || []).length * 3);
  if (page.wordCount >= 700) priority += 10;
  else if (page.wordCount < 150) priority -= 15;

  let action: PageClassification['action'] = 'score';
  if (LOW_VALUE_PATTERNS.some(re => re.test(page.url)) || page.contentType === 'non_html') action = 'ignore';
  else if (page.wordCount < 150 && commercialIntent < 2 && trustSignal < 2) action = 'summarize';

  if (commercialIntent >= 2) reasons.push('commercial intent');
  if (aiRelevance >= 2) reasons.push('ai relevance');
  if (trustSignal >= 2) reasons.push('trust/proof');

  return {
    priority: clamp(priority, 1, 175),
    urlType: base.urlType,
    reasons: [...new Set(reasons)],
    action,
    commercialIntent,
    aiRelevance,
    trustSignal,
    uniquenessHint: normalizedContentHash(page.title + ' ' + page.excerpt).length ? Math.min(10, Math.round((new Set((page.excerpt || '').toLowerCase().split(/\W+/).filter(Boolean)).size / 40) * 10)) : 0
  };
}

export function normalizedContentHash(text: string): string {
  const normalized = String(text || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^a-z0-9\s]/g, '')
    .trim();
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

function scoreTerms(text: string, terms: string[]): number {
  return terms.reduce((score, term) => score + (text.includes(term) ? 1 : 0), 0);
}

function safePath(url: string) {
  try {
    const u = new URL(url);
    return `${u.pathname} ${u.search}`.toLowerCase();
  } catch {
    return String(url || '').toLowerCase();
  }
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, Math.round(n)));
}
