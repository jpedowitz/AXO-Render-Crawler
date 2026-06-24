import type { CrawledPage } from './crawler.js';

export function reducePages(pages: CrawledPage[]) {
  const scorable = pages.filter(p => p.classification?.action !== 'ignore');
  const sorted = [...scorable].sort((a, b) => (b.classification?.priority || 0) - (a.classification?.priority || 0) || b.aeoSignal - a.aeoSignal || b.wordCount - a.wordCount);
  const recommendedPagesForLLM = sorted.slice(0, 100);
  const gapPages = [...scorable].sort((a, b) => a.aeoSignal - b.aeoSignal || (b.classification?.commercialIntent || 0) - (a.classification?.commercialIntent || 0)).slice(0, 50);
  const typeDistribution = countBy(pages, p => p.contentType);
  const topSignals = countSignals(pages);
  const avgAeoSignal = pages.length ? Number((pages.reduce((s, p) => s + p.aeoSignal, 0) / pages.length).toFixed(1)) : 0;
  const prioritizedPages = sorted.slice(0, 50);
  const changedPages = pages.filter(p => p.changed === true);
  const unchangedPages = pages.filter(p => p.changed === false);
  return {
    pagesFetched: pages.length,
    pagesFailed: 0,
    totalDiscovered: pages.length,
    recommendedPagesForLLM,
    prioritizedPages,
    changedPages,
    unchangedPages,
    topPages: sorted.slice(0, 25),
    gapPages,
    typeDistribution,
    topSignals,
    avgAeoSignal,
    highAeo: pages.filter(p => p.aeoSignal >= 7).length,
    midAeo: pages.filter(p => p.aeoSignal >= 4 && p.aeoSignal < 7).length,
    lowAeo: pages.filter(p => p.aeoSignal < 4).length
  };
}

function countBy<T>(items: T[], fn: (item: T) => string) {
  return items.reduce<Record<string, number>>((acc, item) => {
    const k = fn(item);
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});
}

function countSignals(pages: CrawledPage[]) {
  const out: Record<string, number> = {};
  for (const p of pages) for (const s of p.signals) out[s] = (out[s] || 0) + 1;
  return out;
}

export function buildCompactPrompt(domain: string, summary: ReturnType<typeof reducePages>) {
  const top = summary.recommendedPagesForLLM.slice(0, 12).map(p => ({
    title: p.title.slice(0, 90),
    url: p.url,
    type: p.contentType,
    aeo: p.aeoSignal,
    priority: p.classification?.priority || 0,
    action: p.classification?.action || 'score',
    signals: p.signals,
    excerpt: p.excerpt.slice(0, 260)
  }));

  const changed = summary.changedPages.slice(0, 10).map(p => ({
    title: p.title.slice(0, 90),
    url: p.url,
    type: p.contentType,
    aeo: p.aeoSignal,
    excerpt: p.excerpt.slice(0, 180)
  }));

  return `You are an AEO/AXO readiness scorer. Analyze this website crawl data and return ONLY a JSON object with no markdown, no explanation, no code fences, just raw JSON starting with { and ending with }.

Domain: ${domain}
Pages fetched: ${summary.pagesFetched}
Avg AEO signal: ${summary.avgAeoSignal}
High AEO pages: ${summary.highAeo}
Mid AEO pages: ${summary.midAeo}
Low AEO pages: ${summary.lowAeo}
Top signals: ${JSON.stringify(summary.topSignals)}
Changed pages: ${summary.changedPages.length}

Top pages:
${top.map(p => `- ${p.title} (aeo:${p.aeo}, signals:${p.signals.join(',')})\n  excerpt: ${p.excerpt}`).join('\n')}

${changed.length ? `Changed pages since last scan:\n${changed.map(p => `- ${p.title} (aeo:${p.aeo})`).join('\n')}` : ''}

Return this exact JSON structure with no other text:
{
  "companyName": "<the official organization name as a buyer would say it, e.g. 'Kimmeridge Energy' not the bare domain>",
  "aeoReadinessScore": <number 0-100>,
  "rationale": "<2 sentences explaining the score>",
  "quickWins": ["<win1>", "<win2>", "<win3>", "<win4>", "<win5>"],
  "topContentGaps": ["<gap1>", "<gap2>", "<gap3>", "<gap4>", "<gap5>"],
  "missingFAQOpportunities": ["<question1>", "<question2>", "<question3>"],
  "buyerPersonas": ["<persona1>", "<persona2>", "<persona3>", "<persona4>"],
  "schemaOpportunities": ["<opportunity1>", "<opportunity2>", "<opportunity3>"],
  "buyerJourneyGaps": {
    "awareness": "<gap>",
    "consideration": "<gap>",
    "decision": "<gap>"
  }
}`;
}
