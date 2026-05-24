import type { CrawledPage } from './crawler.js';
import type { reducePages } from './reducer.js';

export type CitationSimulation = {
  citationProbability: number;
  answerabilityScore: number;
  trustScore: number;
  semanticCompleteness: number;
  likelyCitedPages: Array<{ url: string; title: string; probability: number; reasons: string[] }>;
  gaps: string[];
};

export function simulateCitationReadiness(summary: ReturnType<typeof reducePages>): CitationSimulation {
  const candidates = summary.recommendedPagesForLLM
    .filter(p => p.classification?.action !== 'ignore')
    .slice(0, 30)
    .map(p => scorePageCitation(p))
    .sort((a, b) => b.probability - a.probability);

  const top = candidates.slice(0, 10);
  const citationProbability = top.length ? Math.round(top.reduce((s, p) => s + p.probability, 0) / Math.min(5, top.length)) : 0;
  const answerabilityScore = scoreDimension(summary, ['faq_schema', 'heading_structure'], ['faq', 'article', 'page']);
  const trustScore = scoreDimension(summary, ['proof_language', 'structured_data'], ['case_study', 'pricing', 'comparison']);
  const semanticCompleteness = Math.round((summary.highAeo * 2 + summary.midAeo) / Math.max(1, summary.pagesFetched) * 100);

  const gaps: string[] = [];
  if ((summary.topSignals.faq_schema || 0) < 3) gaps.push('Add FAQ schema and direct-answer sections to top commercial pages.');
  if ((summary.topSignals.structured_data || 0) < 5) gaps.push('Expand structured data coverage across product, service, proof, and comparison pages.');
  if ((summary.typeDistribution.comparison || 0) < 2) gaps.push('Create named comparison and alternatives pages so answer engines can confidently differentiate the brand.');
  if ((summary.typeDistribution.case_study || 0) < 2) gaps.push('Add more proof-oriented customer outcome pages with measurable results.');

  return {
    citationProbability: clamp(citationProbability, 0, 100),
    answerabilityScore: clamp(answerabilityScore, 0, 100),
    trustScore: clamp(trustScore, 0, 100),
    semanticCompleteness: clamp(semanticCompleteness, 0, 100),
    likelyCitedPages: top,
    gaps
  };
}

function scorePageCitation(page: CrawledPage): { url: string; title: string; probability: number; reasons: string[] } {
  const reasons: string[] = [];
  let score = 12;
  if (page.signals.includes('structured_data')) { score += 16; reasons.push('structured data'); }
  if (page.signals.includes('faq_schema')) { score += 18; reasons.push('FAQ schema'); }
  if (page.signals.includes('heading_structure')) { score += 10; reasons.push('clear heading structure'); }
  if (page.signals.includes('proof_language')) { score += 10; reasons.push('proof language'); }
  if (page.signals.includes('comparison_language')) { score += 9; reasons.push('comparison language'); }
  if (page.wordCount >= 700) { score += 10; reasons.push('substantial content'); }
  if (['faq', 'comparison', 'case_study', 'pricing'].includes(page.contentType)) { score += 9; reasons.push(`${page.contentType} page type`); }
  if (page.classification?.trustSignal && page.classification.trustSignal >= 2) { score += 8; reasons.push('trust indicators'); }
  if (page.classification?.commercialIntent && page.classification.commercialIntent >= 2) { score += 5; reasons.push('commercial relevance'); }
  return { url: page.url, title: page.title, probability: clamp(score, 0, 100), reasons };
}

function scoreDimension(summary: ReturnType<typeof reducePages>, signals: string[], types: string[]): number {
  const signalPoints = signals.reduce((sum, s) => sum + (summary.topSignals[s] || 0), 0);
  const typePoints = types.reduce((sum, t) => sum + (summary.typeDistribution[t] || 0), 0);
  return Math.round(Math.min(100, ((signalPoints * 8) + (typePoints * 5) + (summary.avgAeoSignal * 6))));
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, Math.round(n)));
}
