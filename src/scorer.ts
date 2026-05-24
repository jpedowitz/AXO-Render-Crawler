import type { reducePages } from './reducer.js';
import type { EngineResult } from './llm.js';

export function deterministicScore(summary: ReturnType<typeof reducePages>) {
  const pages = summary.pagesFetched || 0;
  let score = 18;
  score += Math.min(30, summary.avgAeoSignal * 3);
  score += Math.min(10, Math.log10(Math.max(1, pages)) * 5);
  score += Math.min(10, (summary.highAeo / Math.max(1, pages)) * 50);
  score += Math.min(6, (summary.midAeo / Math.max(1, pages)) * 20);
  score += Math.min(8, (summary.topSignals.faq_schema || 0) * 1);
  score += Math.min(6, (summary.topSignals.structured_data || 0) * 0.6);
  let ceiling = 88;
  if (pages < 25) ceiling = 45;
  else if (pages < 50) ceiling = 52;
  else if (pages < 150) ceiling = 62;
  else if (pages < 500) ceiling = 72;
  else if (pages < 1000) ceiling = 80;
  return Math.max(10, Math.min(ceiling, Math.round(score)));
}

export function blendScores(summary: ReturnType<typeof reducePages>, panel: EngineResult[]) {
  const llmScores = panel.filter(r => r.ok && typeof r.score === 'number').map(r => r.score as number);
  const byEngine = Object.fromEntries(panel.map(r => [r.engine, r.score ?? null]));
  if (!llmScores.length) {
    const fallback = deterministicScore(summary);
    return { score: fallback, byEngine: { deterministic: fallback, ...byEngine }, enginesUsed: ['deterministic'], nullEngines: panel.map(r => r.engine) };
  }
  const avg = Math.round(llmScores.reduce((a, b) => a + b, 0) / llmScores.length);
  return { score: avg, byEngine, enginesUsed: panel.filter(r => r.ok && r.score != null).map(r => r.engine), nullEngines: panel.filter(r => !r.ok || r.score == null).map(r => r.engine) };
}
