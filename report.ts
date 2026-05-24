import type { reducePages } from './reducer.js';
import type { EngineResult } from './llm.js';
import type { CitationSimulation } from './citation.js';

export function buildReport(args: {
  job: any;
  summary: ReturnType<typeof reducePages>;
  llmPanel: EngineResult[];
  blended: { score: number; byEngine: Record<string, unknown>; enginesUsed: string[]; nullEngines: string[] };
  competitorSummaries?: any[];
  citationSimulation?: CitationSimulation;
  embeddingResult?: { enabled: boolean; stored: number };
}) {
  const bestData = args.llmPanel.find(r => r.ok && r.data && Object.keys(r.data).length)?.data || {};
  return {
    jobId: args.job.id,
    domain: args.job.domain,
    generatedAt: new Date().toISOString(),
    axoSnapshot: {
      aeoReadinessScore: args.blended.score,
      aeoScoreByEngine: args.blended.byEngine,
      enginesUsed: args.blended.enginesUsed,
      nullEngines: args.blended.nullEngines,
      pagesFetched: args.summary.pagesFetched,
      avgAeoSignal: args.summary.avgAeoSignal,
      changedPages: args.summary.changedPages.length,
      unchangedPages: args.summary.unchangedPages.length,
      citationProbability: args.citationSimulation?.citationProbability ?? null,
      embeddingsStored: args.embeddingResult?.stored ?? 0
    },
    intelligence: {
      companySummary: bestData.companySummary || '',
      buyerPersonas: bestData.buyerPersonas || [],
      topContentGaps: bestData.topContentGaps || [],
      missingFAQOpportunities: bestData.missingFAQOpportunities || [],
      buyerJourneyGaps: bestData.buyerJourneyGaps || {},
      quickWins: bestData.quickWins || [],
      schemaOpportunities: bestData.schemaOpportunities || []
    },
    citationSimulation: args.citationSimulation || null,
    embeddingLayer: args.embeddingResult || { enabled: false, stored: 0 },
    siteWideStats: {
      typeDistribution: args.summary.typeDistribution,
      topSignals: args.summary.topSignals,
      highAeo: args.summary.highAeo,
      midAeo: args.summary.midAeo,
      lowAeo: args.summary.lowAeo
    },
    topPages: args.summary.topPages.slice(0, 20).map(p => ({ url: p.url, title: p.title, aeoSignal: p.aeoSignal, type: p.contentType, signals: p.signals, priority: p.classification?.priority, action: p.classification?.action, changed: p.changed })),
    prioritizedPages: args.summary.prioritizedPages.slice(0, 20).map(p => ({ url: p.url, title: p.title, priority: p.classification?.priority, reasons: p.classification?.reasons, commercialIntent: p.classification?.commercialIntent, aiRelevance: p.classification?.aiRelevance })),
    gapPages: args.summary.gapPages.slice(0, 20).map(p => ({ url: p.url, title: p.title, aeoSignal: p.aeoSignal, type: p.contentType, signals: p.signals, priority: p.classification?.priority, changed: p.changed })),
    changedPages: args.summary.changedPages.slice(0, 20).map(p => ({ url: p.url, title: p.title, contentHash: p.contentHash, aeoSignal: p.aeoSignal, priority: p.classification?.priority })),
    competitors: args.competitorSummaries || []
  };
}
