import type { reducePages } from './reducer.js';
import type { EngineResult } from './llm.js';
import type { CitationSimulation } from './citation.js';
import {
  computeVocabularyCoverage,
  vocabularyHeadline,
  computeContentFormatMix,
  type PresenceReportFields,
  type PresenceObservation,
} from './presence.js';

// ── Query generation ──────────────────────────────────────────────────
// NOTE: This bank is retained only as a fallback list for display when the
// live presence measurement (presence.ts) did not run. When presence runs,
// the report's authoritative question set + results come from the measured
// observations (queryAudit), not from this generator.

function generateQueries(
  domain: string,
  score: number,
  intelligence: {
    buyerPersonas?: string[];
    topContentGaps?: string[];
    missingFAQOpportunities?: string[];
    buyerJourneyGaps?: Record<string, string>;
    quickWins?: string[];
  },
  competitorDomains: string[],
  stageScores: Record<string, number>
): Array<{ id: string; stage: string; q: string; engines: string[] }> {

  const dn = domain.split('.')[0];
  const personas = (intelligence.buyerPersonas || []).map(p => String(p));
  const faqs = (intelligence.missingFAQOpportunities || []).map(f => String(f));
  const gaps = (intelligence.topContentGaps || []).map(g => String(g));
  const bjg = intelligence.buyerJourneyGaps || {};
  const comp0 = competitorDomains[0] ? competitorDomains[0].split('.')[0] : 'competitors';
  const comp1 = competitorDomains[1] ? competitorDomains[1].split('.')[0] : 'alternatives';
  const p0 = personas[0] ? personas[0].split(' ').slice(0, 4).join(' ') : 'buyers';
  const p1 = personas[1] ? personas[1].split(' ').slice(0, 4).join(' ') : 'decision makers';

  const banks: Record<string, string[]> = {
    unaware: [
      `What is ${dn} and what problem does it solve?`,
      `What does ${dn} do for ${p0}?`,
      `Who uses ${dn} and why?`,
      `What category of service is ${dn}?`,
      `Why would a ${p0} need ${dn}?`,
      `How does ${dn} work at a high level?`,
      `What outcomes do ${dn} customers achieve?`,
      `What is the core value proposition of ${dn}?`,
      `What problems does ${dn} solve that others cannot?`,
      `What markets does ${dn} serve?`,
      `How is AI changing how buyers research solutions like ${dn}?`,
      `What does ${dn} automate for ${p0}?`,
      `Is ${dn} right for enterprise organizations?`,
      `What is the business model of ${dn}?`,
      `What are the main use cases for ${dn}?`,
      `How does ${dn} help teams save time and reduce costs?`,
      `What makes ${dn} different from traditional approaches?`,
      `What does a ${p0} gain from working with ${dn}?`,
      `How long has ${dn} been in this space?`,
      `What industries does ${dn} specialize in?`,
      gaps[0] ? `How does ${dn} address: ${gaps[0].substring(0, 60)}?` : `What thought leadership does ${dn} produce?`,
      gaps[1] ? `What content does ${dn} publish about: ${gaps[1].substring(0, 50)}?` : `Does ${dn} publish research or benchmarks?`,
      `What trends is ${dn} tracking in this industry?`,
      `How does ${dn} define success for its customers?`,
      `What is ${dn}'s approach to ${p1} challenges?`,
    ],
    aware: [
      `What services does ${dn} offer?`,
      `How do I get started with ${dn}?`,
      `What does ${dn} cost and how is it priced?`,
      `Does ${dn} offer a free assessment or trial?`,
      `How does ${dn} onboard new clients?`,
      `What support does ${dn} provide during and after engagement?`,
      `Is ${dn} the right fit for a ${p0}?`,
      `How long does a typical ${dn} engagement take?`,
      `What does the ${dn} team look like?`,
      `Does ${dn} work with mid-market companies?`,
      `What certifications or partnerships does ${dn} hold?`,
      `How does ${dn} handle data security and compliance?`,
      `Does ${dn} integrate with existing technology stacks?`,
      faqs[0] || `What is the first step to working with ${dn}?`,
      faqs[1] || `How does ${dn} measure success?`,
      `What does ${dn} need from a client to get started?`,
      `Does ${dn} have a specific methodology or framework?`,
      `How does ${dn} staff engagements?`,
      `What is ${dn}'s geographic reach?`,
    ],
    compare: [
      `${dn} vs ${comp0} — which is better for ${p0}?`,
      `How does ${dn} compare to ${comp0}?`,
      `What makes ${dn} different from ${comp1}?`,
      `${dn} pricing vs ${comp0} — what is the cost difference?`,
      `Is ${dn} worth it compared to doing this in-house?`,
      `What are the pros and cons of ${dn} vs ${comp0}?`,
      `Why choose ${dn} over ${comp1}?`,
      `How does ${dn} score on analyst rankings vs competitors?`,
      `What features does ${dn} have that ${comp0} lacks?`,
      `${dn} vs ${comp0}: which delivers faster ROI?`,
      `How does ${dn} approach this differently than ${comp1}?`,
      `What do analysts say about ${dn} vs the competitive set?`,
      gaps[2] ? `Does ${dn} offer: ${gaps[2].substring(0, 60)}?` : `Does ${dn} have a stronger methodology than ${comp0}?`,
      `Which companies switch from ${comp0} to ${dn} and why?`,
      `What is the switching cost from ${comp0} to ${dn}?`,
      `How do ${dn} and ${comp0} differ in their approach to ${p0}?`,
      `${dn} vs ${comp0}: implementation timeline comparison`,
      `What do ${p0} prefer about ${dn} over alternatives?`,
      `How does ${dn}'s track record compare to ${comp0}?`,
      `What is unique about ${dn} that no competitor offers?`,
      bjg.consideration ? bjg.consideration.substring(0, 100) + '?' : `Why do buyers choose ${dn} at the evaluation stage?`,
      `${dn} vs ${comp0}: support and ongoing services comparison`,
      `Is ${dn} a better long-term investment than ${comp0}?`,
      `What do G2 or Gartner reviews say about ${dn}?`,
    ],
    consider: [
      `${dn} customer reviews from ${p0}`,
      `Is ${dn} trustworthy and reliable?`,
      `What results do ${dn} clients actually get?`,
      `How fast does ${dn} deliver measurable ROI?`,
      `What do existing ${dn} customers say about the experience?`,
      `Is ${dn} a good long-term investment?`,
      faqs[2] || `What is ${dn}'s customer retention rate?`,
      `How does ${dn} handle issues or underperformance?`,
      `Are there case studies from ${dn} for ${p0}?`,
      `What is the NPS or satisfaction score for ${dn}?`,
      `How does ${dn} handle enterprise-scale requirements?`,
      `What SLAs or guarantees does ${dn} offer?`,
      gaps[3] ? `What proof does ${dn} have for: ${gaps[3].substring(0, 60)}?` : `What is ${dn}'s most impressive client outcome?`,
      `How transparent is ${dn} about results and reporting?`,
      `What do ${p1} say about working with ${dn}?`,
      `Does ${dn} have references or referrals available?`,
      `How does ${dn} ensure quality control?`,
      `What risks should I be aware of when working with ${dn}?`,
      bjg.awareness ? bjg.awareness.substring(0, 100) + '?' : `What is ${dn} known for in the industry?`,
    ],
    decide: [
      `How do I start an engagement with ${dn}?`,
      `How do I contact ${dn} to discuss my needs?`,
      `What is ${dn}'s contract structure and terms?`,
      `How do I request a proposal from ${dn}?`,
      `What does onboarding look like in the first 30 days with ${dn}?`,
      `How long does it take to go live with ${dn}?`,
      `What is the procurement process for engaging ${dn}?`,
      `Does ${dn} offer pilot or phased engagements?`,
      `What approvals do I need internally to move forward with ${dn}?`,
      `How does ${dn} handle contract renewals and expansions?`,
      bjg.decision ? bjg.decision.substring(0, 100) + '?' : `What is the fastest way to get started with ${dn}?`,
      `What information does ${dn} need to prepare a proposal?`,
      `Does ${dn} have a standard engagement template or is everything custom?`,
    ],
  };

  const stages = ['unaware', 'aware', 'compare', 'consider', 'decide'];
  const scores = stages.map(s => stageScores[s] || 50);
  const invWeights = scores.map(v => Math.max(5, 100 - v));
  const totalInv = invWeights.reduce((a, b) => a + b, 0);
  let counts = invWeights.map(w => Math.max(8, Math.floor((w / totalInv) * 100)));
  let rem = 100 - counts.reduce((a, b) => a + b, 0);
  for (let k = 0; k < stages.length && rem > 0; k++) { counts[k]++; rem--; }

  const queries: Array<{ id: string; stage: string; q: string; engines: string[] }> = [];

  stages.forEach((stage, si) => {
    const count = counts[si];
    const pool = banks[stage] || [];
    const used = new Set<string>();

    for (let j = 0; j < count; j++) {
      const id = stage.charAt(0).toUpperCase() + String(j + 1).padStart(2, '0');
      let q = '';
      if (j < pool.length) {
        q = pool[j];
      } else {
        const contexts = ['for enterprise organizations', 'for mid-market companies', 'in 2026', 'for ' + p0, 'compared to industry benchmarks'];
        q = pool[j % pool.length].replace('?', '') + ' ' + contexts[Math.floor(j / pool.length) % contexts.length] + '?';
      }
      if (used.has(q)) q = q.replace('?', ' (follow-up)?');
      used.add(q);
      queries.push({ id, stage, q, engines: ['claude', 'openai'] });
    }
  });

  return queries;
}

// ── Stage score derivation ────────────────────────────────────────────
// Heuristic stage scores from buyer-journey gap text. Used ONLY to weight
// query sampling. When live presence runs, real stagePresence overrides these
// in the report output. Exported so the worker weights sampling identically.

export function deriveStageScores(
  buyerJourneyGaps: Record<string, string>,
  overallScore: number
): Record<string, number> {
  const scoreText = (text: string): number => {
    if (!text) return 40;
    const t = text.toLowerCase();
    let s = 50;
    ['missing', 'absent', 'gap', 'weak', 'poor', 'limited', 'lacks', 'thin', 'insufficient', 'no ', 'minimal'].forEach(w => {
      if (t.includes(w)) s -= 10;
    });
    ['strong', 'well', 'good', 'cited', 'present', 'effective', 'extensive', 'comprehensive'].forEach(w => {
      if (t.includes(w)) s += 8;
    });
    return Math.max(10, Math.min(90, s));
  };

  return {
    unaware:  Math.max(10, Math.min(90, scoreText(buyerJourneyGaps.awareness || ''))),
    aware:    Math.max(10, Math.min(90, scoreText(buyerJourneyGaps.awareness || ''))),
    compare:  Math.max(10, Math.min(90, scoreText(buyerJourneyGaps.consideration || ''))),
    consider: Math.max(10, Math.min(90, scoreText(buyerJourneyGaps.consideration || ''))),
    decide:   Math.max(10, Math.min(90, scoreText(buyerJourneyGaps.decision || ''))),
  };
}

// ── Cluster builder ───────────────────────────────────────────────────
// Clusters map personas to their highest-gap stage. When presence is measured,
// each cluster's strength and query count are overwritten with REAL values
// (see enrichClustersWithPresence below) — no fabricated/random numbers.

function buildClusters(
  intelligence: {
    buyerPersonas?: string[];
    topContentGaps?: string[];
    missingFAQOpportunities?: string[];
    buyerJourneyGaps?: Record<string, string>;
  },
  stageScores: Record<string, number>,
  competitorDomains: string[]
): Array<{
  name: string;
  strength: number | null;
  queryMatches: number | null;
  stages: Record<string, string>;
  gaps: Array<{ ttl: string }>;
}> {
  const personas = (intelligence.buyerPersonas || []).slice(0, 5);
  const gaps = (intelligence.topContentGaps || []).slice(0, 10);
  const faqs = (intelligence.missingFAQOpportunities || []).slice(0, 5);

  const stages = ['unaware', 'aware', 'compare', 'consider', 'decide'];
  const stageWeakness = stages.map(s => ({ stage: s, score: stageScores[s] || 50 }));
  stageWeakness.sort((a, b) => a.score - b.score);

  return personas.map((persona, i) => {
    const primaryStage = stageWeakness[i % stageWeakness.length].stage;
    const secondaryStage = stageWeakness[(i + 1) % stageWeakness.length].stage;

    const clusterGaps = gaps
      .filter((_, gi) => gi % personas.length === i || gi % (personas.length + 1) === i)
      .slice(0, 3)
      .map(g => ({ ttl: g }));

    if (faqs[i]) clusterGaps.push({ ttl: faqs[i] });

    const stageMap: Record<string, string> = {};
    stageMap[primaryStage] = 'hot';
    stageMap[secondaryStage] = 'warm';

    return {
      name: persona,
      // null until presence measurement fills these in — never fabricated
      strength: null,
      queryMatches: null,
      stages: stageMap,
      gaps: clusterGaps,
    };
  });
}

// Overwrite cluster strength + query counts with REAL measured presence.
function enrichClustersWithPresence(
  clusters: ReturnType<typeof buildClusters>,
  presence: PresenceReportFields | null
) {
  if (!presence) return clusters;
  const byPersona = new Map(presence.personaScores.map(p => [p.persona, p]));
  return clusters.map(c => {
    // match cluster persona to the measured persona (exact, then prefix)
    const exact = byPersona.get(c.name);
    const fuzzy = exact || presence.personaScores.find(p =>
      p.persona.toLowerCase().startsWith(String(c.name).toLowerCase().slice(0, 12)) ||
      String(c.name).toLowerCase().startsWith(p.persona.toLowerCase().slice(0, 12))
    );
    return {
      ...c,
      strength: fuzzy ? fuzzy.score : c.strength,
      queryMatches: fuzzy ? fuzzy.queries : c.queryMatches,
    };
  });
}

// ── Main report builder ───────────────────────────────────────────────

export function buildReport(args: {
  job: any;
  summary: ReturnType<typeof reducePages>;
  llmPanel: EngineResult[];
  blended: { score: number; byEngine: Record<string, unknown>; enginesUsed: string[]; nullEngines: string[] };
  competitorSummaries?: any[];
  citationSimulation?: CitationSimulation;
  embeddingResult?: { enabled: boolean; stored: number };
  crawlPageCount?: number;
  presence?: PresenceReportFields | null;          // REAL measured presence
  observations?: PresenceObservation[];            // raw, for the audit appendix
}) {
  const bestData = args.llmPanel.find(r => r.ok && r.data && Object.keys(r.data).length)?.data || {};

  const intelligence = {
    companyName: bestData.companyName || '',
    companySummary: bestData.companySummary || '',
    buyerPersonas: bestData.buyerPersonas || [],
    topContentGaps: bestData.topContentGaps || [],
    missingFAQOpportunities: bestData.missingFAQOpportunities || [],
    buyerJourneyGaps: bestData.buyerJourneyGaps || {},
    quickWins: bestData.quickWins || [],
    schemaOpportunities: bestData.schemaOpportunities || [],
  };

  const competitorSummaries = (args.competitorSummaries || []).filter(c => c.domain && c.score != null);
  const competitorDomains = competitorSummaries.map(c => String(c.domain));

  const stageScores = deriveStageScores(intelligence.buyerJourneyGaps, args.blended.score);

  const queries = generateQueries(
    args.job.domain, args.blended.score, intelligence, competitorDomains, stageScores
  );

  const presence = args.presence || null;

  let clusters = buildClusters(intelligence, stageScores, competitorDomains);
  clusters = enrichClustersWithPresence(clusters, presence);

  const crawlPageCount = args.crawlPageCount ?? args.summary.pagesFetched;
  const crawlConfidence = crawlPageCount >= 75 ? 'high'
    : crawlPageCount >= 30 ? 'medium'
    : crawlPageCount >= 10 ? 'low'
    : 'insufficient';

  return {
    jobId: args.job.id,
    domain: args.job.domain,
    generatedAt: new Date().toISOString(),
    axoSnapshot: {
      // headline score: AI VISIBILITY (measured, crawl-gated). Falls back to
      // crawl-analysis blend only when presence did not run.
      aeoReadinessScore: presence ? presence.aiVisibilityScore : args.blended.score,
      aiVisibilityScore: presence ? presence.aiVisibilityScore : args.blended.score,
      // Brand fame, reported separately — NOT part of the headline.
      brandAwarenessScore: presence ? presence.brandAwarenessScore : null,
      liveCitationRate: presence ? presence.liveCitationRate : null,
      vocabularyCoveragePct: presence ? presence.vocabularyCoveragePct : vocabularyHeadline(args.summary),
      corpusHealthScore: presence ? presence.corpusHealthScore : args.blended.score,
      analysisScore: args.blended.score,                 // crawl-analysis blend (context)
      aeoScoreByEngine: presence ? presence.byEngine : args.blended.byEngine,
      engineModes: presence ? presence.engineModes : null,
      enginesUsed: presence ? presence.enginesUsed : args.blended.enginesUsed,
      nullEngines: args.blended.nullEngines,
      pagesFetched: args.summary.pagesFetched,
      avgAeoSignal: args.summary.avgAeoSignal,
      changedPages: args.summary.changedPages.length,
      unchangedPages: args.summary.unchangedPages.length,
      citationProbability: args.citationSimulation?.citationProbability ?? null,
      embeddingsStored: args.embeddingResult?.stored ?? 0,
      crawlPageCount,
      crawlConfidence,
      presenceMeasured: !!presence,
    },
    intelligence,
    stageScores,

    // ── REAL measured fields (null when measurement did not run) ────────
    presence,
    stagePresence: presence ? presence.stagePresence : null,
    vocabulary: {
      headlinePct: vocabularyHeadline(args.summary),
      terms: computeVocabularyCoverage(args.summary),
    },
    contentFormatMix: computeContentFormatMix(args.summary),
    citationCounts: presence ? presence.citationCounts : [],
    competitorShareOfVoice: presence ? presence.competitorShareOfVoice : [],
    engineByPersona: presence ? presence.engineByPersona : [],
    // Auditable: real questions asked + which engines answered + the result.
    queryAudit: (args.observations || []).map(o => ({
      id: o.queryId, stage: o.stage, persona: o.persona, engine: o.engine,
      mode: o.mode, brandPresent: o.brandPresent, brandCited: o.brandCited,
      prominence: o.prominence, competitorsNamed: o.competitorsNamed,
    })),

    clusters,
    // fallback display list only; appendix should render from queryAudit when present
    queries,
    citationSimulation: args.citationSimulation || null,
    embeddingLayer: args.embeddingResult || { enabled: false, stored: 0 },
    siteWideStats: {
      typeDistribution: args.summary.typeDistribution,
      topSignals: args.summary.topSignals,
      highAeo: args.summary.highAeo,
      midAeo: args.summary.midAeo,
      lowAeo: args.summary.lowAeo,
    },
    topPages: args.summary.topPages.slice(0, 20).map(p => ({
      url: p.url, title: p.title, aeoSignal: p.aeoSignal,
      type: p.contentType, signals: p.signals,
      priority: p.classification?.priority,
      action: p.classification?.action, changed: p.changed,
    })),
    prioritizedPages: args.summary.prioritizedPages.slice(0, 20).map(p => ({
      url: p.url, title: p.title,
      priority: p.classification?.priority,
      reasons: p.classification?.reasons,
      commercialIntent: p.classification?.commercialIntent,
      aiRelevance: p.classification?.aiRelevance,
    })),
    gapPages: args.summary.gapPages.slice(0, 20).map(p => ({
      url: p.url, title: p.title, aeoSignal: p.aeoSignal,
      type: p.contentType, signals: p.signals,
      priority: p.classification?.priority, changed: p.changed,
    })),
    changedPages: args.summary.changedPages.slice(0, 20).map(p => ({
      url: p.url, title: p.title,
      contentHash: p.contentHash, aeoSignal: p.aeoSignal,
      priority: p.classification?.priority,
    })),
    competitors: competitorSummaries,
  };
}
