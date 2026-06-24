// ─────────────────────────────────────────────────────────────────────────
// presence.ts — Real AI Presence Measurement
// ─────────────────────────────────────────────────────────────────────────
// This module replaces every fabricated number in the AXO report with a
// measured one. It executes the buyer-question set against the live engines,
// detects whether the brand (and named competitors) actually appear in each
// answer, captures real citation URLs from retrieval engines, and aggregates
// the observations into the exact fields the report/renderer consume.
//
// Methodology honesty:
//   - Claude (API) and gpt-4o-mini answer from PARAMETRIC memory: no live
//     retrieval, no URLs. They measure brand SALIENCE in training data.
//   - Perplexity (sonar) and Gemini (grounded) RETRIEVE live and return real
//     citation URLs. Only these can produce a true "cited URL".
// Every observation carries its `mode` so the report can label what each
// engine actually measured. We never present parametric salience as citation.
// ─────────────────────────────────────────────────────────────────────────

import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { config } from './config.js';
import type { reducePages } from './reducer.js';

type Summary = ReturnType<typeof reducePages>;

// ── Types ──────────────────────────────────────────────────────────────

export type EngineMode = 'parametric' | 'retrieval';

export type QuerySpec = {
  id: string;        // "C04", "U11", etc. — matches appendix labels
  stage: string;     // unaware | aware | compare | consider | decide
  persona: string;   // persona this query is tagged to ('' if category-level)
  q: string;
};

export type PresenceObservation = {
  queryId: string;
  stage: string;
  persona: string;
  engine: string;
  mode: EngineMode;
  brandPresent: boolean;     // brand name/domain appears in the answer text
  prominence: number;        // 0 absent · 1 mentioned · 2 featured · 3 primary
  citedUrls: string[];       // real citation URLs (retrieval engines only)
  brandCited: boolean;       // a citedUrl host belongs to the brand
  competitorsNamed: string[];// competitor names/domains found in answer/citations
  answerExcerpt: string;     // audit trail — first ~240 chars of the answer
  ok: boolean;
  error?: string;
  ms: number;
};

export type EngineConfig = {
  engine: string;
  mode: EngineMode;
  // fraction of the query set to sample on this engine (1 = all 100)
  sample: number;
};

// Default panel. Retrieval engines carry the citation claim and are sampled
// fully; parametric engines are cheap and also run fully by default. Tune via
// AXO_PRESENCE_PANEL env if cost/runtime needs trimming.
export const DEFAULT_PANEL: EngineConfig[] = [
  { engine: 'claude',     mode: 'parametric', sample: 1.0 },
  { engine: 'openai',     mode: 'parametric', sample: 1.0 },
  { engine: 'perplexity', mode: 'retrieval',  sample: 1.0 },
  { engine: 'gemini',     mode: 'retrieval',  sample: 1.0 },
];

// ── Query spec builder ───────────────────────────────────────────────────
// Builds the 100-question set, stage-tagged AND persona-tagged. Persona tagging
// is what makes real per-persona / per-cluster aggregation possible later.
// Stage allocation is inverse-weighted to stage scores (weak stages sampled
// more heavily) — same intent as before, but now the queries are actually run.

export function buildQuerySpecs(
  domain: string,
  personas: string[],
  intelligence: {
    topContentGaps?: string[];
    missingFAQOpportunities?: string[];
    buyerJourneyGaps?: Record<string, string>;
  },
  competitorDomains: string[],
  stageScores: Record<string, number>
): QuerySpec[] {
  const dn = domain.split('.')[0];
  // Clean persona labels: the model emits "Role - descriptor"; keep the role,
  // drop the descriptor and any trailing dash so labels read cleanly.
  const P = (personas.length ? personas : ['buyers', 'decision makers'])
    .map(p => String(p).split(/\s+[-–—]\s+/)[0].replace(/[-–—\s]+$/, '').trim())
    .filter(Boolean);
  const comp0 = competitorDomains[0] ? competitorDomains[0].split('.')[0] : 'alternatives';
  const comp1 = competitorDomains[1] ? competitorDomains[1].split('.')[0] : 'in-house';
  const gaps = (intelligence.topContentGaps || []).map(String);
  const faqs = (intelligence.missingFAQOpportunities || []).map(String);

  // Question generators per stage. Each takes a persona so the question is
  // genuinely persona-specific where it matters (not a fixed services script).
  const banks: Record<string, (p: string) => string[]> = {
    unaware: (p) => [
      `What is ${dn} and what problem does it solve?`,
      `What does ${dn} do for ${p}?`,
      `Why would ${p} work with ${dn}?`,
      `What category of firm is ${dn}?`,
      `How does ${dn} work at a high level?`,
      `What outcomes do ${dn} clients achieve?`,
      `What markets and sectors does ${dn} serve?`,
      `What makes ${dn}'s approach distinct?`,
      gaps[0] ? `How does ${dn} address ${gaps[0].slice(0, 60)}?` : `What does ${dn} publish for ${p}?`,
    ],
    aware: (p) => [
      `What does ${dn} offer ${p}?`,
      `How does a ${p} engage ${dn}?`,
      `What is ${dn}'s track record with ${p}?`,
      `What is ${dn}'s investment or engagement approach?`,
      `Does ${dn} fit the needs of ${p}?`,
      `What does the ${dn} team and leadership look like?`,
      faqs[0] || `What is the first step for ${p} working with ${dn}?`,
    ],
    compare: (p) => [
      `${dn} vs ${comp0}: which is better for ${p}?`,
      `How does ${dn} compare to ${comp0}?`,
      `What makes ${dn} different from ${comp1}?`,
      `Why would ${p} choose ${dn} over ${comp0}?`,
      `What are the pros and cons of ${dn} vs ${comp0} for ${p}?`,
      `How does ${dn}'s track record compare to ${comp0}?`,
      `What does ${dn} offer that ${comp0} does not, for ${p}?`,
      gaps[1] ? `On ${gaps[1].slice(0, 50)}, how does ${dn} compare for ${p}?` : `Is ${dn} a stronger fit than ${comp0} for ${p}?`,
    ],
    consider: (p) => [
      `What results do ${dn} clients like ${p} actually get?`,
      `Is ${dn} trustworthy and proven for ${p}?`,
      `What proof points support ${dn}'s track record with ${p}?`,
      `Are there case studies from ${dn} relevant to ${p}?`,
      `How does ${dn} handle the needs of ${p} at scale?`,
      faqs[1] || `What risks should ${p} weigh when working with ${dn}?`,
    ],
    decide: (p) => [
      `How does ${p} start an engagement with ${dn}?`,
      `How does ${p} contact ${dn} to discuss needs?`,
      `What does onboarding with ${dn} look like for ${p}?`,
      `What does ${dn} need from ${p} to begin?`,
      `What is ${dn}'s engagement or contract structure?`,
    ],
  };

  const stages = ['unaware', 'aware', 'compare', 'consider', 'decide'];
  // Unique id prefix per stage. 'consider' uses V so it does not collide with
  // 'compare' (both start with C).
  const stagePrefix: Record<string, string> = {
    unaware: 'U', aware: 'A', compare: 'C', consider: 'V', decide: 'D',
  };
  const scores = stages.map(s => stageScores[s] ?? 50);
  const invW = scores.map(v => Math.max(5, 100 - v));
  const totalInv = invW.reduce((a, b) => a + b, 0);
  let counts = invW.map(w => Math.max(8, Math.floor((w / totalInv) * 100)));
  let rem = 100 - counts.reduce((a, b) => a + b, 0);
  for (let k = 0; k < stages.length && rem > 0; k++) { counts[k]++; rem--; }

  const specs: QuerySpec[] = [];
  stages.forEach((stage, si) => {
    const count = counts[si];
    const seen = new Set<string>();
    let made = 0;
    let persIdx = 0;
    // round-robin personas across this stage's queries so every persona is
    // tested at every stage (this is what powers real per-persona scoring)
    while (made < count) {
      const persona = P[persIdx % P.length];
      const pool = banks[stage](persona);
      for (let j = 0; j < pool.length && made < count; j++) {
        let q = pool[j];
        if (seen.has(q)) continue;
        seen.add(q);
        const id = stagePrefix[stage] + String(made + 1).padStart(2, '0');
        specs.push({ id, stage, persona, q });
        made++;
      }
      persIdx++;
      // safety: if persona pools exhausted before count, append benchmark variants
      if (persIdx > P.length * 6 && made < count) {
        const persona2 = P[made % P.length];
        const id = stagePrefix[stage] + String(made + 1).padStart(2, '0');
        specs.push({ id, stage, persona: persona2, q: `${banks[stage](persona2)[0].replace('?', '')} in 2026?` });
        made++;
      }
    }
  });

  return specs;
}

// ── Engine callers (raw answers + real citation capture) ──────────────────
// These ask the buyer question as a buyer would and return the natural answer.
// Retrieval engines additionally return the URLs they cited.

type RawAnswer = { text: string; citedUrls: string[] };

const ASK_SYSTEM =
  'You are answering a real buyer who is researching vendors. Answer naturally and ' +
  'specifically, naming the companies you would actually recommend or mention. ' +
  'Do not hedge by refusing to name companies. Keep it under 180 words.';

async function askClaude(q: string): Promise<RawAnswer> {
  const client = new Anthropic({ apiKey: config.anthropicApiKey });
  const resp = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 500,
    temperature: 0,
    system: ASK_SYSTEM,
    messages: [{ role: 'user', content: q }],
  });
  const text = resp.content.map((c: any) => (c.type === 'text' ? c.text : '')).join('\n');
  return { text, citedUrls: [] }; // parametric — no live URLs
}

async function askOpenAI(q: string): Promise<RawAnswer> {
  const client = new OpenAI({ apiKey: config.openaiApiKey });
  const resp = await client.chat.completions.create({
    model: 'gpt-4o-mini',
    max_tokens: 500,
    temperature: 0,
    messages: [{ role: 'system', content: ASK_SYSTEM }, { role: 'user', content: q }],
  });
  return { text: resp.choices[0]?.message?.content || '', citedUrls: [] }; // parametric
}

async function askPerplexity(q: string): Promise<RawAnswer> {
  const resp = await fetch('https://api.perplexity.ai/chat/completions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${config.perplexityApiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'sonar',
      max_tokens: 500,
      temperature: 0,
      messages: [{ role: 'system', content: ASK_SYSTEM }, { role: 'user', content: q }],
    }),
  });
  const json: any = await resp.json();
  const text = json.choices?.[0]?.message?.content || '';
  // sonar returns citations at the top level (array of URL strings)
  const citedUrls: string[] = Array.isArray(json.citations)
    ? json.citations.map((c: any) => String(c)).filter(Boolean)
    : Array.isArray(json.choices?.[0]?.message?.citations)
      ? json.choices[0].message.citations.map((c: any) => String(c)).filter(Boolean)
      : [];
  return { text, citedUrls };
}

async function askGemini(q: string): Promise<RawAnswer> {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${config.geminiApiKey}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: ASK_SYSTEM }] },
      contents: [{ role: 'user', parts: [{ text: q }] }],
      tools: [{ google_search: {} }],            // enable live grounding
      generationConfig: { temperature: 0, maxOutputTokens: 1200 },
    }),
  });
  const json: any = await resp.json();
  const cand = json.candidates?.[0];
  const text = cand?.content?.parts?.map((p: any) => p.text || '').join('\n') || '';
  // grounding metadata carries the real retrieved URLs
  const chunks = cand?.groundingMetadata?.groundingChunks || [];
  const citedUrls: string[] = chunks
    .map((c: any) => c?.web?.uri)
    .filter(Boolean)
    .map((u: string) => String(u));
  return { text, citedUrls };
}

function callerFor(engine: string): ((q: string) => Promise<RawAnswer>) | null {
  switch (engine) {
    case 'claude':     return config.anthropicApiKey ? askClaude : null;
    case 'openai':     return config.openaiApiKey ? askOpenAI : null;
    case 'perplexity': return config.perplexityApiKey ? askPerplexity : null;
    case 'gemini':     return config.geminiApiKey ? askGemini : null;
    default:           return null;
  }
}

// ── Brand / competitor detection ──────────────────────────────────────────

function hostOf(u: string): string {
  try { return new URL(u).hostname.replace(/^www\./, '').toLowerCase(); }
  catch { return String(u).toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0]; }
}

function buildMatcher(name: string): RegExp {
  // word-ish boundary, case-insensitive; escapes regex chars
  const esc = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^a-z0-9])${esc}([^a-z0-9]|$)`, 'i');
}

function detect(
  answer: RawAnswer,
  brand: { name: string; domain: string },
  competitors: Array<{ name: string; domain: string }>
): {
  brandPresent: boolean; prominence: number; brandCited: boolean; competitorsNamed: string[];
} {
  const text = answer.text || '';
  const lower = text.toLowerCase();
  const brandRoot = brand.domain.split('.')[0];
  const brandRe = buildMatcher(brand.name);
  const brandRootRe = buildMatcher(brandRoot);

  const brandPresent = brandRe.test(text) || brandRootRe.test(text);

  // prominence by first-occurrence position within the answer
  let prominence = 0;
  if (brandPresent) {
    const idx = Math.min(
      ...[lower.indexOf(brand.name.toLowerCase()), lower.indexOf(brandRoot.toLowerCase())]
        .filter(i => i >= 0)
    );
    const frac = text.length ? idx / text.length : 1;
    prominence = frac <= 0.15 ? 3 : frac <= 0.45 ? 2 : 1;
  }

  // real citation: a retrieved URL host matches the brand domain
  const brandCited = answer.citedUrls.some(u => {
    const h = hostOf(u);
    return h === brand.domain || h.endsWith('.' + brand.domain) || h.includes(brandRoot);
  });

  const competitorsNamed = competitors
    .filter(c => {
      const root = c.domain.split('.')[0];
      const inText = buildMatcher(c.name).test(text) || buildMatcher(root).test(text);
      const inCites = answer.citedUrls.some(u => {
        const h = hostOf(u);
        return h === c.domain || h.endsWith('.' + c.domain) || h.includes(root);
      });
      return inText || inCites;
    })
    .map(c => c.name);

  return { brandPresent, prominence, brandCited, competitorsNamed };
}

// ── Concurrency pool ──────────────────────────────────────────────────────

async function pool<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  const workers = new Array(Math.min(limit, items.length)).fill(0).map(async () => {
    while (true) {
      const i = next++;
      if (i >= items.length) break;
      out[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return out;
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`presence timeout ${ms}ms`)), ms);
    p.then(v => { clearTimeout(t); resolve(v); }, e => { clearTimeout(t); reject(e); });
  });
}

// ── Main measurement ──────────────────────────────────────────────────────

export async function measurePresence(args: {
  brand: { name: string; domain: string };
  competitors: Array<{ name: string; domain: string }>;
  specs: QuerySpec[];
  panel?: EngineConfig[];
  concurrency?: number;
  onProgress?: (done: number, total: number) => void;
}): Promise<PresenceObservation[]> {
  const panel = (args.panel || DEFAULT_PANEL).filter(e => callerFor(e.engine));
  const concurrency = args.concurrency ?? 6;
  const timeoutMs = config.llmTimeoutMs ?? 25000;

  // Build the full task list: (query × engine), honoring per-engine sampling.
  type Task = { spec: QuerySpec; engine: EngineConfig };
  const tasks: Task[] = [];
  for (const engine of panel) {
    const n = Math.max(1, Math.round(args.specs.length * engine.sample));
    // deterministic even sampling across the ordered spec list
    const step = args.specs.length / n;
    const picked = new Set<number>();
    for (let k = 0; k < n; k++) picked.add(Math.min(args.specs.length - 1, Math.floor(k * step)));
    for (const idx of picked) tasks.push({ spec: args.specs[idx], engine });
  }

  let done = 0;
  const total = tasks.length;

  const observations = await pool(tasks, concurrency, async ({ spec, engine }): Promise<PresenceObservation> => {
    const start = Date.now();
    const base = {
      queryId: spec.id, stage: spec.stage, persona: spec.persona,
      engine: engine.engine, mode: engine.mode,
    };
    try {
      const caller = callerFor(engine.engine)!;
      const answer = await withTimeout(caller(spec.q), timeoutMs);
      const d = detect(answer, args.brand, args.competitors);
      done++; args.onProgress?.(done, total);
      return {
        ...base,
        brandPresent: d.brandPresent,
        prominence: d.prominence,
        citedUrls: answer.citedUrls,
        brandCited: d.brandCited,
        competitorsNamed: d.competitorsNamed,
        answerExcerpt: (answer.text || '').slice(0, 240),
        ok: true,
        ms: Date.now() - start,
      };
    } catch (err: any) {
      done++; args.onProgress?.(done, total);
      return {
        ...base,
        brandPresent: false, prominence: 0, citedUrls: [], brandCited: false,
        competitorsNamed: [], answerExcerpt: '',
        ok: false, error: err?.message || String(err), ms: Date.now() - start,
      };
    }
  });

  return observations;
}

// ── Aggregation: turn observations into real report fields ─────────────────

const STAGES = ['unaware', 'aware', 'compare', 'consider', 'decide'];

function presenceValue(o: PresenceObservation): number {
  // 0..1 strength for averaging. Retrieval citation counts full; a parametric
  // mention counts by prominence. Absent = 0.
  if (!o.ok) return 0;
  if (o.mode === 'retrieval') {
    if (o.brandCited) return 1;
    return o.brandPresent ? 0.6 : 0; // named but not cited = partial
  }
  return o.prominence / 3; // parametric: 0, .33, .66, 1
}

function rate(obs: PresenceObservation[]): number {
  const ok = obs.filter(o => o.ok);
  if (!ok.length) return 0;
  return Math.round((ok.reduce((s, o) => s + presenceValue(o), 0) / ok.length) * 100);
}

// Headline AXO score. The product promises live AI visibility, so the headline
// weights retrieval (live citation) above parametric (training-data salience).
// This prevents two parametric engines that "know" the brand from masking a
// retrieval engine that never cites it. If only one mode ran, that mode stands
// alone. Weights: 65% retrieval / 35% parametric.
function headlineScore(ok: PresenceObservation[]): number {
  const para = ok.filter(o => o.mode === 'parametric');
  const retr = ok.filter(o => o.mode === 'retrieval');
  const paraRate = para.length ? rate(para) : null;
  const retrRate = retr.length ? rate(retr) : null;
  if (paraRate != null && retrRate != null) return Math.round(0.35 * paraRate + 0.65 * retrRate);
  return (retrRate ?? paraRate ?? 0);
}

export type PresenceReportFields = {
  // headline + per-engine (real)
  aeoPresenceScore: number;         // citation-weighted headline
  knowledgeScore: number;           // parametric engines only (training salience)
  citationScore: number;            // retrieval engines only (live citation)
  byEngine: Record<string, number>;
  engineModes: Record<string, EngineMode>;
  enginesUsed: string[];
  // stage presence (real) — replaces the fabricated 23/43/23/38/60
  stagePresence: Record<string, number>;
  // per-persona (real) — replaces the fabricated 82/54/22/10
  personaScores: Array<{ persona: string; score: number; queries: number }>;
  // per-persona × per-engine matrix (real) — replaces the 16 copied cells
  engineByPersona: Array<{ persona: string; byEngine: Record<string, number> }>;
  // real citation counts per URL — replaces 8/7/6/5/4 ordinals
  citationCounts: Array<{ url: string; citations: number }>;
  // competitive share of voice (real) — replaces fabricated competitor scores
  competitorShareOfVoice: Array<{ name: string; mentions: number; sharePct: number }>;
  // coverage / audit
  totalObservations: number;
  brandCitedCount: number;       // retrieval-engine citations of the brand
  retrievalCoverage: number;     // % of retrieval observations where brand cited
  measuredAt: string;
};

export function buildPresenceReportFields(
  observations: PresenceObservation[],
  brandDomain: string
): PresenceReportFields {
  const ok = observations.filter(o => o.ok);
  const engines = Array.from(new Set(ok.map(o => o.engine)));

  const byEngine: Record<string, number> = {};
  const engineModes: Record<string, EngineMode> = {};
  for (const e of engines) {
    byEngine[e] = rate(ok.filter(o => o.engine === e));
    engineModes[e] = ok.find(o => o.engine === e)?.mode || 'parametric';
  }

  const stagePresence: Record<string, number> = {};
  for (const s of STAGES) stagePresence[s] = rate(ok.filter(o => o.stage === s));

  const personas = Array.from(new Set(ok.map(o => o.persona).filter(Boolean)));
  const personaScores = personas.map(p => {
    const po = ok.filter(o => o.persona === p);
    return { persona: p, score: rate(po), queries: new Set(po.map(o => o.queryId)).size };
  }).sort((a, b) => b.score - a.score);

  const engineByPersona = personas.map(p => {
    const be: Record<string, number> = {};
    for (const e of engines) be[e] = rate(ok.filter(o => o.persona === p && o.engine === e));
    return { persona: p, byEngine: be };
  });

  // real citation counts: how many retrieval observations cited each brand URL
  const urlCounts = new Map<string, number>();
  const brandRoot = brandDomain.split('.')[0];
  for (const o of ok) {
    for (const u of o.citedUrls) {
      const h = hostOf(u);
      if (h === brandDomain || h.endsWith('.' + brandDomain) || h.includes(brandRoot)) {
        const key = u.split('#')[0].split('?')[0];
        urlCounts.set(key, (urlCounts.get(key) || 0) + 1);
      }
    }
  }
  const citationCounts = Array.from(urlCounts.entries())
    .map(([url, citations]) => ({ url, citations }))
    .sort((a, b) => b.citations - a.citations)
    .slice(0, 12);

  // competitive share of voice from real mentions across all observations
  const compCounts = new Map<string, number>();
  for (const o of ok) for (const c of o.competitorsNamed) compCounts.set(c, (compCounts.get(c) || 0) + 1);
  const totalMentions = Array.from(compCounts.values()).reduce((a, b) => a + b, 0) || 1;
  const competitorShareOfVoice = Array.from(compCounts.entries())
    .map(([name, mentions]) => ({ name, mentions, sharePct: Math.round((mentions / totalMentions) * 100) }))
    .sort((a, b) => b.mentions - a.mentions);

  const retrievalObs = ok.filter(o => o.mode === 'retrieval');
  const parametricObs = ok.filter(o => o.mode === 'parametric');
  const brandCitedCount = retrievalObs.filter(o => o.brandCited).length;
  const retrievalCoverage = retrievalObs.length
    ? Math.round((brandCitedCount / retrievalObs.length) * 100) : 0;

  const aeoPresenceScore = headlineScore(ok);
  const knowledgeScore = parametricObs.length ? rate(parametricObs) : 0;
  const citationScore = retrievalObs.length ? rate(retrievalObs) : 0;

  return {
    aeoPresenceScore,
    knowledgeScore,
    citationScore,
    byEngine,
    engineModes,
    enginesUsed: engines,
    stagePresence,
    personaScores,
    engineByPersona,
    citationCounts,
    competitorShareOfVoice,
    totalObservations: ok.length,
    brandCitedCount,
    retrievalCoverage,
    measuredAt: new Date().toISOString(),
  };
}

// ── Deterministic corpus analysis (real, from crawl — not from score) ──────
// Vocabulary coverage and content-format mix are NOT measured by querying
// engines; they are real properties of the crawled corpus. Computed here so
// the renderer stops deriving them from the AXO score.

const VOCAB_LABELS: Record<string, string> = {
  faq_schema:         'FAQ / direct-answer',
  proof_language:     'Proof language',
  comparison_language:'Comparison language',
  ai_language:        'Explanatory / how-to language',
  decision_language:  'Decision / CTA language',
  heading_structure:  'Heading structure',
};

export function computeVocabularyCoverage(summary: Summary): Array<{ term: string; signal: string; coveragePct: number }> {
  const denom = Math.max(1, summary.pagesFetched);
  return Object.keys(VOCAB_LABELS).map(sig => ({
    term: VOCAB_LABELS[sig],
    signal: sig,
    // real coverage = fraction of crawled pages carrying this signal
    coveragePct: Math.round(((summary.topSignals[sig] || 0) / denom) * 100),
  })).sort((a, b) => b.coveragePct - a.coveragePct);
}

export function vocabularyHeadline(summary: Summary): number {
  // honest single number: mean coverage across the six canonical terms
  const terms = computeVocabularyCoverage(summary);
  if (!terms.length) return 0;
  return Math.round(terms.reduce((s, t) => s + t.coveragePct, 0) / terms.length);
}

export function computeContentFormatMix(summary: Summary): Array<{ format: string; pct: number; count: number }> {
  // real distribution from the crawler's contentType classification + signals
  const total = Math.max(1, summary.pagesFetched);
  const td = summary.typeDistribution || {};
  const sig = summary.topSignals || {};
  const buckets: Record<string, number> = {
    'Explainer / guide':  (sig.ai_language || 0),
    'Case study / proof': (sig.proof_language || 0),
    'Comparison':         (sig.comparison_language || 0),
    'Conversion':         (td.conversion || 0),
    'FAQ / structured':   (sig.faq_schema || 0),
  };
  return Object.entries(buckets)
    .map(([format, count]) => ({ format, count, pct: Math.round((count / total) * 100) }))
    .sort((a, b) => b.pct - a.pct);
}
