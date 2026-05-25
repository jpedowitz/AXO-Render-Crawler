import { Worker } from 'bullmq';
import { config, requireConfig } from '../config.js';
import { redis } from '../queue.js';
import { one, query } from '../db.js';
import { crawlSite, type CrawledPage } from '../crawler.js';
import { reducePages, buildCompactPrompt } from '../reducer.js';
import { runLLMPanel } from '../llm.js';
import { blendScores, deterministicScore } from '../scorer.js';
import { buildReport } from '../report.js';
import { simulateCitationReadiness } from '../citation.js';
import { maybeStorePageEmbeddings } from '../embeddings.js';
import { event, updateJob } from '../api/jobService.js';

requireConfig();

// ── Competitor discovery via Claude ───────────────────────────────────
// Called after LLM panel completes. Uses Claude to identify real industry
// competitors based on the domain, buyer personas, and site summary.
// No static lists, no fallbacks — always live from Claude.

async function findCompetitors(
  domain: string,
  personas: string[],
  siteSummary: string
): Promise<Array<{ name: string; domain: string }>> {
  try {
    const personaText = personas.slice(0, 3).join(', ');
    const summaryText = siteSummary.substring(0, 600);

    const prompt = `You are a competitive intelligence analyst. Identify the 4 most relevant direct competitors to this company.

Company domain: ${domain}
Buyer personas: ${personaText}
Site context: ${summaryText}

Rules:
- Competitors must operate in the same industry and serve the same buyer personas
- Do not include the company itself (${domain})
- Do not include generic consulting firms (Accenture, Deloitte, McKinsey etc.) unless this company IS a consulting firm
- Do not include software testing tools unless this company IS a software testing company
- Return real companies with real public websites
- If you are not confident about a competitor, omit it rather than guessing

Return ONLY a valid JSON array with exactly this shape, nothing else:
[
  {"name": "CompanyName", "domain": "companydomain.com"},
  {"name": "CompanyName2", "domain": "companydomain2.com"}
]`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': config.anthropicApiKey,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        model: 'claude-opus-4-5',
        max_tokens: 512,
        temperature: 0,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    if (!response.ok) {
      console.warn(`[findCompetitors] Claude API ${response.status}`);
      return [];
    }

    const data = await response.json() as any;
    const text = (data.content?.[0]?.text || '[]').replace(/```json|```/g, '').trim();

    // Extract JSON array even if Claude adds preamble
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) return [];

    const parsed = JSON.parse(match[0]);
    if (!Array.isArray(parsed)) return [];

    return parsed
      .filter((c: any) => c && typeof c.domain === 'string' && typeof c.name === 'string')
      .map((c: any) => ({
        name: String(c.name).trim(),
        domain: String(c.domain).trim().replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '').toLowerCase(),
      }))
      .filter((c: any) => c.domain && c.domain !== domain && !c.domain.includes(' '))
      .slice(0, 4);

  } catch (err: any) {
    console.warn(`[findCompetitors] failed: ${err?.message}`);
    return [];
  }
}

// ── Page cache annotation ─────────────────────────────────────────────

async function annotatePageChangesAndUpdateCache(domain: string, pages: CrawledPage[]) {
  const batchSize = 100;
  for (let offset = 0; offset < pages.length; offset += batchSize) {
    const batch = pages.slice(offset, offset + batchSize);
    const urls = batch.map(p => p.url);
    const existing = await query<{ url: string; content_hash: string }>(
      `select url, content_hash from axo_page_cache where domain = $1 and url = any($2::text[])`,
      [domain, urls]
    );
    const hashByUrl = new Map(existing.map(r => [r.url, r.content_hash]));
    for (const page of batch) {
      const previous = hashByUrl.get(page.url);
      page.changed = previous ? previous !== page.contentHash : true;
    }

    const values: unknown[] = [];
    const placeholders = batch.map((p, i) => {
      const base = i * 9;
      values.push(domain, p.url, p.contentHash, p.title, p.contentType, p.wordCount, p.aeoSignal, JSON.stringify(p.classification || {}), p.excerpt);
      return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9})`;
    }).join(',');

    await query(
      `insert into axo_page_cache (domain, url, content_hash, title, content_type, word_count, aeo_signal, classification, excerpt)
       values ${placeholders}
       on conflict (domain, url) do update set
         content_hash = excluded.content_hash,
         title = excluded.title,
         content_type = excluded.content_type,
         word_count = excluded.word_count,
         aeo_signal = excluded.aeo_signal,
         classification = excluded.classification,
         excerpt = excluded.excerpt,
         last_seen_at = now()`,
      values
    );
  }
}

// ── Bulk page upsert ──────────────────────────────────────────────────

async function bulkUpsertPages(jobId: string, pages: CrawledPage[]) {
  const batchSize = 50;
  for (let offset = 0; offset < pages.length; offset += batchSize) {
    const batch = pages.slice(offset, offset + batchSize);
    const values: unknown[] = [];
    const placeholders = batch.map((p, i) => {
      const base = i * 13;
      values.push(
        jobId,
        p.url,
        p.title,
        p.statusCode,
        p.contentType,
        p.wordCount,
        p.aeoSignal,
        JSON.stringify(p.signals),
        p.excerpt,
        JSON.stringify({ priority: p.classification?.priority, action: p.classification?.action, changed: p.changed }),
        p.contentHash,
        p.changed ?? null,
        JSON.stringify(p.classification || {})
      );
      return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13})`;
    }).join(',');

    await query(
      `insert into axo_pages (job_id, url, title, status_code, content_type, word_count, aeo_signal, signals, excerpt, raw, content_hash, changed, classification)
       values ${placeholders}
       on conflict (job_id, url) do update set
         title = excluded.title,
         status_code = excluded.status_code,
         content_type = excluded.content_type,
         word_count = excluded.word_count,
         aeo_signal = excluded.aeo_signal,
         signals = excluded.signals,
         excerpt = excluded.excerpt,
         raw = excluded.raw,
         content_hash = excluded.content_hash,
         changed = excluded.changed,
         classification = excluded.classification`,
      values
    );
  }
}

// ── Main diagnostic pipeline ──────────────────────────────────────────

async function runDiagnostic(jobId: string) {
  const job = await one<any>('select * from axo_jobs where id = $1', [jobId]);
  if (!job) throw new Error(`Job not found: ${jobId}`);

  // ── Phase 1: Crawl ────────────────────────────────────────────────
  await updateJob(jobId, { status: 'running', stage: 'crawling' });
  await event(jobId, 'crawl.started', { domain: job.domain });

  const pages = await crawlSite({
    startUrl: job.start_url,
    domain: job.domain,
    maxPages: job.max_pages,
    depth: job.depth,
    includeSubdomains: job.include_subdomains,
    timeoutMs: config.crawlTimeoutMs,
    concurrency: config.crawlConcurrency,
    perHostConcurrency: config.crawlPerHostConcurrency,
  });

  await annotatePageChangesAndUpdateCache(job.domain, pages);
  await bulkUpsertPages(jobId, pages);

  const summary = reducePages(pages);
  const citationSimulation = simulateCitationReadiness(summary);

  await query(
    `insert into axo_citation_simulations (job_id, citation_probability, answerability_score, trust_score, semantic_completeness, simulation)
     values ($1,$2,$3,$4,$5,$6)
     on conflict (job_id) do update set
       citation_probability=excluded.citation_probability,
       answerability_score=excluded.answerability_score,
       trust_score=excluded.trust_score,
       semantic_completeness=excluded.semantic_completeness,
       simulation=excluded.simulation,
       created_at=now()`,
    [jobId, citationSimulation.citationProbability, citationSimulation.answerabilityScore, citationSimulation.trustScore, citationSimulation.semanticCompleteness, JSON.stringify(citationSimulation)]
  );

  const embeddingResult = await maybeStorePageEmbeddings(jobId, pages);

  await event(jobId, 'crawl.completed', {
    pagesFetched: summary.pagesFetched,
    avgAeoSignal: summary.avgAeoSignal,
    changedPages: summary.changedPages.length,
    citationProbability: citationSimulation.citationProbability,
    embeddings: embeddingResult,
  });

  // ── Phase 2: LLM scoring ──────────────────────────────────────────
  await updateJob(jobId, { stage: 'scoring' });
  const prompt = buildCompactPrompt(job.domain, summary);
  const llmPanel = await runLLMPanel(prompt);
  const blended = blendScores(summary, llmPanel);

  // ── Phase 3: Competitor discovery (Claude) + crawl ────────────────
  // User-specified competitors take priority. If none provided, ask Claude
  // to identify real industry competitors based on personas and site context.
  const userSpecifiedCompetitors: string[] = Array.isArray(job.competitors) ? job.competitors : [];

  let competitorDomains = userSpecifiedCompetitors;

  if (competitorDomains.length === 0) {
    await updateJob(jobId, { stage: 'competitor_discovery' });
    const intelligence = llmPanel.intelligence || {};
    const personas: string[] = Array.isArray(intelligence.buyerPersonas) ? intelligence.buyerPersonas : [];
    const siteSummary = [
      intelligence.companySummary || '',
      (intelligence.topContentGaps || []).slice(0, 3).join('. '),
      (intelligence.quickWins || []).slice(0, 2).join('. '),
    ].filter(Boolean).join(' ');

    const discovered = await findCompetitors(job.domain, personas, siteSummary);
    competitorDomains = discovered.map(c => c.domain);
    console.log(`[worker] Claude identified ${competitorDomains.length} competitors for ${job.domain}: ${competitorDomains.join(', ')}`);
  }

  // ── Phase 4: Competitor crawl + scoring ───────────────────────────
  let competitorSummaries: any[] = [];

  if (competitorDomains.length > 0) {
    await updateJob(jobId, { stage: 'competitor_scoring' });
    competitorSummaries = await Promise.all(
      competitorDomains.slice(0, 5).map(async (compDomain: string) => {
        try {
          await query(
            `insert into axo_competitors (job_id, domain, status) values ($1,$2,'running')
             on conflict (job_id, domain) do update set status='running'`,
            [jobId, compDomain]
          );

          const compPages = await Promise.race([
            crawlSite({
              startUrl: `https://${compDomain}`,
              domain: compDomain,
              maxPages: job.competitor_limit,
              depth: 3,
              includeSubdomains: false,
              timeoutMs: config.crawlTimeoutMs,
              concurrency: Math.min(config.crawlConcurrency, 15),
              perHostConcurrency: config.crawlPerHostConcurrency,
            }),
            new Promise<never>((_, reject) =>
              setTimeout(() => reject(new Error(`Competitor crawl timeout: ${compDomain}`)), 45000)
            ),
          ]);

          await annotatePageChangesAndUpdateCache(compDomain, compPages);
          const compSummary = reducePages(compPages);
          const compScore = deterministicScore(compSummary);

          await query(
            `update axo_competitors set status='complete', pages_fetched=$3, summary=$4, score=$5, completed_at=now()
             where job_id=$1 and domain=$2`,
            [jobId, compDomain, compSummary.pagesFetched, JSON.stringify(compSummary), compScore]
          );

          return { domain: compDomain, score: compScore, pagesFetched: compSummary.pagesFetched, avgAeoSignal: compSummary.avgAeoSignal };
        } catch (err: any) {
          console.log(`[competitor] ${compDomain} failed: ${err?.message}`);
          await query(
            `update axo_competitors set status='failed', completed_at=now() where job_id=$1 and domain=$2`,
            [jobId, compDomain]
          );
          return { domain: compDomain, score: 0, pagesFetched: 0, avgAeoSignal: 0, error: err?.message };
        }
      })
    );
  }

  // ── Phase 5: Build and store report ──────────────────────────────
  await updateJob(jobId, { stage: 'reporting' });
  const report = buildReport({ job, summary, llmPanel, blended, competitorSummaries, citationSimulation, embeddingResult });

  await query(
    `insert into axo_results (job_id, score, scores_by_engine, engines_used, report)
     values ($1,$2,$3,$4,$5)
     on conflict (job_id) do update set
       score=excluded.score,
       scores_by_engine=excluded.scores_by_engine,
       engines_used=excluded.engines_used,
       report=excluded.report,
       generated_at=now()`,
    [jobId, blended.score, JSON.stringify(blended.byEngine), JSON.stringify(blended.enginesUsed), JSON.stringify(report)]
  );

  await updateJob(jobId, { status: 'complete', stage: 'complete', completedAt: new Date() });
  await event(jobId, 'report.completed', { score: blended.score, enginesUsed: blended.enginesUsed });

  // Notify n8n if webhook configured
  if (config.n8nWebhookUrl) {
    await fetch(config.n8nWebhookUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'axo.report.completed', jobId, domain: job.domain, score: blended.score }),
    }).catch(() => undefined);
  }

  return report;
}

// ── BullMQ worker ─────────────────────────────────────────────────────

new Worker('axo-diagnostic', async bullJob => {
  const { jobId } = bullJob.data as { jobId: string };
  try {
    return await runDiagnostic(jobId);
  } catch (err: any) {
    await updateJob(jobId, { status: 'failed', stage: 'failed', error: err?.message || String(err) });
    await event(jobId, 'job.failed', { error: err?.message || String(err) });
    throw err;
  }
}, { connection: redis, concurrency: config.jobConcurrency });

console.log(`[AXO worker] running with concurrency=${config.jobConcurrency}`);
