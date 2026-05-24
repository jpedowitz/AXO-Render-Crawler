import { Worker } from 'bullmq';
import { config, requireConfig } from '../config.js';
import { redis } from '../queue.js';
import { one, query } from '../db.js';
import { crawlSite, type CrawledPage } from '../crawler.js';
import { reducePages, buildCompactPrompt } from '../reducer.js';
import { runLLMPanel } from '../llm.js';
import { blendScores, deterministicScore } from '../scorer.js';
import { buildReport } from '../report.js';
import { simulateCitationReadiness } from '..citation.js';
import { maybeStorePageEmbeddings } from '../embeddings.js';
import { event, updateJob } from '../jobService.js';

requireConfig();


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

async function runDiagnostic(jobId: string) {
  const job = await one<any>('select * from axo_jobs where id = $1', [jobId]);
  if (!job) throw new Error(`Job not found: ${jobId}`);

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
    perHostConcurrency: config.crawlPerHostConcurrency
  });

  await annotatePageChangesAndUpdateCache(job.domain, pages);
  await bulkUpsertPages(jobId, pages);

  const summary = reducePages(pages);
  const citationSimulation = simulateCitationReadiness(summary);
  await query(
    `insert into axo_citation_simulations (job_id, citation_probability, answerability_score, trust_score, semantic_completeness, simulation)
     values ($1,$2,$3,$4,$5,$6)
     on conflict (job_id) do update set citation_probability=excluded.citation_probability, answerability_score=excluded.answerability_score, trust_score=excluded.trust_score, semantic_completeness=excluded.semantic_completeness, simulation=excluded.simulation, created_at=now()`,
    [jobId, citationSimulation.citationProbability, citationSimulation.answerabilityScore, citationSimulation.trustScore, citationSimulation.semanticCompleteness, JSON.stringify(citationSimulation)]
  );
  const embeddingResult = await maybeStorePageEmbeddings(jobId, pages);
  await event(jobId, 'crawl.completed', { pagesFetched: summary.pagesFetched, avgAeoSignal: summary.avgAeoSignal, changedPages: summary.changedPages.length, citationProbability: citationSimulation.citationProbability, embeddings: embeddingResult });

  await updateJob(jobId, { stage: 'scoring' });
  const prompt = buildCompactPrompt(job.domain, summary);
  const llmPanel = await runLLMPanel(prompt);
  const blended = blendScores(summary, llmPanel);

  let competitorSummaries: any[] = [];
  const competitors: string[] = Array.isArray(job.competitors) ? job.competitors : [];
  if (competitors.length) {
    await updateJob(jobId, { stage: 'competitor_scoring' });
    competitorSummaries = await Promise.all(competitors.slice(0, 5).map(async domain => {
      await query(`insert into axo_competitors (job_id, domain, status) values ($1,$2,'running') on conflict (job_id, domain) do update set status='running'`, [jobId, domain]);
      const compPages = await crawlSite({
        startUrl: `https://${domain}`,
        domain,
        maxPages: job.competitor_limit,
        depth: 3,
        includeSubdomains: false,
        timeoutMs: config.crawlTimeoutMs,
        concurrency: Math.min(config.crawlConcurrency, 15),
        perHostConcurrency: config.crawlPerHostConcurrency
      });
      await annotatePageChangesAndUpdateCache(domain, compPages);
      const compSummary = reducePages(compPages);
      const compScore = deterministicScore(compSummary);
      await query(`update axo_competitors set status='complete', pages_fetched=$3, summary=$4, score=$5, completed_at=now() where job_id=$1 and domain=$2`, [jobId, domain, compSummary.pagesFetched, JSON.stringify(compSummary), compScore]);
      return { domain, score: compScore, pagesFetched: compSummary.pagesFetched, avgAeoSignal: compSummary.avgAeoSignal };
    }));
  }

  await updateJob(jobId, { stage: 'reporting' });
  const report = buildReport({ job, summary, llmPanel, blended, competitorSummaries, citationSimulation, embeddingResult });
  await query(
    `insert into axo_results (job_id, score, scores_by_engine, engines_used, report)
     values ($1,$2,$3,$4,$5)
     on conflict (job_id) do update set score=excluded.score, scores_by_engine=excluded.scores_by_engine,
     engines_used=excluded.engines_used, report=excluded.report, generated_at=now()`,
    [jobId, blended.score, JSON.stringify(blended.byEngine), JSON.stringify(blended.enginesUsed), JSON.stringify(report)]
  );

  await updateJob(jobId, { status: 'complete', stage: 'complete', completedAt: new Date() });
  await event(jobId, 'report.completed', { score: blended.score, enginesUsed: blended.enginesUsed });

  if (config.n8nWebhookUrl) {
    await fetch(config.n8nWebhookUrl, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ type: 'axo.report.completed', jobId, domain: job.domain, score: blended.score })
    }).catch(() => undefined);
  }

  return report;
}

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
