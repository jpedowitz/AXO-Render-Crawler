import { nanoid } from 'nanoid';
import { one, query } from '../db.js';
import { diagnosticQueue } from '../queue.js';
import { config } from '../config.js';

export type CreateJobInput = {
  url: string;
  competitors?: string[];
  personas?: string[];
  maxPages?: number;
  depth?: number;
  includeSubdomains?: boolean;
  metadata?: Record<string, unknown>;
  forceRefresh?: boolean;
};

export function normalizeUrl(input: string): { startUrl: string; domain: string } {
  let url = String(input || '').trim();
  if (!url) throw new Error('url is required');
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  const parsed = new URL(url);
  const domain = parsed.hostname.toLowerCase().replace(/^www\./, '');
  return { startUrl: url, domain };
}

export async function createJob(input: CreateJobInput) {
  const { startUrl, domain } = normalizeUrl(input.url);
  const id = `axo_${Date.now()}_${nanoid(8)}`;
  const maxPages = Math.min(Math.max(Number(input.maxPages || config.defaultMaxPages), 25), 50000);
  const competitorLimit = config.defaultCompetitorPages;
  const depth = Math.min(Math.max(Number(input.depth || 4), 1), 10);
  const competitors = (input.competitors || []).slice(0, 5).map(c => normalizeUrl(c).domain);
  const personas = (input.personas || []).slice(0, 10).map(String);

  if (!input.forceRefresh) {
    const cached = await one<any>(
      `select r.job_id, r.score, r.generated_at, j.domain, j.status, j.stage
       from axo_results r
       join axo_jobs j on j.id = r.job_id
       where j.domain = $1
         and r.generated_at > now() - ($2::text || ' days')::interval
       order by r.generated_at desc
       limit 1`,
      [domain, config.resultCacheTtlDays]
    );

    if (cached) {
      return {
        success: true,
        cached: true,
        jobId: cached.job_id,
        domain: cached.domain,
        status: cached.status || 'complete',
        stage: cached.stage || 'complete',
        score: cached.score ?? null,
        generatedAt: cached.generated_at,
        readyForResults: true
      };
    }
  }

  await query(
    `insert into axo_jobs (id, domain, start_url, max_pages, competitor_limit, depth, include_subdomains, personas, competitors, metadata)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [id, domain, startUrl, maxPages, competitorLimit, depth, !!input.includeSubdomains, JSON.stringify(personas), JSON.stringify(competitors), JSON.stringify(input.metadata || {})]
  );

  await event(id, 'job.created', { domain, startUrl, maxPages, competitors, personas });
  await diagnosticQueue.add('run-diagnostic', { jobId: id }, { jobId: id });
  return getJobStatus(id);
}

export async function event(jobId: string, type: string, payload: Record<string, unknown> = {}) {
  await query(`insert into axo_events (job_id, type, payload) values ($1,$2,$3)`, [jobId, type, JSON.stringify(payload)]);
}

export async function updateJob(jobId: string, patch: Record<string, unknown>) {
  const keys = Object.keys(patch);
  if (!keys.length) return;
  const sets = keys.map((k, i) => `${camelToSnake(k)} = $${i + 2}`).join(', ');
  await query(`update axo_jobs set ${sets}, updated_at = now() where id = $1`, [jobId, ...keys.map(k => patch[k])]);
}

function camelToSnake(s: string) {
  return s.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
}

export async function getJobStatus(jobId: string) {
  const job = await one<any>(`select * from axo_jobs where id = $1`, [jobId]);
  if (!job) return null;
  const result = await one<any>(`select score, generated_at from axo_results where job_id = $1`, [jobId]);
  return {
    success: true,
    jobId: job.id,
    domain: job.domain,
    status: job.status,
    stage: job.stage,
    score: result?.score ?? null,
    createdAt: job.created_at,
    updatedAt: job.updated_at,
    completedAt: job.completed_at,
    readyForResults: !!result
  };
}

export async function getJobResults(jobId: string) {
  const job = await one<any>(`select * from axo_jobs where id = $1`, [jobId]);
  const result = await one<any>(`select * from axo_results where job_id = $1`, [jobId]);
  if (!job) return null;
  return { success: true, job, result, readyForResults: !!result };
}
