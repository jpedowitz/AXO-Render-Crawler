import { query } from './db.js';

// ── Self-bootstrapping presence schema ─────────────────────────────────
// External DB access is disabled on this instance, so the worker creates the
// presence tables itself on startup using the internal connection it already
// has. Idempotent: every statement is "if not exists", safe to run on every
// boot. Matches the on-delete-cascade pattern of the other axo_ child tables.

let ensured = false;

export async function ensurePresenceSchema(): Promise<void> {
  if (ensured) return; // only once per process

  await query(`
    create table if not exists axo_query_observations (
      id                bigserial primary key,
      job_id            text        not null references axo_jobs(id) on delete cascade,
      query_id          text        not null,
      stage             text        not null,
      persona           text        not null default '',
      engine            text        not null,
      mode              text        not null,
      question          text        not null,
      brand_present     boolean     not null default false,
      prominence        smallint    not null default 0,
      brand_cited       boolean     not null default false,
      cited_urls        jsonb       not null default '[]'::jsonb,
      competitors_named jsonb       not null default '[]'::jsonb,
      answer_excerpt    text,
      ok                boolean     not null default true,
      error             text,
      ms                integer     not null default 0,
      created_at        timestamptz not null default now()
    )
  `);

  await query(`create index if not exists idx_axo_qobs_job         on axo_query_observations(job_id)`);
  await query(`create index if not exists idx_axo_qobs_job_stage   on axo_query_observations(job_id, stage)`);
  await query(`create index if not exists idx_axo_qobs_job_persona on axo_query_observations(job_id, persona)`);
  await query(`create index if not exists idx_axo_qobs_job_engine  on axo_query_observations(job_id, engine)`);

  await query(`
    create table if not exists axo_presence_summaries (
      job_id             text primary key references axo_jobs(id) on delete cascade,
      presence_score     smallint,
      by_engine          jsonb,
      engine_modes       jsonb,
      stage_presence     jsonb,
      persona_scores     jsonb,
      engine_by_persona  jsonb,
      citation_counts    jsonb,
      competitor_sov     jsonb,
      total_observations integer,
      brand_cited_count  integer,
      retrieval_coverage smallint,
      measured_at        timestamptz not null default now()
    )
  `);

  ensured = true;
  console.log('[migrate] presence schema ensured');
}
