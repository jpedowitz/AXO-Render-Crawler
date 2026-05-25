CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS axo_jobs (
  id TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  start_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  stage TEXT NOT NULL DEFAULT 'queued',
  max_pages INTEGER NOT NULL DEFAULT 250,
  competitor_limit INTEGER NOT NULL DEFAULT 75,
  depth INTEGER NOT NULL DEFAULT 4,
  include_subdomains BOOLEAN NOT NULL DEFAULT false,
  personas JSONB NOT NULL DEFAULT '[]',
  competitors JSONB NOT NULL DEFAULT '[]',
  metadata JSONB NOT NULL DEFAULT '{}',
  error TEXT,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS axo_pages (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  title TEXT,
  status_code INTEGER,
  content_type TEXT,
  word_count INTEGER,
  aeo_signal REAL,
  signals JSONB DEFAULT '{}',
  excerpt TEXT,
  raw JSONB DEFAULT '{}',
  content_hash TEXT,
  changed BOOLEAN,
  classification JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(job_id, url)
);

CREATE TABLE IF NOT EXISTS axo_competitors (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  pages_fetched INTEGER,
  summary JSONB,
  score REAL,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(job_id, domain)
);

CREATE TABLE IF NOT EXISTS axo_results (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  score REAL,
  scores_by_engine JSONB DEFAULT '{}',
  engines_used JSONB DEFAULT '[]',
  report JSONB DEFAULT '{}',
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(job_id)
);

CREATE TABLE IF NOT EXISTS axo_events (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  payload JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS axo_page_cache (
  id BIGSERIAL PRIMARY KEY,
  domain TEXT NOT NULL,
  url TEXT NOT NULL,
  content_hash TEXT,
  title TEXT,
  content_type TEXT,
  word_count INTEGER,
  aeo_signal REAL,
  classification JSONB DEFAULT '{}',
  excerpt TEXT,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(domain, url)
);

CREATE TABLE IF NOT EXISTS axo_citation_simulations (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  citation_probability REAL,
  answerability_score REAL,
  trust_score REAL,
  semantic_completeness REAL,
  simulation JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(job_id)
);

CREATE TABLE IF NOT EXISTS axo_embeddings (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES axo_jobs(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  embedding JSONB DEFAULT '[]',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(job_id, url)
);

CREATE INDEX IF NOT EXISTS idx_axo_jobs_domain ON axo_jobs(domain);
CREATE INDEX IF NOT EXISTS idx_axo_jobs_status ON axo_jobs(status);
CREATE INDEX IF NOT EXISTS idx_axo_pages_job_id ON axo_pages(job_id);
CREATE INDEX IF NOT EXISTS idx_axo_events_job_id ON axo_events(job_id);
CREATE INDEX IF NOT EXISTS idx_axo_page_cache_domain ON axo_page_cache(domain);
CREATE INDEX IF NOT EXISTS idx_axo_results_job_id ON axo_results(job_id);
