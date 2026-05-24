create table if not exists axo_jobs (
  id text primary key,
  domain text not null,
  start_url text not null,
  status text not null default 'queued',
  stage text not null default 'queued',
  max_pages integer not null default 250,
  competitor_limit integer not null default 75,
  depth integer not null default 4,
  include_subdomains boolean not null default false,
  personas jsonb not null default '[]'::jsonb,
  competitors jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

create table if not exists axo_pages (
  id bigserial primary key,
  job_id text not null references axo_jobs(id) on delete cascade,
  url text not null,
  title text,
  status_code integer,
  content_type text,
  word_count integer default 0,
  aeo_signal numeric default 0,
  signals jsonb not null default '[]'::jsonb,
  excerpt text,
  raw jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique(job_id, url)
);

create table if not exists axo_competitors (
  id bigserial primary key,
  job_id text not null references axo_jobs(id) on delete cascade,
  domain text not null,
  status text not null default 'queued',
  pages_fetched integer default 0,
  summary jsonb not null default '{}'::jsonb,
  score numeric,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  unique(job_id, domain)
);

create table if not exists axo_results (
  job_id text primary key references axo_jobs(id) on delete cascade,
  score numeric,
  scores_by_engine jsonb not null default '{}'::jsonb,
  engines_used jsonb not null default '[]'::jsonb,
  report jsonb not null default '{}'::jsonb,
  generated_at timestamptz not null default now()
);

create table if not exists axo_events (
  id bigserial primary key,
  job_id text references axo_jobs(id) on delete cascade,
  type text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_axo_jobs_status on axo_jobs(status, stage);
create index if not exists idx_axo_pages_job on axo_pages(job_id);
create index if not exists idx_axo_events_job on axo_events(job_id, created_at desc);

-- Cache and operational lookup indexes.
create index if not exists idx_axo_jobs_domain on axo_jobs(domain);
create index if not exists idx_axo_results_generated_at on axo_results(generated_at desc);
create index if not exists idx_axo_results_job_generated_at on axo_results(job_id, generated_at desc);
create index if not exists idx_axo_competitors_job on axo_competitors(job_id);

-- v4 enhancements: page classification, content diffing, embeddings, and citation simulation.
alter table axo_pages add column if not exists content_hash text;
alter table axo_pages add column if not exists changed boolean;
alter table axo_pages add column if not exists classification jsonb not null default '{}'::jsonb;
create index if not exists idx_axo_pages_content_hash on axo_pages(content_hash);
create index if not exists idx_axo_pages_classification on axo_pages using gin(classification);

create table if not exists axo_page_cache (
  domain text not null,
  url text not null,
  content_hash text not null,
  title text,
  content_type text,
  word_count integer default 0,
  aeo_signal numeric default 0,
  classification jsonb not null default '{}'::jsonb,
  excerpt text,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  primary key(domain, url)
);
create index if not exists idx_axo_page_cache_domain_seen on axo_page_cache(domain, last_seen_at desc);
create index if not exists idx_axo_page_cache_hash on axo_page_cache(content_hash);

create table if not exists axo_embeddings (
  id bigserial primary key,
  job_id text not null references axo_jobs(id) on delete cascade,
  url text not null,
  content_hash text,
  model text not null,
  dimensions integer not null default 0,
  embedding jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique(job_id, url, model)
);
create index if not exists idx_axo_embeddings_job on axo_embeddings(job_id);
create index if not exists idx_axo_embeddings_hash on axo_embeddings(content_hash);

create table if not exists axo_citation_simulations (
  job_id text primary key references axo_jobs(id) on delete cascade,
  citation_probability numeric,
  answerability_score numeric,
  trust_score numeric,
  semantic_completeness numeric,
  simulation jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
