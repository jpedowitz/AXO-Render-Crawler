# AXO Diagnostic Engine — Render/Postgres/n8n Architecture

This package moves the AXO diagnostic execution engine out of n8n and into a proper application runtime while keeping n8n for orchestration, CRM handoffs, notifications, and sales workflows.

The current n8n system already proved the workflow shape: submit job, start crawler, receive callback, run four-LLM analysis, then expose status/results. The new design keeps that product logic but replaces long-running n8n execution chains with Render services, Postgres state, Redis/BullMQ queues, concurrent crawling, and parallel LLM scoring.

## Core design principle

n8n should be the traffic controller, not the engine.

Use n8n for:

- Form/webhook intake if desired
- HubSpot contact/company/deal updates
- Slack/Teams alerts
- Email notifications
- Follow-up workflows
- Calling the AXO API

Do not use n8n for:

- Page crawling
- Competitor fanout coordination
- LLM execution chains
- State management
- Timeout recovery
- Report assembly

## Services

### 1. axo-api

Render web service. Owns:

- `POST /axo/jobs`
- `GET /axo/jobs/:jobId/status`
- `GET /axo/jobs/:jobId/results`
- job creation
- domain-level cache lookup
- queue submission

### 2. axo-worker

Render background worker. Owns:

- concurrent crawling
- page reduction
- page persistence
- parallel LLM scoring
- competitor crawling/scoring
- report JSON assembly
- result persistence
- optional n8n completion webhook

### 3. Postgres

Stores durable state:

- `axo_jobs`
- `axo_pages`
- `axo_competitors`
- `axo_results`
- `axo_events`

### 4. Redis

Backs BullMQ job queue.

## Request flow

```text
Landing page / n8n / client app
        |
        v
POST /axo/jobs
        |
        v
Postgres job record + cache lookup
        |
        v
BullMQ queue
        |
        v
axo-worker
  - crawl target site concurrently
  - bulk insert pages
  - reduce pages into compact summary
  - run four LLM scorers in parallel
  - crawl competitors concurrently and score deterministically
  - build report JSON
        |
        v
Postgres result record
        |
        v
GET /status + GET /results
        |
        v
n8n HubSpot/email/Slack handoffs
```

## Performance model

The goal is a full prospect diagnostic in roughly 60 seconds under normal conditions.

Expected timeline with defaults:

```text
0-3s     job creation, cache check, queue start
3-30s    crawl up to 250 pages with concurrent fetches
30-45s   four LLM scorers run in parallel
45-55s   competitor crawls and deterministic competitor scoring
55-60s   report assembly and Postgres write
```

The key controls are:

- `DEFAULT_MAX_PAGES=250`
- `DEFAULT_COMPETITOR_PAGES=75`
- `CRAWL_CONCURRENCY=25`
- `CRAWL_PER_HOST_CONCURRENCY=5`
- `LLM_TIMEOUT_MS=25000`

## Important improvements in this version

### Concurrent crawler

`src/services/crawler.ts` no longer fetches pages one at a time. It uses concurrent batches with a per-host concurrency limit.

Recommended defaults:

```env
CRAWL_CONCURRENCY=25
CRAWL_PER_HOST_CONCURRENCY=5
```

The per-host limit matters because most crawls hit one primary host. Five concurrent connections is usually aggressive enough without hammering the prospect's site.

### Bulk page inserts

`src/workers/index.ts` writes pages to Postgres in batches of 50 instead of one row per round trip. This reduces database overhead materially for 250+ page jobs.

### Domain result cache

`createJob()` checks for a recent result for the same normalized domain before creating a new job.

Default TTL:

```env
RESULT_CACHE_TTL_DAYS=7
```

A cache hit returns immediately:

```json
{
  "success": true,
  "cached": true,
  "jobId": "axo_...",
  "domain": "example.com",
  "score": 72,
  "readyForResults": true
}
```

Use this to support many concurrent users. Most repeat runs should not recrawl.

To force a new crawl:

```json
{
  "url": "example.com",
  "forceRefresh": true
}
```

## Environment variables

```env
NODE_ENV=production
PORT=3000
DATABASE_URL=postgres://...
REDIS_URL=redis://...
PUBLIC_BASE_URL=https://your-api.onrender.com
N8N_WEBHOOK_URL=https://your-n8n-webhook-if-needed
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
PERPLEXITY_API_KEY=
GEMINI_API_KEY=
DEFAULT_MAX_PAGES=250
DEFAULT_COMPETITOR_PAGES=75
JOB_CONCURRENCY=3
LLM_TIMEOUT_MS=25000
CRAWL_TIMEOUT_MS=45000
CRAWL_CONCURRENCY=25
CRAWL_PER_HOST_CONCURRENCY=5
RESULT_CACHE_TTL_DAYS=7
```

## Local setup

```bash
npm install
cp .env.example .env
npm run db:migrate
npm run dev:api
npm run dev:worker
```

Submit a test job:

```bash
curl -X POST http://localhost:3000/axo/jobs \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.pedowitzgroup.com","maxPages":50,"competitors":["demandgen.com","sixandflow.com"]}'
```

Check status:

```bash
curl http://localhost:3000/axo/jobs/<jobId>/status
```

Get results:

```bash
curl http://localhost:3000/axo/jobs/<jobId>/results
```

## Render deployment

This repo includes `render.yaml` with:

- `axo-api` web service
- `axo-worker` background worker
- Redis
- Postgres

Deploy options:

1. Push this repo to GitHub.
2. In Render, create a Blueprint from `render.yaml`.
3. Add API keys as environment variables.
4. Run the migration command once:

```bash
npm run db:migrate
```

## n8n integration pattern

### New submit flow

Replace the current heavy n8n submit/crawl workflow with a light webhook:

1. n8n receives form submission.
2. n8n calls `POST /axo/jobs`.
3. n8n stores returned `jobId` in HubSpot.
4. n8n either polls status or waits for `N8N_WEBHOOK_URL` completion callback.
5. n8n sends email/Slack/HubSpot notifications when complete.

### Recommended n8n responsibilities

- Create/update HubSpot company/contact
- Store `jobId`
- Notify sales on high score or target account
- Send prospect email when report is ready
- Route high-fit domains to follow-up sequence

## API contract

### POST /axo/jobs

Request:

```json
{
  "url": "https://example.com",
  "competitors": ["competitor1.com", "competitor2.com"],
  "personas": ["CMO", "VP Marketing", "Marketing Ops"],
  "maxPages": 250,
  "depth": 4,
  "includeSubdomains": false,
  "forceRefresh": false,
  "metadata": {
    "source": "hubspot-form",
    "email": "person@example.com"
  }
}
```

Response for new job:

```json
{
  "success": true,
  "jobId": "axo_...",
  "domain": "example.com",
  "status": "queued",
  "stage": "queued",
  "score": null,
  "readyForResults": false
}
```

Response for cache hit:

```json
{
  "success": true,
  "cached": true,
  "jobId": "axo_...",
  "domain": "example.com",
  "status": "complete",
  "stage": "complete",
  "score": 72,
  "readyForResults": true
}
```

### GET /axo/jobs/:jobId/status

Returns lightweight job status.

### GET /axo/jobs/:jobId/results

Returns job record and report result.

## Report object

The report builder returns a JSON object shaped for the current AXO report UI:

- company/domain
- score
- engine scores
- crawl summary
- competitor summaries
- quick wins
- content gaps
- page roadmap inputs

The frontend should consume the report object rather than reconstructing scoring logic client-side.

## Build sequence

1. Deploy Postgres/Redis.
2. Run schema migration.
3. Deploy `axo-api`.
4. Deploy `axo-worker`.
5. Submit a 25-page smoke test.
6. Submit a 250-page test.
7. Add n8n callback/webhook handoff.
8. Replace old n8n-heavy endpoints in the public HTML.
9. Run side-by-side with the old n8n version.
10. Cut over when runtime and report quality are validated.

## Operational notes

- Keep `DEFAULT_MAX_PAGES=250` for prospect runs.
- Use `forceRefresh=true` only for sales/internal tests or paid diagnostics.
- Raise `JOB_CONCURRENCY` carefully. Each job can create 25 crawl requests plus LLM calls.
- Watch Postgres connection limits on lower Render plans.
- Increase Redis memory if job volume spikes.
- Log all failures to `axo_events`.

## Next enhancements

- Add sitemap-first discovery before page traversal.
- Add robots.txt respect mode.
- Add per-domain crawl cooldowns.
- Add report PDF rendering service.
- Add competitor auto-discovery as a separate optional worker.
- Add incremental enrich mode: first report in 60 seconds, competitor benchmark later.

## v4 Enhancements

This package adds four production enhancements on top of v3.

### 1. Crawl prioritization engine

`src/services/pageClassifier.ts` classifies URLs and fetched pages before expensive downstream work. The crawler now sorts the queue by priority before each concurrent batch. High-value pages such as pricing, comparison, case studies, product/service pages, FAQs, demo/contact pages, and authority content move ahead of low-value pages such as legal, careers, tags, author archives, and media assets.

Each crawled page now includes:

- `classification.priority`
- `classification.action` (`score`, `summarize`, `ignore`)
- `classification.commercialIntent`
- `classification.aiRelevance`
- `classification.trustSignal`
- `classification.reasons`

The reducer favors prioritized/scorable pages for LLM prompts and report sections. This reduces token waste and improves the quality of the diagnostic.

### 2. Embeddings layer

`src/services/embeddings.ts` adds an optional embeddings path using OpenAI embeddings. It is disabled by default.

Environment variables:

```bash
ENABLE_EMBEDDINGS=false
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_MAX_PAGES=50
```

Embeddings are stored in `axo_embeddings` as JSONB. This avoids requiring pgvector at initial deploy while keeping the schema ready for a later pgvector migration. Recommended next step: add a separate `pgvector_optional.sql` migration when the Render Postgres instance confirms vector extension support.

### 3. AI citation simulation

`src/services/citation.ts` estimates whether an AI answer engine would cite the site based on:

- structured data
- FAQ schema
- heading structure
- proof language
- comparison language
- content depth
- commercial relevance
- trust indicators

The worker stores the output in `axo_citation_simulations` and the final report includes `citationSimulation` plus `axoSnapshot.citationProbability`.

### 4. Snapshot diffing and changed-page tracking

The crawler now computes a normalized SHA-256 `contentHash` per page. The worker compares each page against `axo_page_cache` by domain and URL, marks the page as `changed`, and updates the cache.

This supports:

- changed-page reporting
- future delta rescoring
- repeat-run economics
- future alerting when high-value pages change

The final report includes a `changedPages` array and counts for changed/unchanged pages in `axoSnapshot`.

## Deployment Notes for v4

Run the schema migration again before deploying the worker:

```bash
npm run db:migrate
```

The migration is additive and safe for the existing v3 schema.

If embeddings are enabled, make sure `OPENAI_API_KEY` is present and set `ENABLE_EMBEDDINGS=true`. Leave it off for the first production deploy unless you are ready to manage embedding costs.

Recommended initial v4 settings:

```bash
DEFAULT_MAX_PAGES=250
DEFAULT_COMPETITOR_PAGES=75
CRAWL_CONCURRENCY=25
CRAWL_PER_HOST_CONCURRENCY=5
LLM_TIMEOUT_MS=25000
ENABLE_EMBEDDINGS=false
RESULT_CACHE_TTL_DAYS=7
```
