# AXO Render Crawler

This is a lightweight FastAPI crawler service for Render. It replaces the fragile n8n batch crawl path with a dedicated crawler that n8n can call.

## Endpoints

POST `/crawl/start`

```json
{
  "jobId": "crawl_123",
  "startUrl": "https://thepedowitzgroup.com",
  "limit": 13000,
  "mode": "full",
  "includeSubdomains": true,
  "fetchContent": true
}
```

GET `/crawl/status/{job_id}`

GET `/crawl/result/{job_id}`

GET `/crawl/result/{job_id}?includePages=true`

## Render deploy

1. Create a new Render Web Service.
2. Connect this repo or upload these files.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
5. Use at least a paid `standard` instance for 10K+ page crawls.

## n8n usage

1. WF1 creates job.
2. n8n POSTs to `https://YOUR-RENDER-APP.onrender.com/crawl/start`.
3. n8n polls `/crawl/status/{jobId}` every 5-10 seconds.
4. When `status=complete`, n8n pulls `/crawl/result/{jobId}`.
5. Send `summary.recommendedPagesForLLM`, `summary.topPages`, `summary.gapPages`, and type/signal counts to your AXO analysis workflow.

## Important

This is in-memory job storage. For production, add Redis/Postgres or write completed job payloads to S3/R2/Supabase.
