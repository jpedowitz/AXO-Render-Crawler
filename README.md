# AXO Render Crawler v2

Render-ready crawler API for AXO diagnostics.

This version removes FastAPI/Pydantic to avoid pydantic-core build failures and uses Starlette directly.

## Deploy

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

Set Render env var if available:

```text
PYTHON_VERSION=3.11.9
```

## Endpoints

POST `/crawl/start`

```json
{
  "jobId": "crawl_123",
  "startUrl": "https://thepedowitzgroup.com",
  "limit": 13000,
  "includeSubdomains": true,
  "fetchContent": true
}
```

GET `/crawl/status/{jobId}`

GET `/crawl/result/{jobId}`

GET `/crawl/result/{jobId}?mode=analysis`

GET `/crawl/result/{jobId}?mode=inventory`

## n8n changes

Bypass the old WF2/WF3 batch crawler. n8n should call Render, poll status, pull summary, and pass summary to WF4.
