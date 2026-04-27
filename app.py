import asyncio
import hashlib
import os
import re
import time
import uuid
import gzip
import io
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

APP_NAME = "AXO Render Crawler"
VERSION = "1.0.0"

CRAWL_CONCURRENCY = int(os.getenv("CRAWL_CONCURRENCY", "150"))
SITEMAP_CONCURRENCY = int(os.getenv("SITEMAP_CONCURRENCY", "50"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
MAX_BYTES_PER_PAGE = int(os.getenv("MAX_BYTES_PER_PAGE", "750000"))
MAX_BYTES_PER_SITEMAP = int(os.getenv("MAX_BYTES_PER_SITEMAP", "20000000"))
MAX_CONTENT_CHARS = int(os.getenv("MAX_CONTENT_CHARS", "50000"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "15000"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "100000"))
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "14400"))
USER_AGENT = os.getenv(
    "CRAWLER_USER_AGENT",
    "AXO-Diagnostic-Crawler/1.0 (+https://thepedowitzgroup.com)"
)

app = FastAPI(title=APP_NAME, version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

JOBS: Dict[str, Dict[str, Any]] = {}
JOB_LOCK = asyncio.Lock()


class CrawlStartRequest(BaseModel):
    jobId: Optional[str] = None
    startUrl: str
    limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)
    mode: str = "full"
    includeSubdomains: bool = True
    fetchContent: bool = True
    callbackUrl: Optional[str] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_start_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("startUrl is required")
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    parsed = urlparse(raw)
    if not parsed.netloc:
        raise ValueError("Invalid startUrl")
    path = parsed.path or "/"
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path, "", parsed.query, ""))


def root_domain(host: str) -> str:
    host = (host or "").lower().replace("www.", "", 1)
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    return ".".join(parts[-2:])


def same_site(url: str, base_host: str, include_subdomains: bool = True) -> bool:
    try:
        h = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return False
    base = base_host.lower().split(":")[0]
    if h == base or h == "www." + base or h.replace("www.", "", 1) == base.replace("www.", "", 1):
        return True
    if include_subdomains:
        rd = root_domain(base)
        return h == rd or h.endswith("." + rd)
    return False


def normalize_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return None
        host = p.netloc.lower()
        path = p.path or "/"
        query_pairs = [
            (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in {
                "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                "gclid", "fbclid", "_ga", "ref"
            }
        ]
        query = urlencode(query_pairs)
        if path != "/":
            path = path.rstrip("/")
        return urlunparse((p.scheme, host, path, "", query, ""))
    except Exception:
        return None


def is_asset_or_noise(url: str) -> bool:
    path = urlparse(url).path.lower()
    if re.search(r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|woff|woff2|ttf|zip|mp4|webp|avif|xml|json|csv|txt)$", path):
        return True
    if re.search(r"(wp-json|wp-admin|wp-login|xmlrpc|cdn-cgi|_next/static|/feed/|/tag/|/author/)", path):
        return True
    return False


def content_type_from_url(url: str) -> str:
    p = urlparse(url).path.lower()
    if p in ("", "/"):
        return "homepage"
    if re.search(r"pricing|plans|buy", p):
        return "pricing"
    if re.search(r"demo|trial|get-started|contact-sales", p):
        return "conversion"
    if re.search(r"solution|product|platform|feature|service|use-case", p):
        return "product_solution"
    if re.search(r"case-stud|customer|success-stor|client", p):
        return "social_proof"
    if re.search(r"resource|whitepaper|guide|report|ebook|webinar|checklist|template", p):
        return "resource"
    if re.search(r"blog|article|post|news|insight", p):
        return "blog"
    if re.search(r"about|team|company|leadership", p):
        return "company"
    if re.search(r"faq|help|support|docs", p):
        return "support_faq"
    return "other"


def priority_score(url: str) -> int:
    t = content_type_from_url(url)
    return {
        "homepage": 100,
        "pricing": 95,
        "conversion": 90,
        "product_solution": 85,
        "social_proof": 80,
        "resource": 70,
        "company": 65,
        "support_faq": 62,
        "blog": 50,
        "other": 40,
    }.get(t, 40)


def axo_url_score(url: str) -> Dict[str, Any]:
    p = urlparse(url).path.lower()
    score = 1.0
    signals = []
    ctype = content_type_from_url(url)

    if ctype == "homepage":
        score += 1.5
        signals.append("homepage")
    if ctype in ("pricing", "conversion"):
        score += 2.0
        signals.append("decision_intent")
    if ctype in ("product_solution", "social_proof"):
        score += 1.5
        signals.append("buyer_relevance")
    if ctype in ("resource", "support_faq"):
        score += 2.0
        signals.append("answerable_content")
    if ctype == "blog":
        score += 1.0
        signals.append("educational_content")
    if re.search(r"compare|alternative|vs-|versus|best|top|roi|cost|calculator", p):
        score += 1.5
        signals.append("comparison_or_roi")
    if len([x for x in p.split("/") if x]) > 4:
        score -= 0.5
        signals.append("deep_url_penalty")

    return {
        "aeoSignal": max(0, min(10, round(score, 1))),
        "contentType": ctype,
        "signals": signals,
        "confidence": "url_only"
    }


async def bounded_fetch(client: httpx.AsyncClient, url: str, max_bytes: int) -> Dict[str, Any]:
    started = time.time()
    try:
        async with client.stream("GET", url, follow_redirects=True) as resp:
            status = resp.status_code
            chunks = []
            total = 0
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > max_bytes:
                    break
                chunks.append(chunk)
            body = b"".join(chunks)
            text = body.decode(resp.encoding or "utf-8", errors="ignore")
            return {
                "ok": 200 <= status < 400,
                "statusCode": status,
                "url": str(resp.url),
                "html": text,
                "bytes": len(body),
                "ms": int((time.time() - started) * 1000),
                "error": None
            }
    except Exception as e:
        return {
            "ok": False,
            "statusCode": None,
            "url": url,
            "html": "",
            "bytes": 0,
            "ms": int((time.time() - started) * 1000),
            "error": str(e)[:300]
        }


def extract_page(html: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")

    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header"]):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    desc = ""
    md = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if md and md.get("content"):
        desc = md.get("content", "").strip()

    h1s = [x.get_text(" ", strip=True) for x in soup.find_all("h1")][:3]
    h2s = [x.get_text(" ", strip=True) for x in soup.find_all("h2")][:10]
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    word_count = len(re.findall(r"\w+", text))

    schema_text = " ".join(
        [s.get_text(" ", strip=True)[:2000] for s in soup.find_all("script", type="application/ld+json")]
    )
    has_schema = bool(schema_text)
    has_faq = "FAQPage" in schema_text
    question_count = len(re.findall(r"\?", text[:MAX_CONTENT_CHARS]))

    url_scored = axo_url_score(url)
    score = url_scored["aeoSignal"]
    signals = list(url_scored["signals"])

    if has_faq:
        score += 2
        signals.append("faq_schema")
    if has_schema:
        score += 1
        signals.append("structured_data")
    if question_count >= 5:
        score += 1.5
        signals.append("question_rich")
    elif question_count >= 2:
        score += 0.75
        signals.append("has_questions")
    if re.search(r"\b(in summary|bottom line|key takeaway|the answer is|here's how)\b", text, re.I):
        score += 1
        signals.append("direct_answer")
    if word_count < 100:
        score -= 1
        signals.append("thin_content")
    elif word_count > 600:
        score += 0.5
        signals.append("substantial_content")

    return {
        "url": url,
        "finalUrl": url,
        "title": title[:200],
        "description": desc[:300],
        "h1s": h1s,
        "h2s": h2s,
        "text": text[:MAX_CONTENT_CHARS],
        "contentHash": hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:16],
        "wordCount": word_count,
        "statusCode": 200,
        "contentType": url_scored["contentType"],
        "aeoSignal": max(0, min(10, round(score, 1))),
        "signals": signals,
        "confidence": "html" if html else "url_only",
        "hasStructuredData": has_schema,
        "hasFaqSchema": has_faq,
        "questionCount": question_count
    }


async def fetch_text(client: httpx.AsyncClient, url: str, max_bytes: int) -> Optional[str]:
    try:
        res = await bounded_fetch(client, url, max_bytes)
        if res["ok"] and res["html"]:
            return res["html"]
    except Exception:
        pass
    return None


def parse_locs(xml: str) -> List[str]:
    if not xml:
        return []
    return list(dict.fromkeys([m.group(1).strip() for m in re.finditer(r"<loc>\s*([^<\s]+)\s*</loc>", xml, re.I)]))


async def discover_urls(start_url: str, limit: int, include_subdomains: bool) -> Dict[str, Any]:
    parsed = urlparse(start_url)
    scheme = parsed.scheme or "https"
    host = parsed.netloc.lower()
    apex = host.replace("www.", "", 1)

    headers = {"User-Agent": USER_AGENT}
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
    urls = set()
    sitemap_urls = []
    errors = []

    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        bases = [
            f"{scheme}://{host}",
            f"https://{apex}",
            f"https://www.{apex}",
        ]
        bases = list(dict.fromkeys(bases))

        for base in bases:
            robots = await fetch_text(client, f"{base}/robots.txt", MAX_BYTES_PER_SITEMAP)
            if robots:
                for line in robots.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sm = line.split(":", 1)[1].strip()
                        if sm:
                            sitemap_urls.append(sm)

        for base in bases:
            sitemap_urls.extend([
                f"{base}/sitemap.xml",
                f"{base}/sitemap_index.xml",
                f"{base}/sitemap-index.xml",
                f"{base}/wp-sitemap.xml",
                f"{base}/page-sitemap.xml",
                f"{base}/post-sitemap.xml",
                f"{base}/hs-sitemap-index.xml",
            ])

        sitemap_urls = list(dict.fromkeys(sitemap_urls))

        async def fetch_sitemap(sm_url: str) -> List[str]:
            xml = await fetch_text(client, sm_url, MAX_BYTES_PER_SITEMAP)
            if not xml:
                return []
            return parse_locs(xml)

        queue = list(sitemap_urls)
        seen_sitemaps = set()
        sem = asyncio.Semaphore(SITEMAP_CONCURRENCY)
        page_locs = set()
        child_sitemaps = set()

        async def process_sitemap(sm: str):
            async with sem:
                if sm in seen_sitemaps:
                    return
                seen_sitemaps.add(sm)
                try:
                    locs = await fetch_sitemap(sm)
                    for loc in locs:
                        if re.search(r"\.xml(\.gz)?($|\?)", loc, re.I):
                            child_sitemaps.add(loc)
                        else:
                            page_locs.add(loc)
                except Exception as e:
                    errors.append(f"sitemap failed {sm}: {str(e)[:150]}")

        for _ in range(4):
            if not queue:
                break
            batch = queue[:2000]
            queue = queue[2000:]
            await asyncio.gather(*[process_sitemap(x) for x in batch])
            new_children = [x for x in child_sitemaps if x not in seen_sitemaps]
            queue.extend(new_children[:5000])
            if len(page_locs) >= limit:
                break

        for loc in page_locs:
            n = normalize_url(loc)
            if n and same_site(n, host, include_subdomains) and not is_asset_or_noise(n):
                urls.add(n)

        homepage = await fetch_text(client, start_url, MAX_BYTES_PER_PAGE)
        if homepage:
            for m in re.finditer(r'href=["\']([^"\']+)["\']', homepage, re.I):
                href = m.group(1).strip()
                if href.startswith(("mailto:", "tel:", "javascript:", "#")):
                    continue
                absolute = urljoin(start_url, href)
                n = normalize_url(absolute)
                if n and same_site(n, host, include_subdomains) and not is_asset_or_noise(n):
                    urls.add(n)

    ranked = sorted(urls, key=lambda u: priority_score(u), reverse=True)
    return {
        "urls": ranked[:limit],
        "totalDiscovered": len(ranked),
        "sitemapsSeen": len(seen_sitemaps) if "seen_sitemaps" in locals() else 0,
        "errors": errors[:50]
    }


async def crawl_job(job_id: str, req: CrawlStartRequest):
    async with JOB_LOCK:
        job = JOBS[job_id]
        job.update({
            "status": "discovering",
            "stage": "discovering",
            "updatedAt": now_iso()
        })

    try:
        start_url = normalize_start_url(req.startUrl)
        discovered = await discover_urls(start_url, req.limit, req.includeSubdomains)
        urls = discovered["urls"]

        async with JOB_LOCK:
            job = JOBS[job_id]
            job.update({
                "status": "crawling",
                "stage": "crawling",
                "urlsFound": len(urls),
                "totalDiscovered": discovered["totalDiscovered"],
                "sitemapsSeen": discovered["sitemapsSeen"],
                "discoveryErrors": discovered["errors"],
                "updatedAt": now_iso()
            })

        pages = []
        failures = []
        content_hashes = set()
        headers = {"User-Agent": USER_AGENT}
        timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
        sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
        started = time.time()

        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            async def fetch_and_extract(i: int, url: str):
                async with sem:
                    res = await bounded_fetch(client, url, MAX_BYTES_PER_PAGE)
                    if res["ok"] and res["html"] and req.fetchContent:
                        page = extract_page(res["html"], url)
                        page["statusCode"] = res["statusCode"]
                        page["fetchMs"] = res["ms"]
                    else:
                        s = axo_url_score(url)
                        page = {
                            "url": url,
                            "finalUrl": res.get("url", url),
                            "title": "",
                            "description": "",
                            "h1s": [],
                            "h2s": [],
                            "text": "",
                            "contentHash": hashlib.sha256(url.encode()).hexdigest()[:16],
                            "wordCount": 0,
                            "statusCode": res.get("statusCode"),
                            "contentType": s["contentType"],
                            "aeoSignal": s["aeoSignal"],
                            "signals": s["signals"] + (["fetch_failed"] if not res["ok"] else []),
                            "confidence": "url_only",
                            "hasStructuredData": False,
                            "hasFaqSchema": False,
                            "questionCount": 0,
                            "fetchMs": res["ms"]
                        }
                        if not res["ok"]:
                            failures.append({"url": url, "error": res.get("error"), "statusCode": res.get("statusCode")})

                    if page["contentHash"] not in content_hashes:
                        content_hashes.add(page["contentHash"])
                        pages.append(page)

                    if i % 25 == 0 or i == len(urls):
                        async with JOB_LOCK:
                            job = JOBS[job_id]
                            job.update({
                                "pagesFetched": len(pages),
                                "pagesAttempted": i,
                                "pagesFailed": len(failures),
                                "elapsedSeconds": round(time.time() - started, 1),
                                "updatedAt": now_iso()
                            })

            await asyncio.gather(*[fetch_and_extract(i + 1, u) for i, u in enumerate(urls)])

        summary = build_summary(start_url, urls, pages, failures, discovered)
        async with JOB_LOCK:
            job = JOBS[job_id]
            job.update({
                "status": "complete",
                "stage": "complete",
                "completedAt": now_iso(),
                "updatedAt": now_iso(),
                "pagesFetched": len(pages),
                "pagesFailed": len(failures),
                "elapsedSeconds": round(time.time() - started, 1),
                "result": {
                    "summary": summary,
                    "pages": pages,
                    "failures": failures[:500]
                }
            })

        if req.callbackUrl:
            await send_callback(req.callbackUrl, job_id)

    except Exception as e:
        async with JOB_LOCK:
            JOBS[job_id].update({
                "status": "failed",
                "stage": "failed",
                "error": str(e)[:1000],
                "updatedAt": now_iso()
            })


def build_summary(start_url: str, urls: List[str], pages: List[Dict[str, Any]], failures: List[Dict[str, Any]], discovered: Dict[str, Any]) -> Dict[str, Any]:
    type_counts = Counter([p.get("contentType", "other") for p in pages])
    signal_counts = Counter()
    for p in pages:
        for s in p.get("signals", []):
            signal_counts[s] += 1

    scores = [float(p.get("aeoSignal", 0)) for p in pages]
    avg_score = round(sum(scores) / len(scores), 2) if scores else 0

    top_pages = sorted(
        [
            {
                "url": p["url"],
                "title": p.get("title", ""),
                "contentType": p.get("contentType", "other"),
                "aeoSignal": p.get("aeoSignal", 0),
                "wordCount": p.get("wordCount", 0),
                "signals": p.get("signals", [])
            }
            for p in pages
        ],
        key=lambda x: x["aeoSignal"],
        reverse=True
    )[:250]

    gap_pages = sorted(
        [
            {
                "url": p["url"],
                "title": p.get("title", ""),
                "contentType": p.get("contentType", "other"),
                "aeoSignal": p.get("aeoSignal", 0),
                "wordCount": p.get("wordCount", 0),
                "signals": p.get("signals", [])
            }
            for p in pages
        ],
        key=lambda x: x["aeoSignal"]
    )[:250]

    representative = sample_representative_pages(pages, max_pages=500)

    return {
        "siteUrl": start_url,
        "urlsFound": len(urls),
        "totalDiscovered": discovered.get("totalDiscovered", len(urls)),
        "sitemapsSeen": discovered.get("sitemapsSeen", 0),
        "pagesFetched": len(pages),
        "pagesFailed": len(failures),
        "avgAeoSignal": avg_score,
        "highAeo": len([s for s in scores if s >= 7]),
        "midAeo": len([s for s in scores if 4 <= s < 7]),
        "lowAeo": len([s for s in scores if s < 4]),
        "typeDistribution": dict(type_counts),
        "topSignals": dict(signal_counts.most_common(25)),
        "topPages": top_pages,
        "gapPages": gap_pages,
        "recommendedPagesForLLM": representative,
        "crawlFinishedAt": now_iso()
    }


def sample_representative_pages(pages: List[Dict[str, Any]], max_pages: int = 500) -> List[Dict[str, Any]]:
    buckets = defaultdict(list)
    for p in pages:
        key = p.get("contentType", "other")
        buckets[key].append(p)

    selected = []
    for _, bucket in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True):
        bucket_sorted = sorted(bucket, key=lambda x: x.get("aeoSignal", 0), reverse=True)
        take = max(10, min(75, max_pages // max(1, len(buckets))))
        selected.extend(bucket_sorted[:take])

    selected = selected[:max_pages]
    return [
        {
            "url": p.get("url"),
            "title": p.get("title", ""),
            "description": p.get("description", ""),
            "h1s": p.get("h1s", []),
            "h2s": p.get("h2s", [])[:5],
            "contentType": p.get("contentType", "other"),
            "aeoSignal": p.get("aeoSignal", 0),
            "wordCount": p.get("wordCount", 0),
            "signals": p.get("signals", []),
            "textExcerpt": p.get("text", "")[:1200]
        }
        for p in selected
    ]


async def send_callback(callback_url: str, job_id: str):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(callback_url, json={"jobId": job_id, "status": "complete"})
    except Exception:
        pass


async def cleanup_loop():
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - JOB_TTL_SECONDS
        async with JOB_LOCK:
            stale = [
                jid for jid, j in JOBS.items()
                if datetime.fromisoformat(j["createdAt"]).timestamp() < cutoff
            ]
            for jid in stale:
                JOBS.pop(jid, None)


@app.on_event("startup")
async def startup():
    asyncio.create_task(cleanup_loop())


@app.get("/")
async def root():
    return {
        "service": APP_NAME,
        "version": VERSION,
        "ok": True,
        "endpoints": ["POST /crawl/start", "GET /crawl/status/{job_id}", "GET /crawl/result/{job_id}", "GET /health"]
    }


@app.get("/health")
async def health():
    return {"ok": True, "service": APP_NAME, "version": VERSION, "jobs": len(JOBS)}


@app.post("/crawl/start")
async def crawl_start(req: CrawlStartRequest, background_tasks: BackgroundTasks):
    try:
        start_url = normalize_start_url(req.startUrl)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    job_id = req.jobId or f"crawl_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
    async with JOB_LOCK:
        JOBS[job_id] = {
            "jobId": job_id,
            "status": "queued",
            "stage": "queued",
            "startUrl": start_url,
            "limit": req.limit,
            "createdAt": now_iso(),
            "updatedAt": now_iso(),
            "urlsFound": 0,
            "pagesAttempted": 0,
            "pagesFetched": 0,
            "pagesFailed": 0,
            "elapsedSeconds": 0
        }

    background_tasks.add_task(crawl_job, job_id, req)
    return {"accepted": True, "jobId": job_id, "status": "queued", "startUrl": start_url}


@app.get("/crawl/status/{job_id}")
async def crawl_status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return {k: v for k, v in job.items() if k != "result"}


@app.get("/crawl/result/{job_id}")
async def crawl_result(job_id: str, includePages: bool = False):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job["status"] != "complete":
        return {k: v for k, v in job.items() if k != "result"}
    result = job.get("result", {})
    if includePages:
        return {"job": {k: v for k, v in job.items() if k != "result"}, **result}
    return {
        "job": {k: v for k, v in job.items() if k != "result"},
        "summary": result.get("summary", {}),
        "failures": result.get("failures", [])[:100]
    }


@app.get("/crawl/list")
async def crawl_list():
    return {
        "jobs": [
            {k: v for k, v in job.items() if k != "result"}
            for job in sorted(JOBS.values(), key=lambda j: j["createdAt"], reverse=True)
        ][:100]
    }
