import asyncio, hashlib, json, os, re, time, uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import httpx
from bs4 import BeautifulSoup
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse, Response
from starlette.routing import Route

APP_NAME = "AXO Render Crawler"
VERSION = "2.0.0"

def env_int(key, default, low=1, high=10**9):
    try:
        return max(low, min(high, int(os.getenv(key, str(default)))))
    except Exception:
        return default

CRAWL_CONCURRENCY = env_int("CRAWL_CONCURRENCY", 250, 1, 3000)
PER_HOST_CONCURRENCY = env_int("PER_HOST_CONCURRENCY", 50, 1, 500)
SITEMAP_CONCURRENCY = env_int("SITEMAP_CONCURRENCY", 100, 1, 1000)
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "8"))
MAX_BYTES_PER_PAGE = env_int("MAX_BYTES_PER_PAGE", 400000, 50000, 5000000)
MAX_BYTES_PER_SITEMAP = env_int("MAX_BYTES_PER_SITEMAP", 25000000, 1000000, 100000000)
MAX_TEXT_CHARS_PER_PAGE = env_int("MAX_TEXT_CHARS_PER_PAGE", 25000, 1000, 250000)
DEFAULT_LIMIT = env_int("DEFAULT_LIMIT", 15000, 1, 150000)
MAX_LIMIT = env_int("MAX_LIMIT", 150000, 1, 1000000)
ANALYSIS_READY_MIN_PAGES = env_int("ANALYSIS_READY_MIN_PAGES", 750, 1, 50000)
ANALYSIS_READY_MAX_SECONDS = env_int("ANALYSIS_READY_MAX_SECONDS", 60, 10, 3600)
RECOMMENDED_LLM_PAGES = env_int("RECOMMENDED_LLM_PAGES", 600, 50, 5000)
TOP_GAP_PAGES = env_int("TOP_GAP_PAGES", 300, 50, 5000)
JOB_TTL_SECONDS = env_int("JOB_TTL_SECONDS", 21600, 600, 86400)
STORE_DIR = Path(os.getenv("STORE_DIR", "/tmp/axo-crawls"))
BASE_DIR = Path(__file__).resolve().parent
HTML_FILE = os.getenv("HTML_FILE", "AXO_Diagnostic_n8n.html")
HTML_CANDIDATES = [HTML_FILE, "AXO_Diagnostic_n8n.html", "AXO_Diagnostic_n8n(1).html", "index.html"]
USER_AGENT = os.getenv("CRAWLER_USER_AGENT", "AXO-Diagnostic-Crawler/2.0 (+https://thepedowitzgroup.com)")
HTTP2_ENABLED = os.getenv("HTTP2_ENABLED", "1").lower() not in ("0", "false", "no", "off")

STORE_DIR.mkdir(parents=True, exist_ok=True)
JOBS: Dict[str, Dict[str, Any]] = {}
JOB_LOCK = asyncio.Lock()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_start_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("startUrl is required")
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    if not p.netloc:
        raise ValueError("Invalid startUrl")
    return urlunparse((p.scheme, p.netloc.lower(), p.path or "/", "", p.query, ""))

def root_domain(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    return host if len(parts) <= 2 else ".".join(parts[-2:])

def same_site(url: str, base_host: str, include_subdomains=True) -> bool:
    try:
        h = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return False
    base = base_host.lower().split(":")[0]
    base_no_www = base[4:] if base.startswith("www.") else base
    h_no_www = h[4:] if h.startswith("www.") else h
    if h_no_www == base_no_www:
        return True
    return include_subdomains and (h == root_domain(base) or h.endswith("." + root_domain(base)))

def normalize_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.netloc:
            return None
        path = p.path or "/"
        if path != "/":
            path = path.rstrip("/")
        query_pairs = [
            (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","_ga","ref","mc_cid","mc_eid"}
        ]
        return urlunparse((p.scheme, p.netloc.lower(), path, "", urlencode(query_pairs), ""))
    except Exception:
        return None

def is_asset_or_noise(url: str) -> bool:
    path = urlparse(url).path.lower()
    if re.search(r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|woff|woff2|ttf|zip|mp4|webp|avif|xml|json|csv|txt|rss)$", path):
        return True
    if re.search(r"(wp-json|wp-admin|wp-login|xmlrpc|cdn-cgi|_next/static|/feed/?$|/tag/|/author/|/category/|/page/\d+)", path):
        return True
    if re.search(r"/(login|signin|signup|cart|checkout|account)(/|$)", path):
        return True
    return False

def content_type_from_url(url: str) -> str:
    p = urlparse(url).path.lower()
    if p in ("", "/"): return "homepage"
    if re.search(r"pricing|plans|buy|cost", p): return "pricing"
    if re.search(r"compare|comparison|alternative|alternatives|vs-|versus", p): return "comparison"
    if re.search(r"demo|trial|get-started|contact-sales|contact", p): return "conversion"
    if re.search(r"solution|solutions|product|products|platform|feature|features|service|services|use-case|use-cases", p): return "product_solution"
    if re.search(r"case-stud|customer|success-stor|client|testimonial", p): return "social_proof"
    if re.search(r"resource|resources|whitepaper|guide|report|ebook|webinar|checklist|template|tool|calculator", p): return "resource"
    if re.search(r"faq|help|support|docs|documentation|knowledge", p): return "support_faq"
    if re.search(r"about|team|company|leadership|careers", p): return "company"
    if re.search(r"blog|article|post|news|insight|insights", p): return "blog"
    return "other"

def priority_score(url: str) -> int:
    weights = {"homepage":100,"pricing":96,"comparison":94,"conversion":90,"product_solution":86,"social_proof":82,"resource":74,"support_faq":68,"company":62,"blog":52,"other":40}
    score = weights.get(content_type_from_url(url), 40)
    p = urlparse(url).path.lower()
    if re.search(r"roi|calculator|benchmark|assessment|maturity|readiness|framework", p): score += 8
    if len([x for x in p.split("/") if x]) > 5: score -= 5
    return max(1, min(100, score))

def axo_url_score(url: str) -> Dict[str, Any]:
    ctype = content_type_from_url(url)
    score, signals = 1.0, []
    weights = {
        "homepage": (2.0, "homepage"), "pricing": (3.0, "decision_intent"), "comparison": (3.0, "comparison_content"),
        "conversion": (2.5, "conversion_intent"), "product_solution": (2.0, "buyer_relevance"),
        "social_proof": (2.0, "proof_content"), "resource": (2.0, "answerable_content"),
        "support_faq": (2.0, "faq_or_support"), "company": (1.0, "company_context"), "blog": (1.0, "educational_content")
    }
    if ctype in weights:
        add, signal = weights[ctype]
        score += add
        signals.append(signal)
    p = urlparse(url).path.lower()
    if re.search(r"compare|alternative|vs-|versus|best|top|roi|cost|calculator|benchmark|assessment", p):
        score += 1.5
        signals.append("ai_query_intent")
    if len([x for x in p.split("/") if x]) > 5:
        score -= 0.5
        signals.append("deep_url_penalty")
    return {"aeoSignal": max(0, min(10, round(score, 1))), "contentType": ctype, "signals": signals, "confidence": "url_only"}

def safe_job_id(job_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", job_id)

def safe_jsonl(job_id: str) -> Path:
    return STORE_DIR / f"{safe_job_id(job_id)}.jsonl"

def safe_manifest(job_id: str) -> Path:
    return STORE_DIR / f"{safe_job_id(job_id)}.manifest.json"

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None

def public_job(job: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in job.items() if k not in ("result", "partialSummary", "inventorySummary")}

def persist_job_snapshot(job_id: str) -> None:
    job = JOBS.get(job_id)
    if job:
        write_json(safe_manifest(job_id), public_job(job))

def load_job_snapshot(job_id: str) -> Optional[Dict[str, Any]]:
    return read_json(safe_manifest(job_id))

def html_path() -> Optional[Path]:
    for name in HTML_CANDIDATES:
        candidate = Path(name)
        if not candidate.is_absolute():
            candidate = BASE_DIR / candidate
        if candidate.exists() and candidate.is_file():
            return candidate
    return None

async def serve_html(request):
    path = html_path()
    if not path:
        return JSONResponse({"error":"HTML file not found", "lookedFor":[str(BASE_DIR / n) for n in HTML_CANDIDATES]}, status_code=404)
    return FileResponse(str(path), media_type="text/html")

async def favicon(request):
    return Response(status_code=204)

def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

async def fetch_limited(client: httpx.AsyncClient, url: str, max_bytes: int) -> Dict[str, Any]:
    started = time.time()
    try:
        async with client.stream("GET", url, follow_redirects=True) as resp:
            chunks, total = [], 0
            async for chunk in resp.aiter_bytes():
                if total + len(chunk) > max_bytes:
                    remaining = max_bytes - total
                    if remaining > 0:
                        chunks.append(chunk[:remaining])
                        total += remaining
                    break
                chunks.append(chunk)
                total += len(chunk)
            body = b"".join(chunks)
            text = body.decode(resp.encoding or "utf-8", errors="ignore")
            return {"ok": 200 <= resp.status_code < 400, "statusCode": resp.status_code, "finalUrl": str(resp.url), "html": text, "bytes": len(body), "ms": int((time.time()-started)*1000), "error": None}
    except Exception as e:
        return {"ok": False, "statusCode": None, "finalUrl": url, "html": "", "bytes": 0, "ms": int((time.time()-started)*1000), "error": str(e)[:300]}

def extract_page(html: str, url: str, final_url: Optional[str]=None, status_code: Optional[int]=None, fetch_ms: Optional[int]=None) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    schema_text = " ".join([s.get_text(" ", strip=True)[:4000] for s in soup.find_all("script", type="application/ld+json")])
    for tag in soup(["script","style","noscript","svg","nav","footer","header"]):
        tag.decompose()
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    desc = ""
    md = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if md and md.get("content"): desc = md.get("content","").strip()
    h1s = [x.get_text(" ", strip=True) for x in soup.find_all("h1") if x.get_text(" ", strip=True)][:3]
    h2s = [x.get_text(" ", strip=True) for x in soup.find_all("h2") if x.get_text(" ", strip=True)][:12]
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
    clipped = text[:MAX_TEXT_CHARS_PER_PAGE]
    word_count = len(re.findall(r"\w+", text))
    has_schema, has_faq = bool(schema_text), "FAQPage" in schema_text
    question_count = clipped.count("?")
    scored = axo_url_score(url)
    score, signals = scored["aeoSignal"], list(scored["signals"])
    if has_faq: score += 2; signals.append("faq_schema")
    if has_schema: score += 1; signals.append("structured_data")
    if question_count >= 5: score += 1.5; signals.append("question_rich")
    elif question_count >= 2: score += .75; signals.append("has_questions")
    if re.search(r"\b(in summary|bottom line|key takeaway|the answer is|here's how|how it works)\b", clipped, re.I):
        score += 1; signals.append("direct_answer")
    if re.search(r"\b(compare|alternative|versus|vs\.|roi|cost|pricing|calculator|benchmark)\b", clipped[:5000], re.I):
        score += 1; signals.append("comparison_or_roi_language")
    if word_count < 100: score -= 1; signals.append("thin_content")
    elif word_count > 600: score += .5; signals.append("substantial_content")
    return {
        "url": url, "finalUrl": final_url or url, "title": title[:220], "description": desc[:350],
        "h1s": h1s, "h2s": h2s, "text": clipped,
        "contentHash": hashlib.sha256(clipped.encode("utf-8", errors="ignore")).hexdigest()[:16],
        "wordCount": word_count, "statusCode": status_code, "contentType": scored["contentType"],
        "aeoSignal": max(0, min(10, round(score, 1))), "signals": list(dict.fromkeys(signals)),
        "confidence": "html", "hasStructuredData": has_schema, "hasFaqSchema": has_faq,
        "questionCount": question_count, "fetchMs": fetch_ms
    }

async def fetch_text(client, url, max_bytes):
    res = await fetch_limited(client, url, max_bytes)
    return res["html"] if res["ok"] and res["html"] else None

def parse_locs(xml: str) -> List[str]:
    return list(dict.fromkeys([m.group(1).strip() for m in re.finditer(r"<loc>\s*([^<\s]+)\s*</loc>", xml or "", re.I)]))

async def discover_urls(start_url: str, limit: int, include_subdomains: bool) -> Dict[str, Any]:
    parsed = urlparse(start_url); host = parsed.netloc.lower(); apex = host[4:] if host.startswith("www.") else host
    headers = {"User-Agent": USER_AGENT}; timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
    sitemap_candidates, errors, page_locs, seen_sitemaps, child_sitemaps = [], [], set(), set(), set()
    async with httpx.AsyncClient(headers=headers, timeout=timeout, http2=HTTP2_ENABLED) as client:
        bases = list(dict.fromkeys([f"{parsed.scheme}://{host}", f"https://{apex}", f"https://www.{apex}"]))
        for base in bases:
            try: robots = await fetch_text(client, f"{base}/robots.txt", MAX_BYTES_PER_SITEMAP)
            except Exception: robots = None
            if robots:
                for line in robots.splitlines():
                    if line.lower().startswith("sitemap:"): sitemap_candidates.append(line.split(":",1)[1].strip())
        for base in bases:
            sitemap_candidates += [f"{base}/sitemap.xml", f"{base}/sitemap_index.xml", f"{base}/sitemap-index.xml", f"{base}/wp-sitemap.xml", f"{base}/page-sitemap.xml", f"{base}/post-sitemap.xml", f"{base}/hs-sitemap-index.xml"]
        sitemap_candidates = list(dict.fromkeys([x for x in sitemap_candidates if x]))
        sem = asyncio.Semaphore(SITEMAP_CONCURRENCY)
        async def process_sitemap(sm):
            async with sem:
                if sm in seen_sitemaps: return
                seen_sitemaps.add(sm)
                try:
                    xml = await fetch_text(client, sm, MAX_BYTES_PER_SITEMAP)
                    if not xml: return
                    for loc in parse_locs(xml):
                        if re.search(r"\.xml(\.gz)?($|\?)", loc, re.I): child_sitemaps.add(loc)
                        else: page_locs.add(loc)
                except Exception as e:
                    errors.append(f"sitemap failed {sm}: {str(e)[:150]}")
        queue = list(sitemap_candidates)
        for _ in range(6):
            if not queue or len(page_locs) >= limit: break
            batch, queue = queue[:5000], queue[5000:]
            await asyncio.gather(*[process_sitemap(x) for x in batch])
            queue += [x for x in child_sitemaps if x not in seen_sitemaps][:10000]
        homepage = await fetch_text(client, start_url, MAX_BYTES_PER_PAGE)
        if homepage:
            for m in re.finditer(r'href=["\']([^"\']+)["\']', homepage, re.I):
                href = m.group(1).strip()
                if href.startswith(("mailto:","tel:","javascript:","#")): continue
                page_locs.add(urljoin(start_url, href))
    urls = set()
    for loc in page_locs:
        n = normalize_url(loc)
        if n and same_site(n, host, include_subdomains) and not is_asset_or_noise(n):
            urls.add(n)
    if not urls: urls.add(normalize_url(start_url) or start_url)
    ranked = sorted(urls, key=lambda u: priority_score(u), reverse=True)
    return {"urls": ranked[:limit], "totalDiscovered": len(ranked), "sitemapsSeen": len(seen_sitemaps), "errors": errors[:100], "inventoryBuiltAt": now_iso()}

def page_stub_from_url(url, status_code=None, error=None):
    s = axo_url_score(url)
    return {"url":url, "finalUrl":url, "title":"", "description":"", "h1s":[], "h2s":[], "text":"", "contentHash":hashlib.sha256(url.encode()).hexdigest()[:16], "wordCount":0, "statusCode":status_code, "contentType":s["contentType"], "aeoSignal":s["aeoSignal"], "signals":list(dict.fromkeys(s["signals"] + (["fetch_failed"] if error else []))), "confidence":"url_only", "hasStructuredData":False, "hasFaqSchema":False, "questionCount":0, "fetchMs":None, "error":error}

def compact_page(p, include_excerpt=False):
    row = {"url":p.get("url"), "title":p.get("title",""), "description":p.get("description",""), "contentType":p.get("contentType","other"), "aeoSignal":p.get("aeoSignal",0), "wordCount":p.get("wordCount",0), "signals":p.get("signals",[]), "confidence":p.get("confidence",""), "statusCode":p.get("statusCode")}
    if include_excerpt:
        row.update({"h1s":p.get("h1s",[]), "h2s":p.get("h2s",[])[:5], "textExcerpt":p.get("text","")[:1200]})
    return row

def sample_representative_pages(pages, max_pages):
    buckets = defaultdict(list)
    for p in pages: buckets[p.get("contentType","other")].append(p)
    selected = []
    per_bucket = max(15, max_pages // max(1, len(buckets)))
    for _, bucket in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True):
        selected += sorted(bucket, key=lambda x: x.get("aeoSignal",0), reverse=True)[:per_bucket]
    seen = {p.get("url") for p in selected}
    for p in sorted(pages, key=lambda x: x.get("aeoSignal",0), reverse=True):
        if len(selected) >= max_pages: break
        if p.get("url") not in seen:
            selected.append(p); seen.add(p.get("url"))
    return selected[:max_pages]

def build_summary(start_url, urls, pages, failures, discovered, full_complete):
    type_counts = Counter([p.get("contentType","other") for p in pages])
    signal_counts = Counter()
    for p in pages:
        for s in p.get("signals",[]): signal_counts[s]+=1
    scores = [float(p.get("aeoSignal",0)) for p in pages]
    return {
        "siteUrl": start_url, "urlsFound": len(urls), "totalDiscovered": discovered.get("totalDiscovered", len(urls)),
        "sitemapsSeen": discovered.get("sitemapsSeen",0), "pagesFetched": len(pages), "pagesFailed": len(failures),
        "fullContentComplete": full_complete, "analysisReady": len(pages)>0, "avgAeoSignal": round(sum(scores)/len(scores),2) if scores else 0,
        "highAeo": len([s for s in scores if s>=7]), "midAeo": len([s for s in scores if 4<=s<7]), "lowAeo": len([s for s in scores if s<4]),
        "typeDistribution": dict(type_counts), "topSignals": dict(signal_counts.most_common(30)),
        "topPages": [compact_page(p) for p in sorted(pages, key=lambda x:x.get("aeoSignal",0), reverse=True)[:TOP_GAP_PAGES]],
        "gapPages": [compact_page(p) for p in sorted(pages, key=lambda x:x.get("aeoSignal",0))[:TOP_GAP_PAGES]],
        "recommendedPagesForLLM": [compact_page(p, True) for p in sample_representative_pages(pages, RECOMMENDED_LLM_PAGES)],
        "crawlFinishedAt": now_iso() if full_complete else None
    }

async def crawl_job(job_id, payload):
    started = time.time()
    start_url = payload["startUrl"]; limit = max(1, min(MAX_LIMIT, int(payload.get("limit", DEFAULT_LIMIT))))
    include_subdomains = bool(payload.get("includeSubdomains", True)); fetch_content = bool(payload.get("fetchContent", True)); callback_url = payload.get("callbackUrl")
    jsonl_path = safe_jsonl(job_id)
    if jsonl_path.exists(): jsonl_path.unlink()
    try:
        async with JOB_LOCK:
            JOBS[job_id].update({"status":"discovering","stage":"discovering","updatedAt":now_iso()})
            persist_job_snapshot(job_id)
        discovered = await discover_urls(start_url, limit, include_subdomains); urls = discovered["urls"]
        inventory_summary = {"siteUrl":start_url, "urlsFound":len(urls), "totalDiscovered":discovered.get("totalDiscovered",len(urls)), "sitemapsSeen":discovered.get("sitemapsSeen",0), "typeDistribution":dict(Counter([content_type_from_url(u) for u in urls])), "topPages":[compact_page(page_stub_from_url(u)) for u in sorted(urls, key=priority_score, reverse=True)[:TOP_GAP_PAGES]], "gapPages":[], "recommendedPagesForLLM":[], "avgAeoSignal":0, "analysisReady":False, "fullContentComplete":False, "inventoryOnly":True}
        async with JOB_LOCK:
            JOBS[job_id].update({"status":"content_crawling","stage":"content_crawling","urlsFound":len(urls),"totalDiscovered":discovered.get("totalDiscovered",len(urls)),"sitemapsSeen":discovered.get("sitemapsSeen",0),"discoveryErrors":discovered.get("errors",[]),"inventorySummary":inventory_summary,"updatedAt":now_iso()})
            persist_job_snapshot(job_id)
        pages, failures, content_hashes = [], [], set()
        headers={"User-Agent":USER_AGENT}; timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
        global_sem=asyncio.Semaphore(CRAWL_CONCURRENCY); host_sems=defaultdict(lambda: asyncio.Semaphore(PER_HOST_CONCURRENCY))
        last_summary_at=0; analysis_ready_sent=False
        async with httpx.AsyncClient(headers=headers, timeout=timeout, http2=HTTP2_ENABLED) as client:
            async def process_one(i, url):
                nonlocal last_summary_at, analysis_ready_sent
                async with global_sem:
                    async with host_sems[urlparse(url).netloc.lower()]:
                        if fetch_content:
                            res=await fetch_limited(client,url,MAX_BYTES_PER_PAGE)
                            if res["ok"] and res["html"]: page=extract_page(res["html"],url,res["finalUrl"],res["statusCode"],res["ms"])
                            else:
                                page=page_stub_from_url(url,res.get("statusCode"),res.get("error")); failures.append({"url":url,"error":res.get("error"),"statusCode":res.get("statusCode")})
                        else: page=page_stub_from_url(url)
                        if page["contentHash"] not in content_hashes or page["confidence"]=="url_only":
                            content_hashes.add(page["contentHash"]); pages.append(page); append_jsonl(jsonl_path,page)
                        elapsed=time.time()-started
                        enough = (len(pages)>=min(ANALYSIS_READY_MIN_PAGES,max(1,len(urls))) or elapsed>=ANALYSIS_READY_MAX_SECONDS) and len(pages)>0
                        should_update = i%25==0 or i==len(urls) or time.time()-last_summary_at>5 or (enough and not analysis_ready_sent)
                        if should_update:
                            last_summary_at=time.time(); partial=build_summary(start_url,urls,pages,failures,discovered,False)
                            async with JOB_LOCK:
                                JOBS[job_id].update({"pagesAttempted":i,"pagesFetched":len(pages),"pagesFailed":len(failures),"elapsedSeconds":round(elapsed,1),"analysisReady":bool(enough),"analysisReadyAt":JOBS[job_id].get("analysisReadyAt") or (now_iso() if enough else None),"partialSummary":partial,"updatedAt":now_iso()})
                                persist_job_snapshot(job_id)
                            if enough and not analysis_ready_sent:
                                analysis_ready_sent=True
                                if callback_url: await send_callback(callback_url,job_id,"analysis_ready",partial)
            await asyncio.gather(*[process_one(i+1,u) for i,u in enumerate(urls)])
        final=build_summary(start_url,urls,pages,failures,discovered,True)
        async with JOB_LOCK:
            JOBS[job_id].update({"status":"complete","stage":"complete","completedAt":now_iso(),"updatedAt":now_iso(),"pagesAttempted":len(urls),"pagesFetched":len(pages),"pagesFailed":len(failures),"elapsedSeconds":round(time.time()-started,1),"analysisReady":True,"jsonlPath":str(jsonl_path),"result":{"summary":final,"failures":failures[:500]}})
            write_json(safe_manifest(job_id), {**public_job(JOBS[job_id]), "summary": final, "failures": failures[:500]})
        if callback_url: await send_callback(callback_url,job_id,"complete",final)
    except Exception as e:
        async with JOB_LOCK:
            JOBS[job_id].update({"status":"failed","stage":"failed","error":str(e)[:1000],"updatedAt":now_iso()})
            persist_job_snapshot(job_id)

async def cleanup_loop():
    while True:
        await asyncio.sleep(300)
        cutoff=time.time()-JOB_TTL_SECONDS
        async with JOB_LOCK:
            for jid, job in list(JOBS.items()):
                try:
                    if datetime.fromisoformat(job["createdAt"]).timestamp()<cutoff: JOBS.pop(jid,None)
                except Exception: JOBS.pop(jid,None)

async def startup(): asyncio.create_task(cleanup_loop())

async def root(request): return await serve_html(request)
async def diagnostic(request): return await serve_html(request)
async def app_page(request): return await serve_html(request)
async def api_info(request): return JSONResponse({"service":APP_NAME,"version":VERSION,"ok":True,"model":"two-lane crawler"})
async def health(request): return JSONResponse({"ok":True,"service":APP_NAME,"version":VERSION,"jobs":len(JOBS),"settings":{"CRAWL_CONCURRENCY":CRAWL_CONCURRENCY,"PER_HOST_CONCURRENCY":PER_HOST_CONCURRENCY,"SITEMAP_CONCURRENCY":SITEMAP_CONCURRENCY,"REQUEST_TIMEOUT_SECONDS":REQUEST_TIMEOUT_SECONDS,"HTTP2_ENABLED":HTTP2_ENABLED}})
async def crawl_start(request):
    try: payload=await request.json()
    except Exception: return JSONResponse({"error":"Invalid JSON body"}, status_code=400)
    try: start_url=normalize_start_url(payload.get("startUrl") or payload.get("url") or "")
    except Exception as e: return JSONResponse({"error":str(e)}, status_code=400)
    job_id=payload.get("jobId") or f"crawl_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
    payload["startUrl"]=start_url; payload["limit"]=max(1,min(MAX_LIMIT,int(payload.get("limit",DEFAULT_LIMIT))))
    async with JOB_LOCK:
        JOBS[job_id]={"jobId":job_id,"status":"queued","stage":"queued","startUrl":start_url,"limit":payload["limit"],"createdAt":now_iso(),"updatedAt":now_iso(),"urlsFound":0,"totalDiscovered":0,"pagesAttempted":0,"pagesFetched":0,"pagesFailed":0,"elapsedSeconds":0,"analysisReady":False,"jsonlPath":str(safe_jsonl(job_id)),"manifestPath":str(safe_manifest(job_id)),"statusUrl":f"/crawl/status/{job_id}","resultUrl":f"/crawl/result/{job_id}","pagesUrl":f"/crawl/pages/{job_id}"}
        persist_job_snapshot(job_id)
    return JSONResponse({"accepted":True,"jobId":job_id,"status":"queued","startUrl":start_url,"statusUrl":f"/crawl/status/{job_id}","resultUrl":f"/crawl/result/{job_id}","pagesUrl":f"/crawl/pages/{job_id}","jsonlPath":str(safe_jsonl(job_id))}, background=BackgroundTask(crawl_job,job_id,payload))
async def crawl_status(request):
    job_id = request.path_params["job_id"]
    job = JOBS.get(job_id)
    if job:
        return JSONResponse(public_job(job))
    snapshot = load_job_snapshot(job_id)
    if snapshot:
        return JSONResponse(snapshot)
    return JSONResponse({"error":"job not found", "jobId": job_id, "hint":"This job is not in memory and no manifest exists. Make sure the frontend polls the exact jobId returned by /crawl/start."}, status_code=404)
async def crawl_pages(request):
    """Return pages in paginated chunks from JSONL. offset + limit params."""
    job_id = request.path_params["job_id"]
    offset = int(request.query_params.get("offset", 0))
    limit  = int(request.query_params.get("limit", 100))
    job = JOBS.get(job_id)
    snapshot = load_job_snapshot(job_id) or {}
    jsonl_path = (job or snapshot).get("jsonlPath") or str(safe_jsonl(job_id))
    if not Path(jsonl_path).exists():
        # Fall back to partialSummary pages if JSONL gone
        ps = (job or {}).get("partialSummary") or {}
        pages = ps.get("recommendedPagesForLLM", [])
        return JSONResponse({"pages": pages[offset:offset+limit], "total": len(pages), "offset": offset, "limit": limit, "source": "partial_summary"})
    all_pages = [json.loads(line) for line in Path(jsonl_path).open("r", encoding="utf-8") if line.strip()]
    chunk = all_pages[offset:offset+limit]
    return JSONResponse({"pages": chunk, "total": len(all_pages), "offset": offset, "limit": limit, "source": "jsonl"})

async def crawl_result(request):
    job_id=request.path_params["job_id"]; mode=request.query_params.get("mode","final"); include_pages=request.query_params.get("includePages","false").lower() in ("1","true","yes")
    job=JOBS.get(job_id)
    snapshot = load_job_snapshot(job_id)
    if not job and not snapshot: return JSONResponse({"error":"job not found", "jobId": job_id}, status_code=404)
    if not job:
        # Durable completed/failed snapshot after a restart or multi-instance route.
        if snapshot.get("summary"):
            return JSONResponse({"job": {k:v for k,v in snapshot.items() if k not in ("summary","failures")}, "summary": snapshot.get("summary",{}), "failures": snapshot.get("failures",[])[:100], "source":"manifest"})
        return JSONResponse(snapshot)
    public=public_job(job)
    if job.get("status")=="complete":
        result=job.get("result",{}); payload={"job":public,"summary":result.get("summary",{}),"failures":result.get("failures",[])[:100]}
        if include_pages and job.get("jsonlPath") and Path(job["jsonlPath"]).exists():
            payload["pages"]=[json.loads(line) for line in Path(job["jsonlPath"]).open("r",encoding="utf-8") if line.strip()]
        return JSONResponse(payload)
    if mode in ("analysis","partial") and job.get("analysisReady") and job.get("partialSummary"): return JSONResponse({"job":public,"summary":job.get("partialSummary"),"partial":True})
    if mode=="inventory" and job.get("inventorySummary"): return JSONResponse({"job":public,"summary":job.get("inventorySummary"),"inventory":True})
    return JSONResponse(public)

async def send_callback(callback_url, job_id, status, summary=None):
    try:
        payload = {"jobId": job_id, "status": status, "statusUrl": f"/crawl/status/{job_id}", "resultUrl": f"/crawl/result/{job_id}", "pagesUrl": f"/crawl/pages/{job_id}"}
        if summary is not None:
            # Send lean summary -- capped to avoid n8n payload limits (~100KB max)
            lean = {
                "pagesFetched":    summary.get("pagesFetched", 0),
                "pagesFailed":     summary.get("pagesFailed", 0),
                "urlsFound":       summary.get("urlsFound", 0),
                "totalDiscovered": summary.get("totalDiscovered", 0),
                "avgAeoSignal":    summary.get("avgAeoSignal", 0),
                "highAeo":         summary.get("highAeo", 0),
                "midAeo":          summary.get("midAeo", 0),
                "lowAeo":          summary.get("lowAeo", 0),
                "typeDistribution":summary.get("typeDistribution", {}),
                "topSignals":      dict(list(summary.get("topSignals", {}).items())[:20]),
                "recommendedPagesForLLM": summary.get("recommendedPagesForLLM", [])[:30],
                "topPages":        summary.get("topPages", [])[:20],
                "gapPages":        summary.get("gapPages", [])[:20],
                "partial":         summary.get("partial", False),
            }
            payload["summary"] = lean
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(callback_url, json=payload)
    except Exception: pass

async def crawl_list(request):
    async with JOB_LOCK: jobs=[{k:v for k,v in j.items() if k not in ("result","partialSummary","inventorySummary")} for j in JOBS.values()]
    jobs.sort(key=lambda j:j.get("createdAt",""), reverse=True)
    return JSONResponse({"jobs":jobs[:100]})

routes=[Route("/",root),Route("/diagnostic",diagnostic),Route("/app",app_page),Route("/index.html",diagnostic),Route("/favicon.ico",favicon),Route("/api",api_info),Route("/health",health),Route("/crawl/start",crawl_start,methods=["POST"]),Route("/crawl/status/{job_id}",crawl_status),Route("/crawl/result/{job_id}",crawl_result),Route("/crawl/pages/{job_id}",crawl_pages),Route("/crawl/list",crawl_list)]
class FrameEmbeddingMiddleware:
    """Remove frame-blocking headers so the diagnostic can load inside HubSpot/custom-domain iframes.

    Optional: set FRAME_ANCESTORS to a CSP value such as:
      'self' https://*.hubspot.com https://*.hs-sites.com https://www.yourdomain.com
    If FRAME_ANCESTORS is empty, no frame-ancestors CSP is sent, which allows embedding.
    """
    def __init__(self, app):
        self.app = app
        self.frame_ancestors = os.getenv("FRAME_ANCESTORS", "").strip()

    async def __call__(self, scope, receive, send):
        async def send_wrapper(message):
            if message.get("type") == "http.response.start":
                headers = []
                for k, v in message.get("headers", []):
                    lk = k.lower()
                    # These are what cause Chrome/HubSpot to show "refused to connect" in iframes.
                    if lk in (b"x-frame-options", b"frame-options"):
                        continue
                    # Remove existing CSP so a stale/restrictive frame-ancestors does not win.
                    if lk == b"content-security-policy":
                        continue
                    headers.append((k, v))
                if self.frame_ancestors:
                    csp = f"frame-ancestors {self.frame_ancestors};".encode("utf-8")
                    headers.append((b"content-security-policy", csp))
                message["headers"] = headers
            await send(message)
        await self.app(scope, receive, send_wrapper)


app=Starlette(debug=False,routes=routes,on_startup=[startup])

# Basic CORS support for the HubSpot-hosted page and n8n/browser requests.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Must be added after CORS so it runs first on the outbound response and strips any
# frame-blocking headers. This allows HubSpot iframe embedding, including custom domains.
app.add_middleware(FrameEmbeddingMiddleware)
