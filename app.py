"""
AXO Enterprise Site Corpus Builder

Purpose
- Fully crawl a target website using sitemap discovery plus recursive internal link discovery.
- Build a durable JSONL corpus for downstream n8n workflows and multi-LLM analysis.
- Keep memory usage bounded by storing full page records on disk and compact metadata in memory.
- Provide job lifecycle endpoints, compact status/result payloads, and safe callback handoff.

Recommended deployment
- Run behind an API gateway or private network.
- Set ALLOWED_CALLBACK_HOSTS for n8n/webhook domains.
- Mount STORE_DIR to persistent storage in production.

Start example
uvicorn app_enterprise:app --host 0.0.0.0 --port 8000

POST /crawl/start
{
  "startUrl": "https://example.com",
  "limit": 25000,
  "callbackUrl": "https://your-n8n-domain/webhook/axo-crawl-complete",
  "includeSubdomains": true,
  "respectRobots": true,
  "fetchContent": true
}
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
import re
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup
from starlette.applications import Starlette
from starlette.background import BackgroundTask
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, PlainTextResponse
from starlette.routing import Route

APP_NAME = "AXO Enterprise Corpus Builder"
VERSION = "3.0.0"


def env_int(key: str, default: int, low: int = 1, high: int = 10**9) -> int:
    try:
        return max(low, min(high, int(os.getenv(key, str(default)))))
    except Exception:
        return default


def env_float(key: str, default: float, low: float = 0.1, high: float = 10**9) -> float:
    try:
        return max(low, min(high, float(os.getenv(key, str(default)))))
    except Exception:
        return default


# Conservative defaults. Override only when you control the target or have permission.
CRAWL_CONCURRENCY = env_int("CRAWL_CONCURRENCY", 60, 1, 1000)
PER_HOST_CONCURRENCY = env_int("PER_HOST_CONCURRENCY", 8, 1, 100)
SITEMAP_CONCURRENCY = env_int("SITEMAP_CONCURRENCY", 25, 1, 250)
REQUEST_TIMEOUT_SECONDS = env_float("REQUEST_TIMEOUT_SECONDS", 15.0, 1.0, 120.0)
RETRY_COUNT = env_int("RETRY_COUNT", 2, 0, 5)
RETRY_BACKOFF_SECONDS = env_float("RETRY_BACKOFF_SECONDS", 0.7, 0.1, 10.0)
MAX_BYTES_PER_PAGE = env_int("MAX_BYTES_PER_PAGE", 750_000, 50_000, 10_000_000)
MAX_BYTES_PER_SITEMAP = env_int("MAX_BYTES_PER_SITEMAP", 30_000_000, 1_000_000, 250_000_000)
MAX_TEXT_CHARS_PER_PAGE = env_int("MAX_TEXT_CHARS_PER_PAGE", 60_000, 1_000, 500_000)
DEFAULT_LIMIT = env_int("DEFAULT_LIMIT", 25_000, 1, 500_000)
MAX_LIMIT = env_int("MAX_LIMIT", 250_000, 1, 1_000_000)
MAX_CRAWL_SECONDS = env_int("MAX_CRAWL_SECONDS", 14_400, 60, 86_400)
JOB_TTL_SECONDS = env_int("JOB_TTL_SECONDS", 86_400, 600, 604_800)
STATUS_FLUSH_SECONDS = env_float("STATUS_FLUSH_SECONDS", 5.0, 1.0, 60.0)
MAX_LINKS_EXTRACTED_PER_PAGE = env_int("MAX_LINKS_EXTRACTED_PER_PAGE", 1000, 50, 10_000)
RECOMMENDED_LLM_PAGES = env_int("RECOMMENDED_LLM_PAGES", 600, 50, 5000)
TOP_GAP_PAGES = env_int("TOP_GAP_PAGES", 300, 50, 5000)
STORE_DIR = Path(os.getenv("STORE_DIR", "/tmp/axo-crawls"))
USER_AGENT = os.getenv("CRAWLER_USER_AGENT", "AXO-Diagnostic-Crawler/3.0 (+https://thepedowitzgroup.com)")
HTTP2_ENABLED = os.getenv("HTTP2_ENABLED", "1").lower() not in {"0", "false", "no", "off"}
DEFAULT_RESPECT_ROBOTS = os.getenv("RESPECT_ROBOTS", "1").lower() not in {"0", "false", "no", "off"}
ALLOWED_CALLBACK_HOSTS = {
    h.strip().lower()
    for h in os.getenv("ALLOWED_CALLBACK_HOSTS", "pedowitzgroup.app.n8n.cloud,n8n.cloud").split(",")
    if h.strip()
}
ALLOW_ALL_CALLBACKS = os.getenv("ALLOW_ALL_CALLBACKS", "1").lower() in {"1", "true", "yes", "on"}

STORE_DIR.mkdir(parents=True, exist_ok=True)
JOBS: Dict[str, Dict[str, Any]] = {}
JOB_LOCK = asyncio.Lock()

TRACKING_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "gclid", "fbclid", "msclkid", "_ga", "_gl", "mc_cid", "mc_eid", "ref", "source"
}

ASSET_EXTENSIONS = re.compile(
    r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|ico|woff|woff2|ttf|otf|zip|gz|mp4|mov|webm|webp|avif|json|csv|txt|rss|atom|doc|docx|ppt|pptx|xls|xlsx)$",
    re.I,
)

NOISE_PATHS = re.compile(
    r"(wp-json|wp-admin|wp-login|xmlrpc|cdn-cgi|_next/static|/feed/?$|/tag/|/author/|/category/|/page/\d+|/cart|/checkout|/account|/login|/signin)",
    re.I,
)


@dataclass(frozen=True)
class FetchResult:
    ok: bool
    status_code: Optional[int]
    final_url: str
    body: bytes
    content_type: str
    ms: int
    error: Optional[str] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


def normalize_start_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("startUrl is required")
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    if p.scheme not in {"http", "https"} or not p.netloc:
        raise ValueError("Invalid startUrl")
    path = p.path or "/"
    return urlunparse((p.scheme, p.netloc.lower(), path, "", p.query, ""))


def root_domain(host: str) -> str:
    host = (host or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    # Simple heuristic. For public suffix precision, install/use tldextract.
    return host if len(parts) <= 2 else ".".join(parts[-2:])


def same_site(url: str, base_host: str, include_subdomains: bool = True) -> bool:
    try:
        h = urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return False
    base = base_host.lower().split(":")[0]
    base_no_www = base[4:] if base.startswith("www.") else base
    h_no_www = h[4:] if h.startswith("www.") else h
    if h_no_www == base_no_www:
        return True
    if not include_subdomains:
        return False
    rd = root_domain(base)
    return h_no_www == rd or h_no_www.endswith("." + rd)


def normalize_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if p.scheme not in {"http", "https"} or not p.netloc:
            return None
        path = p.path or "/"
        if path != "/":
            path = re.sub(r"/{2,}", "/", path).rstrip("/")
        query_pairs = [
            (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in TRACKING_QUERY_KEYS
        ]
        query_pairs.sort(key=lambda kv: kv[0].lower())
        return urlunparse((p.scheme, p.netloc.lower(), path, "", urlencode(query_pairs), ""))
    except Exception:
        return None


def is_asset_or_noise(url: str) -> bool:
    path = urlparse(url).path.lower()
    if ASSET_EXTENSIONS.search(path):
        return True
    if NOISE_PATHS.search(path):
        return True
    return False


def content_type_from_url(url: str) -> str:
    p = urlparse(url).path.lower()
    if p in {"", "/"}:
        return "homepage"
    if re.search(r"pricing|plans|buy|cost", p):
        return "pricing"
    if re.search(r"compare|comparison|alternative|alternatives|vs-|versus", p):
        return "comparison"
    if re.search(r"demo|trial|get-started|contact-sales|contact", p):
        return "conversion"
    if re.search(r"solution|solutions|product|products|platform|feature|features|service|services|use-case|use-cases", p):
        return "product_solution"
    if re.search(r"case-stud|customer|success-stor|client|testimonial", p):
        return "social_proof"
    if re.search(r"resource|resources|whitepaper|guide|report|ebook|webinar|checklist|template|tool|calculator", p):
        return "resource"
    if re.search(r"faq|help|support|docs|documentation|knowledge", p):
        return "support_faq"
    if re.search(r"about|team|company|leadership|careers", p):
        return "company"
    if re.search(r"blog|article|post|news|insight|insights", p):
        return "blog"
    return "other"


def priority_score(url: str) -> int:
    weights = {
        "homepage": 100,
        "pricing": 96,
        "comparison": 94,
        "conversion": 90,
        "product_solution": 86,
        "social_proof": 82,
        "resource": 74,
        "support_faq": 68,
        "company": 62,
        "blog": 52,
        "other": 40,
    }
    score = weights.get(content_type_from_url(url), 40)
    p = urlparse(url).path.lower()
    if re.search(r"roi|calculator|benchmark|assessment|maturity|readiness|framework", p):
        score += 8
    if len([x for x in p.split("/") if x]) > 5:
        score -= 5
    return max(1, min(100, score))


def axo_url_score(url: str) -> Dict[str, Any]:
    ctype = content_type_from_url(url)
    score = 1.0
    signals: List[str] = []
    weights = {
        "homepage": (2.0, "homepage"),
        "pricing": (3.0, "decision_intent"),
        "comparison": (3.0, "comparison_content"),
        "conversion": (2.5, "conversion_intent"),
        "product_solution": (2.0, "buyer_relevance"),
        "social_proof": (2.0, "proof_content"),
        "resource": (2.0, "answerable_content"),
        "support_faq": (2.0, "faq_or_support"),
        "company": (1.0, "company_context"),
        "blog": (1.0, "educational_content"),
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
    return {
        "aeoSignal": max(0, min(10, round(score, 1))),
        "contentType": ctype,
        "signals": signals,
        "confidence": "url_only",
    }


def safe_job_id(job_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", job_id)


def jsonl_path_for(job_id: str) -> Path:
    return STORE_DIR / f"{safe_job_id(job_id)}.pages.jsonl"


def manifest_path_for(job_id: str) -> Path:
    return STORE_DIR / f"{safe_job_id(job_id)}.manifest.json"


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_jsonl_limited(path: Path, limit: int = 1000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if len(rows) >= limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def decode_body(body: bytes, encoding: Optional[str]) -> str:
    if not body:
        return ""
    enc = encoding or "utf-8"
    try:
        return body.decode(enc, errors="ignore")
    except Exception:
        return body.decode("utf-8", errors="ignore")


async def fetch_limited(client: httpx.AsyncClient, url: str, max_bytes: int) -> FetchResult:
    last_error: Optional[str] = None
    for attempt in range(RETRY_COUNT + 1):
        started = time.time()
        try:
            async with client.stream("GET", url, follow_redirects=True) as resp:
                chunks: List[bytes] = []
                total = 0
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
                content_type = resp.headers.get("content-type", "")[:200]
                ok = 200 <= resp.status_code < 400
                if ok or resp.status_code not in {408, 425, 429, 500, 502, 503, 504}:
                    return FetchResult(ok, resp.status_code, str(resp.url), body, content_type, int((time.time() - started) * 1000))
                last_error = f"retryable status {resp.status_code}"
        except Exception as e:
            last_error = str(e)[:300]
        if attempt < RETRY_COUNT:
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * (2**attempt))
    return FetchResult(False, None, url, b"", "", 0, last_error)


def extract_links(html: str, base_url: str, base_host: str, include_subdomains: bool) -> List[str]:
    links: List[str] = []
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup.find_all("a", href=True):
        href = (tag.get("href") or "").strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        normalized = normalize_url(urljoin(base_url, href))
        if not normalized:
            continue
        if not same_site(normalized, base_host, include_subdomains):
            continue
        if is_asset_or_noise(normalized):
            continue
        links.append(normalized)
        if len(links) >= MAX_LINKS_EXTRACTED_PER_PAGE:
            break
    return list(dict.fromkeys(links))


def extract_schema_text(soup: BeautifulSoup) -> str:
    pieces: List[str] = []
    for s in soup.find_all("script", type="application/ld+json"):
        pieces.append(s.get_text(" ", strip=True)[:5000])
    return " ".join(pieces)


def extract_page(html: str, requested_url: str, final_url: str, status_code: Optional[int], fetch_ms: int, links: List[str]) -> Dict[str, Any]:
    soup = BeautifulSoup(html or "", "html.parser")
    schema_text = extract_schema_text(soup)
    canonical = ""
    canonical_tag = soup.find("link", rel=lambda value: value and "canonical" in value)
    if canonical_tag and canonical_tag.get("href"):
        canonical = normalize_url(urljoin(final_url, canonical_tag.get("href", ""))) or ""

    for tag in soup(["script", "style", "noscript", "svg", "nav", "footer", "header"]):
        tag.decompose()

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    desc = ""
    md = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    if md and md.get("content"):
        desc = md.get("content", "").strip()

    h1s = [x.get_text(" ", strip=True) for x in soup.find_all("h1") if x.get_text(" ", strip=True)][:3]
    h2s = [x.get_text(" ", strip=True) for x in soup.find_all("h2") if x.get_text(" ", strip=True)][:16]
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
    clipped = text[:MAX_TEXT_CHARS_PER_PAGE]
    word_count = len(re.findall(r"\w+", text))
    has_schema = bool(schema_text)
    has_faq = "FAQPage" in schema_text
    has_how_to = "HowTo" in schema_text
    has_breadcrumb = "BreadcrumbList" in schema_text
    question_count = clipped.count("?")

    scored = axo_url_score(final_url or requested_url)
    score = scored["aeoSignal"]
    signals = list(scored["signals"])

    if has_faq:
        score += 2
        signals.append("faq_schema")
    if has_how_to:
        score += 1.5
        signals.append("howto_schema")
    if has_breadcrumb:
        score += 0.5
        signals.append("breadcrumb_schema")
    if has_schema:
        score += 1
        signals.append("structured_data")
    if question_count >= 5:
        score += 1.5
        signals.append("question_rich")
    elif question_count >= 2:
        score += 0.75
        signals.append("has_questions")
    if re.search(r"\b(in summary|bottom line|key takeaway|the answer is|here's how|how it works|what this means)\b", clipped, re.I):
        score += 1
        signals.append("direct_answer")
    if re.search(r"\b(compare|alternative|versus|vs\.|roi|cost|pricing|calculator|benchmark|pros and cons)\b", clipped[:7000], re.I):
        score += 1
        signals.append("comparison_or_roi_language")
    if word_count < 100:
        score -= 1
        signals.append("thin_content")
    elif word_count > 600:
        score += 0.5
        signals.append("substantial_content")
    elif word_count > 1200:
        score += 1
        signals.append("long_form_content")

    content_hash = hashlib.sha256(clipped.encode("utf-8", errors="ignore")).hexdigest()[:20]
    return {
        "url": requested_url,
        "finalUrl": final_url or requested_url,
        "canonicalUrl": canonical,
        "title": title[:260],
        "description": desc[:500],
        "h1s": h1s,
        "h2s": h2s,
        "text": clipped,
        "contentHash": content_hash,
        "wordCount": word_count,
        "statusCode": status_code,
        "contentType": scored["contentType"],
        "aeoSignal": max(0, min(10, round(score, 1))),
        "signals": list(dict.fromkeys(signals)),
        "confidence": "html",
        "hasStructuredData": has_schema,
        "hasFaqSchema": has_faq,
        "hasHowToSchema": has_how_to,
        "hasBreadcrumbSchema": has_breadcrumb,
        "questionCount": question_count,
        "outboundInternalLinks": links[:250],
        "fetchMs": fetch_ms,
        "crawledAt": now_iso(),
    }


def page_stub_from_url(url: str, status_code: Optional[int] = None, error: Optional[str] = None) -> Dict[str, Any]:
    s = axo_url_score(url)
    signals = list(dict.fromkeys(s["signals"] + (["fetch_failed"] if error else [])))
    return {
        "url": url,
        "finalUrl": url,
        "canonicalUrl": "",
        "title": "",
        "description": "",
        "h1s": [],
        "h2s": [],
        "text": "",
        "contentHash": hashlib.sha256(url.encode()).hexdigest()[:20],
        "wordCount": 0,
        "statusCode": status_code,
        "contentType": s["contentType"],
        "aeoSignal": s["aeoSignal"],
        "signals": signals,
        "confidence": "url_only",
        "hasStructuredData": False,
        "hasFaqSchema": False,
        "hasHowToSchema": False,
        "hasBreadcrumbSchema": False,
        "questionCount": 0,
        "outboundInternalLinks": [],
        "fetchMs": None,
        "error": error,
        "crawledAt": now_iso(),
    }


def compact_page(p: Dict[str, Any], include_excerpt: bool = False) -> Dict[str, Any]:
    row = {
        "url": p.get("url"),
        "finalUrl": p.get("finalUrl"),
        "title": p.get("title", ""),
        "description": p.get("description", ""),
        "contentType": p.get("contentType", "other"),
        "aeoSignal": p.get("aeoSignal", 0),
        "wordCount": p.get("wordCount", 0),
        "signals": p.get("signals", []),
        "confidence": p.get("confidence", ""),
        "statusCode": p.get("statusCode"),
        "hasFaqSchema": p.get("hasFaqSchema", False),
        "hasStructuredData": p.get("hasStructuredData", False),
    }
    if include_excerpt:
        row.update({
            "h1s": p.get("h1s", []),
            "h2s": p.get("h2s", [])[:6],
            "textExcerpt": (p.get("text") or "")[:1500],
        })
    return row


def parse_locs(xml: str) -> List[str]:
    return list(dict.fromkeys([m.group(1).strip() for m in re.finditer(r"<loc>\s*([^<]+?)\s*</loc>", xml or "", re.I)]))


def maybe_decode_gzip(body: bytes, url: str) -> str:
    if url.lower().endswith(".gz"):
        try:
            body = gzip.decompress(body)
        except Exception:
            pass
    return body.decode("utf-8", errors="ignore")


async def fetch_text(client: httpx.AsyncClient, url: str, max_bytes: int) -> Optional[str]:
    res = await fetch_limited(client, url, max_bytes)
    if not res.ok or not res.body:
        return None
    return maybe_decode_gzip(res.body, url)


async def build_robots(client: httpx.AsyncClient, start_url: str, respect: bool) -> Tuple[Optional[RobotFileParser], List[str]]:
    if not respect:
        return None, []
    p = urlparse(start_url)
    robots_url = f"{p.scheme}://{p.netloc}/robots.txt"
    text = await fetch_text(client, robots_url, MAX_BYTES_PER_SITEMAP)
    if not text:
        return None, []
    parser = RobotFileParser()
    parser.set_url(robots_url)
    parser.parse(text.splitlines())
    sitemaps: List[str] = []
    for line in text.splitlines():
        if line.lower().startswith("sitemap:"):
            sm = line.split(":", 1)[1].strip()
            if sm:
                sitemaps.append(sm)
    return parser, sitemaps


def robots_allowed(parser: Optional[RobotFileParser], url: str) -> bool:
    if not parser:
        return True
    try:
        return parser.can_fetch(USER_AGENT, url)
    except Exception:
        return True


async def discover_sitemap_urls(client: httpx.AsyncClient, start_url: str, limit: int, include_subdomains: bool, base_host: str, robots_sitemaps: List[str]) -> Dict[str, Any]:
    parsed = urlparse(start_url)
    host = parsed.netloc.lower()
    apex = host[4:] if host.startswith("www.") else host
    bases = list(dict.fromkeys([f"{parsed.scheme}://{host}", f"https://{apex}", f"https://www.{apex}"]))

    candidates: List[str] = list(robots_sitemaps)
    for base in bases:
        candidates.extend([
            f"{base}/sitemap.xml",
            f"{base}/sitemap_index.xml",
            f"{base}/sitemap-index.xml",
            f"{base}/wp-sitemap.xml",
            f"{base}/page-sitemap.xml",
            f"{base}/post-sitemap.xml",
            f"{base}/hs-sitemap-index.xml",
        ])
    candidates = list(dict.fromkeys([x for x in candidates if x]))

    seen_sitemaps: Set[str] = set()
    page_locs: Set[str] = set()
    errors: List[str] = []
    sem = asyncio.Semaphore(SITEMAP_CONCURRENCY)

    async def process_sitemap(sm: str) -> Tuple[List[str], List[str]]:
        async with sem:
            if sm in seen_sitemaps:
                return [], []
            seen_sitemaps.add(sm)
            try:
                xml = await fetch_text(client, sm, MAX_BYTES_PER_SITEMAP)
                if not xml:
                    return [], []
                child_sitemaps: List[str] = []
                pages: List[str] = []
                for loc in parse_locs(xml):
                    if re.search(r"\.xml(\.gz)?($|\?)", loc, re.I):
                        child_sitemaps.append(loc)
                    else:
                        pages.append(loc)
                return child_sitemaps, pages
            except Exception as e:
                errors.append(f"sitemap failed {sm}: {str(e)[:180]}")
                return [], []

    queue: Deque[str] = deque(candidates)
    rounds = 0
    while queue and len(page_locs) < limit and rounds < 8:
        rounds += 1
        batch: List[str] = []
        while queue and len(batch) < 2000:
            sm = queue.popleft()
            if sm not in seen_sitemaps:
                batch.append(sm)
        if not batch:
            break
        results = await asyncio.gather(*[process_sitemap(sm) for sm in batch])
        for child_sitemaps, pages in results:
            for child in child_sitemaps:
                if child not in seen_sitemaps:
                    queue.append(child)
            for loc in pages:
                normalized = normalize_url(loc)
                if not normalized:
                    continue
                if not same_site(normalized, base_host, include_subdomains):
                    continue
                if is_asset_or_noise(normalized):
                    continue
                page_locs.add(normalized)
                if len(page_locs) >= limit:
                    break

    ranked = sorted(page_locs, key=priority_score, reverse=True)
    return {
        "urls": ranked[:limit],
        "sitemapsSeen": len(seen_sitemaps),
        "sitemapErrors": errors[:100],
    }


def sample_representative_pages(metadata: Iterable[Dict[str, Any]], max_pages: int) -> List[Dict[str, Any]]:
    pages = list(metadata)
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for p in pages:
        buckets[p.get("contentType", "other")].append(p)
    selected: List[Dict[str, Any]] = []
    if buckets:
        per_bucket = max(15, max_pages // max(1, len(buckets)))
        for _, bucket in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True):
            selected.extend(sorted(bucket, key=lambda x: x.get("aeoSignal", 0), reverse=True)[:per_bucket])
    seen = {p.get("url") for p in selected}
    for p in sorted(pages, key=lambda x: x.get("aeoSignal", 0), reverse=True):
        if len(selected) >= max_pages:
            break
        if p.get("url") not in seen:
            selected.append(p)
            seen.add(p.get("url"))
    return selected[:max_pages]


def build_summary(start_url: str, metadata: List[Dict[str, Any]], failures: List[Dict[str, Any]], discovered_count: int, sitemaps_seen: int, full_complete: bool) -> Dict[str, Any]:
    type_counts = Counter([p.get("contentType", "other") for p in metadata])
    signal_counts: Counter[str] = Counter()
    for p in metadata:
        for s in p.get("signals", []):
            signal_counts[s] += 1
    scores = [float(p.get("aeoSignal", 0)) for p in metadata]
    top_pages = sorted(metadata, key=lambda x: x.get("aeoSignal", 0), reverse=True)[:TOP_GAP_PAGES]
    gap_pages = sorted(metadata, key=lambda x: (x.get("aeoSignal", 0), -(x.get("wordCount", 0) or 0)))[:TOP_GAP_PAGES]
    return {
        "siteUrl": start_url,
        "pagesFetched": len(metadata),
        "pagesFailed": len(failures),
        "totalDiscovered": discovered_count,
        "sitemapsSeen": sitemaps_seen,
        "fullContentComplete": full_complete,
        "analysisReady": len(metadata) > 0,
        "avgAeoSignal": round(sum(scores) / len(scores), 2) if scores else 0,
        "highAeo": len([s for s in scores if s >= 7]),
        "midAeo": len([s for s in scores if 4 <= s < 7]),
        "lowAeo": len([s for s in scores if s < 4]),
        "typeDistribution": dict(type_counts),
        "topSignals": dict(signal_counts.most_common(40)),
        "topPages": [compact_page(p) for p in top_pages],
        "gapPages": [compact_page(p) for p in gap_pages],
        "recommendedPagesForLLM": [compact_page(p, include_excerpt=True) for p in sample_representative_pages(metadata, RECOMMENDED_LLM_PAGES)],
        "crawlFinishedAt": now_iso() if full_complete else None,
    }


def build_lean_callback_summary(metadata: List[Dict[str, Any]], failures: List[Dict[str, Any]], discovered_count: int, sitemaps_seen: int, full_complete: bool, start_url: str) -> Dict[str, Any]:
    """Compact summary for callback payload — stays under ~50KB."""
    LEAN_LLM = 20
    LEAN_TOP = 15
    LEAN_GAP = 15
    EXCERPT_CHARS = 300
    scores = [float(p.get("aeoSignal", 0)) for p in metadata]
    type_counts = Counter([p.get("contentType", "other") for p in metadata])
    signal_counts: Counter[str] = Counter()
    for p in metadata:
        for s in p.get("signals", []):
            signal_counts[s] += 1

    def lean_page(p: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "url": p.get("url"),
            "title": (p.get("title") or "")[:60],
            "contentType": p.get("contentType", "other"),
            "aeoSignal": p.get("aeoSignal", 0),
            "wordCount": p.get("wordCount", 0),
            "signals": p.get("signals", [])[:5],
            "textExcerpt": (p.get("textExcerpt") or p.get("text") or "")[:EXCERPT_CHARS],
            "h1s": p.get("h1s", [])[:2],
        }

    sorted_by_signal = sorted(metadata, key=lambda x: x.get("aeoSignal", 0), reverse=True)
    sorted_by_gap = sorted(metadata, key=lambda x: (x.get("aeoSignal", 0), -(x.get("wordCount") or 0)))
    return {
        "siteUrl": start_url,
        "pagesFetched": len(metadata),
        "pagesFailed": len(failures),
        "totalDiscovered": discovered_count,
        "sitemapsSeen": sitemaps_seen,
        "fullContentComplete": full_complete,
        "avgAeoSignal": round(sum(scores) / len(scores), 2) if scores else 0,
        "highAeo": len([s for s in scores if s >= 7]),
        "midAeo": len([s for s in scores if 4 <= s < 7]),
        "lowAeo": len([s for s in scores if s < 4]),
        "typeDistribution": dict(type_counts),
        "topSignals": dict(signal_counts.most_common(20)),
        "recommendedPagesForLLM": [lean_page(p) for p in sample_representative_pages(sorted_by_signal[:200], LEAN_LLM)],
        "topPages": [lean_page(p) for p in sorted_by_signal[:LEAN_TOP]],
        "gapPages": [lean_page(p) for p in sorted_by_gap[:LEAN_GAP]],
        "partial": not full_complete,
    }



    if not callback_url:
        return False
    if ALLOW_ALL_CALLBACKS:
        return True
    try:
        p = urlparse(callback_url)
        host = p.netloc.lower().split(":")[0]
        return p.scheme == "https" and host in ALLOWED_CALLBACK_HOSTS
    except Exception:
        return False


async def send_callback(callback_url: Optional[str], payload: Dict[str, Any]) -> None:
    if not callback_url or not callback_allowed(callback_url):
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(callback_url, json=payload)
    except Exception:
        # Callback failure should not fail the crawl. n8n can poll status/result.
        return


async def update_job(job_id: str, updates: Dict[str, Any]) -> None:
    async with JOB_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(updates)
            JOBS[job_id]["updatedAt"] = now_iso()


async def crawl_job(job_id: str, payload: Dict[str, Any]) -> None:
    started = time.time()
    start_url = payload["startUrl"]
    parsed = urlparse(start_url)
    base_host = parsed.netloc.lower()
    limit = max(1, min(MAX_LIMIT, int(payload.get("limit", DEFAULT_LIMIT))))
    include_subdomains = bool(payload.get("includeSubdomains", True))
    fetch_content = bool(payload.get("fetchContent", True))
    respect_robots = bool(payload.get("respectRobots", DEFAULT_RESPECT_ROBOTS))
    callback_url = payload.get("callbackUrl")
    max_crawl_seconds = max(60, min(MAX_CRAWL_SECONDS, int(payload.get("maxCrawlSeconds", MAX_CRAWL_SECONDS))))

    jsonl_path = jsonl_path_for(job_id)
    manifest_path = manifest_path_for(job_id)
    jsonl_path.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)

    metadata: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()
    queued_urls: Set[str] = set()
    content_hashes: Set[str] = set()
    canonical_seen: Set[str] = set()
    discovered_count = 0
    sitemaps_seen = 0
    sitemap_errors: List[str] = []
    last_flush = 0.0

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS)
    limits = httpx.Limits(max_connections=max(CRAWL_CONCURRENCY * 2, 100), max_keepalive_connections=max(CRAWL_CONCURRENCY, 50))
    global_sem = asyncio.Semaphore(CRAWL_CONCURRENCY)
    host_sems: Dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(PER_HOST_CONCURRENCY))

    async def flush_status(stage: str, force: bool = False) -> None:
        nonlocal last_flush
        if not force and time.time() - last_flush < STATUS_FLUSH_SECONDS:
            return
        last_flush = time.time()
        summary = build_summary(start_url, metadata, failures, discovered_count, sitemaps_seen, False)
        manifest = {
            "jobId": job_id,
            "status": "running",
            "stage": stage,
            "startUrl": start_url,
            "jsonlPath": str(jsonl_path),
            "resultUrl": f"/crawl/result/{job_id}",
            "downloadUrl": f"/crawl/download/{job_id}",
            "summary": summary,
            "failuresSample": failures[:100],
            "updatedAt": now_iso(),
        }
        write_json(manifest_path, manifest)
        await update_job(job_id, {
            "status": "running",
            "stage": stage,
            "pagesFetched": len(metadata),
            "pagesFailed": len(failures),
            "totalDiscovered": discovered_count,
            "sitemapsSeen": sitemaps_seen,
            "elapsedSeconds": round(time.time() - started, 1),
            "analysisReady": len(metadata) > 0,
            "jsonlPath": str(jsonl_path),
            "manifestPath": str(manifest_path),
            "summary": summary,
        })

    try:
        await update_job(job_id, {
            "status": "running",
            "stage": "initializing",
            "jsonlPath": str(jsonl_path),
            "manifestPath": str(manifest_path),
        })

        async with httpx.AsyncClient(headers=headers, timeout=timeout, http2=HTTP2_ENABLED, limits=limits) as client:
            robots_parser, robots_sitemaps = await build_robots(client, start_url, respect_robots)
            sitemap_discovery = await discover_sitemap_urls(client, start_url, limit, include_subdomains, base_host, robots_sitemaps)
            sitemap_urls = sitemap_discovery["urls"]
            sitemaps_seen = sitemap_discovery["sitemapsSeen"]
            sitemap_errors = sitemap_discovery["sitemapErrors"]

            queue: asyncio.Queue[str] = asyncio.Queue()

            def enqueue(url: str) -> None:
                nonlocal discovered_count
                if len(queued_urls) >= limit:
                    return
                normalized = normalize_url(url)
                if not normalized:
                    return
                if normalized in queued_urls or normalized in seen_urls:
                    return
                if not same_site(normalized, base_host, include_subdomains):
                    return
                if is_asset_or_noise(normalized):
                    return
                queued_urls.add(normalized)
                discovered_count = max(discovered_count, len(queued_urls))
                queue.put_nowait(normalized)

            enqueue(start_url)
            for u in sitemap_urls:
                enqueue(u)

            await flush_status("crawling", force=True)

            async def process_url(url: str) -> None:
                async with global_sem:
                    host = urlparse(url).netloc.lower()
                    async with host_sems[host]:
                        if respect_robots and not robots_allowed(robots_parser, url):
                            failures.append({"url": url, "error": "blocked_by_robots", "statusCode": None})
                            return
                        if not fetch_content:
                            page = page_stub_from_url(url)
                            metadata.append(compact_page(page))
                            append_jsonl(jsonl_path, page)
                            return

                        res = await fetch_limited(client, url, MAX_BYTES_PER_PAGE)
                        if not res.ok or not res.body:
                            error = res.error or f"http_status_{res.status_code}"
                            page = page_stub_from_url(url, res.status_code, error)
                            failures.append({"url": url, "error": error, "statusCode": res.status_code})
                            append_jsonl(jsonl_path, page)
                            return

                        content_type = (res.content_type or "").lower()
                        if "text/html" not in content_type and "application/xhtml" not in content_type and content_type:
                            page = page_stub_from_url(url, res.status_code, f"non_html_content_type:{content_type[:80]}")
                            failures.append({"url": url, "error": page["error"], "statusCode": res.status_code})
                            append_jsonl(jsonl_path, page)
                            return

                        html = decode_body(res.body, None)
                        links = extract_links(html, res.final_url or url, base_host, include_subdomains)
                        page = extract_page(html, url, res.final_url or url, res.status_code, res.ms, links)

                        # URL-level dedupe happens before fetch. Content-level dedupe avoids duplicate templates.
                        canonical = page.get("canonicalUrl") or page.get("finalUrl") or page.get("url")
                        content_hash = page.get("contentHash")
                        duplicate = False
                        if canonical and canonical in canonical_seen:
                            duplicate = True
                            page["signals"] = list(dict.fromkeys(page.get("signals", []) + ["duplicate_canonical"]))
                        if content_hash and content_hash in content_hashes and page.get("wordCount", 0) > 50:
                            duplicate = True
                            page["signals"] = list(dict.fromkeys(page.get("signals", []) + ["duplicate_content"]))
                        if canonical:
                            canonical_seen.add(canonical)
                        if content_hash:
                            content_hashes.add(content_hash)

                        append_jsonl(jsonl_path, page)
                        if not duplicate:
                            metadata.append(compact_page(page, include_excerpt=True))

                        for link in links:
                            if len(queued_urls) >= limit:
                                break
                            enqueue(link)

            workers = max(1, CRAWL_CONCURRENCY)

            async def worker() -> None:
                while True:
                    if time.time() - started > max_crawl_seconds:
                        return
                    if len(seen_urls) >= limit:
                        return
                    try:
                        url = await asyncio.wait_for(queue.get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        return
                    if url in seen_urls:
                        queue.task_done()
                        continue
                    seen_urls.add(url)
                    try:
                        await process_url(url)
                    finally:
                        queue.task_done()
                        await flush_status("crawling")

            await asyncio.gather(*[worker() for _ in range(workers)])
            await queue.join()

        final_summary = build_summary(start_url, metadata, failures, discovered_count, sitemaps_seen, True)
        completed_payload = {
            "jobId": job_id,
            "status": "complete",
            "stage": "complete",
            "startUrl": start_url,
            "jsonlPath": str(jsonl_path),
            "manifestPath": str(manifest_path),
            "resultUrl": f"/crawl/result/{job_id}",
            "downloadUrl": f"/crawl/download/{job_id}",
            "pagesFetched": len(metadata),
            "pagesFailed": len(failures),
            "totalDiscovered": discovered_count,
            "sitemapsSeen": sitemaps_seen,
            "sitemapErrors": sitemap_errors,
            "elapsedSeconds": round(time.time() - started, 1),
            "summary": final_summary,
            "failuresSample": failures[:250],
            "completedAt": now_iso(),
        }
        write_json(manifest_path, completed_payload)
        await update_job(job_id, completed_payload)

        await send_callback(callback_url, {
            "jobId": job_id,
            "status": "complete",
            "startUrl": start_url,
            "resultUrl": f"/crawl/result/{job_id}",
            "downloadUrl": f"/crawl/download/{job_id}",
            "jsonlPath": str(jsonl_path),
            "manifestPath": str(manifest_path),
            "pagesFetched": len(metadata),
            "pagesFailed": len(failures),
            "totalDiscovered": discovered_count,
            "elapsedSeconds": round(time.time() - started, 1),
            "summary": build_lean_callback_summary(metadata, failures, discovered_count, sitemaps_seen, True, start_url),
        })

    except Exception as e:
        error_payload = {
            "jobId": job_id,
            "status": "failed",
            "stage": "failed",
            "error": str(e)[:1200],
            "jsonlPath": str(jsonl_path),
            "manifestPath": str(manifest_path),
            "resultUrl": f"/crawl/result/{job_id}",
            "downloadUrl": f"/crawl/download/{job_id}",
            "pagesFetched": len(metadata),
            "pagesFailed": len(failures),
            "elapsedSeconds": round(time.time() - started, 1),
            "updatedAt": now_iso(),
        }
        write_json(manifest_path, error_payload)
        await update_job(job_id, error_payload)
        await send_callback(callback_url, error_payload)


async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - JOB_TTL_SECONDS
        async with JOB_LOCK:
            for jid, job in list(JOBS.items()):
                try:
                    if parse_iso(job.get("createdAt", now_iso())) < cutoff:
                        JOBS.pop(jid, None)
                except Exception:
                    JOBS.pop(jid, None)


async def startup() -> None:
    asyncio.create_task(cleanup_loop())


async def root(request: Request) -> JSONResponse:
    return JSONResponse({
        "service": APP_NAME,
        "version": VERSION,
        "ok": True,
        "role": "enterprise_corpus_builder",
    })


async def health(request: Request) -> JSONResponse:
    return JSONResponse({
        "ok": True,
        "service": APP_NAME,
        "version": VERSION,
        "jobs": len(JOBS),
        "settings": {
            "CRAWL_CONCURRENCY": CRAWL_CONCURRENCY,
            "PER_HOST_CONCURRENCY": PER_HOST_CONCURRENCY,
            "SITEMAP_CONCURRENCY": SITEMAP_CONCURRENCY,
            "REQUEST_TIMEOUT_SECONDS": REQUEST_TIMEOUT_SECONDS,
            "RETRY_COUNT": RETRY_COUNT,
            "MAX_BYTES_PER_PAGE": MAX_BYTES_PER_PAGE,
            "DEFAULT_LIMIT": DEFAULT_LIMIT,
            "MAX_LIMIT": MAX_LIMIT,
            "HTTP2_ENABLED": HTTP2_ENABLED,
            "RESPECT_ROBOTS_DEFAULT": DEFAULT_RESPECT_ROBOTS,
            "CALLBACK_RESTRICTED": not ALLOW_ALL_CALLBACKS,
            "ALLOWED_CALLBACK_HOSTS": sorted(ALLOWED_CALLBACK_HOSTS),
        },
    })


async def crawl_start(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    try:
        start_url = normalize_start_url(payload.get("startUrl") or payload.get("url") or "")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    callback_url = payload.get("callbackUrl")
    if callback_url and not callback_allowed(callback_url):
        return JSONResponse({
            "error": "callbackUrl is not allowed. Set ALLOWED_CALLBACK_HOSTS or ALLOW_ALL_CALLBACKS=1 for trusted private deployments.",
            "callbackUrl": callback_url,
        }, status_code=400)

    job_id = payload.get("jobId") or f"crawl_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    payload["startUrl"] = start_url
    payload["limit"] = max(1, min(MAX_LIMIT, int(payload.get("limit", DEFAULT_LIMIT))))

    async with JOB_LOCK:
        if job_id in JOBS and JOBS[job_id].get("status") in {"queued", "running"}:
            return JSONResponse({"error": "jobId already exists and is active", "jobId": job_id}, status_code=409)
        JOBS[job_id] = {
            "jobId": job_id,
            "status": "queued",
            "stage": "queued",
            "startUrl": start_url,
            "limit": payload["limit"],
            "createdAt": now_iso(),
            "updatedAt": now_iso(),
            "pagesFetched": 0,
            "pagesFailed": 0,
            "totalDiscovered": 0,
            "elapsedSeconds": 0,
            "analysisReady": False,
            "jsonlPath": str(jsonl_path_for(job_id)),
            "manifestPath": str(manifest_path_for(job_id)),
            "resultUrl": f"/crawl/result/{job_id}",
            "downloadUrl": f"/crawl/download/{job_id}",
        }

    return JSONResponse({
        "accepted": True,
        "jobId": job_id,
        "status": "queued",
        "startUrl": start_url,
        "resultUrl": f"/crawl/result/{job_id}",
        "downloadUrl": f"/crawl/download/{job_id}",
        "jsonlPath": str(jsonl_path_for(job_id)),
    }, background=BackgroundTask(crawl_job, job_id, payload))


async def crawl_status(request: Request) -> JSONResponse:
    job = JOBS.get(request.path_params["job_id"])
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    public = {k: v for k, v in job.items() if k not in {"summary"}}
    return JSONResponse(public)


async def crawl_result(request: Request) -> JSONResponse:
    job_id = request.path_params["job_id"]
    job = JOBS.get(job_id)
    manifest_path = manifest_path_for(job_id)
    if not job and not manifest_path.exists():
        return JSONResponse({"error": "job not found"}, status_code=404)

    manifest: Dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}

    base = manifest or job or {}
    include_pages = request.query_params.get("includePages", "false").lower() in {"1", "true", "yes"}
    page_limit = env_int("RESULT_PAGE_LIMIT", int(request.query_params.get("pageLimit", "1000")), 1, 25_000)
    if include_pages:
        rows = read_jsonl_limited(jsonl_path_for(job_id), page_limit)
        base = dict(base)
        base["pages"] = rows
        base["pagesIncluded"] = len(rows)
        base["pageLimit"] = page_limit
    return JSONResponse(base)


async def crawl_download(request: Request):
    job_id = request.path_params["job_id"]
    path = jsonl_path_for(job_id)
    if not path.exists():
        return JSONResponse({"error": "jsonl not found"}, status_code=404)
    return FileResponse(path, media_type="application/x-ndjson", filename=path.name)


async def crawl_pages(request: Request) -> JSONResponse:
    """Paginated JSONL reader for WF4 batch scoring. Works during and after crawl."""
    job_id = request.path_params["job_id"]
    offset = int(request.query_params.get("offset", 0))
    limit = min(int(request.query_params.get("limit", 200)), 500)
    path = jsonl_path_for(job_id)
    if not path.exists():
        # Fall back to metadata in memory via job summary
        job = JOBS.get(job_id)
        if not job:
            return JSONResponse({"error": "job not found"}, status_code=404)
        summary = job.get("summary") or {}
        pages = summary.get("recommendedPagesForLLM", [])
        return JSONResponse({"pages": pages[offset:offset + limit], "total": len(pages), "offset": offset, "limit": limit, "source": "summary"})
    try:
        all_lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        total = len(all_lines)
        chunk = []
        for line in all_lines[offset:offset + limit]:
            try:
                chunk.append(json.loads(line))
            except Exception:
                continue
        return JSONResponse({"pages": chunk, "total": total, "offset": offset, "limit": limit, "source": "jsonl"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def crawl_list(request: Request) -> JSONResponse:
    async with JOB_LOCK:
        jobs = [{k: v for k, v in j.items() if k != "summary"} for j in JOBS.values()]
    jobs.sort(key=lambda j: j.get("createdAt", ""), reverse=True)
    return JSONResponse({"jobs": jobs[:100]})


async def robots_note(request: Request) -> PlainTextResponse:
    return PlainTextResponse("This service can respect robots.txt when respectRobots=true. Use only on sites you are authorized to crawl.\n")


routes = [
    Route("/", root),
    Route("/health", health),
    Route("/robots-note", robots_note),
    Route("/crawl/start", crawl_start, methods=["POST"]),
    Route("/crawl/status/{job_id}", crawl_status),
    Route("/crawl/result/{job_id}", crawl_result),
    Route("/crawl/pages/{job_id}", crawl_pages),
    Route("/crawl/download/{job_id}", crawl_download),
    Route("/crawl/list", crawl_list),
]

app = Starlette(debug=False, routes=routes, on_startup=[startup])
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin for origin in os.getenv("CORS_ALLOW_ORIGINS", "*").split(",") if origin],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
