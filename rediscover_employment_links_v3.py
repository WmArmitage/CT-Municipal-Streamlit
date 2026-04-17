#!/usr/bin/env python3
"""
rediscover_employment_links_v3.py

Platform-aware rediscovery for municipal employment links.

Key improvements:
- CivicPlus: tries Jobs.aspx (case-insensitive), QuickLinks, Search, common slugs; parses footer/nav.
- CivicLift: supports stable landing pages OR marks as "ephemeral_posts" (jobs appear as articles).
- Granicus: follows redirects; prefers ATS vendor (GovernmentJobs/NEOGOV).
- Other CMS: falls back to homepage crawl + keyword scoring, treating "Human Resources" as a strong signal.
- Host normalization (www/non-www) so same-site checks don't drop valid candidates.
- Hard-block social links as final employment targets.
- Town-only mode: --town "Essex" to test quickly.

Usage:
  python rediscover_employment_links_v3.py input.json output.json report.csv
  python rediscover_employment_links_v3.py input.json output.json report.csv --town Essex
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, unquote

import requests
from bs4 import BeautifulSoup

from utils.url_utils import detect_soft404, is_html_content_type, is_url, normalize_homepage


# -------------------- Config --------------------
TIMEOUT_SECS = 25
VERIFY_TLS = True
SLEEP_BETWEEN_REQUESTS_SECS = 0.25

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Only attempt rediscovery if current employment URL is "broken-ish"
REDISCOVER_IF_STATUS_IN = {404, 410, None, -1}
REDISCOVER_IF_SOFT404_TRUE = True
DO_NOT_REWRITE_IF_STATUS_IN = {401}  # keep 403 as uncertain

# CivicPlus endpoints / patterns
CIVICPLUS_COMMON_PATHS = [
    "/Jobs.aspx", "/jobs.aspx",
    "/Employment", "/employment",
    "/Employment-Opportunities", "/employment-opportunities",
    "/Careers", "/careers",
    "/Human-Resources", "/human-resources",
    "/QuickLinks.aspx", "/quicklinks.aspx",
    "/211/Departments",  # common CivicPlus departments listing page
]

# Discovery keywords
KW_EMPLOYMENT = [
    "employment",
    "employment opportunities",
    "jobs",
    "job",
    "job openings",
    "job opportunities",
    "careers",
    "career opportunities",
    "human resources",
    "civil service",
    "hr",
    "vacancies",
    "openings",
]
KW_STRONG_LABELS = [
    "employment opportunities",
    "job openings",
    "career opportunities",
    "human resources",
    "civil service",
]
GENERIC_NAV_TERMS = [
    "quicklinks",
    "formcenter",
    "documentcenter",
    "home",
    "index",
]
GENERIC_DEPARTMENT_TERMS = [
    "departments",
    "department",
]

# ATS/Vendor hints (allow off-site canonical if it matches)
ATS_HINTS = [
    "governmentjobs.com",  # NEOGOV / GovernmentJobs
    "neogov.com",
    "appone.com",
    "paycomonline.net",
    "jobapscloud.com",
    "frontlineeducation.com",
    "applitrack.com",
]

# Hard-block social/irrelevant
SOCIAL_BLOCK = [
    "facebook.com", "twitter.com", "x.com", "instagram.com", "youtube.com", "linkedin.com"
]

# Soft-404 and interstitial detection
BLOCKED_PATTERNS = [
    "checking your browser",
    "ddos protection",
    "attention required",
    "cloudflare",
    "please enable javascript",
    "enable javascript",
    "enable cookies",
    "access denied",
    "temporarily unavailable",
    "verify you are human",
]

# CivicPlus page-id path pattern e.g. /354/Employment-Opportunities
CIVICPLUS_PAGEID_RE = re.compile(r"^/\d{2,6}/", re.IGNORECASE)

# Application PDF hints
APPLICATION_HINTS = [
    "application for employment",
    "employment application",
    "job application",
    "civil service application",
    "employment-app",
    "employment_app",
]
APPLICATION_NEGATIVE = [
    "building permit",
    "building_permit",
    "permit",
    "zoning",
    "wetlands",
    "dog license",
    "marriage",
    "birth certificate",
    "death certificate",
    "parking permit",
    "septic",
    "blight",
]
DOC_EXTENSIONS = (".pdf", ".doc", ".docx")
SITEMAP_TIMEOUT_SECS = 5
SITEMAP_MAX_URLS_PER_TOWN = 200
SITEMAP_INCLUDE_TERMS = ["jobs", "employment", "career", "human-resources", "civil-service"]
SITEMAP_HARD_REJECT_TERMS = ["quicklinks", "formcenter"]
SITEMAP_GENERIC_LAST_SEGMENTS = {"", "home", "index", "index.aspx", "default.aspx"}


# -------------------- Helpers --------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def is_pdf(url: str) -> bool:
    return (url or "").lower().split("?")[0].endswith(".pdf")


def is_document_file(url: str) -> bool:
    low = (url or "").lower().split("?", 1)[0]
    return any(low.endswith(ext) for ext in DOC_EXTENSIONS)


def blocked_reason(html: str) -> Optional[str]:
    h = (html or "").lower()
    for p in BLOCKED_PATTERNS:
        if p in h:
            return p
    return None


def looks_soft404(resp: requests.Response) -> bool:
    if resp is None or resp.status_code != 200:
        return False
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if not is_html_content_type(ctype):
        return False
    text = resp.text[:250_000] if resp.text else ""
    return detect_soft404(text)


def host_norm(url: str) -> str:
    try:
        h = urlparse(url).netloc.lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""


def same_site(a: str, b: str) -> bool:
    ha = host_norm(a)
    hb = host_norm(b)
    return ha != "" and ha == hb


def is_social(url: str) -> bool:
    lu = (url or "").lower()
    return any(d in lu for d in SOCIAL_BLOCK)


def is_ats(url: str) -> bool:
    lu = (url or "").lower()
    return any(h in lu for h in ATS_HINTS)


def kw_hit(s: str) -> bool:
    s = (s or "").lower()
    return any(k in s for k in KW_EMPLOYMENT)


def text_blob(*parts: str) -> str:
    return " ".join((p or "").strip().lower() for p in parts if isinstance(p, str))


def has_strong_employment_signal(url: str, label: str = "") -> bool:
    return kw_hit(text_blob(url, label))


def is_generic_navigation_candidate(url: str, label: str = "") -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").strip("/").lower()
    segments = [seg for seg in path.split("/") if seg]
    last = segments[-1] if segments else ""
    blob = text_blob(url, label)

    generic = any(term in blob for term in GENERIC_NAV_TERMS)
    if last in {"", "home", "index", "index.aspx", "default.aspx"}:
        generic = True
    if any(term in blob for term in GENERIC_DEPARTMENT_TERMS):
        generic = True

    if not generic:
        return False
    return not has_strong_employment_signal(url, label)


def has_application_signal(url: str, label: str = "") -> bool:
    blob = text_blob(url, label)
    if any(term in blob for term in APPLICATION_HINTS):
        return True
    return "application" in blob and any(term in blob for term in ["employment", "job", "civil service"])


def has_application_negative_signal(url: str, label: str = "") -> bool:
    blob = text_blob(url, label)
    if "employment application" in blob or "application for employment" in blob:
        return False
    return any(term in blob for term in APPLICATION_NEGATIVE)


def get(url: str) -> Tuple[Optional[requests.Response], Optional[str]]:
    try:
        r = requests.get(
            url,
            timeout=TIMEOUT_SECS,
            allow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            verify=VERIFY_TLS,
        )
        return r, None
    except requests.RequestException as e:
        return None, str(e)


def _get_sitemap_response(url: str) -> Optional[requests.Response]:
    try:
        return requests.get(
            url,
            timeout=SITEMAP_TIMEOUT_SECS,
            allow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/xml,text/xml,text/plain;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            verify=VERIFY_TLS,
        )
    except requests.RequestException:
        return None


def _xml_tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1].lower()
    return (tag or "").lower()


def _extract_sitemap_locs(xml_text: str) -> Tuple[str, List[str]]:
    root = ET.fromstring(xml_text)
    root_name = _xml_tag_name(root.tag)
    locs: List[str] = []
    for elem in root.iter():
        if _xml_tag_name(elem.tag) != "loc":
            continue
        val = (elem.text or "").strip()
        if val:
            locs.append(val)
    if root_name == "sitemapindex":
        return "index", locs
    if root_name == "urlset":
        return "urlset", locs
    return "unknown", locs


def _get_robot_sitemaps(base_url: str) -> List[str]:
    robots_url = urljoin(base_url, "/robots.txt")
    resp = _get_sitemap_response(robots_url)
    if not resp or resp.status_code >= 400:
        return []
    out: List[str] = []
    seen = set()
    for line in (resp.text or "").splitlines():
        stripped = line.strip()
        if not stripped.lower().startswith("sitemap:"):
            continue
        raw = stripped.split(":", 1)[1].strip()
        if not raw:
            continue
        sm_url = urljoin(resp.url or base_url, raw)
        if not is_url(sm_url) or sm_url in seen:
            continue
        seen.add(sm_url)
        out.append(sm_url)
    return out


def get_sitemap_urls(base_url: str) -> List[str]:
    if not is_url(base_url):
        return []

    try:
        seeds = [urljoin(base_url, "/sitemap.xml"), urljoin(base_url, "/sitemap_index.xml")]
        seeds.extend(_get_robot_sitemaps(base_url))

        sitemap_queue: deque[str] = deque()
        queued = set()
        for s in seeds:
            if not is_url(s):
                continue
            s = s.strip()
            if s in queued:
                continue
            queued.add(s)
            sitemap_queue.append(s)

        visited_sitemaps = set()
        discovered: List[str] = []
        seen_urls = set()

        while sitemap_queue and len(discovered) < SITEMAP_MAX_URLS_PER_TOWN:
            sitemap_url = sitemap_queue.popleft()
            if sitemap_url in visited_sitemaps:
                continue
            visited_sitemaps.add(sitemap_url)

            resp = _get_sitemap_response(sitemap_url)
            if not resp or resp.status_code >= 400 or not (resp.text or "").strip():
                continue

            try:
                kind, locs = _extract_sitemap_locs(resp.text or "")
            except Exception:
                continue

            for loc in locs:
                candidate = urljoin(resp.url or base_url, loc)
                if not is_url(candidate):
                    continue
                candidate = candidate.strip()

                is_child_sitemap = kind == "index" or candidate.lower().split("?", 1)[0].endswith(".xml")
                if is_child_sitemap:
                    if candidate not in visited_sitemaps and candidate not in queued:
                        queued.add(candidate)
                        sitemap_queue.append(candidate)
                    continue

                if candidate in seen_urls:
                    continue
                seen_urls.add(candidate)
                discovered.append(candidate)
                if len(discovered) >= SITEMAP_MAX_URLS_PER_TOWN:
                    break

        return discovered[:SITEMAP_MAX_URLS_PER_TOWN]
    except Exception:
        return []


def _is_generic_home_or_index(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").strip("/").lower()
    if not path:
        return True
    last = path.split("/")[-1]
    return last in SITEMAP_GENERIC_LAST_SEGMENTS


def filter_sitemap_candidates(sitemap_urls: List[str]) -> List[Tuple[str, str, str]]:
    accepted: List[Tuple[str, str, str]] = []
    seen = set()

    for raw in sitemap_urls[:SITEMAP_MAX_URLS_PER_TOWN]:
        if not is_url(raw):
            continue
        u = raw.strip()
        if not u or u in seen:
            continue
        seen.add(u)

        low = u.lower()
        blob = text_blob(u)

        if any(term in low for term in SITEMAP_HARD_REJECT_TERMS):
            continue
        if _is_generic_home_or_index(u):
            continue

        if "documentcenter" in low:
            if not is_document_file(u):
                continue
            if "employment" not in blob and "application" not in blob:
                continue

        if is_document_file(u):
            if has_application_negative_signal(u):
                continue
            if "application" in blob or "employment" in blob:
                accepted.append((u, "SITEMAP_APPLICATION", "sitemap"))
                continue

        if not any(term in low for term in SITEMAP_INCLUDE_TERMS):
            continue
        accepted.append((u, "SITEMAP_URL", "sitemap"))

    return accepted


def extract_links(base_url: str, html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    out: List[Tuple[str, str]] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)

        # gather visible text + aria/title for icon links/footers
        txt = " ".join(a.get_text(" ", strip=True).split())
        aria = (a.get("aria-label") or "").strip()
        title = (a.get("title") or "").strip()
        combined = " ".join([x for x in [txt, aria, title] if x]).strip()

        out.append((abs_url, combined))

    return out


def extract_links_with_selector(base_url: str, html: str, selector: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    out: List[Tuple[str, str]] = []

    for a in soup.select(selector):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)

        txt = " ".join(a.get_text(" ", strip=True).split())
        aria = (a.get("aria-label") or "").strip()
        title = (a.get("title") or "").strip()
        combined = " ".join([x for x in [txt, aria, title] if x]).strip()

        out.append((abs_url, combined))

    return out


def civicplus_search_urls(base_home: str) -> List[str]:
    # CivicPlus internal search endpoint
    # /Search?searchPhrase=employment
    phrases = ["employment", "jobs", "Employment Opportunities", "human resources"]
    urls = []
    for p in phrases:
        urls.append(urljoin(base_home, "Search?" + urlencode({"searchPhrase": p})))
    return urls


def detect_platform(rec: Dict[str, Any]) -> str:
    """
    Best-effort platform detection using:
    - explicit field "ATS or Platform (if known)"
    - URL hints
    - page HTML hints (optional)
    """
    known = (rec.get("ATS or Platform (if known)") or "").strip().lower()
    if "civicplus" in known:
        return "civicplus"
    if "civiclift" in known:
        return "civiclift"
    if "granicus" in known:
        return "granicus"

    # URL heuristics
    home = rec.get("Town Website") or ""
    emp = rec.get("Employment Page URL") or ""
    blob = f"{home} {emp}".lower()
    if "civicplus.com" in blob or "jobs.aspx" in blob or "quicklinks.aspx" in blob:
        return "civicplus"
    if "civiclift" in blob:
        return "civiclift"
    if "granicus" in blob:
        return "granicus"

    # default unknown/other
    return "other"


def score_candidate(url: str, label: str, base_home: str, source: str) -> int:
    """
    Higher is better.
    Prefers same-site employment pages, allows ATS vendors, avoids social.
    """
    if is_social(url):
        return -10_000

    u = (url or "").lower()
    t = (label or "").lower()
    strong_signal = has_strong_employment_signal(url, label)
    generic_nav = is_generic_navigation_candidate(url, label)
    s = 0

    if same_site(url, base_home):
        s += 35
    if is_ats(url):
        s += 45

    if source in {"civicplus_path", "civiclift_path", "granicus_path"}:
        s += 12
    if source in {"quicklinks", "nav_footer_link"}:
        s -= 10

    if strong_signal:
        s += 60
    else:
        s -= 120
    if any(k in t for k in KW_STRONG_LABELS):
        s += 12
    if generic_nav:
        s -= 140

    # CivicPlus page-id is a strong signal for real content pages
    try:
        path = urlparse(url).path or ""
        if CIVICPLUS_PAGEID_RE.match(path) and strong_signal:
            s += 20
    except Exception:
        pass

    # prefer HTML pages over PDFs
    if is_document_file(url):
        s -= 80

    # prefer "employment/jobs/careers" in path
    if any(x in u for x in ["employment", "jobs", "careers", "human-resources", "civil-service"]):
        s += 10
    if "jobs.aspx" in u:
        s += 18
    if "application" in u or "application" in t:
        s -= 35

    if any(bad in u for bad in GENERIC_DEPARTMENT_TERMS) and not strong_signal:
        s -= 40

    return s


def validate_candidate(url: str, base_home: str) -> Tuple[bool, Optional[str], str, Optional[str]]:
    """
    Returns (ok, final_url, reason, blocked_reason)
    Acceptable:
    - same-site HTML page
    - ATS vendor page
    - PDF only if it is same-site AND strongly labeled (handled by scoring)
    """
    resp, err = get(url)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if resp is None:
        return False, None, f"fetch_error: {err}", None

    final = resp.url or url

    # Preserve ATS careers path if the request normalized to the root
    if is_ats(url):
        parsed_orig = urlparse(url)
        parsed_final = urlparse(final)

    # If final URL collapsed to site root but original had a careers path, keep original
        if (parsed_orig.path or "").startswith("/careers/") and (parsed_final.path or "") in {"", "/"}:
            final = url


    # If Granicus "splash" wrapper points to an ATS vendor, accept the ATS URL directly
    splash_target = unwrap_granicus_splash(final)
    if splash_target and is_ats(splash_target):
        return True, splash_target, "ok_granicus_splash_to_ats", None

    if resp.status_code == 403:
        return False, final, "status_403_uncertain", None
    if resp.status_code >= 400:
        return False, final, f"status_{resp.status_code}", None

    # if HTML looks blocked/interstitial, mark explicitly so we understand failures
    ctype = (resp.headers.get("Content-Type") or "").lower()
    html = resp.text if resp.text else ""
    if ("text/html" in ctype or ctype == ""):
        block = blocked_reason(html)
        
# Many legit ATS pages include "enable javascript" in <noscript>.
# Don't treat that as a hard block for ATS targets.
        if block in {"enable javascript", "please enable javascript"} and is_ats(final):
            block = None

        if block:
            return False, final, "blocked_or_interstitial", block

    if looks_soft404(resp):
        return False, final, "soft404", None

    # Reject social
    if is_social(final):
        return False, final, "social_blocked", None

    # Require same-site unless ATS vendor
    if not same_site(final, base_home) and not is_ats(final):
        return False, final, "offsite_not_ats", None

    return True, final, "ok", None


def score_application_candidate(url: str, label: str, base_home: str, source: str) -> int:
    if is_social(url):
        return -10_000
    if has_application_negative_signal(url, label):
        return -300

    score = 0
    if same_site(url, base_home):
        score += 30
    if is_document_file(url):
        score += 45
    if has_application_signal(url, label):
        score += 55
    elif "application" in text_blob(url, label):
        score += 15
    else:
        score -= 60

    if has_strong_employment_signal(url, label):
        score += 20
    if is_ats(url) and has_strong_employment_signal(url, label):
        score += 12
    if source.startswith("employment"):
        score += 12
    if "quicklinks" in source:
        score -= 6

    return score


def validate_application_candidate(url: str, base_home: str) -> Tuple[bool, Optional[str], str]:
    resp, err = get(url)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if resp is None:
        return False, None, f"fetch_error:{err}"

    final = resp.url or url
    if resp.status_code == 403:
        return False, final, "status_403_uncertain"
    if resp.status_code >= 400:
        return False, final, f"status_{resp.status_code}"
    if looks_soft404(resp):
        return False, final, "soft404"
    if is_social(final):
        return False, final, "social_blocked"
    if has_application_negative_signal(final):
        return False, final, "unrelated_form"
    if not same_site(final, base_home) and not is_ats(final):
        return False, final, "offsite_not_ats"

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if is_document_file(final):
        return True, final, "ok_document"
    if "application/pdf" in ctype:
        return True, final, "ok_pdf_content_type"
    if is_ats(final) and has_strong_employment_signal(final):
        return True, final, "ok_ats_application_entry"
    return False, final, "not_application_document"


def gather_application_candidates(
    base_home: str,
    employment_url: Optional[str],
    employment_candidates: List[Tuple[str, str, str]],
) -> List[Tuple[str, str, str]]:
    candidates: List[Tuple[str, str, str]] = []
    seed_pages: List[Tuple[str, str]] = []
    if is_url(employment_url):
        seed_pages.append((employment_url or "", "employment_page"))
    seed_pages.append((base_home, "town_home"))
    seed_pages.append((urljoin(base_home, "QuickLinks.aspx"), "quicklinks"))

    for page_url, source in seed_pages:
        resp, _ = get(page_url)
        time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
        if not resp or resp.status_code >= 400 or looks_soft404(resp):
            continue
        final_page = resp.url or page_url
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if is_document_file(final_page):
            candidates.append((final_page, f"{source}:direct_document", f"{source}_direct"))
            continue
        if "text/html" not in ctype and ctype != "":
            continue
        for u, t in extract_links(final_page, resp.text or ""):
            blob = text_blob(u, t)
            if is_document_file(u) or "application" in blob or has_strong_employment_signal(u, t):
                candidates.append((u, t, source))

    for u, t, src in employment_candidates[:20]:
        blob = text_blob(u, t)
        if is_document_file(u) or "application" in blob or has_application_signal(u, t):
            candidates.append((u, t, f"employment_candidate:{src}"))

    for u, t, src in employment_candidates:
        if src != "sitemap":
            continue
        blob = text_blob(u, t)
        if not is_document_file(u):
            continue
        if has_application_negative_signal(u, t):
            continue
        if "application" in blob or "employment" in blob:
            candidates.append((u, t, "employment_candidate:sitemap"))

    deduped: List[Tuple[str, str, str]] = []
    seen = set()
    for u, t, src in candidates:
        if not is_url(u):
            continue
        key = (u.strip(), (t or "").strip().lower(), src)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((u.strip(), t, src))
    return deduped


def find_application_document(
    base_home: str,
    employment_url: Optional[str],
    employment_candidates: List[Tuple[str, str, str]],
) -> Tuple[Optional[str], str, int, str]:
    candidates = gather_application_candidates(base_home, employment_url, employment_candidates)
    if not candidates:
        return None, "no_application_candidates", 0, ""

    scored = [
        (score_application_candidate(u, t, base_home, src), u, t, src)
        for u, t, src in candidates
    ]
    scored.sort(reverse=True, key=lambda x: x[0])

    for score, u, t, src in scored[:60]:
        if score < 35:
            continue
        if has_application_negative_signal(u, t):
            continue
        ok, final, reason = validate_application_candidate(u, base_home)
        if not ok or not final:
            continue
        if not has_application_signal(final, t) and not (is_ats(final) and has_strong_employment_signal(final, t)):
            continue
        return final, reason, score, src

    return None, "no_valid_application_candidate", 0, ""

# NeoGov specific
def unwrap_granicus_splash(url: str) -> Optional[str]:
    try:
        u = urlparse(url)
        qs = parse_qs(u.query or "")
        splash = qs.get("splash", [None])[0]
        if not splash:
            return None
        splash = unquote(splash)
        if is_url(splash):
            return splash
        return None
    except Exception:
        return None

def granicus_ats_fallback_candidates(town: str) -> List[Tuple[str, str, str]]:
    """
    If a Granicus site blocks requests (403), try common GovernmentJobs (NEOGOV) patterns.
    """
    slug = re.sub(r"[^a-z0-9]+", "", (town or "").lower())
    return [
        (f"https://www.governmentjobs.com/careers/{slug}ct", "ATS_FALLBACK:governmentjobs_slug_ct", "ats_fallback"),
        (f"https://www.governmentjobs.com/careers/{slug}", "ATS_FALLBACK:governmentjobs_slug", "ats_fallback"),
    ]



# -------------------- Platform-specific discovery --------------------
def discover_civicplus(base_home: str) -> List[Tuple[str, str, str]]:
    """
    Returns candidate URLs as (url, label, source)
    """
    cand: List[Tuple[str, str, str]] = []

    # 1) direct endpoints
    for p in CIVICPLUS_COMMON_PATHS:
        cand.append((urljoin(base_home, p.lstrip("/")), f"CIVICPLUS_PATH:{p}", "civicplus_path"))

    # 2) homepage crawl (includes footer)
    h_resp, _ = get(base_home)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if h_resp and h_resp.status_code < 400 and not looks_soft404(h_resp):
        page_url = h_resp.url or base_home
        html = h_resp.text or ""
        for u, t in extract_links(page_url, html):
            # keep anything with keyword OR civicplus page-id
            if has_strong_employment_signal(u, t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if has_strong_employment_signal(u, t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
                cand.append((u, t, "nav_footer_link"))

    # 3) quicklinks page crawl (often contains Jobs.aspx)
    ql = urljoin(base_home, "QuickLinks.aspx")
    q_resp, _ = get(ql)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if q_resp and q_resp.status_code < 400 and not looks_soft404(q_resp):
        for u, t in extract_links(q_resp.url, q_resp.text or ""):
            if has_strong_employment_signal(u, t) or "jobs.aspx" in u.lower():
                cand.append((u, t, "quicklinks"))

    # 4) civicplus search crawl (this should catch Essex)
    for s_url in civicplus_search_urls(base_home):
        s_resp, _ = get(s_url)
        time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
        if not s_resp or s_resp.status_code >= 400 or looks_soft404(s_resp):
            continue
        for u, t in extract_links(s_resp.url, s_resp.text or ""):
            if has_strong_employment_signal(u, t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
                cand.append((u, t, "civicplus_search"))

    return cand


def discover_civiclift(base_home: str) -> List[Tuple[str, str, str]]:
    """
    CivicLift sometimes uses stable pages (e.g., /employment or /job-openings),
    sometimes job postings are just articles that come and go.
    We'll return both stable page candidates and a search/fallback candidate.
    """
    cand: List[Tuple[str, str, str]] = []
    for p in ["/employment", "/job-openings", "/jobs", "/career-opportunities", "/careers"]:
        cand.append((urljoin(base_home, p.lstrip("/")), f"CIVICLIFT_PATH:{p}", "civiclift_path"))

    # crawl homepage for keywords
    h_resp, _ = get(base_home)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if h_resp and h_resp.status_code < 400 and not looks_soft404(h_resp):
        page_url = h_resp.url or base_home
        html = h_resp.text or ""
        for u, t in extract_links(page_url, html):
            if has_strong_employment_signal(u, t):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if has_strong_employment_signal(u, t):
                cand.append((u, t, "nav_footer_link"))

    # civic lift often has a site search; generic fallback: use internal civicplus-style Search if present
    # otherwise keep homepage as fallback and mark ephemeral in final decision.
    cand.append((base_home, "CIVICLIFT_FALLBACK_HOME", "fallback_home"))
    return cand


def discover_granicus(base_home: str) -> List[Tuple[str, str, str]]:
    """
    Granicus sites often link to an ATS vendor for jobs (NEOGOV / GovernmentJobs).
    We'll crawl homepage and look for ATS hints.
    """
    cand: List[Tuple[str, str, str]] = []

    h_resp, _ = get(base_home)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if h_resp and h_resp.status_code < 400 and not looks_soft404(h_resp):
        page_url = h_resp.url or base_home
        html = h_resp.text or ""
        for u, t in extract_links(page_url, html):
            if is_ats(u) or has_strong_employment_signal(u, t):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if is_ats(u) or has_strong_employment_signal(u, t):
                cand.append((u, t, "nav_footer_link"))

    # also try common HR/jobs pages
    for p in ["/government/human-resources", "/government/human-resources/city-jobs", "/jobs"]:
        cand.append((urljoin(base_home, p.lstrip("/")), f"GRANICUS_PATH:{p}", "granicus_path"))

    return cand


def discover_other(base_home: str) -> List[Tuple[str, str, str]]:
    """
    Generic: crawl homepage links; treat "Human Resources" as strong signal.
    """
    cand: List[Tuple[str, str, str]] = []
    h_resp, _ = get(base_home)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if h_resp and h_resp.status_code < 400 and not looks_soft404(h_resp):
        page_url = h_resp.url or base_home
        html = h_resp.text or ""
        for u, t in extract_links(page_url, html):
            lt = (t or "").lower()
            if has_strong_employment_signal(u, t) or "human resources" in lt:
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            lt = (t or "").lower()
            if has_strong_employment_signal(u, t) or "human resources" in lt:
                cand.append((u, t, "nav_footer_link"))
    return cand


# -------------------- Main per-town logic --------------------
def should_attempt(rec: Dict[str, Any]) -> bool:
    status = parse_int(rec.get("employment_url_status_code"))
    soft404 = bool(rec.get("employment_url_soft404"))
    if status in DO_NOT_REWRITE_IF_STATUS_IN:
        return False
    if status in REDISCOVER_IF_STATUS_IN:
        return True
    if REDISCOVER_IF_SOFT404_TRUE and soft404:
        return True
    return False


def should_attempt_application(rec: Dict[str, Any], employment_attempted: bool) -> bool:
    if employment_attempted:
        return True
    app_status = parse_int(rec.get("application_url_status_code"))
    app_soft404 = bool(rec.get("application_url_soft404"))
    current_app = rec.get("Application Form URL")
    if app_status in {404, 410, -1} or app_soft404:
        return True
    if not is_url(current_app):
        return True
    return False


def update_record(
    rec: Dict[str, Any],
    new_emp: str,
    platform: str,
    change_reason: str,
    confidence: int,
    discovery_method: str,
    discovery_score: int,
    validation_reason: str,
) -> None:
    rec["platform_detected"] = platform
    rec["Employment Page URL"] = new_emp
    rec["employment_url_final"] = new_emp
    rec["employment_url_last_checked_at"] = now_utc_iso()
    rec["employment_url_change_reason"] = change_reason
    rec["employment_url_confidence"] = confidence
    rec["employment_url_discovery_method"] = discovery_method
    rec["employment_url_discovery_score"] = discovery_score
    rec["employment_url_validation_reason"] = validation_reason


def update_application_record(
    rec: Dict[str, Any],
    new_app: str,
    reason: str,
    score: int,
) -> None:
    if "Application Form URL (original)" not in rec and isinstance(rec.get("Application Form URL"), str):
        rec["Application Form URL (original)"] = rec["Application Form URL"]
    rec["Application Form URL"] = new_app
    rec["application_url_final"] = new_app
    rec["application_url_last_checked_at"] = now_utc_iso()
    rec["application_url_change_reason"] = reason
    rec["application_url_confidence"] = max(75, min(95, score))


def rediscover_for_town(rec: Dict[str, Any]) -> Dict[str, Any]:
    town = rec.get("Town") or "(unknown)"

    # Ensure Town Website is homepage
    if is_url(rec.get("Town Website")):
        rec["Town Website"] = normalize_homepage(rec["Town Website"]) or rec["Town Website"]
    else:
        if is_url(rec.get("Employment Page URL")):
            rec["Town Website"] = normalize_homepage(rec["Employment Page URL"]) or rec.get("Town Website")

    base_home = rec.get("Town Website")
    if not is_url(base_home):
        return {"Town": town, "action": "skipped", "reason": "missing_town_homepage"}

    platform = detect_platform(rec)
    rec["platform_detected"] = platform

    employment_attempt = should_attempt(rec)
    application_attempt = should_attempt_application(rec, employment_attempted=employment_attempt)
    if not employment_attempt and not application_attempt:
        return {"Town": town, "action": "no_change", "reason": "not_marked_for_rediscovery", "platform": platform}

    # Gather candidates
    cand: List[Tuple[str, str, str]] = []
    if platform == "civicplus":
        cand = discover_civicplus(base_home)
    elif platform == "civiclift":
        cand = discover_civiclift(base_home)
    elif platform == "granicus":
        cand = discover_granicus(base_home)

    # If the Granicus site is bot-blocked, still try ATS fallbacks
        cand.extend(granicus_ats_fallback_candidates(town))
    else:
        cand = discover_other(base_home)

    sitemap_urls = get_sitemap_urls(base_home)
    sitemap_candidates = filter_sitemap_candidates(sitemap_urls)
    print(f"SITEMAP: {town} -> {len(sitemap_urls)} URLs, {len(sitemap_candidates)} candidates accepted")
    cand.extend(sitemap_candidates)

    # De-dupe URLs
    seen = set()
    deduped: List[Tuple[str, str, str]] = []
    for u, t, src in cand:
        if not is_url(u):
            continue
        u = u.strip()
        if u in seen:
            continue
        seen.add(u)
        deduped.append((u, t, src))

    # Score + validate best employment candidates
    scored = [(score_candidate(u, t, base_home, src), u, t, src) for (u, t, src) in deduped]
    scored.sort(reverse=True, key=lambda x: x[0])

    chosen: Optional[Tuple[int, str, str, str]] = None
    last_blocked_reason: Optional[str] = None

    if employment_attempt:
        for s, u, t, src in scored[:50]:
            if s < 0:
                continue
            if not is_ats(u) and not has_strong_employment_signal(u, t):
                continue
            if is_generic_navigation_candidate(u, t):
                continue
            if is_document_file(u):
                continue
            if "application" in text_blob(u, t):
                continue

            ok, final, why, blocked = validate_candidate(u, base_home)
            if blocked:
                last_blocked_reason = blocked
            if not ok or not final:
                continue
            chosen = (s, final, src, why)
            break

    old_emp = rec.get("Employment Page URL") or ""
    old_app = rec.get("Application Form URL") or ""
    employment_updated = False
    application_updated = False

    if chosen:
        s, new_emp, src, why = chosen
        conf = 70
        low = new_emp.lower()
        if has_strong_employment_signal(new_emp):
            conf += 15
        if CIVICPLUS_PAGEID_RE.match(urlparse(new_emp).path or ""):
            conf += 10
        if is_ats(new_emp):
            conf = max(conf, 85)
        if src == "sitemap":
            conf += 1
        conf = min(conf, 95)

        if is_ats(new_emp):
            rec["employment_page_type"] = "ats_vendor"
        elif platform == "civicplus" and ("jobs.aspx" in low or CIVICPLUS_PAGEID_RE.match(urlparse(new_emp).path or "")):
            rec["employment_page_type"] = "module_page"
        elif "human-resources" in low or "civil-service" in low:
            rec["employment_page_type"] = "hr_page"
        else:
            rec["employment_page_type"] = "page"

        if new_emp != old_emp:
            update_record(
                rec,
                new_emp,
                platform,
                f"rediscovered_from_{src}",
                conf,
                src,
                s,
                why,
            )
            employment_updated = True

    if application_attempt:
        app, app_reason, app_score, app_source = find_application_document(
            base_home=base_home,
            employment_url=rec.get("Employment Page URL") if is_url(rec.get("Employment Page URL")) else None,
            employment_candidates=deduped,
        )
        if app and app != old_app and not has_application_negative_signal(app):
            update_application_record(
                rec=rec,
                new_app=app,
                reason=f"{app_reason}:{app_source}" if app_source else app_reason,
                score=app_score,
            )
            application_updated = True

    if employment_updated or application_updated:
        return {
            "Town": town,
            "action": "updated",
            "platform": platform,
            "old_employment_url": old_emp,
            "new_employment_url": rec.get("Employment Page URL") or "",
            "old_application_url": old_app,
            "new_application_url": rec.get("Application Form URL") or "",
            "employment_updated": employment_updated,
            "application_updated": application_updated,
            "source": rec.get("employment_url_discovery_method") or "",
            "employment_page_type": rec.get("employment_page_type") or "",
        }

    if employment_attempt:
        reason = "no_candidate_validated"
        if last_blocked_reason:
            reason = f"no_candidate_validated:{last_blocked_reason}"
        return {
            "Town": town,
            "action": "needs_review",
            "reason": reason,
            "platform": platform,
            "employment_updated": False,
            "application_updated": False,
        }

    return {
        "Town": town,
        "action": "no_change",
        "reason": "application_not_improved",
        "platform": platform,
        "employment_updated": False,
        "application_updated": False,
    }


# -------------------- CLI --------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_json")
    ap.add_argument("output_json")
    ap.add_argument("report_csv")
    ap.add_argument("--town", help="Run rediscovery only for a single town name (case-insensitive).")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    in_json = Path(args.input_json)
    out_json = Path(args.output_json)
    out_csv = Path(args.report_csv)

    data = json.loads(in_json.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Expected JSON to be a list (array) of objects.")

    target = (args.town or "").strip().lower() or None

    report_rows: List[Dict[str, Any]] = []
    updated = 0
    needs_review = 0

    for i, rec in enumerate(data):
        if not isinstance(rec, dict):
            continue
        town_name = (rec.get("Town") or "").strip()
        if target and town_name.lower() != target:
            continue

        row = rediscover_for_town(rec)
        report_rows.append(row)
        if row.get("action") == "updated":
            updated += 1
        elif row.get("action") == "needs_review":
            needs_review += 1

        if not target and (i + 1) % 25 == 0:
            print(f"Processed {i+1}/{len(data)}... updated={updated}, needs_review={needs_review}")

    out_json.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    all_keys = set()
    for r in report_rows:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys) if report_rows else ["Town", "action", "reason"]

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(report_rows)

    print("\nDone.")
    print(f"Wrote updated JSON: {out_json}")
    print(f"Wrote report CSV:   {out_csv}")
    print(f"Updated towns: {updated}")
    print(f"Needs review:  {needs_review}")
    if target:
        print(f"(Town-only mode: {args.town})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
