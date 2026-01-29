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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, unquote

import requests
from bs4 import BeautifulSoup


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
DO_NOT_REWRITE_IF_STATUS_IN = {401, 403}  # bot-block likely

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
    "employment", "employment opportunities",
    "jobs", "job", "job openings", "job opportunities",
    "careers", "career opportunities",
    "human resources", "hr",
    "vacancies", "openings",
    "apply", "application",
]
KW_STRONG_LABELS = [
    "employment opportunities",
    "job openings",
    "career opportunities",
    "human resources",
]
KW_NEGATIVE = [
    "departments",
    "department",
    "about",
    "contact",
    "news",
    "calendar",
    "events",
    "agenda",
    "minutes",
    "meetings",
    "boards",
    "commissions",
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
SOFT404_RE = re.compile(
    r"(page not found|404|the page you requested|does not exist|not be found)",
    re.IGNORECASE,
)
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
    "application for employment", "employment application", "job application",
    "application", "fillable", "empapp", "employment-app",
]


# -------------------- Helpers --------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_url(v: Any) -> bool:
    return isinstance(v, str) and v.strip().lower().startswith(("http://", "https://"))


def is_pdf(url: str) -> bool:
    return (url or "").lower().split("?")[0].endswith(".pdf")


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
    if "text/html" not in ctype and "application/xhtml" not in ctype and ctype != "":
        return False
    text = resp.text[:250_000] if resp.text else ""
    return bool(SOFT404_RE.search(text))


def homepage(url: str) -> Optional[str]:
    if not isinstance(url, str) or not url.strip():
        return None
    u = urlparse(url.strip())
    if not u.scheme or not u.netloc:
        return None
    return f"{u.scheme}://{u.netloc}/"


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
    s = 0

    if same_site(url, base_home):
        s += 40
    if is_ats(url):
        s += 45

    if source in {"civicplus_path", "civiclift_path", "granicus_path"}:
        s += 20

    if kw_hit(u) or kw_hit(t):
        s += 50
    if any(k in t for k in KW_STRONG_LABELS):
        s += 15

    # CivicPlus page-id is a strong signal for real content pages
    try:
        path = urlparse(url).path or ""
        if CIVICPLUS_PAGEID_RE.match(path) and ("employment" in u or "job" in u or "career" in u):
            s += 30
    except Exception:
        pass

    # prefer HTML pages over PDFs
    if is_pdf(url):
        s -= 25

    # prefer "employment/jobs/careers" in path
    if any(x in u for x in ["employment", "jobs", "careers", "human-resources"]):
        s += 10
    if "jobs.aspx" in u:
        s += 20

    if any(bad in u for bad in KW_NEGATIVE) and not kw_hit(u):
        s -= 15
    if any(bad in t for bad in KW_NEGATIVE) and not kw_hit(t):
        s -= 10

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


def find_application_pdf(employment_url: str, base_home: str) -> Tuple[Optional[str], str]:
    """
    Look for a plausible application PDF on an employment page.
    Prefer same-site PDFs. Don't overwrite with dead off-domain PDFs.
    """
    resp, err = get(employment_url)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if resp is None:
        return None, f"employment_fetch_error: {err}"
    if resp.status_code >= 400 or looks_soft404(resp):
        return None, f"employment_not_ok: {resp.status_code}"

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" not in ctype and ctype != "":
        return None, "employment_not_html"

    links = extract_links(resp.url or employment_url, resp.text or "")
    pdfs = [(u, t) for u, t in links if is_pdf(u)]
    if not pdfs:
        return None, "no_pdf_links"

    best = None
    best_score = -999
    for u, t in pdfs:
        hay = (u + " " + t).lower()
        s = 0
        if same_site(u, base_home):
            s += 20
        for hint in APPLICATION_HINTS:
            if hint in hay:
                s += 25
        if "application" in hay:
            s += 10
        if s > best_score:
            best_score = s
            best = u

    if best and best_score >= 35:
        # validate the pdf is alive
        ok, final, reason, _ = validate_candidate(best, base_home)
        if ok:
            return final, "application_pdf_found_on_employment_page"
        return None, f"application_pdf_candidate_invalid:{reason}"

    return None, "no_confident_application_pdf_found"

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
    This is a best-effort fallback; some towns use different slugs.
    """
    slug = re.sub(r"[^a-z0-9]+", "", (town or "").lower())
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
            if kw_hit(u) or kw_hit(t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if kw_hit(u) or kw_hit(t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
                cand.append((u, t, "nav_footer_link"))

    # 3) quicklinks page crawl (often contains Jobs.aspx)
    ql = urljoin(base_home, "QuickLinks.aspx")
    q_resp, _ = get(ql)
    time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
    if q_resp and q_resp.status_code < 400 and not looks_soft404(q_resp):
        for u, t in extract_links(q_resp.url, q_resp.text or ""):
            if kw_hit(u) or kw_hit(t) or "jobs.aspx" in u.lower():
                cand.append((u, t, "quicklinks"))

    # 4) civicplus search crawl (this should catch Essex)
    for s_url in civicplus_search_urls(base_home):
        s_resp, _ = get(s_url)
        time.sleep(SLEEP_BETWEEN_REQUESTS_SECS)
        if not s_resp or s_resp.status_code >= 400 or looks_soft404(s_resp):
            continue
        for u, t in extract_links(s_resp.url, s_resp.text or ""):
            if kw_hit(u) or kw_hit(t) or CIVICPLUS_PAGEID_RE.match(urlparse(u).path or ""):
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
            if kw_hit(u) or kw_hit(t):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if kw_hit(u) or kw_hit(t):
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
            if is_ats(u) or kw_hit(u) or kw_hit(t):
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            if is_ats(u) or kw_hit(u) or kw_hit(t):
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
            if kw_hit(u) or kw_hit(t) or "human resources" in lt:
                cand.append((u, t, "homepage_link"))
        for u, t in extract_links_with_selector(page_url, html, "nav a[href], footer a[href]"):
            lt = (t or "").lower()
            if kw_hit(u) or kw_hit(t) or "human resources" in lt:
                cand.append((u, t, "nav_footer_link"))
    return cand


# -------------------- Main per-town logic --------------------
def should_attempt(rec: Dict[str, Any]) -> bool:
    status = rec.get("employment_url_status_code")
    soft404 = bool(rec.get("employment_url_soft404"))
    if status in DO_NOT_REWRITE_IF_STATUS_IN:
        return False
    if status in REDISCOVER_IF_STATUS_IN:
        return True
    if REDISCOVER_IF_SOFT404_TRUE and soft404:
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


def rediscover_for_town(rec: Dict[str, Any]) -> Dict[str, Any]:
    town = rec.get("Town") or "(unknown)"

    # Ensure Town Website is homepage
    if is_url(rec.get("Town Website")):
        rec["Town Website"] = homepage(rec["Town Website"]) or rec["Town Website"]
    else:
        if is_url(rec.get("Employment Page URL")):
            rec["Town Website"] = homepage(rec["Employment Page URL"]) or rec.get("Town Website")

    base_home = rec.get("Town Website")
    if not is_url(base_home):
        return {"Town": town, "action": "skipped", "reason": "missing_town_homepage"}

    platform = detect_platform(rec)
    rec["platform_detected"] = platform

    status = rec.get("employment_url_status_code")
    if status in DO_NOT_REWRITE_IF_STATUS_IN:
        return {"Town": town, "action": "no_change", "reason": f"status_{status}_bot_block_likely", "platform": platform}

    if not should_attempt(rec):
        return {"Town": town, "action": "no_change", "reason": "employment_not_marked_broken", "platform": platform}

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

    # Score + validate best candidates
    scored = [(score_candidate(u, t, base_home, src), u, t, src) for (u, t, src) in deduped]
    scored.sort(reverse=True, key=lambda x: x[0])

    best_html: Optional[Tuple[int, str, str, str]] = None  # (score, final_url, src, reason)
    best_pdf: Optional[Tuple[int, str, str, str]] = None
    last_blocked_reason: Optional[str] = None

    # Validate up to top N
    for s, u, t, src in scored[:40]:
        if s < 0:
            continue
        if is_social(u):
            continue

        ok, final, why, blocked = validate_candidate(u, base_home)
        if blocked:
            last_blocked_reason = blocked
        if not ok or not final:
            continue

        # Keep ATS even if offsite; otherwise same-site enforced by validate_candidate
        if is_pdf(final):
            # only accept PDF as last resort
            if best_pdf is None or s > best_pdf[0]:
                best_pdf = (s, final, src, why)
        else:
            if best_html is None or s > best_html[0]:
                best_html = (s, final, src, why)

    chosen = best_html or best_pdf
    if not chosen:
        # CivicLift fallback: mark as ephemeral_posts instead of failure if nothing found
        if platform == "civiclift":
            rec["employment_page_type"] = "ephemeral_posts"
            return {
                "Town": town,
                "action": "updated",
                "platform": platform,
                "reason": "no_stable_employment_page_detected_ephemeral_posts",
                "new_employment_url": base_home,
                "confidence": 60,
                "employment_page_type": "ephemeral_posts",
            }
        if last_blocked_reason:
            rec["employment_url_last_blocked_reason"] = last_blocked_reason
            return {
                "Town": town,
                "action": "needs_review",
                "reason": "no_candidate_validated",
                "platform": platform,
                "blocked_reason": last_blocked_reason,
            }
        return {"Town": town, "action": "needs_review", "reason": "no_candidate_validated", "platform": platform}

    s, new_emp, src, why = chosen
    old_emp = rec.get("Employment Page URL")

    # Confidence
    conf = 70
    low = new_emp.lower()
    if any(k in low for k in ["employment", "jobs", "careers", "human-resources"]):
        conf += 15
    if CIVICPLUS_PAGEID_RE.match(urlparse(new_emp).path or ""):
        conf += 10
    if is_ats(new_emp):
        conf = max(conf, 85)
    if is_pdf(new_emp):
        conf = min(conf, 75)
    conf = min(conf, 95)

    # Employment page type
    if is_ats(new_emp):
        rec["employment_page_type"] = "ats_vendor"
    elif platform == "civicplus" and ("jobs.aspx" in new_emp.lower() or CIVICPLUS_PAGEID_RE.match(urlparse(new_emp).path or "")):
        rec["employment_page_type"] = "module_page"
    elif "human-resources" in new_emp.lower() or "human resources" in (rec.get("Notes") or "").lower():
        rec["employment_page_type"] = "hr_page"
    elif is_pdf(new_emp):
        rec["employment_page_type"] = "pdf_posting"
    else:
        rec["employment_page_type"] = "page"

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

    # Optionally refresh application PDF if employment is HTML and same-site
    if not is_pdf(new_emp) and same_site(new_emp, base_home):
        pdf, pdf_reason = find_application_pdf(new_emp, base_home)
        if pdf:
            if "Application Form URL (original)" not in rec and isinstance(rec.get("Application Form URL"), str):
                rec["Application Form URL (original)"] = rec["Application Form URL"]
            rec["Application Form URL"] = pdf
            rec["application_url_final"] = pdf
            rec["application_url_last_checked_at"] = now_utc_iso()
            rec["application_url_change_reason"] = pdf_reason
            rec["application_url_confidence"] = 85

    return {
        "Town": town,
        "action": "updated",
        "platform": platform,
        "old_employment_url": old_emp or "",
        "new_employment_url": new_emp,
        "confidence": conf,
        "source": src,
        "employment_page_type": rec.get("employment_page_type") or "",
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
