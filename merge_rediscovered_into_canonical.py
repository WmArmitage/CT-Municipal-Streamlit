#!/usr/bin/env python3
"""
merge_rediscovered_into_canonical.py

Conservative merge of rediscovered municipal link candidates into the canonical dataset.

Typical usage (PowerShell):
  python .\\merge_rediscovered_into_canonical.py `
    --canonical "CT_Municipal_Employment_Pages.json" `
    --rediscovered "CT_Municipal_Employment_Pages.rediscovered.json" `
    --out "CT_Municipal_Employment_Pages.json" `
    --report "merge_report.csv" `
    --audit "merge_audit.json"
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from utils.url_utils import detect_soft404, is_html_content_type

JsonObj = Dict[str, Any]
Json = Union[JsonObj, List[Any]]

ATS_HINTS = (
    "governmentjobs.com",
    "neogov.com",
    "appone.com",
    "paycomonline.net",
    "jobapscloud.com",
    "frontlineeducation.com",
    "applitrack.com",
)

EMPLOYMENT_SIGNAL_TERMS = (
    "employment",
    "job",
    "jobs",
    "career",
    "careers",
    "human-resources",
    "human resources",
    "civil-service",
    "civil service",
    "vacancies",
    "job-openings",
    "employment-opportunities",
    "jobs.aspx",
)
GENERIC_NAV_TERMS = (
    "quicklinks",
    "formcenter",
    "documentcenter",
    "/home",
    "/index",
    "/departments",
    "/department",
)
APPLICATION_HINTS = (
    "application for employment",
    "employment application",
    "job application",
    "civil service application",
    "employment-app",
    "employment_app",
)
APPLICATION_NEGATIVE_TERMS = (
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
)
DOC_EXTENSIONS = (".pdf", ".doc", ".docx")

FIELD_RULES: Dict[str, Dict[str, str]] = {
    "Employment Page URL": {
        "report_field_name": "employment_page",
        "status_code_key": "employment_url_status_code",
        "soft404_key": "employment_url_soft404",
        "confidence_key": "employment_url_confidence",
        "score_key": "employment_url_discovery_score",
        "validation_reason_key": "employment_url_validation_reason",
    },
    "Application Form URL": {
        "report_field_name": "application_form",
        "status_code_key": "application_url_status_code",
        "soft404_key": "application_url_soft404",
        "confidence_key": "application_url_confidence",
        "score_key": "",
        "validation_reason_key": "",
    },
}

LIVE_TIMEOUT_SECS = 20
MAX_BYTES_TO_SCAN = 250_000
USER_AGENT = "CT-MuniJobs-MergeValidator/1.0 (+github.com/WmArmitage/municipal-employment-data)"


def load_json(path: str) -> Json:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Json, pretty: bool = True) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if pretty:
            json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(data, f, ensure_ascii=False)


def norm_town_name(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def looks_like_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    s = value.strip().lower()
    return s.startswith("http://") or s.startswith("https://")


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() == "true"


def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


@dataclass
class LiveURLCheck:
    url: str
    final_url: Optional[str]
    status_code: Optional[int]
    redirected: bool
    soft404: bool
    validation_status: str
    error: Optional[str]


def normalize_url_for_compare(url: Optional[str]) -> str:
    if not isinstance(url, str):
        return ""
    return url.strip().rstrip("/")


def validate_url_live(url: str, cache: Dict[str, LiveURLCheck]) -> LiveURLCheck:
    normalized = normalize_url_for_compare(url)
    if normalized in cache:
        return cache[normalized]

    result = LiveURLCheck(
        url=url,
        final_url=None,
        status_code=None,
        redirected=False,
        soft404=False,
        validation_status="broken",
        error=None,
    )
    try:
        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "*/*",
            },
            method="GET",
        )
        with urlopen(request, timeout=LIVE_TIMEOUT_SECS) as response:
            final_url = response.geturl() or url
            status_code = int(response.getcode() or 0)
            content_type = response.headers.get("Content-Type", "")
            body = b""
            soft404 = False
            if status_code == 200 and is_html_content_type(content_type):
                body = response.read(MAX_BYTES_TO_SCAN)
                text = body.decode("utf-8", errors="ignore")
                soft404 = detect_soft404(text)

            redirected = normalize_url_for_compare(url) != normalize_url_for_compare(final_url)
            if status_code == 403:
                validation_status = "uncertain"
            elif status_code >= 400:
                validation_status = "broken"
            elif soft404:
                validation_status = "suspicious"
            elif redirected:
                validation_status = "redirected"
            else:
                validation_status = "working"

            result = LiveURLCheck(
                url=url,
                final_url=final_url,
                status_code=status_code,
                redirected=redirected,
                soft404=soft404,
                validation_status=validation_status,
                error=None,
            )
    except HTTPError as exc:
        final_url = exc.geturl() or url
        status = "uncertain" if exc.code == 403 else "broken"
        result = LiveURLCheck(
            url=url,
            final_url=final_url,
            status_code=exc.code,
            redirected=normalize_url_for_compare(url) != normalize_url_for_compare(final_url),
            soft404=False,
            validation_status=status,
            error=str(exc),
        )
    except URLError as exc:
        result = LiveURLCheck(
            url=url,
            final_url=None,
            status_code=None,
            redirected=False,
            soft404=False,
            validation_status="broken",
            error=str(exc),
        )
    except Exception as exc:  # pragma: no cover
        result = LiveURLCheck(
            url=url,
            final_url=None,
            status_code=None,
            redirected=False,
            soft404=False,
            validation_status="broken",
            error=str(exc),
        )

    cache[normalized] = result
    return result


def host_norm(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


def same_host(a: str, b: str) -> bool:
    ha = host_norm(a)
    hb = host_norm(b)
    return ha != "" and hb != "" and ha == hb


def is_ats(url: str) -> bool:
    lower = (url or "").lower()
    return any(hint in lower for hint in ATS_HINTS)


def text_blob(*parts: Any) -> str:
    return " ".join(str(part or "").strip().lower() for part in parts)


def has_strong_employment_signal(url: str) -> bool:
    blob = text_blob(url)
    return any(term in blob for term in EMPLOYMENT_SIGNAL_TERMS)


def is_generic_navigation_url(url: str) -> bool:
    blob = text_blob(url)
    generic = any(term in blob for term in GENERIC_NAV_TERMS)
    if not generic:
        return False
    return not has_strong_employment_signal(url)


def is_document_file(url: str) -> bool:
    normalized = normalize_url_for_compare(url).lower().split("?", 1)[0]
    return any(normalized.endswith(ext) for ext in DOC_EXTENSIONS)


def has_application_negative_signal(url: str) -> bool:
    blob = text_blob(url)
    if "employment application" in blob or "application for employment" in blob:
        return False
    return any(term in blob for term in APPLICATION_NEGATIVE_TERMS)


def has_application_signal(url: str) -> bool:
    blob = text_blob(url)
    if any(term in blob for term in APPLICATION_HINTS):
        return True
    return "application" in blob and any(term in blob for term in ("employment", "job", "civil service"))


def employment_specificity_score(url: str) -> int:
    score = 0
    lower = text_blob(url)
    if has_strong_employment_signal(url):
        score += 4
    if "jobs.aspx" in lower:
        score += 2
    if is_ats(url):
        score += 3
    if is_generic_navigation_url(url):
        score -= 4
    if "application" in lower:
        score -= 2
    if is_document_file(url):
        score -= 3
    return score


def application_specificity_score(url: str) -> int:
    score = 0
    if has_application_negative_signal(url):
        score -= 6
    if has_application_signal(url):
        score += 4
    if is_document_file(url):
        score += 4
    if is_ats(url) and has_strong_employment_signal(url):
        score += 2
    if is_generic_navigation_url(url):
        score -= 2
    return score


def canonical_field_name(name: str) -> Optional[str]:
    normalized = (name or "").strip().lower()
    if normalized in {"employment_page", "employment_page_url", "employment"}:
        return "employment_page"
    if normalized in {"application_form", "application_pdf", "application"}:
        return "application_form"
    return None


def report_field_from_legacy_label(label: str) -> Optional[str]:
    if label == "Employment Page URL":
        return "employment_page"
    if label == "Application Form URL":
        return "application_form"
    return None


def row_validation_status(row: Dict[str, Any]) -> str:
    status = str(row.get("validation_status") or "").strip().lower()
    if status in {"working", "redirected", "broken", "suspicious", "uncertain"}:
        return status

    status_code = parse_int(row.get("status_code") or row.get("Status"))
    soft404 = parse_bool(row.get("soft404") if "soft404" in row else row.get("Soft404"))
    error = str(row.get("error") or row.get("Error") or "").strip()
    redirected = parse_bool(row.get("redirected"))

    if status_code == 403:
        return "uncertain"
    if error or status_code is None or status_code >= 400:
        return "broken"
    if soft404:
        return "suspicious"
    if redirected:
        return "redirected"
    return "working"


def load_link_health(
    csv_path: Optional[str],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if not csv_path:
        return {}
    if not os.path.exists(csv_path):
        return {}

    severity = {"working": 0, "redirected": 0, "suspicious": 1, "uncertain": 1, "broken": 2}
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            town = str(row.get("Town") or "").strip()
            if not town:
                continue

            field_name = canonical_field_name(str(row.get("field_name") or ""))
            if not field_name:
                field_name = report_field_from_legacy_label(str(row.get("Field") or ""))
            if not field_name:
                continue

            key = (norm_town_name(town), field_name)
            status = row_validation_status(row)
            previous = out.get(key, {})
            prev_status = str(previous.get("validation_status") or "")
            if previous and severity.get(status, 0) < severity.get(prev_status, 0):
                continue

            out[key] = {
                "validation_status": status,
                "status_code": parse_int(row.get("status_code") or row.get("Status")),
            }
    return out


def guess_town_field(rec: JsonObj) -> Optional[str]:
    for key in ("town", "Town", "municipality", "Municipality", "name", "Name"):
        if key in rec and isinstance(rec[key], str) and rec[key].strip():
            return key
    return None


@dataclass
class NormalizedData:
    kind: str
    town_key_field: Optional[str]
    map: Dict[str, JsonObj]
    original: Json


def normalize_to_map(data: Json) -> NormalizedData:
    if isinstance(data, dict):
        out: Dict[str, JsonObj] = {}
        for town, rec in data.items():
            if not isinstance(town, str):
                continue
            if not isinstance(rec, dict):
                rec = {"value": rec}
            out[norm_town_name(town)] = rec
        return NormalizedData(kind="dict", town_key_field=None, map=out, original=data)

    if isinstance(data, list):
        out = {}
        town_field: Optional[str] = None
        for rec in data:
            if isinstance(rec, dict):
                town_field = guess_town_field(rec)
                if town_field:
                    break
        if not town_field:
            raise ValueError("Could not find a town/name field in list-shaped JSON.")

        for rec in data:
            if not isinstance(rec, dict):
                continue
            town = rec.get(town_field)
            if isinstance(town, str) and town.strip():
                out[norm_town_name(town)] = rec
        return NormalizedData(kind="list", town_key_field=town_field, map=out, original=data)

    raise ValueError("Unsupported JSON root type (must be dict or list).")


def rebuild_original_from_map(norm: NormalizedData, new_map: Dict[str, JsonObj]) -> Json:
    if norm.kind == "dict":
        assert isinstance(norm.original, dict)
        rebuilt: Dict[str, Any] = {}
        for town_key, rec in norm.original.items():
            if isinstance(town_key, str):
                rebuilt[town_key] = new_map.get(norm_town_name(town_key), rec)
        return rebuilt

    assert norm.kind == "list"
    assert isinstance(norm.original, list)
    tf = norm.town_key_field
    if not tf:
        raise ValueError("List-shaped data missing town key field.")

    out: List[Any] = []
    for rec in norm.original:
        if not isinstance(rec, dict):
            out.append(rec)
            continue
        town = rec.get(tf)
        if isinstance(town, str) and town.strip():
            out.append(new_map.get(norm_town_name(town), rec))
        else:
            out.append(rec)
    return out


def town_label_from_norm(town_norm: str, canon_norm: NormalizedData) -> str:
    if canon_norm.kind == "dict" and isinstance(canon_norm.original, dict):
        for key in canon_norm.original.keys():
            if isinstance(key, str) and norm_town_name(key) == town_norm:
                return key
    if canon_norm.kind == "list" and canon_norm.town_key_field:
        rec = canon_norm.map.get(town_norm, {})
        v = rec.get(canon_norm.town_key_field)
        if isinstance(v, str) and v.strip():
            return v
    return town_norm


def broken_or_suspicious_evidence(
    town_norm: str,
    canonical_rec: JsonObj,
    field_name: str,
    rule: Dict[str, str],
    link_health: Dict[Tuple[str, str], Dict[str, Any]],
) -> Tuple[bool, str]:
    status_key = rule["status_code_key"]
    soft_key = rule["soft404_key"]

    if status_key in canonical_rec:
        status_code = parse_int(canonical_rec.get(status_key))
        if status_code is None:
            return True, "canonical_status_missing_after_check"
        if status_code == 403:
            return False, "canonical_status_403_uncertain"
        if status_code >= 400:
            return True, f"canonical_status_{status_code}"

    if parse_bool(canonical_rec.get(soft_key)):
        return True, "canonical_soft404"

    health = link_health.get((town_norm, field_name))
    if health:
        h_status = str(health.get("validation_status") or "").lower()
        if h_status in {"broken", "suspicious"}:
            return True, f"link_health_{h_status}"
        return False, f"link_health_{h_status or 'unknown'}"

    return False, "no_breakage_evidence"


def numeric_evidence(rec: JsonObj, key: str) -> Optional[int]:
    if not key:
        return None
    return parse_int(rec.get(key))


def decide_field_action(
    town_norm: str,
    canonical_rec: JsonObj,
    rediscovered_rec: JsonObj,
    field_label: str,
    rule: Dict[str, str],
    link_health: Dict[Tuple[str, str], Dict[str, Any]],
    live_cache: Dict[str, LiveURLCheck],
) -> Tuple[str, str, Dict[str, Any]]:
    original = canonical_rec.get(field_label)
    candidate = rediscovered_rec.get(field_label)
    field_name = rule["report_field_name"]
    evidence: Dict[str, Any] = {
        "canonical_live_status": "",
        "canonical_live_status_code": "",
        "canonical_live_final_url": "",
        "candidate_live_status": "",
        "candidate_live_status_code": "",
        "candidate_live_final_url": "",
    }

    if original == candidate:
        return "skipped", "candidate_equals_original", evidence
    if candidate in (None, ""):
        return "skipped", "candidate_missing", evidence
    if not looks_like_url(candidate):
        return "manual_review_needed", "candidate_not_url", evidence

    town_site = canonical_rec.get("Town Website")
    same_original_host = looks_like_url(original) and same_host(candidate, str(original))
    same_town_host = looks_like_url(town_site) and same_host(candidate, str(town_site))
    ats_candidate = is_ats(candidate)

    candidate_specificity = 0
    canonical_specificity = 0
    if field_name == "employment_page":
        candidate_specificity = employment_specificity_score(str(candidate))
        canonical_specificity = employment_specificity_score(str(original)) if looks_like_url(original) else 0
        if is_document_file(str(candidate)):
            return "skipped", "candidate_is_document_not_employment_page", evidence
        if is_generic_navigation_url(str(candidate)) and not has_strong_employment_signal(str(candidate)):
            return "skipped", "candidate_is_generic_navigation_page", evidence
        if not (has_strong_employment_signal(str(candidate)) or ats_candidate):
            return "skipped", "candidate_missing_strong_employment_signal", evidence
        if (
            looks_like_url(original)
            and has_strong_employment_signal(str(original))
            and not is_generic_navigation_url(str(original))
            and is_generic_navigation_url(str(candidate))
        ):
            return "skipped", "keep_direct_canonical_over_navigation_hub", evidence
    else:
        candidate_specificity = application_specificity_score(str(candidate))
        canonical_specificity = application_specificity_score(str(original)) if looks_like_url(original) else 0
        if has_application_negative_signal(str(candidate)):
            return "skipped", "candidate_is_unrelated_municipal_form", evidence
        if candidate_specificity < 2:
            return "manual_review_needed", "candidate_not_confident_application_form", evidence

    # Missing/blank original can be filled with a valid candidate.
    if not looks_like_url(original):
        candidate_live = validate_url_live(str(candidate), live_cache)
        evidence["candidate_live_status"] = candidate_live.validation_status
        evidence["candidate_live_status_code"] = (
            str(candidate_live.status_code) if candidate_live.status_code is not None else ""
        )
        evidence["candidate_live_final_url"] = candidate_live.final_url or ""
        if (
            candidate_live.validation_status in {"working", "redirected"}
            and (same_town_host or ats_candidate)
            and candidate_specificity >= 4
        ):
            return "applied", "original_missing_with_validated_safe_candidate", evidence
        return "manual_review_needed", "original_missing_candidate_not_safe_or_not_working", evidence

    canonical_live = validate_url_live(str(original), live_cache)
    candidate_live = validate_url_live(str(candidate), live_cache)
    evidence["canonical_live_status"] = canonical_live.validation_status
    evidence["canonical_live_status_code"] = (
        str(canonical_live.status_code) if canonical_live.status_code is not None else ""
    )
    evidence["canonical_live_final_url"] = canonical_live.final_url or ""
    evidence["candidate_live_status"] = candidate_live.validation_status
    evidence["candidate_live_status_code"] = (
        str(candidate_live.status_code) if candidate_live.status_code is not None else ""
    )
    evidence["candidate_live_final_url"] = candidate_live.final_url or ""

    canonical_ok = canonical_live.validation_status in {"working", "redirected"}
    candidate_ok = candidate_live.validation_status in {"working", "redirected"}
    canonical_uncertain = canonical_live.validation_status == "uncertain"
    candidate_uncertain = candidate_live.validation_status == "uncertain"

    status_rank = {"working": 4, "redirected": 4, "suspicious": 3, "uncertain": 2, "broken": 1}
    canonical_rank = status_rank.get(canonical_live.validation_status, 1)
    candidate_rank = status_rank.get(candidate_live.validation_status, 1)
    if candidate_rank < canonical_rank:
        return "skipped", "candidate_weaker_than_canonical", evidence

    if canonical_uncertain and candidate_uncertain:
        return "manual_review_needed", "both_urls_uncertain", evidence
    if canonical_uncertain and candidate_ok:
        if field_name == "employment_page" and is_generic_navigation_url(str(candidate)):
            return "skipped", "candidate_not_clearly_stronger_than_uncertain_canonical", evidence
        if candidate_specificity >= canonical_specificity + 2:
            return "manual_review_needed", "canonical_uncertain_candidate_appears_stronger_manual_confirmation", evidence
        return "skipped", "candidate_not_clearly_stronger_than_uncertain_canonical", evidence
    if canonical_uncertain and not candidate_ok:
        return "manual_review_needed", "canonical_uncertain_candidate_not_working", evidence
    if candidate_uncertain and canonical_ok:
        return "skipped", "candidate_uncertain_canonical_working", evidence

    # Protect known/manual canonical links from weaker candidates.
    if canonical_ok and not candidate_ok:
        return "skipped", "candidate_weaker_than_canonical", evidence

    # If canonical works, only replace with a clearly better candidate.
    if canonical_ok and candidate_ok:
        canonical_final = normalize_url_for_compare(canonical_live.final_url or str(original))
        candidate_final = normalize_url_for_compare(candidate_live.final_url or str(candidate))

        # Strict better rule: canonical redirects to the same destination as candidate,
        # and candidate is the stable direct URL.
        if (
            canonical_live.validation_status == "redirected"
            and canonical_final == candidate_final
            and normalize_url_for_compare(str(candidate)) == candidate_final
        ):
            return "applied", "candidate_is_direct_stable_target_of_canonical_redirect", evidence

        if field_name == "employment_page":
            if (
                has_strong_employment_signal(str(original))
                and not is_generic_navigation_url(str(original))
                and is_generic_navigation_url(str(candidate))
            ):
                return "skipped", "keep_direct_canonical_over_navigation_hub", evidence
            if (
                ats_candidate
                and has_strong_employment_signal(str(original))
                and canonical_specificity >= candidate_specificity
            ):
                return "skipped", "preserve_official_gateway_over_ats_candidate", evidence

        if field_name == "application_form":
            if is_document_file(str(original)) and not is_document_file(str(candidate)):
                return "skipped", "keep_direct_application_document_over_non_document_candidate", evidence

        if candidate_specificity >= canonical_specificity + 2 and (same_original_host or same_town_host or ats_candidate):
            return "applied", "candidate_more_specific_than_canonical", evidence
        return "skipped", "candidate_not_clearly_better_than_canonical", evidence

    # If canonical is not working, candidate must be working and reasonably safe.
    if not canonical_ok and candidate_ok:
        if field_name == "employment_page" and is_generic_navigation_url(str(candidate)):
            return "manual_review_needed", "candidate_generic_navigation_not_safe_for_auto_replace", evidence
        if (same_original_host or same_town_host or ats_candidate) and candidate_specificity >= 4:
            return "applied", "candidate_stronger_than_nonworking_canonical", evidence

        confidence = numeric_evidence(rediscovered_rec, rule.get("confidence_key", ""))
        score = numeric_evidence(rediscovered_rec, rule.get("score_key", ""))
        validation_reason_key = rule.get("validation_reason_key", "")
        validation_reason = str(rediscovered_rec.get(validation_reason_key) or "").lower() if validation_reason_key else ""
        if (
            (confidence is not None and confidence >= 90)
            or (score is not None and score >= 90)
            or validation_reason == "ok"
        ):
            return "manual_review_needed", "candidate_working_but_offsite_requires_manual_review", evidence
        return "manual_review_needed", "candidate_working_but_not_safe_enough", evidence

    # If both fail live checks, keep prior conservative fallback evidence.
    if canonical_uncertain or candidate_uncertain:
        return "manual_review_needed", "both_urls_nonworking_or_uncertain", evidence

    broken, broken_reason = broken_or_suspicious_evidence(
        town_norm=town_norm,
        canonical_rec=canonical_rec,
        field_name=field_name,
        rule=rule,
        link_health=link_health,
    )
    if not broken:
        return "skipped", f"candidate_weaker_than_canonical:{broken_reason}", evidence
    return "manual_review_needed", f"both_urls_not_working:{broken_reason}", evidence


def write_report_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fieldnames = [
        "town",
        "field_affected",
        "original_value",
        "candidate_value",
        "action_taken",
        "reason",
        "timestamp",
    ]
    extras: List[str] = []
    seen = set(fieldnames)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                extras.append(key)
                seen.add(key)
    fieldnames.extend(extras)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--canonical", required=True, help="Path to canonical CT_Municipal_Employment_Pages.json")
    ap.add_argument("--rediscovered", required=True, help="Path to rediscovered CT_Municipal_Employment_Pages.rediscovered.json")
    ap.add_argument("--out", required=True, help="Where to write merged canonical JSON")
    ap.add_argument("--report", default="merge_report.csv", help="CSV report output path")
    ap.add_argument("--audit", default="", help="Optional JSON audit output (before/after per field)")
    ap.add_argument(
        "--link-health",
        default="reports/link_health_report.csv",
        help="Optional link health CSV for breakage evidence (default: reports/link_health_report.csv)",
    )
    ap.add_argument("--allow-new-towns", action="store_true", help="Allow adding towns not present in canonical")
    # Backward-compatible argument shims (unused by new logic).
    ap.add_argument("--promote-status", action="append", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--overwrite-all-fields", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--overwrite-vendor-fields", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--overwrite-status-field", action="store_true", help=argparse.SUPPRESS)
    args = ap.parse_args()

    canonical = load_json(args.canonical)
    rediscovered = load_json(args.rediscovered)
    canon_norm = normalize_to_map(canonical)
    redisc_norm = normalize_to_map(rediscovered)
    link_health = load_link_health(args.link_health)

    now = datetime.now().isoformat(timespec="seconds")
    merged_map: Dict[str, JsonObj] = copy.deepcopy(canon_norm.map)
    report_rows: List[Dict[str, Any]] = []
    audit: Dict[str, Any] = {"generated_at": now, "applied": {}, "manual_review_needed": {}, "skipped": {}}
    live_cache: Dict[str, LiveURLCheck] = {}

    for town_norm, redisc_rec in redisc_norm.map.items():
        town_label = town_label_from_norm(town_norm, canon_norm)
        if town_norm not in canon_norm.map:
            action = "manual_review_needed"
            reason = "town_not_in_canonical"
            if args.allow_new_towns:
                merged_map[town_norm] = redisc_rec
                action = "applied"
                reason = "new_town_added"
            report_rows.append(
                {
                    "town": town_label,
                    "field_affected": "(town_record)",
                    "original_value": "",
                    "candidate_value": "[record]",
                    "action_taken": action,
                    "reason": reason,
                    "timestamp": now,
                }
            )
            audit[action][f"{town_label}::(town_record)"] = {"before": None, "after": "[record]", "reason": reason}
            continue

        canonical_rec = copy.deepcopy(merged_map[town_norm])

        for field_label, rule in FIELD_RULES.items():
            original = canonical_rec.get(field_label)
            candidate = redisc_rec.get(field_label)
            if original == candidate:
                continue

            action, reason, evidence = decide_field_action(
                town_norm=town_norm,
                canonical_rec=canonical_rec,
                rediscovered_rec=redisc_rec,
                field_label=field_label,
                rule=rule,
                link_health=link_health,
                live_cache=live_cache,
            )

            if action == "applied":
                merged_map[town_norm][field_label] = candidate
                if merged_map[town_norm].get(field_label) != candidate:
                    raise RuntimeError(
                        f"Failed to apply merge for {town_label} {field_label}: "
                        "assignment did not persist in merged map."
                    )

            report_rows.append(
                {
                    "town": town_label,
                    "field_affected": field_label,
                    "original_value": original if original is not None else "",
                    "candidate_value": candidate if candidate is not None else "",
                    "action_taken": action,
                    "reason": reason,
                    "timestamp": now,
                    "report_field_name": rule["report_field_name"],
                    "canonical_live_status": evidence.get("canonical_live_status", ""),
                    "canonical_live_status_code": evidence.get("canonical_live_status_code", ""),
                    "canonical_live_final_url": evidence.get("canonical_live_final_url", ""),
                    "candidate_live_status": evidence.get("candidate_live_status", ""),
                    "candidate_live_status_code": evidence.get("candidate_live_status_code", ""),
                    "candidate_live_final_url": evidence.get("candidate_live_final_url", ""),
                }
            )
            audit[action][f"{town_label}::{field_label}"] = {
                "before": original,
                "after": candidate,
                "reason": reason,
            }

    merged_json = rebuild_original_from_map(canon_norm, merged_map)
    save_json(args.out, merged_json, pretty=True)
    write_report_csv(args.report, report_rows)

    if args.audit:
        save_json(args.audit, audit, pretty=True)

    applied_count = sum(1 for r in report_rows if r["action_taken"] == "applied")
    skipped_count = sum(1 for r in report_rows if r["action_taken"] == "skipped")
    manual_count = sum(1 for r in report_rows if r["action_taken"] == "manual_review_needed")

    print(f"Wrote merged canonical: {args.out}")
    print(f"Wrote report CSV:      {args.report}")
    if args.audit:
        print(f"Wrote audit JSON:      {args.audit}")
    print(
        f"Applied: {applied_count} | "
        f"Skipped: {skipped_count} | "
        f"Manual review: {manual_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
