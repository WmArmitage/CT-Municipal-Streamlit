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
from urllib.parse import urlparse

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
    if status in {"working", "redirected", "broken", "suspicious"}:
        return status

    status_code = parse_int(row.get("status_code") or row.get("Status"))
    soft404 = parse_bool(row.get("soft404") if "soft404" in row else row.get("Soft404"))
    error = str(row.get("error") or row.get("Error") or "").strip()
    redirected = parse_bool(row.get("redirected"))

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

    severity = {"working": 0, "redirected": 0, "suspicious": 1, "broken": 2}
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
) -> Tuple[str, str]:
    original = canonical_rec.get(field_label)
    candidate = rediscovered_rec.get(field_label)
    field_name = rule["report_field_name"]

    if original == candidate:
        return "skipped", "candidate_equals_original"
    if candidate in (None, ""):
        return "skipped", "candidate_missing"
    if not looks_like_url(candidate):
        return "manual_review_needed", "candidate_not_url"

    town_site = canonical_rec.get("Town Website")
    same_original_host = looks_like_url(original) and same_host(candidate, str(original))
    same_town_host = looks_like_url(town_site) and same_host(candidate, str(town_site))
    ats_candidate = is_ats(candidate)

    # Missing/blank original can be filled with a valid candidate.
    if not looks_like_url(original):
        if same_town_host or ats_candidate:
            return "applied", "original_missing_or_non_url_with_safe_candidate"
        return "manual_review_needed", "original_missing_candidate_offsite"

    broken, broken_reason = broken_or_suspicious_evidence(
        town_norm=town_norm,
        canonical_rec=canonical_rec,
        field_name=field_name,
        rule=rule,
        link_health=link_health,
    )
    if not broken:
        return "skipped", f"original_not_marked_broken:{broken_reason}"

    confidence = numeric_evidence(rediscovered_rec, rule.get("confidence_key", ""))
    score = numeric_evidence(rediscovered_rec, rule.get("score_key", ""))
    validation_reason_key = rule.get("validation_reason_key", "")
    validation_reason = str(rediscovered_rec.get(validation_reason_key) or "").lower() if validation_reason_key else ""
    confidence_ok = bool(
        (confidence is not None and confidence >= 80)
        or (score is not None and score >= 80)
        or validation_reason == "ok"
    )

    if same_original_host or same_town_host or ats_candidate or confidence_ok:
        reason_bits = [broken_reason]
        if same_original_host:
            reason_bits.append("same_original_host")
        if same_town_host:
            reason_bits.append("same_town_host")
        if ats_candidate:
            reason_bits.append("ats_candidate")
        if confidence_ok:
            reason_bits.append("rediscovery_confident")
        return "applied", ";".join(reason_bits)

    return "manual_review_needed", f"{broken_reason};candidate_not_safe_enough"


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
        mutable_rec = merged_map[town_norm]

        for field_label, rule in FIELD_RULES.items():
            original = canonical_rec.get(field_label)
            candidate = redisc_rec.get(field_label)
            if original == candidate:
                continue

            action, reason = decide_field_action(
                town_norm=town_norm,
                canonical_rec=canonical_rec,
                rediscovered_rec=redisc_rec,
                field_label=field_label,
                rule=rule,
                link_health=link_health,
            )

            if action == "applied":
                mutable_rec[field_label] = candidate

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
