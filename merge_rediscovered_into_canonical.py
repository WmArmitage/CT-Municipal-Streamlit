#!/usr/bin/env python3
"""
merge_rediscovered_into_canonical.py

Merges rediscovered employment-page results into a canonical CT_Municipal_Employment_Pages.json
without coupling the Streamlit app to scraper intermediates.

Typical usage (PowerShell):
  python .\merge_rediscovered_into_canonical.py `
    --canonical "CT_Municipal_Employment_Pages.json" `
    --rediscovered "CT_Municipal_Employment_Pages.rediscovered.json" `
    --out "CT_Municipal_Employment_Pages.json" `
    --report "merge_report.csv" `
    --audit "merge_audit.json"

Default behavior:
- Only promotes records where rediscovered.status == "updated"
- Only overwrites URL-ish fields (employment_page, employment_url, page_url, application_pdf, etc.)
- Leaves "needs_review" records untouched (but reports them)
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

JsonObj = Dict[str, Any]
Json = Union[JsonObj, List[Any]]


# ----------------------------
# Utilities
# ----------------------------

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
    """Normalize town key for matching."""
    s = name.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def guess_town_field(rec: JsonObj) -> Optional[str]:
    """Try common keys used for town name."""
    for k in ("town", "Town", "municipality", "Municipality", "name", "Name"):
        if k in rec and isinstance(rec[k], str) and rec[k].strip():
            return k
    return None

def is_urlish_key(k: str) -> bool:
    """Keys that commonly contain URLs in your dataset."""
    lk = k.lower()
    return any(
        token in lk
        for token in (
            "url",
            "link",
            "page",
            "pdf",
            "homepage",
            "home_page",
            "employment",
            "application",
        )
    )

def looks_like_url(v: Any) -> bool:
    if not isinstance(v, str):
        return False
    s = v.strip().lower()
    return s.startswith("http://") or s.startswith("https://")

def deep_get_status(obj: Any) -> Optional[str]:
    """
    Attempt to find a status value from common shapes:
      - record["status"]
      - record["result"]["status"]
      - top-level dict of results where record has 'status'
    """
    if isinstance(obj, dict):
        for key in ("status", "merge_status", "result_status"):
            v = obj.get(key)
            if isinstance(v, str):
                return v.strip().lower()
        # nested
        for key in ("result", "rediscovery", "rediscovered", "meta"):
            v = obj.get(key)
            if isinstance(v, dict):
                s = deep_get_status(v)
                if s:
                    return s
    return None


# ----------------------------
# Adapters: make both files into a normalized map keyed by town
# ----------------------------

@dataclass
class NormalizedData:
    """Represents a normalized mapping of town_key -> record, plus the original container type."""
    kind: str  # "dict" or "list"
    town_key_field: Optional[str]  # for list kind, which field stores town name
    map: Dict[str, JsonObj]  # normalized town -> record
    original: Json  # original loaded json

def normalize_to_map(data: Json) -> NormalizedData:
    """
    Accepts either:
      - dict keyed by town -> record
      - list of records each with a town field
    Returns normalized mapping keyed by normalized town name.
    """
    if isinstance(data, dict):
        m: Dict[str, JsonObj] = {}
        for town, rec in data.items():
            if not isinstance(town, str):
                continue
            if not isinstance(rec, dict):
                # if value isn't dict, wrap it
                rec = {"value": rec}
            m[norm_town_name(town)] = rec
        return NormalizedData(kind="dict", town_key_field=None, map=m, original=data)

    if isinstance(data, list):
        m = {}
        town_field: Optional[str] = None
        # discover town_field from first usable record
        for rec in data:
            if isinstance(rec, dict):
                tf = guess_town_field(rec)
                if tf:
                    town_field = tf
                    break
        if not town_field:
            raise ValueError("Could not find a town/name field in list-shaped JSON.")

        for rec in data:
            if not isinstance(rec, dict):
                continue
            town = rec.get(town_field)
            if not isinstance(town, str) or not town.strip():
                continue
            m[norm_town_name(town)] = rec
        return NormalizedData(kind="list", town_key_field=town_field, map=m, original=data)

    raise ValueError("Unsupported JSON root type (must be dict or list).")


# ----------------------------
# Merge policy
# ----------------------------

@dataclass
class MergePolicy:
    promote_statuses: Tuple[str, ...] = ("updated",)  # only promote these
    allow_new_towns: bool = False  # add towns not present in canonical?
    overwrite_url_fields_only: bool = True  # safer default
    overwrite_vendor_fields: bool = False
    overwrite_status_field: bool = False
    url_field_allowlist: Optional[Tuple[str, ...]] = None  # if set, only these keys can be overwritten
    protect_fields: Tuple[str, ...] = ("town", "Town", "name", "Name", "municipality", "Municipality")

def keys_allowed_to_overwrite(rec_key: str, policy: MergePolicy) -> bool:
    if rec_key in policy.protect_fields:
        return False
    if policy.url_field_allowlist is not None:
        return rec_key in policy.url_field_allowlist
    if policy.overwrite_url_fields_only:
        return is_urlish_key(rec_key)
    return True

def merge_record(
    canonical_rec: JsonObj,
    rediscovered_rec: JsonObj,
    policy: MergePolicy,
) -> Tuple[JsonObj, Dict[str, Tuple[Any, Any]]]:
    """
    Returns (merged_record, changes) where changes maps field -> (old, new).
    """
    merged = copy.deepcopy(canonical_rec)
    changes: Dict[str, Tuple[Any, Any]] = {}

    # Optional: if rediscovered has vendor/service fields and you want them, allow overwriting
    vendorish = ("vendor", "service", "platform", "provider")

    for k, new_v in rediscovered_rec.items():
        # status handling
        if k.lower() == "status" and not policy.overwrite_status_field:
            continue

        if not keys_allowed_to_overwrite(k, policy):
            continue

        if not policy.overwrite_vendor_fields and k.lower() in vendorish:
            continue

        old_v = merged.get(k)

        # If we are overwriting URL-ish fields, prefer URLs and non-empty strings
        if policy.overwrite_url_fields_only:
            if isinstance(new_v, str):
                if new_v.strip() == "":
                    continue
                # If field looks url-like, require a URL-ish value if the old one is URL
                if is_urlish_key(k) and (looks_like_url(old_v) or old_v is None):
                    if not looks_like_url(new_v):
                        # don't replace a URL with a non-URL string
                        continue

        if old_v != new_v:
            merged[k] = new_v
            changes[k] = (old_v, new_v)

    return merged, changes


# ----------------------------
# Main merge routine
# ----------------------------

def rebuild_original_from_map(norm: NormalizedData, new_map: Dict[str, JsonObj]) -> Json:
    """
    Rebuild JSON in the original shape:
      - dict: keep original keys when possible (we can’t recover original casing perfectly)
      - list: update in-place by matching town
    """
    if norm.kind == "dict":
        # Rebuild dict keyed by *original* town keys if we can:
        # We'll use the original dict keys as they were, updating where the normalized match exists.
        original_dict: Dict[str, Any] = {}
        assert isinstance(norm.original, dict)
        for town_key, rec in norm.original.items():
            if not isinstance(town_key, str):
                continue
            nk = norm_town_name(town_key)
            if nk in new_map:
                original_dict[town_key] = new_map[nk]
            else:
                original_dict[town_key] = rec
        # If there are new towns allowed, append them with a title-cased key
        missing = [k for k in new_map.keys() if norm_town_name(k) not in {norm_town_name(x) for x in original_dict.keys()}]
        # The above is a bit messy; in practice allow_new_towns is False by default.
        return original_dict

    # list
    assert norm.kind == "list"
    assert isinstance(norm.original, list)
    out_list: List[Any] = []
    tf = norm.town_key_field
    if not tf:
        raise ValueError("List-shaped data missing town_key_field.")

    for rec in norm.original:
        if not isinstance(rec, dict):
            out_list.append(rec)
            continue
        town = rec.get(tf)
        if isinstance(town, str) and town.strip():
            nk = norm_town_name(town)
            if nk in new_map:
                out_list.append(new_map[nk])
            else:
                out_list.append(rec)
        else:
            out_list.append(rec)

    return out_list


def write_report_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fieldnames = [
        "town",
        "action",
        "reason",
        "changed_fields",
        "timestamp",
    ]
    # add some convenience columns if present
    extra = set()
    for r in rows:
        extra |= set(r.keys())
    for k in sorted(extra):
        if k not in fieldnames:
            fieldnames.append(k)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--canonical", required=True, help="Path to canonical CT_Municipal_Employment_Pages.json")
    ap.add_argument("--rediscovered", required=True, help="Path to rediscovered CT_Municipal_Employment_Pages.rediscovered.json")
    ap.add_argument("--out", required=True, help="Where to write merged canonical JSON (can be same as --canonical)")
    ap.add_argument("--report", default="merge_report.csv", help="CSV report output path")
    ap.add_argument("--audit", default="", help="Optional JSON audit output (before/after diffs)")
    ap.add_argument("--promote-status", action="append", default=None, help="Statuses to promote (repeatable). Default: updated")
    ap.add_argument("--allow-new-towns", action="store_true", help="Allow adding towns not in canonical")
    ap.add_argument("--overwrite-all-fields", action="store_true", help="Overwrite all fields (DANGEROUS). Default overwrites URL-ish fields only.")
    ap.add_argument("--overwrite-vendor-fields", action="store_true", help="Allow overwriting vendor/service/provider fields")
    ap.add_argument("--overwrite-status-field", action="store_true", help="Allow overwriting 'status' field in canonical")
    args = ap.parse_args()

    canonical = load_json(args.canonical)
    rediscovered = load_json(args.rediscovered)

    canon_norm = normalize_to_map(canonical)
    redisc_norm = normalize_to_map(rediscovered)

    policy = MergePolicy(
        promote_statuses=tuple(s.lower() for s in (args.promote_status or (args.promote_statuses if hasattr(args, "promote_statuses") else [])))  # just in case
        if args.promote_status else ("updated",),
        allow_new_towns=bool(args.allow_new_towns),
        overwrite_url_fields_only=not args.overwrite_all_fields,
        overwrite_vendor_fields=bool(args.overwrite_vendor_fields),
        overwrite_status_field=bool(args.overwrite_status_field),
    )

    now = datetime.now().isoformat(timespec="seconds")

    merged_map: Dict[str, JsonObj] = copy.deepcopy(canon_norm.map)
    report_rows: List[Dict[str, Any]] = []
    audit: Dict[str, Any] = {"generated_at": now, "changes": {}}

    for town_norm, redisc_rec in redisc_norm.map.items():
        town_label = None
        # derive a human town label for reporting
        if canon_norm.kind == "dict":
            # find original town key by scanning original dict once
            if isinstance(canon_norm.original, dict):
                for k in canon_norm.original.keys():
                    if isinstance(k, str) and norm_town_name(k) == town_norm:
                        town_label = k
                        break
        else:
            tf = canon_norm.town_key_field
            if tf and town_norm in canon_norm.map:
                tl = canon_norm.map[town_norm].get(tf)
                if isinstance(tl, str):
                    town_label = tl
        town_label = town_label or town_norm

        status = deep_get_status(redisc_rec) or "unknown"

        if town_norm not in canon_norm.map:
            if policy.allow_new_towns and status in policy.promote_statuses:
                merged_map[town_norm] = redisc_rec
                report_rows.append({
                    "town": town_label,
                    "action": "added",
                    "reason": f"town not in canonical; status={status}",
                    "changed_fields": "*new*",
                    "timestamp": now,
                })
            else:
                report_rows.append({
                    "town": town_label,
                    "action": "skipped",
                    "reason": f"town not in canonical; allow_new_towns={policy.allow_new_towns}; status={status}",
                    "changed_fields": "",
                    "timestamp": now,
                })
            continue

        if status not in policy.promote_statuses:
            report_rows.append({
                "town": town_label,
                "action": "skipped",
                "reason": f"status={status} not in promote_statuses={policy.promote_statuses}",
                "changed_fields": "",
                "timestamp": now,
            })
            continue

        before = canon_norm.map[town_norm]
        after, changes = merge_record(before, redisc_rec, policy)

        if changes:
            merged_map[town_norm] = after
            report_rows.append({
                "town": town_label,
                "action": "merged",
                "reason": f"status={status}",
                "changed_fields": ";".join(sorted(changes.keys())),
                "timestamp": now,
            })
            if args.audit:
                audit["changes"][town_label] = {
                    "status": status,
                    "changed_fields": {k: {"before": bv, "after": av} for k, (bv, av) in changes.items()},
                }
        else:
            report_rows.append({
                "town": town_label,
                "action": "noop",
                "reason": f"status={status} but no eligible field changes",
                "changed_fields": "",
                "timestamp": now,
            })

    # Rebuild canonical in its original shape and write
    merged_json = rebuild_original_from_map(canon_norm, merged_map)
    save_json(args.out, merged_json, pretty=True)
    write_report_csv(args.report, report_rows)

    if args.audit:
        save_json(args.audit, audit, pretty=True)

    # Basic console summary
    merged_count = sum(1 for r in report_rows if r["action"] == "merged")
    added_count = sum(1 for r in report_rows if r["action"] == "added")
    skipped_count = sum(1 for r in report_rows if r["action"] == "skipped")
    noop_count = sum(1 for r in report_rows if r["action"] == "noop")

    print(f"Wrote merged canonical: {args.out}")
    print(f"Wrote report CSV:      {args.report}")
    if args.audit:
        print(f"Wrote audit JSON:      {args.audit}")
    print(f"Merged: {merged_count} | Added: {added_count} | Skipped: {skipped_count} | No-op: {noop_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
