#!/usr/bin/env python3
"""
apply_url_check_summary.py

Applies link-health CSV results back into your JSON.

What it does:
- Writes status/final/last_checked metadata for employment + application URLs.
- Optionally updates canonical URL when it's safe:
  - redirect detected (final URL differs and status < 400)
  - CivicPlus page-id canonicalization (e.g., /home/pages/... -> /354/Employment-Opportunities)

What it does NOT do:
- "Fix" 403s by changing URLs (often bot-blocking).
- Guess new URLs when truly dead (that's the next script: rediscovery).

Usage:
  python apply_url_check_summary.py CT_Municipal_Employment_Pages.migrated.json reports/link_health_report.csv CT_Municipal_Employment_Pages.updated.json
"""

from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from utils.url_utils import normalize_homepage


CIVICPLUS_PAGEID_RE = re.compile(r"^/(\d{2,6})/[^/]+", re.IGNORECASE)

# If true, update canonical URL on safe conditions:
UPDATE_CANONICAL_ON_REDIRECT = True
UPDATE_CANONICAL_ON_CIVICPLUS_PAGEID = True

# 403/401 are usually bot blocks; don't rewrite canonical based on them.
DO_NOT_REWRITE_STATUSES = {401, 403}


def parse_iso_any(s: str) -> str:
    # keep whatever timestamp the CSV gave us; if missing, use now in local offset-less ISO.
    if isinstance(s, str) and s.strip():
        return s.strip()
    return datetime.now().isoformat()


def civicplus_pageid_path(u: str) -> Optional[str]:
    """Return '/<id>/<slug>' path if present, else None."""
    try:
        p = urlparse(u).path
        m = CIVICPLUS_PAGEID_RE.match(p)
        if not m:
            return None
        return p
    except Exception:
        return None


def field_to_prefix(field: str) -> Optional[str]:
    if field == "Employment Page URL":
        return "employment"
    if field == "Application Form URL":
        return "application"
    return None


def canonical_field_name_to_dataset_key(field_name: str) -> Optional[str]:
    normalized = (field_name or "").strip().lower()
    if normalized == "employment_page":
        return "Employment Page URL"
    if normalized in {"application_form", "application_pdf"}:
        return "Application Form URL"
    return None


def row_first_value(row: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() == "true"


def main() -> int:
    if len(sys.argv) != 4:
        print("Usage: python apply_url_check_summary.py <input.json> <link_health_report.csv> <output.json>")
        return 2

    json_in = Path(sys.argv[1])
    csv_in = Path(sys.argv[2])
    json_out = Path(sys.argv[3])

    data = json.loads(json_in.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Expected JSON to be a list (array) of objects.")

    # Index records by Town name (your dataset uses unique Town)
    by_town: Dict[str, Dict[str, Any]] = {}
    for rec in data:
        if isinstance(rec, dict) and isinstance(rec.get("Town"), str):
            by_town[rec["Town"]] = rec

    updates = 0
    with csv_in.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            town = (row.get("Town") or "").strip()
            field = (row.get("Field") or "").strip()
            if not field:
                field = canonical_field_name_to_dataset_key(str(row.get("field_name") or "")) or ""
            prefix = field_to_prefix(field)
            if not town or not prefix:
                continue
            rec = by_town.get(town)
            if not rec:
                continue

            # Stamp metadata
            checked_at = parse_iso_any(str(row_first_value(row, ("checked_at", "checked_at_utc")) or ""))
            status_str = str(row_first_value(row, ("status_code", "Status")) or "").strip()
            final_url = str(row_first_value(row, ("final_url", "Final URL")) or "").strip() or None
            soft404 = parse_bool(row_first_value(row, ("soft404", "Soft404")))
            err = str(row_first_value(row, ("error", "Error")) or "").strip() or None

            try:
                status = int(float(status_str)) if status_str else None
            except Exception:
                status = None

            rec[f"{prefix}_url_status_code"] = status
            rec[f"{prefix}_url_final"] = final_url
            rec[f"{prefix}_url_last_checked_at"] = checked_at
            rec[f"{prefix}_url_soft404"] = soft404
            rec[f"{prefix}_url_error"] = err

            # Ensure Town Website homepage
            if isinstance(rec.get("Town Website"), str):
                rec["Town Website"] = normalize_homepage(rec["Town Website"]) or rec["Town Website"]
            else:
                # derive from current URL
                cur = rec.get(field)
                if isinstance(cur, str):
                    rec["Town Website"] = normalize_homepage(cur) or rec.get("Town Website")

            # Optionally rewrite canonical URLs on safe conditions
            cur_url = rec.get(field)
            if isinstance(cur_url, str) and final_url and status is not None:
                if status in DO_NOT_REWRITE_STATUSES:
                    pass  # don't touch canonical based on bot-block status
                elif soft404:
                    pass  # don't rewrite based only on a soft404 result
                else:
                    # 1) Redirect/Final URL rewrite
                    if UPDATE_CANONICAL_ON_REDIRECT and status < 400 and final_url != cur_url:
                        rec[field] = final_url
                        rec[f"{prefix}_url_change_reason"] = "redirect_canonicalized"
                        rec[f"{prefix}_url_confidence"] = 95
                        updates += 1

                    # 2) CivicPlus page-id canonicalization: prefer /<id>/<slug> over /home/pages/...
                    if UPDATE_CANONICAL_ON_CIVICPLUS_PAGEID and status < 400 and final_url:
                        cur_pid = civicplus_pageid_path(cur_url) is not None
                        fin_pid = civicplus_pageid_path(final_url) is not None
                        if fin_pid and not cur_pid:
                            rec[field] = final_url
                            rec[f"{prefix}_url_change_reason"] = "civicplus_pageid_canonicalized"
                            rec[f"{prefix}_url_confidence"] = 95
                            updates += 1

    json_out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Done. Wrote {json_out}. Canonical URL rewrites applied: {updates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
