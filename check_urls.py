#!/usr/bin/env python3
"""
Fast URL checker for CT municipal employment JSON.

- Input can be a local JSON file path OR an https URL (e.g. raw GitHub).
- Uses GET (not HEAD), follows redirects.
- Concurrent checks for speed.
- Flags soft-404 pages (200 but "page not found" content).
- Outputs:
  - url_check_summary.csv
  - url_check_failures.csv
  - url_check_results.json
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------- Config ----------------
TIMEOUT_SECS = 20
MAX_WORKERS = 20          # increase if you want faster, but too high may get rate-limited
MAX_BYTES_TO_SCAN = 250_000
VERIFY_TLS = True

USER_AGENT = "CT-MuniJobs-LinkChecker/1.1 (+github.com/WmArmitage/municipal-employment-data)"

# Fields in your dataset to check (label must match JSON keys)
URL_FIELDS: List[str] = [
    "Employment Page URL",
    "Application Form URL",
    # "Town Website",  # optional; enable if you want
]

SOFT_404_PATTERNS = [
    r"\bpage not found\b",
    r"\b404\b",
    r"\bthe page you requested\b",
    r"\bdoes not exist\b",
    r"\bnot be found\b",
]
SOFT_404_RE = re.compile("|".join(SOFT_404_PATTERNS), re.IGNORECASE)


@dataclass
class CheckResult:
    town: str
    field: str
    original_url: str
    ok: bool
    status: Optional[int]
    final_url: Optional[str]
    soft_404: bool
    elapsed_ms: Optional[int]
    error: Optional[str]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_url(v: Any) -> bool:
    return isinstance(v, str) and v.strip().lower().startswith(("http://", "https://"))


def load_json_from_path_or_url(src: str) -> List[Dict[str, Any]]:
    if src.lower().startswith(("http://", "https://")):
        r = requests.get(src, timeout=TIMEOUT_SECS, headers={"User-Agent": USER_AGENT}, verify=VERIFY_TLS)
        r.raise_for_status()
        return r.json()
    else:
        p = Path(src)
        if not p.exists():
            raise FileNotFoundError(f"File not found: {p}")
        return json.loads(p.read_text(encoding="utf-8"))


def check_one(field: str, town: str, url: str) -> CheckResult:
    t0 = time.perf_counter()
    try:
        resp = requests.get(
            url,
            timeout=TIMEOUT_SECS,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            verify=VERIFY_TLS,
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)

        status = resp.status_code
        final_url = str(resp.url) if resp.url else None

        soft_404 = False
        content_type = (resp.headers.get("Content-Type") or "").lower()
        if status == 200 and ("text/html" in content_type or "application/xhtml" in content_type or content_type == ""):
            chunk = resp.content[:MAX_BYTES_TO_SCAN]
            text = chunk.decode(resp.encoding or "utf-8", errors="ignore")
            if SOFT_404_RE.search(text):
                soft_404 = True

        ok = (200 <= status < 400) and not soft_404

        return CheckResult(
            town=town,
            field=field,
            original_url=url,
            ok=ok,
            status=status,
            final_url=final_url,
            soft_404=soft_404,
            elapsed_ms=elapsed_ms,
            error=None,
        )
    except requests.RequestException as e:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        return CheckResult(
            town=town,
            field=field,
            original_url=url,
            ok=False,
            status=None,
            final_url=None,
            soft_404=False,
            elapsed_ms=elapsed_ms,
            error=str(e),
        )


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage:\n  python check_urls.py <path-or-url-to-json>\n")
        return 2

    src = sys.argv[1]
    data = load_json_from_path_or_url(src)
    if not isinstance(data, list):
        print("Error: JSON must be a list (array) of objects.")
        return 2

    run_at = now_utc_iso()

    jobs: List[Tuple[str, str, str]] = []
    for rec in data:
        town = rec.get("Town") or rec.get("town") or "(unknown)"
        for field in URL_FIELDS:
            u = rec.get(field)
            if is_url(u):
                jobs.append((field, town, u.strip()))

    print(f"Loaded {len(data)} records; checking {len(jobs)} URLs with {MAX_WORKERS} workers...")

    results: List[CheckResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(check_one, field, town, url) for (field, town, url) in jobs]
        for i, fut in enumerate(as_completed(futs), start=1):
            res = fut.result()
            results.append(res)
            if i % 100 == 0:
                print(f"  completed {i}/{len(futs)}...")

    # Write outputs next to current directory (or alongside a file input if you prefer)
    out_summary_csv = Path("url_check_summary.csv")
    out_failures_csv = Path("url_check_failures.csv")
    out_results_json = Path("url_check_results.json")

    rows = [{
        "checked_at_utc": run_at,
        "Town": r.town,
        "Field": r.field,
        "Original URL": r.original_url,
        "OK": r.ok,
        "Status": r.status,
        "Final URL": r.final_url,
        "Soft404": r.soft_404,
        "Elapsed ms": r.elapsed_ms,
        "Error": r.error,
    } for r in results]

    with out_summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            w.writeheader()
            w.writerows(rows)

    failures = [row for row in rows if row["OK"] is False]
    with out_failures_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            w.writeheader()
            w.writerows(failures)

    out_results_json.write_text(json.dumps({
        "source": src,
        "checked_at_utc": run_at,
        "counts": {
            "records": len(data),
            "urls_checked": len(jobs),
            "failures": len(failures),
        },
        "results": [asdict(r) for r in results],
    }, indent=2), encoding="utf-8")

    print("\nDone.")
    print(f"Wrote: {out_summary_csv}  (all checks)")
    print(f"Wrote: {out_failures_csv} (only broken/soft-404/errors)")
    print(f"Wrote: {out_results_json} (structured output)")
    print(f"Failures: {len(failures)} / {len(rows)}")

    # show top 25 failures in console
    if failures:
        print("\nTop failures:")
        for row in failures[:25]:
            print(f"- {row['Town']} | {row['Field']} | {row['Status']} | {row['Original URL']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
