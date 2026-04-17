#!/usr/bin/env python3
"""
Fast URL checker for CT municipal employment JSON.

- Input can be a local JSON file path OR an https URL (e.g. raw GitHub).
- Uses GET (not HEAD), follows redirects.
- Concurrent checks for speed.
- Flags soft-404 pages (200 but "page not found" content).
- Outputs:
  - reports/link_health_report.csv
  - reports/link_health_report.json
"""

from __future__ import annotations

import csv
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.url_utils import detect_soft404, is_html_content_type, is_url


# ---------------- Config ----------------
TIMEOUT_SECS = 20
MAX_WORKERS = 20
MAX_BYTES_TO_SCAN = 250_000
VERIFY_TLS = True

USER_AGENT = "CT-MuniJobs-LinkChecker/1.2 (+github.com/WmArmitage/municipal-employment-data)"

# Dataset keys to validate.
URL_FIELDS: List[str] = [
    "Employment Page URL",
    "Application Form URL",
]

FIELD_NAME_MAP: Dict[str, str] = {
    "Employment Page URL": "employment_page",
    "Application Form URL": "application_form",
}


@dataclass
class CheckResult:
    town: str
    field_label: str
    field_name: str
    original_url: str
    final_url: Optional[str]
    status_code: Optional[int]
    redirected: bool
    soft404: bool
    validation_status: str
    elapsed_ms: Optional[int]
    error: Optional[str]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_validation_status(
    status_code: Optional[int],
    redirected: bool,
    soft404: bool,
    error: Optional[str],
) -> str:
    if status_code == 403:
        return "uncertain"
    if error or status_code is None or status_code >= 400:
        return "broken"
    if soft404:
        return "suspicious"
    if redirected:
        return "redirected"
    return "working"


def load_json_from_path_or_url(src: str) -> List[Dict[str, Any]]:
    if src.lower().startswith(("http://", "https://")):
        response = requests.get(
            src,
            timeout=TIMEOUT_SECS,
            headers={"User-Agent": USER_AGENT},
            verify=VERIFY_TLS,
        )
        response.raise_for_status()
        return response.json()

    path = Path(src)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def check_one(field_label: str, town: str, url: str) -> CheckResult:
    t0 = time.perf_counter()
    field_name = FIELD_NAME_MAP.get(field_label, field_label.lower().replace(" ", "_"))
    try:
        response = requests.get(
            url,
            timeout=TIMEOUT_SECS,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            verify=VERIFY_TLS,
        )
        elapsed_ms = int((time.perf_counter() - t0) * 1000)

        status_code = response.status_code
        final_url = str(response.url) if response.url else None
        redirected = bool(final_url and final_url != url)

        soft404 = False
        content_type = response.headers.get("Content-Type") or ""
        if status_code == 200 and is_html_content_type(content_type):
            chunk = response.content[:MAX_BYTES_TO_SCAN]
            text = chunk.decode(response.encoding or "utf-8", errors="ignore")
            soft404 = detect_soft404(text)

        validation_status = classify_validation_status(status_code, redirected, soft404, None)
        return CheckResult(
            town=town,
            field_label=field_label,
            field_name=field_name,
            original_url=url,
            final_url=final_url,
            status_code=status_code,
            redirected=redirected,
            soft404=soft404,
            validation_status=validation_status,
            elapsed_ms=elapsed_ms,
            error=None,
        )
    except requests.RequestException as exc:
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        validation_status = classify_validation_status(None, False, False, str(exc))
        return CheckResult(
            town=town,
            field_label=field_label,
            field_name=field_name,
            original_url=url,
            final_url=None,
            status_code=None,
            redirected=False,
            soft404=False,
            validation_status=validation_status,
            elapsed_ms=elapsed_ms,
            error=str(exc),
        )


def result_to_row(result: CheckResult, checked_at: str) -> Dict[str, Any]:
    ok = result.validation_status in {"working", "redirected"}
    return {
        # canonical output
        "checked_at": checked_at,
        "Town": result.town,
        "field_name": result.field_name,
        "original_url": result.original_url,
        "final_url": result.final_url,
        "status_code": result.status_code,
        "redirected": result.redirected,
        "soft404": result.soft404,
        "validation_status": result.validation_status,
        "error": result.error,
        "elapsed_ms": result.elapsed_ms,
        # legacy aliases for compatibility
        "checked_at_utc": checked_at,
        "Field": result.field_label,
        "Original URL": result.original_url,
        "Final URL": result.final_url,
        "Status": result.status_code,
        "Soft404": result.soft404,
        "OK": ok,
    }


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage:\n  python check_urls.py <path-or-url-to-json>\n")
        return 2

    src = sys.argv[1]
    data = load_json_from_path_or_url(src)
    if not isinstance(data, list):
        print("Error: JSON must be a list (array) of objects.")
        return 2

    checked_at = now_utc_iso()
    jobs: List[Tuple[str, str, str]] = []
    for rec in data:
        town = rec.get("Town") or rec.get("town") or "(unknown)"
        for field_label in URL_FIELDS:
            value = rec.get(field_label)
            if is_url(value):
                jobs.append((field_label, town, value.strip()))

    print(f"Loaded {len(data)} records; checking {len(jobs)} URLs with {MAX_WORKERS} workers...")

    results: List[CheckResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(check_one, field_label, town, url) for (field_label, town, url) in jobs]
        for idx, future in enumerate(as_completed(futures), start=1):
            results.append(future.result())
            if idx % 100 == 0:
                print(f"  completed {idx}/{len(futures)}...")

    rows = [result_to_row(result, checked_at) for result in results]

    counts = {
        "records": len(data),
        "urls_checked": len(rows),
        "working": sum(1 for row in rows if row["validation_status"] == "working"),
        "redirected": sum(1 for row in rows if row["validation_status"] == "redirected"),
        "uncertain": sum(1 for row in rows if row["validation_status"] == "uncertain"),
        "broken": sum(1 for row in rows if row["validation_status"] == "broken"),
        "suspicious": sum(1 for row in rows if row["validation_status"] == "suspicious"),
    }

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_csv = reports_dir / "link_health_report.csv"
    out_json = reports_dir / "link_health_report.json"

    if rows:
        with out_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    else:
        out_csv.write_text("", encoding="utf-8")

    out_json.write_text(
        json.dumps(
            {
                "source": src,
                "checked_at": checked_at,
                "counts": counts,
                "results": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\nDone.")
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_json}")
    print(
        "Counts: "
        f"working={counts['working']}, "
        f"redirected={counts['redirected']}, "
        f"uncertain={counts['uncertain']}, "
        f"suspicious={counts['suspicious']}, "
        f"broken={counts['broken']}"
    )

    failures = [row for row in rows if row["validation_status"] in {"broken", "suspicious"}]
    if failures:
        print("\nTop failures:")
        for row in failures[:25]:
            print(
                f"- {row['Town']} | {row['field_name']} | "
                f"{row['status_code']} | {row['original_url']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
