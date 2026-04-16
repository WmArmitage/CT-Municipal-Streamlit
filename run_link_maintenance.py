#!/usr/bin/env python3
"""
Semi-automated maintenance runner for CT municipal employment links.

This runner:
1) Validates canonical URLs and writes link health reports.
2) Targets only broken/suspicious towns for rediscovery.
3) Runs a test merge only (never writes canonical by default).
4) Prints a concise summary for review.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "reports"

CANONICAL_DEFAULT = ROOT / "CT_Municipal_Employment_Pages.json"
LINK_HEALTH_CSV = REPORTS_DIR / "link_health_report.csv"
MERGED_TEST_JSON = REPORTS_DIR / "CT_Municipal_Employment_Pages.merged.test.json"
MERGE_REPORT_CSV = REPORTS_DIR / "merge_report.csv"
MERGE_AUDIT_JSON = REPORTS_DIR / "merge_audit.json"
REDISCOVERY_INPUT_JSON = REPORTS_DIR / "_maintenance.rediscovery.input.json"
REDISCOVERED_TARGET_JSON = REPORTS_DIR / "CT_Municipal_Employment_Pages.rediscovered.targeted.json"
REDISCOVERY_REPORT_CSV = REPORTS_DIR / "rediscovery_report.targeted.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run validation, targeted rediscovery, and test-merge without overwriting canonical JSON.",
    )
    parser.add_argument(
        "--canonical",
        default=str(CANONICAL_DEFAULT),
        help="Canonical dataset JSON path (default: CT_Municipal_Employment_Pages.json)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Run check_urls only and stop after reporting link health summary.",
    )
    parser.add_argument(
        "--include-rediscovery",
        action="store_true",
        help="Explicit flag for targeted rediscovery (rediscovery is already included unless --validate-only).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned commands and counts without executing child scripts or writing temp files.",
    )
    return parser.parse_args()


def normalize_town(town: str) -> str:
    return " ".join((town or "").strip().lower().split())


def parse_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def field_name_from_row(row: Dict[str, Any]) -> str:
    direct = str(row.get("field_name") or "").strip().lower()
    if direct:
        return direct
    legacy = str(row.get("Field") or "").strip().lower()
    if legacy == "employment page url":
        return "employment_page"
    if legacy == "application form url":
        return "application_form"
    return direct


def fmt_cmd(cmd: Iterable[str]) -> str:
    parts: List[str] = []
    for part in cmd:
        if " " in part or "\t" in part:
            parts.append(f'"{part}"')
        else:
            parts.append(part)
    return " ".join(parts)


def run_command(cmd: List[str], dry_run: bool) -> None:
    print(f"$ {fmt_cmd(cmd)}")
    if dry_run:
        return
    completed = subprocess.run(cmd, cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {fmt_cmd(cmd)}")


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def load_link_health_counts(path: Path) -> Dict[str, Any]:
    rows = read_csv_rows(path)
    towns = set()
    broken = 0
    suspicious = 0
    issues: List[Dict[str, Any]] = []

    for row in rows:
        town = str(row.get("Town") or "").strip()
        if town:
            towns.add(normalize_town(town))
        status = str(row.get("validation_status") or "").strip().lower()
        if status == "broken":
            broken += 1
        if status == "suspicious":
            suspicious += 1
        if status in {"broken", "suspicious"}:
            issues.append(row)

    return {
        "rows": rows,
        "issue_rows": issues,
        "total_towns_checked": len(towns),
        "broken_links_count": broken,
        "suspicious_links_count": suspicious,
    }


def pick_worst_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    rank = {"broken": 2, "suspicious": 1}
    return max(rows, key=lambda r: rank.get(str(r.get("validation_status") or "").strip().lower(), 0))


def build_targeted_rediscovery_input(
    canonical_path: Path,
    issue_rows: List[Dict[str, Any]],
    out_path: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    issue_index: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in issue_rows:
        town = str(row.get("Town") or "").strip()
        field_name = field_name_from_row(row)
        if not town or field_name not in {"employment_page", "application_form"}:
            continue
        issue_index[normalize_town(town)][field_name].append(row)

    if dry_run:
        return {
            "targeted_towns": len(issue_index),
            "targeted_records": len(issue_index),
        }

    with canonical_path.open("r", encoding="utf-8") as handle:
        canonical_data = json.load(handle)
    if not isinstance(canonical_data, list):
        raise ValueError("Canonical JSON must be a list of records.")

    targeted_records: List[Dict[str, Any]] = []
    for rec in canonical_data:
        if not isinstance(rec, dict):
            continue
        town = str(rec.get("Town") or rec.get("town") or "").strip()
        if not town:
            continue
        key = normalize_town(town)
        if key not in issue_index:
            continue

        cloned = copy.deepcopy(rec)
        emp_issue_row = pick_worst_row(issue_index[key].get("employment_page", []))
        if emp_issue_row:
            status_code = parse_int(emp_issue_row.get("status_code") or emp_issue_row.get("Status"))
            soft404 = parse_bool(
                emp_issue_row.get("soft404")
                if "soft404" in emp_issue_row
                else emp_issue_row.get("Soft404")
            )
            cloned["employment_url_status_code"] = status_code
            cloned["employment_url_soft404"] = soft404
        else:
            # Keep employment untouched unless absent; application-only issues can still be reviewed in merge.
            cloned.setdefault("employment_url_status_code", 200)
            cloned.setdefault("employment_url_soft404", False)

        targeted_records.append(cloned)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(targeted_records, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "targeted_towns": len(issue_index),
        "targeted_records": len(targeted_records),
    }


def count_rediscovery_candidates(path: Path) -> int:
    rows = read_csv_rows(path)
    candidates = 0
    for row in rows:
        if str(row.get("action") or "").strip().lower() != "updated":
            continue
        new_url = str(row.get("new_employment_url") or "").strip()
        old_url = str(row.get("old_employment_url") or "").strip()
        if new_url and new_url != old_url:
            candidates += 1
    return candidates


def load_merge_counts(path: Path) -> Dict[str, int]:
    rows = read_csv_rows(path)
    counts = Counter()
    for row in rows:
        action = str(row.get("action_taken") or "").strip().lower()
        if action:
            counts[action] += 1
    return {
        "applied": counts.get("applied", 0),
        "manual_review_needed": counts.get("manual_review_needed", 0),
    }


def main() -> int:
    args = parse_args()
    canonical_path = Path(args.canonical)
    include_rediscovery = not args.validate_only or args.include_rediscovery

    if not canonical_path.exists() and not args.dry_run:
        raise FileNotFoundError(f"Canonical JSON not found: {canonical_path}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    run_command(
        [sys.executable, str(ROOT / "check_urls.py"), str(canonical_path)],
        dry_run=args.dry_run,
    )

    link_health = load_link_health_counts(LINK_HEALTH_CSV)

    candidate_replacements_found = 0
    applied_changes = 0
    manual_review_needed = 0

    if not args.validate_only and include_rediscovery:
        targeting = build_targeted_rediscovery_input(
            canonical_path=canonical_path,
            issue_rows=link_health["issue_rows"],
            out_path=REDISCOVERY_INPUT_JSON,
            dry_run=args.dry_run,
        )
        print(
            "Targeted rediscovery scope: "
            f"{targeting['targeted_towns']} town(s), "
            f"{targeting['targeted_records']} record(s)"
        )

        run_command(
            [
                sys.executable,
                str(ROOT / "rediscover_employment_links_v3.py"),
                str(REDISCOVERY_INPUT_JSON),
                str(REDISCOVERED_TARGET_JSON),
                str(REDISCOVERY_REPORT_CSV),
            ],
            dry_run=args.dry_run,
        )

        run_command(
            [
                sys.executable,
                str(ROOT / "merge_rediscovered_into_canonical.py"),
                "--canonical",
                str(canonical_path),
                "--rediscovered",
                str(REDISCOVERED_TARGET_JSON),
                "--out",
                str(MERGED_TEST_JSON),
                "--report",
                str(MERGE_REPORT_CSV),
                "--audit",
                str(MERGE_AUDIT_JSON),
                "--link-health",
                str(LINK_HEALTH_CSV),
            ],
            dry_run=args.dry_run,
        )

        if not args.dry_run:
            candidate_replacements_found = count_rediscovery_candidates(REDISCOVERY_REPORT_CSV)
            merge_counts = load_merge_counts(MERGE_REPORT_CSV)
            applied_changes = merge_counts["applied"]
            manual_review_needed = merge_counts["manual_review_needed"]

    print("\nMaintenance summary")
    print(f"- Total towns checked: {link_health['total_towns_checked']}")
    print(f"- Broken links count: {link_health['broken_links_count']}")
    print(f"- Suspicious links count: {link_health['suspicious_links_count']}")
    print(f"- Candidate replacements found: {candidate_replacements_found}")
    print(f"- Applied changes in test merge: {applied_changes}")
    print(f"- Manual review needed count: {manual_review_needed}")

    print("\nOutputs")
    print(f"- Link health CSV: {LINK_HEALTH_CSV}")
    if not args.validate_only and include_rediscovery:
        print(f"- Test-merged JSON: {MERGED_TEST_JSON}")
        print(f"- Merge report CSV: {MERGE_REPORT_CSV}")
        print(f"- Merge audit JSON: {MERGE_AUDIT_JSON}")

    if args.validate_only:
        print("\nvalidate-only mode: rediscovery and merge steps were skipped.")
    if args.dry_run:
        print("\ndry-run mode: commands were printed but not executed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
