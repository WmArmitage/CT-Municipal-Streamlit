#!/usr/bin/env python3
"""
Generate sellable CSV exports for the CT municipal employment dataset.

Outputs:
- exports/ct_municipal_jobs_full.csv
- exports/ct_municipal_jobs_verified.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


OUTPUT_COLUMNS = [
    "Town",
    "Town Website",
    "Employment Page URL",
    "Application Form URL",
    "ATS or Platform (if known)",
    "Employment Status",
    "Application Status",
    "Last Checked (employment)",
    "Last Checked (application)",
]

FIELD_MAP = {
    "employment_page": "Employment Page URL",
    "application_form": "Application Form URL",
}

VALIDATION_TO_INTERNAL_STATUS = {
    "working": "verified",
    "verified": "verified",
    "redirected": "redirected",
    "suspicious": "suspicious",
    "broken": "suspicious",
}

INTERNAL_TO_LABEL = {
    "verified": "Verified",
    "redirected": "Verified",
    "suspicious": "Check link",
    "unavailable": "Unavailable",
    "unverified": "Available",
}

VERIFIED_LABEL_BLOCKLIST_DEFAULT = {"Unavailable", "Check link"}


@dataclass
class LinkMeta:
    validation_status: str
    status_code: Optional[int]
    final_url: Optional[str]
    soft404: bool
    checked_at_raw: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export sellable CT municipal employment CSV datasets."
    )
    parser.add_argument(
        "--input-json",
        default="CT_Municipal_Employment_Pages.json",
        help="Path to canonical municipal dataset JSON.",
    )
    parser.add_argument(
        "--link-health",
        default=None,
        help="Optional path to link_health_report.csv (auto-detected if omitted).",
    )
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Directory for output CSV files.",
    )
    parser.add_argument(
        "--include-check-link",
        action="store_true",
        help="Keep 'Check link' rows in the verified export.",
    )
    return parser.parse_args()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_url(value: Any) -> str:
    return normalize_text(value).rstrip("/").lower()


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return normalize_text(value).lower() == "true"


def to_int(value: Any) -> Optional[int]:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def parse_iso_datetime(value: str) -> Optional[datetime]:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def resolve_timezone(tz_name: str) -> Any:
    try:
        return ZoneInfo(tz_name)
    except Exception:
        local_tz = datetime.now().astimezone().tzinfo
        return local_tz or timezone.utc


def format_human_date(value: Optional[str], tz_name: str = "America/New_York") -> str:
    if not value:
        return ""
    dt = parse_iso_datetime(value)
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_local = dt.astimezone(resolve_timezone(tz_name))
    return f"{dt_local.strftime('%b')} {dt_local.day}, {dt_local.year}"


def pick_latest_meta(current: Optional[LinkMeta], candidate: LinkMeta) -> LinkMeta:
    if current is None:
        return candidate
    cur_dt = parse_iso_datetime(current.checked_at_raw or "")
    cand_dt = parse_iso_datetime(candidate.checked_at_raw or "")
    if cand_dt and cur_dt:
        return candidate if cand_dt >= cur_dt else current
    if cand_dt and not cur_dt:
        return candidate
    return current


def read_link_health(path: Path) -> Dict[Tuple[str, str], LinkMeta]:
    lookup: Dict[Tuple[str, str], LinkMeta] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            town = normalize_text(row.get("Town")).lower()
            field_name = normalize_text(row.get("field_name")).lower()

            if not field_name:
                field_alias = normalize_text(row.get("Field")).lower()
                if "employment" in field_alias:
                    field_name = "employment_page"
                elif "application" in field_alias:
                    field_name = "application_form"

            if not town or field_name not in FIELD_MAP:
                continue

            original_url = normalize_text(row.get("original_url") or row.get("Original URL"))
            meta = LinkMeta(
                validation_status=normalize_text(row.get("validation_status")).lower(),
                status_code=to_int(row.get("status_code") or row.get("Status")),
                final_url=normalize_text(row.get("final_url") or row.get("Final URL")) or None,
                soft404=to_bool(row.get("soft404") if "soft404" in row else row.get("Soft404")),
                checked_at_raw=normalize_text(row.get("checked_at") or row.get("checked_at_utc")) or None,
            )

            key = (town, field_name)
            lookup[key] = pick_latest_meta(lookup.get(key), meta)

            if original_url:
                url_key = ("url", normalize_url(original_url))
                lookup[url_key] = pick_latest_meta(lookup.get(url_key), meta)

    return lookup


def auto_detect_link_health() -> Optional[Path]:
    candidates = [
        Path("link_health_report.csv"),
        Path("reports") / "link_health_report.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def resolve_link_meta(
    town: str,
    original_url: str,
    field_name: str,
    lookup: Dict[Tuple[str, str], LinkMeta],
) -> LinkMeta:
    key = (town.lower(), field_name)
    meta = lookup.get(key)
    if meta is None and original_url:
        meta = lookup.get(("url", normalize_url(original_url)))
    if meta is None:
        return LinkMeta(
            validation_status="",
            status_code=None,
            final_url=None,
            soft404=False,
            checked_at_raw=None,
        )
    return meta


def compute_internal_status(original_url: str, meta: LinkMeta) -> str:
    if not original_url:
        return "unavailable"

    normalized_validation = normalize_text(meta.validation_status).lower()
    mapped = VALIDATION_TO_INTERNAL_STATUS.get(normalized_validation)
    if mapped:
        return mapped

    if meta.soft404:
        return "suspicious"
    if meta.status_code is not None and meta.status_code >= 400:
        return "suspicious"
    if meta.final_url and normalize_url(meta.final_url) != normalize_url(original_url):
        if meta.status_code is None or meta.status_code < 400:
            return "redirected"
        return "suspicious"
    if meta.status_code is not None and meta.status_code < 400:
        return "verified"

    return "unverified"


def compute_label(original_url: str, meta: LinkMeta) -> str:
    internal_status = compute_internal_status(original_url, meta)
    return INTERNAL_TO_LABEL[internal_status]


def build_export_rows(
    records: Iterable[Dict[str, Any]],
    lookup: Dict[Tuple[str, str], LinkMeta],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for record in records:
        town = normalize_text(record.get("Town"))
        website = normalize_text(record.get("Town Website"))
        employment_url = normalize_text(record.get("Employment Page URL"))
        application_url = normalize_text(record.get("Application Form URL"))
        platform = normalize_text(record.get("ATS or Platform (if known)"))

        emp_meta = resolve_link_meta(town, employment_url, "employment_page", lookup)
        app_meta = resolve_link_meta(town, application_url, "application_form", lookup)

        row = {
            "Town": town,
            "Town Website": website,
            "Employment Page URL": employment_url,
            "Application Form URL": application_url,
            "ATS or Platform (if known)": platform,
            "Employment Status": compute_label(employment_url, emp_meta),
            "Application Status": compute_label(application_url, app_meta),
            "Last Checked (employment)": format_human_date(emp_meta.checked_at_raw),
            "Last Checked (application)": format_human_date(app_meta.checked_at_raw),
        }
        rows.append(row)

    rows.sort(key=lambda row: row["Town"].lower())
    return rows


def row_is_verified(row: Dict[str, str], exclude_check_link: bool) -> bool:
    blocked = {"Unavailable"}
    if exclude_check_link:
        blocked.add("Check link")

    # Employment link is required for verified export.
    if row["Employment Status"] in blocked:
        return False

    # Application status is only enforced when an application URL exists.
    has_application_url = bool(normalize_text(row["Application Form URL"]))
    if has_application_url and row["Application Status"] in blocked:
        return False

    return True


def write_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            output_row = {column: normalize_text(row.get(column, "")) for column in OUTPUT_COLUMNS}
            writer.writerow(output_row)


def ensure_list_of_dicts(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be an array of objects.")
    cleaned: List[Dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            cleaned.append(item)
    return cleaned


def main() -> int:
    args = parse_args()
    input_json = Path(args.input_json)
    output_dir = Path(args.output_dir)

    if not input_json.exists():
        raise FileNotFoundError(f"Input JSON not found: {input_json}")

    link_health_path: Optional[Path]
    if args.link_health:
        candidate = Path(args.link_health)
        link_health_path = candidate if candidate.exists() else None
    else:
        link_health_path = auto_detect_link_health()

    with input_json.open("r", encoding="utf-8") as handle:
        records = ensure_list_of_dicts(json.load(handle))

    link_lookup: Dict[Tuple[str, str], LinkMeta] = {}
    if link_health_path:
        link_lookup = read_link_health(link_health_path)

    full_rows = build_export_rows(records, link_lookup)
    verified_rows = [
        row for row in full_rows if row_is_verified(row, exclude_check_link=not args.include_check_link)
    ]

    full_path = output_dir / "ct_municipal_jobs_full.csv"
    verified_path = output_dir / "ct_municipal_jobs_verified.csv"
    write_csv(full_path, full_rows)
    write_csv(verified_path, verified_rows)

    print(f"Created: {full_path}")
    print(f"Created: {verified_path}")
    print(f"Rows (full): {len(full_rows)}")
    print(f"Rows (verified): {len(verified_rows)}")
    print(f"Link health source: {link_health_path if link_health_path else 'none (status inferred from URLs only)'}")
    print(
        "Verified filter: exclude Employment Status == 'Unavailable'"
        + (" and 'Check link'" if not args.include_check_link else "")
        + "; for Application Status, apply the same exclusion only when Application Form URL is present."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
