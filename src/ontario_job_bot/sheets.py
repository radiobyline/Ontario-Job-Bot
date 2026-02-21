from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from .config import Settings
from .utils import normalize_text, normalize_url


SHEET_COLUMNS = [
    "posting_uid",
    "first_seen_at",
    "last_seen_at",
    "is_active",
    "org_ids",
    "org_names",
    "board_url",
    "jobs_source_type",
    "adapter",
    "title",
    "posting_url",
    "location",
    "posting_date",
    "closing_date",
    "status",
    "applied_date",
    "notes",
]

MANUAL_COLUMNS = {"status", "applied_date", "notes"}
FULL_ORGS_COLUMNS = ("org_name", "org_type", "homepage_url", "jobs_url")
ORG_MATCH_COLUMNS = ("org_id", "org_name", "homepage_url")


def _normalize_header(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    aliases = {
        "organization_id": "org_id",
        "organization_name": "org_name",
        "organization_type": "org_type",
        "organization_url": "homepage_url",
        "website_url": "homepage_url",
        "website": "homepage_url",
        "home_page_url": "homepage_url",
        "job_url": "jobs_url",
        "job_urls": "jobs_url",
        "jobs_careers_url": "jobs_url",
        "careers_url": "jobs_url",
        "job_board_url": "jobs_url",
        "employment_url": "jobs_url",
        "municipality": "org_name",
        "organization": "org_name",
        "type": "org_type",
    }
    return aliases.get(text, text)


def _row_values_to_dict(headers: list[str], values: list[str]) -> dict[str, str]:
    row: dict[str, str] = {}
    for idx, header in enumerate(headers):
        row[header] = values[idx].strip() if idx < len(values) else ""
    return row


def _worksheet_matches_orgs(headers: list[str]) -> bool:
    normalized = {_normalize_header(h) for h in headers if h}
    return all(col in normalized for col in FULL_ORGS_COLUMNS)


def _worksheet_supports_org_updates(headers: list[str]) -> bool:
    normalized = {_normalize_header(h) for h in headers if h}
    return "jobs_url" in normalized and any(col in normalized for col in ORG_MATCH_COLUMNS)


def _worksheet_mode(headers: list[str]) -> str:
    if _worksheet_matches_orgs(headers):
        return "full"
    if _worksheet_supports_org_updates(headers):
        return "delta"
    return ""


def _select_orgs_worksheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str,
) -> tuple[gspread.Worksheet, str]:
    if worksheet_name:
        worksheet = spreadsheet.worksheet(worksheet_name)
        headers = worksheet.row_values(1)
        mode = _worksheet_mode(headers)
        if not mode:
            raise ValueError(
                f"Worksheet '{worksheet_name}' does not contain a usable org URL layout."
            )
        return worksheet, mode

    delta_match: gspread.Worksheet | None = None
    for worksheet in spreadsheet.worksheets():
        headers = worksheet.row_values(1)
        mode = _worksheet_mode(headers)
        if mode == "full":
            return worksheet, "full"
        if mode == "delta" and delta_match is None:
            delta_match = worksheet

    if delta_match is not None:
        return delta_match, "delta"

    raise ValueError(
        "No worksheet found with org URL columns. Needed either full org columns "
        f"({', '.join(FULL_ORGS_COLUMNS)}) or update columns including jobs_url and one of "
        f"{', '.join(ORG_MATCH_COLUMNS)}."
    )


def _client_from_settings(settings: Settings) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if settings.google_service_account_json:
        info = json.loads(settings.google_service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    else:
        path = Path(settings.google_service_account_json_path)
        creds = Credentials.from_service_account_file(path, scopes=scopes)

    return gspread.authorize(creds)


def _row_dict(headers: list[str], values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for i, h in enumerate(headers):
        result[h] = values[i] if i < len(values) else ""
    return result


def _normalized_headers(raw_headers: list[str]) -> list[str]:
    normalized_headers: list[str] = []
    counts: dict[str, int] = {}
    for header in raw_headers:
        base = _normalize_header(header)
        if not base:
            base = "column"
        count = counts.get(base, 0) + 1
        counts[base] = count
        normalized_headers.append(base if count == 1 else f"{base}_{count}")
    return normalized_headers


def _match_key(value: str) -> str:
    return normalize_text(value)


def _load_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    return headers, rows


def _write_csv_rows(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def upsert_postings_sheet(settings: Settings, posting_rows: list[dict]) -> bool:
    if not settings.sheets_enabled:
        return False

    client = _client_from_settings(settings)
    spreadsheet = client.open_by_key(settings.google_sheets_spreadsheet_id)
    try:
        worksheet = spreadsheet.worksheet(settings.google_sheets_worksheet)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=settings.google_sheets_worksheet, rows=2000, cols=24)

    existing_values = worksheet.get_all_values()
    existing_headers = existing_values[0] if existing_values else SHEET_COLUMNS
    existing_map: dict[str, dict[str, str]] = {}

    for row in existing_values[1:]:
        record = _row_dict(existing_headers, row)
        uid = record.get("posting_uid", "")
        if uid:
            existing_map[uid] = record

    values_out: list[list[str]] = [SHEET_COLUMNS]

    for row in posting_rows:
        uid = str(row.get("posting_uid", ""))
        prev = existing_map.get(uid, {})

        current = {
            "posting_uid": uid,
            "first_seen_at": str(row.get("first_seen_at", "")),
            "last_seen_at": str(row.get("last_seen_at", "")),
            "is_active": str(row.get("is_active", "")),
            "org_ids": str(row.get("org_ids", "")),
            "org_names": str(row.get("org_names", "")),
            "board_url": str(row.get("board_url", "")),
            "jobs_source_type": str(row.get("jobs_source_type", "")),
            "adapter": str(row.get("adapter", "")),
            "title": str(row.get("title", "")),
            "posting_url": str(row.get("posting_url", "")),
            "location": str(row.get("location", "")),
            "posting_date": str(row.get("posting_date") or row.get("posted_date", "")),
            "closing_date": str(row.get("closing_date", "")),
            "status": "",
            "applied_date": "",
            "notes": "",
        }

        for manual_col in MANUAL_COLUMNS:
            if prev.get(manual_col):
                current[manual_col] = prev.get(manual_col, "")

        values_out.append([current[col] for col in SHEET_COLUMNS])

    worksheet.clear()
    worksheet.update("A1", values_out, value_input_option="RAW")
    return True


def export_orgs_csv_from_sheet(
    settings: Settings,
    output_csv: Path,
    worksheet_name: str = "",
) -> dict[str, Any]:
    if not settings.sheets_enabled:
        raise ValueError("Google Sheets is not configured.")

    client = _client_from_settings(settings)
    spreadsheet_id = settings.google_orgs_spreadsheet_id or settings.google_sheets_spreadsheet_id
    if not spreadsheet_id:
        raise ValueError("GOOGLE_ORGS_SPREADSHEET_ID or GOOGLE_SHEETS_SPREADSHEET_ID must be set.")

    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet, mode = _select_orgs_worksheet(spreadsheet, worksheet_name or settings.google_orgs_worksheet)

    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"Worksheet '{worksheet.title}' is empty.")

    raw_headers = [h.strip() for h in values[0]]
    normalized_headers = _normalized_headers(raw_headers)

    rows: list[dict[str, str]] = []
    for record in values[1:]:
        item = _row_values_to_dict(normalized_headers, record)
        if not any(item.values()):
            continue
        rows.append(item)

    if mode == "full":
        _write_csv_rows(output_csv, normalized_headers, rows)
        return {
            "spreadsheet_id": spreadsheet_id,
            "worksheet": worksheet.title,
            "mode": "full",
            "row_count": len(rows),
            "output_csv": str(output_csv),
        }

    base_headers, base_rows = _load_csv_rows(output_csv)
    if not base_rows:
        raise ValueError(
            f"Worksheet '{worksheet.title}' has only partial columns and baseline CSV {output_csv} is missing."
        )

    if "jobs_url" not in base_headers:
        base_headers.append("jobs_url")

    by_org_id: dict[str, dict[str, str]] = {}
    by_org_name: dict[str, dict[str, str]] = {}
    by_homepage: dict[str, dict[str, str]] = {}
    for row in base_rows:
        org_id = (row.get("org_id") or "").strip()
        org_name = _match_key(row.get("org_name") or "")
        homepage = normalize_url(row.get("homepage_url") or "")
        if org_id:
            by_org_id[org_id] = row
        if org_name:
            by_org_name[org_name] = row
        if homepage:
            by_homepage[homepage] = row

    updated = 0
    unmatched = 0
    for patch in rows:
        jobs_url = (patch.get("jobs_url") or "").strip()
        if not jobs_url:
            continue

        target = None
        org_id = (patch.get("org_id") or "").strip()
        if org_id:
            target = by_org_id.get(org_id)
        if target is None:
            org_name = _match_key(patch.get("org_name") or "")
            if org_name:
                target = by_org_name.get(org_name)
        if target is None:
            homepage = normalize_url(patch.get("homepage_url") or "")
            if homepage:
                target = by_homepage.get(homepage)

        if target is None:
            unmatched += 1
            continue

        if (target.get("jobs_url") or "").strip() != jobs_url:
            target["jobs_url"] = jobs_url
            updated += 1

    _write_csv_rows(output_csv, base_headers, base_rows)

    return {
        "spreadsheet_id": spreadsheet_id,
        "worksheet": worksheet.title,
        "mode": "delta",
        "patch_rows": len(rows),
        "jobs_url_updates": updated,
        "unmatched_rows": unmatched,
        "row_count": len(base_rows),
        "output_csv": str(output_csv),
    }
