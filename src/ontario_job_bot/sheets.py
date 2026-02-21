from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from .config import Settings


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
REQUIRED_ORGS_COLUMNS = ("org_id", "org_name", "org_type", "homepage_url", "jobs_url")


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
    }
    return aliases.get(text, text)


def _row_values_to_dict(headers: list[str], values: list[str]) -> dict[str, str]:
    row: dict[str, str] = {}
    for idx, header in enumerate(headers):
        row[header] = values[idx].strip() if idx < len(values) else ""
    return row


def _worksheet_matches_orgs(headers: list[str]) -> bool:
    normalized = {_normalize_header(h) for h in headers if h}
    return all(col in normalized for col in REQUIRED_ORGS_COLUMNS)


def _select_orgs_worksheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str,
) -> gspread.Worksheet:
    if worksheet_name:
        worksheet = spreadsheet.worksheet(worksheet_name)
        headers = worksheet.row_values(1)
        if not _worksheet_matches_orgs(headers):
            raise ValueError(
                f"Worksheet '{worksheet_name}' is missing one or more required columns: {', '.join(REQUIRED_ORGS_COLUMNS)}"
            )
        return worksheet

    for worksheet in spreadsheet.worksheets():
        headers = worksheet.row_values(1)
        if _worksheet_matches_orgs(headers):
            return worksheet

    raise ValueError(
        "No worksheet found with required org columns: "
        + ", ".join(REQUIRED_ORGS_COLUMNS)
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
    worksheet = _select_orgs_worksheet(spreadsheet, worksheet_name or settings.google_orgs_worksheet)

    values = worksheet.get_all_values()
    if not values:
        raise ValueError(f"Worksheet '{worksheet.title}' is empty.")

    raw_headers = [h.strip() for h in values[0]]
    if not _worksheet_matches_orgs(raw_headers):
        raise ValueError(
            f"Worksheet '{worksheet.title}' is missing one or more required columns: {', '.join(REQUIRED_ORGS_COLUMNS)}"
        )

    normalized_headers: list[str] = []
    counts: dict[str, int] = {}
    for header in raw_headers:
        base = _normalize_header(header)
        if not base:
            base = "column"
        count = counts.get(base, 0) + 1
        counts[base] = count
        normalized_headers.append(base if count == 1 else f"{base}_{count}")

    rows: list[dict[str, str]] = []
    for record in values[1:]:
        item = _row_values_to_dict(normalized_headers, record)
        if not any(item.values()):
            continue
        rows.append(item)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=normalized_headers)
        writer.writeheader()
        writer.writerows(rows)

    return {
        "spreadsheet_id": spreadsheet_id,
        "worksheet": worksheet.title,
        "row_count": len(rows),
        "output_csv": str(output_csv),
    }
