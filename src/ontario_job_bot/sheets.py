from __future__ import annotations

import json
from pathlib import Path

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
