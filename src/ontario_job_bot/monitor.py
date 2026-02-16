from __future__ import annotations

import asyncio
import csv
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .adapters.common import is_blocked_posting_url
from .adapters.registry import get_adapter
from .classifiers import classify_url
from .config import Settings
from .db import (
    connect,
    fetch_all_postings_for_sheet,
    fetch_postings_with_orgs,
    finish_run,
    init_db,
    map_org_board,
    replace_posting_org_links,
    rows_to_dicts,
    start_run,
    update_board_scrape_status,
    upsert_board,
    upsert_postings,
)
from .emailer import send_digest_email
from .http_client import AsyncHttpHelper, url_variants
from .sheets import upsert_postings_sheet
from .title_normalize_and_validate import validate_title_and_job_gate
from .utils import normalize_text, normalize_url, stable_hash


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def adapter_from_row(row: dict[str, Any]) -> tuple[str, str]:
    source = (row.get("jobs_source_type") or "").strip().lower()
    adapter = (row.get("adapter") or "").strip().lower()

    if adapter:
        return source or "unknown", adapter

    source_map = {
        "ats_workday": "workday",
        "ats_taleo": "taleo",
        "ats_icims": "icims",
        "ats_neogov": "neogov",
        "ats_utipro": "utipro",
        "ats_adp": "adp",
        "html_list": "html_list",
        "pdf": "pdf",
        "unknown": "generic",
    }
    if source in source_map:
        return source, source_map[source]

    target_url = row.get("canonical_jobs_url") or row.get("jobs_url") or ""
    detected = classify_url(target_url)
    if detected:
        return detected.jobs_source_type, detected.adapter

    return source or "unknown", "generic"


def build_board_map(rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, str], list[dict[str, Any]]]:
    boards: dict[str, dict[str, Any]] = {}
    org_name_map: dict[str, str] = {}
    first_nations: list[dict[str, Any]] = []

    for row in rows:
        org_id = (row.get("org_id") or "").strip()
        org_name = (row.get("org_name") or "").strip()
        org_type = (row.get("org_type") or "").strip().lower()
        if org_id:
            org_name_map[org_id] = org_name

        if org_type == "first_nation":
            first_nations.append(row)

        board_url = normalize_url((row.get("canonical_jobs_url") or row.get("jobs_url") or "").strip())
        if not board_url:
            continue

        source, adapter = adapter_from_row(row)
        data = boards.setdefault(
            board_url,
            {
                "canonical_jobs_url": board_url,
                "jobs_source_type": source,
                "adapter": adapter,
                "owner_org_ids": set(),
            },
        )
        data["owner_org_ids"].add(org_id)

    return boards, org_name_map, first_nations


def build_first_nation_aliases(rows: list[dict[str, Any]]) -> list[tuple[re.Pattern[str], str]]:
    rules: list[tuple[re.Pattern[str], str]] = []
    seen: set[tuple[str, str]] = set()

    for row in rows:
        org_id = (row.get("org_id") or "").strip()
        org_name = (row.get("org_name") or "").strip()
        if not org_id or not org_name:
            continue

        aliases = {normalize_text(org_name)}
        short = normalize_text(org_name.replace("First Nation", "").replace("first nation", ""))
        if short:
            aliases.add(short)

        for alias in aliases:
            alias = alias.strip()
            if len(alias) < 5:
                continue
            key = (alias, org_id)
            if key in seen:
                continue
            seen.add(key)
            pattern = re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
            rules.append((pattern, org_id))

    return rules


async def resolve_working_url(board_url: str, http: AsyncHttpHelper) -> tuple[str, str]:
    for variant in url_variants(board_url):
        result = await http.resolve_redirects(variant)
        if result.ok and result.status_code < 500:
            final = normalize_url(result.final_url)
            if final:
                if final != normalize_url(board_url):
                    return final, f"url_repair:{variant}"
                return final, "canonical_ok"
    return normalize_url(board_url), "url_unresolved"


def posting_to_db_row(posting) -> tuple[dict[str, str] | None, dict[str, Any]]:
    posting_url = normalize_url(str(posting.posting_url or ""))
    if not posting_url or is_blocked_posting_url(posting_url):
        return None, {
            "accepted": False,
            "normalized_title": str(posting.title or ""),
            "cleaned": False,
            "rejection_reason": "blocked posting URL",
            "rejection_type": "blocklist",
        }

    validation = validate_title_and_job_gate(
        candidate_title=str(posting.title or ""),
        posting_url=posting_url,
        source_url=str(posting.source_url or posting.board_url or posting_url),
        title_source=str(posting.title_source or ""),
        listing_text=str(posting.summary or ""),
        detail_text=str(posting.raw_text or ""),
        has_jsonld_jobposting=bool(posting.has_jobposting_schema),
        listing_signal=bool(posting.listing_signal),
    )
    if not validation.accepted:
        return None, {
            "accepted": False,
            "normalized_title": validation.normalized_title,
            "cleaned": validation.cleaned,
            "rejection_reason": validation.rejection_reason,
            "rejection_type": validation.rejection_type,
        }

    title = validation.normalized_title

    posting_uid = stable_hash(f"{posting.board_url}|{posting.external_id}|{posting.posting_url}")[:40]
    content_seed = "|".join(
        [
            title,
            posting_url,
            posting.location,
            posting.posting_date,
            posting.closing_date,
            posting.summary,
        ]
    )
    return {
        "posting_uid": posting_uid,
        "external_id": posting.external_id,
        "title": title[:500],
        "posting_url": posting_url,
        "location": posting.location[:250],
        "posting_date": posting.posting_date[:80],
        "closing_date": posting.closing_date[:80],
        "summary": posting.summary[:3000],
        "content_hash": stable_hash(content_seed),
        "raw_text": posting.raw_text,
    }, {
        "accepted": True,
        "normalized_title": title,
        "cleaned": validation.cleaned,
        "rejection_reason": "",
        "rejection_type": "",
    }


def write_title_rejections_report(rejections: list[dict[str, str]]) -> Path:
    report_dir = Path("reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    date_stamp = datetime.now(timezone.utc).date().isoformat()
    report_path = report_dir / f"title_rejections_{date_stamp}.csv"

    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["org_id", "org_name", "source_url", "candidate_title", "rejection_reason"],
        )
        writer.writeheader()
        for row in rejections:
            writer.writerow(
                {
                    "org_id": row.get("org_id", ""),
                    "org_name": row.get("org_name", ""),
                    "source_url": row.get("source_url", ""),
                    "candidate_title": row.get("candidate_title", ""),
                    "rejection_reason": row.get("rejection_reason", ""),
                }
            )

    return report_path


def posting_org_links(
    owner_org_ids: set[str],
    raw_text: str,
    fn_alias_rules: list[tuple[re.Pattern[str], str]],
) -> list[tuple[str, str]]:
    links: set[tuple[str, str]] = set()
    for org_id in owner_org_ids:
        if org_id:
            links.add((org_id, "owner"))

    text = normalize_text(raw_text)
    if text:
        for pattern, org_id in fn_alias_rules:
            if pattern.search(text):
                links.add((org_id, "mentioned_in_text"))

    return sorted(links)


def render_digest(new_rows: list[dict[str, Any]], org_name_map: dict[str, str]) -> tuple[str, str, str]:
    count = len(new_rows)
    subject = f"Ontario Job Bot Weekly Digest: {count} new posting(s)"

    if not new_rows:
        body = "No new postings were found this run."
        return subject, body, f"<p>{body}</p>"

    lines = [f"Found {count} new posting(s):", ""]
    html_items = []

    for row in new_rows[:200]:
        org_ids = [x for x in str(row.get("org_ids", "")).split("|") if x]
        org_names = [org_name_map.get(oid, oid) for oid in org_ids]
        org_label = ", ".join(sorted(set(org_names))) if org_names else "Unattributed"

        title = str(row.get("title", ""))
        posting_url = str(row.get("posting_url", ""))
        board_url = str(row.get("board_url", ""))
        posting_date = str(row.get("posting_date", ""))
        closing_date = str(row.get("closing_date", ""))
        lines.append(f"- {title}")
        lines.append(f"  Org(s): {org_label}")
        if posting_date:
            lines.append(f"  Posted: {posting_date}")
        if closing_date:
            lines.append(f"  Closing: {closing_date}")
        lines.append(f"  Posting: {posting_url}")
        lines.append(f"  Board: {board_url}")
        lines.append("")

        html_items.append(
            "".join(
                [
                    "<li>",
                    f"<strong>{title}</strong><br>",
                    f"Org(s): {org_label}<br>",
                    f"{('Posted: ' + posting_date + '<br>') if posting_date else ''}",
                    f"{('Closing: ' + closing_date + '<br>') if closing_date else ''}",
                    f"Posting: <a href=\"{posting_url}\">{posting_url}</a><br>",
                    f"Board: <a href=\"{board_url}\">{board_url}</a>",
                    "</li>",
                ]
            )
        )

    body_text = "\n".join(lines)
    body_html = f"<p>Found {count} new posting(s):</p><ul>{''.join(html_items)}</ul>"
    return subject, body_text, body_html


async def run_monitor(
    settings: Settings,
    input_csv: Path,
    max_boards: int | None = None,
) -> dict[str, Any]:
    rows = load_rows(input_csv)
    boards, org_name_map, first_nations = build_board_map(rows)
    board_items = list(boards.items())
    if max_boards is not None:
        board_items = board_items[:max_boards]

    conn = connect(settings.db_path)
    init_db(conn)
    run_id = start_run(conn, "monitor")

    for board_url, board_data in board_items:
        upsert_board(conn, board_url, board_data["jobs_source_type"], board_data["adapter"])
        for org_id in board_data["owner_org_ids"]:
            map_org_board(conn, org_id, board_url)

    fn_alias_rules = build_first_nation_aliases(first_nations)

    helper = AsyncHttpHelper(
        timeout_seconds=settings.request_timeout_seconds,
        max_redirects=settings.max_redirects,
        per_domain_rps=settings.per_domain_rps,
    )

    semaphore = asyncio.Semaphore(min(settings.global_concurrency, 40))

    stats = {
        "boards_total": len(board_items),
        "boards_success": 0,
        "boards_failed": 0,
        "postings_seen": 0,
        "new_postings": 0,
        "email_sent": False,
        "sheet_synced": False,
        "titles_rejected_blocklist_count": 0,
        "titles_rejected_validation_gate_count": 0,
        "titles_cleaned_count": 0,
        "failures": [],
    }
    new_posting_uids: set[str] = set()
    title_rejections: list[dict[str, str]] = []

    async def scrape_one(board_url: str, board_data: dict[str, Any]) -> None:
        nonlocal stats
        async with semaphore:
            working_url, repair_note = await resolve_working_url(board_url, helper)
            adapter = get_adapter(board_data["adapter"])

            try:
                postings = await adapter.scrape(working_url, helper, settings)
            except Exception as exc:
                postings = []
                stats["boards_failed"] += 1
                stats["failures"].append({"board_url": board_url, "error": str(exc)[:300]})
                update_board_scrape_status(conn, board_url, f"error:{str(exc)[:120]}")
                return

            db_payload: list[dict[str, str]] = []
            for posting in postings:
                converted, validation = posting_to_db_row(posting)
                if validation.get("cleaned"):
                    stats["titles_cleaned_count"] += 1
                if converted is None:
                    rejection_type = validation.get("rejection_type", "")
                    if rejection_type == "blocklist":
                        stats["titles_rejected_blocklist_count"] += 1
                    else:
                        stats["titles_rejected_validation_gate_count"] += 1

                    for org_id in board_data["owner_org_ids"]:
                        title_rejections.append(
                            {
                                "org_id": org_id,
                                "org_name": org_name_map.get(org_id, org_id),
                                "source_url": str(posting.source_url or board_url),
                                "candidate_title": str(posting.title or ""),
                                "rejection_reason": str(validation.get("rejection_reason", "rejected")),
                            }
                        )
                    continue
                db_payload.append(converted)
            new_rows = upsert_postings(conn, board_url, db_payload)
            new_uids_local = {row["posting_uid"] for row in new_rows}
            new_posting_uids.update(new_uids_local)

            for item in db_payload:
                raw_text = " ".join([item.get("title", ""), item.get("summary", ""), item.get("raw_text", "")])
                links = posting_org_links(board_data["owner_org_ids"], raw_text, fn_alias_rules)
                replace_posting_org_links(conn, item["posting_uid"], links)

            stats["boards_success"] += 1
            stats["postings_seen"] += len(db_payload)
            status = f"ok:{len(db_payload)}:{repair_note}"
            update_board_scrape_status(conn, board_url, status)

    try:
        await asyncio.gather(*(scrape_one(board_url, board_data) for board_url, board_data in board_items))
        report_path = write_title_rejections_report(title_rejections)
        stats["title_rejections_report"] = str(report_path)

        new_rows = rows_to_dicts(fetch_postings_with_orgs(conn, sorted(new_posting_uids)))
        cleaned_new_rows: list[dict[str, Any]] = []
        for row in new_rows:
            posting_url = normalize_url(str(row.get("posting_url", "")))
            if not posting_url or is_blocked_posting_url(posting_url):
                continue
            row["posting_url"] = posting_url
            cleaned_new_rows.append(row)
        new_rows = cleaned_new_rows
        stats["new_postings"] = len(new_rows)

        if new_rows or settings.send_empty_digest:
            subject, body_text, body_html = render_digest(new_rows, org_name_map)
            stats["email_sent"] = send_digest_email(settings, subject, body_text, body_html)

        sheet_rows_raw = rows_to_dicts(fetch_all_postings_for_sheet(conn))
        sheet_rows: list[dict[str, Any]] = []
        for row in sheet_rows_raw:
            validation = validate_title_and_job_gate(
                candidate_title=str(row.get("title", "")),
                posting_url=str(row.get("posting_url", "")),
                source_url=str(row.get("board_url", "")),
                title_source="sheet_row",
                listing_text=str(row.get("summary", "")),
                detail_text=str(row.get("summary", "")),
                has_jsonld_jobposting=False,
                listing_signal=False,
            )
            if not validation.accepted:
                continue
            posting_url = normalize_url(str(row.get("posting_url", "")))
            if not posting_url or is_blocked_posting_url(posting_url):
                continue
            row["title"] = validation.normalized_title
            row["posting_url"] = posting_url
            org_ids = [x for x in str(row.get("org_ids", "")).split("|") if x]
            row["org_names"] = " | ".join(sorted({org_name_map.get(oid, oid) for oid in org_ids}))
            sheet_rows.append(row)
        stats["sheet_rows_filtered"] = len(sheet_rows_raw) - len(sheet_rows)

        stats["sheet_synced"] = upsert_postings_sheet(settings, sheet_rows)

        finish_run(conn, run_id, True, stats)
        return stats
    except Exception as exc:  # pragma: no cover
        stats["error"] = str(exc)
        finish_run(conn, run_id, False, stats)
        raise
    finally:
        await helper.aclose()
        conn.close()
