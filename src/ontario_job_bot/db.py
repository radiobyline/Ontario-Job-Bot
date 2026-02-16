from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .models import ResolutionResult
from .utils import json_dumps, utc_now_iso, url_hash


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS resolution_cache (
            seed_hash TEXT PRIMARY KEY,
            seed_url TEXT NOT NULL,
            canonical_jobs_url TEXT NOT NULL,
            jobs_source_type TEXT NOT NULL,
            adapter TEXT NOT NULL,
            confidence REAL NOT NULL,
            discovered_via TEXT NOT NULL,
            notes TEXT NOT NULL,
            manual_review INTEGER NOT NULL DEFAULT 0,
            checked_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS board (
            canonical_jobs_url TEXT PRIMARY KEY,
            jobs_source_type TEXT NOT NULL,
            adapter TEXT NOT NULL,
            last_scraped_at TEXT,
            last_status TEXT
        );

        CREATE TABLE IF NOT EXISTS org_board (
            org_id TEXT NOT NULL,
            canonical_jobs_url TEXT NOT NULL,
            PRIMARY KEY (org_id, canonical_jobs_url)
        );

        CREATE TABLE IF NOT EXISTS posting (
            posting_uid TEXT PRIMARY KEY,
            board_url TEXT NOT NULL,
            external_id TEXT NOT NULL,
            title TEXT NOT NULL,
            posting_url TEXT NOT NULL,
            location TEXT,
            posted_date TEXT,
            summary TEXT,
            content_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_posting_board ON posting(board_url);
        CREATE INDEX IF NOT EXISTS idx_posting_first_seen ON posting(first_seen_at);

        CREATE TABLE IF NOT EXISTS posting_org (
            posting_uid TEXT NOT NULL,
            org_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            PRIMARY KEY (posting_uid, org_id, reason)
        );

        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            ok INTEGER,
            stats_json TEXT
        );
        """
    )
    conn.commit()


def start_run(conn: sqlite3.Connection, run_type: str) -> int:
    cur = conn.execute(
        "INSERT INTO run_history (run_type, started_at, ok) VALUES (?, ?, NULL)",
        (run_type, utc_now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, ok: bool, stats: dict) -> None:
    conn.execute(
        "UPDATE run_history SET finished_at = ?, ok = ?, stats_json = ? WHERE id = ?",
        (utc_now_iso(), 1 if ok else 0, json_dumps(stats), run_id),
    )
    conn.commit()


def get_cached_resolution(conn: sqlite3.Connection, seed_url: str) -> ResolutionResult | None:
    seed_h = url_hash(seed_url)
    row = conn.execute(
        """
        SELECT seed_url, canonical_jobs_url, jobs_source_type, adapter,
               confidence, discovered_via, notes, manual_review, expires_at
        FROM resolution_cache
        WHERE seed_hash = ?
        """,
        (seed_h,),
    ).fetchone()
    if not row:
        return None

    expires = datetime.fromisoformat(row["expires_at"])
    if expires < datetime.now(timezone.utc):
        return None

    return ResolutionResult(
        seed_url=row["seed_url"],
        canonical_jobs_url=row["canonical_jobs_url"],
        jobs_source_type=row["jobs_source_type"],
        adapter=row["adapter"],
        confidence=float(row["confidence"]),
        discovered_via=row["discovered_via"],
        notes=row["notes"],
        manual_review=bool(row["manual_review"]),
    )


def cache_resolution(
    conn: sqlite3.Connection,
    result: ResolutionResult,
    ttl_days: int,
) -> None:
    checked_at = datetime.now(timezone.utc)
    expires_at = checked_at + timedelta(days=ttl_days)
    conn.execute(
        """
        INSERT INTO resolution_cache (
            seed_hash, seed_url, canonical_jobs_url, jobs_source_type, adapter,
            confidence, discovered_via, notes, manual_review, checked_at, expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(seed_hash) DO UPDATE SET
            seed_url = excluded.seed_url,
            canonical_jobs_url = excluded.canonical_jobs_url,
            jobs_source_type = excluded.jobs_source_type,
            adapter = excluded.adapter,
            confidence = excluded.confidence,
            discovered_via = excluded.discovered_via,
            notes = excluded.notes,
            manual_review = excluded.manual_review,
            checked_at = excluded.checked_at,
            expires_at = excluded.expires_at
        """,
        (
            url_hash(result.seed_url),
            result.seed_url,
            result.canonical_jobs_url,
            result.jobs_source_type,
            result.adapter,
            result.confidence,
            result.discovered_via,
            result.notes,
            1 if result.manual_review else 0,
            checked_at.replace(microsecond=0).isoformat(),
            expires_at.replace(microsecond=0).isoformat(),
        ),
    )
    conn.commit()


def upsert_board(conn: sqlite3.Connection, canonical_jobs_url: str, jobs_source_type: str, adapter: str) -> None:
    conn.execute(
        """
        INSERT INTO board (canonical_jobs_url, jobs_source_type, adapter)
        VALUES (?, ?, ?)
        ON CONFLICT(canonical_jobs_url) DO UPDATE SET
            jobs_source_type = excluded.jobs_source_type,
            adapter = excluded.adapter
        """,
        (canonical_jobs_url, jobs_source_type, adapter),
    )


def map_org_board(conn: sqlite3.Connection, org_id: str, canonical_jobs_url: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO org_board (org_id, canonical_jobs_url)
        VALUES (?, ?)
        """,
        (org_id, canonical_jobs_url),
    )


def update_board_scrape_status(
    conn: sqlite3.Connection,
    canonical_jobs_url: str,
    status: str,
) -> None:
    conn.execute(
        """
        UPDATE board
        SET last_scraped_at = ?, last_status = ?
        WHERE canonical_jobs_url = ?
        """,
        (utc_now_iso(), status, canonical_jobs_url),
    )


def upsert_postings(
    conn: sqlite3.Connection,
    board_url: str,
    postings: Iterable[dict],
) -> list[sqlite3.Row]:
    now = utc_now_iso()
    seen_uids: set[str] = set()
    new_uids: list[str] = []

    for p in postings:
        uid = p["posting_uid"]
        seen_uids.add(uid)
        existing = conn.execute(
            "SELECT posting_uid FROM posting WHERE posting_uid = ?",
            (uid,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE posting
                SET board_url = ?,
                    external_id = ?,
                    title = ?,
                    posting_url = ?,
                    location = ?,
                    posted_date = ?,
                    summary = ?,
                    content_hash = ?,
                    last_seen_at = ?,
                    is_active = 1
                WHERE posting_uid = ?
                """,
                (
                    board_url,
                    p["external_id"],
                    p["title"],
                    p["posting_url"],
                    p.get("location", ""),
                    p.get("posted_date", ""),
                    p.get("summary", ""),
                    p["content_hash"],
                    now,
                    uid,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO posting (
                    posting_uid, board_url, external_id, title, posting_url,
                    location, posted_date, summary, content_hash,
                    first_seen_at, last_seen_at, is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    uid,
                    board_url,
                    p["external_id"],
                    p["title"],
                    p["posting_url"],
                    p.get("location", ""),
                    p.get("posted_date", ""),
                    p.get("summary", ""),
                    p["content_hash"],
                    now,
                    now,
                ),
            )
            new_uids.append(uid)

    if seen_uids:
        placeholders = ",".join("?" for _ in seen_uids)
        conn.execute(
            f"""
            UPDATE posting
            SET is_active = 0
            WHERE board_url = ?
              AND posting_uid NOT IN ({placeholders})
            """,
            (board_url, *seen_uids),
        )
    else:
        conn.execute(
            "UPDATE posting SET is_active = 0 WHERE board_url = ?",
            (board_url,),
        )

    conn.commit()
    if not new_uids:
        return []

    placeholders = ",".join("?" for _ in new_uids)
    return conn.execute(
        f"SELECT * FROM posting WHERE posting_uid IN ({placeholders})",
        tuple(new_uids),
    ).fetchall()


def replace_posting_org_links(conn: sqlite3.Connection, posting_uid: str, org_links: list[tuple[str, str]]) -> None:
    conn.execute("DELETE FROM posting_org WHERE posting_uid = ?", (posting_uid,))
    for org_id, reason in org_links:
        conn.execute(
            "INSERT OR IGNORE INTO posting_org (posting_uid, org_id, reason) VALUES (?, ?, ?)",
            (posting_uid, org_id, reason),
        )
    conn.commit()


def fetch_all_postings_for_sheet(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            p.posting_uid,
            p.first_seen_at,
            p.last_seen_at,
            p.board_url,
            p.title,
            p.posting_url,
            p.location,
            p.posted_date,
            p.is_active,
            b.jobs_source_type,
            b.adapter,
            GROUP_CONCAT(po.org_id, '|') AS org_ids
        FROM posting p
        LEFT JOIN board b ON b.canonical_jobs_url = p.board_url
        LEFT JOIN posting_org po ON po.posting_uid = p.posting_uid
        GROUP BY p.posting_uid
        ORDER BY p.first_seen_at DESC
        """
    ).fetchall()


def fetch_postings_with_orgs(conn: sqlite3.Connection, posting_uids: list[str]) -> list[sqlite3.Row]:
    if not posting_uids:
        return []
    placeholders = ",".join("?" for _ in posting_uids)
    return conn.execute(
        f"""
        SELECT
            p.posting_uid,
            p.first_seen_at,
            p.last_seen_at,
            p.board_url,
            p.title,
            p.posting_url,
            p.location,
            p.posted_date,
            p.summary,
            b.jobs_source_type,
            b.adapter,
            GROUP_CONCAT(po.org_id, '|') AS org_ids
        FROM posting p
        LEFT JOIN board b ON b.canonical_jobs_url = p.board_url
        LEFT JOIN posting_org po ON po.posting_uid = p.posting_uid
        WHERE p.posting_uid IN ({placeholders})
        GROUP BY p.posting_uid
        ORDER BY p.first_seen_at DESC
        """,
        tuple(posting_uids),
    ).fetchall()


def fetch_org_names(conn: sqlite3.Connection, org_ids: list[str]) -> dict[str, str]:
    if not org_ids:
        return {}
    # org names come from CSV runtime data, so this helper remains optional.
    return {oid: oid for oid in org_ids}


def fetch_last_monitor_finished_at(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT finished_at
        FROM run_history
        WHERE run_type = 'monitor' AND ok = 1 AND finished_at IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return row["finished_at"] if row else None


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict]:
    return [dict(row) for row in rows]
