from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..models import Posting
from ..utils import normalize_text, normalize_url, stable_hash


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _posting_external_id(title: str, posting_url: str) -> str:
    return stable_hash(f"{title}|{posting_url}")[:20]


def parse_jobposting_jsonld(board_url: str, html: str) -> list[Posting]:
    soup = BeautifulSoup(html, "lxml")
    postings: list[Posting] = []

    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        raw = script.string or script.text or ""
        raw = raw.strip()
        if not raw:
            continue

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue

        items: list[dict[str, Any]] = []
        if isinstance(parsed, dict):
            if "@graph" in parsed and isinstance(parsed["@graph"], list):
                items.extend(i for i in parsed["@graph"] if isinstance(i, dict))
            else:
                items.append(parsed)
        elif isinstance(parsed, list):
            items.extend(i for i in parsed if isinstance(i, dict))

        for item in items:
            typ = str(item.get("@type", "")).lower()
            if "jobposting" not in typ:
                continue

            title = _clean(str(item.get("title", "")))
            if not title:
                continue

            posting_url = normalize_url(str(item.get("url") or board_url))
            location = _clean(str(item.get("jobLocation") or ""))
            posted_date = _clean(str(item.get("datePosted") or ""))
            summary = _clean(str(item.get("description") or ""))
            external_id = _clean(str(item.get("identifier") or ""))
            if not external_id:
                external_id = _posting_external_id(title, posting_url)

            postings.append(
                Posting(
                    board_url=normalize_url(board_url),
                    external_id=external_id,
                    title=title,
                    posting_url=posting_url,
                    location=location,
                    posted_date=posted_date,
                    summary=summary,
                    raw_text=summary,
                )
            )

    return postings


def parse_job_links(board_url: str, html: str) -> list[Posting]:
    soup = BeautifulSoup(html, "lxml")
    postings: list[Posting] = []

    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        text = _clean(" ".join(a.stripped_strings))
        if not href or not text:
            continue

        target = urljoin(board_url, href)
        target_n = normalize_url(target)
        if not target_n or target_n in seen_urls:
            continue

        low = normalize_text(f"{text} {target_n}")
        if not any(k in low for k in ("job", "career", "employment", "apply", "position", "vacan")):
            continue

        if len(text) < 4:
            continue

        seen_urls.add(target_n)
        postings.append(
            Posting(
                board_url=normalize_url(board_url),
                external_id=_posting_external_id(text, target_n),
                title=text,
                posting_url=target_n,
                summary="",
                raw_text=text,
            )
        )

    return postings


def dedupe_postings(postings: list[Posting]) -> list[Posting]:
    deduped: list[Posting] = []
    seen: set[str] = set()
    for p in postings:
        key = f"{p.external_id}|{p.posting_url}|{p.title.lower()}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


def fallback_generic_html(board_url: str, html: str) -> list[Posting]:
    combined = parse_jobposting_jsonld(board_url, html)
    if not combined:
        combined = parse_job_links(board_url, html)
    else:
        combined.extend(parse_job_links(board_url, html))
    return dedupe_postings(combined)
