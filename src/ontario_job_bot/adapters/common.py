from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..title_normalize_and_validate import (
    analyze_listing_signals,
    extract_title_hierarchy_from_detail,
    is_anchor_job_title_candidate,
    normalize_job_title,
)
from ..utils import normalize_url, stable_hash


SOCIAL_DOMAINS = {
    "twitter.com",
    "x.com",
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "youtube.com",
    "tiktok.com",
    "pinterest.com",
}

BLOCKED_URL_TOKENS = (
    "intent/tweet",
    "twitter.com/intent",
    "facebook.com/sharer",
    "facebook.com/share",
    "linkedin.com/sharing",
    "mailto:",
    "tel:",
    "javascript:",
)

NAV_CLASS_TOKENS = (
    "nav",
    "menu",
    "footer",
    "header",
    "social",
    "breadcrumb",
    "site-map",
)

DATE_LITERAL_PATTERN = (
    r"(?:\b\d{4}-\d{2}-\d{2}\b"
    r"|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b"
    r"|\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?"
    r"|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"[\s_-]*\d{4}\b"
    r"|\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?"
    r"|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{1,2}(?:,\s*\d{4})?\b)"
)
DATE_LITERAL_RE = re.compile(DATE_LITERAL_PATTERN, flags=re.IGNORECASE)
POSTING_DATE_RE = re.compile(
    rf"(?:posted|posting date|date posted|posted on|open date|date opened)[^\n\r]{{0,80}}?({DATE_LITERAL_PATTERN})",
    flags=re.IGNORECASE,
)
CLOSING_DATE_RE = re.compile(
    rf"(?:closing|close date|applications? close|applications? due|deadline|apply by|closing date)[^\n\r]{{0,80}}?({DATE_LITERAL_PATTERN})",
    flags=re.IGNORECASE,
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _posting_external_id(title: str, posting_url: str) -> str:
    return stable_hash(f"{title}|{posting_url}")[:20]


def normalize_date(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""
    try:
        dt = date_parser.parse(text, fuzzy=False, dayfirst=False)
        if dt.year < 1990 or dt.year > 2100:
            return ""
        return dt.date().isoformat()
    except Exception:
        return ""


def extract_dates_from_text(text: str) -> tuple[str, str]:
    cleaned = _clean(text)
    if not cleaned:
        return "", ""

    posting_date = ""
    closing_date = ""

    posting_match = POSTING_DATE_RE.search(cleaned)
    if posting_match:
        posting_date = normalize_date(posting_match.group(1))

    closing_match = CLOSING_DATE_RE.search(cleaned)
    if closing_match:
        closing_date = normalize_date(closing_match.group(1))

    if not posting_date:
        first_literal = DATE_LITERAL_RE.search(cleaned)
        if first_literal:
            posting_date = normalize_date(first_literal.group(0))

    return posting_date, closing_date


def _looks_like_social_host(host: str) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in SOCIAL_DOMAINS)


def is_blocked_posting_url(url: str) -> bool:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return True

    host = (parsed.hostname or "").lower()
    if not host or _looks_like_social_host(host):
        return True

    full = f"{host}{parsed.path}?{parsed.query}".lower()
    if any(token in full for token in BLOCKED_URL_TOKENS):
        return True

    return False


def _path_slug(url: str) -> str:
    parsed = urlparse(url)
    path_bits = [p for p in parsed.path.split("/") if p]
    if not path_bits:
        return ""

    slug = unquote(path_bits[-1])
    slug = re.sub(r"\.(pdf|html?|php|aspx?)$", "", slug, flags=re.IGNORECASE)
    slug = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", slug)
    slug = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", slug)
    slug = slug.replace("+", " ")
    slug = re.sub(r"[_\-]+", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip(" -_")
    return slug


def derive_title_from_url(url: str) -> str:
    slug = _path_slug(url)
    if not slug:
        return ""
    words = [w for w in slug.split() if w]
    if not words:
        return ""

    normalized_words = []
    for word in words:
        if word.isupper() and len(word) <= 5:
            normalized_words.append(word)
        elif len(word) <= 4 and word.isalpha() and word.upper() in {"HR", "IT", "CEO", "CFO", "COO"}:
            normalized_words.append(word.upper())
        else:
            normalized_words.append(word.capitalize())

    return normalize_job_title(_clean(" ".join(normalized_words)))


def _is_navigation_link(anchor) -> bool:
    for idx, node in enumerate([anchor, *list(anchor.parents)[:4]]):
        name = (getattr(node, "name", "") or "").lower()
        if name in {"nav", "header", "footer"}:
            return True

        classes = node.get("class") if hasattr(node, "get") else None
        if isinstance(classes, list):
            class_str = " ".join(str(c) for c in classes)
        else:
            class_str = str(classes or "")
        id_str = str(node.get("id") or "") if hasattr(node, "get") else ""
        attr_text = f"{class_str} {id_str}".lower()

        if any(token in attr_text for token in NAV_CLASS_TOKENS):
            return True
        if idx >= 2 and name == "li" and "menu" in attr_text:
            return True

    return False


def _extract_location_from_jsonld(value: Any) -> str:
    if isinstance(value, str):
        return _clean(value)
    if isinstance(value, dict):
        address = value.get("address")
        if isinstance(address, dict):
            parts = [
                _clean(str(address.get("addressLocality", ""))),
                _clean(str(address.get("addressRegion", ""))),
            ]
            joined = ", ".join(part for part in parts if part)
            if joined:
                return joined
        name = _clean(str(value.get("name") or ""))
        if name:
            return name
    if isinstance(value, list):
        for item in value:
            loc = _extract_location_from_jsonld(item)
            if loc:
                return loc
    return ""


def _extract_identifier(value: Any) -> str:
    if isinstance(value, dict):
        return _clean(str(value.get("value") or value.get("@value") or value.get("name") or ""))
    return _clean(str(value or ""))


def parse_jobposting_jsonld(board_url: str, html: str) -> list[Posting]:
    soup = BeautifulSoup(html, "lxml")
    postings: list[Posting] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.text or "").strip()
        if not raw:
            continue

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue

        items: list[dict[str, Any]] = []
        if isinstance(parsed, dict):
            if isinstance(parsed.get("@graph"), list):
                items.extend(i for i in parsed["@graph"] if isinstance(i, dict))
            else:
                items.append(parsed)
        elif isinstance(parsed, list):
            items.extend(i for i in parsed if isinstance(i, dict))

        for item in items:
            typ = _clean(str(item.get("@type", ""))).lower()
            if "jobposting" not in typ:
                continue

            posting_url = normalize_url(str(item.get("url") or board_url))
            if is_blocked_posting_url(posting_url):
                continue

            title = normalize_job_title(str(item.get("title") or "")) or derive_title_from_url(posting_url)
            if not title:
                continue

            summary = _clean(str(item.get("description") or ""))
            posting_date = normalize_date(str(item.get("datePosted") or ""))
            closing_date = normalize_date(
                str(item.get("validThrough") or item.get("dateValidThrough") or item.get("validUntil") or "")
            )
            if not posting_date or not closing_date:
                inferred_posting, inferred_closing = extract_dates_from_text(summary)
                posting_date = posting_date or inferred_posting
                closing_date = closing_date or inferred_closing

            external_id = _extract_identifier(item.get("identifier"))
            if not external_id:
                external_id = _posting_external_id(title, posting_url)

            postings.append(
                Posting(
                    board_url=normalize_url(board_url),
                    external_id=external_id,
                    title=title,
                    posting_url=posting_url,
                    location=_extract_location_from_jsonld(item.get("jobLocation")),
                    posting_date=posting_date,
                    closing_date=closing_date,
                    summary=summary,
                    raw_text=summary,
                    title_source="jsonld",
                    has_jobposting_schema=True,
                    source_url=normalize_url(board_url),
                )
            )

    return postings


def parse_job_links(board_url: str, html: str) -> list[Posting]:
    soup = BeautifulSoup(html, "lxml")
    postings: list[Posting] = []
    seen_urls: set[str] = set()

    listing_signals = analyze_listing_signals(html)

    for anchor in soup.find_all("a", href=True):
        if _is_navigation_link(anchor):
            continue

        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        target_n = normalize_url(urljoin(board_url, href))
        if not target_n or target_n in seen_urls:
            continue
        if target_n == normalize_url(board_url):
            continue
        if is_blocked_posting_url(target_n):
            continue

        label = _clean(" ".join(anchor.stripped_strings))
        if not label:
            label = _clean(str(anchor.get("title") or anchor.get("aria-label") or ""))

        title = normalize_job_title(label)
        if not title or not is_anchor_job_title_candidate(title):
            title = derive_title_from_url(target_n)

        if not title:
            continue

        parent_text = _clean(anchor.parent.get_text(" ", strip=True)) if anchor.parent else ""
        context = _clean(f"{label} {parent_text}")[:1200]
        posting_date, closing_date = extract_dates_from_text(context)

        seen_urls.add(target_n)
        postings.append(
            Posting(
                board_url=normalize_url(board_url),
                external_id=_posting_external_id(title, target_n),
                title=title,
                posting_url=target_n,
                posting_date=posting_date,
                closing_date=closing_date,
                summary=context,
                raw_text=context,
                title_source="anchor",
                has_jobposting_schema=False,
                listing_signal=listing_signals.is_signal_true,
                source_url=normalize_url(board_url),
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


async def enrich_postings_with_detail_titles(
    board_url: str,
    listing_html: str,
    postings: list[Posting],
    http: AsyncHttpHelper,
    max_html_bytes: int,
) -> list[Posting]:
    if not postings:
        return []

    board_norm = normalize_url(board_url)
    listing_signals = analyze_listing_signals(listing_html)
    semaphore = asyncio.Semaphore(10)
    cache: dict[str, tuple[str, str]] = {}

    async def fetch_detail(url: str) -> tuple[str, str]:
        normalized = normalize_url(url)
        if not normalized:
            return "", ""
        if normalized in cache:
            return cache[normalized]

        async with semaphore:
            html, final_url = await http.fetch_html_lite(normalized, max_bytes=max_html_bytes)
        cache[normalized] = (html, final_url)
        return cache[normalized]

    async def enrich_one(posting: Posting) -> Posting:
        posting.source_url = posting.source_url or board_norm
        posting.listing_signal = posting.listing_signal or listing_signals.is_signal_true

        detail_html = ""
        detail_url = posting.posting_url
        if normalize_url(posting.posting_url) and normalize_url(posting.posting_url) != board_norm:
            detail_html, final_url = await fetch_detail(posting.posting_url)
            if final_url:
                posting.posting_url = normalize_url(final_url)
                detail_url = posting.posting_url
        else:
            detail_html = listing_html
            detail_url = board_norm

        resolution = extract_title_hierarchy_from_detail(
            detail_html=detail_html,
            page_url=detail_url,
            fallback_anchor_title=posting.title,
        )

        if resolution.title:
            posting.title = resolution.title
            posting.title_source = resolution.title_source or posting.title_source

        posting.has_jobposting_schema = posting.has_jobposting_schema or resolution.has_jsonld_jobposting
        merged_text = _clean(f"{posting.raw_text} {resolution.page_text}")
        posting.raw_text = merged_text[:9000]

        if not posting.posting_date or not posting.closing_date:
            inferred_posting, inferred_closing = extract_dates_from_text(merged_text)
            posting.posting_date = posting.posting_date or inferred_posting
            posting.closing_date = posting.closing_date or inferred_closing

        return posting

    enriched = await asyncio.gather(*(enrich_one(p) for p in postings))
    return dedupe_postings(enriched)


def fallback_generic_html(board_url: str, html: str) -> list[Posting]:
    combined = parse_jobposting_jsonld(board_url, html)
    combined.extend(parse_job_links(board_url, html))
    return dedupe_postings(combined)
