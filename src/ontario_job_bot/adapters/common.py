from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from ..models import Posting
from ..utils import normalize_text, normalize_url, stable_hash


JOB_HINT_KEYWORDS = (
    "job",
    "jobs",
    "career",
    "careers",
    "employment",
    "opportun",
    "vacan",
    "position",
    "posting",
    "recruit",
    "apply",
)

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

BLOCKED_TEXT_EXACT = {
    "twitter",
    "facebook",
    "instagram",
    "linkedin",
    "youtube",
    "skip to content",
    "skip to main content",
    "home",
    "menu",
    "search",
    "contact",
}

BLOCKED_TEXT_CONTAINS = (
    "share",
    "privacy",
    "terms",
    "accessibility",
    "cookie",
    "copyright",
    "follow us",
    "all rights reserved",
)

GENERIC_TITLE_TEXT = {
    "apply",
    "apply now",
    "apply today",
    "job posting",
    "details",
    "read more",
    "learn more",
    "click here",
    "view",
    "view details",
    "download",
    "pdf",
    "career",
    "careers",
    "employment",
    "employment opportunities",
    "job opportunities",
    "opportunities",
    "view jobs",
    "view job openings",
    "open positions",
    "current opportunities",
    "apply here",
    "job board",
    "jobs board",
    "full list of jobs",
    "employment and training services",
    "employment training services",
    "careers nogdawindamin",
    "job opportunity",
    "job opportunities",
    "openings",
    "index",
    "index cfm",
}

NAV_CLASS_TOKENS = (
    "nav",
    "menu",
    "footer",
    "header",
    "social",
    "breadcrumb",
    "site-map",
)

HARD_NON_JOB_URL_TOKENS = (
    "/laws/",
    "/regulation/",
    "/permits/",
    "building-permit",
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
TITLE_SPLIT_RE = re.compile(r"\s[-|:]\s")
TITLE_NOISE_SEGMENT_RE = re.compile(
    r"\b(?:recruitment|job posting|job description|closed|posting id|req(?:uisition)?\.?|competition)\b",
    flags=re.IGNORECASE,
)
TITLE_PREFIX_PATTERNS = (
    r"^(?:closed\s*[-:|]\s*)?recruitment\s*[-:|]\s*\d{4,8}\s*[-:|]\s*",
    r"^(?:closed\s*[-:|]\s*)?recruitment\s*[-:|]\s*",
    r"^\d{4,8}\s*[-:|]\s*",
    r"^(?:job\s*(?:posting|description)\s*[-:|]\s*)+",
)
TITLE_SUFFIX_PATTERNS = (
    r"\s*[-:|]?\s*job\s*(?:description|posting|advertisement|ad|profile)\b(?:\s+\d{2,4})?\s*$",
    r"\s*[-:|]?\s*job\s*(?:description|posting|advertisement|ad|profile)\b(?:\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4})\s*$",
    r"\s+\bjob\b\s*$",
    r"\s*[-:|]?\s*recruitment\b(?:\s+\d{4,8})?\s*$",
    r"\s*[-:|]?\s*(?:posting|req(?:uisition)?|competition)\s*(?:id|#)?\s*[a-z0-9-]+\s*$",
    r"\s+\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+\d{1,2}(?:,\s*)?)?\s+\d{4}\s*$",
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
    if any(token in full for token in HARD_NON_JOB_URL_TOKENS):
        return True

    return False


def _path_slug(url: str) -> str:
    parsed = urlparse(url)
    path_bits = [p for p in parsed.path.split("/") if p]
    if not path_bits:
        return ""
    slug = unquote(path_bits[-1])
    slug = re.sub(r"\.(pdf|html?|php|aspx?)$", "", slug, flags=re.IGNORECASE)
    slug = slug.replace("+", " ")
    slug = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", slug)
    slug = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", slug)
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


def _title_segment_score(segment: str) -> int:
    low = normalize_text(segment)
    score = 0
    if len(low.split()) >= 2:
        score += 2
    if any(word in low for word in ROLE_HINT_WORDS):
        score += 4
    if any(word in low for word in ("job", "position", "opportun", "career", "employment")):
        score += 2
    if TITLE_NOISE_SEGMENT_RE.search(low):
        score -= 4
    if re.search(r"\d{4,}", low):
        score -= 1
    return score


def _strip_trailing_title_date(value: str) -> str:
    return re.sub(
        r"\s+\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+\d{1,2}(?:,\s*)?)?\s+\d{4}\s*$",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip(" -|:")


def normalize_job_title(value: str) -> str:
    title = _clean(value)
    if not title:
        return ""

    title = title.replace("—", " - ").replace("–", " - ")
    title = re.sub(r"\s+", " ", title).strip(" -|:")

    for _ in range(3):
        previous = title
        for pattern in TITLE_PREFIX_PATTERNS:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip(" -|:")
        if title == previous:
            break

    for pattern in TITLE_SUFFIX_PATTERNS:
        title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip(" -|:")

    parts = [part.strip(" -|:") for part in TITLE_SPLIT_RE.split(title) if part.strip(" -|:")]
    if len(parts) > 1:
        filtered: list[str] = []
        for part in parts:
            if re.fullmatch(r"\d{4,8}", part):
                continue
            if TITLE_NOISE_SEGMENT_RE.search(part):
                continue
            cleaned_part = _strip_trailing_title_date(part)
            if not cleaned_part:
                continue
            filtered.append(cleaned_part)
        if filtered:
            part_scores = [(part, _title_segment_score(part)) for part in filtered]
            if len(part_scores) >= 2 and part_scores[0][1] >= 3 and part_scores[1][1] >= 1 and len(part_scores[1][0].split()) >= 2:
                title = f"{part_scores[0][0]} - {part_scores[1][0]}"
            else:
                scored = sorted(part_scores, key=lambda item: (item[1], len(item[0])), reverse=True)
                title = scored[0][0] if scored[0][1] >= 1 else " - ".join(part for part, _ in part_scores)

    title = _strip_trailing_title_date(title)

    title = _clean(title.strip(" -|:"))
    return title


def is_noise_title(title: str) -> bool:
    cleaned = normalize_text(title)
    if not cleaned:
        return True
    if len(re.findall(r"[a-z]", cleaned)) < 3:
        return True
    if cleaned in BLOCKED_TEXT_EXACT:
        return True
    if cleaned in GENERIC_TITLE_TEXT:
        return True
    if any(token in cleaned for token in BLOCKED_TEXT_CONTAINS):
        return True
    if cleaned.startswith("closed") and any(word in cleaned for word in ("position", "job", "recruitment")):
        return True
    if any(term in cleaned for term in NON_JOB_TITLE_TERMS) and not looks_like_role_title(cleaned):
        return True
    if any(term in cleaned for term in ("employment", "career", "job board", "view jobs")) and not looks_like_role_title(
        cleaned
    ):
        return True
    if re.search(r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", cleaned) and re.search(
        r"\b\d{1,2}:\d{2}\b", cleaned
    ):
        return True
    if len(cleaned) < 4:
        return True
    return False


ROLE_HINT_WORDS = (
    "officer",
    "manager",
    "coordinator",
    "director",
    "administrator",
    "supervisor",
    "clerk",
    "analyst",
    "specialist",
    "worker",
    "technician",
    "assistant",
    "engineer",
    "planner",
    "facilitator",
    "consultant",
    "chief",
    "advisor",
    "educator",
    "instructor",
    "operator",
    "lead",
    "trustee",
    "labourer",
    "lifeguard",
    "driver",
    "navigator",
    "teacher",
    "counsellor",
    "counselor",
    "co-op",
    "intern",
    "student",
    "guard",
    "cook",
    "janitor",
    "caretaker",
    "attendant",
    "worker",
)

NON_JOB_TITLE_TERMS = (
    "permit form",
    "application form",
    "statute",
    "regulation",
    "terms and conditions",
    "privacy policy",
    "closed and filled position",
    "closed and not filled position",
)


def looks_like_role_title(title: str) -> bool:
    low = normalize_text(title)
    if not low:
        return False
    return any(word in low for word in ROLE_HINT_WORDS)


def looks_like_job_title(title: str) -> bool:
    if is_noise_title(title):
        return False
    return _has_job_signal(title) or looks_like_role_title(title)


def _has_job_signal(text: str) -> bool:
    low = normalize_text(text)
    return any(word in low for word in JOB_HINT_KEYWORDS)


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


def _best_title(anchor_text: str, posting_url: str) -> str:
    candidate = normalize_job_title(_clean(anchor_text))
    if not candidate or is_noise_title(candidate):
        candidate = normalize_job_title(derive_title_from_url(posting_url))
    if is_noise_title(candidate):
        return ""
    return candidate


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

            posting_url = normalize_url(str(item.get("url") or board_url))
            if is_blocked_posting_url(posting_url):
                continue

            title = _best_title(_clean(str(item.get("title", ""))), posting_url)
            if not title:
                continue

            location = _extract_location_from_jsonld(item.get("jobLocation"))
            posting_date = normalize_date(str(item.get("datePosted") or ""))
            closing_date = normalize_date(
                str(item.get("validThrough") or item.get("dateValidThrough") or item.get("validUntil") or "")
            )
            summary = _clean(str(item.get("description") or ""))

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
                    location=location,
                    posting_date=posting_date,
                    closing_date=closing_date,
                    summary=summary,
                    raw_text=f"{title} {summary} {location}",
                )
            )

    return postings


def parse_job_links(board_url: str, html: str) -> list[Posting]:
    soup = BeautifulSoup(html, "lxml")
    postings: list[Posting] = []

    seen_urls: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if _is_navigation_link(anchor):
            continue

        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        target = urljoin(board_url, href)
        target_n = normalize_url(target)
        if not target_n or target_n in seen_urls:
            continue
        if target_n == normalize_url(board_url):
            continue
        if is_blocked_posting_url(target_n):
            continue

        label = _clean(" ".join(anchor.stripped_strings))
        if not label:
            label = _clean(str(anchor.get("title") or anchor.get("aria-label") or ""))

        parent_text = _clean(anchor.parent.get_text(" ", strip=True)) if anchor.parent else ""
        context = _clean(f"{label} {parent_text}")[:900]
        doc_link = bool(re.search(r"\.(pdf|docx?)$", target_n, flags=re.IGNORECASE))
        if not _has_job_signal(f"{context} {target_n}"):
            if not doc_link:
                continue
            inferred = derive_title_from_url(target_n)
            if not (_has_job_signal(inferred) or looks_like_role_title(inferred)):
                continue

        title = _best_title(label, target_n)
        if not title:
            continue
        if not looks_like_job_title(title):
            continue

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
