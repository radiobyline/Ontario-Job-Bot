from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .utils import normalize_text


TITLE_BLOCKLIST_EXACT = {
    "submit a service",
    "submit a request",
    "services",
    "service request",
    "notices",
    "notice",
    "news",
    "events",
    "tenders",
    "procurement",
    "rfp",
    "rft",
    "by laws",
    "bylaws",
    "by laws",
}

NAV_LIKE_SHORT_TERMS = {
    "services",
    "service request",
    "notices",
    "notice",
    "news",
    "events",
    "tenders",
    "procurement",
    "careers",
    "jobs",
    "employment",
    "view postings",
    "view posting",
}

NON_JOB_CATEGORY_TERMS = (
    "services",
    "service",
    "notices",
    "notice",
    "news",
    "events",
    "procurement",
    "tender",
    "rfp",
    "rft",
    "bylaw",
    "by law",
)

JOB_URL_HINTS = (
    "/careers",
    "/career",
    "/jobs",
    "/job",
    "/employment",
    "/opportunit",
    "/recruit",
    "/posting",
    "/vacan",
    "jobid",
    "job-id",
)

JOB_NEAR_TITLE_KEYWORDS = (
    "apply",
    "closing date",
    "salary",
    "position",
    "department",
    "job id",
    "requisition",
    "competition",
)

SITE_NAME_MARKERS = (
    "city of",
    "town of",
    "township of",
    "municipality of",
    "county of",
    "regional municipality",
    "first nation",
    "careers",
    "employment",
)

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
    "intern",
    "student",
)

TITLE_PREFIX_PATTERNS = (
    r"^(?:closed\s*[-:|]\s*)?recruitment\s*[-:|]\s*\d{4,8}\s*[-:|]\s*",
    r"^(?:closed\s*[-:|]\s*)?recruitment\s*[-:|]\s*",
    r"^\d{4,8}\s*[-:|]\s*",
    r"^(?:job\s*(?:posting|description)\s*[-:|]\s*)+",
)

TITLE_SUFFIX_PATTERNS = (
    r"\s*[-:|]?\s*job\s*(?:description|posting|advertisement|ad|profile)\b(?:\s+\d{2,4})?\s*$",
    r"\s*[-:|]?\s*recruitment\b(?:\s+\d{4,8})?\s*$",
    r"\s*[-:|]?\s*(?:posting|req(?:uisition)?|competition)\s*(?:id|#)?\s*[a-z0-9-]+\s*$",
)

GENERIC_TITLE_PATTERNS = (
    re.compile(
        r"^(?:view|see|browse|learn|open|click)\s+(?:current\s+)?(?:job\s+)?(?:posting|postings|opportunities|opportunity|careers?)\b",
        flags=re.IGNORECASE,
    ),
    re.compile(r"\bemployment\s+opportunit(?:y|ies)\b", flags=re.IGNORECASE),
)


@dataclass
class ListingSignals:
    role_like_links_count: int
    has_apply_pattern: bool

    @property
    def is_signal_true(self) -> bool:
        return self.role_like_links_count >= 2 and self.has_apply_pattern


@dataclass
class DetailTitleResolution:
    title: str
    title_source: str
    has_jsonld_jobposting: bool
    page_text: str


@dataclass
class TitleValidationResult:
    accepted: bool
    normalized_title: str
    cleaned: bool
    rejection_reason: str = ""
    rejection_type: str = ""
    signal_count: int = 0
    signals: tuple[str, ...] = ()


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _contains_role_word(value: str) -> bool:
    low = normalize_text(value)
    return any(word in low for word in ROLE_HINT_WORDS)


def _looks_generic_title(value: str) -> bool:
    low = normalize_text(value)
    if low in TITLE_BLOCKLIST_EXACT:
        return True
    if any(pattern.search(low) for pattern in GENERIC_TITLE_PATTERNS):
        return True
    return False


def is_anchor_job_title_candidate(value: str) -> bool:
    title = normalize_job_title(value)
    if not title:
        return False
    if _looks_generic_title(title):
        return False

    low = normalize_text(title)
    words = [w for w in low.split() if w]
    if len(words) < 2:
        return False
    if words[0] in {"view", "see", "browse", "learn", "open", "click", "submit"}:
        return False
    if _contains_role_word(low):
        return True
    if any(k in low for k in ("job", "position", "vacan", "recruit", "department")):
        return True
    return False


def _remove_duplicate_punctuation(value: str) -> str:
    value = re.sub(r"([|:;,-])\1+", r"\1", value)
    value = re.sub(r"\s*([|:;,-])\s*", r" \1 ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -|:;")


def _segment_title(value: str) -> list[str]:
    return [p.strip(" -|:;") for p in re.split(r"\s(?:\||-|–|—|:)\s", value) if p.strip(" -|:;")]


def _segment_score(segment: str) -> int:
    low = normalize_text(segment)
    score = 0
    words = [w for w in low.split() if w]
    if len(words) >= 2:
        score += 2
    if _contains_role_word(low):
        score += 4
    if any(k in low for k in ("job", "position", "vacan", "opportun", "recruit")):
        score += 2
    if any(marker in low for marker in SITE_NAME_MARKERS):
        score -= 2
    if _looks_generic_title(low):
        score -= 5
    if re.search(r"\d{4,}", low):
        score -= 1
    return score


def normalize_job_title(raw_title: str) -> str:
    title = _clean(raw_title)
    if not title:
        return ""

    title = _remove_duplicate_punctuation(title.replace("—", "-").replace("–", "-"))

    for _ in range(3):
        prev = title
        for pattern in TITLE_PREFIX_PATTERNS:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip(" -|:;")
        if title == prev:
            break

    for pattern in TITLE_SUFFIX_PATTERNS:
        title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip(" -|:;")

    parts = _segment_title(title)
    if len(parts) > 1:
        scored = sorted(parts, key=lambda p: (_segment_score(p), len(p)), reverse=True)
        best = scored[0]
        if _segment_score(best) >= 1:
            title = best

    title = _clean(title.strip(" -|:;"))
    return title


def title_blocklist_reason(title: str, source_url: str, posting_url: str, page_hint_text: str = "") -> str:
    normalized = normalize_text(title)
    source_path = (urlparse(source_url).path or "").lower()
    posting_path = (urlparse(posting_url).path or "").lower()
    hint = normalize_text(page_hint_text)

    if normalized in TITLE_BLOCKLIST_EXACT:
        return f"title blocklist exact: {normalized}"

    if "notice" in normalized and ("/notice/" in posting_path or "/notices/" in posting_path):
        return "title-notice and notice path"

    words = [w for w in normalized.split() if w]
    if len(words) <= 3 and normalized in NAV_LIKE_SHORT_TERMS:
        return "short generic nav title"

    combined_context = " ".join([source_path, posting_path, hint]).lower()
    if any(term in combined_context for term in NON_JOB_CATEGORY_TERMS):
        if not any(job_term in combined_context for job_term in ("job", "career", "employment", "recruit", "vacan")):
            return "non-job category context"

    if _looks_generic_title(normalized):
        return "generic title pattern"

    return ""


def _url_signal(posting_url: str) -> bool:
    low = posting_url.lower()
    return any(token in low for token in JOB_URL_HINTS)


def _near_title_signal(title: str, listing_text: str, detail_text: str) -> bool:
    blob = normalize_text(f"{title} {listing_text} {detail_text}")
    return any(k in blob for k in JOB_NEAR_TITLE_KEYWORDS)


def analyze_listing_signals(listing_html: str) -> ListingSignals:
    soup = BeautifulSoup(listing_html or "", "lxml")
    role_like = 0
    has_apply = False

    for a in soup.find_all("a", href=True):
        text = _clean(" ".join(a.stripped_strings))
        href = (a.get("href") or "").lower()
        low = normalize_text(text)

        if "apply" in low or "apply" in href:
            has_apply = True

        if not text:
            continue
        if _looks_generic_title(text):
            continue
        if _contains_role_word(text):
            role_like += 1
            continue
        if any(word in low for word in ("position", "opportunity", "vacancy", "job")) and len(low.split()) >= 2:
            role_like += 1

    return ListingSignals(role_like_links_count=role_like, has_apply_pattern=has_apply)


def _parse_jsonld_jobposting(soup: BeautifulSoup) -> tuple[str, bool, str]:
    title = ""
    has_jobposting = False
    details_blob: list[str] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (script.string or script.text or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue

        entries: list[dict] = []
        if isinstance(parsed, dict):
            if isinstance(parsed.get("@graph"), list):
                entries.extend(item for item in parsed["@graph"] if isinstance(item, dict))
            else:
                entries.append(parsed)
        elif isinstance(parsed, list):
            entries.extend(item for item in parsed if isinstance(item, dict))

        for entry in entries:
            typ = normalize_text(str(entry.get("@type") or ""))
            if "jobposting" not in typ:
                continue
            has_jobposting = True
            if not title:
                title = _clean(str(entry.get("title") or ""))
            description = _clean(str(entry.get("description") or ""))
            if description:
                details_blob.append(description)

    return title, has_jobposting, _clean(" ".join(details_blob))


def extract_title_hierarchy_from_detail(
    detail_html: str,
    page_url: str,
    fallback_anchor_title: str,
) -> DetailTitleResolution:
    soup = BeautifulSoup(detail_html or "", "lxml")
    jsonld_title, has_jobposting, jsonld_blob = _parse_jsonld_jobposting(soup)

    og_title = ""
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "og:title"})
    if og:
        og_title = _clean(str(og.get("content") or ""))

    main = soup.find("main") or soup.find(attrs={"role": "main"}) or soup.find(id=re.compile("main", re.IGNORECASE)) or soup
    h1 = main.find("h1") if main else None
    h2 = main.find("h2") if main else None
    h1_text = _clean(h1.get_text(" ", strip=True)) if h1 else ""
    h2_text = _clean(h2.get_text(" ", strip=True)) if h2 else ""

    html_title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""

    page_text = _clean((main.get_text(" ", strip=True) if main else "")[:6000])
    if jsonld_blob:
        page_text = _clean(f"{jsonld_blob} {page_text}")

    candidates = [
        ("jsonld", jsonld_title),
        ("og_title", og_title),
        ("h1", h1_text),
        ("h2", h2_text),
    ]

    # <title> is last-resort only when it looks role-like and not generic.
    if html_title and (_contains_role_word(html_title) or any(k in normalize_text(html_title) for k in ("position", "officer", "manager", "coordinator"))):
        candidates.append(("html_title", html_title))

    candidates.append(("anchor", fallback_anchor_title))

    for source, raw in candidates:
        cleaned = normalize_job_title(raw)
        if not cleaned:
            continue
        reason = title_blocklist_reason(cleaned, source_url=page_url, posting_url=page_url, page_hint_text=page_text)
        if reason:
            continue
        return DetailTitleResolution(
            title=cleaned,
            title_source=source,
            has_jsonld_jobposting=has_jobposting,
            page_text=page_text,
        )

    return DetailTitleResolution(
        title="",
        title_source="",
        has_jsonld_jobposting=has_jobposting,
        page_text=page_text,
    )


def validate_title_and_job_gate(
    candidate_title: str,
    posting_url: str,
    source_url: str,
    title_source: str,
    listing_text: str,
    detail_text: str,
    has_jsonld_jobposting: bool,
    listing_signal: bool,
) -> TitleValidationResult:
    normalized = normalize_job_title(candidate_title)
    cleaned = _clean(candidate_title) != normalized

    if not normalized:
        return TitleValidationResult(
            accepted=False,
            normalized_title="",
            cleaned=cleaned,
            rejection_reason="empty normalized title",
            rejection_type="blocklist",
        )

    blocklist_reason = title_blocklist_reason(
        title=normalized,
        source_url=source_url,
        posting_url=posting_url,
        page_hint_text=f"{listing_text} {detail_text}",
    )
    if blocklist_reason:
        return TitleValidationResult(
            accepted=False,
            normalized_title=normalized,
            cleaned=cleaned,
            rejection_reason=blocklist_reason,
            rejection_type="blocklist",
        )

    signals: list[str] = []
    if _url_signal(posting_url):
        signals.append("url_hint")
    if _near_title_signal(normalized, listing_text, detail_text):
        signals.append("keywords_near_title")
    if has_jsonld_jobposting:
        signals.append("jsonld_jobposting")
    if listing_signal:
        signals.append("listing_apply_pattern")
    if title_source == "ats_native":
        signals.append("ats_native_title")
    if _contains_role_word(normalized):
        signals.append("role_like_title")
    if title_source in {"jsonld", "h1", "h2", "og_title"}:
        signals.append(f"title_source_{title_source}")
    if posting_url.lower().endswith(".pdf") and (_contains_role_word(normalized) or "job" in normalize_text(normalized)):
        signals.append("pdf_role_title")

    if len(signals) < 2:
        return TitleValidationResult(
            accepted=False,
            normalized_title=normalized,
            cleaned=cleaned,
            rejection_reason=f"validation gate failed ({len(signals)}/2): {', '.join(signals) if signals else 'no signals'}",
            rejection_type="validation_gate",
            signal_count=len(signals),
            signals=tuple(signals),
        )

    return TitleValidationResult(
        accepted=True,
        normalized_title=normalized,
        cleaned=cleaned,
        signal_count=len(signals),
        signals=tuple(signals),
    )
