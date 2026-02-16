from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from .utils import normalize_url


@dataclass(frozen=True)
class ClassifierHit:
    jobs_source_type: str
    adapter: str
    confidence: float
    reason: str


ATS_RULES: list[tuple[re.Pattern[str], ClassifierHit]] = [
    (
        re.compile(r"(?:^|\.)myworkdayjobs\.com$|/wday/cxs/", re.IGNORECASE),
        ClassifierHit("ats_workday", "workday", 0.98, "matched workday domain/path"),
    ),
    (
        re.compile(r"(?:^|\.)taleo\.net$|careersection|candidateexperience", re.IGNORECASE),
        ClassifierHit("ats_taleo", "taleo", 0.98, "matched taleo/oracle careers pattern"),
    ),
    (
        re.compile(r"(?:^|\.)icims\.com$|jobs\.icims\.com", re.IGNORECASE),
        ClassifierHit("ats_icims", "icims", 0.98, "matched icims domain"),
    ),
    (
        re.compile(r"governmentjobs\.com|(?:^|\.)neogov\.com$", re.IGNORECASE),
        ClassifierHit("ats_neogov", "neogov", 0.98, "matched neogov/governmentjobs"),
    ),
    (
        re.compile(r"recruiting\.ultipro\.(?:ca|com)|ukg|ultipro", re.IGNORECASE),
        ClassifierHit("ats_utipro", "utipro", 0.98, "matched ultipro/ukg recruiting"),
    ),
    (
        re.compile(r"workforcenow\.adp\.com|adp\.com/.*/recruit", re.IGNORECASE),
        ClassifierHit("ats_adp", "adp", 0.98, "matched adp recruitment path"),
    ),
]


JOB_KEYWORDS = (
    "job",
    "jobs",
    "career",
    "careers",
    "employment",
    "opportunity",
    "opportunities",
    "vacancy",
    "vacancies",
)


def classify_url(url: str) -> ClassifierHit | None:
    norm = normalize_url(url)
    if not norm:
        return None

    parsed = urlparse(norm)
    host = parsed.hostname or ""
    full = f"{host}{parsed.path}"

    for pattern, hit in ATS_RULES:
        if pattern.search(host) or pattern.search(full):
            return hit

    if parsed.path.lower().endswith(".pdf"):
        return ClassifierHit("pdf", "pdf", 0.75, "pdf path detected")

    return None


def classify_chain(url_chain: list[str]) -> ClassifierHit | None:
    for idx, url in enumerate(url_chain):
        hit = classify_url(url)
        if hit is not None:
            if idx == 0:
                return hit
            return ClassifierHit(
                jobs_source_type=hit.jobs_source_type,
                adapter=hit.adapter,
                confidence=max(0.85, hit.confidence - 0.05),
                reason=f"redirect chain: {hit.reason}",
            )
    return None


def looks_like_job_link(url: str, anchor_text: str = "") -> bool:
    target = f"{url} {anchor_text}".lower()
    return any(word in target for word in JOB_KEYWORDS)
