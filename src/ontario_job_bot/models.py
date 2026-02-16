from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Organization:
    org_id: str
    org_name: str
    org_type: str
    homepage_url: str
    jobs_url: str
    canonical_jobs_url: str = ""
    jobs_source_type: str = ""
    adapter: str = ""
    confidence: float = 0.0
    discovered_via: str = ""
    last_verified: str = ""
    notes: str = ""
    manual_review: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolutionResult:
    seed_url: str
    canonical_jobs_url: str
    jobs_source_type: str
    adapter: str
    confidence: float
    discovered_via: str
    notes: str
    manual_review: bool = False


@dataclass
class Posting:
    board_url: str
    external_id: str
    title: str
    posting_url: str
    location: str = ""
    posting_date: str = ""
    closing_date: str = ""
    summary: str = ""
    raw_text: str = ""
    title_source: str = ""
    has_jobposting_schema: bool = False
    listing_signal: bool = False
    source_url: str = ""

    @property
    def posting_uid_seed(self) -> str:
        return f"{self.board_url}|{self.external_id}|{self.posting_url}|{self.title}"
