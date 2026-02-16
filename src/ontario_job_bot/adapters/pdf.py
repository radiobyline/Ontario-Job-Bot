from __future__ import annotations

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..title_normalize_and_validate import is_anchor_job_title_candidate
from ..utils import normalize_url, stable_hash
from .common import derive_title_from_url, extract_dates_from_text


class PdfAdapter:
    async def scrape(self, board_url: str, http: AsyncHttpHelper, settings: Settings) -> list[Posting]:
        normalized = normalize_url(board_url)
        inferred_title = derive_title_from_url(normalized) or "Job Posting"
        if not is_anchor_job_title_candidate(inferred_title):
            return []
        posting_date, closing_date = extract_dates_from_text(inferred_title)
        return [
            Posting(
                board_url=normalized,
                external_id=stable_hash(normalized)[:20],
                title=inferred_title,
                posting_url=normalized,
                posting_date=posting_date,
                closing_date=closing_date,
                summary="PDF posting",
                raw_text="PDF posting",
                title_source="url_slug",
                source_url=normalized,
            )
        ]
