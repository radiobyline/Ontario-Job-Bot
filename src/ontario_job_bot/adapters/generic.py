from __future__ import annotations

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..utils import normalize_url, stable_hash
from .common import fallback_generic_html


class GenericAdapter:
    async def scrape(
        self,
        board_url: str,
        http: AsyncHttpHelper,
        settings: Settings,
    ) -> list[Posting]:
        normalized = normalize_url(board_url)
        if normalized.lower().endswith(".pdf"):
            return [
                Posting(
                    board_url=normalized,
                    external_id=stable_hash(normalized)[:20],
                    title="Job Posting PDF",
                    posting_url=normalized,
                    summary="PDF posting",
                    raw_text="PDF posting",
                )
            ]

        html, final_url = await http.fetch_html_lite(normalized, settings.max_html_bytes)
        if not html:
            return []
        return fallback_generic_html(normalize_url(final_url or normalized), html)
