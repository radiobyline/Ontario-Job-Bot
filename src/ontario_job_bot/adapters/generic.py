from __future__ import annotations

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..utils import normalize_url, stable_hash
from .common import enrich_postings_with_detail_titles, fallback_generic_html


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
                    title_source="url_slug",
                    source_url=normalized,
                )
            ]

        html, final_url = await http.fetch_html_lite(normalized, settings.max_html_bytes)
        if not html:
            return []
        board_final = normalize_url(final_url or normalized)
        parsed = fallback_generic_html(board_final, html)
        return await enrich_postings_with_detail_titles(
            board_url=board_final,
            listing_html=html,
            postings=parsed,
            http=http,
            max_html_bytes=settings.max_html_bytes,
        )
