from __future__ import annotations

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..utils import normalize_url, stable_hash


class PdfAdapter:
    async def scrape(self, board_url: str, http: AsyncHttpHelper, settings: Settings) -> list[Posting]:
        normalized = normalize_url(board_url)
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
