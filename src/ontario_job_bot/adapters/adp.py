from __future__ import annotations

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from .generic import GenericAdapter


class AdpAdapter:
    async def scrape(self, board_url: str, http: AsyncHttpHelper, settings: Settings) -> list[Posting]:
        return await GenericAdapter().scrape(board_url, http, settings)
