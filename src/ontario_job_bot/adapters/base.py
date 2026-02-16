from __future__ import annotations

from typing import Protocol

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting


class Adapter(Protocol):
    async def scrape(
        self,
        board_url: str,
        http: AsyncHttpHelper,
        settings: Settings,
    ) -> list[Posting]:
        ...
