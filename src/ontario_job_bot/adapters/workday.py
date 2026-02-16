from __future__ import annotations

from urllib.parse import urlparse

from ..config import Settings
from ..http_client import AsyncHttpHelper
from ..models import Posting
from ..utils import normalize_url
from .common import fallback_generic_html
from .generic import GenericAdapter


class WorkdayAdapter:
    async def scrape(
        self,
        board_url: str,
        http: AsyncHttpHelper,
        settings: Settings,
    ) -> list[Posting]:
        normalized = normalize_url(board_url)
        parsed = urlparse(normalized)
        host = parsed.hostname or ""
        path_parts = [p for p in parsed.path.split("/") if p]

        tenant = host.split(".")[0] if host else ""
        site = ""
        if len(path_parts) >= 2 and path_parts[0].lower() in {"en-us", "fr-ca", "en-ca"}:
            site = path_parts[1]
        elif path_parts:
            site = path_parts[-1]

        if tenant and site:
            endpoint = f"{parsed.scheme}://{parsed.netloc}/wday/cxs/{tenant}/{site}/jobs"
            payload = {
                "appliedFacets": {},
                "limit": 100,
                "offset": 0,
                "searchText": "",
            }
            try:
                await http.rate_limiter.wait(endpoint)
                resp = await http.client.post(endpoint, json=payload)
                if resp.status_code < 400:
                    data = resp.json()
                    postings: list[Posting] = []
                    for item in data.get("jobPostings", []):
                        title = str(item.get("title") or "").strip()
                        if not title:
                            continue
                        ext_id = str(item.get("bulletFields") or item.get("externalPath") or title)
                        posting_path = str(item.get("externalPath") or "")
                        posting_url = normalize_url(f"{parsed.scheme}://{parsed.netloc}{posting_path}") if posting_path.startswith("/") else normalized
                        location = ""
                        bullet = item.get("bulletFields")
                        if isinstance(bullet, list) and bullet:
                            location = str(bullet[0])

                        postings.append(
                            Posting(
                                board_url=normalized,
                                external_id=ext_id[:120],
                                title=title,
                                posting_url=posting_url,
                                location=location,
                                posted_date=str(item.get("postedOn") or ""),
                                summary="",
                                raw_text=f"{title} {location}",
                            )
                        )
                    if postings:
                        return postings
            except Exception:
                pass

        html, final_url = await http.fetch_html_lite(normalized, settings.max_html_bytes)
        if html:
            parsed_generic = fallback_generic_html(normalize_url(final_url or normalized), html)
            if parsed_generic:
                return parsed_generic

        return await GenericAdapter().scrape(normalized, http, settings)
