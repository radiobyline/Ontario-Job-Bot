from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlparse, urlunparse

import httpx

from .utils import normalize_url


def _is_supported_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


@dataclass
class RedirectResult:
    requested_url: str
    final_url: str
    chain: list[str]
    status_code: int
    method: str
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error and self.status_code > 0


class DomainRateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self._next_allowed: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def wait(self, url: str) -> None:
        if self.interval <= 0:
            return
        domain = self._domain_key(url)
        lock = self._locks.setdefault(domain, asyncio.Lock())
        async with lock:
            now = asyncio.get_running_loop().time()
            next_allowed = self._next_allowed.get(domain, now)
            if next_allowed > now:
                await asyncio.sleep(next_allowed - now)
            self._next_allowed[domain] = asyncio.get_running_loop().time() + self.interval

    @staticmethod
    def _domain_key(url: str) -> str:
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return ""
        parts = host.split(".")
        if len(parts) <= 2:
            return host
        return ".".join(parts[-2:])


class AsyncHttpHelper:
    def __init__(
        self,
        timeout_seconds: int,
        max_redirects: int,
        per_domain_rps: float,
    ) -> None:
        timeout = httpx.Timeout(timeout_seconds, connect=min(4, timeout_seconds))
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=60)
        self.client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            limits=limits,
            headers={
                "User-Agent": "OntarioJobBot/1.0 (+https://github.com/radiobyline/Ontario-Job-Bot)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            max_redirects=max_redirects,
        )
        self.rate_limiter = DomainRateLimiter(per_domain_rps)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _request_with_retries(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                await self.rate_limiter.wait(url)
                response = await self.client.request(method, url, **kwargs)
                if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
                    await asyncio.sleep((2**attempt) * 0.4)
                    continue
                return response
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as exc:
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep((2**attempt) * 0.4)
                    continue
                raise
        assert last_exc is not None
        raise last_exc

    async def resolve_redirects(self, url: str) -> RedirectResult:
        normalized = normalize_url(url)
        if not normalized:
            return RedirectResult(url, url, [], 0, "HEAD", error="empty url")
        if not _is_supported_http_url(normalized):
            return RedirectResult(url, normalized, [normalized], 0, "HEAD", error="unsupported url")

        methods = ["HEAD", "GET"]
        for method in methods:
            try:
                response = await self._request_with_retries(method, normalized)
                chain = [str(item.url) for item in response.history] + [str(response.url)]
                return RedirectResult(
                    requested_url=normalized,
                    final_url=str(response.url),
                    chain=chain,
                    status_code=response.status_code,
                    method=method,
                )
            except httpx.HTTPError as exc:
                if method == "GET":
                    return RedirectResult(
                        requested_url=normalized,
                        final_url=normalized,
                        chain=[normalized],
                        status_code=0,
                        method=method,
                        error=str(exc),
                    )

        return RedirectResult(
            requested_url=normalized,
            final_url=normalized,
            chain=[normalized],
            status_code=0,
            method="HEAD",
            error="could not resolve redirects",
        )

    async def fetch_html_lite(self, url: str, max_bytes: int) -> tuple[str, str]:
        normalized = normalize_url(url)
        if not normalized:
            return "", ""
        if not _is_supported_http_url(normalized):
            return "", normalized

        await self.rate_limiter.wait(normalized)
        try:
            async with self.client.stream("GET", normalized) as response:
                response.raise_for_status()
                final_url = str(response.url)
                content_type = response.headers.get("content-type", "")
                if "text/html" not in content_type and "xml" not in content_type and "json" not in content_type:
                    return "", final_url

                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= max_bytes:
                        break

                body = b"".join(chunks).decode(response.encoding or "utf-8", errors="replace")
                return body, final_url
        except httpx.HTTPError:
            return "", normalized

    async def fetch_text(self, url: str, max_bytes: int = 350_000) -> tuple[str, str]:
        return await self.fetch_html_lite(url=url, max_bytes=max_bytes)


def url_variants(url: str) -> list[str]:
    normalized = normalize_url(url)
    if not normalized:
        return []
    if not _is_supported_http_url(normalized):
        return []

    parsed = urlparse(normalized)
    host = parsed.hostname or ""
    scheme = parsed.scheme
    variants: list[str] = [normalized]

    alt_scheme = "https" if scheme == "http" else "http"
    variants.append(urlunparse((alt_scheme, parsed.netloc, parsed.path, "", parsed.query, "")))

    if host.startswith("www."):
        alt_host = host[4:]
    else:
        alt_host = f"www.{host}"

    if parsed.port:
        alt_netloc = f"{alt_host}:{parsed.port}"
    else:
        alt_netloc = alt_host

    variants.append(urlunparse((scheme, alt_netloc, parsed.path, "", parsed.query, "")))

    deduped: list[str] = []
    seen: set[str] = set()
    for item in variants:
        norm_item = normalize_url(item)
        if norm_item and norm_item not in seen:
            deduped.append(norm_item)
            seen.add(norm_item)
    return deduped
