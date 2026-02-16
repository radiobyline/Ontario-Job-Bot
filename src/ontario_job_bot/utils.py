from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

TRACKING_QUERY_PREFIXES = (
    "utm_",
    "fbclid",
    "gclid",
    "mc_",
    "mkt_",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    parsed = urlparse(raw)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/":
        path = re.sub(r"/{2,}", "/", path).rstrip("/") or "/"

    query_items = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=True):
        key_l = key.lower()
        if any(key_l.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        query_items.append((key, val))
    query = urlencode(query_items)

    return urlunparse((scheme.lower(), netloc, path, "", query, ""))


def hostname(url: str) -> str:
    return urlparse(url).hostname or ""


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def to_float_str(value: float) -> str:
    return f"{value:.2f}"


def json_dumps(value: dict) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)
