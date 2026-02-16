from __future__ import annotations

import asyncio
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .classifiers import classify_chain, classify_url, looks_like_job_link
from .config import Settings
from .db import cache_resolution, connect, get_cached_resolution, init_db, map_org_board, rows_to_dicts, start_run, finish_run, upsert_board
from .http_client import AsyncHttpHelper, url_variants
from .models import ResolutionResult
from .utils import normalize_url


RE_META_REFRESH_URL = re.compile(r"url\s*=\s*['\"]?([^'\"\s>]+)", re.IGNORECASE)


@dataclass
class CandidateLink:
    url: str
    text: str
    source: str


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def save_rows(csv_path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_candidates(html: str, base_url: str) -> list[CandidateLink]:
    soup = BeautifulSoup(html, "lxml")
    candidates: list[CandidateLink] = []

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        txt = " ".join(a.stripped_strings)
        candidates.append(CandidateLink(abs_url, txt, "a_href"))

    for form in soup.find_all("form"):
        action = (form.get("action") or "").strip()
        if not action:
            continue
        abs_url = urljoin(base_url, action)
        candidates.append(CandidateLink(abs_url, "", "form_action"))

    for meta in soup.find_all("meta"):
        if (meta.get("http-equiv") or "").lower() != "refresh":
            continue
        content = meta.get("content") or ""
        match = RE_META_REFRESH_URL.search(content)
        if not match:
            continue
        abs_url = urljoin(base_url, match.group(1).strip())
        candidates.append(CandidateLink(abs_url, "", "meta_refresh"))

    deduped: list[CandidateLink] = []
    seen: set[str] = set()
    for item in candidates:
        norm = normalize_url(item.url)
        parsed = urlparse(norm)
        if (
            not norm
            or parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or norm in seen
        ):
            continue
        seen.add(norm)
        deduped.append(CandidateLink(norm, item.text, item.source))
    return deduped


def rank_candidates(candidates: list[CandidateLink], base_url: str) -> list[CandidateLink]:
    base_host = (urlparse(base_url).hostname or "").lower()

    def score(item: CandidateLink) -> int:
        sc = 0
        url = item.url.lower()
        text = item.text.lower()

        hit = classify_url(item.url)
        if hit is not None:
            sc += 100

        if looks_like_job_link(url, text):
            sc += 35

        host = (urlparse(item.url).hostname or "").lower()
        if host and host != base_host:
            sc += 20

        if any(w in url for w in ("apply", "recruit", "vacancy")):
            sc += 10

        if item.source == "meta_refresh":
            sc += 8
        elif item.source == "form_action":
            sc += 6

        return sc

    return sorted(candidates, key=score, reverse=True)


def detect_html_list(candidates: list[CandidateLink]) -> bool:
    jobish = [c for c in candidates if looks_like_job_link(c.url, c.text)]
    return len(jobish) >= 2


def build_sitemap_candidates(sitemap_xml: str) -> list[str]:
    locs = re.findall(r"<loc>(.*?)</loc>", sitemap_xml, flags=re.IGNORECASE)
    result: list[str] = []
    for loc in locs:
        norm = normalize_url(loc)
        if not norm:
            continue
        low = norm.lower()
        if any(k in low for k in ("job", "career", "employment", "opportun")):
            result.append(norm)
    deduped: list[str] = []
    seen: set[str] = set()
    for u in result:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped[:3]


async def resolve_seed(
    seed_url: str,
    http: AsyncHttpHelper,
    settings: Settings,
    stage_counts: dict[str, int],
) -> ResolutionResult:
    normalized_seed = normalize_url(seed_url)
    if not normalized_seed:
        stage_counts["manual_review"] += 1
        return ResolutionResult(
            seed_url=seed_url,
            canonical_jobs_url=seed_url,
            jobs_source_type="unknown",
            adapter="generic",
            confidence=0.0,
            discovered_via="invalid_input",
            notes="jobs_url missing or invalid",
            manual_review=True,
        )

    direct = classify_url(normalized_seed)
    if direct:
        stage_counts["pattern"] += 1
        return ResolutionResult(
            seed_url=seed_url,
            canonical_jobs_url=normalized_seed,
            jobs_source_type=direct.jobs_source_type,
            adapter=direct.adapter,
            confidence=direct.confidence,
            discovered_via="url_pattern",
            notes=direct.reason,
            manual_review=False,
        )

    best_redirect = None
    for variant in url_variants(normalized_seed):
        redirect_result = await http.resolve_redirects(variant)
        if not redirect_result.ok:
            continue
        best_redirect = redirect_result
        chain_hit = classify_chain(redirect_result.chain)
        if chain_hit:
            stage_counts["redirect"] += 1
            return ResolutionResult(
                seed_url=seed_url,
                canonical_jobs_url=normalize_url(redirect_result.final_url),
                jobs_source_type=chain_hit.jobs_source_type,
                adapter=chain_hit.adapter,
                confidence=chain_hit.confidence,
                discovered_via="redirect_chain",
                notes=chain_hit.reason,
                manual_review=False,
            )
        break

    if not best_redirect:
        stage_counts["manual_review"] += 1
        return ResolutionResult(
            seed_url=seed_url,
            canonical_jobs_url=normalized_seed,
            jobs_source_type="unknown",
            adapter="generic",
            confidence=0.2,
            discovered_via="failed_request",
            notes="all URL variants failed",
            manual_review=True,
        )

    final_url = normalize_url(best_redirect.final_url)
    final_hit = classify_url(final_url)
    if final_hit and final_hit.jobs_source_type == "pdf":
        stage_counts["pdf"] += 1
        return ResolutionResult(
            seed_url=seed_url,
            canonical_jobs_url=final_url,
            jobs_source_type="pdf",
            adapter="pdf",
            confidence=final_hit.confidence,
            discovered_via="redirect_pdf",
            notes="final URL is PDF",
            manual_review=False,
        )

    html, html_url = await http.fetch_html_lite(final_url, max_bytes=settings.max_html_bytes)
    html_base = normalize_url(html_url or final_url)

    if html:
        candidates = rank_candidates(extract_candidates(html, html_base), html_base)

        for candidate in candidates[:3]:
            chain = await http.resolve_redirects(candidate.url)
            if not chain.ok:
                continue
            hit = classify_chain(chain.chain)
            if hit is not None:
                stage_counts["html"] += 1
                return ResolutionResult(
                    seed_url=seed_url,
                    canonical_jobs_url=normalize_url(chain.final_url),
                    jobs_source_type=hit.jobs_source_type,
                    adapter=hit.adapter,
                    confidence=min(0.92, hit.confidence),
                    discovered_via=f"html_{candidate.source}",
                    notes=f"{hit.reason} via {candidate.source}",
                    manual_review=False,
                )

            final_candidate = normalize_url(chain.final_url)
            maybe_pdf = classify_url(final_candidate)
            if maybe_pdf and maybe_pdf.jobs_source_type == "pdf":
                stage_counts["pdf"] += 1
                return ResolutionResult(
                    seed_url=seed_url,
                    canonical_jobs_url=final_candidate,
                    jobs_source_type="pdf",
                    adapter="pdf",
                    confidence=0.78,
                    discovered_via=f"html_{candidate.source}",
                    notes="candidate link resolved to PDF",
                    manual_review=False,
                )

        if detect_html_list(candidates):
            stage_counts["html_list"] += 1
            return ResolutionResult(
                seed_url=seed_url,
                canonical_jobs_url=html_base,
                jobs_source_type="html_list",
                adapter="html_list",
                confidence=0.68,
                discovered_via="html_parse",
                notes="job-like links found on landing page",
                manual_review=False,
            )

    parsed = urlparse(final_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    sitemap_url = f"{root}/sitemap.xml"
    sitemap_body, _ = await http.fetch_text(sitemap_url, max_bytes=settings.max_html_bytes)
    if sitemap_body:
        sitemap_candidates = build_sitemap_candidates(sitemap_body)
        for candidate in sitemap_candidates:
            chain = await http.resolve_redirects(candidate)
            if not chain.ok:
                continue
            hit = classify_chain(chain.chain)
            if hit:
                stage_counts["sitemap"] += 1
                return ResolutionResult(
                    seed_url=seed_url,
                    canonical_jobs_url=normalize_url(chain.final_url),
                    jobs_source_type=hit.jobs_source_type,
                    adapter=hit.adapter,
                    confidence=min(0.88, hit.confidence),
                    discovered_via="sitemap_hint",
                    notes=hit.reason,
                    manual_review=False,
                )

    stage_counts["manual_review"] += 1
    return ResolutionResult(
        seed_url=seed_url,
        canonical_jobs_url=final_url,
        jobs_source_type="unknown",
        adapter="generic",
        confidence=0.3,
        discovered_via="fallback_unknown",
        notes="unable to classify; requires manual review",
        manual_review=True,
    )


async def discover_urls(
    settings: Settings,
    input_csv: Path,
    output_csv: Path,
    limit: int | None = None,
) -> dict[str, Any]:
    rows = load_rows(input_csv)
    if limit is not None:
        rows = rows[:limit]

    conn = connect(settings.db_path)
    init_db(conn)
    run_id = start_run(conn, "discover")

    required_columns = [
        "org_id",
        "org_name",
        "org_type",
        "homepage_url",
        "jobs_url",
        "canonical_jobs_url",
        "jobs_source_type",
        "adapter",
        "confidence",
        "discovered_via",
        "last_verified",
        "notes",
        "manual_review",
    ]

    seeds: dict[str, str] = {}
    for row in rows:
        seed = normalize_url(row.get("jobs_url", ""))
        if seed:
            seeds.setdefault(seed, row.get("jobs_url", ""))

    stage_counts = {
        "cache": 0,
        "pattern": 0,
        "redirect": 0,
        "html": 0,
        "sitemap": 0,
        "html_list": 0,
        "pdf": 0,
        "manual_review": 0,
    }

    helper = AsyncHttpHelper(
        timeout_seconds=settings.request_timeout_seconds,
        max_redirects=settings.max_redirects,
        per_domain_rps=settings.per_domain_rps,
    )

    semaphore = asyncio.Semaphore(settings.global_concurrency)
    results_by_seed: dict[str, ResolutionResult] = {}

    async def run_one(seed: str, original_seed: str) -> None:
        cached = get_cached_resolution(conn, original_seed)
        if cached:
            stage_counts["cache"] += 1
            results_by_seed[seed] = cached
            return

        async with semaphore:
            result = await resolve_seed(original_seed, helper, settings, stage_counts)
            cache_resolution(conn, result, settings.discovery_cache_ttl_days)
            results_by_seed[seed] = result

    try:
        await asyncio.gather(*(run_one(seed, original_seed) for seed, original_seed in seeds.items()))

        now = datetime.now(timezone.utc).date().isoformat()
        output_rows: list[dict[str, Any]] = []

        for row in rows:
            row_out = dict(row)
            seed_norm = normalize_url(row.get("jobs_url", ""))
            result = results_by_seed.get(seed_norm)
            if result:
                row_out["canonical_jobs_url"] = result.canonical_jobs_url
                row_out["jobs_source_type"] = result.jobs_source_type
                row_out["adapter"] = result.adapter
                row_out["confidence"] = f"{result.confidence:.2f}"
                row_out["discovered_via"] = result.discovered_via
                row_out["last_verified"] = now
                row_out["notes"] = result.notes
                row_out["manual_review"] = "1" if result.manual_review else "0"

                upsert_board(conn, result.canonical_jobs_url, result.jobs_source_type, result.adapter)
                map_org_board(conn, row.get("org_id", ""), result.canonical_jobs_url)
            else:
                row_out.setdefault("canonical_jobs_url", "")
                row_out.setdefault("manual_review", "1")

            output_rows.append(row_out)

        fieldnames = list(required_columns)
        for row in output_rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        save_rows(output_csv, output_rows, fieldnames)

        stats = {
            "input_rows": len(rows),
            "unique_seeds": len(seeds),
            "output_rows": len(output_rows),
            "stage_counts": stage_counts,
        }
        finish_run(conn, run_id, True, stats)
        return stats
    except Exception as exc:  # pragma: no cover
        finish_run(conn, run_id, False, {"error": str(exc), "stage_counts": stage_counts})
        raise
    finally:
        await helper.aclose()
        conn.commit()
        conn.close()
