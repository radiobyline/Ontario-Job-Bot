# Ontario Job Bot

Fast two-part job monitoring for Ontario municipalities and Ontario First Nations.

## What it does

- Part A (`discover`): canonicalizes each organization's `jobs_url` into a direct, scrapeable board URL.
- Part B (`monitor`): scrapes canonical boards weekly, detects new postings via SQLite history, emails a digest, and upserts to Google Sheets without overwriting manual columns (`status`, `applied_date`, `notes`).
  - Captures `posting_date` and `closing_date` when available.
  - Filters social/share/navigation links so `posting_url` and `title` are job-focused.
  - Applies a title validation gate and rejects generic/non-job labels (e.g., notices, services, submit request links).

## Key design constraints implemented

- No human-style browsing by default.
- No Playwright/Selenium in standard flow.
- Discovery priority order:
  1. URL pattern classification
  2. Redirect chain resolution
  3. Lightweight HTML parse (`a[href]`, `form[action]`, `meta refresh`)
  4. Sitemap hints only if needed
- Per-domain polite limit (`1 req/sec`) with cross-domain concurrency.
- URL dedupe and cache to avoid repeated work.

## Setup

1. Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment:

```bash
cp .env.example .env
# fill required values
```

3. Place organizations CSV at `data/orgs.csv` (already included here).

## Commands

Run discovery:

```bash
PYTHONPATH=src python -m ontario_job_bot discover --input data/orgs.csv --output data/orgs_enriched.csv
```

Run weekly monitor:

```bash
PYTHONPATH=src python -m ontario_job_bot monitor --input data/orgs_enriched.csv
```

Run both:

```bash
PYTHONPATH=src python -m ontario_job_bot run-all --input data/orgs.csv --output data/orgs_enriched.csv
```

Smoke test with smaller scope:

```bash
PYTHONPATH=src python -m ontario_job_bot discover --limit 20
PYTHONPATH=src python -m ontario_job_bot monitor --max-boards 10
```

## SQLite state

State persists at `state/postings.sqlite`.

Used for:
- canonicalization cache
- board mapping
- posting history (`first_seen_at` / `last_seen_at`)
- new-posting detection
- attribution links

## Title quality controls

- Shared utility: `src/ontario_job_bot/title_normalize_and_validate.py`
- Deterministic title hierarchy:
  1. ATS-native fields
  2. Detail page JSON-LD / OpenGraph
  3. Detail page H1 (fallback H2)
  4. Listing anchor text (only if title-like)
- Validation gate requires at least 2 job signals before a posting is accepted.

Per run monitor stats include:
- `titles_rejected_blocklist_count`
- `titles_rejected_validation_gate_count`
- `titles_cleaned_count`

Per run rejection report:
- `reports/title_rejections_YYYY-MM-DD.csv`

## Google Sheets behavior

Sheet tab: `Postings` (configurable).

System-managed columns include:
- `posting_uid`
- `title`
- `posting_url`
- `posting_date`
- `closing_date`
- `org_ids` / `org_names`
- `board_url`

Upsert keeps manual columns untouched for existing rows:
- `status`
- `applied_date`
- `notes`

## GitHub Actions deployment

Workflow file: `.github/workflows/weekly-monitor.yml`

Trigger:
- Weekly Monday run (`0 13 * * 1` UTC)
- Manual dispatch

Required repository secrets:
- `BREVO_SMTP_LOGIN`
- `BREVO_SMTP_KEY`
- `EMAIL_FROM`
- `EMAIL_TO`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`

`GOOGLE_SERVICE_ACCOUNT_JSON` should be the full service-account JSON string.
