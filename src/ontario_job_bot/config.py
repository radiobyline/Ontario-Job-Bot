from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in TRUE_VALUES


def _as_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    return int(value)


def _as_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    return float(value)


@dataclass
class Settings:
    orgs_csv: Path
    orgs_enriched_csv: Path
    db_path: Path

    global_concurrency: int
    per_domain_rps: float
    request_timeout_seconds: int
    max_redirects: int
    max_html_bytes: int
    discovery_cache_ttl_days: int
    enable_js_fallback: bool

    brevo_smtp_server: str
    brevo_smtp_port: int
    brevo_smtp_login: str
    brevo_smtp_key: str
    email_from: str
    email_to: str
    send_empty_digest: bool

    google_sheets_spreadsheet_id: str
    google_sheets_worksheet: str
    google_orgs_spreadsheet_id: str
    google_orgs_worksheet: str
    google_service_account_json: str
    google_service_account_json_path: str


    @property
    def smtp_enabled(self) -> bool:
        return bool(
            self.brevo_smtp_login
            and self.brevo_smtp_key
            and self.email_from
            and self.email_to
        )

    @property
    def sheets_enabled(self) -> bool:
        return bool(
            self.google_sheets_spreadsheet_id
            and (self.google_service_account_json or self.google_service_account_json_path)
        )


def load_settings() -> Settings:
    load_dotenv(override=False)

    return Settings(
        orgs_csv=Path(os.getenv("ORGS_CSV", "data/orgs.csv")),
        orgs_enriched_csv=Path(os.getenv("ORGS_ENRICHED_CSV", "data/orgs_enriched.csv")),
        db_path=Path(os.getenv("DB_PATH", "state/postings.sqlite")),
        global_concurrency=_as_int(os.getenv("GLOBAL_CONCURRENCY"), 80),
        per_domain_rps=_as_float(os.getenv("PER_DOMAIN_RPS"), 1.0),
        request_timeout_seconds=_as_int(os.getenv("REQUEST_TIMEOUT_SECONDS"), 10),
        max_redirects=_as_int(os.getenv("MAX_REDIRECTS"), 8),
        max_html_bytes=_as_int(os.getenv("MAX_HTML_BYTES"), 350_000),
        discovery_cache_ttl_days=_as_int(os.getenv("DISCOVERY_CACHE_TTL_DAYS"), 45),
        enable_js_fallback=_as_bool(os.getenv("ENABLE_JS_FALLBACK"), False),
        brevo_smtp_server=os.getenv("BREVO_SMTP_SERVER", "smtp-relay.brevo.com"),
        brevo_smtp_port=_as_int(os.getenv("BREVO_SMTP_PORT"), 587),
        brevo_smtp_login=os.getenv("BREVO_SMTP_LOGIN", ""),
        brevo_smtp_key=os.getenv("BREVO_SMTP_KEY", ""),
        email_from=os.getenv("EMAIL_FROM", ""),
        email_to=os.getenv("EMAIL_TO", ""),
        send_empty_digest=_as_bool(os.getenv("SEND_EMPTY_DIGEST"), False),
        google_sheets_spreadsheet_id=os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", ""),
        google_sheets_worksheet=os.getenv("GOOGLE_SHEETS_WORKSHEET", "Postings"),
        google_orgs_spreadsheet_id=os.getenv("GOOGLE_ORGS_SPREADSHEET_ID", ""),
        google_orgs_worksheet=os.getenv("GOOGLE_ORGS_WORKSHEET", ""),
        google_service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
        google_service_account_json_path=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", ""),
    )
