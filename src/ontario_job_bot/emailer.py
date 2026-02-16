from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from .config import Settings


def send_digest_email(settings: Settings, subject: str, body_text: str, body_html: str | None = None) -> bool:
    if not settings.smtp_enabled:
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.email_from
    msg["To"] = settings.email_to

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    if body_html:
        msg.attach(MIMEText(body_html, "html", "utf-8"))

    with smtplib.SMTP(settings.brevo_smtp_server, settings.brevo_smtp_port, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(settings.brevo_smtp_login, settings.brevo_smtp_key)
        smtp.sendmail(settings.email_from, [settings.email_to], msg.as_string())

    return True
