from __future__ import annotations

from datetime import datetime, timezone

import httpx

from .config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


async def send_lead_notification(*, excerpt: str, summary: str, link: str) -> None:
    if not settings.tg_bot_token or not settings.tg_chat_id:
        raise RuntimeError("tg_bot_token/tg_chat_id not set")

    excerpt = (excerpt or "").strip()
    summary = (summary or "").strip()
    link = (link or "").strip()

    parts: list[str] = []
    if excerpt:
        parts.append(f"<b>Запрос:</b> {_escape_html(excerpt)}")
    if summary:
        parts.append(f"<b>Кратко:</b> {_escape_html(summary)}")
    if link:
        safe_link = _escape_html(link)
        parts.append(f"<b>Ссылка:</b> <a href=\"{safe_link}\">открыть</a>")

    text = "\n\n".join(parts) if parts else "Lead"

    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"
    payload = {
        "chat_id": settings.tg_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, timeout=15.0)
        r.raise_for_status()
