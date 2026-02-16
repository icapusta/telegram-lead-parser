from __future__ import annotations

from datetime import datetime, timezone

import httpx

from .config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _escape_md(s: str) -> str:
    # Minimal escaping for MarkdownV2 to avoid broken formatting.
    for ch in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, f"\\{ch}")
    return s


async def send_lead_notification(*, excerpt: str, summary: str, link: str) -> None:
    if not settings.tg_bot_token or not settings.tg_chat_id:
        raise RuntimeError("tg_bot_token/tg_chat_id not set")

    excerpt = (excerpt or "").strip()
    summary = (summary or "").strip()
    link = (link or "").strip()

    text_parts: list[str] = []
    if excerpt:
        text_parts.append(f"*Запрос:* {_escape_md(excerpt)}")
    if summary:
        text_parts.append(f"*Кратко:* {_escape_md(summary)}")
    if link:
        text_parts.append(f"*Ссылка:* {_escape_md(link)}")

    text = "\n\n".join(text_parts) if text_parts else "Lead"

    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"
    payload = {
        "chat_id": settings.tg_chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(url, json=payload, timeout=15.0)
        r.raise_for_status()

