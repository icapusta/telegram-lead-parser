from __future__ import annotations

from datetime import datetime, timezone

from .config import settings
from .tg_bot_api import send_message_html


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


async def send_lead_notification(*, event_id: int, excerpt: str, summary: str, link: str) -> None:
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

    reply_markup = {
        "inline_keyboard": [
            [{"text": "Лид", "callback_data": f"fb:lead:{event_id}"}],
            [{"text": "Не лид", "callback_data": f"fb:not_lead:{event_id}"}],
        ]
    }

    await send_message_html(chat_id=settings.tg_chat_id, html=text, reply_markup=reply_markup)

