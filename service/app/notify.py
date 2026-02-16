from __future__ import annotations

from datetime import datetime, timezone
import html

from .config import settings
from .tg_bot_api import send_message_html


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _escape_html(s: str) -> str:
    return html.escape(s or "", quote=False)


def _escape_href(s: str) -> str:
    # Escape quotes too because it's used inside href="...".
    return html.escape(s or "", quote=True)


def _trim(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    cut = s[:max_len]
    # Try to cut at a space to avoid mid-word truncation.
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut.rstrip() + "…"


async def send_lead_notification(*, event_id: int, excerpt: str, summary: str, link: str) -> None:
    # Backward-compatible helper: "lead" notification.
    await send_event_notification(event_id=event_id, excerpt=excerpt, summary=summary, link=link, is_lead=True)


async def send_event_notification(*, event_id: int, excerpt: str, summary: str, link: str, is_lead: bool | None) -> None:
    if not settings.tg_bot_token or not settings.tg_chat_id:
        raise RuntimeError("tg_bot_token/tg_chat_id not set")

    excerpt = _trim(excerpt, 700)
    summary = _trim(summary, 240)
    link = (link or "").strip()

    parts: list[str] = []

    if is_lead is True:
        parts.append(f"<b>Лид</b>  <code>#{event_id}</code>")
    elif is_lead is False:
        parts.append(f"<b>Не лид</b>  <code>#{event_id}</code>")
    else:
        parts.append(f"<b>Сообщение</b>  <code>#{event_id}</code>")

    if excerpt:
        # Use <pre> to keep the message readable (wrap + line breaks).
        parts.append("<b>Запрос</b>\n<pre>" + _escape_html(excerpt) + "</pre>")
    if summary:
        parts.append("<b>Кратко</b>\n" + _escape_html(summary))
    if link:
        safe_link = _escape_href(link)
        parts.append(f"<b>Ссылка</b> <a href=\"{safe_link}\">открыть</a>")

    text = "\n\n".join(parts) if parts else "Event"

    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "Лид", "callback_data": f"fb:lead:{event_id}"},
                {"text": "Не лид", "callback_data": f"fb:not_lead:{event_id}"},
            ],
        ]
    }

    await send_message_html(chat_id=settings.tg_chat_id, html=text, reply_markup=reply_markup)
