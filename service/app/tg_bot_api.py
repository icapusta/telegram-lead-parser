from __future__ import annotations

import httpx

from .config import settings


def _bot_url(method: str) -> str:
    return f"https://api.telegram.org/bot{settings.tg_bot_token}/{method}"


async def set_webhook(*, url: str, secret_token: str) -> None:
    if not settings.tg_bot_token:
        raise RuntimeError("tg_bot_token not set")
    body = {
        "url": url,
        "secret_token": secret_token,
        "allowed_updates": ["callback_query"],
        "drop_pending_updates": False,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(_bot_url("setWebhook"), json=body, timeout=20.0)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"setWebhook failed: {data}")


async def answer_callback_query(*, callback_query_id: str, text: str) -> None:
    body = {"callback_query_id": callback_query_id, "text": text, "show_alert": False}
    async with httpx.AsyncClient() as client:
        r = await client.post(_bot_url("answerCallbackQuery"), json=body, timeout=20.0)
        r.raise_for_status()


async def edit_message_reply_markup(*, chat_id: int, message_id: int) -> None:
    body = {"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}}
    async with httpx.AsyncClient() as client:
        r = await client.post(_bot_url("editMessageReplyMarkup"), json=body, timeout=20.0)
        r.raise_for_status()


async def send_message_html(*, chat_id: str, html: str, reply_markup: dict | None = None) -> None:
    body: dict = {
        "chat_id": chat_id,
        "text": html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        body["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        r = await client.post(_bot_url("sendMessage"), json=body, timeout=20.0)
        r.raise_for_status()

