from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import requests
from telethon import TelegramClient, events, utils
from telethon.tl import functions, types


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or not str(v).strip():
        return default
    return int(v)


def _split_lines(s: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for line in (s or "").splitlines():
        t = line.strip()
        if not t:
            continue
        t = t.casefold()
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


@dataclass(frozen=True)
class FilterConfig:
    keywords_enabled: bool
    stopwords_enabled: bool
    llm_enabled: bool
    keywords: list[str]
    stopwords: list[str]


def hard_pass(cfg: FilterConfig, text: str) -> tuple[bool, str]:
    t = (text or "").casefold()
    if cfg.stopwords_enabled and cfg.stopwords:
        for sw in cfg.stopwords:
            if sw and sw in t:
                return False, f"stopword:{sw}"
    if cfg.keywords_enabled and cfg.keywords:
        for kw in cfg.keywords:
            if kw and kw in t:
                return True, "ok"
        return False, "no_keywords"
    return True, "ok"


class JoinBudget:
    def __init__(self, *, path: str, per_day: int, per_hour: int) -> None:
        self.path = Path(path)
        self.per_day = per_day
        self.per_hour = per_hour
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"day": "", "day_count": 0, "hour": "", "hour_count": 0}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {"day": "", "day_count": 0, "hour": "", "hour_count": 0}

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state), encoding="utf-8")
        tmp.replace(self.path)

    def can_join(self) -> tuple[bool, str]:
        now = utcnow()
        day_key = now.strftime("%Y-%m-%d")
        hour_key = now.strftime("%Y-%m-%dT%H")
        if self.state.get("day") != day_key:
            self.state["day"] = day_key
            self.state["day_count"] = 0
        if self.state.get("hour") != hour_key:
            self.state["hour"] = hour_key
            self.state["hour_count"] = 0

        if int(self.state.get("day_count", 0)) >= self.per_day:
            return False, "day_budget"
        if int(self.state.get("hour_count", 0)) >= self.per_hour:
            return False, "hour_budget"
        return True, "ok"

    def mark_join(self) -> None:
        self.state["day_count"] = int(self.state.get("day_count", 0)) + 1
        self.state["hour_count"] = int(self.state.get("hour_count", 0)) + 1
        self._save()


API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_PATH = os.environ.get("TG_SESSION_PATH", "/data/monitor_session")

PARSER_INGEST_URL = os.environ.get("PARSER_INGEST_URL", "http://parser-service:8080/api/ingest/tg-monitor")
PARSER_INTERNAL_SETTINGS_URL = os.environ.get("PARSER_INTERNAL_SETTINGS_URL", "http://parser-service:8080/api/internal/settings")
PARSER_INTERNAL_TOKEN = os.environ.get("PARSER_INTERNAL_TOKEN", "")

BUSINESS_FOLDER_QUERY = os.environ.get("BUSINESS_FOLDER_QUERY", "бизнес").casefold()

DISCOVERY_ENABLED = env_bool("DISCOVERY_ENABLED", False)
DISCOVERY_QUERIES = _split_lines(os.environ.get("DISCOVERY_QUERIES", ""))
DISCOVERY_INTERVAL_S = float(os.environ.get("DISCOVERY_INTERVAL_S", "1800"))  # 30m

SCAN_DAYS = env_int("SCAN_DAYS", 30)
SCAN_MAX_MESSAGES = env_int("SCAN_MAX_MESSAGES", 300)

JOIN_PER_DAY = env_int("JOIN_PER_DAY", 8)
JOIN_PER_HOUR = env_int("JOIN_PER_HOUR", 2)
JOIN_BUDGET_PATH = os.environ.get("JOIN_BUDGET_PATH", "/data/join_budget.json")

AUTO_LEAVE_IF_NO_CANDIDATES = env_bool("AUTO_LEAVE_IF_NO_CANDIDATES", True)


client = TelegramClient(SESSION_PATH, API_ID, API_HASH)


async def _get_business_filter() -> tuple[int, types.DialogFilter] | None:
    resp = await client(functions.messages.GetDialogFiltersRequest())
    filters = getattr(resp, "filters", resp) or []
    for f in filters:
        if hasattr(f, "title"):
            title = f.title.text if hasattr(f.title, "text") else str(f.title)
            if BUSINESS_FOLDER_QUERY in title.strip().casefold():
                # Telethon uses id on filter object
                fid = getattr(f, "id", None)
                if fid is None:
                    fid = getattr(f, "filter_id", None)
                if fid is None:
                    # Some builds use index position; last resort: keep None
                    continue
                return int(fid), f
    return None


async def _add_to_business(peer) -> bool:
    bf = await _get_business_filter()
    if not bf:
        return False
    fid, flt = bf
    include = list(getattr(flt, "include_peers", []) or [])
    pid = utils.get_peer_id(peer)
    existing = {utils.get_peer_id(p) for p in include}
    if pid in existing:
        return True
    include.append(peer)
    new_filter = types.DialogFilter(
        title=flt.title,
        pinned_peers=getattr(flt, "pinned_peers", None),
        include_peers=include,
        exclude_peers=getattr(flt, "exclude_peers", None),
        groups=getattr(flt, "groups", None),
        broadcasts=getattr(flt, "broadcasts", None),
        bots=getattr(flt, "bots", None),
        contacts=getattr(flt, "contacts", None),
        non_contacts=getattr(flt, "non_contacts", None),
        exclude_muted=getattr(flt, "exclude_muted", None),
        exclude_read=getattr(flt, "exclude_read", None),
        exclude_archived=getattr(flt, "exclude_archived", None),
        emoticon=getattr(flt, "emoticon", None),
        color=getattr(flt, "color", None),
    )
    await client(functions.messages.UpdateDialogFilterRequest(id=fid, filter=new_filter))
    return True


def _extract_invite_hash(url: str) -> str | None:
    u = (url or "").strip()
    m = re.search(r"t\\.me/(?:joinchat/|\\+)([A-Za-z0-9_-]+)", u)
    if m:
        return m.group(1)
    return None


async def _join_target(target: str):
    target = (target or "").strip()
    if not target:
        return None
    inv = _extract_invite_hash(target)
    if inv:
        return await client(functions.messages.ImportChatInviteRequest(hash=inv))
    # username or t.me/username
    m = re.search(r"(?:https?://)?t\\.me/([A-Za-z0-9_]{5,})", target)
    username = m.group(1) if m else target.lstrip("@")
    ent = await client.get_entity(username)
    await client(functions.channels.JoinChannelRequest(channel=ent))
    return ent


async def _fetch_filter_config() -> FilterConfig:
    headers = {"x-internal-token": PARSER_INTERNAL_TOKEN} if PARSER_INTERNAL_TOKEN else {}
    async with httpx.AsyncClient() as h:
        r = await h.get(PARSER_INTERNAL_SETTINGS_URL, headers=headers, timeout=10.0)
        r.raise_for_status()
        data = r.json()
    return FilterConfig(
        keywords_enabled=bool(data.get("keywords_enabled", True)),
        stopwords_enabled=bool(data.get("stopwords_enabled", True)),
        llm_enabled=bool(data.get("llm_enabled", True)),
        keywords=_split_lines(data.get("keywords_text", "")),
        stopwords=_split_lines(data.get("stopwords_text", "")),
    )


async def _scan_recent_messages(entity, cfg: FilterConfig) -> int:
    cutoff = utcnow() - timedelta(days=SCAN_DAYS)
    candidates = 0
    async for msg in client.iter_messages(entity, offset_date=cutoff, limit=SCAN_MAX_MESSAGES):
        if not getattr(msg, "message", None):
            continue
        ok, _reason = hard_pass(cfg, msg.message)
        if not ok:
            continue
        candidates += 1
        # Push into parser-service for full LLM decision + bot notify.
        try:
            chat = await client.get_entity(entity)
            title = getattr(chat, "title", "") or getattr(chat, "username", "") or "Unknown"
            payload = {
                "text": msg.message,
                "chat_title": title,
                "sender_name": "",
                "sender_link": "",
                "sender_handle": "",
                "link": "",
                "keywords": [],
            }
            requests.post(PARSER_INGEST_URL, json=payload, timeout=10)
        except Exception:
            pass
        # Keep it small; we only need a signal.
        if candidates >= 12:
            break
    return candidates


async def discovery_loop() -> None:
    budget = JoinBudget(path=JOIN_BUDGET_PATH, per_day=JOIN_PER_DAY, per_hour=JOIN_PER_HOUR)
    while True:
        try:
            if not DISCOVERY_ENABLED or not DISCOVERY_QUERIES:
                await asyncio.sleep(30)
                continue

            cfg = await _fetch_filter_config()
            for q in DISCOVERY_QUERIES:
                can, why = budget.can_join()
                if not can:
                    break

                # Telegram search for public groups/channels.
                try:
                    res = await client(functions.contacts.SearchRequest(q=q, limit=20))
                except Exception:
                    continue

                chats = list(getattr(res, "chats", []) or [])
                # Prefer megagroups and channels with linked chats.
                for ch in chats:
                    can, why = budget.can_join()
                    if not can:
                        break

                    if isinstance(ch, types.Channel):
                        is_megagroup = bool(getattr(ch, "megagroup", False))
                        has_linked = bool(getattr(ch, "linked_chat_id", None))
                        if not is_megagroup and not has_linked:
                            continue
                    else:
                        continue

                    username = getattr(ch, "username", None)
                    if not username:
                        # We can only join public targets without username via invite links (handled separately).
                        continue

                    try:
                        await client(functions.channels.JoinChannelRequest(channel=ch))
                        budget.mark_join()
                    except Exception:
                        continue

                    # Add to Business folder.
                    try:
                        peer = await client.get_input_entity(ch)
                        await _add_to_business(peer)
                    except Exception:
                        pass

                    # Scan last N days; if no candidates, optionally leave.
                    try:
                        cands = await _scan_recent_messages(ch, cfg)
                        if AUTO_LEAVE_IF_NO_CANDIDATES and cands == 0:
                            try:
                                await client(functions.channels.LeaveChannelRequest(channel=ch))
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # Small pause between joins to be gentle.
                    await asyncio.sleep(8)

            await asyncio.sleep(DISCOVERY_INTERVAL_S)
        except Exception:
            await asyncio.sleep(60)


async def monitor_loop() -> None:
    watch_chat_ids: list[int] = []

    async def refresh_watch() -> None:
        nonlocal watch_chat_ids
        bf = await _get_business_filter()
        if not bf:
            watch_chat_ids = []
            return
        _fid, flt = bf
        ids = []
        for peer in getattr(flt, "include_peers", []) or []:
            ids.append(utils.get_peer_id(peer))
        watch_chat_ids = ids
        print(f"watching {len(watch_chat_ids)} business chats", flush=True)

    await refresh_watch()

    @client.on(events.NewMessage(incoming=True))
    async def _on_new_message(event):
        try:
            if not event.message or not event.message.message:
                return
            if watch_chat_ids and event.chat_id not in watch_chat_ids:
                return

            chat = await event.get_chat()
            sender = await event.get_sender()

            chat_username = getattr(chat, "username", "") or ""
            if chat_username:
                msg_link = f"https://t.me/{chat_username}/{event.message.id}"
            else:
                clean_id = str(event.chat_id).replace("-100", "")
                msg_link = f"https://t.me/c/{clean_id}/{event.message.id}"

            sender_username = getattr(sender, "username", None)
            if sender_username:
                user_link = f"https://t.me/{sender_username}"
                user_handle = f"@{sender_username}"
            else:
                user_link = f"tg://user?id={sender.id}"
                user_handle = "No Username"

            payload = {
                "text": event.message.message,
                "chat_title": getattr(chat, "title", "Unknown") or "Unknown",
                "sender_name": getattr(sender, "first_name", "Unknown") or "Unknown",
                "sender_link": user_link,
                "sender_handle": user_handle,
                "link": msg_link,
                "keywords": [],
            }
            requests.post(PARSER_INGEST_URL, json=payload, timeout=10)
        except Exception as e:
            print(f"monitor error: {e}", flush=True)

    # Periodically refresh folder membership (in case you add/remove chats manually).
    while True:
        await asyncio.sleep(300)
        try:
            await refresh_watch()
        except Exception:
            pass


async def main() -> None:
    # Simple guard: internal token is required for discovery scans (settings fetch).
    if DISCOVERY_ENABLED and not PARSER_INTERNAL_TOKEN:
        print("DISCOVERY_ENABLED=1 but PARSER_INTERNAL_TOKEN is empty", flush=True)

    await client.start()
    tasks = [asyncio.create_task(monitor_loop())]
    if DISCOVERY_ENABLED:
        tasks.append(asyncio.create_task(discovery_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
