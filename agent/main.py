from __future__ import annotations

import asyncio
import json
import os
import re
import time
from urllib.parse import quote_plus, unquote
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import requests
from telethon import TelegramClient, events, utils
from telethon.tl import functions, types
from telethon.errors import UserAlreadyParticipantError


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
    # Allow passing values like "a\\nb\\nc" via env.
    s = (s or "").replace("\\n", "\n")
    for line in s.splitlines():
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


@dataclass(frozen=True)
class DiscoveryConfig:
    enabled: bool
    queries: list[str]
    interval_s: float
    scan_days: int
    scan_max_messages: int
    scan_llm_sample: int
    join_per_day: int
    join_per_hour: int
    auto_leave_if_no_candidates: bool
    search_requests_per_day: int
    search_requests_per_hour: int
    queries_per_tick: int
    search_results_per_query: int


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
PARSER_INTERNAL_CLASSIFY_URL = os.environ.get("PARSER_INTERNAL_CLASSIFY_URL", "http://parser-service:8080/api/internal/classify")
PARSER_INTERNAL_DISCOVERY_LOG_URL = os.environ.get("PARSER_INTERNAL_DISCOVERY_LOG_URL", "http://parser-service:8080/api/internal/discovery/log")
PARSER_INTERNAL_DISCOVERY_TARGET_URL = os.environ.get("PARSER_INTERNAL_DISCOVERY_TARGET_URL", "http://parser-service:8080/api/internal/discovery/target")
PARSER_INTERNAL_TOKEN = os.environ.get("PARSER_INTERNAL_TOKEN", "")

BUSINESS_FOLDER_QUERY = os.environ.get("BUSINESS_FOLDER_QUERY", "бизнес").casefold()

DEFAULT_DISCOVERY_ENABLED = env_bool("DISCOVERY_ENABLED", False)
DEFAULT_DISCOVERY_QUERIES = _split_lines(os.environ.get("DISCOVERY_QUERIES", ""))
DEFAULT_DISCOVERY_INTERVAL_S = float(os.environ.get("DISCOVERY_INTERVAL_S", "1800"))

DEFAULT_SCAN_DAYS = env_int("SCAN_DAYS", 30)
DEFAULT_SCAN_MAX_MESSAGES = env_int("SCAN_MAX_MESSAGES", 300)
DEFAULT_SCAN_LLM_SAMPLE = env_int("SCAN_LLM_SAMPLE", 12)

DEFAULT_JOIN_PER_DAY = env_int("JOIN_PER_DAY", 8)
DEFAULT_JOIN_PER_HOUR = env_int("JOIN_PER_HOUR", 2)
JOIN_BUDGET_PATH = os.environ.get("JOIN_BUDGET_PATH", "/data/join_budget.json")
SEARCH_BUDGET_PATH = os.environ.get("SEARCH_BUDGET_PATH", "/data/search_budget.json")
AGENT_STATE_PATH = os.environ.get("AGENT_STATE_PATH", "/data/agent_state.json")

DEFAULT_AUTO_LEAVE_IF_NO_CANDIDATES = env_bool("AUTO_LEAVE_IF_NO_CANDIDATES", True)
DEFAULT_SEARCH_REQUESTS_PER_DAY = env_int("SEARCH_REQUESTS_PER_DAY", 300)
DEFAULT_SEARCH_REQUESTS_PER_HOUR = env_int("SEARCH_REQUESTS_PER_HOUR", 30)
DEFAULT_QUERIES_PER_TICK = env_int("QUERIES_PER_TICK", 25)
DEFAULT_SEARCH_RESULTS_PER_QUERY = env_int("SEARCH_RESULTS_PER_QUERY", 20)
EXTERNAL_SEARCH_ENABLED = env_bool("EXTERNAL_SEARCH_ENABLED", True)
SEARCH_ENGINE_ENABLED = env_bool("SEARCH_ENGINE_ENABLED", True)
TGSTAT_SEARCH_ENABLED = env_bool("TGSTAT_SEARCH_ENABLED", True)
EXTERNAL_RESULTS_PER_QUERY = env_int("EXTERNAL_RESULTS_PER_QUERY", 8)


client = TelegramClient(SESSION_PATH, API_ID, API_HASH)


class AgentState:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"seen_usernames": [], "seen_targets": []}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if "seen_usernames" not in data:
                data["seen_usernames"] = []
            if "seen_targets" not in data:
                data["seen_targets"] = []
            return data
        except Exception:
            return {"seen_usernames": [], "seen_targets": []}

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data), encoding="utf-8")
        tmp.replace(self.path)

    def seen(self, username: str) -> bool:
        u = (username or "").casefold()
        return u in set(x.casefold() for x in (self.data.get("seen_usernames") or []))

    def mark_seen(self, username: str) -> None:
        u = (username or "").strip()
        if not u:
            return
        arr = list(self.data.get("seen_usernames") or [])
        if u.casefold() in set(x.casefold() for x in arr):
            return
        arr.append(u)
        # keep last 2000
        self.data["seen_usernames"] = arr[-2000:]
        self._save()

    def seen_target(self, target: str) -> bool:
        t = (target or "").casefold()
        return t in set(x.casefold() for x in (self.data.get("seen_targets") or []))

    def mark_target(self, target: str) -> None:
        t = (target or "").strip()
        if not t:
            return
        arr = list(self.data.get("seen_targets") or [])
        if t.casefold() in set(x.casefold() for x in arr):
            return
        arr.append(t)
        # keep last 5000
        self.data["seen_targets"] = arr[-5000:]
        self._save()


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
        id=getattr(flt, "id", fid),
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


async def _remove_from_business(peer) -> bool:
    bf = await _get_business_filter()
    if not bf:
        return False
    fid, flt = bf
    include = list(getattr(flt, "include_peers", []) or [])
    pid = utils.get_peer_id(peer)
    kept = [p for p in include if utils.get_peer_id(p) != pid]
    if len(kept) == len(include):
        return True

    new_filter = types.DialogFilter(
        id=getattr(flt, "id", fid),
        title=flt.title,
        pinned_peers=getattr(flt, "pinned_peers", None),
        include_peers=kept,
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
    m = re.search(r"t\.me/(?:joinchat/|\+)([A-Za-z0-9_-]+)", u)
    if m:
        return m.group(1)
    return None


def _extract_username_from_target(target: str) -> str | None:
    t = (target or "").strip()
    if not t:
        return None
    m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{5,})", t)
    if m:
        return m.group(1).casefold()
    if t.startswith("@") and len(t) >= 6:
        return t[1:].casefold()
    # plain username
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", t):
        return t.casefold()
    return None


def _target_key(target: str) -> str:
    inv = _extract_invite_hash(target)
    if inv:
        return f"invite:{inv.casefold()}"
    u = _extract_username_from_target(target)
    if u:
        return f"user:{u}"
    return (target or "").strip().casefold()


def _extract_tme_targets_from_text(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    src = text or ""
    for m in re.finditer(r"(https?://t\.me/[A-Za-z0-9_+/.-]+)", src):
        t = m.group(1).rstrip(").,;")
        k = _target_key(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    # capture @username mentions as fallback
    for m in re.finditer(r"@([A-Za-z0-9_]{5,})", src):
        t = "@" + m.group(1)
        k = _target_key(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


async def _search_engine_targets(query: str, limit: int) -> list[str]:
    # DuckDuckGo HTML SERP as a lightweight source for t.me links.
    q = f"site:t.me {query}"
    url = f"https://duckduckgo.com/html/?q={quote_plus(q)}"
    try:
        async with httpx.AsyncClient() as h:
            r = await h.get(url, timeout=12.0)
            r.raise_for_status()
            html = r.text
    except Exception:
        return []

    # decode redirected URLs if present.
    decoded = html
    for m in re.finditer(r"uddg=([^\"&]+)", html):
        try:
            decoded += "\n" + unquote(m.group(1))
        except Exception:
            continue
    return _extract_tme_targets_from_text(decoded)[:limit]


async def _tgstat_targets(query: str, limit: int) -> list[str]:
    # TGStat search pages often contain references to telegram links.
    url = f"https://tgstat.ru/search?search={quote_plus(query)}"
    try:
        async with httpx.AsyncClient() as h:
            r = await h.get(url, timeout=12.0)
            r.raise_for_status()
            html = r.text
    except Exception:
        return []
    return _extract_tme_targets_from_text(html)[:limit]


async def _join_target(target: str):
    target = (target or "").strip()
    if not target:
        return None
    inv = _extract_invite_hash(target)
    if inv:
        return await client(functions.messages.ImportChatInviteRequest(hash=inv))
    # username or t.me/username
    m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{5,})", target)
    username = m.group(1) if m else target.lstrip("@")
    ent = await client.get_entity(username)
    await client(functions.channels.JoinChannelRequest(channel=ent))
    return ent


async def _push_discovery_log(*, level: str, event: str, message: str, chat_username: str = "", query: str = "") -> None:
    headers = {"x-internal-token": PARSER_INTERNAL_TOKEN} if PARSER_INTERNAL_TOKEN else {}
    body = {
        "level": (level or "info")[:20],
        "event": (event or "")[:80],
        "message": (message or "")[:2000],
        "chat_username": (chat_username or "")[:120],
        "query": (query or "")[:200],
    }
    try:
        async with httpx.AsyncClient() as h:
            await h.post(PARSER_INTERNAL_DISCOVERY_LOG_URL, headers=headers, json=body, timeout=5.0)
    except Exception:
        pass


async def _log(level: str, event: str, message: str, chat_username: str = "", query: str = "") -> None:
    print(f"[{level}] {event} {chat_username} {query} {message}", flush=True)
    await _push_discovery_log(level=level, event=event, message=message, chat_username=chat_username, query=query)


async def _save_discovery_target(
    *,
    target_key: str,
    target: str,
    username: str,
    source: str,
    query: str,
    status: str,
    note: str = "",
) -> bool | None:
    """
    Upsert target in parser-service.
    Returns:
      True  -> new row created
      False -> duplicate/update existing
      None  -> request failed
    """
    headers = {"x-internal-token": PARSER_INTERNAL_TOKEN} if PARSER_INTERNAL_TOKEN else {}
    body = {
        "target_key": target_key,
        "target": target,
        "username": username,
        "source": source,
        "query": query,
        "status": status,
        "note": note,
    }
    try:
        async with httpx.AsyncClient() as h:
            r = await h.post(PARSER_INTERNAL_DISCOVERY_TARGET_URL, headers=headers, json=body, timeout=8.0)
            r.raise_for_status()
            data = r.json()
            return bool(data.get("created"))
    except Exception:
        return None


async def _fetch_runtime_config() -> tuple[FilterConfig, DiscoveryConfig]:
    headers = {"x-internal-token": PARSER_INTERNAL_TOKEN} if PARSER_INTERNAL_TOKEN else {}
    async with httpx.AsyncClient() as h:
        r = await h.get(PARSER_INTERNAL_SETTINGS_URL, headers=headers, timeout=10.0)
        r.raise_for_status()
        data = r.json()
    filter_cfg = FilterConfig(
        keywords_enabled=bool(data.get("keywords_enabled", True)),
        stopwords_enabled=bool(data.get("stopwords_enabled", True)),
        llm_enabled=bool(data.get("llm_enabled", True)),
        keywords=_split_lines(data.get("keywords_text", "")),
        stopwords=_split_lines(data.get("stopwords_text", "")),
    )
    discovery_cfg = DiscoveryConfig(
        enabled=bool(data.get("discovery_enabled", DEFAULT_DISCOVERY_ENABLED)),
        queries=_split_lines(data.get("discovery_queries_text", "")) or DEFAULT_DISCOVERY_QUERIES,
        interval_s=float(data.get("discovery_interval_s", DEFAULT_DISCOVERY_INTERVAL_S)),
        scan_days=max(1, int(data.get("scan_days", DEFAULT_SCAN_DAYS))),
        scan_max_messages=max(30, int(data.get("scan_max_messages", DEFAULT_SCAN_MAX_MESSAGES))),
        scan_llm_sample=max(1, int(data.get("scan_llm_sample", DEFAULT_SCAN_LLM_SAMPLE))),
        join_per_day=max(1, int(data.get("join_per_day", DEFAULT_JOIN_PER_DAY))),
        join_per_hour=max(1, int(data.get("join_per_hour", DEFAULT_JOIN_PER_HOUR))),
        auto_leave_if_no_candidates=bool(data.get("auto_leave_if_no_candidates", DEFAULT_AUTO_LEAVE_IF_NO_CANDIDATES)),
        search_requests_per_day=max(20, int(data.get("search_requests_per_day", DEFAULT_SEARCH_REQUESTS_PER_DAY))),
        search_requests_per_hour=max(5, int(data.get("search_requests_per_hour", DEFAULT_SEARCH_REQUESTS_PER_HOUR))),
        queries_per_tick=max(1, int(data.get("queries_per_tick", DEFAULT_QUERIES_PER_TICK))),
        search_results_per_query=max(5, min(50, int(data.get("search_results_per_query", DEFAULT_SEARCH_RESULTS_PER_QUERY)))),
    )
    return filter_cfg, discovery_cfg


async def _internal_classify_text(h: httpx.AsyncClient, text: str) -> bool:
    headers = {"x-internal-token": PARSER_INTERNAL_TOKEN} if PARSER_INTERNAL_TOKEN else {}
    r = await h.post(PARSER_INTERNAL_CLASSIFY_URL, headers=headers, json={"text": text}, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    return bool(data.get("is_lead", False))


async def _scan_recent_messages(entity, cfg: FilterConfig, dcfg: DiscoveryConfig) -> tuple[int, int]:
    cutoff = utcnow() - timedelta(days=dcfg.scan_days)
    hard_candidates = 0
    llm_positive = 0
    chat = await client.get_entity(entity)
    chat_username = getattr(chat, "username", None) or ""
    chat_peer_id = utils.get_peer_id(chat)
    clean_id = str(chat_peer_id).replace("-100", "")
    sampled_texts: list[str] = []

    async for msg in client.iter_messages(entity, offset_date=cutoff, limit=dcfg.scan_max_messages):
        if not getattr(msg, "message", None):
            continue
        ok, _reason = hard_pass(cfg, msg.message)
        if not ok:
            continue
        hard_candidates += 1
        if len(sampled_texts) < dcfg.scan_llm_sample:
            sampled_texts.append(msg.message)
        # Push into parser-service for full LLM decision + bot notify.
        try:
            title = getattr(chat, "title", "") or chat_username or "Unknown"
            if chat_username:
                link = f"https://t.me/{chat_username}/{msg.id}"
            else:
                link = f"https://t.me/c/{clean_id}/{msg.id}"
            payload = {
                "text": msg.message,
                "chat_title": title,
                "sender_name": "",
                "sender_link": "",
                "sender_handle": "",
                "link": link,
                "keywords": [],
            }
            requests.post(PARSER_INGEST_URL, json=payload, timeout=10)
        except Exception:
            pass

    # LLM-backed stay/leave signal: stay only if we found at least one lead in sampled history.
    if sampled_texts:
        async with httpx.AsyncClient() as h:
            for t in sampled_texts:
                try:
                    if await _internal_classify_text(h, t):
                        llm_positive += 1
                        break
                except Exception:
                    # Ignore single-sample failures; decision remains conservative.
                    continue

    return hard_candidates, llm_positive


async def _post_join_flow(
    channel_entity,
    *,
    cfg: FilterConfig,
    dcfg: DiscoveryConfig,
    username_for_log: str,
    query: str,
    source: str,
    target: str,
    target_key: str,
) -> None:
    await _save_discovery_target(
        target_key=target_key,
        target=target,
        username=username_for_log.lstrip("@"),
        source=source,
        query=query,
        status="joined",
        note="joined",
    )

    # Add to Business folder.
    try:
        peer = await client.get_input_entity(channel_entity)
        ok = await _add_to_business(peer)
        await _log("info", "business_add", f"ok={ok}", chat_username=username_for_log, query=query)
    except Exception as e:
        await _log("warn", "business_add_failed", str(e), chat_username=username_for_log, query=query)

    # Scan history; leave if no LLM candidates.
    try:
        hard_cands, llm_cands = await _scan_recent_messages(channel_entity, cfg, dcfg)
        await _log(
            "info",
            "scan",
            f"hard_candidates={hard_cands}, llm_candidates={llm_cands}",
            chat_username=username_for_log,
            query=query,
        )
        if dcfg.auto_leave_if_no_candidates and llm_cands == 0:
            try:
                await client(functions.channels.LeaveChannelRequest(channel=channel_entity))
                try:
                    peer = await client.get_input_entity(channel_entity)
                    await _remove_from_business(peer)
                except Exception:
                    pass
                await _log("info", "leave", "no_llm_candidates", chat_username=username_for_log, query=query)
                await _save_discovery_target(
                    target_key=target_key,
                    target=target,
                    username=username_for_log.lstrip("@"),
                    source=source,
                    query=query,
                    status="left",
                    note="no_llm_candidates",
                )
            except Exception as e:
                await _log("error", "leave_failed", str(e), chat_username=username_for_log, query=query)
                await _save_discovery_target(
                    target_key=target_key,
                    target=target,
                    username=username_for_log.lstrip("@"),
                    source=source,
                    query=query,
                    status="failed",
                    note=f"leave_failed:{str(e)[:120]}",
                )
        elif llm_cands > 0:
            await _log("info", "keep", "llm_candidate_found", chat_username=username_for_log, query=query)
            await _save_discovery_target(
                target_key=target_key,
                target=target,
                username=username_for_log.lstrip("@"),
                source=source,
                query=query,
                status="kept",
                note="llm_candidate_found",
            )
    except Exception as e:
        await _log("error", "scan_failed", str(e), chat_username=username_for_log, query=query)
        await _save_discovery_target(
            target_key=target_key,
            target=target,
            username=username_for_log.lstrip("@"),
            source=source,
            query=query,
            status="failed",
            note=f"scan_failed:{str(e)[:120]}",
        )


async def discovery_loop() -> None:
    budget = JoinBudget(path=JOIN_BUDGET_PATH, per_day=DEFAULT_JOIN_PER_DAY, per_hour=DEFAULT_JOIN_PER_HOUR)
    search_budget = JoinBudget(path=SEARCH_BUDGET_PATH, per_day=DEFAULT_SEARCH_REQUESTS_PER_DAY, per_hour=DEFAULT_SEARCH_REQUESTS_PER_HOUR)
    state = AgentState(AGENT_STATE_PATH)
    while True:
        try:
            cfg, dcfg = await _fetch_runtime_config()
            budget.per_day = dcfg.join_per_day
            budget.per_hour = dcfg.join_per_hour
            search_budget.per_day = dcfg.search_requests_per_day
            search_budget.per_hour = dcfg.search_requests_per_hour

            if not dcfg.enabled or not dcfg.queries:
                await asyncio.sleep(30)
                continue

            await _log("info", "tick", f"queries={len(dcfg.queries)} interval={int(dcfg.interval_s)}s")
            processed_queries = 0
            for q in dcfg.queries:
                if processed_queries >= dcfg.queries_per_tick:
                    await _log("warn", "queries_tick_limit", f"limit={dcfg.queries_per_tick}", query=q)
                    break
                processed_queries += 1

                # Telegram search for public groups/channels.
                can_s, why_s = search_budget.can_join()
                if not can_s:
                    await _log("warn", "search_budget_block", why_s, query=q)
                    break
                try:
                    res = await client(functions.contacts.SearchRequest(q=q, limit=dcfg.search_results_per_query))
                    search_budget.mark_join()
                except Exception as e:
                    await _log("error", "search_failed", str(e), query=q)
                    continue

                chats = list(getattr(res, "chats", []) or [])
                await _log("info", "search", f"found={len(chats)}", query=q)
                # Prefer megagroups and channels with linked chats.
                for ch in chats:
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
                    target_key = _target_key("@" + username)
                    created = await _save_discovery_target(
                        target_key=target_key,
                        target="@" + username,
                        username=username,
                        source="tg_search",
                        query=q,
                        status="discovered",
                    )
                    if created is True:
                        await _log("info", "target_saved", "added to table", chat_username=f"@{username}", query=q)
                    elif created is False:
                        await _log("info", "target_duplicate", "already in table", chat_username=f"@{username}", query=q)
                    else:
                        await _log("warn", "target_save_failed", "internal api failed", chat_username=f"@{username}", query=q)
                    if state.seen(username):
                        continue

                    can, why = budget.can_join()
                    if not can:
                        await _log("warn", "budget_block", why, chat_username=f"@{username}", query=q)
                        continue

                    state.mark_seen(username)

                    try:
                        await _log("info", "join_try", "joining", chat_username=f"@{username}", query=q)
                        try:
                            await client(functions.channels.JoinChannelRequest(channel=ch))
                            budget.mark_join()
                            await _log("info", "join_ok", "joined", chat_username=f"@{username}", query=q)
                        except UserAlreadyParticipantError:
                            await _log("info", "join_ok", "already_participant", chat_username=f"@{username}", query=q)
                    except Exception as e:
                        await _log("error", "join_failed", str(e), chat_username=f"@{username}", query=q)
                        await _save_discovery_target(
                            target_key=target_key,
                            target="@" + username,
                            username=username,
                            source="tg_search",
                            query=q,
                            status="failed",
                            note=f"join_failed:{str(e)[:120]}",
                        )
                        continue

                    await _post_join_flow(
                        ch,
                        cfg=cfg,
                        dcfg=dcfg,
                        username_for_log=f"@{username}",
                        query=q,
                        source="tg_search",
                        target="@" + username,
                        target_key=target_key,
                    )

                    # Small pause between joins to be gentle.
                    await asyncio.sleep(8)

                # Extra sources: search engines + TGStat.
                external_targets: list[tuple[str, str]] = []
                if EXTERNAL_SEARCH_ENABLED:
                    if SEARCH_ENGINE_ENABLED:
                        can_s, why_s = search_budget.can_join()
                        if can_s:
                            external_targets.extend([("search_engine", t) for t in (await _search_engine_targets(q, dcfg.search_results_per_query))])
                            search_budget.mark_join()
                        else:
                            await _log("warn", "search_budget_block", why_s, query=q)
                    if TGSTAT_SEARCH_ENABLED:
                        can_s, why_s = search_budget.can_join()
                        if can_s:
                            external_targets.extend([("tgstat", t) for t in (await _tgstat_targets(q, dcfg.search_results_per_query))])
                            search_budget.mark_join()
                        else:
                            await _log("warn", "search_budget_block", why_s, query=q)

                # Dedupe by normalized key and cap attempts per query.
                uniq_targets: list[tuple[str, str]] = []
                seen_keys: set[str] = set()
                for src, t in external_targets:
                    k = _target_key(t)
                    if not k or k in seen_keys:
                        continue
                    seen_keys.add(k)
                    uniq_targets.append((src, t))
                    if len(uniq_targets) >= dcfg.search_results_per_query:
                        break

                if uniq_targets:
                    await _log("info", "external_search", f"targets={len(uniq_targets)}", query=q)

                for src, target in uniq_targets:
                    tk = _target_key(target)
                    username = _extract_username_from_target(target) or ""
                    created = await _save_discovery_target(
                        target_key=tk,
                        target=target,
                        username=username,
                        source=src,
                        query=q,
                        status="discovered",
                    )
                    if created is True:
                        await _log("info", "target_saved", "added to table", chat_username=("@" + username) if username else target, query=q)
                    elif created is False:
                        await _log("info", "target_duplicate", "already in table", chat_username=("@" + username) if username else target, query=q)
                    else:
                        await _log("warn", "target_save_failed", "internal api failed", chat_username=("@" + username) if username else target, query=q)
                    if state.seen_target(tk):
                        continue
                    can, why = budget.can_join()
                    if not can:
                        await _log("warn", "budget_block", why, chat_username=("@" + username) if username else target, query=q)
                        continue

                    state.mark_target(tk)

                    if username and state.seen(username):
                        continue
                    if username:
                        state.mark_seen(username)

                    display_name = ("@" + username) if username else target
                    try:
                        await _log("info", "join_try", f"joining external target={target}", chat_username=display_name, query=q)
                        joined = await _join_target(target)
                        budget.mark_join()
                        await _log("info", "join_ok", "joined external", chat_username=display_name, query=q)
                    except UserAlreadyParticipantError:
                        await _log("info", "join_ok", "already_participant external", chat_username=display_name, query=q)
                        try:
                            joined = await client.get_entity(username or target)
                        except Exception:
                            joined = None
                    except Exception as e:
                        await _log("error", "join_failed", str(e), chat_username=display_name, query=q)
                        await _save_discovery_target(
                            target_key=tk,
                            target=target,
                            username=username,
                            source=src,
                            query=q,
                            status="failed",
                            note=f"join_failed:{str(e)[:120]}",
                        )
                        continue

                    # Resolve a channel-like entity for post-join processing.
                    channel_entity = None
                    if isinstance(joined, types.Channel):
                        channel_entity = joined
                    else:
                        try:
                            channel_entity = await client.get_entity(username or target)
                        except Exception:
                            channel_entity = None

                    if not isinstance(channel_entity, types.Channel):
                        await _log("warn", "skip_non_channel", "joined target is not channel/group", chat_username=display_name, query=q)
                        continue

                    await _post_join_flow(
                        channel_entity,
                        cfg=cfg,
                        dcfg=dcfg,
                        username_for_log=display_name,
                        query=q,
                        source=src,
                        target=target,
                        target_key=tk,
                    )
                    await asyncio.sleep(8)

            await asyncio.sleep(max(60.0, float(dcfg.interval_s)))
        except Exception as e:
            await _log("error", "loop_error", str(e))
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
    if DEFAULT_DISCOVERY_ENABLED and not PARSER_INTERNAL_TOKEN:
        print("DISCOVERY_ENABLED=1 but PARSER_INTERNAL_TOKEN is empty", flush=True)

    await client.start()
    tasks = [asyncio.create_task(monitor_loop())]
    tasks.append(asyncio.create_task(discovery_loop()))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
