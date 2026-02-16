from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlmodel import SQLModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventStatus(str, Enum):
    pending = "pending"
    processing = "processing"
    done = "done"
    failed = "failed"


class TgMonitorEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    created_at: datetime = Field(default_factory=utcnow, index=True)
    status: EventStatus = Field(default=EventStatus.pending, index=True)

    # Raw payload
    text: str
    chat_title: str = ""
    sender_name: str = ""
    sender_link: str = ""
    sender_handle: str = ""
    link: str = ""
    keywords_json: str = "[]"

    # LLM output
    is_lead: Optional[bool] = Field(default=None, index=True)
    summary: str = ""
    confidence: Optional[float] = None
    model_used: str = ""
    attempts: int = 0
    last_error: str = ""

    # Notification
    notified: bool = Field(default=False, index=True)
    notified_at: Optional[datetime] = None

    # Human feedback from Telegram buttons
    feedback: str = Field(default="", index=True)  # "lead" | "not_lead"
    feedback_at: Optional[datetime] = None
    stopwords_added: str = ""  # newline-separated additions made after feedback


class AppSettings(SQLModel, table=True):
    """
    Single-user settings row (id=1). We keep it in DB so it can be edited via UI.
    """

    id: Optional[int] = Field(default=1, primary_key=True)

    keywords_enabled: bool = Field(default=True)
    stopwords_enabled: bool = Field(default=True)
    llm_enabled: bool = Field(default=True)

    # Newline-separated terms (case-insensitive substring match).
    keywords_text: str = ""
    stopwords_text: str = ""

    # LLM routing settings (runtime-overridable from UI).
    llm_model_priority_text: str = (
        "glm-4.7-flash\n"
        "or-llama-3.3-70b-free\n"
        "or-hermes-3-405b-free\n"
        "or-deepseek-r1-0528-free\n"
        "or-mistral-small-3.1-free\n"
        "or-gpt-oss-120b-free\n"
        "or-step-3.5-flash-free\n"
        "or-qwen3-next-80b-free\n"
        "or-qwen3-coder-free\n"
        "or-qwen3-4b-free\n"
    )
    llm_max_attempts: int = Field(default=3)
    llm_timeout_s: float = Field(default=15.0)
    exclude_openai_owned_models: bool = Field(default=True)

    # Discovery settings (tg-agent).
    discovery_enabled: bool = Field(default=True)
    discovery_queries_text: str = ""
    discovery_interval_s: int = Field(default=1800)
    scan_days: int = Field(default=30)
    scan_max_messages: int = Field(default=300)
    scan_llm_sample: int = Field(default=12)
    join_per_day: int = Field(default=8)
    join_per_hour: int = Field(default=2)
    auto_leave_if_no_candidates: bool = Field(default=True)
    # Search safety limits (to avoid API abuse / account bans).
    search_requests_per_day: int = Field(default=300)
    search_requests_per_hour: int = Field(default=30)
    queries_per_tick: int = Field(default=25)
    search_results_per_query: int = Field(default=20)


class DiscoveryLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    level: str = Field(default="info", index=True)
    event: str = Field(default="", index=True)
    message: str = ""
    chat_username: str = Field(default="", index=True)
    query: str = ""


class DiscoveryTarget(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)

    # Normalized dedupe key, e.g. "user:mychat" / "invite:abcdef".
    target_key: str = Field(default="", index=True)
    target: str = Field(default="", index=True)  # @username or t.me link
    username: str = Field(default="", index=True)
    source: str = Field(default="", index=True)  # tg_search | search_engine | tgstat
    query: str = Field(default="", index=True)

    # Last known status: discovered | joined | left | kept | failed
    status: str = Field(default="discovered", index=True)
    note: str = ""
    queued_join_scan: bool = Field(default=False, index=True)
    queued_at: Optional[datetime] = None
