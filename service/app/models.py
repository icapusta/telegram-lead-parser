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
    # JSON dict with per-model config:
    # { "model_id": { "enabled": true, "timeout_s": 15, "link": "...", "notes": "..." } }
    llm_models_config_json: str = Field(default="{}")

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

    # amoCRM integration
    amo_enabled: bool = Field(default=False)
    amo_subdomain: str = ""
    amo_client_id: str = ""
    amo_client_secret: str = ""
    amo_redirect_uri: str = ""
    amo_access_token: str = ""
    amo_refresh_token: str = ""
    amo_token_expires_at: Optional[datetime] = None
    amo_pipeline_id: int = Field(default=4301503)
    amo_status_unprocessed_id: int = Field(default=40181908)
    amo_status_primary_contact_id: int = Field(default=40181911)
    amo_webhook_secret: str = ""


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


class LLMModelStat(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)

    # YYYY-MM-DD in UTC; enough for per-day aggregates.
    day_utc: str = Field(default="", index=True)
    model_id: str = Field(default="", index=True)

    calls_ok: int = Field(default=0)
    calls_error: int = Field(default=0)
    prompt_tokens: int = Field(default=0)
    completion_tokens: int = Field(default=0)
    total_tokens: int = Field(default=0)

    last_http_status: int = Field(default=0)
    last_latency_ms: int = Field(default=0)
    last_provider_status: str = ""
    last_reset_in_sec: int = Field(default=0)
    last_rate_limits_json: str = "{}"
    last_error: str = ""


class AmoLeadInbox(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)

    amo_lead_id: int = Field(default=0, index=True)
    amo_account_id: int = Field(default=0, index=True)
    source: str = Field(default="webhook", index=True)
    raw_json: str = "{}"

    status: str = Field(default="new", index=True)  # new | sent_to_tg | queued_accept | accepted | rejected | error
    decision: str = Field(default="", index=True)   # accept | reject
    result_json: str = "{}"
    error: str = ""
    accept_attempts: int = Field(default=0)
    accept_started_at: Optional[datetime] = None
    accept_deadline_at: Optional[datetime] = None
    accept_last_try_at: Optional[datetime] = None

    tg_message_id: int = Field(default=0, index=True)
