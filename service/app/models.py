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

