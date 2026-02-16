from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class TgMonitorPayload(BaseModel):
    text: str = Field(min_length=1)
    chat_title: str = ""
    sender_name: str = ""
    sender_link: str = ""
    sender_handle: str = ""
    link: str = ""
    keywords: List[str] = Field(default_factory=list)


class LLMResult(BaseModel):
    is_lead: bool
    summary: str
    confidence: float | None = None


class InternalClassifyRequest(BaseModel):
    text: str = Field(min_length=1)


class InternalClassifyResponse(BaseModel):
    passed_hard_filter: bool
    hard_filter_reason: str
    is_lead: bool
    summary: str
    confidence: float | None = None
    model_used: str


class InternalDiscoveryLogRequest(BaseModel):
    level: str = "info"
    event: str = ""
    message: str
    chat_username: str = ""
    query: str = ""


class InternalDiscoveryTargetRequest(BaseModel):
    target_key: str = Field(min_length=2)
    target: str = ""
    username: str = ""
    source: str = ""
    query: str = ""
    status: str = "discovered"
    note: str = ""
