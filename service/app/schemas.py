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
    queued_join_scan: bool | None = None


class LLMModelTestRequest(BaseModel):
    model_id: str = Field(min_length=1)
    timeout_s: float | None = None


class LLMModelsBulkTestRequest(BaseModel):
    model_ids: list[str] = Field(default_factory=list)
    limit: int = 20
    timeout_s: float | None = None


class ProviderModelsRequest(BaseModel):
    provider: str = Field(min_length=2)
    api_key: str = ""
    base_url: str = ""
