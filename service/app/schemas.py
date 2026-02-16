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


class SettingsPayload(BaseModel):
    keywords_enabled: bool = True
    stopwords_enabled: bool = True
    llm_enabled: bool = True
    keywords_text: str = ""
    stopwords_text: str = ""


class LLMResult(BaseModel):
    is_lead: bool
    summary: str
    confidence: float | None = None
