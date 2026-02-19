from __future__ import annotations

import asyncio
import json
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import re

import httpx
from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlmodel import select

from .config import settings
from .db import init_db, session
from .models import TgMonitorEvent, EventStatus, AppSettings, DiscoveryLog, DiscoveryTarget, LLMModelStat, AmoLeadInbox
from .schemas import (
    TgMonitorPayload,
    InternalClassifyRequest,
    InternalClassifyResponse,
    InternalDiscoveryLogRequest,
    InternalDiscoveryTargetRequest,
    LLMModelTestRequest,
    LLMModelsBulkTestRequest,
    ProviderModelsRequest,
)
from .filtering import hard_filter, request_intent_filter
from .llm import (
    classify,
    suggest_stopwords,
    list_models,
    model_block_reason,
    test_model_availability,
    LLMRoutingOptions,
    ModelInfo,
    default_model_priority_text,
)
from .notify import send_event_notification, utcnow
from .tg_bot_api import set_webhook, answer_callback_query, edit_message_reply_markup, send_message_html


app = FastAPI(title="telegram-lead-parser", version="0.1.0")
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()
MSK_TZ = timezone(timedelta(hours=3))


@app.middleware("http")
async def no_cache_middleware(request: Request, call_next):
    response = await call_next(request)
    # UI/JSON should always reflect latest deploy/settings without stale browser cache.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

DEFAULT_DISCOVERY_QUERIES_TEXT = (
    "Р С‘РЎвЂ°РЎС“ Р С—Р С•Р Т‘РЎР‚РЎРЏР Т‘РЎвЂЎР С‘Р С”Р В°\n"
    "Р Р…РЎС“Р В¶Р ВµР Р… РЎРѓР С—Р ВµРЎвЂ Р С‘Р В°Р В»Р С‘РЎРѓРЎвЂљ\n"
    "Р С‘РЎвЂ°Р ВµР С Р С‘РЎРѓР С—Р С•Р В»Р Р…Р С‘РЎвЂљР ВµР В»РЎРЏ\n"
    "РЎвЂЎР В°РЎвЂљ Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р ВµР в„–\n"
    "Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ Р С”Р В»РЎС“Р В± РЎвЂЎР В°РЎвЂљ\n"
    "Р Р…Р ВµРЎвЂљР Р†Р С•РЎР‚Р С”Р С‘Р Р…Р С– РЎвЂЎР В°РЎвЂљ\n"
    "Р В±Р С‘РЎР‚Р В¶Р В° Р В·Р В°Р С”Р В°Р В·Р С•Р Р†\n"
    "РЎвЂљР ВµР Р…Р Т‘Р ВµРЎР‚РЎвЂ№ Р Р…Р В° РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљР С”РЎС“\n"
    "Р С—РЎР‚Р С•Р ВµР С”РЎвЂљРЎвЂ№ Р Р…Р В° Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂ Р С‘РЎР‹\n"
    "crm РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р Р…Р ВµР Т‘РЎР‚Р ВµР Р…Р С‘Р Вµ crm\n"
    "Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚РЎвЂ№ crm\n"
    "Р Р…Р В°РЎРѓРЎвЂљРЎР‚Р С•Р в„–Р С”Р В° crm\n"
    "Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В°\n"
    "Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ Р С•РЎвЂљР Т‘Р ВµР В»Р В° Р С—РЎР‚Р С•Р Т‘Р В°Р В¶\n"
    "Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ Р С—РЎР‚Р С•РЎвЂ Р ВµРЎРѓРЎРѓРЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•Р Р…РЎвЂљРЎР‚Р С•Р В»РЎРЉ Р СР ВµР Р…Р ВµР Т‘Р В¶Р ВµРЎР‚Р С•Р Р†\n"
    "Р Р†Р С•РЎР‚Р С•Р Р…Р С”Р С‘ Р С—РЎР‚Р С•Р Т‘Р В°Р В¶ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓР С”Р Р†Р С•Р В·Р Р…Р В°РЎРЏ Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "bi Р В°Р Р…Р В°Р В»Р С‘РЎвЂљР С‘Р С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "amocrm РЎвЂЎР В°РЎвЂљ\n"
    "amocrm Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚РЎвЂ№\n"
    "amocrm Р С—Р В°РЎР‚РЎвЂљР Р…Р ВµРЎР‚РЎвЂ№\n"
    "amocrm РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµРЎРѓРЎвЂљР Р†Р С•\n"
    "Р В±Р С‘РЎвЂљРЎР‚Р С‘Р С”РЎРѓ24 РЎвЂЎР В°РЎвЂљ\n"
    "Р В±Р С‘РЎвЂљРЎР‚Р С‘Р С”РЎРѓ24 РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С”Р С‘\n"
    "Р В±Р С‘РЎвЂљРЎР‚Р С‘Р С”РЎРѓ24 Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚РЎвЂ№\n"
    "bitrix24 community\n"
    "yclients РЎвЂЎР В°РЎвЂљ\n"
    "yclients Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚РЎвЂ№\n"
    "getcourse РЎвЂЎР В°РЎвЂљ\n"
    "getcourse Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂ Р С‘РЎРЏ\n"
    "retailcrm РЎвЂЎР В°РЎвЂљ\n"
    "megaplan РЎвЂЎР В°РЎвЂљ\n"
    "Р СР С•Р в„–РЎРѓР С”Р В»Р В°Р Т‘ РЎвЂЎР В°РЎвЂљ\n"
    "1РЎРѓ crm РЎвЂЎР В°РЎвЂљ\n"
    "digital Р В°Р С–Р ВµР Р…РЎвЂљРЎРѓРЎвЂљР Р†Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р СР В°РЎР‚Р С”Р ВµРЎвЂљР С‘Р Р…Р С– РЎвЂЎР В°РЎвЂљ\n"
    "smm РЎвЂЎР В°РЎвЂљ\n"
    "seo РЎвЂЎР В°РЎвЂљ\n"
    "web РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "backend РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "frontend РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "python РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "php РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљРЎвЂЎР С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "laravel РЎвЂЎР В°РЎвЂљ\n"
    "node js РЎвЂЎР В°РЎвЂљ\n"
    "Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂ Р С‘РЎРЏ api\n"
    "webhook РЎвЂЎР В°РЎвЂљ\n"
    "it РЎвЂћРЎР‚Р С‘Р В»Р В°Р Р…РЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "it Р В±Р С‘РЎР‚Р В¶Р В° Р В·Р В°Р С”Р В°Р В·Р С•Р Р†\n"
    "saas РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљР В°РЎР‚РЎвЂљР В°Р С— РЎвЂЎР В°РЎвЂљ\n"
    "product manager РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В° РЎвЂЎР В°РЎвЂљ\n"
    "Р СР В°Р В»РЎвЂ№Р в„– Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎР‚Р ВµР Т‘Р Р…Р С‘Р в„– Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "b2b Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚РЎС“Р С”Р С•Р Р†Р С•Р Т‘Р С‘РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚Р В° РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓР С•Р В±РЎРѓРЎвЂљР Р†Р ВµР Р…Р Р…Р С‘Р С”Р С‘ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В° РЎвЂЎР В°РЎвЂљ\n"
    "ceo РЎвЂЎР В°РЎвЂљ\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В° РЎвЂЎР В°РЎвЂљ\n"
    "Р С•РЎвЂљР Т‘Р ВµР В» Р С—РЎР‚Р С•Р Т‘Р В°Р В¶ РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚Р С•Р С— РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚РЎС“Р С”Р С•Р Р†Р С•Р Т‘Р С‘РЎвЂљР ВµР В»РЎРЉ Р С•РЎвЂљР Т‘Р ВµР В»Р В° Р С—РЎР‚Р С•Р Т‘Р В°Р В¶ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С‘Р Т‘РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С•Р Р…Р В»Р В°Р в„–Р Р… РЎв‚¬Р С”Р С•Р В»Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ Р С•Р Р…Р В»Р В°Р в„–Р Р… РЎв‚¬Р С”Р С•Р В» РЎвЂЎР В°РЎвЂљ\n"
    "Р С‘Р Р…РЎвЂћР С•Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "Р В·Р В°Р С—РЎС“РЎРѓР С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "edtech РЎвЂЎР В°РЎвЂљ\n"
    "Р В±РЎРЉРЎР‹РЎвЂљР С‘ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓР В°Р В»Р С•Р Р… Р С”РЎР‚Р В°РЎРѓР С•РЎвЂљРЎвЂ№ Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•РЎРѓР СР ВµРЎвЂљР С•Р В»Р С•Р С–Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљР С•Р СР В°РЎвЂљР С•Р В»Р С•Р С–Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂЎР В°РЎРѓРЎвЂљР Р…Р В°РЎРЏ Р С”Р В»Р С‘Р Р…Р С‘Р С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ Р С”Р В»Р С‘Р Р…Р С‘Р С” РЎвЂЎР В°РЎвЂљ\n"
    "Р В°Р С–Р ВµР Р…РЎвЂљРЎРѓРЎвЂљР Р†Р С• Р Р…Р ВµР Т‘Р Р†Р С‘Р В¶Р С‘Р СР С•РЎРѓРЎвЂљР С‘ РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚Р С‘РЎРЊР В»РЎвЂљР С•РЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р В·Р В°РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎвЂ°Р С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р В°Р Р†РЎвЂљР С•РЎРѓР ВµРЎР‚Р Р†Р С‘РЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљР С• Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С‘Р Р…РЎвЂљР ВµРЎР‚Р Р…Р ВµРЎвЂљ Р СР В°Р С–Р В°Р В·Р С‘Р Р… РЎвЂЎР В°РЎвЂљ\n"
    "ecommerce РЎвЂЎР В°РЎвЂљ\n"
    "wildberries Р С—РЎР‚Р С•Р Т‘Р В°Р Р†РЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "ozon Р С—РЎР‚Р С•Р Т‘Р В°Р Р†РЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "shopify РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С•Р С—РЎвЂљР С•Р Р†Р С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р С‘РЎРѓРЎвЂљРЎР‚Р С‘Р В±РЎРЉРЎР‹РЎвЂљР С•РЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚Р ВµРЎРѓРЎвЂљР С•РЎР‚Р В°Р Р…Р Р…РЎвЂ№Р в„– Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ Р С”Р В°РЎвЂћР Вµ РЎвЂЎР В°РЎвЂљ\n"
    "horeca РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂћР С‘РЎвЂљР Р…Р ВµРЎРѓ Р С”Р В»РЎС“Р В± Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎРѓР С‘РЎвЂ¦Р С•Р В»Р С•Р С–Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•РЎС“РЎвЂЎР С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•Р Р…РЎРѓРЎС“Р В»РЎРЉРЎвЂљР В°Р Р…РЎвЂљРЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С‘РЎвЂЎР Р…РЎвЂ№Р в„– Р В±РЎР‚Р ВµР Р…Р Т‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘Р С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р С–РЎР‚РЎС“Р В·Р С•Р С—Р ВµРЎР‚Р ВµР Р†Р С•Р В·Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉР Р…Р В°РЎРЏ Р С”Р С•Р СР С—Р В°Р Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—Р С•Р Т‘РЎР‚РЎРЏР Т‘РЎвЂЎР С‘Р С”Р С‘ РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С• РЎвЂЎР В°РЎвЂљ\n"
    "РЎР‚Р ВµР СР С•Р Р…РЎвЂљ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓР ВµРЎР‚Р Р†Р С‘РЎРѓР Р…РЎвЂ№Р в„– Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ\n"
    "crm РЎвЂЎР В°РЎвЂљ Р СР С•РЎРѓР С”Р Р†Р В°\n"
    "amocrm РЎвЂЎР В°РЎвЂљ Р СР С•РЎРѓР С”Р Р†Р В°\n"
    "Р В±Р С‘РЎвЂљРЎР‚Р С‘Р С”РЎРѓ24 РЎвЂЎР В°РЎвЂљ Р СР С•РЎРѓР С”Р Р†Р В°\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ Р СР С•РЎРѓР С”Р Р†Р В°\n"
    "crm РЎвЂЎР В°РЎвЂљ РЎРѓР С—Р В±\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ РЎРѓР С—Р В±\n"
    "crm РЎвЂЎР В°РЎвЂљ Р ВµР С”Р В°РЎвЂљР ВµРЎР‚Р С‘Р Р…Р В±РЎС“РЎР‚Р С–\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ Р С”Р В°Р В·Р В°Р Р…РЎРЉ\n"
    "crm РЎвЂЎР В°РЎвЂљ Р С”РЎР‚Р В°РЎРѓР Р…Р С•Р Т‘Р В°РЎР‚\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ Р Р…Р С•Р Р†Р С•РЎРѓР С‘Р В±Р С‘РЎР‚РЎРѓР С”\n"
    "crm РЎвЂЎР В°РЎвЂљ Р В°Р В»Р СР В°РЎвЂљРЎвЂ№\n"
    "Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ Р В°РЎРѓРЎвЂљР В°Р Р…Р В°\n"
    "Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ Р В±Р ВµР В»Р В°РЎР‚РЎС“РЎРѓРЎРЉ\n"
    "РЎР‚РЎС“РЎРѓРЎРѓР С”Р С•РЎРЏР В·РЎвЂ№РЎвЂЎР Р…РЎвЂ№Р Вµ Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎвЂЎР В°РЎвЂљ Р Т‘РЎС“Р В±Р В°Р в„–\n"
    "РЎвЂЎР В°РЎвЂљ Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р ВµР в„– РЎРѓР Р…Р С–\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В° + Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ\n"
    "digital Р В°Р С–Р ВµР Р…РЎвЂљРЎРѓРЎвЂљР Р†Р В° + Р С—Р С•Р Т‘РЎР‚РЎРЏР Т‘РЎвЂЎР С‘Р С”Р С‘\n"
    "Р В±РЎРЉРЎР‹РЎвЂљР С‘ Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ + Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ\n"
    "Р СР ВµР Т‘Р С‘РЎвЂ Р С‘Р Р…Р В° + Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ crm\n"
    "Р С•Р Р…Р В»Р В°Р в„–Р Р… РЎв‚¬Р С”Р С•Р В»РЎвЂ№ + Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂ Р С‘Р С‘\n"
    "Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ b2b\n"
    "b2b Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р ВµР Р…Р Р…РЎвЂ№Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С•Р С—РЎвЂљР С•Р Р†Р В°РЎРЏ РЎвЂљР С•РЎР‚Р С–Р С•Р Р†Р В»РЎРЏ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р С‘РЎРѓРЎвЂљРЎР‚Р С‘Р В±РЎРЉРЎР‹РЎвЂљР С•РЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р В·Р В°Р Р†Р С•Р Т‘ Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "РЎвЂћР В°Р В±РЎР‚Р С‘Р С”Р В° Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘\n"
    "Р С—РЎР‚Р С•Р СРЎвЂ№РЎв‚¬Р В»Р ВµР Р…Р Р…Р С•РЎРѓРЎвЂљРЎРЉ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРЊР С”РЎРѓР С—Р С•РЎР‚РЎвЂљРЎвЂРЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С‘Р СР С—Р С•РЎР‚РЎвЂљРЎвЂРЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•Р Р…РЎвЂљРЎР‚Р В°Р С”РЎвЂљР Р…Р С•Р Вµ Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂљР ВµР Р…Р Т‘Р ВµРЎР‚Р Р…РЎвЂ№Р Вµ Р С—РЎР‚Р С•Р Т‘Р В°Р В¶Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С–Р С•РЎРѓР В·Р В°Р С”Р В°Р В·РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "b2b + Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• + Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘\n"
    "Р С•Р С—РЎвЂљР С•Р Р†Р В°РЎРЏ Р С”Р С•Р СР С—Р В°Р Р…Р С‘РЎРЏ + РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉР Р…Р В°РЎРЏ Р С”Р С•Р СР С—Р В°Р Р…Р С‘РЎРЏ РЎвЂЎР В°РЎвЂљ\n"
    "Р В·Р В°РЎРѓРЎвЂљРЎР‚Р С•Р в„–РЎвЂ°Р С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р ВµР Р†Р ВµР В»Р С•Р С—Р ВµРЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р С–Р ВµР Р…Р С—Р С•Р Т‘РЎР‚РЎРЏР Т‘РЎвЂЎР С‘Р С”Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р В°РЎРЏ Р Р…Р ВµР Т‘Р Р†Р С‘Р В¶Р С‘Р СР С•РЎРѓРЎвЂљРЎРЉ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р СРЎвЂ№РЎв‚¬Р В»Р ВµР Р…Р Р…Р С•Р Вµ РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С•\n"
    "Р С‘Р Р…Р В¶Р ВµР Р…Р ВµРЎР‚Р Р…РЎвЂ№Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р ВµР С”РЎвЂљР Р…РЎвЂ№Р Вµ Р В±РЎР‹РЎР‚Р С• РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "Р В°Р Р†РЎвЂљР С•Р СР В°РЎвЂљР С‘Р В·Р В°РЎвЂ Р С‘РЎРЏ Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р В° РЎвЂЎР В°РЎвЂљ\n"
    "РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С•Р С\n"
    "erp РЎвЂЎР В°РЎвЂљ\n"
    "1РЎРѓ Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘РЎРЏРЎвЂљР С‘Р Вµ РЎвЂЎР В°РЎвЂљ\n"
    "1РЎРѓ Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂ Р С‘РЎРЏ\n"
    "mes РЎРѓР С‘РЎРѓРЎвЂљР ВµР СРЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘РЎвЂЎР ВµРЎРѓР С”Р С‘Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С–РЎР‚РЎС“Р В·Р С•Р С—Р ВµРЎР‚Р ВµР Р†Р С•Р В·Р С”Р С‘ b2b\n"
    "РЎРЊР С”РЎРѓР С—Р ВµР Т‘Р С‘РЎвЂљР С•РЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂљРЎР‚Р В°Р Р…РЎРѓР С—Р С•РЎР‚РЎвЂљР Р…РЎвЂ№Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘ Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "РЎРѓР С”Р В»Р В°Р Т‘РЎРѓР С”Р В°РЎРЏ Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘Р С”Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р С”Р С•РЎР‚Р С—Р С•РЎР‚Р В°РЎвЂљР С‘Р Р†Р Р…РЎвЂ№Р Вµ Р С—РЎР‚Р С•Р Т‘Р В°Р В¶Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "Р С”РЎР‚РЎС“Р С—Р Р…РЎвЂ№Р Вµ РЎРѓР Т‘Р ВµР В»Р С”Р С‘ b2b\n"
    "Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚Р В° Р С—Р С• РЎР‚Р В°Р В·Р Р†Р С‘РЎвЂљР С‘РЎР‹\n"
    "bdm РЎвЂЎР В°РЎвЂљ\n"
    "enterprise Р С—РЎР‚Р С•Р Т‘Р В°Р В¶Р С‘\n"
    "РЎР‚РЎС“Р С”Р С•Р Р†Р С•Р Т‘Р С‘РЎвЂљР ВµР В»РЎРЉ Р С•РЎвЂљР Т‘Р ВµР В»Р В° Р С—РЎР‚Р С•Р Т‘Р В°Р В¶\n"
    "РЎР‚Р С•Р С— РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµРЎРѓРЎвЂљР Р†Р С•\n"
    "Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ Р С—Р С• Р С—РЎР‚Р С•Р Т‘Р В°Р В¶Р В°Р С\n"
    "Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ Р С•РЎвЂљР Т‘Р ВµР В»Р В° Р С—РЎР‚Р С•Р Т‘Р В°Р В¶\n"
    "РЎРѓР С‘РЎРѓРЎвЂљР ВµР СР Р…РЎвЂ№Р в„– Р С•РЎвЂљР Т‘Р ВµР В» Р С—РЎР‚Р С•Р Т‘Р В°Р В¶\n"
    "Р С”Р С•Р Р…РЎвЂљРЎР‚Р С•Р В»РЎРЉ Р СР ВµР Р…Р ВµР Т‘Р В¶Р ВµРЎР‚Р С•Р Р†\n"
    "РЎР‚Р ВµР С–Р В»Р В°Р СР ВµР Р…РЎвЂљРЎвЂ№ Р С—РЎР‚Р С•Р Т‘Р В°Р В¶\n"
    "РЎРѓР С‘РЎРѓРЎвЂљР ВµР СР Р…РЎвЂ№Р Вµ Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "it Р С‘Р Р…РЎвЂљР ВµР С–РЎР‚Р В°РЎвЂљР С•РЎР‚ РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµРЎРѓРЎвЂљР Р†Р С•\n"
    "Р Р†Р ВµР В±-РЎРѓРЎвЂљРЎС“Р Т‘Р С‘Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "digital Р В°Р С–Р ВµР Р…РЎвЂљРЎРѓРЎвЂљР Р†Р В° Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "Р СР В°РЎР‚Р С”Р ВµРЎвЂљР С‘Р Р…Р С–Р С•Р Р†РЎвЂ№Р Вµ Р В°Р С–Р ВµР Р…РЎвЂљРЎРѓРЎвЂљР Р†Р В° Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№\n"
    "Р В°РЎС“РЎвЂљРЎРѓР С•РЎР‚РЎРѓ РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р В°\n"
    "enterprise РЎР‚Р В°Р В·РЎР‚Р В°Р В±Р С•РЎвЂљР С”Р В°\n"
    "Р С‘Р Р…Р Р†Р ВµРЎРѓРЎвЂљР С‘РЎвЂ Р С‘Р С•Р Р…Р Р…РЎвЂ№Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "РЎС“Р С—РЎР‚Р В°Р Р†Р В»РЎРЏРЎР‹РЎвЂ°Р С‘Р Вµ Р С”Р С•Р СР С—Р В°Р Р…Р С‘Р С‘\n"
    "РЎвЂћР С‘Р Р…Р В°Р Р…РЎРѓР С•Р Р†РЎвЂ№Р Вµ Р С”Р С•Р Р…РЎРѓРЎС“Р В»РЎРЉРЎвЂљР В°Р Р…РЎвЂљРЎвЂ№\n"
    "private equity РЎвЂЎР В°РЎвЂљ\n"
    "Р Р†Р ВµР Р…РЎвЂЎРЎС“РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂћРЎР‚Р В°Р Р…РЎвЂЎР В°Р в„–Р В·Р С‘ РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂћРЎР‚Р В°Р Р…РЎвЂЎР В°Р в„–Р В·Р С‘Р Р…Р С– РЎРѓР С•Р С•Р В±РЎвЂ°Р ВµРЎРѓРЎвЂљР Р†Р С•\n"
    "РЎРѓР ВµРЎвЂљРЎРЉ РЎвЂћР С‘Р В»Р С‘Р В°Р В»Р С•Р Р†\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ РЎРѓР ВµРЎвЂљР С‘\n"
    "РЎС“Р С—РЎР‚Р В°Р Р†Р В»Р ВµР Р…Р С‘Р Вµ РЎвЂћР С‘Р В»Р С‘Р В°Р В»Р В°Р СР С‘\n"
    "Р СР С•РЎРѓР С”Р Р†Р В° Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ Р С”Р В»РЎС“Р В±\n"
    "Р СР С•РЎРѓР С”Р Р†Р В° Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘\n"
    "Р Т‘РЎС“Р В±Р В°Р в„– Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ РЎР‚РЎС“РЎРѓРЎРѓР С”Р С‘Р Вµ\n"
    "Р ВµР Р†РЎР‚Р С•Р С—Р В° b2b\n"
    "Р С”Р В°Р В·Р В°РЎвЂ¦РЎРѓРЎвЂљР В°Р Р… Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С•\n"
    "Р В°Р В»Р СР В°РЎвЂљРЎвЂ№ Р С—РЎР‚Р ВµР Т‘Р С—РЎР‚Р С‘Р Р…Р С‘Р СР В°РЎвЂљР ВµР В»Р С‘\n"
    "РЎРѓР С—Р В± Р С—РЎР‚Р С•Р СРЎвЂ№РЎв‚¬Р В»Р ВµР Р…Р Р…Р С•РЎРѓРЎвЂљРЎРЉ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С• Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘Р С”Р В° Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "Р Т‘Р ВµР Р†Р ВµР В»Р С•Р С—Р СР ВµР Р…РЎвЂљ Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "РЎвЂћР С‘Р Р…Р В°Р Р…РЎРѓРЎвЂ№ Р Р†Р В»Р В°Р Т‘Р ВµР В»РЎРЉРЎвЂ РЎвЂ№ РЎвЂЎР В°РЎвЂљ\n"
    "b2b Р С–Р ВµР Р…Р ВµРЎР‚Р В°Р В»РЎРЉР Р…РЎвЂ№Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р С–Р ВµР Р…Р ВµРЎР‚Р В°Р В»РЎРЉР Р…РЎвЂ№Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С• Р С–Р ВµР Р…Р ВµРЎР‚Р В°Р В»РЎРЉР Р…РЎвЂ№Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘Р С”Р В° Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С• Р С”Р С•Р СР СР ВµРЎР‚РЎвЂЎР ВµРЎРѓР С”Р С‘Р в„– Р Т‘Р С‘РЎР‚Р ВµР С”РЎвЂљР С•РЎР‚ РЎвЂЎР В°РЎвЂљ\n"
    "b2b Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ Р С”Р В»РЎС“Р В± РЎвЂЎР В°РЎвЂљ\n"
    "Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р С• Р В±Р С‘Р В·Р Р…Р ВµРЎРѓ Р С”Р В»РЎС“Р В±\n"
    "РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉРЎРѓРЎвЂљР Р†Р С• Р В·Р В°Р С”РЎР‚РЎвЂ№РЎвЂљРЎвЂ№Р в„– Р С”Р В»РЎС“Р В±\n"
    "Р В»Р С•Р С–Р С‘РЎРѓРЎвЂљР С‘Р С”Р В° Р В·Р В°Р С”РЎР‚РЎвЂ№РЎвЂљРЎвЂ№Р в„– Р С”Р В»РЎС“Р В±\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ Р С—РЎР‚Р С•Р С‘Р В·Р Р†Р С•Р Т‘РЎРѓРЎвЂљР Р†Р В° РЎвЂЎР В°РЎвЂљ\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ b2b Р С—РЎР‚Р С•Р Т‘Р В°Р В¶ РЎвЂЎР В°РЎвЂљ\n"
    "Р СР В°РЎРѓРЎв‚¬РЎвЂљР В°Р В±Р С‘РЎР‚Р С•Р Р†Р В°Р Р…Р С‘Р Вµ РЎРѓРЎвЂљРЎР‚Р С•Р С‘РЎвЂљР ВµР В»РЎРЉР Р…Р С•Р С–Р С• Р В±Р С‘Р В·Р Р…Р ВµРЎРѓР В° РЎвЂЎР В°РЎвЂљ\n"
)


def msk_hhmm(dt: datetime | None) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK_TZ).strftime("%H:%M")


templates.env.globals["msk_hhmm"] = msk_hhmm


@app.on_event("startup")
async def _startup() -> None:
    init_db()
    await _ensure_tg_webhook()


async def _ensure_tg_webhook() -> None:
    if not settings.tg_bot_token:
        return
    if not settings.tg_webhook_secret:
        return
    url = settings.tg_webhook_url.strip() or (settings.public_base_url.rstrip("/") + "/api/tg/webhook")
    await set_webhook(url=url, secret_token=settings.tg_webhook_secret)


def _db():
    with session() as s:
        yield s


def _require_auth(creds: HTTPBasicCredentials = Depends(security)) -> None:
    """
    Protect UI endpoints with Basic auth. Ingest stays open for tg-monitor.
    If auth_user is empty, auth is disabled (useful for local dev).
    """
    if not settings.auth_user:
        return
    ok_user = secrets.compare_digest(creds.username or "", settings.auth_user)
    ok_pass = secrets.compare_digest(creds.password or "", settings.auth_pass)
    if ok_user and ok_pass:
        return
    raise HTTPException(
        status_code=401,
        detail="unauthorized",
        headers={"WWW-Authenticate": "Basic"},
    )


def _require_internal_token(request: Request) -> None:
    if not settings.internal_token:
        raise HTTPException(status_code=500, detail="internal token not configured")
    tok = request.headers.get("x-internal-token", "")
    if secrets.compare_digest(tok, settings.internal_token):
        return
    raise HTTPException(status_code=401, detail="unauthorized")


def _get_settings(db) -> AppSettings:
    row = db.exec(select(AppSettings).where(AppSettings.id == 1)).first()
    if row:
        need_update = False
        # Backfill reasonable defaults for discovery queries on existing DBs.
        if not (row.discovery_queries_text or "").strip():
            row.discovery_queries_text = DEFAULT_DISCOVERY_QUERIES_TEXT
            need_update = True
        if not (row.llm_model_priority_text or "").strip():
            row.llm_model_priority_text = default_model_priority_text()
            need_update = True
        if not (row.llm_models_config_json or "").strip():
            row.llm_models_config_json = "{}"
            need_update = True
        if row.llm_max_attempts <= 0:
            row.llm_max_attempts = 3
            need_update = True
        if row.llm_timeout_s <= 0:
            row.llm_timeout_s = 15.0
            need_update = True
        if (row.amo_pipeline_id or 0) <= 0:
            row.amo_pipeline_id = 4301503
            need_update = True
        if (row.amo_status_unprocessed_id or 0) <= 0:
            row.amo_status_unprocessed_id = 40181908
            need_update = True
        if (row.amo_status_primary_contact_id or 0) <= 0:
            row.amo_status_primary_contact_id = 40181911
            need_update = True
        if not (row.amo_redirect_uri or "").strip():
            row.amo_redirect_uri = settings.public_base_url.rstrip("/") + "/api/amo/oauth/callback"
            need_update = True
        if need_update:
            db.add(row)
            db.commit()
            db.refresh(row)
        return row
    row = AppSettings(
        id=1,
        discovery_queries_text=DEFAULT_DISCOVERY_QUERIES_TEXT,
        llm_model_priority_text=default_model_priority_text(),
        llm_models_config_json="{}",
        amo_pipeline_id=4301503,
        amo_status_unprocessed_id=40181908,
        amo_status_primary_contact_id=40181911,
        amo_redirect_uri=settings.public_base_url.rstrip("/") + "/api/amo/oauth/callback",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _llm_opts(cfg: AppSettings) -> LLMRoutingOptions:
    return LLMRoutingOptions(
        model_priority_text=cfg.llm_model_priority_text,
        llm_max_attempts=max(1, int(cfg.llm_max_attempts or 3)),
        llm_timeout_s=max(1.0, float(cfg.llm_timeout_s or 15.0)),
        exclude_openai_owned_models=bool(cfg.exclude_openai_owned_models),
        models_config_json=cfg.llm_models_config_json or "{}",
    )


def _amo_base_url(cfg: AppSettings) -> str:
    sub = (cfg.amo_subdomain or "").strip()
    if not sub:
        raise RuntimeError("amo_subdomain not configured")
    if sub.startswith("http://") or sub.startswith("https://"):
        return sub.rstrip("/")
    return f"https://{sub}.amocrm.ru"


async def _amo_refresh_token_if_needed(db, cfg: AppSettings) -> None:
    now = utcnow()
    if cfg.amo_access_token and cfg.amo_token_expires_at and cfg.amo_token_expires_at > (now + timedelta(minutes=2)):
        return
    if not cfg.amo_refresh_token:
        raise RuntimeError("amo refresh_token missing")
    base = _amo_base_url(cfg)
    payload = {
        "client_id": cfg.amo_client_id,
        "client_secret": cfg.amo_client_secret,
        "grant_type": "refresh_token",
        "refresh_token": cfg.amo_refresh_token,
        "redirect_uri": cfg.amo_redirect_uri,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{base}/oauth2/access_token", json=payload, timeout=25.0)
    if r.status_code >= 400:
        raise RuntimeError(f"amo token refresh failed: {r.text[:300]}")
    data = r.json()
    cfg.amo_access_token = str(data.get("access_token") or "")
    cfg.amo_refresh_token = str(data.get("refresh_token") or cfg.amo_refresh_token)
    expires_in = int(data.get("expires_in") or 3600)
    cfg.amo_token_expires_at = now + timedelta(seconds=max(60, expires_in - 30))
    db.add(cfg)
    db.commit()


async def _amo_api_json(
    db,
    cfg: AppSettings,
    *,
    method: str,
    path: str,
    json_body: dict | list | None = None,
    params: dict | None = None,
) -> dict:
    await _amo_refresh_token_if_needed(db, cfg)
    base = _amo_base_url(cfg)
    headers = {"Authorization": f"Bearer {cfg.amo_access_token}"}
    async with httpx.AsyncClient() as client:
        r = await client.request(
            method=method.upper(),
            url=f"{base}{path}",
            json=json_body,
            params=params,
            headers=headers,
            timeout=30.0,
        )
    if r.status_code >= 400:
        raise RuntimeError(f"amo api {method} {path} failed: {r.status_code} {r.text[:300]}")
    if r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}


async def _amo_accept_lead(db, cfg: AppSettings, lead_id: int) -> dict:
    before = await _amo_api_json(db, cfg, method="GET", path=f"/api/v4/leads/{int(lead_id)}")
    body = [
        {
            "id": int(lead_id),
            "pipeline_id": int(cfg.amo_pipeline_id),
            "status_id": int(cfg.amo_status_primary_contact_id),
        }
    ]
    await _amo_api_json(db, cfg, method="PATCH", path="/api/v4/leads", json_body=body)
    after = await _amo_api_json(db, cfg, method="GET", path=f"/api/v4/leads/{int(lead_id)}")
    after_pipeline_id = int(after.get("pipeline_id") or 0) if isinstance(after, dict) else 0
    after_status_id = int(after.get("status_id") or 0) if isinstance(after, dict) else 0
    target_pipeline_id = int(cfg.amo_pipeline_id or 0)
    target_status_id = int(cfg.amo_status_primary_contact_id or 0)
    moved = (
        target_pipeline_id > 0
        and target_status_id > 0
        and after_pipeline_id == target_pipeline_id
        and after_status_id == target_status_id
    )
    return {
        "lead": after or {"id": int(lead_id)},
        "before_status_id": (before or {}).get("status_id"),
        "before_pipeline_id": (before or {}).get("pipeline_id"),
        "target_pipeline_id": target_pipeline_id,
        "target_status_id": target_status_id,
        "moved": moved,
    }


async def _amo_fetch_full_lead(db, cfg: AppSettings, lead_id: int) -> dict:
    """
    Fetch full lead data with contacts, company, custom fields, tags, etc.
    """
    base = _amo_base_url(cfg)
    
    # Get lead details
    lead = await _amo_api_json(db, cfg, method="GET", path=f"/api/v4/leads/{int(lead_id)}")
    if not lead:
        return {}
    
    # Get embedded data (contacts, company, tags)
    embedded = lead.get("_embedded", {})
    
    result = {
        "lead": lead,
        "contacts": [],
        "company": None,
        "tags": embedded.get("tags", []),
        "custom_fields": lead.get("custom_fields", []),
    }
    
    # Fetch contacts
    contacts = embedded.get("contacts", [])
    if contacts:
        contact_ids = [c.get("id") for c in contacts if c.get("id")]
        if contact_ids:
            contacts_data = await _amo_api_json(
                db, cfg, 
                method="POST", 
                path="/api/v4/leads/links/contacts",
                json_body={"leads": [{"id": int(lead_id)}]}
            )
            # Alternative: fetch contacts directly
            for cid in contact_ids[:5]:  # limit to 5 contacts
                try:
                    contact = await _amo_api_json(db, cfg, method="GET", path=f"/api/v4/contacts/{cid}")
                    if contact:
                        result["contacts"].append(contact)
                except Exception:
                    pass
    
    # Fetch company
    company_id = embedded.get("companies", [{}])[0].get("id") if embedded.get("companies") else None
    if company_id:
        try:
            result["company"] = await _amo_api_json(db, cfg, method="GET", path=f"/api/v4/companies/{int(company_id)}")
        except Exception:
            pass
    
    return result


async def _amo_accept_backoff_seconds(started_at: datetime, now: datetime) -> int:
    # Phase 1: up to 19 minutes from webhook -> every 60s.
    # Phase 2: from 19 to 22 minutes -> every 10s.
    elapsed = (now - started_at).total_seconds()
    if elapsed < 19 * 60:
        return 60
    return 10


async def _process_amo_accept_async(inbox_id: int) -> None:
    timeout_window = timedelta(minutes=22)

    while True:
        with session() as db:
            row = db.exec(select(AmoLeadInbox).where(AmoLeadInbox.id == inbox_id)).first()
            if not row:
                return
            if row.status in ("accepted", "rejected"):
                return

            cfg = _get_settings(db)
            now = utcnow()
            if not row.accept_started_at:
                row.accept_started_at = row.created_at or now
            if not row.accept_deadline_at:
                row.accept_deadline_at = row.accept_started_at + timeout_window

            deadline = row.accept_deadline_at
            if deadline and now >= deadline:
                row.status = "error"
                row.error = "accept_timeout_20m"
                row.updated_at = now
                db.add(row)
                db.commit()
                if settings.tg_chat_id:
                    await send_message_html(
                        chat_id=settings.tg_chat_id,
                        html=(
                            "<b>amo: lead was not accepted</b><br/>"
                            f"ID: {row.amo_lead_id}<br/>"
                            "Reason: moderation window expired (22 minutes)."
                        ),
                    )
                return

            row.status = "processing_accept"
            row.accept_attempts = int(row.accept_attempts or 0) + 1
            row.accept_last_try_at = now
            row.updated_at = now
            db.add(row)
            db.commit()

            attempt_no = int(row.accept_attempts or 1)
            lead_id = int(row.amo_lead_id)

        move_result = None
        full_data = None
        last_err = ""
        moved = False

        with session() as db:
            cfg = _get_settings(db)
            try:
                move_result = await _amo_accept_lead(db, cfg, lead_id)
                moved = bool((move_result or {}).get("moved"))
                if moved:
                    full_data = await _amo_fetch_full_lead(db, cfg, lead_id)
            except Exception as e:
                last_err = str(e)[:800]

            row = db.exec(select(AmoLeadInbox).where(AmoLeadInbox.id == inbox_id)).first()
            if not row:
                return

            if moved:
                lead = (move_result or {}).get("lead") or {}
                row.status = "accepted"
                row.error = ""
                row.result_json = json.dumps(
                    {
                        "move_result": move_result,
                        "full_data": full_data or {},
                        "attempts": attempt_no,
                    },
                    ensure_ascii=False,
                )
                row.updated_at = utcnow()
                db.add(row)
                db.commit()

                if settings.tg_chat_id:
                    lead_name = (lead.get("name") or "").strip() or f"Lead #{lead_id}"
                    lead_price = lead.get("price")
                    lead_status = lead.get("status_id")
                    lead_pipeline = lead.get("pipeline_id")
                    before_status = (move_result or {}).get("before_status_id")
                    before_pipeline = (move_result or {}).get("before_pipeline_id")
                    lead_link = f"https://{cfg.amo_subdomain.strip()}.amocrm.ru/leads/detail/{lead_id}"
                    budget_line = f"Budget: {int(lead_price):,} RUB<br/>" if lead_price else "Budget: -<br/>"
                    await send_message_html(
                        chat_id=settings.tg_chat_id,
                        html=(
                            "<b>amo: lead accepted</b><br/>"
                            f"ID: {lead_id}<br/>"
                            + f"Lead: {str(lead_name).replace('<','&lt;').replace('>','&gt;')}<br/>"
                            + budget_line
                            + f"Pipeline: {before_pipeline} -> {lead_pipeline}<br/>"
                            + f"Status: {before_status} -> {lead_status}<br/>"
                            + f"Attempts: {attempt_no}<br/>"
                            + f"<a href=\"{lead_link}\">Open in amoCRM</a>"
                        ),
                    )
                return

            # Not moved yet or request failed: keep task queued and retry later.
            row.status = "queued_accept"
            row.error = last_err if last_err else "moderation_pending"
            row.updated_at = utcnow()
            db.add(row)
            db.commit()

        start_at = row.accept_started_at or row.created_at or utcnow()
        wait_s = await _amo_accept_backoff_seconds(start_at, utcnow())
        if row.accept_deadline_at:
            remaining = int((row.accept_deadline_at - utcnow()).total_seconds())
            wait_s = max(1, min(wait_s, remaining))
        await asyncio.sleep(max(1, wait_s))

def _parse_models_cfg_json(raw: str | None) -> dict[str, dict]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if not isinstance(v, dict):
            continue
        out[k.strip()] = v
    return out


def _extract_amo_lead_ids(raw_text: str, payload_json: dict | None) -> list[int]:
    out: list[int] = []
    if isinstance(payload_json, dict):
        try:
            emb = payload_json.get("_embedded") or {}
            leads = emb.get("leads") or []
            if isinstance(leads, list):
                for x in leads:
                    if isinstance(x, dict) and x.get("id"):
                        out.append(int(x.get("id")))
        except Exception:
            pass
    # amo legacy webhooks may contain keys like leads[add][0][id]=123
    for m in re.findall(r"leads\[[^\]]+\]\[\d+\]\[id\]=(\d+)", raw_text or ""):
        try:
            out.append(int(m))
        except Exception:
            pass
    # fallback generic pattern for id values in form-encoded dump
    for m in re.findall(r"\[id\]=(\d+)", raw_text or ""):
        try:
            out.append(int(m))
        except Exception:
            pass
    dedup: list[int] = []
    seen: set[int] = set()
    for x in out:
        if x <= 0 or x in seen:
            continue
        seen.add(x)
        dedup.append(x)
    return dedup


def _utc_day_key(dt: datetime | None = None) -> str:
    now = dt or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _upsert_llm_model_stat(db, *, model_id: str, probe: dict, success: bool) -> None:
    mid = (model_id or "").strip()
    if not mid:
        return
    day = _utc_day_key()
    row = db.exec(
        select(LLMModelStat).where(
            LLMModelStat.day_utc == day,
            LLMModelStat.model_id == mid,
        )
    ).first()
    if not row:
        row = LLMModelStat(day_utc=day, model_id=mid)

    usage = probe.get("usage") or {}
    row.calls_ok += 1 if success else 0
    row.calls_error += 0 if success else 1
    row.prompt_tokens += int(usage.get("prompt_tokens") or 0)
    row.completion_tokens += int(usage.get("completion_tokens") or 0)
    row.total_tokens += int(usage.get("total_tokens") or 0)
    row.last_http_status = int(probe.get("status_code") or row.last_http_status or 0)
    row.last_latency_ms = int(probe.get("latency_ms") or row.last_latency_ms or 0)
    row.last_provider_status = str(probe.get("provider_status") or row.last_provider_status or "")[:80]
    row.last_reset_in_sec = int(probe.get("reset_in_sec") or row.last_reset_in_sec or 0)
    row.last_rate_limits_json = json.dumps(probe.get("rate_limits") or {}, ensure_ascii=False)
    row.last_error = str(probe.get("error") or "")[:500]
    row.updated_at = utcnow()
    db.add(row)
    db.commit()


@app.get("/health")
def health() -> dict:
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}


@app.post("/api/ingest/tg-monitor")
async def ingest(payload: TgMonitorPayload, db=Depends(_db)) -> dict:
    cfg = _get_settings(db)
    hf = hard_filter(
        text=payload.text,
        keywords_enabled=cfg.keywords_enabled,
        stopwords_enabled=cfg.stopwords_enabled,
        keywords_text=cfg.keywords_text,
        stopwords_text=cfg.stopwords_text,
    )

    ev = TgMonitorEvent(
        text=payload.text,
        chat_title=payload.chat_title,
        sender_name=payload.sender_name,
        sender_link=payload.sender_link,
        sender_handle=payload.sender_handle,
        link=payload.link,
        # Store either tg-monitor provided keywords (if any), or our own matched list.
        keywords_json=json.dumps(payload.keywords or hf.matched_keywords, ensure_ascii=False),
        status=EventStatus.pending,
    )
    db.add(ev)
    db.commit()
    db.refresh(ev)

    # Fire-and-forget: return immediately; do classification in background with its own DB session.
    asyncio.create_task(process_event(ev.id))

    return {"ok": True, "id": ev.id}


@app.get("/api/internal/settings")
def internal_settings(db=Depends(_db), _it=Depends(_require_internal_token)) -> dict:
    cfg = _get_settings(db)
    return {
        "keywords_enabled": cfg.keywords_enabled,
        "stopwords_enabled": cfg.stopwords_enabled,
        "llm_enabled": cfg.llm_enabled,
        "keywords_text": cfg.keywords_text,
        "stopwords_text": cfg.stopwords_text,
        "llm_model_priority_text": cfg.llm_model_priority_text,
        "llm_max_attempts": cfg.llm_max_attempts,
        "llm_timeout_s": cfg.llm_timeout_s,
        "exclude_openai_owned_models": cfg.exclude_openai_owned_models,
        "llm_models_config_json": cfg.llm_models_config_json,
        "discovery_enabled": cfg.discovery_enabled,
        "discovery_queries_text": cfg.discovery_queries_text,
        "discovery_interval_s": cfg.discovery_interval_s,
        "scan_days": cfg.scan_days,
        "scan_max_messages": cfg.scan_max_messages,
        "scan_llm_sample": cfg.scan_llm_sample,
        "join_per_day": cfg.join_per_day,
        "join_per_hour": cfg.join_per_hour,
        "auto_leave_if_no_candidates": cfg.auto_leave_if_no_candidates,
        "search_requests_per_day": cfg.search_requests_per_day,
        "search_requests_per_hour": cfg.search_requests_per_hour,
        "queries_per_tick": cfg.queries_per_tick,
        "search_results_per_query": cfg.search_results_per_query,
        "amo_enabled": cfg.amo_enabled,
        "amo_subdomain": cfg.amo_subdomain,
        "amo_client_id": cfg.amo_client_id,
        "amo_redirect_uri": cfg.amo_redirect_uri,
        "amo_pipeline_id": cfg.amo_pipeline_id,
        "amo_status_unprocessed_id": cfg.amo_status_unprocessed_id,
        "amo_status_primary_contact_id": cfg.amo_status_primary_contact_id,
    }


@app.get("/api/llm/models")
async def llm_models(db=Depends(_db), _auth=Depends(_require_auth)) -> list[dict]:
    cfg = _get_settings(db)
    preferred = [x.strip() for x in (cfg.llm_model_priority_text or "").splitlines() if x.strip()]
    preferred_set = {x.casefold() for x in preferred}
    models_cfg = _parse_models_cfg_json(cfg.llm_models_config_json)
    day = _utc_day_key()
    stats_rows = db.exec(select(LLMModelStat).where(LLMModelStat.day_utc == day)).all()
    stats_by_model = {r.model_id.casefold(): r for r in stats_rows}
    async with httpx.AsyncClient() as client:
        models = await list_models(client)
    # Include manually configured models that may not exist in cliproxy /models (e.g. direct OpenAI).
    known_ids = {m.id.casefold() for m in models}
    for mid in preferred:
        if mid.casefold() in known_ids:
            continue
        mc = models_cfg.get(mid) or {}
        provider_id = str(mc.get("provider_id") or mc.get("provider") or "").strip()
        if provider_id:
            models.append(ModelInfo(id=mid, owned_by=provider_id))

    out: list[dict] = []
    for m in models:
        mc = models_cfg.get(m.id) or {}
        provider_id = str(mc.get("provider_id") or mc.get("provider") or "").strip()
        has_api_key = bool(str(mc.get("api_key") or "").strip())
        br = model_block_reason(
            model_id=m.id,
            owned_by=m.owned_by,
            exclude_openai_owned_models=bool(cfg.exclude_openai_owned_models),
        )
        # If user configured direct provider credentials for this model, allow it regardless of cliproxy policy.
        if provider_id and has_api_key:
            br = None
        st = stats_by_model.get((m.id or "").casefold())
        rl = {}
        if st and (st.last_rate_limits_json or "").strip():
            try:
                rl = json.loads(st.last_rate_limits_json)
            except Exception:
                rl = {}
        out.append(
            {
                "id": m.id,
                "owned_by": m.owned_by or "",
                "allowed": br is None,
                "block_reason": br or "",
                "in_priority": m.id.casefold() in preferred_set,
                "provider_id": provider_id,
                "has_api_key": has_api_key,
                "calls_ok_today": int(st.calls_ok) if st else 0,
                "calls_error_today": int(st.calls_error) if st else 0,
                "tokens_total_today": int(st.total_tokens) if st else 0,
                "tokens_prompt_today": int(st.prompt_tokens) if st else 0,
                "tokens_completion_today": int(st.completion_tokens) if st else 0,
                "last_http_status": int(st.last_http_status) if st else 0,
                "last_latency_ms": int(st.last_latency_ms) if st else 0,
                "last_provider_status": st.last_provider_status if st else "",
                "last_reset_in_sec": int(st.last_reset_in_sec) if st else 0,
                "last_rate_limits": rl,
                "last_error": st.last_error if st else "",
            }
        )
    return out


@app.post("/api/llm/test-model")
async def llm_test_model(req: LLMModelTestRequest, db=Depends(_db), _auth=Depends(_require_auth)) -> dict:
    cfg = _get_settings(db)
    timeout_s = float(req.timeout_s if req.timeout_s is not None else cfg.llm_timeout_s)
    timeout_s = max(3.0, min(90.0, timeout_s))
    models_cfg = _parse_models_cfg_json(cfg.llm_models_config_json)
    mc = models_cfg.get((req.model_id or "").strip()) or {}
    provider_id = str(mc.get("provider_id") or mc.get("provider") or "").strip()
    api_key = str(mc.get("api_key") or "").strip()
    base_url = str(mc.get("base_url") or "").strip()
    res = await test_model_availability(
        model_id=req.model_id.strip(),
        timeout_s=timeout_s,
        provider_id=provider_id or None,
        api_key=api_key or None,
        base_url=base_url or None,
    )
    _upsert_llm_model_stat(
        db,
        model_id=req.model_id.strip(),
        probe=res,
        success=bool(res.get("ok")),
    )
    return res


@app.post("/api/llm/test-models")
async def llm_test_models(req: LLMModelsBulkTestRequest, db=Depends(_db), _auth=Depends(_require_auth)) -> dict:
    cfg = _get_settings(db)
    models_cfg = _parse_models_cfg_json(cfg.llm_models_config_json)
    timeout_s = float(req.timeout_s if req.timeout_s is not None else cfg.llm_timeout_s)
    timeout_s = max(3.0, min(90.0, timeout_s))
    lim = max(1, min(60, int(req.limit or 20)))

    model_ids = [x.strip() for x in (req.model_ids or []) if x and x.strip()]
    if not model_ids:
        preferred = [x.strip() for x in (cfg.llm_model_priority_text or "").splitlines() if x.strip()]
        model_ids = preferred[:lim]
    else:
        model_ids = model_ids[:lim]

    # Keep pressure moderate on provider.
    sem = asyncio.Semaphore(4)

    async def _one(mid: str) -> dict:
        async with sem:
            mc = models_cfg.get(mid) or {}
            return await test_model_availability(
                model_id=mid,
                timeout_s=timeout_s,
                provider_id=str(mc.get("provider_id") or mc.get("provider") or "").strip() or None,
                api_key=str(mc.get("api_key") or "").strip() or None,
                base_url=str(mc.get("base_url") or "").strip() or None,
            )

    results = await asyncio.gather(*[_one(mid) for mid in model_ids])
    for r in results:
        _upsert_llm_model_stat(
            db,
            model_id=str(r.get("model_id") or ""),
            probe=r,
            success=bool(r.get("ok")),
        )
    return {"results": results}


@app.post("/api/llm/provider-models")
async def llm_provider_models(req: ProviderModelsRequest, _auth=Depends(_require_auth)) -> dict:
    provider = (req.provider or "").strip().casefold()
    api_key = (req.api_key or "").strip()
    base_url = (req.base_url or "").strip()

    presets = {
        "openai": {
            "url": "https://api.openai.com/v1/models",
            "docs": "https://platform.openai.com/docs/models",
            "headers": lambda k: {"Authorization": f"Bearer {k}"} if k else {},
        },
        "openrouter": {
            "url": "https://openrouter.ai/api/v1/models",
            "docs": "https://openrouter.ai/models",
            "headers": lambda k: {"Authorization": f"Bearer {k}"} if k else {},
        },
        "anthropic": {
            "url": "https://api.anthropic.com/v1/models",
            "docs": "https://docs.anthropic.com/claude/docs/models-overview",
            "headers": lambda k: (
                {"x-api-key": k, "anthropic-version": "2023-06-01"} if k else {"anthropic-version": "2023-06-01"}
            ),
        },
        "gemini": {
            "url": "https://generativelanguage.googleapis.com/v1beta/models",
            "docs": "https://ai.google.dev/gemini-api/docs/models",
            "headers": lambda _k: {},
        },
        "cliapiproxy": {
            "url": f"{settings.cliproxy_base_url.rstrip('/')}/models",
            "docs": "https://parser.tunecrm.su/settings",
            "headers": lambda k: {"Authorization": f"Bearer {k}"} if k else {},
        },
    }
    if provider not in presets:
        raise HTTPException(status_code=400, detail="unsupported provider")

    p = presets[provider]
    url = base_url or p["url"]
    headers = p["headers"](api_key)
    params = {}
    if provider == "gemini":
        if not api_key:
            raise HTTPException(status_code=400, detail="api_key required for gemini")
        params["key"] = api_key

    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, headers=headers, params=params, timeout=20.0)
        try:
            data = r.json()
        except Exception:
            data = {}
        if r.status_code >= 400:
            msg = ""
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):
                    msg = str(err.get("message") or "")
                elif isinstance(err, str):
                    msg = err
            msg = msg or r.text[:240] or f"http={r.status_code}"
            raise HTTPException(status_code=400, detail=f"{provider}: {msg}")

        models: list[dict] = []
        if provider in ("openai", "anthropic", "openrouter", "cliapiproxy"):
            items = data.get("data") if isinstance(data, dict) else []
            if isinstance(items, list):
                for x in items:
                    if not isinstance(x, dict):
                        continue
                    mid = str(x.get("id") or "").strip()
                    if not mid:
                        continue
                    models.append({"id": mid, "name": mid})
        elif provider == "gemini":
            items = data.get("models") if isinstance(data, dict) else []
            if isinstance(items, list):
                for x in items:
                    if not isinstance(x, dict):
                        continue
                    raw_name = str(x.get("name") or "").strip()
                    mid = raw_name.replace("models/", "", 1) if raw_name.startswith("models/") else raw_name
                    if not mid:
                        continue
                    display = str(x.get("displayName") or mid).strip()
                    models.append({"id": mid, "name": display})

        dedup = {}
        for m in models:
            dedup[m["id"].casefold()] = m
        out = sorted(dedup.values(), key=lambda x: x["id"])

        return {
            "provider": provider,
            "docs_url": p["docs"],
            "count": len(out),
            "models": out,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"{provider}: {str(e)[:240]}")


@app.post("/api/internal/classify")
async def internal_classify(req: InternalClassifyRequest, db=Depends(_db), _it=Depends(_require_internal_token)) -> InternalClassifyResponse:
    """
    Synchronous internal classify endpoint for trusted agents (e.g. tg-agent discovery probing).
    Does NOT write events to DB.
    """
    cfg = _get_settings(db)
    hf = hard_filter(
        text=req.text,
        keywords_enabled=cfg.keywords_enabled,
        stopwords_enabled=cfg.stopwords_enabled,
        keywords_text=cfg.keywords_text,
        stopwords_text=cfg.stopwords_text,
    )
    intent = request_intent_filter(text=req.text)
    if not intent.passed:
        return InternalClassifyResponse(
            passed_hard_filter=False,
            hard_filter_reason=intent.reason,
            is_lead=False,
            summary=f"hard_filter:{intent.reason}",
            confidence=None,
            model_used="hard-filter",
        )
    if not hf.passed:
        return InternalClassifyResponse(
            passed_hard_filter=False,
            hard_filter_reason=hf.reason,
            is_lead=False,
            summary=f"hard_filter:{hf.reason}",
            confidence=None,
            model_used="hard-filter",
        )
    if not cfg.llm_enabled:
        return InternalClassifyResponse(
            passed_hard_filter=True,
            hard_filter_reason="ok",
            is_lead=False,
            summary="llm_disabled",
            confidence=None,
            model_used="hard-filter",
        )

    res, model_used, meta = await classify(req.text, options=_llm_opts(cfg))
    _upsert_llm_model_stat(db, model_id=model_used, probe=meta, success=True)
    return InternalClassifyResponse(
        passed_hard_filter=True,
        hard_filter_reason="ok",
        is_lead=bool(res.is_lead),
        summary=res.summary,
        confidence=res.confidence,
        model_used=model_used,
    )


async def process_event(event_id: int) -> None:
    # Run with isolated DB session to be safe as a background task.
    with session() as db:
        ev = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == event_id)).one()
        if ev.status in (EventStatus.processing, EventStatus.done):
            return

        ev.status = EventStatus.processing
        db.add(ev)
        db.commit()

        try:
            cfg = _get_settings(db)
            hf = hard_filter(
                text=ev.text,
                keywords_enabled=cfg.keywords_enabled,
                stopwords_enabled=cfg.stopwords_enabled,
                keywords_text=cfg.keywords_text,
                stopwords_text=cfg.stopwords_text,
            )
            intent = request_intent_filter(text=ev.text)
            if not intent.passed:
                ev.is_lead = False
                ev.summary = f"hard_filter:{intent.reason}"
                ev.confidence = None
                ev.model_used = "hard-filter"
                ev.attempts = min(settings.llm_max_attempts, ev.attempts + 1)
                ev.status = EventStatus.done
                db.add(ev)
                db.commit()
                return

            if not hf.passed:
                ev.is_lead = False
                ev.summary = f"hard_filter:{hf.reason}"
                ev.confidence = None
                ev.model_used = "hard-filter"
                ev.attempts = min(settings.llm_max_attempts, ev.attempts + 1)
                ev.status = EventStatus.done
                db.add(ev)
                db.commit()
                return

            if not cfg.llm_enabled:
                ev.is_lead = False
                ev.summary = "llm_disabled"
                ev.confidence = None
                ev.model_used = "hard-filter"
                ev.attempts = min(settings.llm_max_attempts, ev.attempts + 1)
                ev.status = EventStatus.done
                db.add(ev)
                db.commit()
                return

            res, model_used, meta = await classify(ev.text, options=_llm_opts(cfg))
            ev.is_lead = res.is_lead
            ev.summary = res.summary
            ev.confidence = res.confidence
            ev.model_used = model_used
            ev.attempts = min(settings.llm_max_attempts, ev.attempts + 1)
            ev.status = EventStatus.done
            _upsert_llm_model_stat(db, model_id=model_used, probe=meta, success=True)

            if res.is_lead or settings.tg_notify_all:
                excerpt = (ev.text or "").strip().replace("\n", " ")[:240]
                try:
                    await send_event_notification(
                        event_id=ev.id or event_id,
                        excerpt=excerpt,
                        summary=ev.summary,
                        link=ev.link,
                        is_lead=ev.is_lead,
                    )
                    ev.notified = True
                    ev.notified_at = utcnow()
                except Exception as e:
                    ev.notified = False
                    ev.last_error = (ev.last_error + "\n" if ev.last_error else "") + f"notify: {e}"

            db.add(ev)
            db.commit()

        except Exception as e:
            ev.status = EventStatus.failed
            ev.last_error = str(e)[:2000]
            db.add(ev)
            db.commit()


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    now_msk = datetime.now(MSK_TZ)
    day_start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_utc = day_start_msk.astimezone(timezone.utc)
    week_cutoff_utc = (now_msk - timedelta(days=7)).astimezone(timezone.utc)
    month_cutoff_utc = (now_msk - timedelta(days=30)).astimezone(timezone.utc)

    total_messages = db.exec(select(func.count()).select_from(TgMonitorEvent)).one() or 0
    leads_day = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= day_start_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )
    leads_week = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= week_cutoff_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )
    leads_month = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= month_cutoff_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )
    messages_month = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= month_cutoff_utc)
        ).one()
        or 0
    )
    lead_rate_month = round((float(leads_month) / float(messages_month) * 100.0), 1) if int(messages_month) > 0 else 0.0

    added_today = (
        db.exec(
            select(func.count())
            .select_from(DiscoveryTarget)
            .where(DiscoveryTarget.created_at >= day_start_utc)
        ).one()
        or 0
    )
    joins_today = (
        db.exec(
            select(func.count())
            .select_from(DiscoveryLog)
            .where(DiscoveryLog.created_at >= day_start_utc, DiscoveryLog.event == "join_ok")
        ).one()
        or 0
    )
    leaves_today = (
        db.exec(
            select(func.count())
            .select_from(DiscoveryLog)
            .where(DiscoveryLog.created_at >= day_start_utc, DiscoveryLog.event == "leave")
        ).one()
        or 0
    )

    rows = db.exec(
        select(TgMonitorEvent).order_by(TgMonitorEvent.created_at.desc()).limit(200)
    ).all()
    cfg = _get_settings(db)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "rows": rows,
            "public_base_url": settings.public_base_url.rstrip("/"),
            "cfg": cfg,
            "stats": {
                "total_messages": int(total_messages),
                "leads_day": int(leads_day),
                "leads_week": int(leads_week),
                "leads_month": int(leads_month),
                "added_today": int(added_today),
                "joins_today": int(joins_today),
                "leaves_today": int(leaves_today),
                "lead_rate_month": lead_rate_month,
            },
        },
    )


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    cfg = _get_settings(db)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "cfg": cfg,
            "public_base_url": settings.public_base_url.rstrip("/"),
        },
    )


@app.get("/amo", response_class=HTMLResponse)
def amo_settings_page(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    cfg = _get_settings(db)
    connected = bool((cfg.amo_access_token or "").strip())
    webhook_url = settings.public_base_url.rstrip("/") + "/api/amo/webhook"
    if (cfg.amo_webhook_secret or "").strip():
        webhook_url = webhook_url + "?secret=" + (cfg.amo_webhook_secret or "").strip()
    return templates.TemplateResponse(
        request,
        "amo_settings.html",
        {
            "cfg": cfg,
            "connected": connected,
            "webhook_url": webhook_url,
        },
    )


@app.post("/amo/settings")
def amo_update_settings(
    amo_enabled: bool = Form(False),
    amo_subdomain: str = Form(""),
    amo_client_id: str = Form(""),
    amo_client_secret: str = Form(""),
    amo_redirect_uri: str = Form(""),
    amo_pipeline_id: int = Form(4301503),
    amo_status_unprocessed_id: int = Form(40181908),
    amo_status_primary_contact_id: int = Form(40181911),
    amo_webhook_secret: str = Form(""),
    db=Depends(_db),
    _auth=Depends(_require_auth),
):
    cfg = _get_settings(db)
    cfg.amo_enabled = bool(amo_enabled)
    cfg.amo_subdomain = (amo_subdomain or "").strip()
    cfg.amo_client_id = (amo_client_id or "").strip()
    cfg.amo_client_secret = (amo_client_secret or "").strip()
    cfg.amo_redirect_uri = (amo_redirect_uri or "").strip() or (settings.public_base_url.rstrip("/") + "/api/amo/oauth/callback")
    cfg.amo_pipeline_id = int(amo_pipeline_id or 4301503)
    cfg.amo_status_unprocessed_id = int(amo_status_unprocessed_id or 40181908)
    cfg.amo_status_primary_contact_id = int(amo_status_primary_contact_id or 40181911)
    cfg.amo_webhook_secret = (amo_webhook_secret or "").strip()
    db.add(cfg)
    db.commit()
    return RedirectResponse(url="/amo", status_code=303)


@app.get("/amo/leads", response_class=HTMLResponse)
def amo_leads_page(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    cfg = _get_settings(db)
    rows = db.exec(select(AmoLeadInbox).order_by(AmoLeadInbox.updated_at.desc()).limit(500)).all()
    return templates.TemplateResponse(request, "amo_leads.html", {"rows": rows, "cfg": cfg})


@app.get("/api/amo/oauth/start")
def amo_oauth_start(db=Depends(_db), _auth=Depends(_require_auth)):
    cfg = _get_settings(db)
    if not cfg.amo_subdomain or not cfg.amo_client_id:
        raise HTTPException(status_code=400, detail="Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р Р‹Р РЋРІвЂћСћР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’Вµ amo_subdomain Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В amo_client_id Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В  Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎСљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В¦")
    state = secrets.token_urlsafe(24)
    # stateless for MVP; callback will still work without strict state binding
    params = {
        "client_id": cfg.amo_client_id,
        "mode": "post_message",
        "redirect_uri": cfg.amo_redirect_uri,
        "response_type": "code",
        "state": state,
    }
    url = f"https://www.amocrm.ru/oauth?{urlencode(params)}"
    return RedirectResponse(url=url, status_code=302)


@app.get("/api/amo/oauth/revoke")
def amo_oauth_revoke(db=Depends(_db), _auth=Depends(_require_auth)):
    """Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р РЋРЎв„ўР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В·Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В° OAuth Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’ВµР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В¦Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р РЋРІвЂћСћ (Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р В РІР‚в„–Р В Р’В Р В Р вЂ№Р В Р вЂ Р В РІР‚С™Р РЋРІР‚СњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В°Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В РІР‚В Р В Р вЂ Р В РІР‚С™Р РЋРІР‚С”Р В Р Р‹Р РЋРІР‚С”Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В»Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’ВР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р В Р вЂ№Р В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћвЂ“Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р В Р вЂ№Р В Р Р‹Р Р†Р вЂљРЎвЂќР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР Р†Р вЂљРІР‚СљР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р’В Р Р†Р вЂљР’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р РЋРІвЂћСћР В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В° access_token Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р вЂ Р Р†Р вЂљРЎвЂєР РЋРЎвЂєР В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РІР‚вЂњР В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р вЂ™Р’В Р В Р’В Р вЂ™Р’В Р В РІР‚в„ўР вЂ™Р’В Р В Р’В Р В РІР‚В Р В Р’В Р Р†Р вЂљРЎв„ўР В Р Р‹Р Р†РІР‚С›РЎС›Р В Р’В Р вЂ™Р’В Р В Р вЂ Р В РІР‚С™Р Р†РІР‚С›РЎС›Р В Р’В Р Р†Р вЂљРІвЂћСћР В РІР‚в„ўР вЂ™Р’В refresh_token)."""
    cfg = _get_settings(db)
    cfg.amo_access_token = ""
    cfg.amo_refresh_token = ""
    cfg.amo_token_expires_at = None
    db.add(cfg)
    db.commit()
    return RedirectResponse(url="/amo", status_code=302)


@app.get("/api/amo/oauth/callback")
async def amo_oauth_callback(code: str = "", db=Depends(_db)):
    cfg = _get_settings(db)
    if not code:
        raise HTTPException(status_code=400, detail="missing code")
    base = _amo_base_url(cfg)
    payload = {
        "client_id": cfg.amo_client_id,
        "client_secret": cfg.amo_client_secret,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": cfg.amo_redirect_uri,
    }
    async with httpx.AsyncClient() as client:
        r = await client.post(f"{base}/oauth2/access_token", json=payload, timeout=25.0)
    if r.status_code >= 400:
        raise HTTPException(status_code=400, detail=f"oauth failed: {r.text[:300]}")
    data = r.json()
    cfg.amo_access_token = str(data.get("access_token") or "")
    cfg.amo_refresh_token = str(data.get("refresh_token") or "")
    expires_in = int(data.get("expires_in") or 3600)
    cfg.amo_token_expires_at = utcnow() + timedelta(seconds=max(60, expires_in - 30))
    db.add(cfg)
    db.commit()
    return RedirectResponse(url="/amo", status_code=302)


async def _amo_fetch_lead_preview(db, cfg: AppSettings, lead_id: int) -> dict:
    """
    Fetch full lead data and prepare compact preview fields for TG notification.
    """
    try:
        full = await _amo_fetch_full_lead(db, cfg, int(lead_id))
        lead = (full or {}).get("lead") or {}
        if not lead:
            return {}
        contacts = (full or {}).get("contacts") or []
        company = (full or {}).get("company") or {}
        contact_name = ""
        if contacts and isinstance(contacts[0], dict):
            contact_name = str(contacts[0].get("name") or "")
        company_name = str(company.get("name") or "")
        embedded = lead.get("_embedded", {}) if isinstance(lead, dict) else {}

        return {
            "full_data": full or {},
            "lead_id": int(lead.get("id") or lead_id),
            "lead_name": lead.get("name", ""),
            "price": lead.get("price"),
            "status_id": int(lead.get("status_id") or 0),
            "pipeline_id": int(lead.get("pipeline_id") or 0),
            "contact_name": contact_name,
            "company_name": company_name,
            "tags": embedded.get("tags", []),
        }
    except Exception:
        return {}


@app.post("/api/amo/webhook")
async def amo_webhook(request: Request, db=Depends(_db)):
    cfg = _get_settings(db)
    if not cfg.amo_enabled:
        return JSONResponse({"ok": True, "ignored": "amo_disabled"})

    if cfg.amo_webhook_secret:
        sec = request.headers.get("x-amo-signature", "") or request.query_params.get("secret", "")
        if not sec or not secrets.compare_digest(sec, cfg.amo_webhook_secret):
            raise HTTPException(status_code=401, detail="unauthorized")

    raw = (await request.body()).decode("utf-8", "ignore")
    payload_json = None
    try:
        payload_json = await request.json()
    except Exception:
        payload_json = None

    lead_ids = _extract_amo_lead_ids(raw_text=raw, payload_json=payload_json)
    created_ids: list[int] = []
    skipped_not_unprocessed: list[int] = []

    for lid in lead_ids:
        exists = db.exec(
            select(AmoLeadInbox)
            .where(AmoLeadInbox.amo_lead_id == lid)
            .order_by(AmoLeadInbox.created_at.desc())
        ).first()
        if exists and exists.status in ("new", "sent_to_tg", "queued_accept", "processing_accept"):
            continue

        # Webhook payloads are often partial, always pull full lead data from amo API.
        preview = await _amo_fetch_lead_preview(db, cfg, int(lid))
        status_id = int(preview.get("status_id") or 0)
        unprocessed_id = int(cfg.amo_status_unprocessed_id or 0)
        if unprocessed_id > 0 and status_id > 0 and status_id != unprocessed_id:
            skipped_not_unprocessed.append(int(lid))
            continue

        row = AmoLeadInbox(
            amo_lead_id=int(lid),
            source="webhook",
            raw_json=raw[:20000],
            status="new",
            updated_at=utcnow(),
            result_json=json.dumps(preview.get("full_data") or {}, ensure_ascii=False)[:200000],
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        created_ids.append(int(row.id or 0))

        if settings.tg_chat_id:
            lead_name = preview.get("lead_name") or f"Lead #{lid}"
            price = preview.get("price")
            contact_name = preview.get("contact_name")
            company_name = preview.get("company_name")
            tags = preview.get("tags", [])

            parts = ["<b>New amoCRM partner lead</b>"]
            parts.append(f"<b>Lead:</b> {str(lead_name).replace('<', '&lt;').replace('>', '&gt;')}")
            parts.append(f"<b>ID:</b> {lid}")
            if price:
                parts.append(f"<b>Budget:</b> {int(price):,} RUB")
            if contact_name:
                parts.append(f"<b>Contact:</b> {str(contact_name).replace('<', '&lt;').replace('>', '&gt;')}")
            if company_name:
                parts.append(f"<b>Company:</b> {str(company_name).replace('<', '&lt;').replace('>', '&gt;')}")
            if tags:
                tags_str = ", ".join(str(t) for t in tags[:5])
                parts.append(f"<b>Tags:</b> {tags_str}")

            amo_link = f"https://{cfg.amo_subdomain.strip()}.amocrm.ru/leads/detail/{lid}"
            parts.append(f"<a href=\"{amo_link}\">Open in amoCRM</a>")

            kb = {
                "inline_keyboard": [[
                    {"text": "Accept", "callback_data": f"amo:accept:{row.id}"},
                    {"text": "Reject", "callback_data": f"amo:reject:{row.id}"},
                ]]
            }

            await send_message_html(
                chat_id=settings.tg_chat_id,
                html="\n\n".join(parts),
                reply_markup=kb,
            )
            row.status = "sent_to_tg"
            row.updated_at = utcnow()
            db.add(row)
            db.commit()

    return JSONResponse(
        {
            "ok": True,
            "created": len(created_ids),
            "ids": created_ids,
            "skipped_not_unprocessed": skipped_not_unprocessed,
        }
    )

@app.get("/discovery", response_class=HTMLResponse)
def discovery_page(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    cfg = _get_settings(db)
    now_msk = datetime.now(MSK_TZ)
    day_start_msk = now_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_utc = day_start_msk.astimezone(timezone.utc)
    week_cutoff_utc = (now_msk - timedelta(days=7)).astimezone(timezone.utc)
    month_cutoff_utc = (now_msk - timedelta(days=30)).astimezone(timezone.utc)

    total_messages = db.exec(select(func.count()).select_from(TgMonitorEvent)).one() or 0
    leads_day = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= day_start_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )
    leads_week = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= week_cutoff_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )
    leads_month = (
        db.exec(
            select(func.count())
            .select_from(TgMonitorEvent)
            .where(TgMonitorEvent.created_at >= month_cutoff_utc, TgMonitorEvent.is_lead == True)  # noqa: E712
        ).one()
        or 0
    )

    added_today = len(
        db.exec(select(DiscoveryTarget).where(DiscoveryTarget.created_at >= day_start_utc)).all()
    )
    joins_today = len(
        db.exec(
            select(DiscoveryLog).where(
                DiscoveryLog.created_at >= day_start_utc,
                DiscoveryLog.event == "join_ok",
            )
        ).all()
    )
    leaves_today = len(
        db.exec(
            select(DiscoveryLog).where(
                DiscoveryLog.created_at >= day_start_utc,
                DiscoveryLog.event == "leave",
            )
        ).all()
    )

    rows = db.exec(
        select(DiscoveryLog).order_by(DiscoveryLog.created_at.desc()).limit(200)
    ).all()
    return templates.TemplateResponse(
        request,
        "discovery.html",
        {
            "cfg": cfg,
            "rows": rows,
            "stats": {
                "total_messages": int(total_messages),
                "leads_day": int(leads_day),
                "leads_week": int(leads_week),
                "leads_month": int(leads_month),
                "added_today": added_today,
                "joins_today": joins_today,
                "leaves_today": leaves_today,
            },
        },
    )


@app.get("/discovery/targets", response_class=HTMLResponse)
def discovery_targets_page(request: Request, db=Depends(_db), _auth=Depends(_require_auth)):
    rows = db.exec(
        select(DiscoveryTarget).order_by(DiscoveryTarget.updated_at.desc()).limit(1000)
    ).all()
    return templates.TemplateResponse(
        request,
        "discovery_targets.html",
        {
            "rows": rows,
        },
    )


@app.post("/discovery/targets/{target_id}/queue")
def queue_discovery_target(target_id: int, db=Depends(_db), _auth=Depends(_require_auth)):
    row = db.exec(select(DiscoveryTarget).where(DiscoveryTarget.id == target_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    row.queued_join_scan = True
    row.queued_at = utcnow()
    row.status = "queued_join_scan"
    row.note = "queued_by_user"
    row.updated_at = utcnow()
    db.add(row)
    db.commit()
    return RedirectResponse(url="/discovery/targets", status_code=303)


@app.post("/settings")
def update_settings(
    keywords_enabled: bool = Form(False),
    stopwords_enabled: bool = Form(False),
    llm_enabled: bool = Form(False),
    keywords_text: str = Form(""),
    stopwords_text: str = Form(""),
    llm_model_priority_text: str = Form(""),
    llm_max_attempts: int = Form(3),
    llm_timeout_s: float = Form(15.0),
    exclude_openai_owned_models: bool = Form(False),
    llm_models_config_json: str = Form("{}"),
    db=Depends(_db),
    _auth=Depends(_require_auth),
):
    cfg = _get_settings(db)
    cfg.keywords_enabled = bool(keywords_enabled)
    cfg.stopwords_enabled = bool(stopwords_enabled)
    cfg.llm_enabled = bool(llm_enabled)
    cfg.keywords_text = keywords_text or ""
    cfg.stopwords_text = stopwords_text or ""
    cfg.llm_model_priority_text = (llm_model_priority_text or "").strip() + "\n" if (llm_model_priority_text or "").strip() else default_model_priority_text()
    cfg.llm_max_attempts = max(1, min(12, int(llm_max_attempts or 3)))
    cfg.llm_timeout_s = max(3.0, min(90.0, float(llm_timeout_s or 15.0)))
    cfg.exclude_openai_owned_models = bool(exclude_openai_owned_models)
    # Normalize to valid compact JSON; fallback to empty object on invalid payload.
    try:
        parsed_cfg = json.loads((llm_models_config_json or "{}").strip() or "{}")
        if not isinstance(parsed_cfg, dict):
            parsed_cfg = {}
    except Exception:
        parsed_cfg = {}
    cfg.llm_models_config_json = json.dumps(parsed_cfg, ensure_ascii=False)
    db.add(cfg)
    db.commit()
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/discovery")
def update_discovery_settings(
    discovery_enabled: bool = Form(False),
    discovery_queries_text: str = Form(""),
    discovery_interval_s: int = Form(1800),
    scan_days: int = Form(30),
    scan_max_messages: int = Form(300),
    scan_llm_sample: int = Form(12),
    join_per_day: int = Form(8),
    join_per_hour: int = Form(2),
    auto_leave_if_no_candidates: bool = Form(False),
    search_requests_per_day: int = Form(300),
    search_requests_per_hour: int = Form(30),
    queries_per_tick: int = Form(25),
    search_results_per_query: int = Form(20),
    db=Depends(_db),
    _auth=Depends(_require_auth),
):
    cfg = _get_settings(db)
    cfg.discovery_enabled = bool(discovery_enabled)
    cfg.discovery_queries_text = discovery_queries_text or ""
    cfg.discovery_interval_s = max(60, int(discovery_interval_s or 1800))
    cfg.scan_days = max(1, int(scan_days or 30))
    cfg.scan_max_messages = max(30, int(scan_max_messages or 300))
    cfg.scan_llm_sample = max(1, int(scan_llm_sample or 12))
    cfg.join_per_day = max(1, int(join_per_day or 8))
    cfg.join_per_hour = max(1, int(join_per_hour or 2))
    cfg.auto_leave_if_no_candidates = bool(auto_leave_if_no_candidates)
    cfg.search_requests_per_day = max(20, int(search_requests_per_day or 300))
    cfg.search_requests_per_hour = max(5, int(search_requests_per_hour or 30))
    cfg.queries_per_tick = max(1, int(queries_per_tick or 25))
    cfg.search_results_per_query = max(5, min(50, int(search_results_per_query or 20)))
    db.add(cfg)
    db.commit()
    return RedirectResponse(url="/discovery", status_code=303)


@app.get("/api/discovery/logs")
def discovery_logs(limit: int = 200, db=Depends(_db), _auth=Depends(_require_auth)) -> list[dict]:
    lim = min(max(int(limit or 200), 1), 1000)
    rows = db.exec(select(DiscoveryLog).order_by(DiscoveryLog.created_at.desc()).limit(lim)).all()
    return [
        {
            "id": r.id,
            "created_at": r.created_at.isoformat(),
            "level": r.level,
            "event": r.event,
            "message": r.message,
            "chat_username": r.chat_username,
            "query": r.query,
        }
        for r in rows
    ]


@app.get("/api/discovery/targets")
def discovery_targets(limit: int = 500, db=Depends(_db), _auth=Depends(_require_auth)) -> list[dict]:
    lim = min(max(int(limit or 500), 1), 5000)
    rows = db.exec(select(DiscoveryTarget).order_by(DiscoveryTarget.updated_at.desc()).limit(lim)).all()
    return [
        {
            "id": r.id,
            "created_at": r.created_at.isoformat(),
            "updated_at": r.updated_at.isoformat(),
            "target_key": r.target_key,
            "target": r.target,
            "username": r.username,
            "source": r.source,
            "query": r.query,
            "status": r.status,
            "note": r.note,
            "queued_join_scan": bool(r.queued_join_scan),
            "queued_at": r.queued_at.isoformat() if r.queued_at else "",
        }
        for r in rows
    ]


@app.get("/api/internal/discovery/targets/pending")
def internal_discovery_targets_pending(limit: int = 100, db=Depends(_db), _it=Depends(_require_internal_token)) -> list[dict]:
    lim = min(max(int(limit or 100), 1), 1000)
    rows = db.exec(
        select(DiscoveryTarget)
        .where(DiscoveryTarget.queued_join_scan == True)  # noqa: E712
        .order_by(DiscoveryTarget.queued_at.asc(), DiscoveryTarget.updated_at.asc())
        .limit(lim)
    ).all()
    return [
        {
            "id": r.id,
            "target_key": r.target_key,
            "target": r.target,
            "username": r.username,
            "source": r.source,
            "query": r.query,
            "status": r.status,
            "note": r.note,
            "queued_at": r.queued_at.isoformat() if r.queued_at else "",
        }
        for r in rows
    ]


@app.post("/api/internal/discovery/log")
def internal_discovery_log(req: InternalDiscoveryLogRequest, db=Depends(_db), _it=Depends(_require_internal_token)) -> dict:
    row = DiscoveryLog(
        level=(req.level or "info")[:20],
        event=(req.event or "")[:80],
        message=(req.message or "")[:2000],
        chat_username=(req.chat_username or "")[:120],
        query=(req.query or "")[:200],
    )
    db.add(row)
    db.commit()
    return {"ok": True, "id": row.id}


@app.post("/api/internal/discovery/target")
def internal_discovery_target(req: InternalDiscoveryTargetRequest, db=Depends(_db), _it=Depends(_require_internal_token)) -> dict:
    key = (req.target_key or "").strip().casefold()
    if len(key) < 2:
        raise HTTPException(status_code=400, detail="target_key is required")

    now = utcnow()
    row = db.exec(select(DiscoveryTarget).where(DiscoveryTarget.target_key == key)).first()
    if row:
        # Update metadata, but keep original created_at.
        row.updated_at = now
        if req.target:
            row.target = (req.target or "")[:300]
        if req.username:
            row.username = (req.username or "")[:120]
        if req.source:
            row.source = (req.source or "")[:40]
        if req.query:
            row.query = (req.query or "")[:200]
        if req.status:
            row.status = (req.status or "")[:40]
        if req.note:
            row.note = (req.note or "")[:500]
        if req.queued_join_scan is not None:
            row.queued_join_scan = bool(req.queued_join_scan)
            row.queued_at = utcnow() if row.queued_join_scan else None
        db.add(row)
        db.commit()
        return {"ok": True, "created": False, "id": row.id}

    row = DiscoveryTarget(
        created_at=now,
        updated_at=now,
        target_key=key,
        target=(req.target or "")[:300],
        username=(req.username or "")[:120],
        source=(req.source or "")[:40],
        query=(req.query or "")[:200],
        status=(req.status or "discovered")[:40],
        note=(req.note or "")[:500],
        queued_join_scan=bool(req.queued_join_scan) if req.queued_join_scan is not None else False,
        queued_at=utcnow() if req.queued_join_scan else None,
    )
    db.add(row)
    db.commit()
    return {"ok": True, "created": True, "id": row.id}


@app.post("/api/events/{event_id}/retry")
async def retry_event(event_id: int, db=Depends(_db), _auth=Depends(_require_auth)) -> JSONResponse:
    row = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == event_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    row.status = EventStatus.pending
    row.last_error = ""
    db.add(row)
    db.commit()
    await process_event(event_id)
    return JSONResponse({"ok": True})


@app.post("/api/test/notify")
async def test_notify(_auth=Depends(_require_auth)) -> JSONResponse:
    # Minimal smoke test to validate bot credentials and outbound connectivity.
    await send_event_notification(
        event_id=0,
        excerpt="TEST notification",
        summary="Тестовое уведомление: проверка отправки в Telegram.",
        link=settings.public_base_url.rstrip("/") + "/",
        is_lead=None,
    )
    return JSONResponse({"ok": True})


@app.post("/api/tg/webhook")
async def tg_webhook(request: Request) -> JSONResponse:
    # Validate Telegram secret token header.
    if not settings.tg_webhook_secret:
        raise HTTPException(status_code=500, detail="TG_WEBHOOK_SECRET not configured")
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not secrets.compare_digest(secret, settings.tg_webhook_secret):
        raise HTTPException(status_code=401, detail="unauthorized")

    update = await request.json()
    cq = update.get("callback_query")
    if not cq:
        return JSONResponse({"ok": True})

    cq_id = cq.get("id", "")
    data = cq.get("data", "") or ""
    msg = cq.get("message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")

    # Supported callback data:
    # - fb:lead:{id} / fb:not_lead:{id}
    # - amo:accept:{id} / amo:reject:{id}
    parts = data.split(":")
    if len(parts) != 3:
        if cq_id:
            await answer_callback_query(callback_query_id=cq_id, text="Unknown button")
        return JSONResponse({"ok": True})

    namespace = parts[0]
    action = parts[1]
    try:
        obj_id = int(parts[2])
    except Exception:
        if cq_id:
            await answer_callback_query(callback_query_id=cq_id, text="Invalid id")
        return JSONResponse({"ok": True})

    if namespace == "amo":
        with session() as db:
            row = db.exec(select(AmoLeadInbox).where(AmoLeadInbox.id == obj_id)).first()
            if not row:
                if cq_id:
                    await answer_callback_query(callback_query_id=cq_id, text="Lead not found")
                return JSONResponse({"ok": True})

            if action == "accept":
                if row.status in ("queued_accept", "processing_accept"):
                    if cq_id:
                        await answer_callback_query(callback_query_id=cq_id, text="Already processing")
                    return JSONResponse({"ok": True})
                if row.status == "accepted":
                    if cq_id:
                        await answer_callback_query(callback_query_id=cq_id, text="Already accepted")
                    return JSONResponse({"ok": True})
                row.decision = "accept"
                row.status = "queued_accept"
                now = utcnow()
                row.accept_attempts = 0
                row.accept_started_at = row.created_at or now
                row.accept_deadline_at = row.accept_started_at + timedelta(minutes=22)
                row.accept_last_try_at = None
                row.error = ""
                row.updated_at = utcnow()
                db.add(row)
                db.commit()
                asyncio.create_task(_process_amo_accept_async(int(row.id)))
                if cq_id:
                    await answer_callback_query(callback_query_id=cq_id, text="Accepted, processing")
            elif action == "reject":
                if row.status == "accepted":
                    if cq_id:
                        await answer_callback_query(callback_query_id=cq_id, text="Already accepted")
                    return JSONResponse({"ok": True})
                row.decision = "reject"
                row.status = "rejected"
                row.updated_at = utcnow()
                db.add(row)
                db.commit()
                if cq_id:
                    await answer_callback_query(callback_query_id=cq_id, text="Marked as rejected")
            else:
                if cq_id:
                    await answer_callback_query(callback_query_id=cq_id, text="Unknown action")
                return JSONResponse({"ok": True})

        if chat_id is not None and message_id is not None:
            try:
                await edit_message_reply_markup(chat_id=int(chat_id), message_id=int(message_id))
            except Exception:
                pass
        return JSONResponse({"ok": True})
    if namespace != "fb":
        if cq_id:
            await answer_callback_query(callback_query_id=cq_id, text="Unknown button")
        return JSONResponse({"ok": True})

    additions: list[str] = []
    with session() as db:
        ev = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == obj_id)).first()
        if not ev:
            if cq_id:
                await answer_callback_query(callback_query_id=cq_id, text="Not found")
            return JSONResponse({"ok": True})

        ev.feedback = "lead" if action == "lead" else "not_lead"
        ev.feedback_at = utcnow()
        db.add(ev)
        db.commit()

        if action != "lead":
            cfg = _get_settings(db)
            try:
                additions, _model = await suggest_stopwords(
                    text=ev.text,
                    existing_stopwords_text=cfg.stopwords_text,
                    options=_llm_opts(cfg),
                )
            except Exception as e:
                ev.last_error = (ev.last_error + "\n" if ev.last_error else "") + f"stopwords: {e}"
                db.add(ev)
                db.commit()
                additions = []

            if additions:
                existing = [x.strip() for x in (cfg.stopwords_text or "").splitlines() if x.strip()]
                existing_set = set(x.casefold() for x in existing)
                new = [w for w in additions if w.casefold() not in existing_set]
                if new:
                    cfg.stopwords_text = (cfg.stopwords_text.rstrip() + "\n" if cfg.stopwords_text.strip() else "") + "\n".join(new) + "\n"
                    db.add(cfg)
                    db.commit()
                    ev.stopwords_added = "\n".join(new)
                    db.add(ev)
                    db.commit()

    if cq_id:
        if action == "lead":
            await answer_callback_query(callback_query_id=cq_id, text="Marked: lead")
        else:
            msg_txt = "Marked: not lead"
            if additions:
                msg_txt += f" (stopwords: {', '.join(additions[:4])})"
            await answer_callback_query(callback_query_id=cq_id, text=msg_txt)

    if chat_id is not None and message_id is not None:
        try:
            await edit_message_reply_markup(chat_id=int(chat_id), message_id=int(message_id))
        except Exception:
            pass

    if action != "lead" and additions and settings.tg_chat_id:
        try:
            await send_message_html(
                chat_id=settings.tg_chat_id,
                html="<b>Stopwords updated:</b> " + ", ".join([w.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;') for w in additions]),
            )
        except Exception:
            pass

    return JSONResponse({"ok": True})


