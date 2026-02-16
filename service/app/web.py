from __future__ import annotations

import asyncio
import json
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlmodel import select

from .config import settings
from .db import init_db, session
from .models import TgMonitorEvent, EventStatus, AppSettings, DiscoveryLog, DiscoveryTarget
from .schemas import (
    TgMonitorPayload,
    InternalClassifyRequest,
    InternalClassifyResponse,
    InternalDiscoveryLogRequest,
    InternalDiscoveryTargetRequest,
)
from .filtering import hard_filter, request_intent_filter
from .llm import classify, suggest_stopwords
from .notify import send_event_notification, utcnow
from .tg_bot_api import set_webhook, answer_callback_query, edit_message_reply_markup, send_message_html


app = FastAPI(title="telegram-lead-parser", version="0.1.0")
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()
MSK_TZ = timezone(timedelta(hours=3))

DEFAULT_DISCOVERY_QUERIES_TEXT = (
    "ищу подрядчика\n"
    "нужен специалист\n"
    "ищем исполнителя\n"
    "чат предпринимателей\n"
    "бизнес клуб чат\n"
    "нетворкинг чат\n"
    "биржа заказов\n"
    "тендеры на разработку\n"
    "проекты на интеграцию\n"
    "crm чат\n"
    "внедрение crm\n"
    "интеграторы crm\n"
    "настройка crm\n"
    "автоматизация бизнеса\n"
    "автоматизация отдела продаж\n"
    "бизнес процессы чат\n"
    "контроль менеджеров\n"
    "воронки продаж чат\n"
    "сквозная аналитика чат\n"
    "bi аналитика чат\n"
    "amocrm чат\n"
    "amocrm интеграторы\n"
    "amocrm партнеры\n"
    "amocrm сообщество\n"
    "битрикс24 чат\n"
    "битрикс24 разработчики\n"
    "битрикс24 интеграторы\n"
    "bitrix24 community\n"
    "yclients чат\n"
    "yclients интеграторы\n"
    "getcourse чат\n"
    "getcourse интеграция\n"
    "retailcrm чат\n"
    "megaplan чат\n"
    "мойсклад чат\n"
    "1с crm чат\n"
    "digital агентства чат\n"
    "маркетинг чат\n"
    "smm чат\n"
    "seo чат\n"
    "web разработка чат\n"
    "backend разработчики чат\n"
    "frontend разработчики чат\n"
    "python разработчики чат\n"
    "php разработчики чат\n"
    "laravel чат\n"
    "node js чат\n"
    "интеграция api\n"
    "webhook чат\n"
    "it фриланс чат\n"
    "it биржа заказов\n"
    "saas чат\n"
    "стартап чат\n"
    "product manager чат\n"
    "предприниматели чат\n"
    "владельцы бизнеса чат\n"
    "малый бизнес чат\n"
    "средний бизнес чат\n"
    "b2b предприниматели чат\n"
    "руководители чат\n"
    "директора чат\n"
    "собственники бизнеса чат\n"
    "ceo чат\n"
    "масштабирование бизнеса чат\n"
    "отдел продаж чат\n"
    "роп чат\n"
    "руководитель отдела продаж чат\n"
    "коммерческий директор чат\n"
    "лиды чат\n"
    "онлайн школа чат\n"
    "владельцы онлайн школ чат\n"
    "инфобизнес чат\n"
    "запуски чат\n"
    "edtech чат\n"
    "бьюти бизнес чат\n"
    "салон красоты владельцы чат\n"
    "косметология чат\n"
    "стоматология чат\n"
    "частная клиника чат\n"
    "владельцы клиник чат\n"
    "агентство недвижимости чат\n"
    "риэлторы чат\n"
    "застройщики чат\n"
    "автосервис чат\n"
    "сто владельцы чат\n"
    "интернет магазин чат\n"
    "ecommerce чат\n"
    "wildberries продавцы чат\n"
    "ozon продавцы чат\n"
    "shopify чат\n"
    "производство владельцы чат\n"
    "оптовики чат\n"
    "дистрибьюторы чат\n"
    "ресторанный бизнес чат\n"
    "владельцы кафе чат\n"
    "horeca чат\n"
    "фитнес клуб владельцы чат\n"
    "психологи чат\n"
    "коучи чат\n"
    "консультанты чат\n"
    "личный бренд чат\n"
    "логистика чат\n"
    "грузоперевозки чат\n"
    "строительная компания чат\n"
    "подрядчики строительство чат\n"
    "ремонт бизнес чат\n"
    "сервисный бизнес чат\n"
    "crm чат москва\n"
    "amocrm чат москва\n"
    "битрикс24 чат москва\n"
    "предприниматели чат москва\n"
    "crm чат спб\n"
    "предприниматели чат спб\n"
    "crm чат екатеринбург\n"
    "предприниматели чат казань\n"
    "crm чат краснодар\n"
    "предприниматели чат новосибирск\n"
    "crm чат алматы\n"
    "предприниматели чат астана\n"
    "бизнес чат беларусь\n"
    "русскоязычные предприниматели чат\n"
    "бизнес чат дубай\n"
    "чат предпринимателей снг\n"
    "владельцы бизнеса + автоматизация\n"
    "digital агентства + подрядчики\n"
    "бьюти бизнес + автоматизация\n"
    "медицина + автоматизация crm\n"
    "онлайн школы + интеграции\n"
    "владельцы b2b\n"
    "b2b предприниматели чат\n"
    "производственные компании чат\n"
    "оптовая торговля чат\n"
    "дистрибьюторы чат\n"
    "завод владельцы\n"
    "фабрика предприниматели\n"
    "промышленность чат\n"
    "экспортёры чат\n"
    "импортёры чат\n"
    "контрактное производство чат\n"
    "тендерные продажи чат\n"
    "госзаказы чат\n"
    "b2b + владельцы\n"
    "производство + предприниматели\n"
    "оптовая компания + чат\n"
    "строительная компания чат\n"
    "застройщики чат\n"
    "девелоперы чат\n"
    "генподрядчики чат\n"
    "коммерческая недвижимость чат\n"
    "промышленное строительство\n"
    "инженерные компании чат\n"
    "проектные бюро чат\n"
    "производство владельцы\n"
    "автоматизация производства чат\n"
    "управление производством\n"
    "erp чат\n"
    "1с предприятие чат\n"
    "1с интеграция\n"
    "mes системы чат\n"
    "логистические компании чат\n"
    "грузоперевозки b2b\n"
    "экспедиторы чат\n"
    "транспортные компании владельцы\n"
    "складская логистика чат\n"
    "корпоративные продажи чат\n"
    "крупные сделки b2b\n"
    "коммерческий директор чат\n"
    "директора по развитию\n"
    "bdm чат\n"
    "enterprise продажи\n"
    "руководитель отдела продаж\n"
    "роп сообщество\n"
    "директор по продажам\n"
    "коммерческий директор\n"
    "масштабирование отдела продаж\n"
    "системный отдел продаж\n"
    "контроль менеджеров\n"
    "регламенты продаж\n"
    "системные интеграторы чат\n"
    "it интегратор сообщество\n"
    "веб-студии чат\n"
    "digital агентства владельцы\n"
    "маркетинговые агентства владельцы\n"
    "аутсорс разработка\n"
    "enterprise разработка\n"
    "инвестиционные компании чат\n"
    "управляющие компании\n"
    "финансовые консультанты\n"
    "private equity чат\n"
    "венчур чат\n"
    "франчайзи чат\n"
    "франчайзинг сообщество\n"
    "сеть филиалов\n"
    "масштабирование сети\n"
    "управление филиалами\n"
    "москва бизнес клуб\n"
    "москва предприниматели\n"
    "дубай бизнес русские\n"
    "европа b2b\n"
    "казахстан производство\n"
    "алматы предприниматели\n"
    "спб промышленность\n"
    "производство владельцы чат\n"
    "строительство владельцы чат\n"
    "логистика владельцы чат\n"
    "девелопмент владельцы чат\n"
    "финансы владельцы чат\n"
    "b2b генеральный директор чат\n"
    "производство генеральный директор чат\n"
    "строительство генеральный директор чат\n"
    "логистика коммерческий директор чат\n"
    "производство коммерческий директор чат\n"
    "строительство коммерческий директор чат\n"
    "b2b бизнес клуб чат\n"
    "производство бизнес клуб\n"
    "строительство закрытый клуб\n"
    "логистика закрытый клуб\n"
    "масштабирование производства чат\n"
    "масштабирование b2b продаж чат\n"
    "масштабирование строительного бизнеса чат\n"
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
        # Backfill reasonable defaults for discovery queries on existing DBs.
        if not (row.discovery_queries_text or "").strip():
            row.discovery_queries_text = DEFAULT_DISCOVERY_QUERIES_TEXT
            db.add(row)
            db.commit()
            db.refresh(row)
        return row
    row = AppSettings(id=1, discovery_queries_text=DEFAULT_DISCOVERY_QUERIES_TEXT)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


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
    }


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

    res, model_used = await classify(req.text)
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

            res, model_used = await classify(ev.text)
            ev.is_lead = res.is_lead
            ev.summary = res.summary
            ev.confidence = res.confidence
            ev.model_used = model_used
            ev.attempts = min(settings.llm_max_attempts, ev.attempts + 1)
            ev.status = EventStatus.done

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
    db=Depends(_db),
    _auth=Depends(_require_auth),
):
    cfg = _get_settings(db)
    cfg.keywords_enabled = bool(keywords_enabled)
    cfg.stopwords_enabled = bool(stopwords_enabled)
    cfg.llm_enabled = bool(llm_enabled)
    cfg.keywords_text = keywords_text or ""
    cfg.stopwords_text = stopwords_text or ""
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
        summary="Если ты видишь это сообщение, значит уведомления настроены правильно.",
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

    # Expect: fb:lead:{id} or fb:not_lead:{id}
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "fb":
        if cq_id:
            await answer_callback_query(callback_query_id=cq_id, text="Неизвестная кнопка")
        return JSONResponse({"ok": True})

    action = parts[1]
    try:
        event_id = int(parts[2])
    except Exception:
        if cq_id:
            await answer_callback_query(callback_query_id=cq_id, text="Некорректный id")
        return JSONResponse({"ok": True})

    # Update DB feedback
    additions: list[str] = []
    with session() as db:
        ev = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == event_id)).first()
        if not ev:
            if cq_id:
                await answer_callback_query(callback_query_id=cq_id, text="Не найдено")
            return JSONResponse({"ok": True})

        ev.feedback = "lead" if action == "lead" else "not_lead"
        ev.feedback_at = utcnow()
        db.add(ev)
        db.commit()

        if action != "lead":
            cfg = _get_settings(db)
            try:
                additions, _model = await suggest_stopwords(text=ev.text, existing_stopwords_text=cfg.stopwords_text)
            except Exception as e:
                ev.last_error = (ev.last_error + "\n" if ev.last_error else "") + f"stopwords: {e}"
                db.add(ev)
                db.commit()
                additions = []

            if additions:
                # Append new stopwords
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

    # UX: acknowledge and remove buttons
    if cq_id:
        if action == "lead":
            await answer_callback_query(callback_query_id=cq_id, text="Отмечено: Лид")
        else:
            msg_txt = "Отмечено: Не лид"
            if additions:
                msg_txt += f" (стоп-слова: {', '.join(additions[:4])})"
            await answer_callback_query(callback_query_id=cq_id, text=msg_txt)

    if chat_id is not None and message_id is not None:
        try:
            await edit_message_reply_markup(chat_id=int(chat_id), message_id=int(message_id))
        except Exception:
            pass

    # Optional: notify about added stopwords
    if action != "lead" and additions and settings.tg_chat_id:
        try:
            await send_message_html(
                chat_id=settings.tg_chat_id,
                html="<b>Стоп-слова обновлены:</b> " + ", ".join([w.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;') for w in additions]),
            )
        except Exception:
            pass

    return JSONResponse({"ok": True})
