from __future__ import annotations

import json
import secrets
from datetime import datetime

from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlmodel import select

from .config import settings
from .db import init_db, session
from .models import TgMonitorEvent, EventStatus, AppSettings
from .schemas import TgMonitorPayload
from .filtering import hard_filter
from .llm import classify
from .notify import send_lead_notification, utcnow


app = FastAPI(title="telegram-lead-parser", version="0.1.0")
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()


@app.on_event("startup")
def _startup() -> None:
    init_db()


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


def _get_settings(db) -> AppSettings:
    row = db.exec(select(AppSettings).where(AppSettings.id == 1)).first()
    if row:
        return row
    row = AppSettings(id=1)
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

    # Fire-and-forget processing is fine for MVP; we also expose a manual retry in UI.
    # If this raises, we keep the event pending so a later worker can pick it up.
    try:
        await _process_event(ev.id, db)
    except Exception:
        pass

    return {"ok": True, "id": ev.id}


async def _process_event(event_id: int, db) -> None:
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

        if res.is_lead:
            excerpt = (ev.text or "").strip().replace("\n", " ")[:240]
            try:
                await send_lead_notification(excerpt=excerpt, summary=ev.summary, link=ev.link)
                ev.notified = True
                ev.notified_at = utcnow()
            except Exception as e:
                # Notifications are optional for now; we don't want the whole pipeline to fail
                # when bot settings are missing or Telegram is temporarily unavailable.
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


@app.post("/api/events/{event_id}/retry")
async def retry_event(event_id: int, db=Depends(_db), _auth=Depends(_require_auth)) -> JSONResponse:
    row = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == event_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    row.status = EventStatus.pending
    row.last_error = ""
    db.add(row)
    db.commit()
    await _process_event(event_id, db)
    return JSONResponse({"ok": True})


@app.post("/api/test/notify")
async def test_notify(_auth=Depends(_require_auth)) -> JSONResponse:
    # Minimal smoke test to validate bot credentials and outbound connectivity.
    await send_lead_notification(
        excerpt="TEST lead notification",
        summary="If you see this message, bot notify is wired correctly.",
        link=settings.public_base_url.rstrip("/") + "/",
    )
    return JSONResponse({"ok": True})
