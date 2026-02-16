from __future__ import annotations

import json
from datetime import datetime

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import select

from .config import settings
from .db import init_db, session
from .models import TgMonitorEvent, EventStatus
from .schemas import TgMonitorPayload
from .llm import classify
from .notify import send_lead_notification, utcnow


app = FastAPI(title="telegram-lead-parser", version="0.1.0")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
def _startup() -> None:
    init_db()


def _db():
    with session() as s:
        yield s


@app.get("/health")
def health() -> dict:
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}


@app.post("/api/ingest/tg-monitor")
async def ingest(payload: TgMonitorPayload, db=Depends(_db)) -> dict:
    ev = TgMonitorEvent(
        text=payload.text,
        chat_title=payload.chat_title,
        sender_name=payload.sender_name,
        sender_link=payload.sender_link,
        sender_handle=payload.sender_handle,
        link=payload.link,
        keywords_json=json.dumps(payload.keywords, ensure_ascii=False),
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
def dashboard(request: Request, db=Depends(_db)):
    rows = db.exec(
        select(TgMonitorEvent).order_by(TgMonitorEvent.created_at.desc()).limit(200)
    ).all()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "rows": rows,
            "public_base_url": settings.public_base_url.rstrip("/"),
        },
    )


@app.post("/api/events/{event_id}/retry")
async def retry_event(event_id: int, db=Depends(_db)) -> JSONResponse:
    row = db.exec(select(TgMonitorEvent).where(TgMonitorEvent.id == event_id)).first()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    row.status = EventStatus.pending
    row.last_error = ""
    db.add(row)
    db.commit()
    await _process_event(event_id, db)
    return JSONResponse({"ok": True})
