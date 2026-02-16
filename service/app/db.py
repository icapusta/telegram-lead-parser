from __future__ import annotations

from sqlmodel import SQLModel, create_engine, Session
from sqlalchemy.pool import NullPool

from .config import settings


if settings.db_url.startswith("sqlite:"):
    # SQLite + many short-lived background tasks: avoid QueuePool exhaustion.
    engine = create_engine(
        settings.db_url,
        connect_args={"check_same_thread": False, "timeout": 30},
        poolclass=NullPool,
    )
else:
    engine = create_engine(settings.db_url)


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    _migrate_sqlite()


def _migrate_sqlite() -> None:
    # SQLModel/SQLAlchemy doesn't auto-migrate schemas. For this single-user SQLite MVP,
    # we apply minimal additive migrations in-place.
    if not settings.db_url.startswith("sqlite:"):
        return
    try:
        with engine.connect() as conn:
            res = conn.exec_driver_sql("PRAGMA table_info(tgmonitorevent)").fetchall()
            cols = {r[1] for r in (res or [])}  # (cid, name, type, notnull, dflt, pk)
            stmts: list[str] = []
            if "feedback" not in cols:
                stmts.append("ALTER TABLE tgmonitorevent ADD COLUMN feedback VARCHAR DEFAULT ''")
            if "feedback_at" not in cols:
                stmts.append("ALTER TABLE tgmonitorevent ADD COLUMN feedback_at DATETIME")
            if "stopwords_added" not in cols:
                stmts.append("ALTER TABLE tgmonitorevent ADD COLUMN stopwords_added VARCHAR DEFAULT ''")

            app_res = conn.exec_driver_sql("PRAGMA table_info(appsettings)").fetchall()
            app_cols = {r[1] for r in (app_res or [])}
            if "discovery_enabled" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN discovery_enabled BOOLEAN DEFAULT 1")
            if "discovery_queries_text" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN discovery_queries_text VARCHAR DEFAULT ''")
            if "discovery_interval_s" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN discovery_interval_s INTEGER DEFAULT 1800")
            if "scan_days" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN scan_days INTEGER DEFAULT 30")
            if "scan_max_messages" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN scan_max_messages INTEGER DEFAULT 300")
            if "scan_llm_sample" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN scan_llm_sample INTEGER DEFAULT 12")
            if "join_per_day" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN join_per_day INTEGER DEFAULT 8")
            if "join_per_hour" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN join_per_hour INTEGER DEFAULT 2")
            if "auto_leave_if_no_candidates" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN auto_leave_if_no_candidates BOOLEAN DEFAULT 1")
            if "search_requests_per_day" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN search_requests_per_day INTEGER DEFAULT 300")
            if "search_requests_per_hour" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN search_requests_per_hour INTEGER DEFAULT 30")
            if "queries_per_tick" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN queries_per_tick INTEGER DEFAULT 25")
            if "search_results_per_query" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN search_results_per_query INTEGER DEFAULT 20")
            if "llm_model_priority_text" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN llm_model_priority_text VARCHAR DEFAULT ''")
            if "llm_max_attempts" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN llm_max_attempts INTEGER DEFAULT 3")
            if "llm_timeout_s" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN llm_timeout_s FLOAT DEFAULT 15.0")
            if "exclude_openai_owned_models" not in app_cols:
                stmts.append("ALTER TABLE appsettings ADD COLUMN exclude_openai_owned_models BOOLEAN DEFAULT 1")

            tgt_res = conn.exec_driver_sql("PRAGMA table_info(discoverytarget)").fetchall()
            tgt_cols = {r[1] for r in (tgt_res or [])}
            if tgt_cols:
                if "queued_join_scan" not in tgt_cols:
                    stmts.append("ALTER TABLE discoverytarget ADD COLUMN queued_join_scan BOOLEAN DEFAULT 0")
                if "queued_at" not in tgt_cols:
                    stmts.append("ALTER TABLE discoverytarget ADD COLUMN queued_at DATETIME")

            for st in stmts:
                conn.exec_driver_sql(st)
    except Exception:
        # Non-fatal: app can still run if table doesn't exist yet.
        return


def session() -> Session:
    return Session(engine)
