from __future__ import annotations

from sqlmodel import SQLModel, create_engine, Session

from .config import settings


engine = create_engine(settings.db_url, connect_args={"check_same_thread": False})


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

            for st in stmts:
                conn.exec_driver_sql(st)
    except Exception:
        # Non-fatal: app can still run if table doesn't exist yet.
        return


def session() -> Session:
    return Session(engine)
