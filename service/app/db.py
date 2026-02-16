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
            for st in stmts:
                conn.exec_driver_sql(st)
    except Exception:
        # Non-fatal: app can still run if table doesn't exist yet.
        return


def session() -> Session:
    return Session(engine)
