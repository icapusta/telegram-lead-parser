from __future__ import annotations

from sqlmodel import SQLModel, create_engine, Session

from .config import settings


engine = create_engine(settings.db_url, connect_args={"check_same_thread": False})


def init_db() -> None:
    SQLModel.metadata.create_all(engine)


def session() -> Session:
    return Session(engine)

