import logging
from pathlib import Path
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import text
from fastapi import Depends

from .config import get_settings

logger = logging.getLogger(__name__)

_engine = None
_session_factory = None


async def connect() -> None:
    global _engine, _session_factory
    if _engine is not None:
        return
    settings = get_settings()
    url = settings.database_url
    if url.startswith("sqlite://"):
        url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    logger.info("Connecting to database", extra={"db_url": url})
    _engine = create_async_engine(url, echo=False)
    _session_factory = async_sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    await _run_migrations()


async def disconnect() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL into individual statements (SQLite executes one at a time)."""
    statements = []
    for stmt in sql.split(";"):
        lines = [l for l in stmt.split("\n") if not l.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


async def _run_migrations() -> None:
    root = Path(__file__).resolve().parents[2]
    schema_path = root / "sql" / "schema.sql"
    functions_path = root / "sql" / "functions.sql"

    async with _session_factory() as session:
        for path in (schema_path, functions_path):
            sql = path.read_text(encoding="utf-8")
            for stmt in _split_sql_statements(sql):
                if stmt:
                    logger.info("Applying SQL file", extra={"path": str(path)})
                    await session.execute(text(stmt))
        await session.commit()


async def get_session() -> AsyncIterator[AsyncSession]:
    async with _session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit()


def get_session_dep(session: AsyncSession = Depends(get_session)) -> AsyncSession:
    return session
