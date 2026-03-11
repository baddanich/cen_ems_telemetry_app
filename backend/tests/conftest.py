import os
from pathlib import Path
from typing import AsyncIterator

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (AsyncSession,
                                    async_sessionmaker,
                                    create_async_engine)
from backend.app.config import get_settings

# Use shared in-memory SQLite for tests (so app and tests use same DB)
os.environ.setdefault(
    "DATABASE_URL",
    "sqlite+aiosqlite:///file:testmem?mode=memory&cache=shared",
)


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL into individual statements (SQLite executes one at a time)."""
    statements = []
    for stmt in sql.split(";"):
        lines = [
            el for el in stmt.split("\n")
            if not el.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if stmt:
            statements.append(stmt)
    return statements


async def _apply_migrations(session: AsyncSession) -> None:
    root = Path(__file__).resolve().parents[2]
    schema_path = root / "sql" / "schema.sql"

    # Drop tables so CREATE TABLE IF NOT EXISTS uses latest schema
    for table in ("measurements", "raw_events"):
        await session.execute(text(f"DROP TABLE IF EXISTS {table}"))
    await session.commit()

    sql = schema_path.read_text(encoding="utf-8")
    for stmt in _split_sql_statements(sql):
        if stmt:
            await session.execute(text(stmt))
    await session.commit()


@pytest_asyncio.fixture
async def db_session() -> AsyncIterator[AsyncSession]:
    settings = get_settings()
    url = settings.database_url
    if url.startswith("sqlite://"):
        url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    engine = create_async_engine(url, echo=False)
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )

    async with session_factory() as session:
        await _apply_migrations(session)

    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.rollback()


@pytest_asyncio.fixture
async def db_pool(db_session: AsyncSession) -> AsyncSession:
    """
    Alias for compatibility with existing tests that expect db_pool.
    """
    return db_session


@pytest_asyncio.fixture
async def ensure_db_connected():
    """
    Ensure the app's db module is connected
    (for API tests that use create_app)."""
    from backend.app.db import connect
    await connect()
    yield


@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_session: AsyncSession) -> AsyncIterator[None]:
    await db_session.execute(text("DELETE FROM measurements"))
    await db_session.execute(text("DELETE FROM raw_events"))
    await db_session.commit()
    yield
