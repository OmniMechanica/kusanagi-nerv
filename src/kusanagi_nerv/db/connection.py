"""Explicit database configuration and bounded asynchronous connections."""

import os

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def database_url(value: str | None = None) -> URL:
    raw = value if value is not None else os.environ.get("KUSANAGI_DATABASE_URL")
    if not raw:
        raise ValueError("Set KUSANAGI_DATABASE_URL to a postgresql+asyncpg URL")
    try:
        url = make_url(raw)
    except (ArgumentError, ValueError):
        raise ValueError("Invalid KUSANAGI_DATABASE_URL") from None
    if url.drivername != "postgresql+asyncpg" or not url.database:
        raise ValueError(
            "KUSANAGI_DATABASE_URL must select a postgresql+asyncpg database"
        )
    return url


def create_database_engine(url: URL) -> AsyncEngine:
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        pool_timeout=5,
        hide_parameters=True,
        connect_args={
            "timeout": 5,
            "command_timeout": 10,
            "server_settings": {"statement_timeout": "10000", "lock_timeout": "5000"},
        },
    )
