"""Each test owns a newly created database; the supplied database is never reset."""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from kusanagi_nerv.db.connection import create_database_engine, database_url
from kusanagi_nerv.repositories.postgres import PostgresJobRepository

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def empty_database_url() -> Iterator[str]:
    configured = os.environ.get("KUSANAGI_TEST_DATABASE_URL")
    if not configured:
        pytest.skip("Set KUSANAGI_TEST_DATABASE_URL to run real PostgreSQL tests")
    admin_url = database_url(configured)
    name = f"kusanagi_test_{uuid4().hex}"

    async def manage(*, create: bool) -> None:
        connection = await asyncpg.connect(
            admin_url.set(drivername="postgresql").render_as_string(
                hide_password=False
            ),
            timeout=5,
        )
        try:
            # name is generated here from a fixed prefix and UUID, never user SQL.
            if create:
                await connection.execute(f'CREATE DATABASE "{name}"')
            else:
                await connection.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        finally:
            await connection.close()

    asyncio.run(manage(create=True))
    try:
        yield admin_url.set(database=name).render_as_string(hide_password=False)
    finally:
        asyncio.run(manage(create=False))


@pytest.fixture
def migrate(empty_database_url: str) -> Callable[..., subprocess.CompletedProcess[str]]:
    def run(*args: str) -> subprocess.CompletedProcess[str]:
        env = {**os.environ, "KUSANAGI_DATABASE_URL": empty_database_url}
        result = subprocess.run(
            [sys.executable, "-m", "alembic", *args],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    return run


@pytest.fixture
def postgres_url(empty_database_url: str, migrate: Callable[..., object]) -> str:
    migrate("upgrade", "head")
    return empty_database_url


@pytest.fixture
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_database_engine(database_url(postgres_url))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def repository(engine: AsyncEngine) -> PostgresJobRepository:
    return PostgresJobRepository(async_sessionmaker(engine, expire_on_commit=False))
