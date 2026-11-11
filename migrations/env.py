"""Run explicit migrations using the same URL contract as the API."""

import asyncio

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import create_async_engine

from kusanagi_nerv.db.connection import database_url
from kusanagi_nerv.db.models import Base


def run_on_connection(connection: Connection) -> None:
    context.configure(
        connection=connection, target_metadata=Base.metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    engine = create_async_engine(
        database_url(),
        poolclass=pool.NullPool,
        hide_parameters=True,
        connect_args={"timeout": 5, "command_timeout": 30},
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_on_connection)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=database_url(),
        target_metadata=Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(run_online())
