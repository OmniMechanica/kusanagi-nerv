"""Short transactions with sanitized storage failures and no implicit retries."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from asyncpg import PostgresError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kusanagi_nerv.repositories.jobs import RepositoryUnavailable

logger = logging.getLogger(__name__)


class TransactionalRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        try:
            async with self._sessions.begin() as session:
                yield session
        except (SQLAlchemyError, PostgresError, OSError, TimeoutError) as exc:
            logger.warning("Storage operation failed (%s)", type(exc).__name__)
            raise RepositoryUnavailable("Job storage is unavailable") from exc
