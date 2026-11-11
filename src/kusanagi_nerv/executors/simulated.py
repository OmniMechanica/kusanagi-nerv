"""SIMULATED execution: elapsed database time, no model training or artifacts."""

import math
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kusanagi_nerv.db.models import SimulatedExecutionRow
from kusanagi_nerv.domain.execution import Claim, ExecutionStatus
from kusanagi_nerv.repositories.transactions import TransactionalRepository


class SimulatedExecutor(TransactionalRepository):
    kind = "local-simulated-v1"
    max_gpus = 0

    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], *, duration_seconds: float = 3
    ) -> None:
        super().__init__(sessions)
        if not math.isfinite(duration_seconds) or not 0 <= duration_seconds <= 3600:
            raise ValueError("Simulation duration must be finite and in [0, 3600]")
        self._duration = timedelta(seconds=duration_seconds)

    async def lookup(self, submission_id: str) -> ExecutionStatus:
        async with self._transaction() as session:
            row = await session.scalar(
                select(SimulatedExecutionRow).where(
                    SimulatedExecutionRow.submission_id == submission_id
                )
            )
            if row is None:
                return ExecutionStatus.NOT_FOUND
            now = await session.scalar(select(func.clock_timestamp()))
            return (
                ExecutionStatus.SUCCEEDED
                if now >= row.finish_at
                else ExecutionStatus.RUNNING
            )

    async def submit(self, claim: Claim) -> ExecutionStatus:
        # The controller filters GPU requests before creating any attempt.
        if claim.job.spec.resources.gpus > self.max_gpus:
            return ExecutionStatus.REJECTED
        async with self._transaction() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            await session.execute(
                insert(SimulatedExecutionRow)
                .values(
                    submission_id=claim.submission_id,
                    accepted_at=now,
                    finish_at=now + self._duration,
                )
                .on_conflict_do_nothing(index_elements=["submission_id"])
            )
        # State lives outside the controller process; restart can reconcile it.
        return await self.lookup(claim.submission_id)
