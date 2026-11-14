"""One active attempt per controller process; scale by running more processes."""

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import uuid4

from kusanagi_nerv.domain.execution import (
    Claim,
    ExecutionObservation,
    ExecutionStatus,
    Executor,
    ExecutorUnavailable,
    LeaseLost,
)
from kusanagi_nerv.repositories.attempts import AttemptRepository, lease_interval
from kusanagi_nerv.repositories.jobs import RepositoryUnavailable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ControllerSettings:
    lease_seconds: float = 30
    poll_seconds: float = 1
    executor_timeout_seconds: float = 5

    def __post_init__(self) -> None:
        lease_interval(self.lease_seconds)
        if not all(
            math.isfinite(value) and value > 0
            for value in (self.poll_seconds, self.executor_timeout_seconds)
        ):
            raise ValueError(
                "Poll interval and executor timeout must be finite/positive"
            )
        if (
            max(self.poll_seconds, self.executor_timeout_seconds) * 3
            >= self.lease_seconds
        ):
            raise ValueError(
                "Lease must exceed three times poll interval and call timeout"
            )


class Controller:
    def __init__(
        self,
        store: AttemptRepository,
        executor: Executor,
        *,
        settings: ControllerSettings | None = None,
        owner: str | None = None,
    ) -> None:
        self.store = store
        self.executor = executor
        self.settings = settings or ControllerSettings()
        self.owner = owner or f"controller-{uuid4()}"
        self.claim: Claim | None = None

    async def _call(
        self, operation: Callable[[], Awaitable[ExecutionObservation]]
    ) -> ExecutionObservation:
        try:
            async with asyncio.timeout(self.settings.executor_timeout_seconds):
                return await operation()
        except (TimeoutError, ExecutorUnavailable, RepositoryUnavailable, OSError):
            # A lost reply says nothing about whether the executor accepted work.
            return ExecutionObservation(ExecutionStatus.UNKNOWN)

    async def tick(self) -> None:
        """Run one bounded observation; call serially, never concurrently."""
        try:
            if self.claim is None:
                self.claim = await self.store.claim_next(
                    owner=self.owner,
                    executor_kind=self.executor.kind,
                    lease_seconds=self.settings.lease_seconds,
                    max_gpus=self.executor.max_gpus,
                    executor_target=getattr(self.executor, "target", "local"),
                )
            if self.claim is None:
                return
            self.claim = await self.store.renew(
                self.claim, lease_seconds=self.settings.lease_seconds
            )
            claim = self.claim
            if claim.job.requested_cancel_at is not None:
                observation = await self._call(
                    lambda: self.executor.cancel(claim.submission_id)
                )
            else:
                observation = await self._call(
                    lambda: self.executor.lookup(claim.submission_id)
                )
            if (
                claim.job.requested_cancel_at is None
                and observation.status == ExecutionStatus.NOT_FOUND
                and not claim.accepted
            ):
                # Check authority again after lookup, before any dispatch.
                claim = self.claim = await self.store.renew(
                    claim, lease_seconds=self.settings.lease_seconds
                )
                if claim.job.requested_cancel_at is not None:
                    observation = await self._call(
                        lambda: self.executor.cancel(claim.submission_id)
                    )
                else:
                    observation = await self._call(lambda: self.executor.submit(claim))
            self.claim = await self.store.observe(
                claim,
                observation.status,
                failure_kind=observation.failure_kind,
                result=observation.result,
            )
            logger.info(
                "owner=%s job=%s attempt=%s submission=%s epoch=%s observation=%s",
                claim.owner,
                claim.job.job_id,
                claim.attempt_id,
                claim.submission_id,
                claim.epoch,
                observation.status.value,
            )
        except LeaseLost:
            logger.info("owner=%s lease lost; dropping local claim", self.owner)
            self.claim = None
        except RepositoryUnavailable:
            # Do not dispatch after an unconfirmed DB operation. A future owner
            # will recover the same identity after expiry, even if commit succeeded.
            logger.warning(
                "owner=%s storage unavailable; dropping local claim", self.owner
            )
            self.claim = None

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(self.settings.poll_seconds)
