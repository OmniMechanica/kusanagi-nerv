"""PostgreSQL claim/recovery and fenced observations, with no executor I/O."""

import math
import random
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from kusanagi_nerv.db.models import AttemptRow, JobRow
from kusanagi_nerv.domain.execution import (
    CheckpointRef,
    Claim,
    ExecutionResult,
    ExecutionStatus,
    FailureKind,
    LeaseLost,
    TrainingExecutionResult,
)
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.retries import RetryPolicy
from kusanagi_nerv.repositories.postgres import to_job
from kusanagi_nerv.repositories.transactions import TransactionalRepository


def lease_interval(seconds: float) -> timedelta:
    if not math.isfinite(seconds) or not 0 < seconds <= 3600:
        raise ValueError("Lease duration must be finite and in (0, 3600] seconds")
    return timedelta(seconds=seconds)


def to_claim(job: JobRow, attempt: AttemptRow) -> Claim:
    return Claim(
        job=to_job(job),
        attempt_id=attempt.attempt_id,
        submission_id=attempt.submission_id,
        owner=attempt.lease_owner,
        epoch=attempt.lease_epoch,
        accepted=attempt.accepted_at is not None,
        executor_target=attempt.executor_target,
        resume_checkpoint=CheckpointRef(**attempt.resume_checkpoint)
        if attempt.resume_checkpoint
        else None,
    )


@dataclass(frozen=True, slots=True)
class DispatchRecord:
    claim: Claim
    payload_hash: str
    started: bool
    revoked: bool


def dispatch_record(job: JobRow, attempt: AttemptRow) -> DispatchRecord:
    return DispatchRecord(
        to_claim(job, attempt),
        job.payload_hash,
        attempt.dispatch_started_at is not None,
        attempt.dispatch_revoked_at is not None,
    )


class AttemptRepository(TransactionalRepository):
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        retry_policy: RetryPolicy | None = None,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        super().__init__(sessions)
        self._retry_policy = retry_policy or RetryPolicy()
        self._jitter = jitter

    async def claim_next(
        self,
        *,
        owner: str,
        executor_kind: str,
        lease_seconds: float,
        max_gpus: int = 0,
        executor_target: str = "local",
    ) -> Claim | None:
        duration = lease_interval(lease_seconds)
        if not 0 < len(owner) <= 128 or not 0 < len(executor_kind) <= 64:
            raise ValueError("Invalid owner or executor identity")
        if max_gpus < 0:
            raise ValueError("GPU capacity must be nonnegative")
        if not 0 < len(executor_target) <= 512:
            raise ValueError("Invalid executor target")
        async with self._transaction() as session:
            # Every attempt operation locks its job first. SKIP LOCKED lets
            # other controllers claim different jobs without waiting on this one.
            job = await session.scalar(
                select(JobRow)
                .join(AttemptRow, AttemptRow.job_id == JobRow.job_id)
                .where(
                    JobRow.status.in_(
                        [JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.CANCELLING]
                    ),
                    AttemptRow.finished_at.is_(None),
                    AttemptRow.executor_kind == executor_kind,
                    AttemptRow.executor_target == executor_target,
                    AttemptRow.lease_expires_at <= func.clock_timestamp(),
                )
                .order_by(AttemptRow.lease_expires_at, JobRow.job_id)
                .limit(1)
                .with_for_update(of=JobRow, skip_locked=True)
            )
            if job is not None:
                attempt = await session.scalar(
                    select(AttemptRow)
                    .where(
                        AttemptRow.job_id == job.job_id,
                        AttemptRow.finished_at.is_(None),
                        AttemptRow.executor_kind == executor_kind,
                        AttemptRow.executor_target == executor_target,
                        AttemptRow.lease_expires_at <= func.clock_timestamp(),
                    )
                    .with_for_update()
                )
                # Recheck after the job lock; a concurrent heartbeat may have won.
                if attempt is None:
                    return None
                now = await session.scalar(select(func.clock_timestamp()))
                attempt.lease_owner = owner
                attempt.lease_epoch += 1
                attempt.claimed_at = attempt.heartbeat_at = now
                attempt.lease_expires_at = now + duration
            else:
                job = await session.scalar(
                    select(JobRow)
                    .where(
                        JobRow.requested_cancel_at.is_(None),
                        or_(
                            and_(
                                JobRow.status == JobStatus.QUEUED, JobRow.attempt == 0
                            ),
                            and_(
                                JobRow.status == JobStatus.RETRYING,
                                JobRow.retry_not_before <= func.clock_timestamp(),
                                JobRow.attempt
                                <= JobRow.spec_json["max_retries"].as_integer(),
                                select(AttemptRow.attempt_id)
                                .where(
                                    AttemptRow.job_id == JobRow.job_id,
                                    AttemptRow.attempt_no == JobRow.attempt,
                                    AttemptRow.finished_at.is_not(None),
                                    AttemptRow.last_observation.in_(
                                        [
                                            ExecutionStatus.FAILED,
                                            ExecutionStatus.REJECTED,
                                        ]
                                    ),
                                    AttemptRow.failure_kind == FailureKind.TRANSIENT,
                                    # A training checkpoint is tied to the
                                    # training executor and its recorded target.
                                    or_(
                                        and_(
                                            AttemptRow.executor_kind
                                            != "ray-training-v1",
                                            executor_kind != "ray-training-v1",
                                        ),
                                        and_(
                                            AttemptRow.executor_kind == executor_kind,
                                            AttemptRow.executor_target
                                            == executor_target,
                                        ),
                                    ),
                                    or_(
                                        AttemptRow.executor_kind != "ray-training-v1",
                                        and_(
                                            AttemptRow.retry_checkpoint.is_not(None),
                                            func.jsonb_typeof(
                                                AttemptRow.retry_checkpoint
                                            )
                                            == "object",
                                        ),
                                    ),
                                )
                                .exists(),
                            ),
                        ),
                        JobRow.spec_json["resources"]["gpus"].as_integer() <= max_gpus,
                        ~select(AttemptRow.attempt_id)
                        .where(
                            AttemptRow.job_id == JobRow.job_id,
                            AttemptRow.finished_at.is_(None),
                        )
                        .exists(),
                    )
                    .order_by(JobRow.created_at, JobRow.job_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if job is None:
                    return None
                now = await session.scalar(select(func.clock_timestamp()))
                attempt_id = uuid4()
                resume_checkpoint = None
                if job.status == JobStatus.RETRYING:
                    resume_checkpoint = await session.scalar(
                        select(AttemptRow.retry_checkpoint).where(
                            AttemptRow.job_id == job.job_id,
                            AttemptRow.attempt_no == job.attempt,
                        )
                    )
                attempt = AttemptRow(
                    attempt_id=attempt_id,
                    job_id=job.job_id,
                    attempt_no=job.attempt + 1,
                    submission_id=f"kusanagi-{attempt_id}",
                    executor_kind=executor_kind,
                    executor_target=executor_target,
                    resume_checkpoint=resume_checkpoint,
                    lease_owner=owner,
                    lease_epoch=1,
                    lease_expires_at=now + duration,
                    claimed_at=now,
                    heartbeat_at=now,
                    submission_intent_at=now,
                    last_observation=ExecutionStatus.UNKNOWN,
                )
                session.add(attempt)
                if job.status == JobStatus.RETRYING:
                    job.status = transition(JobStatus.RETRYING, JobStatus.QUEUED)
                job.attempt += 1
                job.retry_not_before = None
                job.waiting_reason = None
                job.error = None
                job.state_version += 1
                job.updated_at = now
            await session.flush()
            claim = to_claim(job, attempt)
        # A failed/ambiguous commit raises; no caller can dispatch from it.
        return claim

    async def _owned(
        self, session: AsyncSession, claim: Claim, *, duration: timedelta | None = None
    ) -> tuple[JobRow, AttemptRow]:
        job = await session.scalar(
            select(JobRow).where(JobRow.job_id == claim.job.job_id).with_for_update()
        )
        # Cancellation advances the job version without revoking its attempt.
        # The current lease owner consumes that newer intent under this lock;
        # late observations may finish it but can never resume normal dispatch.
        if (
            job is None
            or (
                job.state_version != claim.job.state_version
                and not (
                    job.status == JobStatus.CANCELLING
                    and job.requested_cancel_at is not None
                    and job.state_version > claim.job.state_version
                )
            )
            or job.status
            not in (JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.CANCELLING)
        ):
            raise LeaseLost("Job changed or is no longer executable")
        values = {"heartbeat_at": func.clock_timestamp()}
        if duration is not None:
            values["lease_expires_at"] = func.clock_timestamp() + duration
        # Evaluate wall-clock time at the write, after acquiring the job lock.
        # now()/transaction_timestamp() would allow a blocked, expired writer.
        attempt = await session.scalar(
            update(AttemptRow)
            .where(
                AttemptRow.attempt_id == claim.attempt_id,
                AttemptRow.job_id == job.job_id,
                AttemptRow.attempt_no == job.attempt,
                AttemptRow.lease_owner == claim.owner,
                AttemptRow.lease_epoch == claim.epoch,
                AttemptRow.finished_at.is_(None),
                AttemptRow.lease_expires_at > func.clock_timestamp(),
            )
            .values(**values)
            .returning(AttemptRow)
        )
        if attempt is None:
            raise LeaseLost("Attempt lease is no longer authoritative")
        return job, attempt

    async def renew(self, claim: Claim, *, lease_seconds: float) -> Claim:
        duration = lease_interval(lease_seconds)
        async with self._transaction() as session:
            job, attempt = await self._owned(session, claim, duration=duration)
            return to_claim(job, attempt)

    async def begin_dispatch(self, claim: Claim, kind: str = "ray-jobs-v1") -> bool:
        """Commit the possibility of an in-flight request before external I/O."""
        async with self._transaction() as session:
            job, attempt = await self._owned(session, claim)
            if (
                attempt.executor_kind != kind
                or attempt.executor_target != claim.executor_target
            ):
                raise LeaseLost("Executor target does not match the stored attempt")
            if (
                job.requested_cancel_at is not None
                or attempt.dispatch_revoked_at is not None
            ):
                attempt.dispatch_revoked_at = (
                    attempt.dispatch_revoked_at
                    or await session.scalar(select(func.clock_timestamp()))
                )
                return False
            attempt.dispatch_started_at = (
                attempt.dispatch_started_at
                or await session.scalar(select(func.clock_timestamp()))
            )
        return True

    async def read_dispatch(
        self, submission_id: str, target: str, kind: str = "ray-jobs-v1"
    ) -> DispatchRecord | None:
        async with self._transaction() as session:
            row = (
                await session.execute(
                    select(JobRow, AttemptRow)
                    .join(AttemptRow, AttemptRow.job_id == JobRow.job_id)
                    .where(
                        AttemptRow.submission_id == submission_id,
                        AttemptRow.executor_kind == kind,
                        AttemptRow.executor_target == target,
                    )
                )
            ).one_or_none()
            return None if row is None else dispatch_record(*row)

    async def revoke_dispatch(
        self, submission_id: str, target: str, kind: str = "ray-jobs-v1"
    ) -> DispatchRecord | None:
        """Serialize revocation with begin_dispatch, always locking job first."""
        async with self._transaction() as session:
            row = (
                await session.execute(
                    select(JobRow, AttemptRow)
                    .join(AttemptRow, AttemptRow.job_id == JobRow.job_id)
                    .where(
                        AttemptRow.submission_id == submission_id,
                        AttemptRow.executor_kind == kind,
                        AttemptRow.executor_target == target,
                    )
                    .with_for_update(of=JobRow)
                )
            ).one_or_none()
            if row is None:
                return None
            job, attempt = row
            # This is an executor-internal operation, not an alternative cancel API.
            if job.requested_cancel_at is None:
                return None
            await session.refresh(attempt, with_for_update=True)
            attempt.dispatch_revoked_at = (
                attempt.dispatch_revoked_at
                or await session.scalar(select(func.clock_timestamp()))
            )
            return dispatch_record(job, attempt)

    async def observe(
        self,
        claim: Claim,
        status: ExecutionStatus,
        *,
        failure_kind: FailureKind = FailureKind.PERMANENT,
        result: ExecutionResult | None = None,
        checkpoint: CheckpointRef | None = None,
        retry_checkpoint: CheckpointRef | None = None,
        checkpoint_reconciled: bool = False,
    ) -> Claim | None:
        if not isinstance(status, ExecutionStatus):
            raise TypeError("Expected ExecutionStatus")
        if not isinstance(failure_kind, FailureKind):
            raise TypeError("Expected FailureKind")
        async with self._transaction() as session:
            job, attempt = await self._owned(session, claim)
            now = await session.scalar(select(func.clock_timestamp()))
            if status == ExecutionStatus.NOT_FOUND or (
                status == ExecutionStatus.REJECTED and attempt.accepted_at is not None
            ):
                status = ExecutionStatus.UNKNOWN
            attempt.last_observation = status
            previous = (job.status, job.waiting_reason, job.error, job.checkpoint_uri)
            current_checkpoint = attempt.published_checkpoint
            if checkpoint is not None and (
                current_checkpoint is None
                or checkpoint.epoch > current_checkpoint["epoch"]
            ):
                attempt.published_checkpoint = asdict(checkpoint)
                job.checkpoint_uri = checkpoint.uri
            job.waiting_reason = None
            if status == ExecutionStatus.UNKNOWN:
                job.waiting_reason = "executor_outcome_unknown"
            elif status == ExecutionStatus.PENDING:
                job.waiting_reason = "executor_pending"
            if status in (
                ExecutionStatus.PENDING,
                ExecutionStatus.RUNNING,
                ExecutionStatus.SUCCEEDED,
                ExecutionStatus.FAILED,
            ):
                attempt.accepted_at = attempt.accepted_at or now
            if status == ExecutionStatus.RUNNING:
                attempt.started_at = attempt.started_at or now
                if job.status == JobStatus.QUEUED:
                    job.status = transition(JobStatus(job.status), JobStatus.RUNNING)
            terminal = status in (
                ExecutionStatus.SUCCEEDED,
                ExecutionStatus.FAILED,
                ExecutionStatus.REJECTED,
                ExecutionStatus.CANCELLED,
            )
            if terminal:
                cancelling = job.requested_cancel_at is not None
                failed = status in (ExecutionStatus.FAILED, ExecutionStatus.REJECTED)
                if failed:
                    attempt.failure_kind = failure_kind
                    if retry_checkpoint is not None:
                        attempt.retry_checkpoint = asdict(retry_checkpoint)
                if status == ExecutionStatus.FAILED and checkpoint_reconciled:
                    job.checkpoint_uri = (
                        retry_checkpoint.uri if retry_checkpoint else None
                    )
                if status == ExecutionStatus.SUCCEEDED:
                    new_status = JobStatus.SUCCEEDED
                elif cancelling and status in (
                    ExecutionStatus.CANCELLED,
                    ExecutionStatus.REJECTED,
                ):
                    new_status = JobStatus.CANCELLED
                elif (
                    not cancelling
                    and failed
                    and failure_kind == FailureKind.TRANSIENT
                    and (
                        attempt.executor_kind != "ray-training-v1"
                        or retry_checkpoint is not None
                    )
                    and job.attempt <= job.spec_json["max_retries"]
                ):
                    new_status = JobStatus.RETRYING
                    job.retry_not_before = now + timedelta(
                        seconds=self._retry_policy.delay(job.attempt, self._jitter())
                    )
                    job.waiting_reason = "retry_backoff"
                else:
                    new_status = JobStatus.FAILED
                job.status = transition(JobStatus(job.status), new_status)
                job.error = (
                    None
                    if new_status in (JobStatus.SUCCEEDED, JobStatus.CANCELLED)
                    else f"executor_{status.value}"
                )
                attempt.finished_at = now
                if status == ExecutionStatus.SUCCEEDED and result is not None:
                    attempt.execution_result = asdict(result)
                    if isinstance(result, TrainingExecutionResult):
                        job.artifact_uri = result.artifact_uri
            if previous != (
                job.status,
                job.waiting_reason,
                job.error,
                job.checkpoint_uri,
            ):
                job.state_version += 1
                job.updated_at = now
            await session.flush()
            return None if terminal else to_claim(job, attempt)
