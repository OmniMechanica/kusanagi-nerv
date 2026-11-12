"""Transactional PostgreSQL ledger with explicit optimistic state updates."""

from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from kusanagi_nerv.db.models import AttemptRow, JobRow
from kusanagi_nerv.domain.idempotency import canonical_spec, validate_key
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.repositories.jobs import (
    IdempotencyConflict,
    JobUpdateConflict,
    RepositoryUnavailable,
)
from kusanagi_nerv.repositories.transactions import TransactionalRepository


def to_job(row: JobRow) -> Job:
    spec = row.spec_json
    return Job(
        job_id=row.job_id,
        tenant_id=row.tenant_id,
        spec=JobSpecification(
            model=spec["model"],
            dataset=spec["dataset"],
            epochs=spec["epochs"],
            resources=JobResources(**spec["resources"]),
            max_retries=spec["max_retries"],
        ),
        status=JobStatus(row.status),
        state_version=row.state_version,
        created_at=row.created_at,
        updated_at=row.updated_at,
        attempt=row.attempt,
        checkpoint_uri=row.checkpoint_uri,
        artifact_uri=row.artifact_uri,
        result_model_id=row.result_model_id,
        result_model_version=row.result_model_version,
        waiting_reason=row.waiting_reason,
        error=row.error,
        requested_cancel_at=row.requested_cancel_at,
        retry_not_before=row.retry_not_before,
    )


class PostgresJobRepository(TransactionalRepository):
    async def admit(self, job: Job, *, idempotency_key: str | None = None) -> Job:
        validate_key(idempotency_key)
        if idempotency_key is None:
            await self.add(job)
            return job
        spec, digest = canonical_spec(job.spec)
        async with self._transaction() as session:
            row = await session.scalar(
                insert(JobRow)
                .values(
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    spec_json=spec,
                    payload_hash=digest,
                    idempotency_key=idempotency_key,
                    status=JobStatus.QUEUED,
                    created_at=job.created_at,
                    updated_at=job.updated_at,
                )
                .on_conflict_do_nothing(constraint="uq_jobs_tenant_key")
                .returning(JobRow)
            )
            if row is None:
                # A new statement snapshot sees the winner after INSERT waited
                # on its unique key. Do not overwrite its status or timestamps.
                row = await session.scalar(
                    select(JobRow).where(
                        JobRow.tenant_id == job.tenant_id,
                        JobRow.idempotency_key == idempotency_key,
                    )
                )
                if row is None:
                    raise RepositoryUnavailable("Admission outcome is unavailable")
                if row.payload_hash != digest:
                    raise IdempotencyConflict("Idempotency key has a different payload")
            return to_job(row)

    async def request_cancel(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        async with self._transaction() as session:
            job = await session.scalar(
                select(JobRow)
                .where(JobRow.job_id == job_id, JobRow.tenant_id == tenant_id)
                .with_for_update()
            )
            if job is None:
                return None
            if job.status in (
                JobStatus.SUCCEEDED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
                JobStatus.CANCELLING,
            ):
                return to_job(job)
            # Same job-first lock order as claim/finalization; cancellation cannot
            # slip between eligibility validation and committed dispatch intent.
            active = await session.scalar(
                select(AttemptRow.attempt_id).where(
                    AttemptRow.job_id == job_id, AttemptRow.finished_at.is_(None)
                )
            )
            safe = active is None and (
                job.status == JobStatus.RETRYING
                or (
                    job.status in (JobStatus.CREATED, JobStatus.QUEUED)
                    and job.attempt == 0
                )
            )
            job.status = transition(
                JobStatus(job.status),
                JobStatus.CANCELLED if safe else JobStatus.CANCELLING,
            )
            now = await session.scalar(select(func.clock_timestamp()))
            job.requested_cancel_at = now
            job.retry_not_before = None
            job.waiting_reason = None
            job.state_version += 1
            job.updated_at = now
            await session.flush()
            return to_job(job)

    async def add(self, job: Job) -> None:
        spec, digest = canonical_spec(job.spec)
        async with self._transaction() as session:
            session.add(
                JobRow(
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    spec_json=spec,
                    payload_hash=digest,
                    idempotency_key=None,
                    status=job.status.value,
                    state_version=job.state_version,
                    created_at=job.created_at,
                    updated_at=job.updated_at,
                    attempt=job.attempt,
                    checkpoint_uri=job.checkpoint_uri,
                    artifact_uri=job.artifact_uri,
                    result_model_id=job.result_model_id,
                    result_model_version=job.result_model_version,
                    waiting_reason=job.waiting_reason,
                    error=job.error,
                    requested_cancel_at=job.requested_cancel_at,
                    retry_not_before=job.retry_not_before,
                )
            )
        # The transaction commits before returning to the API.

    async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        async with self._transaction() as session:
            row = await session.scalar(
                select(JobRow).where(
                    JobRow.job_id == job_id,
                    JobRow.tenant_id == tenant_id,
                )
            )
            return to_job(row) if row is not None else None

    async def update_status(
        self,
        *,
        tenant_id: str,
        job_id: UUID,
        expected_status: JobStatus,
        expected_version: int,
        new_status: JobStatus,
    ) -> Job:
        status = transition(expected_status, new_status)
        async with self._transaction() as session:
            row = await session.scalar(
                update(JobRow)
                .where(
                    JobRow.job_id == job_id,
                    JobRow.tenant_id == tenant_id,
                    JobRow.status == expected_status.value,
                    JobRow.state_version == expected_version,
                    # Once dispatch intent exists, only the fenced attempt store
                    # may change execution state (including cancellation later).
                    JobRow.attempt == 0,
                )
                .values(
                    status=status.value,
                    state_version=JobRow.state_version + 1,
                    updated_at=func.now(),
                )
                .returning(JobRow)
            )
            if row is None:
                raise JobUpdateConflict("Job state changed or job is unavailable")
            return to_job(row)
