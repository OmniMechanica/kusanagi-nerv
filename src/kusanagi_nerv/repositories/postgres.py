"""Transactional PostgreSQL ledger with explicit optimistic state updates."""

import hashlib
import json
from dataclasses import asdict
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update

from kusanagi_nerv.db.models import JobRow
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.repositories.jobs import JobUpdateConflict
from kusanagi_nerv.repositories.transactions import TransactionalRepository


def canonical_spec(spec: JobSpecification) -> tuple[dict[str, Any], str]:
    payload = asdict(spec)
    # Normalize the numeric resource representation before future idempotency checks.
    payload["resources"]["cpus"] = float(spec.resources.cpus)
    payload["resources"]["memory_gib"] = float(spec.resources.memory_gib)
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return payload, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


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
    )


class PostgresJobRepository(TransactionalRepository):
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
