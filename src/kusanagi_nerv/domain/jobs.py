"""Immutable job records, independent of transport and storage libraries."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from kusanagi_nerv.domain.job_lifecycle import JobStatus


@dataclass(frozen=True, slots=True)
class JobResources:
    cpus: float
    gpus: int
    memory_gib: float


@dataclass(frozen=True, slots=True)
class JobSpecification:
    """Submission values validated at the application boundary."""

    model: str
    dataset: str
    epochs: int
    resources: JobResources
    max_retries: int


@dataclass(frozen=True, slots=True)
class Job:
    job_id: UUID
    tenant_id: str
    spec: JobSpecification
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    attempt: int = 0
    checkpoint_uri: str | None = None
    artifact_uri: str | None = None
    result_model_id: UUID | None = None
    result_model_version: str | None = None
    waiting_reason: str | None = None
    error: str | None = None
