"""Public request, response and error contracts for /v1/jobs."""

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification

type Identifier = Annotated[
    str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
]


class ResourceRequest(BaseModel):
    """Validation ceilings, not a guarantee of available cluster capacity."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    cpus: float = Field(gt=0, le=64, allow_inf_nan=False)
    gpus: int = Field(default=0, ge=0, le=8)
    memory_gib: float = Field(gt=0, le=512, allow_inf_nan=False)


class JobSpec(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        json_schema_extra={
            "examples": [
                {
                    "model": "tiny-mlp",
                    "dataset": "synthetic-v1",
                    "epochs": 5,
                    "resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
                    "max_retries": 2,
                }
            ]
        },
    )

    model: Identifier
    dataset: Identifier
    epochs: int = Field(ge=1, le=10_000)
    resources: ResourceRequest
    max_retries: int = Field(default=0, ge=0, le=10)

    def to_domain(self) -> JobSpecification:
        return JobSpecification(
            model=self.model,
            dataset=self.dataset,
            epochs=self.epochs,
            resources=JobResources(
                cpus=self.resources.cpus,
                gpus=self.resources.gpus,
                memory_gib=self.resources.memory_gib,
            ),
            max_retries=self.max_retries,
        )


class JobAccepted(BaseModel):
    job_id: UUID
    status: Literal[JobStatus.QUEUED]


class JobView(BaseModel):
    job_id: UUID
    status: JobStatus
    model: str
    dataset: str
    epochs: int
    max_retries: int
    requested_resources: ResourceRequest
    attempt: int = Field(ge=0)
    checkpoint_uri: str | None
    artifact_uri: str | None
    result_model_id: UUID | None
    result_model_version: str | None
    waiting_reason: str | None
    error: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_job(cls, job: Job) -> Self:
        return cls(
            job_id=job.job_id,
            status=job.status,
            model=job.spec.model,
            dataset=job.spec.dataset,
            epochs=job.spec.epochs,
            max_retries=job.spec.max_retries,
            requested_resources=ResourceRequest(
                cpus=job.spec.resources.cpus,
                gpus=job.spec.resources.gpus,
                memory_gib=job.spec.resources.memory_gib,
            ),
            attempt=job.attempt,
            checkpoint_uri=job.checkpoint_uri,
            artifact_uri=job.artifact_uri,
            result_model_id=job.result_model_id,
            result_model_version=job.result_model_version,
            waiting_reason=job.waiting_reason,
            error=job.error,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class ValidationIssue(BaseModel):
    location: list[str | int]
    type: str
    message: str


class ApiError(BaseModel):
    code: Literal["validation_error", "job_not_found"]
    message: str
    details: list[ValidationIssue] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: ApiError
