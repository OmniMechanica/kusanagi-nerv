"""MLP-002: admit and inspect jobs without running training in HTTP handlers."""

from datetime import UTC, datetime
from typing import Annotated, cast
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from kusanagi_nerv.api.schemas import (
    ApiError,
    ErrorResponse,
    JobAccepted,
    JobSpec,
    JobView,
    ValidationIssue,
)
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job
from kusanagi_nerv.repositories.jobs import InMemoryJobRepository, JobRepository


async def get_repository(request: Request) -> JobRepository:
    return cast(JobRepository, request.app.state.job_repository)


async def get_tenant_id() -> str:
    """Fixed local test identity; replace with trusted authentication in MLP-017."""
    return "local-test-tenant"


def create_app(repository: JobRepository | None = None) -> FastAPI:
    app = FastAPI(
        title="KUSANAGI NERV",
        version="0.1.0",
        description=(
            "Local learning API with a fixed test tenant. Jobs are stored in memory "
            "and remain queued; no executor is connected. Each POST creates a new "
            "job, including requests with Idempotency-Key. Storage is lost on "
            "restart and is not shared between worker processes."
        ),
    )
    app.state.job_repository = (
        repository if repository is not None else InMemoryJobRepository()
    )

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        body = ErrorResponse(
            error=ApiError(
                code="validation_error",
                message="Request validation failed",
                details=[
                    ValidationIssue(
                        location=list(error["loc"]),
                        type=error["type"],
                        message=error["msg"],
                    )
                    for error in exc.errors()
                ],
            )
        )
        # Do not reflect raw input/ctx: it can contain secrets or non-JSON values.
        return JSONResponse(status_code=422, content=body.model_dump(mode="json"))

    @app.post(
        "/v1/jobs",
        status_code=202,
        response_model=JobAccepted,
        summary="Accept a job into the local in-memory queue",
        responses={
            202: {
                "headers": {
                    "Location": {
                        "description": "Relative URL of the job status endpoint",
                        "schema": {"type": "string"},
                    }
                },
                "content": {
                    "application/json": {
                        "example": {
                            "job_id": "00000000-0000-4000-8000-000000000001",
                            "status": "queued",
                        }
                    }
                },
            },
            422: {"model": ErrorResponse, "description": "Invalid job specification"},
        },
    )
    async def create_job(
        spec: JobSpec,
        request: Request,
        response: Response,
        jobs: Annotated[JobRepository, Depends(get_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
    ) -> JobAccepted:
        now = datetime.now(UTC)
        job = Job(
            job_id=uuid4(),
            tenant_id=tenant_id,
            spec=spec.to_domain(),
            status=transition(JobStatus.CREATED, JobStatus.QUEUED),
            created_at=now,
            updated_at=now,
        )
        await jobs.add(job)
        response.headers["Location"] = request.url_for(
            "get_job", job_id=str(job.job_id)
        ).path
        return JobAccepted(job_id=job.job_id, status=JobStatus.QUEUED)

    @app.get(
        "/v1/jobs/{job_id}",
        response_model=JobView,
        summary="Read the job owned by the current test tenant",
        responses={
            404: {"model": ErrorResponse, "description": "Unknown or other-tenant job"},
            422: {"model": ErrorResponse, "description": "Job ID is not a UUID"},
        },
    )
    async def get_job(
        job_id: UUID,
        jobs: Annotated[JobRepository, Depends(get_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
    ) -> JobView | JSONResponse:
        job = await jobs.get(tenant_id=tenant_id, job_id=job_id)
        if job is None:
            body = ErrorResponse(
                error=ApiError(code="job_not_found", message="Job not found")
            )
            return JSONResponse(status_code=404, content=body.model_dump(mode="json"))
        return JobView.from_job(job)

    return app
