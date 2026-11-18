"""Admit and inspect durable jobs without running training in HTTP handlers."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Literal, cast
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.api.schemas import (
    ApiError,
    ErrorResponse,
    JobAccepted,
    JobSpec,
    JobView,
    ModelView,
    PredictionView,
    ValidationIssue,
)
from kusanagi_nerv.db.connection import create_database_engine
from kusanagi_nerv.db.connection import database_url as resolve_database_url
from kusanagi_nerv.domain.execution import ExecutorUnavailable
from kusanagi_nerv.domain.job_lifecycle import JobStatus, transition
from kusanagi_nerv.domain.jobs import Job
from kusanagi_nerv.domain.models import DeployState
from kusanagi_nerv.repositories.jobs import (
    IdempotencyConflict,
    JobRepository,
    RepositoryUnavailable,
)
from kusanagi_nerv.repositories.models import ModelRepository
from kusanagi_nerv.repositories.postgres import PostgresJobRepository
from kusanagi_nerv.serving.artifacts import (
    ArtifactUnavailable,
    InvalidArtifact,
    load_artifact,
)
from kusanagi_nerv.serving.prediction import PredictRequest, ServePredictor


async def get_repository(request: Request) -> JobRepository:
    return cast(JobRepository, request.app.state.job_repository)


async def get_model_repository(request: Request) -> ModelRepository:
    models = getattr(request.app.state, "model_repository", None)
    if models is None:
        raise RepositoryUnavailable("Model storage is unavailable")
    return models


async def get_predictor(request: Request) -> ServePredictor:
    return request.app.state.predictor


def model_error(
    status: int,
    code: Literal[
        "model_not_found",
        "invalid_artifact",
        "artifact_unavailable",
        "model_not_ready",
        "serving_unavailable",
    ],
    message: str,
) -> JSONResponse:
    body = ErrorResponse(error=ApiError(code=code, message=message))
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"))


async def get_tenant_id() -> str:
    """Fixed local test identity; replace with trusted authentication in MLP-017."""
    return "local-test-tenant"


def create_app(
    repository: JobRepository | None = None, *, database_url: str | None = None
) -> FastAPI:
    url = resolve_database_url(database_url) if repository is None else None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        predictor = app.state.predictor = ServePredictor()
        try:
            if repository is not None:
                app.state.job_repository = repository
                yield
            else:
                assert url is not None
                engine = create_database_engine(url)
                sessions = async_sessionmaker(engine, expire_on_commit=False)
                app.state.job_repository = PostgresJobRepository(sessions)
                app.state.model_repository = ModelRepository(sessions)
                try:
                    yield
                finally:
                    await engine.dispose()
        finally:
            await predictor.aclose()

    app = FastAPI(
        title="KUSANAGI NERV",
        version="0.1.0",
        description=(
            "Learning API with a fixed test tenant. Jobs are committed to PostgreSQL "
            "before acceptance and survive API restarts. A separate controller can "
            "run simulated jobs, dummy CPU workloads or CPU training with "
            "checkpoint recovery. "
            "Idempotency-Key deduplicates admission within a tenant. Cancellation "
            "records intent before confirming worker termination."
        ),
        lifespan=lifespan,
    )

    @app.exception_handler(RepositoryUnavailable)
    async def storage_error(
        request: Request, exc: RepositoryUnavailable
    ) -> JSONResponse:
        body = ErrorResponse(
            error=ApiError(
                code="storage_unavailable", message="Job storage is unavailable"
            )
        )
        return JSONResponse(status_code=503, content=body.model_dump(mode="json"))

    @app.exception_handler(IdempotencyConflict)
    async def idempotency_error(
        request: Request, exc: IdempotencyConflict
    ) -> JSONResponse:
        body = ErrorResponse(
            error=ApiError(
                code="idempotency_conflict",
                message="Idempotency key has a different payload",
            )
        )
        return JSONResponse(status_code=409, content=body.model_dump(mode="json"))

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
        summary="Commit a job to the durable queue",
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
            409: {
                "model": ErrorResponse,
                "description": "Key already used for a different payload",
            },
            422: {
                "model": ErrorResponse,
                "description": "Invalid job specification or key",
            },
            503: {
                "model": ErrorResponse,
                "description": "Storage operation could not be confirmed",
            },
        },
    )
    async def create_job(
        spec: JobSpec,
        request: Request,
        response: Response,
        jobs: Annotated[JobRepository, Depends(get_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
        idempotency_key: Annotated[
            str | None,
            Header(
                alias="Idempotency-Key",
                min_length=1,
                max_length=255,
                pattern=r"^[!-~]+$",
            ),
        ] = None,
    ) -> JobAccepted:
        if len(request.headers.getlist("Idempotency-Key")) > 1:
            raise RequestValidationError(
                [
                    {
                        "loc": ("header", "Idempotency-Key"),
                        "type": "value_error",
                        "msg": "Exactly one Idempotency-Key header is allowed",
                    }
                ]
            )
        now = datetime.now(UTC)
        job = Job(
            job_id=uuid4(),
            tenant_id=tenant_id,
            spec=spec.to_domain(),
            status=transition(JobStatus.CREATED, JobStatus.QUEUED),
            created_at=now,
            updated_at=now,
        )
        job = await jobs.admit(job, idempotency_key=idempotency_key)
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
            503: {"model": ErrorResponse, "description": "Storage unavailable"},
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

    @app.post(
        "/v1/jobs/{job_id}/cancel",
        response_model=JobView,
        summary="Request cancellation; terminal outcomes require execution evidence",
        responses={
            202: {"model": JobView, "description": "Cancellation intent recorded"},
            404: {"model": ErrorResponse, "description": "Unknown or other-tenant job"},
            422: {"model": ErrorResponse, "description": "Job ID is not a UUID"},
            503: {"model": ErrorResponse, "description": "Storage unavailable"},
        },
    )
    async def cancel_job(
        job_id: UUID,
        response: Response,
        jobs: Annotated[JobRepository, Depends(get_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
    ) -> JobView | JSONResponse:
        job = await jobs.request_cancel(tenant_id=tenant_id, job_id=job_id)
        if job is None:
            body = ErrorResponse(
                error=ApiError(code="job_not_found", message="Job not found")
            )
            return JSONResponse(status_code=404, content=body.model_dump(mode="json"))
        response.status_code = 202 if job.status == JobStatus.CANCELLING else 200
        return JobView.from_job(job)

    @app.get(
        "/v1/models/{model_id}",
        response_model=ModelView,
        responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
    )
    async def get_model(
        model_id: UUID,
        models: Annotated[ModelRepository, Depends(get_model_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
    ) -> ModelView | JSONResponse:
        model = await models.get(tenant_id=tenant_id, model_id=model_id)
        if model is None:
            return model_error(404, "model_not_found", "Model not found")
        return ModelView.from_model(model)

    @app.post(
        "/v1/models/{model_id}/deploy",
        response_model=ModelView,
        responses={
            202: {"model": ModelView},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def deploy_model(
        model_id: UUID,
        response: Response,
        models: Annotated[ModelRepository, Depends(get_model_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
    ) -> ModelView | JSONResponse:
        model = await models.get(tenant_id=tenant_id, model_id=model_id)
        if model is None:
            return model_error(404, "model_not_found", "Model not found")
        if model.deploy_state in (DeployState.NOT_DEPLOYED, DeployState.FAILED):
            try:
                await asyncio.to_thread(
                    load_artifact, model.artifact_uri, model.artifact_sha256
                )
            except InvalidArtifact:
                return model_error(
                    409, "invalid_artifact", "Model artifact is invalid or missing"
                )
            except ArtifactUnavailable:
                return model_error(
                    503, "artifact_unavailable", "Model artifact could not be verified"
                )
            model = await models.request_deploy(tenant_id=tenant_id, model_id=model_id)
            if model is None:
                return model_error(404, "model_not_found", "Model not found")
        response.status_code = 200 if model.deploy_state is DeployState.READY else 202
        response.headers["Location"] = f"/v1/models/{model_id}"
        return ModelView.from_model(model)

    @app.post(
        "/v1/models/{model_id}/predict",
        response_model=PredictionView,
        responses={status: {"model": ErrorResponse} for status in (404, 409, 422, 503)},
    )
    async def predict_model(
        model_id: UUID,
        batch: PredictRequest,
        models: Annotated[ModelRepository, Depends(get_model_repository)],
        tenant_id: Annotated[str, Depends(get_tenant_id)],
        predictor: Annotated[ServePredictor, Depends(get_predictor)],
    ) -> PredictionView | JSONResponse:
        model = await models.get(tenant_id=tenant_id, model_id=model_id)
        if model is None:
            return model_error(404, "model_not_found", "Model not found")
        if model.deploy_state is not DeployState.READY:
            return model_error(409, "model_not_ready", "Model deployment is not ready")
        try:
            predictions = await predictor.predict(model, batch)
        except ExecutorUnavailable:
            return model_error(
                503, "serving_unavailable", "Predictions are unavailable"
            )
        return PredictionView(
            model_id=model_id, version=model.version, predictions=predictions
        )

    return app
