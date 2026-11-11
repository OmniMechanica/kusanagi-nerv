"""Request-level contract tests, including failures and repository substitution."""

import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from kusanagi_nerv.api.app import create_app, get_repository, get_tenant_id
from kusanagi_nerv.api.schemas import ErrorResponse, JobAccepted, JobView
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job
from kusanagi_nerv.repositories.jobs import InMemoryJobRepository, RepositoryUnavailable


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(create_app(InMemoryJobRepository())) as test_client:
        yield test_client


@pytest.fixture
def payload() -> dict[str, Any]:
    return {
        "model": "tiny-mlp",
        "dataset": "synthetic-v1",
        "epochs": 5,
        "resources": {"cpus": 2, "gpus": 0, "memory_gib": 2},
        "max_retries": 2,
    }


def test_create_then_get_job(client: TestClient, payload: dict[str, Any]) -> None:
    response = client.post("/v1/jobs", json=payload)
    assert response.status_code == 202
    accepted = JobAccepted.model_validate_json(response.content)
    assert accepted.status is JobStatus.QUEUED
    assert accepted.job_id.version == 4
    assert response.headers["Location"] == f"/v1/jobs/{accepted.job_id}"

    result = client.get(response.headers["Location"])
    assert result.status_code == 200
    job = JobView.model_validate_json(result.content)
    assert job.job_id == accepted.job_id
    assert job.status is JobStatus.QUEUED
    assert (job.model, job.dataset, job.epochs, job.max_retries) == (
        "tiny-mlp",
        "synthetic-v1",
        5,
        2,
    )
    assert job.requested_resources.model_dump() == payload["resources"]
    assert job.attempt == 0
    for key in (
        "checkpoint_uri",
        "artifact_uri",
        "result_model_id",
        "result_model_version",
        "waiting_reason",
        "error",
    ):
        assert result.json()[key] is None
    assert job.created_at == job.updated_at
    assert job.created_at.utcoffset() is not None
    assert "tenant_id" not in result.json()


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("epochs",), 0),
        (("epochs",), -1),
        (("epochs",), 10_001),
        (("epochs",), 1.5),
        (("epochs",), True),
        (("epochs",), "5"),
        (("epochs",), None),
        (("max_retries",), -1),
        (("max_retries",), 11),
        (("max_retries",), 1.5),
        (("max_retries",), False),
        (("max_retries",), "2"),
        (("resources", "cpus"), 0),
        (("resources", "cpus"), -1),
        (("resources", "cpus"), 64.1),
        (("resources", "cpus"), True),
        (("resources", "cpus"), "2"),
        (("resources", "cpus"), None),
        (("resources", "gpus"), -1),
        (("resources", "gpus"), 9),
        (("resources", "gpus"), 0.5),
        (("resources", "gpus"), True),
        (("resources", "gpus"), "1"),
        (("resources", "memory_gib"), 0),
        (("resources", "memory_gib"), -1),
        (("resources", "memory_gib"), 512.1),
        (("resources", "memory_gib"), True),
        (("resources", "memory_gib"), "2"),
        (("model",), ""),
        (("model",), " "),
        (("model",), "arbitrary/script.py"),
        (("model",), "x" * 129),
        (("model",), None),
        (("dataset",), ""),
        (("dataset",), "data\nset"),
        (("dataset",), "x" * 129),
        (("tenant_id",), "untrusted-tenant"),
        (("unknown",), 1),
        (("resources", "tenant_id"), "untrusted-tenant"),
    ],
)
def test_invalid_spec_returns_typed_422(
    client: TestClient, payload: dict[str, Any], path: tuple[str, ...], value: object
) -> None:
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 422
    error = ErrorResponse.model_validate_json(result.content).error
    assert error.code == "validation_error"
    assert ["body", *path] in [issue.location for issue in error.details]
    assert "Location" not in result.headers
    assert all(
        set(issue) == {"location", "type", "message"}
        for issue in result.json()["error"]["details"]
    )


@pytest.mark.parametrize("field", ["model", "dataset", "epochs", "resources"])
def test_required_fields(
    client: TestClient, payload: dict[str, Any], field: str
) -> None:
    del payload[field]
    result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 422
    assert result.json()["error"]["details"][0]["location"] == ["body", field]


@pytest.mark.parametrize("field", ["cpus", "memory_gib"])
def test_required_resources(
    client: TestClient, payload: dict[str, Any], field: str
) -> None:
    del payload["resources"][field]
    result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 422
    assert result.json()["error"]["details"][0]["location"] == [
        "body",
        "resources",
        field,
    ]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("field", ["cpus", "memory_gib"])
def test_non_finite_resources_return_json_errors(
    client: TestClient, payload: dict[str, Any], field: str, value: float
) -> None:
    payload["resources"][field] = value
    result = client.post(
        "/v1/jobs",
        content=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
    assert result.status_code == 422
    error = ErrorResponse.model_validate_json(result.content).error
    assert error.code == "validation_error"
    assert error.details[0].location == ["body", "resources", field]


def test_defaults_and_fractional_cpu_memory(
    client: TestClient, payload: dict[str, Any]
) -> None:
    del payload["max_retries"]
    payload["resources"] = {"cpus": 0.25, "memory_gib": 0.5}
    result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 202
    job = client.get(result.headers["Location"]).json()
    assert job["max_retries"] == 0
    assert job["requested_resources"] == {"cpus": 0.25, "gpus": 0, "memory_gib": 0.5}


def test_upper_bounds_are_inclusive_and_gpu_is_preserved(
    client: TestClient, payload: dict[str, Any]
) -> None:
    payload.update(epochs=10_000, max_retries=10)
    payload["resources"] = {"cpus": 64, "gpus": 8, "memory_gib": 512}
    result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 202
    job = client.get(result.headers["Location"]).json()
    assert job["requested_resources"] == payload["resources"]
    assert job["status"] == "queued"
    assert job["attempt"] == 0


@pytest.mark.parametrize("body", ["{invalid", "[]", "null", ""])
def test_invalid_bodies_use_the_error_envelope(client: TestClient, body: str) -> None:
    result = client.post(
        "/v1/jobs", content=body, headers={"Content-Type": "application/json"}
    )
    assert result.status_code == 422
    assert (
        ErrorResponse.model_validate_json(result.content).error.code
        == "validation_error"
    )


def test_unknown_job_is_404(client: TestClient) -> None:
    result = client.get(f"/v1/jobs/{uuid4()}")
    assert result.status_code == 404
    assert result.json() == {
        "error": {"code": "job_not_found", "message": "Job not found", "details": []}
    }


def test_malformed_job_id_is_422(client: TestClient) -> None:
    result = client.get("/v1/jobs/not-a-uuid")
    assert result.status_code == 422
    error = ErrorResponse.model_validate_json(result.content).error
    assert error.code == "validation_error"
    assert error.details[0].location == ["path", "job_id"]


def test_jobs_are_scoped_to_the_injected_tenant(payload: dict[str, Any]) -> None:
    app = create_app(InMemoryJobRepository())
    app.dependency_overrides[get_tenant_id] = lambda: "tenant-a"
    with TestClient(app) as client:
        created = client.post("/v1/jobs", json=payload)
        assert created.status_code == 202
        location = created.headers["Location"]
        app.dependency_overrides[get_tenant_id] = lambda: "tenant-b"
        other_tenant = client.get(location)
        unknown = client.get(f"/v1/jobs/{uuid4()}")
        assert other_tenant.status_code == unknown.status_code == 404
        assert other_tenant.json() == unknown.json()
        app.dependency_overrides[get_tenant_id] = lambda: "tenant-a"
        assert client.get(location).status_code == 200


def test_new_app_does_not_share_in_memory_jobs(payload: dict[str, Any]) -> None:
    with TestClient(create_app(InMemoryJobRepository())) as first:
        location = first.post("/v1/jobs", json=payload).headers["Location"]
    with TestClient(create_app(InMemoryJobRepository())) as second:
        assert second.get(location).status_code == 404


def test_idempotency_is_explicitly_not_implemented(
    client: TestClient, payload: dict[str, Any]
) -> None:
    headers = {"Idempotency-Key": "same-request"}
    first = client.post("/v1/jobs", json=payload, headers=headers)
    second = client.post("/v1/jobs", json=payload, headers=headers)
    assert first.status_code == second.status_code == 202
    assert first.json()["job_id"] != second.json()["job_id"]


class RecordingRepository:
    """A substitute without executor methods: only admission and tenant lookup."""

    def __init__(self) -> None:
        self.jobs: list[Job] = []

    async def add(self, job: Job) -> None:
        self.jobs.append(job)

    async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
        return next(
            (
                job
                for job in self.jobs
                if job.tenant_id == tenant_id and job.job_id == job_id
            ),
            None,
        )

    async def update_status(
        self,
        *,
        tenant_id: str,
        job_id: UUID,
        expected_status: JobStatus,
        expected_version: int,
        new_status: JobStatus,
    ) -> Job:
        raise AssertionError("The create/get API must not update execution state")


def test_repository_can_be_replaced_and_admission_finishes_before_202(
    payload: dict[str, Any],
) -> None:
    repository = RecordingRepository()
    app = create_app(InMemoryJobRepository())
    app.dependency_overrides[get_repository] = lambda: repository
    with TestClient(app) as client:
        result = client.post("/v1/jobs", json=payload)
        assert result.status_code == 202
        assert len(repository.jobs) == 1
        job = repository.jobs[0]
        assert isinstance(job, Job)
        assert job.status is JobStatus.QUEUED
        assert job.attempt == 0
        assert isinstance(job.created_at, datetime)
        assert client.get(result.headers["Location"]).status_code == 200


def test_invalid_request_never_reaches_repository(payload: dict[str, Any]) -> None:
    repository = RecordingRepository()
    payload["tenant_id"] = "spoofed"
    with TestClient(create_app(repository)) as client:
        assert client.post("/v1/jobs", json=payload).status_code == 422
    assert repository.jobs == []


def test_failed_admission_never_returns_202(payload: dict[str, Any]) -> None:
    class UnavailableRepository(RecordingRepository):
        async def add(self, job: Job) -> None:
            raise RuntimeError("Storage unavailable")

    with TestClient(
        create_app(UnavailableRepository()), raise_server_exceptions=False
    ) as client:
        result = client.post("/v1/jobs", json=payload)
    assert result.status_code == 500
    assert "Location" not in result.headers


def test_openapi_documents_the_implemented_contract(client: TestClient) -> None:
    result = client.get("/openapi.json")
    assert result.status_code == 200
    schema = result.json()
    post = schema["paths"]["/v1/jobs"]["post"]
    get = schema["paths"]["/v1/jobs/{job_id}"]["get"]
    assert set(post["responses"]) == {"202", "422", "503"}
    assert set(get["responses"]) == {"200", "404", "422", "503"}
    assert "Location" in post["responses"]["202"]["headers"]
    for operation, status, model in (
        (post, "202", "JobAccepted"),
        (post, "422", "ErrorResponse"),
        (get, "200", "JobView"),
        (get, "404", "ErrorResponse"),
        (get, "422", "ErrorResponse"),
        (post, "503", "ErrorResponse"),
        (get, "503", "ErrorResponse"),
    ):
        assert operation["responses"][status]["content"]["application/json"][
            "schema"
        ] == {"$ref": f"#/components/schemas/{model}"}
    schemas = schema["components"]["schemas"]
    assert schemas["JobSpec"]["additionalProperties"] is False
    assert schemas["ResourceRequest"]["additionalProperties"] is False
    assert schemas["JobSpec"]["examples"][0]["model"] == "tiny-mlp"
    assert schemas["ResourceRequest"]["properties"]["cpus"]["maximum"] == 64
    assert "Idempotency-Key" in schema["info"]["description"]


@pytest.mark.parametrize("method", ["post", "get"])
def test_storage_failures_use_a_sanitized_503(
    payload: dict[str, Any], method: str
) -> None:
    class BrokenRepository(RecordingRepository):
        async def add(self, job: Job) -> None:
            raise RepositoryUnavailable("sensitive driver details")

        async def get(self, *, tenant_id: str, job_id: UUID) -> Job | None:
            raise RepositoryUnavailable("sensitive driver details")

    with TestClient(create_app(BrokenRepository())) as client:
        result = (
            client.post("/v1/jobs", json=payload)
            if method == "post"
            else client.get(f"/v1/jobs/{uuid4()}")
        )
    assert result.status_code == 503
    assert result.json() == {
        "error": {
            "code": "storage_unavailable",
            "message": "Job storage is unavailable",
            "details": [],
        }
    }
    assert "Location" not in result.headers


def test_default_app_requires_explicit_database_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KUSANAGI_DATABASE_URL", raising=False)
    with pytest.raises(ValueError, match="Set KUSANAGI_DATABASE_URL"):
        create_app()
