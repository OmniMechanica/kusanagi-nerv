"""Transport contract tests; these do not constitute real Ray evidence."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import httpx2
import pytest

from kusanagi_nerv.domain.execution import Claim, ExecutionStatus, ExecutorUnavailable
from kusanagi_nerv.domain.idempotency import canonical_spec
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.executors.ray_jobs import RayJobsExecutor, RayJobsSettings, metadata
from kusanagi_nerv.repositories.attempts import DispatchRecord
from kusanagi_nerv.workloads.dummy import PREFIX


@pytest.fixture
def settings() -> RayJobsSettings:
    return RayJobsSettings("http://127.0.0.1:8265", "session_test", "a" * 64)


@pytest.fixture
def record(settings: RayJobsSettings) -> DispatchRecord:
    now = datetime.now(UTC)
    spec = JobSpecification(
        "tiny-mlp", "synthetic-v1", 3, JobResources(0.5, 0, 0.25), 2
    )
    job = Job(uuid4(), "test", spec, JobStatus.QUEUED, now, now, attempt=1)
    attempt_id = uuid4()
    claim = Claim(
        job, attempt_id, f"kusanagi-{attempt_id}", "owner", 1, False, settings.target
    )
    return DispatchRecord(claim, canonical_spec(spec)[1], False, False)


class Store:
    def __init__(self, record: DispatchRecord):
        self.record = record
        self.dispatch_calls = 0

    async def read_dispatch(self, *_):
        return self.record

    async def begin_dispatch(self, *_):
        self.dispatch_calls += 1
        if self.record.revoked:
            return False
        self.record = replace(self.record, started=True)
        return True

    async def revoke_dispatch(self, *_):
        self.record = replace(self.record, revoked=True)
        return self.record


def details(executor: RayJobsExecutor, record: DispatchRecord, status: str) -> dict:
    return {
        "submission_id": record.claim.submission_id,
        "entrypoint": executor.entrypoint,
        "metadata": metadata(record),
        "status": status,
    }


def logs(record: DispatchRecord, event: str) -> str:
    return PREFIX + json.dumps(
        {
            "event": event,
            "submission_id": record.claim.submission_id,
            "worker_pid": 123,
            "completed_steps": 3,
            "checksum": "b" * 64,
        }
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("address", "http://example.com:8265"),
        ("address", "http://user:password@127.0.0.1:8265"),
        ("address", "http://127.0.0.1:8265/path"),
        ("address", "http://127.0.0.1:8265?token=value"),
        ("cluster_id", "auto"),
        ("token", ""),
        ("timeout_seconds", 0),
        ("timeout_seconds", float("nan")),
    ],
)
def test_invalid_settings(settings: RayJobsSettings, field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(settings, **{field: value})


def test_token_is_excluded_from_settings_repr(settings: RayJobsSettings) -> None:
    assert settings.token not in repr(settings)


@pytest.mark.parametrize(
    ("status", "event", "expected"),
    [
        ("PENDING", "", ExecutionStatus.PENDING),
        ("RUNNING", "", ExecutionStatus.PENDING),
        ("RUNNING", "started", ExecutionStatus.RUNNING),
        ("SUCCEEDED", "completed", ExecutionStatus.SUCCEEDED),
        ("SUCCEEDED", "", ExecutionStatus.UNKNOWN),
        ("FAILED", "", ExecutionStatus.FAILED),
        ("STOPPED", "", ExecutionStatus.CANCELLED),
        ("unexpected", "", ExecutionStatus.UNKNOWN),
        ({"malformed": True}, "", ExecutionStatus.UNKNOWN),
    ],
)
async def test_status_requires_workload_evidence(
    settings, record, status, event, expected
):
    def respond(request):
        if request.url.path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        if request.url.path.endswith("/logs"):
            return httpx2.Response(
                200, json={"logs": logs(record, event) if event else ""}
            )
        return httpx2.Response(200, json=details(executor, record, status))

    executor = RayJobsExecutor(
        Store(record), settings, transport=httpx2.MockTransport(respond)
    )
    try:
        observation = await executor.lookup(record.claim.submission_id)
        assert observation.status == expected
        if expected == ExecutionStatus.SUCCEEDED:
            assert observation.result.worker_pid == 123
            assert observation.result.completed_steps == 3
    finally:
        await executor.aclose()


@pytest.mark.parametrize("collision", ["metadata", "entrypoint", "submission_id"])
async def test_foreign_identity_is_not_adopted_or_stopped(settings, record, collision):
    methods = []

    def respond(request):
        methods.append(request.method)
        if request.url.path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        data = details(executor, record, "RUNNING")
        data[collision] = {} if collision == "metadata" else "foreign"
        return httpx2.Response(200, json=data)

    executor = RayJobsExecutor(
        Store(replace(record, started=True)),
        settings,
        transport=httpx2.MockTransport(respond),
    )
    try:
        assert (
            await executor.cancel(record.claim.submission_id)
        ).status == ExecutionStatus.UNKNOWN
        assert "POST" not in methods
    finally:
        await executor.aclose()


async def test_changed_cluster_prevents_dispatch(settings, record):
    store = Store(record)
    executor = RayJobsExecutor(
        store,
        settings,
        transport=httpx2.MockTransport(
            lambda request: httpx2.Response(200, json={"session_name": "session_other"})
        ),
    )
    try:
        with pytest.raises(ExecutorUnavailable):
            await executor.submit(record.claim)
        assert store.dispatch_calls == 0
    finally:
        await executor.aclose()


@pytest.mark.parametrize("started", [False, True])
async def test_cancel_missing_job_requires_proof_no_dispatch_started(
    settings, record, started
):
    methods = []

    def respond(request):
        methods.append(request.method)
        return (
            httpx2.Response(200, json={"session_name": settings.cluster_id})
            if request.url.path == "/api/version"
            else httpx2.Response(404)
        )

    executor = RayJobsExecutor(
        Store(replace(record, started=started)),
        settings,
        transport=httpx2.MockTransport(respond),
    )
    try:
        observation = await executor.cancel(record.claim.submission_id)
        assert observation.status == (
            ExecutionStatus.UNKNOWN if started else ExecutionStatus.CANCELLED
        )
        assert "POST" not in methods
        if not started:
            assert methods == []
    finally:
        await executor.aclose()


async def test_stop_ack_is_not_terminal_evidence(settings, record):
    methods = []

    def respond(request):
        methods.append(request.method)
        if request.url.path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        if request.url.path.endswith("/stop"):
            return httpx2.Response(200, json={"stopped": True})
        return httpx2.Response(200, json=details(executor, record, "PENDING"))

    executor = RayJobsExecutor(
        Store(replace(record, started=True)),
        settings,
        transport=httpx2.MockTransport(respond),
    )
    try:
        assert (
            await executor.cancel(record.claim.submission_id)
        ).status == ExecutionStatus.PENDING
        assert methods.count("POST") == 1
    finally:
        await executor.aclose()


async def test_duplicate_submit_response_is_reconciled_by_same_id(settings, record):
    requests = []

    def respond(request):
        requests.append(request)
        if request.url.path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        if request.method == "POST":
            return httpx2.Response(400, text="Duplicate identity")
        return httpx2.Response(200, json=details(executor, record, "PENDING"))

    executor = RayJobsExecutor(
        Store(record), settings, transport=httpx2.MockTransport(respond)
    )
    try:
        assert (await executor.submit(record.claim)).status == ExecutionStatus.PENDING
        submitted = json.loads(next(r.content for r in requests if r.method == "POST"))
        assert submitted["submission_id"] == record.claim.submission_id
        assert submitted["entrypoint_num_cpus"] == 0.5
        assert submitted["entrypoint_memory"] == 256 * 1024**2
        assert submitted["entrypoint_num_gpus"] == 0
        assert "KUSANAGI_DATABASE_URL" not in submitted["runtime_env"]["env_vars"]
    finally:
        await executor.aclose()


@pytest.mark.parametrize("failure", ["network", "malformed", "unauthorized"])
async def test_uncertainty_never_becomes_a_retryable_execution_failure(
    settings, record, failure
):
    def respond(request):
        if failure == "network":
            raise httpx2.ReadTimeout("Lost response", request=request)
        return httpx2.Response(
            401 if failure == "unauthorized" else 200, text="not JSON"
        )

    executor = RayJobsExecutor(
        Store(record), settings, transport=httpx2.MockTransport(respond)
    )
    try:
        with pytest.raises(ExecutorUnavailable):
            await executor.lookup(record.claim.submission_id)
    finally:
        await executor.aclose()
