"""Async Ray Jobs REST adapter with PostgreSQL dispatch fences and target pinning."""

import json
import math
import re
import shlex
import sys
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx2

from kusanagi_nerv.domain.execution import (
    Claim,
    ExecutionObservation,
    ExecutionResult,
    ExecutionStatus,
    ExecutorUnavailable,
)
from kusanagi_nerv.repositories.attempts import AttemptRepository, DispatchRecord
from kusanagi_nerv.workloads.dummy import PREFIX

UNKNOWN = ExecutionObservation(ExecutionStatus.UNKNOWN)


@dataclass(frozen=True)
class RayJobsSettings:
    address: str
    cluster_id: str
    token: str = field(repr=False)
    timeout_seconds: float = 3

    def __post_init__(self) -> None:
        url = urlsplit(self.address)
        if (
            url.scheme != "http"
            or url.hostname not in {"127.0.0.1", "localhost", "::1"}
            or url.port is None
            or url.username
            or url.password
            or url.path
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "Ray address must be a loopback HTTP origin with an explicit port"
            )
        if not re.fullmatch(r"session_[A-Za-z0-9_.-]{1,160}", self.cluster_id):
            raise ValueError("Expected the Ray cluster's session_name")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", self.token):
            raise ValueError("Expected a 256-bit Ray token")
        if (
            not math.isfinite(self.timeout_seconds)
            or not 0 < self.timeout_seconds <= 30
        ):
            raise ValueError("Ray HTTP timeout must be in (0, 30]")

    @property
    def target(self) -> str:
        return f"{self.address}#{self.cluster_id}"


def workload_steps(record: DispatchRecord) -> int:
    return min(record.claim.job.spec.epochs, 8)


def metadata(record: DispatchRecord) -> dict[str, str]:
    return {
        "kusanagi_job_id": str(record.claim.job.job_id),
        "kusanagi_attempt_id": str(record.claim.attempt_id),
        "kusanagi_payload_hash": record.payload_hash,
        "kusanagi_workload": "dummy-v1",
    }


def workload_events(logs: str, submission_id: str) -> list[dict]:
    events = []
    for line in logs.splitlines():
        if not line.startswith(PREFIX):
            continue
        try:
            event = json.loads(line[len(PREFIX) :])
        except ValueError:
            continue
        if (
            isinstance(event, dict)
            and event.get("submission_id") == submission_id
            and type(event.get("worker_pid")) is int
            and event["worker_pid"] > 0
        ):
            events.append(event)
    return events


class RayJobsExecutor:
    kind = "ray-jobs-v1"
    max_gpus = 0

    def __init__(
        self,
        store: AttemptRepository,
        settings: RayJobsSettings,
        *,
        transport: httpx2.AsyncBaseTransport | None = None,
    ) -> None:
        self.store = store
        self.settings = settings
        self.target = settings.target
        # This increment runs on a separately started cluster on the same host
        # and in the same installed Python environment. No user-supplied shell.
        self.entrypoint = shlex.join(
            [sys.executable, "-m", "kusanagi_nerv.workloads.dummy"]
        )
        self.client = httpx2.AsyncClient(
            base_url=settings.address,
            headers={"Authorization": f"Bearer {settings.token}"},
            timeout=settings.timeout_seconds,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx2.Response:
        try:
            return await self.client.request(method, path, **kwargs)
        except httpx2.HTTPError as exc:
            raise ExecutorUnavailable("Ray transport outcome is unknown") from exc

    @staticmethod
    def _json(response: httpx2.Response) -> dict:
        try:
            data = response.json()
        except ValueError as exc:
            raise ExecutorUnavailable("Invalid Ray response") from exc
        if not isinstance(data, dict):
            raise ExecutorUnavailable("Invalid Ray response")
        return data

    async def _check_cluster(self) -> None:
        response = await self._request("GET", "/api/version")
        if (
            response.status_code != 200
            or self._json(response).get("session_name") != self.settings.cluster_id
        ):
            raise ExecutorUnavailable("Ray cluster identity is unavailable or changed")

    async def _lookup(self, record: DispatchRecord) -> ExecutionObservation:
        if record.revoked and not record.started:
            return ExecutionObservation(ExecutionStatus.CANCELLED)
        await self._check_cluster()
        path = f"/api/jobs/{record.claim.submission_id}"
        response = await self._request("GET", path)
        if response.status_code == 404:
            return (
                UNKNOWN
                if record.revoked
                else ExecutionObservation(ExecutionStatus.NOT_FOUND)
            )
        if response.status_code != 200:
            return UNKNOWN
        data = self._json(response)
        if (
            data.get("submission_id") != record.claim.submission_id
            or data.get("entrypoint") != self.entrypoint
            or not isinstance(data.get("metadata"), dict)
            or any(
                data["metadata"].get(key) != value
                for key, value in metadata(record).items()
            )
        ):
            return UNKNOWN  # Never adopt a foreign job under a colliding identity.
        status = data.get("status")
        if not isinstance(status, str):
            return UNKNOWN
        if status == "STOPPED":
            return ExecutionObservation(ExecutionStatus.CANCELLED)
        if status == "FAILED":
            # Ray status does not prove a failure is transient; conservative default.
            return ExecutionObservation(ExecutionStatus.FAILED)
        if status == "PENDING":
            return ExecutionObservation(ExecutionStatus.PENDING)
        if status not in {"RUNNING", "SUCCEEDED"}:
            return UNKNOWN
        response = await self._request("GET", path + "/logs")
        if response.status_code != 200:
            return UNKNOWN
        logs = self._json(response).get("logs")
        if not isinstance(logs, str):
            return UNKNOWN
        events = workload_events(logs, record.claim.submission_id)
        if status == "RUNNING":
            return ExecutionObservation(
                ExecutionStatus.RUNNING
                if any(event.get("event") == "started" for event in events)
                else ExecutionStatus.PENDING
            )
        completed = [event for event in events if event.get("event") == "completed"]
        if len(completed) != 1:
            return (
                UNKNOWN  # Do not invent a result; missing logs require reconciliation.
            )
        result = completed[0]
        if (
            type(result.get("completed_steps")) is not int
            or result["completed_steps"] != workload_steps(record)
            or not isinstance(result.get("checksum"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", result["checksum"])
        ):
            return UNKNOWN
        return ExecutionObservation(
            ExecutionStatus.SUCCEEDED,
            result=ExecutionResult(
                result["worker_pid"], result["completed_steps"], result["checksum"]
            ),
        )

    async def lookup(self, submission_id: str) -> ExecutionObservation:
        record = await self.store.read_dispatch(submission_id, self.target)
        return UNKNOWN if record is None else await self._lookup(record)

    async def submit(self, claim: Claim) -> ExecutionObservation:
        if claim.executor_target != self.target or claim.job.spec.resources.gpus != 0:
            return UNKNOWN
        await self._check_cluster()
        if not await self.store.begin_dispatch(claim):
            return await self.cancel(claim.submission_id)
        record = await self.store.read_dispatch(claim.submission_id, self.target)
        if record is None:
            return UNKNOWN
        # A revocation may commit after admission; cancellation then knows this
        # request can still arrive and cannot finalize from NOT_FOUND.
        if record.revoked or record.claim.job.requested_cancel_at is not None:
            return await self.cancel(claim.submission_id)
        resources = claim.job.spec.resources
        payload = {
            "submission_id": claim.submission_id,
            "entrypoint": self.entrypoint,
            "metadata": metadata(record),
            "entrypoint_num_cpus": resources.cpus,
            "entrypoint_num_gpus": 0,
            "entrypoint_memory": int(resources.memory_gib * 1024**3),
            "runtime_env": {
                "env_vars": {
                    "KUSANAGI_DUMMY_INPUT": json.dumps(
                        {
                            "submission_id": claim.submission_id,
                            "payload_hash": record.payload_hash,
                            "steps": workload_steps(record),
                        }
                    )
                }
            },
        }
        # Even a 400 can mean an existing identity. Never treat an HTTP response
        # (or a lost response) as a confirmed execution failure; look up the ID.
        await self._request("POST", "/api/jobs/", json=payload)
        return await self.lookup(claim.submission_id)

    async def cancel(self, submission_id: str) -> ExecutionObservation:
        record = await self.store.revoke_dispatch(submission_id, self.target)
        if record is None:
            return UNKNOWN
        observation = await self._lookup(record)
        if observation.status in {
            ExecutionStatus.SUCCEEDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            return observation
        if observation.status in {ExecutionStatus.PENDING, ExecutionStatus.RUNNING}:
            await self._request("POST", f"/api/jobs/{submission_id}/stop")
            return await self.lookup(submission_id)
        return UNKNOWN
