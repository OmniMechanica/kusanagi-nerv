"""Use the fenced Ray Jobs protocol for durable training attempts."""

import asyncio
import json
import re
import shlex
import sys
from dataclasses import asdict

from kusanagi_nerv.domain.execution import (
    CheckpointRef,
    Claim,
    ExecutionObservation,
    ExecutionStatus,
    FailureKind,
    TrainingExecutionResult,
)
from kusanagi_nerv.repositories.attempts import AttemptRepository, DispatchRecord
from kusanagi_nerv.training.distributed_config import START_PREFIX
from kusanagi_nerv.training.object_store import (
    ObjectIntegrityError,
    ObjectNotFound,
    ObjectStore,
    ObjectStoreSettings,
    attempt_prefix,
    checkpoint_key,
)
from kusanagi_nerv.training.recovery_config import (
    CHECKPOINT_PREFIX,
    INPUT_ENV,
    RECOVERY_RESULT_PREFIX,
    WORKER_LOST_EXIT,
    RecoveryConfig,
)

from .ray_jobs import UNKNOWN, RayJobsExecutor, RayJobsSettings


def events(logs: str, prefix: str, submission_id: str) -> list[dict]:
    found = []
    for line in logs.splitlines():
        marker, sep, content = line.partition(prefix)
        if not sep or (marker and "RayTrainWorker pid=" not in marker):
            continue
        try:
            event = json.loads(content)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get("submission_id") == submission_id:
            found.append(event)
    return found


class RayTrainingExecutor(RayJobsExecutor):
    kind = "ray-training-v1"

    def __init__(
        self,
        store: AttemptRepository,
        settings: RayJobsSettings,
        objects: ObjectStoreSettings,
        *,
        kill_after_epoch: int | None = None,
        transport=None,
    ) -> None:
        if kill_after_epoch is not None and not 1 <= kill_after_epoch < 200:
            raise ValueError("Experiment checkpoint epoch must be between 1 and 199")
        super().__init__(store, settings, transport=transport)
        self.objects = ObjectStore(objects)
        self.kill_after_epoch = kill_after_epoch
        self.target = (
            f"{settings.target}#training:{objects.identity}:{kill_after_epoch or 0}"
        )
        self.entrypoint = shlex.join(
            [sys.executable, "-m", "kusanagi_nerv.workloads.recovery"]
        )

    async def aclose(self) -> None:
        await super().aclose()
        self.objects.client.close()

    async def submit(self, claim: Claim) -> ExecutionObservation:
        spec = claim.job.spec
        if (
            spec.model != "tiny-mlp"
            or spec.dataset != "synthetic-v1"
            or not 1 <= spec.epochs <= 200
            or spec.resources.cpus != 2
            or (
                self.kill_after_epoch is not None
                and self.kill_after_epoch >= spec.epochs
            )
        ):
            return ExecutionObservation(ExecutionStatus.REJECTED)
        return await super().submit(claim)

    def _metadata(self, record: DispatchRecord) -> dict[str, str]:
        return {
            **super()._metadata(record),
            "kusanagi_workload": "recovery-v1",
            "kusanagi_recovery_target": self.target,
            "kusanagi_resume_sha256": record.claim.resume_checkpoint.sha256
            if record.claim.resume_checkpoint
            else "",
        }

    def _payload(self, record: DispatchRecord) -> dict:
        claim = record.claim
        recovery = RecoveryConfig(
            str(claim.job.job_id),
            self.objects.settings.identity,
            claim.resume_checkpoint,
            self.kill_after_epoch if claim.job.attempt == 1 else None,
        )
        payload = super()._payload(record)
        payload["entrypoint_num_cpus"] = 0  # CPUs belong to the two Train workers.
        payload["runtime_env"] = {
            "env_vars": {
                INPUT_ENV: json.dumps(
                    {
                        "submission_id": claim.submission_id,
                        "epochs": claim.job.spec.epochs,
                        "recovery": asdict(recovery),
                    }
                )
            }
        }
        return payload

    @staticmethod
    def _prefix(record: DispatchRecord) -> str:
        return attempt_prefix(str(record.claim.job.job_id), record.claim.submission_id)

    async def _failed(self, record: DispatchRecord, data: dict) -> ExecutionObservation:
        if data.get("driver_exit_code") != WORKER_LOST_EXIT:
            return ExecutionObservation(ExecutionStatus.FAILED)
        inherited = record.claim.resume_checkpoint
        checkpoint = await asyncio.to_thread(
            self.objects.latest_checkpoint,
            self._prefix(record),
            record.claim.job.spec.epochs - 1,
            after_epoch=inherited.epoch if inherited else 0,
        )
        retry = checkpoint
        if (
            retry is None
            and inherited
            and await self._verified(inherited.uri, inherited.sha256)
        ):
            retry = inherited
        return ExecutionObservation(
            ExecutionStatus.FAILED,
            FailureKind.TRANSIENT if retry else FailureKind.PERMANENT,
            checkpoint=checkpoint,
            retry_checkpoint=retry,
            checkpoint_reconciled=True,
        )

    async def _verified(self, uri: str, checksum: str) -> bool:
        try:
            await asyncio.to_thread(self.objects.read, uri, checksum)
        except (ObjectIntegrityError, ObjectNotFound):
            return False
        return True

    async def _checkpoint_from_logs(
        self, record: DispatchRecord, logs: str
    ) -> CheckpointRef | None:
        reports = events(logs, CHECKPOINT_PREFIX, record.claim.submission_id)
        valid = [
            event
            for event in reports
            if type(event.get("epoch")) is int
            and 1 <= event["epoch"] < record.claim.job.spec.epochs
        ]
        if not valid:
            return None
        latest = max(valid, key=lambda event: event["epoch"])
        key = checkpoint_key(self._prefix(record), latest["epoch"])
        uri = self.objects.uri(key)
        if latest.get("uri") != uri:
            return None
        try:
            digest = await asyncio.to_thread(self.objects.digest, key)
        except ObjectIntegrityError:
            return None
        return (
            CheckpointRef(uri, digest, latest["epoch"])
            if digest == latest.get("sha256")
            else None
        )

    async def _from_logs(
        self, record: DispatchRecord, status: str, logs: str
    ) -> ExecutionObservation:
        submission_id = record.claim.submission_id
        resume = record.claim.resume_checkpoint
        first_epoch = resume.epoch + 1 if resume else 1
        if status == "RUNNING":
            if not any(
                event.get("rank") == 0
                and event.get("world_size") == 2
                and event.get("device") == "cpu"
                and event.get("first_epoch") == first_epoch
                for event in events(logs, START_PREFIX, submission_id)
            ):
                return ExecutionObservation(ExecutionStatus.PENDING)
            checkpoint = await self._checkpoint_from_logs(record, logs)
            return ExecutionObservation(ExecutionStatus.RUNNING, checkpoint=checkpoint)

        reports = events(logs, RECOVERY_RESULT_PREFIX, submission_id)
        if not reports or any(report != reports[0] for report in reports):
            return UNKNOWN
        event = reports[0]
        key = f"{self._prefix(record)}/model.pt"
        expected_step = 6 * resume.epoch if resume else 0
        if (
            event.get("artifact_uri") != self.objects.uri(key)
            or type(event.get("worker_pid")) is not int
            or event["worker_pid"] <= 0
            or event.get("first_epoch") != first_epoch
            or event.get("restored_optimizer_step") != expected_step
            or event.get("completed_steps") != record.claim.job.spec.epochs
            or not isinstance(event.get("checksum"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", event["checksum"])
        ):
            return UNKNOWN
        digest = event["checksum"]
        if not await self._verified(event["artifact_uri"], digest):
            return UNKNOWN
        final_epoch = record.claim.job.spec.epochs
        checkpoint = await asyncio.to_thread(
            self.objects.latest_checkpoint,
            self._prefix(record),
            final_epoch,
            after_epoch=final_epoch - 1,
        )
        if checkpoint is None:
            return UNKNOWN
        return ExecutionObservation(
            ExecutionStatus.SUCCEEDED,
            result=TrainingExecutionResult(
                event["worker_pid"],
                event["completed_steps"],
                digest,
                event["artifact_uri"],
                first_epoch,
                expected_step,
            ),
            checkpoint=checkpoint,
        )
