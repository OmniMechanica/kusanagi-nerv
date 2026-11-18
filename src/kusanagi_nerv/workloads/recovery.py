"""Ray Jobs entrypoint for one controller-owned training attempt."""

import json
import os
import tempfile
import traceback
from pathlib import Path

import ray
import torch
from ray import train
from ray.exceptions import ActorDiedError
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError

from kusanagi_nerv.training.config import TrainingConfig
from kusanagi_nerv.training.distributed import train_distributed
from kusanagi_nerv.training.distributed_config import DistributedConfig
from kusanagi_nerv.training.object_store import (
    ObjectStore,
    ObjectStoreSettings,
    attempt_prefix,
)
from kusanagi_nerv.training.recovery_config import (
    INPUT_ENV,
    RECOVERY_RESULT_PREFIX,
    WORKER_LOST_EXIT,
    RecoveryConfig,
)


def main() -> int:
    payload = json.loads(os.environ[INPUT_ENV])
    recovery = RecoveryConfig.from_payload(payload["recovery"])
    config = DistributedConfig(
        payload["submission_id"], TrainingConfig(epochs=payload["epochs"])
    )
    store = ObjectStore(ObjectStoreSettings.from_environment())
    if store.settings.identity != recovery.storage_identity:
        raise ValueError("Recovery object store identity changed")
    # Ray Jobs sets the exact cluster address for the launched entrypoint.
    ray.init(address=os.environ["RAY_ADDRESS"], log_to_driver=True)
    try:
        with tempfile.TemporaryDirectory(prefix="kn-recovery-") as directory:
            path = Path(directory) / "model.pt"
            try:
                output = train_distributed(config, path, recovery=recovery)
            except train.WorkerGroupError as exc:
                # Ray 2.60 can retain the actor death inside its health-check
                # wrapper. Ordinary Python exceptions/timeouts are not worker loss.
                failures = (
                    error.health_check_failure
                    if isinstance(error, WorkerHealthCheckFailedError)
                    else error
                    for error in exc.worker_failures.values()
                )
                if any(isinstance(error, ActorDiedError) for error in failures):
                    traceback.print_exc()
                    return WORKER_LOST_EXIT
                raise
            artifact_uri, sha256 = store.publish(
                f"{attempt_prefix(recovery.job_id, config.submission_id)}/model.pt",
                path.read_bytes(),
            )
            artifact = torch.load(path, weights_only=True, map_location="cpu")
            evidence = artifact["distributed"]
            print(
                RECOVERY_RESULT_PREFIX
                + json.dumps(
                    {
                        "submission_id": config.submission_id,
                        "worker_pid": output.workers[0].pid,
                        "completed_steps": output.training.completed_epochs,
                        "checksum": sha256,
                        "artifact_uri": artifact_uri,
                        "first_epoch": evidence["first_epoch"],
                        "restored_optimizer_step": evidence["restored_optimizer_step"],
                    },
                    allow_nan=False,
                ),
                flush=True,
            )
            return 0
    finally:
        ray.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
