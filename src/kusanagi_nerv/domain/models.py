"""Registered model identity and durable deployment state."""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class DeployState(StrEnum):
    NOT_DEPLOYED = "not_deployed"
    DEPLOYING = "deploying"
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Model:
    model_id: UUID
    version: str
    artifact_uri: str
    artifact_sha256: str
    deploy_state: DeployState
    deploy_revision: int
    deploy_error: str | None

    @property
    def deployment_identity(self) -> str:
        return f"kn-model-{self.model_id}"
