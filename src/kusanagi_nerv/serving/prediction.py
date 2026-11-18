"""Validate inference payloads and forward predictions to the loopback proxy."""

import asyncio
import os
from typing import Annotated

import httpx2
from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, ValidationError

from kusanagi_nerv.domain.execution import ExecutorUnavailable
from kusanagi_nerv.domain.models import Model

FLOAT32_MAX = float.fromhex("0x1.fffffep+127")
MAX_BATCH = 256
type Feature = Annotated[
    float, Field(ge=-FLOAT32_MAX, le=FLOAT32_MAX, allow_inf_nan=False)
]
type FeatureRow = Annotated[list[Feature], Field(min_length=4, max_length=4)]


class PredictRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    instances: list[FeatureRow] = Field(min_length=1, max_length=MAX_BATCH)


class ReplicaPrediction(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    model_id: str
    version: str
    artifact_sha256: str
    deployment_revision: str
    predictions: list[FiniteFloat] = Field(min_length=1, max_length=MAX_BATCH)


class ServePredictor:
    def __init__(self, *, transport=None) -> None:
        try:
            port = int(os.environ.get("KUSANAGI_SERVE_PORT", "8001"))
            if not 1 <= port <= 65535:
                raise ValueError
        except ValueError as exc:
            raise ValueError("KUSANAGI_SERVE_PORT must be in 1..65535") from exc
        self.client = httpx2.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            timeout=3,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def predict(self, model: Model, batch: PredictRequest) -> list[float]:
        try:
            async with asyncio.timeout(3):
                response = await self.client.post(
                    f"/models/{model.model_id}", json=batch.model_dump(mode="json")
                )
            response.raise_for_status()
        except (httpx2.HTTPError, TimeoutError) as exc:
            raise ExecutorUnavailable("Ray Serve predictions are unavailable") from exc
        try:
            result = ReplicaPrediction.model_validate_json(response.content)
        except ValidationError as exc:
            raise ExecutorUnavailable(
                "Ray Serve returned an invalid prediction"
            ) from exc
        if (
            response.status_code != 200
            or result.model_id != str(model.model_id)
            or result.version != model.version
            or result.artifact_sha256 != model.artifact_sha256
            or result.deployment_revision != str(model.deploy_revision)
            or len(result.predictions) != len(batch.instances)
        ):
            raise ExecutorUnavailable("Prediction does not match the ready deployment")
        return result.predictions
