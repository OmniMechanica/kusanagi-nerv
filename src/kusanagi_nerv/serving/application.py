"""Ray Serve application built from immutable registry metadata."""

from copy import deepcopy

import torch
from pydantic import ValidationError
from ray import serve
from starlette.requests import Request
from starlette.responses import JSONResponse

from kusanagi_nerv.serving.artifacts import load_artifact
from kusanagi_nerv.serving.prediction import PredictRequest, ReplicaPrediction


@serve.deployment(ray_actor_options={"num_cpus": 0.25}, num_replicas=1)
class CpuModel:
    def __init__(
        self, uri: str, sha256: str, version: str, revision: str, model_id: str
    ) -> None:
        self.model = load_artifact(uri, sha256)
        self.float64_model = deepcopy(self.model).double()
        self.model_id = model_id
        self.sha256 = sha256
        self.version = version
        self.revision = revision

    async def __call__(self, request: Request) -> JSONResponse:
        if request.method == "GET":
            return JSONResponse(
                {
                    "status": "ready",
                    "version": self.version,
                    "deployment_revision": self.revision,
                }
            )
        if request.method != "POST":
            return JSONResponse({"error": "Method not allowed"}, status_code=405)
        try:
            batch = PredictRequest.model_validate_json(await request.body())
        except ValidationError:
            return JSONResponse({"error": "Invalid prediction input"}, status_code=422)
        with torch.inference_mode():
            features = torch.tensor(batch.instances, dtype=torch.float32, device="cpu")
            values = self.model(features)
            overflow = ~torch.isfinite(values)
            if overflow.any():
                values = values.double()
                values[overflow] = self.float64_model(features[overflow].double())
        try:
            result = ReplicaPrediction(
                model_id=self.model_id,
                version=self.version,
                artifact_sha256=self.sha256,
                deployment_revision=self.revision,
                predictions=values.tolist(),
            )
        except ValidationError:
            return JSONResponse({"error": "Prediction unavailable"}, status_code=503)
        return JSONResponse(result.model_dump(mode="json"))


def build(args: dict[str, str]) -> serve.Application:
    return CpuModel.bind(
        args["artifact_uri"],
        args["artifact_sha256"],
        args["version"],
        args["revision"],
        args["model_id"],
    )
