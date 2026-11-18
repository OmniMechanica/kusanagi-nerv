import math
import socket

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, FiniteFloat
from ray import serve
from scoring import MODEL_VERSION, score

app = FastAPI()


class PredictionRequest(BaseModel):
    instances: list[tuple[FiniteFloat, FiniteFloat, FiniteFloat, FiniteFloat]] = Field(
        min_length=1, max_length=32
    )


@serve.deployment
@serve.ingress(app)
class Predictor:
    @app.post("/predict")
    def predict(self, request: PredictionRequest) -> dict[str, str | list[float]]:
        predictions = [score(row) for row in request.instances]
        if not all(map(math.isfinite, predictions)):
            raise HTTPException(
                status_code=422, detail="Prediction exceeds numeric range"
            )
        return {
            "version": MODEL_VERSION,
            "worker": socket.gethostname(),
            "predictions": predictions,
        }


application = Predictor.bind()
