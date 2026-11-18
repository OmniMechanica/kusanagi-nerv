import json
import socket

import ray
from scoring import MODEL_VERSION, score


@ray.remote(num_cpus=1, max_retries=0)
def score_row(row: tuple[float, float, float, float]) -> tuple[str, float]:
    return socket.gethostname(), score(row)


def main() -> None:
    ray.init(address="auto")
    try:
        driver = socket.gethostname()
        results = ray.get(
            [score_row.remote((2, 4, 0, 1)), score_row.remote((0, 0, 4, 0))],
            timeout=60,
        )
        workers, predictions = zip(*results, strict=True)
        if driver in workers or predictions != (1.0, 3.0):
            raise RuntimeError(f"Unexpected scoring results: {results!r}")
        print(
            "KUBERAY_BATCH "
            + json.dumps(
                {
                    "version": MODEL_VERSION,
                    "driver": driver,
                    "workers": workers,
                    "predictions": predictions,
                }
            ),
            flush=True,
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
