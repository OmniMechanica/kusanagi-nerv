"""Deterministic CPU evidence for MLP-007, not model training."""

import hashlib
import json
import os

PREFIX = "KUSANAGI_DUMMY "
ITERATIONS_PER_STEP = 1_000_000


def main() -> None:
    payload = json.loads(os.environ["KUSANAGI_DUMMY_INPUT"])
    submission_id = payload["submission_id"]
    steps = payload["steps"]
    if type(steps) is not int or not 1 <= steps <= 8:
        raise ValueError("Invalid dummy step count")
    digest = bytes.fromhex(payload["payload_hash"])
    if len(digest) != 32:
        raise ValueError("Invalid payload digest")
    common = {"submission_id": submission_id, "worker_pid": os.getpid()}
    print(PREFIX + json.dumps({**common, "event": "started"}), flush=True)
    for _ in range(steps):
        for _ in range(ITERATIONS_PER_STEP):
            digest = hashlib.sha256(digest).digest()
    print(
        PREFIX
        + json.dumps(
            {
                **common,
                "event": "completed",
                "completed_steps": steps,
                "checksum": digest.hex(),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
