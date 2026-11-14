"""Foreground, separately owned local Ray Jobs cluster for development/tests."""

import argparse
import json
import os
import signal
import tempfile
import threading
from pathlib import Path

import httpx2

from .environment import runtime_environment


def serve(info_file: Path, *, cpus: int = 2, port: int = 8265) -> None:
    if info_file.exists():
        raise ValueError("The cluster info file already exists; choose an unused path")
    stopped = threading.Event()
    previous = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, lambda *_: stopped.set())
    published = False
    try:
        with tempfile.TemporaryDirectory(
            prefix="kn-ray-jobs-", dir="/tmp"
        ) as directory:
            with runtime_environment(directory):
                import ray

                try:
                    context = ray.init(
                        address="local",
                        num_cpus=cpus,
                        num_gpus=0,
                        include_dashboard=True,
                        dashboard_host="127.0.0.1",
                        dashboard_port=port,
                        object_store_memory=128 * 1024 * 1024,
                        _node_ip_address="127.0.0.1",
                        _temp_dir=directory,
                        log_to_driver=False,
                    )
                    address = "http://" + context.address_info["webui_url"]
                    token_file = Path(os.environ["RAY_AUTH_TOKEN_PATH"])
                    with httpx2.Client(trust_env=False, timeout=10) as client:
                        response = client.get(
                            address + "/api/version",
                            headers={
                                "Authorization": "Bearer "
                                + token_file.read_text().strip(),
                            },
                        )
                        response.raise_for_status()
                    info = {
                        "address": address,
                        "cluster_id": response.json()["session_name"],
                        "token_file": str(token_file),
                        "owner_pid": os.getpid(),
                    }
                    # Publish complete JSON atomically without replacing an
                    # existing file. Readers may start as soon as it appears.
                    fd, staged = tempfile.mkstemp(
                        prefix=".kn-ray-info-", dir=info_file.parent
                    )
                    try:
                        with os.fdopen(fd, "w") as output:
                            json.dump(info, output)
                        os.link(staged, info_file)
                        published = True
                    finally:
                        Path(staged).unlink(missing_ok=True)
                    print("KUSANAGI_RAY_CLUSTER " + json.dumps(info), flush=True)
                    stopped.wait()
                finally:
                    ray.shutdown()
    finally:
        if published:
            info_file.unlink(missing_ok=True)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Own a local CPU Ray Jobs cluster until SIGINT/SIGTERM"
    )
    parser.add_argument("--info-file", type=Path, required=True)
    parser.add_argument("--cpus", type=int, default=2)
    parser.add_argument("--port", type=int, default=8265)
    args = parser.parse_args()
    if not 1 <= args.cpus <= 64 or not 0 <= args.port <= 65535:
        parser.error("cpus must be 1..64 and port 0..65535")
    try:
        serve(args.info_file, cpus=args.cpus, port=args.port)
    except ModuleNotFoundError as exc:
        parser.error(
            f"Missing runtime dependency ({exc.name}); run uv sync --locked --extra ray-jobs"
        )
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
