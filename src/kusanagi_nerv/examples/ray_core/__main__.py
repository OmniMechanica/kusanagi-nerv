"""Run with ``python -m kusanagi_nerv.examples.ray_core --help``."""

import argparse
import json
import sys

from .model import DemoConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Isolated CPU-only Ray Core example")
    parser.add_argument("--tasks", type=int, default=8)
    parser.add_argument(
        "--workers", type=int, default=2, help="Task CPU slots; actor uses one more"
    )
    parser.add_argument(
        "--max-outstanding", type=int, default=4, help="Submitted, unconsumed tasks"
    )
    parser.add_argument("--work-iterations", type=int, default=250_000)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60,
        help="Batch deadline after Ray startup",
    )
    parser.add_argument(
        "--fail-at", type=int, help="Inject a task error at this zero-based index"
    )
    try:
        config = DemoConfig(**vars(parser.parse_args()))
    except ValueError as exc:
        parser.error(str(exc))

    try:
        from .runner import run_demo

        report = run_demo(config)
    except ModuleNotFoundError as exc:
        if exc.name != "ray":
            raise
        print("Install Ray first: uv sync --locked --extra ray", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Ray example failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    print("RAY_DEMO_REPORT " + json.dumps(report, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
