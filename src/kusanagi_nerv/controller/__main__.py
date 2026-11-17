"""Run a separate durable controller with an explicitly selected executor."""

import argparse
import asyncio
import json
import logging
import signal
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.controller.service import Controller, ControllerSettings
from kusanagi_nerv.db.connection import create_database_engine, database_url
from kusanagi_nerv.executors.ray_jobs import RayJobsExecutor, RayJobsSettings
from kusanagi_nerv.executors.simulated import SimulatedExecutor
from kusanagi_nerv.repositories.attempts import AttemptRepository


async def serve(args: argparse.Namespace) -> None:
    settings = ControllerSettings(
        lease_seconds=args.lease_seconds,
        poll_seconds=args.poll_seconds,
        executor_timeout_seconds=args.executor_timeout_seconds,
    )
    engine = create_database_engine(database_url())
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        store = AttemptRepository(sessions)
        if args.executor in {"ray", "ray-training"}:
            if args.ray_info_file is None:
                raise ValueError("--ray-info-file is required with a Ray executor")
            info = json.loads(args.ray_info_file.read_text())
            ray_settings = RayJobsSettings(
                address=info["address"],
                cluster_id=info["cluster_id"],
                token=Path(info["token_file"]).read_text().strip(),
                timeout_seconds=min(args.executor_timeout_seconds, 3),
            )
            if args.executor == "ray-training":
                from kusanagi_nerv.executors.ray_training import RayTrainingExecutor
                from kusanagi_nerv.training.object_store import ObjectStoreSettings

                executor = RayTrainingExecutor(
                    store,
                    ray_settings,
                    ObjectStoreSettings.from_environment(),
                    kill_after_epoch=args.experiment_kill_after_epoch,
                )
            else:
                executor = RayJobsExecutor(store, ray_settings)
        else:
            executor = SimulatedExecutor(
                sessions, duration_seconds=args.simulation_seconds
            )
        controller = Controller(store, executor, settings=settings)
        task = asyncio.create_task(controller.run())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        logging.getLogger(__name__).info(
            "Starting executor=%s owner=%s",
            executor.kind,
            controller.owner,
        )
        try:
            await task
        except asyncio.CancelledError:
            pass  # Leave the attempt for expiry/reconciliation, never mark cancelled.
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
            if isinstance(executor, RayJobsExecutor):
                await executor.aclose()
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="KUSANAGI NERV durable controller")
    parser.add_argument(
        "--executor", required=True, choices=["simulated", "ray", "ray-training"]
    )
    parser.add_argument("--ray-info-file", type=Path)
    parser.add_argument("--lease-seconds", type=float, default=30)
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--executor-timeout-seconds", type=float, default=5)
    parser.add_argument("--simulation-seconds", type=float, default=3)
    parser.add_argument(
        "--experiment-kill-after-epoch",
        type=int,
        help="Kill rank zero after publishing this epoch on attempt one",
    )
    args = parser.parse_args()
    if args.experiment_kill_after_epoch is not None and args.executor != "ray-training":
        parser.error(
            "The checkpoint failure experiment requires --executor ray-training"
        )
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(serve(args))
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
