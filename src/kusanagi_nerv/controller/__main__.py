"""Run with python -m kusanagi_nerv.controller (explicit simulated executor)."""

import argparse
import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.controller.service import Controller, ControllerSettings
from kusanagi_nerv.db.connection import create_database_engine, database_url
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
        executor = SimulatedExecutor(sessions, duration_seconds=args.simulation_seconds)
        controller = Controller(
            AttemptRepository(sessions), executor, settings=settings
        )
        task = asyncio.create_task(controller.run())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        logging.getLogger(__name__).info(
            "Starting SIMULATED executor, owner=%s; no training is performed",
            controller.owner,
        )
        try:
            await task
        except asyncio.CancelledError:
            pass  # Leave the attempt for expiry/reconciliation, never mark cancelled.
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="KUSANAGI NERV durable controller")
    parser.add_argument("--executor", required=True, choices=["simulated"])
    parser.add_argument("--lease-seconds", type=float, default=30)
    parser.add_argument("--poll-seconds", type=float, default=1)
    parser.add_argument("--executor-timeout-seconds", type=float, default=5)
    parser.add_argument("--simulation-seconds", type=float, default=3)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(serve(args))
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
