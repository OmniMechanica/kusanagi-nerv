"""Reconcile the model ledger with a local Ray Serve runtime."""

import argparse
import asyncio
import logging
import math
import signal
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.db.connection import create_database_engine, database_url
from kusanagi_nerv.domain.execution import ExecutorUnavailable
from kusanagi_nerv.domain.models import DeployState
from kusanagi_nerv.executors.ray_jobs import RayJobsSettings
from kusanagi_nerv.repositories.jobs import RepositoryUnavailable
from kusanagi_nerv.repositories.models import ModelRepository
from kusanagi_nerv.serving.runtime import (
    ServeListenerMismatch,
    ServeRuntime,
    application_config,
)

logger = logging.getLogger(__name__)
SERVE_STATES = {
    "RUNNING": DeployState.READY,
    "DEPLOY_FAILED": DeployState.FAILED,
    "UNHEALTHY": DeployState.FAILED,
}


class DeploymentController:
    def __init__(self, store: ModelRepository, runtime: ServeRuntime) -> None:
        self.store = store
        self.runtime = runtime

    async def tick(self) -> None:
        try:
            async with self.store.reconcile() as session:
                if session is None:
                    return
                models = await self.store.requested_models(session)
                if not models:
                    return
                try:
                    apps = await self.runtime.applications()
                except ServeListenerMismatch as exc:
                    for model in models:
                        if model.deploy_state is DeployState.READY:
                            await self.store.observe(
                                session, model, DeployState.DEPLOYING
                            )
                    logger.warning("Deployment reconciliation deferred: %s", exc)
                    return
                known = {model.deployment_identity for model in models}
                desired = []
                observations = []
                needs_apply = False
                observed_failure = False
                for model in models:
                    name = model.deployment_identity
                    app = apps.get(name, {})
                    if model.deploy_state is DeployState.FAILED:
                        # A complete PUT would reapply a retained failed config.
                        needs_apply |= name in apps and app.get("status") != "DELETING"
                        continue
                    config = application_config(model)
                    deployed = app.get("deployed_app_config")
                    matches = isinstance(deployed, dict) and all(
                        deployed.get(key) == value for key, value in config.items()
                    )
                    status = app.get("status") if matches else None
                    if matches and not isinstance(status, str):
                        raise ExecutorUnavailable(
                            "Invalid Ray Serve application status"
                        )
                    state = SERVE_STATES.get(status, DeployState.DEPLOYING)
                    observations.append((model, state))
                    if state is DeployState.FAILED:
                        observed_failure = True
                        continue
                    desired.append(config)
                    needs_apply |= not matches

                # Commit new failures before a PUT can remove their apps.
                # A restart after removal must find the failed revision in the ledger.
                if needs_apply and not observed_failure:
                    # Commit observations from GET even if publication fails.
                    try:
                        if apps.keys() - known:
                            raise ExecutorUnavailable(
                                "Serve runtime contains applications outside this ledger"
                            )
                        await self.runtime.apply(desired)
                    except ExecutorUnavailable as exc:
                        logger.warning("Deployment publication deferred: %s", exc)
                for model, state in observations:
                    await self.store.observe(session, model, state)
        except (ExecutorUnavailable, RepositoryUnavailable) as exc:
            logger.warning("Deployment reconciliation deferred: %s", exc)

    async def run(self, poll_seconds: float) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(poll_seconds)


async def run(args: argparse.Namespace) -> None:
    settings = RayJobsSettings.from_info_file(args.ray_info_file)
    engine = create_database_engine(database_url())
    runtime = ServeRuntime(settings, port=args.serve_port)
    try:
        store = ModelRepository(async_sessionmaker(engine, expire_on_commit=False))
        task = asyncio.create_task(
            DeploymentController(store, runtime).run(args.poll_seconds)
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
    finally:
        await runtime.aclose()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ray-info-file", required=True, type=Path)
    parser.add_argument("--serve-port", type=int, default=8001)
    parser.add_argument("--poll-seconds", type=float, default=1)
    args = parser.parse_args()
    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0:
        parser.error("Poll interval must be finite and positive")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run(args))
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
