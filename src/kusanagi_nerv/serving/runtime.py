"""Use the authenticated dashboard API to reconcile Ray Serve applications."""

import httpx2

from kusanagi_nerv.domain.execution import ExecutorUnavailable
from kusanagi_nerv.domain.models import Model
from kusanagi_nerv.executors.ray_jobs import RayJobsSettings

SERVE_PATH = "/api/serve/applications/"


class ServeListenerMismatch(ExecutorUnavailable):
    """The effective Serve configuration cannot support loopback inference."""


def application_config(model: Model) -> dict:
    return {
        "name": model.deployment_identity,
        "route_prefix": f"/models/{model.model_id}",
        "import_path": "kusanagi_nerv.serving.application:build",
        "args": {
            "model_id": str(model.model_id),
            "artifact_uri": model.artifact_uri,
            "artifact_sha256": model.artifact_sha256,
            "version": model.version,
            "revision": str(model.deploy_revision),
        },
    }


class ServeRuntime:
    def __init__(
        self, settings: RayJobsSettings, *, port: int = 8001, transport=None
    ) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("Serve HTTP port must be in 1..65535")
        self.settings = settings
        self.port = port
        self.client = httpx2.AsyncClient(
            base_url=settings.address,
            timeout=settings.timeout_seconds,
            headers={"Authorization": f"Bearer {settings.token}"},
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> httpx2.Response:
        try:
            response = await self.client.request(method, path, **kwargs)
            response.raise_for_status()
            return response
        except httpx2.HTTPStatusError as exc:
            raise ExecutorUnavailable(
                f"Ray Serve {method} {path} returned HTTP {exc.response.status_code}"
            ) from exc
        except httpx2.HTTPError as exc:
            raise ExecutorUnavailable(
                "Ray Serve request outcome is unavailable"
            ) from exc

    async def _get(self, path: str) -> dict:
        response = await self._request("GET", path)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ExecutorUnavailable("Invalid Ray Serve response") from exc
        if not isinstance(payload, dict):
            raise ExecutorUnavailable("Invalid Ray Serve response")
        return payload

    async def _check_cluster(self) -> None:
        version = await self._get("/api/version")
        if version.get("session_name") != self.settings.cluster_id:
            raise ExecutorUnavailable("Ray session identity changed")

    async def applications(self) -> dict[str, dict]:
        await self._check_cluster()
        details = await self._get(SERVE_PATH)
        apps = details.get("applications")
        if not isinstance(apps, dict) or any(
            not isinstance(app, dict) for app in apps.values()
        ):
            raise ExecutorUnavailable("Invalid Ray Serve applications response")
        http = details.get("http_options")
        proxy_location = details.get("proxy_location")
        if not apps and http is None and proxy_location is None:
            return apps  # Serve has not started yet.
        if (
            not isinstance(http, dict)
            or proxy_location not in ("HeadOnly", "EveryNode")
            or http.get("host") != "127.0.0.1"
            or http.get("port") != self.port
        ):
            raise ServeListenerMismatch(
                "Ray Serve HTTP listener differs from the configured loopback listener"
            )
        return apps

    async def apply(self, applications: list[dict]) -> None:
        await self._check_cluster()
        await self._request(
            "PUT",
            SERVE_PATH,
            json={
                "proxy_location": "HeadOnly",
                "http_options": {"host": "127.0.0.1", "port": self.port},
                "applications": applications,
            },
        )
