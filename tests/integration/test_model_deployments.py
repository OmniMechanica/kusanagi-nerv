"""Public model deployment and recovery against a dedicated local Ray Serve instance."""

import asyncio
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from uuid import UUID, uuid4

import httpx2
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from kusanagi_nerv.api import app as api_app
from kusanagi_nerv.controller.deployments import DeploymentController
from kusanagi_nerv.domain.execution import ExecutionStatus, TrainingExecutionResult
from kusanagi_nerv.domain.job_lifecycle import JobStatus
from kusanagi_nerv.domain.jobs import Job, JobResources, JobSpecification
from kusanagi_nerv.executors.ray_jobs import RayJobsSettings
from kusanagi_nerv.repositories.attempts import AttemptRepository
from kusanagi_nerv.repositories.models import ModelRepository
from kusanagi_nerv.repositories.postgres import PostgresJobRepository
from kusanagi_nerv.serving.artifacts import ArtifactUnavailable, InvalidArtifact
from kusanagi_nerv.serving.prediction import FLOAT32_MAX, ServePredictor
from kusanagi_nerv.serving.runtime import SERVE_PATH, ServeRuntime
from tests.integration.test_api_restart import ROOT, api_process
from tests.integration.test_ray_jobs import owned_ray_cluster, stop
from tests.integration.test_training_recovery import object_store as object_store

pytestmark = pytest.mark.integration
TENANT = "local-test-tenant"
INSTANCES = [[0.5, -2, 0.25, 0], [1, 2, 3, 4]]


async def register_model(engine, artifact_uri, sha256, *, tenant=TENANT, job_id=None):
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    jobs = PostgresJobRepository(sessions)
    attempts = AttemptRepository(sessions)
    if job_id is None:
        job_id = uuid4()
        now = datetime.now(UTC)
        await jobs.add(
            Job(
                job_id,
                tenant,
                JobSpecification(
                    "tiny-mlp", "synthetic-v1", 1, JobResources(2, 0, 0.25), 0
                ),
                JobStatus.QUEUED,
                now,
                now,
            )
        )
    claim = await attempts.claim_next(
        owner="trainer", executor_kind="ray-training-v1", lease_seconds=30
    )
    assert claim is not None and claim.job.job_id == job_id
    await attempts.observe(
        claim,
        ExecutionStatus.SUCCEEDED,
        result=TrainingExecutionResult(123, 1, sha256, artifact_uri, 1, 0),
    )
    job = await jobs.get(tenant_id=tenant, job_id=job_id)
    assert job is not None and job.result_model_id is not None
    return job.result_model_id


async def test_deploy_requests_and_reconciliation_preserve_identity(
    engine, postgres_url, monkeypatch
):
    model_id = await register_model(engine, "s3://test-bucket/good.pt", "a" * 64)
    foreign = await register_model(
        engine, "s3://test-bucket/foreign.pt", "b" * 64, tenant="other"
    )
    invalid = await register_model(engine, "s3://test-bucket/invalid.pt", "c" * 64)
    unavailable = await register_model(
        engine, "s3://test-bucket/unavailable.pt", "d" * 64
    )
    verified = []

    def verify(uri, _):
        verified.append(uri)
        if uri.endswith("invalid.pt"):
            raise InvalidArtifact("private error")
        if uri.endswith("unavailable.pt"):
            raise ArtifactUnavailable("private error")

    monkeypatch.setattr(api_app, "load_artifact", verify)
    settings = RayJobsSettings("http://127.0.0.1:8265", "session_serving", "a" * 64)
    apps = {}
    puts = []
    put_status = 200
    proxy_location = "HeadOnly"
    crash_after_apply = False
    predictions = []
    reply = dict(
        model_id=str(model_id),
        version="v1",
        artifact_sha256="a" * 64,
        deployment_revision="1",
        predictions=[0.25, -1.5],
    )
    invalid_reply = {}

    def predict_response(request):
        assert request.method == "POST" and request.url.path == f"/models/{model_id}"
        predictions.append(json.loads(request.content))
        if invalid_reply.get("outage"):
            raise httpx2.ConnectError("private connection error", request=request)
        return httpx2.Response(200, json={**reply, **invalid_reply})

    monkeypatch.setattr(
        api_app,
        "ServePredictor",
        lambda: ServePredictor(transport=httpx2.MockTransport(predict_response)),
    )

    def ray_response(request):
        assert request.headers["Authorization"] == f"Bearer {settings.token}"
        if request.url.path == "/api/version":
            return httpx2.Response(200, json={"session_name": settings.cluster_id})
        assert request.url.path == SERVE_PATH
        if request.method == "PUT":
            manifest = json.loads(request.content)
            puts.append(manifest)
            if put_status == 200:
                desired = {
                    config["name"]: config for config in manifest["applications"]
                }
                for name in apps.keys() - desired.keys():
                    del apps[name]
                for name, config in desired.items():
                    if apps.get(name, {}).get("deployed_app_config") != config:
                        apps[name] = {
                            "status": "DEPLOYING",
                            "deployed_app_config": config,
                        }
                if crash_after_apply:
                    raise asyncio.CancelledError
            return httpx2.Response(put_status)
        details = {"applications": apps}
        if apps:
            details.update(
                proxy_location=proxy_location,
                http_options={"host": "127.0.0.1", "port": 8001},
            )
        return httpx2.Response(200, json=details)

    runtime = ServeRuntime(settings, transport=httpx2.MockTransport(ray_response))
    controller = DeploymentController(
        ModelRepository(async_sessionmaker(engine, expire_on_commit=False)), runtime
    )
    app = api_app.create_app(database_url=postgres_url)
    location = f"/v1/models/{model_id}"
    deploy = location + "/deploy"
    predict = location + "/predict"
    try:
        async with (
            app.router.lifespan_context(app),
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app), base_url="http://test"
            ) as api,
        ):
            assert (await api.get(location)).json()["deploy_state"] == "not_deployed"
            assert (await api.get(f"/v1/models/{foreign}")).status_code == 404
            assert (await api.post(f"/v1/models/{foreign}/deploy")).status_code == 404
            assert not verified
            assert (
                await api.post(
                    f"/v1/models/{foreign}/predict", json={"instances": INSTANCES}
                )
            ).status_code == 404
            assert (
                await api.post(
                    f"/v1/models/{uuid4()}/predict", json={"instances": INSTANCES}
                )
            ).status_code == 404
            not_ready = await api.post(predict, json={"instances": INSTANCES})
            assert (
                not_ready.status_code == 409
                and not_ready.json()["error"]["code"] == "model_not_ready"
            )
            assert not predictions
            rejected = await api.post(f"/v1/models/{invalid}/deploy")
            assert (
                rejected.status_code == 409
                and rejected.json()["error"]["code"] == "invalid_artifact"
            )
            assert (await api.get(f"/v1/models/{invalid}")).json()[
                "deploy_state"
            ] == "not_deployed"
            outage = await api.post(f"/v1/models/{unavailable}/deploy")
            assert (
                outage.status_code == 503
                and outage.json()["error"]["code"] == "artifact_unavailable"
            )
            first, replay = await asyncio.gather(api.post(deploy), api.post(deploy))
            assert first.status_code == replay.status_code == 202
            assert first.headers["Location"] == location
            assert first.json() == replay.json()
            name = first.json()["deployment_identity"]

            async with controller.store.reconcile() as lock:
                assert lock is not None
                await controller.tick()
                assert not puts
            await controller.tick()
            assert len(puts) == 1
            assert puts[0]["applications"][0]["args"]["revision"] == "1"
            apps[name]["status"] = "RUNNING"
            proxy_location = "Disabled"
            await controller.tick()
            assert (await api.get(location)).json()["deploy_state"] == "deploying"
            proxy_location = "HeadOnly"
            await controller.tick()
            assert (await api.get(location)).json()["deploy_state"] == "ready"
            proxy_location = "Disabled"
            await controller.tick()
            assert (await api.get(location)).json()["deploy_state"] == "deploying"
            assert len(puts) == 1
            proxy_location = "HeadOnly"
            second = await register_model(
                engine, "s3://test-bucket/second.pt", "e" * 64
            )
            second_location = f"/v1/models/{second}"
            requested = await api.post(second_location + "/deploy")
            assert requested.status_code == 202
            second_name = requested.json()["deployment_identity"]
            apps[name]["status"] = "RUNNING"
            put_status = 503
            await controller.tick()
            assert (await api.get(location)).json()["deploy_state"] == "ready"
            assert (await api.get(second_location)).json()[
                "deploy_state"
            ] == "deploying"
            assert (await api.post(deploy)).status_code == 200
            assert len(puts) == 2 and second_name not in apps

            for invalid_input in (
                [],
                [[1, 2, 3]],
                [[1, 2, 3, 4, 5]],
                [[True, 2, 3, 4]],
                [["1", 2, 3, 4]],
                [[1e100, 2, 3, 4]],
                INSTANCES * 129,
            ):
                rejected = await api.post(predict, json={"instances": invalid_input})
                assert rejected.status_code == 422
                assert rejected.json()["error"]["code"] == "validation_error"
            non_finite = await api.post(
                predict,
                content='{"instances":[[1,2,3,NaN]]}',
                headers={"Content-Type": "application/json"},
            )
            assert non_finite.status_code == 422
            assert not predictions
            result = await api.post(predict, json={"instances": INSTANCES})
            assert result.status_code == 200
            assert predictions == [{"instances": INSTANCES}]
            assert result.json() == {
                "model_id": str(model_id),
                "version": "v1",
                "predictions": reply["predictions"],
            }
            for field, value in [
                ("model_id", str(uuid4())),
                ("version", "v2"),
                ("artifact_sha256", "f" * 64),
                ("deployment_revision", "2"),
                ("predictions", [0.25]),
                ("outage", True),
            ]:
                invalid_reply[field] = value
                unavailable_prediction = await api.post(
                    predict, json={"instances": INSTANCES}
                )
                assert unavailable_prediction.status_code == 503
                assert (
                    unavailable_prediction.json()["error"]["code"]
                    == "serving_unavailable"
                )
                invalid_reply.clear()

            apps.clear()
            put_status = 200
            await controller.tick()
            assert len(puts) == 3 and puts[-1] == puts[-2]
            assert (await api.get(location)).json()["deploy_state"] == "deploying"
            apps[name]["status"] = "DEPLOY_FAILED"
            await controller.tick()
            failed = (await api.get(location)).json()
            assert (
                failed["deploy_state"] == "failed"
                and failed["deploy_error"] == "serve_deployment_failed"
            )
            assert len(puts) == 3 and name in apps
            put_status = 503
            await controller.tick()
            assert len(puts) == 4 and name in apps
            assert {config["name"] for config in puts[-1]["applications"]} == {
                second_name
            }
            third = await register_model(engine, "s3://test-bucket/third.pt", "f" * 64)
            requested = await api.post(f"/v1/models/{third}/deploy")
            assert requested.status_code == 202
            third_name = requested.json()["deployment_identity"]
            put_status = 200
            crash_after_apply = True
            with pytest.raises(asyncio.CancelledError):
                await controller.tick()
            assert len(puts) == 5
            assert set(apps) == {second_name, third_name}
            assert (await api.get(location)).json()["deploy_state"] == "failed"
            crash_after_apply = False
            controller = DeploymentController(controller.store, runtime)
            await controller.tick()
            assert len(puts) == 5
            assert (await api.post(deploy)).status_code == 202
            apps["outside-this-ledger"] = {"status": "RUNNING"}
            await controller.tick()
            assert len(puts) == 5
            del apps["outside-this-ledger"]
            await controller.tick()
            manifests = {config["name"]: config for config in puts[-1]["applications"]}
            assert len(puts) == 6 and set(manifests) == {name, second_name, third_name}
            assert manifests[name]["args"]["revision"] == "2"
            assert manifests[second_name]["args"]["revision"] == "1"
            assert manifests[third_name]["args"]["revision"] == "1"
    finally:
        await runtime.aclose()


@contextmanager
def deploy_process(postgres_url, ray_info_file, port, *, crash_after_apply=False):
    command = [sys.executable, "-m", "kusanagi_nerv.controller.deployments"]
    if crash_after_apply:
        script = """import os
from kusanagi_nerv.controller.deployments import main
from kusanagi_nerv.serving.runtime import ServeRuntime
original = ServeRuntime.apply
async def crash(self, apps):
    await original(self, apps)
    os._exit(23)
ServeRuntime.apply = crash
main()
"""
        command = [sys.executable, "-c", script]
    command += [
        "--ray-info-file",
        str(ray_info_file),
        "--serve-port",
        str(port),
        "--poll-seconds",
        "0.2",
    ]
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env={**os.environ, "KUSANAGI_DATABASE_URL": postgres_url},
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            yield process, log
        finally:
            stop(process)


@pytest.fixture(scope="module")
def serve_ray_info_file(tmp_path_factory):
    if any(
        importlib.util.find_spec(name) is None for name in ("ray", "grpc_reflection")
    ):
        pytest.skip("Install the serving extra for real Ray Serve evidence")
    path = tmp_path_factory.mktemp("ray-serve") / "cluster.json"
    with owned_ray_cluster(path) as info:
        yield info


@pytest.mark.serving
async def test_real_serve_prediction_survives_lost_ack_and_controller_restart(
    engine, postgres_url, serve_ray_info_file, object_store, tmp_path, monkeypatch
):
    import torch

    from kusanagi_nerv.training.config import TrainingConfig
    from kusanagi_nerv.training.loop import load_model, train

    store, owned = object_store
    prefix = str(uuid4())
    owned.append(prefix)
    bad_artifact = store.publish(f"jobs/{prefix}/invalid.pt", b"J")
    invalid = await register_model(engine, *bad_artifact)
    with socket.socket() as address:
        address.bind(("127.0.0.1", 0))
        port = address.getsockname()[1]
    monkeypatch.setenv("KUSANAGI_SERVE_PORT", str(port))
    with api_process(postgres_url) as (base_url, _):
        async with httpx2.AsyncClient(
            base_url=base_url, timeout=15, trust_env=False
        ) as api:
            submitted = await api.post(
                "/v1/jobs",
                json={
                    "model": "tiny-mlp",
                    "dataset": "synthetic-v1",
                    "epochs": 1,
                    "resources": {"cpus": 2, "gpus": 0, "memory_gib": 0.25},
                },
            )
            assert submitted.status_code == 202
            job_id = UUID(submitted.json()["job_id"])
            weights = tmp_path / "model.pt"
            train(TrainingConfig(epochs=1), weights)
            artifact = store.publish(f"jobs/{prefix}/model.pt", weights.read_bytes())
            model_id = await register_model(engine, *artifact, job_id=job_id)
            completed = (await api.get(submitted.headers["Location"])).json()
            assert completed["status"] == "succeeded"
            assert completed["result_model_id"] == str(model_id)
            assert completed["artifact_uri"] == artifact[0]
            with torch.inference_mode():
                reference = load_model(weights)
                expected = reference(
                    torch.tensor(INSTANCES, dtype=torch.float32)
                ).tolist()
                boundary = [[-FLOAT32_MAX] * 4, INSTANCES[0]]
                boundary_features = torch.tensor(boundary, dtype=torch.float32)
                boundary_value = reference(boundary_features)[0]
                if not torch.isfinite(boundary_value):
                    boundary_value = reference.double()(boundary_features.double())[0]
                boundary_expected = boundary_value.item()

            invalid_response = await api.post(f"/v1/models/{invalid}/deploy")
            assert invalid_response.status_code == 409, invalid_response.text
            assert invalid_response.json()["error"]["code"] == "invalid_artifact"
            for nonfinite in ("nan", "inf"):
                unusable = torch.load(weights, weights_only=True)
                output_bias = next(reversed(unusable["model_state_dict"].values()))
                output_bias.fill_(float(nonfinite))
                data = BytesIO()
                torch.save(unusable, data)
                unusable_id = await register_model(
                    engine,
                    *store.publish(f"jobs/{prefix}/{nonfinite}.pt", data.getvalue()),
                )
                rejected = await api.post(f"/v1/models/{unusable_id}/deploy")
                assert rejected.status_code == 409, rejected.text
                assert rejected.json()["error"]["code"] == "invalid_artifact"
                unusable_model = await api.get(f"/v1/models/{unusable_id}")
                assert unusable_model.json()["deploy_state"] == "not_deployed"
            response = await api.post(f"/v1/models/{model_id}/deploy")
            assert response.status_code == 202, response.text
            location = response.headers["Location"]
            model = response.json()
            predict = location + "/predict"
            batch = {"instances": INSTANCES}
            assert (await api.post(predict, json=batch)).status_code == 409
            with deploy_process(
                postgres_url, serve_ray_info_file, port, crash_after_apply=True
            ) as (process, log):
                try:
                    async with asyncio.timeout(25):
                        while process.poll() is None:
                            await asyncio.sleep(0.1)
                except TimeoutError:
                    log.seek(0)
                    pytest.fail(f"Serve apply did not return: {log.read()[-4000:]}")
                log.seek(0)
                assert process.returncode == 23, log.read()
            assert (await api.get(location)).json()["deploy_state"] == "deploying"
            with deploy_process(postgres_url, serve_ray_info_file, port) as (
                process,
                log,
            ):
                async with asyncio.timeout(65):
                    while True:
                        log.seek(0)
                        assert process.poll() is None, log.read()
                        ready = (await api.get(location)).json()
                        assert ready["deploy_state"] != "failed", ready
                        if ready["deploy_state"] == "ready":
                            break
                        await asyncio.sleep(0.2)
                assert ready["deployment_identity"] == model["deployment_identity"]
                assert ready["artifact_sha256"] == artifact[1]
                assert (await api.post(location + "/deploy")).status_code == 200
                async with httpx2.AsyncClient(trust_env=False) as serve:
                    replica = await serve.get(
                        f"http://127.0.0.1:{port}/models/{model_id}"
                    )
                assert replica.status_code == 200, replica.text
                assert replica.json() == {
                    "status": "ready",
                    "version": "v1",
                    "deployment_revision": "1",
                }
                prediction = await api.post(predict, json=batch)
                assert prediction.status_code == 200, prediction.text
                wide = await api.post(predict, json={"instances": boundary})
                assert wide.status_code == 200, wide.text
                extreme, ordinary = wide.json()["predictions"]
                assert extreme == pytest.approx(boundary_expected)
                assert ordinary == pytest.approx(expected[0], abs=1e-6)
            settings = RayJobsSettings.from_info_file(serve_ray_info_file)
            async with httpx2.AsyncClient(
                base_url=settings.address,
                headers={"Authorization": f"Bearer {settings.token}"},
                timeout=25,
                trust_env=False,
            ) as dashboard:
                # This cluster belongs to the test. Restart its Serve service
                # while the persisted deployment remains desired.
                stopped = await dashboard.delete(SERVE_PATH)
                assert stopped.is_success, stopped.text
            async with asyncio.timeout(10):
                while (
                    while_stopped := await api.post(predict, json=batch)
                ).status_code == 200:
                    await asyncio.sleep(0.2)
            assert while_stopped.status_code == 503, while_stopped.text
            assert while_stopped.json()["error"]["code"] == "serving_unavailable"
            with deploy_process(postgres_url, serve_ray_info_file, port) as (
                process,
                log,
            ):
                async with asyncio.timeout(65):
                    while True:
                        log.seek(0)
                        assert process.poll() is None, log.read()
                        recovered = (await api.get(location)).json()
                        assert recovered["deploy_state"] != "failed", recovered
                        if recovered["deploy_state"] == "ready":
                            resumed_prediction = await api.post(predict, json=batch)
                            if resumed_prediction.status_code == 200:
                                break
                        await asyncio.sleep(0.2)
                assert recovered["deployment_identity"] == ready["deployment_identity"]
                replies = await asyncio.gather(
                    *(api.post(predict, json=batch) for _ in range(4))
                )
            for returned in (prediction, resumed_prediction, *replies):
                assert returned.status_code == 200, returned.text
                result = returned.json()
                assert result["model_id"] == str(model_id)
                assert result["version"] == completed["result_model_version"]
                assert result["predictions"] == pytest.approx(expected, abs=1e-6)
