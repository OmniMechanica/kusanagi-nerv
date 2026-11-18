#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

for tool in docker kind kubectl helm python3; do
    command -v "$tool" >/dev/null || { echo "Missing tool: $tool" >&2; exit 1; }
done

if [[ -n "${KUSANAGI_DOCKER_CONTEXT:-}" ]]; then
    export DOCKER_CONTEXT="$KUSANAGI_DOCKER_CONTEXT"
fi

state=$(mktemp -d "${TMPDIR:-/tmp}/kusanagi-kind.XXXXXXXX")
suffix=$(printf '%s' "${state##*.}" | tr '[:upper:]' '[:lower:]')
cluster="kusanagi-$suffix"
namespace=kusanagi-nerv
export KUBECONFIG="$state/kubeconfig"
export HELM_CONFIG_HOME="$state/helm-config"
export HELM_CACHE_HOME="$state/helm-cache"
export HELM_DATA_HOME="$state/helm-data"

k() {
    kubectl --context "kind-$cluster" -n "$namespace" "$@"
}

cleanup() {
    local result=$?
    if [[ $result -ne 0 && -s "$KUBECONFIG" ]]; then
        k --request-timeout=10s get rayclusters,rayjobs,rayservices,pods,services -o wide >&2 || true
        k --request-timeout=10s get events --sort-by=.lastTimestamp >&2 || true
    fi
    kind delete cluster --name "$cluster" || result=1
    rm -rf -- "$state"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

kind create cluster --name "$cluster" --wait 120s \
    --image kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f
helm upgrade --install kuberay-operator kuberay-operator \
    --repo https://ray-project.github.io/kuberay-helm/ --version 1.6.2 \
    --kube-context "kind-$cluster" -n "$namespace" --create-namespace \
    --set singleNamespaceInstall=true --wait --timeout 180s
k wait --for=condition=Established --timeout=60s \
    crd/rayclusters.ray.io crd/rayjobs.ray.io crd/rayservices.ray.io

k apply --dry-run=server --validate=strict -k deploy/kind
k apply --dry-run=server --validate=strict -f deploy/kind/ray-job.yaml
k apply -k deploy/kind
k wait --for=jsonpath='{.status.readyWorkerReplicas}'=1 --timeout=240s raycluster/cpu-batch
k apply -f deploy/kind/ray-job.yaml

deadline=$((SECONDS + 210))
while true; do
    job_deployment=$(k --request-timeout=15s get rayjob/cpu-batch-score \
        -o jsonpath='{.status.jobDeploymentStatus}')
    case "$job_deployment" in
        Complete) break ;;
        Failed|ValidationFailed) k describe rayjob/cpu-batch-score >&2; exit 1 ;;
    esac
    if (( SECONDS >= deadline )); then
        echo "Timed out waiting for RayJob (status: $job_deployment)" >&2
        exit 1
    fi
    sleep 2
done

job_status=$(k get rayjob/cpu-batch-score -o jsonpath='{.status.jobStatus}')
if [[ "$job_status" != SUCCEEDED ]]; then
    echo "Expected RayJob to succeed, got $job_status" >&2
    exit 1
fi

k wait --for=condition=Ready --timeout=240s rayservice/cpu-predictor
serve_cluster=$(k get rayservice/cpu-predictor \
    -o jsonpath='{.status.activeServiceStatus.rayClusterName}')
job_id=$(k get rayjob/cpu-batch-score -o jsonpath='{.status.jobId}')
batch_head=$(k get pod -l 'ray.io/cluster=cpu-batch,ray.io/node-type=head' \
    -o jsonpath='{.items[0].metadata.name}')
batch_worker=$(k get pod -l 'ray.io/cluster=cpu-batch,ray.io/node-type=worker' \
    -o jsonpath='{.items[0].metadata.name}')
serve_worker=$(k get pod -l "ray.io/cluster=$serve_cluster,ray.io/node-type=worker" \
    -o jsonpath='{.items[0].metadata.name}')
k wait --for=condition=Ready --timeout=90s "pod/$serve_worker"

k exec "$batch_head" -- ray job logs --address http://127.0.0.1:8265 "$job_id" \
    > "$state/job.log"
k exec -i "$batch_head" -- python -I - > "$state/predict.json" <<'PY'
import json
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def prediction_request(instances):
    return Request(
        "http://cpu-predictor-serve-svc:8000/predict",
        data=json.dumps({"instances": instances}).encode(),
        headers={"Content-Type": "application/json"},
    )


request = prediction_request([[2, 4, 0, 1], [0, 0, 4, 0]])
for attempt in range(4):
    try:
        with urlopen(request, timeout=5) as response:
            result = {"http_status": response.status, "body": json.load(response)}
        break
    except (TimeoutError, URLError):
        if attempt == 3:
            raise
        sleep(1)

overflow = prediction_request([[1e308, -1e308, 1e308, 1e308]])
try:
    with urlopen(overflow, timeout=5):
        raise AssertionError("Expected an overflowing prediction to be rejected")
except HTTPError as exc:
    with exc:
        assert exc.code == 422, exc.code
        assert json.load(exc)["detail"] == "Prediction exceeds numeric range"
        result["overflow_status"] = exc.code
print(json.dumps(result))
PY

python3 -I - "$state/job.log" "$state/predict.json" "$batch_head" "$batch_worker" "$serve_worker" <<'PY'
import json
import sys
from pathlib import Path

job_log, predict_json, batch_head, batch_worker, serve_worker = sys.argv[1:]
prefix = "KUBERAY_BATCH "
results = [
    line.removeprefix(prefix)
    for line in Path(job_log).read_text().splitlines()
    if line.startswith(prefix)
]
assert len(results) == 1, results
batch = json.loads(results[0])
response = json.loads(Path(predict_json).read_text())
assert response["http_status"] == 200 and response["overflow_status"] == 422, response
prediction = response["body"]
assert batch["driver"] == batch_head and batch["workers"] == [batch_worker] * 2, batch
assert prediction["worker"] == serve_worker, prediction
assert batch["version"] == prediction["version"] == "linear-cpu-v1", (batch, prediction)
assert batch["predictions"] == prediction["predictions"] == [1.0, 3.0], (batch, prediction)
print(prefix + json.dumps(batch))
evidence = {
    "http_status": response["http_status"],
    "overflow_status": response["overflow_status"],
    **prediction,
}
print("KUBERAY_PREDICT " + json.dumps(evidence))
PY

k get rayclusters,rayjobs,rayservices,pods,services -o wide
