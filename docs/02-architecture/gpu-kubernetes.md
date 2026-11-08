# GPU and Kubernetes learning design

**Scope:** production-aware design in P0; actual GPU deployment/performance measurements are P2 because hardware is optional.

## GPU resource flow

1. GPU exists on a Kubernetes node with compatible drivers and runtime.
2. NVIDIA device plugin advertises extended resources such as `nvidia.com/gpu`.
3. Pod requests `nvidia.com/gpu` through resource limits (and matching requests where required by runtime policy).
4. Kubernetes scheduler places the Pod on a node with allocatable capacity and compatible node constraints.
5. Ray/KubeRay tracks runtime resource capacity and launches suitable tasks/train workers.
6. DCGM/NVIDIA telemetry measures usage; allocated GPU count alone is **not** utilization.

## Illustrative pod resources

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-worker-demo
spec:
  restartPolicy: Never
  containers:
    - name: trainer
      image: example/trainer:replace-me
      resources:
        limits:
          nvidia.com/gpu: 1
          cpu: '4'
          memory: 8Gi
```

This is a **design-only example**, not a deployable artifact: replace the image, install an appropriate device plugin/operator, and verify GPU nodes and runtime before running.

## Know the distinction

| Topic | Why it matters |
|---|---|
| Dedicated GPU vs MIG | Physical partitioning provides different isolation/capacity properties |
| Time slicing | Sharing time, not the same isolation as physical MIG slices |
| GPU requests vs utilization | Scheduling is based on advertised capacity; DCGM measures live use |
| GPU memory | Out-of-memory can occur even if compute utilization is low |
| DDP vs FSDP | Replicated training vs sharded model state; different memory/network tradeoffs |
| NCCL / networking | Collective communications affect distributed GPU efficiency |
| Locality / topology | Same-node communication and GPU interconnect affect placement and throughput |
| Scale to zero | Saves idle capacity but introduces startup/queue/initialization delays |
| Ray autoscaling | Runtime worker demand, KubeRay Pods, and node autoscaling are distinct loops |

## Required deliverables

- CPU KubeRay manifests and validation result.
- GPU request example + explanation of driver/device plugin/runtime dependencies.
- Diagram of queues, admission control, GPU quota and capacity/autoscaler boundaries.
- Explicit resource shortage behavior and tenant fairness tradeoff.
- Optional actual GPU trace (`nvidia-smi`, DCGM, Ray logs), with real machine and run metadata.

For authoritative up-to-date setup consult [official resources](../04-learning/official-resources.md).
