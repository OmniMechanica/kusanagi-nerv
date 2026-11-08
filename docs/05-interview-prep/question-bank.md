# Interview drills and scorecard

## Python backend (coding)

1. Implement bounded-concurrency async worker queue; what does `asyncio.Semaphore` actually limit?
2. Explain `async def` vs CPU-bound Python training and GIL. Why does FastAPI request await not make training parallel?
3. Design idempotent `POST /jobs` under concurrent requests and DB unique constraint.
4. Implement retries with exponential backoff, jitter, maximum attempts and classification of fatal vs retryable errors.
5. Model a cancellation race and test it deterministically.
6. Implement paginated listing with stable ordering and tenant authorization checks.
7. What happens if an API process dies after COMMIT but before replying?
8. What happens if a dispatcher sends a Ray job but crashes before recording its execution ID?
9. How do you test a coroutine with timeouts without introducing flaky sleeps?
10. How would you design a typed Python SDK with a synchronous and async client?

## ML platform / GPU (system design)

1. Design an API to schedule 500 concurrent GPU training requests on a 32-GPU shared cluster. Specify queueing and admission control.
2. What is the role of Kubernetes device plugins? How are `nvidia.com/gpu` resources requested?
3. Contrast MIG, time slicing, and dedicated GPU allocation; isolation and performance tradeoffs.
4. Contrast Ray tasks, actors, Ray Train, Ray Serve, KubeRay RayJob and Flyte workflow semantics.
5. PyTorch DDP vs FSDP; model sharding and synchronization; when does NCCL matter?
6. Worker process failure vs worker node failure vs driver failure. Who owns retry state?
7. How are model / optimizer / epoch state persisted and restored without repeating a completed epoch?
8. How do you protect against overcommit, head-of-line blocking and resource starvation?
9. Ray Serve autoscale replicas, Ray worker Pods autoscaling, Kubernetes node autoscaling: three separate loops.
10. How do you calculate GPU utilization, GPU-hours, queue wait and cost per tenant? Why aren't allocation costs equal to cloud invoice?
11. How would you protect tenant artifacts and secrets and stop a tenant from viewing others' jobs?
12. How would you implement rolling deployment of a new model version with rollback?
13. What is real serverless GPU training? Who owns cold starts, fleet provisioning and isolation?
14. How would you design SLOs for training availability versus low-latency inference?
15. Which APIs require strong consistency, and where is eventual consistency fine?

## Stories you must be ready to tell

- DataRobot: an ML/GPU/Python production problem **you personally** solved; design decisions, reliability, failure recovery, result.
- DataRobot: a difficult scheduler/orchestrator failure (specific failure evidence, incident response, prevention).
- OpenVPN: scaling 1000+ node Kubernetes and internal developer platform, impact for engineers.
- Capgemini: mentoring / leading a 10-person team, architecture disagreements and measurable delivery.
- A mistake: what failed, how you fixed it, how you changed your engineering process.

## Mock interview scoring (0–4 per category)

| Criterion | 0 | 2 | 4 |
|---|---|---|---|
| Requirements | Doesn't ask | Main API + users | Defines scale/SLO/failure/budget constraints |
| Architecture | Unclear | Working single design | Planes, boundaries, alternatives and scaling |
| Python engineering | Incorrect | Works | Typed, testable, concurrency & failure-aware |
| GPU/ML | Vague | Understands training | Resource limits, DDP, checkpoint, model serving |
| Reliability | Happy path only | Retries | Races, recovery, idempotency, observability |
| Communication | Unstructured | Mostly clear | Concise, evidence-based, tradeoffs explicit |

**Target:** >=19/24 overall, no category <2. Score reflects preparation practice, not an employer's hiring criteria.
