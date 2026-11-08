# Parallel coursework: targeted lessons, not full-course completion

## Time budget

Recommended 21-day regime: ~1h/day of selective video/reading on relevant days, ~1.5h build/testing, ~0.5h interview drill. The four courses add up to substantially more than the available attention if watched end-to-end; focus on high-signal chapters instead.

### 1. CourseHunter FastAPI

https://coursehunter.net/course/fastapi

- Days 1–2: lessons **34–45** (Pydantic contracts, OpenAPI/API versioning).
- Days 2–3: lessons **47–58** (dependency injection, layered architecture, configuration).
- Days 3–4: lessons **70–89** (PostgreSQL, async SQLAlchemy, Alembic, CRUD, filtering).
- Day 17: lessons **90–101** (JWT, middleware, auth).
- Also practice **Python asyncio cancellation and TaskGroup** from official Python docs: FastAPI course alone is not a substitute for production concurrency reasoning.

### 2. CourseHunter Machine Learning in Production (Kyryl Truskovskyi)

https://coursehunter.net/course/mashinnoe-obuchenie-v-prodakshene

- Days 6–7: **50–61** (orchestration, Kubeflow/Airflow/Dagster). Compare their responsibilities with Ray and Flyte.
- Days 11–12: **67–90** (inference server, serving patterns, scaling). Watch selections rather than all demos.
- Day 18: **95–102** (monitoring) and selected platforms overview.

### 3. CourseHunter Machine Learning System Design Interview (ByteByteGo)

https://coursehunter.net/course/sobesedovanie-po-proektirovaniyu-sistem-mashinnogo-obucheniya

- **Reading/diagrams, no video lessons.** Start the 7-step interview method on Day 1; use 20-min design drills on Days 7, 14, 19, 21.
- Focus on requirement clarification, capacity estimates, failure modes, architecture diagram, tradeoffs and metrics for *ML compute platforms*, not just model selection.

### 4. CourseHunter MLOps Bootcamp (Zero To Mastery)

https://coursehunter.net/course/mlops-bootcamp-postroenie-infrastruktury-dlya-realnyh-ii-sistem

- Days 8–9: **16–25** (train script, metrics, artifacts and GPU training job).
- Days 11–12: **26–31** (inference endpoint and monitoring).
- Day 18: **33–47** selective (retraining workflow, promotion gates).
- Most examples are SageMaker / AWS specific, so compare them with the self-managed Kubernetes + Ray execution plane; do **not** replicate the entire AWS stack here.

## Required official supplements (course catalog does not fully cover them)

- Ray Core tasks: https://docs.ray.io/en/latest/ray-core/tasks.html
- Ray Train checkpoints: https://docs.ray.io/en/latest/train/user-guides/checkpoints.html
- Ray Train fault tolerance: https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html
- Ray Serve autoscaling: https://docs.ray.io/en/latest/serve/autoscaling-guide.html
- KubeRay: https://docs.ray.io/en/latest/cluster/kubernetes/index.html
- Kubernetes GPU scheduling: https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/
- PyTorch distributed overview: https://docs.pytorch.org/tutorials/beginner/dist_overview.html
- NVIDIA GPU Operator and MIG: https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-operator-mig.html
