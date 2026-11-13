# Isolated Ray Core CPU example

MLP-006 exercises real Ray tasks and a stateful actor in separate processes.
Evidence is **TESTED-CPU**: deterministic arithmetic/hashing on one local node.
The API/controller still uses the **SIMULATED** adapter. Connecting durable jobs
to Ray Jobs belongs to MLP-007; this example does not train a model or use a GPU.

## Run it

From the repository root, with the pinned Python 3.12 interpreter:

```bash
uv sync --locked --extra ray
uv run --locked --extra ray python -m kusanagi_nerv.examples.ray_core \
  --tasks 8 --workers 2 --max-outstanding 4 --work-iterations 1000000
```

Ray 2.60.0 is pinned in the optional `ray` extra. Keep `--extra ray` on `uv run`:
an ordinary sync/run can remove unselected extras. PostgreSQL, the API, Docker
and a pre-existing Ray cluster are not required. `--help` works without Ray.
Run the command as a fresh process; the driver refuses a process that has
already imported Ray, whose authentication settings may already be cached.

Worker logs contain `task_start` / `task_end`, task index, PID and monotonic
timestamp. The last `RAY_DEMO_REPORT` line contains a JSON report, including
every task's interval, output, worker PID, actor snapshots and measured limits.
Ray log forwarding is asynchronous; the report is the complete result record.

Example observed on macOS arm64, 2026-11-13 (PIDs change on every run):

| Measurement | Observed |
|---|---:|
| Driver PID | 79095 |
| Actor PID, before and after | 79126 |
| Task worker PIDs | 79125, 79127 |
| Completed tasks / actor count | 8 / 8 |
| Actor accumulated value | 682780 |
| `max_running` | 2 |
| `peak_outstanding` | 4 |
| `backpressure_waits` | 4 |
| Logical CPUs / GPUs | 3 / 0 |
| Runtime shut down | true |

## Follow the execution

1. The driver starts its own local Ray runtime with `workers + 1` logical CPUs.
   One CPU is reserved for the actor; each task requests one CPU. All request
   zero GPUs. The actor's initial snapshot is awaited before submitting tasks,
   so its CPU reservation is established first.
2. `ray.put(tuple(range(64)))` creates a shared input and returns an `ObjectRef`.
   Passing that ref as a top-level task argument lets Ray resolve its value
   before entering the task. An ObjectRef is a handle to an eventual object,
   not the object itself and not a durable application job ID.
3. `.remote(...)` submits tasks without waiting for their results. The driver
   fills at most `max_outstanding` slots, then waits with `ray.wait`. It retrieves
   one ready result, records it in the actor and only then releases that slot.
4. The actor processes one method at a time and retains count, sum and completion
   order between calls. Each actor call is awaited, so the driver also avoids
   building an unbounded actor mailbox. Tasks retain no application state.
5. Once all results are collected, the driver stops its actor and shuts down its
   owned runtime. The temporary token, logs and runtime directory are removed.

The report distinguishes two different limits:

| Mechanism | What it bounds | What it does not bound |
|---|---|---|
| `num_cpus=1` per task, actor reserves one extra CPU | Ray scheduler admission for running tasks | OS CPU usage or arbitrary threads created by task code |
| `max_outstanding` with `ray.wait` | Submitted tasks whose results the driver has not consumed | All application memory, output size or CPU usage |

Outstanding tasks include queued, running and ready-but-unconsumed work. The
report does not pretend to measure scheduler queue length. `backpressure_waits`
counts occasions when the window was full while more input remained; it is not
a duration metric. `max_running` comes from overlap of task-body intervals using
the same node's monotonic clock. It is not a multi-node clock synchronization
solution or a speedup benchmark. Short tasks may run serially even with spare CPUs.

Try the same workload with `--workers 1 --max-outstanding 3`, then with
`--workers 2 --max-outstanding 1`. The first limits execution to one task while
allowing outstanding work; the second serializes submission despite spare CPUs.

## Failure boundaries and cleanup

```bash
# Both commands intentionally exit 1 without a success report.
uv run --locked --extra ray python -m kusanagi_nerv.examples.ray_core --fail-at 0
uv run --locked --extra ray python -m kusanagi_nerv.examples.ray_core --timeout-seconds 0.001
```

Task exceptions reach the driver through `ray.get` as `RayTaskError`. Task retries,
actor restart and actor method retries are disabled explicitly. An actor crash
loses its in-memory aggregate; the actor is not the durable PostgreSQL ledger.
There is no implicit controller retry or recovery of this example's work.

`--timeout-seconds` bounds batch waits **after Ray startup**. It includes actor
startup/method waits and task completion, but is not a wall-clock deadline for
`ray.init`, shutdown or the whole OS process. The integration tests use a separate
120-second subprocess timeout; the CI job has a ten-minute outer bound.

On success or a handled error/deadline, the driver cancels its outstanding tasks,
kills its actor, shuts down its own runtime and removes the temporary directory.
`RAY_DEMO_CLEANUP` reports the runtime path and whether Ray remains initialized;
tests also verify that the reported path no longer exists after process exit.
Abrupt OS termination such as SIGKILL can prevent Python cleanup. No global
`ray stop` or cleanup of another runtime is performed.

Each run uses `address="local"`, a unique namespace, a short `/tmp/kn-ray-*`
directory and a fresh authentication token with file mode `0600`. Authentication
is configured before importing Ray; existing environment values are restored
on exit. The example does not read/write `~/.ray/auth_token` or attach to another
cluster. Usage reporting and dashboard are disabled, object-store capacity is
128 MiB, and the advertised node IP is loopback. This is a local learning setup,
not a production network-isolation or multi-tenant security design.

Inputs are bounded before runtime startup: at most 1,000 tasks, 8 task CPU slots,
128 outstanding tasks and 5,000,000 hashing iterations per task. The driver keeps
the final results for the report; the window alone is not a total-memory bound.

## Reproduce the checks

```bash
uv run --locked --extra ray pytest -q tests/ray
uv run --locked --extra ray pytest -q -m 'not integration and not ray'
```

The five real Ray cases cover CPU/window combinations `(2, 4)`, `(1, 3)`, `(2, 1)`,
actor state, shared-input results, distinct process IDs, task failure, deadline
and cleanup. They assert an upper bound on concurrency, not timing-dependent
speedup or a minimum overlap. Without the optional dependency, Ray cases skip;
the dedicated CI job installs it explicitly. Pure input/measurement/environment
tests remain in the fast suite.

Code: [model](../../src/kusanagi_nerv/examples/ray_core/model.py),
[tasks and actor](../../src/kusanagi_nerv/examples/ray_core/workers.py),
[driver](../../src/kusanagi_nerv/examples/ray_core/runner.py),
[real runtime tests](../../tests/ray/test_ray_core.py).

## Learning checkpoint

Explain, without reading the code:

1. Why can four outstanding ObjectRefs coexist with only two running tasks?
2. What does the object store hold, and what changes when a ref is a top-level
   task argument versus a value returned by `ray.get`?
3. Why does the actor retain a counter between calls, and what happens if it dies?
4. Why are logical CPU requests not an OS CPU limit? When would a process pool
   be sufficient for this single-machine workload?
5. Why cannot this driver replace the durable controller and attempt ledger?

The learner explanation remains pending even after the technical checks pass.

Primary references: [pending-task backpressure](https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html),
[logical resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html),
[ray.init](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html),
[token authentication](https://docs.ray.io/en/latest/ray-security/token-auth.html).
