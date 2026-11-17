# Reproducible CPU training

MLP-008 trains a tiny PyTorch regressor locally. The CLI writes a versioned model
artifact and returns a JSON report with its absolute path, SHA-256 and metrics.
Ray Train will reuse the workload in MLP-009. The durable controller still runs
its dummy workload in this increment.

## Run and load the model

From the repository root, using the pinned Python interpreter:

```bash
uv sync --locked --extra training
training_dir=$(mktemp -d)
uv run --locked --extra training python -m kusanagi_nerv.training \
  --seed 42 --epochs 20 --batch-size 32 --learning-rate 0.01 \
  --output "$training_dir/model.pt"
```

PyTorch 2.15.0 comes from the [official CPU wheel index](https://download.pytorch.org/whl/cpu/torch/).
The `training` extra also provides NumPy for PyTorch initialization. The workload
generates its inputs locally and requires no dataset download, database or cluster.
Use a new output path for each run. The trainer publishes a complete file via a
hard link from a temporary file in the same directory. On a filesystem that
supports hard links, a reader can see the file only after serialization completes,
and concurrent runs cannot replace each other's artifacts.

Use the same file in a fresh process:

```bash
uv run --locked --extra training python - "$training_dir/model.pt" <<'PY'
import sys
from pathlib import Path

import torch
from kusanagi_nerv.training.loop import load_model

model = load_model(Path(sys.argv[1]))
with torch.inference_mode():
    print(model(torch.tensor([[0.25, -0.5, 0.1, 0.3]])))
PY
```

The loader rebuilds the expected architecture on CPU, checks the format/model
version and loads the `state_dict` with `weights_only=True` and strict key matching.
Use artifacts from trusted local runs. The model takes four float32 features and
produces a scalar prediction for each row.

## What the run measures

The `synthetic-v1` dataset draws 512 rows of four independent features and a
target with linear terms, a feature interaction and small additive noise. A
separate seeded permutation assigns 384 rows to training and 128 to evaluation.
The data and split seeds are fixed by the dataset version, so changing `--seed`
changes weight initialization and minibatch order while keeping evaluation rows
comparable. The evaluation split is used for reporting, with a fixed epoch budget.
Each model uses its own CPU generator for parameter initialization. Concurrent
Python callers get the same initial weights as sequential calls with their seeds
and leave PyTorch's default CPU RNG state untouched.

Each training batch performs an Adam update on mean squared error (MSE). The
reported training MSE averages batch losses weighted by the number of examples.
Evaluation runs on the held-out rows before the first update and after each epoch
with gradient tracking disabled. The artifact stores the full MSE/MAE history,
seeds, hyperparameters, split sizes and Python/PyTorch/platform details. The CLI
sets one PyTorch thread and enables deterministic algorithms. Reproducibility
comparisons use the same environment and configuration. PyTorch documents that
release and platform changes can affect results. [PyTorch reproducibility notes](https://docs.pytorch.org/docs/stable/notes/randomness.html)

Observed on macOS arm64 with Python 3.12.16 and PyTorch 2.15.0, using the command above:

| Measurement | Value |
|---|---:|
| Completed epochs | 20 |
| Initial evaluation MSE | 7.496293 |
| Final training MSE | 0.045307 |
| Final evaluation MSE | 0.103600 |
| Final evaluation MAE | 0.201685 |
| Prediction on the example row | 1.550198 |

Two independent runs produced the same artifact SHA-256 locally,
`8dd05ba6660a304ac91122b6500c901fcd1823f57c0cad191b0e30b18a783fda`.
The hash identifies the exact file bytes for integrity checks. The reproducibility
test compares metric history and weight tensors within the same environment.

## Handoff to the execution adapter

Python callers can use `train(TrainingConfig(...), Path(...))` and get a typed
`TrainingResult`. A process caller can parse the CLI's single JSON object after
exit code 0. It includes `artifact_path`, `artifact_sha256`, `completed_epochs`,
`final_train_mse`, `initial_eval_mse`, `final_eval_mse` and `final_eval_mae`. The
path identifies the actual file on that worker's filesystem. MLP-010 owns upload
to durable storage and publication of a remotely readable URI.

This artifact is for inference. A training checkpoint also needs optimizer state,
completed epoch or global step, RNG/sampler state or a deterministic reconstruction
rule, and schedule/scaler state when used. MLP-010 will verify recovery from a
published checkpoint after an injected failure.

## Reproduce the checks

```bash
uv run --locked --extra training pytest -q tests/training
```

CI runs the same CPU workload on Linux. Tests train in separate processes, verify
the returned path and file hash, compare repeat runs, reload the model for an
independent forward pass and check its reported metrics against that output. The
failure cases cover write interruption and an existing destination. A threaded
test synchronizes model construction and verifies both the weights and the CPU
RNG state across concurrent calls.

Learning checkpoint: explain why the held-out evaluation MSE measures something
different from controller lease health, and which state would let training resume
at epoch N+1.

Implementation: [training loop and artifact](../../src/kusanagi_nerv/training/loop.py),
[typed inputs and result](../../src/kusanagi_nerv/training/config.py),
[CLI](../../src/kusanagi_nerv/training/__main__.py),
[CPU tests](../../tests/training/test_cpu_training.py).
