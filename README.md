# gust_py

Python runtime/package for Gust DAG authoring, parsing, and task execution.

## What This Project Provides

- A Python authoring API:
  - `Dag`
  - `@task(...)`
  - `log(...)`
  - runtime calls: `get_task_by_name_run(...)`, `get_secret_by_name(...)`
- A parser that converts Python DAG files into a stable JSON spec.
- A task invoker to execute one task from a DAG file.
- A run-completion invoker to execute a DAG callback (`run done`).

## Requirements

- Python `>=3.10`
- `uv`

## Install

```bash
uv sync --dev
```

## DAG Authoring API

```python
from gust import Dag, task, get_task_by_name_run, get_secret_by_name, log


class MyDag(Dag):
    def __init__(self):
        super().__init__(schedule="daily", on_finished_callback="done_my_run")

    @task(downstream=("prepare",), save=True)
    def hello(self, ctx):
        run_id = ctx["run_id"]
        prepare_run = get_task_by_name_run("prepare", run_id)
        secret = get_secret_by_name("SUPER_SECRET")
        log(f"prepare={prepare_run}, secret_loaded={bool(secret)}")
        return {"ok": True}

    def done_my_run(self, status, run):
        log(f"run {run['run_id']} finished with status={status}", level="info")
```

## Runtime Messaging Protocol

`gust` runtime messages use a framed protocol on stdio:

1. 4-byte big-endian frame length
2. UTF-8 JSON payload

Helpers:

- `send_frame(payload)` writes one frame to `stdout`.
- `_read_frame()` reads one frame from `stdin`.

Call helpers emit messages and wait for a response:

- `get_task_by_name_run(name, run_id)` emits:
  - `{"type": "call", "op": "get_task_by_name_run", "run_id": run_id, "name": name}`
- `get_secret_by_name(name)` emits:
  - `{"type": "call", "op": "get_secret_by_name", "name": name}`

Logging helper emits:

- `log(msg, level="info")`:
  - `{"type": "log", "level": level, "msg": msg}`

## CLI

The package exposes a `gust` CLI.

### Parse DAGs

```bash
gust parse --file path/to/dag_file.py
```

Output is JSON (printed to stdout), one list entry per discovered DAG class.

### Run a Task

```bash
gust task run \
  --file path/to/dag_file.py \
  --dag MyDag \
  --task hello \
  --ctx-json '{"run_id":"123"}'
```

Output is a framed JSON payload:

- Success:
  - `{"type":"result","ok":true,"data":{"value": ...}}`
- Error:
  - `{"type":"result","ok":false,"error":{"message":"...","type":"...","details":"..."}}`

### Run Completion Callback

```bash
gust run done \
  --file path/to/dag_file.py \
  --dag MyDag \
  --fn-name done_my_run \
  --status ok \
  --run-id 123
```

Behavior:

- On success: no payload output.
- On failure: process exits with code `1`.

## Development

Run tests:

```bash
uv run pytest -q
```

Run lint:

```bash
uv run ruff check .
```
