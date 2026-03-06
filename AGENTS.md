This repository contains **Gust’s Python runtime/package** (aka `gust_py`), which provides:
- A Python authoring API for DAGs (`Dag`, `@task`, `log`, helpers)
- A parser that converts Python DAG files into a stable JSON spec consumed by Gust (Elixir)
- An invoker that runs a single task (ideal for BEAM Ports / subprocess execution)
- Optional hooks/callbacks (e.g., run completion)

## Quick start

### Requirements
- Python: **3.13+**
- Tooling: **uv**

### Install dev deps
```bash
uv add --dev pytest pytest-cov ruff
