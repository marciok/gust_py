from __future__ import annotations

import json
import struct
import sys
from collections.abc import Callable
from typing import Any


class Dag:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._gust_args = args
        self._gust_kwargs = kwargs


def task(
    func: Callable[..., Any] | None = None,
    *,
    deps: Any | None = None,
    save: bool = False,
) -> Callable[..., Any]:
    def decorator(inner: Callable[..., Any]) -> Callable[..., Any]:
        return inner

    if func is None:
        return decorator
    return decorator(func)


def send_frame(payload: dict[str, Any]) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    sys.stdout.buffer.write(struct.pack(">I", len(data)))
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


def _read_exact(size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sys.stdin.buffer.read(remaining)
        if not chunk:
            raise EOFError("Unexpected EOF while reading frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_frame() -> Any:
    size_data = _read_exact(4)
    size = struct.unpack(">I", size_data)[0]
    payload = _read_exact(size)
    return json.loads(payload.decode("utf-8"))


def get_task_by_name_run(name: str, run_id: str) -> Any:
    send_frame(
        {"type": "call", "op": "get_task_by_name_run", "run_id": run_id, "name": name}
    )
    return _read_frame()


def get_secret_by_name(name: str) -> Any:
    send_frame({"type": "call", "op": "get_secret_by_name", "name": name})
    return _read_frame()


def log(msg: str, level: str = "info"):
    send_frame({"type": "log", "level": level, "msg": msg})


__all__ = ["Dag", "task", "get_task_by_name_run", "get_secret_by_name", "send_frame"]
