from __future__ import annotations

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


__all__ = ["Dag", "task"]
