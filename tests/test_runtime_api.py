from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

import gust  # noqa: E402


def test_get_task_by_name_run_sends_call_and_returns_frame(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []
    expected = {"type": "result", "ok": True, "data": {"id": 1}}

    def fake_send_frame(payload: dict[str, Any]) -> None:
        sent.append(payload)

    def fake_read_frame() -> dict[str, Any]:
        return expected

    monkeypatch.setattr(gust, "send_frame", fake_send_frame)
    monkeypatch.setattr(gust, "_read_frame", fake_read_frame)

    result = gust.get_task_by_name_run("hello", "run-123")

    assert sent == [
        {
            "type": "call",
            "op": "get_task_by_name_run",
            "run_id": "run-123",
            "name": "hello",
        }
    ]
    assert result == expected


def test_get_secret_by_name_sends_call_and_returns_frame(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []
    expected = {"type": "result", "ok": True, "data": {"value": "abc"}}

    def fake_send_frame(payload: dict[str, Any]) -> None:
        sent.append(payload)

    def fake_read_frame() -> dict[str, Any]:
        return expected

    monkeypatch.setattr(gust, "send_frame", fake_send_frame)
    monkeypatch.setattr(gust, "_read_frame", fake_read_frame)

    result = gust.get_secret_by_name("SUPER_SECRET")

    assert sent == [
        {
            "type": "call",
            "op": "get_secret_by_name",
            "name": "SUPER_SECRET",
        }
    ]
    assert result == expected


def test_log_sends_info_level_by_default(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []

    def fake_send_frame(payload: dict[str, Any]) -> None:
        sent.append(payload)

    monkeypatch.setattr(gust, "send_frame", fake_send_frame)

    result = gust.log("hello")

    assert sent == [{"type": "log", "level": "info", "msg": "hello"}]
    assert result is None


def test_log_sends_custom_level(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []

    def fake_send_frame(payload: dict[str, Any]) -> None:
        sent.append(payload)

    monkeypatch.setattr(gust, "send_frame", fake_send_frame)

    gust.log("something happened", level="warn")

    assert sent == [{"type": "log", "level": "warn", "msg": "something happened"}]
