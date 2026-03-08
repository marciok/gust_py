from __future__ import annotations

import json
import struct
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gust.cli import main, run_done_from_file, run_task_from_file  # noqa: E402


def _write_file(tmp_path: Path, name: str, contents: str) -> Path:
    path = tmp_path / name
    path.write_text(contents, encoding="utf-8")
    return path


def test_run_task_success(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "shalom.py",
        "\n".join(
            [
                "class Shalom:",
                "    def hello(self, ctx):",
                "        assert ctx[\"run_id\"] == \"34809\"",
                "        return 123",
            ]
        ),
    )

    result = run_task_from_file(str(path), "Shalom", "hello", "{\"run_id\":\"34809\"}")
    assert result == {"type": "result", "ok": True, "data": {"value": 123}}


def test_run_task_invalid_ctx_json(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "simple.py",
        "\n".join(["class Shalom:", "    def hello(self, ctx):", "        return 0"]),
    )

    result = run_task_from_file(str(path), "Shalom", "hello", "{oops}")
    assert result["ok"] is False
    assert result["error"]["type"] == "JSONDecodeError"


def test_run_task_non_object_ctx(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "simple.py",
        "\n".join(["class Shalom:", "    def hello(self, ctx):", "        return 0"]),
    )

    result = run_task_from_file(str(path), "Shalom", "hello", "[]")
    assert result["ok"] is False
    assert result["error"]["message"] == "ctx-json must be a JSON object"


def test_run_task_missing_dag(tmp_path: Path) -> None:
    path = _write_file(tmp_path, "simple.py", "class Shalom:\n    pass")
    result = run_task_from_file(str(path), "Nope", "hello", "{}")
    assert result["ok"] is False
    assert result["error"]["type"] == "ValueError"


def test_run_task_missing_task(tmp_path: Path) -> None:
    path = _write_file(tmp_path, "simple.py", "class Shalom:\n    pass")
    result = run_task_from_file(str(path), "Shalom", "hello", "{}")
    assert result["ok"] is False
    assert result["error"]["message"] == "Task 'hello' not found on dag 'Shalom'"


def test_run_task_can_import_local_module(tmp_path: Path) -> None:
    _write_file(tmp_path, "helpers.py", "VALUE = 321\n")
    path = _write_file(
        tmp_path,
        "simple.py",
        "\n".join(
            [
                "from helpers import VALUE",
                "",
                "class Shalom:",
                "    def hello(self, ctx):",
                "        return VALUE",
            ]
        ),
    )

    result = run_task_from_file(str(path), "Shalom", "hello", "{}")
    assert result == {"type": "result", "ok": True, "data": {"value": 321}}


def test_run_task_with_gust_imports(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "attr_dag.py",
        "\n".join(
            [
                "import gust",
                "from gust import task",
                "",
                "class AttrDag(gust.Dag):",
                "    def __init__(self):",
                "        super().__init__(schedule=\"daily\")",
                "",
                "    @task(deps=(\"a\", \"b\"), save=True)",
                "    def t1(self, ctx):",
                "        return 5",
            ]
        ),
    )

    result = run_task_from_file(str(path), "AttrDag", "t1", "{}")
    assert result == {"type": "result", "ok": True, "data": {"value": 5}}


def test_main_task_run(tmp_path: Path, capsysbinary) -> None:
    path = _write_file(
        tmp_path,
        "simple.py",
        "\n".join(["class Shalom:", "    def hello(self, ctx):", "        return 7"]),
    )

    argv = sys.argv
    sys.argv = [
        "gust",
        "task",
        "run",
        "--file",
        str(path),
        "--dag",
        "Shalom",
        "--task",
        "hello",
        "--ctx-json",
        "{}",
    ]
    try:
        main()
    finally:
        sys.argv = argv

    captured = capsysbinary.readouterr()
    raw = captured.out
    length = struct.unpack(">I", raw[:4])[0]
    payload = json.loads(raw[4 : 4 + length].decode("utf-8"))
    assert payload == {"type": "result", "ok": True, "data": {"value": 7}}


def test_run_done_success(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "done.py",
        "\n".join(
            [
                "class MyDag:",
                "    def done_my_run(self, status, run):",
                "        assert status == \"ok\"",
                "        assert run == {\"run_id\": \"123\"}",
                "        return \"done\"",
            ]
        ),
    )

    result = run_done_from_file(str(path), "MyDag", "done_my_run", "ok", "123")
    assert result is None


def test_main_run_done(tmp_path: Path, capsysbinary) -> None:
    path = _write_file(
        tmp_path,
        "done.py",
        "\n".join(
            [
                "class MyDag:",
                "    def done_my_run(self, status, run):",
                "        return status + run[\"run_id\"]",
            ]
        ),
    )

    argv = sys.argv
    sys.argv = [
        "gust",
        "run",
        "done",
        "--file",
        str(path),
        "--dag",
        "MyDag",
        "--fn-name",
        "done_my_run",
        "--status",
        "ok",
        "--run-id",
        "123",
    ]
    try:
        main()
    finally:
        sys.argv = argv

    captured = capsysbinary.readouterr()
    assert captured.out == b""
