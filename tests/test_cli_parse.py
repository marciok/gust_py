from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gust.cli import main, parse_dags_from_file  # noqa: E402


def _write_file(tmp_path: Path, name: str, contents: str) -> Path:
    path = tmp_path / name
    path.write_text(contents, encoding="utf-8")
    return path


def test_main_parse_emits_json(tmp_path: Path, capsys) -> None:
    path = _write_file(
        tmp_path,
        "simple.py",
        "\n".join(
            [
                "from gust import Dag, task",
                "",
                "class Simple(Dag):",
                "    @task",
                "    def hello(self, ctx):",
                "        return None",
            ]
        ),
    )

    argv = sys.argv
    sys.argv = ["gust", "parse", "--file", str(path)]
    try:
        main()
    finally:
        sys.argv = argv

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())
    assert payload[0]["mod"] == "Simple"
    assert payload[0]["task_list"] == ["hello"]
    assert payload[0]["tasks"]["hello"] == {"downstream": [], "save": False}
    assert payload[0]["options"] == {"schedule": None, "on_finished_callback": None}
    assert payload[0]["file_path"] == str(path)


def test_parse_attr_base_and_options(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "mixed.py",
        "\n".join(
            [
                "import gust",
                "from gust import task",
                "",
                "class NotDag:",
                "    pass",
                "",
                "class AttrDag(gust.Dag):",
                "    x = 1",
                "    def __init__(self):",
                '        super().__init__(schedule="daily", on_finished_callback="cb")',
                "",
                "    def helper(self):",
                "        return None",
                "",
                '    @task(downstream=("a", "b"), save=True)',
                "    def t1(self, ctx):",
                "        return None",
                "",
                '    @task(downstream="solo")',
                "    def t2(self, ctx):",
                "        return None",
            ]
        ),
    )

    result = parse_dags_from_file(str(path))
    assert len(result) == 1
    dag = result[0]
    assert dag["mod"] == "AttrDag"
    assert dag["options"] == {"schedule": "daily", "on_finished_callback": "cb"}
    assert dag["task_list"] == ["t1", "t2"]
    assert dag["tasks"]["t1"] == {"downstream": ["a", "b"], "save": True}
    assert dag["tasks"]["t2"] == {"downstream": ["solo"], "save": False}


def test_parse_super_name_target_and_nonconst(tmp_path: Path) -> None:
    path = _write_file(
        tmp_path,
        "odd.py",
        "\n".join(
            [
                "from gust import Dag, task",
                "",
                'foo = "weekly"',
                'bar = "done"',
                "flag = True",
                "",
                "class Odd(Dag):",
                "    def __init__(self):",
                "        self.other()",
                "        super.__init__(self, schedule=foo, on_finished_callback=bar)",
                "",
                "    def other(self):",
                "        return None",
                "",
                '    @task(downstream=[1, "x"], save=flag)',
                "    def t1(self, ctx):",
                "        return None",
                "",
                "    @task(downstream=1)",
                "    def t2(self, ctx):",
                "        return None",
            ]
        ),
    )

    result = parse_dags_from_file(str(path))
    dag = result[0]
    assert dag["options"] == {"schedule": None, "on_finished_callback": None}
    assert dag["tasks"]["t1"] == {"downstream": ["x"], "save": False}
    assert dag["tasks"]["t2"] == {"downstream": [], "save": False}
