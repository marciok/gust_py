import argparse
import ast
import importlib.util
import json
import os
import sys
from hashlib import sha256
from types import ModuleType
from typing import Any


def main():
    p = argparse.ArgumentParser(prog="gust")
    sub = p.add_subparsers(dest="cmd", required=True)

    parse = sub.add_parser("parse", help="Parse DAGs from a python file")
    parse.add_argument("--file", required=True)

    task = sub.add_parser("task", help="Task operations")
    task_sub = task.add_subparsers(dest="task_cmd", required=True)
    run = task_sub.add_parser("run", help="Run a task from a DAG file")
    run.add_argument("--file", required=True)
    run.add_argument("--dag", required=True)
    run.add_argument("--task", required=True)
    run.add_argument("--ctx-json", default="{}")

    args = p.parse_args()

    if args.cmd == "parse":
        result = parse_dags_from_file(args.file)
        print(json.dumps(result))
    elif args.cmd == "task" and args.task_cmd == "run":
        result = run_task_from_file(args.file, args.dag, args.task, args.ctx_json)
        print(json.dumps(result))


def parse_dags_from_file(path: str) -> list[dict[str, Any]]:
    abs_path = os.path.abspath(path)
    with open(abs_path, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source, filename=abs_path)

    dags: list[dict[str, Any]] = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not _is_dag_class(node):
            continue

        dag_info = _parse_dag_class(node)
        dag_info["file_path"] = abs_path
        dags.append(dag_info)

    return dags


def _is_dag_class(node: ast.ClassDef) -> bool:
    for base in node.bases:
        if isinstance(base, ast.Name) and base.id == "Dag":
            return True
        if isinstance(base, ast.Attribute) and base.attr == "Dag":
            return True
    return False


def _parse_dag_class(node: ast.ClassDef) -> dict[str, Any]:
    schedule, on_finished_callback = _parse_init_options(node)
    task_list, tasks = _parse_tasks(node)

    return {
        "mod": node.name,
        "error": {},
        "messages": [],
        "task_list": task_list,
        "stages": [],
        "tasks": tasks,
        "file_path": "",
        "options": {
            "schedule": schedule,
            "on_finished_callback": on_finished_callback,
        },
    }


def _parse_init_options(node: ast.ClassDef) -> tuple[Any | None, Any | None]:
    schedule: Any | None = None
    on_finished_callback: Any | None = None

    init_func = next(
        (
            item
            for item in node.body
            if isinstance(item, ast.FunctionDef) and item.name == "__init__"
        ),
        None,
    )
    if init_func is None:
        return schedule, on_finished_callback

    for call in _iter_calls(init_func):
        if not _is_super_init_call(call):
            continue
        for kw in call.keywords:
            if kw.arg == "schedule":
                schedule = _const_value(kw.value, default=schedule)
            elif kw.arg == "on_finished_callback":
                on_finished_callback = _const_value(kw.value, default=on_finished_callback)

    return schedule, on_finished_callback


def _parse_tasks(node: ast.ClassDef) -> tuple[list[str], dict[str, dict[str, Any]]]:
    task_list: list[str] = []
    tasks: dict[str, dict[str, Any]] = {}

    for item in node.body:
        if not isinstance(item, ast.FunctionDef):
            continue
        if item.name == "__init__":
            continue

        decorator = _find_task_decorator(item.decorator_list)
        if decorator is None:
            continue

        deps, save = _parse_task_decorator(decorator)
        task_list.append(item.name)
        tasks[item.name] = {"deps": deps, "save": save}

    return task_list, tasks


def _find_task_decorator(
    decorators: list[ast.expr],
) -> ast.expr | None:
    for dec in decorators:
        if isinstance(dec, ast.Name) and dec.id == "task":
            return dec
        if isinstance(dec, ast.Call):
            if isinstance(dec.func, ast.Name) and dec.func.id == "task":
                return dec
    return None


def _parse_task_decorator(dec: ast.expr) -> tuple[list[str], bool]:
    deps: list[str] = []
    save = False

    if isinstance(dec, ast.Call):
        for kw in dec.keywords:
            if kw.arg == "deps":
                deps = _parse_string_list(kw.value)
            elif kw.arg == "save":
                save = bool(_const_value(kw.value, default=save))

    return deps, save


def _parse_string_list(value: ast.expr) -> list[str]:
    if isinstance(value, (ast.List, ast.Tuple)):
        items: list[str] = []
        for elt in value.elts:
            const = _const_value(elt, default=None)
            if isinstance(const, str):
                items.append(const)
        return items
    const = _const_value(value, default=None)
    if isinstance(const, str):
        return [const]
    return []


def _iter_calls(func: ast.FunctionDef):
    for item in ast.walk(func):
        if isinstance(item, ast.Call):
            yield item


def _is_super_init_call(call: ast.Call) -> bool:
    if not isinstance(call.func, ast.Attribute):
        return False
    if call.func.attr != "__init__":
        return False
    target = call.func.value
    if (
        isinstance(target, ast.Call)
        and isinstance(target.func, ast.Name)
        and target.func.id == "super"
    ):
        return True
    if isinstance(target, ast.Name) and target.id == "super":
        return True
    return False


def _const_value(value: ast.expr, default: Any) -> Any:
    if isinstance(value, ast.Constant):
        return value.value
    return default


def run_task_from_file(
    path: str,
    dag_name: str,
    task_name: str,
    ctx_json: str,
) -> dict[str, Any]:
    try:
        ctx = json.loads(ctx_json) if ctx_json else {}
    except json.JSONDecodeError as exc:
        return _error_result("ctx-json is not valid JSON", exc)
    if not isinstance(ctx, dict):
        return _error_result("ctx-json must be a JSON object")

    try:
        module = _load_module(path)
    except Exception as exc:
        return _error_result(f"Failed to load module from {path}", exc)

    try:
        dag_obj = _resolve_dag(module, dag_name)
    except Exception as exc:
        return _error_result(str(exc), exc)

    task_fn = getattr(dag_obj, task_name, None)
    if task_fn is None:
        return _error_result(f"Task '{task_name}' not found on dag '{dag_name}'")
    if not callable(task_fn):
        return _error_result(f"Task '{task_name}' on dag '{dag_name}' is not callable")

    try:
        value = task_fn(ctx)
    except Exception as exc:
        return _error_result("Task execution failed", exc)

    return {"type": "result", "ok": True, "data": {"value": value}}


def _load_module(path: str) -> ModuleType:
    abs_path = os.path.abspath(path)
    digest = sha256(abs_path.encode("utf-8")).hexdigest()[:12]
    module_name = f"gust_dag_{digest}"
    spec = importlib.util.spec_from_file_location(module_name, abs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import module from {abs_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    module_dir = os.path.dirname(abs_path)
    added_path = False
    if module_dir and module_dir not in sys.path:
        sys.path.insert(0, module_dir)
        added_path = True
    try:
        spec.loader.exec_module(module)
    finally:
        if added_path:
            try:
                sys.path.remove(module_dir)
            except ValueError:
                pass
    return module


def _resolve_dag(module: ModuleType, dag_name: str) -> Any:
    if not hasattr(module, dag_name):
        raise ValueError(f"Dag '{dag_name}' not found in module")
    dag_obj = getattr(module, dag_name)
    if isinstance(dag_obj, type):
        return dag_obj()
    return dag_obj


def _error_result(message: str, exc: Exception | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": "result", "ok": False, "error": {"message": message}}
    if exc is not None:
        payload["error"]["type"] = exc.__class__.__name__
        payload["error"]["details"] = str(exc)
    return payload
