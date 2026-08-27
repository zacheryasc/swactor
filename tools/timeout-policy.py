#!/usr/bin/env python3
"""Reject unregistered Python timeout and sleep call sites."""

from __future__ import annotations

import ast
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOTS = ("apps", "crates", "src", "tests", "tools", "xtask")
SKIPPED_PARTS = {".venv", "__pycache__", "target"}


@dataclass(frozen=True)
class Allowance:
    path: str
    caller: str
    callee: str
    calls: int
    purpose: str


ALLOWANCES = (
    Allowance(
        "apps/myelin/node-image/tinygrad_worker.py",
        "fetch_whole",
        "urllib.request.urlopen",
        1,
        "model download request deadline",
    ),
    Allowance(
        "apps/myelin/node-image/tinygrad_worker.py",
        "CpuLineSampler._run",
        "self._stop.wait",
        1,
        "cpu sampler poll interval",
    ),
    Allowance(
        "crates/bindings/python/tests/test_bootstrap.py",
        "test_real_exec_attachment_and_blob_mapping",
        "subprocess.run",
        1,
        "child-process test completion fuse",
    ),
)


class TimingVisitor(ast.NodeVisitor):
    def __init__(self, path: str) -> None:
        self.path = path
        self.aliases: dict[str, str] = {}
        self.scopes: list[str] = []
        self.occurrences: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.violations: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:
        for name in node.names:
            if name.asname:
                self.aliases[name.asname] = name.name
            else:
                root = name.name.split(".", 1)[0]
                self.aliases[root] = root

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module is not None:
            for name in node.names:
                self.aliases[name.asname or name.name] = f"{node.module}.{name.name}"

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scopes.append(node.name)
        self.generic_visit(node)
        self.scopes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.scopes.append(node.name)
        self.generic_visit(node)
        self.scopes.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.scopes.append(node.name)
        self.generic_visit(node)
        self.scopes.pop()

    def visit_Call(self, node: ast.Call) -> None:
        callee = self._callee(node.func)
        if callee is not None and self._is_guarded(node, callee):
            caller = ".".join(self.scopes) or "<module>"
            key = (caller, callee)
            self.occurrences[key] += 1
            occurrence = self.occurrences[key]
            allowance = next(
                (
                    item
                    for item in ALLOWANCES
                    if item.path == self.path
                    and item.caller == caller
                    and item.callee == callee
                ),
                None,
            )
            if allowance is None:
                self.violations.append(
                    f"{self.path}:{node.lineno}: unapproved timing primitive "
                    f"`{callee}` in `{caller}`"
                )
            elif occurrence > allowance.calls:
                self.violations.append(
                    f"{self.path}:{node.lineno}: timing occurrence {occurrence} exceeds "
                    f"the {allowance.calls} audited call(s) in `{caller}` "
                    f"(`{allowance.purpose}`)"
                )
        self.generic_visit(node)

    def _callee(self, node: ast.expr) -> str | None:
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name):
            return None
        root = self.aliases.get(node.id, node.id)
        return ".".join((root, *reversed(parts)))

    @staticmethod
    def _is_guarded(node: ast.Call, callee: str) -> bool:
        if any(keyword.arg == "timeout" for keyword in node.keywords):
            return True
        return callee in {"asyncio.sleep", "asyncio.wait_for", "time.sleep"} or callee.endswith(
            (".set_read_timeout", ".set_write_timeout", ".settimeout", ".wait")
        )


def python_sources() -> list[Path]:
    sources: list[Path] = []
    for source_root in SOURCE_ROOTS:
        for path in (ROOT / source_root).rglob("*.py"):
            if not SKIPPED_PARTS.intersection(path.parts):
                sources.append(path)
    return sorted(sources)


def check_source(path: Path) -> list[str]:
    relative = path.relative_to(ROOT).as_posix()
    source = path.read_text(encoding="utf-8")
    syntax = ast.parse(source, filename=relative)
    visitor = TimingVisitor(relative)
    visitor.visit(syntax)
    return visitor.violations


def self_test() -> None:
    visitor = TimingVisitor("probe.py")
    visitor.visit(ast.parse("import time as clock\ndef work():\n    clock.sleep(1)\n"))
    assert visitor.violations == [
        "probe.py:3: unapproved timing primitive `time.sleep` in `work`"
    ]

    overage = TimingVisitor("crates/bindings/python/tests/test_bootstrap.py")
    overage.visit(
        ast.parse(
            "import subprocess\n"
            "def test_real_exec_attachment_and_blob_mapping():\n"
            "    subprocess.run([], timeout=1)\n"
            "    subprocess.run([], timeout=1)\n"
        )
    )
    assert overage.violations == [
        "crates/bindings/python/tests/test_bootstrap.py:4: timing occurrence 2 "
        "exceeds the 1 audited call(s) in "
        "`test_real_exec_attachment_and_blob_mapping` "
        "(`child-process test completion fuse`)"
    ]


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return 0
    if sys.argv[1:]:
        print("usage: timeout-policy.py [--self-test]", file=sys.stderr)
        return 2

    self_test()

    violations = [violation for path in python_sources() for violation in check_source(path)]
    if violations:
        print("Python timeout policy violations:", file=sys.stderr)
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("python-timeout-policy: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
