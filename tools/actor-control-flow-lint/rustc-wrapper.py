#!/usr/bin/env python3
"""Build, cache, and execute the repository-owned rustc policy driver."""

from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
import subprocess
import sys


def fail(message: str) -> "None":
    print(f"actor-control-flow wrapper: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if len(sys.argv) < 2:
        fail("Cargo did not supply the real rustc path")

    real_rustc = sys.argv[1]
    rustc_args = sys.argv[2:]
    root = Path(__file__).resolve().parents[2]
    source = Path(__file__).with_name("driver.rs")
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", root / "target"))
    if not target_root.is_absolute():
        target_root = root / target_root
    cache = target_root / "actor-control-flow-lint"
    cache.mkdir(parents=True, exist_ok=True)

    bootstrap_env = os.environ.copy()
    bootstrap_env.pop("CARGO_MAKEFLAGS", None)
    bootstrap_env.pop("MAKEFLAGS", None)

    try:
        version = subprocess.check_output(
            [real_rustc, "--version", "--verbose"],
            text=True,
            stderr=subprocess.STDOUT,
            env=bootstrap_env,
        )
        sysroot = subprocess.check_output(
            [real_rustc, "--print", "sysroot"],
            text=True,
            stderr=subprocess.STDOUT,
            env=bootstrap_env,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot inspect pinned rustc: {error}")

    digest = hashlib.sha256(source.read_bytes() + version.encode()).hexdigest()[:20]
    driver = cache / f"driver-{digest}"
    lock_path = cache / "build.lock"

    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not driver.exists():
            temporary = cache / f".{driver.name}.{os.getpid()}.tmp"
            command = [
                real_rustc,
                str(source),
                "--crate-name",
                "myelin_actor_control_flow_lint",
                "--edition=2024",
                "-Cprefer-dynamic",
                "-L",
                str(Path(sysroot) / "lib"),
                "-o",
                str(temporary),
            ]
            result = subprocess.run(command, env=bootstrap_env)
            if result.returncode != 0:
                temporary.unlink(missing_ok=True)
                fail(
                    "failed to build compiler driver; the pinned toolchain must include "
                    "the rustc-dev component"
                )
            os.replace(temporary, driver)

    package = os.environ.get("CARGO_PKG_NAME", "unknown")
    test_build = "--test" in rustc_args
    child_env = os.environ.copy()
    child_env["MYELIN_ACTOR_LINT_PACKAGE"] = package
    if test_build:
        child_env["MYELIN_ACTOR_LINT_TEST_BUILD"] = "1"
    else:
        child_env.pop("MYELIN_ACTOR_LINT_TEST_BUILD", None)

    rustc_lib = str(Path(sysroot) / "lib")
    current_library_path = child_env.get("LD_LIBRARY_PATH")
    child_env["LD_LIBRARY_PATH"] = (
        f"{rustc_lib}:{current_library_path}" if current_library_path else rustc_lib
    )

    os.execvpe(str(driver), [real_rustc, *rustc_args], child_env)


if __name__ == "__main__":
    main()
