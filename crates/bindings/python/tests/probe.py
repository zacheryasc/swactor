#!/usr/bin/env python3
"""L3 probe for the bootstrap slice.

Runs under ``swactor.run`` and reports facts observable without any
data-plane primitives: context shape, wake readability, and descriptor
hygiene across an exec boundary.
"""

from __future__ import annotations

import os
import select
import subprocess
import sys

import swactor

_CHILD_CHECK = (
    "import os, sys; "
    "sys.exit(0 if sys.argv[1] not in os.listdir('/proc/self/fd') else 3)"
)


async def main(ctx: swactor.Context) -> None:
    facts = [f"HAS_DATA={int(isinstance(ctx.data, swactor.DataPlane))}"]

    wake = int(os.environ["SWACTOR_WAKE_FD"])
    readable = bool(select.select([wake], [], [], 0)[0])
    facts.append(f"WAKE_READABLE={int(readable)}")

    # B5: a close_fds=False child must not see the CLOEXEC-armed wake fd.
    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD_CHECK, str(wake)],
        close_fds=False,
    )
    child.wait()
    facts.append(f"WAKE_LEAKED={int(child.returncode == 3)}")
    print(" ".join(facts))


if __name__ == "__main__":
    swactor.run(main)
