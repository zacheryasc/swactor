#!/usr/bin/env python3
"""Real-exec probe for actor attachment and zero-copy blob mapping."""

from __future__ import annotations

import os
import struct

import swactor


async def main(ctx: swactor.Context) -> None:
    blob = await ctx.data.read_blob("/models/tiny-linear/weights")
    with blob.map() as mapped:
        values = struct.unpack_from("<6f", mapped)

    try:
        os.fstat(198)
    except OSError:
        bootstrap_open = 0
    else:
        bootstrap_open = 1

    public = {name for name in dir(ctx.data) if not name.startswith("_")}
    leaked = public & {
        "actor",
        "actor_id",
        "edge",
        "edge_id",
        "offset",
        "peer",
        "ring",
        "socket",
    }
    leaked_environment = {
        name
        for name in (
            "SWACTOR_ARENA_FD",
            "SWACTOR_DATA_PLANE_ACTOR",
            "SWACTOR_JOB_CAPABILITY",
            "SWACTOR_DATA_PLANE_ENDPOINT",
        )
        if name in os.environ
    }
    rendered = ",".join(f"{value:g}" for value in values)
    print(
        f"HAS_DATA={int(isinstance(ctx.data, swactor.DataPlane))} "
        f"LENGTH={blob.length} VALUES={rendered} BOOTSTRAP_OPEN={bootstrap_open} "
        f"IDENTITY_LEAKS={len(leaked) + len(leaked_environment)}"
    )


if __name__ == "__main__":
    swactor.run(main)
