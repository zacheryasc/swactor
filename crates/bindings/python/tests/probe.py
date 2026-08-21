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

    arena_fd = int(os.environ["SWACTOR_ARENA_FD"])
    try:
        arena_target = os.readlink(f"/proc/self/fd/{arena_fd}")
    except OSError:
        arena_open = 0
    else:
        arena_open = int("data-plane-arena" in arena_target)

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
    rendered = ",".join(f"{value:g}" for value in values)
    print(
        f"HAS_DATA={int(isinstance(ctx.data, swactor.DataPlane))} "
        f"LENGTH={blob.length} VALUES={rendered} ARENA_OPEN={arena_open} "
        f"IDENTITY_LEAKS={len(leaked)}"
    )


if __name__ == "__main__":
    swactor.run(main)
