#!/usr/bin/env python3
"""Deterministic GPU-only linear inference over swactor data-plane paths."""

from __future__ import annotations

import json
import struct

import swactor
from tinygrad import Device, Tensor

_WEIGHT_COUNT = 6
_EXPECTED_WEIGHT_BYTES = _WEIGHT_COUNT * 4
_INPUT = [2.0, -1.0]


async def main(ctx: swactor.Context) -> None:
    weights_blob = await ctx.data.read_blob("/models/tiny-linear/weights")
    if weights_blob.length != _EXPECTED_WEIGHT_BYTES:
        raise ValueError(
            f"expected {_EXPECTED_WEIGHT_BYTES} model bytes, "
            f"received {weights_blob.length}"
        )
    with weights_blob.map() as mapped:
        weights = struct.unpack_from("<6f", mapped)

    matrix = Tensor(weights[:4]).reshape(2, 2)
    bias = Tensor(weights[4:])
    output = (Tensor(_INPUT).reshape(1, 2) @ matrix + bias).realize()
    device = Device.DEFAULT
    if not device.startswith("CUDA"):
        raise RuntimeError(f"GPU required; tinygrad selected {device}")

    payload = json.dumps(
        {"device": device, "output": output.tolist()[0]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    async with ctx.data.write_stream("/runs/self/results/inference") as results:
        await results.write(payload)


if __name__ == "__main__":
    swactor.run(main)
