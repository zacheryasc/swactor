#!/usr/bin/env python3
"""Deterministic GPU-only linear inference over swactor data-plane paths."""

from __future__ import annotations

import asyncio
import json
import struct

import swactor
from tinygrad import Device, Tensor

_WEIGHT_COUNT = 6
_EXPECTED_WEIGHT_BYTES = _WEIGHT_COUNT * 4
_INPUT = [2.0, -1.0]

_RESULT_PATH = "/runs/self/results/inference"


def emit(stage: str, **facts: object) -> None:
    print(
        json.dumps({"stage": stage, **facts}, separators=(",", ":"), sort_keys=True),
        flush=True,
    )


async def receive_result(data: swactor.DataPlane) -> bytes:
    reader = await data.read_stream(_RESULT_PATH)
    chunks = []
    while (chunk := await reader.read()) is not None:
        chunks.append(bytes(chunk))
    return b"".join(chunks)


async def main(ctx: swactor.Context) -> None:
    weights_blob = await ctx.data.read_blob("/models/tiny-linear/weights")
    if weights_blob.length != _EXPECTED_WEIGHT_BYTES:
        raise ValueError(
            f"expected {_EXPECTED_WEIGHT_BYTES} model bytes, "
            f"received {weights_blob.length}"
        )
    with weights_blob.map() as mapped:
        weights = struct.unpack_from("<6f", mapped)
    emit("weights_loaded", bytes=weights_blob.length, values=len(weights))

    matrix = Tensor(weights[:4]).reshape(2, 2)
    bias = Tensor(weights[4:])
    output = (Tensor(_INPUT).reshape(1, 2) @ matrix + bias).realize()
    device = Device.DEFAULT
    if not device.startswith("CUDA"):
        raise RuntimeError(f"GPU required; tinygrad selected {device}")

    values = output.tolist()[0]
    emit("calculated", device=device, output=values)
    payload = json.dumps(
        {"device": device, "output": values},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    receiver = asyncio.create_task(receive_result(ctx.data))
    async with ctx.data.write_stream(_RESULT_PATH) as results:
        await results.write(payload)
    received = await receiver
    if received != payload:
        raise RuntimeError(
            f"result stream changed payload: expected {payload!r}, received {received!r}"
        )
    emit("result_stream_received", path=_RESULT_PATH, bytes=len(received))


if __name__ == "__main__":
    swactor.run(main)
