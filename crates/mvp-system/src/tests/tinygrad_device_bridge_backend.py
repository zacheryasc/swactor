#!/usr/bin/env python3
"""Tinygrad-backed device bridge probe for Rust MVP bridge tests.

Line-delimited JSON control only. Payload bytes live in the arena file whose
path Rust passes during initialize.
"""

from __future__ import annotations

import json
import os
import struct
import sys
from dataclasses import dataclass
from typing import Any

try:
    from tinygrad import Tensor  # type: ignore
except Exception as exc:  # pragma: no cover - exercised from Rust process tests
    print(
        json.dumps(
            {
                "type": "worker_fatal",
                "reason": "tinygrad_unavailable",
                "message": str(exc),
            }
        ),
        flush=True,
    )
    raise SystemExit(2)


@dataclass
class DeviceObject:
    dtype: str
    shape: str
    extent: int
    values: list[int]
    tensor: Any


@dataclass
class PendingCopy:
    kind: str
    copy_id: int
    handle_id: int
    host_offset: int
    device_offset: int
    length: int


arena_path: str | None = None
arena_bytes = 0
generation = 0
objects: dict[int, DeviceObject] = {}
pending_copies: dict[int, PendingCopy] = {}
fail_next: str | None = None


def emit(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, separators=(",", ":")), flush=True)


def backend_error(reason: str) -> None:
    emit({"type": "backend_error", "reason": reason})


def fatal(reason: str, message: str) -> None:
    emit({"type": "worker_fatal", "reason": reason, "message": message})


def require_arena() -> str:
    if arena_path is None:
        raise RuntimeError("arena is not initialized")
    return arena_path


def consume_failure(reason: str) -> bool:
    global fail_next
    if fail_next == reason:
        fail_next = None
        backend_error(reason)
        return True
    return False


def check_u32_range(offset: int, length: int) -> None:
    if offset < 0 or length < 0 or offset % 4 != 0 or length % 4 != 0:
        raise ValueError("u32 ranges must be non-negative and 4-byte aligned")


def tensor_from_values(values: list[int]) -> Any:
    try:
        return Tensor(values, dtype="uint32").realize()
    except Exception:
        return Tensor(values, dtype="int32").realize()


def allocate_tensor(dtype: str, extent: int) -> tuple[list[int], Any]:
    if dtype == "u32":
        if extent % 4 != 0:
            raise ValueError("u32 extent must be 4-byte aligned")
        values = [0] * (extent // 4)
        return values, tensor_from_values(values)
    if dtype == "f16":
        values = [0] * (extent // 2)
        return values, Tensor(values, dtype="float16").realize()
    raise ValueError(f"unsupported dtype {dtype!r}")


def update_tensor(obj: DeviceObject) -> None:
    if obj.dtype == "u32":
        obj.tensor = tensor_from_values(obj.values)
    elif obj.dtype == "f16":
        obj.tensor = Tensor(obj.values, dtype="float16").realize()
    else:
        raise ValueError(f"unsupported dtype {obj.dtype!r}")


def materialized_values(obj: DeviceObject) -> list[int]:
    return [int(v) for v in obj.tensor.tolist()]


def read_arena(offset: int, length: int) -> bytes:
    path = require_arena()
    with open(path, "rb", buffering=0) as f:
        f.seek(offset)
        data = f.read(length)
    if len(data) != length:
        raise EOFError("short arena read")
    return data


def write_arena(offset: int, data: bytes) -> None:
    path = require_arena()
    with open(path, "r+b", buffering=0) as f:
        f.seek(offset)
        f.write(data)
        f.flush()


def perform_host_to_device(copy: PendingCopy) -> None:
    obj = objects[copy.handle_id]
    if obj.dtype != "u32":
        raise ValueError("payload copy is implemented for u32 test objects only")
    check_u32_range(copy.device_offset, copy.length)
    payload = read_arena(copy.host_offset, copy.length)
    words = list(struct.unpack("<" + "I" * (copy.length // 4), payload))
    start = copy.device_offset // 4
    end = start + len(words)
    if end > len(obj.values):
        raise ValueError("device range out of bounds")
    obj.values[start:end] = words
    update_tensor(obj)


def perform_device_to_host(copy: PendingCopy) -> None:
    obj = objects[copy.handle_id]
    if obj.dtype != "u32":
        raise ValueError("payload copy is implemented for u32 test objects only")
    check_u32_range(copy.device_offset, copy.length)
    values = materialized_values(obj)
    start = copy.device_offset // 4
    end = start + (copy.length // 4)
    if end > len(values):
        raise ValueError("device range out of bounds")
    payload = struct.pack("<" + "I" * (end - start), *values[start:end])
    write_arena(copy.host_offset, payload)


def perform_copy(copy: PendingCopy) -> None:
    if copy.kind == "host_to_device":
        perform_host_to_device(copy)
    elif copy.kind == "device_to_host":
        perform_device_to_host(copy)
    else:
        raise ValueError(f"unknown copy kind {copy.kind!r}")


def handle(req: dict[str, Any]) -> bool:
    global arena_path, arena_bytes, generation, fail_next

    typ = req.get("type")
    if typ == "initialize":
        arena_path = str(req["arena_path"])
        arena_bytes = int(req["arena_bytes"])
        generation = int(req["generation"])
        with open(arena_path, "r+b", buffering=0) as f:
            f.truncate(arena_bytes)
        emit({"type": "worker_ready", "generation": generation})
        return True

    if typ == "alloc":
        if consume_failure("allocation_failed"):
            return True
        handle_id = int(req["handle_id"])
        dtype = str(req["dtype"])
        shape = str(req["shape"])
        extent = int(req["extent"])
        values, tensor = allocate_tensor(dtype, extent)
        objects[handle_id] = DeviceObject(dtype=dtype, shape=shape, extent=extent, values=values, tensor=tensor)
        emit({"type": "allocated", "handle_id": handle_id})
        return True

    if typ in ("host_to_device", "device_to_host"):
        if consume_failure("copy_failed"):
            return True
        copy = PendingCopy(
            kind=typ,
            copy_id=int(req["copy_id"]),
            handle_id=int(req["handle_id"]),
            host_offset=int(req["host_offset"]),
            device_offset=int(req["device_offset"]),
            length=int(req["len"]),
        )
        if copy.handle_id not in objects:
            backend_error("copy_failed")
            return True
        if bool(req.get("defer", False)):
            pending_copies[copy.copy_id] = copy
            emit({"type": "copy_started", "copy_id": copy.copy_id})
        else:
            perform_copy(copy)
            emit({"type": "copy_completed", "copy_id": copy.copy_id})
        return True

    if typ == "complete_copy":
        copy_id = int(req["copy_id"])
        copy = pending_copies.pop(copy_id, None)
        if copy is None:
            backend_error("copy_failed")
            return True
        perform_copy(copy)
        emit({"type": "copy_completed", "copy_id": copy_id})
        return True

    if typ == "wrap_for_tinygrad":
        if consume_failure("invalid_view"):
            return True
        handle_id = int(req["handle_id"])
        obj = objects.get(handle_id)
        if obj is None:
            backend_error("invalid_view")
            return True
        dtype = str(req["dtype"])
        shape = str(req["shape"])
        if obj.dtype != dtype or obj.shape != shape:
            backend_error("invalid_view")
            return True
        # Force materialization at view time so success depends on live tensor state.
        _ = obj.tensor.tolist()
        emit({"type": "view", "handle_id": handle_id, "dtype": dtype, "shape": shape})
        return True

    if typ == "free":
        handle_id = int(req["handle_id"])
        if handle_id not in objects:
            backend_error("invalid_view")
            return True
        del objects[handle_id]
        emit({"type": "freed", "handle_id": handle_id})
        return True

    if typ == "restart":
        generation = int(req["generation"])
        objects.clear()
        pending_copies.clear()
        fail_next = None
        emit({"type": "worker_ready", "generation": generation})
        return True

    if typ == "fail_next":
        fail_next = str(req["failure"])
        emit({"type": "ok"})
        return True

    if typ == "shutdown":
        emit({"type": "worker_stopped"})
        return False

    fatal("protocol_error", f"unknown command type {typ!r}")
    return False


def main() -> int:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            if not isinstance(req, dict):
                raise ValueError("request must be an object")
            if not handle(req):
                return 0
        except SystemExit:
            raise
        except Exception as exc:
            fatal("backend_exception", str(exc))
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
