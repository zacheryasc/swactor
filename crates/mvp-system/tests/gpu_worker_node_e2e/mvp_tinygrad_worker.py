#!/usr/bin/env python3
from __future__ import annotations

import json
import mmap
import os
import socket
import struct
import sys
import time
from typing import Any

from tinygrad import Tensor, dtypes

HEADER_LEN = 48
GENERATION = 1

arena: mmap.mmap | None = None
telemetry: socket.socket | None = None
telemetry_path = os.environ["SWACTOR_WORKER_EVENT_SOCK"]
rings: dict[int, dict[str, Any]] = {}
objects: dict[int, dict[str, Any]] = {}
role_configured = False
next_handle = 42


def control(**event: Any) -> None:
    print(json.dumps(event, separators=(",", ":")), flush=True)


def observe(kind: str, **fields: Any) -> None:
    global telemetry
    if telemetry is None:
        telemetry = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    event = {
        "schema": "mvp.worker.event.v1",
        "kind": kind,
        "node_id": int(os.environ["SWACTOR_NODE_ID"]),
        "run_id": int(os.environ["SWACTOR_RUN_ID"]),
        "stage_index": int(os.environ["SWACTOR_STAGE_INDEX"]),
        "worker_generation": GENERATION,
        "ts_ns": time.time_ns(),
    }
    event.update(fields)
    telemetry.sendto(json.dumps(event, separators=(",", ":")).encode(), telemetry_path)


def fatal(reason: str, **fields: Any) -> None:
    observe("worker_fatal", reason=reason, **fields)
    control(type="WorkerFatal", reason=reason, **fields)
    raise SystemExit(1)


def require_arena() -> mmap.mmap:
    if arena is None:
        fatal("ArenaNotMapped")
    return arena


def initialize(cmd: dict[str, Any]) -> None:
    global arena
    if int(cmd["helper_abi_version"]) != 1:
        fatal("UnsupportedHelperAbi", helper_abi_version=cmd["helper_abi_version"])
    fd = int(os.environ["SWACTOR_ARENA_FD"])
    size = int(os.environ["SWACTOR_ARENA_BYTES"])
    arena = mmap.mmap(fd, size)
    Tensor([1], dtype=dtypes.int32).realize()
    observe("backend_initialized")
    observe("worker_ready")
    control(type="WorkerReady", generation=GENERATION)


def install_ring(cmd: dict[str, Any]) -> None:
    ring_id = int(cmd["ring_id"])
    rings[ring_id] = {
        "ring_id": ring_id,
        "edge_id": int(cmd["edge_id"]),
        "port_id": cmd["port_id"],
        "direction": cmd["direction"],
        "base": int(cmd["base"]),
        "bytes": int(cmd["bytes"]),
        "max_extent": int(cmd["object_spec"]["max_extent"]),
        "alignment": int(cmd["object_spec"]["alignment"]),
    }
    observe("ring_installed", ring_id=ring_id, edge_id=rings[ring_id]["edge_id"], direction=rings[ring_id]["direction"])
    control(type="RingInstalled", ring_id=ring_id)


def configure_role(cmd: dict[str, Any]) -> None:
    global role_configured
    role_configured = True
    observe("role_loaded", role_id=int(cmd["role_id"]))
    control(type="RoleLoaded", role_id=int(cmd["role_id"]))


def parse_record(base: int, committed_bytes: int, ring: dict[str, Any]) -> tuple[int, int, int, bytes]:
    view = require_arena()
    if committed_bytes < HEADER_LEN:
        fatal("MalformedHeaderLength", ring_id=ring["ring_id"])
    header = view[base : base + HEADER_LEN]
    if header[0:4] != b"MO01" or header[4] != 1 or header[5] != HEADER_LEN:
        fatal("InvalidObjectHeader", ring_id=ring["ring_id"])
    object_id = struct.unpack_from("<Q", header, 8)[0]
    sequence = struct.unpack_from("<Q", header, 16)[0]
    extent = struct.unpack_from("<Q", header, 24)[0]
    encoded_max = struct.unpack_from("<Q", header, 32)[0]
    encoded_alignment = struct.unpack_from("<Q", header, 40)[0]
    if encoded_max != ring["max_extent"] or encoded_alignment != ring["alignment"]:
        fatal("ObjectSpecMismatch", ring_id=ring["ring_id"], object_id=object_id)
    if extent > ring["max_extent"] or (ring["alignment"] and extent % ring["alignment"]):
        fatal("ObjectExtentInvalid", ring_id=ring["ring_id"], object_id=object_id, extent=extent)
    total = HEADER_LEN + extent
    if committed_bytes < total:
        fatal("EofBeforeFullPayload", ring_id=ring["ring_id"], object_id=object_id)
    payload = bytes(view[base + HEADER_LEN : base + total])
    return object_id, sequence, extent, payload


def ring_readable(cmd: dict[str, Any]) -> None:
    global next_handle
    ring_id = int(cmd["ring_id"])
    ring = rings[ring_id]
    if ring["direction"] != "ingress":
        fatal("WrongRingDirection", ring_id=ring_id)
    object_id, sequence, extent, payload = parse_record(ring["base"], int(cmd["committed_bytes"]), ring)
    observe("object_copy_started", ring_id=ring_id, edge_id=ring["edge_id"], object_id=object_id, sequence=sequence, extent=extent)
    values = list(struct.unpack(f"<{extent // 4}i", payload))
    tensor = Tensor(values, dtype=dtypes.int32).realize()
    handle = next_handle
    next_handle += 1
    device_sum = int(tensor.sum().item())
    objects[handle] = {"object_id": object_id, "sequence": sequence, "tensor": tensor, "extent": extent}
    observe("object_loaded", ring_id=ring_id, edge_id=ring["edge_id"], object_id=object_id, sequence=sequence, extent=extent, handle=handle, device_sum=device_sum)
    control(type="ObjectLoaded", ring_id=ring_id, edge_id=ring["edge_id"], object_id=object_id, sequence=sequence, extent=extent, handle={"generation": GENERATION, "id": handle})


def write_record(ring: dict[str, Any], object_id: int, sequence: int, words: list[int]) -> int:
    payload = b"".join(struct.pack("<i", word) for word in words)
    extent = len(payload)
    if extent > ring["max_extent"]:
        fatal("OutputExtentInvalid", ring_id=ring["ring_id"], extent=extent)
    header = bytearray(HEADER_LEN)
    header[0:4] = b"MO01"
    header[4] = 1
    header[5] = HEADER_LEN
    struct.pack_into("<Q", header, 8, object_id)
    struct.pack_into("<Q", header, 16, sequence)
    struct.pack_into("<Q", header, 24, extent)
    struct.pack_into("<Q", header, 32, ring["max_extent"])
    struct.pack_into("<Q", header, 40, ring["alignment"])
    view = require_arena()
    base = ring["base"]
    view[base : base + HEADER_LEN] = header
    view[base + HEADER_LEN : base + HEADER_LEN + extent] = payload
    return HEADER_LEN + extent


def execute_step(cmd: dict[str, Any]) -> None:
    if not role_configured:
        fatal("RoleNotConfigured")
    handle = int(cmd["input_handle"])
    step_id = int(cmd["step_id"])
    output_object_id = int(cmd["output_object_id"])
    egress_ring_id = int(cmd["egress_ring_id"])
    obj = objects[handle]
    observe("execute_step_started", step_id=step_id, object_id=obj["object_id"], sequence=obj["sequence"], handle=handle)
    output = (obj["tensor"] * 2).realize()
    words = [int(value) for value in output.numpy().tolist()]
    ring = rings[egress_ring_id]
    committed = write_record(ring, output_object_id, int(obj["sequence"]), words)
    output_sum = sum(words)
    observe("object_produced", ring_id=egress_ring_id, edge_id=ring["edge_id"], object_id=output_object_id, sequence=obj["sequence"], extent=len(words) * 4, device_sum=output_sum, committed_bytes=committed)
    observe("step_completed", step_id=step_id)
    control(type="ObjectProduced", ring_id=egress_ring_id, object_id=output_object_id, sequence=obj["sequence"], committed_bytes=committed)
    control(type="StepCompleted", step_id=step_id)


def release_device_object(cmd: dict[str, Any]) -> None:
    handle = int(cmd["handle"])
    objects.pop(handle, None)
    observe("device_object_released", handle=handle)
    control(type="DeviceObjectReleased", handle=handle)


handlers = {
    "InitializeWorker": initialize,
    "InstallRing": install_ring,
    "ConfigureRole": configure_role,
    "RingReadable": ring_readable,
    "ExecuteStep": execute_step,
    "ReleaseDeviceObject": release_device_object,
}

for raw in sys.stdin:
    if not raw.strip():
        continue
    command = json.loads(raw)
    if command["type"] == "ShutdownWorker":
        observe("worker_stopped")
        control(type="WorkerStopped", generation=GENERATION)
        break
    handlers[command["type"]](command)
