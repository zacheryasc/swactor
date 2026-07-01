#!/usr/bin/env python3
from __future__ import annotations

import json
import mmap
import os
import struct
import sys
from typing import Any

HEADER_LEN = 40
worker_generation = 0
arena: mmap.mmap | None = None
rings: dict[int, dict[str, Any]] = {}
objects: dict[int, dict[str, Any]] = {}
role: dict[str, Any] = {}
Tensor: Any = None
dtypes: Any = None
next_handle = 42


def control(**event: Any) -> None:
    print(json.dumps(event, separators=(",", ":")), flush=True)


def fatal(reason: str, **fields: Any) -> None:
    control(type="WorkerFatal", reason=reason, **fields)
    raise SystemExit(1)


def require_arena() -> mmap.mmap:
    if arena is None:
        fatal("ArenaNotMapped")
    return arena


def require_tinygrad() -> tuple[Any, Any]:
    if Tensor is None or dtypes is None:
        fatal("BackendNotInitialized")
    return Tensor, dtypes


def initialize(cmd: dict[str, Any]) -> None:
    global arena, Tensor, dtypes, worker_generation
    if int(cmd["required_ring_helper_abi"]) != 1:
        fatal("UnsupportedHelperAbi", required_ring_helper_abi=cmd["required_ring_helper_abi"])
    worker_generation = int(cmd["worker_generation"])
    fd = int(os.environ["SWACTOR_ARENA_FD"])
    size = int(cmd.get("arena_ceiling", os.environ["SWACTOR_ARENA_BYTES"]))
    arena = mmap.mmap(fd, size)
    os.environ.setdefault("DEV", cmd.get("backend", {}).get("device", "CPU"))
    from tinygrad import Tensor as TinyTensor, dtypes as tiny_dtypes

    Tensor = TinyTensor
    dtypes = tiny_dtypes
    Tensor([1], dtype=dtypes.int32).realize().numpy().tolist()
    control(
        type="WorkerReady",
        pid=os.getpid(),
        worker_generation=worker_generation,
        ring_helper_abi=1,
        backend={"device": os.environ.get("DEV", "CPU")},
    )


def install_ring(cmd: dict[str, Any]) -> None:
    ring_id = int(cmd["ring_id"])
    layout = cmd["layout"]
    spec = cmd["object_spec"]
    rings[ring_id] = {
        "ring_id": ring_id,
        "edge_id": int(cmd["edge_id"]),
        "port_id": cmd["port_id"],
        "direction": cmd["direction"],
        "data_offset": int(layout["data_offset"]),
        "data_capacity": int(layout["data_capacity"]),
        "max_extent": int(spec["max_extent"]),
        "alignment": int(spec["alignment"]),
        "next_sequence": 0,
    }
    control(type="RingInstalled", ring_id=ring_id, edge_id=rings[ring_id]["edge_id"], port_id=cmd["port_id"])


def configure_role(cmd: dict[str, Any]) -> None:
    config = cmd["config"]
    role.clear()
    role.update(
        role_id=int(cmd["role_id"]),
        run_id=int(config["run_id"]),
        stage_index=int(config["stage_index"]),
        layer_start=int(config["layer_start"]),
        layer_end_exclusive=int(config["layer_end_exclusive"]),
    )
    control(type="RoleConfigured", role_id=role["role_id"])


def load_weights(cmd: dict[str, Any]) -> None:
    if not role:
        fatal("RoleNotConfigured")
    role["model_id"] = cmd["model_id"]
    role["gguf_source"] = cmd["gguf_source"]
    role["tokenizer"] = cmd["tokenizer"]
    role["weight_layer_start"] = int(cmd["layer_start"])
    role["weight_layer_end_exclusive"] = int(cmd["layer_end_exclusive"])
    control(
        type="WeightsLoaded",
        model_id=role["model_id"],
        layer_start=role["weight_layer_start"],
        layer_end_exclusive=role["weight_layer_end_exclusive"],
    )


def parse_record(ring: dict[str, Any]) -> tuple[int, int, int, bytes]:
    view = require_arena()
    base = ring["data_offset"]
    header = view[base : base + HEADER_LEN]
    version = struct.unpack_from("<H", header, 4)[0]
    header_len = struct.unpack_from("<H", header, 6)[0]
    if header[0:4] != b"MO01" or version != 1 or header_len != HEADER_LEN:
        fatal("InvalidObjectHeader", ring_id=ring["ring_id"])
    object_id = struct.unpack_from("<Q", header, 8)[0]
    sequence = struct.unpack_from("<Q", header, 16)[0]
    extent = struct.unpack_from("<Q", header, 24)[0]
    flags = struct.unpack_from("<I", header, 32)[0]
    reserved = struct.unpack_from("<I", header, 36)[0]
    del flags, reserved
    if extent > ring["max_extent"] or (ring["alignment"] and extent % ring["alignment"]):
        fatal("ObjectExtentInvalid", ring_id=ring["ring_id"], object_id=object_id, extent=extent)
    if sequence != ring["next_sequence"]:
        fatal("SequenceViolation", ring_id=ring["ring_id"], expected=ring["next_sequence"], actual=sequence)
    payload = bytes(view[base + HEADER_LEN : base + HEADER_LEN + extent])
    ring["next_sequence"] += 1
    return object_id, sequence, extent, payload


def ring_readable(cmd: dict[str, Any]) -> None:
    global next_handle
    tensor, dtype_mod = require_tinygrad()
    ring_id = int(cmd["ring_id"])
    ring = rings[ring_id]
    if ring["direction"] != "ingress":
        fatal("WrongRingDirection", ring_id=ring_id)
    object_id, sequence, extent, payload = parse_record(ring)
    values = list(struct.unpack(f"<{extent // 4}i", payload))
    loaded = tensor(values, dtype=dtype_mod.int32).realize()
    handle = next_handle
    next_handle += 1
    objects[handle] = {
        "object_id": object_id,
        "sequence": sequence,
        "tensor": loaded,
        "extent": extent,
    }
    control(
        type="ObjectLoaded",
        ring_id=ring_id,
        edge_id=ring["edge_id"],
        port_id=ring["port_id"],
        object_id=object_id,
        sequence=sequence,
        extent=extent,
        device_handle={"worker_generation": worker_generation, "id": handle},
    )


def write_record(ring: dict[str, Any], object_id: int, sequence: int, words: list[int], flags: int) -> int:
    payload = b"".join(struct.pack("<i", word) for word in words)
    extent = len(payload)
    if extent > ring["max_extent"]:
        fatal("OutputExtentInvalid", ring_id=ring["ring_id"], extent=extent)
    header = bytearray(HEADER_LEN)
    header[0:4] = b"MO01"
    struct.pack_into("<H", header, 4, 1)
    struct.pack_into("<H", header, 6, HEADER_LEN)
    struct.pack_into("<Q", header, 8, object_id)
    struct.pack_into("<Q", header, 16, sequence)
    struct.pack_into("<Q", header, 24, extent)
    struct.pack_into("<I", header, 32, flags)
    struct.pack_into("<I", header, 36, 0)
    view = require_arena()
    base = ring["data_offset"]
    view[base : base + HEADER_LEN] = header
    view[base + HEADER_LEN : base + HEADER_LEN + extent] = payload
    return HEADER_LEN + extent


def execute_step(cmd: dict[str, Any]) -> None:
    if not role:
        fatal("RoleNotConfigured")
    tensor, _ = require_tinygrad()
    del tensor
    role_id = int(cmd["role_id"])
    if role_id != role["role_id"]:
        fatal("RoleMismatch", expected=role["role_id"], actual=role_id)
    step_id = int(cmd["step_id"])
    input_binding = cmd["inputs"][0]
    output_binding = cmd["outputs"][0]
    device_handle = input_binding["device_handle"]
    if int(device_handle["worker_generation"]) != worker_generation:
        fatal("OldGenerationHandle", handle=device_handle)
    handle = int(device_handle["id"])
    obj = objects[handle]
    if int(input_binding["object_id"]) != obj["object_id"] or int(input_binding["sequence"]) != obj["sequence"]:
        fatal("InputBindingMismatch", step_id=step_id)
    transformed = int(obj["tensor"].sum().item()) + role["layer_start"] + role["layer_end_exclusive"] + role["stage_index"]
    if bool(cmd.get("runtime", {}).get("final_stage")):
        words = [6 if transformed % 2 == 1 else 8]
    else:
        words = [transformed if transformed > 0 else 1]
    ring = rings[int(output_binding["ring_id"])]
    if ring["direction"] != "egress":
        fatal("WrongRingDirection", ring_id=ring["ring_id"])
    committed = write_record(
        ring,
        int(output_binding["object_id"]),
        int(output_binding["sequence"]),
        words,
        int(output_binding.get("flags", 0)),
    )
    control(
        type="ObjectProduced",
        ring_id=ring["ring_id"],
        edge_id=ring["edge_id"],
        port_id=ring["port_id"],
        object_id=int(output_binding["object_id"]),
        sequence=int(output_binding["sequence"]),
        committed_bytes=committed,
    )
    if bool(cmd.get("release_inputs_after")):
        objects.pop(handle, None)
    control(type="StepCompleted", role_id=role_id, step_id=step_id)


def release_device_object(cmd: dict[str, Any]) -> None:
    handle = int(cmd["device_handle"]["id"])
    objects.pop(handle, None)
    control(type="DeviceObjectReleased", device_handle=cmd["device_handle"])


def uninstall_ring(cmd: dict[str, Any]) -> None:
    ring_id = int(cmd["ring_id"])
    rings.pop(ring_id, None)
    control(type="RingQuiesced", ring_id=ring_id)


def shutdown_worker(_: dict[str, Any]) -> None:
    control(type="WorkerStopped", reason="Graceful")
    raise SystemExit(0)


HANDLERS = {
    "InitializeWorker": initialize,
    "InstallRing": install_ring,
    "ConfigureRole": configure_role,
    "LoadWeights": load_weights,
    "RingReadable": ring_readable,
    "ExecuteStep": execute_step,
    "ReleaseDeviceObject": release_device_object,
    "UninstallRing": uninstall_ring,
    "ShutdownWorker": shutdown_worker,
}

for raw in sys.stdin:
    if not raw.strip():
        continue
    try:
        command = json.loads(raw)
    except json.JSONDecodeError as exc:
        fatal("InvalidJson", error=str(exc))
    handler = HANDLERS.get(command.get("type"))
    if handler is None:
        fatal("UnknownCommand", command=command.get("type"))
    handler(command)
