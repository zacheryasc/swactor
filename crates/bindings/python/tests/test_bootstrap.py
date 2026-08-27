"""Context bootstrap and zero-copy data-plane guarantees."""

from __future__ import annotations

import asyncio
import ctypes
import errno
import importlib
import json
import os
import struct
import subprocess
import sys
from pathlib import Path

import pytest

import swactor

_native = importlib.import_module("swactor.swactor")
BOOTSTRAP_FD = 198
FORBIDDEN_BOOTSTRAP_ENV = (
    "SWACTOR_ARENA_FD",
    "SWACTOR_DATA_PLANE_ACTOR",
    "SWACTOR_JOB_CAPABILITY",
    "SWACTOR_DATA_PLANE_ENDPOINT",
)
PROBE = Path(__file__).parent / "probe.py"


class PyBuffer(ctypes.Structure):
    _fields_ = [
        ("buf", ctypes.c_void_p),
        ("obj", ctypes.c_void_p),
        ("len", ctypes.c_ssize_t),
        ("itemsize", ctypes.c_ssize_t),
        ("readonly", ctypes.c_int),
        ("ndim", ctypes.c_int),
        ("format", ctypes.c_char_p),
        ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
        ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
        ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
        ("internal", ctypes.c_void_p),
    ]


_PY_GET_BUFFER = ctypes.pythonapi.PyObject_GetBuffer
_PY_GET_BUFFER.argtypes = [ctypes.py_object, ctypes.POINTER(PyBuffer), ctypes.c_int]
_PY_GET_BUFFER.restype = ctypes.c_int
_PY_RELEASE_BUFFER = ctypes.pythonapi.PyBuffer_Release
_PY_RELEASE_BUFFER.argtypes = [ctypes.POINTER(PyBuffer)]
_PY_RELEASE_BUFFER.restype = None


@pytest.fixture
def host():
    return _native._test_data_plane_host()


def install_bootstrap(host, *, invalid_capability=False, corrupt_arena=False) -> int:
    fd = host.install_bootstrap(invalid_capability, corrupt_arena)
    assert fd == BOOTSTRAP_FD
    return fd


def mapping_for_address(address: int) -> str:
    for line in Path("/proc/self/maps").read_text().splitlines():
        extent = line.split(maxsplit=1)[0]
        start, end = (int(value, 16) for value in extent.split("-"))
        if start <= address < end:
            return line
    raise AssertionError(f"no process mapping contains {address:#x}")


def test_run_attaches_before_main_and_maps_blob_buffer_directly(host):
    inherited = install_bootstrap(host)
    observed = {}

    async def main(ctx):
        assert isinstance(ctx, swactor.Context)
        assert isinstance(ctx.data, swactor.DataPlane)
        blob = await ctx.data.read_blob("/models/tiny-linear/weights")
        assert isinstance(blob, swactor.Blob)
        assert blob.length == 24
        assert blob.digest is None
        with blob.map() as mapped:
            assert isinstance(mapped, swactor.BlobView)
            assert struct.unpack_from("<6f", mapped) == pytest.approx(
                (1.5, -2.0, 0.5, 4.0, 0.25, -0.75)
            )
            view = memoryview(mapped)
            assert view.readonly
            assert view.nbytes == 24
            view.release()

            with pytest.raises(BufferError, match="read-only"):
                _PY_GET_BUFFER(mapped, ctypes.byref(PyBuffer()), 1)

            exported = PyBuffer()
            assert _PY_GET_BUFFER(mapped, ctypes.byref(exported), 0) == 0
            try:
                mapping = mapping_for_address(exported.buf)
                assert "data-plane-arena" in mapping
                assert exported.readonly == 1
                assert exported.len == 24
            finally:
                _PY_RELEASE_BUFFER(ctypes.byref(exported))

        public = {name for name in dir(ctx.data) if not name.startswith("_")}
        assert {
            "open",
            "lookup",
            "unlink",
            "rename",
            "read_blob",
            "write_blob",
            "read_stream",
            "write_stream",
        } <= public
        assert isinstance(swactor.O_RDONLY, int)
        assert not public & {
            "actor",
            "actor_id",
            "edge",
            "edge_id",
            "offset",
            "peer",
            "ring",
            "socket",
        }
        observed["ran"] = True

    swactor.run(main)
    assert observed == {"ran": True}
    with pytest.raises(OSError):
        os.fstat(inherited)


def test_write_blob_seals_cleanly_and_exception_aborts(host):
    install_bootstrap(host)

    async def main(ctx):
        async with ctx.data.write_blob(
            "/runs/self/results/complete", length=6
        ) as blob:
            assert blob.length == 6
            with blob.map() as mapped:
                struct.pack_into("6s", mapped, 0, b"result")

        class AbortWrite(Exception):
            pass

        try:
            async with ctx.data.write_blob(
                "/runs/self/results/aborted", length=4
            ) as blob:
                with blob.map() as mapped:
                    struct.pack_into("4s", mapped, 0, b"nope")
                raise AbortWrite
        except AbortWrite:
            pass

        complete = await ctx.data.read_blob("/runs/self/results/complete")
        with complete.map() as mapped:
            assert bytes(mapped) == b"result"
        with pytest.raises(FileNotFoundError, match="data path not found"):
            await ctx.data.read_blob("/runs/self/results/aborted")

    swactor.run(main)

def test_namespace_lookup_rename_unlink_and_authorization_are_typed(host):
    install_bootstrap(host)

    async def main(ctx):
        model = await ctx.data.lookup("/models/tiny-linear/weights")
        assert isinstance(model, swactor.NamespaceEntry)
        assert model.kind == "blob"
        assert model.revision > 0
        assert model.active is False

        source = "/runs/self/results/namespace-source"
        destination = "/runs/self/results/namespace-destination"
        async with ctx.data.write_blob(source, length=4) as blob:
            with blob.map() as mapped:
                view = memoryview(mapped)
                view[:] = b"move"
                view.release()

        with pytest.raises(PermissionError):
            await ctx.data.rename(source, "/models/forbidden")
        assert (await ctx.data.lookup(source)).kind == "blob"

        revision = await ctx.data.rename(source, destination)
        renamed = await ctx.data.lookup(destination)
        assert renamed.kind == "blob"
        assert renamed.revision == revision
        with pytest.raises(FileNotFoundError):
            await ctx.data.lookup(source)

        unlink_revision = await ctx.data.unlink(destination)
        assert unlink_revision > revision
        with pytest.raises(FileNotFoundError):
            await ctx.data.lookup(destination)

    swactor.run(main)


def test_raw_descriptor_blob_io_mapping_and_errno(host):
    install_bootstrap(host)

    async def main(ctx):
        logical = "/runs/self/results/raw-python"
        writer = await ctx.data.open(
            logical,
            swactor.O_WRONLY | swactor.O_CREAT | swactor.O_TRUNC,
            length=8,
        )
        assert isinstance(writer, swactor.Descriptor)
        assert writer.kind == "blob"
        assert await writer.write(b"abc") == 3
        assert await writer.writefrom(b"defgh") == 5
        await writer.close()
        with pytest.raises(OSError) as closed:
            await writer.close()
        assert closed.value.errno == errno.EBADF

        reader = await ctx.data.open(logical, swactor.O_RDONLY)
        destination = bytearray(b"\xa5" * 10)
        assert await reader.readinto(memoryview(destination)[1:7]) == 6
        assert destination == b"\xa5abcdef\xa5\xa5\xa5"
        assert await reader.read(8) == b"gh"
        assert await reader.read(8) == b""
        with pytest.raises(OSError) as wrong_access:
            await reader.write(b"x")
        assert wrong_access.value.errno == errno.EBADF

        mapping = await reader.map(length=8)
        assert isinstance(mapping, swactor.DescriptorMapping)
        exported = memoryview(mapping)
        assert exported.readonly
        await reader.close()
        assert bytes(exported) == b"abcdefgh"
        with pytest.raises(BufferError, match="active buffer exports"):
            mapping.close()
        exported.release()
        mapping.close()

        with pytest.raises(FileNotFoundError):
            await ctx.data.open("/models/raw-missing", swactor.O_RDONLY)
        with pytest.raises(OSError) as unsupported:
            await ctx.data.open(logical, swactor.O_RDONLY | swactor.O_NONBLOCK)
        assert unsupported.value.errno in {errno.ENOTSUP, errno.EOPNOTSUPP}

    swactor.run(main)


def test_missing_path_and_authorization_are_typed(host):
    install_bootstrap(host)

    async def main(ctx):
        with pytest.raises(FileNotFoundError, match="data path not found"):
            await ctx.data.read_blob("/models/missing")
        with pytest.raises(PermissionError):
            await ctx.data.read_blob("/runs/self/private")

    swactor.run(main)


def test_native_stream_round_trip_has_ordered_eof(host):
    install_bootstrap(host)
    received = []
    raw_received = []

    async def main(ctx):
        async def receive():
            reader = await ctx.data.read_stream("/runs/self/results/predictions")
            buffer = bytearray(3)
            while (count := await reader.readinto(buffer)) != 0:
                received.append(bytes(buffer[:count]))

        receiver = asyncio.create_task(receive())
        async with ctx.data.write_stream("/runs/self/results/predictions") as stream:
            await stream.write(b"native-")
            await stream.write(b"result")
        await receiver

        raw_reader, raw_writer = await asyncio.gather(
            ctx.data.open("/runs/self/results/predictions", swactor.O_RDONLY),
            ctx.data.open("/runs/self/results/predictions", swactor.O_WRONLY),
        )
        assert await raw_writer.write(b"raw-stream") == len(b"raw-stream")
        await raw_writer.close()
        buffer = bytearray(4)
        while (count := await raw_reader.readinto(buffer)) != 0:
            raw_received.append(bytes(buffer[:count]))
        await raw_reader.close()
        before_replace = (await ctx.data.lookup("/runs/self/results/predictions")).revision

        async def replace_stream():
            async with ctx.data.write_stream(
                "/runs/self/results/predictions", replace=True
            ) as stream:
                await stream.write(b"replacement")

        replacing = asyncio.create_task(replace_stream())
        replacement_reader = await ctx.data.read_stream(
            "/runs/self/results/predictions"
        )
        replacement_chunks = []
        while (chunk := await replacement_reader.read()) is not None:
            replacement_chunks.append(bytes(chunk))
        await replacing
        assert b"".join(replacement_chunks) == b"replacement"
        after_replace = (await ctx.data.lookup("/runs/self/results/predictions")).revision
        assert after_replace > before_replace

    swactor.run(main)
    assert b"".join(received) == b"native-result"
    assert b"".join(raw_received) == b"raw-stream"


def test_invalid_capability_prevents_main(host):
    install_bootstrap(host, invalid_capability=True)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    with pytest.raises(swactor.SessionError, match="CapabilityRejected"):
        swactor.run(main)
    assert calls == []


def test_malformed_arena_fails_before_route_or_main(host):
    install_bootstrap(host, corrupt_arena=True)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    with pytest.raises(swactor.BootstrapError, match="magic"):
        swactor.run(main)
    assert calls == []


def test_missing_bootstrap_descriptor_prevents_main():
    try:
        os.close(BOOTSTRAP_FD)
    except OSError:
        pass
    calls = []

    async def main(_ctx):
        calls.append("ran")

    with pytest.raises(swactor.BootstrapError, match="descriptor 198"):
        swactor.run(main)
    assert calls == []


def test_bootstrap_uses_one_fixed_descriptor_and_no_environment_contract(host):
    inherited = install_bootstrap(host)
    os.fstat(inherited)
    assert all(name not in os.environ for name in FORBIDDEN_BOOTSTRAP_ENV)
    os.close(inherited)


def test_duplicate_run_cannot_reclaim_bootstrap(host):
    install_bootstrap(host)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    swactor.run(main)
    with pytest.raises(swactor.BootstrapError, match="descriptor 198"):
        swactor.run(main)
    assert calls == ["ran"]


def test_user_exception_remains_distinct_from_bootstrap_failure(host):
    install_bootstrap(host)

    async def main(_ctx):
        raise ValueError("user failure")

    with pytest.raises(ValueError, match="user failure"):
        swactor.run(main)


def clean_child_environment() -> dict[str, str]:
    return {
        key: value
        for key, value in os.environ.items()
        if key not in FORBIDDEN_BOOTSTRAP_ENV
    }


def test_real_exec_attachment_and_blob_mapping(host):
    inherited = install_bootstrap(host)
    try:
        result = subprocess.run(
            [sys.executable, str(PROBE)],
            env=clean_child_environment(),
            pass_fds=(inherited,),
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
    finally:
        try:
            os.close(inherited)
        except OSError:
            pass
    assert result.returncode == 0, result.stderr
    facts = dict(
        item.split("=", 1) for item in result.stdout.strip().split() if "=" in item
    )
    assert facts == {
        "HAS_DATA": "1",
        "LENGTH": "24",
        "VALUES": "1.5,-2,0.5,4,0.25,-0.75",
        "BOOTSTRAP_OPEN": "0",
        "IDENTITY_LEAKS": "0",
    }


def test_contextual_spawner_launches_real_python_with_usable_context(host):
    events = host.run_contextual_process(sys.executable, str(PROBE))
    started = next(
        index for index, event in enumerate(events) if event.startswith("started:")
    )
    ready = events.index("context_ready")
    exited = next(
        index for index, event in enumerate(events) if event.startswith("exited:")
    )
    assert started < ready < exited, events
    stdout = "".join(
        event.removeprefix("stdout:")
        for event in events
        if event.startswith("stdout:")
    )
    assert "HAS_DATA=1" in stdout
    assert "LENGTH=24" in stdout
    assert "BOOTSTRAP_OPEN=0" in stdout
    assert "IDENTITY_LEAKS=0" in stdout


@pytest.mark.skipif(
    not Path("/dev/nvidia0").exists(),
    reason="CUDA device is unavailable",
)
def test_real_exec_tinygrad_cuda_scenario(host):
    capture = host.capture_stream("/runs/self/results/inference")
    inherited = install_bootstrap(host)
    env = clean_child_environment()
    env.update({"CUDA_PTX": "1", "DEV": "CUDA"})
    script = (
        Path(__file__).resolve().parents[4]
        / "apps"
        / "myelin"
        / "testdata"
        / "tiny_linear_inference.py"
    )
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            env=env,
            pass_fds=(inherited,),
            text=True,
            capture_output=True,
            check=False,
        )
    finally:
        try:
            os.close(inherited)
        except OSError:
            pass
    assert result.returncode == 0, result.stderr
    payload = json.loads(capture.result())
    assert payload["device"].startswith("CUDA")
    assert payload["output"] == pytest.approx([2.75, -8.75])
