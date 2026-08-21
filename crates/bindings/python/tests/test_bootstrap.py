"""Actor attachment and zero-copy job data-plane guarantees."""

from __future__ import annotations

import ctypes
import json
import os
import struct
import subprocess
import importlib
import socket
import sys
import tempfile
import threading
from pathlib import Path

import pytest

import swactor

ENV_ARENA = "SWACTOR_ARENA_FD"
_native = importlib.import_module("swactor.swactor")
ENV_ACTOR = "SWACTOR_DATA_PLANE_ACTOR"
ENV_CAPABILITY = "SWACTOR_JOB_CAPABILITY"
ENV_ENDPOINT = "SWACTOR_DATA_PLANE_ENDPOINT"
BOOTSTRAP_ENV = (ENV_ARENA, ENV_ACTOR, ENV_CAPABILITY, ENV_ENDPOINT)
OLD_WAKE_ENV = ("SWACTOR_WAKE_FD", "SWACTOR_HOST_WAKE_FD")
MAGIC = int.from_bytes(b"SWBS", "little")
VERSION = 2
HEADER_LEN = 64
ARENA_SIZE = 4096
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


def install_host_env(monkeypatch, host) -> int:
    env = dict(host.env())
    inherited = os.dup(host.arena_fd())
    env[ENV_ARENA] = str(inherited)
    for name in BOOTSTRAP_ENV:
        monkeypatch.setenv(name, env[name])
    for name in OLD_WAKE_ENV:
        monkeypatch.delenv(name, raising=False)
    return inherited


def pack_header(
    *,
    magic: int = MAGIC,
    version: int = VERSION,
    arena_size: int = ARENA_SIZE,
    generation: int = 1,
    control_offset: int = 0,
    control_length: int = 0,
    reserved: tuple[int, int, int] = (0, 0, 0),
) -> bytes:
    return struct.pack(
        "<IHH7Q",
        magic,
        version,
        0,
        arena_size,
        generation,
        control_offset,
        control_length,
        *reserved,
    )


def make_arena(header: bytes, size: int = ARENA_SIZE) -> int:
    fd, path = tempfile.mkstemp(prefix="python-bootstrap-test-")
    os.unlink(path)
    os.ftruncate(fd, size)
    os.pwrite(fd, header, 0)
    return fd


def set_arena(monkeypatch, fd: int) -> None:
    monkeypatch.setenv(ENV_ARENA, str(fd))


def mapping_for_address(address: int) -> str:
    for line in Path("/proc/self/maps").read_text().splitlines():
        extent = line.split(maxsplit=1)[0]
        start, end = (int(value, 16) for value in extent.split("-"))
        if start <= address < end:
            return line
    raise AssertionError(f"no process mapping contains {address:#x}")


def test_run_attaches_before_main_and_maps_blob_buffer_directly(monkeypatch, host):
    inherited = install_host_env(monkeypatch, host)
    observed = {}

    async def main(ctx):
        assert isinstance(ctx, swactor.Context)
        assert isinstance(ctx.data, swactor.DataPlane)
        blob = await ctx.data.read_blob("/models/tiny-linear/weights")
        assert isinstance(blob, swactor.Blob)
        assert blob.length == 24
        assert len(blob.digest) == 64
        with blob.map() as mapped:
            assert isinstance(mapped, swactor.ArenaView)
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
        assert {"read_blob", "write_blob", "read_stream", "write_stream"} <= public
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


def test_write_blob_seals_cleanly_and_exception_aborts(monkeypatch, host):
    install_host_env(monkeypatch, host)

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

    swactor.run(main)
    assert bytes(host.published("/runs/self/results/complete")) == b"result"
    assert host.published("/runs/self/results/aborted") is None


def test_missing_path_and_authorization_are_typed(monkeypatch, host):
    install_host_env(monkeypatch, host)

    async def main(ctx):
        with pytest.raises(swactor.DataPathError, match="not found"):
            await ctx.data.read_blob("/models/missing")
        with pytest.raises(PermissionError):
            await ctx.data.read_blob("/runs/self/results/private")

    swactor.run(main)


def test_temporary_output_stream_bridge_remains_available(monkeypatch, host, tmp_path):
    install_host_env(monkeypatch, host)
    socket_path = tmp_path / "output.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    monkeypatch.setenv("SWACTOR_DATA_PLANE_OUTPUT", str(socket_path))
    received = []

    def receive():
        connection, _ = listener.accept()
        with connection:
            chunks = []
            while chunk := connection.recv(4096):
                chunks.append(chunk)
        received.append(b"".join(chunks))

    receiver = threading.Thread(target=receive)
    receiver.start()

    async def main(ctx):
        async with ctx.data.write_stream(
            "/runs/self/results/predictions"
        ) as stream:
            await stream.write(b"temporary-result")

    try:
        swactor.run(main)
        receiver.join(timeout=2)
        assert not receiver.is_alive()
        assert received == [b"temporary-result"]
    finally:
        listener.close()


def test_invalid_capability_prevents_main(monkeypatch, host):
    install_host_env(monkeypatch, host)
    monkeypatch.setenv(ENV_CAPABILITY, "00" * 32)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    with pytest.raises(swactor.SessionError, match="CapabilityRejected"):
        swactor.run(main)
    assert calls == []


@pytest.mark.parametrize(
    ("header", "match"),
    [
        (pack_header(magic=MAGIC ^ 0xFF), "magic"),
        (pack_header(version=VERSION + 1), "version"),
        (pack_header(generation=0), "generation"),
        (pack_header(arena_size=ARENA_SIZE + 1), "backing length"),
        (pack_header(control_offset=64, control_length=0), "both offset and length"),
        (pack_header(reserved=(0, 1, 0)), "reserved"),
    ],
)
def test_malformed_arena_fails_before_route_or_main(monkeypatch, host, header, match):
    env = dict(host.env())
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    fd = make_arena(header)
    set_arena(monkeypatch, fd)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    try:
        with pytest.raises(swactor.BootstrapError, match=match):
            swactor.run(main)
        assert calls == []
        with pytest.raises(OSError):
            os.fstat(fd)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_missing_bootstrap_metadata_prevents_main(monkeypatch):
    for name in (*BOOTSTRAP_ENV, *OLD_WAKE_ENV):
        monkeypatch.delenv(name, raising=False)
    calls = []

    async def main(_ctx):
        calls.append("ran")

    with pytest.raises(swactor.BootstrapError, match=ENV_ARENA):
        swactor.run(main)
    assert calls == []


def test_handoff_uses_one_descriptor_and_no_wake_names(host):
    env = dict(host.env())
    assert set(env) == set(BOOTSTRAP_ENV)
    assert all(name not in env for name in OLD_WAKE_ENV)
    assert env[ENV_ARENA] == str(host.arena_fd())


def test_real_exec_attachment_and_blob_mapping(host):
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in (*BOOTSTRAP_ENV, *OLD_WAKE_ENV)
    }
    env.update(dict(host.env()))
    result = subprocess.run(
        [sys.executable, str(PROBE)],
        env=env,
        pass_fds=(host.arena_fd(),),
        text=True,
        capture_output=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    facts = dict(
        item.split("=", 1) for item in result.stdout.strip().split() if "=" in item
    )
    assert facts == {
        "HAS_DATA": "1",
        "LENGTH": "24",
        "VALUES": "1.5,-2,0.5,4,0.25,-0.75",
        "ARENA_OPEN": "0",
        "IDENTITY_LEAKS": "0",
    }

@pytest.mark.skipif(
    not Path("/dev/nvidia0").exists(),
    reason="CUDA device is unavailable",
)
def test_real_exec_tinygrad_cuda_scenario(host, tmp_path):
    socket_path = tmp_path / "cuda-output.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    listener.listen(1)
    received = []

    def receive():
        connection, _ = listener.accept()
        with connection:
            chunks = []
            while chunk := connection.recv(4096):
                chunks.append(chunk)
        received.append(b"".join(chunks))

    receiver = threading.Thread(target=receive)
    receiver.start()
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in (*BOOTSTRAP_ENV, *OLD_WAKE_ENV)
    }
    env.update(dict(host.env()))
    env.update(
        {
            "CUDA_PTX": "1",
            "DEV": "CUDA",
            "SWACTOR_DATA_PLANE_OUTPUT": str(socket_path),
        }
    )
    script = (
        Path(__file__).resolve().parents[4]
        / "apps"
        / "myelin"
        / "jobs"
        / "tiny_linear_inference.py"
    )
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            env=env,
            pass_fds=(host.arena_fd(),),
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
        receiver.join(timeout=2)
        assert result.returncode == 0, result.stderr
        assert not receiver.is_alive()
        payload = json.loads(received[0])
        assert payload["device"].startswith("CUDA")
        assert payload["output"] == pytest.approx([2.75, -8.75])
    finally:
        listener.close()
