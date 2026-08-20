"""Bootstrap-slice behavior guarantees for the ``swactor`` Python binding.

Defends the agreed invariants:
- B1  deterministic handoff: a valid exec-time bootstrap reaches ``main``
- B2  fail-fast purity: every bootstrap defect raises ``BootstrapError``
      before ``main`` is invoked
- B3  version gate rejects, never guesses
- B4  arbitrary header bytes never crash the process
- B5  grandchild isolation: post-bootstrap children inherit no bootstrap fds
- B6  wake opacity: bootstrap does not consume the wake descriptor
- B7  exit-code contract across a real process boundary
- B8  the two environment names are the whole handoff ABI
- B9  no identity leakage on the Python-visible surface
"""

from __future__ import annotations

import os
import random
import select
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import swactor

MAGIC = int.from_bytes(b"SWBS", "little")
ENV_ARENA = "SWACTOR_ARENA_FD"
ENV_WAKE = "SWACTOR_WAKE_FD"
VERSION = 1
ARENA_SIZE = 1 << 20

MAIN_RAN = []


# ─── fixtures ────────────────────────────────────────────────────────────────

def pack_header(
    arena_size=ARENA_SIZE,
    ring_offset=4096,
    ring_capacity=8192,
    ring_generation=1,
    *,
    magic=MAGIC,
    version=VERSION,
    reserved0=0,
    reserved1=0,
):
    return struct.pack(
        "<IHHQQQQ",
        magic,
        version,
        reserved0,
        arena_size,
        ring_offset,
        ring_capacity,
        ring_generation,
    ) + struct.pack("<Q", reserved1)


def _create_backing() -> int:
    if hasattr(os, "memfd_create"):
        return os.memfd_create("test-arena", 0)
    # Not every interpreter exposes memfd_create; an unlinked temp file is an
    # equally anonymous regular-file inode for ftruncate/pwrite/mmap.
    fd, path = tempfile.mkstemp(prefix="swactor-test-arena-")
    os.unlink(path)
    return fd


def make_arena(header: bytes, size: int = ARENA_SIZE) -> int:
    fd = _create_backing()
    os.ftruncate(fd, size)
    os.pwrite(fd, header, 0)
    return fd


def make_wake(signaled: bool) -> tuple[int, int]:
    read_fd, write_fd = os.pipe()
    if signaled:
        os.write(write_fd, b"x")
    # Keep the write end open: closing it would signal EOF and make the read
    # end "readable" without a wake ever being delivered.
    return read_fd, write_fd


@pytest.fixture
def valid_env(monkeypatch):
    """A complete, valid bootstrap in the current process."""
    keep_open = []

    def install(*, header=None, arena_size=ARENA_SIZE, signaled=True):
        arena_fd = make_arena(
            header if header is not None else pack_header(arena_size=arena_size),
            arena_size,
        )
        wake_fd, write_fd = make_wake(signaled)
        keep_open.append(write_fd)
        # Make the wake fd inheritable so only run()'s CLOEXEC can hide it
        # from children (B5 evidence).
        os.set_inheritable(wake_fd, True)
        monkeypatch.setenv(ENV_ARENA, str(arena_fd))
        monkeypatch.setenv(ENV_WAKE, str(wake_fd))
        return arena_fd, wake_fd

    yield install
    for write_fd in keep_open:
        os.close(write_fd)

# ─── B1 / B9: valid bootstrap reaches main with a clean surface ─────────────

def test_run_constructs_context_and_data(valid_env):
    valid_env()
    seen = {}

    async def main(ctx):
        seen["ctx"] = ctx

    swactor.run(main)
    assert isinstance(seen["ctx"], swactor.Context)
    assert isinstance(seen["ctx"].data, swactor.DataPlane)
    # B9: no identities leak onto the surface.
    assert {a for a in dir(seen["ctx"]) if not a.startswith("_")} == {"data"}
    leaked = {"arena", "resolved", "control_ring", "offset", "generation"}
    assert not leaked & {a for a in dir(seen["ctx"].data) if not a.startswith("_")}

def test_run_passes_the_same_context_instance(valid_env):
    valid_env()
    seen = {}

    async def main(ctx):
        seen["one"] = ctx
        seen["data"] = ctx.data

    swactor.run(main)
    assert seen["one"].data is seen["data"]


# ─── B5: fd hygiene ──────────────────────────────────────────────────────────

def test_arena_fd_closed_after_run(valid_env):
    arena_fd, _ = valid_env()

    async def main(ctx):
        pass

    swactor.run(main)
    with pytest.raises(OSError):
        os.fstat(arena_fd)


# ─── B6: wake opacity ────────────────────────────────────────────────────────


@pytest.mark.parametrize("signaled,expected", [(True, 1), (False, 0)])
def test_bootstrap_does_not_consume_wake(valid_env, signaled, expected):
    valid_env(signaled=signaled)
    readable = {}

    async def main(ctx):
        wake = int(os.environ[ENV_WAKE])
        readable["value"] = int(bool(select.select([wake], [], [], 0)[0]))

    swactor.run(main)
    assert readable["value"] == expected



def test_wake_fd_cloexec_armed_by_run(valid_env):
    _, wake_fd = valid_env()

    async def main(ctx):
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import os, sys; "
                "sys.exit(0 if sys.argv[1] not in os.listdir('/proc/self/fd') else 3)",
                str(wake_fd),
            ],
            close_fds=False,
        )
        child.wait()
        assert child.returncode != 3, "close_fds=False child saw the wake fd"

    swactor.run(main)


# ─── B2 / B3 / B4: defect table, in process ─────────────────────────────────

def _expect_bootstrap_failure(monkeypatch, header=None, *, env=None, arena_size=ARENA_SIZE, match=""):
    calls = []

    async def main(ctx):  # pragma: no cover - must never run
        calls.append(ctx)

    if env is not None:
        monkeypatch.setenv(ENV_ARENA, str(env[0]))
        monkeypatch.setenv(ENV_WAKE, str(env[1]))
    else:
        arena_fd = make_arena(header if header is not None else pack_header(arena_size=arena_size), arena_size)
        wake_fd, _keep_wake = make_wake(True)
        monkeypatch.setenv(ENV_ARENA, str(arena_fd))
        monkeypatch.setenv(ENV_WAKE, str(wake_fd))

    with pytest.raises(swactor.BootstrapError, match=match) as excinfo:
        swactor.run(main)
    assert isinstance(excinfo.value, swactor.SwactorError)
    assert calls == [], "main must never run on a bootstrap defect"
def test_missing_env_rejected(monkeypatch):
    monkeypatch.delenv(ENV_ARENA, raising=False)
    monkeypatch.delenv(ENV_WAKE, raising=False)
    calls = []

    async def main(ctx):  # pragma: no cover - must never run
        calls.append(ctx)

    with pytest.raises(swactor.BootstrapError, match="is not set"):
        swactor.run(main)
    assert calls == []


def test_non_numeric_fd_rejected(monkeypatch):
    _expect_bootstrap_failure(
        monkeypatch, env=("not-a-number", "also-not"), match="not a descriptor number"
    )



def test_negative_fd_rejected(monkeypatch):
    _expect_bootstrap_failure(monkeypatch, env=("-1", "-1"), match="non-negative")


def test_closed_fd_rejected(monkeypatch):
    closed = os.pipe()[0]
    os.close(closed)
    _expect_bootstrap_failure(monkeypatch, env=(closed, 1), match="stat arena descriptor")


def test_short_backing_rejected(monkeypatch):
    _expect_bootstrap_failure(
        monkeypatch,
        header=b"\0" * 16,
        arena_size=16,
        match="shorter than the 48-byte bootstrap header",
    )


@pytest.mark.parametrize(
    "header,match",
    [
        (pack_header(magic=MAGIC ^ 0xFF), "magic mismatch"),
        (pack_header(version=0), "unsupported bootstrap version 0"),
        (pack_header(version=2), "unsupported bootstrap version 2"),
        (pack_header(reserved0=1), "reserved bytes at offset 6"),
        (pack_header(reserved1=1), "reserved bytes at offset 40"),
        (
            pack_header(arena_size=ARENA_SIZE, ring_offset=16),
            "overlaps the bootstrap header",
        ),
        (
            pack_header(ring_offset=ARENA_SIZE, ring_capacity=1),
            "exceeds arena size",
        ),
        (
            pack_header(ring_offset=2**64 - 8, ring_capacity=16),
            "exceeds arena size",
        ),
        (pack_header(ring_capacity=0), "capacity is zero"),
        (pack_header(ring_generation=0), "generation is zero"),
    ],
)
def test_header_defects_rejected(monkeypatch, header, match):
    _expect_bootstrap_failure(monkeypatch, header, match=match)


def test_lying_arena_size_rejected(monkeypatch):
    _expect_bootstrap_failure(
        monkeypatch,
        pack_header(arena_size=ARENA_SIZE),
        arena_size=ARENA_SIZE + 1,
        match="disagrees with backing length",
    )


def test_arbitrary_header_bytes_never_crash(monkeypatch):
    rng = random.Random(0x53574253)
    for _ in range(128):
        fields = {
            "arena_size": rng.choice([rng.getrandbits(64), ARENA_SIZE, 0, 1]),
            "ring_offset": rng.choice(
                [rng.getrandbits(64), 16, 4096, ARENA_SIZE, 2**64 - 8]
            ),
            "ring_capacity": rng.choice([rng.getrandbits(64), 0, 1, 8192, 2**64 - 1]),
            "ring_generation": rng.choice([rng.getrandbits(64), 0, 1, 7]),
        }
        header = pack_header(**fields)
        ran = []

        async def main(ctx):  # pragma: no cover - only on accidental success
            ran.append(ctx)

        arena_fd = make_arena(header)
        wake_fd, _keep_wake = make_wake(False)
        monkeypatch.setenv(ENV_ARENA, str(arena_fd))
        monkeypatch.setenv(ENV_WAKE, str(wake_fd))

        end = fields["ring_offset"] + fields["ring_capacity"]
        should_parse = (
            fields["arena_size"] == ARENA_SIZE
            and fields["ring_capacity"] > 0
            and fields["ring_generation"] > 0
            and fields["ring_offset"] >= 48
            and end <= ARENA_SIZE
        )
        if should_parse:
            swactor.run(main)
            assert ran, "parser accepted fields that satisfy no invariant"
        else:
            with pytest.raises(swactor.BootstrapError):
                swactor.run(main)
            assert not ran


# ─── B7 / B1 / B8: real process boundary ────────────────────────────────────

PROBE = Path(__file__).parent / "probe.py"


def _spawn(argv, *, arena_fd, wake_fd, env_extra=None):
    env = {k: v for k, v in os.environ.items() if k not in (ENV_ARENA, ENV_WAKE)}
    env[ENV_ARENA] = str(arena_fd)
    env[ENV_WAKE] = str(wake_fd)
    if env_extra:
        env.update(env_extra)
    for fd in (arena_fd, wake_fd):
        os.set_inheritable(fd, True)
    return subprocess.run(
        argv,
        env=env,
        pass_fds=(arena_fd, wake_fd),
        capture_output=True,
        text=True,
        timeout=60,
    )


def _parse_facts(stdout):
    facts = dict(
        part.split("=", 1) for part in stdout.strip().split() if "=" in part
    )
    return facts


def test_probe_succeeds_across_exec_boundary():
    arena_fd = make_arena(pack_header())
    wake_fd, _keep_wake = make_wake(signaled=True)
    result = _spawn([sys.executable, str(PROBE)], arena_fd=arena_fd, wake_fd=wake_fd)
    assert result.returncode == 0, result.stderr
    facts = _parse_facts(result.stdout)
    assert facts["HAS_DATA"] == "1"
    assert facts["WAKE_READABLE"] == "1", "B6: presignaled wake must survive exec"
    assert facts["WAKE_LEAKED"] == "0", "B5: grandchild must not see the wake fd"


def test_probe_reports_unsignaled_wake_across_exec_boundary():
    arena_fd = make_arena(pack_header())
    wake_fd, _keep_wake = make_wake(signaled=False)
    result = _spawn([sys.executable, str(PROBE)], arena_fd=arena_fd, wake_fd=wake_fd)
    assert result.returncode == 0, result.stderr
    assert _parse_facts(result.stdout)["WAKE_READABLE"] == "0"


_MAIN_RETURN_SCRIPT = (
    "import swactor\n"
    "async def main(ctx):\n"
    "    print('MAIN_OK')\n"
    "swactor.run(main)\n"
)
_MAIN_RAISE_SCRIPT = (
    "import swactor\n"
    "async def main(ctx):\n"
    "    raise RuntimeError('boom')\n"
    "swactor.run(main)\n"
)
_NEVER_RUN_SCRIPT = (
    "import swactor\n"
    "async def main(ctx):\n"
    "    raise AssertionError('must not run')\n"
    "swactor.run(main)\n"
)


def test_exit_zero_when_main_returns():
    arena_fd = make_arena(pack_header())
    wake_fd, _keep_wake = make_wake(True)
    result = _spawn(
        [sys.executable, "-c", _MAIN_RETURN_SCRIPT],
        arena_fd=arena_fd,
        wake_fd=wake_fd,
    )
    assert result.returncode == 0, result.stderr
    assert "MAIN_OK" in result.stdout


def test_exit_nonzero_when_main_raises_with_traceback():
    arena_fd = make_arena(pack_header())
    wake_fd, _keep_wake = make_wake(True)
    result = _spawn(
        [sys.executable, "-c", _MAIN_RAISE_SCRIPT],
        arena_fd=arena_fd,
        wake_fd=wake_fd,
    )
    assert result.returncode != 0
    assert "boom" in result.stderr


def test_exit_nonzero_on_bootstrap_defect_without_entering_main():
    arena_fd = make_arena(pack_header(magic=MAGIC ^ 0xFF))
    wake_fd, _keep_wake = make_wake(True)
    result = _spawn(
        [sys.executable, "-c", _NEVER_RUN_SCRIPT],
        arena_fd=arena_fd,
        wake_fd=wake_fd,
    )
    assert result.returncode != 0
    assert "BootstrapError" in result.stderr
    assert "must not run" not in result.stderr


def test_handoff_uses_only_the_two_env_names():
    arena_fd = make_arena(pack_header())
    wake_fd, _keep_wake = make_wake(True)
    result = _spawn(
        [sys.executable, "-c", _MAIN_RETURN_SCRIPT],
        arena_fd=arena_fd,
        wake_fd=wake_fd,
    )
    assert result.returncode == 0
    # B8 is structural: the binding reads exactly ENV_ARENA and ENV_WAKE.
    # Prove it by renaming one and observing failure.
    env = {k: v for k, v in os.environ.items() if k not in (ENV_ARENA, ENV_WAKE)}
    env["SWACTOR_DATA_PLANE_INPUT"] = "ignored-legacy-name"
    env[ENV_WAKE] = str(wake_fd)
    for fd in (arena_fd, wake_fd):
        os.set_inheritable(fd, True)
    missing = subprocess.run(
        [sys.executable, "-c", _NEVER_RUN_SCRIPT],
        env=env,
        pass_fds=(arena_fd, wake_fd),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert missing.returncode != 0
    assert ENV_ARENA in missing.stderr
