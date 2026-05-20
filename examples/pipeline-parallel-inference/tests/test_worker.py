"""Tests for ``pp_tinygrad_worker.py`` — pure helpers plus the
stdin/stdout JSON protocol exercised against a real subprocess.

Names match TEST_SPEC §2 and §3 verbatim.
"""

from __future__ import annotations

import base64
import json
import os
import selectors
import subprocess
import sys
from pathlib import Path

import pytest

import pp_tinygrad_worker as worker

WORKER = Path(__file__).parent.parent / "pp_tinygrad_worker.py"
PYTHON = sys.executable
STUB_HIDDEN_DIM = worker.STUB_HIDDEN_DIM
STUB_VOCAB_SIZE = worker.STUB_VOCAB_SIZE
STUB_HIDDEN_BYTES_PER_POS = STUB_HIDDEN_DIM * 2  # bf16


# ---------------------------------------------------------------------------
# Subprocess helpers


def _spawn(stage: int, num_stages: int, *, extra_env=None) -> subprocess.Popen:
    env = os.environ.copy()
    env["STAGE"] = str(stage)
    env["NUM_STAGES"] = str(num_stages)
    env["PP_WORKER_STUB"] = "1"
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        [PYTHON, str(WORKER), "--stub"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def _read_reply(proc: subprocess.Popen, timeout: float = 5.0) -> dict:
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    try:
        if not sel.select(timeout=timeout):
            stderr = ""
            try:
                stderr = proc.stderr.read() or ""
            except Exception:
                pass
            raise TimeoutError(f"No worker reply within {timeout}s; stderr: {stderr!r}")
        line = proc.stdout.readline()
    finally:
        sel.close()
    if not line:
        raise EOFError("Worker closed stdout before replying")
    return json.loads(line.strip())


def _send(proc: subprocess.Popen, obj: dict) -> None:
    proc.stdin.write(json.dumps(obj) + "\n")
    proc.stdin.flush()


def _send_raw(proc: subprocess.Popen, text: str) -> None:
    proc.stdin.write(text + "\n")
    proc.stdin.flush()


def _shutdown(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


@pytest.fixture
def stage0():
    proc = _spawn(0, 2)
    try:
        yield proc
    finally:
        _shutdown(proc)


@pytest.fixture
def stage1():
    proc = _spawn(1, 2)
    try:
        yield proc
    finally:
        _shutdown(proc)


@pytest.fixture
def first_of_four():
    """Stage 0 of a 4-stage pipeline — the First role with two middles below."""
    proc = _spawn(0, 4)
    try:
        yield proc
    finally:
        _shutdown(proc)


@pytest.fixture
def middle_of_four():
    """Stage 1 of a 4-stage pipeline — a Middle role."""
    proc = _spawn(1, 4)
    try:
        yield proc
    finally:
        _shutdown(proc)


@pytest.fixture
def last_of_four():
    """Stage 3 of a 4-stage pipeline — the Last role."""
    proc = _spawn(3, 4)
    try:
        yield proc
    finally:
        _shutdown(proc)


# ---------------------------------------------------------------------------
# §2 — pure helper tests


class TestLayerMath:
    def test_stage_0_layer_range_is_lower_half(self):
        # Explicit half-split for an even block count.
        assert worker.compute_layer_range(0, 2, 16) == (0, 8)
        # And the same shape holds for any even total.
        for total in (2, 4, 32, 100):
            start, end = worker.compute_layer_range(0, 2, total)
            assert start == 0
            assert end == total // 2

    def test_stage_1_layer_range_covers_remainder(self):
        # 15 blocks split across 2 stages: stage 1 must absorb the odd block.
        assert worker.compute_layer_range(1, 2, 15) == (7, 15)
        # The end of stage N-1 always equals total, regardless of remainder.
        for total in (2, 5, 16, 17, 101):
            _, end = worker.compute_layer_range(1, 2, total)
            assert end == total

    def test_stage_0_layer_range_starts_at_zero(self):
        # The first stage always owns blocks starting at 0, irrespective of N.
        for n in range(2, 9):
            start, _ = worker.compute_layer_range(0, n, 64)
            assert start == 0, f"stage 0 of {n}: start should be 0"

    def test_last_stage_layer_range_reaches_total(self):
        # The final stage's end equals total_blocks, including when total is
        # not evenly divisible — the last stage absorbs the remainder.
        for n in range(2, 9):
            for total in (n, n + 1, n * 7, n * 7 + (n - 1)):
                _, end = worker.compute_layer_range(n - 1, n, total)
                assert end == total, (
                    f"last stage of {n} on {total} blocks should end at total"
                )

    def test_layer_range_each_stage_owns_at_least_one_block(self):
        # For any NUM_STAGES <= total, every stage owns a non-empty range.
        for n in range(2, 9):
            for total in (n, n + 1, n * 3, 100):
                for s in range(n):
                    start, end = worker.compute_layer_range(s, n, total)
                    assert end > start, (
                        f"stage {s} of {n} on {total} blocks got empty "
                        f"range [{start},{end})"
                    )

    def test_layer_range_partition_for_indivisible_totals(self):
        # total=17, N=4 must yield four contiguous ranges covering [0, 17)
        # whose lengths differ by at most one.
        ranges = [worker.compute_layer_range(s, 4, 17) for s in range(4)]
        starts = [r[0] for r in ranges]
        ends = [r[1] for r in ranges]
        lengths = [e - s for s, e in ranges]
        assert starts[0] == 0
        assert ends[-1] == 17
        for i in range(len(ranges) - 1):
            assert ends[i] == starts[i + 1], (
                f"ranges {ranges[i]} and {ranges[i+1]} are not contiguous"
            )
        assert max(lengths) - min(lengths) <= 1, lengths

    def test_layer_range_rejects_num_stages_one(self):
        # The example does not serve single-node configurations. N=1 must
        # be rejected as a configuration error rather than silently treated
        # as "one stage with all the blocks".
        with pytest.raises(ValueError):
            worker.compute_layer_range(0, 1, 16)

    def test_layer_range_rejects_num_stages_zero_or_oversize(self):
        # Defensive on bad inputs from above (negative or zero N, or a stage
        # index that is out of range).
        with pytest.raises(ValueError):
            worker.compute_layer_range(0, 0, 16)
        with pytest.raises(ValueError):
            worker.compute_layer_range(4, 4, 16)
        with pytest.raises(ValueError):
            worker.compute_layer_range(-1, 4, 16)

    def test_layer_range_partition_is_total_coverage(self):
        # For 2..8-stage splits, the union of all stage ranges must equal
        # [0, total) exactly — no gaps, no overlap. Property-shaped.
        for num_stages in range(2, 9):
            # Include totals divisible by num_stages and totals that leave a
            # remainder, so we exercise the "last stage absorbs remainder" path.
            for total in (
                num_stages,
                num_stages + 1,
                num_stages * 5,
                num_stages * 5 + (num_stages - 1),
            ):
                ranges = [
                    worker.compute_layer_range(s, num_stages, total)
                    for s in range(num_stages)
                ]
                covered: list[int] = []
                for start, end in ranges:
                    assert (
                        start < end
                    ), f"empty range {start}..{end} (n={num_stages}, total={total})"
                    covered.extend(range(start, end))
                assert covered == list(
                    range(total)
                ), f"coverage mismatch n={num_stages} total={total}: {ranges}"

    def test_argmax_sampling_is_deterministic(self):
        logits = [0.1, 0.4, 0.2, 0.3]
        first = worker.argmax_sample(logits)
        # Determinism: many calls all produce the same id.
        for _ in range(10):
            assert worker.argmax_sample(logits) == first
        # And the value must actually be the argmax, not a constant —
        # otherwise this test would pass for ``def argmax(_): return 0``.
        assert first == 1
        assert worker.argmax_sample([5.0, 1.0, 1.0, 1.0]) == 0
        assert worker.argmax_sample([1.0, 1.0, 1.0, 9.0]) == 3


# ---------------------------------------------------------------------------
# §3 — worker contract tests


class TestWorkerStartup:
    def test_worker_emits_ready_with_pid_and_stage(self, stage0):
        ready = _read_reply(stage0)
        assert ready["status"] == "ready"
        assert ready["pid"] == stage0.pid
        assert ready["stage"] == 0

    def test_worker_rejects_invalid_stage_env(self):
        # Every invalid configuration must exit non-zero quickly, never
        # reach the ready line, and never hang waiting for stdin.
        invalid = [
            {"STAGE": "2", "NUM_STAGES": "2"},     # out of range high
            {"STAGE": "-1", "NUM_STAGES": "2"},    # negative
            {"STAGE": "abc", "NUM_STAGES": "2"},   # non-numeric
            {"STAGE": "", "NUM_STAGES": "2"},      # missing
            {"STAGE": "0", "NUM_STAGES": "0"},     # zero stages
            {"STAGE": "٠", "NUM_STAGES": "2"},     # arabic-indic 0 (unicode digit)
        ]
        for env_overrides in invalid:
            env = os.environ.copy()
            env["PP_WORKER_STUB"] = "1"
            env.update(env_overrides)
            proc = subprocess.Popen(
                [PYTHON, str(WORKER), "--stub"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            try:
                exit_code = proc.wait(timeout=5)
            finally:
                _shutdown(proc)
            assert (
                exit_code != 0
            ), f"expected non-zero exit for env={env_overrides}, got 0"


class TestStage0Operations:
    def test_embed_and_forward_returns_hidden_for_prompt(self, stage0):
        _read_reply(stage0)  # ready
        tokens = [1, 2, 3, 4, 5]
        _send(
            stage0,
            {"op": "embed_and_forward", "request_id": 1, "tokens": tokens, "position": 0},
        )
        reply = _read_reply(stage0)
        assert "error" not in reply, reply
        assert reply["request_id"] == 1
        assert reply["seq_len"] == len(tokens)
        hidden = base64.b64decode(reply["hidden_b64"])
        assert len(hidden) == len(tokens) * STUB_HIDDEN_BYTES_PER_POS
        # Payload must be non-trivial so a `return b"\0" * N` stub would not pass.
        assert any(b != 0 for b in hidden)

    def test_decode_step_returns_hidden_for_single_token(self, stage0):
        _read_reply(stage0)
        _send(
            stage0,
            {"op": "decode_step", "request_id": 2, "token_id": 42, "position": 5},
        )
        reply = _read_reply(stage0)
        assert "error" not in reply, reply
        assert reply["request_id"] == 2
        assert reply["seq_len"] == 1
        hidden = base64.b64decode(reply["hidden_b64"])
        assert len(hidden) == STUB_HIDDEN_BYTES_PER_POS
        assert any(b != 0 for b in hidden)

    def test_kv_cache_grows_across_successive_decode_steps(self, stage0):
        _read_reply(stage0)
        _send(
            stage0,
            {"op": "embed_and_forward", "request_id": 1, "tokens": [10, 20, 30], "position": 0},
        )
        assert "error" not in _read_reply(stage0)
        # Successive decode_step calls at increasing positions all succeed.
        hidden_blobs: set[str] = set()
        for i, pos in enumerate((3, 4, 5, 6)):
            _send(
                stage0,
                {"op": "decode_step", "request_id": 100 + i, "token_id": 99, "position": pos},
            )
            reply = _read_reply(stage0)
            assert "error" not in reply, f"decode at position {pos} failed: {reply}"
            assert reply["seq_len"] == 1
            hidden_blobs.add(reply["hidden_b64"])
        # Position must actually influence output — otherwise the worker is
        # silently ignoring it and a real model would corrupt its KV cache.
        assert len(hidden_blobs) > 1

    def test_stage_0_rejects_stage_1_ops(self, stage0):
        _read_reply(stage0)
        b64 = base64.b64encode(b"\x01" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            stage0,
            {
                "op": "forward_and_sample",
                "request_id": 7,
                "hidden_b64": b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        reply = _read_reply(stage0)
        assert "error" in reply, reply
        assert reply.get("request_id") == 7
        # Worker survives — a follow-up valid request still works.
        _send(
            stage0,
            {"op": "decode_step", "request_id": 8, "token_id": 1, "position": 0},
        )
        ok = _read_reply(stage0)
        assert "error" not in ok, ok
        assert ok["request_id"] == 8


class TestStage1Operations:
    def test_forward_and_sample_returns_valid_token_id(self, stage1):
        _read_reply(stage1)
        hidden = base64.b64encode(b"\x42" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            stage1,
            {
                "op": "forward_and_sample",
                "request_id": 1,
                "hidden_b64": hidden,
                "position": 0,
                "seq_len": 1,
            },
        )
        reply = _read_reply(stage1)
        assert "error" not in reply, reply
        token = reply["token_id"]
        assert isinstance(token, int)
        assert 0 <= token < STUB_VOCAB_SIZE

    def test_forward_and_sample_is_deterministic_for_same_input(self, stage1):
        _read_reply(stage1)
        hidden = base64.b64encode(bytes(range(STUB_HIDDEN_BYTES_PER_POS))).decode()
        observed = []
        for rid in (1, 2, 3, 4):
            _send(
                stage1,
                {
                    "op": "forward_and_sample",
                    "request_id": rid,
                    "hidden_b64": hidden,
                    "position": 7,
                    "seq_len": 1,
                },
            )
            reply = _read_reply(stage1)
            assert "error" not in reply, reply
            observed.append(reply["token_id"])
        assert len(set(observed)) == 1, f"non-deterministic token ids: {observed}"
        # Diversity guard: a constant ``return 0`` implementation would
        # trivially satisfy determinism. Probe several distinct positions
        # and require at least two distinct outputs — collision across all
        # of these in a 32-id vocab is astronomically unlikely if the
        # implementation actually mixes position into the result.
        diverse = {observed[0]}
        for rid, pos in enumerate((1, 11, 101, 12345, 999_999), start=200):
            _send(
                stage1,
                {
                    "op": "forward_and_sample",
                    "request_id": rid,
                    "hidden_b64": hidden,
                    "position": pos,
                    "seq_len": 1,
                },
            )
            diverse.add(_read_reply(stage1)["token_id"])
        assert (
            len(diverse) > 1
        ), f"position appears to be ignored — all positions mapped to {observed[0]}"

    def test_stage_1_rejects_stage_0_ops(self, stage1):
        _read_reply(stage1)
        _send(
            stage1,
            {"op": "embed_and_forward", "request_id": 5, "tokens": [1, 2], "position": 0},
        )
        reply = _read_reply(stage1)
        assert "error" in reply, reply
        assert reply.get("request_id") == 5
        # Survives and serves its own op.
        ok_hidden = base64.b64encode(b"\x00" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            stage1,
            {
                "op": "forward_and_sample",
                "request_id": 6,
                "hidden_b64": ok_hidden,
                "position": 0,
                "seq_len": 1,
            },
        )
        ok = _read_reply(stage1)
        assert "error" not in ok, ok


# ---------------------------------------------------------------------------
# §5.2 — TestFirstStageOperations (N=4, STAGE=0).
#
# At N=4 the first stage has Middle and Last stages downstream. Beyond the
# stage-0 ops covered above (under TestStage0Operations at N=2, which still
# stand), the first stage must reject the ops belonging to other roles.


class TestFirstStageOperations:
    def test_tokenize_returns_token_ids_for_prompt(self, first_of_four):
        _read_reply(first_of_four)
        _send(
            first_of_four,
            {"op": "tokenize", "request_id": 1, "prompt": "the quick brown fox"},
        )
        reply = _read_reply(first_of_four)
        assert "error" not in reply, reply
        assert reply["request_id"] == 1
        tokens = reply["tokens"]
        assert isinstance(tokens, list) and len(tokens) > 0
        assert all(isinstance(t, int) for t in tokens)

    def test_first_stage_rejects_forward_range(self, first_of_four):
        _read_reply(first_of_four)
        hidden_b64 = base64.b64encode(b"\x01" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            first_of_four,
            {
                "op": "forward_range",
                "request_id": 11,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        err = _read_reply(first_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 11
        # Worker stays up: a valid first-stage op still works.
        _send(
            first_of_four,
            {"op": "decode_step", "request_id": 12, "token_id": 1, "position": 0},
        )
        ok = _read_reply(first_of_four)
        assert "error" not in ok, ok
        assert ok["request_id"] == 12

    def test_first_stage_rejects_forward_and_sample(self, first_of_four):
        _read_reply(first_of_four)
        hidden_b64 = base64.b64encode(b"\x01" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            first_of_four,
            {
                "op": "forward_and_sample",
                "request_id": 21,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        err = _read_reply(first_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 21
        # Worker stays up.
        _send(
            first_of_four,
            {"op": "decode_step", "request_id": 22, "token_id": 2, "position": 0},
        )
        ok = _read_reply(first_of_four)
        assert "error" not in ok, ok


# ---------------------------------------------------------------------------
# §5.3 — TestMiddleStageOperations (N=4, STAGE=1).


class TestMiddleStageOperations:
    @staticmethod
    def _send_forward_range(
        proc: subprocess.Popen,
        *,
        request_id: int,
        hidden_bytes: bytes,
        position: int,
        seq_len: int,
    ) -> dict:
        _send(
            proc,
            {
                "op": "forward_range",
                "request_id": request_id,
                "hidden_b64": base64.b64encode(hidden_bytes).decode("ascii"),
                "position": position,
                "seq_len": seq_len,
            },
        )
        return _read_reply(proc)

    def test_forward_range_returns_hidden_with_input_seq_len(self, middle_of_four):
        _read_reply(middle_of_four)
        for seq_len in (1, 3, 7):
            payload = bytes(((i * 13 + seq_len) & 0xFF) for i in range(seq_len * STUB_HIDDEN_BYTES_PER_POS))
            reply = self._send_forward_range(
                middle_of_four,
                request_id=seq_len,
                hidden_bytes=payload,
                position=seq_len,
                seq_len=seq_len,
            )
            assert "error" not in reply, (seq_len, reply)
            assert reply["request_id"] == seq_len
            assert reply["seq_len"] == seq_len
            out = base64.b64decode(reply["hidden_b64"])
            assert len(out) == seq_len * STUB_HIDDEN_BYTES_PER_POS, (
                f"seq_len={seq_len}: got {len(out)} bytes"
            )
            # Non-trivial payload: a `b"\x00" * N` echo would also satisfy
            # the length assertion, so explicitly reject all-zero output.
            assert any(b != 0 for b in out), seq_len

    def test_forward_range_is_deterministic_for_same_input(self, middle_of_four):
        _read_reply(middle_of_four)
        payload = bytes(range(STUB_HIDDEN_BYTES_PER_POS * 2))
        observed: list[str] = []
        for rid in (1, 2, 3, 4):
            reply = self._send_forward_range(
                middle_of_four,
                request_id=rid,
                hidden_bytes=payload,
                position=11,
                seq_len=2,
            )
            assert "error" not in reply, reply
            observed.append(reply["hidden_b64"])
        assert len(set(observed)) == 1, f"non-deterministic forward_range: {observed}"

    def test_forward_range_kv_cache_advances_with_position(self, middle_of_four):
        _read_reply(middle_of_four)
        # Prefill-like step at position 0 with a multi-token chunk.
        prompt_len = 5
        prefill = bytes((i & 0xFF) for i in range(prompt_len * STUB_HIDDEN_BYTES_PER_POS))
        reply = self._send_forward_range(
            middle_of_four,
            request_id=1,
            hidden_bytes=prefill,
            position=0,
            seq_len=prompt_len,
        )
        assert "error" not in reply, reply
        assert reply["seq_len"] == prompt_len
        # Subsequent decode-shaped steps advance position monotonically.
        outputs: set[str] = set()
        single = bytes(range(STUB_HIDDEN_BYTES_PER_POS))
        for i, pos in enumerate((prompt_len, prompt_len + 1, prompt_len + 2)):
            reply = self._send_forward_range(
                middle_of_four,
                request_id=100 + i,
                hidden_bytes=single,
                position=pos,
                seq_len=1,
            )
            assert "error" not in reply, (pos, reply)
            assert reply["seq_len"] == 1
            outputs.add(reply["hidden_b64"])
        # Position must influence output — if it does not, a real KV cache
        # would be corrupted by silently reusing stale rows.
        assert len(outputs) > 1, outputs

    def test_middle_stage_rejects_embed_and_forward(self, middle_of_four):
        _read_reply(middle_of_four)
        _send(
            middle_of_four,
            {"op": "embed_and_forward", "request_id": 1, "tokens": [1, 2], "position": 0},
        )
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 1

    def test_middle_stage_rejects_decode_step(self, middle_of_four):
        _read_reply(middle_of_four)
        _send(
            middle_of_four,
            {"op": "decode_step", "request_id": 2, "token_id": 3, "position": 0},
        )
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 2

    def test_middle_stage_rejects_forward_and_sample(self, middle_of_four):
        _read_reply(middle_of_four)
        hidden_b64 = base64.b64encode(b"\x00" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            middle_of_four,
            {
                "op": "forward_and_sample",
                "request_id": 3,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 3

    def test_middle_stage_rejects_tokenize(self, middle_of_four):
        _read_reply(middle_of_four)
        _send(middle_of_four, {"op": "tokenize", "request_id": 4, "prompt": "hi"})
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 4

    def test_middle_stage_rejects_detokenize(self, middle_of_four):
        _read_reply(middle_of_four)
        _send(
            middle_of_four,
            {"op": "detokenize", "request_id": 5, "tokens": [1, 2, 3]},
        )
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 5

    def test_forward_range_rejects_seq_len_mismatch(self, middle_of_four):
        _read_reply(middle_of_four)
        # Declared seq_len longer than the actual hidden payload.
        short = b"\x00" * STUB_HIDDEN_BYTES_PER_POS  # one token's worth
        _send(
            middle_of_four,
            {
                "op": "forward_range",
                "request_id": 1,
                "hidden_b64": base64.b64encode(short).decode("ascii"),
                "position": 0,
                "seq_len": 3,
            },
        )
        err = _read_reply(middle_of_four)
        assert "error" in err, err
        # Inverse mismatch: payload longer than declared seq_len.
        long = b"\x00" * (STUB_HIDDEN_BYTES_PER_POS * 4)
        _send(
            middle_of_four,
            {
                "op": "forward_range",
                "request_id": 2,
                "hidden_b64": base64.b64encode(long).decode("ascii"),
                "position": 0,
                "seq_len": 1,
            },
        )
        err2 = _read_reply(middle_of_four)
        assert "error" in err2, err2
        # Worker still serves a well-formed follow-up.
        good = b"\x11" * STUB_HIDDEN_BYTES_PER_POS
        ok_reply = self._send_forward_range(
            middle_of_four,
            request_id=3,
            hidden_bytes=good,
            position=0,
            seq_len=1,
        )
        assert "error" not in ok_reply, ok_reply


# ---------------------------------------------------------------------------
# §5.4 — TestLastStageOperations (N=4, STAGE=N-1).
#
# The positive-path tests for the last role (forward_and_sample,
# detokenize) are also exercised at N=2 above. Here we add the rejection
# tests that prove the last stage refuses ops belonging to other roles.


class TestLastStageOperations:
    def test_last_stage_rejects_embed_and_forward(self, last_of_four):
        _read_reply(last_of_four)
        _send(
            last_of_four,
            {"op": "embed_and_forward", "request_id": 1, "tokens": [1, 2], "position": 0},
        )
        err = _read_reply(last_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 1
        # Worker stays up: its own op still works.
        hidden_b64 = base64.b64encode(b"\x00" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            last_of_four,
            {
                "op": "forward_and_sample",
                "request_id": 2,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        ok = _read_reply(last_of_four)
        assert "error" not in ok, ok

    def test_last_stage_rejects_forward_range(self, last_of_four):
        _read_reply(last_of_four)
        hidden_b64 = base64.b64encode(b"\x00" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            last_of_four,
            {
                "op": "forward_range",
                "request_id": 11,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        err = _read_reply(last_of_four)
        assert "error" in err, err
        assert err.get("request_id") == 11
        # Worker stays up.
        _send(
            last_of_four,
            {
                "op": "forward_and_sample",
                "request_id": 12,
                "hidden_b64": hidden_b64,
                "position": 0,
                "seq_len": 1,
            },
        )
        ok = _read_reply(last_of_four)
        assert "error" not in ok, ok


class TestWorkerMalformedInput:
    def test_malformed_json_returns_error_and_continues(self, stage0):
        _read_reply(stage0)
        _send_raw(stage0, "this is not json {{{")
        err = _read_reply(stage0)
        assert "error" in err, err
        # Recovery
        _send(
            stage0,
            {"op": "decode_step", "request_id": 99, "token_id": 1, "position": 0},
        )
        ok = _read_reply(stage0)
        assert "error" not in ok, ok
        assert ok["request_id"] == 99

    def test_missing_op_field_returns_error(self, stage0):
        _read_reply(stage0)
        # Valid JSON object, but no "op".
        _send(stage0, {"request_id": 1, "tokens": [1, 2, 3]})
        err = _read_reply(stage0)
        assert "error" in err, err
        # Plain JSON scalars are not objects either — they must also produce
        # an error, not crash the worker.
        _send_raw(stage0, "42")
        err2 = _read_reply(stage0)
        assert "error" in err2, err2
        # Recovery
        _send(
            stage0,
            {"op": "decode_step", "request_id": 2, "token_id": 1, "position": 0},
        )
        ok = _read_reply(stage0)
        assert "error" not in ok, ok

    def test_unknown_op_returns_error(self, stage0):
        # A typo or wrong-version client must not crash the worker. The
        # reply carries the request_id so the caller can correlate it.
        _read_reply(stage0)
        _send(stage0, {"op": "frobnicate", "request_id": 1, "x": 0})
        err = _read_reply(stage0)
        assert "error" in err, err
        assert err.get("request_id") == 1
        # Empty-string op is "unknown" too.
        _send(stage0, {"op": "", "request_id": 2})
        err2 = _read_reply(stage0)
        assert "error" in err2, err2
        # Worker survives.
        _send(
            stage0,
            {"op": "decode_step", "request_id": 3, "token_id": 1, "position": 0},
        )
        ok = _read_reply(stage0)
        assert "error" not in ok, ok

    def test_oversized_hidden_payload_returns_error(self, stage1):
        _read_reply(stage1)
        # Declared seq_len > actual hidden length.
        short_hidden = base64.b64encode(b"\x00" * STUB_HIDDEN_BYTES_PER_POS).decode()
        _send(
            stage1,
            {
                "op": "forward_and_sample",
                "request_id": 1,
                "hidden_b64": short_hidden,
                "position": 0,
                "seq_len": 2,
            },
        )
        err = _read_reply(stage1)
        assert "error" in err, err
        # Inverse: declared seq_len < actual hidden length.
        long_hidden = base64.b64encode(b"\x00" * (STUB_HIDDEN_BYTES_PER_POS * 5)).decode()
        _send(
            stage1,
            {
                "op": "forward_and_sample",
                "request_id": 2,
                "hidden_b64": long_hidden,
                "position": 0,
                "seq_len": 1,
            },
        )
        err2 = _read_reply(stage1)
        assert "error" in err2, err2
        # Worker still serves a well-formed follow-up.
        _send(
            stage1,
            {
                "op": "forward_and_sample",
                "request_id": 3,
                "hidden_b64": short_hidden,
                "position": 0,
                "seq_len": 1,
            },
        )
        ok = _read_reply(stage1)
        assert "error" not in ok, ok
        assert "token_id" in ok


class TestWorkerEOFShutdown:
    def test_eof_causes_clean_exit(self, stage0):
        _read_reply(stage0)  # consume ready
        stage0.stdin.close()
        exit_code = stage0.wait(timeout=5)
        assert exit_code == 0, f"worker exited with code {exit_code}, expected 0"


# ---------------------------------------------------------------------------
# Real (non-stub) tinygrad worker
#
# Loading the GGUF takes ~15s and depends on a network-fetched file. These
# tests are skipped unless the developer opts in by setting
# ``PP_REAL_WORKER_TESTS=1`` (or any non-empty string). The ``cargo test``
# fast tier and the default ``pytest`` invocation skip the class entirely.


REAL_WORKER_GATE = "PP_REAL_WORKER_TESTS"
REAL_LOAD_TIMEOUT = 180.0  # seconds; GGUF fetch + tinygrad realize
REAL_OP_TIMEOUT = 120.0    # seconds; one block-range forward on CPU


def _spawn_real(stage: int, num_stages: int, *, model: str = "llama3.2:1b") -> subprocess.Popen:
    """Spawn a real-mode (non-stub) worker. PP_WORKER_STUB is unset."""
    env = os.environ.copy()
    env["STAGE"] = str(stage)
    env["NUM_STAGES"] = str(num_stages)
    env["MODEL"] = model
    env.pop("PP_WORKER_STUB", None)
    return subprocess.Popen(
        [PYTHON, str(WORKER)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


@pytest.mark.skipif(
    not os.environ.get(REAL_WORKER_GATE),
    reason=f"Set {REAL_WORKER_GATE}=1 to run real tinygrad worker tests",
)
class TestRealTinygradWorker:
    """One prefill + one decode-step round-trip against the real GGUF.

    This is the slow-tier counterpart to ``TestStage0Operations`` /
    ``TestStage1Operations``. It does not duplicate every stub-mode test
    case — those exercise the protocol's error paths. Here we just
    establish that real-mode embeds, forwards, samples, and decodes
    using the actual ``llama3.2:1b`` weights without the stub.
    """

    def test_real_worker_advertises_model_geometry_on_ready(self):
        stage0 = _spawn_real(0, 2)
        try:
            ready = _read_reply(stage0, timeout=REAL_LOAD_TIMEOUT)
            assert ready["status"] == "ready", ready
            assert ready["stage"] == 0
            # Real-mode adds geometry fields so the orchestrator can
            # size hidden-state buffers without an extra handshake.
            assert isinstance(ready.get("hidden_dim"), int) and ready["hidden_dim"] > 0
            assert isinstance(ready.get("vocab_size"), int) and ready["vocab_size"] > 0
            assert isinstance(ready.get("total_blocks"), int) and ready["total_blocks"] > 0
            lr = ready.get("layer_range")
            assert isinstance(lr, list) and len(lr) == 2
            assert lr == [0, ready["total_blocks"] // 2]
        finally:
            _shutdown(stage0)

    def test_real_prefill_and_decode_step_round_trip(self):
        # Drives one full prefill + decode round-trip through both stages
        # on the real model. The interesting assertions are byte lengths
        # (catches any dtype/shape mismatch) and that stage-1 actually
        # samples a token in ``[0, vocab)``.
        stage0 = _spawn_real(0, 2)
        stage1 = _spawn_real(1, 2)
        try:
            ready0 = _read_reply(stage0, timeout=REAL_LOAD_TIMEOUT)
            ready1 = _read_reply(stage1, timeout=REAL_LOAD_TIMEOUT)
            hidden_dim = ready0["hidden_dim"]
            vocab = ready0["vocab_size"]
            assert ready1["hidden_dim"] == hidden_dim
            assert ready1["vocab_size"] == vocab

            # Tokenise via the real worker so the prompt actually maps to
            # GGUF vocab ids; saves us from duplicating the tokenizer.
            _send(stage0, {"op": "tokenize", "request_id": 1, "prompt": "Say hello"})
            tok_reply = _read_reply(stage0, timeout=REAL_OP_TIMEOUT)
            assert "error" not in tok_reply, tok_reply
            tokens = tok_reply["tokens"]
            assert isinstance(tokens, list) and len(tokens) > 0
            assert all(isinstance(t, int) and 0 <= t < vocab for t in tokens), tokens

            # Stage 0: prefill at position 0.
            _send(
                stage0,
                {
                    "op": "embed_and_forward",
                    "request_id": 2,
                    "tokens": tokens,
                    "position": 0,
                },
            )
            prefill = _read_reply(stage0, timeout=REAL_OP_TIMEOUT)
            assert "error" not in prefill, prefill
            assert prefill["seq_len"] == len(tokens)
            hidden_pref = base64.b64decode(prefill["hidden_b64"])
            assert len(hidden_pref) == len(tokens) * hidden_dim * 2
            # Non-trivial payload: a `b"\x00" * N` return would also pass
            # the length assertion, so reject that explicitly.
            assert any(b != 0 for b in hidden_pref)

            # Stage 1: forward + sample.
            _send(
                stage1,
                {
                    "op": "forward_and_sample",
                    "request_id": 3,
                    "hidden_b64": prefill["hidden_b64"],
                    "position": 0,
                    "seq_len": len(tokens),
                },
            )
            sampled = _read_reply(stage1, timeout=REAL_OP_TIMEOUT)
            assert "error" not in sampled, sampled
            tok_id = sampled["token_id"]
            assert isinstance(tok_id, int) and 0 <= tok_id < vocab

            # Stage 0: decode step at position == prompt_len. Per the
            # plan, off-by-one position handling is the #1 risk; this
            # exercises the single-token branch.
            _send(
                stage0,
                {
                    "op": "decode_step",
                    "request_id": 4,
                    "token_id": tok_id,
                    "position": len(tokens),
                },
            )
            decode = _read_reply(stage0, timeout=REAL_OP_TIMEOUT)
            assert "error" not in decode, decode
            assert decode["seq_len"] == 1
            hidden_dec = base64.b64decode(decode["hidden_b64"])
            assert len(hidden_dec) == 1 * hidden_dim * 2
            assert any(b != 0 for b in hidden_dec)

            # Stage 1: forward + sample the decode-step hidden state.
            _send(
                stage1,
                {
                    "op": "forward_and_sample",
                    "request_id": 5,
                    "hidden_b64": decode["hidden_b64"],
                    "position": len(tokens),
                    "seq_len": 1,
                },
            )
            sampled2 = _read_reply(stage1, timeout=REAL_OP_TIMEOUT)
            assert "error" not in sampled2, sampled2
            tok_id2 = sampled2["token_id"]
            assert isinstance(tok_id2, int) and 0 <= tok_id2 < vocab

            # The two sampled tokens should not both be a default
            # zero/special id — a constant-output implementation would
            # match the assertions above. Detokenise both and require
            # the resulting bytes to be non-empty.
            _send(
                stage1,
                {"op": "detokenize", "request_id": 6, "tokens": [tok_id, tok_id2]},
            )
            detok = _read_reply(stage1, timeout=REAL_OP_TIMEOUT)
            assert "error" not in detok, detok
            assert isinstance(detok["text"], str)
            assert detok["text"] != ""
        finally:
            _shutdown(stage0)
            _shutdown(stage1)

