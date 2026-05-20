#!/usr/bin/env python3
"""Pipeline-parallel tinygrad worker — stdin/stdout JSON protocol.

Each worker hosts one *stage* of a pipeline-parallel inference run. The
stage index, total stage count, and the model name are read from
environment variables at startup:

    STAGE=0 NUM_STAGES=2 MODEL=llama3.2:1b

The worker runs in one of two modes:

* **stub** (``--stub`` flag or ``PP_WORKER_STUB=1`` env): skips loading
  any real model. Stage-0 ops return deterministic pseudo-bf16 hidden
  bytes derived from the inputs; the final-stage op returns a
  deterministic ``token_id``. ``tokenize`` whitespace-splits and hashes;
  ``detokenize`` formats the token ids as text. This mode is used by
  the Rust actor and integration tests so they never need a GPU.

* **real** (default): loads ``$MODEL`` via ``Transformer.from_gguf`` and
  ``SimpleTokenizer.from_gguf_kv`` and runs the per-stage forward pass
  directly against ``model.blk[start:end]`` (bypassing
  ``Transformer.forward`` and ``forward_jit``). Hidden state is cast to
  float16 on the wire (2 bytes per element); decoded back to float16
  on the receiving stage. Stage-1's ``forward_and_sample`` follows the
  block range with ``output_norm`` + ``output`` and argmax-samples the
  last position's logits.

Protocol — one JSON line in, one JSON line out.

Each op is restricted to the role that owns it (First = stage 0,
Last = stage N-1, Middle = anything in between). Ops invoked on the
wrong role return ``{"error": ...}`` and the worker keeps serving.

First-stage ops (``STAGE == 0``)::

    -> {"op": "embed_and_forward", "request_id": <int>,
        "tokens": [<int>, ...], "position": <int>}
    <- {"request_id": <int>, "hidden_b64": "<base64>", "seq_len": <int>}

    -> {"op": "decode_step", "request_id": <int>,
        "token_id": <int>, "position": <int>}
    <- {"request_id": <int>, "hidden_b64": "<base64>", "seq_len": 1}

    -> {"op": "tokenize", "request_id": <int>, "prompt": "<text>"}
    <- {"request_id": <int>, "tokens": [<int>, ...]}

Middle-stage op (``0 < STAGE < NUM_STAGES - 1``)::

    -> {"op": "forward_range", "request_id": <int>,
        "hidden_b64": "<base64>", "position": <int>, "seq_len": <int>}
    <- {"request_id": <int>, "hidden_b64": "<base64>", "seq_len": <int>}

Last-stage ops (``STAGE == NUM_STAGES - 1``)::

    -> {"op": "forward_and_sample", "request_id": <int>,
        "hidden_b64": "<base64>", "position": <int>, "seq_len": <int>}
    <- {"request_id": <int>, "token_id": <int>}

    -> {"op": "detokenize", "request_id": <int>, "tokens": [<int>, ...]}
    <- {"request_id": <int>, "text": "<text>"}

Errors of any kind reply with::

    <- {"request_id": <int>?, "error": "<message>"}

and the worker keeps serving. EOF on stdin causes a clean exit 0.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
from typing import Sequence

# Stub-mode constants — small so test payloads stay tiny. Both values
# must match what the Rust stub-mode actor tests assume.
STUB_HIDDEN_DIM = 16
STUB_VOCAB_SIZE = 32
BYTES_PER_ELEM = 2  # bf16

# ASCII-only integer; rejects unicode digits and surrounding whitespace.
_INT_RE = re.compile(r"-?[0-9]+")


def compute_layer_range(stage: int, num_stages: int, total_blocks: int) -> tuple[int, int]:
    """Return the half-open ``[start, end)`` block range owned by ``stage``.

    The split is ``total_blocks // num_stages`` per stage; the final stage
    absorbs any remainder. ``num_stages`` must be ``>= 2`` — the example
    does not serve single-node configurations (see
    ``examples/single-gpu-inference`` for that). Raises ``ValueError``
    on out-of-range inputs.
    """
    if num_stages < 2:
        raise ValueError(f"num_stages must be >= 2, got {num_stages}")
    if not (0 <= stage < num_stages):
        raise ValueError(f"stage {stage} out of range [0, {num_stages})")
    if total_blocks < num_stages:
        raise ValueError(
            f"total_blocks {total_blocks} cannot be split into {num_stages} stages"
        )
    k = total_blocks // num_stages
    start = stage * k
    end = (stage + 1) * k if stage < num_stages - 1 else total_blocks
    return start, end


def argmax_sample(logits: Sequence[float]) -> int:
    """Return the index of the maximum element in ``logits``.

    On ties, the lowest index wins. Raises ``ValueError`` on empty input.
    """
    if len(logits) == 0:
        raise ValueError("argmax over empty logits")
    best_idx = 0
    best_val = logits[0]
    for i in range(1, len(logits)):
        if logits[i] > best_val:
            best_val = logits[i]
            best_idx = i
    return best_idx


def _stub_hidden_bytes(tokens: Sequence[int], position: int) -> bytes:
    """Deterministic pseudo-bf16 bytes of length ``len(tokens) * hidden_dim * 2``.

    Same ``(tokens, position)`` always produce the same bytes; different
    inputs almost always differ. Bytes are derived from SHA-256 so the
    payload is non-trivial (not all zeros, not monotonic).
    """
    seed = hashlib.sha256(
        b"pp-stub:"
        + str(position).encode()
        + b"|"
        + b",".join(str(int(t)).encode() for t in tokens)
    ).digest()
    n_bytes = len(tokens) * STUB_HIDDEN_DIM * BYTES_PER_ELEM
    out = bytearray()
    counter = 0
    while len(out) < n_bytes:
        out.extend(hashlib.sha256(seed + counter.to_bytes(8, "little")).digest())
        counter += 1
    return bytes(out[:n_bytes])


def _stub_forward_range_bytes(hidden: bytes, position: int, seq_len: int) -> bytes:
    """Deterministic pseudo-bf16 bytes for a middle stage's ``forward_range``.

    Output length is ``seq_len * STUB_HIDDEN_DIM * 2`` (matches the input
    hidden's expected length). Same ``(hidden, position, seq_len)`` always
    produce the same bytes; differing inputs almost always differ. Used
    only in stub mode.
    """
    n_bytes = seq_len * STUB_HIDDEN_DIM * BYTES_PER_ELEM
    seed = hashlib.sha256(
        b"pp-stub-fr:" + position.to_bytes(8, "little", signed=False) + hidden
    ).digest()
    out = bytearray()
    counter = 0
    while len(out) < n_bytes:
        out.extend(hashlib.sha256(seed + counter.to_bytes(8, "little")).digest())
        counter += 1
    return bytes(out[:n_bytes])


def _stub_token_id(hidden: bytes, position: int) -> int:
    """Deterministic stub token id in ``[0, STUB_VOCAB_SIZE)``."""
    digest = hashlib.sha256(
        b"pp-stub-sample:" + position.to_bytes(8, "little", signed=False) + hidden
    ).digest()
    return int.from_bytes(digest[:4], "little") % STUB_VOCAB_SIZE


def _parse_env_int(name: str) -> int:
    raw = os.environ.get(name, "")
    stripped = raw.strip()
    if not stripped:
        _die(f"env {name} is required")
    if not _INT_RE.fullmatch(stripped):
        _die(f"env {name}={raw!r} is not an ASCII integer")
    return int(stripped)


def _die(msg: str) -> "None":
    print(f"pp_tinygrad_worker: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(2)


def _write(obj) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _is_nonneg_int(x) -> bool:
    # ``bool`` is a subclass of ``int`` in Python; reject it explicitly so
    # ``{"position": true}`` doesn't sneak through.
    return isinstance(x, int) and not isinstance(x, bool) and x >= 0


class _RealModelState:
    """Holds the loaded tinygrad model + tokenizer for real mode.

    All tinygrad imports live inside the loader so stub-mode never pays
    the import cost. Once loaded, the state is reused for every request.
    """

    def __init__(self, model_name: str, stage: int, num_stages: int):
        # Import tinygrad lazily so stub-mode never touches it.
        import numpy as np
        from tinygrad import Tensor
        from tinygrad.helpers import fetch
        from tinygrad.apps.llm import Transformer, SimpleTokenizer, models

        if model_name not in models:
            available = ", ".join(sorted(models.keys()))
            _die(f"unknown MODEL {model_name!r}; available: {available}")

        url = models[model_name]
        print(
            f"pp_tinygrad_worker: stage={stage}/{num_stages} fetching {model_name}",
            file=sys.stderr,
            flush=True,
        )
        gguf_path = fetch(url)
        print(
            f"pp_tinygrad_worker: stage={stage} loading model from {gguf_path}",
            file=sys.stderr,
            flush=True,
        )
        model, kv = Transformer.from_gguf(Tensor(gguf_path), max_context=512)
        tokenizer = SimpleTokenizer.from_gguf_kv(kv)

        arch = kv["general.architecture"]
        hidden_dim = int(kv[f"{arch}.embedding_length"])
        total_blocks = int(kv[f"{arch}.block_count"])
        vocab_size = len(kv["tokenizer.ggml.tokens"])

        start, end = compute_layer_range(stage, num_stages, total_blocks)

        # Pin EOS token ids (best-effort) for callers that want to detect
        # end-of-text from the sampled stream. We don't enforce stop here
        # — the Rust Stage-1 actor owns the EOS decision — but exposing
        # them in the ready line lets the orchestrator configure itself.
        tokens_list = kv.get("tokenizer.ggml.tokens", [])
        eos_ids: list[int] = []
        for i, tok in enumerate(tokens_list):
            if tok in ("<|end_of_text|>", "<|eot_id|>", "</s>", "<|endoftext|>"):
                eos_ids.append(i)

        # Tinygrad and numpy are kept as instance attributes so the
        # per-request handlers don't re-import them.
        self._np = np
        self._Tensor = Tensor
        self.model = model
        self.tokenizer = tokenizer
        self.hidden_dim = hidden_dim
        self.vocab_size = vocab_size
        self.total_blocks = total_blocks
        self.stage = stage
        self.num_stages = num_stages
        self.start = start
        self.end = end
        self.eos_ids = eos_ids

        print(
            f"pp_tinygrad_worker: stage={stage} ready "
            f"(blocks={total_blocks}, range=[{start},{end}), "
            f"hidden_dim={hidden_dim}, vocab={vocab_size}, eos={eos_ids})",
            file=sys.stderr,
            flush=True,
        )

    # --- per-stage forward ops -------------------------------------------

    def embed_and_forward(self, tokens: Sequence[int], position: int) -> tuple[bytes, int]:
        Tensor = self._Tensor
        t = Tensor([list(tokens)], dtype="int32")
        x = self.model.token_embd(t)
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        # Cast to half (2 bytes/elem) for the wire format, matching the
        # stub. The model's weights are float16; the op output may have
        # promoted to float32, so an explicit cast normalises this.
        x = x.cast("half").realize()
        arr = x.numpy()  # shape (1, seq_len, hidden_dim), dtype float16
        return arr.tobytes(), int(arr.shape[1])

    def forward_range(
        self, hidden_bytes: bytes, position: int, seq_len: int
    ) -> tuple[bytes, int]:
        """Run this middle stage's block range over an incoming hidden state.

        Input is a flat float16 buffer of shape ``(1, seq_len, hidden_dim)``;
        output is the same shape after applying ``model.blk[start:end]``
        with KV-cache ``position``. Returns ``(bytes, out_seq_len)``.
        """
        np = self._np
        Tensor = self._Tensor
        expected = seq_len * self.hidden_dim * BYTES_PER_ELEM
        if len(hidden_bytes) != expected:
            raise ValueError(
                f"hidden length {len(hidden_bytes)} does not match "
                f"seq_len*hidden_dim*2 ({seq_len}*{self.hidden_dim}*{BYTES_PER_ELEM} "
                f"= {expected})"
            )
        arr = (
            np.frombuffer(hidden_bytes, dtype=np.float16)
            .reshape((1, seq_len, self.hidden_dim))
            .copy()
        )
        x = Tensor(arr)
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        x = x.cast("half").realize()
        out = x.numpy()
        return out.tobytes(), int(out.shape[1])

    def forward_and_sample(
        self, hidden_bytes: bytes, position: int, seq_len: int
    ) -> int:
        np = self._np
        Tensor = self._Tensor
        expected = seq_len * self.hidden_dim * BYTES_PER_ELEM
        if len(hidden_bytes) != expected:
            raise ValueError(
                f"hidden length {len(hidden_bytes)} does not match "
                f"seq_len*hidden_dim*2 ({seq_len}*{self.hidden_dim}*{BYTES_PER_ELEM} "
                f"= {expected})"
            )
        arr = (
            np.frombuffer(hidden_bytes, dtype=np.float16)
            .reshape((1, seq_len, self.hidden_dim))
            .copy()
        )
        x = Tensor(arr)
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        x = self.model.output_norm(x)
        logits = self.model.output(x)
        # Argmax on the last position's logits. Matches what
        # ``Transformer.forward`` does at llm.py:178.
        token_id = int(logits[0, -1, :].argmax().item())
        return token_id

    def generate_full(self, prompt: str, max_tokens: int) -> list[int]:
        """End-to-end inference over the full block range.

        Used as the reference for the sliced-vs-full equivalence tests.
        Runs the same unjitted block iteration that the per-stage ops use
        (``embed_and_forward`` + ``forward_and_sample``), but on all
        ``model.blk`` blocks within a single process — so the result is
        bit-identical to a correctly-sliced pipeline with argmax sampling.

        Returns the ``max_tokens`` sampled token ids (excluding the
        prompt). Each invocation starts a fresh prefill at ``position=0``;
        the lazily-allocated per-block KV cache is overwritten in place
        as positions are revisited, so the same worker can serve multiple
        independent prompts.
        """
        Tensor = self._Tensor
        if max_tokens <= 0:
            return []
        prompt_tokens = self.tokenizer.encode(prompt)
        if len(prompt_tokens) == 0:
            raise ValueError("prompt tokenized to an empty list")

        def _forward_sample(token_ids: Sequence[int], position: int) -> int:
            t = Tensor([list(token_ids)], dtype="int32")
            x = self.model.token_embd(t)
            for block in self.model.blk[0 : self.total_blocks]:
                x = block(x, position)
            x = self.model.output_norm(x)
            logits = self.model.output(x)
            return int(logits[0, -1, :].argmax().item())

        out: list[int] = []
        # Prefill: full prompt at position 0; sample at the last position.
        next_id = _forward_sample(prompt_tokens, 0)
        out.append(next_id)
        # Autoregressive decode: feed the newly-sampled token at the
        # next position. This mirrors what ``Stage0Actor`` does over the
        # wire when ``NextToken { position, token_id }`` arrives.
        pos = len(prompt_tokens)
        for _ in range(max_tokens - 1):
            next_id = _forward_sample([next_id], pos)
            out.append(next_id)
            pos += 1
        return out


def _validate_tokens_list(tokens) -> str | None:
    """Return None if ``tokens`` is a list of non-bool ints, else an error msg."""
    if not isinstance(tokens, list):
        return "'tokens' must be a list of ints"
    for t in tokens:
        if not isinstance(t, int) or isinstance(t, bool) or t < 0:
            return "'tokens' must be a list of non-negative ints"
    return None


def _handle_request(
    req: dict,
    stage: int,
    num_stages: int,
    real_state: "_RealModelState | None" = None,
) -> dict:
    rid = req.get("request_id")
    if "op" not in req:
        return {"request_id": rid, "error": "missing 'op' field"}
    op = req["op"]
    is_first = stage == 0
    is_last = stage == num_stages - 1
    is_middle = not is_first and not is_last

    # Tokenize lives on the first stage; detokenize on the last. Routing
    # both through their natural roles avoids ambiguity when an N-stage
    # cluster has a tokenizer-bearing worker on every node (real mode).
    if op == "tokenize":
        if not is_first:
            return {
                "request_id": rid,
                "error": f"op {op!r} is only valid on stage 0 "
                f"(this worker is stage {stage} of {num_stages})",
            }
        prompt = req.get("prompt")
        if not isinstance(prompt, str):
            return {"request_id": rid, "error": "'prompt' must be a string"}
        if real_state is not None:
            tokens = real_state.tokenizer.encode(prompt)
        else:
            tokens = [
                int(t) for t in _stub_tokenize(prompt)
            ]
        return {"request_id": rid, "tokens": tokens}

    if op == "detokenize":
        if not is_last:
            return {
                "request_id": rid,
                "error": f"op {op!r} is only valid on the final stage "
                f"(this worker is stage {stage} of {num_stages})",
            }
        tokens = req.get("tokens")
        err = _validate_tokens_list(tokens)
        if err is not None:
            return {"request_id": rid, "error": err}
        if real_state is not None:
            text = real_state.tokenizer.decode(tokens)
        else:
            text = " ".join(str(t) for t in tokens)
        return {"request_id": rid, "text": text}

    if op in ("embed_and_forward", "decode_step"):
        if not is_first:
            return {
                "request_id": rid,
                "error": f"op {op!r} is only valid on stage 0 "
                f"(this worker is stage {stage} of {num_stages})",
            }
        if op == "embed_and_forward":
            tokens = req.get("tokens")
            position = req.get("position", 0)
            err = _validate_tokens_list(tokens)
            if err is not None:
                return {"request_id": rid, "error": err}
            if len(tokens) == 0:
                return {"request_id": rid, "error": "'tokens' must be non-empty"}
            if not _is_nonneg_int(position):
                return {"request_id": rid, "error": "'position' must be a non-negative int"}
            if real_state is not None:
                hidden, out_seq_len = real_state.embed_and_forward(tokens, position)
            else:
                hidden = _stub_hidden_bytes(tokens, position)
                out_seq_len = len(tokens)
            return {
                "request_id": rid,
                "hidden_b64": base64.b64encode(hidden).decode("ascii"),
                "seq_len": out_seq_len,
            }
        # decode_step
        token_id = req.get("token_id")
        position = req.get("position")
        if not _is_nonneg_int(token_id):
            return {"request_id": rid, "error": "'token_id' must be a non-negative int"}
        if not _is_nonneg_int(position):
            return {"request_id": rid, "error": "'position' must be a non-negative int"}
        if real_state is not None:
            hidden, out_seq_len = real_state.embed_and_forward([token_id], position)
        else:
            hidden = _stub_hidden_bytes([token_id], position)
            out_seq_len = 1
        return {
            "request_id": rid,
            "hidden_b64": base64.b64encode(hidden).decode("ascii"),
            "seq_len": out_seq_len,
        }

    if op == "forward_range":
        if not is_middle:
            return {
                "request_id": rid,
                "error": f"op {op!r} is only valid on a middle stage "
                f"(this worker is stage {stage} of {num_stages})",
            }
        hidden_b64 = req.get("hidden_b64")
        position = req.get("position")
        seq_len = req.get("seq_len")
        if not isinstance(hidden_b64, str):
            return {"request_id": rid, "error": "'hidden_b64' must be a string"}
        if not _is_nonneg_int(position):
            return {"request_id": rid, "error": "'position' must be a non-negative int"}
        if not _is_nonneg_int(seq_len) or seq_len == 0:
            return {"request_id": rid, "error": "'seq_len' must be a positive int"}
        try:
            hidden = base64.b64decode(hidden_b64, validate=True)
        except (base64.binascii.Error, ValueError) as e:
            return {"request_id": rid, "error": f"invalid base64 in hidden_b64: {e}"}
        hidden_dim = real_state.hidden_dim if real_state is not None else STUB_HIDDEN_DIM
        expected = seq_len * hidden_dim * BYTES_PER_ELEM
        if len(hidden) != expected:
            return {
                "request_id": rid,
                "error": (
                    f"hidden length {len(hidden)} does not match "
                    f"seq_len*hidden_dim*2 ({seq_len}*{hidden_dim}*{BYTES_PER_ELEM} "
                    f"= {expected})"
                ),
            }
        if real_state is not None:
            hidden_out, out_seq_len = real_state.forward_range(hidden, position, seq_len)
        else:
            hidden_out = _stub_forward_range_bytes(hidden, position, seq_len)
            out_seq_len = seq_len
        return {
            "request_id": rid,
            "hidden_b64": base64.b64encode(hidden_out).decode("ascii"),
            "seq_len": out_seq_len,
        }

    if op == "generate_full":
        # Reference path used by the sliced-vs-full equivalence tests. Always
        # runs over the full block range, so the worker's STAGE/NUM_STAGES
        # are ignored here — any ``NUM_STAGES >= 2`` works since the op
        # iterates ``model.blk`` directly. The example does not boot at
        # ``NUM_STAGES=1`` (single-node configurations belong to
        # ``examples/single-gpu-inference``).
        if real_state is None:
            return {
                "request_id": rid,
                "error": "op 'generate_full' requires real mode (stub mode has no full model)",
            }
        prompt = req.get("prompt")
        max_tokens = req.get("max_tokens")
        if not isinstance(prompt, str):
            return {"request_id": rid, "error": "'prompt' must be a string"}
        if not _is_nonneg_int(max_tokens) or max_tokens == 0:
            return {"request_id": rid, "error": "'max_tokens' must be a positive int"}
        try:
            tokens = real_state.generate_full(prompt, max_tokens)
        except Exception as e:
            return {"request_id": rid, "error": f"generate_full: {e}"}
        return {"request_id": rid, "tokens": tokens}

    if op == "forward_and_sample":
        if not is_last:
            return {
                "request_id": rid,
                "error": f"op {op!r} is only valid on the final stage "
                f"(this worker is stage {stage} of {num_stages})",
            }
        hidden_b64 = req.get("hidden_b64")
        position = req.get("position")
        seq_len = req.get("seq_len")
        if not isinstance(hidden_b64, str):
            return {"request_id": rid, "error": "'hidden_b64' must be a string"}
        if not _is_nonneg_int(position):
            return {"request_id": rid, "error": "'position' must be a non-negative int"}
        if not _is_nonneg_int(seq_len) or seq_len == 0:
            return {"request_id": rid, "error": "'seq_len' must be a positive int"}
        try:
            hidden = base64.b64decode(hidden_b64, validate=True)
        except (base64.binascii.Error, ValueError) as e:
            return {"request_id": rid, "error": f"invalid base64 in hidden_b64: {e}"}
        if real_state is not None:
            hidden_dim = real_state.hidden_dim
        else:
            hidden_dim = STUB_HIDDEN_DIM
        expected = seq_len * hidden_dim * BYTES_PER_ELEM
        if len(hidden) != expected:
            return {
                "request_id": rid,
                "error": (
                    f"hidden length {len(hidden)} does not match "
                    f"seq_len*hidden_dim*2 ({seq_len}*{hidden_dim}*{BYTES_PER_ELEM} "
                    f"= {expected})"
                ),
            }
        if real_state is not None:
            token_id = real_state.forward_and_sample(hidden, position, seq_len)
        else:
            token_id = _stub_token_id(hidden, position)
        return {"request_id": rid, "token_id": token_id}

    return {"request_id": rid, "error": f"unknown op {op!r}"}


def _stub_tokenize(prompt: str) -> list[int]:
    """Whitespace-split a prompt into deterministic small integer ids.

    Only used in stub mode. The exact mapping is not part of the
    worker's contract — callers just need a list of ints whose length
    equals the number of whitespace-separated words.
    """
    out: list[int] = []
    for i, word in enumerate(prompt.split()):
        s = sum(ord(c) for c in word)
        out.append((s % 1024) + i)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="pipeline-parallel tinygrad worker")
    parser.add_argument(
        "--stub",
        action="store_true",
        help="Stub mode: skip model loading, use deterministic in-memory ops",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model name (defaults to $MODEL or llama3.2:1b in real mode)",
    )
    args = parser.parse_args(argv)

    stub_mode = args.stub or os.environ.get("PP_WORKER_STUB", "").strip() == "1"

    stage = _parse_env_int("STAGE")
    num_stages = _parse_env_int("NUM_STAGES")
    if num_stages < 2:
        _die(
            f"NUM_STAGES must be >= 2 (single-node configurations are not "
            f"served by this example), got {num_stages}"
        )
    if not (0 <= stage < num_stages):
        _die(f"STAGE {stage} out of range [0, {num_stages})")

    real_state: _RealModelState | None = None
    if not stub_mode:
        model_name = (args.model or os.environ.get("MODEL", "")).strip() or "llama3.2:1b"
        try:
            real_state = _RealModelState(model_name, stage, num_stages)
        except SystemExit:
            raise
        except Exception as e:
            import traceback

            print(traceback.format_exc(), file=sys.stderr, flush=True)
            _die(f"failed to load model {model_name!r}: {e}")

    ready: dict = {"status": "ready", "pid": os.getpid(), "stage": stage}
    if real_state is not None:
        ready["hidden_dim"] = real_state.hidden_dim
        ready["vocab_size"] = real_state.vocab_size
        ready["total_blocks"] = real_state.total_blocks
        ready["layer_range"] = [real_state.start, real_state.end]
        ready["eos_token_ids"] = real_state.eos_ids
    _write(ready)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except (json.JSONDecodeError, ValueError) as e:
            _write({"error": f"invalid JSON: {e}"})
            continue
        if not isinstance(req, dict):
            _write({"error": f"request must be a JSON object, got {type(req).__name__}"})
            continue
        try:
            reply = _handle_request(req, stage, num_stages, real_state=real_state)
        except Exception as e:  # last-ditch safety net so the worker stays up
            import traceback

            print(traceback.format_exc(), file=sys.stderr, flush=True)
            reply = {"request_id": req.get("request_id"), "error": f"internal: {e}"}
        _write(reply)

    return 0


if __name__ == "__main__":
    sys.exit(main())
