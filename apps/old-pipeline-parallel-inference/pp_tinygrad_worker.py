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
import functools
import hashlib
import io
import json
import os
import re
import signal
import struct
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
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


# ggml type tables for the sharded loader: quantized types map to
# (elements_per_block, bytes_per_block); native types map to byte width. These
# mirror tinygrad 0.12.0's ggml_data_to_tensor and let us size each tensor's raw
# byte slice so only the kept weights are copied off disk.
_GGML_QUANT_BLOCK = {2: (32, 18), 3: (32, 20), 8: (32, 34), 12: (256, 144), 14: (256, 210), 39: (32, 17)}
_GGML_NATIVE_ITEMSIZE = {0: 4, 1: 2, 16: 1, 17: 2, 18: 4}


def _ggml_tensor_nbytes(n_elements: int, ggml_type: int) -> int:
    """Raw byte size of an ``n_elements`` ggml tensor of ``ggml_type``."""
    if ggml_type in _GGML_NATIVE_ITEMSIZE:
        return _GGML_NATIVE_ITEMSIZE[ggml_type] * n_elements
    if ggml_type in _GGML_QUANT_BLOCK:
        elems_per_block, bytes_per_block = _GGML_QUANT_BLOCK[ggml_type]
        return (n_elements // elems_per_block) * bytes_per_block
    raise ValueError(f"unsupported ggml type {ggml_type}")


# ─── Sharded download (spec §4.1 / §4.2 / §4.7) ───────────────────────────
#
# The worker fetches only the byte ranges its stage actually needs
# (§4.1), caches the partial GGUF idempotently (§4.2), and emits
# `pp_download_progress` events with bounded latency while downloading
# (§4.7). The cached file is a SPARSE file with the same apparent size
# as the source — kept tensors live at their original byte offsets, so
# `_load_sharded_transformer` opens it unchanged. Filesystem holes
# absorb the non-kept regions so the actual disk usage is
# O(per-stage shard size), not O(full model size).

_PP_DOWNLOAD_READ_CHUNK = 256 * 1024
_PP_HEADER_INITIAL = 1024 * 1024
_PP_HEADER_MAX = 64 * 1024 * 1024


def _pp_round_up(n: int, align: int) -> int:
    if align <= 0:
        return n
    return ((n + align - 1) // align) * align


class _PpHeaderTooShort(Exception):
    """Raised mid-parse when the header buffer ran out — caller grows it."""


def _pp_parse_gguf_header(buf: bytes) -> "tuple[list[tuple[str, tuple, int, int]], int, dict]":
    """Parse a GGUF header from `buf`. Returns (t_infos, data_start, kv).

    `t_infos` is a list of ``(name, dims, ggml_type, offset)`` tuples
    matching what ``_load_sharded_transformer``'s in-file parser
    produces. `data_start` is the absolute byte offset where tensor
    data begins. Raises `_PpHeaderTooShort` if the header is larger
    than `buf` — caller should re-fetch with a larger buffer.

    Format reference: tinygrad 0.12.0's gguf reader. GGUF versions 2
    and 3 share the parse shape; the file's u32 version is checked.
    """
    bio = io.BytesIO(buf)

    def _need(n: int) -> bytes:
        start = bio.tell()
        out = bio.read(n)
        if len(out) != n:
            raise _PpHeaderTooShort(f"need {n} bytes at {start}, got {len(out)}")
        return out

    def _unpack(fmt: str, nbytes: int):
        return struct.unpack(fmt, _need(nbytes))[0]

    def _read_u32() -> int:
        return _unpack("<I", 4)

    def _read_i32() -> int:
        return _unpack("<i", 4)

    def _read_u64() -> int:
        return _unpack("<Q", 8)

    def _read_str() -> str:
        length = _read_u64()
        return _need(length).decode("utf-8")

    def _read_arr():
        elem_type = _read_i32()
        count = _read_u64()
        return [_readers[elem_type]() for _ in range(count)]

    _readers = {
        0: lambda: _unpack("<b", 1),
        1: lambda: _unpack("<B", 1),
        2: lambda: _unpack("<h", 2),
        3: lambda: _unpack("<H", 2),
        4: _read_u32,
        5: _read_i32,
        6: lambda: _unpack("<f", 4),
        7: lambda: _unpack("<?", 1),
        8: _read_str,
        9: _read_arr,
        10: _read_u64,
        11: lambda: _unpack("<q", 8),
        12: lambda: _unpack("<d", 8),
    }

    magic = _need(4)
    if magic != b"GGUF":
        raise ValueError(f"not a GGUF artifact (magic={magic!r})")
    version = _read_i32()
    if version not in (2, 3):
        raise ValueError(f"unsupported GGUF version {version}")
    n_tensors = _read_u64()
    n_kv = _read_u64()

    kv: "dict[str, object]" = {}
    for _ in range(n_kv):
        key = _read_str()
        typ = _read_i32()
        kv[key] = _readers[typ]()

    t_infos: "list[tuple[str, tuple, int, int]]" = []
    for _ in range(n_tensors):
        name = _read_str()
        n_dims = _read_u32()
        dims = tuple(_read_u64() for _ in range(n_dims))
        ggml_type = _read_i32()
        offset = _read_u64()
        t_infos.append((name, dims, ggml_type, offset))

    alignment = int(kv.get("general.alignment", 32))
    data_start = _pp_round_up(bio.tell(), alignment)
    return t_infos, data_start, kv


def _pp_kept_names(t_infos, stage: int, num_stages: int, kv: dict) -> "set[str]":
    """Return the set of tensor names this stage requires. Matches the
    `_kept` predicate inside `_load_sharded_transformer` — both code
    paths must agree on the kept set or the loader would try to realize
    a tensor whose bytes were not fetched."""
    arch = str(kv["general.architecture"])
    total_blocks = int(kv[f"{arch}.block_count"])
    start, end = compute_layer_range(stage, num_stages, total_blocks)
    is_last = stage == num_stages - 1
    names = {info[0] for info in t_infos}
    tied_output = "output.weight" not in names
    kept: "set[str]" = set()
    for info in t_infos:
        name = info[0]
        keep = False
        for i in range(start, end):
            if name.startswith(f"blk.{i}."):
                keep = True
                break
        if not keep:
            if name == "token_embd.weight" and (stage == 0 or (is_last and tied_output)):
                keep = True
            elif name == "output_norm.weight" and is_last:
                keep = True
            elif name == "output.weight" and is_last and not tied_output:
                keep = True
        if keep:
            kept.add(name)
    return kept


def _pp_kept_byte_ranges(t_infos, data_start: int, kept_names: "set[str]") -> "list[tuple[int, int]]":
    """Return the ``[(absolute_offset, nbytes)]`` byte ranges this stage
    keeps, ordered by offset (so a streamed download writes ascending
    offsets and the filesystem allocates fewer fragmented holes)."""
    out: list[tuple[int, int]] = []
    for name, dims, ggml_type, offset in t_infos:
        if name not in kept_names:
            continue
        n_elements = 1
        for d in dims:
            n_elements *= int(d)
        nbytes = _ggml_tensor_nbytes(n_elements, ggml_type)
        out.append((data_start + int(offset), nbytes))
    out.sort()
    return out


def _pp_cache_paths(url: str) -> "tuple[Path, Path, Path]":
    """Return (cache_path, meta_path, partial_path) for ``url``.

    Cache root defaults to ``$PP_MODEL_CACHE_DIR`` then ``~/.cache/pp-pipeline``.
    The filename is ``<short-url-hash>-<basename>`` so two URLs that share a
    basename cannot collide.
    """
    raw = os.environ.get("PP_MODEL_CACHE_DIR", "").strip()
    cache_root = Path(raw).expanduser() if raw else Path.home() / ".cache" / "pp-pipeline"
    cache_root.mkdir(parents=True, exist_ok=True)
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    basename = os.path.basename(urllib.parse.urlparse(url).path) or "model.gguf"
    cache_path = cache_root / f"{url_hash}-{basename}"
    meta_path = cache_path.with_name(cache_path.name + ".pp_meta")
    partial_path = cache_path.with_name(cache_path.name + ".partial")
    return cache_path, meta_path, partial_path


def _pp_head(url: str) -> "tuple[int, str]":
    """HEAD request; return (Content-Length, Accept-Ranges header lowercased)."""
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=30) as resp:
        total = int(resp.headers.get("Content-Length", "0"))
        accept = (resp.headers.get("Accept-Ranges") or "").lower()
    return total, accept


def _pp_range_get(url: str, start: int, end_inclusive: int) -> bytes:
    """Issue a Range GET; return the body bytes."""
    req = urllib.request.Request(
        url, headers={"Range": f"bytes={start}-{end_inclusive}"}
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def _pp_emit_progress(stage: int, bytes_done: int, bytes_total: int, started_at_mono: float) -> None:
    """Emit one `pp_download_progress` event with the spec's field set
    (§4.7). `started_at_mono` is `time.monotonic()` captured before the
    first event so `elapsed_ms` is monotonic across the fetch."""
    elapsed_ms = int((time.monotonic() - started_at_mono) * 1000)
    mbps = round(((bytes_done * 8) / 1_000_000) / max(elapsed_ms / 1000, 1e-3), 1)
    _emit_event(
        "pp_download_progress",
        stage_index=stage,
        bytes_done=bytes_done,
        bytes_total=bytes_total,
        elapsed_ms=elapsed_ms,
        mbps=mbps,
    )


def _pp_fingerprint(f, offset: int, nbytes: int) -> str:
    """Hash the first 4KB + last 4KB of a kept tensor (or the whole
    tensor if shorter). The §4.2 content-derived check: matches the
    fingerprint recorded in the sidecar at download time."""
    sample = 4096
    f.seek(offset)
    head = f.read(min(sample, nbytes))
    if nbytes > sample:
        f.seek(offset + nbytes - sample)
        tail = f.read(sample)
    else:
        tail = b""
    h = hashlib.sha256()
    h.update(head)
    h.update(tail)
    h.update(nbytes.to_bytes(8, "little"))
    return h.hexdigest()


def _pp_write_meta(
    meta_path: Path,
    cache_path: Path,
    stage: int,
    num_stages: int,
    url: str,
    total_size: int,
    kept_ranges: "list[tuple[int, int]]",
) -> None:
    fingerprints = []
    with open(cache_path, "rb") as f:
        for offset, nbytes in kept_ranges:
            fingerprints.append({
                "offset": offset,
                "nbytes": nbytes,
                "fingerprint": _pp_fingerprint(f, offset, nbytes),
            })
    meta_path.write_text(json.dumps({
        "schema": 1,
        "url": url,
        "stage": stage,
        "num_stages": num_stages,
        "total_size": total_size,
        "kept": fingerprints,
    }))


def _pp_verify_cache(
    cache_path: Path,
    meta_path: Path,
    stage: int,
    num_stages: int,
) -> bool:
    """Spec §4.2 integrity check: file present at the expected apparent
    size AND every recorded fingerprint re-matches the cached bytes.
    Returns False on any discrepancy (including missing files, missing
    sidecar, mismatched stage / num_stages, size mismatch, or any
    fingerprint mismatch). A passing cache is used as-is — no refetch."""
    if not cache_path.exists() or not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text())
    except (OSError, ValueError):
        return False
    if meta.get("schema") != 1:
        return False
    if meta.get("stage") != stage or meta.get("num_stages") != num_stages:
        return False
    if cache_path.stat().st_size != meta.get("total_size"):
        return False
    kept = meta.get("kept") or []
    if not kept:
        return False
    try:
        with open(cache_path, "rb") as f:
            for entry in kept:
                offset = int(entry["offset"])
                nbytes = int(entry["nbytes"])
                expected = entry["fingerprint"]
                if _pp_fingerprint(f, offset, nbytes) != expected:
                    return False
    except (OSError, KeyError, ValueError):
        return False
    return True


def _pp_download_sharded(url: str, stage: int, num_stages: int) -> str:
    """Spec §4.1 + §4.2 + §4.7: fetch only this stage's tensor bytes,
    cache idempotently, emit progress events.

    Returns the path to the on-disk file (sparse — apparent size matches
    the source; only the kept ranges occupy disk blocks). On cache hit,
    NO `pp_download_progress` events are emitted (spec §4.7).
    """
    cache_path, meta_path, partial_path = _pp_cache_paths(url)

    # §4.2: orphan-cleanup any leftover .partial from a previous killed
    # fetch BEFORE any new fetch is initiated. Emit an event so the
    # bundle reader can see that a stale temp was reaped.
    if partial_path.exists():
        try:
            partial_path.unlink()
            _emit_event(
                "pp_cache_orphan_cleaned",
                stage_index=stage,
                path=str(partial_path),
            )
        except OSError:
            pass

    # §4.2 cache hit — return the cached file as-is.
    if _pp_verify_cache(cache_path, meta_path, stage, num_stages):
        _emit_event(
            "pp_cache_hit",
            stage_index=stage,
            path=str(cache_path),
        )
        return str(cache_path)

    # §4.1 fail-fast on no-range support.
    total_size, accept_ranges = _pp_head(url)
    if "bytes" not in accept_ranges:
        _emit_event(
            "pp_download_failed",
            stage_index=stage,
            reason="no_byte_range_support",
            url=url,
            accept_ranges=accept_ranges,
        )
        _die(f"source does not support byte-range requests: {url}")
    if total_size <= 0:
        _emit_event(
            "pp_download_failed",
            stage_index=stage,
            reason="no_content_length",
            url=url,
        )
        _die(f"source did not advertise Content-Length: {url}")

    # Fetch the header in growing increments until we can parse it.
    header_size = min(_PP_HEADER_INITIAL, total_size)
    while True:
        try:
            header_bytes = _pp_range_get(url, 0, header_size - 1)
            t_infos, data_start, kv = _pp_parse_gguf_header(header_bytes)
            if data_start <= len(header_bytes):
                break
            # Parser succeeded structurally but data_start sits past our
            # buffer — re-fetch enough to include the tensor data start.
            header_size = min(data_start + 1024, total_size)
        except _PpHeaderTooShort:
            new_size = min(header_size * 2, total_size)
            if new_size == header_size or new_size > _PP_HEADER_MAX:
                _emit_event(
                    "pp_download_failed",
                    stage_index=stage,
                    reason="header_too_large",
                    header_size=header_size,
                )
                _die(f"GGUF header exceeded {_PP_HEADER_MAX} bytes")
            header_size = new_size

    kept_names = _pp_kept_names(t_infos, stage, num_stages, kv)
    kept_ranges = _pp_kept_byte_ranges(t_infos, data_start, kept_names)

    # bytes_total is the bytes this stage will pull from the network:
    # header + kept-tensor regions. Not the full file (spec §4.1 means
    # we never fetch the rest).
    header_keep_bytes = data_start
    kept_bytes_total = sum(nb for _, nb in kept_ranges)
    bytes_total = header_keep_bytes + kept_bytes_total

    interval_s_raw = os.environ.get("PP_DOWNLOAD_PROGRESS_INTERVAL_SECS", "").strip()
    try:
        interval_s = float(interval_s_raw) if interval_s_raw else 10.0
    except ValueError:
        interval_s = 10.0
    if interval_s <= 0:
        interval_s = 10.0

    # Write the sparse output to `.partial`; rename on success. Opening
    # with "wb" then truncate(total_size) creates a sparse file on
    # Linux: only blocks we actually `write()` allocate disk.
    started_at = time.monotonic()
    with open(partial_path, "wb") as f:
        f.truncate(total_size)
        f.seek(0)
        f.write(header_bytes[:data_start])
        bytes_done = data_start

        # Spec §4.7: first event MUST be at start of fetch, AFTER we
        # know bytes_total. We have that now.
        _pp_emit_progress(stage, bytes_done, bytes_total, started_at)
        last_emit = time.monotonic()

        for offset, nbytes in kept_ranges:
            req = urllib.request.Request(
                url,
                headers={"Range": f"bytes={offset}-{offset + nbytes - 1}"},
            )
            try:
                resp = urllib.request.urlopen(req, timeout=300)
            except Exception as e:
                _emit_event(
                    "pp_download_failed",
                    stage_index=stage,
                    reason="range_get_failed",
                    offset=offset,
                    nbytes=nbytes,
                    error=str(e),
                )
                # Spec §4.7: final event MUST be emitted on fetch failure.
                _pp_emit_progress(stage, bytes_done, bytes_total, started_at)
                _die(f"range GET failed at offset {offset}: {e}")
            try:
                f.seek(offset)
                while True:
                    chunk = resp.read(_PP_DOWNLOAD_READ_CHUNK)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_done += len(chunk)
                    now = time.monotonic()
                    if now - last_emit >= interval_s:
                        _pp_emit_progress(stage, bytes_done, bytes_total, started_at)
                        last_emit = now
            finally:
                resp.close()

    # Spec §4.7: final event at completion.
    _pp_emit_progress(stage, bytes_done, bytes_total, started_at)

    # Sidecar before rename so a crash between rename + meta-write does
    # not leave a "valid file, no sidecar" → would fail _pp_verify and
    # refetch. Writing the sidecar first means a crash here leaves
    # cache_path absent and partial_path present (which orphan-cleanup
    # reaps on next boot).
    _pp_write_meta(
        meta_path, partial_path, stage, num_stages, url, total_size, kept_ranges
    )
    os.replace(partial_path, cache_path)
    return str(cache_path)


def _load_sharded_transformer(gguf_path, stage: int, num_stages: int, max_context: int = 512):
    """Load only this stage's slice of the model onto the compute device.

    Stock ``Transformer.from_gguf`` copies the *entire* GGUF onto the compute
    device before any layer runs (it does ``gguf.to(None)``), so an 18 GB model
    OOMs a 12 GB GPU no matter how the layers are split. Instead we parse the
    GGUF header on the DISK device and copy only the tensors this stage needs —
    ``blk[start:end]`` plus ``token_embd`` (stage 0) and ``output_norm`` /
    ``output`` (last stage) — dequantizing each on the compute device. Returns
    ``(model, kv, start, end)``. Vendored against tinygrad 0.12.0's gguf format.
    """
    import io
    import struct
    import functools

    from tinygrad import Tensor, Device, nn
    from tinygrad.helpers import prod, round_up, getenv
    from tinygrad.nn.state import TensorIO, ggml_data_to_tensor
    from tinygrad.apps.llm import Transformer

    _t0 = time.monotonic()
    gguf = Tensor(Path(gguf_path))  # device is DISK:<path> — nothing is copied to the GPU yet

    # --- parse the GGUF header (kv metadata + tensor directory) off disk ---
    reader = io.BufferedReader(TensorIO(gguf), 1_000_000)

    def _unpack(fmt, nbytes):
        return struct.unpack(fmt, reader.read(nbytes))[0]

    def _read_str():
        return str(reader.read(_read_u64()), "utf-8")

    def _read_arr():
        elem_reader, count = _readers[_read_i32()], _read_u64()
        return [elem_reader() for _ in range(count)]

    _readers = {8: _read_str, 9: _read_arr, **{t: functools.partial(_unpack, "<" + f, nb) for t, f, nb in
        [(0, "c", 1), (1, "b", 1), (2, "H", 2), (3, "h", 2), (4, "I", 4), (5, "i", 4),
         (6, "f", 4), (7, "?", 1), (10, "Q", 8), (11, "q", 8), (12, "d", 8)]}}
    _read_u32, _read_i32, _read_u64 = _readers[4], _readers[5], _readers[10]

    magic, version = reader.read(4), _read_i32()
    n_tensors, n_kv = _read_u64(), _read_u64()
    if magic != b"GGUF" or version not in (2, 3):
        raise ValueError(f"invalid GGUF (magic={magic!r} version={version})")
    kv = {}
    for _ in range(n_kv):
        key, typ = _read_str(), _read_i32()
        kv[key] = _readers[typ]()
    t_infos = [(_read_str(), tuple(_read_u64() for _ in range(_read_u32())), _read_i32(), _read_u64())
               for _ in range(n_tensors)]
    data_start = round_up(reader.tell(), kv.get("general.alignment", 32))
    _t_header = time.monotonic()

    arch = kv["general.architecture"]
    total_blocks = int(kv[f"{arch}.block_count"])
    start, end = compute_layer_range(stage, num_stages, total_blocks)
    is_last = stage == num_stages - 1
    names = {info[0] for info in t_infos}
    tied_output = "output.weight" not in names  # small models tie output to token_embd

    def _kept(name: str) -> bool:
        for i in range(start, end):
            if name.startswith(f"blk.{i}."):
                return True
        if name == "token_embd.weight" and (stage == 0 or (is_last and tied_output)):
            return True
        if name == "output_norm.weight" and is_last:
            return True
        if name == "output.weight" and is_last and not tied_output:
            return True
        return False

    half, device = getenv("HALF", 1), Device.DEFAULT
    state_dict = {}
    bytes_copied = 0
    kept_count = 0
    for name, dims, ggml_type, offset in t_infos:
        n_elements = prod(dims)
        if _kept(name):
            nbytes = _ggml_tensor_nbytes(n_elements, ggml_type)
            bytes_copied += nbytes
            kept_count += 1
            raw = gguf[data_start + offset: data_start + offset + nbytes].to(device)
            tensor = ggml_data_to_tensor(raw, n_elements, ggml_type).reshape(*reversed(dims))
            if arch == "llama":  # interleaved -> half-split RoPE layout (llama-style only)
                n_heads, n_kv_heads = kv[f"{arch}.attention.head_count"], kv[f"{arch}.attention.head_count_kv"]
                if "attn_q.weight" in name:
                    tensor = tensor.rearrange("(n h two) d -> (n two h) d", n=n_heads, two=2)
                if "attn_k.weight" in name:
                    tensor = tensor.rearrange("(n h two) d -> (n two h) d", n=n_kv_heads, two=2)
            state_dict[name] = tensor.cast("float16") if half else tensor
        else:
            # DISK-rooted lazy tensor: only its .shape is read (model construction); never realized.
            state_dict[name] = ggml_data_to_tensor(gguf[data_start + offset:], n_elements, ggml_type).reshape(*reversed(dims))
    if tied_output and is_last:
        state_dict["output.weight"] = state_dict["token_embd.weight"]
    _t_statedict = time.monotonic()

    n_heads = kv[f"{arch}.attention.head_count"]
    model = Transformer(
        num_blocks=total_blocks, dim=kv[f"{arch}.embedding_length"],
        hidden_dim=kv.get(f"{arch}.expert_feed_forward_length", kv[f"{arch}.feed_forward_length"]),
        n_heads=n_heads, n_kv_heads=kv[f"{arch}.attention.head_count_kv"],
        norm_eps=kv[f"{arch}.attention.layer_norm_rms_epsilon"], vocab_size=len(kv["tokenizer.ggml.tokens"]),
        head_dim=kv.get(f"{arch}.attention.key_length", kv[f"{arch}.embedding_length"] // n_heads),
        rope_theta=kv[f"{arch}.rope.freq_base"], max_context=max_context,
        qk_norm=int(state_dict["blk.0.attn_q_norm.weight"].shape[0]) if "blk.0.attn_q_norm.weight" in state_dict else 0,
        num_experts=kv.get(f"{arch}.expert_count", 0), num_experts_per_tok=kv.get(f"{arch}.expert_used_count", 0))

    _t_construct = time.monotonic()

    # Provide only the kept weights; strict=False leaves the other blocks at their
    # (lazy, never-run) init so they never touch the compute device.
    kept = {name: tensor for name, tensor in state_dict.items()
            if _kept(name) or (tied_output and is_last and name == "output.weight")}
    # This is where the kept tensors are actually copied off disk and
    # dequantized on the compute device — the dominant load cost.
    nn.state.load_state_dict(model, kept, strict=False, verbose=False, consume=True, realize=True)
    _t_realize = time.monotonic()

    realize_ms = (_t_realize - _t_construct) * 1000
    _emit_event(
        "model_load_breakdown",
        stage=stage,
        resident_blocks=end - start,
        total_blocks=total_blocks,
        kept_tensors=kept_count,
        bytes_copied=bytes_copied,
        mb_copied=round(bytes_copied / 1_000_000, 1),
        header_ms=round((_t_header - _t0) * 1000, 1),
        statedict_build_ms=round((_t_statedict - _t_header) * 1000, 1),
        construct_ms=round((_t_construct - _t_statedict) * 1000, 1),
        realize_ms=round(realize_ms, 1),
        realize_mb_per_s=round((bytes_copied / 1_000_000) / max(realize_ms / 1000, 1e-3), 1),
        rss_mb=_rss_mb(),
    )
    return model, kv, start, end


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


# ─── Lifecycle event emission ─────────────────────────────────────────────
#
# The Rust StageActor parses every stdout line as JSON; any line carrying
# `"event": "<kind>"` is re-emitted as `Custom("worker_<kind>")` into the
# diagnostic bundle. The worker subprocess is otherwise opaque to the
# Rust side, so these are the only diagnostic signal the bundle ever sees
# from the Python layer (apart from exit code + stderr tail). We do NOT
# emit a `request_id` on event lines so the actor never confuses an event
# with an op reply.

_WORKER_START_MONOTONIC = time.monotonic()
_REQUESTS_SERVED = 0


def _emit_event(kind: str, **fields) -> None:
    """Emit a structured lifecycle event on stdout. The Rust actor folds
    these into the diag bundle as `Custom("worker_<kind>")`."""
    payload = {"event": kind, **fields}
    try:
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()
    except Exception:
        # Best-effort: never let a logging failure crash the worker.
        pass


def _uptime_ms() -> int:
    return int((time.monotonic() - _WORKER_START_MONOTONIC) * 1000)


def _wall_ms() -> int:
    """Epoch milliseconds. Lets the bundle align worker events across nodes
    and against the orchestrator's vast.ai create/lease timestamps — e.g.
    (worker `starting`.wall_ms − instance create_ms) is the image-pull +
    container-boot + worker-spawn cost the node can't see itself."""
    return int(time.time() * 1000)


def _rss_mb() -> "int | None":
    """Resident-set size in MB, read from /proc/self/status (Linux).
    Returns None on non-Linux or when the read fails — the field is
    informational, never required."""
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    # VmRSS:    12345 kB
                    return int(parts[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _install_excepthook() -> None:
    """Catch every uncaught exception and emit a structured event before
    the interpreter prints the traceback to stderr (which the actor's
    ring buffer will also capture)."""

    def _hook(exc_type, exc_value, exc_tb):
        import traceback

        tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        _emit_event(
            "uncaught_exception",
            type=exc_type.__name__,
            value=str(exc_value),
            traceback=tb_text,
            uptime_ms=_uptime_ms(),
        )
        # Preserve the default behaviour so stderr still shows the trace
        # (the actor's stderr ring buffer is a belt-and-braces backup).
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = _hook


def _install_signal_handlers() -> None:
    """Emit `signal_received` and exit cleanly on SIGTERM/SIGINT.
    SIGKILL and SIGSEGV cannot be caught — the Rust side relies on the
    exit code / signal field of the eventual `worker_exited` Custom
    event for those."""

    def _on_signal(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = f"signal-{signum}"
        _emit_event(
            "signal_received",
            signum=signum,
            name=name,
            uptime_ms=_uptime_ms(),
        )
        # 128 + signum is the conventional exit code for signal-driven
        # termination; matches what /bin/sh reports.
        sys.exit(128 + signum)

    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(s, _on_signal)
        except (ValueError, OSError):
            # Some environments (e.g. non-main thread) don't allow
            # signal install — silently skip rather than crash here.
            pass


def _start_heartbeat(interval_s: float = 30.0) -> None:
    """Daemon thread emitting `heartbeat` events. Lets the post-processor
    distinguish *hung* (heartbeats stop but process alive — no exit event)
    from *dead* (no heartbeat AND no exit — likely SIGKILL/SIGSEGV)."""

    def _loop():
        while True:
            time.sleep(interval_s)
            _emit_event(
                "heartbeat",
                uptime_ms=_uptime_ms(),
                rss_mb=_rss_mb(),
                requests_served=_REQUESTS_SERVED,
            )

    t = threading.Thread(target=_loop, name="pp-worker-heartbeat", daemon=True)
    t.start()


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
        # Import tinygrad lazily so stub-mode never touches it. This is
        # the most likely crash site in real mode — emit lifecycle
        # events around the import so the bundle records exactly when
        # the worker started loading and how long it took.
        _emit_event("importing_tinygrad", stage=stage, wall_ms=_wall_ms())
        _import_start = time.monotonic()
        import numpy as np
        from tinygrad import Tensor, Device
        from tinygrad.helpers import fetch, getenv
        from tinygrad.apps.llm import SimpleTokenizer, models

        import_ms = int((time.monotonic() - _import_start) * 1000)
        _emit_event("tinygrad_imported", stage=stage, elapsed_ms=import_ms)
        # Device + precision context: which backend the shard lands on and the
        # toggles (HALF/JIT/BEAM) that dominate load + inference cost. Correlate
        # with model_load_breakdown / op events to attribute time to dequant vs
        # kernel compile vs steady-state matmul.
        _emit_event(
            "device",
            stage=stage,
            default_device=str(Device.DEFAULT),
            half=getenv("HALF", 1),
            jit=getenv("JIT", 1),
            beam=getenv("BEAM", 0),
            cuda_visible=os.environ.get("CUDA_VISIBLE_DEVICES"),
        )

        if model_name not in models:
            available = ", ".join(sorted(models.keys()))
            _die(f"unknown MODEL {model_name!r}; available: {available}")

        url = models[model_name]
        _emit_event("fetching_model", stage=stage, model=model_name, url=url, wall_ms=_wall_ms())
        print(
            f"pp_tinygrad_worker: stage={stage}/{num_stages} fetching {model_name}",
            file=sys.stderr,
            flush=True,
        )
        _fetch_start = time.monotonic()
        # Spec §4.1: download only this stage's tensor byte ranges.
        # Spec §4.2: cache idempotently with an integrity check; a
        # complete-and-valid cache MUST NOT trigger a network fetch.
        # Spec §4.7: `pp_download_progress` events are emitted by
        # `_pp_download_sharded` while the fetch is in progress and
        # NEVER on a cache hit. The unused `fetch` import remains as
        # documentation of the prior code path; the sharded fetcher
        # replaces it.
        _ = fetch  # silence the linter; kept for the diff reader
        gguf_path = _pp_download_sharded(url, stage, num_stages)
        fetch_ms = int((time.monotonic() - _fetch_start) * 1000)
        # `gguf_bytes` is the file's apparent size (matches the source's
        # total_size); actual on-disk usage is O(per-stage shard). Bundle
        # readers reading `model_fetched.gguf_bytes` see the same value
        # they did before the §4.1 change — the per-stage usage shows up
        # in `pp_download_progress.bytes_total` (header + kept ranges).
        try:
            gguf_bytes = os.path.getsize(gguf_path)
        except OSError:
            gguf_bytes = 0
        fetch_mb = gguf_bytes / 1_000_000
        # A near-instant return with a non-zero apparent size means the
        # sharded cache hit short-circuited the fetch. Distinguishable
        # in the bundle from a real download via the presence (or not)
        # of `pp_download_progress` events.
        cache_hit = gguf_bytes > 0 and fetch_ms < 2000
        download_mb_per_s = None if cache_hit else round(fetch_mb / max(fetch_ms / 1000, 1e-3), 1)
        _emit_event(
            "model_fetched",
            stage=stage,
            elapsed_ms=fetch_ms,
            gguf_path=str(gguf_path),
            gguf_bytes=gguf_bytes,
            gguf_mb=round(fetch_mb, 1),
            download_mb_per_s=download_mb_per_s,
            cache_hit=cache_hit,
        )
        print(
            f"pp_tinygrad_worker: stage={stage} loading model from {gguf_path}",
            file=sys.stderr,
            flush=True,
        )
        _emit_event("loading_model", stage=stage, model=model_name, wall_ms=_wall_ms())
        _load_start = time.monotonic()
        # Shard at load time: only this stage's block range (+ embed/output on the
        # end stages) is copied to the compute device, so an 18 GB model fits on a
        # 12 GB GPU. See _load_sharded_transformer for why stock from_gguf can't.
        # The loader emits its own `model_load_breakdown` (header/dequant/realize).
        model, kv, start, end = _load_sharded_transformer(
            gguf_path, stage, num_stages, max_context=512
        )
        load_ms = int((time.monotonic() - _load_start) * 1000)
        tokenizer = SimpleTokenizer.from_gguf_kv(kv)

        arch = kv["general.architecture"]
        hidden_dim = int(kv[f"{arch}.embedding_length"])
        total_blocks = int(kv[f"{arch}.block_count"])
        vocab_size = len(kv["tokenizer.ggml.tokens"])

        _emit_event(
            "model_loaded",
            stage=stage,
            elapsed_ms=load_ms,
            rss_mb=_rss_mb(),
            blocks_resident=end - start,
            total_blocks=total_blocks,
        )
        # Stash the cold-start breakdown so main() can emit one `boot_profile`
        # summary once the worker is ready (import + fetch + load + total).
        self.timing = {
            "import_ms": import_ms,
            "fetch_ms": fetch_ms,
            "fetch_cache_hit": cache_hit,
            "gguf_bytes": gguf_bytes,
            "download_mb_per_s": download_mb_per_s,
            "load_ms": load_ms,
        }

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
        # Per-op compute breakdown (deserialize / compute / host-copy ms), set by
        # the forward ops and folded into the serve loop's `op` timing event.
        self._last_compute: "dict | None" = None

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
        t0 = time.monotonic()
        t = Tensor([list(tokens)], dtype="int32")
        x = self.model.token_embd(t)
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        # Cast to half (2 bytes/elem) for the wire format, matching the
        # stub. The model's weights are float16; the op output may have
        # promoted to float32, so an explicit cast normalises this. tinygrad is
        # lazy: embed + blocks + cast all *execute* at .realize() below (incl.
        # JIT kernel compile on the first call), so compute_ms captures them.
        t1 = time.monotonic()
        x = x.cast("half").realize()
        t2 = time.monotonic()
        arr = x.numpy()  # shape (1, seq_len, hidden_dim), dtype float16
        out = arr.tobytes()
        t3 = time.monotonic()
        self._last_compute = {
            "build_ms": round((t1 - t0) * 1000, 2),
            "compute_ms": round((t2 - t1) * 1000, 2),
            "host_copy_ms": round((t3 - t2) * 1000, 2),
            "out_bytes": len(out),
        }
        return out, int(arr.shape[1])

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
        t0 = time.monotonic()
        arr = (
            np.frombuffer(hidden_bytes, dtype=np.float16)
            .reshape((1, seq_len, self.hidden_dim))
            .copy()
        )
        x = Tensor(arr)
        t1 = time.monotonic()
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        x = x.cast("half").realize()
        t2 = time.monotonic()
        out_arr = x.numpy()
        out = out_arr.tobytes()
        t3 = time.monotonic()
        self._last_compute = {
            "deserialize_ms": round((t1 - t0) * 1000, 2),
            "compute_ms": round((t2 - t1) * 1000, 2),
            "host_copy_ms": round((t3 - t2) * 1000, 2),
            "in_bytes": len(hidden_bytes),
            "out_bytes": len(out),
        }
        return out, int(out_arr.shape[1])

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
        t0 = time.monotonic()
        arr = (
            np.frombuffer(hidden_bytes, dtype=np.float16)
            .reshape((1, seq_len, self.hidden_dim))
            .copy()
        )
        x = Tensor(arr)
        t1 = time.monotonic()
        for block in self.model.blk[self.start : self.end]:
            x = block(x, position)
        x = self.model.output_norm(x)
        logits = self.model.output(x)
        # Argmax on the last position's logits. Matches what
        # ``Transformer.forward`` does at llm.py:178. The .item() forces the
        # blocks + output projection (over the full vocab) to execute here.
        token_id = int(logits[0, -1, :].argmax().item())
        t2 = time.monotonic()
        self._last_compute = {
            "deserialize_ms": round((t1 - t0) * 1000, 2),
            "compute_ms": round((t2 - t1) * 1000, 2),
            "in_bytes": len(hidden_bytes),
            "token_id": token_id,
        }
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
        if self.start != 0 or self.end != self.total_blocks:
            raise RuntimeError(
                "generate_full needs the whole model resident, but this stage only "
                f"holds blk[{self.start}:{self.end}) of {self.total_blocks}. Use the "
                "per-stage ops (embed_and_forward / forward_range / forward_and_sample)."
            )
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
    # Install diagnostic hooks first thing so any failure during arg
    # parsing or env validation still produces a structured event.
    _install_excepthook()
    _install_signal_handlers()

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

    _emit_event(
        "starting",
        pid=os.getpid(),
        stage=stage,
        num_stages=num_stages,
        stub=stub_mode,
        model=(args.model or os.environ.get("MODEL", "")).strip() or None,
        python_version=sys.version.split()[0],
        argv=list(sys.argv),
        # wall_ms anchors this worker's boot against the orchestrator's vast.ai
        # instance create/lease time — the only way to measure image-pull +
        # container-boot latency, which the worker can't observe directly.
        wall_ms=_wall_ms(),
        host=os.uname().nodename,
    )
    _start_heartbeat()

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

            tb_text = traceback.format_exc()
            _emit_event(
                "model_load_failed",
                stage=stage,
                model=model_name,
                type=type(e).__name__,
                value=str(e),
                traceback=tb_text,
            )
            print(tb_text, file=sys.stderr, flush=True)
            _die(f"failed to load model {model_name!r}: {e}")

    ready: dict = {"status": "ready", "pid": os.getpid(), "stage": stage}
    if real_state is not None:
        ready["hidden_dim"] = real_state.hidden_dim
        ready["vocab_size"] = real_state.vocab_size
        ready["total_blocks"] = real_state.total_blocks
        ready["layer_range"] = [real_state.start, real_state.end]
        ready["eos_token_ids"] = real_state.eos_ids
    _write(ready)
    # Mirror ready as a structured event under the spec's event name
    # (§4.6 / §6.9: per-stage filter on `pp_worker_ready`). The
    # `pp_*` event kind is passed through unchanged by the Rust actor
    # so the bundle records `Custom("pp_worker_ready")`, matching the
    # name the orchestrator-side wired check filters on. The protocol
    # `status: "ready"` line above is unchanged for the actor's ready
    # signal.
    _emit_event(
        "pp_worker_ready",
        pid=os.getpid(),
        stage_index=stage,
        uptime_ms=_uptime_ms(),
        rss_mb=_rss_mb(),
    )
    # One-stop cold-start breakdown so a single event answers "where did
    # bring-up time go" per node: import + (fetch|cache) + load == time-to-ready.
    if real_state is not None:
        _emit_event(
            "boot_profile",
            stage=stage,
            total_to_ready_ms=_uptime_ms(),
            rss_mb=_rss_mb(),
            blocks_resident=real_state.end - real_state.start,
            **real_state.timing,
        )

    global _REQUESTS_SERVED
    seen_ops: set = set()
    last_op_end = time.monotonic()
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
        op = req.get("op")
        if real_state is not None:
            real_state._last_compute = None
        t_start = time.monotonic()
        # idle_ms_before is the pipeline bubble: how long this worker sat
        # blocked on its upstream stage between finishing the last op and
        # receiving this one. High idle => the bottleneck is elsewhere.
        idle_ms = round((t_start - last_op_end) * 1000, 2)
        try:
            reply = _handle_request(req, stage, num_stages, real_state=real_state)
        except Exception as e:  # last-ditch safety net so the worker stays up
            import traceback

            print(traceback.format_exc(), file=sys.stderr, flush=True)
            reply = {"request_id": req.get("request_id"), "error": f"internal: {e}"}
        _write(reply)
        t_end = time.monotonic()
        # Per-op trace — the granular signal for end-to-end latency. `first_call`
        # flags the JIT-compile-bearing first invocation of each op (kernels are
        # compiled once, then cached). `rid` (not `request_id`) keeps the Rust
        # actor from ever mistaking this event line for an op reply.
        is_first = op not in seen_ops
        seen_ops.add(op)
        _emit_event(
            "op",
            op=op,
            rid=req.get("request_id"),
            stage=stage,
            duration_ms=round((t_end - t_start) * 1000, 2),
            idle_ms_before=idle_ms,
            first_call=is_first,
            ok=isinstance(reply, dict) and "error" not in reply,
            in_tokens=len(req["tokens"]) if isinstance(req.get("tokens"), list) else None,
            in_seq_len=req.get("seq_len"),
            out_seq_len=reply.get("seq_len") if isinstance(reply, dict) else None,
            compute=(real_state._last_compute if real_state is not None else None),
            uptime_ms=_uptime_ms(),
        )
        last_op_end = t_end
        _REQUESTS_SERVED += 1

    _emit_event(
        "exiting",
        reason="eof",
        uptime_ms=_uptime_ms(),
        requests_served=_REQUESTS_SERVED,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
