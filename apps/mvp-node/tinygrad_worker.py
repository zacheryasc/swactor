#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import mmap
import linecache
import os
import sys
import threading
import time
import struct
import traceback
import shutil
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

Tensor: Any = None
dtypes: Any = None
model: Any = None
tokenizer: Any = None
role: dict[str, Any] = {}
loaded: dict[str, Any] = {}
arena: mmap.mmap | None = None
rings: dict[int, dict[str, Any]] = {}
device_objects: dict[int, dict[str, Any]] = {}
next_handle = 42
HEADER_LEN = 40
FLAG_BEGIN_SEQUENCE = 1 << 1
WORKER_GENERATION = 1
BENCHMARK_SCHEMA = 1
_benchmark_start = time.monotonic()
_benchmark_seq = 0


class CpuLineSampler:
    def __init__(
        self,
        *,
        phase: str,
        request_id: int | None,
        model_id: str | None,
        interval_secs: float,
    ) -> None:
        self.phase = phase
        self.request_id = request_id
        self.model_id = model_id
        self.interval_secs = interval_secs
        self.target_thread_id = threading.get_ident()
        self.samples: dict[tuple[str, int, str], int] = {}
        self.wall_start = time.perf_counter()
        self.process_cpu_start = time.process_time()
        self._running = True
        self._thread = threading.Thread(target=self._run, name="cpu-line-sampler", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while self._running:
            frame = sys._current_frames().get(self.target_thread_id)
            if frame is not None:
                code = frame.f_code
                key = (code.co_filename, frame.f_lineno, code.co_name)
                self.samples[key] = self.samples.get(key, 0) + 1
            time.sleep(self.interval_secs)

    def stop(self) -> None:
        self._running = False
        self._thread.join(timeout=max(0.25, self.interval_secs * 4.0))
        wall_elapsed_ms = (time.perf_counter() - self.wall_start) * 1000.0
        process_cpu_elapsed_ms = (time.process_time() - self.process_cpu_start) * 1000.0
        total_samples = sum(self.samples.values())
        top = []
        for (filename, line, function), count in sorted(
            self.samples.items(), key=lambda item: item[1], reverse=True
        )[:32]:
            top.append(
                {
                    "file": filename,
                    "line": line,
                    "function": function,
                    "source": linecache.getline(filename, line).strip(),
                    "samples": count,
                    "percent": round((count * 100.0 / total_samples), 2) if total_samples else 0.0,
                }
            )
        control(
            type="CpuLineProfileSummary",
            phase=self.phase,
            request_id=self.request_id,
            model_id=self.model_id,
            interval_ms=round(self.interval_secs * 1000.0, 3),
            wall_elapsed_ms=round(wall_elapsed_ms, 3),
            process_cpu_elapsed_ms=round(process_cpu_elapsed_ms, 3),
            process_cpu_over_wall=round(process_cpu_elapsed_ms / wall_elapsed_ms, 4)
            if wall_elapsed_ms > 0.0
            else 0.0,
            total_samples=total_samples,
            top=top,
        )


def start_cpu_line_sampler(
    *,
    phase: str,
    request_id: int | None,
    model_id: str | None,
) -> CpuLineSampler | None:
    raw = os.environ.get("MVP_CPU_LINE_PROFILE")
    if not env_flag("MVP_CPU_LINE_PROFILE", False):
        control(
            type="CpuLineProfileSkipped",
            phase=phase,
            request_id=request_id,
            model_id=model_id,
            env_value=raw,
        )
        return None
    interval_ms = float(os.environ.get("MVP_CPU_LINE_PROFILE_INTERVAL_MS", "2"))
    interval_secs = max(0.0005, interval_ms / 1000.0)
    control(
        type="CpuLineProfileStarted",
        phase=phase,
        request_id=request_id,
        model_id=model_id,
        interval_ms=round(interval_secs * 1000.0, 3),
    )
    return CpuLineSampler(
        phase=phase,
        request_id=request_id,
        model_id=model_id,
        interval_secs=interval_secs,
    )


def stop_cpu_line_sampler(sampler: CpuLineSampler | None) -> None:
    if sampler is not None:
        sampler.stop()


def env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def benchmark_stamp() -> dict[str, Any]:
    global _benchmark_seq
    _benchmark_seq += 1
    return {
        "schema": BENCHMARK_SCHEMA,
        "component": "tinygrad-worker",
        "pid": os.getpid(),
        "seq": _benchmark_seq,
        "wall_unix_ms": time.time_ns() // 1_000_000,
        "mono_ms": int((time.monotonic() - _benchmark_start) * 1000),
    }


def env_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def control(**event: Any) -> None:
    event.setdefault("benchmark", benchmark_stamp())
    if (run_id := env_int("MVP_RUN_ID")) is not None:
        event.setdefault("run_id", run_id)
    if (node_id := env_int("MVP_LOGICAL_NODE_ID")) is not None:
        event.setdefault("node_id", node_id)
    if (stage_index := env_int("MVP_STAGE_INDEX")) is not None:
        event.setdefault("stage_index", stage_index)
    print(json.dumps(event, separators=(",", ":")), flush=True)


def log(message: str) -> None:
    print(f"mvp_tinygrad_worker: {message}", file=sys.stderr, flush=True)


def fatal(reason: str, **fields: Any) -> None:
    control(type="WorkerFatal", reason=reason, **fields)
    raise SystemExit(1)



def configure_tinygrad_cuda_compiler(device: str) -> None:
    if device.split(":", 1)[0].upper() != "CUDA":
        return
    if os.environ.get("CUDA_PTX") or os.environ.get("CUDA_CC"):
        return
    if shutil.which("nvcc") is not None:
        return
    os.environ["CUDA_PTX"] = "1"
    control(type="TinygradCudaCompilerSelected", requested_device=device, compiler="PTX", reason="nvcc_not_found")

def select_tinygrad_device(device: str) -> str:
    device_kind = device.split(":", 1)[0].upper()
    if device_kind == "CPU" and ":" not in device and shutil.which("clang") is None:
        selected = "CPU:X86"
        os.environ["DEV"] = selected
        control(type="TinygradCpuCompilerSelected", requested_device=device, selected_device=selected, compiler="X86", reason="clang_not_found")
        return selected
    os.environ["DEV"] = device
    configure_tinygrad_cuda_compiler(device)
    return device



def initialize(cmd: dict[str, Any]) -> None:
    global Tensor, dtypes, arena
    if int(cmd.get("helper_abi_version", 1)) != 1:
        fatal("UnsupportedHelperAbi", helper_abi_version=cmd.get("helper_abi_version"))
    requested_device = str(cmd.get("backend", {}).get("device") or os.environ.get("DEV") or "CUDA")
    device = select_tinygrad_device(requested_device)
    arena_fd = os.environ.get("MVP_ARENA_FD")
    if arena_fd is not None:
        arena_bytes = int(os.environ.get("MVP_ARENA_BYTES", "0") or "0")
        if arena_bytes > 0:
            arena = mmap.mmap(int(arena_fd), arena_bytes)
    started = time.monotonic()
    control(type="TinygradImportStarted", requested_device=device, env_DEV=os.environ.get("DEV"))
    from tinygrad import Tensor as TinyTensor, dtypes as tiny_dtypes

    control(type="TinygradImportReady", requested_device=device, env_DEV=os.environ.get("DEV"))
    Tensor = TinyTensor
    dtypes = tiny_dtypes
    control(type="TinygradDeviceProbeStarted", requested_device=device)
    value = Tensor([1], dtype=dtypes.int32).realize().numpy().tolist()
    control(type="TinygradDeviceProbeReady", requested_device=device, probe_result=value)
    control(
        type="WorkerReady",
        pid=os.getpid(),
        backend={"requested_device": device, "env_DEV": os.environ.get("DEV"), "tinygrad_device": device},
        cuda_probe=value,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def configure_role(cmd: dict[str, Any]) -> None:
    config = cmd.get("config", {})
    role.clear()
    role.update(
        role_id=int(cmd.get("role_id", 1)),
        run_id=int(config.get("run_id", 1)),
        stage_index=int(config.get("stage_index", 0)),
        layer_start=int(config.get("layer_start", 0)),
        layer_end_exclusive=int(config.get("layer_end_exclusive", 0)),
    )
    control(type="RoleConfigured", role_id=role["role_id"], stage_index=role["stage_index"])


def cache_root() -> Path:
    raw = os.environ.get("MVP_MODEL_CACHE_DIR", "").strip()
    root = Path(raw).expanduser() if raw else Path.home() / ".cache" / "mvp-node"
    root.mkdir(parents=True, exist_ok=True)
    return root


def hf_url(repo: str, file: str, revision: str | None) -> str:
    encoded_file = "/".join(urllib.parse.quote(part) for part in file.split("/"))
    return f"https://huggingface.co/{repo}/resolve/{revision or 'main'}/{encoded_file}"


def source_url(source: dict[str, Any]) -> str | None:
    if "HuggingFaceGguf" not in source:
        return None
    hf = source["HuggingFaceGguf"]
    return hf_url(str(hf["repo"]), str(hf["file"]), hf.get("revision"))


def source_path(source: dict[str, Any]) -> Path | None:
    if "LocalPath" not in source:
        return None
    return Path(str(source["LocalPath"])).expanduser()


def source_kind(source: dict[str, Any]) -> str:
    if "LocalPath" in source:
        return "LocalPath"
    if "HuggingFaceGguf" in source:
        return "HuggingFaceGguf"
    return "Unknown"


def cache_path_for(url: str) -> Path:
    parsed = urllib.parse.urlparse(url)
    basename = Path(parsed.path).name or "model.gguf"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return cache_root() / f"{digest}-{basename}"


def request_headers() -> dict[str, str]:
    headers = {"User-Agent": "swactor-mvp-node/0.1"}
    token = os.environ.get("HF_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_whole(source: dict[str, Any]) -> Path:
    local = source_path(source)
    if local is not None:
        control(type="GgufLocalPathStatStarted", path=str(local))
        if not local.is_file():
            fatal("GgufLocalPathMissing", path=str(local))
        stat = local.stat()
        control(type="GgufCacheReady", path=str(local), bytes=stat.st_size, cache_hit=True, source="local")
        return local

    url = source_url(source)
    if not url:
        fatal("UnsupportedGgufSource", source=source)
    target = cache_path_for(url)
    if target.is_file() and target.stat().st_size > 0:
        control(type="GgufCacheReady", path=str(target), bytes=target.stat().st_size, cache_hit=True, url=url)
        return target

    partial = target.with_name(target.name + ".partial")
    started = time.monotonic()
    req = urllib.request.Request(url, headers=request_headers())
    control(type="GgufDownloadStarted", url=url, path=str(target))
    try:
        with urllib.request.urlopen(req, timeout=60) as response, partial.open("wb") as out:
            total = int(response.headers.get("Content-Length") or 0)
            done = 0
            last_event = 0.0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                done += len(chunk)
                now = time.monotonic()
                if now - last_event >= float(os.environ.get("MVP_DOWNLOAD_PROGRESS_SECS", "5")):
                    control(
                        type="GgufDownloadProgress",
                        bytes_done=done,
                        bytes_total=total,
                        elapsed_ms=int((now - started) * 1000),
                    )
                    last_event = now
        partial.replace(target)
    except Exception as exc:
        try:
            partial.unlink(missing_ok=True)
        except Exception:
            pass
        fatal("GgufDownloadFailed", url=url, error=str(exc))
    control(
        type="GgufCacheReady",
        path=str(target),
        bytes=target.stat().st_size,
        cache_hit=False,
        elapsed_ms=int((time.monotonic() - started) * 1000),
        url=url,
    )
    return target


def require_tinygrad() -> Any:
    if Tensor is None:
        fatal("BackendNotInitialized")
    return Tensor


class PipelineStageTinygradModel:
    def __init__(
        self,
        *,
        block_count: int,
        dim: int,
        hidden_dim: int,
        n_heads: int,
        n_kv_heads: int,
        norm_eps: float,
        vocab_size: int,
        head_dim: int,
        rope_theta: float,
        rope_dim: int,
        v_head_dim: int,
        max_context: int,
        qk_norm: int,
        num_experts: int,
        num_experts_per_tok: int,
        norm_topk_prob: bool,
        qkv_bias: bool,
        expert_bias: bool,
        first_stage: bool,
        final_stage: bool,
        nn_mod: Any,
        config_cls: Any | None,
        block_cls: Any,
    ) -> None:
        if config_cls is None:
            self.blk = [
                block_cls(
                    dim,
                    hidden_dim,
                    n_heads,
                    n_kv_heads,
                    norm_eps,
                    head_dim,
                    rope_theta,
                    max_context,
                    qk_norm,
                    num_experts,
                    num_experts_per_tok,
                )
                for _ in range(block_count)
            ]
        else:
            block_config = config_cls(
                num_blocks=block_count,
                dim=dim,
                hidden_dim=hidden_dim,
                n_heads=n_heads,
                n_kv_heads=n_kv_heads,
                norm_eps=norm_eps,
                vocab_size=vocab_size,
                head_dim=head_dim,
                rope_theta=rope_theta,
                rope_dim=rope_dim,
                v_head_dim=v_head_dim,
                max_context=max_context,
                qk_norm=qk_norm,
                num_experts=num_experts,
                num_experts_per_tok=num_experts_per_tok,
                norm_topk_prob=norm_topk_prob,
                qkv_bias=qkv_bias,
                expert_bias=expert_bias,
            )
            self.blk = [block_cls(block_config) for _ in range(block_count)]
        self.max_context = max_context
        self.hidden_dim = dim
        self.first_stage = first_stage
        self.final_stage = final_stage
        if first_stage:
            self.token_embd = nn_mod.Embedding(vocab_size, dim)
        if final_stage:
            self.output_norm = nn_mod.RMSNorm(dim, norm_eps)
            self.output = nn_mod.Linear(dim, vocab_size, bias=False)

    def token_hidden(self, tokens_tensor: Any) -> Any:
        return self.token_embd(tokens_tensor).float()

    def forward_hidden(self, hidden: Any, start_pos: Any) -> Any:
        for block in self.blk:
            hidden = block(hidden, start_pos)
        return hidden.contiguous()

    def next_token(self, hidden: Any) -> Any:
        return self.output(self.output_norm(hidden))[:, -1, :].argmax(-1, keepdim=True)

    def __call__(self, tokens_tensor: Any, start_pos: Any) -> Any:
        return self.next_token(self.forward_hidden(self.token_hidden(tokens_tensor), start_pos))

def remap_stage_state_dict(
    state_dict: dict[str, Any],
    *,
    layer_start: int,
    layer_end_exclusive: int,
    first_stage: bool,
    final_stage: bool,
) -> dict[str, Any]:
    if final_stage and "output.weight" not in state_dict and "token_embd.weight" in state_dict:
        state_dict["output.weight"] = state_dict["token_embd.weight"]
    remapped: dict[str, Any] = {}
    prefix = "blk."
    for key, value in state_dict.items():
        if key.startswith(prefix):
            parts = key.split(".", 2)
            if len(parts) != 3:
                continue
            block_index = int(parts[1])
            if layer_start <= block_index < layer_end_exclusive:
                remapped[f"blk.{block_index - layer_start}.{parts[2]}"] = value
        elif first_stage and key == "token_embd.weight":
            remapped[key] = value
        elif final_stage and (key == "output_norm.weight" or key == "output.weight"):
            remapped[key] = value
    return remapped


def load_pipeline_stage_model(
    path: Path,
    *,
    max_context: int,
    layer_start: int,
    layer_end_exclusive: int,
) -> tuple[PipelineStageTinygradModel, dict[str, Any]]:
    TensorCls = require_tinygrad()
    from tinygrad import nn

    try:
        from tinygrad.llm.gguf import gguf_load
        from tinygrad.llm.model import TransformerBlock, TransformerConfig

        kv, state_dict = gguf_load(path)
        block_cls = TransformerBlock
        config_cls = TransformerConfig
    except ModuleNotFoundError as exc:
        if exc.name is not None and not exc.name.startswith("tinygrad.llm"):
            raise
        from tinygrad.apps.llm import TransformerBlock

        kv, state_dict = nn.state.gguf_load(TensorCls(path).to(None))
        block_cls = TransformerBlock
        config_cls = None
    state_dict = {key: value.cast("float16") if env_flag("HALF", True) else value for key, value in state_dict.items()}
    if "output.weight" not in state_dict and "token_embd.weight" in state_dict:
        state_dict["output.weight"] = state_dict["token_embd.weight"]
    arch = kv["general.architecture"]
    max_context = min(max_context, int(kv[f"{arch}.context_length"]))
    n_heads = int(kv[f"{arch}.attention.head_count"])
    n_kv_heads = int(kv[f"{arch}.attention.head_count_kv"])
    dim = int(kv[f"{arch}.embedding_length"])
    kv_lora_rank = int(kv.get(f"{arch}.attention.kv_lora_rank", 0))
    head_dim = int(kv.get(f"{arch}.attention.key_length_mla", kv.get(f"{arch}.attention.key_length", dim // n_heads)))
    rope_dim = int(kv.get(f"{arch}.rope.dimension_count", head_dim))
    for name in list(state_dict):
        if ("attn_q.weight" in name or "attn_q_b.weight" in name) and (arch == "llama" or kv_lora_rank):
            weight = state_dict[name].reshape(n_heads, state_dict[name].shape[0] // n_heads, -1)
            prefix = head_dim - rope_dim
            state_dict[name] = (
                weight[:, :prefix]
                .cat(weight[:, prefix:].rearrange("n (h two) d -> n (two h) d", two=2), dim=1)
                .reshape(-1, weight.shape[-1])
            )
        elif arch == "llama" and "attn_k.weight" in name:
            weight = state_dict[name].reshape(n_kv_heads, state_dict[name].shape[0] // n_kv_heads, -1)
            state_dict[name] = weight.rearrange("n (h two) d -> n (two h) d", two=2).reshape(-1, weight.shape[-1])
        elif kv_lora_rank and "attn_kv_a_mqa.weight" in name:
            state_dict[name] = state_dict[name][:kv_lora_rank].cat(
                state_dict[name][kv_lora_rank:].rearrange("(h two) d -> (two h) d", two=2),
                dim=0,
            )
    total_layers = int(kv[f"{arch}.block_count"]) - int(kv.get(f"{arch}.nextn_predict_layers", 0))
    first_stage = layer_start == 0
    final_stage = layer_end_exclusive >= total_layers
    qk_key = f"blk.{layer_start}.attn_q_norm.weight"
    qk_norm = int(state_dict[qk_key].shape[0]) if qk_key in state_dict else 0
    stage_model = PipelineStageTinygradModel(
        block_count=layer_end_exclusive - layer_start,
        dim=dim,
        hidden_dim=int(kv.get(f"{arch}.expert_feed_forward_length", kv.get(f"{arch}.feed_forward_length", 0))),
        n_heads=n_heads,
        n_kv_heads=n_kv_heads,
        norm_eps=float(kv[f"{arch}.attention.layer_norm_rms_epsilon"]),
        vocab_size=len(kv["tokenizer.ggml.tokens"]),
        head_dim=head_dim,
        rope_theta=float(kv[f"{arch}.rope.freq_base"]),
        rope_dim=rope_dim,
        v_head_dim=int(kv.get(f"{arch}.attention.value_length_mla", kv.get(f"{arch}.attention.value_length", head_dim))),
        max_context=max_context,
        qk_norm=qk_norm,
        num_experts=int(kv.get(f"{arch}.expert_count", 0)),
        num_experts_per_tok=int(kv.get(f"{arch}.expert_used_count", 0)),
        norm_topk_prob=bool(kv.get(f"{arch}.expert_weights_norm", arch in ("qwen3moe", "qwen35moe"))),
        qkv_bias="blk.0.attn_q.bias" in state_dict,
        expert_bias=f"blk.{int(kv.get(f'{arch}.leading_dense_block_count', 0))}.exp_probs_b.bias" in state_dict,
        first_stage=first_stage,
        final_stage=final_stage,
        nn_mod=nn,
        config_cls=config_cls,
        block_cls=block_cls,
    )
    stage_state = remap_stage_state_dict(
        state_dict,
        layer_start=layer_start,
        layer_end_exclusive=layer_end_exclusive,
        first_stage=first_stage,
        final_stage=final_stage,
    )
    loaded_params = nn.state.load_state_dict(stage_model, stage_state, verbose=False, consume=True, realize=False)
    for param in loaded_params:
        param.replace(param.contiguous())
    if loaded_params:
        TensorCls.realize(*loaded_params)
    return stage_model, kv


def load_weights(cmd: dict[str, Any]) -> None:
    global model, tokenizer
    started = time.monotonic()
    model_id = str(cmd["model_id"])
    source = cmd["gguf_source"]
    control(
        type="LoadWeightsStarted",
        model_id=model_id,
        source_kind=source_kind(source),
        layer_start=int(cmd.get("layer_start", 0)),
        layer_end_exclusive=int(cmd.get("layer_end_exclusive", 0)),
    )
    control(type="GgufResolveStarted", model_id=model_id, source_kind=source_kind(source))
    path = fetch_whole(source)
    model_bytes = path.stat().st_size
    control(type="GgufResolveReady", model_id=model_id, path=str(path), bytes=model_bytes)
    layer_start = int(cmd.get("layer_start", 0))
    layer_end_exclusive = int(cmd.get("layer_end_exclusive", 0))
    try:
        control(type="TinygradLlmImportStarted", model_id=model_id)
        try:
            from tinygrad.llm.cli import SimpleTokenizer

            llm_backend = "tinygrad.llm"
        except ModuleNotFoundError as exc:
            if exc.name is not None and not exc.name.startswith("tinygrad.llm"):
                raise
            from tinygrad.apps.llm import SimpleTokenizer

            llm_backend = "tinygrad.apps.llm"

        control(type="TinygradLlmImportReady", model_id=model_id, backend=llm_backend)
        max_context_raw = os.environ.get("MVP_MAX_CONTEXT", "512")
        max_context = int(max_context_raw) if max_context_raw else 512
        control(
            type="PipelineStageFromGgufStarted",
            model_id=model_id,
            path=str(path),
            bytes=model_bytes,
            max_context=max_context,
            layer_start=layer_start,
            layer_end_exclusive=layer_end_exclusive,
            requested_device=os.environ.get("DEV"),
            llm_backend=llm_backend,
        )
        model, kv = load_pipeline_stage_model(
            path,
            max_context=max_context,
            layer_start=layer_start,
            layer_end_exclusive=layer_end_exclusive,
        )
        control(
            type="PipelineStageFromGgufReady",
            model_id=model_id,
            path=str(path),
            bytes=model_bytes,
            max_context=model.max_context,
            layer_start=layer_start,
            layer_end_exclusive=layer_end_exclusive,
            first_stage=model.first_stage,
            final_stage=model.final_stage,
            requested_device=os.environ.get("DEV"),
            llm_backend=llm_backend,
        )
        tok_src = cmd.get("tokenizer", {"EmbeddedGguf": None})
        if "EmbeddedGguf" in tok_src:
            tokenizer_pre = str(kv.get("tokenizer.ggml.pre", "")).lower()
            if tokenizer_pre == "smollm":
                kv = dict(kv)
                kv["tokenizer.ggml.pre"] = "qwen2"
            control(type="TokenizerBuildStarted", model_id=model_id, source="EmbeddedGguf")
            tokenizer = SimpleTokenizer.from_gguf_kv(kv)
            control(type="TokenizerBuildReady", model_id=model_id, source="EmbeddedGguf")
        else:
            fatal("UnsupportedTokenizerSource", tokenizer=tok_src)
    except SystemExit:
        raise
    except Exception as exc:
        tb = traceback.format_exc()
        print(tb, file=sys.stderr, flush=True)
        fatal("ModelLoadFailed", error=str(exc), traceback=tb)
    loaded.clear()
    loaded.update(
        model_id=model_id,
        path=str(path),
        layer_start=layer_start,
        layer_end_exclusive=layer_end_exclusive,
        hidden_dim=int(getattr(model, "hidden_dim", 0)),
        max_context=int(getattr(model, "max_context", 0)),
        eos_token_id=int(kv.get("tokenizer.ggml.eos_token_id", 0)),
        tokenizer_pre=tokenizer_pre,
    )
    control(
        type="WeightsLoaded",
        model_id=model_id,
        path=str(path),
        layer_start=layer_start,
        layer_end_exclusive=layer_end_exclusive,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )



def prompt_template_name() -> str:
    explicit = os.environ.get("MVP_PROMPT_TEMPLATE")
    if explicit is not None:
        return explicit.strip().lower()
    tokenizer_pre = str(loaded.get("tokenizer_pre", "")).lower()
    if "smollm" in tokenizer_pre or tokenizer_pre == "qwen2":
        return "smollm-chat"
    if "llama" in tokenizer_pre:
        return "llama3-chat"
    model_id = str(loaded.get("model_id", "")).lower()
    if "smollm" in model_id:
        return "smollm-chat"
    return "llama3-chat"


def model_prompt_text(prompt: str) -> tuple[str, str]:
    template = prompt_template_name()
    if template in {"", "raw", "none", "off", "false", "0"}:
        return prompt, "raw"
    if template in {"llama3", "llama3-chat", "llama-3", "llama-3-chat"}:
        return (
            "<|begin_of_text|>"
            "<|start_header_id|>user<|end_header_id|>\n\n"
            f"{prompt}"
            "<|eot_id|>"
            "<|start_header_id|>assistant<|end_header_id|>\n\n",
            "llama3-chat",
        )
    if template in {"smollm", "smollm-chat", "smollm2", "smollm2-chat"}:
        return (
            "<|im_start|>user\n"
            f"{prompt}"
            "<|im_end|>\n"
            "<|im_start|>assistant\n",
            "smollm-chat",
        )
    return prompt, "raw"


def strip_chat_stop_markers(text: str) -> str:
    cut = len(text)
    for marker in (
        "<|eot_id|>",
        "<|end_of_text|>",
        "<|start_header_id|>",
        "<|im_end|>",
        "<|endoftext|>",
        "<|im_start|>",
    ):
        index = text.find(marker)
        if index >= 0:
            cut = min(cut, index)
    return text[:cut].rstrip()


def decode_greedy_device_resident(
    prompt_tokens: list[int],
    max_tokens: int,
    *,
    request_id: int | None,
    model_id: str | None,
    progress_every: int,
    decode_started_at: float,
) -> list[int]:
    if max_tokens <= 0:
        return []
    max_context = int(getattr(model, "max_context", len(prompt_tokens) + max_tokens))
    generation_limit = min(max_tokens, max(0, max_context - len(prompt_tokens)))
    if generation_limit <= 0:
        control(
            type="DecodeContextFull",
            request_id=request_id,
            model_id=model_id,
            prompt_tokens=len(prompt_tokens),
            max_context=max_context,
        )
        return []
    if generation_limit < max_tokens:
        control(
            type="DecodeLimitedByContext",
            request_id=request_id,
            model_id=model_id,
            prompt_tokens=len(prompt_tokens),
            requested_tokens=max_tokens,
            generation_limit=generation_limit,
            max_context=max_context,
        )

    TensorCls = require_tinygrad()
    from tinygrad.uop.ops import UOp

    if hasattr(model, "forward_jit"):
        model.forward_jit.reset()
    use_symbolic_pos = os.environ.get("SYM", "1").strip().lower() not in {"0", "false", "no", "off"}
    pos_upper_bound = max(1, max_context - 1)
    symbolic_start_pos = UOp.variable("start_pos", 1, pos_upper_bound)
    next_token = model(TensorCls([prompt_tokens], dtype="int32"), 0).realize()
    generated_tensors = []

    for token_index in range(generation_limit):
        generated_tensors.append(next_token.clone().realize())
        tokens_generated = token_index + 1
        if tokens_generated == 1:
            control(
                type="FirstTokenReady",
                request_id=request_id,
                model_id=model_id,
                token_index=1,
                prompt_tokens=len(prompt_tokens),
                first_token_elapsed_ms=int((time.monotonic() - decode_started_at) * 1000),
            )
        elif progress_every > 0 and tokens_generated % progress_every == 0:
            control(
                type="TokenProgress",
                request_id=request_id,
                model_id=model_id,
                tokens_generated=tokens_generated,
                prompt_tokens=len(prompt_tokens),
            )
        if tokens_generated >= generation_limit:
            break
        start_pos = len(prompt_tokens) + token_index
        pos = symbolic_start_pos.bind(start_pos) if use_symbolic_pos else start_pos
        next_token = model(next_token, pos).realize()
    generated_tensor = (
        generated_tensors[0]
        if len(generated_tensors) == 1
        else generated_tensors[0].cat(*generated_tensors[1:], dim=1)
    )
    generated_array = generated_tensor.numpy().reshape(-1).tolist()
    return [int(token) for token in generated_array]


def infer_prompt(cmd: dict[str, Any]) -> None:
    if model is None or tokenizer is None:
        fatal("WeightsNotLoaded")
    prompt = str(cmd.get("prompt", ""))
    max_tokens = int(cmd.get("max_tokens", 1))
    request_id_raw = cmd.get("request_id")
    request_id = int(request_id_raw) if request_id_raw is not None else None
    started = time.monotonic()
    control(
        type="PromptStarted",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_bytes=len(prompt.encode("utf-8")),
        prompt_chars=len(prompt),
        max_tokens=max_tokens,
    )
    encode_started = time.monotonic()
    model_prompt, prompt_template = model_prompt_text(prompt)
    control(
        type="PromptEncodeStarted",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_template=prompt_template,
    )
    prompt_tokens = tokenizer.encode(model_prompt)
    control(
        type="PromptEncodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_bytes=len(prompt.encode("utf-8")),
        model_prompt_bytes=len(model_prompt.encode("utf-8")),
        prompt_template=prompt_template,
        prompt_tokens=len(prompt_tokens),
        elapsed_ms=int((time.monotonic() - encode_started) * 1000),
    )
    progress_every = int(os.environ.get("MVP_TOKEN_PROGRESS_EVERY", "16") or "16")
    decode_started = time.monotonic()
    control(
        type="DecodeStarted",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_tokens=len(prompt_tokens),
        max_tokens=max_tokens,
        decode_impl="device_resident_greedy",
    )
    cpu_sampler = start_cpu_line_sampler(
        phase="decode",
        request_id=request_id,
        model_id=loaded.get("model_id"),
    )
    try:
        generated = decode_greedy_device_resident(
            prompt_tokens,
            max_tokens,
            request_id=request_id,
            model_id=loaded.get("model_id"),
            progress_every=progress_every,
            decode_started_at=decode_started,
        )
    finally:
        stop_cpu_line_sampler(cpu_sampler)
    control(
        type="DecodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_tokens=len(prompt_tokens),
        tokens_generated=len(generated),
        elapsed_ms=int((time.monotonic() - decode_started) * 1000),
    )
    text_decode_started = time.monotonic()
    control(type="TextDecodeStarted", request_id=request_id, model_id=loaded.get("model_id"), tokens_generated=len(generated))
    raw_text = tokenizer.decode(generated) if generated else ""
    text = strip_chat_stop_markers(raw_text)
    control(
        type="TextDecodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        tokens_generated=len(generated),
        text_bytes=len(text.encode("utf-8")),
        elapsed_ms=int((time.monotonic() - text_decode_started) * 1000),
    )
    control(
        type="PromptCompleted",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_tokens=prompt_tokens,
        generated_tokens=generated,
        text=text,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def require_arena() -> mmap.mmap:
    if arena is None:
        fatal("ArenaNotMapped")
    return arena


def install_ring(cmd: dict[str, Any]) -> None:
    ring_id = int(cmd["ring_id"])
    layout = cmd["layout"]
    spec = cmd["object_spec"]
    rings[ring_id] = {
        "ring_id": ring_id,
        "edge_id": int(cmd["edge_id"]),
        "port": str(cmd.get("port", "")),
        "direction": str(cmd["direction"]),
        "data_offset": int(layout["data_offset"]),
        "data_capacity": int(layout["data_bytes"]),
        "max_extent": int(spec["max_extent"]),
        "alignment": int(spec["alignment"]),
        "next_sequence": 0,
    }
    control(
        type="RingInstalled",
        ring_id=ring_id,
        edge_id=rings[ring_id]["edge_id"],
        port=rings[ring_id]["port"],
        direction=rings[ring_id]["direction"],
        data_capacity=rings[ring_id]["data_capacity"],
        max_extent=rings[ring_id]["max_extent"],
        alignment=rings[ring_id]["alignment"],
    )


def uninstall_ring(cmd: dict[str, Any]) -> None:
    ring_id = int(cmd["ring_id"])
    rings.pop(ring_id, None)
    control(type="RingUninstalled", ring_id=ring_id)


def parse_record(ring: dict[str, Any]) -> tuple[int, int, int, int, bytes]:
    view = require_arena()
    base = ring["data_offset"]
    header = view[base : base + HEADER_LEN]
    if len(header) < HEADER_LEN:
        fatal("EofBeforeFullHeader", ring_id=ring["ring_id"])
    if header[0:4] != b"MO01":
        fatal("InvalidObjectMagic", ring_id=ring["ring_id"])
    version = struct.unpack_from("<H", header, 4)[0]
    header_len = struct.unpack_from("<H", header, 6)[0]
    if version != 1 or header_len != HEADER_LEN:
        fatal("InvalidObjectHeader", ring_id=ring["ring_id"], version=version, header_len=header_len)
    object_id = struct.unpack_from("<Q", header, 8)[0]
    sequence = struct.unpack_from("<Q", header, 16)[0]
    extent = struct.unpack_from("<Q", header, 24)[0]
    flags = struct.unpack_from("<I", header, 32)[0]
    reserved = struct.unpack_from("<I", header, 36)[0]
    if reserved != 0:
        fatal("InvalidObjectHeader", ring_id=ring["ring_id"], reserved=reserved)
    if extent > ring["max_extent"]:
        fatal("ObjectExtentInvalid", ring_id=ring["ring_id"], object_id=object_id, extent=extent)
    if ring["alignment"] and extent % ring["alignment"] != 0:
        fatal(
            "ObjectExtentAlignmentViolation",
            ring_id=ring["ring_id"],
            object_id=object_id,
            extent=extent,
            alignment=ring["alignment"],
        )
    if sequence != ring["next_sequence"]:
        fatal("SequenceViolation", ring_id=ring["ring_id"], expected=ring["next_sequence"], actual=sequence)
    payload = bytes(view[base + HEADER_LEN : base + HEADER_LEN + extent])
    ring["next_sequence"] += 1
    return object_id, sequence, extent, flags, payload


def payload_words(payload: bytes) -> list[int]:
    if len(payload) % 4 != 0:
        fatal("PayloadNotU32Aligned", extent=len(payload))
    if not payload:
        return []
    return list(struct.unpack(f"<{len(payload) // 4}I", payload))

def object_start_pos(sequence: int, token_count: int, flags: int) -> int:
    if flags & FLAG_BEGIN_SEQUENCE or sequence == 0:
        role["prompt_tokens"] = token_count
        role["prompt_decode_index"] = 0
        return 0
    decode_index = int(role.get("prompt_decode_index", max(0, sequence - 1)))
    role["prompt_decode_index"] = decode_index + 1
    return int(role.get("prompt_tokens", token_count)) + decode_index


def materialize_object(payload: bytes, sequence: int, flags: int) -> dict[str, Any]:
    if not isinstance(model, PipelineStageTinygradModel):
        TensorCls = require_tinygrad()
        tokens = payload_words(payload)
        token_count = len(tokens)
        return {
            "kind": "tokens",
            "tokens": tokens,
            "token_count": token_count,
            "tensor": TensorCls([tokens], dtype="int32").realize(),
            "start_pos": object_start_pos(sequence, token_count, flags),
        }
    TensorCls = require_tinygrad()
    if bool(getattr(model, "first_stage", False)) and int(role.get("layer_start", 0)) == 0:
        tokens = payload_words(payload)
        token_count = len(tokens)
        return {
            "kind": "tokens",
            "tokens": tokens,
            "token_count": token_count,
            "tensor": TensorCls([tokens], dtype="int32").realize(),
            "start_pos": object_start_pos(sequence, token_count, flags),
        }
    import numpy as np

    hidden_dim = int(loaded.get("hidden_dim") or getattr(model, "hidden_dim", 0))
    if hidden_dim <= 0:
        fatal("HiddenDimMissing")
    bytes_per_token = hidden_dim * 2
    if len(payload) % bytes_per_token != 0:
        fatal("ActivationExtentInvalid", extent=len(payload), hidden_dim=hidden_dim)
    token_count = len(payload) // bytes_per_token
    array = np.frombuffer(payload, dtype=np.float16).copy().reshape(1, token_count, hidden_dim)
    return {
        "kind": "activation",
        "token_count": token_count,
        "hidden_dim": hidden_dim,
        "tensor": TensorCls(array).realize(),
        "start_pos": object_start_pos(sequence, token_count, flags),
    }


def ring_readable(cmd: dict[str, Any]) -> None:
    global next_handle
    ring_id = int(cmd["ring_id"])
    ring = rings[ring_id]
    if ring["direction"] != "ingress":
        fatal("WrongRingDirection", ring_id=ring_id, direction=ring["direction"])
    started = time.monotonic()
    object_id, sequence, extent, flags, payload = parse_record(ring)
    handle = next_handle
    next_handle += 1
    materialized = materialize_object(payload, sequence, flags)
    materialized.update(
        object_id=object_id,
        edge_id=ring["edge_id"],
        sequence=sequence,
        extent=extent,
        flags=flags,
    )
    device_objects[handle] = materialized
    control(
        type="ObjectLoaded",
        ring_id=ring_id,
        edge_id=ring["edge_id"],
        object_id=object_id,
        sequence=sequence,
        extent=extent,
        handle_generation=WORKER_GENERATION,
        handle_id=handle,
        kind=materialized.get("kind"),
        token_count=materialized.get("token_count"),
        hidden_dim=materialized.get("hidden_dim"),
        start_pos=materialized.get("start_pos"),
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def encode_record(object_id: int, sequence: int, payload: bytes, flags: int = 0) -> bytes:
    header = bytearray(HEADER_LEN)
    header[0:4] = b"MO01"
    struct.pack_into("<H", header, 4, 1)
    struct.pack_into("<H", header, 6, HEADER_LEN)
    struct.pack_into("<Q", header, 8, object_id)
    struct.pack_into("<Q", header, 16, sequence)
    struct.pack_into("<Q", header, 24, len(payload))
    struct.pack_into("<I", header, 32, flags)
    struct.pack_into("<I", header, 36, 0)
    return bytes(header) + payload


def write_record(ring: dict[str, Any], object_id: int, sequence: int, payload: bytes, flags: int = 0) -> int:
    if len(payload) > ring["max_extent"]:
        fatal("OutputExtentInvalid", ring_id=ring["ring_id"], extent=len(payload), max_extent=ring["max_extent"])
    if ring["alignment"] and len(payload) % ring["alignment"] != 0:
        fatal("OutputExtentAlignmentViolation", ring_id=ring["ring_id"], extent=len(payload), alignment=ring["alignment"])
    record = encode_record(object_id, sequence, payload, flags)
    if len(record) > ring["data_capacity"]:
        fatal("OutputRingCapacityExceeded", ring_id=ring["ring_id"], record_bytes=len(record), capacity=ring["data_capacity"])
    view = require_arena()
    base = ring["data_offset"]
    view[base : base + len(record)] = record
    return len(record)


def execute_step(cmd: dict[str, Any]) -> None:
    if not role:
        fatal("RoleNotConfigured")
    step_started = time.monotonic()
    handle = int(cmd["input_handle_id"])
    obj = device_objects.get(handle)
    if obj is None:
        fatal("UnknownDeviceObject", handle_id=handle)
    if int(cmd["input_object_id"]) != obj["object_id"] or int(cmd["input_sequence"]) != obj["sequence"]:
        fatal("InputBindingMismatch", handle_id=handle, step_id=int(cmd["step_id"]))
    output_ring_id = int(cmd["output_ring_id"])
    ring = rings[output_ring_id]
    if ring["direction"] != "egress":
        fatal("WrongRingDirection", ring_id=output_ring_id, direction=ring["direction"])
    final_stage = bool(cmd.get("final_stage"))
    input_kind = obj.get("kind")
    input_extent = int(obj.get("extent", 0))
    validation_ready = time.monotonic()
    input_prepare_ms = 0
    forward_ms = 0
    realize_ms = 0
    payload_pack_ms = 0
    execution_backend = "pipeline_stage"
    if not isinstance(model, PipelineStageTinygradModel):
        execution_backend = "full_transformer"
        if not final_stage:
            fatal("FullTransformerNonFinalStageUnsupported", step_id=int(cmd["step_id"]))
        if input_kind != "tokens":
            fatal("FullTransformerInputUnsupported", step_id=int(cmd["step_id"]), kind=input_kind)
        if int(obj.get("flags", 0)) & FLAG_BEGIN_SEQUENCE and hasattr(model, "forward_jit"):
            model.forward_jit.reset()
        forward_started = time.monotonic()
        token_array = model(obj["tensor"], int(obj.get("start_pos", 0))).realize().numpy().reshape(-1)
        forward_ready = time.monotonic()
        forward_ms = int((forward_ready - forward_started) * 1000)
        token = int(token_array[0])
        payload_started = time.monotonic()
        payload = struct.pack("<I", token)
        output_kind = "token"
        flags = 1 if token == int(loaded.get("eos_token_id", 0)) else 0
        payload_pack_ms = int((time.monotonic() - payload_started) * 1000)
    else:
        if final_stage != bool(getattr(model, "final_stage", False)):
            fatal("FinalStageMismatch", command_final_stage=final_stage, model_final_stage=bool(getattr(model, "final_stage", False)))
        input_started = time.monotonic()
        input_tensor = model.token_hidden(obj["tensor"]) if input_kind == "tokens" else obj["tensor"]
        input_ready = time.monotonic()
        input_prepare_ms = int((input_ready - input_started) * 1000)
        forward_started = time.monotonic()
        hidden = model.forward_hidden(input_tensor, int(obj.get("start_pos", 0)))
        forward_ready = time.monotonic()
        forward_ms = int((forward_ready - forward_started) * 1000)
        if final_stage:
            realize_started = time.monotonic()
            token_array = model.next_token(hidden).realize().numpy().reshape(-1)
            realize_ready = time.monotonic()
            realize_ms = int((realize_ready - realize_started) * 1000)
            token = int(token_array[0])
            payload_started = time.monotonic()
            payload = struct.pack("<I", token)
            output_kind = "token"
            flags = 1 if token == int(loaded.get("eos_token_id", 0)) else 0
            payload_pack_ms = int((time.monotonic() - payload_started) * 1000)
        else:
            import numpy as np

            realize_started = time.monotonic()
            activation = hidden.realize().numpy().astype(np.float16, copy=False)
            realize_ready = time.monotonic()
            realize_ms = int((realize_ready - realize_started) * 1000)
            payload_started = time.monotonic()
            payload = activation.tobytes()
            output_kind = "activation"
            flags = 0
            payload_pack_ms = int((time.monotonic() - payload_started) * 1000)
    compute_ready = time.monotonic()
    committed = write_record(
        ring,
        int(cmd["output_object_id"]),
        int(cmd["output_sequence"]),
        payload,
        flags,
    )
    write_ready = time.monotonic()
    control(
        type="StepExecuted",
        step_id=int(cmd["step_id"]),
        ring_id=output_ring_id,
        role_id=int(cmd["role_id"]),
        stage_index=max(0, int(cmd["role_id"]) - 1),
        object_id=int(cmd["output_object_id"]),
        sequence=int(cmd["output_sequence"]),
        committed_bytes=committed,
        execution_backend=execution_backend,
        final_stage=final_stage,
        input_kind=input_kind,
        input_extent=input_extent,
        output_kind=output_kind,
        payload_bytes=len(payload),
        record_bytes=committed,
        input_handle_id=handle,
        input_object_id=int(cmd["input_object_id"]),
        input_sequence=int(cmd["input_sequence"]),
        input_edge_id=obj.get("edge_id"),
        input_prepare_ms=input_prepare_ms,
        model_forward_ms=forward_ms,
        output_realize_ms=realize_ms,
        payload_pack_ms=payload_pack_ms,
        validation_ms=int((validation_ready - step_started) * 1000),
        stage_execution_ms=int((compute_ready - step_started) * 1000),
        record_write_ms=int((write_ready - compute_ready) * 1000),
        elapsed_ms=int((write_ready - step_started) * 1000),
    )


def release_device_object(cmd: dict[str, Any]) -> None:
    handle_id = int(cmd["handle_id"])
    device_objects.pop(handle_id, None)
    control(type="DeviceObjectReleased", handle_id=handle_id)


def encode_prompt(cmd: dict[str, Any]) -> None:
    started = time.monotonic()
    prompt = str(cmd.get("prompt", ""))
    if tokenizer is not None:
        model_prompt, _ = model_prompt_text(prompt)
        tokens = [int(token) for token in tokenizer.encode(model_prompt)]
    else:
        model_prompt = prompt
        tokens = [int(byte) for byte in prompt.encode("utf-8")] or [0]
    control(
        type="PromptEncoded",
        request_id=cmd.get("request_id"),
        tokens=tokens,
        prompt_bytes=len(prompt.encode("utf-8")),
        model_prompt_bytes=len(model_prompt.encode("utf-8")),
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def decode_tokens(cmd: dict[str, Any]) -> None:
    started = time.monotonic()
    tokens = [int(token) for token in cmd.get("tokens", [])]
    if tokenizer is not None:
        text = strip_chat_stop_markers(tokenizer.decode(tokens))
    else:
        text = "".join(chr(token) if 32 <= token <= 126 else f"<tok:{token}>" for token in tokens)
    control(
        type="TokensDecoded",
        request_id=cmd.get("request_id"),
        text=text,
        tokens=len(tokens),
        text_bytes=len(text.encode("utf-8")),
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def shutdown_worker(_: dict[str, Any]) -> None:
    control(type="WorkerStopped", reason="Graceful")
    raise SystemExit(0)


HANDLERS = {
    "InitializeWorker": initialize,
    "ConfigureRole": configure_role,
    "LoadWeights": load_weights,
    "InferPrompt": infer_prompt,
    "InstallRing": install_ring,
    "UninstallRing": uninstall_ring,
    "RingReadable": ring_readable,
    "ExecuteStep": execute_step,
    "ReleaseDeviceObject": release_device_object,
    "EncodePrompt": encode_prompt,
    "DecodeTokens": decode_tokens,
    "ShutdownWorker": shutdown_worker,
}

for raw in sys.stdin:
    if not raw.strip():
        continue
    try:
        command = json.loads(raw)
        handler = HANDLERS.get(command.get("type"))
        if handler is None:
            fatal("UnknownCommand", command=command.get("type"))
        handler(command)
    except SystemExit:
        raise
    except Exception as exc:
        tb = traceback.format_exc()
        print(tb, file=sys.stderr, flush=True)
        fatal("UnhandledWorkerException", error=str(exc), traceback=tb)
