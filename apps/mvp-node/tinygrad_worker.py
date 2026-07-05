#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import linecache
import os
import subprocess
import sys
import threading
import time
import traceback
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
_nvidia_smi_available: bool | None = None


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


def parse_int(value: str) -> int | None:
    stripped = value.strip()
    if not stripped or stripped == "[Not Supported]":
        return None
    try:
        return int(float(stripped))
    except ValueError:
        return None


def run_nvidia_smi(args: list[str]) -> tuple[bool, str, str]:
    global _nvidia_smi_available
    if _nvidia_smi_available is False:
        return False, "", "nvidia-smi unavailable"
    try:
        result = subprocess.run(
            ["nvidia-smi", *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=float(os.environ.get("MVP_GPU_SAMPLE_TIMEOUT_SECS", "2")),
        )
    except FileNotFoundError:
        _nvidia_smi_available = False
        return False, "", "nvidia-smi not found"
    except Exception as exc:
        return False, "", str(exc)
    _nvidia_smi_available = True
    return result.returncode == 0, result.stdout, result.stderr.strip()


def parse_gpu_rows(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in csv.reader(raw.splitlines()):
        if len(row) < 5:
            continue
        memory_total = parse_int(row[2])
        memory_used = parse_int(row[3])
        utilization = parse_int(row[4])
        rows.append(
            {
                "index": parse_int(row[0]),
                "name": row[1].strip(),
                "memory_total_mib": memory_total,
                "memory_used_mib": memory_used,
                "utilization_gpu_percent": utilization,
            }
        )
    return rows


def parse_process_rows(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in csv.reader(raw.splitlines()):
        if len(row) < 2:
            continue
        pid = parse_int(row[0])
        memory_used = parse_int(row[1])
        if pid is None:
            continue
        rows.append({"pid": pid, "used_memory_mib": memory_used})
    return rows


def gpu_sample(label: str, **fields: Any) -> None:
    if not env_flag("MVP_GPU_SAMPLE", True):
        control(type="GpuSample", label=label, pid=os.getpid(), enabled=False, **fields)
        return
    gpu_ok, gpu_stdout, gpu_error = run_nvidia_smi(
        [
            "--query-gpu=index,name,memory.total,memory.used,utilization.gpu",
            "--format=csv,noheader,nounits",
        ]
    )
    proc_ok, proc_stdout, proc_error = run_nvidia_smi(
        [
            "--query-compute-apps=pid,used_memory",
            "--format=csv,noheader,nounits",
        ]
    )
    pid = os.getpid()
    processes = parse_process_rows(proc_stdout) if proc_ok else []
    worker_processes = [process for process in processes if process.get("pid") == pid]
    control(
        type="GpuSample",
        label=label,
        pid=pid,
        enabled=True,
        nvidia_smi_available=(_nvidia_smi_available is True),
        gpu_query_ok=gpu_ok,
        process_query_ok=proc_ok,
        gpu_query_error=None if gpu_ok else gpu_error,
        process_query_error=None if proc_ok else proc_error,
        gpus=parse_gpu_rows(gpu_stdout) if gpu_ok else [],
        processes=processes,
        worker_processes=worker_processes,
        **fields,
    )


def control(**event: Any) -> None:
    print(json.dumps(event, separators=(",", ":")), flush=True)


def log(message: str) -> None:
    print(f"mvp_tinygrad_worker: {message}", file=sys.stderr, flush=True)


def fatal(reason: str, **fields: Any) -> None:
    control(type="WorkerFatal", reason=reason, **fields)
    raise SystemExit(1)


def test_mode() -> bool:
    return os.environ.get("MVP_TINYGRAD_TEST_MODE", "").strip().lower() in {"1", "true", "yes", "on"}



def initialize(cmd: dict[str, Any]) -> None:
    global Tensor, dtypes
    if int(cmd.get("helper_abi_version", 1)) != 1:
        fatal("UnsupportedHelperAbi", helper_abi_version=cmd.get("helper_abi_version"))
    device = str(cmd.get("backend", {}).get("device") or os.environ.get("DEV") or "CUDA")
    os.environ["DEV"] = device
    started = time.monotonic()
    control(type="TinygradImportStarted", requested_device=device, env_DEV=os.environ.get("DEV"))
    from tinygrad import Tensor as TinyTensor, dtypes as tiny_dtypes

    control(type="TinygradImportReady", requested_device=device, env_DEV=os.environ.get("DEV"))
    Tensor = TinyTensor
    dtypes = tiny_dtypes
    control(type="TinygradDeviceProbeStarted", requested_device=device)
    value = Tensor([1], dtype=dtypes.int32).realize().numpy().tolist()
    control(type="TinygradDeviceProbeReady", requested_device=device, probe_result=value)
    gpu_sample("after_worker_probe", requested_device=device)
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


def load_weights(cmd: dict[str, Any]) -> None:
    global model, tokenizer
    TensorCls = require_tinygrad()
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
    if test_mode():
        model = {"test_mode": True}
        tokenizer = {"test_mode": True}
        loaded.clear()
        loaded.update(
            model_id=model_id,
            path="mvp-tinygrad-test-mode",
            layer_start=int(cmd.get("layer_start", 0)),
            layer_end_exclusive=int(cmd.get("layer_end_exclusive", 0)),
        )
        control(
            type="WeightsLoaded",
            model_id=model_id,
            path=loaded["path"],
            test_mode=True,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
        return
    control(type="GgufResolveStarted", model_id=model_id, source_kind=source_kind(source))
    path = fetch_whole(source)
    model_bytes = path.stat().st_size
    control(type="GgufResolveReady", model_id=model_id, path=str(path), bytes=model_bytes)
    try:
        control(type="TinygradLlmImportStarted", model_id=model_id)
        from tinygrad.apps.llm import SimpleTokenizer, Transformer

        control(type="TinygradLlmImportReady", model_id=model_id)
        max_context_raw = os.environ.get("MVP_MAX_CONTEXT", "512")
        max_context = int(max_context_raw) if max_context_raw else 512
        gpu_sample("before_model_load", model_id=model_id, path=str(path), bytes=model_bytes)
        control(
            type="TransformerFromGgufStarted",
            model_id=model_id,
            path=str(path),
            bytes=model_bytes,
            max_context=max_context,
            realize=True,
            requested_device=os.environ.get("DEV"),
        )
        model, kv = Transformer.from_gguf(TensorCls(path), max_context=max_context, realize=True)
        control(
            type="TransformerFromGgufReady",
            model_id=model_id,
            path=str(path),
            bytes=model_bytes,
            max_context=max_context,
            realize=True,
            requested_device=os.environ.get("DEV"),
        )
        gpu_sample("after_model_load", model_id=model_id, path=str(path), bytes=model_bytes)
        tok_src = cmd.get("tokenizer", {"EmbeddedGguf": None})
        if "EmbeddedGguf" in tok_src:
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
        layer_start=int(cmd.get("layer_start", 0)),
        layer_end_exclusive=int(cmd.get("layer_end_exclusive", 0)),
    )
    control(
        type="WeightsLoaded",
        model_id=model_id,
        path=str(path),
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )



def decode_greedy_device_resident(
    prompt_tokens: list[int],
    max_tokens: int,
    *,
    request_id: int | None,
    model_id: str | None,
    progress_every: int,
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
            )
            gpu_sample("after_first_token", request_id=request_id, model_id=model_id)
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
    gpu_sample("before_prompt", request_id=request_id, model_id=loaded.get("model_id"))
    if test_mode():
        text = f"mvp-test response: {prompt}"
        control(
            type="PromptCompleted",
            request_id=request_id,
            model_id=loaded.get("model_id"),
            prompt_tokens=[],
            generated_tokens=list(range(min(max_tokens, 3))),
            text=text,
            test_mode=True,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
        return
    control(type="PromptEncodeStarted", request_id=request_id, model_id=loaded.get("model_id"))
    prompt_tokens = tokenizer.encode(prompt)
    control(
        type="PromptEncodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_bytes=len(prompt.encode("utf-8")),
        prompt_tokens=len(prompt_tokens),
    )
    progress_every = int(os.environ.get("MVP_TOKEN_PROGRESS_EVERY", "16") or "16")
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
        )
    finally:
        stop_cpu_line_sampler(cpu_sampler)
    control(
        type="DecodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        prompt_tokens=len(prompt_tokens),
        tokens_generated=len(generated),
    )
    gpu_sample("after_decode", request_id=request_id, model_id=loaded.get("model_id"))
    control(type="TextDecodeStarted", request_id=request_id, model_id=loaded.get("model_id"), tokens_generated=len(generated))
    text = tokenizer.decode(generated) if generated else ""
    control(
        type="TextDecodeReady",
        request_id=request_id,
        model_id=loaded.get("model_id"),
        tokens_generated=len(generated),
        text_bytes=len(text.encode("utf-8")),
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


def shutdown_worker(_: dict[str, Any]) -> None:
    control(type="WorkerStopped", reason="Graceful")
    raise SystemExit(0)


HANDLERS = {
    "InitializeWorker": initialize,
    "ConfigureRole": configure_role,
    "LoadWeights": load_weights,
    "InferPrompt": infer_prompt,
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
