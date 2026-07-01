#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
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
    control(type="TinygradImportStarted", device=device)
    from tinygrad import Tensor as TinyTensor, dtypes as tiny_dtypes

    Tensor = TinyTensor
    dtypes = tiny_dtypes
    value = Tensor([1], dtype=dtypes.int32).realize().numpy().tolist()
    control(
        type="WorkerReady",
        pid=os.getpid(),
        backend={"device": device},
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
        if not local.is_file():
            fatal("GgufLocalPathMissing", path=str(local))
        control(type="GgufCacheReady", path=str(local), cache_hit=True, source="local")
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
    source = cmd["gguf_source"]
    path = fetch_whole(source)
    try:
        from tinygrad.apps.llm import SimpleTokenizer, Transformer

        max_context_raw = os.environ.get("MVP_MAX_CONTEXT", "512")
        max_context = int(max_context_raw) if max_context_raw else 512
        model, kv = Transformer.from_gguf(TensorCls(path), max_context=max_context, realize=True)
        tok_src = cmd.get("tokenizer", {"EmbeddedGguf": None})
        if "EmbeddedGguf" in tok_src:
            tokenizer = SimpleTokenizer.from_gguf_kv(kv)
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


def infer_prompt(cmd: dict[str, Any]) -> None:
    if model is None or tokenizer is None:
        fatal("WeightsNotLoaded")
    prompt = str(cmd.get("prompt", ""))
    max_tokens = int(cmd.get("max_tokens", 1))
    started = time.monotonic()
    if test_mode():
        text = f"mvp-test response: {prompt}"
        control(
            type="PromptCompleted",
            model_id=loaded.get("model_id"),
            prompt_tokens=[],
            generated_tokens=list(range(min(max_tokens, 3))),
            text=text,
            test_mode=True,
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
        return
    prompt_tokens = tokenizer.encode(prompt)
    generated: list[int] = []
    for token in model.generate(list(prompt_tokens)):
        generated.append(int(token))
        if len(generated) >= max_tokens:
            break
    text = tokenizer.decode(generated) if generated else ""
    control(
        type="PromptCompleted",
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
