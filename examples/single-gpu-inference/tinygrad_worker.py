#!/usr/bin/env python3
"""tinygrad compute worker — stdin/stdout JSON protocol.

Startup: loads a model (or uses --stub for testing), prints {"status": "ready"}.

Protocol (newline-delimited JSON):
  → stdin:  {"prompt": "Say hello", "max_tokens": 64, "temperature": 0.7}
  ← stdout: {"response": "Hello! How can I help you today?"}

Errors:
  ← stdout: {"error": "description of what went wrong"}

Flags:
  --stub         Skip model loading; return a canned response for every request.
                 Used for component tests that exercise the protocol without a GPU.
  --model NAME   Model from tinygrad's built-in catalog (default: llama3.2:1b).
"""

import argparse
import json
import os
import sys


def main():
    parser = argparse.ArgumentParser(description="tinygrad inference worker")
    parser.add_argument("--stub", action="store_true",
                        help="Stub mode: skip model loading, return canned responses")
    parser.add_argument("--model", default="llama3.2:1b",
                        help="Model name from tinygrad catalog (default: llama3.2:1b)")
    args = parser.parse_args()

    if args.stub:
        model_data = None
    else:
        try:
            model_data = _load_model(args.model)
        except Exception as e:
            import traceback
            _log(traceback.format_exc())
            _write({"error": f"model load failed: {e}"})
            sys.exit(1)

    # Signal readiness
    _write({"status": "ready", "pid": os.getpid()})

    # Request loop
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except (json.JSONDecodeError, ValueError) as e:
            _write({"error": f"invalid JSON: {e}"})
            continue

        if "prompt" not in request:
            _write({"error": "missing 'prompt' field"})
            continue

        prompt = request["prompt"]
        max_tokens = request.get("max_tokens", 64)
        temperature = request.get("temperature", 0.7)

        try:
            text = _generate(model_data, prompt, max_tokens, temperature, stub=args.stub)
            _write({"response": text})
        except Exception as e:
            _write({"error": f"generation failed: {e}"})


def _write(obj):
    """Write a JSON object as a single line to stdout and flush."""
    print(json.dumps(obj), flush=True)


def _log(msg):
    """Write a log message to stderr (not part of the JSON protocol)."""
    print(msg, file=sys.stderr, flush=True)


def _load_model(model_name):
    """Load a GGUF model via tinygrad 0.12.0's built-in catalog."""
    from tinygrad import Tensor
    from tinygrad.helpers import fetch
    from tinygrad.apps.llm import Transformer, SimpleTokenizer, models

    if model_name not in models:
        available = ", ".join(models.keys())
        raise ValueError(f"Unknown model '{model_name}'. Available: {available}")

    url = models[model_name]
    _log(f"Downloading {model_name} from {url}...")
    gguf_path = fetch(url)

    _log(f"Loading model from {gguf_path}...")
    model, kv = Transformer.from_gguf(Tensor(gguf_path), max_context=512)
    tokenizer = SimpleTokenizer.from_gguf_kv(kv)

    # Find stop token IDs for generation
    tokens_list = kv.get("tokenizer.ggml.tokens", [])
    stop_ids = set()
    for i, tok in enumerate(tokens_list):
        if tok in ("<|end_of_text|>", "<|eot_id|>", "</s>", "<|endoftext|>"):
            stop_ids.add(i)

    # Find EOS token ID for chat template end-of-turn
    eot_id = None
    for i, tok in enumerate(tokens_list):
        if tok == "<|eot_id|>":
            eot_id = i
            break
    if eot_id is None:
        for i, tok in enumerate(tokens_list):
            if tok in ("</s>", "<|end_of_text|>"):
                eot_id = i
                break

    _log(f"Model loaded. Stop IDs: {stop_ids}, EOT ID: {eot_id}")
    return {"model": model, "tokenizer": tokenizer, "stop_ids": stop_ids, "eot_id": eot_id}


def _format_chat_tokens(tokenizer, prompt, eot_id):
    """Format a prompt using Llama 3 instruct chat template."""
    tokens = tokenizer.role("user")
    tokens += tokenizer.encode(prompt)
    if eot_id is not None:
        tokens += tokenizer.end_turn(eot_id)
    tokens += tokenizer.role("assistant")
    return tokens


def _generate(model_data, prompt, max_tokens, temperature, stub=False):
    """Generate text from a prompt."""
    if stub:
        return f"stub response to: {prompt}"

    model = model_data["model"]
    tokenizer = model_data["tokenizer"]
    stop_ids = model_data["stop_ids"]
    eot_id = model_data["eot_id"]

    # Use chat template for instruction-tuned models
    tokens = _format_chat_tokens(tokenizer, prompt, eot_id)
    prompt_len = len(tokens)

    for i, tok_id in enumerate(model.generate(tokens)):
        if tok_id in stop_ids:
            tokens.pop()  # remove the stop token from output
            break
        if i + 1 >= max_tokens:
            break

    return tokenizer.decode(tokens[prompt_len:])


if __name__ == "__main__":
    main()
