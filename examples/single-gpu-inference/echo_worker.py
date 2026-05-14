#!/usr/bin/env python3
"""Stub worker for testing. Same JSON protocol as tinygrad_worker.py."""
import json, os, sys

print(json.dumps({"status": "ready", "pid": os.getpid()}), flush=True)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
        if req.get("prompt") == "__crash__":
            os._exit(1)
        print(json.dumps({"response": f"echo: {req['prompt']}"}), flush=True)
    except Exception as e:
        print(json.dumps({"error": str(e)}), flush=True)
