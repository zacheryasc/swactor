#!/usr/bin/env bash
# swactor ping-pong -- compile the demo to WebAssembly and run it.
#
# One-liner from this directory:   ./run.sh [volleys]
#
# Compiles the Rust cdylib to ./pkg/ via wasm-pack, then drives it with Node.
set -euo pipefail
cd "$(dirname "$0")"

wasm-pack build --target nodejs --out-name pingpong
node run.mjs "$@"
