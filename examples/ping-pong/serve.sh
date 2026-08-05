#!/usr/bin/env bash
# swactor live demo -- serve the ping-pong wasm page AND the swactor dashboard.
#
# One-liner from this directory:   ./serve.sh [port]
#
# Brings up two local servers:
#   - swactor dashboard (native Rust; a dummy node feeds it live actor stats)
#       at http://localhost:9090/
#   - ping-pong wasm demo (static files)
#       at http://localhost:<port>/   (default 8000)
#
# Ctrl+C stops both.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"

port="${1:-8000}"
dash_port=9090

# --- swactor dashboard -------------------------------------------------------
# The dashboard is a workspace member; build it from the root manifest, then run
# the dummy-node binary, which starts the Axum server and publishes live stats.
echo ">> building swactor dashboard..."
cargo build --manifest-path "$ROOT/Cargo.toml" -p dashboard --bin swactor_dummy_node
echo ">> launching dashboard on :${dash_port}"
"$ROOT/target/debug/swactor_dummy_node" &
dash_pid=$!
cleanup() { kill "$dash_pid" 2>/dev/null || true; wait "$dash_pid" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# --- ping-pong wasm page -----------------------------------------------------
wasm-pack build --target web --out-name pingpong --out-dir pkg-web

echo
echo ">>  ping-pong  : http://localhost:${port}/"
echo ">>  dashboard  : http://localhost:${dash_port}/   (live swactor stats)"
echo ">>  (Ctrl+C to stop)"
echo

python3 -m http.server "$port"
