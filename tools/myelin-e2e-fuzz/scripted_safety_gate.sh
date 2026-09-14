#!/usr/bin/env bash
# Gate B only: run after Gate A. Every scenario invokes the real paid CLI
# against an independently owned loopback HTTP provider and request ledger.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-$ROOT/target/vastai-gate-b-scripted-1}"
cd "$ROOT"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
# An ordered owner can supply its already-built immutable binary. Standalone
# invocation builds both required binaries once, never once per scenario.
if [ -z "${SCRIPTED_HARNESS_BINARY:-}" ]; then
  cargo build --release -q -p myelin --bins -p myelin-e2e-fuzz
  SCRIPTED_HARNESS_BINARY="$TARGET/release/myelin-e2e-fuzz"
fi
exec python3 -E -B "$ROOT/tools/myelin-e2e-fuzz/scripted_provider.py" \
  --gate "$ROOT" "$OUT" "$SCRIPTED_HARNESS_BINARY"
