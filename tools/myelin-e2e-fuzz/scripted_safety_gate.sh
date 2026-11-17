#!/usr/bin/env bash
# Exercise retained cleanup supervision against a loopback HTTP fixture.
# This does not qualify runtime deployment or the retired full campaign.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-$ROOT/target/scripted-retained-cleanup-1}"
if [[ "$OUT" != /* ]]; then
  OUT="$ROOT/$OUT"
fi
cd "$ROOT"
cargo test -q -p actor-control-flow-lint-tests --test contracts
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
# Callers may supply an already-built binary. No runtime image or deployment
# artifacts are needed by the retained cleanup lifecycle.
if [ -z "${SCRIPTED_HARNESS_BINARY:-}" ]; then
  cargo build --release -q -p myelin-e2e-fuzz --bin myelin-e2e-fuzz
  SCRIPTED_HARNESS_BINARY="$TARGET/release/myelin-e2e-fuzz"
fi
exec python3 -E -B "$ROOT/tools/myelin-e2e-fuzz/scripted_provider.py" \
  --retained-cleanup "$ROOT" "$OUT" "$SCRIPTED_HARNESS_BINARY"
