#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${1:-.}")" && pwd)/$(basename "${1:-traces}")"
PORT="${2:-8080}"

rm -rf "$DIR"
mkdir -p "$DIR"

echo "Running distribution sim tests with trace export..."
SWACTOR_TRACE_DIR="$DIR" cargo test -p simulation \
  --test distribution_registry \
  --test distribution_lifecycle \
  --test distribution_properties || echo "WARNING: some tests failed (traces from passing tests are still available)"

COUNT=$(find "$DIR" -name '*.trace.json' 2>/dev/null | wc -l)
echo "$COUNT traces in $DIR/"
echo "Dashboard at http://localhost:$PORT"
cargo run -p simulation --features dashboard --example replay -- "$DIR" "$PORT"
