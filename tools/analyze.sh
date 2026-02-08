#!/usr/bin/env bash
set -euo pipefail

# Usage: ./tools/analyze.sh <crate-src-dir> [output-dir]
# Example: ./tools/analyze.sh crates/swactor-gossip/src

SRC_DIR="${1:?Usage: $0 <crate-src-dir> [output-dir]}"
OUT_DIR="${2:-$(dirname "$SRC_DIR")/docs/connectome}"

cargo run --manifest-path tools/depgraph/Cargo.toml -- \
  --src-dir "$SRC_DIR" --output-dir "$OUT_DIR"

uv run --with numpy --with scipy \
  python tools/spectral/spectral_analysis.py \
  "$OUT_DIR/deps.dot" --json --no-plots -o "$OUT_DIR"
