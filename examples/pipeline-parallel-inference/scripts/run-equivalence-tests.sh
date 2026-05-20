#!/usr/bin/env bash
# run-equivalence-tests.sh — slow-lane runner for the sliced-vs-full
# equivalence tests (TEST_SPEC §12).
#
# These tests are `#[ignore]` because each one spawns a real-tinygrad
# worker per stage, loads the full llama3.2:1b GGUF on every worker, and
# runs CPU forward passes for up to `EQUIVALENCE_MAX_TOKENS` decode steps
# per prompt. A single test typically takes minutes; the full set takes
# tens of minutes on a workstation. Don't put these in per-commit CI;
# run them on touch to the worker, the actor's worker-IPC, or the
# message codec — they are the load-bearing correctness check.
#
# Requirements:
#   * Working Python with tinygrad installed and reachable.
#   * Enough disk space for the GGUF (~700MB) under
#     $HOME/.cache/tinygrad (or wherever tinygrad's `fetch` caches).
#
# Usage:
#   scripts/run-equivalence-tests.sh                # all §12 tests
#   scripts/run-equivalence-tests.sh say_hello      # name-filter
#   PP_TEST_THREADS=1 scripts/run-equivalence-tests.sh
set -euo pipefail

cd "$(dirname "$0")/.."

THREADS="${PP_TEST_THREADS:-1}"

# Default to the union of every §12 test; allow a positional name-filter
# (matched as a Cargo test-name substring) for spot-checking one case.
FILTER="${1:-sliced_}"

echo "== running TEST_SPEC §12 equivalence tests (filter='$FILTER', threads=$THREADS)"
echo "   each test spawns N+1 real-tinygrad workers; expect minutes per case."

exec cargo test \
    -p pipeline-parallel-inference \
    --test t_integration \
    -- \
    --ignored \
    --test-threads "$THREADS" \
    --nocapture \
    "$FILTER"
