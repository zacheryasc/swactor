#!/usr/bin/env bash
# docker-e2e.sh — the Stage 11 pre-deploy gate.
#
# Brings up `N` stub-mode `pp-gpu-node` containers on localhost, drives
# one InferenceRequest through them via `pp-smoke-run --seed`, and tears
# everything down. The image is built locally from the workspace's
# release artifacts; no GPU, no tinygrad, no GGUF required.
#
# Usage:
#   examples/pipeline-parallel-inference/scripts/docker-e2e.sh [N]
#
# Environment overrides:
#   PP_IMAGE            container image tag (default: pp-gpu-node-stub:latest)
#   PP_CONTAINER_PREFIX container name prefix (default: pp-stage)
#   PP_MAX_TOKENS       max decode tokens (default: 4)
#   PP_PROMPT           inference prompt (default: "Say hello")
#   PP_SKIP_BUILD       if set, skip cargo build (use existing target/)
#   PP_SKIP_IMAGE_BUILD if set, skip docker image build (use existing tag)
#
# Exit code is 0 only when the response banner was non-empty AND no
# stage containers remain afterwards.
set -euo pipefail

NUM_STAGES="${1:-3}"
IMAGE="${PP_IMAGE:-pp-gpu-node-stub:latest}"
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"
PROMPT="${PP_PROMPT:-Say hello}"

if ! [[ "$NUM_STAGES" =~ ^[0-9]+$ ]] || [ "$NUM_STAGES" -lt 2 ]; then
    echo "docker-e2e: NUM_STAGES must be an integer >= 2, got '$NUM_STAGES'" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker-e2e: docker not on PATH; install Docker or run on a host that has it" >&2
    exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "docker-e2e: docker daemon unreachable (need to start it, or fix DOCKER_HOST)" >&2
    exit 2
fi

# Locate the example crate and workspace root. The script lives in
# `<crate>/scripts/`; the workspace root is two levels above.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CRATE_DIR/../.." && pwd)"
SMOKE_RUN_BIN="$CRATE_DIR/target/release/pp-smoke-run"
GPU_NODE_BIN="$CRATE_DIR/target/release/pp-gpu-node"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"

# Step 1: build release artifacts the docker image will package.
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "docker-e2e: building pp-gpu-node + pp-smoke-run (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-gpu-node --bin pp-smoke-run
fi
for f in "$SMOKE_RUN_BIN" "$GPU_NODE_BIN" "$WORKER_PY"; do
    [ -f "$f" ] || { echo "docker-e2e: missing $f" >&2; exit 1; }
done

# Step 2: build the stub-mode image. Build context is the workspace
# root because the Dockerfile copies from `examples/...`.
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "docker-e2e: building $IMAGE"
    docker build \
        -f "$CRATE_DIR/Dockerfile.stub" \
        -t "$IMAGE" \
        "$WORKSPACE_DIR"
fi

# Step 3: clean up any stage containers left over from prior failed runs.
cleanup_containers() {
    local ids
    ids=$(docker ps -aq --filter "name=^${PREFIX}-[0-9]+$" || true)
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}
cleanup_containers

# Step 4: drive pp-smoke-run with the docker shim as its --gpu-node.
# The shim consults PP_IMAGE / PP_CONTAINER_PREFIX / PP_DEV from its env.
OUTPUT_DIR="$(mktemp -d)"
STDOUT_LOG="$OUTPUT_DIR/stdout.log"
STDERR_LOG="$OUTPUT_DIR/stderr.log"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

set +e
PP_WORKER_STUB=1 \
PP_IMAGE="$IMAGE" \
PP_CONTAINER_PREFIX="$PREFIX" \
PP_DEV=CPU \
"$SMOKE_RUN_BIN" \
    --seed \
    --num-stages "$NUM_STAGES" \
    --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
    --worker "$WORKER_PY" \
    --prompt "$PROMPT" \
    --max-tokens "$MAX_TOKENS" \
    >"$STDOUT_LOG" 2>"$STDERR_LOG"
SMOKE_STATUS=$?
set -e

if [ $SMOKE_STATUS -ne 0 ]; then
    echo "docker-e2e: pp-smoke-run exited $SMOKE_STATUS" >&2
    echo "----- stdout -----" >&2
    cat "$STDOUT_LOG" >&2
    echo "----- stderr (last 60) -----" >&2
    tail -n 60 "$STDERR_LOG" >&2
    cleanup_containers
    exit 1
fi

# Step 5: verify the orchestrator printed a non-empty response between
# its banner lines.
HEADER='=== pipeline-parallel Inference Response ==='
FOOTER='============================================'
RESPONSE=$(awk -v hdr="$HEADER" -v ftr="$FOOTER" \
    'BEGIN{in_body=0} $0==hdr{in_body=1;next} $0==ftr{in_body=0;exit} in_body{print}' \
    "$STDOUT_LOG")
if [ -z "$RESPONSE" ]; then
    echo "docker-e2e: response banner missing or empty" >&2
    echo "----- stdout -----" >&2
    cat "$STDOUT_LOG" >&2
    cleanup_containers
    exit 1
fi

# Step 6: verify cleanup — no stage container may survive a clean run.
LEFTOVERS=$(docker ps -aq --filter "name=^${PREFIX}-[0-9]+$" || true)
if [ -n "$LEFTOVERS" ]; then
    echo "docker-e2e: leftover stage containers after exit:" >&2
    docker ps -a --filter "name=^${PREFIX}-[0-9]+$" >&2 || true
    cleanup_containers
    exit 1
fi

cat "$STDOUT_LOG"
echo "docker-e2e: PASS at N=$NUM_STAGES (response length ${#RESPONSE} chars)"
