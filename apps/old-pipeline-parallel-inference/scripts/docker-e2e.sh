#!/usr/bin/env bash
# docker-e2e.sh — the Stage 11 pre-deploy gate.
#
# Brings up `N` stub-mode `pp-worker` containers on localhost, drives
# one InferenceRequest through them via `pp-orchestrator --seed`, and tears
# everything down. The image is built locally from the workspace's
# release artifacts; no GPU, no tinygrad, no GGUF required.
#
# Usage:
#   apps/pipeline-parallel-inference/scripts/docker-e2e.sh [N]
#
# Environment overrides:
#   PP_IMAGE            code image tag (default: swactor-pp-gpu:latest)
#   PP_BASE_IMAGE       base image tag (default: swactor-pp-base:cuda12.6)
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
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"
PROMPT="${PP_PROMPT:-Say hello}"

# Real vs stub mode — a runtime toggle on ONE image, not two images. Stub
# (default) runs the Python worker in PP_WORKER_STUB mode — no tinygrad, no
# GPU, no GGUF — so the harness exercises orchestration/convergence/teardown
# on any host (the CUDA base is inert under the stub). PP_REAL runs real
# tinygrad inference on a GPU: each stage loads its own model shard via
# NVRTC, and requires a CUDA GPU reachable through `docker run --gpus`.
IMAGE="${PP_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"
if [ -n "${PP_REAL:-}" ]; then
    MODEL="${MODEL:-llama3.2:1b}"
    # Root of the GGUF shard cache. The shim gives each stage its own
    # subdir under here (the worker keys cache files by URL, not stage, so
    # stages must not share one dir) — letting each stage reuse its own
    # downloaded shard across runs.
    PP_MODEL_CACHE_DIR="${PP_MODEL_CACHE_DIR:-$HOME/.cache/pp-pipeline}"
fi

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
ORCHESTRATOR_BIN="$CRATE_DIR/target/release/pp-orchestrator"
WORKER_BIN="$CRATE_DIR/target/release/pp-worker"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"

# Step 1: build the release artifacts the docker image will package. Fleet
# telemetry rides the datastream from the pp-worker binary itself, so there are
# no separate collector binaries to build.
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "docker-e2e: building pp-worker + pp-orchestrator (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-worker --bin pp-orchestrator
fi
for f in "$ORCHESTRATOR_BIN" "$WORKER_BIN" "$WORKER_PY"; do
    [ -f "$f" ] || { echo "docker-e2e: missing $f" >&2; exit 1; }
done

# Step 2: build the layered image — the heavy base (CUDA + tinygrad + sshd)
# then the thin code layer on top. Build context is the workspace root
# because the Dockerfiles copy from `examples/...` and `target/...`. Stub
# mode is a runtime toggle (PP_WORKER_STUB=1 below), so the same CUDA image
# serves both stub (no GPU) and real runs.
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "docker-e2e: building $BASE_IMAGE (base)"
    docker build \
        -f "$CRATE_DIR/Dockerfile.base" \
        -t "$BASE_IMAGE" \
        "$WORKSPACE_DIR"
    echo "docker-e2e: building $IMAGE (code)"
    docker build \
        -f "$CRATE_DIR/Dockerfile" \
        --build-arg "BASE_IMAGE=$BASE_IMAGE" \
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

# Step 4: drive pp-orchestrator with the docker shim as its --gpu-node.
# The shim consults PP_IMAGE / PP_CONTAINER_PREFIX / PP_DEV from its env.
OUTPUT_DIR="$(mktemp -d)"
STDOUT_LOG="$OUTPUT_DIR/stdout.log"
STDERR_LOG="$OUTPUT_DIR/stderr.log"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

set +e
if [ -n "${PP_REAL:-}" ]; then
    # Real CUDA inference: no stub, attach a GPU, point the worker at the
    # model and the shared shard cache. PP_DEV=CUDA selects tinygrad's CUDA
    # backend (the slim image has no host compiler for the CPU backend).
    MODEL="$MODEL" \
    PP_IMAGE="$IMAGE" \
    PP_CONTAINER_PREFIX="$PREFIX" \
    PP_DEV=CUDA \
    PP_GPUS="${PP_GPUS:-all}" \
    PP_MODEL_CACHE_DIR="$PP_MODEL_CACHE_DIR" \
    "$ORCHESTRATOR_BIN" \
        --seed \
        --num-stages "$NUM_STAGES" \
        --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
        --worker "$WORKER_PY" \
        --prompt "$PROMPT" \
        --max-tokens "$MAX_TOKENS" \
        >"$STDOUT_LOG" 2>"$STDERR_LOG"
else
    PP_WORKER_STUB=1 \
    PP_IMAGE="$IMAGE" \
    PP_CONTAINER_PREFIX="$PREFIX" \
    PP_DEV=CPU \
    "$ORCHESTRATOR_BIN" \
        --seed \
        --num-stages "$NUM_STAGES" \
        --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
        --worker "$WORKER_PY" \
        --prompt "$PROMPT" \
        --max-tokens "$MAX_TOKENS" \
        >"$STDOUT_LOG" 2>"$STDERR_LOG"
fi
ORCH_STATUS=$?
set -e

if [ $ORCH_STATUS -ne 0 ]; then
    echo "docker-e2e: pp-orchestrator exited $ORCH_STATUS" >&2
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
