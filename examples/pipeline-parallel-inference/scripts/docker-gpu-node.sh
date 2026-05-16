#!/usr/bin/env bash
# docker-gpu-node.sh — shim that pp-smoke-run can spawn instead of the
# pp-gpu-node binary directly. Boots one pp-gpu-node container per stage
# on the host network so iroh can dial without NAT.
#
# Required env (forwarded by pp-smoke-run):
#   STAGE, NUM_STAGES, SEED_ADDR, SEED_DIRECT, MAX_TOKENS
# Optional env (forwarded if present):
#   MODEL, PP_WORKER_STUB, PEER_NODE_ID, PEER_DIRECT
#
# Configurable via this shim:
#   PP_IMAGE      — image tag (default: swactor-pp-gpu:latest)
#   PP_CONTAINER_PREFIX — name prefix (default: pp-stage)
#   PP_DEV        — tinygrad device override (default: CPU)
#   PP_CACHE_DIR  — host path to tinygrad cache (default: $HOME/.cache/tinygrad)
set -euo pipefail

IMAGE="${PP_IMAGE:-swactor-pp-gpu:latest}"
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
DEV="${PP_DEV:-CPU}"
CACHE_DIR="${PP_CACHE_DIR:-$HOME/.cache/tinygrad}"
NAME="${PREFIX}-${STAGE}"

# Idempotent cleanup of any stale container with the same name.
docker rm -f "$NAME" >/dev/null 2>&1 || true

# Ensure cache dir exists so the volume mount doesn't create a root-owned dir.
mkdir -p "$CACHE_DIR"

exec docker run --rm \
    --name "$NAME" \
    --network host \
    -e STAGE \
    -e NUM_STAGES \
    -e SEED_ADDR \
    -e SEED_DIRECT \
    -e MAX_TOKENS \
    -e MODEL \
    -e PP_WORKER_STUB \
    -e PEER_NODE_ID \
    -e PEER_DIRECT \
    -e DEV="$DEV" \
    -e WORKER_SCRIPT=/usr/local/share/pp_tinygrad_worker.py \
    -v "$CACHE_DIR":/root/.cache/tinygrad \
    "$IMAGE" \
    sh -c '[ -n "$DEV" ] && unset CUDA; exec pp-gpu-node'
