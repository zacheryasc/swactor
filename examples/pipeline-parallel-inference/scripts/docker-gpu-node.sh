#!/usr/bin/env bash
# docker-gpu-node.sh — shim that pp-smoke-run can spawn instead of the
# pp-gpu-node binary directly. Boots one pp-gpu-node container per stage
# on the host network so iroh can dial without NAT.
#
# Required env (forwarded by pp-smoke-run):
#   STAGE, NUM_STAGES, SEED_ADDR, SEED_DIRECT, MAX_TOKENS
# Optional env (forwarded if present):
#   MODEL, PP_WORKER_STUB, WORKER_CMD,
#   PEER_NODE_ID, PEER_DIRECT,
#   FIRST_PEER_NODE_ID, FIRST_PEER_DIRECT,
#   PP_BOOT_DELAY_STAGE, PP_BOOT_DELAY_SECS
#
# Configurable via this shim's own environment:
#   PP_IMAGE            — image tag (default: swactor-pp-gpu:latest)
#   PP_CONTAINER_PREFIX — name prefix (default: pp-stage)
#   PP_DEV              — tinygrad device override; empty/unset leaves the
#                         image's CUDA=1 default in place (default: CPU,
#                         which is right for both the stub image and
#                         workstation real-mode runs)
#   PP_CACHE_DIR        — host path for tinygrad's cache (default:
#                         $HOME/.cache/tinygrad). Created if missing.
set -euo pipefail

IMAGE="${PP_IMAGE:-swactor-pp-gpu:latest}"
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
DEV="${PP_DEV:-CPU}"
CACHE_DIR="${PP_CACHE_DIR:-$HOME/.cache/tinygrad}"
NAME="${PREFIX}-${STAGE}"

# Idempotent cleanup of any stale container with the same name so a
# re-run after a crash never collides with a leftover.
docker rm -f "$NAME" >/dev/null 2>&1 || true

# Ensure the cache dir exists so the volume mount doesn't create a
# root-owned dir under $HOME.
mkdir -p "$CACHE_DIR"

# Forward every env var the orchestrator sets. `-e VAR` (no `=value`)
# tells `docker run` to copy the value from this shim's environment.
# Variables that are unset on the host are simply omitted by docker.
exec docker run --rm --init \
    --name "$NAME" \
    --network host \
    -e STAGE \
    -e NUM_STAGES \
    -e SEED_ADDR \
    -e SEED_DIRECT \
    -e MAX_TOKENS \
    -e MODEL \
    -e PP_WORKER_STUB \
    -e WORKER_CMD \
    -e PEER_NODE_ID \
    -e PEER_DIRECT \
    -e FIRST_PEER_NODE_ID \
    -e FIRST_PEER_DIRECT \
    -e PP_BOOT_DELAY_STAGE \
    -e PP_BOOT_DELAY_SECS \
    -e SWACTOR_DIAG_COLLECTOR_URL \
    -e SWACTOR_DIAG_RUN_ID \
    -e SWACTOR_DIAG_NODE_ROLE \
    -e SWACTOR_DIAG_STAGE_INDEX \
    -e SWACTOR_DIAG_STAGE_COUNT \
    -e SWACTOR_DIAG_SPOOL_DIR \
    -e SWACTOR_DIAG_UDP_ECHO \
    -e DEV="$DEV" \
    -e WORKER_SCRIPT=/usr/local/share/pp_tinygrad_worker.py \
    -v "$CACHE_DIR":/root/.cache/tinygrad \
    "$IMAGE" \
    pp-gpu-node
