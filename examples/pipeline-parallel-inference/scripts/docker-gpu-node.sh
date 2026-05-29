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
#   PP_DIAG_NETWORK     — docker network mode for the stage container
#                         (default: host). Set to `container:<id>` to make
#                         stages join an existing container's netns; this is
#                         what lets the E2E run inside a nested-container
#                         sandbox where `--network host` would land in the
#                         daemon's namespace instead of the orchestrator's.
set -euo pipefail

IMAGE="${PP_IMAGE:-swactor-pp-gpu:latest}"
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
DEV="${PP_DEV:-CPU}"
CACHE_DIR="${PP_CACHE_DIR:-$HOME/.cache/tinygrad}"
NAME="${PREFIX}-${STAGE}"

# Optional GPU passthrough for real-mode (tinygrad CUDA) runs. The slim
# runtime image has no host compiler, so the CPU backend can't JIT — real
# inference needs a GPU. Set PP_GPUS (e.g. "all" or "device=0") to attach
# one; left unset the container runs CPU-only, which is right for the stub
# image.
GPU_ARGS=()
if [ -n "${PP_GPUS:-}" ]; then
    GPU_ARGS+=(--gpus "$PP_GPUS")
fi
# PP_MODEL_CACHE_DIR, when set, is bind-mounted so a stage reuses its
# downloaded GGUF shard across runs. The mount MUST be per-stage: the
# worker keys its cache paths (`<file>.gguf`, `.partial`, `.pp_meta`) by
# the source URL alone, not by stage, so several stages sharing one cache
# dir would collide on the same .partial — one stage's os.replace/cleanup
# races another's meta-write (FileNotFoundError on the .partial). On
# vast.ai each stage owns its machine so this never bites; locally we give
# each stage its own subdir. Each stage only fetches its own tensor byte
# ranges, so the per-stage dirs stay small.
if [ -n "${PP_MODEL_CACHE_DIR:-}" ]; then
    STAGE_MODEL_CACHE="$PP_MODEL_CACHE_DIR/stage-$STAGE"
    mkdir -p "$STAGE_MODEL_CACHE"
    GPU_ARGS+=(-v "$STAGE_MODEL_CACHE":/root/.cache/pp-pipeline -e PP_MODEL_CACHE_DIR=/root/.cache/pp-pipeline)
fi

# Idempotent cleanup of any stale container with the same name so a
# re-run after a crash never collides with a leftover.
docker rm -f "$NAME" >/dev/null 2>&1 || true

# The tinygrad JIT/kernel cache is a sqlite db; several stages writing one
# shared copy concurrently can hit "database is locked". Give each stage
# its own when a GPU is attached (real mode); stub/CPU runs keep the shared
# default since they never touch it.
if [ -n "${PP_GPUS:-}" ]; then
    CACHE_DIR="$CACHE_DIR/stage-$STAGE"
fi

# Ensure the cache dir exists so the volume mount doesn't create a
# root-owned dir under $HOME.
mkdir -p "$CACHE_DIR"

# Forward every env var the orchestrator sets. `-e VAR` (no `=value`)
# tells `docker run` to copy the value from this shim's environment.
# Variables that are unset on the host are simply omitted by docker.
#
# --entrypoint runs the binary directly, bypassing the image's default
# pp_entrypoint.sh (sshd + postmortem hold). That supervisor is for remote
# vast.ai nodes; a local docker stage should exit cleanly when pp-gpu-node
# does so --rm reaps it and the E2E's no-leftover-container check holds.
exec docker run --rm --init \
    --entrypoint /usr/local/bin/pp-gpu-node \
    --name "$NAME" \
    --network "${PP_DIAG_NETWORK:-host}" \
    "${GPU_ARGS[@]}" \
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
    -e PP_STAGE_DASHBOARD \
    -e PP_STAGE_DASHBOARD_PORT_BASE \
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
    "$IMAGE"
