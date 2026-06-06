#!/usr/bin/env bash
# docker-dashboard-e2e.sh — the docker-e2e run, held open under the live
# swactor dashboard, one dashboard PER STAGE.
#
# Brings up `N` stub-mode `pp-worker` containers on localhost and drives
# one InferenceRequest through them, exactly like `docker-e2e.sh` — but each
# stage serves the live swactor dashboard (PP_STAGE_DASHBOARD) and the
# orchestrator HOLDS after the drive (PP_HOLD). The stage containers run on
# `--network host`, so each stage's dashboard is reachable on the host at
#   http://localhost:<BASE + stage>      (BASE default 9100)
# i.e. stage 0 → 9100, stage 1 → 9101, … Each board shows that stage's
# StageActor + bridge actors and live message activity as tokens flow.
#
# This targets the STAGE runtimes deliberately: the orchestrator's own
# runtime is near-empty (it sends one request and waits), so there is nothing
# to see there — the actors that do the work live inside the stage processes.
#
# The cluster stays up until you press Enter in this terminal, at which point
# the orchestrator unwinds and tears everything down.
#
# Usage:
#   examples/pipeline-parallel-inference/scripts/docker-dashboard-e2e.sh [N]
#
# Environment overrides:
#   PP_STAGE_DASHBOARD_PORT_BASE  base port; stage K serves BASE+K (default 9100)
#   PP_IMAGE            code image tag (default: swactor-pp-gpu:latest)
#   PP_BASE_IMAGE       base image tag (default: swactor-pp-base:cuda12.6)
#   PP_CONTAINER_PREFIX container name prefix (default: pp-stage)
#   PP_MAX_TOKENS       max decode tokens (default: 4)
#   PP_PROMPT           inference prompt (default: "Say hello")
#   PP_SKIP_BUILD       skip cargo build (use existing target/)
#   PP_SKIP_IMAGE_BUILD skip docker image build (use existing tag)
set -euo pipefail

NUM_STAGES="${1:-3}"
PREFIX="${PP_CONTAINER_PREFIX:-pp-stage}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"
PROMPT="${PP_PROMPT:-Say hello}"
PORT_BASE="${PP_STAGE_DASHBOARD_PORT_BASE:-9100}"
ORCH_PORT="${PP_DASHBOARD_PORT:-9099}"
IMAGE="${PP_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"

if ! [[ "$NUM_STAGES" =~ ^[0-9]+$ ]] || [ "$NUM_STAGES" -lt 2 ]; then
    echo "docker-dashboard-e2e: NUM_STAGES must be an integer >= 2, got '$NUM_STAGES'" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker-dashboard-e2e: docker not on PATH" >&2
    exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "docker-dashboard-e2e: docker daemon unreachable" >&2
    exit 2
fi

# Fail loudly on a port clash for ANY stage port. The dashboard's HTTP server
# is spawned on the driver's tokio runtime and `.expect()`s its bind; a
# collision panics that task silently and the stage keeps running, so the
# browser just shows whatever already owns the port. Catch it here instead.
if (exec 3<>"/dev/tcp/127.0.0.1/${ORCH_PORT}") 2>/dev/null; then
    exec 3>&- 3<&-
    echo "docker-dashboard-e2e: orchestrator port ${ORCH_PORT} is already in use." \
         "Pick a free one: PP_DASHBOARD_PORT=9098 $0 ${NUM_STAGES}" >&2
    exit 2
fi
for ((k = 0; k < NUM_STAGES; k++)); do
    p=$((PORT_BASE + k))
    if (exec 3<>"/dev/tcp/127.0.0.1/${p}") 2>/dev/null; then
        exec 3>&- 3<&-
        echo "docker-dashboard-e2e: port ${p} (stage ${k}) is already in use." \
             "Pick a free base: PP_STAGE_DASHBOARD_PORT_BASE=9200 $0 ${NUM_STAGES}" >&2
        exit 2
    fi
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CRATE_DIR/../.." && pwd)"
ORCHESTRATOR_BIN="$CRATE_DIR/target/release/pp-orchestrator"
WORKER_BIN="$CRATE_DIR/target/release/pp-worker"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"

# Step 1: build the release artifacts the docker image packages (same set
# docker-e2e.sh builds).
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "docker-dashboard-e2e: building pp-worker + pp-orchestrator (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-worker --bin pp-orchestrator
fi
for f in "$ORCHESTRATOR_BIN" "$WORKER_BIN" "$WORKER_PY"; do
    [ -f "$f" ] || { echo "docker-dashboard-e2e: missing $f" >&2; exit 1; }
done

# Step 2: build the layered image (heavy CUDA base, then thin code layer).
# The stage dashboard lives in the pp-worker binary baked into this image,
# so a stale image without it will show nothing — rebuild unless you know the
# current image already carries the dashboard-enabled binary.
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "docker-dashboard-e2e: building $BASE_IMAGE (base)"
    docker build -f "$CRATE_DIR/Dockerfile.base" -t "$BASE_IMAGE" "$WORKSPACE_DIR"
    echo "docker-dashboard-e2e: building $IMAGE (code)"
    docker build -f "$CRATE_DIR/Dockerfile" \
        --build-arg "BASE_IMAGE=$BASE_IMAGE" -t "$IMAGE" "$WORKSPACE_DIR"
fi

# Step 3: clean up stage containers from prior runs, and on exit (the
# orchestrator's ChainGuard kills its own children, but a Ctrl-C mid-run can
# leave strays).
cleanup_containers() {
    local ids
    ids=$(docker ps -aq --filter "name=^${PREFIX}-[0-9]+$" || true)
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}
cleanup_containers
trap cleanup_containers EXIT

echo "docker-dashboard-e2e: dashboards will come up at:"
echo "    orchestrator: http://localhost:${ORCH_PORT}  (overview / actors / topology / distribution)"
for ((k = 0; k < NUM_STAGES; k++)); do
    echo "    stage ${k}:      http://localhost:$((PORT_BASE + k))"
done

# Step 4: drive pp-orchestrator with the docker shim. The orchestrator serves its
# own dashboard (PP_DASHBOARD) — including the live SWIM distribution graph and
# message tallies — and each stage serves its own (PP_STAGE_DASHBOARD). PP_HOLD
# makes the orchestrator block at the end, ticking the driver so the
# distribution view keeps updating. stdin/stdout stay on this terminal so the
# hold can read your Enter.
PP_WORKER_STUB=1 \
PP_IMAGE="$IMAGE" \
PP_CONTAINER_PREFIX="$PREFIX" \
PP_DEV=CPU \
PP_HOLD=1 \
PP_DASHBOARD=1 \
PP_DASHBOARD_PORT="$ORCH_PORT" \
PP_STAGE_DASHBOARD=1 \
PP_STAGE_DASHBOARD_PORT_BASE="$PORT_BASE" \
"$ORCHESTRATOR_BIN" \
    --seed \
    --num-stages "$NUM_STAGES" \
    --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
    --worker "$WORKER_PY" \
    --prompt "$PROMPT" \
    --max-tokens "$MAX_TOKENS"
