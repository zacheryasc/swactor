#!/usr/bin/env bash
# remote-collector-fleet.sh — like demo-fleet.sh, but the diagnostics collector
# lives OFF-box on the real VPS (prod topology) instead of on localhost.
#
# Brings up, on THIS machine, the docker stage fleet you would normally deploy
# (N stub-mode pp-worker containers on --network host) plus pp-orchestrator
# hosting the FULL swactor dashboard locally. Every stage's in-VM monitor ships
# its VastaiSample/VastaiLogs records to the REMOTE collector, and the
# orchestrator's Fleet tab subscribes to that same remote collector's SSE
# stream (/diag/stream/<run_id>) and folds the records — so the Fleet tab is
# exercised end-to-end against the production collector over the public WAN.
#
# The collector is NOT started here; it must already be running on the VPS and
# its port reachable (ufw). This is the "test the prod collector locally" path.
#
# Usage:
#   examples/pipeline-parallel-inference/scripts/remote-collector-fleet.sh [N]
#
# Environment overrides:
#   PP_COLLECTOR_HOST    VPS host running swactor-diag-collector (default 139.59.195.69)
#   PP_COLLECTOR_PORT    collector HTTP port  (default 9080)
#   PP_COLLECTOR_UDP     collector UDP echo port (default 9081)
#   PP_RUN_ID            diagnostics run id   (default remote-fleet-<epoch>)
#   PP_DIAG_IMAGE        code image tag       (default swactor-pp-gpu:latest)
#   PP_BASE_IMAGE        base image tag       (default swactor-pp-base:cuda12.6)
#   PP_SKIP_BUILD        skip cargo build (reuse target/)
#   PP_SKIP_IMAGE_BUILD  skip docker image build (reuse tag)
#   PP_DASHBOARD_PORT    orchestrator dashboard port (default 9095)
#   PP_PROMPT            inference prompt     (default "remote fleet demo")
#   PP_MAX_TOKENS        decode token cap     (default 4)
#   PP_NO_OPEN           if set, don't open a browser
set -euo pipefail

NUM_STAGES="${1:-3}"
COLLECTOR_HOST="${PP_COLLECTOR_HOST:-139.59.195.69}"
COLLECTOR_PORT="${PP_COLLECTOR_PORT:-9080}"
COLLECTOR_UDP="${PP_COLLECTOR_UDP:-9081}"
RUN_ID="${PP_RUN_ID:-remote-fleet-$(date +%s)}"
IMAGE="${PP_DIAG_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"
PROMPT="${PP_PROMPT:-remote fleet demo}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"
DASH_PORT="${PP_DASHBOARD_PORT:-9095}"
CONTAINER_PREFIX="remote-fleet-stage"
COLLECTOR_URL="http://${COLLECTOR_HOST}:${COLLECTOR_PORT}"
DASH_URL="http://127.0.0.1:${DASH_PORT}/"

if ! [[ "$NUM_STAGES" =~ ^[0-9]+$ ]] || [ "$NUM_STAGES" -lt 2 ]; then
    echo "remote-fleet: N must be an integer >= 2 (seed mode needs >=2 stages), got '$NUM_STAGES'" >&2
    exit 2
fi
if ! command -v docker >/dev/null 2>&1; then echo "remote-fleet: docker not on PATH" >&2; exit 2; fi
if ! docker info >/dev/null 2>&1; then echo "remote-fleet: docker daemon unreachable" >&2; exit 2; fi
if (exec 3<>"/dev/tcp/127.0.0.1/${DASH_PORT}") 2>/dev/null; then
    exec 3>&- 3<&-
    echo "remote-fleet: dashboard port ${DASH_PORT} in use. Pick another: PP_DASHBOARD_PORT=9096 $0 ${NUM_STAGES}" >&2
    exit 2
fi

# Preflight: the remote collector must be reachable, else the Fleet tab and the
# stages' shipping will silently get nothing. Fail loudly here instead.
echo "remote-fleet: checking remote collector at ${COLLECTOR_URL} …"
if ! curl -fsS -m 8 -o /dev/null "${COLLECTOR_URL}/diag/runs"; then
    echo "remote-fleet: cannot reach ${COLLECTOR_URL}/diag/runs — is the collector up and the port open (ufw)?" >&2
    exit 1
fi
echo "remote-fleet: remote collector reachable."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CRATE_DIR/../.." && pwd)"
ORCHESTRATOR_BIN="$CRATE_DIR/target/release/pp-orchestrator"
WORKER_BIN="$CRATE_DIR/target/release/pp-worker"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"

if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "remote-fleet: cargo build pp-orchestrator + pp-worker (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release --bin pp-worker --bin pp-orchestrator
fi
for f in "$ORCHESTRATOR_BIN" "$WORKER_BIN" "$WORKER_PY"; do
    [ -f "$f" ] || { echo "remote-fleet: missing $f" >&2; exit 1; }
done
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "remote-fleet: docker build $BASE_IMAGE (base)"
    docker build -f "$CRATE_DIR/Dockerfile.base" -t "$BASE_IMAGE" "$WORKSPACE_DIR"
    echo "remote-fleet: docker build $IMAGE (code)"
    docker build -f "$CRATE_DIR/Dockerfile" --build-arg "BASE_IMAGE=$BASE_IMAGE" -t "$IMAGE" "$WORKSPACE_DIR"
fi

WORKDIR="$(mktemp -d -t remote-fleet.XXXXXX)"
SPOOL_DIR="$WORKDIR/spool"
ORCH_LOG="$WORKDIR/orch.log"
FIFO="$WORKDIR/orch.stdin"
mkdir -p "$SPOOL_DIR"
mkfifo "$FIFO"
ORCH_PID=""
CLEANED=""
cleanup() {
    [ -n "$CLEANED" ] && return 0
    CLEANED=1
    set +e
    echo; echo "remote-fleet: tearing down…"
    local ids
    ids=$(docker ps -aq --filter "name=^${CONTAINER_PREFIX}-[0-9]+$")
    [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
    [ -n "$ORCH_PID" ] && kill "$ORCH_PID" >/dev/null 2>&1
    exec 3>&- 2>/dev/null
    [ -d "$WORKDIR" ] && rm -rf "$WORKDIR"
    set -e
    echo "remote-fleet: done."
}
trap cleanup EXIT
trap 'exit 130' INT TERM

exec 3<>"$FIFO"
echo "remote-fleet: launching orchestrator + ${NUM_STAGES} stage containers"
echo "remote-fleet:   collector = ${COLLECTOR_URL}   run_id = ${RUN_ID}"
export PP_HOLD=1
export PP_WORKER_STUB=1
export PP_DEV=CPU
export PP_IMAGE="$IMAGE"
export PP_CONTAINER_PREFIX="$CONTAINER_PREFIX"
export PP_DASHBOARD=1
export PP_DASHBOARD_PORT="$DASH_PORT"
export SWACTOR_DIAG_COLLECTOR_URL="$COLLECTOR_URL"
export SWACTOR_DIAG_RUN_ID="$RUN_ID"
export SWACTOR_DIAG_SPOOL_DIR="$SPOOL_DIR"
export SWACTOR_DIAG_UDP_ECHO="${COLLECTOR_HOST}:${COLLECTOR_UDP}"
"$ORCHESTRATOR_BIN" \
    --seed --num-stages "$NUM_STAGES" \
    --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
    --worker "$WORKER_PY" \
    --prompt "$PROMPT" --max-tokens "$MAX_TOKENS" \
    <"$FIFO" >"$ORCH_LOG" 2>&1 &
ORCH_PID=$!

WAITED=0
until (echo > "/dev/tcp/127.0.0.1/${DASH_PORT}") >/dev/null 2>&1; do
    if ! kill -0 "$ORCH_PID" >/dev/null 2>&1; then
        echo "remote-fleet: orchestrator exited before its dashboard came up." >&2
        tail -n 40 "$ORCH_LOG" >&2 || true
        exit 1
    fi
    WAITED=$((WAITED + 1))
    [ "$WAITED" -ge 30 ] && { echo "remote-fleet: dashboard did not bind :${DASH_PORT} in 30s" >&2; tail -n 40 "$ORCH_LOG" >&2; exit 1; }
    sleep 1
done
echo
echo "  ┌─────────────────────────────────────────────────────────────┐"
echo "  │  Full swactor dashboard:  $DASH_URL"
echo "  │  Fleet tab pulls from remote collector: ${COLLECTOR_URL}/diag/stream/${RUN_ID}"
echo "  │  Remote collector board:  ${COLLECTOR_URL}/dashboard?run=${RUN_ID}"
echo "  └─────────────────────────────────────────────────────────────┘"
echo
if [ -z "${PP_NO_OPEN:-}" ]; then
    if command -v xdg-open >/dev/null 2>&1; then (xdg-open "$DASH_URL" >/dev/null 2>&1 &) || true
    elif command -v open >/dev/null 2>&1; then (open "$DASH_URL" >/dev/null 2>&1 &) || true
    fi
fi

echo "remote-fleet: waiting for the cluster to converge (first inference drive)…"
WAITED=0
until grep -q "holding cluster open" "$ORCH_LOG" 2>/dev/null; do
    if ! kill -0 "$ORCH_PID" >/dev/null 2>&1; then
        echo "remote-fleet: orchestrator exited before holding — drive failed." >&2
        tail -n 40 "$ORCH_LOG" >&2 || true
        exit 1
    fi
    WAITED=$((WAITED + 1))
    [ "$WAITED" -ge 180 ] && { echo "remote-fleet: cluster did not converge within 180s" >&2; tail -n 40 "$ORCH_LOG" >&2; exit 1; }
    sleep 1
done
RUNNING=$(docker ps -q --filter "name=^${CONTAINER_PREFIX}-[0-9]+$" | wc -l | tr -d ' ')
echo
echo "remote-fleet: ✅ fleet up — ${RUNNING}/${NUM_STAGES} stage containers shipping to ${COLLECTOR_URL}."
echo "remote-fleet:    watch the Fleet tab live at  $DASH_URL"
echo "remote-fleet:    press Ctrl+C to tear everything down."
echo
wait "$ORCH_PID"
