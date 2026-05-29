#!/usr/bin/env bash
# demo-fleet.sh — one-command local mock of a vast.ai fleet, watchable live.
#
# Brings up, from a single command, a self-contained demo that streams the
# REAL host metrics each container measures (no synthetic data) into a live
# browser dashboard:
#
#   - swactor-diag-collector on the HOST (HTTP 9080 + UDP 9081), serving the
#     live fleet board at http://127.0.0.1:9080/dashboard
#   - pp-smoke-run on the HOST in --seed mode, spawning N pp-gpu-node
#     containers (one per stage) via docker-gpu-node.sh, each on --network host
#   - PP_HOLD=1 keeps the cluster up after the first drive, so every stage's
#     in-VM monitor keeps shipping host_sample + log records (~every 5s) and
#     the dashboard animates in real time.
#
# Ctrl+C (or any exit) tears everything down: stage containers, collector,
# orchestrator, and all temp files.
#
# Usage:
#   examples/pipeline-parallel-inference/scripts/demo-fleet.sh [N]   # N stages, default 3, >= 2
#
# Environment overrides:
#   PP_DIAG_IMAGE        code image tag       (default: swactor-pp-gpu:latest)
#   PP_BASE_IMAGE        base image tag       (default: swactor-pp-base:cuda12.6)
#   PP_SKIP_BUILD        skip cargo build (reuse existing target/)
#   PP_SKIP_IMAGE_BUILD  skip docker image build (reuse existing tag)
#   PP_GPUS              attach GPUs to each stage (e.g. "all" or "device=0").
#                        Unset => CPU stub worker with real host metrics; the
#                        GPU gauges populate once a real GPU source is wired.
#   PP_PROMPT            inference prompt     (default: "fleet demo")
#   PP_MAX_TOKENS        decode token cap     (default: 4)
#   PP_BIND_HOST         collector bind host  (default: 127.0.0.1)
#   PP_PORT              collector HTTP port  (default: 9080)
#   PP_NO_OPEN           if set, don't try to open the dashboard in a browser
#   PP_DIAG_NETWORK      docker network for stages (default: host)
set -euo pipefail

NUM_STAGES="${1:-3}"
IMAGE="${PP_DIAG_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"
PROMPT="${PP_PROMPT:-fleet demo}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"
BIND_HOST="${PP_BIND_HOST:-127.0.0.1}"
PORT="${PP_PORT:-9080}"
UDP_PORT=$((PORT + 1))
CONTAINER_PREFIX="demo-fleet-stage"
RUN_ID="demo-fleet-$(date +%s)"
DASH_URL="http://${BIND_HOST}:${PORT}/dashboard"

if ! [[ "$NUM_STAGES" =~ ^[0-9]+$ ]] || [ "$NUM_STAGES" -lt 2 ]; then
    echo "demo-fleet: N must be an integer >= 2 (seed mode needs >=2 stages), got '$NUM_STAGES'" >&2
    exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "demo-fleet: docker not on PATH" >&2; exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "demo-fleet: docker daemon unreachable" >&2; exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CRATE_DIR/../.." && pwd)"

SMOKE_RUN_BIN="$CRATE_DIR/target/release/pp-smoke-run"
GPU_NODE_BIN="$CRATE_DIR/target/release/pp-gpu-node"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"
COLLECTOR_BIN="$WORKSPACE_DIR/target/release/swactor-diag-collector"

# ── Step 1: build release artifacts ───────────────────────────────────────
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "demo-fleet: cargo build pp-smoke-run + pp-gpu-node (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-gpu-node --bin pp-smoke-run
    echo "demo-fleet: cargo build swactor-diag-collector (release, --features collector)"
    cargo build --manifest-path "$WORKSPACE_DIR/Cargo.toml" --release \
        -p distribution --features collector --bin swactor-diag-collector
fi
for f in "$SMOKE_RUN_BIN" "$GPU_NODE_BIN" "$WORKER_PY" "$COLLECTOR_BIN"; do
    [ -f "$f" ] || { echo "demo-fleet: missing $f (run without PP_SKIP_BUILD)" >&2; exit 1; }
done

# ── Step 2: build the layered image (heavy base, thin code layer) ──────────
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "demo-fleet: docker build $BASE_IMAGE (base)"
    docker build -f "$CRATE_DIR/Dockerfile.base" -t "$BASE_IMAGE" "$WORKSPACE_DIR"
    echo "demo-fleet: docker build $IMAGE (code)"
    docker build -f "$CRATE_DIR/Dockerfile" --build-arg "BASE_IMAGE=$BASE_IMAGE" \
        -t "$IMAGE" "$WORKSPACE_DIR"
fi

# ── Working dirs + FIFO that keeps the orchestrator's stdin open ───────────
WORKDIR="$(mktemp -d -t demo-fleet.XXXXXX)"
COLLECTOR_ROOT="$WORKDIR/collector"
SPOOL_DIR="$WORKDIR/spool"
ORCH_LOG="$WORKDIR/orch.log"
COLLECTOR_LOG="$WORKDIR/collector.log"
FIFO="$WORKDIR/orch.stdin"
mkdir -p "$COLLECTOR_ROOT" "$SPOOL_DIR"
mkfifo "$FIFO"

COLLECTOR_PID=""
ORCH_PID=""
CLEANED=""

cleanup() {
    [ -n "$CLEANED" ] && return 0
    CLEANED=1
    set +e
    echo
    echo "demo-fleet: tearing down…"
    # Stage containers first so the orchestrator's iroh peers vanish cleanly.
    local ids
    ids=$(docker ps -aq --filter "name=^${CONTAINER_PREFIX}-[0-9]+$")
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1
    fi
    [ -n "$ORCH_PID" ] && kill "$ORCH_PID" >/dev/null 2>&1
    [ -n "$COLLECTOR_PID" ] && kill "$COLLECTOR_PID" >/dev/null 2>&1
    # Release the write end of the FIFO and remove the work tree.
    exec 3>&- 2>/dev/null
    [ -d "$WORKDIR" ] && rm -rf "$WORKDIR"
    set -e
    echo "demo-fleet: done."
}
# Cleanup runs once, on any exit. INT/TERM just trigger an exit so the single
# EXIT handler does the teardown (avoids double-running the logic).
trap cleanup EXIT
trap 'exit 130' INT TERM

# ── Step 3: collector on the host (serves the dashboard) ───────────────────
echo "demo-fleet: starting collector on ${BIND_HOST}:${PORT} (root=$COLLECTOR_ROOT)"
"$COLLECTOR_BIN" --bind "${BIND_HOST}:${PORT}" --root "$COLLECTOR_ROOT" \
    --udp "${BIND_HOST}:${UDP_PORT}" >"$COLLECTOR_LOG" 2>&1 &
COLLECTOR_PID=$!

WAITED=0
until (echo > "/dev/tcp/${BIND_HOST}/${PORT}") >/dev/null 2>&1; do
    if ! kill -0 "$COLLECTOR_PID" >/dev/null 2>&1; then
        echo "demo-fleet: collector exited during startup" >&2
        cat "$COLLECTOR_LOG" >&2 || true
        exit 1
    fi
    WAITED=$((WAITED + 1))
    [ "$WAITED" -ge 20 ] && { echo "demo-fleet: collector did not bind :${PORT} in 20s" >&2; cat "$COLLECTOR_LOG" >&2; exit 1; }
    sleep 1
done
echo "demo-fleet: collector ready"

# ── Step 4: announce + open the dashboard ──────────────────────────────────
echo
echo "  ┌─────────────────────────────────────────────────────────────┐"
echo "  │  Live fleet dashboard:  $DASH_URL"
echo "  └─────────────────────────────────────────────────────────────┘"
echo
if [ -z "${PP_NO_OPEN:-}" ]; then
    if command -v xdg-open >/dev/null 2>&1; then (xdg-open "$DASH_URL" >/dev/null 2>&1 &) || true
    elif command -v open >/dev/null 2>&1; then (open "$DASH_URL" >/dev/null 2>&1 &) || true
    fi
fi

# ── Step 5: orchestrator (holds the cluster open, real metrics enabled) ─────
# stdin is the FIFO; we hold its write end open on fd 3 so hold_open() never
# sees EOF and the cluster stays up until we tear down.
exec 3<>"$FIFO"
echo "demo-fleet: launching orchestrator + ${NUM_STAGES} stage containers (run_id=$RUN_ID)"
# The orchestrator (and, via inheritance, docker-gpu-node.sh) read these from
# the environment. PP_HOLD keeps the cluster up; the SWACTOR_DIAG_* vars enable
# each stage's in-VM monitor so real host metrics ship to the collector.
export PP_HOLD=1
export PP_WORKER_STUB=1
export PP_DEV=CPU
export PP_IMAGE="$IMAGE"
export PP_CONTAINER_PREFIX="$CONTAINER_PREFIX"
export SWACTOR_DIAG_COLLECTOR_URL="http://${BIND_HOST}:${PORT}"
export SWACTOR_DIAG_RUN_ID="$RUN_ID"
export SWACTOR_DIAG_SPOOL_DIR="$SPOOL_DIR"
export SWACTOR_DIAG_UDP_ECHO="${BIND_HOST}:${UDP_PORT}"
[ -n "${PP_GPUS:-}" ] && export PP_GPUS
[ -n "${PP_DIAG_NETWORK:-}" ] && export PP_DIAG_NETWORK
"$SMOKE_RUN_BIN" \
    --seed \
    --num-stages "$NUM_STAGES" \
    --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
    --worker "$WORKER_PY" \
    --prompt "$PROMPT" \
    --max-tokens "$MAX_TOKENS" \
    <"$FIFO" >"$ORCH_LOG" 2>&1 &
ORCH_PID=$!

# ── Step 6: wait until the cluster is converged + held open ────────────────
echo "demo-fleet: waiting for the cluster to converge (first inference drive)…"
WAITED=0
until grep -q "holding cluster open" "$ORCH_LOG" 2>/dev/null; do
    if ! kill -0 "$ORCH_PID" >/dev/null 2>&1; then
        echo "demo-fleet: orchestrator exited before holding — drive failed." >&2
        echo "----- orchestrator log (last 40) -----" >&2
        tail -n 40 "$ORCH_LOG" >&2 || true
        echo "----- collector log (last 20) -----" >&2
        tail -n 20 "$COLLECTOR_LOG" >&2 || true
        exit 1
    fi
    WAITED=$((WAITED + 1))
    [ "$WAITED" -ge 180 ] && { echo "demo-fleet: cluster did not converge within 180s" >&2; tail -n 40 "$ORCH_LOG" >&2; exit 1; }
    sleep 1
done

RUNNING=$(docker ps -q --filter "name=^${CONTAINER_PREFIX}-[0-9]+$" | wc -l | tr -d ' ')
echo
echo "demo-fleet: ✅ fleet up — ${RUNNING}/${NUM_STAGES} stage containers streaming real metrics."
echo "demo-fleet:    watch live at  $DASH_URL"
echo "demo-fleet:    press Ctrl+C to tear everything down."
echo

# Block in the foreground until the orchestrator exits or Ctrl+C fires the trap.
wait "$ORCH_PID"
