#!/usr/bin/env bash
# demo-fleet.sh — one-command local mock of a vast.ai fleet.
#
# Brings up, from a single command, a self-contained demo of the production
# topology with NO off-box collector. The orchestrator runs locally in seed
# mode and spawns N pp-worker containers via docker-gpu-node.sh on
# --network host. PP_HOLD=1 keeps the cluster up after the first drive for
# manual inspection.
#
# Ctrl+C (or any exit) tears everything down: stage containers, orchestrator,
# and all temp files.
#
# Usage:
#   apps/pipeline-parallel-inference/scripts/demo-fleet.sh [N]   # N stages, default 3, >= 2
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
#   PP_STAGE_NETWORK     docker network for stages (default: host)
set -euo pipefail

NUM_STAGES="${1:-3}"
IMAGE="${PP_DIAG_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"
PROMPT="${PP_PROMPT:-fleet demo}"
MAX_TOKENS="${PP_MAX_TOKENS:-4}"

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

ORCHESTRATOR_BIN="$CRATE_DIR/target/release/pp-orchestrator"
WORKER_BIN="$CRATE_DIR/target/release/pp-worker"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"

# ── Step 1: build release artifacts ───────────────────────────────────────
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "demo-fleet: cargo build pp-orchestrator + pp-worker (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-worker --bin pp-orchestrator
fi
for f in "$ORCHESTRATOR_BIN" "$WORKER_BIN" "$WORKER_PY"; do
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
ORCH_LOG="$WORKDIR/orch.log"
FIFO="$WORKDIR/orch.stdin"
mkfifo "$FIFO"

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

# ── Step 3: orchestrator (holds cluster) ──────────────────────────────────
# stdin is the FIFO; we hold its write end open on fd 3 so hold_open() never
# sees EOF and the cluster stays up until we tear down.
exec 3<>"$FIFO"
echo "demo-fleet: launching orchestrator + ${NUM_STAGES} stage containers (run_id=$RUN_ID)"
# The orchestrator (and, via inheritance, docker-gpu-node.sh) read these from
# the environment. PP_HOLD keeps the cluster up after the first drive.
export PP_HOLD=1
export PP_WORKER_STUB=1
export PP_DEV=CPU
export PP_IMAGE="$IMAGE"
export PP_CONTAINER_PREFIX="$CONTAINER_PREFIX"
[ -n "${PP_GPUS:-}" ] && export PP_GPUS
[ -n "${PP_STAGE_NETWORK:-}" ] && export PP_STAGE_NETWORK
"$ORCHESTRATOR_BIN" \
    --seed \
    --num-stages "$NUM_STAGES" \
    --gpu-node "$SCRIPT_DIR/docker-gpu-node.sh" \
    --worker "$WORKER_PY" \
    --prompt "$PROMPT" \
    --max-tokens "$MAX_TOKENS" \
    <"$FIFO" >"$ORCH_LOG" 2>&1 &
ORCH_PID=$!

# ── Step 4: wait until the cluster is converged + held open ────────────────
echo "demo-fleet: waiting for the cluster to converge (first inference drive)…"
WAITED=0
until grep -q "holding cluster open" "$ORCH_LOG" 2>/dev/null; do
    if ! kill -0 "$ORCH_PID" >/dev/null 2>&1; then
        echo "demo-fleet: orchestrator exited before holding — drive failed." >&2
        echo "----- orchestrator log (last 40) -----" >&2
        tail -n 40 "$ORCH_LOG" >&2 || true
        exit 1
    fi
    WAITED=$((WAITED + 1))
    [ "$WAITED" -ge 180 ] && { echo "demo-fleet: cluster did not converge within 180s" >&2; tail -n 40 "$ORCH_LOG" >&2; exit 1; }
    sleep 1
done


RUNNING=$(docker ps -q --filter "name=^${CONTAINER_PREFIX}-[0-9]+$" | wc -l | tr -d ' ')
echo
echo "demo-fleet: fleet up — ${RUNNING}/${NUM_STAGES} stage containers are running."
echo "demo-fleet:    press Ctrl+C to tear everything down."
echo

# Block in the foreground until the orchestrator exits or Ctrl+C fires the trap.
wait "$ORCH_PID"
