#!/usr/bin/env bash
# docker-diag-e2e.sh — the S12 / judge gate for the diagnostics stack.
#
# Brings up:
#   - `swactor-diag-collector` in a container (HTTP 9080 + UDP 9081)
#   - `pp-smoke-run` on the host, in seed mode with N stub-mode stage
#     children spawned via `docker-gpu-node.sh` (each its own
#     container on `--network host`)
#
# Every node — orchestrator + N stages — reads the same
# `SWACTOR_DIAG_*` env vars and ships records into the collector. On
# success the collector writes the run's tarball to a host-bind volume;
# the script then runs `swactor-diag-postproc` against the bundle and
# `assert-bundle.sh` to check identity/snapshot/event invariants.
#
# Usage:
#   examples/pipeline-parallel-inference/scripts/docker-diag-e2e.sh [N]
#
# Environment overrides:
#   PP_DIAG_IMAGE         code image tag (default: swactor-pp-gpu:latest)
#   PP_BASE_IMAGE         base image tag (default: swactor-pp-base:cuda12.6)
#   PP_DIAG_RUN_ID        run identifier (default: pp-diag-<timestamp>)
#   PP_DIAG_BUNDLES_DIR   host path mounted into the collector
#                         (default: a fresh tmpdir; printed on PASS)
#   PP_PROMPT             inference prompt (default: "Diag check")
#   PP_MAX_TOKENS         decode token cap (default: 2)
#   PP_SKIP_BUILD         skip cargo build (uses existing target/)
#   PP_SKIP_IMAGE_BUILD   skip docker image build (uses existing tag)
#   PP_KEEP_BUNDLES_DIR   if set, don't rm the bundles dir on exit
#                         (useful for inspecting the bundle by hand)
#   PP_DIAG_NETWORK       docker network mode for the collector + stage
#                         containers (default: host). Set to
#                         `container:<id>` to make every container join an
#                         existing container's network namespace instead of
#                         the daemon's host namespace. This is what lets the
#                         E2E run from inside a nested-container sandbox: the
#                         orchestrator process and all containers then share
#                         one loopback, so the hardcoded 127.0.0.1 wiring
#                         meets. Non-host values force the direct `docker run`
#                         collector path (compose's network_mode is fixed).
#
# Exits 0 iff every assertion in assert-bundle.sh passes.
set -euo pipefail

NUM_STAGES="${1:-3}"
IMAGE="${PP_DIAG_IMAGE:-swactor-pp-gpu:latest}"
BASE_IMAGE="${PP_BASE_IMAGE:-swactor-pp-base:cuda12.6}"
RUN_ID="${PP_DIAG_RUN_ID:-pp-diag-$(date +%s%N)}"
PROMPT="${PP_PROMPT:-Diag check}"
MAX_TOKENS="${PP_MAX_TOKENS:-2}"
CONTAINER_PREFIX="${PP_CONTAINER_PREFIX:-pp-diag-stage}"
COLLECTOR_NAME="${PP_DIAG_COLLECTOR_NAME:-pp-diag-collector}"
DIAG_NETWORK="${PP_DIAG_NETWORK:-host}"

if ! [[ "$NUM_STAGES" =~ ^[0-9]+$ ]] || [ "$NUM_STAGES" -lt 2 ]; then
    echo "docker-diag-e2e: NUM_STAGES must be an integer >= 2, got '$NUM_STAGES'" >&2
    exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker-diag-e2e: docker not on PATH" >&2
    exit 2
fi
if ! docker info >/dev/null 2>&1; then
    echo "docker-diag-e2e: docker daemon unreachable" >&2
    exit 2
fi

# Detect the docker-compose front-end. Used when present so the
# canonical stack description (`docker-compose.diag.yml`) is the source
# of truth for the collector service. When neither variant is installed
# we fall back to a plain `docker run` of the same image / command —
# this keeps the script runnable in stripped sandboxes that don't ship
# compose, while still preferring the declarative path in production.
if [ "$DIAG_NETWORK" != "host" ]; then
    # The compose file pins `network_mode: host`; a non-host override can
    # only be expressed on the direct `docker run` path, so force it.
    COMPOSE=()
    USE_COMPOSE=0
    echo "docker-diag-e2e: PP_DIAG_NETWORK=$DIAG_NETWORK -> using direct 'docker run' (bypassing compose)"
elif docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
    USE_COMPOSE=1
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
    USE_COMPOSE=1
else
    COMPOSE=()
    USE_COMPOSE=0
    echo "docker-diag-e2e: docker compose not available; using direct 'docker run'"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_DIR="$(cd "$CRATE_DIR/../.." && pwd)"
COMPOSE_FILE="$CRATE_DIR/docker-compose.diag.yml"

SMOKE_RUN_BIN="$CRATE_DIR/target/release/pp-smoke-run"
GPU_NODE_BIN="$CRATE_DIR/target/release/pp-gpu-node"
WORKER_PY="$CRATE_DIR/pp_tinygrad_worker.py"
COLLECTOR_BIN="$WORKSPACE_DIR/target/release/swactor-diag-collector"
POSTPROC_BIN="$WORKSPACE_DIR/target/release/swactor-diag-postproc"

# Step 1: build release artifacts the image will package. The pp binaries
# live in their own workspace; the distribution binaries live at the top.
if [ -z "${PP_SKIP_BUILD:-}" ]; then
    echo "docker-diag-e2e: cargo build pp-smoke-run + pp-gpu-node (release)"
    cargo build --manifest-path "$CRATE_DIR/Cargo.toml" --release \
        --bin pp-gpu-node --bin pp-smoke-run
    echo "docker-diag-e2e: cargo build swactor-diag-{collector,postproc} (release, --features collector)"
    cargo build --manifest-path "$WORKSPACE_DIR/Cargo.toml" --release \
        -p distribution --features collector \
        --bin swactor-diag-collector --bin swactor-diag-postproc
fi
for f in "$SMOKE_RUN_BIN" "$GPU_NODE_BIN" "$WORKER_PY" "$COLLECTOR_BIN" "$POSTPROC_BIN"; do
    [ -f "$f" ] || { echo "docker-diag-e2e: missing $f" >&2; exit 1; }
done

# Step 2: build the layered image — heavy base then thin code layer.
# Build context is the workspace root because the Dockerfiles reach into
# both target/release/ trees. The diagnostics binaries ship in the default
# code image, so this is the same image the GPU nodes run.
if [ -z "${PP_SKIP_IMAGE_BUILD:-}" ]; then
    echo "docker-diag-e2e: docker build $BASE_IMAGE (base)"
    docker build \
        -f "$CRATE_DIR/Dockerfile.base" \
        -t "$BASE_IMAGE" \
        "$WORKSPACE_DIR"
    echo "docker-diag-e2e: docker build $IMAGE (code)"
    docker build \
        -f "$CRATE_DIR/Dockerfile" \
        --build-arg "BASE_IMAGE=$BASE_IMAGE" \
        -t "$IMAGE" \
        "$WORKSPACE_DIR"
fi

# Step 3: pick a host-mounted bundles dir. The collector writes its
# storage tree here; the script reads the finalized tarball back out.
if [ -n "${PP_DIAG_BUNDLES_DIR:-}" ]; then
    BUNDLES_DIR="$PP_DIAG_BUNDLES_DIR"
    mkdir -p "$BUNDLES_DIR"
    CLEANUP_BUNDLES=""
else
    BUNDLES_DIR="$(mktemp -d -t pp-diag-bundles.XXXXXX)"
    CLEANUP_BUNDLES="1"
fi
if [ -n "${PP_KEEP_BUNDLES_DIR:-}" ]; then
    CLEANUP_BUNDLES=""
fi
export PP_DIAG_BUNDLES_DIR="$BUNDLES_DIR"
export PP_DIAG_IMAGE="$IMAGE"
export PP_DIAG_COLLECTOR_NAME="$COLLECTOR_NAME"
export PP_DIAG_NETWORK="$DIAG_NETWORK"

cleanup_stages() {
    local ids
    ids=$(docker ps -aq --filter "name=^${CONTAINER_PREFIX}-[0-9]+$" || true)
    if [ -n "$ids" ]; then
        # shellcheck disable=SC2086
        docker rm -f $ids >/dev/null 2>&1 || true
    fi
}

cleanup_collector() {
    if [ "$USE_COMPOSE" = 1 ]; then
        "${COMPOSE[@]}" -f "$COMPOSE_FILE" down --remove-orphans --volumes >/dev/null 2>&1 || true
    else
        docker rm -f "$COLLECTOR_NAME" >/dev/null 2>&1 || true
    fi
}

cleanup_all() {
    set +e
    cleanup_stages
    cleanup_collector
    if [ -n "$CLEANUP_BUNDLES" ] && [ -d "$BUNDLES_DIR" ]; then
        rm -rf "$BUNDLES_DIR"
    fi
    set -e
}
trap cleanup_all EXIT

# Step 4: start the collector. Both code paths produce an equivalent
# container (same image, same command, same bind-mount); compose is
# just the declarative-config form.
cleanup_stages
cleanup_collector
echo "docker-diag-e2e: starting collector (bundles -> $BUNDLES_DIR)"
if [ "$USE_COMPOSE" = 1 ]; then
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d --remove-orphans collector
else
    # --entrypoint runs the collector directly; the default image entrypoint
    # (pp_entrypoint.sh) would ignore these args and launch pp-gpu-node.
    docker run -d --rm \
        --name "$COLLECTOR_NAME" \
        --network "$DIAG_NETWORK" \
        --entrypoint /usr/local/bin/swactor-diag-collector \
        -v "$BUNDLES_DIR":/var/lib/swactor-diag \
        "$IMAGE" \
            --bind 127.0.0.1:9080 \
            --root /var/lib/swactor-diag \
            --udp 127.0.0.1:9081 \
        >/dev/null
fi

# Wait until the collector's HTTP listener accepts a TCP connection.
# The collector binary returns 404 on `/` (no route) but the port is
# bound, which is all we need.
WAIT_TIMEOUT=20
WAITED=0
until (echo > /dev/tcp/127.0.0.1/9080) >/dev/null 2>&1; do
    WAITED=$((WAITED + 1))
    if [ "$WAITED" -ge "$WAIT_TIMEOUT" ]; then
        echo "docker-diag-e2e: collector did not accept TCP on :9080 within ${WAIT_TIMEOUT}s" >&2
        if [ "$USE_COMPOSE" = 1 ]; then
            "${COMPOSE[@]}" -f "$COMPOSE_FILE" logs collector >&2 || true
        else
            docker logs "$COLLECTOR_NAME" >&2 || true
        fi
        exit 1
    fi
    sleep 1
done
echo "docker-diag-e2e: collector ready"

# Step 5: drive pp-smoke-run with diagnostics env vars set. Stage children
# pick up the same vars via docker-gpu-node.sh's `-e` forwarders.
OUTPUT_DIR="$(mktemp -d)"
STDOUT_LOG="$OUTPUT_DIR/stdout.log"
STDERR_LOG="$OUTPUT_DIR/stderr.log"
cleanup_output() {
    if [ -z "${PP_DIAG_KEEP_LOGS:-}" ] && [ -d "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
    fi
}
trap 'cleanup_output; cleanup_all' EXIT

set +e
PP_WORKER_STUB=1 \
PP_IMAGE="$IMAGE" \
PP_CONTAINER_PREFIX="$CONTAINER_PREFIX" \
PP_DEV=CPU \
SWACTOR_DIAG_COLLECTOR_URL="http://127.0.0.1:9080" \
SWACTOR_DIAG_RUN_ID="$RUN_ID" \
SWACTOR_DIAG_SPOOL_DIR="$OUTPUT_DIR/spool" \
SWACTOR_DIAG_UDP_ECHO="127.0.0.1:9081" \
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

if [ "$SMOKE_STATUS" -ne 0 ]; then
    echo "docker-diag-e2e: pp-smoke-run exited $SMOKE_STATUS" >&2
    echo "----- stdout -----" >&2
    cat "$STDOUT_LOG" >&2
    echo "----- stderr (last 60) -----" >&2
    tail -n 60 "$STDERR_LOG" >&2
    exit 1
fi
echo "docker-diag-e2e: pp-smoke-run exited 0"
if [ -n "${PP_DIAG_VERBOSE:-}" ]; then
    echo "----- pp-smoke-run stderr (last 30) -----"
    tail -n 30 "$STDERR_LOG"
fi

# Step 6: fetch the finalized bundle. The collector writes it both to
# its bind-mounted bundles dir (visible to the host directly) and serves
# it via `GET /diag/bundle/{run_id}` (the in-VPS retrieval path). The
# HTTP path is the one the script uses for verification because it
# works uniformly across docker-in-docker setups where the bind-mount
# path on the daemon side isn't visible to the script's filesystem.
BUNDLE_PATH="$OUTPUT_DIR/${RUN_ID}.tar.gz"
WAIT_TIMEOUT=30
WAITED=0
while true; do
    if curl -fsS -o "$BUNDLE_PATH" \
        "http://127.0.0.1:9080/diag/bundle/${RUN_ID}" 2>/dev/null
    then
        if [ -s "$BUNDLE_PATH" ]; then
            break
        fi
    fi
    WAITED=$((WAITED + 1))
    if [ "$WAITED" -ge "$WAIT_TIMEOUT" ]; then
        echo "docker-diag-e2e: bundle not retrievable from collector within ${WAIT_TIMEOUT}s" >&2
        echo "collector logs (last 50):" >&2
        if [ "$USE_COMPOSE" = 1 ]; then
            "${COMPOSE[@]}" -f "$COMPOSE_FILE" logs collector | tail -n 50 >&2 || true
        else
            docker logs "$COLLECTOR_NAME" 2>&1 | tail -n 50 >&2 || true
        fi
        echo "----- pp-smoke-run stderr (last 60) -----" >&2
        tail -n 60 "$STDERR_LOG" >&2 || true
        exit 1
    fi
    sleep 1
done
echo "docker-diag-e2e: bundle fetched to $BUNDLE_PATH ($(wc -c <"$BUNDLE_PATH") bytes)"

# Step 7: assert bundle invariants. assert-bundle.sh runs the
# post-processor and checks structural counts.
"$SCRIPT_DIR/assert-bundle.sh" "$BUNDLE_PATH" "$NUM_STAGES" "$POSTPROC_BIN"

echo "docker-diag-e2e: PASS at N=$NUM_STAGES (bundle=$BUNDLE_PATH)"
