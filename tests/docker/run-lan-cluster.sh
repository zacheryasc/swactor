#!/usr/bin/env bash
#
# Run a 5-node swactor cluster across two physical machines:
#   hpz       (192.168.1.106) — seed + node-2
#   thinkpad  (192.168.1.102) — node-3, node-4, node-5
#
# Usage:  ./tests/docker/run-lan-cluster.sh [--no-build] [--teardown-only]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

HPZ_IP="192.168.1.106"
THINKPAD_IP="192.168.1.102"
THINKPAD_SSH="thinkpad"
THINKPAD_REPO="/home/zach/swactor-distribution-realization"

HPZ_COMPOSE="$SCRIPT_DIR/docker-compose.lan-hpz.yml"
THINKPAD_COMPOSE="tests/docker/docker-compose.lan-thinkpad.yml"

# Dashboard endpoints
HPZ_DASHBOARDS=("http://127.0.0.1:9091" "http://127.0.0.1:9092")
THINKPAD_DASHBOARDS=("http://$THINKPAD_IP:9093" "http://$THINKPAD_IP:9094" "http://$THINKPAD_IP:9095")
ALL_DASHBOARDS=("${HPZ_DASHBOARDS[@]}" "${THINKPAD_DASHBOARDS[@]}")

CONVERGE_TIMEOUT=60
EXPECTED_ALIVE=4
NO_BUILD=false
TEARDOWN_ONLY=false

for arg in "$@"; do
    case "$arg" in
        --no-build) NO_BUILD=true ;;
        --teardown-only) TEARDOWN_ONLY=true ;;
    esac
done

# ── Cleanup on exit ──────────────────────────────────────────────────────────
teardown() {
    echo ""
    echo "=== Tearing down ==="
    echo "Stopping hpz nodes..."
    docker compose -f "$HPZ_COMPOSE" down --timeout 5 2>/dev/null || true
    echo "Stopping thinkpad nodes..."
    ssh "$THINKPAD_SSH" "cd $THINKPAD_REPO && docker compose -f $THINKPAD_COMPOSE down --timeout 5" 2>/dev/null || true
    echo "Done."
}
trap teardown EXIT

if $TEARDOWN_ONLY; then
    exit 0
fi

# ── Sync repo to thinkpad ───────────────────────────────────────────────────
echo "=== Syncing repo to thinkpad ==="
tar czf /tmp/swactor-repo.tar.gz -C "$REPO_ROOT" --exclude=target --exclude=.git .
scp -q /tmp/swactor-repo.tar.gz "$THINKPAD_SSH":/tmp/
ssh "$THINKPAD_SSH" "mkdir -p $THINKPAD_REPO && tar xzf /tmp/swactor-repo.tar.gz -C $THINKPAD_REPO"
echo "Synced."

# ── Build images ─────────────────────────────────────────────────────────────
if ! $NO_BUILD; then
    echo ""
    echo "=== Building Docker image on hpz ==="
    docker compose -f "$HPZ_COMPOSE" build --quiet

    echo "=== Building Docker image on thinkpad ==="
    ssh "$THINKPAD_SSH" "cd $THINKPAD_REPO && docker compose -f $THINKPAD_COMPOSE build --quiet"
    echo "Images built."
fi

# ── Start clusters ───────────────────────────────────────────────────────────
echo ""
echo "=== Starting hpz nodes (seed + node-2) ==="
docker compose -f "$HPZ_COMPOSE" up -d

echo "=== Starting thinkpad nodes (node-3, node-4, node-5) ==="
ssh "$THINKPAD_SSH" "cd $THINKPAD_REPO && docker compose -f $THINKPAD_COMPOSE up -d"

# ── Wait for convergence ─────────────────────────────────────────────────────
echo ""
echo "=== Waiting for cluster convergence (timeout: ${CONVERGE_TIMEOUT}s) ==="

start_time=$(date +%s)
while true; do
    elapsed=$(( $(date +%s) - start_time ))
    if [ "$elapsed" -ge "$CONVERGE_TIMEOUT" ]; then
        echo ""
        echo "TIMEOUT after ${elapsed}s. Dumping last state:"
        for url in "${ALL_DASHBOARDS[@]}"; do
            echo -n "  $url: "
            curl -sf "$url/api/distribution" 2>/dev/null \
                | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'alive={d[\"alive_count\"]}, routing={d[\"routing_table_size\"]}, dir={d[\"directory_entry_count\"]}, cache={d[\"cache_size\"]}')" \
                2>/dev/null || echo "unreachable"
        done
        echo ""
        echo "FAIL: cluster did not converge within ${CONVERGE_TIMEOUT}s"
        exit 1
    fi

    all_ok=true
    for url in "${ALL_DASHBOARDS[@]}"; do
        alive=$(curl -sf "$url/api/distribution" 2>/dev/null \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('alive_count',0))" 2>/dev/null) || alive=0
        if [ "$alive" -lt "$EXPECTED_ALIVE" ]; then
            all_ok=false
            break
        fi
    done

    if $all_ok; then
        echo "Converged after ${elapsed}s."
        break
    fi

    printf "."
    sleep 1
done

# ── Report ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Cluster Status ==="
printf "%-35s %6s %8s %5s %6s\n" "ENDPOINT" "ALIVE" "ROUTING" "DIR" "CACHE"
for url in "${ALL_DASHBOARDS[@]}"; do
    data=$(curl -sf "$url/api/distribution" 2>/dev/null) || { echo "$url: unreachable"; continue; }
    echo "$data" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f'  {\"$url\":<33} {d[\"alive_count\"]:>6} {d[\"routing_table_size\"]:>8} {d[\"directory_entry_count\"]:>5} {d[\"cache_size\"]:>6}')
"
done

# ── Assertions ───────────────────────────────────────────────────────────────
echo ""
echo "=== Assertions ==="
pass=true

for url in "${ALL_DASHBOARDS[@]}"; do
    data=$(curl -sf "$url/api/distribution" 2>/dev/null) || { echo "FAIL: $url unreachable"; pass=false; continue; }
    alive=$(echo "$data" | python3 -c "import json,sys; print(json.load(sys.stdin)['alive_count'])")
    routing=$(echo "$data" | python3 -c "import json,sys; print(json.load(sys.stdin)['routing_table_size'])")
    dir=$(echo "$data" | python3 -c "import json,sys; print(json.load(sys.stdin)['directory_entry_count'])")

    if [ "$alive" -lt 4 ]; then echo "FAIL: $url alive=$alive (expected >= 4)"; pass=false; fi
    if [ "$routing" -lt 3 ]; then echo "FAIL: $url routing=$routing (expected >= 3)"; pass=false; fi
    if [ "$dir" -lt 2 ]; then echo "FAIL: $url dir=$dir (expected >= 2)"; pass=false; fi
done

if $pass; then
    echo "ALL PASS"
    echo ""
    echo "Cluster is running. Press Ctrl-C to tear down, or run:"
    echo "  $0 --teardown-only"
    # Keep running so user can inspect
    read -r -p "Press Enter to tear down..."
else
    echo ""
    echo "SOME ASSERTIONS FAILED"
    exit 1
fi
