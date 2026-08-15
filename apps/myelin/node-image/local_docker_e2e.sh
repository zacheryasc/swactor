#!/usr/bin/env bash
set -euo pipefail

BASE_IMAGE=${BASE_IMAGE:-myelin-node-base:cuda12.6}
IMAGE=${IMAGE:-myelin-node:latest}
CONTAINER=${CONTAINER:-myelin-node-e2e-$$}
GPUS=${MYELIN_CUDA_GPUS:-all}
PROMPT=${MYELIN_NODE_SELF_TEST_PROMPT:-ping}
TIMEOUT_SECS=${MYELIN_NODE_E2E_TIMEOUT_SECS:-1800}
FRAME_LOG=${MYELIN_TELEMETRY_FRAME_LOG:-/var/log/myelin-telemetry.ndjson}

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cargo build --release -p myelin --bin myelin-worker
docker build -f apps/myelin/node-image/Dockerfile.base -t "$BASE_IMAGE" .
docker build -f apps/myelin/node-image/Dockerfile --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$IMAGE" .

docker run -d \
    --name "$CONTAINER" \
    --gpus "$GPUS" \
    -e MYELIN_NODE_SELF_TEST_PROMPT="$PROMPT" \
    -e MYELIN_NODE_MAX_RUNTIME_SECS=1 \
    -e MYELIN_SELF_TEST_MAX_TOKENS="${MYELIN_SELF_TEST_MAX_TOKENS:-1}" \
    -e MYELIN_MODEL_CACHE_DIR=/var/cache/myelin-models \
    -e MYELIN_TELEMETRY_FRAME_LOG="$FRAME_LOG" \
    ${HF_TOKEN:+-e HF_TOKEN="$HF_TOKEN"} \
    "$IMAGE" >/dev/null

deadline=$((SECONDS + TIMEOUT_SECS))
while (( SECONDS < deadline )); do
    logs=$(docker logs "$CONTAINER" 2>&1 || true)
    if grep -q '"type":"ready"' <<<"$logs" && grep -q '"type":"self_test_completed"' <<<"$logs"; then
        frames=$(docker exec "$CONTAINER" cat "$FRAME_LOG" 2>/dev/null || true)
        if grep -q '"channel":"myelin.node.ready"' <<<"$frames" &&
           grep -q '"channel":"myelin.worker.weights"' <<<"$frames" &&
           grep -q '"channel":"myelin.worker.prompt"' <<<"$frames"; then
            printf '%s\n' "$logs"
            printf '%s\n' "$frames"
            exit 0
        fi
    fi
    if grep -q 'WorkerFatal\|myelin-node: .*failed\|ModelLoadFailed\|GgufDownloadFailed' <<<"$logs"; then
        printf '%s\n' "$logs" >&2
        exit 1
    fi
    sleep 5
done

docker logs "$CONTAINER" 2>&1 || true
echo "myelin-node Docker E2E timed out after ${TIMEOUT_SECS}s" >&2
exit 1
