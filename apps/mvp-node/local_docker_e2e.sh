#!/usr/bin/env bash
set -euo pipefail

BASE_IMAGE=${BASE_IMAGE:-swactor-mvp-node-base:cuda12.6}
IMAGE=${IMAGE:-swactor-mvp-node:latest}
CONTAINER=${CONTAINER:-swactor-mvp-node-e2e-$$}
GPUS=${MVP_CUDA_GPUS:-all}
PROMPT=${MVP_NODE_SELF_TEST_PROMPT:-ping}
TIMEOUT_SECS=${MVP_NODE_E2E_TIMEOUT_SECS:-1800}
FRAME_LOG=${MVP_DATASTREAM_FRAME_LOG:-/var/log/mvp-datastream.ndjson}

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cargo build --release -p mvp-system --bin mvp-node
docker build -f apps/mvp-node/Dockerfile.base -t "$BASE_IMAGE" .
docker build -f apps/mvp-node/Dockerfile --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$IMAGE" .

docker run -d \
    --name "$CONTAINER" \
    --gpus "$GPUS" \
    -e MVP_NODE_SELF_TEST_PROMPT="$PROMPT" \
    -e MVP_NODE_MAX_RUNTIME_SECS=1 \
    -e MVP_SELF_TEST_MAX_TOKENS="${MVP_SELF_TEST_MAX_TOKENS:-1}" \
    -e MVP_MODEL_CACHE_DIR=/var/cache/mvp-models \
    -e MVP_DATASTREAM_FRAME_LOG="$FRAME_LOG" \
    ${HF_TOKEN:+-e HF_TOKEN="$HF_TOKEN"} \
    "$IMAGE" >/dev/null

deadline=$((SECONDS + TIMEOUT_SECS))
while (( SECONDS < deadline )); do
    logs=$(docker logs "$CONTAINER" 2>&1 || true)
    if grep -q '"type":"ready"' <<<"$logs" && grep -q '"type":"self_test_completed"' <<<"$logs"; then
        frames=$(docker exec "$CONTAINER" cat "$FRAME_LOG" 2>/dev/null || true)
        if grep -q '"channel":"mvp.node.ready"' <<<"$frames" &&
           grep -q '"channel":"mvp.worker.weights"' <<<"$frames" &&
           grep -q '"channel":"mvp.worker.prompt"' <<<"$frames"; then
            printf '%s\n' "$logs"
            printf '%s\n' "$frames"
            exit 0
        fi
    fi
    if grep -q 'WorkerFatal\|mvp-node: .*failed\|ModelLoadFailed\|GgufDownloadFailed' <<<"$logs"; then
        printf '%s\n' "$logs" >&2
        exit 1
    fi
    sleep 5
done

docker logs "$CONTAINER" 2>&1 || true
echo "mvp-node Docker E2E timed out after ${TIMEOUT_SECS}s" >&2
exit 1
