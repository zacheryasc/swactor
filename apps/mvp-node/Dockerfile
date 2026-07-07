# MVP node CODE image — thin layer over the CUDA/tinygrad base.
# Build from workspace root after compiling the Rust binary:
#   cargo build --release -p mvp-system --bin mvp-worker-node
#   docker build -f apps/mvp-node/Dockerfile.base -t swactor-mvp-node-base:cuda12.6 .
#   docker build -f apps/mvp-node/Dockerfile --build-arg BASE_IMAGE=swactor-mvp-node-base:cuda12.6 -t swactor-mvp-node:latest .

ARG BASE_IMAGE=swactor-mvp-node-base:cuda12.6
ARG MVP_NODE_BIN=target/release/mvp-worker-node
FROM ${BASE_IMAGE}
ARG MVP_NODE_BIN=target/release/mvp-worker-node

COPY ${MVP_NODE_BIN} /usr/local/bin/mvp-node
COPY apps/mvp-node/tinygrad_worker.py /usr/local/share/mvp/tinygrad_worker.py

RUN chmod +x /usr/local/bin/mvp-node /usr/local/share/mvp/tinygrad_worker.py
