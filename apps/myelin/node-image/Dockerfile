# Myelin node CODE image — thin layer over the CUDA/tinygrad base.
# Build from workspace root after compiling the Rust binary:
#   cargo build --release -p myelin --bin myelin-worker
#   docker build -f apps/myelin/node-image/Dockerfile.base -t myelin-node-base:cuda12.6 .
#   docker build -f apps/myelin/node-image/Dockerfile --build-arg BASE_IMAGE=myelin-node-base:cuda12.6 -t myelin-node:latest .

ARG BASE_IMAGE=myelin-node-base:cuda12.6
ARG MYELIN_NODE_BIN=target/release/myelin-worker
FROM ${BASE_IMAGE}
ARG MYELIN_NODE_BIN=target/release/myelin-worker

COPY ${MYELIN_NODE_BIN} /usr/local/bin/myelin-node
COPY apps/myelin/node-image/tinygrad_worker.py /usr/local/share/myelin/tinygrad_worker.py

RUN chmod +x /usr/local/bin/myelin-node /usr/local/share/myelin/tinygrad_worker.py
