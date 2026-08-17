# Myelin node agent: a thin Rust control-plane layer over the CUDA base.
# Workload frameworks are supplied by job payload images.
# Build from workspace root after compiling the Rust binary:
#   cargo build --release -p myelin --bin myelin-worker
#   docker build -f apps/myelin/node-image/Dockerfile.base -t myelin-node-base:cuda12.6 .
#   docker build -f apps/myelin/node-image/Dockerfile --build-arg BASE_IMAGE=myelin-node-base:cuda12.6 -t myelin-node:latest .

ARG BASE_IMAGE=myelin-node-base:cuda12.6
ARG MYELIN_NODE_BIN=target/release/myelin-worker
FROM ${BASE_IMAGE}
ARG MYELIN_NODE_BIN=target/release/myelin-worker

COPY ${MYELIN_NODE_BIN} /usr/local/bin/myelin-node
RUN chmod +x /usr/local/bin/myelin-node
