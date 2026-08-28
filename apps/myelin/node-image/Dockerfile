# Myelin node agent with the Python Swactor binding used by contextual scripts.
# Build from the workspace root:
#   docker build -f apps/myelin/node-image/Dockerfile.base -t myelin-node-base:cuda12.6 .
#   docker build -f apps/myelin/node-image/Dockerfile -t myelin-node:latest .
ARG BASE_IMAGE=myelin-node-base:cuda12.6
FROM ${BASE_IMAGE} AS wheel-builder
ARG RUST_TOOLCHAIN=nightly-2026-02-07

ENV PATH=/root/.cargo/bin:${PATH}
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        pkg-config \
        python3 \
        python3-dev \
        python3-pip && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --profile minimal --default-toolchain none && \
    rustup toolchain install "${RUST_TOOLCHAIN}" --profile minimal --component rustc-dev && \
    python3 -m pip install --break-system-packages --no-cache-dir 'maturin>=1.7,<2' && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,id=myelin-python-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=myelin-python-target,target=/workspace/target \
    cargo build --release -p myelin --bin myelin-worker && \
    install -Dm0755 target/release/myelin-worker /out/myelin-worker && \
    maturin build \
        --manifest-path crates/bindings/python/Cargo.toml \
        --release \
        --interpreter python3 \
        --out /wheel

FROM ${BASE_IMAGE}
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
           /var/cache/apt/archives/* \
           /var/cache/apt/*.bin \
           /var/log/apt/* \
           /var/log/dpkg.log \
           /tmp/* \
           /var/tmp/*
COPY --from=wheel-builder /out/myelin-worker /usr/local/bin/myelin-node
COPY --from=wheel-builder /wheel/*.whl /tmp/swactor-wheel/
RUN python3 -m pip install --break-system-packages --no-cache-dir /tmp/swactor-wheel/*.whl && \
    rm -rf /tmp/swactor-wheel && \
    apt-get purge -y --auto-remove python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \
           /var/cache/apt/archives/* \
           /var/cache/apt/*.bin \
           /var/log/apt/* \
           /var/log/dpkg.log \
           /tmp/* \
           /var/tmp/* && \
    chmod +x /usr/local/bin/myelin-node
