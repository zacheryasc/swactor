FROM rust:1-bookworm

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        docker.io \
        pkg-config \
        python3 \
        python3-pip && \
    python3 -m pip install --no-cache-dir --break-system-packages tinygrad==0.12.0 numpy && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV DEV=CPU
ENV PYTHONDONTWRITEBYTECODE=1
ENV CARGO_TARGET_DIR=/tmp/mvp-local-e2e-target
COPY . /workspace
WORKDIR /workspace
RUN cargo test -p mvp-system --features local-e2e --test local-e2e-cluster --no-run && \
    test_bin="$(find /tmp/mvp-local-e2e-target/debug/deps -maxdepth 1 -type f -perm /111 \( -name 'local_e2e_cluster-*' -o -name 'local-e2e-cluster-*' \) | head -n 1)" && \
    cp "$test_bin" /usr/local/bin/local-e2e-cluster && \
    rm -rf /tmp/mvp-local-e2e-target

ENTRYPOINT ["/usr/local/bin/local-e2e-cluster"]
