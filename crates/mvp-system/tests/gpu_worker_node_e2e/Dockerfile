FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        build-essential \
        pkg-config \
        python3 \
        python3-pip && \
    python3 -m pip install --no-cache-dir --break-system-packages tinygrad numpy && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH=/root/.cargo/bin:$PATH
ENV DEV=CUDA
ENV PYTHONDONTWRITEBYTECODE=1
COPY . /workspace
WORKDIR /workspace
CMD ["cargo", "test", "-p", "mvp-system", "--features", "local-e2e", "--test", "gpu_worker_node_e2e", "--", "--nocapture"]
