FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        build-essential \
        pkg-config \
        python3 \
        python3-pip \
        cuda-cudart-dev-12-6 \
        cuda-nvrtc-12-6 && \
    python3 -m pip install --no-cache-dir --break-system-packages tinygrad==0.12.0 numpy && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH=/root/.cargo/bin:$PATH
ENV DEV=CUDA
ENV PYTHONDONTWRITEBYTECODE=1
COPY . /workspace
WORKDIR /workspace
CMD ["sh", "-lc", "python3 -c 'import os; print(\"cuda preflight: importing tinygrad\", flush=True); from tinygrad import Tensor,dtypes; print(\"cuda preflight: DEV=\" + str(os.environ.get(\"DEV\")), flush=True); print(\"cuda preflight: realizing Tensor([1])\", flush=True); print(Tensor([1], dtype=dtypes.int32).realize().numpy().tolist(), flush=True)' && cargo test -p mvp-system --features local-e2e --test gpu_worker_node_e2e -- --nocapture"]
