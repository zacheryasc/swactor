FROM nvidia/cuda:12.6.3-devel-ubuntu24.04

# Install Python 3, pip, and CUDA runtime compiler (tinygrad compiles kernels via NVRTC)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install tinygrad and numpy
RUN python3 -m pip install --no-cache-dir --break-system-packages \
    tinygrad==0.12.0 \
    numpy

# Copy the gpu-node binary and tinygrad worker
# Build context should be the workspace root:
#   docker build -f examples/single-gpu-inference/Dockerfile -t <tag> .
COPY examples/single-gpu-inference/target/release/gpu-node /usr/local/bin/gpu-node
COPY examples/single-gpu-inference/tinygrad_worker.py /usr/local/share/tinygrad_worker.py

# Enable CUDA backend for tinygrad
ENV CUDA=1
ENV WORKER_SCRIPT=/usr/local/share/tinygrad_worker.py

CMD ["gpu-node"]
