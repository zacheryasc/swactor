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

# Copy the pipeline-parallel binaries and tinygrad worker
# Build context should be the workspace root:
#   docker build -f examples/pipeline-parallel-inference/Dockerfile -t <tag> .
COPY examples/pipeline-parallel-inference/target/release/pp-gpu-node /usr/local/bin/pp-gpu-node
COPY examples/pipeline-parallel-inference/target/release/pp-smoke-run /usr/local/bin/pp-smoke-run
COPY examples/pipeline-parallel-inference/pp_tinygrad_worker.py /usr/local/share/pp_tinygrad_worker.py

# Enable CUDA backend for tinygrad (override with -e DEV=CPU for CPU runs)
ENV CUDA=1
ENV WORKER_SCRIPT=/usr/local/share/pp_tinygrad_worker.py

CMD ["pp-gpu-node"]
