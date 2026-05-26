FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04

# Runtime base, not devel: the devel base alone is ~5 GB and blew past the
# vastai image-pull budget (the previous 7.55 GB build). NVRTC — the kernel
# compiler tinygrad's CUDA backend uses — ships in the runtime image, but the
# CUDA *toolkit headers* do not, and tinygrad's generated fp16 kernels
# `#include <cuda_fp16.h>`. Pull in just the cudart dev headers (~7 MB) so
# NVRTC's `-I/usr/local/cuda/include` resolves them — the minimal alternative
# to the full devel base. Without this every real-model stage dies at
# graph-realize with NVRTC_ERROR_COMPILATION ("cannot open cuda_fp16.h").
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        ca-certificates \
        cuda-cudart-dev-12-6 && \
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
