# Pipeline-parallel CODE image — thin layer over the heavy base.
#
# Carries only the fast-changing artifacts: the pipeline binaries, the
# diagnostics binaries, and the worker script. Everything heavy (CUDA libs,
# tinygrad, NVRTC headers, sshd, the PID-1 supervisor, ENTRYPOINT) lives in
# the base image, so a code push rebuilds just this handful of COPYs — no apt,
# no pip. The diagnostics binaries ship by default; they are inert unless
# SWACTOR_DIAG_COLLECTOR_URL is set, so they cost nothing at runtime.
#
# Stub mode is a runtime toggle (-e PP_WORKER_STUB=1), not a separate image:
# the worker only imports tinygrad in real mode, so the CUDA base is inert
# under the stub and the same image runs the no-GPU CPU E2E.
#
# Build context must be the workspace root (the COPYs reach into both
# target/release/ trees). Build the base first, then this image:
#   docker build -f examples/pipeline-parallel-inference/Dockerfile.base \
#       -t swactor-pp-base:cuda12.6 .
#   docker build -f examples/pipeline-parallel-inference/Dockerfile \
#       -t swactor-pp-gpu:latest .
ARG BASE_IMAGE=swactor-pp-base:cuda12.6
FROM ${BASE_IMAGE}

# Pipeline binaries (this crate's target/) + diagnostics binaries (the
# workspace-root target/). All are statically linked enough that the base
# stage's libc is all they need; the worker is pure Python.
COPY examples/pipeline-parallel-inference/target/release/pp-worker  /usr/local/bin/pp-worker
COPY examples/pipeline-parallel-inference/target/release/pp-orchestrator /usr/local/bin/pp-orchestrator
COPY target/release/swactor-diag-collector /usr/local/bin/swactor-diag-collector
COPY target/release/swactor-diag-postproc  /usr/local/bin/swactor-diag-postproc
COPY examples/pipeline-parallel-inference/pp_tinygrad_worker.py /usr/local/share/pp_tinygrad_worker.py

ENV WORKER_SCRIPT=/usr/local/share/pp_tinygrad_worker.py
