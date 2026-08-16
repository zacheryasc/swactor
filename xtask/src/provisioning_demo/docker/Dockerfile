# Demo node image: the static-musl xtask binary and nothing else.
# Staged into a one-file build context by
# xtask/src/provisioning_demo/docker.rs (`build_image`); never built from
# the repo root. The binary is fully static (musl + ring), so `scratch`
# needs no libc, no CA bundle (relay disabled; direct iroh only), and no
# shell — kill signals reach PID 1 = the node role directly.
FROM scratch
COPY xtask /xtask
ENTRYPOINT ["/xtask"]
