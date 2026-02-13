FROM rust:1.93-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p node

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/swactor-node /usr/local/bin/
ENTRYPOINT ["swactor-node"]
