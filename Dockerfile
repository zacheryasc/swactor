FROM scratch
COPY target/x86_64-unknown-linux-musl/release/swactor /swactor
COPY target/x86_64-unknown-linux-musl/release/swactor-datastream-collector /collector
ENTRYPOINT ["/swactor"]
