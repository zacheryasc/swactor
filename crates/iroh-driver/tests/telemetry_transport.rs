use std::net::{IpAddr, SocketAddr};

use telemetry::frame::TelemetryEvent;
use telemetry::{
    ChannelContent, TelemetryEndpoint, Lifetime, NodeId, Position, StreamId,
};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use iroh_driver::{
    TELEMETRY_ALPN, TelemetryQuicHeader, read_next_uni_from_connection,
    write_available_subscription,
};
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

/// Telemetry transport test scheduled through `EngineHandle`, not an ambient
/// `#[tokio::test]` runtime (ENGINE_SPEC.md).
#[test]
fn iroh_telemetry_alpn_carries_catalog_and_numeric_frames() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("test backend"),
    )
    .expect("test engine");
    let handle = engine.handle();

    let (done_tx, done_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    let h = handle.clone();
    handle.spawn(async move {
        let source = test_endpoint().await;
        let collector = test_endpoint().await;
        let collector_addr = endpoint_addr(&collector);

        // Accept the incoming connection through an engine-hosted task + oneshot,
        // since EngineHandle::spawn is fire-and-forget (no JoinHandle).
        let (accept_tx, accept_rx) = tokio::sync::oneshot::channel();
        {
            let collector = collector.clone();
            h.spawn(async move {
                let conn = collector
                    .accept()
                    .await
                    .expect("incoming connection")
                    .await
                    .expect("accepted connection");
                let _ = accept_tx.send(conn);
            });
        }

        let stream = StreamId::new(NodeId::new("source-node"), Lifetime(1));
        let endpoint = TelemetryEndpoint::with_capacity(stream.clone(), 8, 8);
        let producer = endpoint.producer();
        let runtime_log = producer.register_channel("runtime.log", ChannelContent::TextStream);
        let subscription = endpoint.subscribe_all("iroh");

        producer.submit_text(runtime_log, "alpha");
        producer.submit_text(runtime_log, "beta");
        endpoint.tick();

        let conn = source
            .connect(collector_addr, TELEMETRY_ALPN)
            .await
            .expect("connect telemetry ALPN");
        let send = conn.open_uni().await.expect("open uni stream");
        let header =
            TelemetryQuicHeader::from_snapshot([7; 16], b"token".to_vec(), subscription.snapshot())
                .expect("header from subscription snapshot");
        let wrote = write_available_subscription(&h, send, &header, &subscription)
            .await
            .expect("write subscription");
        assert_eq!(wrote.events, 2);

        let accepted = accept_rx.await.expect("collector accept task");
        let read = read_next_uni_from_connection(&accepted)
            .await
            .expect("read telemetry uni stream");

        assert_eq!(read.header, header);
        assert_eq!(read.header.stream.stream, stream);
        assert!(
            read.header
                .channels
                .iter()
                .any(|descriptor| descriptor.id == runtime_log && descriptor.name == "runtime.log")
        );
        assert_eq!(read.events.len(), 2);
        match &read.events[0] {
            TelemetryEvent::Frame(frame) => {
                assert_eq!(frame.channel.stream, stream);
                assert_eq!(frame.channel.channel, runtime_log);
                assert_eq!(frame.position, Position(0));
                assert_eq!(frame.payload, b"alpha");
            }
            other => panic!("expected frame event, got {other:?}"),
        }
        match &read.events[1] {
            TelemetryEvent::Frame(frame) => {
                assert_eq!(frame.position, Position(1));
                assert_eq!(frame.payload, b"beta");
            }
            other => panic!("expected frame event, got {other:?}"),
        }

        source.close().await;
        collector.close().await;
        let _ = done_tx.send(Ok(()));
    });

    match done_rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("test failed: {e}"),
        Err(_) => panic!("test task dropped"),
    }
}

async fn test_endpoint() -> Endpoint {
    Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![TELEMETRY_ALPN.to_vec()])
        .bind()
        .await
        .expect("bind test endpoint")
}

fn endpoint_addr(endpoint: &Endpoint) -> EndpointAddr {
    let mut addr = EndpointAddr::new(endpoint.id());
    for socket in endpoint.bound_sockets() {
        addr = addr.with_ip_addr(loopback_if_unspecified(socket));
    }
    addr
}

fn loopback_if_unspecified(socket: SocketAddr) -> SocketAddr {
    match socket.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), socket.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), socket.port())
        }
        _ => socket,
    }
}
