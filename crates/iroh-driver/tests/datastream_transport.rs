use std::net::{IpAddr, SocketAddr};

use datastream::{ChannelId, DatastreamEndpoint, Lifetime, NodeId, Position, StreamId};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use iroh_driver::{
    DATASTREAM_ALPN, DatastreamQuicHeader, read_next_uni_from_connection,
    write_available_subscription,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iroh_datastream_alpn_carries_endpoint_subscription() {
    let source = test_endpoint().await;
    let collector = test_endpoint().await;
    let collector_addr = endpoint_addr(&collector);
    let collector_accept = {
        let collector = collector.clone();
        tokio::spawn(async move {
            collector
                .accept()
                .await
                .expect("incoming connection")
                .await
                .expect("accepted connection")
        })
    };

    let stream = StreamId::new(NodeId::new("source-node"), Lifetime(1));
    let endpoint = DatastreamEndpoint::with_capacity(stream.clone(), 8, 8);
    let subscription = endpoint.subscribe_all("iroh");
    let producer = endpoint.producer();
    producer.set_frame_timing_enabled(false);

    producer.submit_text("runtime.log", "alpha");
    producer.submit_text("runtime.log", "beta");
    endpoint.tick();

    let conn = source
        .connect(collector_addr, DATASTREAM_ALPN)
        .await
        .expect("connect datastream ALPN");
    let send = conn.open_uni().await.expect("open uni stream");
    let header = DatastreamQuicHeader::new([7; 16], b"token".to_vec());
    let wrote = write_available_subscription(send, &header, &subscription)
        .await
        .expect("write subscription");
    assert_eq!(wrote.deliveries, 2);

    let accepted = collector_accept.await.expect("collector accept task");
    let read = read_next_uni_from_connection(&accepted)
        .await
        .expect("read datastream uni stream");

    assert_eq!(read.header, header);
    assert_eq!(read.deliveries.len(), 2);
    assert_eq!(read.deliveries[0].stream, stream);
    assert_eq!(read.deliveries[0].frame.position, Position(0));
    assert_eq!(
        read.deliveries[0].frame.channel,
        ChannelId::new("runtime.log")
    );
    assert_eq!(read.deliveries[0].frame.payload, b"alpha");
    assert_eq!(read.deliveries[1].frame.position, Position(1));
    assert_eq!(read.deliveries[1].frame.payload, b"beta");

    source.close().await;
    collector.close().await;
}

async fn test_endpoint() -> Endpoint {
    Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![DATASTREAM_ALPN.to_vec()])
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
