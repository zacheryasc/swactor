//! Real-I/O checks for the telemetry envelope over loopback UDP.

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use telemetry::frame::Frame;
use telemetry::ingest::Consumer;
use telemetry::mux::Mux;
use telemetry::transport::Delivery;
use telemetry::wire::{decode_delivery, encode_delivery};
use telemetry::{ChannelId, Lifetime, NodeId, Record, StreamId};

const RESOURCE_CHANNEL: ChannelId = ChannelId(1);
const LOG_CHANNEL: ChannelId = ChannelId(2);
const MEMBERSHIP_CHANNEL: ChannelId = ChannelId(3);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ResourceSample {
    tick: u64,
    cpu_pct: f32,
}

impl Record for ResourceSample {
    const CHANNEL: &'static str = "host.resource";
}

fn build_stream() -> (StreamId, Vec<Frame>) {
    let id = StreamId::new(NodeId::new("node-real"), Lifetime(1));
    let mux = Mux::unbounded(id.clone());
    for tick in 0..20 {
        mux.submit(
            RESOURCE_CHANNEL,
            ResourceSample {
                tick,
                cpu_pct: 10.0 + tick as f32,
            }
            .encode(),
        );
    }
    mux.submit(LOG_CHANNEL, b"epoch 1 complete".to_vec());
    mux.submit(MEMBERSHIP_CHANNEL, b"node-x alive->suspect".to_vec());
    (id, mux.drain())
}

fn carry_over_real_socket(stream: &StreamId, frames: &[Frame]) -> Vec<Delivery> {
    let consumer = UdpSocket::bind("127.0.0.1:0").expect("bind consumer socket");
    consumer
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("set timeout");
    let consumer_addr = consumer.local_addr().expect("consumer addr");

    let node = UdpSocket::bind("127.0.0.1:0").expect("bind node socket");
    for frame in frames {
        let datagram = encode_delivery(stream, frame);
        let _ = node.send_to(&datagram, consumer_addr);
    }

    let mut delivered = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match consumer.recv_from(&mut buf) {
            Ok((n, _)) => {
                if let Ok((s, frame)) = decode_delivery(&buf[..n]) {
                    delivered.push(Delivery::new(s, frame));
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }
    }
    delivered
}

#[test]
fn real_transport_stays_within_the_envelope() {
    let (id, sent) = build_stream();
    let delivered = carry_over_real_socket(&id, &sent);

    let by_position: HashMap<u64, &Frame> = sent.iter().map(|f| (f.position.0, f)).collect();
    let mut seen = HashSet::new();
    for d in &delivered {
        assert_eq!(d.stream, id, "the carrier did not alter the stream id");
        let original = by_position
            .get(&d.frame.position.0)
            .expect("a delivered position was never sent");
        assert_eq!(&d.frame, *original);
        assert!(seen.insert(d.frame.position.0), "no duplicate positions");
    }
}

#[test]
fn wiring_smoke_some_frames_arrive_and_reconstruct() {
    let (id, sent) = build_stream();
    let delivered = carry_over_real_socket(&id, &sent);
    let mut consumer = Consumer::new();
    consumer.ingest(delivered);

    let stored = consumer
        .store()
        .stream(&id)
        .expect("the node's frames reached the consumer");
    assert!(
        !stored.is_empty(),
        "some frames arrived over the real transport"
    );

    let by_position: HashMap<u64, &Frame> = sent.iter().map(|f| (f.position.0, f)).collect();
    let mut prev: Option<u64> = None;
    for frame in stored.frames() {
        assert_eq!(
            frame,
            *by_position.get(&frame.position.0).expect("sent frame")
        );
        if let Some(p) = prev {
            assert!(frame.position.0 > p, "reconstructed in position order");
        }
        prev = Some(frame.position.0);
    }
}
