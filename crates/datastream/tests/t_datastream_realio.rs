//! The two real-I/O checks that license trusting the offline work
//! (`DATASTREAM_TESTING_SPEC.md` §9–§10).
//!
//! Every offline test in `t_datastream.rs` replaces the transport with a
//! script. That substitution is honest only if the *real* transport never
//! does anything the script cannot express. These two tests are the only
//! ones that pay the cost of real I/O, and they are what convert "we
//! reasoned about the transport" into "we verified it":
//!
//! * the **envelope-conformance check** (§9) sends a stream over a real
//!   local socket and asserts the carrier never leaves the envelope —
//!   whatever it delivers is a reordered subsequence of what was sent,
//!   payloads byte-identical, positions intact. It asserts *no* delivery,
//!   so loss is allowed and it cannot be flaky;
//! * the **wiring smoke** (§10) stands a node and a consumer up over the
//!   real socket and requires that *some* frames arrive and reconstruct —
//!   proving the path is actually connected, not that it is complete.
//!
//! The "real transport" here is a loopback UDP datagram socket: real OS I/O,
//! best-effort like the production carrier, carrying the very same
//! [`encode_delivery`] envelope a deployed transport would. The production
//! code under test is real throughout — the mux, the wire envelope, ingest,
//! the store, and reconstruction; only the carrier is a local stand-in for
//! the deployed one.

#[path = "datastream_support/mod.rs"]
mod support;

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::time::Duration;

use datastream::frame::{Frame, Lifetime, NodeId, StreamId};
use datastream::ingest::Consumer;
use datastream::transport::Delivery;
use datastream::wire::{decode_delivery, encode_delivery};

use support::schema::ProcStream;
use support::{Node, payloads};

/// A realistic node stream, produced by the real mux.
fn build_stream() -> (StreamId, Vec<Frame>) {
    let id = StreamId::new(NodeId::new("node-real"), Lifetime(1));
    let node = Node::new(id.clone());
    node.emit(&payloads::identity("node-real", 1));
    for tick in 0..20 {
        node.emit(&payloads::resource(tick));
    }
    node.emit_text("trainer", ProcStream::Stdout, "epoch 1 complete");
    node.emit(&payloads::membership("node-x", "alive", "suspect"));
    (id, node.sent())
}

/// Carry a node's frames to a consumer over a real loopback UDP socket,
/// each frame as one [`encode_delivery`] datagram, and return what the
/// consumer actually received. Best-effort: send and receive errors are
/// treated as loss, never as failures.
fn carry_over_real_socket(stream: &StreamId, frames: &[Frame]) -> Vec<Delivery> {
    let consumer = UdpSocket::bind("127.0.0.1:0").expect("bind consumer socket");
    consumer
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("set timeout");
    let consumer_addr = consumer.local_addr().expect("consumer addr");

    let node = UdpSocket::bind("127.0.0.1:0").expect("bind node socket");
    for frame in frames {
        let datagram = encode_delivery(stream, frame);
        // A send failure (e.g. a full socket buffer) is just loss.
        let _ = node.send_to(&datagram, consumer_addr);
    }

    // Drain whatever is waiting; stop on the first read timeout.
    let mut delivered = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match consumer.recv_from(&mut buf) {
            Ok((n, _)) => {
                if let Ok((s, frame)) = decode_delivery(&buf[..n]) {
                    delivered.push(Delivery::new(s, frame));
                }
            }
            Err(ref e)
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

/// §9 conformance — the real transport never leaves the envelope: whatever
/// it delivers is a reordered subsequence of what was sent, byte-identical,
/// positions intact. Asserts no specific delivery, so loss is tolerated and
/// the test is not flaky.
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
            .expect("a delivered position was never sent — the carrier fabricated a frame");
        assert_eq!(
            &d.frame, *original,
            "payload byte-identical, channel and position intact — no corruption or alteration"
        );
        assert!(
            seen.insert(d.frame.position.0),
            "no duplicate — a subsequence has no repeats"
        );
    }
    // Deliberately no assertion on how many arrived: loss is within the
    // envelope, so the check is sound without requiring delivery.
}

/// §10 wiring smoke — telemetry is actually plugged in: a node's stream,
/// produced by the real mux and carried over a real socket, reaches a real
/// consumer and reconstructs. Loose by design (best-effort): it requires
/// *some* frames to arrive, not all, and tolerates loss and reorder.
#[test]
fn wiring_smoke_some_frames_arrive_and_reconstruct() {
    let (id, sent) = build_stream();

    // node (real mux output) → real socket → real ingest → store.
    let delivered = carry_over_real_socket(&id, &sent);
    let mut consumer = Consumer::new();
    consumer.ingest(delivered);

    let stored = consumer
        .store()
        .stream(&id)
        .expect("the path is connected: the node's frames reached the consumer");
    assert!(
        !stored.is_empty(),
        "some frames arrived over the real transport"
    );

    // Whatever arrived reconstructs correctly: each stored frame is the
    // original at that position, and the store is in position order.
    let by_position: HashMap<u64, &Frame> = sent.iter().map(|f| (f.position.0, f)).collect();
    let mut prev: Option<u64> = None;
    for frame in stored.frames() {
        assert_eq!(
            frame,
            *by_position
                .get(&frame.position.0)
                .expect("only sent frames arrive"),
            "a reconstructed frame is the original, byte-identical"
        );
        if let Some(p) = prev {
            assert!(frame.position.0 > p, "reconstructed in position order");
        }
        prev = Some(frame.position.0);
    }
}
