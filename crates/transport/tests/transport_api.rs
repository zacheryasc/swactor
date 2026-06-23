use std::sync::Arc;

use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Runtime, RuntimeConfig},
    Error,
};
use swactor_transport::{
    Codec, CodecRegistry, CodecRemoteSink, InMemoryTransport, NetworkMessage, TransportRouter,
    WireEnvelope,
};

// ---------------------------------------------------------------------------
// Test codec — simple big-endian u32 encoding (no serde dependency)
// ---------------------------------------------------------------------------

/// Minimal hand-rolled codec to prove the framework is serde-agnostic.
struct TestCodec;

impl Codec<Ping> for TestCodec {
    fn encode(&self, msg: &Ping) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(&msg.value.to_be_bytes());
        buf.extend_from_slice(&msg.reply_to.0);
        Ok(buf)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Ping, Error> {
        if bytes.len() < 36 {
            return Err(Error::from("Ping decode: not enough bytes"));
        }
        let value = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let mut addr_bytes = [0u8; 32];
        addr_bytes.copy_from_slice(&bytes[4..36]);
        Ok(Ping {
            value,
            reply_to: ActorAddress(addr_bytes),
        })
    }
}

impl Codec<Pong> for TestCodec {
    fn encode(&self, msg: &Pong) -> Result<Vec<u8>, Error> {
        Ok(msg.value.to_be_bytes().to_vec())
    }
    fn decode(&self, bytes: &[u8]) -> Result<Pong, Error> {
        if bytes.len() < 4 {
            return Err(Error::from("Pong decode: not enough bytes"));
        }
        Ok(Pong {
            value: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        })
    }
}

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Ping {
    value: u32,
    reply_to: ActorAddress,
}

impl NetworkMessage for Ping {
    fn type_tag() -> &'static str {
        "test::Ping"
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Pong {
    value: u32,
}

impl NetworkMessage for Pong {
    fn type_tag() -> &'static str {
        "test::Pong"
    }
}

// ---------------------------------------------------------------------------
// Actor fixtures
// ---------------------------------------------------------------------------

/// Replies with Pong { value: ping.value + 1 }
struct PongActor;

impl ActorInterface for PongActor {
    type Incoming = Ping;
    type Response = Pong;

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(
            msg.reply_to,
            Pong {
                value: msg.value + 1,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<Ping, _>(TestCodec);
    cr.register::<Pong, _>(TestCodec);
    cr
}

fn tick_n(rt: &Runtime, n: usize) {
    for _ in 0..n {
        rt.tick();
    }
}

/// Drain a transport receiver and deliver all envelopes into a runtime.
fn drain_transport(
    rx: &std::sync::mpsc::Receiver<WireEnvelope>,
    codecs: &CodecRegistry,
    rt: &Runtime,
) {
    for envelope in rx.try_iter() {
        let (addr, msg) = codecs.receive(envelope).unwrap();
        rt.deliver_raw(addr, msg).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Given two runtimes connected via InMemoryTransport,
/// when runtime A sends a Ping to an actor on runtime B,
/// then the actor on B receives the deserialized message and processes it.
#[test]
fn two_runtimes_communicate_via_in_memory_transport() {
    let codecs = Arc::new(build_codec_registry());

    // Runtime A — the sender
    let mut rt_a = Runtime::new(RuntimeConfig::default());
    let (transport_a_to_b, rx_b) = InMemoryTransport::pair();
    let router_a = TransportRouter::new();

    // Runtime B — has the PongActor
    let mut rt_b = Runtime::new(RuntimeConfig::default());
    let (transport_b_to_a, rx_a) = InMemoryTransport::pair();
    let router_b = TransportRouter::new();

    // Spawn PongActor on B, drain spawn queue
    let pong_addr = rt_b.spawn(PongActor).unwrap();
    tick_n(&rt_b, 1);

    // Create inbox on A to receive the reply
    let inbox_a = rt_a.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox_a.addr();

    // Register routes: A knows pong_addr is remote (via transport to B)
    router_a.add_route(pong_addr, transport_a_to_b);
    // B knows inbox_addr is remote (via transport to A)
    router_b.add_route(inbox_addr, transport_b_to_a);

    rt_a.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        Arc::new(router_a),
    )));
    rt_b.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        Arc::new(router_b),
    )));

    // A sends Ping to pong_addr — this goes via transport
    rt_a.send_to(
        pong_addr,
        Ping {
            value: 42,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // Deliver from A→B transport, tick B to process
    drain_transport(&rx_b, &codecs, &rt_b);
    tick_n(&rt_b, 1);

    // Deliver reply from B→A transport
    drain_transport(&rx_a, &codecs, &rt_a);

    // A's inbox should have the Pong reply
    let pong = inbox_a.try_recv().expect("should have received Pong");
    assert_eq!(pong, Pong { value: 43 });
}

/// Given a transport route exists but the message type is not registered,
/// when sending to that address,
/// then the error mentions "not registered".
#[test]
fn unregistered_type_produces_clear_error() {
    // Registry with NO types registered
    let codecs = Arc::new(CodecRegistry::new());
    let (transport, _rx) = InMemoryTransport::pair();
    let router = TransportRouter::new();

    let fake_addr = ActorAddress::new_random();
    router.add_route(fake_addr, transport);

    let mut rt = Runtime::new(RuntimeConfig::default());
    rt.set_remote_sink(Arc::new(CodecRemoteSink::new(codecs, Arc::new(router))));

    let result = rt.send_to(
        fake_addr,
        Ping {
            value: 1,
            reply_to: ActorAddress::default(),
        },
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("not registered"),
        "Expected 'not registered' in error, got: {err_msg}"
    );
}

/// Given a WireEnvelope arrives with a type_tag not in the codec registry,
/// when CodecRegistry receives it,
/// then the error mentions "unknown type_tag".
#[test]
fn unknown_type_tag_on_receive_produces_clear_error() {
    let codecs = CodecRegistry::new(); // empty registry

    let envelope = WireEnvelope {
        dest: ActorAddress::default(),
        type_tag: "nonexistent::Type".to_string(),
        payload: vec![1, 2, 3],
    };

    let result = codecs.receive(envelope);
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("unknown type_tag"),
        "Expected 'unknown type_tag' in error, got: {err_msg}"
    );
}

/// Given both local actors and transport routes exist,
/// when an actor sends to another local actor,
/// then the message is delivered locally (no serialization, transport never called).
#[test]
fn local_send_still_bypasses_transport() {
    let codecs = Arc::new(build_codec_registry());
    let (transport, rx) = InMemoryTransport::pair();
    let router = TransportRouter::new();

    // Register a bogus remote route for a random address
    let remote_addr = ActorAddress::new_random();
    router.add_route(remote_addr, transport);

    let mut rt = Runtime::new(RuntimeConfig::default());
    rt.set_remote_sink(Arc::new(CodecRemoteSink::new(codecs, Arc::new(router))));

    // Spawn a local PongActor + inbox
    let pong_addr = rt.spawn(PongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Send locally — should NOT go through transport
    rt.send_to(
        pong_addr,
        Ping {
            value: 10,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    tick_n(&rt, 2);

    // Verify local delivery worked
    let pong = inbox.try_recv().expect("should receive Pong locally");
    assert_eq!(pong, Pong { value: 11 });

    // Verify transport was never used
    assert!(
        rx.try_recv().is_err(),
        "Transport should not have received any envelope"
    );
}

/// Full round-trip: actor on A sends to B, actor on B replies back to A.
/// Both directions go through transports.
#[test]
fn round_trip_across_two_runtimes() {
    let codecs = Arc::new(build_codec_registry());

    // Set up two runtimes with bidirectional transports
    let mut rt_a = Runtime::new(RuntimeConfig::default());
    let mut rt_b = Runtime::new(RuntimeConfig::default());

    let (transport_a2b, rx_b) = InMemoryTransport::pair();
    let (transport_b2a, rx_a) = InMemoryTransport::pair();

    let router_a = TransportRouter::new();
    let router_b = TransportRouter::new();

    // Spawn actors
    let pong_addr = rt_b.spawn(PongActor).unwrap();
    tick_n(&rt_b, 1);

    let inbox_a = rt_a.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox_a.addr();

    // Wire routes
    router_a.add_route(pong_addr, transport_a2b);
    router_b.add_route(inbox_addr, transport_b2a);

    rt_a.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        Arc::new(router_a),
    )));
    rt_b.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        Arc::new(router_b),
    )));

    // Send 3 pings and verify 3 pongs come back
    for i in 0..3u32 {
        rt_a.send_to(
            pong_addr,
            Ping {
                value: i * 10,
                reply_to: inbox_addr,
            },
        )
        .unwrap();
    }

    // Flush A→B
    drain_transport(&rx_b, &codecs, &rt_b);
    tick_n(&rt_b, 1);

    // Flush B→A
    drain_transport(&rx_a, &codecs, &rt_a);

    // Verify all 3 replies
    for i in 0..3u32 {
        let pong = inbox_a.try_recv().expect(&format!("missing pong #{i}"));
        assert_eq!(pong, Pong { value: i * 10 + 1 });
    }
    assert!(inbox_a.try_recv().is_none(), "no extra messages");
}
