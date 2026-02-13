use swactor::actor::ActorAddress;
use swactor::transport::WireEnvelope;

use distribution::codec::distribution_codec_registry;
use distribution::messages::*;
use distribution::transport::{TcpAcceptor, TcpTransport};
use distribution::types::NodeId;

// ─── Wire format round-trip ─────────────────────────────────────────────────

#[test]
fn wire_envelope_roundtrips_through_tcp() {
    let acceptor = TcpAcceptor::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = acceptor.local_addr();

    let original = WireEnvelope {
        dest: ActorAddress::new_random(),
        type_tag: "test::Msg".to_string(),
        payload: vec![1, 2, 3, 4, 5],
    };

    let original_clone = original.clone();
    let sender = std::thread::spawn(move || {
        let transport = TcpTransport::new(addr);
        transport.send_to(addr, original_clone).unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));
    let mut streams = Vec::new();
    let envelopes = loop {
        let envs = acceptor.try_recv(&mut streams);
        if !envs.is_empty() {
            break envs;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    sender.join().unwrap();

    assert_eq!(envelopes.len(), 1);
    let (received, _peer) = &envelopes[0];
    assert_eq!(received.dest, original.dest);
    assert_eq!(received.type_tag, original.type_tag);
    assert_eq!(received.payload, original.payload);
}

#[test]
fn wire_envelope_minimal_roundtrips() {
    let acceptor = TcpAcceptor::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = acceptor.local_addr();

    let original = WireEnvelope {
        dest: ActorAddress::new_random(),
        type_tag: "test::Minimal".to_string(),
        payload: vec![42],
    };

    let original_clone = original.clone();
    let sender = std::thread::spawn(move || {
        let transport = TcpTransport::new(addr);
        transport.send_to(addr, original_clone).unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));
    let mut streams = Vec::new();
    let envelopes = loop {
        let envs = acceptor.try_recv(&mut streams);
        if !envs.is_empty() {
            break envs;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    sender.join().unwrap();

    let (received, _) = &envelopes[0];
    assert_eq!(received.payload, vec![42]);
}

// ─── Codec registry ─────────────────────────────────────────────────────────

#[test]
fn distribution_codec_encodes_and_decodes_ping() {
    let codecs = distribution_codec_registry();
    let ping = Ping {
        from: NodeId([0xAA; 32]),
        from_addr: "127.0.0.1:7000".parse().unwrap(),
        sequence: 42,
        piggyback: vec![],
    };

    let type_id = std::any::TypeId::of::<Ping>();
    let (tag, bytes) = codecs.encode(type_id, Box::new(ping.clone())).unwrap();
    assert_eq!(&tag, "swactor_dist::Ping");

    let decoded_any = codecs.decode(&tag, &bytes).unwrap();
    let decoded: &Ping = decoded_any.downcast_ref().unwrap();
    assert_eq!(decoded.from, ping.from);
    assert_eq!(decoded.sequence, ping.sequence);
}

#[test]
fn distribution_codec_encodes_and_decodes_find_value_response() {
    let codecs = distribution_codec_registry();

    let resp = FindValueResponse::Closer(vec![
        (NodeId([0x11; 32]), "127.0.0.1:8080".parse().unwrap()),
        (NodeId([0x22; 32]), "127.0.0.1:8081".parse().unwrap()),
    ]);

    let type_id = std::any::TypeId::of::<FindValueResponse>();
    let (tag, bytes) = codecs.encode(type_id, Box::new(resp.clone())).unwrap();

    let decoded_any = codecs.decode(&tag, &bytes).unwrap();
    let decoded: &FindValueResponse = decoded_any.downcast_ref().unwrap();
    match decoded {
        FindValueResponse::Closer(nodes) => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0].0, NodeId([0x11; 32]));
        }
        _ => panic!("expected Closer variant"),
    }
}

#[test]
fn all_message_types_registered_in_codec_registry() {
    let codecs = distribution_codec_registry();

    let tags = [
        "swactor_dist::Ping",
        "swactor_dist::Ack",
        "swactor_dist::PingReq",
        "swactor_dist::JoinRequest",
        "swactor_dist::JoinResponse",
        "swactor_dist::FindNodeRequest",
        "swactor_dist::FindNodeResponse",
        "swactor_dist::StoreRequest",
        "swactor_dist::FindValueRequest",
        "swactor_dist::FindValueResponse",
    ];

    for tag in tags {
        let result = codecs.decode(tag, &[]);
        let err = result.unwrap_err();
        let err_str = format!("{}", err);
        assert!(
            !err_str.contains("unknown type_tag"),
            "Decoder not registered for tag '{tag}': {err_str}"
        );
    }
}

// ─── End-to-end: codec + TCP transport ──────────────────────────────────────

#[test]
fn ping_message_survives_codec_and_tcp_roundtrip() {
    let codecs = distribution_codec_registry();

    let acceptor = TcpAcceptor::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let server_addr = acceptor.local_addr();

    let dest = ActorAddress::new_random();
    let ping = Ping {
        from: NodeId([0xBB; 32]),
        from_addr: "127.0.0.1:7001".parse().unwrap(),
        sequence: 99,
        piggyback: vec![],
    };

    let type_id = std::any::TypeId::of::<Ping>();
    let (tag, payload) = codecs.encode(type_id, Box::new(ping.clone())).unwrap();

    let envelope = WireEnvelope {
        dest,
        type_tag: tag,
        payload,
    };

    let envelope_clone = envelope.clone();
    let sender = std::thread::spawn(move || {
        let transport = TcpTransport::new(server_addr);
        transport.send_to(server_addr, envelope_clone).unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(50));
    let mut streams = Vec::new();
    let envelopes = loop {
        let envs = acceptor.try_recv(&mut streams);
        if !envs.is_empty() {
            break envs;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    sender.join().unwrap();

    let (received, _) = &envelopes[0];

    let (addr, msg_any) = codecs.receive(received.clone()).unwrap();
    assert_eq!(addr, dest);
    let decoded: &Ping = msg_any.downcast_ref().unwrap();
    assert_eq!(decoded.from, NodeId([0xBB; 32]));
    assert_eq!(decoded.sequence, 99);
}
