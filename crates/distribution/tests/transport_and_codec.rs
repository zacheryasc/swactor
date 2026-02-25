use distribution::messages::*;
use distribution::types::NodeId;

// ─── Codec registry ─────────────────────────────────────────────────────────

#[test]
fn distribution_codec_encodes_and_decodes_ping() {
    let codecs = distribution_codec_registry();
    let ping = Ping {
        from: NodeId([0xAA; 32]),
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
        NodeId([0x11; 32]),
        NodeId([0x22; 32]),
    ]);

    let type_id = std::any::TypeId::of::<FindValueResponse>();
    let (tag, bytes) = codecs.encode(type_id, Box::new(resp.clone())).unwrap();

    let decoded_any = codecs.decode(&tag, &bytes).unwrap();
    let decoded: &FindValueResponse = decoded_any.downcast_ref().unwrap();
    match decoded {
        FindValueResponse::Closer(nodes) => {
            assert_eq!(nodes.len(), 2);
            assert_eq!(nodes[0], NodeId([0x11; 32]));
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

