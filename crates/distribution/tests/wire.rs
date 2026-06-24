//! Wire codec and transport-bridge contracts.
//!
//! These tests pin the mechanical seams that move distribution messages between actors and
//! runtimes: message registration, actor codec behavior, and route-view egress.
//!
//! Behavioral/correctness guarantees:
//! - Every distribution wire message has an explicit registered codec contract.
//! - Actor transport decodes network frames into the correct actor input and refuses local-only
//!   control messages.
//! - Peer and actor egress are routed through deterministic, lazy, non-blocking transport
//!   bindings.
//! - Route-view based egress resolves host ownership at send time, so routing changes do not
//!   require rebinding.

mod codec_contract {
    //! Codec correctness: distribution frames round-trip, actor ingress tags decode to Incoming
    //! variants, and local-only controls cannot encode.

    use distribution::messages::*;
    use distribution::swim::actor::SwimIn;
    use distribution::types::NodeId;
    use std::any::TypeId;

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
    fn all_message_types_registered_in_codec_registry() {
        let codecs = distribution_codec_registry();

        let tags = [
            "swactor_dist::Ping",
            "swactor_dist::Ack",
            "swactor_dist::PingReq",
            "swactor_dist::JoinRequest",
            "swactor_dist::JoinResponse",
            "swactor_dist::RegistryGossip",
            "swactor_dist::MetadataGossip",
            "swactor_dist::DirectoryGossip",
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

    // ─── Actor transport registry: wire tag ⇄ SwimIn variant ─────────────────────

    /// A SWIM message sent as a `SwimIn` variant encodes to its concrete wire tag,
    /// and a frame on that tag decodes back into the *same* `SwimIn` variant — so
    /// the egress (`ctx.send(SwimIn::Ping)`) and ingress (`deliver_raw(SwimIn::Ping)`)
    /// halves agree on the wire format without anyone hand-dispatching by tag.
    #[test]
    fn actor_registry_round_trips_swim_in_through_its_wire_tag() {
        let codecs = actor_codec_registry();
        let ping = Ping {
            from: NodeId([0xCD; 32]),
            sequence: 7,
            piggyback: vec![1, 2, 3],
        };

        // Egress: a SwimIn variant encodes by its TypeId to the concrete wire tag.
        let (tag, bytes) = codecs
            .encode(TypeId::of::<SwimIn>(), Box::new(SwimIn::Ping(ping.clone())))
            .expect("SwimIn::Ping encodes");
        assert_eq!(&tag, "swactor_dist::Ping");

        // Ingress: that frame decodes back into a SwimIn (not the bare Ping), ready
        // to deliver_raw straight into the SwimActor's mailbox.
        let decoded = codecs.decode(&tag, &bytes).expect("frame decodes");
        match decoded.downcast_ref::<SwimIn>() {
            Some(SwimIn::Ping(p)) => {
                assert_eq!(p.from, ping.from);
                assert_eq!(p.sequence, ping.sequence);
                assert_eq!(p.piggyback, ping.piggyback);
            }
            _ => panic!("expected the frame to decode into SwimIn::Ping"),
        }
    }

    /// Local-control variants (`Tick`, `Subscribe`, …) are not part of the wire
    /// protocol; they must never serialize, so an accidental `ctx.send` of one to a
    /// remote peer fails loudly at the encode boundary rather than shipping garbage.
    #[test]
    fn actor_registry_refuses_to_encode_local_only_variants() {
        let codecs = actor_codec_registry();
        for local in [SwimIn::Leave, SwimIn::Join { seeds: vec![] }] {
            let err = codecs
                .encode(TypeId::of::<SwimIn>(), Box::new(local))
                .expect_err("local-only SwimIn variant must not encode");
            assert!(
                format!("{err}").contains("local-only"),
                "unexpected error: {err}"
            );
        }
    }

    /// Registry/metadata gossip are standalone wire frames (no longer piggybacked on
    /// SWIM); confirm they survive a JSON round-trip through the shared registry.
    #[test]
    fn registry_and_metadata_gossip_round_trip() {
        let codecs = distribution_codec_registry();

        let rg = RegistryGossip { entries: vec![] };
        let (tag, bytes) = codecs
            .encode(TypeId::of::<RegistryGossip>(), Box::new(rg))
            .unwrap();
        assert_eq!(&tag, "swactor_dist::RegistryGossip");
        assert!(codecs.decode(&tag, &bytes).unwrap().is::<RegistryGossip>());

        let mg = MetadataGossip { entries: vec![] };
        let (tag, bytes) = codecs
            .encode(TypeId::of::<MetadataGossip>(), Box::new(mg))
            .unwrap();
        assert_eq!(&tag, "swactor_dist::MetadataGossip");
        assert!(codecs.decode(&tag, &bytes).unwrap().is::<MetadataGossip>());
    }
}

mod route_view_egress {
    //! Transport bridge correctness: peer mailbox mapping and app-message egress through the latest
    //! RouteView.

    use distribution::transport_bridge::{Outbox, RouteView, RouteViewTransport, peer_addr};
    use distribution::types::NodeId;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, RwLock};
    use swactor::actor::ActorAddress;
    use swactor_transport::{Transport, WireEnvelope};

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    #[test]
    fn peer_addr_is_a_reversible_peer_mailbox_mapping() {
        // Correctness: peer egress and send-failure handling both rely on recovering the
        // NodeId from the synthetic ActorAddress without a side table.
        let peer = id(0xA5);
        assert_eq!(NodeId(peer_addr(peer).0), peer);
    }

    #[test]
    fn route_view_transport_resolves_at_send_time_and_drops_missing_routes() {
        // Correctness: app-message egress consults the latest directory RouteView for
        // every send. Missing routes are best-effort drops; moved actors follow the new
        // host without rebinding transport routes.
        let actor = ActorAddress::new_random();
        let first_host = id(1);
        let second_host = id(2);
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
        let transport = RouteViewTransport::new(route_view.clone(), outbox.clone());

        transport
            .send(WireEnvelope {
                dest: actor,
                type_tag: "app::Message".into(),
                payload: vec![1],
            })
            .unwrap();
        assert!(outbox.lock().unwrap().is_empty());

        route_view.write().unwrap().insert(actor, first_host);
        transport
            .send(WireEnvelope {
                dest: actor,
                type_tag: "app::Message".into(),
                payload: vec![2],
            })
            .unwrap();

        route_view.write().unwrap().insert(actor, second_host);
        transport
            .send(WireEnvelope {
                dest: actor,
                type_tag: "app::Message".into(),
                payload: vec![3],
            })
            .unwrap();

        let frames = outbox.lock().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].to, first_host);
        assert_eq!(frames[0].payload, vec![2]);
        assert_eq!(frames[1].to, second_host);
        assert_eq!(frames[1].payload, vec![3]);
    }
}
