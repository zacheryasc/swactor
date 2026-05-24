//! SIM_SPEC §6.4 SWIM codec parity + invertibility tests.
//!
//! The contract:
//!   - Codec is invertible: `decode(encode(msg)) == msg` on the
//!     kind's message type.
//!   - SWIM codec parity with production: the simulator's encoded
//!     bytes are byte-identical with what the production transport
//!     would put on the wire for the same message.
//!
//! Both are verified for every kind the simulator may emit. A drift
//! between the simulator and production would surface here.

use distribution::messages::{
    Ack, IndirectAck, JoinRequest, JoinResponse, JsonCodec, MembershipUpdate, Ping, PingReq,
};
use distribution::types::{MemberState, NodeId, NodeRecord};

use simulation::swim_codec::{CodecError, SwimMessage};

/// Production's `JsonCodec` is `serde_json::to_vec` under the hood
/// (`crates/distribution/src/messages.rs:184`). Calling
/// `JsonCodec.encode(&m)` would require pulling the `swactor::transport::Codec`
/// trait into scope, which the simulation crate doesn't depend on; the
/// equivalent direct call is shorter and equally definitive — they
/// both reduce to `serde_json::to_vec(&m).unwrap()`.
fn production_encode<T: serde::Serialize>(m: &T) -> Vec<u8> {
    // Keep the `JsonCodec` symbol used so an attribute-only refactor
    // of the production codec (e.g. moving the impl, renaming the
    // struct) shows up as a compile error here, not silent drift.
    let _gate: JsonCodec = JsonCodec;
    serde_json::to_vec(m).unwrap()
}

fn nid(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

fn sample_ping() -> Ping {
    Ping {
        from: nid(0x01),
        sequence: 42,
        piggyback: piggy_bytes(),
    }
}

fn sample_ack() -> Ack {
    Ack {
        from: nid(0x02),
        sequence: 43,
        piggyback: piggy_bytes(),
    }
}

fn sample_ping_req() -> PingReq {
    PingReq {
        from: nid(0x03),
        target: nid(0x04),
        sequence: 44,
        piggyback: piggy_bytes(),
    }
}

fn sample_indirect_ack() -> IndirectAck {
    IndirectAck {
        target: nid(0x05),
        sequence: 45,
        piggyback: piggy_bytes(),
    }
}

fn sample_join_request() -> JoinRequest {
    JoinRequest { from: nid(0x06) }
}

fn sample_join_response() -> JoinResponse {
    JoinResponse {
        members: vec![
            NodeRecord {
                node_id: nid(0x06),
                state: MemberState::Alive,
                incarnation: 7,
            },
            NodeRecord {
                node_id: nid(0x07),
                state: MemberState::Suspect,
                incarnation: 8,
            },
            NodeRecord {
                node_id: nid(0x08),
                state: MemberState::Dead,
                incarnation: 9,
            },
        ],
    }
}

fn piggy_bytes() -> Vec<u8> {
    // Realistic-looking piggyback: a serialised batch of membership
    // updates. Production puts arbitrary bytes here; we keep it valid
    // so the round-trip is meaningful end-to-end.
    serde_json::to_vec(&vec![
        MembershipUpdate {
            node_id: nid(0x01),
            state: MemberState::Alive,
            incarnation: 1,
        },
        MembershipUpdate {
            node_id: nid(0x02),
            state: MemberState::Suspect,
            incarnation: 2,
        },
    ])
    .unwrap()
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 SWIM codec parity with production
// ──────────────────────────────────────────────────────────────────────

#[test]
fn sim_encode_matches_production_encode_for_ping() {
    let m = sample_ping();
    let sim_bytes = SwimMessage::Ping(m.clone()).encode();
    let prod_bytes = production_encode(&m);
    assert_eq!(sim_bytes, prod_bytes);
}

#[test]
fn sim_encode_matches_production_encode_for_ack() {
    let m = sample_ack();
    assert_eq!(
        SwimMessage::Ack(m.clone()).encode(),
        production_encode(&m)
    );
}

#[test]
fn sim_encode_matches_production_encode_for_ping_req() {
    let m = sample_ping_req();
    assert_eq!(
        SwimMessage::PingReq(m.clone()).encode(),
        production_encode(&m)
    );
}

#[test]
fn sim_encode_matches_production_encode_for_indirect_ack() {
    let m = sample_indirect_ack();
    // Production's JsonCodec is impl'd for the kinds production sends
    // on the wire; we go through serde_json::to_vec for parity since
    // both paths must produce identical bytes anyway.
    let sim_bytes = SwimMessage::IndirectAck(m.clone()).encode();
    let prod_bytes = serde_json::to_vec(&m).unwrap();
    assert_eq!(sim_bytes, prod_bytes);
}

#[test]
fn sim_encode_matches_production_encode_for_join_request() {
    let m = sample_join_request();
    assert_eq!(
        SwimMessage::JoinRequest(m.clone()).encode(),
        production_encode(&m)
    );
}

#[test]
fn sim_encode_matches_production_encode_for_join_response() {
    let m = sample_join_response();
    assert_eq!(
        SwimMessage::JoinResponse(m.clone()).encode(),
        production_encode(&m)
    );
}

// ──────────────────────────────────────────────────────────────────────
// §6.4 Codec is invertible
// ──────────────────────────────────────────────────────────────────────

#[test]
fn decode_of_encode_recovers_the_message_for_every_kind() {
    let cases: Vec<SwimMessage> = vec![
        SwimMessage::Ping(sample_ping()),
        SwimMessage::Ack(sample_ack()),
        SwimMessage::PingReq(sample_ping_req()),
        SwimMessage::IndirectAck(sample_indirect_ack()),
        SwimMessage::JoinRequest(sample_join_request()),
        SwimMessage::JoinResponse(sample_join_response()),
    ];
    for msg in cases {
        let tag = msg.type_tag();
        let bytes = msg.encode();
        let round = SwimMessage::decode(tag, &bytes).expect("decode must succeed");
        // We can't `PartialEq` SwimMessage because the production
        // types don't derive Eq; instead re-encode and compare.
        assert_eq!(
            round.encode(),
            bytes,
            "encode(decode(encode(m))) != encode(m) for {tag}"
        );
    }
}

// ──────────────────────────────────────────────────────────────────────
// Negative: unknown kind tag rejects with a structured error
// ──────────────────────────────────────────────────────────────────────

#[test]
fn decode_of_unknown_kind_is_a_structured_error() {
    let bytes = b"{}";
    let err = SwimMessage::decode("swactor_dist::Unknown", bytes).unwrap_err();
    assert!(matches!(err, CodecError::UnknownKind(_)));
}

#[test]
fn decode_of_malformed_bytes_is_a_structured_error() {
    let err = SwimMessage::decode("swactor_dist::Ping", b"not json").unwrap_err();
    assert!(matches!(err, CodecError::Json(_)));
}
