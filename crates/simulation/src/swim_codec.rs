//! Simulator-side SWIM message codec (SIM_SPEC §3.3, §6.4).
//!
//! The bandwidth model in §5 needs the byte length of every outgoing
//! SWIM message, and §6.4 demands that the bytes the simulator gives
//! the network equal the bytes the production transport would put on
//! the wire for the same message. The cleanest way to enforce that is
//! for the simulator to reuse production's message types and the same
//! serialisation path. This module is the thin wrapper that does so;
//! it imports the production types from `distribution::messages` and
//! exposes `encode_*` / `decode_*` helpers the SWIM host adapter will
//! call.
//!
//! Drift is caught by the codec parity test in
//! `tests/swim_codec_parity.rs`: every kind of SWIM message goes
//! through both `distribution::messages::JsonCodec::encode` and the
//! helper in this module; the bytes must match.

use distribution::messages::{Ack, IndirectAck, JoinRequest, JoinResponse, Ping, PingReq};

/// Enum naming every SWIM message kind the simulator may carry on the
/// wire. The discriminator is the production `NetworkMessage::type_tag`
/// so the simulator and production speak the same vocabulary.
#[derive(Debug, Clone)]
pub enum SwimMessage {
    Ping(Ping),
    Ack(Ack),
    PingReq(PingReq),
    IndirectAck(IndirectAck),
    JoinRequest(JoinRequest),
    JoinResponse(JoinResponse),
}

impl SwimMessage {
    /// Production-matching type tag for the message kind. Used by the
    /// engine's bundle writer to label outgoing-message events with
    /// the same `kind_tag` the production diagnostics emit.
    pub fn type_tag(&self) -> &'static str {
        match self {
            SwimMessage::Ping(_) => "swactor_dist::Ping",
            SwimMessage::Ack(_) => "swactor_dist::Ack",
            SwimMessage::PingReq(_) => "swactor_dist::PingReq",
            SwimMessage::IndirectAck(_) => "swactor_dist::IndirectAck",
            SwimMessage::JoinRequest(_) => "swactor_dist::JoinRequest",
            SwimMessage::JoinResponse(_) => "swactor_dist::JoinResponse",
        }
    }

    /// Encode the message exactly as production's `JsonCodec` would.
    /// The bytes are byte-identical with what a peer would receive on
    /// the wire for the same message; the §6.4 codec parity test
    /// gates this property against drift.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            SwimMessage::Ping(m) => encode_value(m),
            SwimMessage::Ack(m) => encode_value(m),
            SwimMessage::PingReq(m) => encode_value(m),
            SwimMessage::IndirectAck(m) => encode_value(m),
            SwimMessage::JoinRequest(m) => encode_value(m),
            SwimMessage::JoinResponse(m) => encode_value(m),
        }
    }

    /// Decode a payload according to the tagged kind. The tag is the
    /// `type_tag` the production transport multiplexes on; without it
    /// the byte payload alone is ambiguous between a Ping and an Ack
    /// (both have the same JSON shape).
    pub fn decode(tag: &str, bytes: &[u8]) -> Result<SwimMessage, CodecError> {
        match tag {
            "swactor_dist::Ping" => decode_value(bytes).map(SwimMessage::Ping),
            "swactor_dist::Ack" => decode_value(bytes).map(SwimMessage::Ack),
            "swactor_dist::PingReq" => decode_value(bytes).map(SwimMessage::PingReq),
            "swactor_dist::IndirectAck" => decode_value(bytes).map(SwimMessage::IndirectAck),
            "swactor_dist::JoinRequest" => decode_value(bytes).map(SwimMessage::JoinRequest),
            "swactor_dist::JoinResponse" => decode_value(bytes).map(SwimMessage::JoinResponse),
            other => Err(CodecError::UnknownKind(other.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum CodecError {
    UnknownKind(String),
    Json(String),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::UnknownKind(k) => write!(f, "unknown SWIM message kind: {k:?}"),
            CodecError::Json(e) => write!(f, "JSON codec: {e}"),
        }
    }
}

impl std::error::Error for CodecError {}

fn encode_value<T: serde::Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("SWIM messages serialise by construction")
}

fn decode_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(bytes).map_err(|e| CodecError::Json(e.to_string()))
}
