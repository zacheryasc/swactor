//! Protocol messages for SWIM membership and Kademlia directory, plus JSON codec.

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::transport::{Codec, CodecRegistry, NetworkMessage};
use swactor::Error;

use crate::types::{DirectoryEntry, MemberState, NodeId, NodeRecord};

// ─── SWIM Protocol Messages ────────────────────────────────────────────────

/// SWIM ping — "are you alive?"
///
/// Carries piggybacked membership gossip so that SWIM dissemination
/// propagates cluster state changes on every protocol message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ping {
    pub from: NodeId,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,
}

impl NetworkMessage for Ping {
    fn type_tag() -> &'static str {
        "swactor_dist::Ping"
    }
}

/// SWIM ack — "yes, I'm alive"
///
/// Carries piggybacked membership gossip (same as Ping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub from: NodeId,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,
}

impl NetworkMessage for Ack {
    fn type_tag() -> &'static str {
        "swactor_dist::Ack"
    }
}

/// SWIM indirect ping request — "please ping target on my behalf"
///
/// Carries piggybacked membership gossip (same as Ping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingReq {
    pub from: NodeId,
    pub target: NodeId,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,
}

impl NetworkMessage for PingReq {
    fn type_tag() -> &'static str {
        "swactor_dist::PingReq"
    }
}

/// SWIM indirect ack — "the target you asked me to ping is alive"
///
/// Sent by a relay node back to the original prober after the relay
/// receives an ack from the indirect-ping target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndirectAck {
    pub target: NodeId,
    pub sequence: u64,
    #[serde(default)]
    pub piggyback: Vec<u8>,
}

impl NetworkMessage for IndirectAck {
    fn type_tag() -> &'static str {
        "swactor_dist::IndirectAck"
    }
}

/// SWIM join request — "I want to join the cluster"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub from: NodeId,
}

impl NetworkMessage for JoinRequest {
    fn type_tag() -> &'static str {
        "swactor_dist::JoinRequest"
    }
}

/// SWIM join response — "here's the current member list"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub members: Vec<NodeRecord>,
}

impl NetworkMessage for JoinResponse {
    fn type_tag() -> &'static str {
        "swactor_dist::JoinResponse"
    }
}

// ─── Membership Dissemination ───────────────────────────────────────────────

/// A single membership update, piggybacked on protocol messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipUpdate {
    pub node_id: NodeId,
    pub state: MemberState,
    pub incarnation: u64,
}

// ─── Kademlia Protocol Messages ─────────────────────────────────────────────

/// Kademlia FIND_NODE request — "who are the k closest nodes to this target?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodeRequest {
    pub from: NodeId,
    pub target: NodeId,
}

impl NetworkMessage for FindNodeRequest {
    fn type_tag() -> &'static str {
        "swactor_dist::FindNodeRequest"
    }
}

/// Kademlia FIND_NODE response — closest known nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodeResponse {
    pub closest: Vec<NodeId>,
}

impl NetworkMessage for FindNodeResponse {
    fn type_tag() -> &'static str {
        "swactor_dist::FindNodeResponse"
    }
}

/// Kademlia STORE — "store this directory entry"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRequest {
    pub entry: DirectoryEntry,
}

impl NetworkMessage for StoreRequest {
    fn type_tag() -> &'static str {
        "swactor_dist::StoreRequest"
    }
}

/// Kademlia FIND_VALUE request — "where is this actor?"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindValueRequest {
    pub from: NodeId,
    pub actor_addr: ActorAddress,
}

impl NetworkMessage for FindValueRequest {
    fn type_tag() -> &'static str {
        "swactor_dist::FindValueRequest"
    }
}

/// Kademlia FIND_VALUE response — either the entry or closer nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindValueResponse {
    /// Found the actor — here's the directory entry.
    Found(DirectoryEntry),
    /// Don't have it — here are closer nodes to ask.
    Closer(Vec<NodeId>),
}

impl NetworkMessage for FindValueResponse {
    fn type_tag() -> &'static str {
        "swactor_dist::FindValueResponse"
    }
}

// ─── JSON Codec ─────────────────────────────────────────────────────────────

/// JSON codec for distribution protocol messages.
///
/// Using JSON for simplicity and debuggability. Can be swapped for
/// bincode/msgpack in production via the Codec trait.
pub struct JsonCodec;

macro_rules! impl_json_codec {
    ($ty:ty) => {
        impl Codec<$ty> for JsonCodec {
            fn encode(&self, msg: &$ty) -> Result<Vec<u8>, Error> {
                serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode: {e}")))
            }
            fn decode(&self, bytes: &[u8]) -> Result<$ty, Error> {
                serde_json::from_slice(bytes).map_err(|e| Error::from(format!("decode: {e}")))
            }
        }
    };
}

impl_json_codec!(Ping);
impl_json_codec!(Ack);
impl_json_codec!(PingReq);
impl_json_codec!(JoinRequest);
impl_json_codec!(JoinResponse);
impl_json_codec!(FindNodeRequest);
impl_json_codec!(FindNodeResponse);
impl_json_codec!(StoreRequest);
impl_json_codec!(FindValueRequest);
impl_json_codec!(FindValueResponse);

/// Build a `CodecRegistry` with all distribution protocol messages registered.
pub fn distribution_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<Ping, _>(JsonCodec);
    cr.register::<Ack, _>(JsonCodec);
    cr.register::<PingReq, _>(JsonCodec);
    cr.register::<JoinRequest, _>(JsonCodec);
    cr.register::<JoinResponse, _>(JsonCodec);
    cr.register::<FindNodeRequest, _>(JsonCodec);
    cr.register::<FindNodeResponse, _>(JsonCodec);
    cr.register::<StoreRequest, _>(JsonCodec);
    cr.register::<FindValueRequest, _>(JsonCodec);
    cr.register::<FindValueResponse, _>(JsonCodec);
    cr
}
