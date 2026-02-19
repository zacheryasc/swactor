//! Protocol messages for SWIM membership and Kademlia directory.

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::transport::NetworkMessage;

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
