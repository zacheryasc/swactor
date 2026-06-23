//! Protocol messages for SWIM membership and the standalone gossip frames
//! (registry, node metadata, directory), plus their JSON codec.

use serde::{Deserialize, Serialize};
use swactor::Error;
use swactor_transport::{Codec, CodecRegistry, NetworkMessage};

use crate::node_metadata::NodeMetadataEntry;
use crate::registry::RegistryEntry;
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

// ─── Standalone gossip (decoupled from the SWIM piggyback) ───────────────────

/// Cluster-registry gossip — a batch of name→actor CRDT entries disseminated
/// independently of SWIM membership.
///
/// Replaces the old combined SWIM piggyback (registry entries packed onto
/// Ping/Ack): once SWIM is membership-only, the registry owns its own wire
/// frame and its own dissemination cadence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryGossip {
    pub entries: Vec<RegistryEntry>,
}

impl NetworkMessage for RegistryGossip {
    fn type_tag() -> &'static str {
        "swactor_dist::RegistryGossip"
    }
}

/// Node-metadata gossip — a batch of per-node metadata entries (relay URL,
/// node name) disseminated independently of SWIM membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataGossip {
    pub entries: Vec<NodeMetadataEntry>,
}

impl NetworkMessage for MetadataGossip {
    fn type_tag() -> &'static str {
        "swactor_dist::MetadataGossip"
    }
}

/// Directory gossip — a batch of signed actor→host claims (`DIRECTORY.md` §3),
/// disseminated independently of SWIM membership exactly like `RegistryGossip` /
/// `MetadataGossip`. Each `DirectoryEntry` is the spec's `Claim`: it carries its
/// own `generation` and signature, so merge is self-describing and a forged claim
/// is rejected on receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryGossip {
    pub claims: Vec<DirectoryEntry>,
}

impl NetworkMessage for DirectoryGossip {
    fn type_tag() -> &'static str {
        "swactor_dist::DirectoryGossip"
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
impl_json_codec!(IndirectAck);
impl_json_codec!(JoinRequest);
impl_json_codec!(JoinResponse);
impl_json_codec!(RegistryGossip);
impl_json_codec!(MetadataGossip);
impl_json_codec!(DirectoryGossip);

/// Build a `CodecRegistry` with all distribution protocol messages registered.
pub fn distribution_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<Ping, _>(JsonCodec);
    cr.register::<Ack, _>(JsonCodec);
    cr.register::<PingReq, _>(JsonCodec);
    // §6.1 / §14.5: `IndirectAck` is folded into the shared registry so all six
    // SWIM message types decode through one uniform path (the iroh driver no
    // longer needs to hand-dispatch it by tag).
    cr.register::<IndirectAck, _>(JsonCodec);
    cr.register::<JoinRequest, _>(JsonCodec);
    cr.register::<JoinResponse, _>(JsonCodec);
    cr.register::<RegistryGossip, _>(JsonCodec);
    cr.register::<MetadataGossip, _>(JsonCodec);
    cr.register::<DirectoryGossip, _>(JsonCodec);
    cr
}

/// Build the `CodecRegistry` for the **actor transport**.
///
/// Distinct from [`distribution_codec_registry`] in one key way: here every wire
/// `type_tag` decodes directly into the matching actor `Incoming` *enum variant*
/// (e.g. `"swactor_dist::Ping"` → [`SwimIn::Ping`]), so a decoded frame can be
/// handed straight to the target actor's mailbox via `Runtime::deliver_raw`.
/// Symmetrically, each actor `Incoming` enum registers a variant-multiplexing
/// encoder (one Rust type → several wire tags). Local-only control variants
/// (`Tick`, `Subscribe`, …) deliberately fail to encode — they never cross the
/// wire.
///
/// Mixing these decoders into [`distribution_codec_registry`] would clobber its
/// `type_tag → concrete-type` decoders, so the actor transport keeps its own.
///
/// Grows one actor at a time as the migration lands; today it covers SWIM.
pub fn actor_codec_registry() -> CodecRegistry {
    use crate::swim::actor::SwimIn;

    let mut cr = CodecRegistry::new();

    // ── SWIM: SwimIn ⇄ the six network type_tags ──
    cr.register_encoder::<SwimIn>(|msg| {
        let json = |r: Result<Vec<u8>, serde_json::Error>| {
            r.map_err(|e| Error::from(format!("encode: {e}")))
        };
        let (tag, bytes) = match msg {
            SwimIn::Ping(p) => ("swactor_dist::Ping", json(serde_json::to_vec(p))?),
            SwimIn::Ack(a) => ("swactor_dist::Ack", json(serde_json::to_vec(a))?),
            SwimIn::PingReq(pr) => ("swactor_dist::PingReq", json(serde_json::to_vec(pr))?),
            SwimIn::IndirectAck(ia) => ("swactor_dist::IndirectAck", json(serde_json::to_vec(ia))?),
            SwimIn::JoinRequest(jr) => ("swactor_dist::JoinRequest", json(serde_json::to_vec(jr))?),
            SwimIn::JoinResponse(jr) => {
                ("swactor_dist::JoinResponse", json(serde_json::to_vec(jr))?)
            }
            // Local-control variants (§6.2) never leave the node.
            SwimIn::Tick { .. }
            | SwimIn::SendFailed { .. }
            | SwimIn::Join { .. }
            | SwimIn::Leave
            | SwimIn::Subscribe { .. } => {
                return Err(Error::from(
                    "SwimIn: local-only control variant is not network-encodable",
                ));
            }
        };
        Ok((tag.to_string(), bytes))
    });

    // A generic free fn (not a closure — closures are monomorphic and we decode
    // into six different inner types).
    fn d<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Error> {
        serde_json::from_slice(bytes).map_err(|e| Error::from(format!("decode: {e}")))
    }
    cr.register_decoder::<SwimIn>("swactor_dist::Ping", |b| Ok(SwimIn::Ping(d(b)?)));
    cr.register_decoder::<SwimIn>("swactor_dist::Ack", |b| Ok(SwimIn::Ack(d(b)?)));
    cr.register_decoder::<SwimIn>("swactor_dist::PingReq", |b| Ok(SwimIn::PingReq(d(b)?)));
    cr.register_decoder::<SwimIn>("swactor_dist::IndirectAck", |b| {
        Ok(SwimIn::IndirectAck(d(b)?))
    });
    cr.register_decoder::<SwimIn>("swactor_dist::JoinRequest", |b| {
        Ok(SwimIn::JoinRequest(d(b)?))
    });
    cr.register_decoder::<SwimIn>("swactor_dist::JoinResponse", |b| {
        Ok(SwimIn::JoinResponse(d(b)?))
    });

    // ── Registry: RegistryIn::Gossip ⇄ RegistryGossip frame ──
    use crate::registry_actor::RegistryIn;
    cr.register_encoder::<RegistryIn>(|msg| match msg {
        RegistryIn::Gossip(g) => Ok((
            RegistryGossip::type_tag().to_string(),
            serde_json::to_vec(g).map_err(|e| Error::from(format!("encode: {e}")))?,
        )),
        _ => Err(Error::from("RegistryIn: only Gossip is network-encodable")),
    });
    cr.register_decoder::<RegistryIn>(RegistryGossip::type_tag(), |b| {
        Ok(RegistryIn::Gossip(d(b)?))
    });

    // ── Metadata: MetadataIn::Gossip ⇄ MetadataGossip frame ──
    use crate::node_metadata_actor::MetadataIn;
    cr.register_encoder::<MetadataIn>(|msg| match msg {
        MetadataIn::Gossip(g) => Ok((
            MetadataGossip::type_tag().to_string(),
            serde_json::to_vec(g).map_err(|e| Error::from(format!("encode: {e}")))?,
        )),
        _ => Err(Error::from("MetadataIn: only Gossip is network-encodable")),
    });
    cr.register_decoder::<MetadataIn>(MetadataGossip::type_tag(), |b| {
        Ok(MetadataIn::Gossip(d(b)?))
    });

    // ── Directory: DirectoryIn::Gossip ⇄ DirectoryGossip frame ──
    use crate::directory_actor::DirectoryIn;
    cr.register_encoder::<DirectoryIn>(|msg| match msg {
        DirectoryIn::Gossip(g) => Ok((
            DirectoryGossip::type_tag().to_string(),
            serde_json::to_vec(g).map_err(|e| Error::from(format!("encode: {e}")))?,
        )),
        _ => Err(Error::from("DirectoryIn: only Gossip is network-encodable")),
    });
    cr.register_decoder::<DirectoryIn>(DirectoryGossip::type_tag(), |b| {
        Ok(DirectoryIn::Gossip(d(b)?))
    });

    cr
}
