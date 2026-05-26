//! Transport-agnostic messaging framework.
//!
//! Enables actors on different runtimes to communicate transparently via
//! pluggable codecs (serialization) and transports (delivery protocol).
//!
//! # Two layers of pluggability
//!
//! - **[`Codec<M>`]**: HOW bytes are encoded — gRPC/protobuf, bincode, custom, etc.
//! - **[`Transport`]**: WHERE bytes are sent — in-memory, gRPC channel, etc.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::actor::{ActorAddress, Message};
use crate::delivery::{AddrBuildHasher, AddrMap};
use crate::Error;

// ─── NodeId ─────────────────────────────────────────────────────────────────

/// A node's identity — 32 opaque bytes.
///
/// Typically the raw bytes of an ed25519 public key, but this type
/// carries no cryptographic semantics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeId(pub [u8; 32]);

impl core::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "NodeId(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

impl core::fmt::Display for NodeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

// ─── Hex encoding ───────────────────────────────────────────────────────────

/// Hex-encode a byte slice.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Hex-decode a string into bytes. Returns `None` on invalid input.
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Some(bytes)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ─── Codec ──────────────────────────────────────────────────────────────────

/// User-implemented codec for a specific message type.
///
/// This is where serialization logic lives — gRPC/protobuf, bincode,
/// msgpack, or any custom format. The framework imposes no serialization
/// constraints on message types; the codec defines what it needs from `M`.
pub trait Codec<M>: Send + Sync + 'static {
    fn encode(&self, msg: &M) -> Result<Vec<u8>, Error>;
    fn decode(&self, bytes: &[u8]) -> Result<M, Error>;
}

// ─── NetworkMessage ─────────────────────────────────────────────────────────

/// Marker for messages that can cross runtime boundaries.
///
/// The only requirement is a stable `type_tag` string used for deserialization
/// routing on the receiving side. No serialization bounds — the [`Codec`]
/// handles that separately.
pub trait NetworkMessage: Message {
    /// Stable identifier for this message type, used for deserialization routing.
    /// Must be unique per type and stable across compilations.
    /// Convention: `"crate_name::TypeName"`.
    fn type_tag() -> &'static str;
}

// ─── WireEnvelope ───────────────────────────────────────────────────────────

/// Serialized message ready for transport across runtime boundaries.
///
/// A plain struct — the [`Transport`] implementation decides how to put it
/// on the wire (protobuf, raw bytes, etc.).
#[derive(Debug, Clone)]
pub struct WireEnvelope {
    pub dest: ActorAddress,
    pub type_tag: String,
    pub payload: Vec<u8>,
}

// ─── Transport ──────────────────────────────────────────────────────────────

/// Pluggable transport protocol.
///
/// Implementations queue or send the envelope to a remote runtime.
/// `send` should not block the calling thread.
pub trait Transport: Send + Sync {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error>;
}

// ─── CodecRegistry ──────────────────────────────────────────────────────────

type EncodeFn = Box<dyn Fn(Box<dyn Any + Send>) -> Result<(String, Vec<u8>), Error> + Send + Sync>;
type DecodeFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, Error> + Send + Sync>;

/// Unified registry for encoding (`TypeId` → encoder) and decoding
/// (`type_tag` → decoder).
///
/// Built at setup time via [`register`](Self::register), then shared
/// read-only via `Arc`.
pub struct CodecRegistry {
    encoders: HashMap<TypeId, EncodeFn>,
    decoders: HashMap<String, DecodeFn>,
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecRegistry {
    pub fn new() -> Self {
        Self {
            encoders: HashMap::new(),
            decoders: HashMap::new(),
        }
    }

    /// Register a message type with its codec.
    ///
    /// Both encoding and decoding are handled by the same codec instance.
    pub fn register<M: NetworkMessage, C: Codec<M>>(&mut self, codec: C) {
        let codec = Arc::new(codec);

        // Encoder side — closure downcasts Any → M, encodes, returns (tag, bytes)
        let encode_codec = codec.clone();
        let encode_fn: EncodeFn = Box::new(move |msg: Box<dyn Any + Send>| {
            let typed = msg
                .downcast::<M>()
                .map_err(|_| Error::from("Transport: type downcast failed during encode"))?;
            let bytes = encode_codec.encode(&*typed)?;
            Ok((M::type_tag().to_string(), bytes))
        });
        self.encoders.insert(TypeId::of::<M>(), encode_fn);

        // Decoder side — closure captures Arc<C>
        let decode_fn: DecodeFn = Box::new(move |bytes: &[u8]| {
            let msg: M = codec.decode(bytes)?;
            Ok(Box::new(msg) as Box<dyn Any + Send>)
        });
        self.decoders.insert(M::type_tag().to_string(), decode_fn);
    }

    /// Encode a type-erased message. Returns `(type_tag, payload_bytes)`.
    pub fn encode(
        &self,
        type_id: TypeId,
        msg: Box<dyn Any + Send>,
    ) -> Result<(String, Vec<u8>), Error> {
        let encoder = self.encoders.get(&type_id).ok_or_else(|| {
            Error::from("Transport: message type not registered for remote transport")
        })?;
        encoder(msg)
    }

    /// Decode bytes back to a type-erased message using the `type_tag` key.
    pub fn decode(
        &self,
        type_tag: &str,
        bytes: &[u8],
    ) -> Result<Box<dyn Any + Send>, Error> {
        let decoder = self.decoders.get(type_tag).ok_or_else(|| {
            Error::from(format!("Transport: unknown type_tag '{type_tag}'"))
        })?;
        decoder(bytes)
    }

    /// Deserialize a [`WireEnvelope`] into an address and type-erased message.
    pub fn receive(
        &self,
        envelope: WireEnvelope,
    ) -> Result<(ActorAddress, Box<dyn Any + Send>), Error> {
        let payload = self.decode(&envelope.type_tag, &envelope.payload)?;
        Ok((envelope.dest, payload))
    }
}

// ─── TransportRouter ────────────────────────────────────────────────────────

/// Maps remote actor addresses to their [`Transport`].
pub struct TransportRouter {
    routes: RwLock<AddrMap<Arc<dyn Transport>>>,
}

impl Default for TransportRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportRouter {
    pub fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
        }
    }

    /// Register a remote address as reachable via the given transport.
    pub fn add_route(&self, addr: ActorAddress, transport: Arc<dyn Transport>) {
        self.routes.write().unwrap().insert(addr, transport);
    }

    /// Look up which transport handles a given address.
    pub(crate) fn lookup(&self, addr: &ActorAddress) -> Option<Arc<dyn Transport>> {
        self.routes.read().unwrap().get(addr).cloned()
    }
}

// ─── InMemoryTransport ──────────────────────────────────────────────────────

/// In-process transport connecting two runtimes via an `mpsc` channel.
///
/// Use [`pair`](Self::pair) to create a linked transport + receiver.
pub struct InMemoryTransport {
    tx: std::sync::Mutex<std::sync::mpsc::Sender<WireEnvelope>>,
}

impl InMemoryTransport {
    /// Create a linked pair: the transport sends to the returned receiver.
    pub fn pair() -> (Arc<InMemoryTransport>, std::sync::mpsc::Receiver<WireEnvelope>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let transport = Arc::new(InMemoryTransport {
            tx: std::sync::Mutex::new(tx),
        });
        (transport, rx)
    }
}

impl Transport for InMemoryTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        self.tx
            .lock()
            .unwrap()
            .send(envelope)
            .map_err(|_| Error::from("InMemoryTransport: receiver dropped"))
    }
}

// ─── send_via_transport (crate-internal helper) ─────────────────────────────

/// Attempt to serialize and send a message via the transport router.
///
/// Called by the send paths in `Runtime` and `WorkerContext` when an address
/// is not found locally or in the inbox registry.
pub(crate) fn send_via_transport(
    addr: ActorAddress,
    msg: Box<dyn Any + Send>,
    codec_registry: &CodecRegistry,
    transport_router: &TransportRouter,
) -> Result<(), Error> {
    let transport = transport_router
        .lookup(&addr)
        .ok_or_else(|| Error::from("Address not found"))?;

    let type_id = (*msg).type_id();
    let (type_tag, payload) = codec_registry.encode(type_id, msg)?;

    let wire = WireEnvelope {
        dest: addr,
        type_tag,
        payload,
    };
    transport.send(wire)
}
