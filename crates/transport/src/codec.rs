//! Wire format: codecs, the codec registry, node identity, and hex helpers.
//!
//! This is the serialization half of the transport stack. A [`Codec<M>`]
//! defines HOW a message type turns into bytes; a [`CodecRegistry`] collects
//! per-type encoders/decoders so a type-erased message can be put on the wire
//! and a [`WireEnvelope`] can be turned back into a concrete message.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use swactor::actor::{ActorAddress, Message};
use swactor::Error;

// ─── NodeId ─────────────────────────────────────────────────────────────────

/// A node's identity — 32 opaque bytes.
///
/// Typically the raw bytes of an ed25519 public key, but this type
/// carries no cryptographic semantics.
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
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
/// A plain struct — the [`Transport`](crate::transport::Transport)
/// implementation decides how to put it on the wire (protobuf, raw bytes, etc.).
#[derive(Debug, Clone)]
pub struct WireEnvelope {
    pub dest: ActorAddress,
    pub type_tag: String,
    pub payload: Vec<u8>,
}

// ─── CodecRegistry ──────────────────────────────────────────────────────────

type EncodeFn = Box<dyn Fn(Box<dyn Any + Send>) -> Result<(String, Vec<u8>), Error> + Send + Sync>;
type DecodeFn = Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send>, Error> + Send + Sync>;

/// Unified registry for encoding (`TypeId` → encoder) and decoding
/// (`type_tag` → decoder).
///
/// Built at setup time via [`register`](Self::register) (symmetric, one type
/// ↔ one tag) or the lower-level [`register_encoder`](Self::register_encoder) /
/// [`register_decoder`](Self::register_decoder) (variant-multiplexing encode,
/// fan-in decode), then shared read-only via `Arc`.
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
    /// Both encoding and decoding are handled by the same codec instance, keyed
    /// symmetrically by `M::type_tag()`.
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

    /// Register a **variant-multiplexing** encoder for one Rust type `M`.
    ///
    /// Unlike [`register`](Self::register), the closure inspects the value and
    /// picks the wire `type_tag` per-variant (one Rust type → many tags), and
    /// may refuse to encode local-only variants by returning `Err`. `M` is a
    /// plain [`Message`] (e.g. an actor `Incoming` enum), not a
    /// [`NetworkMessage`] — it has no single canonical `type_tag`.
    pub fn register_encoder<M: Message>(
        &mut self,
        e: impl Fn(&M) -> Result<(String, Vec<u8>), Error> + Send + Sync + 'static,
    ) {
        let encode_fn: EncodeFn = Box::new(move |msg: Box<dyn Any + Send>| {
            let typed = msg
                .downcast::<M>()
                .map_err(|_| Error::from("Transport: type downcast failed during encode"))?;
            e(&*typed)
        });
        self.encoders.insert(TypeId::of::<M>(), encode_fn);
    }

    /// Register a **fan-in** decoder mapping an arbitrary wire `type_tag` to one
    /// Rust type `M`.
    ///
    /// Unlike [`register`](Self::register), the tag is caller-chosen, so several
    /// tags can decode into the same actor enum `M`. `M` is a plain [`Message`]
    /// (e.g. an actor `Incoming` enum), not a [`NetworkMessage`].
    pub fn register_decoder<M: Message>(
        &mut self,
        type_tag: &str,
        d: impl Fn(&[u8]) -> Result<M, Error> + Send + Sync + 'static,
    ) {
        let decode_fn: DecodeFn =
            Box::new(move |bytes: &[u8]| Ok(Box::new(d(bytes)?) as Box<dyn Any + Send>));
        self.decoders.insert(type_tag.to_string(), decode_fn);
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
    pub fn decode(&self, type_tag: &str, bytes: &[u8]) -> Result<Box<dyn Any + Send>, Error> {
        let decoder = self
            .decoders
            .get(type_tag)
            .ok_or_else(|| Error::from(format!("Transport: unknown type_tag '{type_tag}'")))?;
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
