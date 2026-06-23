//! Pipeline-parallel inference message types, codec, and registry.
//!
//! Mirrors the single-GPU `messages.rs` shape — JSON over swactor's codec —
//! and adds the two pipeline-specific message types that close the
//! autoregressive loop between stage 0 and stage 1.

use serde::{Deserialize, Serialize};
use swactor::Error;
use swactor::actor::ActorAddress;
use swactor_transport::{Codec, CodecRegistry, NetworkMessage};

// ─── Message Types ─────────────────────────────────────────────────────────

/// Submitted by the orchestrator to stage 0. `max_tokens` is the upper bound
/// on the decode loop; sampling stops earlier on EOS.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferenceRequest {
    pub reply_to: ActorAddress,
    pub prompt: String,
    pub max_tokens: u32,
}

impl NetworkMessage for InferenceRequest {
    fn type_tag() -> &'static str {
        "pp::InferenceRequest"
    }
}

/// Final detokenized output, sent from stage 1 back to the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferenceResponse {
    pub text: String,
}

impl NetworkMessage for InferenceResponse {
    fn type_tag() -> &'static str {
        "pp::InferenceResponse"
    }
}

/// Hidden-state hand-off between stages. `hidden` carries raw little-endian
/// bf16 bytes of shape `[seq_len, hidden_dim]`. For `is_prefill=true`,
/// `position` is 0 and `seq_len` is the prompt length; for decode steps,
/// `position` is the next slot in the KV cache and `seq_len` is 1.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StageActivation {
    pub request_id: u64,
    pub position: u32,
    pub hidden: Vec<u8>,
    pub seq_len: u32,
    pub is_prefill: bool,
}

impl NetworkMessage for StageActivation {
    fn type_tag() -> &'static str {
        "pp::StageActivation"
    }
}

/// Sampled token fed back from stage 1 to stage 0 to drive the next decode
/// step. `done=true` means the decode loop has terminated (EOS or
/// `max_tokens`) and no further activation should be produced.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NextToken {
    pub request_id: u64,
    pub token_id: u32,
    pub position: u32,
    pub done: bool,
}

impl NetworkMessage for NextToken {
    fn type_tag() -> &'static str {
        "pp::NextToken"
    }
}

// ─── JSON Codec ────────────────────────────────────────────────────────────

pub struct InferenceCodec;

macro_rules! impl_json_codec {
    ($msg:ty, $label:expr) => {
        impl Codec<$msg> for InferenceCodec {
            fn encode(&self, msg: &$msg) -> Result<Vec<u8>, Error> {
                serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode {}: {e}", $label)))
            }
            fn decode(&self, bytes: &[u8]) -> Result<$msg, Error> {
                serde_json::from_slice(bytes)
                    .map_err(|e| Error::from(format!("decode {}: {e}", $label)))
            }
        }
    };
}

impl_json_codec!(InferenceRequest, "InferenceRequest");
impl_json_codec!(InferenceResponse, "InferenceResponse");
impl_json_codec!(NextToken, "NextToken");

// ─── Binary Codec: StageActivation ─────────────────────────────────────────
//
// `StageActivation` is the only message that carries a large payload — the raw
// bf16 hidden state, which can be multiple megabytes. serde_json serializes
// `hidden: Vec<u8>` as a JSON array of decimal integers (`[123,45,255,...]`),
// inflating the tensor ~4x on the wire. The other three message types stay on
// JSON: they are tiny, human-debuggable, and their JSON shape is what makes
// cross-type decode errors trivial to catch.
//
// Wire layout (little-endian), `magic` chosen ≠ `0x7B` (`{`) so a JSON payload
// cross-decoded as a `StageActivation` fails immediately on the magic byte:
//
//   [magic u8][version u8][request_id u64][position u32][seq_len u32]
//   [is_prefill u8][hidden_len u32][hidden bytes…][checksum u32]
//
// The trailing checksum is FNV-1a over every preceding byte (header + hidden),
// so any single-byte flip or truncation is rejected on decode.

/// Magic byte. Must differ from `b'{'` (0x7B) so JSON bytes fed to this
/// decoder fail on the first byte rather than deeper in.
const SA_MAGIC: u8 = 0xB1;
/// Wire-format version, bumped if the layout below ever changes.
const SA_VERSION: u8 = 1;
/// Fixed header size: magic + version + request_id + position + seq_len +
/// is_prefill + hidden_len = 1 + 1 + 8 + 4 + 4 + 1 + 4.
const SA_HEADER_LEN: usize = 23;
/// Trailing checksum width.
const SA_CHECKSUM_LEN: usize = 4;

/// FNV-1a 32-bit hash over `bytes`. Inline so the codec carries no dependency.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in bytes {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

impl Codec<StageActivation> for InferenceCodec {
    fn encode(&self, msg: &StageActivation) -> Result<Vec<u8>, Error> {
        let hidden_len = u32::try_from(msg.hidden.len())
            .map_err(|_| Error::from("encode StageActivation: hidden too large for u32"))?;
        let mut buf = Vec::with_capacity(SA_HEADER_LEN + msg.hidden.len() + SA_CHECKSUM_LEN);
        buf.push(SA_MAGIC);
        buf.push(SA_VERSION);
        buf.extend_from_slice(&msg.request_id.to_le_bytes());
        buf.extend_from_slice(&msg.position.to_le_bytes());
        buf.extend_from_slice(&msg.seq_len.to_le_bytes());
        buf.push(msg.is_prefill as u8);
        buf.extend_from_slice(&hidden_len.to_le_bytes());
        buf.extend_from_slice(&msg.hidden);
        // Checksum over the header + hidden written so far (not over itself).
        let checksum = fnv1a_32(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    fn decode(&self, bytes: &[u8]) -> Result<StageActivation, Error> {
        if bytes.len() < SA_HEADER_LEN + SA_CHECKSUM_LEN {
            return Err(Error::from("decode StageActivation: too short"));
        }
        if bytes[0] != SA_MAGIC {
            return Err(Error::from("decode StageActivation: bad magic"));
        }
        if bytes[1] != SA_VERSION {
            return Err(Error::from("decode StageActivation: unsupported version"));
        }
        let request_id = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let position = u32::from_le_bytes(bytes[10..14].try_into().unwrap());
        let seq_len = u32::from_le_bytes(bytes[14..18].try_into().unwrap());
        let is_prefill = bytes[18] != 0;
        let hidden_len = u32::from_le_bytes(bytes[19..23].try_into().unwrap()) as usize;
        if bytes.len() != SA_HEADER_LEN + hidden_len + SA_CHECKSUM_LEN {
            return Err(Error::from("decode StageActivation: length mismatch"));
        }
        let checksum_off = SA_HEADER_LEN + hidden_len;
        let expected = fnv1a_32(&bytes[..checksum_off]);
        let actual = u32::from_le_bytes(bytes[checksum_off..].try_into().unwrap());
        if expected != actual {
            return Err(Error::from("decode StageActivation: checksum mismatch"));
        }
        let hidden = bytes[SA_HEADER_LEN..checksum_off].to_vec();
        Ok(StageActivation {
            request_id,
            position,
            hidden,
            seq_len,
            is_prefill,
        })
    }
}

// ─── Registry ──────────────────────────────────────────────────────────────

/// Build a `CodecRegistry` with all pipeline-parallel message types
/// registered for the actor transport, layered on top of the distribution
/// protocol codecs. The actor transport is shared between protocol gossip
/// (SWIM / registry / metadata / directory) and pp app traffic, so one
/// registry has to encode/decode both. Tags don't collide — protocol uses
/// `swactor_dist::*` and pp uses pp-specific type tags from `InferenceCodec`.
pub fn inference_codec_registry() -> CodecRegistry {
    let mut cr = distribution::messages::actor_codec_registry();
    cr.register::<InferenceRequest, _>(InferenceCodec);
    cr.register::<InferenceResponse, _>(InferenceCodec);
    cr.register::<StageActivation, _>(InferenceCodec);
    cr.register::<NextToken, _>(InferenceCodec);
    // Fleet telemetry rides the same actor transport: every node ships its
    // datastream as `DatastreamFrame` messages to the orchestrator's
    // `datastream-sink` actor, so both ends must decode them.
    datastream::wire::register_datastream_codec(&mut cr);
    cr
}
