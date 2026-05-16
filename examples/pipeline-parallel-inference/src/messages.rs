//! Pipeline-parallel inference message types, codec, and registry.
//!
//! Mirrors the single-GPU `messages.rs` shape — JSON over swactor's codec —
//! and adds the two pipeline-specific message types that close the
//! autoregressive loop between stage 0 and stage 1.

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::transport::{Codec, CodecRegistry, NetworkMessage};
use swactor::Error;

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
                serde_json::to_vec(msg)
                    .map_err(|e| Error::from(format!("encode {}: {e}", $label)))
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
impl_json_codec!(StageActivation, "StageActivation");
impl_json_codec!(NextToken, "NextToken");

// ─── Registry ──────────────────────────────────────────────────────────────

/// Build a `CodecRegistry` with all pipeline-parallel message types
/// registered for the actor transport.
pub fn inference_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<InferenceRequest, _>(InferenceCodec);
    cr.register::<InferenceResponse, _>(InferenceCodec);
    cr.register::<StageActivation, _>(InferenceCodec);
    cr.register::<NextToken, _>(InferenceCodec);
    cr
}
