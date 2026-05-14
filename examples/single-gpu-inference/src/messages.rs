//! Inference message types, codec, and registry for the smoke test.

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::transport::{Codec, CodecRegistry, NetworkMessage};
use swactor::Error;

// ─── Message Types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferenceRequest {
    pub prompt: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub reply_to: ActorAddress,
}

impl NetworkMessage for InferenceRequest {
    fn type_tag() -> &'static str {
        "smoke::InferenceRequest"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferenceResponse {
    pub text: String,
}

impl NetworkMessage for InferenceResponse {
    fn type_tag() -> &'static str {
        "smoke::InferenceResponse"
    }
}

// ─── JSON Codec ────────────────────────────────────────────────────────────

pub struct InferenceCodec;

impl Codec<InferenceRequest> for InferenceCodec {
    fn encode(&self, msg: &InferenceRequest) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode InferenceRequest: {e}")))
    }
    fn decode(&self, bytes: &[u8]) -> Result<InferenceRequest, Error> {
        serde_json::from_slice(bytes)
            .map_err(|e| Error::from(format!("decode InferenceRequest: {e}")))
    }
}

impl Codec<InferenceResponse> for InferenceCodec {
    fn encode(&self, msg: &InferenceResponse) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(msg).map_err(|e| Error::from(format!("encode InferenceResponse: {e}")))
    }
    fn decode(&self, bytes: &[u8]) -> Result<InferenceResponse, Error> {
        serde_json::from_slice(bytes)
            .map_err(|e| Error::from(format!("decode InferenceResponse: {e}")))
    }
}

// ─── Registry ──────────────────────────────────────────────────────────────

/// Build a `CodecRegistry` with inference message types registered.
pub fn inference_codec_registry() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<InferenceRequest, _>(InferenceCodec);
    cr.register::<InferenceResponse, _>(InferenceCodec);
    cr
}
