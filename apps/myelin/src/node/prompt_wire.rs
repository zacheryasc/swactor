//! Wire types for the dormant stage-pipeline prompt/tokenizer channels.
//!
//! The node runtime still hosts the pipeline stage channels (they are part of
//! the node agent contract); the orchestrator no longer drives prompts, so
//! only the wire structs live on.

use serde::{Deserialize, Serialize};
use swactor_transport::NetworkMessage;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PromptEvent {
    TextDelta {
        request_id: u64,
        text: String,
    },
    Done {
        request_id: u64,
        final_text: String,
        tokens_generated: u32,
        elapsed_ms: u64,
    },
    Fault {
        request_id: u64,
        error: String,
    },
}

impl NetworkMessage for PromptEvent {
    fn type_tag() -> &'static str {
        "myelin::PromptEvent"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TokenizerEvent {
    PromptEncoded { request_id: u64, tokens: Vec<u32> },
    TokensDecoded { request_id: u64, text: String },
    Fault { request_id: u64, error: String },
}

impl NetworkMessage for TokenizerEvent {
    fn type_tag() -> &'static str {
        "myelin::TokenizerEvent"
    }
}
