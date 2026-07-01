use std::io::{BufRead, Write};

use serde::{Deserialize, Serialize};
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::actors::codec::JsonCodec;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitPrompt {
    pub request_id: u64,
    pub prompt_text: String,
    pub max_tokens: u32,
    pub timeout_ms: u64,
}

impl SubmitPrompt {
    pub fn with_defaults(mut self, max_tokens: u32, timeout_ms: u64) -> Self {
        if self.max_tokens == 0 {
            self.max_tokens = max_tokens;
        }
        if self.timeout_ms == 0 {
            self.timeout_ms = timeout_ms;
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptEvent {
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

impl PromptEvent {
    pub fn request_id(&self) -> u64 {
        match self {
            Self::TextDelta { request_id, .. }
            | Self::Done { request_id, .. }
            | Self::Fault { request_id, .. } => *request_id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Fault { .. })
    }
}

impl NetworkMessage for PromptEvent {
    fn type_tag() -> &'static str {
        "mvp_system::PromptEvent"
    }
}

pub fn register_codecs(registry: &mut CodecRegistry) {
    registry.register::<PromptEvent, _>(JsonCodec::<PromptEvent>::default());
}

pub fn write_json_line<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, value).map_err(|e| format!("serialize JSON line: {e}"))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("write JSON line: {e}"))?;
    writer.flush().map_err(|e| format!("flush JSON line: {e}"))
}

pub fn read_submit_prompt(reader: &mut impl BufRead) -> Result<Option<SubmitPrompt>, String> {
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .map_err(|e| format!("read prompt request: {e}"))?;
    if n == 0 {
        return Ok(None);
    }
    serde_json::from_str::<SubmitPrompt>(&line)
        .map(Some)
        .map_err(|e| format!("parse prompt request: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_request_limits_take_loop_defaults() {
        let request = SubmitPrompt {
            request_id: 7,
            prompt_text: "hello".to_owned(),
            max_tokens: 0,
            timeout_ms: 0,
        }
        .with_defaults(32, 1_000);

        assert_eq!(request.max_tokens, 32);
        assert_eq!(request.timeout_ms, 1_000);
    }

    #[test]
    fn event_terminal_state_is_explicit() {
        assert!(
            !PromptEvent::TextDelta {
                request_id: 1,
                text: "a".to_owned(),
            }
            .is_terminal()
        );
        assert!(
            PromptEvent::Done {
                request_id: 1,
                final_text: "a".to_owned(),
                tokens_generated: 1,
                elapsed_ms: 2,
            }
            .is_terminal()
        );
        assert!(
            PromptEvent::Fault {
                request_id: 1,
                error: "boom".to_owned(),
            }
            .is_terminal()
        );
    }
}
