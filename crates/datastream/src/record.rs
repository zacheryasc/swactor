//! Extension contract for typed datastream payloads.
//!
//! The pipe owns frames, ordering, transport, ingest, and storage. It does not
//! own the universe of channel meanings. Producers and consumers define records
//! in their own crates by implementing [`Record`], then optionally compose a
//! [`ChannelRegistry`] when a view needs to render payload bytes by channel name.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// How a view should treat a channel's payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    /// Decode as structured JSON for display.
    Typed,
    /// Decode as UTF-8 text for display.
    Text,
    /// Unknown to this consumer; preserve and render as raw bytes.
    Opaque,
}

/// Caller-owned channel-name classifier used by views.
pub trait ChannelClassifier {
    fn classify(&self, channel_name: &str) -> ChannelKind;
}

impl<F> ChannelClassifier for F
where
    F: Fn(&str) -> ChannelKind,
{
    fn classify(&self, channel_name: &str) -> ChannelKind {
        self(channel_name)
    }
}

/// A typed channel record: crates define their own records and bind each one to
/// the channel name it rides on.
pub trait Record: Serialize + for<'de> Deserialize<'de> + Sized {
    /// The concrete channel name this record is carried on.
    const CHANNEL: &'static str;

    /// The channel name this record is carried on.
    fn channel_name() -> &'static str {
        Self::CHANNEL
    }

    /// Encode this record to opaque payload bytes.
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("record serializes to JSON")
    }

    /// Decode payload bytes back into the record.
    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

/// Small composable classifier for consumers that want registry-style decoding
/// without putting a global catalog inside `datastream`.
#[derive(Debug, Clone, Default)]
pub struct ChannelRegistry {
    typed: BTreeSet<String>,
    text: BTreeSet<String>,
    text_prefixes: Vec<String>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_typed_channel(mut self, channel: impl Into<String>) -> Self {
        self.typed.insert(channel.into());
        self
    }

    pub fn with_record<R: Record>(self) -> Self {
        self.with_typed_channel(R::CHANNEL)
    }

    pub fn with_text_channel(mut self, channel: impl Into<String>) -> Self {
        self.text.insert(channel.into());
        self
    }

    pub fn with_text_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.text_prefixes.push(prefix.into());
        self
    }

    pub fn classify_name(&self, channel: &str) -> ChannelKind {
        if self.typed.contains(channel) {
            ChannelKind::Typed
        } else if self.text.contains(channel)
            || self
                .text_prefixes
                .iter()
                .any(|prefix| channel.starts_with(prefix))
        {
            ChannelKind::Text
        } else {
            ChannelKind::Opaque
        }
    }
}

impl ChannelClassifier for ChannelRegistry {
    fn classify(&self, channel_name: &str) -> ChannelKind {
        self.classify_name(channel_name)
    }
}
