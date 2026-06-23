//! Extension contract for typed datastream payloads.
//!
//! The pipe owns frames, ordering, transport, ingest, and storage. It does not
//! own the universe of channel meanings. Producers and consumers define records
//! in their own crates by implementing [`Record`], then optionally compose a
//! [`ChannelRegistry`] when a view needs to render payload bytes.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::frame::ChannelId;

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

/// Caller-owned channel classifier used by views.
pub trait ChannelClassifier {
    fn classify(&self, channel: &ChannelId) -> ChannelKind;
}

impl<F> ChannelClassifier for F
where
    F: Fn(&ChannelId) -> ChannelKind,
{
    fn classify(&self, channel: &ChannelId) -> ChannelKind {
        self(channel)
    }
}

/// A typed channel record: crates define their own records and bind each one to
/// the channel it rides on.
pub trait Record: Serialize + for<'de> Deserialize<'de> + Sized {
    /// The concrete channel this record is carried on.
    const CHANNEL: &'static str;

    /// The channel id this record is carried on.
    fn channel() -> ChannelId {
        ChannelId::new(Self::CHANNEL)
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

    pub fn classify_channel(&self, channel: &ChannelId) -> ChannelKind {
        let id = channel.as_str();
        if self.typed.contains(id) {
            ChannelKind::Typed
        } else if self.text.contains(id)
            || self
                .text_prefixes
                .iter()
                .any(|prefix| id.starts_with(prefix))
        {
            ChannelKind::Text
        } else {
            ChannelKind::Opaque
        }
    }
}

impl ChannelClassifier for ChannelRegistry {
    fn classify(&self, channel: &ChannelId) -> ChannelKind {
        self.classify_channel(channel)
    }
}
