//! Catalog-aware decoding and rendering for retained failure artifacts.
//!
//! Numeric channel ids are stream-local. A consumer must ingest the subscription
//! snapshot and subsequent catalog events before decoding frames; matching only
//! an id or trying every payload as a transition can misidentify unrelated data.

use std::collections::BTreeSet;
use std::fmt;

use telemetry::frame::TelemetryEvent;
use telemetry::{
    ChannelContent, ChannelDescriptor, ChannelRef, ChannelRegistry, Position, Record,
    TelemetrySnapshot,
};

use crate::{TransitionRecord, VERSION};

/// Classifier for composing transition decoding into the existing telemetry views.
pub fn registry() -> ChannelRegistry {
    ChannelRegistry::new().with_record::<TransitionRecord>()
}

/// One decoded transition with the telemetry pipe's authoritative identity/order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedTransition {
    pub channel: ChannelRef,
    pub position: Position,
    pub record: TransitionRecord,
}

impl fmt::Display for ObservedTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let record = &self.record;
        write!(
            f,
            "{} #{} [{}] protocol={:?}/v{} actor_type={:?} actor=",
            self.channel.stream,
            self.position,
            self.channel.channel,
            record.protocol,
            VERSION,
            record.actor_type,
        )?;
        for byte in record.actor.0 {
            write!(f, "{byte:02x}")?;
        }
        if let Some(operation) = record.operation {
            f.write_str(" operation=")?;
            for byte in operation {
                write!(f, "{byte:02x}")?;
            }
        }
        if let Some(generation) = record.generation {
            write!(f, " generation={generation}")?;
        }
        if let Some(incarnation) = record.incarnation {
            write!(f, " incarnation={incarnation}")?;
        }
        write!(
            f,
            " {:?} -> {:?} reason={:?}",
            record.previous, record.next, record.reason,
        )?;
        if let Some(outcome) = record.outcome {
            write!(f, " outcome={outcome:?}")?;
        }
        Ok(())
    }
}

/// A malformed transition remains attributable to its original retained frame.
#[derive(Debug)]
pub struct TransitionDecodeError {
    pub channel: ChannelRef,
    pub position: Position,
    pub source: rmp_serde::decode::Error,
}

impl fmt::Display for TransitionDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid protocol transition at {} #{} [{}]: {}",
            self.channel.stream, self.position, self.channel.channel, self.source,
        )
    }
}

impl std::error::Error for TransitionDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Decoder for a catalog-aware telemetry subscription or saved event archive.
#[derive(Default)]
pub struct TransitionDecoder {
    channels: BTreeSet<ChannelRef>,
}

impl TransitionDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed decoding from metadata supplied before retained/live frame delivery.
    pub fn from_snapshot(snapshot: &TelemetrySnapshot) -> Self {
        let mut decoder = Self::new();
        for descriptor in &snapshot.channels {
            decoder.register(descriptor);
        }
        decoder
    }

    /// Apply a catalog entry. Re-declarations replace the prior interpretation.
    pub fn register(&mut self, descriptor: &ChannelDescriptor) {
        let channel = ChannelRef {
            stream: descriptor.stream.clone(),
            channel: descriptor.id,
        };
        if descriptor.name == TransitionRecord::CHANNEL
            && matches!(
                &descriptor.content,
                ChannelContent::MessagePackRecord { schema }
                    if schema.as_deref() == Some(TransitionRecord::CHANNEL)
            )
        {
            self.channels.insert(channel);
        } else {
            self.channels.remove(&channel);
        }
    }

    /// Decode only catalog-identified v1 transitions; unrelated events are ignored.
    ///
    /// Decode failure is returned rather than silently removing failure evidence.
    pub fn observe(
        &mut self,
        event: &TelemetryEvent,
    ) -> Result<Option<ObservedTransition>, TransitionDecodeError> {
        match event {
            TelemetryEvent::ChannelDeclared(descriptor) => {
                self.register(descriptor);
                Ok(None)
            }
            TelemetryEvent::StreamEnded(stream) => {
                self.channels.retain(|channel| &channel.stream != stream);
                Ok(None)
            }
            TelemetryEvent::Frame(frame) if self.channels.contains(&frame.channel) => {
                let record = TransitionRecord::decode(&frame.payload).map_err(|source| {
                    TransitionDecodeError {
                        channel: frame.channel.clone(),
                        position: frame.position,
                        source,
                    }
                })?;
                Ok(Some(ObservedTransition {
                    channel: frame.channel.clone(),
                    position: frame.position,
                    record,
                }))
            }
            _ => Ok(None),
        }
    }
}
