//! MVP-system-owned datastream channel records.

use datastream::{ChannelRegistry, Record};
use serde::{Deserialize, Serialize};

use crate::observability_surface as obs;

/// Structured MVP lifecycle facts: run, node, stage, edge, ring, object, step, and worker events.
pub const MVP_LIFECYCLE: &str = "mvp.lifecycle";

/// Datastream payload for the MVP lifecycle channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvpLifecycleRecord {
    pub event: obs::Event,
}

impl MvpLifecycleRecord {
    pub fn new(event: obs::Event) -> Self {
        Self { event }
    }

    pub fn kind(&self) -> obs::EventKind {
        self.event.kind()
    }
}

impl From<obs::Event> for MvpLifecycleRecord {
    fn from(event: obs::Event) -> Self {
        Self::new(event)
    }
}

impl Record for MvpLifecycleRecord {
    const CHANNEL: &'static str = MVP_LIFECYCLE;
}

/// Registry fragment for consumers that want typed MVP datastream decoding.
pub fn channel_registry() -> ChannelRegistry {
    ChannelRegistry::new().with_record::<MvpLifecycleRecord>()
}
