//! MVP-system-owned datastream channel records.

use datastream::hardware::net::HostNetSample;
use datastream::{ChannelRegistry, Record};
use serde::{Deserialize, Serialize};

use crate::node_data::arena::ArenaSample;
use crate::observability::lifecycle as obs;
use crate::orchestration::provisioning::{self, ProvisionLogStream};

/// Structured MVP lifecycle facts: run, node, stage, edge, ring, object, step, and worker events.
pub const MVP_LIFECYCLE: &str = "mvp.lifecycle";
/// Structured node provisioning milestones emitted before a remote swactor runtime is live.
pub const MVP_PROVISIONING_EVENTS: &str = "mvp.provisioning.events";

/// Raw provider/process stream lines captured during provisioning.
pub const MVP_PROVISIONING_LOGS: &str = "mvp.provisioning.logs";

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
/// Datastream payload for provisioning lifecycle events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvpProvisionEventRecord {
    pub event: provisioning::ProvisionEvent,
}

impl MvpProvisionEventRecord {
    pub fn new(event: provisioning::ProvisionEvent) -> Self {
        Self { event }
    }
}

impl Record for MvpProvisionEventRecord {
    const CHANNEL: &'static str = MVP_PROVISIONING_EVENTS;
}

/// Datastream payload for provisioning stdout/stderr/provider lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvpProvisionLogRecord {
    pub line: provisioning::ProvisionLogLine,
}

impl MvpProvisionLogRecord {
    pub fn new(line: provisioning::ProvisionLogLine) -> Self {
        Self { line }
    }
}

pub fn mvp_provision_log_channel(node_id: u64, stream: ProvisionLogStream) -> String {
    let stream = match stream {
        ProvisionLogStream::Stdout => "stdout",
        ProvisionLogStream::Stderr => "stderr",
        ProvisionLogStream::Provider => "provider",
    };
    format!("mvp.provisioning.logs.node.{node_id}.{stream}")
}

impl Record for MvpProvisionLogRecord {
    const CHANNEL: &'static str = MVP_PROVISIONING_LOGS;
}

/// Registry fragment for consumers that want typed MVP datastream decoding.
pub fn channel_registry() -> ChannelRegistry {
    let registry = ChannelRegistry::new()
        .with_record::<MvpLifecycleRecord>()
        .with_record::<MvpProvisionEventRecord>()
        .with_record::<MvpProvisionLogRecord>()
        .with_record::<HostNetSample>()
        .with_record::<ArenaSample>();
    registry
}
