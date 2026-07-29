#![allow(dead_code)]

//! MVP-system-owned datastream channel records.

use datastream::hardware::net::HostNetSample;
use datastream::{ChannelRegistry, Record};
use serde::{Deserialize, Serialize};

use crate::observability::lifecycle as obs;
use crate::provisioning::{self, ProvisionLogStream};
use data_plane::arena::ArenaSample;

/// Structured MVP lifecycle facts: run, node, stage, edge, ring, object, step, and worker events.
pub(crate) const MVP_LIFECYCLE: &str = "mvp.lifecycle";
/// Structured node provisioning milestones emitted before a remote swactor runtime is live.
pub(crate) const MVP_PROVISIONING_EVENTS: &str = "mvp.provisioning.events";

/// Raw provider/process stream lines captured during provisioning.
pub(crate) const MVP_PROVISIONING_LOGS: &str = "mvp.provisioning.logs";

/// Datastream payload for the MVP lifecycle channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MvpLifecycleRecord {
    pub event: obs::Event,
}

impl MvpLifecycleRecord {
    pub(crate) fn new(event: obs::Event) -> Self {
        Self { event }
    }

    pub(crate) fn kind(&self) -> obs::EventKind {
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
pub(crate) struct MvpProvisionEventRecord {
    pub event: provisioning::ProvisionEvent,
}

impl MvpProvisionEventRecord {
    pub(crate) fn new(event: provisioning::ProvisionEvent) -> Self {
        Self { event }
    }
}

impl Record for MvpProvisionEventRecord {
    const CHANNEL: &'static str = MVP_PROVISIONING_EVENTS;
}

/// Datastream payload for provisioning stdout/stderr/provider lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MvpProvisionLogRecord {
    pub line: provisioning::ProvisionLogLine,
}

impl MvpProvisionLogRecord {
    pub(crate) fn new(line: provisioning::ProvisionLogLine) -> Self {
        Self { line }
    }
}

pub(crate) fn mvp_provision_log_channel(node_id: u64, stream: ProvisionLogStream) -> String {
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
pub(crate) fn channel_registry() -> ChannelRegistry {
    let registry = ChannelRegistry::new()
        .with_record::<MvpLifecycleRecord>()
        .with_record::<MvpProvisionEventRecord>()
        .with_record::<MvpProvisionLogRecord>()
        .with_record::<HostNetSample>()
        .with_record::<ArenaSample>();
    registry
}
