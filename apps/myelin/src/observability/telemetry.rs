//! Myelin-system-owned telemetry channel records.

use serde::{Deserialize, Serialize};
use telemetry::Record;

use crate::observability::lifecycle as obs;
use crate::provisioning::{self, ProvisionLogStream};

/// Structured Myelin lifecycle facts: run, node, stage, edge, ring, object, step, and worker events.
pub(crate) const MYELIN_LIFECYCLE: &str = "myelin.lifecycle";
/// Structured node provisioning milestones emitted before a remote swactor runtime is live.
pub(crate) const MYELIN_PROVISIONING_EVENTS: &str = "myelin.provisioning.events";

/// Raw provider/process stream lines captured during provisioning.
pub(crate) const MYELIN_PROVISIONING_LOGS: &str = "myelin.provisioning.logs";

/// Telemetry payload for the Myelin lifecycle channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MyelinLifecycleRecord {
    pub event: obs::Event,
}

impl Record for MyelinLifecycleRecord {
    const CHANNEL: &'static str = MYELIN_LIFECYCLE;
}
/// Telemetry payload for provisioning lifecycle events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MyelinProvisionEventRecord {
    pub event: provisioning::ProvisionEvent,
}

impl MyelinProvisionEventRecord {
    pub(crate) fn new(event: provisioning::ProvisionEvent) -> Self {
        Self { event }
    }
}

impl Record for MyelinProvisionEventRecord {
    const CHANNEL: &'static str = MYELIN_PROVISIONING_EVENTS;
}

/// Telemetry payload for provisioning stdout/stderr/provider lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MyelinProvisionLogRecord {
    pub line: provisioning::ProvisionLogLine,
}

impl MyelinProvisionLogRecord {
    pub(crate) fn new(line: provisioning::ProvisionLogLine) -> Self {
        Self { line }
    }
}

pub(crate) fn myelin_provision_log_channel(node_id: u64, stream: ProvisionLogStream) -> String {
    let stream = match stream {
        ProvisionLogStream::Stdout => "stdout",
        ProvisionLogStream::Stderr => "stderr",
        ProvisionLogStream::Provider => "provider",
    };
    format!("myelin.provisioning.logs.node.{node_id}.{stream}")
}

impl Record for MyelinProvisionLogRecord {
    const CHANNEL: &'static str = MYELIN_PROVISIONING_LOGS;
}
