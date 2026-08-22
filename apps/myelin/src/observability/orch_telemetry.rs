//! Orchestrator-owned telemetry producer plus dashboard sink support.
//!
//! [`OrchTelemetry`] owns the producer-side endpoint that emits bootstrap,
//! prompt, provisioning, and SWIM telemetry. [`DashboardSupport`] adapts the
//! optional live dashboard. Both are consumed by the control loop in
//! `orchestration::app`; frame-bearing read paths live in `frame_collector`.

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use telemetry::frame::{Frame, StreamId};
use telemetry::{
    ChannelContent, ChannelId, Lifetime, NodeId, Record, StreamDescriptor, StreamOrigin,
    TelemetryEndpoint, TelemetryProducer,
};

use crate::observability::benchmark;
use crate::observability::frame_archive::FrameArchive;
use crate::observability::frame_collector::ingest_dashboard_frame;
use crate::observability::telemetry::{
    MYELIN_PROVISIONING_EVENTS, MyelinProvisionEventRecord, MyelinProvisionLogRecord,
    myelin_provision_log_channel,
};
#[cfg(feature = "dashboard")]
use crate::orchestration::app::env_optional;
use crate::provisioning::{ProvisionEvent, ProvisionLogLine};
use distribution::telemetry::{MembershipTransition, SwimProbeEvent};
use swactor::stats::StatsHook;
use swactor_engine::EngineHandle;

pub(crate) const MYELIN_ORCH_BOOTSTRAP: &str = "myelin.orch.bootstrap";
pub(crate) const MYELIN_ORCH_PROMPT: &str = "myelin.orch.prompt";
pub(crate) const MYELIN_SWIM_MEMBERSHIP: &str = "myelin.swim.membership";
pub(crate) const MYELIN_STAGE_ROUTE: &str = "myelin.orch.stage_route";

pub(crate) struct OrchTelemetry {
    endpoint: TelemetryEndpoint,
    producer: TelemetryProducer,
    channels: BTreeMap<String, ChannelId>,
    channel_names: BTreeMap<ChannelId, String>,
    archive: Option<FrameArchive>,
    descriptor: StreamDescriptor,
}

pub(crate) struct BootstrapEmission<'a> {
    pub(crate) dashboard: Option<&'a DashboardSupport>,
    pub(crate) channel: &'a str,
    pub(crate) run_id: u64,
    pub(crate) node_id: u64,
    pub(crate) phase: &'a str,
    pub(crate) status: &'a str,
    pub(crate) detail: Value,
}
impl OrchTelemetry {
    pub(crate) fn new(run_id: u64, frame_log: Option<&Path>) -> Result<Self, String> {
        let stream = StreamId::new(NodeId::new("myelin-orchestrator"), Lifetime(run_id));
        let endpoint = TelemetryEndpoint::with_descriptor(
            StreamDescriptor {
                stream: stream.clone(),
                label: Some("myelin orchestrator".to_owned()),
                origin: StreamOrigin::Orchestrator,
            },
            4096,
            1024,
        );
        let producer = endpoint.producer();
        let descriptor = StreamDescriptor {
            stream: stream.clone(),
            label: Some("myelin orchestrator".to_owned()),
            origin: StreamOrigin::Orchestrator,
        };
        let mut out = Self {
            endpoint,
            producer,
            channels: BTreeMap::new(),
            channel_names: BTreeMap::new(),
            archive: frame_log
                .map(|p| FrameArchive::open_with_label(p, "telemetry frame log"))
                .transpose()?,
            descriptor,
        };
        for name in [
            MYELIN_PROVISIONING_EVENTS,
            MYELIN_ORCH_BOOTSTRAP,
            MYELIN_ORCH_PROMPT,
            MYELIN_SWIM_MEMBERSHIP,
            MYELIN_STAGE_ROUTE,
        ] {
            out.channel_by_name(name);
        }
        out.record_channel::<MembershipTransition>();
        out.record_channel::<SwimProbeEvent>();
        Ok(out)
    }

    pub(crate) fn channel_by_name(&mut self, name: &str) -> ChannelId {
        if let Some(id) = self.channels.get(name).copied() {
            return id;
        }
        let id = self.producer.register_channel(
            name,
            ChannelContent::JsonRecord {
                schema: Some(name.to_owned()),
            },
        );
        self.channels.insert(name.to_owned(), id);
        self.channel_names.insert(id, name.to_owned());
        id
    }

    pub(crate) fn record_channel<R: Record>(&mut self) -> ChannelId {
        if let Some(id) = self.channels.get(R::CHANNEL).copied() {
            return id;
        }
        let id = self.producer.register_record::<R>();
        self.channels.insert(R::CHANNEL.to_owned(), id);
        self.channel_names.insert(id, R::CHANNEL.to_owned());
        id
    }

    pub(crate) fn producer(&self) -> TelemetryProducer {
        self.producer.clone()
    }

    pub(crate) fn emit_event(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        event: ProvisionEvent,
    ) {
        let payload = serde_json::to_vec(&MyelinProvisionEventRecord::new(event))
            .expect("serialize provisioning event");
        self.emit_bytes(dashboard, MYELIN_PROVISIONING_EVENTS, payload);
    }

    pub(crate) fn emit_log(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        line: ProvisionLogLine,
    ) {
        let channel = myelin_provision_log_channel(line.node_id, line.stream);
        let payload = serde_json::to_vec(&MyelinProvisionLogRecord::new(line))
            .expect("serialize provision log");
        self.emit_bytes(dashboard, &channel, payload);
    }

    pub(crate) fn emit_bootstrap(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        run_id: u64,
        node_id: u64,
        phase: &str,
        status: &str,
        detail: Value,
    ) {
        self.emit_bootstrap_to_channel(BootstrapEmission {
            dashboard,
            channel: MYELIN_ORCH_BOOTSTRAP,
            run_id,
            node_id,
            phase,
            status,
            detail,
        });
    }

    pub(crate) fn emit_bootstrap_to_channel(&mut self, emission: BootstrapEmission<'_>) {
        let BootstrapEmission {
            dashboard,
            channel,
            run_id,
            node_id,
            phase,
            status,
            detail,
        } = emission;
        let benchmark = benchmark::stamp("myelin-orchestrator");
        let payload = serde_json::to_vec(&json!({
            "schema_version": benchmark["schema_version"].clone(),
            "type":"OrchBootstrap",
            "event_type":"OrchBootstrap",
            "event_name":phase,
            "phase":phase,
            "status":status,
            "run_id":run_id,
            "node_id":node_id,
            "producer_component":benchmark["producer_component"].clone(),
            "producer_instance_id":benchmark["producer_instance_id"].clone(),
            "producer_process_id":benchmark["producer_process_id"].clone(),
            "producer_sequence":benchmark["producer_sequence"].clone(),
            "wall_clock_unix_ms":benchmark["wall_clock_unix_ms"].clone(),
            "monotonic_ms":benchmark["monotonic_ms"].clone(),
            "clock_source":benchmark["clock_source"].clone(),
            "span_id":format!("myelin-orchestrator:{run_id}:{}:{phase}", benchmark["producer_sequence"]),
            "parent_span_id":Value::Null,
            "benchmark":benchmark,
            "detail":detail,
        }))
        .expect("serialize orch bootstrap event");
        self.emit_bytes(dashboard, channel, payload);
    }

    pub(crate) fn emit_record<R: Record>(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        record: &R,
    ) {
        let id = self.record_channel::<R>();
        self.producer.submit_record(id, record);
        self.flush(dashboard, "orchestrator");
    }

    pub(crate) fn emit_bytes(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: &str,
        payload: Vec<u8>,
    ) {
        self.emit_bytes_from(dashboard, channel, payload, "orchestrator");
    }

    pub(crate) fn emit_bytes_from(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: &str,
        payload: Vec<u8>,
        source: &str,
    ) {
        let id = self.channel_by_name(channel);
        self.producer.submit_bytes(id, payload);
        self.flush(dashboard, source);
    }

    pub(crate) fn flush(&mut self, dashboard: Option<&DashboardSupport>, source: &str) {
        let stream = self.descriptor.stream.clone();
        for frame in self.endpoint.mux().drain() {
            let channel = self
                .channel_names
                .get(&frame.channel)
                .cloned()
                .unwrap_or_else(|| format!("channel#{}", frame.channel.0));
            ingest_dashboard_frame(dashboard, &stream, &channel, &frame, Some(&self.descriptor));
            self.archive_frame(source, &stream, &channel, &frame);
        }
    }

    pub(crate) fn archive_frame(
        &mut self,
        source: &str,
        stream: &StreamId,
        channel: &str,
        frame: &Frame,
    ) {
        if let Some(archive) = &mut self.archive {
            let _ = archive.record(source, stream, channel, frame);
        }
    }

    /// Attach the telemetry stats hook on `channel` so actor snapshots flow to it.
    pub(crate) fn stats_hook_on(&self, channel: ChannelId) -> Arc<dyn StatsHook> {
        self.producer.stats_hook_on(channel)
    }
}

#[cfg(feature = "dashboard")]
pub(crate) struct DashboardSupport {
    handle: dashboard::DashboardHandle,
}

#[cfg(feature = "dashboard")]
impl DashboardSupport {
    fn config() -> Result<dashboard::DashboardConfig, String> {
        let mut config = dashboard::DashboardConfig::default();
        if let Some(port) = env_optional("MYELIN_DASHBOARD_PORT") {
            config.port = port
                .parse::<u16>()
                .map_err(|e| format!("invalid MYELIN_DASHBOARD_PORT={port:?}: {e}"))?;
        }
        Ok(config)
    }

    pub(crate) fn configured_url(enabled: bool) -> Result<Option<String>, String> {
        enabled
            .then(|| Self::config().map(|config| format!("http://127.0.0.1:{}/", config.port)))
            .transpose()
    }

    pub(crate) fn start_with_plugins(
        enabled: bool,
        engine: &EngineHandle,
        plugins: Vec<dashboard::DashboardPlugin>,
    ) -> Result<Option<Self>, String> {
        if !enabled {
            return Ok(None);
        }
        let mut config = Self::config()?;
        config
            .page_script_urls
            .push(crate::orchestration::control::FLEET_CONTROL_SCRIPT_URL.to_owned());
        let handle = dashboard::DashboardHandle::new(config);
        handle.spawn_with_plugins(engine, plugins);
        Ok(Some(Self { handle }))
    }

    /// Publish a frame, classifying the stream by its descriptor's origin so
    /// the fleet view can privilege the orchestrator card.
    pub(crate) fn publish_frame(
        &self,
        stream: &StreamId,
        descriptor: Option<&StreamDescriptor>,
        channel: &str,
        frame: &Frame,
    ) {
        let origin = descriptor.map(|descriptor| {
            match descriptor.origin {
                StreamOrigin::Orchestrator => "orchestrator",
                StreamOrigin::Bootstrap => "bootstrap",
                StreamOrigin::RemoteNode => "remote-node",
            }
            .to_owned()
        });
        self.handle.publish(dashboard::FrameEvent {
            stream: dashboard::StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
                origin,
                label: descriptor.and_then(|descriptor| descriptor.label.clone()),
            },
            channel: channel.to_owned(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        });
    }
}

#[cfg(not(feature = "dashboard"))]
pub(crate) struct DashboardSupport;

#[cfg(not(feature = "dashboard"))]
impl DashboardSupport {
    pub(crate) fn configured_url(enabled: bool) -> Result<Option<String>, String> {
        if enabled {
            return Err(
                "MYELIN_DASHBOARD requires building myelin-system with feature dashboard"
                    .to_owned(),
            );
        }
        Ok(None)
    }

    pub(crate) fn start_with_plugins(
        enabled: bool,
        _engine: &EngineHandle,
        _plugins: Vec<dashboard::DashboardPlugin>,
    ) -> Result<Option<Self>, String> {
        if enabled {
            return Err(
                "MYELIN_DASHBOARD requires building myelin-system with feature dashboard"
                    .to_owned(),
            );
        }
        Ok(None)
    }

    pub(crate) fn publish_frame(
        &self,
        _stream: &StreamId,
        _descriptor: Option<&StreamDescriptor>,
        _channel: &str,
        _frame: &Frame,
    ) {
    }
}
