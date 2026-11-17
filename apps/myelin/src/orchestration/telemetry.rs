//! Orchestration-owned typed telemetry emission and dashboard composition.

use crate::orchestration::app::env_optional;
use crate::orchestration::provisioning_telemetry::{
    MYELIN_PROVISIONING_EVENTS, MyelinProvisionEventRecord, MyelinProvisionLogRecord,
    myelin_provision_log_channel,
};
use crate::provisioning::{ProvisionEvent, ProvisionLogLine, ProvisionLogStream};
use distribution::telemetry::TransportMembershipTransition;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use swactor::stats::StatsHook;
use swactor_engine::{EngineHandle, TaskSpawner};
use telemetry::{
    ChannelContent, ChannelId, Lifetime, NodeId, Record, StreamDescriptor, StreamId, StreamOrigin,
    TelemetryEndpoint, TelemetryProducer,
};

const MYELIN_ORCHESTRATOR_LOGS: &str = "myelin.orchestrator.logs";
pub(crate) const MYELIN_ORCH_BOOTSTRAP: &str = "myelin.orch.bootstrap";
pub(crate) const MYELIN_ORCH_PROMPT: &str = "myelin.orch.prompt";
pub(crate) const MYELIN_MEMBERSHIP: &str = "myelin.membership";
pub(crate) const MYELIN_STAGE_ROUTE: &str = "myelin.orch.stage_route";

pub(crate) struct OrchTelemetry {
    endpoint: Arc<TelemetryEndpoint>,
    producer: TelemetryProducer,
    channels: BTreeMap<String, ChannelId>,
}

pub(crate) struct BootstrapEmission<'a> {
    pub(crate) channel: &'a str,
    pub(crate) run_id: u64,
    pub(crate) node_id: u64,
    pub(crate) phase: &'a str,
    pub(crate) status: &'a str,
    pub(crate) detail: Value,
}

#[derive(Serialize)]
struct BootstrapRecord<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    phase: &'a str,
    status: &'a str,
    run_id: u64,
    node_id: u64,
    detail: Value,
}

impl OrchTelemetry {
    pub(crate) fn new(run_id: u64, state_dir: &Path) -> Result<Self, String> {
        let lifetime = crate::node::telemetry::allocate_telemetry_lifetime(
            &state_dir.join("telemetry"),
            0,
            run_id,
        )?;
        let endpoint = Arc::new(
            TelemetryEndpoint::with_descriptor(
                StreamDescriptor {
                    stream: StreamId::new(NodeId::new("myelin-orchestrator"), Lifetime(lifetime)),
                    label: Some("myelin orchestrator".to_owned()),
                    origin: StreamOrigin::Orchestrator,
                },
                16_384,
                16_384,
            )
            .with_retention(16_384, 16 * 1024 * 1024),
        );
        let mut out = Self {
            producer: endpoint.producer(),
            endpoint,
            channels: BTreeMap::new(),
        };
        for name in [
            MYELIN_PROVISIONING_EVENTS,
            MYELIN_ORCH_BOOTSTRAP,
            MYELIN_ORCH_PROMPT,
            MYELIN_MEMBERSHIP,
            MYELIN_STAGE_ROUTE,
        ] {
            out.channel_by_name(name);
        }
        out.record_channel::<TransportMembershipTransition>();
        Ok(out)
    }

    pub(crate) fn serve(&mut self, tasks: &TaskSpawner, driver: iroh_driver::IrohDriver) {
        let endpoint = Arc::clone(&self.endpoint);
        let mut ingress = driver.ingress_changed();
        let connection_tasks = tasks.clone();
        tasks.spawn(async move {
            loop {
                for (_, connection) in driver.drain_accepted_for_alpn(iroh_driver::TELEMETRY_ALPN) {
                    iroh_driver::spawn_pull_server(
                        &connection_tasks,
                        connection,
                        Arc::clone(&endpoint),
                    );
                }
                if ingress.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    pub(crate) fn channel_by_name(&mut self, name: &str) -> ChannelId {
        if let Some(id) = self.channels.get(name) {
            return *id;
        }
        let id = self.producer.register_channel(
            name,
            ChannelContent::MessagePackRecord {
                schema: Some(name.to_owned()),
            },
        );
        self.channels.insert(name.to_owned(), id);
        id
    }

    pub(crate) fn record_channel<R: Record>(&mut self) -> ChannelId {
        if let Some(id) = self.channels.get(R::CHANNEL) {
            return *id;
        }
        let id = self.producer.register_record::<R>();
        self.channels.insert(R::CHANNEL.to_owned(), id);
        id
    }

    pub(crate) fn producer(&self) -> TelemetryProducer {
        self.producer.clone()
    }

    pub(crate) fn endpoint(&self) -> Arc<TelemetryEndpoint> {
        Arc::clone(&self.endpoint)
    }

    pub(crate) fn emit_event(&mut self, event: ProvisionEvent) {
        self.emit_bytes(
            MYELIN_PROVISIONING_EVENTS,
            MyelinProvisionEventRecord::new(event).encode(),
        );
    }

    pub(crate) fn emit_log(&mut self, line: ProvisionLogLine) {
        let channel = myelin_provision_log_channel(line.node_id, line.stream);
        self.emit_bytes(&channel, MyelinProvisionLogRecord::new(line).encode());
    }

    pub(crate) fn emit_orchestrator_log(&mut self, stream: ProvisionLogStream, line: String) {
        #[derive(Serialize)]
        struct LogRecord {
            source: String,
            line: String,
        }
        let record = LogRecord {
            source: format!("{stream:?}").to_ascii_lowercase(),
            line,
        };
        self.emit_bytes(
            MYELIN_ORCHESTRATOR_LOGS,
            telemetry::encode_record(&record).expect("serialize orchestrator log"),
        );
    }

    pub(crate) fn emit_bootstrap(
        &mut self,
        run_id: u64,
        node_id: u64,
        phase: &str,
        status: &str,
        detail: Value,
    ) {
        self.emit_bootstrap_to_channel(BootstrapEmission {
            channel: MYELIN_ORCH_BOOTSTRAP,
            run_id,
            node_id,
            phase,
            status,
            detail,
        });
    }

    pub(crate) fn emit_bootstrap_to_channel(&mut self, emission: BootstrapEmission<'_>) {
        let record = BootstrapRecord {
            kind: "OrchBootstrap",
            phase: emission.phase,
            status: emission.status,
            run_id: emission.run_id,
            node_id: emission.node_id,
            detail: emission.detail,
        };
        self.emit_bytes(
            emission.channel,
            telemetry::encode_record(&record).expect("serialize orchestration event"),
        );
    }

    pub(crate) fn emit_record<R: Record>(&mut self, record: &R) {
        let id = self.record_channel::<R>();
        self.producer.submit_record(id, record);
        self.flush();
    }

    pub(crate) fn emit_bytes(&mut self, channel: &str, payload: Vec<u8>) {
        let id = self.channel_by_name(channel);
        self.producer.submit_bytes(id, payload);
        self.flush();
    }

    pub(crate) fn flush(&self) {
        self.endpoint.tick();
    }

    pub(crate) fn stats_hook_on(&self, channel: ChannelId) -> Arc<dyn StatsHook> {
        self.producer.stats_hook_on(channel)
    }
}

pub(crate) struct DashboardSupport {
    remote: dashboard::RemoteTelemetry,
}

impl DashboardSupport {
    fn config() -> Result<dashboard::DashboardConfig, String> {
        let mut config = dashboard::DashboardConfig::default();
        if let Some(port) = env_optional("MYELIN_DASHBOARD_PORT") {
            config.port = port
                .parse()
                .map_err(|e| format!("invalid MYELIN_DASHBOARD_PORT={port:?}: {e}"))?;
        }
        Ok(config)
    }

    pub(crate) fn configured_url(enabled: bool) -> Result<Option<String>, String> {
        if enabled && !cfg!(feature = "dashboard") {
            return Err("MYELIN_DASHBOARD requires feature dashboard".to_owned());
        }
        enabled
            .then(|| Self::config().map(|config| format!("http://127.0.0.1:{}/", config.port)))
            .transpose()
    }

    pub(crate) fn start_with_plugins(
        enabled: bool,
        tasks: &TaskSpawner,
        engine: &EngineHandle,
        endpoint: iroh::Endpoint,
        plugins: Vec<dashboard::DashboardPlugin>,
    ) -> Result<Option<Self>, String> {
        Self::configured_url(enabled)?;
        if !enabled {
            return Ok(None);
        }
        let mut config = Self::config()?;
        config
            .page_script_urls
            .push(crate::orchestration::control::FLEET_CONTROL_SCRIPT_URL.to_owned());
        let handle = dashboard::DashboardHandle::new(config);
        handle.spawn_with_plugins(tasks, plugins);
        futures_lite::future::block_on(handle.wait_until_ready())
            .map_err(|error| format!("start dashboard HTTP receiver: {error}"))?;
        Ok(Some(Self {
            remote: dashboard::RemoteTelemetry::new(tasks, engine, endpoint, handle),
        }))
    }

    pub(crate) fn observe_local(&self, endpoint: Arc<TelemetryEndpoint>) {
        self.remote.observe_local(&endpoint);
    }

    pub(crate) fn subscribe_node(&self, peer: iroh::EndpointAddr, run_id: u64, node_id: u64) {
        self.remote.subscribe(peer, run_id, node_id);
    }

    pub(crate) fn unsubscribe_node(&self, run_id: u64, node_id: u64) {
        self.remote.unsubscribe(run_id, node_id);
    }
}
