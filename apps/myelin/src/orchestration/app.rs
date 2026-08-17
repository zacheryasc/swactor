use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use crate::DEFAULT_PIPELINE_CACHED_MODEL_FILE;
use crate::codecs::register_myelin_actor_codecs;
use crate::node_actor::NodeAgentMsg;
use crate::observability::frame_collector::FrameCollector;
use crate::observability::orch_telemetry::{
    DashboardSupport, MYELIN_SWIM_MEMBERSHIP, OrchTelemetry,
};
use crate::orchestration::actor::{OrchestratorActor, OrchestratorReport};
use crate::orchestration::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
use crate::orchestration::daemon;
use dashboard::control::ControlCommand;

use crate::node_provisioning::{ProviderKind, provider_kind};
use crate::orchestration::cluster_reconciler::{ProvisionedClusterGuard, ReconcilerNodeBinding};
use crate::orchestration::distribution_stack::{DistributionRuntimeStack, duration_ms_u64};
use crate::orchestration::provider_adapters::relay::{
    MYELIN_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV,
    relay_mode_env_value, relay_runtime_config_from_settings,
};
use crate::orchestration::provider_adapters::vastai::{
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin,
};
use crate::provisioning::{
    LocalDockerPlugin, LocalProcessPlugin, NodeProvisionSpec, PluginObservation,
    PluginObservationSink, PluginSink, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use crate::run_fsm::{RunConfig, RunId};
use crate::run_plan::{self, GgufSource, TokenizerSource};
use ::provisioning::{
    BootSpec, ClusterShape, DesiredNodeShape, LogicalNodeId as ReconcilerLogicalNodeId,
    NodeGroupId, ProviderKind as ReconcilerProviderKind, RetryPolicy, RoleId,
    RunId as ClusterRunId, RunNodeGroupSpec, SwarmJoinTemplate,
};
use distribution::node::DistributedNodeConfig;
use distribution::swim::telemetry::ObservedTransition;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::EndpointAddr;
use iroh_driver::{EDGE_ALPN, IrohDriver, IrohDriverConfig, TELEMETRY_ALPN};
use iroh_driver::{EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;
use swactor_engine::{Engine, EngineHandle, TokioBackend, TokioConfig};
const DEFAULT_IMAGE: &str = "myelin-node:latest";
const MYELIN_RUNTIME_CONFIG_ENV: &str = "MYELIN_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MYELIN_CACHED_MODEL_HOST_PATH";
const MYELIN_WORKER_BIN_ENV: &str = "MYELIN_WORKER_BIN";
const CACHED_MODEL_CONTAINER_DIR: &str = "/models/cached";
const DEFAULT_PIPELINE_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_STATE_DIR: &str = "./.config";
const DEFAULT_MAX_TOKENS: u32 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const RUNTIME_READY_ACK_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const STAGE_PROVISION_ACTIVE_RESEND_AFTER: Duration = Duration::from_secs(60);
const PIPELINE_PROMPT_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(15);
const TELEMETRY_FRAME_LOG_ENV: &str = "MYELIN_TELEMETRY_FRAME_LOG";

pub(crate) fn run_with_options<I>(
    args: I,
    capture_stdio: bool,
    stop_rx: Option<mpsc::Receiver<()>>,
) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let mut config_builder = ConfigBuilder::hardcoded_defaults();
    if let Some(overlay) = TomlConfigOverlay::load_optional(Path::new(DEFAULT_CONFIG_PATH))? {
        config_builder = config_builder.overlay_toml(overlay)?;
    }
    let mut config = config_builder
        .overlay_env()?
        .overlay_cli(args)?
        .finalize()?;
    config.prepare_vastai_ssh_key()?;
    let state_dir = daemon::StateDir::new(config.state_dir.clone());
    if config.reset_state {
        state_dir.reset()?;
    }
    let identity = state_dir.load_or_create_identity()?;
    let mut snapshot = state_dir.load_snapshot()?;
    if snapshot.run_id == 0 {
        snapshot = daemon::ClusterSnapshot::fresh(config.run_id, daemon_label(&config));
    } else if snapshot.run_id != config.run_id {
        // Node container names and provider labels derive from run_id; the
        // persisted run owns them so adoption addresses the same resources.
        config.run_id = snapshot.run_id;
    }
    if let Some(url) = DashboardSupport::configured_url(config.dashboard)? {
        println!("Myelin dashboard: {url}");
        std::io::stdout()
            .flush()
            .map_err(|error| format!("flush dashboard URL to stdout: {error}"))?;
    }
    let orch_stdio_rx = if capture_stdio {
        OrchStdioCapture::install()?
    } else {
        None
    };
    let mut orch_telemetry =
        OrchTelemetry::new(config.run_id, config.telemetry_frame_log.as_deref())?;
    let run_id = config.run_id;
    let node_id = config.node_id;
    let bootstrap = |ds: &mut OrchTelemetry,
                     dash: Option<&DashboardSupport>,
                     phase: &str,
                     status: &str,
                     detail: Value| {
        ds.emit_bootstrap(dash, run_id, node_id, phase, status, detail);
    };
    bootstrap(
        &mut orch_telemetry,
        None,
        "config",
        "ready",
        json!({
            "config_profile":match config.config_profile {
                RuntimeConfigProfile::Local => "local",
                RuntimeConfigProfile::Deploy => "deploy",
            },
            "image":&config.image,
            "provider":config.provider.as_str(),
            "model_id":&config.model_id,
            "stage_index":config.stage_index,
            "legacy_layer_end_exclusive":config.layer_end_exclusive,
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "provider_config":config.provider_telemetry_detail(),
        }),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "telemetry_preflight",
        "configured",
        json!({
            "producer":"myelin-orchestrator",
            "telemetry_endpoint":{
                "role":"orchestrator-frame-archive",
                "transport":"telemetry-frame-log",
                "configured":config.telemetry_frame_log.is_some(),
                "archive_path":config.telemetry_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
            },
            "expected_worker_producers":["myelin-worker","tinygrad-worker"],
            "provider":config.provider.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "provider_config":config.provider_telemetry_detail(),
        }),
    );
    let orch_synthetic_id = format!("myelin-orchestrator-{}-telemetry-preflight", config.run_id);
    for (phase, status) in [
        ("TelemetryProducerConfigured", "configured"),
        ("TelemetryProducerConnected", "ready"),
        ("TelemetrySyntheticEventSent", "sent"),
        ("TelemetrySyntheticEventObserved", "observed"),
    ] {
        bootstrap(
            &mut orch_telemetry,
            None,
            phase,
            status,
            json!({
                "producer":"myelin-orchestrator",
                "producer_class":"rust-orchestrator",
                "synthetic_id":orch_synthetic_id,
                "telemetry_endpoint":{
                    "role":"orchestrator-frame-archive",
                    "transport":"telemetry-frame-log",
                    "configured":config.telemetry_frame_log.is_some(),
                    "archive_path":config.telemetry_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
                },
            }),
        );
    }
    drain_orch_stdio_capture(
        orch_stdio_rx.as_ref(),
        &mut orch_telemetry,
        None,
        config.run_id,
        config.node_id,
    );
    let actors_channel = orch_telemetry.channel_by_name("runtime.actors");
    let orch_stats_hook = orch_telemetry.stats_hook_on(actors_channel);

    // Build the core swactor runtime parts, clone the routing handle needed by
    // integrations, then hand the workers to the engine. The engine owns both
    // core progression and the Tokio substrate (it schedules all background
    // work); components retain only cheap Runtime handles (ENGINE_SPEC.md).
    let (parts, runtime, codec, transport_router) = DistributionRuntimeStack::build_runtime(
        |registry| {
            register_myelin_actor_codecs(registry);
            telemetry::wire::register_telemetry_codec(registry);
        },
        Some(orch_stats_hook),
    );
    let engine = match TokioBackend::new(TokioConfig::default())
        .and_then(|backend| Engine::new(parts, backend))
    {
        Ok(engine) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "engine",
                "ready",
                json!({"backend":"tokio","owns":"core+substrate"}),
            );
            engine
        }
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "engine",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("create engine: {error}"));
        }
    };
    let mut driver = match IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: Some(identity),
            relay_mode: config.relay.mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec(), TELEMETRY_ALPN.to_vec()],
        },
    ) {
        Ok(driver) => driver,
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "iroh_driver",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("create iroh driver: {error}"));
        }
    };
    let coordinator_endpoint =
        advertised_endpoint(driver.endpoint_addr(), config.endpoint_addr_mask)?;
    bootstrap(
        &mut orch_telemetry,
        None,
        "iroh_driver",
        "ready",
        json!({"endpoint":coordinator_endpoint.clone(),"has_relay":coordinator_endpoint.relay_urls().next().is_some(),"direct_addr_count":coordinator_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay.mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "endpoint_config_snapshot",
        "ready",
        json!({
            "producer":"myelin-orchestrator",
            "coordinator_endpoint":coordinator_endpoint.clone(),
            "has_relay":coordinator_endpoint.relay_urls().next().is_some(),
            "direct_addr_count":coordinator_endpoint.ip_addrs().count(),
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "connectivity_preflight":"ready",
        }),
    );
    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime,
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "distribution_stack",
        "ready",
        json!({"actors":"initialized","route_view":"initialized","swim":"initialized"}),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "codecs",
        "ready",
        json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","telemetry"]}),
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
        stack.outbox.clone(),
    );
    // Engine owns protocol tick injection and core progression; the application
    // loop only drains integration-owned queues (ENGINE_SPEC.md).
    stack.spawn_protocol_ticker(PUMP_INTERVAL);
    driver.install_actor_bridge_pump(PUMP_INTERVAL);
    bootstrap(
        &mut orch_telemetry,
        None,
        "actor_bridge",
        "ready",
        json!({"transport":"iroh","routes":"attached","protocol_ticker":"engine-hosted"}),
    );

    let collector = FrameCollector::new();
    bootstrap(
        &mut orch_telemetry,
        None,
        "telemetry_collector",
        "ready",
        json!({"alpn":String::from_utf8_lossy(TELEMETRY_ALPN)}),
    );
    let dashboard = DashboardSupport::start(config.dashboard, &engine.handle())?;
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "dashboard",
        "ready",
        json!({"enabled":dashboard.is_some()}),
    );

    let orchestrator_reports = match stack.runtime.new_inbox::<OrchestratorReport>() {
        Ok(inbox) => inbox,
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                dashboard.as_ref(),
                "orchestrator_report_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("orchestrator report inbox: {error}"));
        }
    };
    let orchestrator_report_actor = *orchestrator_reports.addr();
    stack.register_local_actor(driver.register_actor(orchestrator_report_actor, 1));
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "orchestrator_report_actor",
        "ready",
        json!({"actor":orchestrator_report_actor}),
    );
    let orchestrator_actor = match stack.runtime.spawn(OrchestratorActor::new(
        RunConfig {
            run_id: RunId(config.run_id),
            max_tokens: u64::from(config.default_max_tokens),
            prompt: Vec::new(),
        },
        Some(orchestrator_report_actor),
    )) {
        Ok(actor) => actor,
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                dashboard.as_ref(),
                "orchestrator_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("spawn orchestrator actor: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(orchestrator_actor, 1));
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "orchestrator_actor",
        "ready",
        json!({"actor":orchestrator_actor}),
    );

    let stop_rx = stop_rx.unwrap_or_else(spawn_stop_listener);

    let provisioner = config.build_provisioner(stack.runtime.clone())?;
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "node_provisioner",
        "ready",
        json!({
            "provider":config.provider.as_str(),
            "owner":"myelin-orchestrator",
            "config":config.provider_telemetry_detail(),
        }),
    );
    let (obs_tx, obs_rx) = mpsc::channel::<PluginObservation>();
    let sink = PluginSink::new(Arc::new(ChannelObservationSink {
        tx: Mutex::new(obs_tx),
    }));

    let (control_tx, control_rx) = mpsc::channel::<ControlCommand>();
    dashboard::control::set_control_sender(control_tx);
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "fleet_control",
        "started",
        json!({
            "mode":"manual",
            "commands":["add","kill","destroy"],
            "transport":"dashboard",
        }),
    );

    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "serve_cluster",
        "started",
        json!({"mode":"daemon","poll_interval_ms":PUMP_INTERVAL.as_millis()}),
    );
    let result = serve_cluster(ServeCluster {
        driver: &mut driver,
        stack: &stack,
        obs_rx: &obs_rx,
        collector: &collector,
        orchestrator_reports: &orchestrator_reports,
        stop_rx: &stop_rx,
        dashboard: dashboard.as_ref(),
        orch_telemetry: &mut orch_telemetry,
        orch_stdio_rx: orch_stdio_rx.as_ref(),
        run_id: config.run_id,
        orchestrator_node_id: config.node_id,
        provider: &config.provider,
        orchestrator_actor,
        coordinator_endpoint,
        engine: engine.handle(),
        runtime: stack.runtime.clone(),
        config: config.clone(),
        provisioner,
        live_clusters: BTreeMap::new(),
        sink,
        state_dir,
        snapshot,
        control_rx: &control_rx,
        destroy_on_exit: config.destroy_on_exit,
    });
    if let Err(error) = &result {
        bootstrap(
            &mut orch_telemetry,
            dashboard.as_ref(),
            "serve_cluster",
            "failed",
            json!({"error":error}),
        );
    }
    result
}

#[derive(Clone)]
struct VastAiRuntimeConfig {
    api_key: Option<String>,
    provisioning: VastAiProvisioningConfig,
    bootstrap_command: Option<String>,
    ssh_identity: Option<PathBuf>,
    ssh_public_fingerprint: Option<String>,
}

impl VastAiRuntimeConfig {
    fn from_builder(builder: &ConfigBuilder) -> Result<Self, String> {
        let mut provisioning = VastAiProvisioningConfig::default();
        macro_rules! raw_config {
            ($parser:ident, $env:literal, $raw:expr, $value:expr) => {
                $raw.as_ref()
                    .map(|value| ConfigBuilder::$parser($env, value))
                    .transpose()?
                    .or($value)
            };
        }
        if let Some(disk_gb) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_DISK_GB",
            builder.vastai_disk_gb_raw,
            builder.vastai_disk_gb
        ) {
            provisioning.disk_gb = disk_gb;
        }
        if let Some(ssh_user) = &builder.vastai_ssh_user {
            provisioning.ssh_user = ssh_user.clone();
        }
        if let Some(confirm_lease) = raw_config!(
            parse_bool,
            "MYELIN_VASTAI_CONFIRM_LEASE",
            builder.vastai_confirm_lease_raw,
            builder.vastai_confirm_lease
        ) {
            provisioning.confirm_lease = confirm_lease;
        }
        provisioning.onstart = builder.vastai_onstart.clone();
        provisioning.selection.gpu_name = builder.vastai_gpu_name.clone();
        if let Some(min_gpu_ram_mb) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_GPU_RAM_MB",
            builder.vastai_min_gpu_ram_mb_raw,
            builder.vastai_min_gpu_ram_mb
        ) {
            provisioning.selection.min_gpu_ram_mb = Some(min_gpu_ram_mb);
        }
        if let Some(min_down_mbps) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_DOWN_MBPS",
            builder.vastai_min_down_mbps_raw,
            builder.vastai_min_down_mbps
        ) {
            provisioning.selection.min_down_mbps = min_down_mbps;
        }
        if let Some(max_dph_total) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MAX_DPH_TOTAL",
            builder.vastai_max_dph_total_raw,
            builder.vastai_max_dph_total
        ) {
            provisioning.selection.max_dph_total = Some(max_dph_total);
        }
        if let Some(min_up_mbps) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_UP_MBPS",
            builder.vastai_min_up_mbps_raw,
            builder.vastai_min_up_mbps
        ) {
            provisioning.selection.min_up_mbps = Some(min_up_mbps);
        }
        if let Some(min_reliability) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_RELIABILITY",
            builder.vastai_min_reliability_raw,
            builder.vastai_min_reliability
        ) {
            provisioning.selection.min_reliability = min_reliability;
        }
        if let Some(require_verified) = raw_config!(
            parse_bool,
            "MYELIN_VASTAI_REQUIRE_VERIFIED",
            builder.vastai_require_verified_raw,
            builder.vastai_require_verified
        ) {
            provisioning.selection.require_verified = require_verified;
        }
        for host_id in &builder.vastai_blacklist_hosts {
            if !provisioning.selection.blacklist_hosts.contains(host_id) {
                provisioning.selection.blacklist_hosts.push(*host_id);
            }
        }
        if let Some(poll_interval_secs) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_POLL_INTERVAL_SECS",
            builder.vastai_poll_interval_secs_raw,
            builder.vastai_poll_interval_secs
        ) {
            provisioning.lifecycle.poll_interval = Duration::from_secs(poll_interval_secs);
        }
        let ssh_identity = builder
            .vastai_ssh_identity_raw
            .as_ref()
            .map(|value| expand_home_path(value))
            .transpose()?;
        Ok(Self {
            api_key: builder.vastai_api_key.clone(),
            provisioning,
            bootstrap_command: builder.vastai_bootstrap_command.clone(),
            ssh_identity,
            ssh_public_fingerprint: None,
        })
    }

    fn telemetry_detail(&self) -> Value {
        json!({
            "disk_gb": self.provisioning.disk_gb,
            "ssh_user": &self.provisioning.ssh_user,
            "gpu_name": &self.provisioning.selection.gpu_name,
            "min_gpu_ram_mb": self.provisioning.selection.min_gpu_ram_mb,
            "min_compute_cap": self.provisioning.selection.min_compute_cap,
            "min_down_mbps": self.provisioning.selection.min_down_mbps,
            "min_up_mbps": self.provisioning.selection.min_up_mbps,
            "max_dph_total": self.provisioning.selection.max_dph_total,
            "min_reliability": self.provisioning.selection.min_reliability,
            "require_verified": self.provisioning.selection.require_verified,
            "blacklist_hosts": &self.provisioning.selection.blacklist_hosts,
            "state_timeout_secs": self.provisioning.lifecycle.state_timeout.as_secs(),
            "confirm_lease": self.provisioning.confirm_lease,
            "has_api_key": self.api_key.is_some(),
            "has_onstart": self.provisioning.onstart.is_some(),
            "has_bootstrap_command": self.bootstrap_command.is_some(),
            "has_ssh_identity": self.ssh_identity.is_some(),
            "ssh_public_fingerprint": self.ssh_public_fingerprint.as_deref(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeConfigProfile {
    Local,
    Deploy,
}

impl RuntimeConfigProfile {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "deploy" => Ok(Self::Deploy),
            other => Err(format!(
                "unsupported {MYELIN_RUNTIME_CONFIG_ENV}={other:?}; use local or deploy"
            )),
        }
    }
}

#[derive(Clone)]
struct CachedModelConfig {
    host_path: PathBuf,
    container_path: String,
}

impl CachedModelConfig {
    fn from_host_path(provider: &str, requested: PathBuf) -> Result<Self, String> {
        if !matches!(provider, "process" | "docker" | "vastai") {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} is a host-local cache path and is only supported by provider=process, provider=docker, or vastai planning"
            ));
        }
        let host_path = requested.canonicalize().map_err(|e| {
            format!(
                "resolve {CACHED_MODEL_HOST_ENV} path {}: {e}",
                requested.display()
            )
        })?;
        if !host_path.is_file() {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} must point at a file: {}",
                host_path.display()
            ));
        }
        let file_name = host_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                format!(
                    "cached model path has no file name: {}",
                    host_path.display()
                )
            })?;
        let container_path = format!("{CACHED_MODEL_CONTAINER_DIR}/{file_name}");
        Ok(Self {
            host_path,
            container_path,
        })
    }

    fn telemetry_detail(&self) -> Value {
        json!({
            "host_path_present": true,
            "file": self.host_path.file_name().and_then(|name| name.to_str()),
            "container_path": &self.container_path,
        })
    }
}

fn default_pipeline_cached_model_path() -> PathBuf {
    let relative = PathBuf::from(".")
        .join(DEFAULT_PIPELINE_MODEL_CACHE_DIR)
        .join(DEFAULT_PIPELINE_CACHED_MODEL_FILE);
    if relative.is_file() {
        return relative;
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(DEFAULT_PIPELINE_MODEL_CACHE_DIR)
        .join(DEFAULT_PIPELINE_CACHED_MODEL_FILE)
}

#[derive(Clone)]
struct Config {
    config_profile: RuntimeConfigProfile,
    image: String,
    docker_gpus: String,
    provider: ProviderKind,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: Option<u32>,
    pipeline_stages: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    state_dir: PathBuf,
    destroy_on_exit: bool,
    reset_state: bool,
    max_context: Option<u32>,
    relay: RelayRuntimeConfig,
    endpoint_addr_mask: EndpointAddrMask,
    vastai: Option<VastAiRuntimeConfig>,
    cached_model: Option<CachedModelConfig>,
    telemetry_frame_log: Option<PathBuf>,
    worker_bin: Option<PathBuf>,
}

#[derive(Clone)]
struct ConfigBuilder {
    config_profile: RuntimeConfigProfile,
    provider: Option<ProviderKind>,
    image: String,
    toml_vastai_image: Option<String>,
    image_overridden_after_toml: bool,
    docker_gpus: String,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: Option<u32>,
    pipeline_stages: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    state_dir: Option<PathBuf>,
    destroy_on_exit: bool,
    reset_state: bool,
    max_context: Option<u32>,
    relay_mode: Option<String>,
    relay_url: Option<String>,
    endpoint_addr_mask: Option<String>,
    vastai_api_key: Option<String>,
    vastai_bootstrap_command: Option<String>,
    vastai_disk_gb: Option<u32>,
    vastai_disk_gb_raw: Option<String>,
    vastai_ssh_user: Option<String>,
    vastai_confirm_lease: Option<bool>,
    vastai_confirm_lease_raw: Option<String>,
    vastai_onstart: Option<String>,
    vastai_ssh_identity_raw: Option<String>,
    vastai_gpu_name: Option<String>,
    vastai_min_gpu_ram_mb: Option<u64>,
    vastai_min_gpu_ram_mb_raw: Option<String>,
    vastai_min_down_mbps: Option<f64>,
    vastai_min_down_mbps_raw: Option<String>,
    vastai_min_up_mbps: Option<f64>,
    vastai_max_dph_total: Option<f64>,
    vastai_max_dph_total_raw: Option<String>,
    vastai_min_up_mbps_raw: Option<String>,
    vastai_min_reliability: Option<f64>,
    vastai_min_reliability_raw: Option<String>,
    vastai_require_verified: Option<bool>,
    vastai_require_verified_raw: Option<String>,
    vastai_poll_interval_secs: Option<u64>,
    vastai_blacklist_hosts: Vec<u64>,
    vastai_poll_interval_secs_raw: Option<String>,
    cached_model_host_path: Option<PathBuf>,
    telemetry_frame_log: Option<PathBuf>,
    worker_bin: Option<PathBuf>,
}

impl ConfigBuilder {
    fn hardcoded_defaults() -> Self {
        Self {
            config_profile: RuntimeConfigProfile::Local,
            provider: None,
            image: DEFAULT_IMAGE.to_owned(),
            toml_vastai_image: None,
            image_overridden_after_toml: false,
            docker_gpus: "all".to_owned(),
            run_id: 1,
            node_id: 1,
            stage_index: 0,
            layer_end_exclusive: None,
            pipeline_stages: 1,
            model_id: DEFAULT_MODEL_ID.to_owned(),
            gguf_source: GgufSource::HuggingFaceGguf {
                repo: DEFAULT_HF_REPO.to_owned(),
                file: DEFAULT_HF_FILE.to_owned(),
                revision: None,
            },
            tokenizer: TokenizerSource::EmbeddedGguf,
            default_max_tokens: DEFAULT_MAX_TOKENS,
            dashboard: true,
            state_dir: None,
            destroy_on_exit: false,
            reset_state: false,
            max_context: None,
            relay_mode: None,
            relay_url: None,
            endpoint_addr_mask: None,
            vastai_api_key: None,
            vastai_bootstrap_command: None,
            vastai_disk_gb: None,
            vastai_disk_gb_raw: None,
            vastai_ssh_user: None,
            vastai_confirm_lease: None,
            vastai_confirm_lease_raw: None,
            vastai_onstart: None,
            vastai_ssh_identity_raw: None,
            vastai_gpu_name: None,
            vastai_min_gpu_ram_mb: None,
            vastai_min_gpu_ram_mb_raw: None,
            vastai_min_down_mbps: None,
            vastai_min_down_mbps_raw: None,
            vastai_max_dph_total: None,
            vastai_max_dph_total_raw: None,
            vastai_min_up_mbps: None,
            vastai_min_up_mbps_raw: None,
            vastai_min_reliability: None,
            vastai_min_reliability_raw: None,
            vastai_require_verified: None,
            vastai_require_verified_raw: None,
            vastai_poll_interval_secs: None,
            vastai_blacklist_hosts: Vec::new(),
            vastai_poll_interval_secs_raw: None,
            cached_model_host_path: None,
            telemetry_frame_log: None,
            worker_bin: None,
        }
    }

    fn overlay_toml(mut self, overlay: TomlConfigOverlay) -> Result<Self, String> {
        macro_rules! apply {
            ($option:expr, |$value:ident| $body:block) => {
                if let Some($value) = $option $body
            };
        }

        apply!(overlay.runtime.profile, |profile| {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        });
        apply!(overlay.runtime.run_id, |run_id| { self.run_id = run_id });
        apply!(overlay.runtime.node_id, |node_id| {
            self.node_id = node_id
        });
        apply!(overlay.runtime.stage_index, |stage_index| {
            self.stage_index = stage_index
        });
        apply!(overlay.runtime.layer_end_exclusive, |layer_end_exclusive| {
            self.layer_end_exclusive = Some(layer_end_exclusive)
        });
        apply!(overlay.runtime.pipeline_stages, |pipeline_stages| {
            self.pipeline_stages = pipeline_stages
        });
        apply!(overlay.provider.kind, |provider| {
            self.provider = Some(provider_kind::parse_deploy(&provider)?);
        });
        apply!(overlay.image.node, |image| { self.image = image });
        apply!(overlay.relay.mode, |mode| { self.relay_mode = Some(mode) });
        apply!(overlay.relay.url, |url| { self.relay_url = Some(url) });
        apply!(overlay.prompt.max_tokens, |max_tokens| {
            self.default_max_tokens = max_tokens
        });
        apply!(overlay.prompt.dashboard, |dashboard| {
            self.dashboard = dashboard
        });
        apply!(overlay.model.id, |model_id| { self.model_id = model_id });
        apply!(overlay.model.gguf_local_path, |path| {
            self.gguf_source = GgufSource::LocalPath(path)
        });
        apply!(overlay.model.gguf_repo, |repo| {
            self.set_hf_source(Some(repo), None, None)
        });
        apply!(overlay.model.gguf_file, |file| {
            self.set_hf_source(None, Some(file), None)
        });
        apply!(overlay.model.gguf_revision, |revision| {
            self.set_hf_source(None, None, Some(Some(revision)))
        });
        apply!(overlay.model.tokenizer_local_path, |path| {
            self.tokenizer = TokenizerSource::LocalPath(path)
        });
        apply!(overlay.model.max_context, |max_context| {
            self.max_context = Some(max_context)
        });
        apply!(overlay.docker.gpus, |gpus| { self.docker_gpus = gpus });
        apply!(overlay.docker.cached_model_host_path, |path| {
            self.cached_model_host_path = Some(PathBuf::from(path))
        });
        apply!(overlay.observability.telemetry_frame_log, |path| {
            self.telemetry_frame_log = Some(PathBuf::from(path))
        });
        apply!(overlay.vastai.image, |image| {
            self.toml_vastai_image = Some(image)
        });
        apply!(overlay.vastai.api_key, |api_key| {
            self.vastai_api_key = Some(api_key)
        });
        apply!(overlay.vastai.bootstrap_command, |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        apply!(overlay.vastai.disk_gb, |disk_gb| {
            self.vastai_disk_gb = Some(disk_gb)
        });
        apply!(overlay.vastai.ssh_user, |ssh_user| {
            self.vastai_ssh_user = Some(ssh_user)
        });
        apply!(overlay.vastai.confirm_lease, |confirm_lease| {
            self.vastai_confirm_lease = Some(confirm_lease)
        });
        apply!(overlay.vastai.onstart, |onstart| {
            self.vastai_onstart = Some(onstart)
        });
        apply!(overlay.vastai.ssh_identity, |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });
        apply!(overlay.vastai.gpu_name, |gpu_name| {
            self.vastai_gpu_name = Some(gpu_name)
        });
        apply!(overlay.vastai.min_gpu_ram_mb, |min_gpu_ram_mb| {
            self.vastai_min_gpu_ram_mb = Some(min_gpu_ram_mb)
        });
        apply!(overlay.vastai.min_down_mbps, |min_down_mbps| {
            self.vastai_min_down_mbps = Some(min_down_mbps)
        });
        apply!(overlay.vastai.max_dph_total, |max_dph_total| {
            self.vastai_max_dph_total = Some(max_dph_total)
        });
        apply!(overlay.vastai.min_up_mbps, |min_up_mbps| {
            self.vastai_min_up_mbps = Some(min_up_mbps)
        });
        apply!(overlay.vastai.min_reliability, |min_reliability| {
            self.vastai_min_reliability = Some(min_reliability)
        });
        apply!(overlay.vastai.require_verified, |require_verified| {
            self.vastai_require_verified = Some(require_verified)
        });
        for host_id in overlay.vastai.blacklist_hosts {
            self.push_vastai_blacklist_host(host_id);
        }
        apply!(overlay.vastai.poll_interval_secs, |poll_interval_secs| {
            self.vastai_poll_interval_secs = Some(poll_interval_secs)
        });
        Ok(self)
    }

    fn overlay_env(mut self) -> Result<Self, String> {
        macro_rules! apply {
            ($option:expr, |$value:ident| $body:block) => {
                if let Some($value) = $option $body
            };
        }
        macro_rules! env_apply {
            ($name:expr, |$value:ident| $body:block) => {
                apply!(env_optional($name), |$value| $body)
            };
        }
        macro_rules! env_parse {
            ($name:expr, |$value:ident| $body:block) => {
                env_apply!($name, |raw| {
                    let $value = Self::parse_value($name, &raw)?;
                    $body
                })
            };
        }

        env_apply!(MYELIN_RUNTIME_CONFIG_ENV, |profile| {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        });
        env_parse!("MYELIN_RUN_ID", |run_id| { self.run_id = run_id });
        env_parse!("MYELIN_LOGICAL_NODE_ID", |node_id| {
            self.node_id = node_id
        });
        env_parse!("MYELIN_STAGE_INDEX", |stage_index| {
            self.stage_index = stage_index
        });
        env_parse!("MYELIN_LAYER_END_EXCLUSIVE", |layer_end_exclusive| {
            self.layer_end_exclusive = Some(layer_end_exclusive)
        });
        env_parse!("MYELIN_PIPELINE_STAGES", |pipeline_stages| {
            self.pipeline_stages = pipeline_stages
        });
        apply!(
            env_optional("MYELIN_NODE_PROVIDER").or_else(|| env_optional("MYELIN_PROVIDER")),
            |provider| {
                self.provider = Some(provider_kind::parse_deploy(&provider)?);
            }
        );
        env_apply!("MYELIN_NODE_IMAGE", |image| {
            self.image = image;
            self.image_overridden_after_toml = true;
        });
        env_apply!("MYELIN_DOCKER_GPUS", |gpus| { self.docker_gpus = gpus });
        env_apply!(CACHED_MODEL_HOST_ENV, |path| {
            self.cached_model_host_path = Some(PathBuf::from(path))
        });
        env_apply!(MYELIN_WORKER_BIN_ENV, |path| {
            self.worker_bin = Some(PathBuf::from(path))
        });
        env_parse!("MYELIN_PROMPT_MAX_TOKENS", |max_tokens| {
            self.default_max_tokens = max_tokens
        });
        env_apply!("MYELIN_DASHBOARD", |dashboard| {
            self.dashboard = Self::parse_bool("MYELIN_DASHBOARD", &dashboard)?;
        });
        env_apply!("MYELIN_STATE_DIR", |state_dir| {
            self.state_dir = Some(PathBuf::from(state_dir))
        });
        env_apply!("MYELIN_DESTROY_ON_EXIT", |destroy_on_exit| {
            self.destroy_on_exit = Self::parse_bool("MYELIN_DESTROY_ON_EXIT", &destroy_on_exit)?;
        });

        env_apply!(TELEMETRY_FRAME_LOG_ENV, |path| {
            self.telemetry_frame_log = Some(PathBuf::from(path))
        });
        env_apply!("MYELIN_MODEL_ID", |model_id| { self.model_id = model_id });
        env_apply!("MYELIN_GGUF_LOCAL_PATH", |path| {
            self.gguf_source = GgufSource::LocalPath(path)
        });
        env_apply!("MYELIN_GGUF_REPO", |repo| {
            self.set_hf_source(Some(repo), None, None)
        });
        env_apply!("MYELIN_GGUF_FILE", |file| {
            self.set_hf_source(None, Some(file), None)
        });
        env_apply!("MYELIN_GGUF_REVISION", |revision| {
            self.set_hf_source(None, None, Some(Some(revision)))
        });
        env_apply!("MYELIN_TOKENIZER_LOCAL_PATH", |path| {
            self.tokenizer = TokenizerSource::LocalPath(path)
        });
        env_parse!("MYELIN_MAX_CONTEXT", |max_context| {
            self.max_context = Some(max_context)
        });
        env_apply!("MYELIN_IROH_RELAY_MODE", |mode| {
            self.relay_mode = Some(mode.to_ascii_lowercase())
        });
        apply!(
            env_optional(MYELIN_IROH_RELAY_URL_ENV)
                .or_else(|| env_optional(SWACTOR_IROH_RELAY_URL_ENV)),
            |url| {
                self.relay_url = Some(url);
            }
        );
        env_apply!(MVP_IROH_ENDPOINT_ADDR_MASK_ENV, |mask| {
            self.endpoint_addr_mask = Some(mask)
        });
        apply!(
            env_optional("VAST_API_KEY")
                .or_else(|| env_optional("MYELIN_VASTAI_API_KEY"))
                .or_else(|| env_optional("VASTAI_API_KEY")),
            |api_key| {
                self.vastai_api_key = Some(api_key);
            }
        );
        env_apply!("MYELIN_VASTAI_BOOTSTRAP_COMMAND", |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        env_apply!("MYELIN_VASTAI_SSH_IDENTITY", |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });
        env_apply!("MYELIN_VASTAI_DISK_GB", |disk_gb| {
            self.vastai_disk_gb_raw = Some(disk_gb)
        });
        env_apply!("MYELIN_VASTAI_SSH_USER", |ssh_user| {
            self.vastai_ssh_user = Some(ssh_user)
        });
        env_apply!("MYELIN_VASTAI_CONFIRM_LEASE", |confirm_lease| {
            self.vastai_confirm_lease_raw = Some(confirm_lease)
        });
        env_apply!("MYELIN_VASTAI_ONSTART", |onstart| {
            self.vastai_onstart = Some(onstart)
        });
        env_apply!("MYELIN_VASTAI_GPU_NAME", |gpu_name| {
            self.vastai_gpu_name = Some(gpu_name)
        });
        env_apply!("MYELIN_VASTAI_MIN_GPU_RAM_MB", |min_gpu_ram_mb| {
            self.vastai_min_gpu_ram_mb_raw = Some(min_gpu_ram_mb)
        });
        env_apply!("MYELIN_VASTAI_MIN_DOWN_MBPS", |min_down_mbps| {
            self.vastai_min_down_mbps_raw = Some(min_down_mbps)
        });
        env_apply!("MYELIN_VASTAI_MAX_DPH_TOTAL", |max_dph_total| {
            self.vastai_max_dph_total_raw = Some(max_dph_total)
        });
        env_apply!("MYELIN_VASTAI_MIN_UP_MBPS", |min_up_mbps| {
            self.vastai_min_up_mbps_raw = Some(min_up_mbps)
        });
        env_apply!("MYELIN_VASTAI_MIN_RELIABILITY", |min_reliability| {
            self.vastai_min_reliability_raw = Some(min_reliability)
        });
        env_apply!("MYELIN_VASTAI_REQUIRE_VERIFIED", |require_verified| {
            self.vastai_require_verified_raw = Some(require_verified)
        });
        env_apply!("MYELIN_VASTAI_BLACKLIST_HOSTS", |blacklist_hosts| {
            for host_id in Self::parse_list("MYELIN_VASTAI_BLACKLIST_HOSTS", &blacklist_hosts)? {
                self.push_vastai_blacklist_host(host_id);
            }
        });
        env_apply!("MYELIN_VASTAI_POLL_INTERVAL_SECS", |poll_interval_secs| {
            self.vastai_poll_interval_secs_raw = Some(poll_interval_secs)
        });
        Ok(self)
    }

    fn overlay_cli(mut self, args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--runtime-config" => {
                    self.config_profile =
                        RuntimeConfigProfile::parse(&next_arg(&mut args, "--runtime-config")?)?
                }
                "--provider" => {
                    self.provider = Some(provider_kind::parse_deploy(&next_arg(
                        &mut args,
                        "--provider",
                    )?)?)
                }
                "--worker-bin" => {
                    self.worker_bin = Some(PathBuf::from(next_arg(&mut args, "--worker-bin")?))
                }
                "--image" => {
                    self.image = next_arg(&mut args, "--image")?;
                    self.image_overridden_after_toml = true;
                }
                "--gpus" => self.docker_gpus = next_arg(&mut args, "--gpus")?,
                "--run-id" => self.run_id = parse_next(&mut args, "--run-id")?,
                "--node-id" => self.node_id = parse_next(&mut args, "--node-id")?,
                "--stage-index" => self.stage_index = parse_next(&mut args, "--stage-index")?,
                "--layer-end-exclusive" => {
                    self.layer_end_exclusive = Some(parse_next(&mut args, "--layer-end-exclusive")?)
                }
                "-N" | "--pipeline-stages" => self.pipeline_stages = parse_next(&mut args, &arg)?,
                "--max-tokens" => self.default_max_tokens = parse_next(&mut args, "--max-tokens")?,
                "--dashboard" => self.dashboard = true,
                "--no-dashboard" => self.dashboard = false,
                "--state-dir" => {
                    self.state_dir = Some(PathBuf::from(next_arg(&mut args, "--state-dir")?))
                }
                "--destroy-on-exit" => self.destroy_on_exit = true,
                "--reset-state" => self.reset_state = true,
                "--telemetry-frame-log" => {
                    self.telemetry_frame_log =
                        Some(PathBuf::from(next_arg(&mut args, "--telemetry-frame-log")?));
                }
                "--model-id" => self.model_id = next_arg(&mut args, "--model-id")?,
                "--gguf-local-path" => {
                    self.gguf_source =
                        GgufSource::LocalPath(next_arg(&mut args, "--gguf-local-path")?)
                }
                "--gguf-repo" => {
                    self.set_hf_source(Some(next_arg(&mut args, "--gguf-repo")?), None, None)
                }
                "--gguf-file" => {
                    self.set_hf_source(None, Some(next_arg(&mut args, "--gguf-file")?), None)
                }
                "--gguf-revision" => self.set_hf_source(
                    None,
                    None,
                    Some(Some(next_arg(&mut args, "--gguf-revision")?)),
                ),
                "--tokenizer-local-path" => {
                    self.tokenizer =
                        TokenizerSource::LocalPath(next_arg(&mut args, "--tokenizer-local-path")?)
                }
                "--max-context" => self.max_context = Some(parse_next(&mut args, "--max-context")?),
                "--cached-model-host-path" => {
                    self.cached_model_host_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--cached-model-host-path",
                    )?));
                }
                "--relay-mode" => self.relay_mode = Some(next_arg(&mut args, "--relay-mode")?),
                "--relay-url" => self.relay_url = Some(next_arg(&mut args, "--relay-url")?),
                "--endpoint-addr-mask" => {
                    self.endpoint_addr_mask = Some(next_arg(&mut args, "--endpoint-addr-mask")?)
                }
                "--vastai-disk-gb" => {
                    self.vastai_disk_gb = Some(parse_next(&mut args, "--vastai-disk-gb")?);
                    self.vastai_disk_gb_raw = None;
                }
                "--vastai-min-gpu-ram-mb" => {
                    self.vastai_min_gpu_ram_mb =
                        Some(parse_next(&mut args, "--vastai-min-gpu-ram-mb")?);
                    self.vastai_min_gpu_ram_mb_raw = None;
                }
                "--vastai-min-down-mbps" => {
                    self.vastai_min_down_mbps =
                        Some(parse_next(&mut args, "--vastai-min-down-mbps")?);
                    self.vastai_min_down_mbps_raw = None;
                }
                "--vastai-max-dph-total" => {
                    self.vastai_max_dph_total =
                        Some(parse_next(&mut args, "--vastai-max-dph-total")?);
                    self.vastai_max_dph_total_raw = None;
                }
                "--vastai-min-up-mbps" => {
                    self.vastai_min_up_mbps = Some(parse_next(&mut args, "--vastai-min-up-mbps")?);
                    self.vastai_min_up_mbps_raw = None;
                }
                "--vastai-min-reliability" => {
                    self.vastai_min_reliability =
                        Some(parse_next(&mut args, "--vastai-min-reliability")?);
                    self.vastai_min_reliability_raw = None;
                }
                "--vastai-blacklist-host" => {
                    let host_id = parse_next(&mut args, "--vastai-blacklist-host")?;
                    self.push_vastai_blacklist_host(host_id);
                }
                "--vastai-poll-interval-secs" => {
                    self.vastai_poll_interval_secs =
                        Some(parse_next(&mut args, "--vastai-poll-interval-secs")?);
                    self.vastai_poll_interval_secs_raw = None;
                }
                "--vastai-api-key" => {
                    self.vastai_api_key = Some(next_arg(&mut args, "--vastai-api-key")?)
                }
                "--vastai-bootstrap-command" => {
                    self.vastai_bootstrap_command =
                        Some(next_arg(&mut args, "--vastai-bootstrap-command")?)
                }
                "--vastai-ssh-identity" => {
                    self.vastai_ssh_identity_raw =
                        Some(next_arg(&mut args, "--vastai-ssh-identity")?);
                }
                "--vastai-ssh-user" => {
                    self.vastai_ssh_user = Some(next_arg(&mut args, "--vastai-ssh-user")?)
                }
                "--vastai-onstart" => {
                    self.vastai_onstart = Some(next_arg(&mut args, "--vastai-onstart")?)
                }
                "--vastai-gpu-name" => {
                    self.vastai_gpu_name = Some(next_arg(&mut args, "--vastai-gpu-name")?)
                }
                "--vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(true);
                    self.vastai_confirm_lease_raw = None;
                }
                "--no-vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(false);
                    self.vastai_confirm_lease_raw = None;
                }
                "--vastai-require-verified" => {
                    self.vastai_require_verified = Some(true);
                    self.vastai_require_verified_raw = None;
                }
                "--no-vastai-require-verified" => {
                    self.vastai_require_verified = Some(false);
                    self.vastai_require_verified_raw = None;
                }
                _ => return Err(format!("unknown argument {arg:?}")),
            }
        }
        Ok(self)
    }

    fn finalize(self) -> Result<Config, String> {
        let provider = self
            .provider
            .clone()
            .unwrap_or_else(|| match self.config_profile {
                RuntimeConfigProfile::Local => provider_kind::process(),
                RuntimeConfigProfile::Deploy => provider_kind::vastai(),
            });
        let provider_name = provider.as_str();
        let mut image = self.image.clone();
        if provider_name == "vastai" && !self.image_overridden_after_toml {
            if let Some(vastai_image) = &self.toml_vastai_image {
                image = vastai_image.clone();
            }
        }
        if self.pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        let mut cached_model_host_path = self.cached_model_host_path.clone();
        if matches!(provider_name, "process" | "docker")
            && self.pipeline_stages > 1
            && cached_model_host_path.is_none()
            && (matches!(
                &self.gguf_source,
                GgufSource::HuggingFaceGguf {
                    repo,
                    file,
                    revision: None,
                } if repo == DEFAULT_HF_REPO && file == DEFAULT_HF_FILE
            ) || matches!(
                &self.gguf_source,
                GgufSource::HuggingFaceGguf {
                    file,
                    revision: None,
                    ..
                } if file == DEFAULT_PIPELINE_CACHED_MODEL_FILE
            ))
        {
            cached_model_host_path = Some(default_pipeline_cached_model_path());
        }
        let cached_model = cached_model_host_path
            .map(|path| CachedModelConfig::from_host_path(provider_name, path))
            .transpose()?;
        let mut gguf_source = self.gguf_source.clone();
        if let Some(cached_model) = &cached_model {
            if provider_name != "vastai" {
                gguf_source = GgufSource::LocalPath(if provider_name == "process" {
                    cached_model.host_path.to_string_lossy().to_string()
                } else {
                    cached_model.container_path.clone()
                });
            }
        }
        let relay = relay_runtime_config_from_settings(
            self.run_id,
            self.relay_mode.as_deref(),
            self.relay_url.as_deref(),
        )?;
        let endpoint_addr_mask = match self.endpoint_addr_mask.as_deref() {
            Some(mask) => EndpointAddrMask::parse(mask)?,
            None => EndpointAddrMask::Full,
        };
        let vastai = (provider_name == "vastai")
            .then(|| VastAiRuntimeConfig::from_builder(&self))
            .transpose()?;
        Ok(Config {
            config_profile: self.config_profile,
            image,
            docker_gpus: self.docker_gpus,
            provider,
            run_id: self.run_id,
            node_id: self.node_id,
            stage_index: self.stage_index,
            layer_end_exclusive: self.layer_end_exclusive,
            pipeline_stages: self.pipeline_stages,
            model_id: self.model_id,
            gguf_source,
            tokenizer: self.tokenizer,
            default_max_tokens: self.default_max_tokens,
            dashboard: self.dashboard,
            state_dir: self
                .state_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR)),
            destroy_on_exit: self.destroy_on_exit,
            reset_state: self.reset_state,
            max_context: self.max_context,
            relay,
            endpoint_addr_mask,
            vastai,
            cached_model,
            worker_bin: self.worker_bin,
            telemetry_frame_log: self.telemetry_frame_log,
        })
    }

    fn set_hf_source(
        &mut self,
        repo: Option<String>,
        file: Option<String>,
        revision: Option<Option<String>>,
    ) {
        let (current_repo, current_file, current_revision) = match &self.gguf_source {
            GgufSource::HuggingFaceGguf {
                repo,
                file,
                revision,
            } => (repo.clone(), file.clone(), revision.clone()),
            GgufSource::LocalPath(_) => {
                (DEFAULT_HF_REPO.to_owned(), DEFAULT_HF_FILE.to_owned(), None)
            }
        };
        self.gguf_source = GgufSource::HuggingFaceGguf {
            repo: repo.unwrap_or(current_repo),
            file: file.unwrap_or(current_file),
            revision: revision.unwrap_or(current_revision),
        };
    }

    fn push_vastai_blacklist_host(&mut self, host_id: u64) {
        if !self.vastai_blacklist_hosts.contains(&host_id) {
            self.vastai_blacklist_hosts.push(host_id);
        }
    }

    fn parse_list<T>(name: &str, value: &str) -> Result<Vec<T>, String>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| Self::parse_value(name, part))
            .collect()
    }

    fn parse_value<T>(name: &str, value: &str) -> Result<T, String>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        value
            .parse::<T>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}"))
    }

    fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
        match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
            )),
        }
    }
}

impl Config {
    fn uses_planned_execution(&self) -> bool {
        self.cached_model.is_some() || self.pipeline_stages > 1
    }

    fn provider_telemetry_detail(&self) -> Value {
        match self.provider.as_str() {
            "process" => json!({
                "worker_bin": self.worker_bin.as_ref().map(|path| path.to_string_lossy().to_string()),
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::telemetry_detail),
            }),
            "docker" => json!({
                "docker_gpus": &self.docker_gpus,
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::telemetry_detail),
            }),
            "vastai" => self
                .vastai
                .as_ref()
                .map_or_else(|| json!({}), VastAiRuntimeConfig::telemetry_detail),
            _ => json!({}),
        }
    }

    fn build_run_plan(&self) -> Result<run_plan::RunPlan, String> {
        let host_path = self.local_planning_gguf_path()?;
        let metadata = crate::staging::gguf_metadata::read_gguf_planning_metadata(&host_path)?;
        let model = metadata.to_model_facts(
            self.model_id.clone(),
            self.gguf_source.clone(),
            self.tokenizer.clone(),
            self.max_context,
        )?;
        if self.pipeline_stages > model.num_layers {
            return Err(format!(
                "--pipeline-stages={} exceeds GGUF layer count {}; choose N <= {}",
                self.pipeline_stages, model.num_layers, model.num_layers
            ));
        }

        let activation_extent = model
            .max_seq_len
            .checked_mul(model.hidden_dim)
            .and_then(|value| value.checked_mul(model.dtype_width_bytes))
            .ok_or_else(|| "activation ring size overflow while planning pipeline".to_owned())?;
        let activation_data_capacity = run_plan::MO01_HEADER_BYTES
            .checked_add(activation_extent)
            .ok_or_else(|| {
            "activation ring data capacity overflow while planning pipeline".to_owned()
        })?;
        let token_extent = model
            .max_seq_len
            .checked_mul(4)
            .ok_or_else(|| "token ring size overflow while planning pipeline".to_owned())?;
        let token_data_capacity = run_plan::MO01_HEADER_BYTES
            .checked_add(token_extent)
            .ok_or_else(|| {
                "token ring data capacity overflow while planning pipeline".to_owned()
            })?;
        let candidate_pool = (0..self.pipeline_stages)
            .map(|stage_index| run_plan::NodeId(self.node_id + 1 + u64::from(stage_index)))
            .collect::<Vec<_>>();
        let placement = run_plan::PlacementInput::FixedLinear(
            candidate_pool
                .iter()
                .enumerate()
                .map(|(stage_index, node_id)| run_plan::StagePlacement {
                    stage_index: stage_index as u32,
                    node_id: *node_id,
                })
                .collect(),
        );

        run_plan::plan_run(run_plan::PlannerInput {
            run_id: run_plan::RunId(self.run_id),
            orchestrator_node_id: run_plan::NodeId(self.node_id),
            model,
            runtime: run_plan::RuntimeConfig {
                max_tokens: self.default_max_tokens,
                sampling: run_plan::SamplingPolicy {
                    temperature_millis: 0,
                    top_k: 1,
                },
            },
            candidate_pool,
            stage_count: self.pipeline_stages,
            placement,
            activation_ring: run_plan::RingSpec {
                data_capacity: activation_data_capacity,
                alignment: 64,
                direction: run_plan::RingDirection::Egress,
                host_pinning: run_plan::HostPinning::Pageable,
                wake_coalescing: run_plan::WakeCoalescing::PendingBit,
            },
            token_ring: run_plan::RingSpec {
                data_capacity: token_data_capacity,
                alignment: 8,
                direction: run_plan::RingDirection::Egress,
                host_pinning: run_plan::HostPinning::Pageable,
                wake_coalescing: run_plan::WakeCoalescing::PendingBit,
            },
        })
        .map_err(|e| format!("plan pipeline run: {:?}", e.kind()))
    }

    fn local_planning_gguf_path(&self) -> Result<PathBuf, String> {
        if let Some(cached_model) = &self.cached_model {
            return Ok(cached_model.host_path.clone());
        }
        match &self.gguf_source {
            GgufSource::LocalPath(path) => {
                let host_path = PathBuf::from(path);
                if host_path.is_file() {
                    Ok(host_path)
                } else {
                    Err(format!(
                        "local pipeline requires a locally inspectable GGUF before provisioning; {path:?} is not a host file, so use --cached-model-host-path"
                    ))
                }
            }
            GgufSource::HuggingFaceGguf {
                repo,
                file,
                revision: None,
            } if self.provider.as_str() == "vastai"
                && file == DEFAULT_PIPELINE_CACHED_MODEL_FILE =>
            {
                let host_path = default_pipeline_cached_model_path();
                if host_path.is_file() {
                    Ok(host_path)
                } else {
                    Err(format!(
                        "VastAI pipeline planning requires local GGUF metadata at {}; selected remote source is {repo}/{file}",
                        host_path.display()
                    ))
                }
            }
            GgufSource::HuggingFaceGguf { repo, file, .. } => Err(format!(
                "local pipeline requires a locally inspectable GGUF before provisioning; selected source {repo}/{file} is remote, so use --cached-model-host-path"
            )),
        }
    }

    fn prepare_vastai_ssh_key(&mut self) -> Result<(), String> {
        if self.provider.as_str() != "vastai" {
            return Ok(());
        }

        let api_key = self
            .vastai
            .as_ref()
            .and_then(|vastai| vastai.api_key.as_deref())
            .ok_or_else(|| {
                "VAST_API_KEY, MYELIN_VASTAI_API_KEY, or VASTAI_API_KEY is required when MYELIN_NODE_PROVIDER=vastai"
                    .to_owned()
            })?
            .to_owned();
        let identity = resolve_vastai_ssh_identity(
            self.vastai
                .as_ref()
                .and_then(|vastai| vastai.ssh_identity.clone()),
        )?;
        if !identity.is_file() {
            return Err(format!(
                "missing VastAI SSH identity {}; create/register one with vastai create ssh-key or set MYELIN_VASTAI_SSH_IDENTITY",
                identity.display()
            ));
        }
        let public_key = derive_ssh_public_key(&identity)?;
        let fingerprint = ssh_public_key_fingerprint(&public_key);
        ensure_vastai_account_ssh_key(&api_key, &public_key)?;

        eprintln!(
            "VastAI SSH identity {} fingerprint {} registered for account",
            identity.display(),
            fingerprint
        );

        let vastai = self
            .vastai
            .as_mut()
            .expect("VastAI config exists when provider is vastai");
        vastai.ssh_identity = Some(identity);
        vastai.provisioning.ssh_public_key = Some(public_key);
        vastai.ssh_public_fingerprint = Some(fingerprint);
        Ok(())
    }

    fn build_provisioner(
        &self,
        bootstrap_runtime: swactor::runtime::Runtime,
    ) -> Result<Box<dyn ProvisionPlugin>, String> {
        match self.provider.as_str() {
            "process" => {
                let worker_bin = match &self.worker_bin {
                    Some(worker_bin) => worker_bin.clone(),
                    None => std::env::current_exe().map_err(|e| format!("current exe: {e}"))?,
                };
                if !worker_bin.is_file() {
                    return Err(format!(
                        "local process worker binary does not exist: {}",
                        worker_bin.display()
                    ));
                }
                Ok(Box::new(LocalProcessPlugin::new(worker_bin)))
            }
            "docker" => Ok(Box::new(LocalDockerPlugin::new(
                env_optional("MYELIN_DOCKER_CONTAINER_PREFIX")
                    .unwrap_or_else(|| "myelin-orchestrator".to_owned()),
            ))),
            "vastai" => {
                let vastai = self.vastai.as_ref().ok_or_else(|| {
                    "VastAI config was not resolved for provider vastai".to_owned()
                })?;
                if vastai.bootstrap_command.is_none() {
                    return Err(
                        "MYELIN_VASTAI_BOOTSTRAP_COMMAND is required when MYELIN_NODE_PROVIDER=vastai"
                            .to_owned(),
                    );
                }
                let api_key = vastai.api_key.clone().ok_or_else(|| {
                    "VAST_API_KEY, MYELIN_VASTAI_API_KEY, or VASTAI_API_KEY is required when MYELIN_NODE_PROVIDER=vastai"
                        .to_owned()
                })?;
                let ssh_identity = vastai
                    .ssh_identity
                    .clone()
                    .ok_or_else(|| "VastAI SSH identity was not prepared".to_owned())?;
                Ok(Box::new(VastAiProvisioningPlugin::new(
                    ToolsVastAiLeaseClient::from_api_key(api_key)?,
                    SshCommandBootstrapLauncher::new(Some(ssh_identity), bootstrap_runtime),
                    vastai.provisioning.clone(),
                )))
            }
            _ => Err("mock provider cannot build a runtime provisioner".to_owned()),
        }
    }

    fn node_spec_env_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = vec![
            "MYELIN_RUN_ID",
            "MYELIN_LOGICAL_NODE_ID",
            "MYELIN_NODE_ATTEMPT_ID",
            "MYELIN_NODE_PROVIDER",
            "MYELIN_AGENT_ONLY",
            "MYELIN_STAGE_INDEX",
            "MYELIN_COORDINATOR_ENDPOINT",
            "MYELIN_ORCHESTRATOR_ACTOR",
            "MYELIN_IROH_RELAY_MODE",
            MVP_IROH_ENDPOINT_ADDR_MASK_ENV,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        keys.extend(self.extra_worker_env().into_iter().map(|(k, _)| k));
        keys
    }

    /// Provider-specific environment shared by filter and launch paths.
    fn extra_worker_env(&self) -> Vec<(String, String)> {
        let mut env = Vec::new();
        if let Some(url) = &self.relay.url {
            env.push((MYELIN_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
        }
        if self.provider.as_str() == "docker" {
            env.push(("MYELIN_DOCKER_GPUS".to_owned(), self.docker_gpus.clone()));
        }
        env
    }

    fn node_spec_for_stage(
        &self,
        coordinator: EndpointAddr,
        orchestrator_actor: ActorAddress,
        logical_node_id: u64,
        stage_index: u32,
    ) -> Result<NodeProvisionSpec, String> {
        let provider_name = self.provider.as_str();
        let mut env = vec![
            ("MYELIN_AGENT_ONLY".to_owned(), "1".to_owned()),
            ("MYELIN_RUN_ID".to_owned(), self.run_id.to_string()),
            (
                "MYELIN_LOGICAL_NODE_ID".to_owned(),
                logical_node_id.to_string(),
            ),
            ("MYELIN_STAGE_INDEX".to_owned(), stage_index.to_string()),
            (
                MVP_IROH_ENDPOINT_ADDR_MASK_ENV.to_owned(),
                self.endpoint_addr_mask.as_str().to_owned(),
            ),
            ("MYELIN_NODE_PROVIDER".to_owned(), provider_name.to_owned()),
            (
                "MYELIN_COORDINATOR_ENDPOINT".to_owned(),
                serde_json::to_string(&coordinator)
                    .map_err(|e| format!("serialize coordinator endpoint: {e}"))?,
            ),
            (
                "MYELIN_ORCHESTRATOR_ACTOR".to_owned(),
                serde_json::to_string(&orchestrator_actor)
                    .map_err(|e| format!("serialize orchestrator actor: {e}"))?,
            ),
            (
                "MYELIN_IROH_RELAY_MODE".to_owned(),
                relay_mode_env_value(&self.relay.mode).to_owned(),
            ),
        ];
        env.extend(self.extra_worker_env());
        let args = match provider_name {
            "vastai" => self
                .vastai
                .as_ref()
                .and_then(|vastai| vastai.bootstrap_command.clone())
                .into_iter()
                .collect(),
            "process" if self.worker_bin.is_none() => {
                vec![crate::ORCHESTRATOR_WORKER_MODE_ARG.to_owned()]
            }
            "process" | "docker" => Vec::new(),
            _ => return Err("myelin-orchestrator does not support mock provider".to_owned()),
        };
        Ok(NodeProvisionSpec {
            run_id: self.run_id,
            node_id: logical_node_id,
            attempt_id: 0,
            stage_index: Some(stage_index),
            image: self.image.clone(),
            env,
            args,
            mounts: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct RuntimeReady {
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    telemetry_publisher: ActorAddress,
    stage_index: u32,
    readiness_id: u64,
    swim_node_id: DistNodeId,
}

#[derive(Clone)]
struct RuntimeReadyAckTarget {
    node_id: u64,
    ready: RuntimeReady,
}

fn runtime_ready_barrier_met(stack: &DistributionRuntimeStack, ready: &RuntimeReady) -> bool {
    stack.member_state(ready.swim_node_id) == Some(MemberState::Alive)
        && stack.route_owner(ready.node_actor) == Some(ready.swim_node_id)
}

fn enqueue_runtime_ready_ack(
    stack: &DistributionRuntimeStack,
    ready: &RuntimeReady,
    run_id: u64,
    node_id: u64,
) -> Result<(), String> {
    stack
        .runtime
        .send_to(
            ready.node_actor,
            NodeAgentMsg::RuntimeReadyAck {
                run_id,
                node_id,
                stage_index: ready.stage_index,
                readiness_id: ready.readiness_id,
            },
        )
        .map_err(|e| format!("send runtime ready ack: {e}"))
}

struct RuntimeReadyAckLoop<'a> {
    driver: &'a mut IrohDriver,
    stack: &'a DistributionRuntimeStack,
    obs_rx: &'a mpsc::Receiver<PluginObservation>,
    collector: &'a FrameCollector,
    orchestrator_reports: &'a swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &'a mpsc::Receiver<()>,
    dashboard: Option<&'a DashboardSupport>,
    orch_telemetry: &'a mut OrchTelemetry,
    orch_stdio_rx: Option<&'a mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: &'a ProviderKind,
    orchestrator_actor: ActorAddress,
}

// synchronous process-control/orchestration sequencing; the engine drives all background work (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn wait_for_runtime_ready_acks(
    ctx: RuntimeReadyAckLoop<'_>,
    targets: &[RuntimeReadyAckTarget],
    cluster: &mut ProvisionedClusterGuard,
) -> Result<bool, String> {
    let RuntimeReadyAckLoop {
        driver,
        stack,
        obs_rx,
        collector,
        orchestrator_reports,
        stop_rx,
        dashboard,
        orch_telemetry,
        orch_stdio_rx,
        run_id,
        orchestrator_node_id,
        provider,
        ..
    } = ctx;
    let bootstrap = |ds: &mut OrchTelemetry, phase: &str, status: &str, detail: Value| {
        ds.emit_bootstrap(
            dashboard,
            run_id,
            orchestrator_node_id,
            phase,
            status,
            detail,
        );
    };
    let mut pending = targets
        .iter()
        .cloned()
        .map(|target| {
            (
                (
                    target.node_id,
                    target.ready.stage_index,
                    target.ready.readiness_id,
                ),
                target,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut attempts = BTreeMap::<(u64, u32, u64), u64>::new();
    let mut last_send = None::<Instant>;

    while !pending.is_empty() {
        cluster
            .poll(SystemTime::now())
            .map_err(|error| format!("cluster reconcile while awaiting ready ack: {error}"))?;
        if targets.iter().any(|target| {
            cluster.current_attempt(target.node_id)
                != Some(::provisioning::NodeAttemptId(target.ready.readiness_id))
        }) {
            return Ok(false);
        }
        collector.pump(driver);
        drain_orch_stdio_capture(
            orch_stdio_rx,
            orch_telemetry,
            dashboard,
            run_id,
            orchestrator_node_id,
        );
        if stop_requested(stop_rx) {
            return Err(
                "shutdown requested while waiting for runtime-ready acknowledgements".to_owned(),
            );
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_telemetry, dashboard, provider, &observation);
        }
        collector.drain(|stream, descriptor, channel, frame| {
            if let Some(d) = dashboard {
                d.publish_collected_frame(stream, descriptor, channel, frame);
            }
            orch_telemetry.archive_frame("node", stream, channel, frame);
        });
        while let Some(report) = orchestrator_reports.try_recv() {
            let OrchestratorReport::NodeRuntimeReadyAck {
                run_id: ack_run_id,
                node_id,
                stage_index,
                readiness_id,
            } = report
            else {
                continue;
            };
            if ack_run_id != run_id {
                continue;
            }
            let key = (node_id, stage_index, readiness_id);
            let Some(target) = pending.remove(&key) else {
                continue;
            };
            bootstrap(
                orch_telemetry,
                "runtime_ready_ack",
                "ready",
                json!({
                    "node_id":node_id,
                    "node_actor":target.ready.node_actor,
                    "readiness_id":readiness_id,
                    "attempts":attempts.get(&key).copied().unwrap_or(0),
                }),
            );
        }
        if pending.is_empty() {
            return Ok(true);
        }
        if last_send.is_none_or(|sent_at| sent_at.elapsed() >= RUNTIME_READY_ACK_RETRY_INTERVAL) {
            for (key, target) in &pending {
                enqueue_runtime_ready_ack(stack, &target.ready, run_id, target.node_id)?;
                let attempt = attempts.entry(*key).or_default();
                *attempt += 1;
                let attempt = *attempt;
                bootstrap(
                    orch_telemetry,
                    "runtime_ready_ack",
                    "sent",
                    json!({
                        "node_id":target.node_id,
                        "node_actor":target.ready.node_actor,
                        "readiness_id":target.ready.readiness_id,
                        "attempt":attempt,
                    }),
                );
            }
            last_send = Some(Instant::now());
        }
        thread::sleep(PUMP_INTERVAL);
    }
    Ok(true)
}

fn reconciler_group(config: &Config, spec: &NodeProvisionSpec) -> RunNodeGroupSpec {
    let group_id = NodeGroupId(format!("node-{}", spec.node_id));
    let ssh_user = config
        .vastai
        .as_ref()
        .map(|vastai| vastai.provisioning.ssh_user.clone())
        .unwrap_or_else(|| "root".to_owned());
    let disk_gb = config
        .vastai
        .as_ref()
        .map(|vastai| vastai.provisioning.disk_gb)
        .unwrap_or_default();
    let orchestrator = spec
        .env
        .iter()
        .find(|(name, _)| name == "MYELIN_ORCHESTRATOR_ACTOR")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    RunNodeGroupSpec {
        run_id: ClusterRunId(config.run_id),
        group_id,
        role: RoleId(format!(
            "stage-{}",
            spec.stage_index.unwrap_or(config.stage_index)
        )),
        count: 1,
        provider: ReconcilerProviderKind::new(config.provider.as_str()),
        shape: DesiredNodeShape {
            image: spec.image.clone(),
            disk_gb,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: false,
            provider_labels: BTreeMap::from([(
                "myelin.provider_config".to_owned(),
                config.provider_telemetry_detail().to_string(),
            )]),
        },
        boot: BootSpec {
            ssh_user,
            verify_commands: Vec::new(),
            start_swactor_command: spec.args.join(" "),
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            env: spec.env.clone(),
            args: spec.args.clone(),
            mounts: spec.mounts.clone(),
        },
        swarm_join: SwarmJoinTemplate {
            orch_swactor_addr: orchestrator,
            join_token_ref: "myelin-runtime-ready".to_owned(),
        },
    }
}

fn build_reconciled_cluster(
    provisioner: Box<dyn ProvisionPlugin>,
    config: &Config,
    stage_specs: &[NodeProvisionSpec],
    runtime: swactor::runtime::Runtime,
    engine: EngineHandle,
    sink: PluginSink,
) -> Result<ProvisionedClusterGuard, String> {
    let groups = stage_specs
        .iter()
        .map(|spec| reconciler_group(config, spec))
        .collect::<Vec<_>>();
    let desired = ClusterShape {
        run_id: ClusterRunId(config.run_id),
        generation: 1,
        groups,
    };
    let expanded = desired.expand().map_err(|error| error.to_string())?;
    let mut first = Some(provisioner);
    let mut bindings = Vec::with_capacity(stage_specs.len());
    for spec in stage_specs {
        let logical_node_id = ReconcilerLogicalNodeId(format!("node-{}-0", spec.node_id));
        if !expanded.contains_key(&logical_node_id) {
            return Err(format!(
                "reconciler shape did not expand node {}",
                logical_node_id.0
            ));
        }
        let plugin = match first.take() {
            Some(plugin) => plugin,
            None => config.build_provisioner(runtime.clone())?,
        };
        bindings.push(ReconcilerNodeBinding {
            logical_node_id,
            provision: spec.clone(),
            plugin,
        });
    }
    ProvisionedClusterGuard::new(desired, bindings, RetryPolicy::default(), engine, sink)
}

// synchronous process-control/orchestration sequencing; the engine drives all background work (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn wait_for_runtime_readies(
    ctx: RuntimeReadyAckLoop<'_>,
    expected_node_ids: &[u64],
    cluster: &mut ProvisionedClusterGuard,
) -> Result<BTreeMap<u64, RuntimeReady>, String> {
    let RuntimeReadyAckLoop {
        driver,
        stack,
        obs_rx,
        collector,
        orchestrator_reports,
        stop_rx,
        dashboard,
        orch_telemetry,
        orch_stdio_rx,
        run_id,
        provider,
        ..
    } = ctx;
    let expected = expected_node_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut pending = BTreeMap::<u64, RuntimeReady>::new();
    loop {
        cluster
            .poll(SystemTime::now())
            .map_err(|error| format!("cluster reconcile while awaiting runtime: {error}"))?;
        pending.retain(|node_id, ready| {
            cluster.current_attempt(*node_id)
                == Some(::provisioning::NodeAttemptId(ready.readiness_id))
        });
        collector.pump(driver);
        emit_swim_transitions(
            orch_telemetry,
            dashboard,
            run_id,
            expected_node_ids.first().copied().unwrap_or(0),
            stack,
        );
        emit_swim_probe_events(orch_telemetry, dashboard, stack, "runtime_ready_wait");
        collector.drain(|stream, descriptor, channel, frame| {
            if let Some(d) = dashboard {
                d.publish_collected_frame(stream, descriptor, channel, frame);
            }
            orch_telemetry.archive_frame("node", stream, channel, frame);
        });
        drain_orch_stdio_capture(
            orch_stdio_rx,
            orch_telemetry,
            dashboard,
            run_id,
            expected_node_ids.first().copied().unwrap_or(0),
        );
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for pipeline nodes ready".to_owned());
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_telemetry, dashboard, provider, &observation);
            match observation {
                PluginObservation::TelemetryFrame { .. }
                | PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. }
                | PluginObservation::Failed { .. }
                | PluginObservation::Exited { .. } => {}
            }
        }
        while let Some(report) = orchestrator_reports.try_recv() {
            if let OrchestratorReport::NodeRuntimeReady {
                run_id: report_run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                telemetry_publisher,
                readiness_id,
            } = report
                && report_run_id == run_id
                && expected.contains(&node_id)
                && cluster.current_attempt(node_id)
                    == Some(::provisioning::NodeAttemptId(readiness_id))
            {
                pending.insert(
                    node_id,
                    RuntimeReady {
                        endpoint: endpoint.clone(),
                        node_actor,
                        telemetry_publisher,
                        stage_index,
                        readiness_id,
                        swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                    },
                );
            }
        }
        if expected.iter().all(|node_id| {
            pending
                .get(node_id)
                .is_some_and(|ready| runtime_ready_barrier_met(stack, ready))
        }) {
            return Ok(pending);
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

struct OrchStdioCapture;

struct OrchStdioLine {
    stream: ProvisionLogStream,
    line: String,
}

#[cfg(target_os = "linux")]
impl OrchStdioCapture {
    fn install() -> Result<Option<mpsc::Receiver<OrchStdioLine>>, String> {
        let stdout_read = Self::redirect_stream(libc::STDOUT_FILENO, "stdout")?;
        let stderr_read = Self::redirect_stream(libc::STDERR_FILENO, "stderr")?;
        let (tx, rx) = mpsc::channel();
        Self::spawn_reader(stdout_read, ProvisionLogStream::Stdout, tx.clone());
        Self::spawn_reader(stderr_read, ProvisionLogStream::Stderr, tx);
        Ok(Some(rx))
    }

    fn redirect_stream(fd: libc::c_int, name: &str) -> Result<File, String> {
        let mut pipe_fds = [0; 2];
        let pipe_result = unsafe { libc::pipe(pipe_fds.as_mut_ptr()) };
        if pipe_result != 0 {
            return Err(format!(
                "create orchestrator {name} capture pipe: {}",
                std::io::Error::last_os_error()
            ));
        }

        let dup_result = unsafe { libc::dup2(pipe_fds[1], fd) };
        let close_write_result = unsafe { libc::close(pipe_fds[1]) };
        if dup_result < 0 {
            let error = std::io::Error::last_os_error();
            let _ = unsafe { libc::close(pipe_fds[0]) };
            return Err(format!("redirect orchestrator {name}: {error}"));
        }
        if close_write_result != 0 {
            let error = std::io::Error::last_os_error();
            let _ = unsafe { libc::close(pipe_fds[0]) };
            return Err(format!("close orchestrator {name} duplicate fd: {error}"));
        }

        Ok(unsafe { File::from_raw_fd(pipe_fds[0]) })
    }

    // provider log capture is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn spawn_reader(file: File, stream: ProvisionLogStream, tx: mpsc::Sender<OrchStdioLine>) {
        thread::spawn(move || {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let Ok(line) = line else {
                    break;
                };
                if tx.send(OrchStdioLine { stream, line }).is_err() {
                    break;
                }
            }
        });
    }
}

#[cfg(not(target_os = "linux"))]
impl OrchStdioCapture {
    fn install() -> Result<Option<mpsc::Receiver<OrchStdioLine>>, String> {
        Ok(None)
    }
}

fn drain_orch_stdio_capture(
    rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    run_id: u64,
    node_id: u64,
) {
    let Some(rx) = rx else {
        return;
    };
    while let Ok(line) = rx.try_recv() {
        telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id,
                node_id,
                stream: line.stream,
                line: line.line,
            },
        );
    }
}

struct ChannelObservationSink {
    tx: Mutex<mpsc::Sender<PluginObservation>>,
}

impl PluginObservationSink for ChannelObservationSink {
    fn observe(&self, observation: PluginObservation) {
        let _ = self.tx.lock().send(observation);
    }
}

fn stop_requested(stop_rx: &mpsc::Receiver<()>) -> bool {
    stop_rx.try_recv().is_ok()
}

// top-level OS signal handling is process control, out of scope (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn spawn_stop_listener() -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel();
    #[cfg(target_os = "linux")]
    {
        thread::spawn(move || {
            let Ok(mut signals) = signal_hook::iterator::Signals::new([
                signal_hook::consts::signal::SIGINT,
                signal_hook::consts::signal::SIGTERM,
            ]) else {
                return;
            };
            if signals.forever().next().is_some() {
                let _ = tx.send(());
            }
        });
    }
    #[cfg(not(target_os = "linux"))]
    {
        drop(tx);
    }
    rx
}

struct ServeCluster<'a> {
    driver: &'a mut IrohDriver,
    stack: &'a DistributionRuntimeStack,
    obs_rx: &'a mpsc::Receiver<PluginObservation>,
    collector: &'a FrameCollector,
    orchestrator_reports: &'a swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &'a mpsc::Receiver<()>,
    dashboard: Option<&'a DashboardSupport>,
    orch_telemetry: &'a mut OrchTelemetry,
    orch_stdio_rx: Option<&'a mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: &'a ProviderKind,
    orchestrator_actor: ActorAddress,
    coordinator_endpoint: EndpointAddr,
    engine: EngineHandle,
    runtime: swactor::runtime::Runtime,
    config: Config,
    provisioner: Box<dyn ProvisionPlugin>,
    live_clusters: BTreeMap<u64, ProvisionedClusterGuard>,
    sink: PluginSink,
    state_dir: daemon::StateDir,
    snapshot: daemon::ClusterSnapshot,
    control_rx: &'a mpsc::Receiver<ControlCommand>,
    destroy_on_exit: bool,
}

fn daemon_label(config: &Config) -> String {
    match config.provider.as_str() {
        "docker" => env_optional("MYELIN_DOCKER_CONTAINER_PREFIX")
            .unwrap_or_else(|| "myelin-orchestrator".to_owned()),
        "vastai" => config
            .vastai
            .as_ref()
            .map(|vastai| vastai.provisioning.label_prefix.clone())
            .unwrap_or_else(|| "myelin".to_owned()),
        provider => format!("myelin-{provider}"),
    }
}

fn parse_control_node_id(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .or_else(|| {
            value
                .split(|ch: char| !ch.is_ascii_digit())
                .filter(|part| !part.is_empty())
                .next_back()
                .and_then(|part| part.parse::<u64>().ok())
        })
        .ok_or_else(|| format!("control node id {value:?} contains no numeric logical id"))
}

impl ServeCluster<'_> {
    fn ack_context(&mut self) -> RuntimeReadyAckLoop<'_> {
        RuntimeReadyAckLoop {
            driver: self.driver,
            stack: self.stack,
            obs_rx: self.obs_rx,
            collector: self.collector,
            orchestrator_reports: self.orchestrator_reports,
            stop_rx: self.stop_rx,
            dashboard: self.dashboard,
            orch_telemetry: self.orch_telemetry,
            orch_stdio_rx: self.orch_stdio_rx,
            run_id: self.run_id,
            orchestrator_node_id: self.orchestrator_node_id,
            provider: self.provider,
            orchestrator_actor: self.orchestrator_actor,
        }
    }

    fn save_snapshot(&self) -> Result<(), String> {
        self.state_dir.save_snapshot(&self.snapshot)
    }

    fn emit_command_event(
        &mut self,
        node_id: u64,
        kind: ProvisionEventKind,
        message: impl Into<String>,
    ) {
        self.orch_telemetry.emit_event(
            self.dashboard,
            ProvisionEvent {
                run_id: self.run_id,
                node_id,
                kind,
                provider: Some(self.provider.as_str().to_owned()),
                message: Some(message.into()),
            },
        );
    }

    fn adopt_snapshot(&mut self) -> Result<(), String> {
        let mut accounted = BTreeSet::new();
        let ids = self
            .snapshot
            .nodes
            .iter()
            .filter_map(|node| node.spec.as_ref().map(|_| node.logical_node_id))
            .collect::<Vec<_>>();
        for node_id in ids {
            let spec = self
                .snapshot
                .node(node_id)
                .and_then(|node| node.spec.clone())
                .expect("filtered snapshot node has spec");
            match self.provisioner.adopt_by_spec(&spec, self.sink.clone()) {
                Ok(Some(adopted)) => {
                    accounted.insert(adopted.provider_ref.clone());
                    if let Some(node) = self.snapshot.node_mut(node_id) {
                        node.provider_ref = Some(adopted.provider_ref);
                        node.status = daemon::NodeStatus::Running;
                        node.last_seen_unix_ms = daemon::unix_ms_now();
                    }
                    if let Some(runtime) = self
                        .snapshot
                        .node(node_id)
                        .and_then(|node| node.runtime.as_ref())
                    {
                        let endpoint = serde_json::from_str::<EndpointAddr>(&runtime.endpoint)
                            .map_err(|error| {
                                format!(
                                    "snapshot node {node_id} telemetry endpoint is invalid: {error}"
                                )
                            })?;
                        self.collector.subscribe_node(
                            &self.engine,
                            self.driver.endpoint(),
                            endpoint,
                            self.run_id,
                            node_id,
                        );
                    }
                    self.emit_command_event(
                        node_id,
                        ProvisionEventKind::NodeLive,
                        "adopted provider resource after daemon restart",
                    );
                }
                Ok(None) => {
                    if let Some(node) = self.snapshot.node_mut(node_id) {
                        node.status = daemon::NodeStatus::Dead;
                    }
                    self.emit_command_event(
                        node_id,
                        ProvisionEventKind::NodeStopped,
                        "snapshot node is absent from provider",
                    );
                }
                Err(error) => {
                    if let Some(node) = self.snapshot.node_mut(node_id) {
                        node.status = daemon::NodeStatus::Dead;
                    }
                    self.emit_command_event(
                        node_id,
                        ProvisionEventKind::ProvisionFailed,
                        format!("provider adoption failed: {error}"),
                    );
                }
            }
        }

        let provider_only = self
            .provisioner
            .list_managed_refs()?
            .into_iter()
            .filter(|provider_ref| !accounted.contains(provider_ref))
            .collect::<Vec<_>>();
        for provider_ref in self.snapshot.sync_orphans(provider_only) {
            self.emit_command_event(
                0,
                ProvisionEventKind::NodeLive,
                format!("unmanaged provider resource discovered: {provider_ref}"),
            );
        }
        self.save_snapshot()
    }

    fn add_node(&mut self) -> Result<u64, String> {
        let logical_node_id = self.snapshot.allocate_node_id();
        // Persist allocation before any provider side effect so a crash never
        // reuses the id or ambiguously attributes a lease.
        self.save_snapshot()?;
        let spec = self.config.node_spec_for_stage(
            self.coordinator_endpoint.clone(),
            self.orchestrator_actor,
            logical_node_id,
            0,
        )?;
        self.emit_command_event(
            logical_node_id,
            ProvisionEventKind::ProvisionStart,
            "manual add-node command accepted",
        );
        let mut cluster = build_reconciled_cluster(
            self.config.build_provisioner(self.runtime.clone())?,
            &self.config,
            std::slice::from_ref(&spec),
            self.runtime.clone(),
            self.engine.clone(),
            self.sink.clone(),
        )?;
        let readies =
            wait_for_runtime_readies(self.ack_context(), &[logical_node_id], &mut cluster)?;
        let ready = readies
            .get(&logical_node_id)
            .cloned()
            .ok_or_else(|| format!("node {logical_node_id} did not announce runtime ready"))?;
        self.collector.subscribe_node(
            &self.engine,
            self.driver.endpoint(),
            ready.endpoint.clone(),
            self.run_id,
            logical_node_id,
        );
        let acknowledged = wait_for_runtime_ready_acks(
            self.ack_context(),
            &[RuntimeReadyAckTarget {
                node_id: logical_node_id,
                ready: ready.clone(),
            }],
            &mut cluster,
        )?;
        if !acknowledged {
            return Err(format!(
                "node {logical_node_id} changed attempt before runtime-ready acknowledgement"
            ));
        }
        let attempt_id = cluster
            .current_attempt(logical_node_id)
            .ok_or_else(|| format!("node {logical_node_id} has no live reconciler attempt"))?
            .0;
        let mut persisted_spec = spec;
        persisted_spec.attempt_id = attempt_id;
        persisted_spec
            .env
            .retain(|(name, _)| name != "MYELIN_NODE_ATTEMPT_ID");
        persisted_spec
            .env
            .push(("MYELIN_NODE_ATTEMPT_ID".to_owned(), attempt_id.to_string()));
        let provider_ref = self.provisioner.provider_ref_for(&persisted_spec);
        if self.provider.as_str() == "process" {
            self.live_clusters.insert(logical_node_id, cluster);
        } else {
            cluster.detach();
        }
        self.snapshot.upsert_node(daemon::SnapshotNode {
            logical_node_id,
            spec: Some(persisted_spec),
            provider_ref: Some(provider_ref),
            status: daemon::NodeStatus::Running,
            runtime: Some(daemon::RuntimeFacts {
                endpoint: serde_json::to_string(&ready.endpoint)
                    .map_err(|error| format!("serialize node endpoint: {error}"))?,
                node_actor: ready.node_actor,
                telemetry_publisher: ready.telemetry_publisher,
                swim_node_id: ready.swim_node_id,
                stage_index: ready.stage_index,
                readiness_id: ready.readiness_id,
            }),
            last_seen_unix_ms: daemon::unix_ms_now(),
        });
        self.save_snapshot()?;
        self.emit_command_event(
            logical_node_id,
            ProvisionEventKind::NodeLive,
            "manual add-node command completed",
        );
        Ok(logical_node_id)
    }

    fn kill_node(&mut self, logical_node_id: u64) -> Result<bool, String> {
        let Some(node) = self.snapshot.node(logical_node_id) else {
            return Ok(false);
        };
        if node.status == daemon::NodeStatus::Dead {
            return Ok(true);
        }
        let Some(spec) = node.spec.clone() else {
            return Ok(false);
        };
        if let Some(mut cluster) = self.live_clusters.remove(&logical_node_id) {
            cluster.stop()?;
        } else {
            let _ = self.provisioner.stop_by_spec(&spec, self.sink.clone())?;
        }
        if let Some(node) = self.snapshot.node_mut(logical_node_id) {
            node.status = daemon::NodeStatus::Dead;
            node.last_seen_unix_ms = daemon::unix_ms_now();
        }
        self.save_snapshot()?;
        self.emit_command_event(
            logical_node_id,
            ProvisionEventKind::NodeStopped,
            "manual kill command completed",
        );
        Ok(true)
    }

    fn destroy_node(&mut self, logical_node_id: u64) -> Result<bool, String> {
        let Some(node) = self.snapshot.node(logical_node_id).cloned() else {
            return Ok(false);
        };
        if let Some(mut cluster) = self.live_clusters.remove(&logical_node_id) {
            cluster.stop()?;
        } else if let Some(spec) = node.spec {
            let _ = self.provisioner.stop_by_spec(&spec, self.sink.clone())?;
        }
        self.snapshot.remove_node(logical_node_id);
        self.save_snapshot()?;
        self.emit_command_event(
            logical_node_id,
            ProvisionEventKind::NodeStopped,
            "manual destroy command completed",
        );
        Ok(true)
    }

    fn handle_dashboard_command(&mut self, command: ControlCommand) {
        let command_id = command.command_id().to_owned();
        match self.snapshot.accept_command(&command_id) {
            Ok(false) => {
                self.emit_command_event(
                    0,
                    ProvisionEventKind::NodeLive,
                    format!("duplicate dashboard command ignored: {command_id}"),
                );
                return;
            }
            Err(error) => {
                self.emit_command_event(0, ProvisionEventKind::ProvisionFailed, error);
                return;
            }
            Ok(true) => {}
        }
        if let Err(error) = self.save_snapshot() {
            self.emit_command_event(
                0,
                ProvisionEventKind::ProvisionFailed,
                format!("persist dashboard command {command_id}: {error}"),
            );
            return;
        }
        let result = match command {
            ControlCommand::Provision { count, .. } => {
                (0..count).try_for_each(|_| self.add_node().map(|_| ()))
            }
            ControlCommand::Kill { node, .. } => {
                parse_control_node_id(&node).and_then(|node_id| self.kill_node(node_id).map(|_| ()))
            }
            ControlCommand::Remove { count, .. } => {
                let mut ids = self
                    .snapshot
                    .nodes
                    .iter()
                    .filter(|node| node.spec.is_some())
                    .map(|node| node.logical_node_id)
                    .collect::<Vec<_>>();
                ids.sort_unstable_by(|left, right| right.cmp(left));
                ids.into_iter()
                    .take(count as usize)
                    .try_for_each(|node_id| self.destroy_node(node_id).map(|_| ()))
            }
            ControlCommand::EstablishEdge { node, .. } => {
                Err(format!("edge establishment for node {node} is deferred"))
            }
        };
        if let Err(error) = result {
            self.emit_command_event(
                0,
                ProvisionEventKind::ProvisionFailed,
                format!("dashboard control {command_id} failed: {error}"),
            );
        }
    }

    fn observe_report(&mut self, report: OrchestratorReport) {
        if let OrchestratorReport::NodeRuntimeReady {
            run_id,
            node_id,
            stage_index,
            endpoint,
            node_actor,
            telemetry_publisher,
            readiness_id,
        } = report
            && run_id == self.run_id
        {
            if let Some(node) = self.snapshot.node_mut(node_id) {
                node.status = daemon::NodeStatus::Running;
                node.runtime = Some(daemon::RuntimeFacts {
                    endpoint: serde_json::to_string(&endpoint).unwrap_or_default(),
                    node_actor,
                    telemetry_publisher,
                    swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                    stage_index,
                    readiness_id,
                });
                node.last_seen_unix_ms = daemon::unix_ms_now();
            }
        }
    }

    fn drain_observations(&mut self) -> Result<(), String> {
        let mut dirty = false;
        while let Ok(observation) = self.obs_rx.try_recv() {
            emit_plugin_observation(
                self.orch_telemetry,
                self.dashboard,
                self.provider,
                &observation,
            );
            match observation {
                PluginObservation::Exited { node_id, .. }
                | PluginObservation::Failed { node_id, .. } => {
                    if let Some(node) = self.snapshot.node_mut(node_id) {
                        node.status = daemon::NodeStatus::Dead;
                        node.last_seen_unix_ms = daemon::unix_ms_now();
                        dirty = true;
                    }
                }
                _ => {}
            }
        }
        if dirty {
            self.save_snapshot()?;
        }
        Ok(())
    }

    fn teardown_if_requested(&mut self) -> Result<(), String> {
        // Local process children cannot be adopted, so detaching them on an
        // interactive shutdown only creates invisible orphan processes.
        if self.destroy_on_exit || self.provider.as_str() == "process" {
            for (_, mut cluster) in std::mem::take(&mut self.live_clusters) {
                cluster.stop()?;
            }
            let specs = self
                .snapshot
                .nodes
                .iter()
                .filter_map(|node| node.spec.clone())
                .collect::<Vec<_>>();
            for spec in specs {
                let _ = self.provisioner.stop_by_spec(&spec, self.sink.clone())?;
            }
            self.snapshot.nodes.clear();
            self.save_snapshot()?;
        } else {
            for (_, mut cluster) in std::mem::take(&mut self.live_clusters) {
                cluster.detach();
            }
            self.provisioner.detach_all();
        }
        Ok(())
    }
}

impl Drop for ServeCluster<'_> {
    fn drop(&mut self) {
        if self.provider.as_str() == "process" {
            for cluster in self.live_clusters.values_mut() {
                let _ = cluster.stop();
            }
        } else {
            for cluster in self.live_clusters.values_mut() {
                cluster.detach();
            }
        }
        self.provisioner.detach_all();
    }
}

#[allow(clippy::disallowed_methods)]
fn serve_cluster(mut ctx: ServeCluster<'_>) -> Result<(), String> {
    ctx.adopt_snapshot()?;
    loop {
        ctx.collector.pump(ctx.driver);
        ctx.collector.drain(|stream, descriptor, channel, frame| {
            if let Some(dashboard) = ctx.dashboard {
                dashboard.publish_collected_frame(stream, descriptor, channel, frame);
            }
            ctx.orch_telemetry
                .archive_frame("node", stream, channel, frame);
        });
        ctx.drain_observations()?;
        while let Some(report) = ctx.orchestrator_reports.try_recv() {
            ctx.observe_report(report);
        }
        emit_swim_transitions(
            ctx.orch_telemetry,
            ctx.dashboard,
            ctx.run_id,
            ctx.orchestrator_node_id,
            ctx.stack,
        );
        emit_swim_probe_events(
            ctx.orch_telemetry,
            ctx.dashboard,
            ctx.stack,
            "daemon_monitor",
        );
        drain_orch_stdio_capture(
            ctx.orch_stdio_rx,
            ctx.orch_telemetry,
            ctx.dashboard,
            ctx.run_id,
            ctx.orchestrator_node_id,
        );
        while let Ok(command) = ctx.control_rx.try_recv() {
            ctx.handle_dashboard_command(command);
        }
        if stop_requested(ctx.stop_rx) {
            break;
        }
        thread::sleep(PUMP_INTERVAL);
    }
    ctx.save_snapshot()?;
    ctx.teardown_if_requested()
}

fn drain_observations_with_exit(
    obs_rx: &mpsc::Receiver<PluginObservation>,
    dashboard: Option<&DashboardSupport>,
    orch_telemetry: &mut OrchTelemetry,
    provider: &ProviderKind,
    exit_message: impl Fn(u64, Option<i32>) -> String,
) -> Result<(), String> {
    while let Ok(observation) = obs_rx.try_recv() {
        emit_plugin_observation(orch_telemetry, dashboard, provider, &observation);
        match observation {
            PluginObservation::Failed { reason, .. } => return Err(reason),
            PluginObservation::Exited {
                node_id, status, ..
            } => return Err(exit_message(node_id, status)),
            PluginObservation::TelemetryFrame { .. }
            | PluginObservation::ProviderLine { .. }
            | PluginObservation::StdoutLine { .. }
            | PluginObservation::StderrLine { .. } => {}
        }
    }
    Ok(())
}

fn emit_plugin_observation(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    provider: &ProviderKind,
    observation: &PluginObservation,
) {
    match observation {
        PluginObservation::StdoutLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Stdout,
                line: line.clone(),
            },
        ),
        PluginObservation::StderrLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Stderr,
                line: line.clone(),
            },
        ),
        PluginObservation::ProviderLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Provider,
                line: line.clone(),
            },
        ),
        PluginObservation::TelemetryFrame {
            channel, payload, ..
        } => orch_telemetry.emit_bytes_from(
            dashboard,
            channel,
            payload.as_bytes().to_vec(),
            "node_bootstrap_stdio",
        ),
        PluginObservation::Exited {
            run_id,
            node_id,
            status,
        } => orch_telemetry.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: ProvisionEventKind::NodeStopped,
                provider: Some(provider.as_str().to_owned()),
                message: Some(format!("node process exited with {status:?}")),
            },
        ),
        PluginObservation::Failed {
            run_id,
            node_id,
            reason,
        } => orch_telemetry.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: ProvisionEventKind::ProvisionFailed,
                provider: Some(provider.as_str().to_owned()),
                message: Some(reason.clone()),
            },
        ),
    }
}

fn emit_swim_transitions(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    run_id: u64,
    node_id: u64,
    stack: &DistributionRuntimeStack,
) -> Vec<ObservedTransition> {
    let transitions = stack.drain_swim_transitions();
    for transition in &transitions {
        let peer = format!("{:?}", transition.peer);
        let from = transition.from.map(|state| format!("{:?}", state));
        let to = format!("{:?}", transition.to);
        let member_state = stack
            .member_state(transition.peer)
            .map(|state| format!("{:?}", state));
        let last_ack_age_ms = transition.last_ack_age.map(duration_ms_u64);
        let consecutive_timeouts = transition.consecutive_timeouts;
        let recent_probe_targets = stack.swim_recent_probe_targets();
        orch_telemetry.emit_bootstrap_to_channel(
            dashboard,
            MYELIN_SWIM_MEMBERSHIP,
            run_id,
            node_id,
            "membership_transition",
            "observed",
            json!({
                "peer":peer.clone(),
                "from":from.clone(),
                "to":to.clone(),
                "reason":transition.reason,
                "last_ack_age_ms":last_ack_age_ms,
                "consecutive_timeouts":consecutive_timeouts,
                "recent_probe_targets":recent_probe_targets.clone(),
                "member_state":member_state.clone(),
            }),
        );
        orch_telemetry.emit_record(dashboard, &stack.membership_transition(transition));
    }
    transitions
}

fn emit_swim_probe_events(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    stack: &DistributionRuntimeStack,
    local_phase: &str,
) {
    for event in stack.drain_swim_probe_events() {
        let record = stack.swim_probe_event_record(event, local_phase);
        orch_telemetry.emit_record(dashboard, &record);
    }
}

pub(crate) fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn local_tinygrad_worker_env(provider: &str) -> Option<(String, String)> {
    env_optional("MYELIN_TINYGRAD_WORKER")
        .map(|value| ("MYELIN_TINYGRAD_WORKER".to_owned(), value))
        .or_else(|| {
            (provider == "process")
                .then(default_local_tinygrad_worker_path)
                .flatten()
                .map(|path| {
                    (
                        "MYELIN_TINYGRAD_WORKER".to_owned(),
                        path.to_string_lossy().to_string(),
                    )
                })
        })
}

fn default_local_tinygrad_worker_path() -> Option<PathBuf> {
    // The tinygrad worker script ships in the node-image build context at
    // `apps/myelin/node-image/tinygrad_worker.py` (see `chat/node_image.rs` and
    // the node-image Dockerfile). The process provider runs it directly via
    // `python3`, so resolve that path from the workspace cwd or this crate's
    // manifest dir. (Previously looked in `apps/myelin-node/`, a path left stale
    // by the `mvp-system` -> `myelin` app refactor and never present on disk.)
    let cwd_candidate = std::env::current_dir().ok().map(|cwd| {
        cwd.join("apps")
            .join("myelin")
            .join("node-image")
            .join("tinygrad_worker.py")
    });
    if let Some(candidate) = cwd_candidate.filter(|path| path.is_file()) {
        return Some(candidate.canonicalize().unwrap_or(candidate));
    }

    let manifest_candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("node-image")
        .join("tinygrad_worker.py");
    manifest_candidate.is_file().then(|| {
        manifest_candidate
            .canonicalize()
            .unwrap_or(manifest_candidate)
    })
}

pub(crate) fn resolve_vastai_ssh_identity(explicit: Option<PathBuf>) -> Result<PathBuf, String> {
    match explicit {
        Some(path) => Ok(path),
        None => {
            let home = std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "MYELIN_VASTAI_SSH_IDENTITY is required because HOME is unset".to_owned()
                })?;
            Ok(PathBuf::from(home).join(".ssh").join("id_ed25519"))
        }
    }
}

pub(crate) fn expand_home_path(value: &str) -> Result<PathBuf, String> {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "MYELIN_VASTAI_SSH_IDENTITY uses ~/ but HOME is unset".to_owned())?;
        return Ok(PathBuf::from(home).join(rest));
    }
    Ok(PathBuf::from(trimmed))
}

pub(crate) fn derive_ssh_public_key(identity: &Path) -> Result<String, String> {
    let output = Command::new("ssh-keygen")
        .arg("-y")
        .arg("-f")
        .arg(identity)
        .output()
        .map_err(|e| {
            format!(
                "derive VastAI SSH public key from {}: {e}",
                identity.display()
            )
        })?;
    let public_key = String::from_utf8_lossy(&output.stdout)
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    if !output.status.success() || public_key.trim().is_empty() {
        return Err(format!(
            "derive VastAI SSH public key from {}: {}",
            identity.display(),
            command_output_failure_detail(&output, None)
        ));
    }
    Ok(public_key)
}

pub(crate) fn ssh_public_key_fingerprint(public_key: &str) -> String {
    const UNAVAILABLE: &str = "unavailable";

    let path =
        std::env::temp_dir().join(format!("myelin-vastai-ssh-key-{}.pub", std::process::id()));
    if std::fs::write(&path, format!("{public_key}\n")).is_err() {
        return UNAVAILABLE.to_owned();
    }
    let output = Command::new("ssh-keygen")
        .arg("-l")
        .arg("-f")
        .arg(&path)
        .output()
        .ok();
    let _ = std::fs::remove_file(&path);
    let Some(output) = output.filter(|output| output.status.success()) else {
        return UNAVAILABLE.to_owned();
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut fields = stdout.split_whitespace();
    fields
        .next()
        .zip(fields.next())
        .map(|(bits, fingerprint)| format!("{bits} {fingerprint}"))
        .unwrap_or_else(|| UNAVAILABLE.to_owned())
}

fn vastai_account_has_ssh_key(api_key: &str, public_key: &str) -> Result<bool, String> {
    let output = Command::new("vastai")
        .args(["show", "ssh-keys", "--raw", "--api-key", api_key])
        .output()
        .map_err(vastai_cli_error)?;
    if !output.status.success() {
        return Err(format!(
            "vastai show ssh-keys failed: {}",
            command_output_failure_detail(&output, Some(api_key))
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(account_ssh_keys_output_contains_public_key(
        &stdout, public_key,
    ))
}

pub(crate) fn ensure_vastai_account_ssh_key(api_key: &str, public_key: &str) -> Result<(), String> {
    if vastai_account_has_ssh_key(api_key, public_key)? {
        return Ok(());
    }

    let output = Command::new("vastai")
        .args(["create", "ssh-key"])
        .arg(public_key)
        .args(["-y", "--api-key", api_key])
        .output()
        .map_err(vastai_cli_error)?;
    if !output.status.success() {
        return Err(format!(
            "vastai create ssh-key failed: {}",
            command_output_failure_detail(&output, Some(api_key))
        ));
    }

    if vastai_account_has_ssh_key(api_key, public_key)? {
        Ok(())
    } else {
        Err("VastAI SSH key registration did not make the selected key visible in vastai show ssh-keys".to_owned())
    }
}

fn account_ssh_keys_output_contains_public_key(output: &str, public_key: &str) -> bool {
    let public_key = public_key.trim();
    !public_key.is_empty()
        && (output.contains(public_key)
            || public_key
                .split_whitespace()
                .nth(1)
                .is_some_and(|body| !body.is_empty() && output.contains(body)))
}

fn vastai_cli_error(error: std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        "vastai CLI is required to verify/register MYELIN_VASTAI_SSH_IDENTITY; install with pip install vastai".to_owned()
    } else {
        format!("run vastai CLI: {error}")
    }
}

fn command_output_failure_detail(output: &std::process::Output, secret: Option<&str>) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut detail = match stderr.trim() {
        "" => output.status.to_string(),
        detail => detail.to_owned(),
    };
    if let Some(secret) = secret.filter(|secret| !secret.is_empty()) {
        detail = detail.replace(secret, "<redacted>");
    }
    detail
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value after {name}"))
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = next_arg(args, name)?;
    value
        .parse::<T>()
        .map_err(|e| format!("invalid {name}={value:?}: {e}"))
}
