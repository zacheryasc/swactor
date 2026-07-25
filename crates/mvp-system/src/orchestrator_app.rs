use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::actors::node_agent::{
    NodeAgentMsg, StageEdgeKindWire, StageInboundEdgeWire, StageObjectSpecWire,
    StageOutboundEdgeWire, StageProvisionWire, StageRingSpecWire,
};
use crate::actors::orchestrator::{OrchestratorActor, OrchestratorReport};
use crate::actors::register_mvp_actor_codecs;
use crate::benchmark_observability;
use crate::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
#[cfg(feature = "dashboard")]
use crate::dashboard_view::MvpClusterDashboardView;
use crate::distribution_stack::DistributionRuntimeStack;
use crate::endpoint_advertisement::{
    EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint,
};
use crate::gpu_worker_ingress_parser as ingress;
use crate::node_provisioning::ProviderKind;
use crate::orchestrator_run_fsm::{RunConfig, RunId};
use crate::prompt_rpc::{
    PromptEvent, SubmitPrompt, TokenizerEvent, read_submit_prompt, write_json_line,
};
use crate::provisioning::{
    LocalDockerPlugin, LocalProcessPlugin, NodeProvisionSpec, PluginObservation,
    PluginObservationSink, PluginSink, ProviderMount, ProvisionEvent, ProvisionEventKind,
    ProvisionLogLine, ProvisionLogStream, ProvisionPlugin,
};
#[cfg(test)]
use crate::relay_provisioning::relay_runtime_config_from_env;
use crate::relay_provisioning::{
    MVP_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV, relay_mode_env_value,
    relay_runtime_config_from_settings,
};
use crate::run_plan::{self, GgufSource, TokenizerSource};
use crate::telemetry::{
    MVP_PROVISIONING_EVENTS, MvpProvisionEventRecord, MvpProvisionLogRecord,
    mvp_provision_log_channel,
};
use crate::vastai_provisioning::{
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin,
};
use datastream::{
    ChannelContent, ChannelId, ChannelRef, DatastreamEndpoint, DatastreamEvent, DatastreamProducer,
    DatastreamPublisherMsg, DatastreamSubscribe, Frame, Lifetime, NodeId, StreamDescriptor,
    StreamId, StreamOrigin, SubscriptionRequest,
};
use distribution::node::DistributedNodeConfig;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::EndpointAddr;
use iroh_driver::{
    DATASTREAM_ALPN, EDGE_ALPN, EdgeSendHandle, EdgeTransportEvent, IrohDriver, IrohDriverConfig,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;
#[cfg(test)]
use tokio::sync::mpsc as tokio_mpsc;

const DEFAULT_IMAGE: &str = "swactor-mvp-node:latest";
const MVP_RUNTIME_CONFIG_ENV: &str = "MVP_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MVP_CACHED_MODEL_HOST_PATH";
const MVP_WORKER_BIN_ENV: &str = "MVP_WORKER_BIN";
const CACHED_MODEL_CONTAINER_DIR: &str = "/models/cached";
const DEFAULT_PIPELINE_CACHED_MODEL_FILE: &str = "SmolLM2-135M-Instruct.Q4_0.gguf";
const DEFAULT_PIPELINE_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_RPC_BIND: &str = "127.0.0.1:19777";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_MAX_TOKENS: u32 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const RUNTIME_READY_ACK_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const RUNTIME_READY_ACK_TIMEOUT: Duration = Duration::from_secs(60);
const RUNTIME_READY_TIMEOUT: Duration = Duration::from_secs(60);
const MVP_ORCH_BOOTSTRAP: &str = "mvp.orch.bootstrap";
const MVP_ORCH_PROMPT: &str = "mvp.orch.prompt";
const MVP_SWIM_MEMBERSHIP: &str = "mvp.swim.membership";
const MVP_STAGE_ROUTE: &str = "mvp.orch.stage_route";
const DATASTREAM_FRAME_LOG_ENV: &str = "MVP_DATASTREAM_FRAME_LOG";
const DEFAULT_DOCKER_CONTAINER_PREFIX: &str = "mvp-orchestrator";
const MVP_DOCKER_CONTAINER_PREFIX_ENV: &str = "MVP_DOCKER_CONTAINER_PREFIX";

pub struct OrchestratorRunOptions {
    pub capture_stdio: bool,
    pub stop_rx: Option<mpsc::Receiver<()>>,
}

impl Default for OrchestratorRunOptions {
    fn default() -> Self {
        Self {
            capture_stdio: true,
            stop_rx: None,
        }
    }
}

pub fn run_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    run_with_options(args, OrchestratorRunOptions::default())
}

pub fn run_in_process_from_args<I>(args: I, stop_rx: mpsc::Receiver<()>) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    run_with_options(
        args,
        OrchestratorRunOptions {
            capture_stdio: false,
            stop_rx: Some(stop_rx),
        },
    )
}

pub fn run_with_options<I>(args: I, options: OrchestratorRunOptions) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config::from_defaults_toml_env_args(args)?;
    config.prepare_vastai_ssh_key()?;
    let orch_stdio_rx = if options.capture_stdio {
        install_orch_stdio_capture()?
    } else {
        None
    };
    let mut orch_datastream =
        OrchDatastream::new(config.run_id, config.datastream_frame_log.as_deref())?;
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "config",
        "ready",
        json!({
            "config_profile":config.config_profile.as_str(),
            "image":&config.image,
            "provider":config.provider.as_str(),
            "rpc_bind":config.rpc_bind.to_string(),
            "model_id":&config.model_id,
            "stage_index":config.stage_index,
            "legacy_layer_end_exclusive":config.layer_end_exclusive,
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "provider_config":config.provider_datastream_detail(),
        }),
    );
    drain_orch_stdio_capture(
        orch_stdio_rx.as_ref(),
        &mut orch_datastream,
        None,
        config.run_id,
        config.node_id,
    );
    let pipeline_plan = if config.uses_planned_execution() {
        let plan = config.build_run_plan()?;
        orch_datastream.emit_bootstrap(
            None,
            config.run_id,
            config.node_id,
            "run_plan",
            "ready",
            json!({
                "stage_count":plan.stages.len(),
                "edge_count":plan.edges.len(),
                "model_layers":plan.model.num_layers,
                "hidden_dim":plan.model.hidden_dim,
                "max_seq_len":plan.model.max_seq_len,
                "eos_token_id":plan.model.eos_token_id,
            }),
        );
        Some(plan)
    } else {
        None
    };
    let _ = &pipeline_plan;

    let tokio = match tokio::runtime::Runtime::new() {
        Ok(runtime) => {
            orch_datastream.emit_bootstrap(
                None,
                config.run_id,
                config.node_id,
                "tokio_runtime",
                "ready",
                json!({"runtime":"tokio"}),
            );
            runtime
        }
        Err(error) => {
            orch_datastream.emit_bootstrap(
                None,
                config.run_id,
                config.node_id,
                "tokio_runtime",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("tokio runtime: {error}"));
        }
    };
    let mut driver = match IrohDriver::with_handle(
        tokio.handle().clone(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: config.relay.mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec(), DATASTREAM_ALPN.to_vec()],
        },
    ) {
        Ok(driver) => driver,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                None,
                config.run_id,
                config.node_id,
                "iroh_driver",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("create iroh driver: {error}"));
        }
    };
    let coordinator_endpoint =
        advertised_endpoint(driver.endpoint_addr(), config.endpoint_addr_mask)?;
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "iroh_driver",
        "ready",
        json!({"endpoint":coordinator_endpoint.clone(),"has_relay":coordinator_endpoint.relay_urls().next().is_some(),"direct_addr_count":coordinator_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay.mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
    );
    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        |registry| {
            register_mvp_actor_codecs(registry);
            datastream::wire::register_datastream_codec(registry);
        },
    );
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "distribution_stack",
        "ready",
        json!({"actors":"initialized","route_view":"initialized","swim":"initialized"}),
    );
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "codecs",
        "ready",
        json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","datastream"]}),
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "actor_bridge",
        "ready",
        json!({"transport":"iroh","routes":"attached"}),
    );

    let (frame_tx, frame_rx) = mpsc::channel::<CollectedDatastreamFrame>();
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "datastream_collector",
        "ready",
        json!({"alpn":String::from_utf8_lossy(DATASTREAM_ALPN)}),
    );
    let dashboard = DashboardSupport::start(config.dashboard)?;
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "dashboard",
        "ready",
        json!({"enabled":dashboard.is_some()}),
    );

    let orchestrator_reports = match stack.runtime.new_inbox::<OrchestratorReport>() {
        Ok(inbox) => inbox,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "orchestrator_report_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("orchestrator report inbox: {error}"));
        }
    };
    let orchestrator_report_actor = *orchestrator_reports.addr();
    stack.register_local_actor(driver.register_actor(orchestrator_report_actor, 1));
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
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
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "orchestrator_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("spawn orchestrator actor: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(orchestrator_actor, 1));
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "orchestrator_actor",
        "ready",
        json!({"actor":orchestrator_actor}),
    );

    let prompt_events = match stack.runtime.new_inbox::<PromptEvent>() {
        Ok(inbox) => inbox,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "prompt_reply_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("prompt event inbox: {error}"));
        }
    };
    let prompt_reply_actor = *prompt_events.addr();
    stack.register_local_actor(driver.register_actor(prompt_reply_actor, 1));
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "prompt_reply_actor",
        "ready",
        json!({"actor":prompt_reply_actor}),
    );

    let tokenizer_events = match stack.runtime.new_inbox::<TokenizerEvent>() {
        Ok(inbox) => inbox,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "tokenizer_reply_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("tokenizer event inbox: {error}"));
        }
    };
    let tokenizer_reply_actor = *tokenizer_events.addr();
    stack.register_local_actor(driver.register_actor(tokenizer_reply_actor, 1));
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "tokenizer_reply_actor",
        "ready",
        json!({"actor":tokenizer_reply_actor}),
    );

    let (work_tx, work_rx) = mpsc::channel::<PromptWork>();
    let stop_rx = options.stop_rx.unwrap_or_else(spawn_stop_listener);

    let provisioner = config.build_provisioner(Arc::clone(&stack.runtime))?;
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "node_provisioner",
        "ready",
        json!({
            "provider":config.provider.as_str(),
            "owner":"mvp-orchestrator",
            "config":config.provider_datastream_detail(),
        }),
    );
    let (obs_tx, obs_rx) = mpsc::channel::<PluginObservation>();
    let sink = PluginSink::new(Arc::new(ChannelObservationSink {
        tx: Mutex::new(obs_tx),
    }));
    let pipeline_coordinator_endpoint = coordinator_endpoint.clone();
    let (mut provisioned_nodes, ready) = start_and_provision_workers(
        provisioner,
        &config,
        pipeline_plan.as_ref(),
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        &frame_tx,
        &orchestrator_reports,
        &stop_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
        orch_stdio_rx.as_ref(),
        sink,
        coordinator_endpoint,
        pipeline_coordinator_endpoint,
        orchestrator_actor,
    )?;

    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "prompt_rpc",
        "started",
        json!({
            "bind":config.rpc_bind.to_string(),
            "default_max_tokens":config.default_max_tokens,
        }),
    );

    let rpc_addr = match spawn_prompt_rpc(config.rpc_bind, work_tx, config.default_max_tokens) {
        Ok(addr) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "prompt_rpc",
                "ready",
                json!({
                    "addr":addr.to_string(),
                    "default_max_tokens":config.default_max_tokens,
                }),
            );
            addr
        }
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "prompt_rpc",
                "failed",
                json!({"error":error}),
            );
            return Err(error);
        }
    };

    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "prompt_loop",
        "ready",
        json!({"addr":rpc_addr.to_string(),"node_actor":ready.first_stage.node_actor}),
    );

    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "serve_prompts",
        "started",
        json!({"mode":"single_active_prompt","poll_interval_ms":PUMP_INTERVAL.as_millis()}),
    );
    let result = serve_prompts(
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        &frame_tx,
        &work_rx,
        &prompt_events,
        &stop_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
        orch_stdio_rx.as_ref(),
        config.run_id,
        config.node_id,
        ready.first_stage.node_actor,
        prompt_reply_actor,
        &tokenizer_events,
        ready.first_stage.node_actor,
        ready.final_stage.node_actor,
        tokenizer_reply_actor,
        config.provider,
        pipeline_plan.as_ref(),
        ready.first_stage.endpoint.clone(),
    );
    if let Err(error) = &result {
        orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "serve_prompts",
            "failed",
            json!({"error":error}),
        );
    }
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "provider_stop",
        "started",
        json!({"provider":config.provider.as_str(),"node_id":config.node_id}),
    );
    let stop_result = provisioned_nodes.stop();
    match &stop_result {
        Ok(()) => orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "provider_stop",
            "ready",
            json!({"provider":config.provider.as_str(),"node_id":config.node_id}),
        ),
        Err(error) => orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "provider_stop",
            "failed",
            json!({"provider":config.provider.as_str(),"node_id":config.node_id,"error":error}),
        ),
    }
    if result.is_ok() && stop_result.is_ok() {
        orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "orch_exit",
            "ready",
            json!({"result":"ok"}),
        );
    }
    result.and(stop_result)
}

#[derive(Clone)]
struct VastAiRuntimeConfig {
    api_key: Option<String>,
    provisioning: VastAiProvisioningConfig,
    bootstrap_command: Option<String>,
    ssh_identity: Option<PathBuf>,
    ssh_public_key: Option<String>,
    ssh_public_fingerprint: Option<String>,
}

impl VastAiRuntimeConfig {
    fn from_builder(builder: &ConfigBuilder) -> Result<Self, String> {
        let mut provisioning = VastAiProvisioningConfig::default();
        let disk_gb = builder
            .vastai_disk_gb_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_DISK_GB", value))
            .transpose()?
            .or(builder.vastai_disk_gb);
        if let Some(disk_gb) = disk_gb {
            provisioning.disk_gb = disk_gb;
        }
        if let Some(ssh_user) = &builder.vastai_ssh_user {
            provisioning.ssh_user = ssh_user.clone();
        }
        let confirm_lease = builder
            .vastai_confirm_lease_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_bool("MVP_VASTAI_CONFIRM_LEASE", value))
            .transpose()?
            .or(builder.vastai_confirm_lease);
        if let Some(confirm_lease) = confirm_lease {
            provisioning.confirm_lease = confirm_lease;
        }
        provisioning.onstart = builder.vastai_onstart.clone();
        provisioning.selection.gpu_name = builder.vastai_gpu_name.clone();
        let min_gpu_ram_mb = builder
            .vastai_min_gpu_ram_mb_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_MIN_GPU_RAM_MB", value))
            .transpose()?
            .or(builder.vastai_min_gpu_ram_mb);
        if let Some(min_gpu_ram_mb) = min_gpu_ram_mb {
            provisioning.selection.min_gpu_ram_mb = Some(min_gpu_ram_mb);
        }
        let min_down_mbps = builder
            .vastai_min_down_mbps_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_MIN_DOWN_MBPS", value))
            .transpose()?
            .or(builder.vastai_min_down_mbps);
        if let Some(min_down_mbps) = min_down_mbps {
            provisioning.selection.min_down_mbps = min_down_mbps;
        }
        let max_dph_total = builder
            .vastai_max_dph_total_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_MAX_DPH_TOTAL", value))
            .transpose()?
            .or(builder.vastai_max_dph_total);
        if let Some(max_dph_total) = max_dph_total {
            provisioning.selection.max_dph_total = Some(max_dph_total);
        }
        let min_up_mbps = builder
            .vastai_min_up_mbps_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_MIN_UP_MBPS", value))
            .transpose()?
            .or(builder.vastai_min_up_mbps);
        if let Some(min_up_mbps) = min_up_mbps {
            provisioning.selection.min_up_mbps = Some(min_up_mbps);
        }
        let min_reliability = builder
            .vastai_min_reliability_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_MIN_RELIABILITY", value))
            .transpose()?
            .or(builder.vastai_min_reliability);
        if let Some(min_reliability) = min_reliability {
            provisioning.selection.min_reliability = min_reliability;
        }
        let require_verified = builder
            .vastai_require_verified_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_bool("MVP_VASTAI_REQUIRE_VERIFIED", value))
            .transpose()?
            .or(builder.vastai_require_verified);
        if let Some(require_verified) = require_verified {
            provisioning.selection.require_verified = require_verified;
        }
        let poll_interval_secs = builder
            .vastai_poll_interval_secs_raw
            .as_ref()
            .map(|value| ConfigBuilder::parse_value("MVP_VASTAI_POLL_INTERVAL_SECS", value))
            .transpose()?
            .or(builder.vastai_poll_interval_secs);
        if let Some(poll_interval_secs) = poll_interval_secs {
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
            ssh_public_key: None,
            ssh_public_fingerprint: None,
        })
    }

    fn datastream_detail(&self) -> Value {
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
                "unsupported {MVP_RUNTIME_CONFIG_ENV}={other:?}; use local or deploy"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Deploy => "deploy",
        }
    }

    fn default_provider(self) -> ProviderKind {
        match self {
            Self::Local => ProviderKind::Process,
            Self::Deploy => ProviderKind::VastAi,
        }
    }
}

#[derive(Clone)]
struct CachedModelConfig {
    host_path: PathBuf,
    container_path: String,
}

impl CachedModelConfig {
    fn from_host_path(provider: ProviderKind, requested: PathBuf) -> Result<Self, String> {
        if !matches!(provider, ProviderKind::Process | ProviderKind::Docker) {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} is a host-local cache path and is only supported by provider=process or provider=docker"
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
        let container_path = cached_model_container_path(&host_path)?;
        Ok(Self {
            host_path,
            container_path,
        })
    }

    fn worker_path(&self, provider: ProviderKind) -> String {
        match provider {
            ProviderKind::Process => self.host_path.to_string_lossy().to_string(),
            ProviderKind::Docker | ProviderKind::VastAi | ProviderKind::Mock => {
                self.container_path.clone()
            }
        }
    }

    fn datastream_detail(&self) -> Value {
        json!({
            "host_path_present": true,
            "file": self.host_path.file_name().and_then(|name| name.to_str()),
            "container_path": &self.container_path,
        })
    }
}

fn cached_model_container_path(host_path: &Path) -> Result<String, String> {
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
    Ok(format!("{CACHED_MODEL_CONTAINER_DIR}/{file_name}"))
}

fn default_worker_bin() -> Result<PathBuf, String> {
    let mut path = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
    path.set_file_name("mvp-worker-node");
    Ok(path)
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

fn gguf_source_is_default_hf(source: &GgufSource) -> bool {
    matches!(
        source,
        GgufSource::HuggingFaceGguf {
            repo,
            file,
            revision: None,
        } if repo == DEFAULT_HF_REPO && file == DEFAULT_HF_FILE
    )
}

fn gguf_source_matches_default_pipeline_cache(source: &GgufSource) -> bool {
    matches!(
        source,
        GgufSource::HuggingFaceGguf {
            file,
            revision: None,
            ..
        } if file == DEFAULT_PIPELINE_CACHED_MODEL_FILE
    )
}

#[derive(Clone)]
struct Config {
    config_profile: RuntimeConfigProfile,
    image: String,
    docker_gpus: String,
    provider: ProviderKind,
    rpc_bind: SocketAddr,
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
    max_context: Option<u32>,
    relay: RelayRuntimeConfig,
    endpoint_addr_mask: EndpointAddrMask,
    vastai: Option<VastAiRuntimeConfig>,
    cached_model: Option<CachedModelConfig>,
    datastream_frame_log: Option<PathBuf>,
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
    rpc_bind: String,
    rpc_bind_label: &'static str,
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
    vastai_poll_interval_secs_raw: Option<String>,
    cached_model_host_path: Option<PathBuf>,
    datastream_frame_log: Option<PathBuf>,
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
            rpc_bind: DEFAULT_RPC_BIND.to_owned(),
            rpc_bind_label: "MVP_PROMPT_RPC_BIND",
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
            dashboard: false,
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
            vastai_poll_interval_secs_raw: None,
            cached_model_host_path: None,
            datastream_frame_log: None,
            worker_bin: None,
        }
    }

    fn overlay_toml(mut self, overlay: TomlConfigOverlay) -> Result<Self, String> {
        if let Some(profile) = overlay.runtime.profile {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        }
        if let Some(run_id) = overlay.runtime.run_id {
            self.run_id = run_id;
        }
        if let Some(node_id) = overlay.runtime.node_id {
            self.node_id = node_id;
        }
        if let Some(stage_index) = overlay.runtime.stage_index {
            self.stage_index = stage_index;
        }
        if let Some(layer_end_exclusive) = overlay.runtime.layer_end_exclusive {
            self.layer_end_exclusive = Some(layer_end_exclusive);
        }
        if let Some(pipeline_stages) = overlay.runtime.pipeline_stages {
            self.pipeline_stages = pipeline_stages;
        }
        if let Some(provider) = overlay.provider.kind {
            self.provider = Some(ProviderKind::parse_deploy(&provider)?);
        }
        if let Some(image) = overlay.image.node {
            self.image = image;
        }
        if let Some(mode) = overlay.relay.mode {
            self.relay_mode = Some(mode);
        }
        if let Some(url) = overlay.relay.url {
            self.relay_url = Some(url);
        }
        if let Some(rpc_bind) = overlay.prompt.rpc_addr {
            self.rpc_bind = rpc_bind;
            self.rpc_bind_label = "[prompt].rpc_addr";
        }
        if let Some(max_tokens) = overlay.prompt.max_tokens {
            self.default_max_tokens = max_tokens;
        }
        if let Some(dashboard) = overlay.prompt.dashboard {
            self.dashboard = dashboard;
        }
        if let Some(model_id) = overlay.model.id {
            self.model_id = model_id;
        }
        if let Some(path) = overlay.model.gguf_local_path {
            self.gguf_source = GgufSource::LocalPath(path);
        }
        if let Some(repo) = overlay.model.gguf_repo {
            self.set_gguf_repo(repo);
        }
        if let Some(file) = overlay.model.gguf_file {
            self.set_gguf_file(file);
        }
        if let Some(revision) = overlay.model.gguf_revision {
            self.set_gguf_revision(Some(revision));
        }
        if let Some(path) = overlay.model.tokenizer_local_path {
            self.tokenizer = TokenizerSource::LocalPath(path);
        }
        if let Some(max_context) = overlay.model.max_context {
            self.max_context = Some(max_context);
        }
        if let Some(gpus) = overlay.docker.gpus {
            self.docker_gpus = gpus;
        }
        if let Some(path) = overlay.docker.cached_model_host_path {
            self.cached_model_host_path = Some(PathBuf::from(path));
        }
        if let Some(path) = overlay.observability.datastream_frame_log {
            self.datastream_frame_log = Some(PathBuf::from(path));
        }
        if let Some(image) = overlay.vastai.image {
            self.toml_vastai_image = Some(image);
        }
        if let Some(api_key) = overlay.vastai.api_key {
            self.vastai_api_key = Some(api_key);
        }
        if let Some(command) = overlay.vastai.bootstrap_command {
            self.vastai_bootstrap_command = Some(command);
        }
        if let Some(disk_gb) = overlay.vastai.disk_gb {
            self.vastai_disk_gb = Some(disk_gb);
        }
        if let Some(ssh_user) = overlay.vastai.ssh_user {
            self.vastai_ssh_user = Some(ssh_user);
        }
        if let Some(confirm_lease) = overlay.vastai.confirm_lease {
            self.vastai_confirm_lease = Some(confirm_lease);
        }
        if let Some(onstart) = overlay.vastai.onstart {
            self.vastai_onstart = Some(onstart);
        }
        if let Some(identity) = overlay.vastai.ssh_identity {
            self.vastai_ssh_identity_raw = Some(identity);
        }
        if let Some(gpu_name) = overlay.vastai.gpu_name {
            self.vastai_gpu_name = Some(gpu_name);
        }
        if let Some(min_gpu_ram_mb) = overlay.vastai.min_gpu_ram_mb {
            self.vastai_min_gpu_ram_mb = Some(min_gpu_ram_mb);
        }
        if let Some(min_down_mbps) = overlay.vastai.min_down_mbps {
            self.vastai_min_down_mbps = Some(min_down_mbps);
        }
        if let Some(max_dph_total) = overlay.vastai.max_dph_total {
            self.vastai_max_dph_total = Some(max_dph_total);
        }
        if let Some(min_up_mbps) = overlay.vastai.min_up_mbps {
            self.vastai_min_up_mbps = Some(min_up_mbps);
        }
        if let Some(min_reliability) = overlay.vastai.min_reliability {
            self.vastai_min_reliability = Some(min_reliability);
        }
        if let Some(require_verified) = overlay.vastai.require_verified {
            self.vastai_require_verified = Some(require_verified);
        }
        if let Some(poll_interval_secs) = overlay.vastai.poll_interval_secs {
            self.vastai_poll_interval_secs = Some(poll_interval_secs);
        }
        Ok(self)
    }

    fn overlay_env(mut self) -> Result<Self, String> {
        if let Some(profile) = env_optional(MVP_RUNTIME_CONFIG_ENV) {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        }
        if let Some(run_id) = env_optional("MVP_RUN_ID") {
            self.run_id = Self::parse_value("MVP_RUN_ID", &run_id)?;
        }
        if let Some(node_id) = env_optional("MVP_LOGICAL_NODE_ID") {
            self.node_id = Self::parse_value("MVP_LOGICAL_NODE_ID", &node_id)?;
        }
        if let Some(stage_index) = env_optional("MVP_STAGE_INDEX") {
            self.stage_index = Self::parse_value("MVP_STAGE_INDEX", &stage_index)?;
        }
        if let Some(layer_end_exclusive) = env_optional("MVP_LAYER_END_EXCLUSIVE") {
            self.layer_end_exclusive = Some(Self::parse_value(
                "MVP_LAYER_END_EXCLUSIVE",
                &layer_end_exclusive,
            )?);
        }
        if let Some(pipeline_stages) = env_optional("MVP_PIPELINE_STAGES") {
            self.pipeline_stages = Self::parse_value("MVP_PIPELINE_STAGES", &pipeline_stages)?;
        }
        if let Some(provider) =
            env_optional("MVP_NODE_PROVIDER").or_else(|| env_optional("MVP_PROVIDER"))
        {
            self.provider = Some(ProviderKind::parse_deploy(&provider)?);
        }
        if let Some(image) = env_optional("MVP_NODE_IMAGE") {
            self.set_process_image(image);
        }
        if let Some(gpus) = env_optional("MVP_DOCKER_GPUS") {
            self.docker_gpus = gpus;
        }
        if let Some(path) = env_optional(CACHED_MODEL_HOST_ENV) {
            self.cached_model_host_path = Some(PathBuf::from(path));
        }
        if let Some(path) = env_optional(MVP_WORKER_BIN_ENV) {
            self.worker_bin = Some(PathBuf::from(path));
        }
        if let Some(rpc_bind) = env_optional("MVP_PROMPT_RPC_BIND") {
            self.rpc_bind = rpc_bind;
            self.rpc_bind_label = "MVP_PROMPT_RPC_BIND";
        }
        if let Some(max_tokens) = env_optional("MVP_PROMPT_MAX_TOKENS") {
            self.default_max_tokens = Self::parse_value("MVP_PROMPT_MAX_TOKENS", &max_tokens)?;
        }
        if let Some(dashboard) = env_optional("MVP_DASHBOARD") {
            self.dashboard = Self::parse_bool("MVP_DASHBOARD", &dashboard)?;
        }
        if let Some(path) = env_optional(DATASTREAM_FRAME_LOG_ENV) {
            self.datastream_frame_log = Some(PathBuf::from(path));
        }
        if let Some(model_id) = env_optional("MVP_MODEL_ID") {
            self.model_id = model_id;
        }
        if let Some(path) = env_optional("MVP_GGUF_LOCAL_PATH") {
            self.gguf_source = GgufSource::LocalPath(path);
        }
        if let Some(repo) = env_optional("MVP_GGUF_REPO") {
            self.set_gguf_repo(repo);
        }
        if let Some(file) = env_optional("MVP_GGUF_FILE") {
            self.set_gguf_file(file);
        }
        if let Some(revision) = env_optional("MVP_GGUF_REVISION") {
            self.set_gguf_revision(Some(revision));
        }
        if let Some(path) = env_optional("MVP_TOKENIZER_LOCAL_PATH") {
            self.tokenizer = TokenizerSource::LocalPath(path);
        }
        if let Some(max_context) = env_optional("MVP_MAX_CONTEXT") {
            self.max_context = Some(Self::parse_value("MVP_MAX_CONTEXT", &max_context)?);
        }
        if let Some(mode) = env_optional("MVP_IROH_RELAY_MODE") {
            self.relay_mode = Some(mode.to_ascii_lowercase());
        }
        if let Some(url) = env_optional(MVP_IROH_RELAY_URL_ENV)
            .or_else(|| env_optional(SWACTOR_IROH_RELAY_URL_ENV))
        {
            self.relay_url = Some(url);
        }
        if let Some(mask) = env_optional(MVP_IROH_ENDPOINT_ADDR_MASK_ENV) {
            self.endpoint_addr_mask = Some(mask);
        }
        if let Some(api_key) = env_optional("VAST_API_KEY")
            .or_else(|| env_optional("MVP_VASTAI_API_KEY"))
            .or_else(|| env_optional("VASTAI_API_KEY"))
        {
            self.vastai_api_key = Some(api_key);
        }
        if let Some(command) = env_optional("MVP_VASTAI_BOOTSTRAP_COMMAND") {
            self.vastai_bootstrap_command = Some(command);
        }
        if let Some(identity) = env_optional("MVP_VASTAI_SSH_IDENTITY") {
            self.vastai_ssh_identity_raw = Some(identity);
        }
        if let Some(disk_gb) = env_optional("MVP_VASTAI_DISK_GB") {
            self.vastai_disk_gb_raw = Some(disk_gb);
        }
        if let Some(ssh_user) = env_optional("MVP_VASTAI_SSH_USER") {
            self.vastai_ssh_user = Some(ssh_user);
        }
        if let Some(confirm_lease) = env_optional("MVP_VASTAI_CONFIRM_LEASE") {
            self.vastai_confirm_lease_raw = Some(confirm_lease);
        }
        if let Some(onstart) = env_optional("MVP_VASTAI_ONSTART") {
            self.vastai_onstart = Some(onstart);
        }
        if let Some(gpu_name) = env_optional("MVP_VASTAI_GPU_NAME") {
            self.vastai_gpu_name = Some(gpu_name);
        }
        if let Some(min_gpu_ram_mb) = env_optional("MVP_VASTAI_MIN_GPU_RAM_MB") {
            self.vastai_min_gpu_ram_mb_raw = Some(min_gpu_ram_mb);
        }
        if let Some(min_down_mbps) = env_optional("MVP_VASTAI_MIN_DOWN_MBPS") {
            self.vastai_min_down_mbps_raw = Some(min_down_mbps);
        }
        if let Some(max_dph_total) = env_optional("MVP_VASTAI_MAX_DPH_TOTAL") {
            self.vastai_max_dph_total_raw = Some(max_dph_total);
        }
        if let Some(min_up_mbps) = env_optional("MVP_VASTAI_MIN_UP_MBPS") {
            self.vastai_min_up_mbps_raw = Some(min_up_mbps);
        }
        if let Some(min_reliability) = env_optional("MVP_VASTAI_MIN_RELIABILITY") {
            self.vastai_min_reliability_raw = Some(min_reliability);
        }
        if let Some(require_verified) = env_optional("MVP_VASTAI_REQUIRE_VERIFIED") {
            self.vastai_require_verified_raw = Some(require_verified);
        }
        if let Some(poll_interval_secs) = env_optional("MVP_VASTAI_POLL_INTERVAL_SECS") {
            self.vastai_poll_interval_secs_raw = Some(poll_interval_secs);
        }
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
                    self.provider = Some(ProviderKind::parse_deploy(&next_arg(
                        &mut args,
                        "--provider",
                    )?)?)
                }
                "--worker-bin" => {
                    self.worker_bin = Some(PathBuf::from(next_arg(&mut args, "--worker-bin")?))
                }
                "--image" => self.set_process_image(next_arg(&mut args, "--image")?),
                "--gpus" => self.docker_gpus = next_arg(&mut args, "--gpus")?,
                "--rpc-bind" => {
                    self.rpc_bind = next_arg(&mut args, "--rpc-bind")?;
                    self.rpc_bind_label = "--rpc-bind";
                }
                "--run-id" => self.run_id = parse_next(&mut args, "--run-id")?,
                "--node-id" => self.node_id = parse_next(&mut args, "--node-id")?,
                "--stage-index" => self.stage_index = parse_next(&mut args, "--stage-index")?,
                "--layer-end-exclusive" => {
                    self.layer_end_exclusive = Some(parse_next(&mut args, "--layer-end-exclusive")?)
                }
                "-N" | "--pipeline-stages" => {
                    self.pipeline_stages = parse_next(&mut args, arg.as_str())?
                }
                "--max-tokens" => self.default_max_tokens = parse_next(&mut args, "--max-tokens")?,
                "--dashboard" => self.dashboard = true,
                "--no-dashboard" => self.dashboard = false,
                "--datastream-frame-log" => {
                    self.datastream_frame_log = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--datastream-frame-log",
                    )?));
                }
                "--model-id" => self.model_id = next_arg(&mut args, "--model-id")?,
                "--gguf-local-path" => {
                    self.gguf_source =
                        GgufSource::LocalPath(next_arg(&mut args, "--gguf-local-path")?)
                }
                "--gguf-repo" => self.set_gguf_repo(next_arg(&mut args, "--gguf-repo")?),
                "--gguf-file" => self.set_gguf_file(next_arg(&mut args, "--gguf-file")?),
                "--gguf-revision" => {
                    self.set_gguf_revision(Some(next_arg(&mut args, "--gguf-revision")?))
                }
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
                "--vastai-disk-gb" => {
                    self.vastai_disk_gb = Some(parse_next(&mut args, "--vastai-disk-gb")?);
                    self.vastai_disk_gb_raw = None;
                }
                "--vastai-ssh-user" => {
                    self.vastai_ssh_user = Some(next_arg(&mut args, "--vastai-ssh-user")?)
                }
                "--vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(true);
                    self.vastai_confirm_lease_raw = None;
                }
                "--no-vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(false);
                    self.vastai_confirm_lease_raw = None;
                }
                "--vastai-onstart" => {
                    self.vastai_onstart = Some(next_arg(&mut args, "--vastai-onstart")?)
                }
                "--vastai-gpu-name" => {
                    self.vastai_gpu_name = Some(next_arg(&mut args, "--vastai-gpu-name")?)
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
                "--vastai-require-verified" => {
                    self.vastai_require_verified = Some(true);
                    self.vastai_require_verified_raw = None;
                }
                "--no-vastai-require-verified" => {
                    self.vastai_require_verified = Some(false);
                    self.vastai_require_verified_raw = None;
                }
                "--vastai-poll-interval-secs" => {
                    self.vastai_poll_interval_secs =
                        Some(parse_next(&mut args, "--vastai-poll-interval-secs")?);
                    self.vastai_poll_interval_secs_raw = None;
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        Ok(self)
    }

    fn finalize(self) -> Result<Config, String> {
        let provider = self
            .provider
            .unwrap_or_else(|| self.config_profile.default_provider());
        let mut image = self.image.clone();
        if provider == ProviderKind::VastAi && !self.image_overridden_after_toml {
            if let Some(vastai_image) = &self.toml_vastai_image {
                image = vastai_image.clone();
            }
        }
        if self.pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        let mut cached_model_host_path = self.cached_model_host_path.clone();
        if matches!(provider, ProviderKind::Process | ProviderKind::Docker)
            && self.pipeline_stages > 1
            && cached_model_host_path.is_none()
            && (gguf_source_is_default_hf(&self.gguf_source)
                || gguf_source_matches_default_pipeline_cache(&self.gguf_source))
        {
            cached_model_host_path = Some(default_pipeline_cached_model_path());
        }
        let cached_model = cached_model_host_path
            .map(|path| CachedModelConfig::from_host_path(provider, path))
            .transpose()?;
        let mut gguf_source = self.gguf_source.clone();
        if let Some(cached_model) = &cached_model {
            gguf_source = GgufSource::LocalPath(cached_model.worker_path(provider));
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
        let vastai = if provider == ProviderKind::VastAi {
            Some(VastAiRuntimeConfig::from_builder(&self)?)
        } else {
            None
        };
        Ok(Config {
            config_profile: self.config_profile,
            image,
            docker_gpus: self.docker_gpus,
            provider,
            rpc_bind: self
                .rpc_bind
                .parse()
                .map_err(|e| format!("invalid {}: {e}", self.rpc_bind_label))?,
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
            max_context: self.max_context,
            relay,
            endpoint_addr_mask,
            vastai,
            cached_model,
            worker_bin: self.worker_bin,
            datastream_frame_log: self.datastream_frame_log,
        })
    }

    fn set_process_image(&mut self, image: String) {
        self.image = image;
        self.image_overridden_after_toml = true;
    }

    fn set_gguf_repo(&mut self, repo: String) {
        let (file, revision) = match &self.gguf_source {
            GgufSource::HuggingFaceGguf { file, revision, .. } => (file.clone(), revision.clone()),
            GgufSource::LocalPath(_) => (DEFAULT_HF_FILE.to_owned(), None),
        };
        self.gguf_source = GgufSource::HuggingFaceGguf {
            repo,
            file,
            revision,
        };
    }

    fn set_gguf_file(&mut self, file: String) {
        let (repo, revision) = match &self.gguf_source {
            GgufSource::HuggingFaceGguf { repo, revision, .. } => (repo.clone(), revision.clone()),
            GgufSource::LocalPath(_) => (DEFAULT_HF_REPO.to_owned(), None),
        };
        self.gguf_source = GgufSource::HuggingFaceGguf {
            repo,
            file,
            revision,
        };
    }

    fn set_gguf_revision(&mut self, revision: Option<String>) {
        let (repo, file) = match &self.gguf_source {
            GgufSource::HuggingFaceGguf { repo, file, .. } => (repo.clone(), file.clone()),
            GgufSource::LocalPath(_) => (DEFAULT_HF_REPO.to_owned(), DEFAULT_HF_FILE.to_owned()),
        };
        self.gguf_source = GgufSource::HuggingFaceGguf {
            repo,
            file,
            revision,
        };
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
    fn from_defaults_toml_env_args(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        Self::from_layers_with_path_and_args(Some(Path::new(DEFAULT_CONFIG_PATH)), args)
    }

    fn from_layers_with_path_and_args(
        path: Option<&Path>,
        args: impl IntoIterator<Item = String>,
    ) -> Result<Self, String> {
        let mut builder = Self::hardcoded_defaults();
        if let Some(path) = path {
            if let Some(overlay) = TomlConfigOverlay::load_optional(path)? {
                builder = builder.overlay_toml(overlay)?;
            }
        }
        builder.overlay_env()?.overlay_cli(args)?.finalize()
    }

    fn hardcoded_defaults() -> ConfigBuilder {
        ConfigBuilder::hardcoded_defaults()
    }

    fn uses_planned_execution(&self) -> bool {
        self.cached_model.is_some() || self.pipeline_stages > 1
    }

    fn provider_datastream_detail(&self) -> Value {
        match self.provider {
            ProviderKind::Process => json!({
                "worker_bin": self.worker_bin.as_ref().map(|path| path.to_string_lossy().to_string()),
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::datastream_detail),
            }),
            ProviderKind::Docker => json!({
                "docker_gpus": &self.docker_gpus,
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::datastream_detail),
            }),
            ProviderKind::VastAi => self
                .vastai
                .as_ref()
                .map_or_else(|| json!({}), VastAiRuntimeConfig::datastream_detail),
            ProviderKind::Mock => json!({}),
        }
    }

    fn build_run_plan(&self) -> Result<run_plan::RunPlan, String> {
        let host_path = self.local_planning_gguf_path()?;
        let metadata = crate::gguf_metadata::read_gguf_planning_metadata(&host_path)?;
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
                prompt: run_plan::PromptSource::Inline(String::new()),
                sampling: run_plan::SamplingPolicy {
                    temperature_millis: 0,
                    top_k: 1,
                },
                token_output_policy: run_plan::TokenOutputPolicy::EmitAll,
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
            GgufSource::HuggingFaceGguf { repo, file, .. }
                if self.provider == ProviderKind::VastAi
                    && gguf_source_matches_default_pipeline_cache(&self.gguf_source) =>
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
        if self.provider != ProviderKind::VastAi {
            return Ok(());
        }

        let api_key = self
            .vastai
            .as_ref()
            .and_then(|vastai| vastai.api_key.as_deref())
            .ok_or_else(|| {
                "VAST_API_KEY, MVP_VASTAI_API_KEY, or VASTAI_API_KEY is required when MVP_NODE_PROVIDER=vastai"
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
                "missing VastAI SSH identity {}; create/register one with vastai create ssh-key or set MVP_VASTAI_SSH_IDENTITY",
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
        vastai.provisioning.ssh_public_key = Some(public_key.clone());
        vastai.ssh_public_key = Some(public_key);
        vastai.ssh_public_fingerprint = Some(fingerprint);
        Ok(())
    }

    fn build_provisioner(
        &self,
        bootstrap_runtime: Arc<swactor::runtime::Runtime>,
    ) -> Result<Box<dyn ProvisionPlugin>, String> {
        match self.provider {
            ProviderKind::Process => {
                let worker_bin = self.worker_bin.clone().unwrap_or(default_worker_bin()?);
                if !worker_bin.is_file() {
                    return Err(format!(
                        "local process worker binary does not exist: {}",
                        worker_bin.display()
                    ));
                }
                Ok(Box::new(LocalProcessPlugin::new(worker_bin)))
            }
            ProviderKind::Docker => Ok(Box::new(LocalDockerPlugin::new(docker_container_prefix()))),
            ProviderKind::VastAi => {
                let vastai = self.vastai.as_ref().ok_or_else(|| {
                    "VastAI config was not resolved for provider vastai".to_owned()
                })?;
                if vastai.bootstrap_command.is_none() {
                    return Err(
                        "MVP_VASTAI_BOOTSTRAP_COMMAND is required when MVP_NODE_PROVIDER=vastai"
                            .to_owned(),
                    );
                }
                let api_key = vastai.api_key.clone().ok_or_else(|| {
                    "VAST_API_KEY, MVP_VASTAI_API_KEY, or VASTAI_API_KEY is required when MVP_NODE_PROVIDER=vastai"
                        .to_owned()
                })?;
                let ssh_identity = vastai
                    .ssh_identity
                    .clone()
                    .ok_or_else(|| "VastAI SSH identity was not prepared".to_owned())?;
                let client = ToolsVastAiLeaseClient::from_api_key(api_key)?;
                Ok(Box::new(VastAiProvisioningPlugin::new(
                    client,
                    SshCommandBootstrapLauncher::new(Some(ssh_identity), bootstrap_runtime),
                    vastai.provisioning.clone(),
                )))
            }
            ProviderKind::Mock => Err("mvp-orchestrator does not support mock provider".to_owned()),
        }
    }

    fn node_spec_env_keys(&self) -> Vec<&'static str> {
        let mut keys = vec![
            "MVP_RUN_ID",
            "MVP_LOGICAL_NODE_ID",
            "MVP_NODE_PROVIDER",
            "MVP_STAGE_INDEX",
            "MVP_COORDINATOR_ENDPOINT",
            "MVP_ORCHESTRATOR_ACTOR",
            "MVP_MODEL_ID",
            "MVP_IROH_RELAY_MODE",
            MVP_IROH_ENDPOINT_ADDR_MASK_ENV,
            "MVP_PIPELINE_STAGES",
        ];
        if self.relay.url.is_some() {
            keys.push(MVP_IROH_RELAY_URL_ENV);
        }
        if self.provider == ProviderKind::Docker {
            keys.push("MVP_DOCKER_GPUS");
        }
        if std::env::var_os("DEV").is_some() {
            keys.push("DEV");
        }
        if local_tinygrad_worker_env(self.provider).is_some() {
            keys.push("MVP_TINYGRAD_WORKER");
        }
        if std::env::var_os("MVP_CPU_LINE_PROFILE").is_some() {
            keys.push("MVP_CPU_LINE_PROFILE");
        }
        if std::env::var_os("MVP_CPU_LINE_PROFILE_INTERVAL_MS").is_some() {
            keys.push("MVP_CPU_LINE_PROFILE_INTERVAL_MS");
        }
        if std::env::var_os("MVP_TOKEN_PROGRESS_EVERY").is_some() {
            keys.push("MVP_TOKEN_PROGRESS_EVERY");
        }
        if std::env::var_os("CUDA_DEVICE_SCHEDULE").is_some() {
            keys.push("CUDA_DEVICE_SCHEDULE");
        }
        if std::env::var_os("MVP_MODEL_CACHE_DIR").is_some() {
            keys.push("MVP_MODEL_CACHE_DIR");
        }
        if std::env::var_os("HF_TOKEN").is_some() {
            keys.push("HF_TOKEN");
        }
        match &self.gguf_source {
            GgufSource::LocalPath(_) => keys.push("MVP_GGUF_LOCAL_PATH"),
            GgufSource::HuggingFaceGguf { revision, .. } => {
                keys.push("MVP_GGUF_REPO");
                keys.push("MVP_GGUF_FILE");
                if revision.is_some() {
                    keys.push("MVP_GGUF_REVISION");
                }
            }
        }
        if matches!(self.tokenizer, TokenizerSource::LocalPath(_)) {
            keys.push("MVP_TOKENIZER_LOCAL_PATH");
        }
        if self.max_context.is_some() {
            keys.push("MVP_MAX_CONTEXT");
        }
        keys
    }

    fn node_spec(
        &self,
        coordinator: EndpointAddr,
        orchestrator_actor: ActorAddress,
    ) -> Result<NodeProvisionSpec, String> {
        self.node_spec_for_stage(
            coordinator,
            orchestrator_actor,
            self.node_id,
            self.stage_index,
        )
    }

    fn node_spec_for_stage(
        &self,
        coordinator: EndpointAddr,
        orchestrator_actor: ActorAddress,
        logical_node_id: u64,
        stage_index: u32,
    ) -> Result<NodeProvisionSpec, String> {
        let mut env = vec![
            ("MVP_RUN_ID".to_owned(), self.run_id.to_string()),
            (
                "MVP_LOGICAL_NODE_ID".to_owned(),
                logical_node_id.to_string(),
            ),
            ("MVP_STAGE_INDEX".to_owned(), stage_index.to_string()),
            (
                "MVP_PIPELINE_STAGES".to_owned(),
                self.pipeline_stages.to_string(),
            ),
            (
                MVP_IROH_ENDPOINT_ADDR_MASK_ENV.to_owned(),
                self.endpoint_addr_mask.as_str().to_owned(),
            ),
            (
                "MVP_NODE_PROVIDER".to_owned(),
                self.provider.as_str().to_owned(),
            ),
            (
                "MVP_COORDINATOR_ENDPOINT".to_owned(),
                serde_json::to_string(&coordinator)
                    .map_err(|e| format!("serialize coordinator endpoint: {e}"))?,
            ),
            (
                "MVP_ORCHESTRATOR_ACTOR".to_owned(),
                serde_json::to_string(&orchestrator_actor)
                    .map_err(|e| format!("serialize orchestrator actor: {e}"))?,
            ),
            ("MVP_MODEL_ID".to_owned(), self.model_id.clone()),
            (
                "MVP_IROH_RELAY_MODE".to_owned(),
                relay_mode_env_value(&self.relay.mode).to_owned(),
            ),
        ];
        if let Some(url) = &self.relay.url {
            env.push((MVP_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
        }
        if self.provider == ProviderKind::Docker {
            env.push(("MVP_DOCKER_GPUS".to_owned(), self.docker_gpus.clone()));
        }
        env.extend(optional_env("DEV"));
        env.extend(local_tinygrad_worker_env(self.provider));
        env.extend(optional_env("MVP_CPU_LINE_PROFILE"));
        env.extend(optional_env("MVP_CPU_LINE_PROFILE_INTERVAL_MS"));
        env.extend(optional_env("MVP_TOKEN_PROGRESS_EVERY"));
        env.extend(optional_env("CUDA_DEVICE_SCHEDULE"));
        env.extend(optional_env("MVP_MODEL_CACHE_DIR"));
        env.extend(optional_env("HF_TOKEN"));
        match &self.gguf_source {
            GgufSource::LocalPath(path) => {
                env.push(("MVP_GGUF_LOCAL_PATH".to_owned(), path.clone()))
            }
            GgufSource::HuggingFaceGguf {
                repo,
                file,
                revision,
            } => {
                env.push(("MVP_GGUF_REPO".to_owned(), repo.clone()));
                env.push(("MVP_GGUF_FILE".to_owned(), file.clone()));
                if let Some(revision) = revision {
                    env.push(("MVP_GGUF_REVISION".to_owned(), revision.clone()));
                }
            }
        }
        if let TokenizerSource::LocalPath(path) = &self.tokenizer {
            env.push(("MVP_TOKENIZER_LOCAL_PATH".to_owned(), path.clone()));
        }
        if let Some(max_context) = self.max_context {
            env.push(("MVP_MAX_CONTEXT".to_owned(), max_context.to_string()));
        }
        let args = match self.provider {
            ProviderKind::VastAi => self
                .vastai
                .as_ref()
                .and_then(|vastai| vastai.bootstrap_command.clone())
                .into_iter()
                .collect(),
            ProviderKind::Process | ProviderKind::Docker => Vec::new(),
            ProviderKind::Mock => {
                return Err("mvp-orchestrator does not support mock provider".to_owned());
            }
        };
        let mounts = if self.provider == ProviderKind::Docker {
            self.cached_model
                .as_ref()
                .map(|cached_model| {
                    vec![ProviderMount {
                        host_path: cached_model.host_path.to_string_lossy().to_string(),
                        container_path: cached_model.container_path.clone(),
                        readonly: self.uses_planned_execution(),
                    }]
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        Ok(NodeProvisionSpec {
            run_id: self.run_id,
            node_id: logical_node_id,
            stage_index: Some(stage_index),
            image: self.image.clone(),
            env,
            args,
            mounts,
        })
    }
}

#[derive(Clone)]
struct RuntimeReady {
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    datastream_publisher: ActorAddress,
    stage_index: u32,
    readiness_id: u64,
    swim_node_id: DistNodeId,
}

#[derive(Clone)]
struct PromptRuntimeReady {
    first_stage: RuntimeReady,
    final_stage: RuntimeReady,
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

fn enqueue_datastream_subscribe(
    stack: &DistributionRuntimeStack,
    ready: &RuntimeReady,
    collector: &EndpointAddr,
    run_id: u64,
    node_id: u64,
) -> Result<(), String> {
    let mut flow_id = [0_u8; 16];
    flow_id[..8].copy_from_slice(&run_id.to_le_bytes());
    flow_id[8..].copy_from_slice(&node_id.to_le_bytes());
    stack
        .runtime
        .send_to(
            ready.datastream_publisher,
            DatastreamPublisherMsg::Subscribe(DatastreamSubscribe {
                collector: collector.clone(),
                request: SubscriptionRequest::all(),
                flow_id,
                token: Vec::new(),
            }),
        )
        .map_err(|e| format!("send datastream subscribe: {e}"))
}

#[allow(clippy::too_many_arguments)]
fn wait_for_runtime_ready_acks(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: ProviderKind,
    targets: &[RuntimeReadyAckTarget],
    collector_endpoint: &EndpointAddr,
) -> Result<(), String> {
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
    let started = Instant::now();
    let mut last_send = None::<Instant>;

    while !pending.is_empty() {
        pump(driver, stack, frame_tx);
        drain_orch_stdio_capture(
            orch_stdio_rx,
            orch_datastream,
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
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited {
                    status, node_id, ..
                } => {
                    return Err(format!("node {node_id} exited before ready: {status:?}"));
                }
            }
        }
        drain_frames(frame_rx, dashboard, orch_datastream);
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
            orch_datastream.emit_bootstrap(
                dashboard,
                run_id,
                orchestrator_node_id,
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
            return Ok(());
        }
        if started.elapsed() >= RUNTIME_READY_ACK_TIMEOUT {
            let pending_list = pending
                .keys()
                .map(|(node_id, stage_index, readiness_id)| {
                    format!("{node_id}/{stage_index}/{readiness_id}")
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "runtime_ready_ack timed out for node(s): {pending_list}"
            ));
        }
        if last_send.is_none_or(|sent_at| sent_at.elapsed() >= RUNTIME_READY_ACK_RETRY_INTERVAL) {
            for (key, target) in &pending {
                if stack.route_owner(target.ready.datastream_publisher)
                    == Some(target.ready.swim_node_id)
                    && let Err(error) = enqueue_datastream_subscribe(
                        stack,
                        &target.ready,
                        collector_endpoint,
                        run_id,
                        target.node_id,
                    )
                {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        orchestrator_node_id,
                        "datastream_subscribe",
                        "failed",
                        json!({"node_id":target.node_id,"error":error}),
                    );
                }
                enqueue_runtime_ready_ack(stack, &target.ready, run_id, target.node_id)?;
                let attempt = attempts.entry(*key).or_default();
                *attempt += 1;
                let attempt = *attempt;
                orch_datastream.emit_bootstrap(
                    dashboard,
                    run_id,
                    orchestrator_node_id,
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
            driver.drain_outbox(&stack.outbox);
            last_send = Some(Instant::now());
        }
        thread::sleep(PUMP_INTERVAL);
    }
    Ok(())
}

#[cfg(test)]
struct ProvisionedNodeGuard<'a> {
    provisioner: &'a mut dyn ProvisionPlugin,
    handle: Option<crate::provisioning::PluginNodeHandle>,
}

#[cfg(test)]
impl<'a> ProvisionedNodeGuard<'a> {
    fn new(
        provisioner: &'a mut dyn ProvisionPlugin,
        handle: crate::provisioning::PluginNodeHandle,
    ) -> Self {
        Self {
            provisioner,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) -> Result<(), String> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        self.provisioner.stop_node(&handle)
    }
}

#[cfg(test)]
impl Drop for ProvisionedNodeGuard<'_> {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct ProvisionedClusterGuard {
    provisioner: Box<dyn ProvisionPlugin>,
    handles: Vec<crate::provisioning::PluginNodeHandle>,
}

impl ProvisionedClusterGuard {
    fn new(
        provisioner: Box<dyn ProvisionPlugin>,
        handles: Vec<crate::provisioning::PluginNodeHandle>,
    ) -> Self {
        Self {
            provisioner,
            handles,
        }
    }

    fn complete_bootstrap_all(&mut self) -> Result<(), String> {
        for handle in &self.handles {
            self.provisioner.complete_bootstrap(handle)?;
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<(), String> {
        let mut first_error = None;
        while let Some(handle) = self.handles.pop() {
            if let Err(error) = self.provisioner.stop_node(&handle)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for ProvisionedClusterGuard {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn start_node_with_stdio_capture(
    provisioner: Box<dyn ProvisionPlugin>,
    node_spec: NodeProvisionSpec,
    sink: PluginSink,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    run_id: u64,
    node_id: u64,
) -> (
    Box<dyn ProvisionPlugin>,
    Result<crate::provisioning::PluginNodeHandle, String>,
) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut provisioner = provisioner;
        let result = provisioner.start_node(node_spec, sink);
        let _ = tx.send((provisioner, result));
    });

    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                drain_orch_stdio_capture(
                    orch_stdio_rx,
                    orch_datastream,
                    dashboard,
                    run_id,
                    node_id,
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return (
                    Box::new(FailedProvisionPlugin),
                    Err("provider start worker disconnected".to_owned()),
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_and_provision_workers(
    mut provisioner: Box<dyn ProvisionPlugin>,
    config: &Config,
    pipeline_plan: Option<&run_plan::RunPlan>,
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    sink: PluginSink,
    coordinator: EndpointAddr,
    pipeline_coordinator: EndpointAddr,
    orchestrator_actor: ActorAddress,
) -> Result<(ProvisionedClusterGuard, PromptRuntimeReady), String> {
    let stage_specs = stage_node_specs(
        config,
        pipeline_plan,
        coordinator.clone(),
        orchestrator_actor,
    )?;
    let expected_node_ids = stage_specs
        .iter()
        .map(|spec| spec.node_id)
        .collect::<Vec<_>>();
    orch_datastream.emit_bootstrap(
        dashboard,
        config.run_id,
        config.node_id,
        "node_spec",
        "ready",
        json!({
            "provider":config.provider.as_str(),
            "image":&config.image,
            "relay_mode":relay_mode_env_value(&config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "docker_gpus":if config.provider == ProviderKind::Docker { Some(config.docker_gpus.as_str()) } else { None },
            "provider_config":config.provider_datastream_detail(),
            "env_keys":config.node_spec_env_keys(),
            "worker_count":stage_specs.len(),
            "worker_node_ids":expected_node_ids,
        }),
    );

    let mut handles = Vec::with_capacity(stage_specs.len());
    for node_spec in stage_specs {
        orch_datastream.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: config.run_id,
                node_id: node_spec.node_id,
                kind: ProvisionEventKind::ProvisionStart,
                provider: Some(config.provider.as_str().to_owned()),
                message: Some(format!(
                    "starting {} image {}",
                    config.provider.as_str(),
                    config.image
                )),
            },
        );
        orch_datastream.emit_bootstrap(
            dashboard,
            config.run_id,
            config.node_id,
            "provider_start",
            "started",
            json!({
                "provider":config.provider.as_str(),
                "image":&config.image,
                "node_id":node_spec.node_id,
                "stage_index":node_spec.stage_index,
            }),
        );
        let (returned_provisioner, handle_result) = start_node_with_stdio_capture(
            provisioner,
            node_spec.clone(),
            sink.clone(),
            orch_stdio_rx,
            dashboard,
            orch_datastream,
            config.run_id,
            node_spec.node_id,
        );
        provisioner = returned_provisioner;
        match handle_result {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                orch_datastream.emit_bootstrap(
                    dashboard,
                    config.run_id,
                    config.node_id,
                    "provider_start",
                    "failed",
                    json!({"provider":config.provider.as_str(),"node_id":node_spec.node_id,"error":error}),
                );
                stop_started_nodes(&mut *provisioner, &mut handles);
                drain_orch_stdio_capture(
                    orch_stdio_rx,
                    orch_datastream,
                    dashboard,
                    config.run_id,
                    config.node_id,
                );
                return Err(error);
            }
        }
    }
    let mut provisioned_nodes = ProvisionedClusterGuard::new(provisioner, handles);
    drain_orch_stdio_capture(
        orch_stdio_rx,
        orch_datastream,
        dashboard,
        config.run_id,
        config.node_id,
    );

    orch_datastream.emit_bootstrap(
        dashboard,
        config.run_id,
        config.node_id,
        "node_runtime_ready",
        "started",
        json!({"worker_count":expected_node_ids.len(),"node_ids":expected_node_ids}),
    );
    let readies = if pipeline_plan.is_some() {
        match wait_for_runtime_readies(
            driver,
            stack,
            obs_rx,
            frame_rx,
            frame_tx,
            orchestrator_reports,
            stop_rx,
            dashboard,
            orch_datastream,
            orch_stdio_rx,
            config.run_id,
            &expected_node_ids,
            config.provider,
        ) {
            Ok(readies) => readies,
            Err(error) => {
                orch_datastream.emit_bootstrap(
                    dashboard,
                    config.run_id,
                    config.node_id,
                    "node_runtime_ready",
                    "failed",
                    json!({"error":error}),
                );
                return Err(error);
            }
        }
    } else {
        let ready = match wait_for_runtime_ready(
            driver,
            stack,
            obs_rx,
            frame_rx,
            frame_tx,
            orchestrator_reports,
            stop_rx,
            dashboard,
            orch_datastream,
            orch_stdio_rx,
            config.run_id,
            config.node_id,
            config.provider,
        ) {
            Ok(ready) => ready,
            Err(error) => {
                orch_datastream.emit_bootstrap(
                    dashboard,
                    config.run_id,
                    config.node_id,
                    "node_runtime_ready",
                    "failed",
                    json!({"error":error}),
                );
                return Err(error);
            }
        };
        BTreeMap::from([(config.node_id, ready)])
    };
    for (node_id, ready) in &readies {
        orch_datastream.emit_bootstrap(
            dashboard,
            config.run_id,
            config.node_id,
            "node_runtime_ready",
            "ready",
            json!({"endpoint":&ready.endpoint,"node_actor":ready.node_actor,"node_id":node_id,"stage_index":ready.stage_index}),
        );
    }
    provisioned_nodes.complete_bootstrap_all()?;
    let ack_targets = readies
        .iter()
        .map(|(node_id, ready)| RuntimeReadyAckTarget {
            node_id: *node_id,
            ready: ready.clone(),
        })
        .collect::<Vec<_>>();
    wait_for_runtime_ready_acks(
        driver,
        stack,
        obs_rx,
        frame_rx,
        frame_tx,
        orchestrator_reports,
        stop_rx,
        dashboard,
        orch_datastream,
        orch_stdio_rx,
        config.run_id,
        config.node_id,
        config.provider,
        &ack_targets,
        &pipeline_coordinator,
    )?;

    orch_datastream.emit_bootstrap(
        dashboard,
        config.run_id,
        config.node_id,
        "stage_provision",
        "started",
        stage_provision_detail(config, pipeline_plan),
    );
    if pipeline_plan.is_none() {
        let ready = readies
            .get(&config.node_id)
            .ok_or_else(|| "missing runtime-ready node for single-stage run".to_owned())?;
        provision_stage(stack, ready.node_actor, config)?;
    }

    orch_datastream.emit_bootstrap(
        dashboard,
        config.run_id,
        config.node_id,
        "weights_loaded",
        "started",
        json!({"model_id":&config.model_id,"expected":expected_node_ids.len()}),
    );
    let weights_result = if pipeline_plan.is_some() {
        wait_for_weights_loaded_count(
            driver,
            stack,
            obs_rx,
            frame_rx,
            frame_tx,
            orchestrator_reports,
            stop_rx,
            dashboard,
            orch_datastream,
            orch_stdio_rx,
            config.run_id,
            config.node_id,
            config.provider,
            expected_node_ids.len(),
            pipeline_plan.expect("pipeline mode requires plan"),
            &readies,
            &pipeline_coordinator,
        )
    } else {
        wait_for_weights_loaded(
            driver,
            stack,
            obs_rx,
            frame_rx,
            frame_tx,
            orchestrator_reports,
            stop_rx,
            dashboard,
            orch_datastream,
            orch_stdio_rx,
            config.run_id,
            config.node_id,
            config.provider,
            config.stage_index,
        )
    };
    match weights_result {
        Ok(()) => orch_datastream.emit_bootstrap(
            dashboard,
            config.run_id,
            config.node_id,
            "weights_loaded",
            "ready",
            json!({"source":"actor_stage_ready","model_id":&config.model_id,"expected":expected_node_ids.len()}),
        ),
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard,
                config.run_id,
                config.node_id,
                "weights_loaded",
                "failed",
                json!({"error":error}),
            );
            drain_orch_stdio_capture(
                orch_stdio_rx,
                orch_datastream,
                dashboard,
                config.run_id,
                config.node_id,
            );
            return Err(error);
        }
    }

    let prompt_ready = if let Some(plan) = pipeline_plan {
        let first_node_id = plan
            .stages
            .iter()
            .find(|stage| stage.stage_index == 0)
            .map(|stage| stage.node_id.0)
            .ok_or_else(|| "pipeline plan missing stage 0".to_owned())?;
        let final_node_id = plan
            .stages
            .iter()
            .find(|stage| stage.stage_index + 1 == stage.stage_count)
            .map(|stage| stage.node_id.0)
            .ok_or_else(|| "pipeline plan missing final stage".to_owned())?;
        PromptRuntimeReady {
            first_stage: readies
                .get(&first_node_id)
                .cloned()
                .ok_or_else(|| "missing stage 0 runtime-ready node".to_owned())?,
            final_stage: readies
                .get(&final_node_id)
                .cloned()
                .ok_or_else(|| "missing final-stage runtime-ready node".to_owned())?,
        }
    } else {
        let ready = readies
            .get(&config.node_id)
            .cloned()
            .ok_or_else(|| "missing single-stage runtime-ready node".to_owned())?;
        PromptRuntimeReady {
            first_stage: ready.clone(),
            final_stage: ready,
        }
    };
    Ok((provisioned_nodes, prompt_ready))
}

fn stage_node_specs(
    config: &Config,
    pipeline_plan: Option<&run_plan::RunPlan>,
    coordinator: EndpointAddr,
    orchestrator_actor: ActorAddress,
) -> Result<Vec<NodeProvisionSpec>, String> {
    if let Some(plan) = pipeline_plan {
        let mut stages = plan.stages.clone();
        stages.sort_by_key(|stage| stage.stage_index);
        stages
            .into_iter()
            .map(|stage| {
                config.node_spec_for_stage(
                    coordinator.clone(),
                    orchestrator_actor,
                    stage.node_id.0,
                    stage.stage_index,
                )
            })
            .collect()
    } else {
        Ok(vec![config.node_spec(coordinator, orchestrator_actor)?])
    }
}

fn stop_started_nodes(
    provisioner: &mut dyn ProvisionPlugin,
    handles: &mut Vec<crate::provisioning::PluginNodeHandle>,
) {
    while let Some(handle) = handles.pop() {
        let _ = provisioner.stop_node(&handle);
    }
}

fn stage_provision_detail(config: &Config, pipeline_plan: Option<&run_plan::RunPlan>) -> Value {
    if let Some(plan) = pipeline_plan {
        json!({
            "run_id":config.run_id,
            "stage_count":plan.stages.len(),
            "stages":plan.stages.iter().map(|stage| {
                json!({
                    "node_id":stage.node_id.0,
                    "stage_index":stage.stage_index,
                    "layer_range":{"start":stage.layer_start,"end_exclusive":stage.layer_end_exclusive},
                    "inbound_edge_id":stage.inbound_edge.0,
                    "outbound_edge_id":stage.outbound_edge.0,
                })
            }).collect::<Vec<_>>(),
            "model_id":&config.model_id,
        })
    } else {
        json!({
            "run_id":config.run_id,
            "node_id":config.node_id,
            "stage_index":config.stage_index,
            "stage_count":1,
            "layer_range":{"start":0,"end_exclusive":config.layer_end_exclusive},
            "model_id":&config.model_id,
        })
    }
}

fn stage_provision_wire_from_plan(
    plan: &run_plan::RunPlan,
    stage_index: u32,
    readies: &BTreeMap<u64, RuntimeReady>,
    coordinator: &EndpointAddr,
) -> Result<StageProvisionWire, String> {
    let provision = run_plan::derive_stage_provision(plan, stage_index)
        .map_err(|e| format!("derive stage {stage_index} provision: {e:?}"))?;
    Ok(StageProvisionWire {
        run_id: provision.run_id.0,
        authorized_orchestrator: 0,
        node_id: provision.node_id.0,
        stage_index: provision.stage_index,
        stage_count: provision.stage_count,
        layer_start: provision.layer_start,
        layer_end_exclusive: provision.layer_end_exclusive,
        inbound_edge_id: provision.inbound.edge_id.0,
        outbound_edge_id: provision.outbound.edge_id.0,
        inbound_edge: Some(StageInboundEdgeWire {
            edge_id: provision.inbound.edge_id.0,
            kind: stage_edge_kind_wire(provision.inbound.kind),
            object_spec: stage_object_spec_wire(provision.inbound.object_spec),
            ring_spec: stage_ring_spec_wire(provision.inbound.ring_spec),
        }),
        outbound_edge: Some(StageOutboundEdgeWire {
            edge_id: provision.outbound.edge_id.0,
            kind: stage_edge_kind_wire(provision.outbound.kind),
            consumer_node_id: provision.outbound.consumer_node_id.0,
            consumer_endpoint: stage_consumer_endpoint(
                provision.outbound.consumer_node_id.0,
                plan,
                readies,
                coordinator,
            )?,
            object_spec: stage_object_spec_wire(provision.outbound.object_spec),
            ring_spec: stage_ring_spec_wire(provision.outbound.ring_spec),
        }),
        model_id: provision.model.model_id,
        gguf_source: provision.gguf_source,
        tokenizer: provision.tokenizer,
    })
}

fn provision_stage_from_plan(
    stack: &DistributionRuntimeStack,
    node_actor: ActorAddress,
    plan: &run_plan::RunPlan,
    stage_index: u32,
    readies: &BTreeMap<u64, RuntimeReady>,
    coordinator: &EndpointAddr,
) -> Result<(), String> {
    let provision = stage_provision_wire_from_plan(plan, stage_index, readies, coordinator)?;
    stack
        .runtime
        .send_to(node_actor, NodeAgentMsg::ProvisionStage(provision))
        .map_err(|e| format!("send stage {stage_index} provision: {e}"))
}

fn stage_consumer_endpoint(
    consumer_node_id: u64,
    plan: &run_plan::RunPlan,
    readies: &BTreeMap<u64, RuntimeReady>,
    coordinator: &EndpointAddr,
) -> Result<Option<EndpointAddr>, String> {
    if consumer_node_id
        == plan.stages.first().map_or(0, |stage| {
            plan.edges
                .iter()
                .find(|edge| edge.kind == run_plan::EdgeKind::TokenIn)
                .and_then(|edge| match edge.producer {
                    run_plan::EdgeEndpoint::Orchestrator { node_id } => Some(node_id.0),
                    run_plan::EdgeEndpoint::Stage { .. } => None,
                })
                .unwrap_or(stage.node_id.0)
        })
    {
        return Ok(Some(coordinator.clone()));
    }
    readies
        .get(&consumer_node_id)
        .map(|ready| Some(ready.endpoint.clone()))
        .ok_or_else(|| {
            format!("missing runtime-ready endpoint for consumer node {consumer_node_id}")
        })
}

fn stage_edge_kind_wire(kind: run_plan::EdgeKind) -> StageEdgeKindWire {
    match kind {
        run_plan::EdgeKind::TokenIn => StageEdgeKindWire::TokenIn,
        run_plan::EdgeKind::Activation => StageEdgeKindWire::Activation,
        run_plan::EdgeKind::TokenOut => StageEdgeKindWire::TokenOut,
    }
}

fn stage_object_spec_wire(spec: run_plan::ObjectSpec) -> StageObjectSpecWire {
    StageObjectSpecWire {
        max_extent: spec.max_extent,
        alignment: spec.alignment,
    }
}

fn stage_ring_spec_wire(spec: run_plan::RingSpec) -> StageRingSpecWire {
    StageRingSpecWire {
        data_capacity: spec.data_capacity,
        alignment: spec.alignment,
    }
}

#[allow(clippy::too_many_arguments)]
fn wait_for_runtime_readies(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    expected_node_ids: &[u64],
    provider: ProviderKind,
) -> Result<BTreeMap<u64, RuntimeReady>, String> {
    let expected = expected_node_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut pending = BTreeMap::<u64, RuntimeReady>::new();
    let started = Instant::now();
    loop {
        pump(driver, stack, frame_tx);
        emit_swim_transitions(
            orch_datastream,
            dashboard,
            run_id,
            expected_node_ids.first().copied().unwrap_or(0),
            stack,
        );
        drain_frames(frame_rx, dashboard, orch_datastream);
        drain_orch_stdio_capture(
            orch_stdio_rx,
            orch_datastream,
            dashboard,
            run_id,
            expected_node_ids.first().copied().unwrap_or(0),
        );
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for pipeline nodes ready".to_owned());
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited {
                    status, node_id, ..
                } => {
                    return Err(format!("node {node_id} exited before ready: {status:?}"));
                }
            }
        }
        while let Some(report) = orchestrator_reports.try_recv() {
            if let OrchestratorReport::NodeRuntimeReady {
                run_id: report_run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                datastream_publisher,
                readiness_id,
            } = report
                && report_run_id == run_id
                && expected.contains(&node_id)
            {
                pending.insert(
                    node_id,
                    RuntimeReady {
                        endpoint: endpoint.clone(),
                        node_actor,
                        datastream_publisher,
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
        if started.elapsed() >= RUNTIME_READY_TIMEOUT {
            let pending_list = expected
                .iter()
                .filter(|node_id| {
                    pending
                        .get(node_id)
                        .is_none_or(|ready| !runtime_ready_barrier_met(stack, ready))
                })
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "runtime_ready timed out for node(s): {pending_list}"
            ));
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

#[allow(clippy::too_many_arguments)]
fn wait_for_weights_loaded_count(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    provider: ProviderKind,
    expected_count: usize,
    pipeline_plan: &run_plan::RunPlan,
    readies: &BTreeMap<u64, RuntimeReady>,
    pipeline_coordinator: &EndpointAddr,
) -> Result<(), String> {
    let expected_stages = pipeline_plan
        .stages
        .iter()
        .map(|stage| stage.stage_index)
        .collect::<BTreeSet<_>>();
    let mut loaded_stages = BTreeSet::<u32>::new();
    let mut last_resend = Instant::now();
    let mut active_stage = None::<u32>;
    let mut resend_attempt = 0_u64;
    let mut stage_resend_counts = BTreeMap::<u32, u64>::new();
    loop {
        pump(driver, stack, frame_tx);
        emit_swim_transitions(orch_datastream, dashboard, run_id, node_id, stack);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for pipeline weights loaded".to_owned());
        }
        if loaded_stages.len() >= expected_count {
            return Ok(());
        }
        if active_stage.map_or(true, |stage_index| loaded_stages.contains(&stage_index)) {
            active_stage =
                next_pipeline_weight_load_stage(pipeline_plan, &loaded_stages, active_stage)
                    .map(|stage| stage.stage_index);
            last_resend = Instant::now() - Duration::from_secs(1);
        }
        if last_resend.elapsed() >= Duration::from_secs(1) {
            resend_attempt += 1;
            let Some(stage) =
                next_pipeline_weight_load_stage(pipeline_plan, &loaded_stages, active_stage)
            else {
                return Err(format!(
                    "missing unloaded pipeline weight stage; loaded {} of {expected_count}",
                    loaded_stages.len()
                ));
            };
            let stage_node_id = stage.node_id.0;
            let stage_send_count = {
                let count = stage_resend_counts.entry(stage.stage_index).or_default();
                *count += 1;
                *count
            };
            let emit_wait_headline = stage_send_count == 1 || stage_send_count % 15 == 0;
            let ready = readies.get(&stage_node_id).ok_or_else(|| {
                format!("missing runtime-ready node for stage {}", stage.stage_index)
            })?;
            orch_datastream.emit_bootstrap(
                dashboard,
                run_id,
                node_id,
                "stage_provision_send",
                "sent",
                json!({
                    "attempt":resend_attempt,
                    "stage_count":pipeline_plan.stages.len(),
                    "stage_index":stage.stage_index,
                    "stage_send_count":stage_send_count,
                    "loaded_stage_count":loaded_stages.len()
                }),
            );
            let route_owner = stack.route_owner(ready.node_actor);
            let member_state = stack.member_state(ready.swim_node_id);
            orch_datastream.emit_bootstrap_to_channel(
                dashboard,
                MVP_STAGE_ROUTE,
                run_id,
                node_id,
                "stage_route_check",
                "observed",
                json!({
                    "attempt":resend_attempt,
                    "stage_index":stage.stage_index,
                    "stage_node_id":stage_node_id,
                    "node_actor":ready.node_actor,
                    "datastream_publisher":ready.datastream_publisher,
                    "swim_node_id":format!("{:?}", ready.swim_node_id),
                    "member_state":member_state.map(|state| format!("{:?}", state)),
                    "route_owner":route_owner.map(|owner| format!("{:?}", owner)),
                    "datastream_route_owner":stack.route_owner(ready.datastream_publisher).map(|owner| format!("{:?}", owner)),
                    "route_matches_ready":route_owner == Some(ready.swim_node_id),
                }),
            );
            if emit_wait_headline {
                orch_datastream.emit_bootstrap(
                    dashboard,
                    run_id,
                    node_id,
                    "stage_provision_wait",
                    "observed",
                    json!({
                        "attempt":resend_attempt,
                        "stage_count":pipeline_plan.stages.len(),
                        "stage_index":stage.stage_index,
                        "stage_node_id":stage_node_id,
                        "stage_send_count":stage_send_count,
                        "loaded_stage_count":loaded_stages.len(),
                        "message":format!(
                            "loaded {} of {}; waiting on stage {}",
                            loaded_stages.len(),
                            pipeline_plan.stages.len(),
                            stage.stage_index
                        )
                    }),
                );
            }
            provision_stage_from_plan(
                stack,
                ready.node_actor,
                pipeline_plan,
                stage.stage_index,
                readies,
                pipeline_coordinator,
            )?;
            pump(driver, stack, frame_tx);
            last_resend = Instant::now();
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited {
                    node_id, status, ..
                } => {
                    return Err(format!(
                        "node {node_id} exited while loading weights: {status:?}"
                    ));
                }
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
            }
        }
        drain_frames(frame_rx, dashboard, orch_datastream);
        while let Some(report) = orchestrator_reports.try_recv() {
            match report {
                OrchestratorReport::WeightsReady {
                    run_id: report_run_id,
                    node_id: _,
                    stage_index,
                } if report_run_id == run_id && expected_stages.contains(&stage_index) => {
                    loaded_stages.insert(stage_index);
                    if active_stage == Some(stage_index) {
                        active_stage = None;
                        last_resend = Instant::now() - Duration::from_secs(1);
                    }
                    if loaded_stages.len() >= expected_count {
                        return Ok(());
                    }
                }
                OrchestratorReport::StageFault {
                    run_id: report_run_id,
                    stage_index,
                } if report_run_id == run_id && expected_stages.contains(&stage_index) => {
                    return Err(format!(
                        "stage {stage_index} faulted while loading pipeline weights"
                    ));
                }
                _ => {}
            }
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn next_pipeline_weight_load_stage<'a>(
    pipeline_plan: &'a run_plan::RunPlan,
    loaded_stages: &BTreeSet<u32>,
    active_stage: Option<u32>,
) -> Option<&'a run_plan::StagePlan> {
    if let Some(stage_index) = active_stage {
        if !loaded_stages.contains(&stage_index) {
            if let Some(stage) = pipeline_plan
                .stages
                .iter()
                .find(|stage| stage.stage_index == stage_index)
            {
                return Some(stage);
            }
        }
    }
    pipeline_plan
        .stages
        .iter()
        .filter(|stage| !loaded_stages.contains(&stage.stage_index))
        .max_by_key(|stage| stage.stage_index)
}

struct FailedProvisionPlugin;

impl ProvisionPlugin for FailedProvisionPlugin {
    fn start_node(
        &mut self,
        _spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<crate::provisioning::PluginNodeHandle, String> {
        Err("provider start worker disconnected".to_owned())
    }

    fn complete_bootstrap(
        &mut self,
        _handle: &crate::provisioning::PluginNodeHandle,
    ) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, _handle: &crate::provisioning::PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }
}

struct PromptWork {
    request: SubmitPrompt,
    events: mpsc::Sender<PromptEvent>,
}

struct ActivePrompt {
    request: SubmitPrompt,
    events: mpsc::Sender<PromptEvent>,
}

#[derive(Clone, Debug)]
struct CollectedDatastreamFrame {
    stream: StreamId,
    channel_name: String,
    frame: Frame,
}

fn drain_datastream_connections(
    driver: &mut IrohDriver,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
) {
    driver.pump_datastream_ingress();
    for read in driver.drain_datastream_reads() {
        let mut channels = read
            .header
            .channels
            .iter()
            .map(|descriptor| {
                (
                    ChannelRef {
                        stream: descriptor.stream.clone(),
                        channel: descriptor.id,
                    },
                    descriptor.name.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for event in read.events {
            match event {
                DatastreamEvent::ChannelDeclared(descriptor) => {
                    channels.insert(
                        ChannelRef {
                            stream: descriptor.stream.clone(),
                            channel: descriptor.id,
                        },
                        descriptor.name,
                    );
                }
                DatastreamEvent::Frame(delivery) => {
                    let channel_name = channels
                        .get(&delivery.channel)
                        .cloned()
                        .unwrap_or_else(|| format!("channel#{}", delivery.channel.channel.0));
                    let frame = Frame::new(
                        delivery.channel.channel,
                        delivery.position,
                        delivery.payload,
                    );
                    if frame_tx
                        .send(CollectedDatastreamFrame {
                            stream: delivery.channel.stream,
                            channel_name,
                            frame,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                DatastreamEvent::StreamDeclared(_) | DatastreamEvent::StreamEnded(_) => {}
            }
        }
    }
}

struct FrameArchive {
    file: File,
    next_seq: u64,
}

impl FrameArchive {
    fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!("create datastream frame log dir {}: {e}", parent.display())
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open datastream frame log {}: {e}", path.display()))?;
        Ok(Self { file, next_seq: 0 })
    }

    fn record(&mut self, source: &str, stream: &StreamId, channel: &str, frame: &Frame) {
        let payload = match std::str::from_utf8(&frame.payload) {
            Ok(text) => json!({"encoding":"utf8","value":text}),
            Err(_) => json!({"encoding":"bytes","value":frame.payload}),
        };
        let record = json!({
            "arrival_seq":self.next_seq,
            "arrival_unix_ms":benchmark_observability::unix_ms_now(),
            "source":source,
            "stream":stream.to_string(),
            "channel":channel,
            "channel_id":frame.channel.0,
            "position":frame.position.0,
            "payload":payload,
        });
        self.next_seq += 1;
        let _ = serde_json::to_writer(&mut self.file, &record);
        let _ = writeln!(self.file);
        let _ = self.file.flush();
    }
}

struct OrchDatastream {
    stream: StreamId,
    endpoint: DatastreamEndpoint,
    producer: DatastreamProducer,
    channels: BTreeMap<String, ChannelId>,
    channel_names: BTreeMap<ChannelId, String>,
    archive: Option<FrameArchive>,
}

impl OrchDatastream {
    fn new(run_id: u64, frame_log: Option<&Path>) -> Result<Self, String> {
        let stream = StreamId::new(NodeId::new("mvp-orchestrator"), Lifetime(run_id));
        let endpoint = DatastreamEndpoint::with_descriptor(
            StreamDescriptor {
                stream: stream.clone(),
                label: Some("mvp orchestrator".to_owned()),
                origin: StreamOrigin::Orchestrator,
            },
            4096,
            1024,
        );
        let producer = endpoint.producer();
        let mut out = Self {
            stream,
            endpoint,
            producer,
            channels: BTreeMap::new(),
            channel_names: BTreeMap::new(),
            archive: frame_log.map(FrameArchive::open).transpose()?,
        };
        for name in [
            MVP_PROVISIONING_EVENTS,
            MVP_ORCH_BOOTSTRAP,
            MVP_ORCH_PROMPT,
            MVP_SWIM_MEMBERSHIP,
            MVP_STAGE_ROUTE,
        ] {
            out.channel_by_name(name);
        }
        Ok(out)
    }

    fn channel_by_name(&mut self, name: &str) -> ChannelId {
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

    fn emit_event(&mut self, dashboard: Option<&DashboardSupport>, event: ProvisionEvent) {
        let payload = serde_json::to_vec(&MvpProvisionEventRecord::new(event))
            .expect("serialize provisioning event");
        self.emit_bytes(dashboard, MVP_PROVISIONING_EVENTS, payload);
    }

    fn emit_log(&mut self, dashboard: Option<&DashboardSupport>, line: ProvisionLogLine) {
        let channel = mvp_provision_log_channel(line.node_id, line.stream);
        let payload =
            serde_json::to_vec(&MvpProvisionLogRecord::new(line)).expect("serialize provision log");
        self.emit_bytes(dashboard, &channel, payload);
    }

    fn emit_bootstrap(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        run_id: u64,
        node_id: u64,
        phase: &str,
        status: &str,
        detail: Value,
    ) {
        self.emit_bootstrap_to_channel(
            dashboard,
            MVP_ORCH_BOOTSTRAP,
            run_id,
            node_id,
            phase,
            status,
            detail,
        );
    }

    fn emit_bootstrap_to_channel(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: &str,
        run_id: u64,
        node_id: u64,
        phase: &str,
        status: &str,
        detail: Value,
    ) {
        let payload = serde_json::to_vec(&json!({
            "type":"OrchBootstrap",
            "phase":phase,
            "status":status,
            "run_id":run_id,
            "node_id":node_id,
            "benchmark":benchmark_observability::stamp("mvp-orchestrator"),
            "detail":detail,
        }))
        .expect("serialize orch bootstrap event");
        self.emit_bytes(dashboard, channel, payload);
    }

    fn emit_prompt(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        run_id: u64,
        node_id: u64,
        request_id: u64,
        phase: &str,
        status: &str,
        detail: Value,
    ) {
        let payload = serde_json::to_vec(&json!({
            "type":"OrchPromptEvent",
            "phase":phase,
            "status":status,
            "run_id":run_id,
            "node_id":node_id,
            "request_id":request_id,
            "benchmark":benchmark_observability::stamp("mvp-orchestrator"),
            "detail":detail,
        }))
        .expect("serialize orch prompt event");
        self.emit_bytes(dashboard, MVP_ORCH_PROMPT, payload);
    }

    fn emit_bytes(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: &str,
        payload: Vec<u8>,
    ) {
        self.emit_bytes_from(dashboard, channel, payload, "orchestrator");
    }

    fn emit_bytes_from(
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

    fn flush(&mut self, dashboard: Option<&DashboardSupport>, source: &str) {
        let stream = self.stream.clone();
        for frame in self.endpoint.mux().drain() {
            let channel = self
                .channel_names
                .get(&frame.channel)
                .cloned()
                .unwrap_or_else(|| format!("channel#{}", frame.channel.0));
            ingest_dashboard_frame(dashboard, &stream, &channel, &frame);
            self.archive_frame(source, &stream, &channel, &frame);
        }
    }

    fn archive_frame(&mut self, source: &str, stream: &StreamId, channel: &str, frame: &Frame) {
        if let Some(archive) = &mut self.archive {
            archive.record(source, stream, channel, frame);
        }
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

fn install_orch_stdio_capture() -> Result<Option<mpsc::Receiver<OrchStdioLine>>, String> {
    OrchStdioCapture::install()
}

fn drain_orch_stdio_capture(
    rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    datastream: &mut OrchDatastream,
    dashboard: Option<&DashboardSupport>,
    run_id: u64,
    node_id: u64,
) {
    let Some(rx) = rx else {
        return;
    };
    while let Ok(line) = rx.try_recv() {
        datastream.emit_log(
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

#[cfg(feature = "dashboard")]
struct DashboardSupport {
    handle: dashboard::DashboardHandle,
    _runtime: tokio::runtime::Runtime,
}

#[cfg(feature = "dashboard")]
impl DashboardSupport {
    fn start(enabled: bool) -> Result<Option<Self>, String> {
        if !enabled {
            return Ok(None);
        }
        let mut config = dashboard::DashboardConfig::default();
        if let Some(port) = env_optional("MVP_DASHBOARD_PORT") {
            config.port = port
                .parse::<u16>()
                .map_err(|e| format!("invalid MVP_DASHBOARD_PORT={port:?}: {e}"))?;
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("dashboard runtime: {e}"))?;
        let handle = dashboard::DashboardHandle::new(config);
        handle.register_view(Arc::new(MvpClusterDashboardView::new()));
        handle.spawn_http(runtime.handle());
        Ok(Some(Self {
            handle,
            _runtime: runtime,
        }))
    }

    fn publish_frame(&self, stream: &StreamId, channel: &str, frame: &Frame) {
        self.handle.publish(dashboard::FrameEvent {
            stream: dashboard::StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
            channel: channel.to_owned(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        });
    }
}

#[cfg(not(feature = "dashboard"))]
struct DashboardSupport;

#[cfg(not(feature = "dashboard"))]
impl DashboardSupport {
    fn start(enabled: bool) -> Result<Option<Self>, String> {
        if enabled {
            return Err(
                "MVP_DASHBOARD requires building mvp-system with feature dashboard".to_owned(),
            );
        }
        Ok(None)
    }

    fn publish_frame(&self, _stream: &StreamId, _channel: &str, _frame: &Frame) {}
}

struct ChannelObservationSink {
    tx: Mutex<mpsc::Sender<PluginObservation>>,
}

impl PluginObservationSink for ChannelObservationSink {
    fn observe(&self, observation: PluginObservation) {
        let _ = self.tx.lock().send(observation);
    }
}

fn spawn_prompt_rpc(
    bind: SocketAddr,
    work_tx: mpsc::Sender<PromptWork>,
    default_max_tokens: u32,
) -> Result<SocketAddr, String> {
    let listener = TcpListener::bind(bind).map_err(|e| format!("bind prompt RPC {bind}: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("read prompt RPC addr: {e}"))?;
    thread::spawn(move || {
        for accepted in listener.incoming() {
            match accepted {
                Ok(stream) => {
                    let tx = work_tx.clone();
                    thread::spawn(move || {
                        let _ = handle_prompt_connection(stream, tx, default_max_tokens);
                    });
                }
                Err(_) => break,
            }
        }
    });
    Ok(addr)
}
fn handle_prompt_connection(
    stream: TcpStream,
    work_tx: mpsc::Sender<PromptWork>,
    default_max_tokens: u32,
) -> Result<(), String> {
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|e| format!("clone prompt stream: {e}"))?,
    );
    let mut writer = stream;
    loop {
        let request = match read_submit_prompt(&mut reader) {
            Ok(Some(request)) => request,
            Ok(None) => break,
            Err(error) if error.contains("expected value at line 1 column 1") => break,
            Err(error) => return Err(error),
        };
        let request = request.with_defaults(default_max_tokens);
        let (event_tx, event_rx) = mpsc::channel();
        work_tx
            .send(PromptWork {
                request,
                events: event_tx,
            })
            .map_err(|_| "prompt loop stopped".to_owned())?;
        for event in event_rx {
            let terminal = event.is_terminal();
            write_json_line(&mut writer, &event)?;
            if terminal {
                break;
            }
        }
    }
    Ok(())
}

fn wait_for_runtime_ready(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    provider: ProviderKind,
) -> Result<RuntimeReady, String> {
    let mut pending_ready: Option<RuntimeReady> = None;
    let mut node_swim_started = false;
    let mut node_swim_ready = false;
    let mut node_route_started = false;
    let started = Instant::now();
    loop {
        pump(driver, stack, frame_tx);
        drain_frames(frame_rx, dashboard, orch_datastream);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for node ready".to_owned());
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited { status, .. } => {
                    return Err(format!("node exited before ready: {status:?}"));
                }
            }
        }
        while let Some(report) = orchestrator_reports.try_recv() {
            if let OrchestratorReport::NodeRuntimeReady {
                run_id: report_run_id,
                node_id: report_node_id,
                stage_index,
                endpoint,
                node_actor,
                datastream_publisher,
                readiness_id,
            } = report
            {
                if report_run_id == run_id && report_node_id == node_id {
                    let reset_progress = pending_ready
                        .as_ref()
                        .map(|ready| ready.readiness_id != readiness_id)
                        .unwrap_or(true);
                    if reset_progress {
                        node_swim_started = false;
                        node_swim_ready = false;
                        node_route_started = false;
                    }
                    let swim_node_id = DistNodeId(*endpoint.id.as_bytes());
                    pending_ready = Some(RuntimeReady {
                        endpoint,
                        node_actor,
                        datastream_publisher,
                        stage_index,
                        readiness_id,
                        swim_node_id,
                    });
                }
            }
        }
        if let Some(ready) = pending_ready.as_ref() {
            if runtime_ready_barrier_met(stack, ready) {
                return Ok(ready.clone());
            }
            let swim_ready = stack.member_state(ready.swim_node_id) == Some(MemberState::Alive);
            let route_ready = stack.route_owner(ready.node_actor) == Some(ready.swim_node_id);
            if !swim_ready {
                if !node_swim_started {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        node_id,
                        "node_swim",
                        "started",
                        json!({"node":format!("{:?}", ready.swim_node_id),"readiness_id":ready.readiness_id}),
                    );
                    node_swim_started = true;
                }
            } else if !route_ready {
                if !node_swim_ready {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        node_id,
                        "node_swim",
                        "ready",
                        json!({"node":format!("{:?}", ready.swim_node_id),"readiness_id":ready.readiness_id}),
                    );
                    node_swim_ready = true;
                }
                if !node_route_started {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        node_id,
                        "node_route",
                        "started",
                        json!({"node_actor":ready.node_actor,"node":format!("{:?}", ready.swim_node_id),"readiness_id":ready.readiness_id}),
                    );
                    node_route_started = true;
                }
            }
        }
        if started.elapsed() >= RUNTIME_READY_TIMEOUT {
            let detail = pending_ready.as_ref().map(|ready| {
                json!({
                    "readiness_id":ready.readiness_id,
                    "swim_alive":stack.member_state(ready.swim_node_id) == Some(MemberState::Alive),
                    "route_owner":stack.route_owner(ready.node_actor).map(|node| format!("{node:?}")),
                })
            });
            return Err(format!(
                "runtime_ready timed out for node {node_id}: {detail:?}"
            ));
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn provision_stage(
    stack: &DistributionRuntimeStack,
    node_actor: ActorAddress,
    config: &Config,
) -> Result<(), String> {
    stack
        .runtime
        .send_to(
            node_actor,
            NodeAgentMsg::ProvisionStage({
                let layer_end_exclusive = config.layer_end_exclusive.ok_or_else(|| {
                    "unplanned single-stage execution requires --layer-end-exclusive or MVP_LAYER_END_EXCLUSIVE; cached-model runs use metadata-derived planning".to_owned()
                })?;
                StageProvisionWire {
                    run_id: config.run_id,
                    authorized_orchestrator: 0,
                    node_id: config.node_id,
                    stage_index: config.stage_index,
                    stage_count: 1,
                    layer_start: 0,
                    layer_end_exclusive,
                    inbound_edge_id: 1,
                    outbound_edge_id: 2,
                    inbound_edge: None,
                    outbound_edge: None,
                    model_id: config.model_id.clone(),
                    gguf_source: config.gguf_source.clone(),
                    tokenizer: config.tokenizer.clone(),
                }
            }),
        )
        .map_err(|e| format!("send stage provision: {e}"))
}

fn wait_for_weights_loaded(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    provider: ProviderKind,
    stage_index: u32,
) -> Result<(), String> {
    loop {
        pump(driver, stack, frame_tx);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for weights loaded".to_owned());
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited { status, .. } => {
                    return Err(format!("node exited while loading weights: {status:?}"));
                }
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
            }
        }
        drain_frames(frame_rx, dashboard, orch_datastream);
        while let Some(report) = orchestrator_reports.try_recv() {
            match report {
                OrchestratorReport::WeightsReady {
                    run_id: report_run_id,
                    node_id: _,
                    stage_index: report_stage_index,
                } if report_run_id == run_id && report_stage_index == stage_index => {
                    return Ok(());
                }
                OrchestratorReport::StageFault {
                    run_id: report_run_id,
                    stage_index: report_stage_index,
                } if report_run_id == run_id && report_stage_index == stage_index => {
                    return Err(format!(
                        "stage {report_stage_index} faulted while loading weights"
                    ));
                }
                _ => {}
            }
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

#[derive(Debug)]
struct PipelineTokenRecord {
    object_id: u64,
    sequence: u64,
    token_id: u32,
    eos: bool,
}

enum PipelineSendHandle {
    Driver(EdgeSendHandle),
    #[cfg(test)]
    Channel(tokio_mpsc::UnboundedSender<Vec<u8>>),
}

impl PipelineSendHandle {
    fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        match self {
            Self::Driver(handle) => handle.send(bytes),
            #[cfg(test)]
            Self::Channel(tx) => tx
                .send(bytes)
                .map_err(|_| "pipeline token-in sender stopped".to_owned()),
        }
    }
}
struct PendingEncode {
    request_id: u64,
}

struct PendingDecode {
    request_id: u64,
    token_id: u32,
    eos: bool,
    reached_limit: bool,
}

struct PipelinePromptRuntime {
    token_in_edge_id: u64,
    token_out_edge_id: u64,
    token_spec: run_plan::ObjectSpec,
    token_out_spec: run_plan::ObjectSpec,
    token_in_sender: PipelineSendHandle,
    recv_rx: mpsc::Receiver<Vec<u8>>,
    recv_tx: mpsc::Sender<Vec<u8>>,
    recv_buffer: Vec<u8>,
    tokenizer_encode_actor: ActorAddress,
    tokenizer_decode_actor: ActorAddress,
    tokenizer_reply_to: ActorAddress,
    pending_encode: Option<PendingEncode>,
    pending_decode: Option<PendingDecode>,
    next_sequence: u64,
    generated_tokens: Vec<u32>,
    final_text: String,
    active: Option<ActivePrompt>,
    started_at: Option<Instant>,
}

impl PipelinePromptRuntime {
    fn new(
        driver: &IrohDriver,
        plan: &run_plan::RunPlan,
        first_stage_endpoint: EndpointAddr,
        tokenizer_encode_actor: ActorAddress,
        tokenizer_decode_actor: ActorAddress,
        tokenizer_reply_to: ActorAddress,
    ) -> Result<Self, String> {
        let token_in_edge = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::TokenIn)
            .ok_or_else(|| "pipeline plan missing token-in edge".to_owned())?;
        let token_out_edge = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::TokenOut)
            .ok_or_else(|| "pipeline plan missing token-out edge".to_owned())?;
        let (recv_tx, recv_rx) = mpsc::channel();
        Ok(Self {
            token_in_edge_id: token_in_edge.edge_id.0,
            token_out_edge_id: token_out_edge.edge_id.0,
            token_spec: token_in_edge.object_spec,
            token_out_spec: token_out_edge.object_spec,
            token_in_sender: PipelineSendHandle::Driver(
                driver.spawn_edge_send_pump(first_stage_endpoint, token_in_edge.edge_id.0)?,
            ),
            recv_rx,
            recv_tx,
            tokenizer_encode_actor,
            tokenizer_decode_actor,
            tokenizer_reply_to,
            pending_encode: None,
            pending_decode: None,
            recv_buffer: Vec::new(),
            next_sequence: 0,
            generated_tokens: Vec::new(),
            final_text: String::new(),
            active: None,
            started_at: None,
        })
    }

    fn is_active(&self) -> bool {
        self.active.is_some()
    }

    fn start_prompt(
        &mut self,
        request: SubmitPrompt,
        events: mpsc::Sender<PromptEvent>,
        runtime: &Arc<swactor::runtime::Runtime>,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        let request_id = request.request_id;
        self.generated_tokens.clear();
        self.final_text.clear();
        self.recv_buffer.clear();
        self.pending_decode = None;
        self.pending_encode = Some(PendingEncode { request_id });
        self.started_at = Some(Instant::now());
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_tokenizer_encode",
            "started",
            json!({"node_actor":self.tokenizer_encode_actor,"reply_to":self.tokenizer_reply_to,"prompt_bytes":request.prompt_text.len()}),
        );
        runtime
            .send_to(
                self.tokenizer_encode_actor,
                NodeAgentMsg::EncodePrompt {
                    request_id,
                    prompt: request.prompt_text.clone(),
                    reply_to: self.tokenizer_reply_to,
                },
            )
            .map_err(|e| format!("send tokenizer encode request: {e}"))?;
        self.active = Some(ActivePrompt { request, events });
        Ok(())
    }

    fn drain_tokenizer_events(
        &mut self,
        runtime: &Arc<swactor::runtime::Runtime>,
        tokenizer_events: &swactor::runtime::Inbox<TokenizerEvent>,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        while let Some(event) = tokenizer_events.try_recv() {
            match event {
                TokenizerEvent::PromptEncoded { request_id, tokens } => self
                    .handle_encoded_prompt(
                        request_id,
                        tokens,
                        dashboard,
                        orch_datastream,
                        run_id,
                        node_id,
                    )?,
                TokenizerEvent::TokensDecoded { request_id, text } => self.handle_decoded_tokens(
                    runtime,
                    request_id,
                    text,
                    dashboard,
                    orch_datastream,
                    run_id,
                    node_id,
                )?,
                TokenizerEvent::Fault { request_id, error } => {
                    self.fault_active(request_id, format!("tokenizer request failed: {error}"));
                }
            }
        }
        Ok(())
    }

    fn handle_encoded_prompt(
        &mut self,
        request_id: u64,
        tokens: Vec<u32>,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        let Some(pending) = self.pending_encode.take() else {
            return Ok(());
        };
        if pending.request_id != request_id {
            self.pending_encode = Some(pending);
            return Ok(());
        }
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };
        if active.request.request_id != request_id {
            return Ok(());
        }
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_tokenizer_encode",
            "ready",
            json!({"node_actor":self.tokenizer_encode_actor,"reply_to":self.tokenizer_reply_to,"tokens":tokens.len()}),
        );
        let sequence = self.next_sequence;
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_token_in",
            "started",
            json!({"edge_id":self.token_in_edge_id,"sequence":sequence,"tokens":tokens.len(),"begin_sequence":true,"token_count":tokens.len(),"token_ids":&tokens}),
        );
        self.send_token_in(sequence, &tokens, true)?;
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_token_in",
            "ready",
            json!({"edge_id":self.token_in_edge_id,"sequence":sequence,"begin_sequence":true,"token_count":tokens.len()}),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_decoded_tokens(
        &mut self,
        _runtime: &Arc<swactor::runtime::Runtime>,
        request_id: u64,
        text: String,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        let Some(pending) = self.pending_decode.take() else {
            return Ok(());
        };
        if pending.request_id != request_id {
            self.pending_decode = Some(pending);
            return Ok(());
        }
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };
        if active.request.request_id != request_id {
            return Ok(());
        }
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_tokenizer_decode",
            "ready",
            json!({"node_actor":self.tokenizer_decode_actor,"reply_to":self.tokenizer_reply_to,"text_bytes":text.len()}),
        );
        self.final_text.push_str(&text);
        if !text.is_empty() {
            let _ = active
                .events
                .send(PromptEvent::TextDelta { request_id, text });
        }
        if pending.eos || pending.reached_limit {
            let elapsed_ms = self
                .started_at
                .map(|started| started.elapsed().as_millis() as u64)
                .unwrap_or(0);
            let final_text = self.final_text.clone();
            let tokens_generated = self.generated_tokens.len() as u32;
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "prompt_complete",
                "ready",
                json!({
                    "event":"Done",
                    "terminal":true,
                    "tokens_generated":tokens_generated,
                    "elapsed_ms":elapsed_ms,
                    "final_text_bytes":final_text.len(),
                }),
            );
            let _ = active.events.send(PromptEvent::Done {
                request_id,
                final_text,
                tokens_generated,
                elapsed_ms,
            });
            self.active = None;
            return Ok(());
        }
        let sequence = self.next_sequence;
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_token_in",
            "started",
            json!({"edge_id":self.token_in_edge_id,"sequence":sequence,"tokens":1,"begin_sequence":false,"token_count":1,"token_id":pending.token_id}),
        );
        self.send_token_in(sequence, &[pending.token_id], false)?;
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_token_in",
            "ready",
            json!({"edge_id":self.token_in_edge_id,"sequence":sequence,"begin_sequence":false,"token_count":1,"token_id":pending.token_id}),
        );
        Ok(())
    }
    fn send_token_in(
        &self,
        sequence: u64,
        tokens: &[u32],
        begin_sequence: bool,
    ) -> Result<(), String> {
        self.token_in_sender.send(encode_token_record_with_flags(
            self.token_spec,
            sequence,
            tokens,
            false,
            begin_sequence,
        )?)
    }

    fn request_decode(
        &mut self,
        runtime: &Arc<swactor::runtime::Runtime>,
        request_id: u64,
        token_id: u32,
        eos: bool,
        reached_limit: bool,
    ) -> Result<(), String> {
        self.pending_decode = Some(PendingDecode {
            request_id,
            token_id,
            eos,
            reached_limit,
        });
        runtime
            .send_to(
                self.tokenizer_decode_actor,
                NodeAgentMsg::DecodeTokens {
                    request_id,
                    tokens: vec![token_id],
                    reply_to: self.tokenizer_reply_to,
                },
            )
            .map_err(|e| format!("send tokenizer decode request: {e}"))
    }

    fn fault_active(&mut self, request_id: u64, error: String) {
        if let Some(active) = self.active.take()
            && active.request.request_id == request_id
        {
            let _ = active.events.send(PromptEvent::Fault { request_id, error });
        }
        self.pending_encode = None;
        self.pending_decode = None;
    }

    fn poll_driver(&mut self, driver: &mut IrohDriver) {
        driver.pump_edge_ingress();
        for event in driver.drain_edge_events() {
            match event {
                EdgeTransportEvent::BytesRead { edge_id, bytes, .. }
                    if edge_id == self.token_out_edge_id =>
                {
                    let _ = self.recv_tx.send(bytes);
                }
                EdgeTransportEvent::StreamFault {
                    edge_id: Some(edge_id),
                    reason,
                    ..
                } if edge_id == self.token_out_edge_id => {
                    if let Some(request_id) =
                        self.active.as_ref().map(|active| active.request.request_id)
                    {
                        self.fault_active(
                            request_id,
                            format!("pipeline token-out stream fault: {reason:?}"),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    fn drain_tokens(
        &mut self,
        runtime: &Arc<swactor::runtime::Runtime>,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) -> Result<(), String> {
        if self.pending_decode.is_some() {
            return Ok(());
        }
        while let Ok(bytes) = self.recv_rx.try_recv() {
            self.recv_buffer.extend_from_slice(&bytes);
            while self.pending_decode.is_none()
                && let Some(record) =
                    take_pipeline_token_record(&mut self.recv_buffer, self.token_out_spec)?
            {
                if record.sequence != self.next_sequence {
                    return Err(format!(
                        "pipeline token sequence violation: expected {}, got {}",
                        self.next_sequence, record.sequence
                    ));
                }
                self.next_sequence = self.next_sequence.saturating_add(1);
                let Some(active) = self.active.as_ref() else {
                    continue;
                };
                let request_id = active.request.request_id;
                orch_datastream.emit_prompt(
                    dashboard,
                    run_id,
                    node_id,
                    request_id,
                    "pipeline_token_out",
                    "observed",
                    json!({"edge_id":self.token_out_edge_id,"object_id":record.object_id,"sequence":record.sequence,"token_id":record.token_id,"eos":record.eos,"generated_index":self.generated_tokens.len() + 1}),
                );
                self.generated_tokens.push(record.token_id);
                let reached_limit = self.generated_tokens.len() as u32 >= active.request.max_tokens;
                orch_datastream.emit_prompt(
                    dashboard,
                    run_id,
                    node_id,
                    request_id,
                    "pipeline_tokenizer_decode",
                    "started",
                    json!({"node_actor":self.tokenizer_decode_actor,"reply_to":self.tokenizer_reply_to,"token_id":record.token_id,"sequence":record.sequence,"generated_index":self.generated_tokens.len()}),
                );
                self.request_decode(
                    runtime,
                    request_id,
                    record.token_id,
                    record.eos,
                    reached_limit,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
fn encode_token_record(
    spec: run_plan::ObjectSpec,
    _edge_id: u64,
    sequence: u64,
    tokens: &[u32],
    eos: bool,
) -> Result<Vec<u8>, String> {
    encode_token_record_with_flags(spec, sequence, tokens, eos, false)
}

fn encode_token_record_with_flags(
    spec: run_plan::ObjectSpec,
    sequence: u64,
    tokens: &[u32],
    eos: bool,
    begin_sequence: bool,
) -> Result<Vec<u8>, String> {
    let payload = tokens
        .iter()
        .flat_map(|token| token.to_le_bytes())
        .collect::<Vec<_>>();
    let mut flags = ingress::ObjectFlags::default();
    flags.end_of_sequence = eos;
    flags.begin_sequence = begin_sequence;
    Ok(
        ingress::ObjectRecordBuilder::new(ingress_object_spec_from_plan(spec))
            .object_id(ingress::ObjectId(9000_u64.saturating_add(sequence)))
            .sequence(sequence)
            .payload(payload)
            .flags(flags)
            .encode(),
    )
}

fn ingress_object_spec_from_plan(spec: run_plan::ObjectSpec) -> ingress::ObjectSpec {
    let extent_alignment = match spec.kind {
        run_plan::ObjectKind::Token => u64::from(spec.dtype_width_bytes),
        _ => u64::from(spec.alignment),
    };
    ingress::ObjectSpec {
        max_extent: spec.max_extent,
        alignment: extent_alignment,
        layout: ingress::ObjectLayout::Token,
    }
}

fn take_pipeline_token_record(
    buffer: &mut Vec<u8>,
    spec: run_plan::ObjectSpec,
) -> Result<Option<PipelineTokenRecord>, String> {
    let record =
        match ingress::read_object_record(buffer, ingress_object_spec_from_plan(spec), false)
            .map_err(|reason| format!("invalid token-out record: {reason:?}"))?
        {
            ingress::ObjectRecordRead::Incomplete => return Ok(None),
            ingress::ObjectRecordRead::Complete(record) => record,
        };
    let payload = record
        .payload(buffer)
        .ok_or_else(|| "token-out record payload missing".to_owned())?;
    if payload.len() != 4 {
        return Err(format!(
            "token-out payload must be exactly one u32, got {}",
            payload.len()
        ));
    }
    let token_id = u32::from_le_bytes(payload.try_into().unwrap());
    let out = PipelineTokenRecord {
        object_id: record.object_id.0,
        sequence: record.sequence,
        token_id,
        eos: record.flags.end_of_sequence,
    };
    buffer.drain(..record.total_len);
    Ok(Some(out))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromptRuntimeMode {
    DirectInferPrompt,
    PipelineTokenEdges,
}

fn prompt_runtime_mode(pipeline_plan: Option<&run_plan::RunPlan>) -> PromptRuntimeMode {
    if pipeline_plan.is_some() {
        PromptRuntimeMode::PipelineTokenEdges
    } else {
        PromptRuntimeMode::DirectInferPrompt
    }
}

fn serve_prompts(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    work_rx: &mpsc::Receiver<PromptWork>,
    prompt_events: &swactor::runtime::Inbox<PromptEvent>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    node_actor: ActorAddress,
    reply_to: ActorAddress,
    tokenizer_events: &swactor::runtime::Inbox<TokenizerEvent>,
    tokenizer_encode_actor: ActorAddress,
    tokenizer_decode_actor: ActorAddress,
    tokenizer_reply_to: ActorAddress,
    provider: ProviderKind,
    pipeline_plan: Option<&run_plan::RunPlan>,
    prompt_endpoint: EndpointAddr,
) -> Result<(), String> {
    let mut pipeline_runtime = match prompt_runtime_mode(pipeline_plan) {
        PromptRuntimeMode::PipelineTokenEdges => Some(PipelinePromptRuntime::new(
            driver,
            pipeline_plan.expect("pipeline mode requires plan"),
            prompt_endpoint,
            tokenizer_encode_actor,
            tokenizer_decode_actor,
            tokenizer_reply_to,
        )?),
        PromptRuntimeMode::DirectInferPrompt => None,
    };
    let mut active: Option<ActivePrompt> = None;
    loop {
        pump(driver, stack, frame_tx);
        if let Some(pipeline) = pipeline_runtime.as_mut() {
            pipeline.poll_driver(driver);
            pipeline.drain_tokenizer_events(
                &stack.runtime,
                tokenizer_events,
                dashboard,
                orch_datastream,
                run_id,
                node_id,
            )?;
            pipeline.drain_tokens(&stack.runtime, dashboard, orch_datastream, run_id, node_id)?;
        }
        drain_observations(obs_rx, dashboard, orch_datastream, provider)?;
        drain_frames(frame_rx, dashboard, orch_datastream);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_rx.try_recv().is_ok() {
            orch_datastream.emit_bootstrap(
                dashboard,
                run_id,
                node_id,
                "shutdown",
                "started",
                json!({"source":"stdin"}),
            );
            return Ok(());
        }

        if active.is_none()
            && pipeline_runtime
                .as_ref()
                .is_none_or(|pipeline| !pipeline.is_active())
            && let Ok(work) = work_rx.try_recv()
        {
            let request = work.request;
            let request_id = request.request_id;
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "prompt_work",
                "observed",
                json!({
                    "prompt_bytes":request.prompt_text.len(),
                    "max_tokens":request.max_tokens,
                }),
            );
            if let Some(pipeline) = pipeline_runtime.as_mut() {
                pipeline.start_prompt(
                    request,
                    work.events,
                    &stack.runtime,
                    dashboard,
                    orch_datastream,
                    run_id,
                    node_id,
                )?;
                continue;
            }
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "node_prompt_send",
                "started",
                json!({"node_actor":node_actor,"reply_to":reply_to}),
            );
            match stack.runtime.send_to(
                node_actor,
                NodeAgentMsg::InferPrompt {
                    request_id,
                    prompt: request.prompt_text.clone(),
                    max_tokens: request.max_tokens,
                    reply_to,
                },
            ) {
                Ok(()) => {
                    orch_datastream.emit_prompt(
                        dashboard,
                        run_id,
                        node_id,
                        request_id,
                        "node_prompt_send",
                        "ready",
                        json!({"node_actor":node_actor,"reply_to":reply_to}),
                    );
                    active = Some(ActivePrompt {
                        request,
                        events: work.events,
                    });
                }
                Err(error) => {
                    orch_datastream.emit_prompt(
                        dashboard,
                        run_id,
                        node_id,
                        request_id,
                        "node_prompt_send",
                        "failed",
                        json!({"node_actor":node_actor,"reply_to":reply_to,"error":error.to_string()}),
                    );
                    return Err(format!("send prompt request: {error}"));
                }
            }
        }

        while let Some(event) = prompt_events.try_recv() {
            let request_id = event.request_id();
            let Some(current) = active.as_ref() else {
                orch_datastream.emit_prompt(
                    dashboard,
                    run_id,
                    node_id,
                    request_id,
                    "node_prompt_event",
                    "dropped",
                    json!({"reason":"no_active_prompt","event":prompt_event_name(&event)}),
                );
                continue;
            };
            if request_id != current.request.request_id {
                orch_datastream.emit_prompt(
                    dashboard,
                    run_id,
                    node_id,
                    request_id,
                    "node_prompt_event",
                    "dropped",
                    json!({
                        "reason":"request_mismatch",
                        "event":prompt_event_name(&event),
                        "active_request_id":current.request.request_id,
                    }),
                );
                continue;
            }

            let terminal = event.is_terminal();
            let completion_status = prompt_completion_status(&event);
            let completion_detail = prompt_completion_detail(&event);
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "node_prompt_event",
                "observed",
                prompt_event_detail(&event),
            );
            let _ = current.events.send(event);
            if terminal {
                orch_datastream.emit_prompt(
                    dashboard,
                    run_id,
                    node_id,
                    request_id,
                    "prompt_complete",
                    completion_status,
                    completion_detail,
                );
                active = None;
            }
        }

        thread::sleep(PUMP_INTERVAL);
    }
}

fn prompt_event_name(event: &PromptEvent) -> &'static str {
    match event {
        PromptEvent::TextDelta { .. } => "TextDelta",
        PromptEvent::Done { .. } => "Done",
        PromptEvent::Fault { .. } => "Fault",
    }
}

fn prompt_event_detail(event: &PromptEvent) -> Value {
    match event {
        PromptEvent::TextDelta { text, .. } => {
            json!({"event":"TextDelta","terminal":false,"text_bytes":text.len()})
        }
        PromptEvent::Done {
            final_text,
            tokens_generated,
            elapsed_ms,
            ..
        } => json!({
            "event":"Done",
            "terminal":true,
            "tokens_generated":tokens_generated,
            "elapsed_ms":elapsed_ms,
            "final_text_bytes":final_text.len(),
        }),
        PromptEvent::Fault { error, .. } => {
            json!({"event":"Fault","terminal":true,"error":error})
        }
    }
}

fn prompt_completion_status(event: &PromptEvent) -> &'static str {
    match event {
        PromptEvent::Done { .. } => "ready",
        PromptEvent::Fault { .. } => "failed",
        PromptEvent::TextDelta { .. } => "observed",
    }
}

fn prompt_completion_detail(event: &PromptEvent) -> Value {
    match event {
        PromptEvent::Done { .. } => json!({"event":"Done"}),
        PromptEvent::Fault { error, .. } => json!({"event":"Fault","error":error}),
        PromptEvent::TextDelta { .. } => json!({"event":"TextDelta"}),
    }
}

fn stop_requested(stop_rx: &mpsc::Receiver<()>) -> bool {
    stop_rx.try_recv().is_ok()
}

fn spawn_stop_listener() -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel();
    let stdin_tx = tx.clone();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.eq_ignore_ascii_case("stop")
                || trimmed.eq_ignore_ascii_case("shutdown")
                || trimmed.eq_ignore_ascii_case("quit")
            {
                let _ = stdin_tx.send(());
                break;
            }
        }
    });
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

fn drain_observations(
    obs_rx: &mpsc::Receiver<PluginObservation>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    provider: ProviderKind,
) -> Result<(), String> {
    while let Ok(observation) = obs_rx.try_recv() {
        emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
        match observation {
            PluginObservation::Failed { reason, .. } => return Err(reason),
            PluginObservation::Exited { status, .. } => {
                return Err(format!("node exited: {status:?}"));
            }
            PluginObservation::DatastreamFrame { .. } => {}
            PluginObservation::ProviderLine { .. }
            | PluginObservation::StdoutLine { .. }
            | PluginObservation::StderrLine { .. } => {}
        }
    }
    Ok(())
}

fn emit_plugin_observation(
    orch_datastream: &mut OrchDatastream,
    dashboard: Option<&DashboardSupport>,
    provider: ProviderKind,
    observation: &PluginObservation,
) {
    match observation {
        PluginObservation::StdoutLine {
            run_id,
            node_id,
            line,
        } => orch_datastream.emit_log(
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
        } => orch_datastream.emit_log(
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
        } => orch_datastream.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Provider,
                line: line.clone(),
            },
        ),
        PluginObservation::DatastreamFrame {
            channel, payload, ..
        } => orch_datastream.emit_bytes_from(
            dashboard,
            channel,
            payload.as_bytes().to_vec(),
            "node_bootstrap_stdio",
        ),
        PluginObservation::Exited {
            run_id,
            node_id,
            status,
        } => orch_datastream.emit_event(
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
        } => orch_datastream.emit_event(
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

fn drain_frames(
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
) {
    while let Ok(collected) = frame_rx.try_recv() {
        ingest_dashboard_frame(
            dashboard,
            &collected.stream,
            &collected.channel_name,
            &collected.frame,
        );
        orch_datastream.archive_frame(
            "node_cluster",
            &collected.stream,
            &collected.channel_name,
            &collected.frame,
        );
    }
}

fn ingest_dashboard_frame(
    dashboard: Option<&DashboardSupport>,
    stream: &StreamId,
    channel: &str,
    frame: &Frame,
) {
    if let Some(dashboard) = dashboard {
        dashboard.publish_frame(stream, channel, frame);
    }
}

fn emit_swim_transitions(
    orch_datastream: &mut OrchDatastream,
    dashboard: Option<&DashboardSupport>,
    run_id: u64,
    node_id: u64,
    stack: &DistributionRuntimeStack,
) {
    for transition in stack.drain_swim_transitions() {
        orch_datastream.emit_bootstrap_to_channel(
            dashboard,
            MVP_SWIM_MEMBERSHIP,
            run_id,
            node_id,
            "membership_transition",
            "observed",
            json!({
                "peer":format!("{:?}", transition.peer),
                "from":transition.from.map(|state| format!("{:?}", state)),
                "to":format!("{:?}", transition.to),
                "reason":transition.reason,
            }),
        );
    }
}

fn pump(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
) {
    stack.tick_protocol_actors(Instant::now());
    driver.pump_inbound_to_actors();
    stack.pump_runtime_once();
    driver.drain_outbox(&stack.outbox);
    drain_datastream_connections(driver, frame_tx);
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn docker_container_prefix() -> String {
    env_optional(MVP_DOCKER_CONTAINER_PREFIX_ENV)
        .unwrap_or_else(|| DEFAULT_DOCKER_CONTAINER_PREFIX.to_owned())
}

fn optional_env(name: &str) -> Option<(String, String)> {
    env_optional(name).map(|value| (name.to_owned(), value))
}

fn local_tinygrad_worker_env(provider: ProviderKind) -> Option<(String, String)> {
    optional_env("MVP_TINYGRAD_WORKER").or_else(|| {
        if provider != ProviderKind::Process {
            return None;
        }
        default_local_tinygrad_worker_path().map(|path| {
            (
                "MVP_TINYGRAD_WORKER".to_owned(),
                path.to_string_lossy().to_string(),
            )
        })
    })
}

fn default_local_tinygrad_worker_path() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("apps").join("mvp-node").join("tinygrad_worker.py"));
    }
    candidates.push(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("apps")
            .join("mvp-node")
            .join("tinygrad_worker.py"),
    );
    for candidate in candidates {
        if candidate.is_file() {
            return Some(candidate.canonicalize().unwrap_or(candidate));
        }
    }
    None
}

fn resolve_vastai_ssh_identity(explicit: Option<PathBuf>) -> Result<PathBuf, String> {
    match explicit {
        Some(path) => Ok(path),
        None => {
            let home = std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "MVP_VASTAI_SSH_IDENTITY is required because HOME is unset".to_owned()
                })?;
            Ok(PathBuf::from(home).join(".ssh").join("id_ed25519"))
        }
    }
}

fn expand_home_path(value: &str) -> Result<PathBuf, String> {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "MVP_VASTAI_SSH_IDENTITY uses ~/ but HOME is unset".to_owned())?;
        return Ok(PathBuf::from(home).join(rest));
    }
    Ok(PathBuf::from(trimmed))
}

fn derive_ssh_public_key(identity: &Path) -> Result<String, String> {
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

fn ssh_public_key_fingerprint(public_key: &str) -> String {
    let path = std::env::temp_dir().join(format!("mvp-vastai-ssh-key-{}.pub", std::process::id()));
    if std::fs::write(&path, format!("{public_key}\n")).is_err() {
        return "unavailable".to_owned();
    }
    let output = Command::new("ssh-keygen")
        .arg("-l")
        .arg("-f")
        .arg(&path)
        .output();
    let _ = std::fs::remove_file(&path);
    let Ok(output) = output else {
        return "unavailable".to_owned();
    };
    if !output.status.success() {
        return "unavailable".to_owned();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut fields = stdout.split_whitespace();
    match (fields.next(), fields.next()) {
        (Some(bits), Some(fingerprint)) => format!("{bits} {fingerprint}"),
        _ => "unavailable".to_owned(),
    }
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

fn ensure_vastai_account_ssh_key(api_key: &str, public_key: &str) -> Result<(), String> {
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
        Err(
            "VastAI SSH key registration did not make the selected key visible in vastai show ssh-keys"
                .to_owned(),
        )
    }
}

fn account_ssh_keys_output_contains_public_key(output: &str, public_key: &str) -> bool {
    let public_key = public_key.trim();
    if public_key.is_empty() {
        return false;
    }
    if output.contains(public_key) {
        return true;
    }
    public_key
        .split_whitespace()
        .nth(1)
        .is_some_and(|body| !body.is_empty() && output.contains(body))
}

fn vastai_cli_error(error: std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        "vastai CLI is required to verify/register MVP_VASTAI_SSH_IDENTITY; install with pip install vastai"
            .to_owned()
    } else {
        format!("run vastai CLI: {error}")
    }
}

fn command_output_failure_detail(output: &std::process::Output, secret: Option<&str>) -> String {
    let mut detail = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if detail.is_empty() {
        detail = output.status.to_string();
    }
    if let Some(secret) = secret.filter(|secret| !secret.is_empty()) {
        detail = detail.replace(secret, "<redacted>");
    }
    detail
}

#[cfg(test)]
fn relay_mode_from_env() -> Result<iroh::RelayMode, String> {
    relay_runtime_config_from_env(1).map(|relay| relay.mode)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::OsString, path::PathBuf};

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    const ENV_KEYS: &[&str] = &[
        CACHED_MODEL_HOST_ENV,
        "HOME",
        "HF_TOKEN",
        "MVP_CPU_LINE_PROFILE",
        "MVP_CPU_LINE_PROFILE_INTERVAL_MS",
        "CUDA_DEVICE_SCHEDULE",
        "MVP_DASHBOARD",
        "MVP_DOCKER_GPUS",
        "MVP_GGUF_FILE",
        "MVP_GGUF_LOCAL_PATH",
        "MVP_GGUF_REPO",
        "MVP_GGUF_REVISION",
        "MVP_IROH_RELAY_MODE",
        MVP_IROH_RELAY_URL_ENV,
        "MVP_LAYER_END_EXCLUSIVE",
        "MVP_LOGICAL_NODE_ID",
        "MVP_MODEL_CACHE_DIR",
        "MVP_MAX_CONTEXT",
        "MVP_MODEL_ID",
        "MVP_NODE_IMAGE",
        "MVP_NODE_PROVIDER",
        "MVP_PROVIDER",
        "MVP_PROMPT_MAX_TOKENS",
        "MVP_PROMPT_RPC_BIND",
        "MVP_RUN_ID",
        "MVP_RUNTIME_CONFIG",
        "MVP_PIPELINE_STAGES",
        "MVP_STAGE_INDEX",
        "MVP_TOKEN_PROGRESS_EVERY",
        "MVP_TINYGRAD_WORKER",
        "MVP_TOKENIZER_LOCAL_PATH",
        "MVP_VASTAI_API_KEY",
        "MVP_VASTAI_BOOTSTRAP_COMMAND",
        "MVP_VASTAI_CONFIRM_LEASE",
        "MVP_VASTAI_DISK_GB",
        "MVP_VASTAI_GPU_NAME",
        "MVP_VASTAI_MIN_DOWN_MBPS",
        "MVP_VASTAI_MIN_GPU_RAM_MB",
        "MVP_VASTAI_MIN_RELIABILITY",
        "MVP_VASTAI_MIN_UP_MBPS",
        "MVP_VASTAI_ONSTART",
        "MVP_VASTAI_POLL_INTERVAL_SECS",
        "MVP_VASTAI_REQUIRE_VERIFIED",
        "MVP_VASTAI_SSH_USER",
        "MVP_VASTAI_SSH_IDENTITY",
        "VASTAI_API_KEY",
        SWACTOR_IROH_RELAY_URL_ENV,
        MVP_DOCKER_CONTAINER_PREFIX_ENV,
        MVP_WORKER_BIN_ENV,
    ];

    struct RestoreEnv {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl Drop for RestoreEnv {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    fn with_clean_env<T>(settings: &[(&'static str, &'static str)], test: impl FnOnce() -> T) -> T {
        let settings = settings
            .iter()
            .map(|(key, value)| (*key, OsString::from(value)))
            .collect::<Vec<_>>();
        with_clean_env_os(&settings, test)
    }

    fn with_clean_env_os<T>(settings: &[(&'static str, OsString)], test: impl FnOnce() -> T) -> T {
        let _lock = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let saved = ENV_KEYS
            .iter()
            .map(|&key| (key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for key in ENV_KEYS {
            unsafe { std::env::remove_var(key) };
        }
        for (key, value) in settings {
            assert!(
                ENV_KEYS.contains(key),
                "test env key {key} must be restored"
            );
            unsafe { std::env::set_var(key, value) };
        }
        let _restore = RestoreEnv { saved };
        test()
    }

    fn selected_provider(settings: &[(&'static str, &'static str)]) -> ProviderKind {
        with_clean_env(settings, || {
            Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                .expect("config parses")
                .provider
        })
    }

    fn canonical_relay_url(raw: &str) -> String {
        raw.parse::<iroh::RelayUrl>()
            .expect("fixture relay URL parses")
            .to_string()
    }

    fn node_spec_env(settings: &[(&'static str, &'static str)]) -> Vec<(String, String)> {
        with_clean_env(settings, || {
            let config = Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                .expect("config parses");
            let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[9; 32]).public());
            let orchestrator_actor = ActorAddress([12; 32]);
            config
                .node_spec(coordinator, orchestrator_actor)
                .expect("node spec builds")
                .env
        })
    }

    fn env_value<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
        env.iter()
            .find(|(env_key, _)| env_key == key)
            .map(|(_, value)| value.as_str())
    }

    fn cached_model_config_with_args(model: &TempModelFile, args: &[&str]) -> Config {
        with_clean_env_os(
            &[
                ("MVP_RUNTIME_CONFIG", OsString::from("local")),
                ("MVP_NODE_PROVIDER", OsString::from("docker")),
                (CACHED_MODEL_HOST_ENV, model.raw_path.as_os_str().to_owned()),
            ],
            || {
                Config::from_layers_with_path_and_args(
                    None,
                    args.iter().copied().map(str::to_owned),
                )
                .expect("cached-model config parses")
            },
        )
    }

    fn pipeline_config_with_cached_model(model: &TempModelFile, pipeline_stages: u32) -> Config {
        let stages_arg = pipeline_stages.to_string();
        cached_model_config_with_args(
            model,
            &[
                "--pipeline-stages",
                stages_arg.as_str(),
                "--max-context",
                "512",
            ],
        )
    }

    fn expected_layer_ranges(num_layers: u32, stage_count: u32) -> Vec<(u32, u32)> {
        (0..stage_count)
            .map(|stage_index| {
                let start = (u64::from(num_layers) * u64::from(stage_index)
                    / u64::from(stage_count)) as u32;
                let end = (u64::from(num_layers) * u64::from(stage_index + 1)
                    / u64::from(stage_count)) as u32;
                (start, end)
            })
            .collect()
    }
    #[test]
    fn pipeline_weight_load_scheduler_keeps_one_active_stage() {
        let model = TempModelFile::with_metadata(
            "seven-stage-scheduler.gguf",
            TestGgufMetadata {
                num_layers: 30,
                ..TestGgufMetadata::default()
            },
        );
        let config = pipeline_config_with_cached_model(&model, 7);
        let plan = config.build_run_plan().expect("seven-stage plan builds");
        let mut loaded = BTreeSet::new();

        let first =
            next_pipeline_weight_load_stage(&plan, &loaded, None).expect("first stage selected");
        assert_eq!(first.stage_index, 6);

        let resent = next_pipeline_weight_load_stage(&plan, &loaded, Some(first.stage_index))
            .expect("active stage is resent before it loads");
        assert_eq!(resent.stage_index, 6);

        for expected_stage in (0..7).rev() {
            let active = next_pipeline_weight_load_stage(&plan, &loaded, None)
                .expect("next unloaded stage selected");
            assert_eq!(active.stage_index, expected_stage);
            loaded.insert(expected_stage);
        }

        assert!(
            next_pipeline_weight_load_stage(&plan, &loaded, None).is_none(),
            "all stages loaded should leave no active load"
        );
    }

    fn assert_plan_matches_metadata(
        config: &Config,
        plan: &run_plan::RunPlan,
        metadata: TestGgufMetadata,
        stage_count: u32,
        requested_context: u64,
    ) {
        assert_eq!(plan.run_id, run_plan::RunId(config.run_id));
        assert_eq!(plan.model.model_id, config.model_id);
        assert_eq!(plan.model.gguf_source, config.gguf_source);
        assert_eq!(plan.model.num_layers, metadata.num_layers);
        assert_eq!(plan.model.hidden_dim, metadata.hidden_dim as u32);
        assert_eq!(
            plan.model.max_seq_len,
            requested_context.min(metadata.context_length) as u32
        );
        assert_eq!(plan.model.eos_token_id, metadata.eos_token_id);
        assert_eq!(plan.stages.len(), stage_count as usize);
        assert_eq!(plan.edges.len(), stage_count as usize + 1);
        assert_eq!(plan.max_tokens, config.default_max_tokens);

        let mut stages = plan.stages.clone();
        stages.sort_by_key(|stage| stage.stage_index);
        let expected_ranges = expected_layer_ranges(metadata.num_layers, stage_count);
        for (stage, expected_range) in stages.iter().zip(expected_ranges) {
            assert_eq!(stage.stage_count, stage_count);
            assert_eq!(
                stage.node_id,
                run_plan::NodeId(config.node_id + 1 + u64::from(stage.stage_index))
            );
            assert_eq!(
                (stage.layer_start, stage.layer_end_exclusive),
                expected_range
            );
            assert!(
                stage.layer_start < stage.layer_end_exclusive,
                "stage {} must have a non-empty layer range",
                stage.stage_index
            );
        }
    }

    fn token_object_spec(max_extent: u64, alignment: u32) -> run_plan::ObjectSpec {
        run_plan::ObjectSpec {
            kind: run_plan::ObjectKind::Token,
            max_extent,
            dtype_family: run_plan::DTypeFamily::BFloat,
            dtype_width_bytes: 4,
            shape: run_plan::ShapeRule::TokenIds,
            layout: run_plan::LayoutRule::Contiguous,
            alignment,
            sequence_policy: run_plan::SequencePolicy::Ordered,
        }
    }

    fn decode_token_record_payload(
        bytes: &[u8],
        spec: run_plan::ObjectSpec,
    ) -> (u64, Vec<u32>, bool) {
        let read = ingress::read_object_record(bytes, ingress_object_spec_from_plan(spec), false)
            .expect("token record should be valid");
        let ingress::ObjectRecordRead::Complete(record) = read else {
            panic!("token record should be complete");
        };
        let payload = record.payload(bytes).expect("token payload is present");
        let tokens = payload
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert!(payload.chunks_exact(4).remainder().is_empty());
        (record.sequence, tokens, record.flags.end_of_sequence)
    }

    fn decode_token_record_with_flags(
        bytes: &[u8],
        spec: run_plan::ObjectSpec,
    ) -> (u64, Vec<u32>, bool, bool) {
        let read = ingress::read_object_record(bytes, ingress_object_spec_from_plan(spec), false)
            .expect("token record should be valid");
        let ingress::ObjectRecordRead::Complete(record) = read else {
            panic!("token record should be complete");
        };
        let payload = record.payload(bytes).expect("token payload is present");
        let tokens = payload
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        assert!(payload.chunks_exact(4).remainder().is_empty());
        (
            record.sequence,
            tokens,
            record.flags.end_of_sequence,
            record.flags.begin_sequence,
        )
    }

    struct PipelineRuntimeTestFixture {
        actor_runtime: Arc<swactor::runtime::Runtime>,
        tokenizer_events: swactor::runtime::Inbox<TokenizerEvent>,
        encode_requests: swactor::runtime::Inbox<NodeAgentMsg>,
        decode_requests: swactor::runtime::Inbox<NodeAgentMsg>,
        runtime: PipelinePromptRuntime,
        token_in_rx: tokio_mpsc::UnboundedReceiver<Vec<u8>>,
    }

    fn pipeline_runtime_fixture() -> PipelineRuntimeTestFixture {
        let spec = token_object_spec(64, 64);
        let (token_in_tx, token_in_rx) = tokio_mpsc::unbounded_channel();
        let (recv_tx, recv_rx) = mpsc::channel();
        pipeline_runtime_fixture_from_specs(
            11,
            12,
            spec,
            spec,
            token_in_tx,
            token_in_rx,
            recv_tx,
            recv_rx,
        )
    }

    fn pipeline_runtime_fixture_from_plan(stage_count: u32) -> PipelineRuntimeTestFixture {
        let model = TempModelFile::new(&format!("prompt-plan-{stage_count}.gguf"));
        let config = pipeline_config_with_cached_model(&model, stage_count);
        let plan = config.build_run_plan().expect("prompt runtime plan builds");
        let token_in = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::TokenIn)
            .expect("token-in edge");
        let token_out = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::TokenOut)
            .expect("token-out edge");
        let (token_in_tx, token_in_rx) = tokio_mpsc::unbounded_channel();
        let (recv_tx, recv_rx) = mpsc::channel();
        pipeline_runtime_fixture_from_specs(
            token_in.edge_id.0,
            token_out.edge_id.0,
            token_in.object_spec,
            token_out.object_spec,
            token_in_tx,
            token_in_rx,
            recv_tx,
            recv_rx,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn pipeline_runtime_fixture_from_specs(
        token_in_edge_id: u64,
        token_out_edge_id: u64,
        token_spec: run_plan::ObjectSpec,
        token_out_spec: run_plan::ObjectSpec,
        token_in_tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
        token_in_rx: tokio_mpsc::UnboundedReceiver<Vec<u8>>,
        recv_tx: mpsc::Sender<Vec<u8>>,
        recv_rx: mpsc::Receiver<Vec<u8>>,
    ) -> PipelineRuntimeTestFixture {
        let actor_runtime = Arc::new(swactor::runtime::Runtime::new(
            swactor::config::RuntimeConfig::default(),
        ));
        let encode_requests = actor_runtime
            .new_inbox::<NodeAgentMsg>()
            .expect("encode request inbox");
        let decode_requests = actor_runtime
            .new_inbox::<NodeAgentMsg>()
            .expect("decode request inbox");
        let tokenizer_events = actor_runtime
            .new_inbox::<TokenizerEvent>()
            .expect("tokenizer event inbox");
        PipelineRuntimeTestFixture {
            runtime: PipelinePromptRuntime {
                token_in_edge_id,
                token_out_edge_id,
                token_spec,
                token_out_spec,
                token_in_sender: PipelineSendHandle::Channel(token_in_tx),
                recv_rx,
                recv_tx,
                recv_buffer: Vec::new(),
                tokenizer_encode_actor: *encode_requests.addr(),
                tokenizer_decode_actor: *decode_requests.addr(),
                tokenizer_reply_to: *tokenizer_events.addr(),
                pending_encode: None,
                pending_decode: None,
                next_sequence: 0,
                generated_tokens: Vec::new(),
                final_text: String::new(),
                active: None,
                started_at: None,
            },
            actor_runtime,
            tokenizer_events,
            encode_requests,
            decode_requests,
            token_in_rx,
        }
    }

    fn start_fixture_prompt(
        fixture: &mut PipelineRuntimeTestFixture,
        request: SubmitPrompt,
        events: mpsc::Sender<PromptEvent>,
        datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) {
        fixture
            .runtime
            .start_prompt(
                request,
                events,
                &fixture.actor_runtime,
                None,
                datastream,
                run_id,
                node_id,
            )
            .expect("pipeline prompt starts");
    }

    fn drain_fixture_tokenizer(
        fixture: &mut PipelineRuntimeTestFixture,
        datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) {
        fixture
            .runtime
            .drain_tokenizer_events(
                &fixture.actor_runtime,
                &fixture.tokenizer_events,
                None,
                datastream,
                run_id,
                node_id,
            )
            .expect("tokenizer events drain");
    }

    fn send_encoded_tokens(
        fixture: &PipelineRuntimeTestFixture,
        request_id: u64,
        tokens: Vec<u32>,
    ) {
        fixture
            .actor_runtime
            .send_to(
                *fixture.tokenizer_events.addr(),
                TokenizerEvent::PromptEncoded { request_id, tokens },
            )
            .expect("send encoded tokens");
    }

    fn send_decoded_text(fixture: &PipelineRuntimeTestFixture, request_id: u64, text: &str) {
        fixture
            .actor_runtime
            .send_to(
                *fixture.tokenizer_events.addr(),
                TokenizerEvent::TokensDecoded {
                    request_id,
                    text: text.to_owned(),
                },
            )
            .expect("send decoded text");
    }

    fn assert_encode_request(fixture: &PipelineRuntimeTestFixture, request_id: u64, prompt: &str) {
        match fixture.encode_requests.try_recv() {
            Some(NodeAgentMsg::EncodePrompt {
                request_id: actual_request_id,
                prompt: actual_prompt,
                reply_to,
            }) => {
                assert_eq!(actual_request_id, request_id);
                assert_eq!(actual_prompt, prompt);
                assert_eq!(reply_to, *fixture.tokenizer_events.addr());
            }
            other => panic!("expected EncodePrompt request, got {other:?}"),
        }
    }

    fn assert_decode_request(fixture: &PipelineRuntimeTestFixture, request_id: u64, token: u32) {
        match fixture.decode_requests.try_recv() {
            Some(NodeAgentMsg::DecodeTokens {
                request_id: actual_request_id,
                tokens,
                reply_to,
            }) => {
                assert_eq!(actual_request_id, request_id);
                assert_eq!(tokens, vec![token]);
                assert_eq!(reply_to, *fixture.tokenizer_events.addr());
            }
            other => panic!("expected DecodeTokens request, got {other:?}"),
        }
    }

    #[test]
    fn pipeline_prompt_token_record_round_trips_one_token_with_eos_and_plan_ring_alignment() {
        let spec = token_object_spec(64, 64);
        let bytes = encode_token_record(spec, 12, 7, &[513], true).expect("token record encodes");
        assert_eq!(&bytes[0..4], b"MO01");

        let mut partial = bytes[..run_plan::MO01_HEADER_BYTES as usize + 2].to_vec();
        assert!(
            take_pipeline_token_record(&mut partial, spec)
                .expect("partial token record is not malformed")
                .is_none()
        );
        assert_eq!(partial.len(), run_plan::MO01_HEADER_BYTES as usize + 2);

        let mut buffer = bytes;
        let record = take_pipeline_token_record(&mut buffer, spec)
            .expect("one-u32 token-out payload should parse despite ring alignment")
            .expect("complete token-out record is available");

        assert_eq!(record.object_id, 9007);
        assert_eq!(record.sequence, 7);
        assert_eq!(record.token_id, 513);
        assert!(record.eos);
        assert!(buffer.is_empty());
    }

    #[test]
    fn plan_derived_mo01_specs_accept_token_and_activation_record_extents() {
        let metadata = TestGgufMetadata {
            num_layers: 7,
            hidden_dim: 13,
            context_length: 64,
            eos_token_id: 11,
        };
        let model = TempModelFile::with_metadata("plan-mo01.gguf", metadata);
        let config = pipeline_config_with_cached_model(&model, 3);
        let plan = config
            .build_run_plan()
            .expect("plan-derived MO01 plan builds");
        let token_in = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::TokenIn)
            .expect("token-in edge");
        let activation = plan
            .edges
            .iter()
            .find(|edge| edge.kind == run_plan::EdgeKind::Activation)
            .expect("activation edge");

        let token_record = encode_token_record(
            token_in.object_spec,
            token_in.edge_id.0,
            0,
            &[65, 195, 169],
            false,
        )
        .expect("plan token record encodes");
        assert_eq!(
            decode_token_record_payload(&token_record, token_in.object_spec),
            (0, vec![65, 195, 169], false)
        );

        let activation_payload = vec![
            0_u8;
            usize::try_from(metadata.hidden_dim * 2)
                .expect("activation payload size fits usize")
        ];
        let activation_record = ingress::ObjectRecordBuilder::new(ingress_object_spec_from_plan(
            activation.object_spec,
        ))
        .object_id(ingress::ObjectId(9100))
        .sequence(0)
        .payload(activation_payload)
        .encode();
        let read = ingress::read_object_record(
            &activation_record,
            ingress_object_spec_from_plan(activation.object_spec),
            false,
        )
        .expect("plan activation record parses");
        let ingress::ObjectRecordRead::Complete(record) = read else {
            panic!("activation record should be complete");
        };
        assert_eq!(record.payload(&activation_record).unwrap().len(), 26);
    }

    #[test]
    fn pipeline_prompt_token_out_parser_rejects_payloads_that_are_not_one_u32() {
        let spec = token_object_spec(64, 64);
        let mut buffer =
            encode_token_record(spec, 12, 0, &[65, 66], false).expect("multi-token record encodes");

        let error = take_pipeline_token_record(&mut buffer, spec)
            .expect_err("token-out records must carry exactly one generated token");

        assert!(
            error.contains("token-out payload must be exactly one u32, got 8"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn pipeline_prompt_runtime_uses_tokenizer_events_for_encode_decode_and_continuations() {
        let mut fixture = pipeline_runtime_fixture();
        let (event_tx, event_rx) = mpsc::channel();
        let mut datastream = OrchDatastream::new(91, None).expect("datastream opens");
        start_fixture_prompt(
            &mut fixture,
            SubmitPrompt {
                request_id: 42,
                prompt_text: "Hi".to_owned(),
                max_tokens: 2,
            },
            event_tx,
            &mut datastream,
            91,
            3,
        );
        assert_encode_request(&fixture, 42, "Hi");
        send_encoded_tokens(&fixture, 42, vec![1001, 1002]);
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 91, 3);

        let initial = fixture
            .token_in_rx
            .try_recv()
            .expect("tokenizer tokens are sent to token-in");
        assert_eq!(
            decode_token_record_payload(&initial, fixture.runtime.token_spec),
            (0, vec![1001, 1002], false)
        );
        assert!(fixture.token_in_rx.try_recv().is_err());

        fixture
            .runtime
            .recv_tx
            .send(
                encode_token_record(
                    fixture.runtime.token_out_spec,
                    fixture.runtime.token_out_edge_id,
                    0,
                    &[79],
                    false,
                )
                .expect("first token-out record encodes"),
            )
            .expect("token-out bytes enqueue");
        fixture
            .runtime
            .drain_tokens(&fixture.actor_runtime, None, &mut datastream, 91, 3)
            .expect("first token drains");
        assert_decode_request(&fixture, 42, 79);
        assert!(event_rx.try_recv().is_err());

        send_decoded_text(&fixture, 42, "O");
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 91, 3);
        assert_eq!(
            event_rx.try_recv().expect("first delta event"),
            PromptEvent::TextDelta {
                request_id: 42,
                text: "O".to_owned(),
            }
        );
        let continuation = fixture
            .token_in_rx
            .try_recv()
            .expect("non-terminal token is fed back to token-in");
        assert_eq!(
            decode_token_record_payload(&continuation, fixture.runtime.token_spec),
            (1, vec![79], false)
        );

        fixture
            .runtime
            .recv_tx
            .send(
                encode_token_record(
                    fixture.runtime.token_out_spec,
                    fixture.runtime.token_out_edge_id,
                    1,
                    &[75],
                    false,
                )
                .expect("second token-out record encodes"),
            )
            .expect("token-out bytes enqueue");
        fixture
            .runtime
            .drain_tokens(&fixture.actor_runtime, None, &mut datastream, 91, 3)
            .expect("second token drains");
        assert_decode_request(&fixture, 42, 75);

        send_decoded_text(&fixture, 42, "K");
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 91, 3);
        assert_eq!(
            event_rx.try_recv().expect("second delta event"),
            PromptEvent::TextDelta {
                request_id: 42,
                text: "K".to_owned(),
            }
        );
        match event_rx.try_recv().expect("done event at max tokens") {
            PromptEvent::Done {
                request_id,
                final_text,
                tokens_generated,
                ..
            } => {
                assert_eq!(request_id, 42);
                assert_eq!(final_text, "OK");
                assert_eq!(tokens_generated, 2);
            }
            event => panic!("expected Done at max tokens, got {event:?}"),
        }
        assert!(event_rx.try_recv().is_err());
        assert!(fixture.token_in_rx.try_recv().is_err());
        assert!(!fixture.runtime.is_active());
    }

    #[test]
    fn pipeline_prompt_runtime_marks_each_prompt_start_without_resetting_stream_sequence() {
        let mut fixture = pipeline_runtime_fixture();
        let (first_event_tx, first_event_rx) = mpsc::channel();
        let mut datastream = OrchDatastream::new(94, None).expect("datastream opens");
        start_fixture_prompt(
            &mut fixture,
            SubmitPrompt {
                request_id: 101,
                prompt_text: "first".to_owned(),
                max_tokens: 1,
            },
            first_event_tx,
            &mut datastream,
            94,
            3,
        );
        assert_encode_request(&fixture, 101, "first");
        send_encoded_tokens(&fixture, 101, vec![10, 11]);
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 94, 3);
        let first_initial = fixture
            .token_in_rx
            .try_recv()
            .expect("first prompt token-in record is emitted");
        assert_eq!(
            decode_token_record_with_flags(&first_initial, fixture.runtime.token_spec),
            (0, vec![10, 11], false, true)
        );
        fixture
            .runtime
            .recv_tx
            .send(
                encode_token_record(
                    fixture.runtime.token_out_spec,
                    fixture.runtime.token_out_edge_id,
                    0,
                    &[21],
                    false,
                )
                .expect("first token-out record encodes"),
            )
            .expect("first token-out bytes enqueue");
        fixture
            .runtime
            .drain_tokens(&fixture.actor_runtime, None, &mut datastream, 94, 3)
            .expect("first token drains");
        assert_decode_request(&fixture, 101, 21);
        send_decoded_text(&fixture, 101, "A");
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 94, 3);
        assert!(matches!(
            first_event_rx.try_recv().expect("first delta"),
            PromptEvent::TextDelta {
                request_id: 101,
                ..
            }
        ));
        assert!(matches!(
            first_event_rx.try_recv().expect("first done"),
            PromptEvent::Done {
                request_id: 101,
                ..
            }
        ));
        assert!(!fixture.runtime.is_active());

        let (second_event_tx, _second_event_rx) = mpsc::channel();
        start_fixture_prompt(
            &mut fixture,
            SubmitPrompt {
                request_id: 102,
                prompt_text: "second".to_owned(),
                max_tokens: 1,
            },
            second_event_tx,
            &mut datastream,
            94,
            3,
        );
        assert_encode_request(&fixture, 102, "second");
        send_encoded_tokens(&fixture, 102, vec![20, 22, 24]);
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 94, 3);
        let second_initial = fixture
            .token_in_rx
            .try_recv()
            .expect("second prompt token-in record is emitted");
        assert_eq!(
            decode_token_record_with_flags(&second_initial, fixture.runtime.token_spec),
            (1, vec![20, 22, 24], false, true)
        );
    }

    #[test]
    fn prompt_runtime_uses_plan_derived_token_edges_for_cached_n_one() {
        let mut fixture = pipeline_runtime_fixture_from_plan(1);
        let (event_tx, event_rx) = mpsc::channel();
        let mut datastream = OrchDatastream::new(93, None).expect("datastream opens");
        start_fixture_prompt(
            &mut fixture,
            SubmitPrompt {
                request_id: 88,
                prompt_text: "Aé".to_owned(),
                max_tokens: 1,
            },
            event_tx,
            &mut datastream,
            93,
            3,
        );
        assert_encode_request(&fixture, 88, "Aé");
        send_encoded_tokens(&fixture, 88, vec![321, 654]);
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 93, 3);

        let initial = fixture
            .token_in_rx
            .try_recv()
            .expect("plan-derived token-in record is emitted");
        assert_eq!(
            decode_token_record_payload(&initial, fixture.runtime.token_spec),
            (0, vec![321, 654], false)
        );
        assert_eq!(fixture.runtime.token_in_edge_id, 1);
        assert_eq!(fixture.runtime.token_out_edge_id, 2);

        fixture
            .runtime
            .recv_tx
            .send(
                encode_token_record(
                    fixture.runtime.token_out_spec,
                    fixture.runtime.token_out_edge_id,
                    0,
                    &[33],
                    false,
                )
                .expect("plan-derived token-out record encodes"),
            )
            .expect("token-out bytes enqueue");
        fixture
            .runtime
            .drain_tokens(&fixture.actor_runtime, None, &mut datastream, 93, 3)
            .expect("plan-derived token drains");
        assert_decode_request(&fixture, 88, 33);
        send_decoded_text(&fixture, 88, "!");
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 93, 3);

        assert_eq!(
            event_rx.try_recv().expect("delta event"),
            PromptEvent::TextDelta {
                request_id: 88,
                text: "!".to_owned(),
            }
        );
        match event_rx.try_recv().expect("done event at max tokens") {
            PromptEvent::Done {
                request_id,
                final_text,
                tokens_generated,
                ..
            } => {
                assert_eq!(request_id, 88);
                assert_eq!(final_text, "!");
                assert_eq!(tokens_generated, 1);
            }
            event => panic!("expected Done at max tokens, got {event:?}"),
        }
        assert!(fixture.token_in_rx.try_recv().is_err());
    }

    #[test]
    fn pipeline_prompt_runtime_finishes_on_eos_without_feedback_token() {
        let mut fixture = pipeline_runtime_fixture();
        let (event_tx, event_rx) = mpsc::channel();
        let mut datastream = OrchDatastream::new(92, None).expect("datastream opens");
        start_fixture_prompt(
            &mut fixture,
            SubmitPrompt {
                request_id: 77,
                prompt_text: "go".to_owned(),
                max_tokens: 8,
            },
            event_tx,
            &mut datastream,
            92,
            4,
        );
        assert_encode_request(&fixture, 77, "go");
        send_encoded_tokens(&fixture, 77, vec![700]);
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 92, 4);
        fixture
            .token_in_rx
            .try_recv()
            .expect("initial prompt token-in record is sent");

        fixture
            .runtime
            .recv_tx
            .send(
                encode_token_record(
                    fixture.runtime.token_out_spec,
                    fixture.runtime.token_out_edge_id,
                    0,
                    &[33],
                    true,
                )
                .expect("eos token-out record encodes"),
            )
            .expect("token-out bytes enqueue");
        fixture
            .runtime
            .drain_tokens(&fixture.actor_runtime, None, &mut datastream, 92, 4)
            .expect("eos token drains");
        assert_decode_request(&fixture, 77, 33);
        send_decoded_text(&fixture, 77, "!");
        drain_fixture_tokenizer(&mut fixture, &mut datastream, 92, 4);

        assert_eq!(
            event_rx.try_recv().expect("delta event before eos done"),
            PromptEvent::TextDelta {
                request_id: 77,
                text: "!".to_owned(),
            }
        );
        match event_rx.try_recv().expect("done event on eos") {
            PromptEvent::Done {
                request_id,
                final_text,
                tokens_generated,
                ..
            } => {
                assert_eq!(request_id, 77);
                assert_eq!(final_text, "!");
                assert_eq!(tokens_generated, 1);
            }
            event => panic!("expected Done on eos, got {event:?}"),
        }
        assert!(event_rx.try_recv().is_err());
        assert!(fixture.token_in_rx.try_recv().is_err());
        assert!(!fixture.runtime.is_active());
    }

    #[test]
    fn planned_prompt_runtime_uses_pipeline_edges_for_single_or_multi_stage_cached_models() {
        assert_eq!(
            prompt_runtime_mode(None),
            PromptRuntimeMode::DirectInferPrompt
        );

        for stage_count in [1_u32, 3] {
            let model = TempModelFile::new(&format!("prompt-runtime-{stage_count}.gguf"));
            let config = pipeline_config_with_cached_model(&model, stage_count);
            let plan = config
                .build_run_plan()
                .expect("cached-model planned prompt runtime plan builds");

            assert_eq!(
                prompt_runtime_mode(Some(&plan)),
                PromptRuntimeMode::PipelineTokenEdges,
                "cached-model N={stage_count} must use the planned token-edge runtime"
            );
        }
    }

    #[test]
    fn docker_container_prefix_defaults_and_trims_env_override() {
        with_clean_env(&[], || {
            assert_eq!(docker_container_prefix(), DEFAULT_DOCKER_CONTAINER_PREFIX);
        });
        with_clean_env(
            &[(MVP_DOCKER_CONTAINER_PREFIX_ENV, " custom-prefix ")],
            || {
                assert_eq!(docker_container_prefix(), "custom-prefix");
            },
        );
    }

    #[test]
    fn expand_home_path_expands_leading_home_segment() {
        with_clean_env(&[("HOME", "/tmp/mvp-vastai-home")], || {
            assert_eq!(
                expand_home_path("~/keys/deploy").expect("home path expands"),
                PathBuf::from("/tmp/mvp-vastai-home/keys/deploy")
            );
            assert_eq!(
                expand_home_path("/tmp/not-~/expanded").expect("literal path stays literal"),
                PathBuf::from("/tmp/not-~/expanded")
            );
        });
    }

    #[test]
    fn account_ssh_keys_output_contains_public_key_matches_exact_key_material() {
        let public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestKeyBody vastai";

        assert!(account_ssh_keys_output_contains_public_key(
            public_key, public_key
        ));
        assert!(account_ssh_keys_output_contains_public_key(
            r#"{"keys":[{"public_key":"AAAAC3NzaC1lZDI1NTE5AAAAITestKeyBody"}]}"#,
            public_key
        ));
        assert!(!account_ssh_keys_output_contains_public_key(
            r#"{"keys":[{"public_key":"AAAAC3NzaC1lZDI1NTE5AAAADifferent"}]}"#,
            public_key
        ));
    }

    #[test]
    fn vastai_config_reads_ssh_identity_without_runtime_preparation() {
        let config = with_clean_env(
            &[
                ("MVP_RUNTIME_CONFIG", "deploy"),
                ("MVP_NODE_PROVIDER", "vastai"),
                ("MVP_VASTAI_API_KEY", "vast-key"),
                ("MVP_VASTAI_BOOTSTRAP_COMMAND", "/usr/local/bin/mvp-node"),
                ("MVP_VASTAI_SSH_IDENTITY", "/tmp/mvp-vastai-key"),
            ],
            || {
                Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                    .expect("vastai config parses without ssh-keygen or vastai CLI")
            },
        );
        let vastai = config.vastai.expect("vastai config is present");
        assert_eq!(
            vastai.ssh_identity.as_deref(),
            Some(Path::new("/tmp/mvp-vastai-key"))
        );
        assert_eq!(vastai.ssh_public_key, None);
        assert_eq!(vastai.ssh_public_fingerprint, None);
    }

    #[derive(Clone, Copy)]
    struct TestGgufMetadata {
        num_layers: u32,
        hidden_dim: u64,
        context_length: u64,
        eos_token_id: u32,
    }

    impl Default for TestGgufMetadata {
        fn default() -> Self {
            Self {
                num_layers: 7,
                hidden_dim: 13,
                context_length: 64,
                eos_token_id: 11,
            }
        }
    }

    struct TempModelFile {
        root: PathBuf,
        raw_path: PathBuf,
        canonical_path: PathBuf,
        metadata: TestGgufMetadata,
    }

    impl TempModelFile {
        fn new(file_name: &str) -> Self {
            Self::with_metadata(file_name, TestGgufMetadata::default())
        }

        fn with_metadata(file_name: &str, metadata: TestGgufMetadata) -> Self {
            let root = std::env::temp_dir().join(format!(
                "mvp-cached-model-test-{}-{}",
                std::process::id(),
                std::thread::current().name().unwrap_or("unnamed")
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(root.join("nested")).expect("create temp model dir");
            let canonical_path = root.join(file_name);
            std::fs::write(&canonical_path, minimal_gguf(metadata))
                .expect("write temp GGUF metadata file");
            let raw_path = root.join("nested").join("..").join(file_name);
            Self {
                root,
                raw_path,
                canonical_path: canonical_path
                    .canonicalize()
                    .expect("canonicalize temp model file"),
                metadata,
            }
        }
    }

    fn minimal_gguf(metadata: TestGgufMetadata) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"GGUF");
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&6_u64.to_le_bytes());
        push_string_kv(&mut bytes, "general.architecture", "llama");
        push_string_kv(&mut bytes, "general.name", "fixture");
        push_u32_kv(&mut bytes, "llama.block_count", metadata.num_layers);
        push_u32_kv(
            &mut bytes,
            "llama.embedding_length",
            metadata
                .hidden_dim
                .try_into()
                .expect("test hidden dimension fits u32"),
        );
        push_u32_kv(
            &mut bytes,
            "llama.context_length",
            metadata
                .context_length
                .try_into()
                .expect("test context length fits u32"),
        );
        push_u32_kv(
            &mut bytes,
            "tokenizer.ggml.eos_token_id",
            metadata.eos_token_id,
        );
        bytes
    }

    fn push_string_kv(bytes: &mut Vec<u8>, key: &str, value: &str) {
        push_gguf_string(bytes, key);
        bytes.extend_from_slice(&8_u32.to_le_bytes());
        push_gguf_string(bytes, value);
    }

    fn push_u32_kv(bytes: &mut Vec<u8>, key: &str, value: u32) {
        push_gguf_string(bytes, key);
        bytes.extend_from_slice(&4_u32.to_le_bytes());
        bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_gguf_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }

    impl Drop for TempModelFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    struct TempTomlFile {
        path: PathBuf,
    }

    impl TempTomlFile {
        fn new(file_name: &str, contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "mvp-orchestrator-config-test-{}-{}-{file_name}",
                std::process::id(),
                std::thread::current().name().unwrap_or("unnamed")
            ));
            let _ = std::fs::remove_file(&path);
            std::fs::write(&path, contents).expect("write temp TOML config");
            Self { path }
        }
    }

    impl Drop for TempTomlFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[test]
    fn frame_archive_writes_jsonl_records_for_text_and_binary_payloads() {
        static NEXT_TEMP_FILE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let suffix = NEXT_TEMP_FILE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mvp-frame-archive-test-{}-{suffix}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let stream = StreamId::new("test-node", Lifetime(42));
        let mut archive = FrameArchive::open(&path).expect("frame archive opens");
        archive.record(
            "orchestrator",
            &stream,
            "stdout",
            &Frame::new(
                ChannelId(1),
                datastream::Position(7),
                b"hello \xce\xbb".to_vec(),
            ),
        );
        archive.record(
            "orchestrator",
            &stream,
            "stderr",
            &Frame::new(
                ChannelId(2),
                datastream::Position(8),
                vec![0xff, 0x00, b'A'],
            ),
        );
        drop(archive);

        let contents = std::fs::read_to_string(&path).expect("read frame archive jsonl");
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("archive line is json"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["arrival_seq"], json!(0));
        assert!(
            records[0]["arrival_unix_ms"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        assert_eq!(records[0]["source"], json!("orchestrator"));
        assert_eq!(records[0]["stream"], json!("test-node#42"));
        assert_eq!(records[0]["channel"], json!("stdout"));
        assert_eq!(records[0]["channel_id"], json!(1));
        assert_eq!(records[0]["position"], json!(7));
        assert_eq!(
            records[0]["payload"],
            json!({"encoding":"utf8","value":"hello λ"})
        );

        assert_eq!(records[1]["arrival_seq"], json!(1));
        assert!(
            records[1]["arrival_unix_ms"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        assert_eq!(records[1]["source"], json!("orchestrator"));
        assert_eq!(records[1]["stream"], json!("test-node#42"));
        assert_eq!(records[1]["channel"], json!("stderr"));
        assert_eq!(records[1]["channel_id"], json!(2));
        assert_eq!(records[1]["position"], json!(8));
        assert_eq!(
            records[1]["payload"],
            json!({"encoding":"bytes","value":[255,0,65]})
        );
    }

    #[test]
    fn orchestrator_stdio_drain_archives_stdout_and_stderr_as_provision_logs() {
        static NEXT_TEMP_FILE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        let suffix = NEXT_TEMP_FILE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mvp-orch-stdio-test-{}-{suffix}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let (tx, rx) = mpsc::channel();
        tx.send(OrchStdioLine {
            stream: ProvisionLogStream::Stdout,
            line: "offer pool selected".to_owned(),
        })
        .expect("send stdout line");
        tx.send(OrchStdioLine {
            stream: ProvisionLogStream::Stderr,
            line: "lease chain detail".to_owned(),
        })
        .expect("send stderr line");

        let mut datastream = OrchDatastream::new(77, Some(&path)).expect("datastream opens");
        drain_orch_stdio_capture(Some(&rx), &mut datastream, None, 77, 9);
        drop(datastream);

        let contents = std::fs::read_to_string(&path).expect("read frame archive jsonl");
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("archive line is json"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);

        let stdout_record = records
            .iter()
            .rev()
            .find(|record| record["channel"] == "mvp.provisioning.logs.node.9.stdout")
            .expect("stdout provisioning log frame archived");
        let stderr_record = records
            .iter()
            .rev()
            .find(|record| record["channel"] == "mvp.provisioning.logs.node.9.stderr")
            .expect("stderr provisioning log frame archived");
        assert_eq!(stdout_record["source"], "orchestrator");
        assert_eq!(stderr_record["source"], "orchestrator");

        let stdout_payload = serde_json::from_str::<Value>(
            stdout_record["payload"]["value"]
                .as_str()
                .expect("stdout payload is archived as text"),
        )
        .expect("stdout payload is log record json");
        let stderr_payload = serde_json::from_str::<Value>(
            stderr_record["payload"]["value"]
                .as_str()
                .expect("stderr payload is archived as text"),
        )
        .expect("stderr payload is log record json");
        assert_eq!(stdout_payload["line"]["run_id"], 77);
        assert_eq!(stdout_payload["line"]["node_id"], 9);
        assert_eq!(stdout_payload["line"]["stream"], "Stdout");
        assert_eq!(stdout_payload["line"]["line"], "offer pool selected");
        assert_eq!(stderr_payload["line"]["stream"], "Stderr");
        assert_eq!(stderr_payload["line"]["line"], "lease chain detail");
    }

    #[test]
    fn config_layers_defaults_toml_env_then_cli() {
        let toml = TempTomlFile::new(
            "layering.toml",
            r#"
[runtime]
profile = "deploy"
run_id = 41
node_id = 9
stage_index = 3
layer_end_exclusive = 24

[provider]
kind = "vastai"

[image]
node = "docker.io/example/from-image-node:toml"

[vastai]
image = "docker.io/example/from-vastai-image:toml"
api_key = "toml-key"
bootstrap_command = "/toml/bootstrap"
disk_gb = 60
gpu_name = "RTX 4090"

[prompt]
rpc_addr = "127.0.0.1:19999"
max_tokens = 17
dashboard = true

[model]
id = "toml-model"
gguf_repo = "toml/repo"
gguf_file = "toml.gguf"
gguf_revision = "toml-rev"
max_context = 384

[relay]
mode = "disabled"
"#,
        );

        let config = with_clean_env(
            &[
                ("MVP_NODE_PROVIDER", "docker"),
                ("MVP_NODE_IMAGE", "docker.io/example/from-env:latest"),
                ("MVP_PROMPT_MAX_TOKENS", "23"),
                ("MVP_MODEL_ID", "env-model"),
                ("MVP_IROH_RELAY_MODE", "default"),
            ],
            || {
                Config::from_layers_with_path_and_args(
                    Some(&toml.path),
                    [
                        "--image",
                        "docker.io/example/from-cli:latest",
                        "--max-tokens",
                        "31",
                        "--model-id",
                        "cli-model",
                        "--max-context",
                        "768",
                    ]
                    .into_iter()
                    .map(str::to_owned),
                )
                .expect("layered config parses")
            },
        );

        assert_eq!(config.provider, ProviderKind::Docker);
        assert_eq!(config.image, "docker.io/example/from-cli:latest");
        assert_eq!(config.default_max_tokens, 31);
        assert_eq!(config.model_id, "cli-model");
        assert_eq!(config.run_id, 41);
        assert_eq!(config.node_id, 9);
        assert_eq!(config.stage_index, 3);
        assert_eq!(config.layer_end_exclusive, Some(24));
        assert!(config.dashboard);
        assert_eq!(config.max_context, Some(768));
        assert!(matches!(config.relay.mode, iroh::RelayMode::Default));
        assert!(config.vastai.is_none());
    }

    #[test]
    fn pipeline_stages_layers_toml_env_then_cli_aliases() {
        let toml = TempTomlFile::new(
            "pipeline-stages.toml",
            r#"
[runtime]
pipeline_stages = 2

[provider]
kind = "docker"
"#,
        );

        for (case, settings, args, expected) in [
            ("toml", vec![], vec![], 2),
            (
                "env-over-toml",
                vec![("MVP_PIPELINE_STAGES", "3")],
                vec![],
                3,
            ),
            (
                "long-cli-over-env",
                vec![("MVP_PIPELINE_STAGES", "3")],
                vec!["--pipeline-stages", "4"],
                4,
            ),
            (
                "short-cli-over-env",
                vec![("MVP_PIPELINE_STAGES", "3")],
                vec!["-N", "5"],
                5,
            ),
        ] {
            let config = with_clean_env(&settings, || {
                Config::from_layers_with_path_and_args(
                    Some(&toml.path),
                    args.iter().copied().map(str::to_owned),
                )
                .unwrap_or_else(|error| {
                    panic!("{case} pipeline stage config should parse: {error}")
                })
            });

            assert_eq!(config.pipeline_stages, expected, "{case}");
        }
    }

    #[test]
    fn pipeline_stages_rejects_zero_missing_and_non_numeric_values() {
        for (case, settings, args, expected) in [
            (
                "env-zero",
                vec![("MVP_PIPELINE_STAGES", "0")],
                vec![],
                "--pipeline-stages must be greater than 0",
            ),
            (
                "env-non-numeric",
                vec![("MVP_PIPELINE_STAGES", "many")],
                vec![],
                "invalid MVP_PIPELINE_STAGES=\"many\"",
            ),
            (
                "long-cli-zero",
                vec![],
                vec!["--pipeline-stages", "0"],
                "--pipeline-stages must be greater than 0",
            ),
            (
                "long-cli-non-numeric",
                vec![],
                vec!["--pipeline-stages", "many"],
                "invalid --pipeline-stages=\"many\"",
            ),
            (
                "short-cli-missing",
                vec![],
                vec!["-N"],
                "missing value after -N",
            ),
        ] {
            let error =
                with_clean_env(&settings, || {
                    match Config::from_layers_with_path_and_args(
                        None,
                        args.iter().copied().map(str::to_owned),
                    ) {
                        Ok(_) => panic!("invalid pipeline stages setting must fail"),
                        Err(error) => error,
                    }
                });

            assert!(
                error.contains(expected),
                "{case} error {error:?} should contain {expected:?}"
            );
        }
    }

    #[test]
    fn vastai_eight_stage_plan_uses_remote_gguf_and_no_mounts() {
        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                None,
                [
                    "--provider",
                    "vastai",
                    "--pipeline-stages",
                    "8",
                    "--model-id",
                    "smollm2-135m-instruct-q4",
                    "--gguf-repo",
                    "QuantFactory/SmolLM2-135M-Instruct-GGUF",
                    "--gguf-file",
                    DEFAULT_PIPELINE_CACHED_MODEL_FILE,
                    "--max-context",
                    "256",
                    "--relay-mode",
                    "default",
                    "--vastai-bootstrap-command",
                    "boot",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .expect("VastAI eight-stage pipeline config parses")
        });
        let plan = config
            .build_run_plan()
            .expect("VastAI eight-stage run plan uses local metadata only");
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[41; 32]).public());
        let orchestrator_actor = ActorAddress([42; 32]);

        let specs = stage_node_specs(&config, Some(&plan), coordinator, orchestrator_actor)
            .expect("VastAI pipeline stage node specs build");

        assert_eq!(config.provider, ProviderKind::VastAi);
        assert!(
            config.cached_model.is_none(),
            "VastAI must not mount host caches"
        );
        assert_eq!(specs.len(), 8);
        for (expected_stage_index, spec) in specs.iter().enumerate() {
            let expected_stage_index =
                u32::try_from(expected_stage_index).expect("fixture stage index fits u32");
            let expected_node_id = config.node_id + 1 + u64::from(expected_stage_index);
            assert_eq!(spec.node_id, expected_node_id);
            assert_eq!(spec.stage_index, Some(expected_stage_index));
            assert_eq!(env_value(&spec.env, "MVP_NODE_PROVIDER"), Some("vastai"));
            assert_eq!(env_value(&spec.env, "MVP_PIPELINE_STAGES"), Some("8"));
            assert_eq!(
                env_value(&spec.env, "MVP_GGUF_REPO"),
                Some("QuantFactory/SmolLM2-135M-Instruct-GGUF")
            );
            assert_eq!(
                env_value(&spec.env, "MVP_GGUF_FILE"),
                Some(DEFAULT_PIPELINE_CACHED_MODEL_FILE)
            );
            assert_eq!(env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"), None);
            assert_eq!(env_value(&spec.env, "MVP_MAX_CONTEXT"), Some("256"));
            assert_eq!(spec.args, vec!["boot".to_owned()]);
            assert!(spec.mounts.is_empty(), "VastAI stage specs must not mount");
        }
    }

    #[test]
    fn pipeline_cached_model_resolution_uses_toml_smollm2_gguf_file_with_cli_stage_count() {
        let toml = TempTomlFile::new(
            "pipeline-smollm2-model.toml",
            r#"
[model]
gguf_repo = "QuantFactory/SmolLM2-135M-Instruct-GGUF"
gguf_file = "SmolLM2-135M-Instruct.Q4_0.gguf"
"#,
        );

        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                Some(&toml.path),
                ["--pipeline-stages", "3"].into_iter().map(str::to_owned),
            )
            .expect("TOML SmolLM2 pipeline config parses")
        });
        let expected_cached_host_path = default_pipeline_cached_model_path()
            .canonicalize()
            .expect("default cached SmolLM2 GGUF is present for cache resolution tests");
        let cached_model = config
            .cached_model
            .as_ref()
            .expect("matching TOML SmolLM2 pipeline source should resolve the default cache");

        assert_eq!(config.pipeline_stages, 3);
        assert_eq!(config.provider, ProviderKind::Process);
        assert_eq!(cached_model.host_path, expected_cached_host_path);
        assert_eq!(
            cached_model.container_path,
            "/models/cached/SmolLM2-135M-Instruct.Q4_0.gguf"
        );
        assert_eq!(
            config.gguf_source,
            GgufSource::LocalPath(expected_cached_host_path.to_string_lossy().to_string())
        );
    }

    #[test]
    fn pipeline_cached_model_resolution_does_not_use_default_cache_for_other_toml_gguf_file() {
        let toml = TempTomlFile::new(
            "pipeline-other-model.toml",
            r#"
[model]
gguf_repo = "QuantFactory/SmolLM2-135M-Instruct-GGUF"
gguf_file = "SmolLM2-135M-Instruct.Q8_0.gguf"
"#,
        );

        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                Some(&toml.path),
                ["--pipeline-stages", "3"].into_iter().map(str::to_owned),
            )
            .expect("non-default TOML GGUF pipeline config parses")
        });

        assert_eq!(config.pipeline_stages, 3);
        assert!(config.cached_model.is_none());
        assert_eq!(
            config.gguf_source,
            GgufSource::HuggingFaceGguf {
                repo: "QuantFactory/SmolLM2-135M-Instruct-GGUF".to_owned(),
                file: "SmolLM2-135M-Instruct.Q8_0.gguf".to_owned(),
                revision: None,
            }
        );

        let error = config
            .build_run_plan()
            .expect_err("non-default remote TOML GGUF should still require an explicit cache");

        assert!(
            error.contains(
                "QuantFactory/SmolLM2-135M-Instruct-GGUF/SmolLM2-135M-Instruct.Q8_0.gguf"
            ),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("--cached-model-host-path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn cached_model_plan_phase_is_metadata_driven_for_each_pipeline_width() {
        let metadata = TestGgufMetadata {
            num_layers: 7,
            hidden_dim: 13,
            context_length: 64,
            eos_token_id: 11,
        };
        let model = TempModelFile::with_metadata("metadata-driven.gguf", metadata);

        for stage_count in [1_u32, 3, metadata.num_layers] {
            let config = pipeline_config_with_cached_model(&model, stage_count);
            assert!(config.uses_planned_execution());
            let plan = config
                .build_run_plan()
                .expect("metadata-derived cached-model plan builds");
            assert_plan_matches_metadata(&config, &plan, metadata, stage_count, 512);
        }
    }

    #[test]
    fn cached_model_implicit_single_stage_matches_explicit_n_one_plan() {
        let model = TempModelFile::new("implicit-n-one.gguf");
        let implicit = cached_model_config_with_args(&model, &["--max-context", "32"]);
        let explicit = cached_model_config_with_args(
            &model,
            &["--pipeline-stages", "1", "--max-context", "32"],
        );

        let implicit_plan = implicit
            .build_run_plan()
            .expect("implicit cached-model N=1 plan builds");
        let explicit_plan = explicit
            .build_run_plan()
            .expect("explicit cached-model N=1 plan builds");

        assert!(implicit.uses_planned_execution());
        assert!(explicit.uses_planned_execution());
        assert_plan_matches_metadata(&implicit, &implicit_plan, model.metadata, 1, 32);
        assert_plan_matches_metadata(&explicit, &explicit_plan, model.metadata, 1, 32);
        assert_eq!(implicit_plan.model, explicit_plan.model);
        assert_eq!(implicit_plan.edges, explicit_plan.edges);
        assert_eq!(implicit_plan.stages, explicit_plan.stages);
    }

    #[test]
    fn cached_model_plan_rejects_pipeline_width_above_model_layers() {
        let metadata = TestGgufMetadata {
            num_layers: 2,
            ..TestGgufMetadata::default()
        };
        let model = TempModelFile::with_metadata("too-many-stages.gguf", metadata);
        let config = pipeline_config_with_cached_model(&model, metadata.num_layers + 1);

        let error = config
            .build_run_plan()
            .expect_err("pipeline width above layer count must reject before provisioning");

        assert!(
            error.contains("--pipeline-stages=3 exceeds GGUF layer count 2"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn pipeline_planning_rejects_remote_gguf_source_without_local_cache() {
        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                None,
                [
                    "--pipeline-stages",
                    "3",
                    "--gguf-repo",
                    "example/remote",
                    "--gguf-file",
                    "remote.gguf",
                ]
                .into_iter()
                .map(str::to_owned),
            )
            .expect("non-default remote pipeline config parses")
        });

        let error = config
            .build_run_plan()
            .expect_err("remote GGUF source cannot be inspected before provisioning");

        assert!(
            error.contains("locally inspectable GGUF before provisioning"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("example/remote/remote.gguf"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains("--cached-model-host-path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn toml_vastai_image_overrides_image_node_for_vastai_provider() {
        let toml = TempTomlFile::new(
            "vastai-image.toml",
            r#"
[runtime]
profile = "deploy"

[provider]
kind = "vastai"

[image]
node = "docker.io/example/generic:toml"

[vastai]
image = "docker.io/example/vastai:toml"
api_key = "k"
bootstrap_command = "/run"
"#,
        );

        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(Some(&toml.path), std::iter::empty::<String>())
                .expect("VastAI TOML config parses")
        });

        assert_eq!(config.provider, ProviderKind::VastAi);
        assert_eq!(config.image, "docker.io/example/vastai:toml");
    }

    #[test]
    fn missing_toml_uses_hardcoded_defaults() {
        let missing_path = std::env::temp_dir().join(format!(
            "mvp-orchestrator-missing-config-{}-{}.toml",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        let _ = std::fs::remove_file(&missing_path);

        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                Some(&missing_path),
                std::iter::empty::<String>(),
            )
            .expect("missing optional TOML config uses defaults")
        });

        assert_eq!(config.provider, ProviderKind::Process);
        assert_eq!(config.image, DEFAULT_IMAGE);
        assert_eq!(config.model_id, DEFAULT_MODEL_ID);
        assert_eq!(config.default_max_tokens, DEFAULT_MAX_TOKENS);
        assert!(!config.dashboard);
        assert_eq!(config.max_context, None);
    }

    #[test]
    fn node_spec_propagates_max_context_when_configured() {
        let config = with_clean_env(&[], || {
            Config::from_layers_with_path_and_args(
                None,
                ["--max-context", "256"].into_iter().map(str::to_owned),
            )
            .expect("CLI max context config parses")
        });
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[3; 32]).public());
        let orchestrator_actor = ActorAddress([18; 32]);
        let spec = config
            .node_spec(coordinator, orchestrator_actor)
            .expect("node spec builds");

        assert_eq!(env_value(&spec.env, "MVP_MAX_CONTEXT"), Some("256"));
    }

    #[test]
    fn runtime_profile_selects_provider_and_node_provider_takes_precedence() {
        assert_eq!(
            selected_provider(&[("MVP_RUNTIME_CONFIG", "local")]),
            ProviderKind::Process
        );
        assert_eq!(
            selected_provider(&[("MVP_RUNTIME_CONFIG", "deploy")]),
            ProviderKind::VastAi
        );
        assert_eq!(
            selected_provider(&[
                ("MVP_RUNTIME_CONFIG", "deploy"),
                ("MVP_NODE_PROVIDER", "docker"),
            ]),
            ProviderKind::Docker
        );
    }

    #[test]
    fn relay_mode_env_uses_default_relay_and_accepts_disabled() {
        with_clean_env(&[], || {
            assert!(matches!(
                relay_mode_from_env().expect("unset relay mode parses"),
                iroh::RelayMode::Default
            ));
        });
        with_clean_env(&[("MVP_IROH_RELAY_MODE", "disabled")], || {
            assert!(matches!(
                relay_mode_from_env().expect("disabled relay mode parses"),
                iroh::RelayMode::Disabled
            ));
        });
    }

    #[test]
    fn node_spec_env_propagates_relay_url_only_for_custom_relay_config() {
        const RELAY_URL: &str = "https://relay-node-spec.example.com";

        let custom_env = node_spec_env(&[(MVP_IROH_RELAY_URL_ENV, RELAY_URL)]);
        let expected_url = canonical_relay_url(RELAY_URL);
        assert_eq!(
            env_value(&custom_env, MVP_IROH_RELAY_URL_ENV),
            Some(expected_url.as_str())
        );

        let disabled_env = node_spec_env(&[
            ("MVP_IROH_RELAY_MODE", "disabled"),
            (MVP_IROH_RELAY_URL_ENV, RELAY_URL),
        ]);
        assert_eq!(env_value(&disabled_env, MVP_IROH_RELAY_URL_ENV), None);
    }

    #[test]
    fn node_spec_preserves_relay_transport_in_coordinator_endpoint() {
        with_clean_env(&[], || {
            let config = Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                .expect("config parses");
            let secret = iroh::SecretKey::from_bytes(&[10; 32]);
            let coordinator = EndpointAddr::new(secret.public()).with_relay_url(
                "http://relay.example.com"
                    .parse::<iroh::RelayUrl>()
                    .unwrap(),
            );
            let orchestrator_actor = ActorAddress([20; 32]);

            let spec = config
                .node_spec(coordinator, orchestrator_actor)
                .expect("node spec builds");
            let coordinator_endpoint_json = env_value(&spec.env, "MVP_COORDINATOR_ENDPOINT")
                .expect("coordinator endpoint env is present");
            let coordinator_endpoint =
                serde_json::from_str::<EndpointAddr>(coordinator_endpoint_json)
                    .expect("coordinator endpoint env deserializes");

            assert_eq!(
                coordinator_endpoint
                    .relay_urls()
                    .next()
                    .map(|url| url.to_string()),
                Some("http://relay.example.com/".to_owned())
            );
        });
    }

    #[test]
    fn docker_config_construction_ignores_malformed_vastai_environment() {
        let config = with_clean_env(
            &[
                ("MVP_RUNTIME_CONFIG", "local"),
                ("MVP_NODE_PROVIDER", "docker"),
                ("MVP_VASTAI_CONFIRM_LEASE", "definitely-not-a-bool"),
                ("MVP_VASTAI_DISK_GB", "not-a-u32"),
                ("MVP_VASTAI_MIN_DOWN_MBPS", "not-a-float"),
            ],
            || {
                Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                    .expect("docker config ignores VastAI-only env")
            },
        );

        assert_eq!(config.provider, ProviderKind::Docker);
        assert!(config.vastai.is_none());
    }

    #[test]
    fn docker_cached_model_builds_local_gguf_env_and_planned_file_mount_from_canonical_host_path() {
        let model = TempModelFile::new("weights-q4.gguf");
        let config = with_clean_env_os(
            &[
                ("MVP_RUNTIME_CONFIG", OsString::from("local")),
                ("MVP_NODE_PROVIDER", OsString::from("docker")),
                (CACHED_MODEL_HOST_ENV, model.raw_path.as_os_str().to_owned()),
            ],
            || {
                Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                    .expect("docker cached model config parses")
            },
        );
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[7; 32]).public());
        let orchestrator_actor = ActorAddress([14; 32]);
        let spec = config
            .node_spec(coordinator, orchestrator_actor)
            .expect("cached model node spec builds");

        assert_eq!(
            env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"),
            Some("/models/cached/weights-q4.gguf")
        );
        assert_eq!(env_value(&spec.env, "MVP_GGUF_REPO"), None);
        assert_eq!(env_value(&spec.env, "MVP_GGUF_FILE"), None);
        assert_eq!(
            spec.mounts,
            vec![ProviderMount {
                host_path: model.canonical_path.to_string_lossy().to_string(),
                container_path: "/models/cached/weights-q4.gguf".to_owned(),
                readonly: true,
            }]
        );
    }

    #[test]
    fn process_cached_model_builds_host_gguf_env_without_mounts() {
        let model = TempModelFile::new("process-weights-q4.gguf");
        let config = with_clean_env_os(
            &[
                ("MVP_RUNTIME_CONFIG", OsString::from("local")),
                (CACHED_MODEL_HOST_ENV, model.raw_path.as_os_str().to_owned()),
            ],
            || {
                Config::from_layers_with_path_and_args(None, std::iter::empty::<String>())
                    .expect("process cached model config parses")
            },
        );
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[8; 32]).public());
        let orchestrator_actor = ActorAddress([15; 32]);
        let spec = config
            .node_spec(coordinator, orchestrator_actor)
            .expect("process cached model node spec builds");
        let host_path = model.canonical_path.to_string_lossy().to_string();

        assert_eq!(config.provider, ProviderKind::Process);
        assert_eq!(env_value(&spec.env, "MVP_NODE_PROVIDER"), Some("process"));
        assert_eq!(
            env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"),
            Some(host_path.as_str())
        );
        assert_eq!(env_value(&spec.env, "MVP_DOCKER_GPUS"), None);
        assert!(spec.mounts.is_empty(), "process workers use host paths");
    }

    #[test]
    fn vectorized_local_docker_stage_node_specs_follow_three_stage_plan() {
        let model = TempModelFile::new("pipeline-node-spec.gguf");
        let config = pipeline_config_with_cached_model(&model, 3);
        let plan = config
            .build_run_plan()
            .expect("stage node spec plan builds");
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[21; 32]).public());
        let orchestrator_actor = ActorAddress([23; 32]);
        let coordinator_env =
            serde_json::to_string(&coordinator).expect("coordinator endpoint serializes");
        let orchestrator_actor_env =
            serde_json::to_string(&orchestrator_actor).expect("orchestrator actor serializes");
        let expected_run_id = config.run_id.to_string();
        let expected_mount = ProviderMount {
            host_path: model.canonical_path.to_string_lossy().to_string(),
            container_path: "/models/cached/pipeline-node-spec.gguf".to_owned(),
            readonly: true,
        };

        let specs = stage_node_specs(&config, Some(&plan), coordinator, orchestrator_actor)
            .expect("pipeline stage node specs build");

        assert_eq!(
            specs.len(),
            3,
            "pipeline_stages=3 should provision three Docker workers, not one"
        );
        assert!(
            specs.iter().all(|spec| spec.node_id != config.node_id),
            "coordinator node {} must not be counted as a worker: {specs:?}",
            config.node_id
        );

        for (expected_stage_index, spec) in specs.iter().enumerate() {
            let expected_stage_index =
                u32::try_from(expected_stage_index).expect("fixture stage index fits u32");
            let expected_node_id = config.node_id + 1 + u64::from(expected_stage_index);
            let expected_node_id_env = expected_node_id.to_string();
            let expected_stage_index_env = expected_stage_index.to_string();

            assert_eq!(spec.run_id, config.run_id);
            assert_eq!(spec.node_id, expected_node_id);
            assert_eq!(spec.stage_index, Some(expected_stage_index));
            assert_eq!(
                env_value(&spec.env, "MVP_RUN_ID"),
                Some(expected_run_id.as_str())
            );
            assert_eq!(
                env_value(&spec.env, "MVP_LOGICAL_NODE_ID"),
                Some(expected_node_id_env.as_str())
            );
            assert_eq!(
                env_value(&spec.env, "MVP_STAGE_INDEX"),
                Some(expected_stage_index_env.as_str())
            );
            assert_eq!(env_value(&spec.env, "MVP_PIPELINE_STAGES"), Some("3"));
            assert_eq!(env_value(&spec.env, "MVP_NODE_PROVIDER"), Some("docker"));
            assert_eq!(
                env_value(&spec.env, "MVP_MODEL_ID"),
                Some(config.model_id.as_str())
            );
            assert_eq!(
                env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"),
                Some("/models/cached/pipeline-node-spec.gguf")
            );
            assert_eq!(env_value(&spec.env, "MVP_MAX_CONTEXT"), Some("512"));
            assert_eq!(
                env_value(&spec.env, "MVP_COORDINATOR_ENDPOINT"),
                Some(coordinator_env.as_str())
            );
            assert_eq!(
                env_value(&spec.env, "MVP_ORCHESTRATOR_ACTOR"),
                Some(orchestrator_actor_env.as_str())
            );
            assert_eq!(
                spec.mounts,
                vec![expected_mount.clone()],
                "pipeline cached GGUF mount should be read-only for stage {expected_stage_index}"
            );
        }
    }

    #[test]
    fn vectorized_local_process_stage_node_specs_follow_three_stage_plan() {
        let model = TempModelFile::new("process-pipeline-node-spec.gguf");
        let config = with_clean_env_os(
            &[
                ("MVP_RUNTIME_CONFIG", OsString::from("local")),
                (CACHED_MODEL_HOST_ENV, model.raw_path.as_os_str().to_owned()),
            ],
            || {
                Config::from_layers_with_path_and_args(
                    None,
                    ["--pipeline-stages", "3", "--max-context", "512"]
                        .into_iter()
                        .map(str::to_owned),
                )
                .expect("process pipeline config parses")
            },
        );
        let plan = config
            .build_run_plan()
            .expect("process stage node spec plan builds");
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[22; 32]).public());
        let orchestrator_actor = ActorAddress([24; 32]);
        let host_path = model.canonical_path.to_string_lossy().to_string();

        let specs = stage_node_specs(&config, Some(&plan), coordinator, orchestrator_actor)
            .expect("process pipeline stage node specs build");

        assert_eq!(config.provider, ProviderKind::Process);
        assert_eq!(specs.len(), 3);
        for (expected_stage_index, spec) in specs.iter().enumerate() {
            let expected_stage_index =
                u32::try_from(expected_stage_index).expect("fixture stage index fits u32");
            assert_eq!(spec.stage_index, Some(expected_stage_index));
            assert_eq!(env_value(&spec.env, "MVP_NODE_PROVIDER"), Some("process"));
            assert_eq!(env_value(&spec.env, "MVP_PIPELINE_STAGES"), Some("3"));
            assert_eq!(
                env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"),
                Some(host_path.as_str())
            );
            assert_eq!(env_value(&spec.env, "MVP_MAX_CONTEXT"), Some("512"));
            assert!(spec.mounts.is_empty(), "process stage specs must not mount");
        }
    }

    #[test]
    fn planned_single_stage_cached_model_node_spec_follows_generated_plan() {
        let model = TempModelFile::new("single-stage-node-spec.gguf");
        let config = pipeline_config_with_cached_model(&model, 1);
        let plan = config
            .build_run_plan()
            .expect("single-stage cached plan builds");
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[31; 32]).public());
        let orchestrator_actor = ActorAddress([33; 32]);

        let specs = stage_node_specs(&config, Some(&plan), coordinator, orchestrator_actor)
            .expect("single planned stage node spec builds");

        assert_eq!(specs.len(), 1);
        let spec = &specs[0];
        let stage = &plan.stages[0];
        assert_eq!(stage.stage_index, 0);
        assert_eq!(stage.layer_start, 0);
        assert_eq!(stage.layer_end_exclusive, model.metadata.num_layers);
        assert_eq!(spec.node_id, stage.node_id.0);
        assert_ne!(
            spec.node_id, config.node_id,
            "planned cached N=1 still provisions a worker separate from the coordinator"
        );
        assert_eq!(spec.stage_index, Some(0));
        assert_eq!(env_value(&spec.env, "MVP_PIPELINE_STAGES"), Some("1"));
        assert_eq!(
            env_value(&spec.env, "MVP_GGUF_LOCAL_PATH"),
            Some("/models/cached/single-stage-node-spec.gguf")
        );
        assert_eq!(
            spec.mounts,
            vec![ProviderMount {
                host_path: model.canonical_path.to_string_lossy().to_string(),
                container_path: "/models/cached/single-stage-node-spec.gguf".to_owned(),
                readonly: true,
            }]
        );
    }

    #[test]
    fn vectorized_local_docker_stage_provision_detail_preserves_plan_edges_and_layers() {
        let model = TempModelFile::new("pipeline-stage-detail.gguf");
        let config = pipeline_config_with_cached_model(&model, 3);
        let plan = config.build_run_plan().expect("stage detail plan builds");

        let detail = stage_provision_detail(&config, Some(&plan));

        assert_eq!(
            detail,
            json!({
                "run_id": config.run_id,
                "stage_count": 3,
                "stages": plan.stages.iter().map(|stage| {
                    json!({
                        "node_id": stage.node_id.0,
                        "stage_index": stage.stage_index,
                        "layer_range": {
                            "start": stage.layer_start,
                            "end_exclusive": stage.layer_end_exclusive
                        },
                        "inbound_edge_id": stage.inbound_edge.0,
                        "outbound_edge_id": stage.outbound_edge.0,
                    })
                }).collect::<Vec<_>>(),
                "model_id": config.model_id,
            })
        );
    }

    #[test]
    fn stage_provision_wire_preserves_plan_edges_for_single_and_multi_stage_cached_models() {
        for stage_count in [1_u32, 3] {
            let model = TempModelFile::new(&format!("wire-{stage_count}.gguf"));
            let config = pipeline_config_with_cached_model(&model, stage_count);
            let plan = config.build_run_plan().expect("wire test plan builds");
            let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[41; 32]).public());
            let readies = plan
                .stages
                .iter()
                .map(|stage| {
                    let byte = u8::try_from(stage.stage_index + 42).expect("fixture byte fits");
                    let endpoint =
                        EndpointAddr::new(iroh::SecretKey::from_bytes(&[byte; 32]).public());
                    (
                        stage.node_id.0,
                        RuntimeReady {
                            endpoint: endpoint.clone(),
                            node_actor: ActorAddress([byte; 32]),
                            datastream_publisher: ActorAddress([byte.wrapping_add(80); 32]),
                            stage_index: stage.stage_index,
                            readiness_id: u64::from(stage.stage_index) + 100,
                            swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();

            for stage in &plan.stages {
                let wire = stage_provision_wire_from_plan(
                    &plan,
                    stage.stage_index,
                    &readies,
                    &coordinator,
                )
                .expect("stage provision wire builds from plan");
                let inbound = wire.inbound_edge.as_ref().expect("planned inbound edge");
                let outbound = wire.outbound_edge.as_ref().expect("planned outbound edge");

                assert_eq!(wire.stage_count, stage_count);
                assert_eq!(wire.node_id, stage.node_id.0);
                assert_eq!(wire.layer_start, stage.layer_start);
                assert_eq!(wire.layer_end_exclusive, stage.layer_end_exclusive);
                assert_eq!(inbound.edge_id, stage.inbound_edge.0);
                assert_eq!(outbound.edge_id, stage.outbound_edge.0);
                assert_eq!(
                    inbound.kind,
                    if stage.stage_index == 0 {
                        StageEdgeKindWire::TokenIn
                    } else {
                        StageEdgeKindWire::Activation
                    }
                );
                assert_eq!(
                    outbound.kind,
                    if stage.stage_index + 1 == stage_count {
                        StageEdgeKindWire::TokenOut
                    } else {
                        StageEdgeKindWire::Activation
                    }
                );
                assert_eq!(wire.model_id, config.model_id);
                assert_eq!(wire.gguf_source, config.gguf_source);
                assert_eq!(wire.tokenizer, config.tokenizer);
            }
        }
    }

    #[test]
    fn cached_model_with_deploy_provider_is_rejected_before_vastai_env_is_parsed() {
        let model = TempModelFile::new("deploy-rejected.gguf");
        let error = with_clean_env_os(
            &[
                ("MVP_RUNTIME_CONFIG", OsString::from("deploy")),
                (CACHED_MODEL_HOST_ENV, model.raw_path.as_os_str().to_owned()),
                (
                    "MVP_VASTAI_CONFIRM_LEASE",
                    OsString::from("definitely-not-a-bool"),
                ),
                ("MVP_VASTAI_DISK_GB", OsString::from("not-a-u32")),
            ],
            || match Config::from_layers_with_path_and_args(None, std::iter::empty::<String>()) {
                Ok(_) => panic!("deploy cached model must be rejected"),
                Err(error) => error,
            },
        );

        assert!(
            error.contains(
                "MVP_CACHED_MODEL_HOST_PATH is a host-local cache path and is only supported by provider=process or provider=docker"
            ),
            "unexpected error: {error}"
        );
        assert!(
            !error.contains("MVP_VASTAI_CONFIRM_LEASE") && !error.contains("MVP_VASTAI_DISK_GB"),
            "cached-model rejection should not require valid VastAI env, got: {error}"
        );
    }

    #[derive(Default)]
    struct FakeProvisionPlugin {
        stopped: Vec<u64>,
    }

    impl ProvisionPlugin for FakeProvisionPlugin {
        fn start_node(
            &mut self,
            _spec: NodeProvisionSpec,
            _sink: PluginSink,
        ) -> Result<crate::provisioning::PluginNodeHandle, String> {
            unreachable!("guard tests construct handles directly")
        }

        fn complete_bootstrap(
            &mut self,
            _handle: &crate::provisioning::PluginNodeHandle,
        ) -> Result<(), String> {
            Ok(())
        }

        fn stop_node(
            &mut self,
            handle: &crate::provisioning::PluginNodeHandle,
        ) -> Result<(), String> {
            self.stopped.push(handle.id);
            Ok(())
        }
    }

    #[test]
    fn provisioned_node_guard_stops_node_on_drop() {
        let mut plugin = FakeProvisionPlugin::default();
        {
            let _guard = ProvisionedNodeGuard::new(
                &mut plugin,
                crate::provisioning::PluginNodeHandle {
                    id: 7,
                    provider_process_id: Some(99),
                },
            );
        }
        assert_eq!(plugin.stopped, vec![7]);
    }

    #[test]
    fn provisioned_node_guard_explicit_stop_runs_once() {
        let mut plugin = FakeProvisionPlugin::default();
        {
            let mut guard = ProvisionedNodeGuard::new(
                &mut plugin,
                crate::provisioning::PluginNodeHandle {
                    id: 8,
                    provider_process_id: None,
                },
            );
            guard.stop().expect("first stop succeeds");
            guard.stop().expect("second stop is a no-op");
        }
        assert_eq!(plugin.stopped, vec![8]);
    }

    #[test]
    fn stop_requested_observes_shutdown_signal_only() {
        let (_tx, rx) = mpsc::channel();
        assert!(!stop_requested(&rx));

        let (tx, rx) = mpsc::channel();
        tx.send(()).expect("send shutdown");
        assert!(stop_requested(&rx));
        assert!(!stop_requested(&rx));
    }

    fn endpoint(seed: u8) -> EndpointAddr {
        EndpointAddr::new(iroh::SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn insert_route(stack: &DistributionRuntimeStack, actor: ActorAddress, owner: DistNodeId) {
        let mut route_view = match stack.route_view.write() {
            Ok(route_view) => route_view,
            Err(poisoned) => poisoned.into_inner(),
        };
        route_view.insert(actor, owner);
    }

    fn mark_alive(stack: &DistributionRuntimeStack, node_id: DistNodeId) {
        stack
            .runtime
            .send_to(
                stack.actors.membership_fanout,
                distribution::swim::actor::MembershipChanged {
                    node_id,
                    state: MemberState::Alive,
                    incarnation: 1,
                },
            )
            .expect("send membership change");
        stack.pump_runtime_once();
    }

    #[test]
    fn runtime_ready_barrier_waits_for_specific_swim_and_route() {
        let remote = DistNodeId([2; 32]);
        let node_actor = ActorAddress::new_random();
        let datastream_publisher = ActorAddress::new_random();
        let ready = RuntimeReady {
            endpoint: endpoint(2),
            node_actor,
            datastream_publisher,
            stage_index: 3,
            readiness_id: 99,
            swim_node_id: remote,
        };

        let stack =
            DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default());
        assert!(!runtime_ready_barrier_met(&stack, &ready));

        let stack =
            DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default());
        mark_alive(&stack, remote);
        assert!(!runtime_ready_barrier_met(&stack, &ready));

        let stack =
            DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default());
        insert_route(&stack, node_actor, remote);
        assert!(!runtime_ready_barrier_met(&stack, &ready));

        let stack =
            DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default());
        mark_alive(&stack, remote);
        insert_route(&stack, node_actor, remote);
        assert!(runtime_ready_barrier_met(&stack, &ready));
    }

    #[test]
    fn enqueue_runtime_ready_ack_reports_to_node_agent() {
        use crate::actors::node_agent::{NodeAgentActor, NodeAgentReport};
        use crate::actors::orchestrator::OrchestratorMsg;
        use crate::stage_controller as stage;

        let stack =
            DistributionRuntimeStack::new(DistNodeId([1; 32]), DistributedNodeConfig::default());
        let orchestrator_inbox = stack
            .runtime
            .new_inbox::<OrchestratorMsg>()
            .expect("orchestrator inbox");
        let reports = stack
            .runtime
            .new_inbox::<NodeAgentReport>()
            .expect("node report inbox");
        let node_actor = stack
            .runtime
            .spawn(NodeAgentActor::new(
                stage::NodeId(11),
                *orchestrator_inbox.addr(),
                Some(*reports.addr()),
            ))
            .expect("spawn node agent");
        let ready = RuntimeReady {
            endpoint: endpoint(9),
            node_actor,
            datastream_publisher: ActorAddress::new_random(),
            stage_index: 3,
            readiness_id: 99,
            swim_node_id: DistNodeId([2; 32]),
        };

        enqueue_runtime_ready_ack(&stack, &ready, 7, 11).expect("enqueue runtime ready ack");
        stack.pump_runtime_once();

        assert_eq!(
            reports.try_recv(),
            Some(NodeAgentReport::RuntimeReadyAck {
                run_id: 7,
                node_id: 11,
                stage_index: ready.stage_index,
                readiness_id: 99,
            })
        );

        assert_eq!(
            orchestrator_inbox.try_recv(),
            Some(OrchestratorMsg::ObserveNodeRuntimeReadyAck {
                run_id: 7,
                node_id: 11,
                stage_index: ready.stage_index,
                readiness_id: 99,
            })
        );
    }
}
