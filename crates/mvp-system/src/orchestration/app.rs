use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::DEFAULT_PIPELINE_CACHED_MODEL_FILE;
use crate::node_actor::{
    NodeAgentMsg, StageEdgeKindWire, StageInboundEdgeWire, StageObjectSpecWire,
    StageOutboundEdgeWire, StageProvisionWire, StageRingSpecWire,
};
#[cfg(feature = "dashboard")]
use crate::observability::dashboard_view::MvpClusterDashboardView;
use crate::observability::{benchmark, frame_archive::FrameArchive};
use crate::orchestration::actor::{OrchestratorActor, OrchestratorReport};
use crate::orchestration::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
use crate::transport::codec_registry::register_mvp_actor_codecs;
const PROVIDER_START_MAX_ATTEMPTS: usize = 4;

use crate::gguf_shard::{StageShardPlan, plan_stage_shard};
use crate::node_provisioning::{ProviderKind, provider_kind};
use crate::observability::telemetry::{
    MVP_PROVISIONING_EVENTS, MvpProvisionEventRecord, MvpProvisionLogRecord,
    mvp_provision_log_channel,
};
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::orchestration::provider_adapters::relay::{
    MVP_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV, relay_mode_env_value,
    relay_runtime_config_from_settings,
};
use crate::orchestration::provider_adapters::vastai::{
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin,
};
use crate::prompt::rpc::{
    PromptEvent, SubmitPrompt, TokenizerEvent, read_submit_prompt, write_json_line,
};
use crate::provisioning::{
    LocalDockerPlugin, LocalProcessPlugin, NodeProvisionSpec, PluginObservation,
    PluginObservationSink, PluginSink, ProviderMount, ProvisionEvent, ProvisionEventKind,
    ProvisionLogLine, ProvisionLogStream, ProvisionPlugin,
};
use crate::run_fsm::{RunConfig, RunId};
use crate::run_plan::{self, GgufSource, TokenizerSource};
use crate::transport::endpoint_advertisement::{
    EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint,
};
use data_plane::object_record as ingress;
use datastream::{
    ChannelContent, ChannelId, ChannelRef, DatastreamEndpoint, DatastreamEvent, DatastreamProducer,
    DatastreamPublisherMsg, DatastreamSubscribe, Frame, Lifetime, NodeId, Record, StreamDescriptor,
    StreamId, StreamOrigin, SubscriptionRequest,
};
use distribution::node::DistributedNodeConfig;
use distribution::swim::telemetry::ObservedProbeEvent;
use distribution::telemetry::{MembershipTransition, SwimProbeEvent};
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::EndpointAddr;
use iroh_driver::{
    DATASTREAM_ALPN, EDGE_ALPN, EdgeSendHandle, EdgeTransportEvent, IrohDriver, IrohDriverConfig,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

const DEFAULT_IMAGE: &str = "swactor-mvp-node:latest";
const MVP_RUNTIME_CONFIG_ENV: &str = "MVP_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MVP_CACHED_MODEL_HOST_PATH";
const MVP_WORKER_BIN_ENV: &str = "MVP_WORKER_BIN";
const CACHED_MODEL_CONTAINER_DIR: &str = "/models/cached";
const DEFAULT_PIPELINE_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_RPC_BIND: &str = "127.0.0.1:19777";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_MAX_TOKENS: u32 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const RUNTIME_READY_ACK_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const STAGE_PROVISION_ACTIVE_RESEND_AFTER: Duration = Duration::from_secs(60);
const PIPELINE_PROMPT_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(15);
const MVP_ORCH_BOOTSTRAP: &str = "mvp.orch.bootstrap";
const MVP_ORCH_PROMPT: &str = "mvp.orch.prompt";
const MVP_SWIM_MEMBERSHIP: &str = "mvp.swim.membership";
const MVP_STAGE_ROUTE: &str = "mvp.orch.stage_route";
const DATASTREAM_FRAME_LOG_ENV: &str = "MVP_DATASTREAM_FRAME_LOG";
const DEFAULT_DOCKER_CONTAINER_PREFIX: &str = "mvp-orchestrator";
const MVP_DOCKER_CONTAINER_PREFIX_ENV: &str = "MVP_DOCKER_CONTAINER_PREFIX";

struct OrchestratorRunOptions {
    capture_stdio: bool,
    stop_rx: Option<mpsc::Receiver<()>>,
}

impl Default for OrchestratorRunOptions {
    fn default() -> Self {
        Self {
            capture_stdio: true,
            stop_rx: None,
        }
    }
}

pub(super) fn run_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    run_with_options(args, OrchestratorRunOptions::default())
}

pub(super) fn run_in_process_from_args<I>(
    args: I,
    stop_rx: mpsc::Receiver<()>,
) -> Result<(), String>
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

fn run_with_options<I>(args: I, options: OrchestratorRunOptions) -> Result<(), String>
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
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "datastream_preflight",
        "configured",
        json!({
            "producer":"mvp-orchestrator",
            "datastream_endpoint":{
                "role":"orchestrator-frame-archive",
                "transport":"datastream-frame-log",
                "configured":config.datastream_frame_log.is_some(),
                "archive_path":config.datastream_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
            },
            "expected_worker_producers":["mvp-worker-node","tinygrad-worker"],
            "provider":config.provider.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "provider_config":config.provider_datastream_detail(),
        }),
    );
    let orch_synthetic_id = format!("mvp-orchestrator-{}-datastream-preflight", config.run_id);
    for (phase, status) in [
        ("DatastreamProducerConfigured", "configured"),
        ("DatastreamProducerConnected", "ready"),
        ("DatastreamSyntheticEventSent", "sent"),
        ("DatastreamSyntheticEventObserved", "observed"),
    ] {
        orch_datastream.emit_bootstrap(
            None,
            config.run_id,
            config.node_id,
            phase,
            status,
            json!({
                "producer":"mvp-orchestrator",
                "producer_class":"rust-orchestrator",
                "synthetic_id":orch_synthetic_id,
                "datastream_endpoint":{
                    "role":"orchestrator-frame-archive",
                    "transport":"datastream-frame-log",
                    "configured":config.datastream_frame_log.is_some(),
                    "archive_path":config.datastream_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
                },
            }),
        );
    }
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
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "endpoint_config_snapshot",
        "ready",
        json!({
            "producer":"mvp-orchestrator",
            "coordinator_endpoint":coordinator_endpoint.clone(),
            "has_relay":coordinator_endpoint.relay_urls().next().is_some(),
            "direct_addr_count":coordinator_endpoint.ip_addrs().count(),
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "connectivity_preflight":"ready",
        }),
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
        RuntimeReadyAckLoop {
            driver: &mut driver,
            stack: &stack,
            obs_rx: &obs_rx,
            frame_rx: &frame_rx,
            frame_tx: &frame_tx,
            orchestrator_reports: &orchestrator_reports,
            stop_rx: &stop_rx,
            dashboard: dashboard.as_ref(),
            orch_datastream: &mut orch_datastream,
            orch_stdio_rx: orch_stdio_rx.as_ref(),
            run_id: config.run_id,
            orchestrator_node_id: config.node_id,
            provider: &config.provider,
        },
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
        RuntimeReadyAckLoop {
            driver: &mut driver,
            stack: &stack,
            obs_rx: &obs_rx,
            frame_rx: &frame_rx,
            frame_tx: &frame_tx,
            orchestrator_reports: &orchestrator_reports,
            stop_rx: &stop_rx,
            dashboard: dashboard.as_ref(),
            orch_datastream: &mut orch_datastream,
            orch_stdio_rx: orch_stdio_rx.as_ref(),
            run_id: config.run_id,
            orchestrator_node_id: config.node_id,
            provider: &config.provider,
        },
        &work_rx,
        &prompt_events,
        ready.first_stage.node_actor,
        prompt_reply_actor,
        &tokenizer_events,
        ready.first_stage.node_actor,
        ready.final_stage.node_actor,
        tokenizer_reply_actor,
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
        for host_id in &builder.vastai_blacklist_hosts {
            if !provisioning.selection.blacklist_hosts.contains(host_id) {
                provisioning.selection.blacklist_hosts.push(*host_id);
            }
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
            Self::Local => provider_kind::process(),
            Self::Deploy => provider_kind::vastai(),
        }
    }
}

#[derive(Clone)]
struct CachedModelConfig {
    host_path: PathBuf,
    container_path: String,
}

impl CachedModelConfig {
    fn from_host_path(provider: &ProviderKind, requested: PathBuf) -> Result<Self, String> {
        if provider != &provider_kind::process()
            && provider != &provider_kind::docker()
            && provider != &provider_kind::vastai()
        {
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
        let container_path = cached_model_container_path(&host_path)?;
        Ok(Self {
            host_path,
            container_path,
        })
    }

    fn worker_path(&self, provider: &ProviderKind) -> String {
        if provider == &provider_kind::process() {
            self.host_path.to_string_lossy().to_string()
        } else {
            self.container_path.clone()
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
    vastai_blacklist_hosts: Vec<u64>,
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
            vastai_blacklist_hosts: Vec::new(),
            vastai_poll_interval_secs_raw: None,
            cached_model_host_path: None,
            datastream_frame_log: None,
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
        apply!(overlay.prompt.rpc_addr, |rpc_bind| {
            self.rpc_bind = rpc_bind;
            self.rpc_bind_label = "[prompt].rpc_addr";
        });
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
        apply!(overlay.model.gguf_repo, |repo| { self.set_gguf_repo(repo) });
        apply!(overlay.model.gguf_file, |file| { self.set_gguf_file(file) });
        apply!(overlay.model.gguf_revision, |revision| {
            self.set_gguf_revision(Some(revision))
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
        apply!(overlay.observability.datastream_frame_log, |path| {
            self.datastream_frame_log = Some(PathBuf::from(path))
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

        env_apply!(MVP_RUNTIME_CONFIG_ENV, |profile| {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        });
        env_parse!("MVP_RUN_ID", |run_id| { self.run_id = run_id });
        env_parse!("MVP_LOGICAL_NODE_ID", |node_id| { self.node_id = node_id });
        env_parse!("MVP_STAGE_INDEX", |stage_index| {
            self.stage_index = stage_index
        });
        env_parse!("MVP_LAYER_END_EXCLUSIVE", |layer_end_exclusive| {
            self.layer_end_exclusive = Some(layer_end_exclusive)
        });
        env_parse!("MVP_PIPELINE_STAGES", |pipeline_stages| {
            self.pipeline_stages = pipeline_stages
        });
        apply!(
            env_optional("MVP_NODE_PROVIDER").or_else(|| env_optional("MVP_PROVIDER")),
            |provider| {
                self.provider = Some(provider_kind::parse_deploy(&provider)?);
            }
        );
        env_apply!("MVP_NODE_IMAGE", |image| { self.set_process_image(image) });
        env_apply!("MVP_DOCKER_GPUS", |gpus| { self.docker_gpus = gpus });
        env_apply!(CACHED_MODEL_HOST_ENV, |path| {
            self.cached_model_host_path = Some(PathBuf::from(path))
        });
        env_apply!(MVP_WORKER_BIN_ENV, |path| {
            self.worker_bin = Some(PathBuf::from(path))
        });
        env_apply!("MVP_PROMPT_RPC_BIND", |rpc_bind| {
            self.rpc_bind = rpc_bind;
            self.rpc_bind_label = "MVP_PROMPT_RPC_BIND";
        });
        env_parse!("MVP_PROMPT_MAX_TOKENS", |max_tokens| {
            self.default_max_tokens = max_tokens
        });
        env_apply!("MVP_DASHBOARD", |dashboard| {
            self.dashboard = Self::parse_bool("MVP_DASHBOARD", &dashboard)?;
        });
        env_apply!(DATASTREAM_FRAME_LOG_ENV, |path| {
            self.datastream_frame_log = Some(PathBuf::from(path))
        });
        env_apply!("MVP_MODEL_ID", |model_id| { self.model_id = model_id });
        env_apply!("MVP_GGUF_LOCAL_PATH", |path| {
            self.gguf_source = GgufSource::LocalPath(path)
        });
        env_apply!("MVP_GGUF_REPO", |repo| { self.set_gguf_repo(repo) });
        env_apply!("MVP_GGUF_FILE", |file| { self.set_gguf_file(file) });
        env_apply!("MVP_GGUF_REVISION", |revision| {
            self.set_gguf_revision(Some(revision))
        });
        env_apply!("MVP_TOKENIZER_LOCAL_PATH", |path| {
            self.tokenizer = TokenizerSource::LocalPath(path)
        });
        env_parse!("MVP_MAX_CONTEXT", |max_context| {
            self.max_context = Some(max_context)
        });
        env_apply!("MVP_IROH_RELAY_MODE", |mode| {
            self.relay_mode = Some(mode.to_ascii_lowercase())
        });
        apply!(
            env_optional(MVP_IROH_RELAY_URL_ENV)
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
                .or_else(|| env_optional("MVP_VASTAI_API_KEY"))
                .or_else(|| env_optional("VASTAI_API_KEY")),
            |api_key| {
                self.vastai_api_key = Some(api_key);
            }
        );
        env_apply!("MVP_VASTAI_BOOTSTRAP_COMMAND", |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        env_apply!("MVP_VASTAI_SSH_IDENTITY", |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });
        env_apply!("MVP_VASTAI_DISK_GB", |disk_gb| {
            self.vastai_disk_gb_raw = Some(disk_gb)
        });
        env_apply!("MVP_VASTAI_SSH_USER", |ssh_user| {
            self.vastai_ssh_user = Some(ssh_user)
        });
        env_apply!("MVP_VASTAI_CONFIRM_LEASE", |confirm_lease| {
            self.vastai_confirm_lease_raw = Some(confirm_lease)
        });
        env_apply!("MVP_VASTAI_ONSTART", |onstart| {
            self.vastai_onstart = Some(onstart)
        });
        env_apply!("MVP_VASTAI_GPU_NAME", |gpu_name| {
            self.vastai_gpu_name = Some(gpu_name)
        });
        env_apply!("MVP_VASTAI_MIN_GPU_RAM_MB", |min_gpu_ram_mb| {
            self.vastai_min_gpu_ram_mb_raw = Some(min_gpu_ram_mb)
        });
        env_apply!("MVP_VASTAI_MIN_DOWN_MBPS", |min_down_mbps| {
            self.vastai_min_down_mbps_raw = Some(min_down_mbps)
        });
        env_apply!("MVP_VASTAI_MAX_DPH_TOTAL", |max_dph_total| {
            self.vastai_max_dph_total_raw = Some(max_dph_total)
        });
        env_apply!("MVP_VASTAI_MIN_UP_MBPS", |min_up_mbps| {
            self.vastai_min_up_mbps_raw = Some(min_up_mbps)
        });
        env_apply!("MVP_VASTAI_MIN_RELIABILITY", |min_reliability| {
            self.vastai_min_reliability_raw = Some(min_reliability)
        });
        env_apply!("MVP_VASTAI_REQUIRE_VERIFIED", |require_verified| {
            self.vastai_require_verified_raw = Some(require_verified)
        });
        env_apply!("MVP_VASTAI_BLACKLIST_HOSTS", |blacklist_hosts| {
            for host_id in Self::parse_list("MVP_VASTAI_BLACKLIST_HOSTS", &blacklist_hosts)? {
                self.push_vastai_blacklist_host(host_id);
            }
        });
        env_apply!("MVP_VASTAI_POLL_INTERVAL_SECS", |poll_interval_secs| {
            self.vastai_poll_interval_secs_raw = Some(poll_interval_secs)
        });
        Ok(self)
    }

    fn apply_core_cli_arg<I>(&mut self, arg: &str, args: &mut I) -> Result<bool, String>
    where
        I: Iterator<Item = String>,
    {
        match arg {
            "--runtime-config" => {
                self.config_profile =
                    RuntimeConfigProfile::parse(&next_arg(args, "--runtime-config")?)?
            }
            "--provider" => {
                self.provider = Some(provider_kind::parse_deploy(&next_arg(args, "--provider")?)?)
            }
            "--worker-bin" => {
                self.worker_bin = Some(PathBuf::from(next_arg(args, "--worker-bin")?))
            }
            "--image" => self.set_process_image(next_arg(args, "--image")?),
            "--gpus" => self.docker_gpus = next_arg(args, "--gpus")?,
            "--rpc-bind" => {
                self.rpc_bind = next_arg(args, "--rpc-bind")?;
                self.rpc_bind_label = "--rpc-bind";
            }
            "--run-id" => self.run_id = parse_next(args, "--run-id")?,
            "--node-id" => self.node_id = parse_next(args, "--node-id")?,
            "--stage-index" => self.stage_index = parse_next(args, "--stage-index")?,
            "--layer-end-exclusive" => {
                self.layer_end_exclusive = Some(parse_next(args, "--layer-end-exclusive")?)
            }
            "-N" | "--pipeline-stages" => self.pipeline_stages = parse_next(args, arg)?,
            "--max-tokens" => self.default_max_tokens = parse_next(args, "--max-tokens")?,
            "--dashboard" => self.dashboard = true,
            "--no-dashboard" => self.dashboard = false,
            "--datastream-frame-log" => {
                self.datastream_frame_log =
                    Some(PathBuf::from(next_arg(args, "--datastream-frame-log")?));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_model_cli_arg<I>(&mut self, arg: &str, args: &mut I) -> Result<bool, String>
    where
        I: Iterator<Item = String>,
    {
        match arg {
            "--model-id" => self.model_id = next_arg(args, "--model-id")?,
            "--gguf-local-path" => {
                self.gguf_source = GgufSource::LocalPath(next_arg(args, "--gguf-local-path")?)
            }
            "--gguf-repo" => self.set_gguf_repo(next_arg(args, "--gguf-repo")?),
            "--gguf-file" => self.set_gguf_file(next_arg(args, "--gguf-file")?),
            "--gguf-revision" => self.set_gguf_revision(Some(next_arg(args, "--gguf-revision")?)),
            "--tokenizer-local-path" => {
                self.tokenizer =
                    TokenizerSource::LocalPath(next_arg(args, "--tokenizer-local-path")?)
            }
            "--max-context" => self.max_context = Some(parse_next(args, "--max-context")?),
            "--cached-model-host-path" => {
                self.cached_model_host_path =
                    Some(PathBuf::from(next_arg(args, "--cached-model-host-path")?));
            }
            "--relay-mode" => self.relay_mode = Some(next_arg(args, "--relay-mode")?),
            "--relay-url" => self.relay_url = Some(next_arg(args, "--relay-url")?),
            "--endpoint-addr-mask" => {
                self.endpoint_addr_mask = Some(next_arg(args, "--endpoint-addr-mask")?)
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_vastai_string_cli_arg<I>(&mut self, arg: &str, args: &mut I) -> Result<bool, String>
    where
        I: Iterator<Item = String>,
    {
        match arg {
            "--vastai-api-key" => self.vastai_api_key = Some(next_arg(args, "--vastai-api-key")?),
            "--vastai-bootstrap-command" => {
                self.vastai_bootstrap_command = Some(next_arg(args, "--vastai-bootstrap-command")?)
            }
            "--vastai-ssh-identity" => {
                self.vastai_ssh_identity_raw = Some(next_arg(args, "--vastai-ssh-identity")?);
            }
            "--vastai-ssh-user" => {
                self.vastai_ssh_user = Some(next_arg(args, "--vastai-ssh-user")?)
            }
            "--vastai-onstart" => self.vastai_onstart = Some(next_arg(args, "--vastai-onstart")?),
            "--vastai-gpu-name" => {
                self.vastai_gpu_name = Some(next_arg(args, "--vastai-gpu-name")?)
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_vastai_numeric_cli_arg<I>(&mut self, arg: &str, args: &mut I) -> Result<bool, String>
    where
        I: Iterator<Item = String>,
    {
        match arg {
            "--vastai-disk-gb" => {
                self.vastai_disk_gb = Some(parse_next(args, "--vastai-disk-gb")?);
                self.vastai_disk_gb_raw = None;
            }
            "--vastai-min-gpu-ram-mb" => {
                self.vastai_min_gpu_ram_mb = Some(parse_next(args, "--vastai-min-gpu-ram-mb")?);
                self.vastai_min_gpu_ram_mb_raw = None;
            }
            "--vastai-min-down-mbps" => {
                self.vastai_min_down_mbps = Some(parse_next(args, "--vastai-min-down-mbps")?);
                self.vastai_min_down_mbps_raw = None;
            }
            "--vastai-max-dph-total" => {
                self.vastai_max_dph_total = Some(parse_next(args, "--vastai-max-dph-total")?);
                self.vastai_max_dph_total_raw = None;
            }
            "--vastai-min-up-mbps" => {
                self.vastai_min_up_mbps = Some(parse_next(args, "--vastai-min-up-mbps")?);
                self.vastai_min_up_mbps_raw = None;
            }
            "--vastai-min-reliability" => {
                self.vastai_min_reliability = Some(parse_next(args, "--vastai-min-reliability")?);
                self.vastai_min_reliability_raw = None;
            }
            "--vastai-blacklist-host" => {
                let host_id = parse_next(args, "--vastai-blacklist-host")?;
                self.push_vastai_blacklist_host(host_id);
            }
            "--vastai-poll-interval-secs" => {
                self.vastai_poll_interval_secs =
                    Some(parse_next(args, "--vastai-poll-interval-secs")?);
                self.vastai_poll_interval_secs_raw = None;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn apply_vastai_bool_cli_arg(&mut self, arg: &str) -> bool {
        match arg {
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
            _ => return false,
        }
        true
    }

    fn overlay_cli(mut self, args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if self.apply_core_cli_arg(&arg, &mut args)?
                || self.apply_model_cli_arg(&arg, &mut args)?
                || self.apply_vastai_string_cli_arg(&arg, &mut args)?
                || self.apply_vastai_numeric_cli_arg(&arg, &mut args)?
                || self.apply_vastai_bool_cli_arg(&arg)
            {
                continue;
            }
            return Err(format!("unknown argument {arg:?}"));
        }
        Ok(self)
    }

    fn finalize(self) -> Result<Config, String> {
        let provider = self
            .provider
            .clone()
            .unwrap_or_else(|| self.config_profile.default_provider());
        let mut image = self.image.clone();
        if provider == provider_kind::vastai() && !self.image_overridden_after_toml {
            if let Some(vastai_image) = &self.toml_vastai_image {
                image = vastai_image.clone();
            }
        }
        if self.pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        let mut cached_model_host_path = self.cached_model_host_path.clone();
        if (provider == provider_kind::process() || provider == provider_kind::docker())
            && self.pipeline_stages > 1
            && cached_model_host_path.is_none()
            && (gguf_source_is_default_hf(&self.gguf_source)
                || gguf_source_matches_default_pipeline_cache(&self.gguf_source))
        {
            cached_model_host_path = Some(default_pipeline_cached_model_path());
        }
        let cached_model = cached_model_host_path
            .map(|path| CachedModelConfig::from_host_path(&provider, path))
            .transpose()?;
        let mut gguf_source = self.gguf_source.clone();
        if let Some(cached_model) = &cached_model {
            if provider != provider_kind::vastai() {
                gguf_source = GgufSource::LocalPath(cached_model.worker_path(&provider));
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
        let vastai = if provider == provider_kind::vastai() {
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
        if self.provider == provider_kind::process() {
            json!({
                "worker_bin": self.worker_bin.as_ref().map(|path| path.to_string_lossy().to_string()),
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::datastream_detail),
            })
        } else if self.provider == provider_kind::docker() {
            json!({
                "docker_gpus": &self.docker_gpus,
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::datastream_detail),
            })
        } else if self.provider == provider_kind::vastai() {
            self.vastai
                .as_ref()
                .map_or_else(|| json!({}), VastAiRuntimeConfig::datastream_detail)
        } else {
            json!({})
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
                if self.provider == provider_kind::vastai()
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
        if self.provider != provider_kind::vastai() {
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
        vastai.provisioning.ssh_public_key = Some(public_key);
        vastai.ssh_public_fingerprint = Some(fingerprint);
        Ok(())
    }

    fn build_provisioner(
        &self,
        bootstrap_runtime: Arc<swactor::runtime::Runtime>,
    ) -> Result<Box<dyn ProvisionPlugin>, String> {
        if self.provider == provider_kind::process() {
            let worker_bin = self.worker_bin.clone().unwrap_or(default_worker_bin()?);
            if !worker_bin.is_file() {
                return Err(format!(
                    "local process worker binary does not exist: {}",
                    worker_bin.display()
                ));
            }
            Ok(Box::new(LocalProcessPlugin::new(worker_bin)))
        } else if self.provider == provider_kind::docker() {
            Ok(Box::new(LocalDockerPlugin::new(docker_container_prefix())))
        } else if self.provider == provider_kind::vastai() {
            let vastai = self
                .vastai
                .as_ref()
                .ok_or_else(|| "VastAI config was not resolved for provider vastai".to_owned())?;
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
            Ok(Box::new(VastAiProvisioningPlugin::new(
                ToolsVastAiLeaseClient::from_api_key(api_key)?,
                SshCommandBootstrapLauncher::new(Some(ssh_identity), bootstrap_runtime),
                vastai.provisioning.clone(),
            )))
        } else {
            Err("mock provider cannot build a runtime provisioner".to_owned())
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
        if self.provider == provider_kind::docker() {
            keys.push("MVP_DOCKER_GPUS");
        }
        if std::env::var_os("DEV").is_some() {
            keys.push("DEV");
        }
        if local_tinygrad_worker_env(&self.provider).is_some() {
            keys.push("MVP_TINYGRAD_WORKER");
        }
        for key in [
            "MVP_CPU_LINE_PROFILE",
            "MVP_CPU_LINE_PROFILE_INTERVAL_MS",
            "MVP_TOKEN_PROGRESS_EVERY",
            "CUDA_DEVICE_SCHEDULE",
            "MVP_MODEL_CACHE_DIR",
            "HF_TOKEN",
        ] {
            if std::env::var_os(key).is_some() {
                keys.push(key);
            }
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
        if self.provider == provider_kind::docker() {
            env.push(("MVP_DOCKER_GPUS".to_owned(), self.docker_gpus.clone()));
        }
        env.extend(optional_env("DEV"));
        env.extend(local_tinygrad_worker_env(&self.provider));
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
        let args = if self.provider == provider_kind::vastai() {
            self.vastai
                .as_ref()
                .and_then(|vastai| vastai.bootstrap_command.clone())
                .into_iter()
                .collect()
        } else if self.provider == provider_kind::process()
            || self.provider == provider_kind::docker()
        {
            Vec::new()
        } else {
            return Err("mvp-orchestrator does not support mock provider".to_owned());
        };
        let mounts = if self.provider == provider_kind::docker() {
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

struct RuntimeReadyAckLoop<'a> {
    driver: &'a mut IrohDriver,
    stack: &'a DistributionRuntimeStack,
    obs_rx: &'a mpsc::Receiver<PluginObservation>,
    frame_rx: &'a mpsc::Receiver<CollectedDatastreamFrame>,
    frame_tx: &'a mpsc::Sender<CollectedDatastreamFrame>,
    orchestrator_reports: &'a swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &'a mpsc::Receiver<()>,
    dashboard: Option<&'a DashboardSupport>,
    orch_datastream: &'a mut OrchDatastream,
    orch_stdio_rx: Option<&'a mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: &'a ProviderKind,
}

fn wait_for_runtime_ready_acks(
    ctx: RuntimeReadyAckLoop<'_>,
    targets: &[RuntimeReadyAckTarget],
    collector_endpoint: &EndpointAddr,
) -> Result<(), String> {
    let RuntimeReadyAckLoop {
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
        run_id,
        orchestrator_node_id,
        provider,
    } = ctx;
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
        drain_observations_with_exit(
            obs_rx,
            dashboard,
            orch_datastream,
            provider,
            |node_id, status| format!("node {node_id} exited before ready: {status:?}"),
        )?;
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

    fn complete_bootstrap(&mut self) -> Result<(), String> {
        let mut first_error = None;
        for handle in &self.handles {
            if let Err(error) = self.provisioner.complete_bootstrap(handle)
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

type ProviderStartResults = Vec<(
    NodeProvisionSpec,
    Result<crate::provisioning::PluginNodeHandle, String>,
)>;

fn start_nodes_with_stdio_capture(
    provisioner: Box<dyn ProvisionPlugin>,
    node_specs: Vec<NodeProvisionSpec>,
    sink: PluginSink,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    run_id: u64,
    node_id: u64,
) -> (Box<dyn ProvisionPlugin>, ProviderStartResults) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut provisioner = provisioner;
        let results = provisioner.start_nodes(node_specs, sink);
        let _ = tx.send((provisioner, results));
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
                    vec![(
                        NodeProvisionSpec {
                            run_id,
                            node_id,
                            stage_index: None,
                            image: String::new(),
                            env: Vec::new(),
                            args: Vec::new(),
                            mounts: Vec::new(),
                        },
                        Err("provider start worker disconnected".to_owned()),
                    )],
                );
            }
        }
    }
}

fn start_and_provision_workers(
    mut provisioner: Box<dyn ProvisionPlugin>,
    config: &Config,
    pipeline_plan: Option<&run_plan::RunPlan>,
    ctx: RuntimeReadyAckLoop<'_>,
    sink: PluginSink,
    coordinator: EndpointAddr,
    pipeline_coordinator: EndpointAddr,
    orchestrator_actor: ActorAddress,
) -> Result<(ProvisionedClusterGuard, PromptRuntimeReady), String> {
    let RuntimeReadyAckLoop {
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
        ..
    } = ctx;
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
            "docker_gpus":if config.provider == provider_kind::docker() { Some(config.docker_gpus.as_str()) } else { None },
            "provider_config":config.provider_datastream_detail(),
            "env_keys":config.node_spec_env_keys(),
            "worker_count":stage_specs.len(),
            "worker_node_ids":expected_node_ids,
        }),
    );

    let stage_shard_plans = if let Some(plan) = pipeline_plan {
        let plans = pipeline_stage_shard_plans(config, plan)?;
        emit_stage_shard_plan_summaries(
            dashboard,
            orch_datastream,
            config.run_id,
            config.node_id,
            &plans,
        );
        plans
    } else {
        BTreeMap::new()
    };
    let mut handles = Vec::with_capacity(stage_specs.len());
    let mut pending_specs = stage_specs;
    for attempt in 1..=PROVIDER_START_MAX_ATTEMPTS {
        for node_spec in &pending_specs {
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
                    "attempt":attempt,
                }),
            );
        }
        let (returned_provisioner, start_results) = start_nodes_with_stdio_capture(
            provisioner,
            pending_specs,
            sink.clone(),
            orch_stdio_rx,
            dashboard,
            orch_datastream,
            config.run_id,
            config.node_id,
        );
        provisioner = returned_provisioner;
        let start_outcome = collect_provider_start_outcome(start_results);
        handles.extend(start_outcome.successful_handles);
        for (node_spec, handle_result) in start_outcome.results {
            match handle_result {
                Ok(_) => {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        config.run_id,
                        config.node_id,
                        "provider_start",
                        "ready",
                        json!({
                            "provider":config.provider.as_str(),
                            "node_id":node_spec.node_id,
                            "stage_index":node_spec.stage_index,
                            "attempt":attempt,
                        }),
                    );
                }
                Err(error) => {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        config.run_id,
                        config.node_id,
                        "provider_start",
                        "failed",
                        json!({
                            "provider":config.provider.as_str(),
                            "node_id":node_spec.node_id,
                            "stage_index":node_spec.stage_index,
                            "attempt":attempt,
                            "error":error,
                        }),
                    );
                }
            }
        }
        if start_outcome.first_error.is_none() {
            break;
        }
        if attempt == PROVIDER_START_MAX_ATTEMPTS {
            let error = start_outcome
                .first_error
                .expect("checked provider-start failure");
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
        pending_specs = start_outcome.failed_specs;
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
            RuntimeReadyAckLoop {
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
                run_id: config.run_id,
                orchestrator_node_id: config.node_id,
                provider: &config.provider,
            },
            &expected_node_ids,
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
        let ready = match wait_for_runtime_ready(RuntimeReadyAckLoop {
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
            run_id: config.run_id,
            orchestrator_node_id: config.node_id,
            provider: &config.provider,
        }) {
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
    let ack_targets = readies
        .iter()
        .map(|(node_id, ready)| RuntimeReadyAckTarget {
            node_id: *node_id,
            ready: ready.clone(),
        })
        .collect::<Vec<_>>();
    wait_for_runtime_ready_acks(
        RuntimeReadyAckLoop {
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
            run_id: config.run_id,
            orchestrator_node_id: config.node_id,
            provider: &config.provider,
        },
        &ack_targets,
        &pipeline_coordinator,
    )?;
    provisioned_nodes
        .complete_bootstrap()
        .map_err(|e| format!("complete provider bootstrap after runtime-ready: {e}"))?;

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
            RuntimeReadyAckLoop {
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
                run_id: config.run_id,
                orchestrator_node_id: config.node_id,
                provider: &config.provider,
            },
            expected_node_ids.len(),
            pipeline_plan.expect("pipeline mode requires plan"),
            &readies,
            &pipeline_coordinator,
            &stage_shard_plans,
        )
    } else {
        wait_for_weights_loaded(
            RuntimeReadyAckLoop {
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
                run_id: config.run_id,
                orchestrator_node_id: config.node_id,
                provider: &config.provider,
            },
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
        Ok(vec![config.node_spec_for_stage(
            coordinator,
            orchestrator_actor,
            config.node_id,
            config.stage_index,
        )?])
    }
}
struct ProviderStartOutcome {
    results: ProviderStartResults,
    successful_handles: Vec<crate::provisioning::PluginNodeHandle>,
    failed_specs: Vec<NodeProvisionSpec>,
    first_error: Option<String>,
}

fn collect_provider_start_outcome(results: ProviderStartResults) -> ProviderStartOutcome {
    let mut successful_handles = Vec::new();
    let mut failed_specs = Vec::new();
    let mut first_error = None;
    for (spec, result) in &results {
        match result {
            Ok(handle) => successful_handles.push(handle.clone()),
            Err(error) => {
                failed_specs.push(spec.clone());
                if first_error.is_none() {
                    first_error = Some(error.clone());
                }
            }
        }
    }
    ProviderStartOutcome {
        results,
        successful_handles,
        failed_specs,
        first_error,
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
    stage_shard_plans: &BTreeMap<u32, StageShardPlan>,
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
        stage_shard_plan: stage_shard_plans.get(&stage_index).cloned(),
    })
}

fn provision_stage_from_plan(
    stack: &DistributionRuntimeStack,
    node_actor: ActorAddress,
    plan: &run_plan::RunPlan,
    stage_index: u32,
    readies: &BTreeMap<u64, RuntimeReady>,
    coordinator: &EndpointAddr,
    stage_shard_plans: &BTreeMap<u32, StageShardPlan>,
) -> Result<(), String> {
    let provision =
        stage_provision_wire_from_plan(plan, stage_index, readies, coordinator, stage_shard_plans)?;
    stack
        .runtime
        .send_to(node_actor, NodeAgentMsg::ProvisionStage(provision))
        .map_err(|e| format!("send stage {stage_index} provision: {e}"))
}

fn pipeline_stage_shard_plans(
    config: &Config,
    plan: &run_plan::RunPlan,
) -> Result<BTreeMap<u32, StageShardPlan>, String> {
    if !matches!(config.gguf_source, GgufSource::HuggingFaceGguf { .. }) {
        return Ok(BTreeMap::new());
    }
    let planning_gguf = config.local_planning_gguf_path()?;
    let mut out = BTreeMap::new();
    for stage in &plan.stages {
        let shard_plan = plan_stage_shard(
            &planning_gguf,
            stage.gguf_source.clone(),
            stage.stage_index,
            stage.stage_count,
            stage.layer_start,
            stage.layer_end_exclusive,
        )
        .map_err(|error| {
            format!(
                "plan stage {} HF shard ranges from {}: {error}",
                stage.stage_index,
                planning_gguf.display()
            )
        })?;
        out.insert(stage.stage_index, shard_plan);
    }
    Ok(out)
}

fn stage_shard_plan_summary_detail(plan: &StageShardPlan) -> Value {
    let planned_fetch_bytes = plan.planned_fetch_bytes();
    json!({
        "stage_index":plan.stage_index,
        "stage_count":plan.stage_count,
        "layer_start":plan.layer_start,
        "layer_end_exclusive":plan.layer_end_exclusive,
        "planned_fetch_bytes":planned_fetch_bytes,
        "source_total_bytes":plan.source_total_bytes,
        "tensor_count":plan.tensors.len(),
        "range_count":plan.planned_range_count(),
        "tensor_range_count":plan.merged_tensor_ranges.len(),
        "metadata_bytes":plan.metadata_end,
        "planned_fraction":if plan.source_total_bytes == 0 {
            Value::Null
        } else {
            json!(planned_fetch_bytes as f64 / plan.source_total_bytes as f64)
        },
    })
}

fn emit_stage_shard_plan_summaries(
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    run_id: u64,
    node_id: u64,
    stage_shard_plans: &BTreeMap<u32, StageShardPlan>,
) {
    for plan in stage_shard_plans.values() {
        orch_datastream.emit_bootstrap(
            dashboard,
            run_id,
            node_id,
            "stage_shard_plan",
            "ready",
            stage_shard_plan_summary_detail(plan),
        );
    }
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

fn wait_for_runtime_readies(
    ctx: RuntimeReadyAckLoop<'_>,
    expected_node_ids: &[u64],
) -> Result<BTreeMap<u64, RuntimeReady>, String> {
    let RuntimeReadyAckLoop {
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
        run_id,
        provider,
        ..
    } = ctx;
    let expected = expected_node_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut pending = BTreeMap::<u64, RuntimeReady>::new();
    loop {
        pump(driver, stack, frame_tx);
        emit_swim_transitions(
            orch_datastream,
            dashboard,
            run_id,
            expected_node_ids.first().copied().unwrap_or(0),
            stack,
        );
        emit_swim_probe_events(orch_datastream, dashboard, stack, "runtime_ready_wait");
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
        thread::sleep(PUMP_INTERVAL);
    }
}

fn wait_for_weights_loaded_count(
    ctx: RuntimeReadyAckLoop<'_>,
    expected_count: usize,
    pipeline_plan: &run_plan::RunPlan,
    readies: &BTreeMap<u64, RuntimeReady>,
    pipeline_coordinator: &EndpointAddr,
    stage_shard_plans: &BTreeMap<u32, StageShardPlan>,
) -> Result<(), String> {
    let RuntimeReadyAckLoop {
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
        run_id,
        orchestrator_node_id: node_id,
        provider,
    } = ctx;
    let expected_stages = pipeline_plan
        .stages
        .iter()
        .map(|stage| stage.stage_index)
        .collect::<BTreeSet<_>>();
    let mut loaded_stages = BTreeSet::<u32>::new();
    let mut last_resend = Instant::now() - Duration::from_secs(15);
    let mut resend_attempt = 0_u64;
    let mut stage_resend_counts = BTreeMap::<u32, u64>::new();
    let mut stage_last_sends = BTreeMap::<u32, Instant>::new();
    let mut load_progress = BTreeMap::<u64, StageLoadProgress>::new();
    loop {
        pump(driver, stack, frame_tx);
        emit_swim_transitions(orch_datastream, dashboard, run_id, node_id, stack);
        emit_swim_probe_events(orch_datastream, dashboard, stack, "weights_loaded_wait");
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for pipeline weights loaded".to_owned());
        }
        if loaded_stages.len() >= expected_count {
            return Ok(());
        }
        if last_resend.elapsed() >= Duration::from_secs(15) {
            resend_attempt += 1;
            let pending = pending_pipeline_weight_load_stages(pipeline_plan, &loaded_stages);
            if pending.is_empty() {
                return Err(format!(
                    "missing unloaded pipeline weight stage; loaded {} of {expected_count}",
                    loaded_stages.len()
                ));
            }
            for stage in pending {
                send_pipeline_stage_provision(
                    driver,
                    stack,
                    frame_tx,
                    dashboard,
                    orch_datastream,
                    run_id,
                    node_id,
                    pipeline_plan,
                    stage,
                    readies,
                    pipeline_coordinator,
                    &stage_shard_plans,
                    &loaded_stages,
                    &mut stage_resend_counts,
                    &mut stage_last_sends,
                    &load_progress,
                    resend_attempt,
                )?;
            }
            last_resend = Instant::now();
        }
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
            match observation {
                PluginObservation::Failed {
                    reason,
                    node_id: failed_node_id,
                    ..
                } => {
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        node_id,
                        "stage_provision_wait",
                        "failed",
                        json!({
                            "classification":"worker_process_failed",
                            "stage_node_id":failed_node_id,
                            "last_load_progress":load_progress.get(&failed_node_id).map(StageLoadProgress::to_json),
                            "reason":reason,
                        }),
                    );
                    return Err(reason);
                }
                PluginObservation::Exited {
                    node_id: exited_node_id,
                    status,
                    ..
                } => {
                    let reason =
                        format!("node {exited_node_id} exited while loading weights: {status:?}");
                    orch_datastream.emit_bootstrap(
                        dashboard,
                        run_id,
                        node_id,
                        "stage_provision_wait",
                        "failed",
                        json!({
                            "classification":"worker_process_exited",
                            "stage_node_id":exited_node_id,
                            "last_load_progress":load_progress.get(&exited_node_id).map(StageLoadProgress::to_json),
                            "status":status,
                            "reason":reason,
                        }),
                    );
                    return Err(reason);
                }
                PluginObservation::DatastreamFrame { .. } => {}
                PluginObservation::ProviderLine { .. }
                | PluginObservation::StdoutLine { .. }
                | PluginObservation::StderrLine { .. } => {}
            }
        }
        drain_frames_with_load_progress(frame_rx, dashboard, orch_datastream, &mut load_progress);
        while let Some(report) = orchestrator_reports.try_recv() {
            match report {
                OrchestratorReport::WeightsReady {
                    run_id: report_run_id,
                    node_id: _,
                    stage_index,
                } if report_run_id == run_id && expected_stages.contains(&stage_index) => {
                    loaded_stages.insert(stage_index);
                    if loaded_stages.len() >= expected_count {
                        return Ok(());
                    }
                }
                OrchestratorReport::StageFault {
                    run_id: report_run_id,
                    stage_index,
                    reason,
                } if report_run_id == run_id && expected_stages.contains(&stage_index) => {
                    let mut error =
                        format!("stage {stage_index} faulted while loading pipeline weights");
                    if let Some(reason) = reason {
                        error.push_str(": ");
                        error.push_str(&reason);
                    }
                    return Err(error);
                }
                _ => {}
            }
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn pending_pipeline_weight_load_stages<'a>(
    pipeline_plan: &'a run_plan::RunPlan,
    loaded_stages: &BTreeSet<u32>,
) -> Vec<&'a run_plan::StagePlan> {
    let mut pending = pipeline_plan
        .stages
        .iter()
        .filter(|stage| !loaded_stages.contains(&stage.stage_index))
        .collect::<Vec<_>>();
    pending.sort_by_key(|stage| stage.stage_index);
    pending
}

#[allow(clippy::too_many_arguments)]
fn send_pipeline_stage_provision(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    frame_tx: &mpsc::Sender<CollectedDatastreamFrame>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    run_id: u64,
    node_id: u64,
    pipeline_plan: &run_plan::RunPlan,
    stage: &run_plan::StagePlan,
    readies: &BTreeMap<u64, RuntimeReady>,
    pipeline_coordinator: &EndpointAddr,
    stage_shard_plans: &BTreeMap<u32, StageShardPlan>,
    loaded_stages: &BTreeSet<u32>,
    stage_resend_counts: &mut BTreeMap<u32, u64>,
    stage_last_sends: &mut BTreeMap<u32, Instant>,
    load_progress: &BTreeMap<u64, StageLoadProgress>,
    attempt: u64,
) -> Result<(), String> {
    let stage_node_id = stage.node_id.0;
    let current_send_count = stage_resend_counts
        .get(&stage.stage_index)
        .copied()
        .unwrap_or_default();
    let now = Instant::now();
    let ready = readies
        .get(&stage_node_id)
        .ok_or_else(|| format!("missing runtime-ready node for stage {}", stage.stage_index))?;
    let route_owner = stack.route_owner(ready.node_actor);
    let datastream_route_owner = stack.route_owner(ready.datastream_publisher);
    let member_state = stack.member_state(ready.swim_node_id);
    let route_matches_ready = route_owner == Some(ready.swim_node_id);
    orch_datastream.emit_bootstrap_to_channel(
        dashboard,
        MVP_STAGE_ROUTE,
        run_id,
        node_id,
        "stage_route_check",
        "observed",
        json!({
            "attempt":attempt,
            "stage_index":stage.stage_index,
            "stage_node_id":stage_node_id,
            "node_actor":ready.node_actor,
            "datastream_publisher":ready.datastream_publisher,
            "swim_node_id":format!("{:?}", ready.swim_node_id),
            "member_state":member_state.map(|state| format!("{:?}", state)),
            "route_owner":route_owner.map(|owner| format!("{:?}", owner)),
            "datastream_route_owner":datastream_route_owner.map(|owner| format!("{:?}", owner)),
            "route_matches_ready":route_matches_ready,
        }),
    );
    if member_state == Some(MemberState::Dead) {
        let reason = format!(
            "stage {} node {} is dead while loading pipeline weights",
            stage.stage_index, stage_node_id
        );
        let liveness = stage_load_liveness_detail(
            load_progress.get(&stage_node_id),
            stage.stage_index,
            stage_node_id,
            member_state,
            route_owner,
            datastream_route_owner,
            route_matches_ready,
            "heartbeat_missed",
        );
        orch_datastream.emit_bootstrap(
            dashboard,
            run_id,
            node_id,
            "stage_provision_wait",
            "failed",
            json!({
                "attempt":attempt,
                "stage_count":pipeline_plan.stages.len(),
                "stage_index":stage.stage_index,
                "stage_node_id":stage_node_id,
                "stage_send_count":current_send_count,
                "loaded_stage_count":loaded_stages.len(),
                "member_state":"Dead",
                "route_owner":route_owner.map(|owner| format!("{:?}", owner)),
                "datastream_route_owner":datastream_route_owner.map(|owner| format!("{:?}", owner)),
                "classification":"heartbeat_missed",
                "liveness":liveness,
                "reason":reason,
            }),
        );
        return Err(reason);
    }
    let (should_send, dispatch_reason) = stage_provision_dispatch(
        load_progress.get(&stage_node_id),
        current_send_count,
        stage_last_sends.get(&stage.stage_index).copied(),
        now,
    );
    if !should_send {
        orch_datastream.emit_bootstrap(
            dashboard,
            run_id,
            node_id,
            "stage_provision_wait",
            "observed",
            json!({
                "attempt":attempt,
                "stage_count":pipeline_plan.stages.len(),
                "stage_index":stage.stage_index,
                "stage_node_id":stage_node_id,
                "stage_send_count":current_send_count,
                "loaded_stage_count":loaded_stages.len(),
                "resend_suppressed":true,
                "resend_reason":dispatch_reason,
                "liveness":stage_load_liveness_detail(
                    load_progress.get(&stage_node_id),
                    stage.stage_index,
                    stage_node_id,
                    member_state,
                    route_owner,
                    datastream_route_owner,
                    route_matches_ready,
                    "waiting",
                ),
                "message":format!(
                    "loaded {} of {}; waiting on stage {}",
                    loaded_stages.len(),
                    pipeline_plan.stages.len(),
                    stage.stage_index
                )
            }),
        );
        return Ok(());
    }
    let stage_send_count = {
        let count = stage_resend_counts.entry(stage.stage_index).or_default();
        *count += 1;
        *count
    };
    stage_last_sends.insert(stage.stage_index, now);
    orch_datastream.emit_bootstrap(
        dashboard,
        run_id,
        node_id,
        "stage_provision_send",
        "sent",
        json!({
            "attempt":attempt,
            "stage_count":pipeline_plan.stages.len(),
            "stage_index":stage.stage_index,
            "stage_send_count":stage_send_count,
            "loaded_stage_count":loaded_stages.len(),
            "parallel_weight_acquisition":true,
            "resend_reason":dispatch_reason,
        }),
    );
    if stage_send_count == 1 || stage_send_count % 15 == 0 {
        orch_datastream.emit_bootstrap(
            dashboard,
            run_id,
            node_id,
            "stage_provision_wait",
            "observed",
            json!({
                "attempt":attempt,
                "stage_count":pipeline_plan.stages.len(),
                "stage_index":stage.stage_index,
                "stage_node_id":stage_node_id,
                "stage_send_count":stage_send_count,
                "loaded_stage_count":loaded_stages.len(),
                "liveness":stage_load_liveness_detail(
                    load_progress.get(&stage_node_id),
                    stage.stage_index,
                    stage_node_id,
                    member_state,
                    route_owner,
                    datastream_route_owner,
                    route_matches_ready,
                    "waiting",
                ),
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
        stage_shard_plans,
    )?;
    pump(driver, stack, frame_tx);
    Ok(())
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

#[derive(Clone, Debug, Default)]
struct StageLoadProgress {
    node_id: u64,
    stage_index: Option<u32>,
    phase: Option<String>,
    bytes_done: Option<u64>,
    bytes_total: Option<u64>,
    last_progress: Option<Instant>,
    last_worker_event: Option<String>,
    failure_reason: Option<String>,
    host_gpu_samples: u64,
}

impl StageLoadProgress {
    fn to_json(&self) -> Value {
        json!({
            "node_id": self.node_id,
            "stage_index": self.stage_index,
            "phase": self.phase.as_deref().unwrap_or("unknown"),
            "bytes_done": self.bytes_done,
            "bytes_total": self.bytes_total,
            "last_progress_age_ms": self.last_progress.map(|at| at.elapsed().as_millis()),
            "last_worker_event": self.last_worker_event,
            "failure_reason": self.failure_reason,
            "host_gpu_samples": self.host_gpu_samples,
            "host_gpu_missing": self.host_gpu_samples == 0,
        })
    }
}

fn stage_load_phase_is_active(phase: Option<&str>) -> bool {
    matches!(
        phase,
        Some(
            "loading_weights"
                | "prefetching_model"
                | "prefetching_stage_shard"
                | "fetching_stage_shard"
                | "stage_shard_cache_ready"
                | "stage_shard_ready"
                | "cache_ready"
                | "constructing_stage"
                | "stage_constructed"
                | "building_tokenizer"
                | "tokenizer_ready"
        )
    )
}

fn stage_provision_dispatch(
    progress: Option<&StageLoadProgress>,
    send_count: u64,
    last_send: Option<Instant>,
    now: Instant,
) -> (bool, &'static str) {
    if send_count == 0 {
        return (true, "initial");
    }
    let Some(progress) = progress else {
        return (true, "no_progress_after_send");
    };
    if progress.failure_reason.is_some() || progress.phase.as_deref() == Some("failed") {
        return (false, "worker_load_failed");
    }
    if progress.phase.as_deref() == Some("weights_loaded") {
        return (false, "weights_loaded_report_pending");
    }
    if !stage_load_phase_is_active(progress.phase.as_deref()) {
        return (true, "unknown_or_inactive_progress");
    }
    let Some(last_progress) = progress.last_progress else {
        return (true, "active_phase_without_progress_time");
    };
    if now.duration_since(last_progress) < STAGE_PROVISION_ACTIVE_RESEND_AFTER {
        return (false, "active_progress");
    }
    if let Some(last_send) = last_send
        && now.duration_since(last_send) < STAGE_PROVISION_ACTIVE_RESEND_AFTER
    {
        return (false, "recent_stale_progress_resend");
    }
    (true, "stale_progress")
}

fn stage_load_liveness_detail(
    progress: Option<&StageLoadProgress>,
    stage_index: u32,
    stage_node_id: u64,
    member_state: Option<MemberState>,
    route_owner: Option<DistNodeId>,
    datastream_route_owner: Option<DistNodeId>,
    route_matches_ready: bool,
    classification: &str,
) -> Value {
    json!({
        "classification": classification,
        "stage_index": stage_index,
        "stage_node_id": stage_node_id,
        "member_state": member_state.map(|state| format!("{:?}", state)),
        "route_owner": route_owner.map(|owner| format!("{:?}", owner)),
        "datastream_route_owner": datastream_route_owner.map(|owner| format!("{:?}", owner)),
        "route_matches_ready": route_matches_ready,
        "load_progress": progress.map(StageLoadProgress::to_json),
        "host_gpu_missing": progress.is_none_or(|progress| progress.host_gpu_samples == 0),
    })
}

fn update_load_progress_from_frame(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    collected: &CollectedDatastreamFrame,
    now: Instant,
) {
    let stream_node_id = collected.stream.node.as_str().parse::<u64>().ok();
    if collected.channel_name == "host.gpu" {
        if let Some(node_id) = stream_node_id {
            let entry = progress
                .entry(node_id)
                .or_insert_with(|| StageLoadProgress {
                    node_id,
                    ..StageLoadProgress::default()
                });
            entry.host_gpu_samples = entry.host_gpu_samples.saturating_add(1);
        }
        return;
    }

    let Ok(value) = serde_json::from_slice::<Value>(&collected.frame.payload) else {
        return;
    };
    if value.get("type").and_then(Value::as_str) == Some("NodeEvent") {
        update_load_progress_from_node_event(progress, &value, now);
        return;
    }
    if collected.channel_name == "mvp.worker.weights" {
        let Some(node_id) = stream_node_id else {
            return;
        };
        update_load_progress_from_worker_event(progress, node_id, None, &value, now);
    }
}

fn update_load_progress_from_node_event(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    value: &Value,
    now: Instant,
) {
    let Some(node_id) = numeric_json_field(value, "node_id") else {
        return;
    };
    let stage_index =
        numeric_json_field(value, "stage_index").and_then(|stage| u32::try_from(stage).ok());
    let phase = value.get("phase").and_then(Value::as_str);
    let status = value.get("status").and_then(Value::as_str);
    let detail = value.get("detail").unwrap_or(&Value::Null);
    if phase == Some("load_weights") {
        let load_phase = match status {
            Some("started") => Some("loading_weights"),
            Some("ready") => Some("weights_loaded"),
            Some("failed") => Some("failed"),
            _ => None,
        };
        if let Some(load_phase) = load_phase {
            let entry = progress
                .entry(node_id)
                .or_insert_with(|| StageLoadProgress {
                    node_id,
                    ..StageLoadProgress::default()
                });
            entry.stage_index = stage_index.or(entry.stage_index);
            entry.phase = Some(load_phase.to_owned());
            entry.last_progress = Some(now);
            if status == Some("failed") {
                entry.failure_reason = detail
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
        }
    }
    if let Some(worker_event) = detail.get("event") {
        update_load_progress_from_worker_event(progress, node_id, stage_index, worker_event, now);
    }
}

fn update_load_progress_from_worker_event(
    progress: &mut BTreeMap<u64, StageLoadProgress>,
    node_id: u64,
    stage_index: Option<u32>,
    event: &Value,
    now: Instant,
) {
    let Some(event_type) = event.get("type").and_then(Value::as_str) else {
        return;
    };
    let Some(phase) = load_phase_for_worker_event(event_type) else {
        return;
    };
    let entry = progress
        .entry(node_id)
        .or_insert_with(|| StageLoadProgress {
            node_id,
            ..StageLoadProgress::default()
        });
    entry.stage_index = stage_index.or(entry.stage_index);
    entry.phase = Some(phase.to_owned());
    entry.last_worker_event = Some(event_type.to_owned());
    entry.last_progress = Some(now);
    if let Some(bytes_done) =
        numeric_json_field(event, "bytes_done").or_else(|| numeric_json_field(event, "bytes"))
    {
        entry.bytes_done = Some(bytes_done);
    }
    if let Some(bytes_total) = numeric_json_field(event, "bytes_total") {
        entry.bytes_total = Some(bytes_total);
    }
}

fn numeric_json_field(value: &Value, field: &str) -> Option<u64> {
    value
        .get(field)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn load_phase_for_worker_event(event_type: &str) -> Option<&'static str> {
    match event_type {
        "GgufDownloadStarted" | "GgufDownloadProgress" => Some("prefetching_model"),
        "GgufCacheReady" => Some("cache_ready"),
        "StageShardFetchStarted"
        | "StageShardRangeFetchStarted"
        | "StageShardRangeFetchReady"
        | "StageShardTensorFetchStarted"
        | "StageShardTensorFetchReady" => Some("fetching_stage_shard"),
        "StageShardCacheReady" => Some("stage_shard_cache_ready"),
        "StageShardReady" => Some("stage_shard_ready"),
        "StageShardFetchFailed" => Some("failed"),
        "PipelineStageFromGgufStarted" => Some("constructing_stage"),
        "PipelineStageFromGgufReady" => Some("stage_constructed"),
        "TokenizerBuildStarted" => Some("building_tokenizer"),
        "TokenizerBuildReady" => Some("tokenizer_ready"),
        "WeightsLoaded" => Some("weights_loaded"),
        "WorkerFatal" => Some("failed"),
        _ => None,
    }
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
        out.record_channel::<MembershipTransition>();
        out.record_channel::<SwimProbeEvent>();
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

    fn record_channel<R: Record>(&mut self) -> ChannelId {
        if let Some(id) = self.channels.get(R::CHANNEL).copied() {
            return id;
        }
        let id = self.producer.register_record::<R>();
        self.channels.insert(R::CHANNEL.to_owned(), id);
        self.channel_names.insert(id, R::CHANNEL.to_owned());
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
        let benchmark = benchmark::stamp("mvp-orchestrator");
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
            "span_id":format!("mvp-orchestrator:{run_id}:{}:{phase}", benchmark["producer_sequence"]),
            "parent_span_id":Value::Null,
            "benchmark":benchmark,
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
        let benchmark = benchmark::stamp("mvp-orchestrator");
        let payload = serde_json::to_vec(&json!({
            "schema_version": benchmark["schema_version"].clone(),
            "type":"OrchPromptEvent",
            "event_type":"OrchPromptEvent",
            "event_name":phase,
            "phase":phase,
            "status":status,
            "run_id":run_id,
            "node_id":node_id,
            "request_id":request_id,
            "producer_component":benchmark["producer_component"].clone(),
            "producer_instance_id":benchmark["producer_instance_id"].clone(),
            "producer_process_id":benchmark["producer_process_id"].clone(),
            "producer_sequence":benchmark["producer_sequence"].clone(),
            "wall_clock_unix_ms":benchmark["wall_clock_unix_ms"].clone(),
            "monotonic_ms":benchmark["monotonic_ms"].clone(),
            "clock_source":benchmark["clock_source"].clone(),
            "span_id":format!("mvp-orchestrator:{run_id}:{request_id}:{}:{phase}", benchmark["producer_sequence"]),
            "parent_span_id":format!("request:{request_id}"),
            "benchmark":benchmark,
            "detail":detail,
        }))
        .expect("serialize orch prompt event");
        self.emit_bytes(dashboard, MVP_ORCH_PROMPT, payload);
    }

    fn emit_record<R: Record>(&mut self, dashboard: Option<&DashboardSupport>, record: &R) {
        let id = self.record_channel::<R>();
        self.producer.submit_record(id, record);
        self.flush(dashboard, "orchestrator");
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
            let _ = archive.record(source, stream, channel, frame);
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

fn wait_for_runtime_ready(ctx: RuntimeReadyAckLoop<'_>) -> Result<RuntimeReady, String> {
    let RuntimeReadyAckLoop {
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
        run_id,
        orchestrator_node_id: node_id,
        provider,
    } = ctx;
    let mut pending_ready: Option<RuntimeReady> = None;
    let mut node_swim_started = false;
    let mut node_swim_ready = false;
    let mut node_route_started = false;
    loop {
        pump(driver, stack, frame_tx);
        drain_frames(frame_rx, dashboard, orch_datastream);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for node ready".to_owned());
        }
        drain_observations_with_exit(obs_rx, dashboard, orch_datastream, provider, |_, status| {
            format!("node exited before ready: {status:?}")
        })?;
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
                    stage_shard_plan: None,
                }
            }),
        )
        .map_err(|e| format!("send stage provision: {e}"))
}

fn wait_for_weights_loaded(ctx: RuntimeReadyAckLoop<'_>, stage_index: u32) -> Result<(), String> {
    let RuntimeReadyAckLoop {
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
        run_id,
        orchestrator_node_id: node_id,
        provider,
    } = ctx;
    loop {
        pump(driver, stack, frame_tx);
        drain_orch_stdio_capture(orch_stdio_rx, orch_datastream, dashboard, run_id, node_id);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for weights loaded".to_owned());
        }
        drain_observations_with_exit(obs_rx, dashboard, orch_datastream, provider, |_, status| {
            format!("node exited while loading weights: {status:?}")
        })?;
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
                    reason,
                } if report_run_id == run_id && report_stage_index == stage_index => {
                    let mut error =
                        format!("stage {report_stage_index} faulted while loading weights");
                    if let Some(reason) = reason {
                        error.push_str(": ");
                        error.push_str(&reason);
                    }
                    return Err(error);
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

type PipelineSendHandle = EdgeSendHandle;
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
    last_progress_at: Option<Instant>,
    next_wait_log_at: Option<Instant>,
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
            token_in_sender: driver
                .spawn_edge_send_pump(first_stage_endpoint, token_in_edge.edge_id.0)?,
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
            last_progress_at: None,
            next_wait_log_at: None,
        })
    }

    fn is_active(&self) -> bool {
        self.active.is_some()
    }

    fn note_progress(&mut self) {
        let now = Instant::now();
        self.last_progress_at = Some(now);
        self.next_wait_log_at = now.checked_add(PIPELINE_PROMPT_WAIT_LOG_INTERVAL);
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
        if self.active.is_some() || self.pending_encode.is_some() || self.pending_decode.is_some() {
            let active_request_id = self.active.as_ref().map(|active| active.request.request_id);
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "pipeline_prompt_busy",
                "failed",
                json!({
                    "active_request_id":active_request_id,
                    "pending_encode":self.pending_encode.is_some(),
                    "pending_decode":self.pending_decode.is_some(),
                }),
            );
            let _ = events.send(PromptEvent::Fault {
                request_id,
                error: "pipeline prompt runtime is busy".to_owned(),
            });
            return Ok(());
        }
        self.generated_tokens.clear();
        self.final_text.clear();
        self.recv_buffer.clear();
        self.pending_decode = None;
        self.pending_encode = Some(PendingEncode { request_id });
        self.started_at = Some(Instant::now());
        self.note_progress();
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

    fn emit_wait_progress(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        orch_datastream: &mut OrchDatastream,
        run_id: u64,
        node_id: u64,
    ) {
        let Some(active) = self.active.as_ref() else {
            return;
        };
        let now = Instant::now();
        let request_id = active.request.request_id;
        let elapsed_ms = self
            .started_at
            .map(|started| duration_ms_u64(now.saturating_duration_since(started)))
            .unwrap_or(0);
        let idle_ms = self
            .last_progress_at
            .map(|last| duration_ms_u64(now.saturating_duration_since(last)))
            .unwrap_or(elapsed_ms);
        if self.next_wait_log_at.is_some_and(|next| now >= next) {
            orch_datastream.emit_prompt(
                dashboard,
                run_id,
                node_id,
                request_id,
                "pipeline_prompt_wait",
                "waiting",
                json!({
                    "elapsed_ms":elapsed_ms,
                    "idle_ms":idle_ms,
                    "pending_encode":self.pending_encode.is_some(),
                    "pending_decode":self.pending_decode.is_some(),
                    "generated_tokens":self.generated_tokens.len(),
                    "next_sequence":self.next_sequence,
                }),
            );
            self.next_wait_log_at = now.checked_add(PIPELINE_PROMPT_WAIT_LOG_INTERVAL);
        }
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
        self.note_progress();
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
        let events = active.events.clone();
        orch_datastream.emit_prompt(
            dashboard,
            run_id,
            node_id,
            request_id,
            "pipeline_tokenizer_decode",
            "ready",
            json!({"node_actor":self.tokenizer_decode_actor,"reply_to":self.tokenizer_reply_to,"text_bytes":text.len()}),
        );
        self.note_progress();
        self.final_text.push_str(&text);
        if !text.is_empty() {
            let _ = events.send(PromptEvent::TextDelta { request_id, text });
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
            let _ = events.send(PromptEvent::Done {
                request_id,
                final_text,
                tokens_generated,
                elapsed_ms,
            });
            self.last_progress_at = None;
            self.started_at = None;
            self.next_wait_log_at = None;
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
        self.note_progress();
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
        let should_fault = self
            .active
            .as_ref()
            .is_some_and(|active| active.request.request_id == request_id);
        if !should_fault {
            return;
        }
        if let Some(active) = self.active.take() {
            let _ = active.events.send(PromptEvent::Fault { request_id, error });
        }
        self.pending_encode = None;
        self.pending_decode = None;
        self.started_at = None;
        self.last_progress_at = None;
        self.next_wait_log_at = None;
    }

    fn poll_driver(&mut self, driver: &mut IrohDriver) {
        driver.pump_edge_ingress();
        for event in driver.drain_edge_events() {
            match event {
                EdgeTransportEvent::BytesRead { edge_id, bytes, .. }
                    if edge_id == self.token_out_edge_id =>
                {
                    self.note_progress();
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
    ctx: RuntimeReadyAckLoop<'_>,
    work_rx: &mpsc::Receiver<PromptWork>,
    prompt_events: &swactor::runtime::Inbox<PromptEvent>,
    node_actor: ActorAddress,
    reply_to: ActorAddress,
    tokenizer_events: &swactor::runtime::Inbox<TokenizerEvent>,
    tokenizer_encode_actor: ActorAddress,
    tokenizer_decode_actor: ActorAddress,
    tokenizer_reply_to: ActorAddress,
    pipeline_plan: Option<&run_plan::RunPlan>,
    prompt_endpoint: EndpointAddr,
) -> Result<(), String> {
    let RuntimeReadyAckLoop {
        driver,
        stack,
        obs_rx,
        frame_rx,
        frame_tx,
        stop_rx,
        dashboard,
        orch_datastream,
        orch_stdio_rx,
        run_id,
        orchestrator_node_id: node_id,
        provider,
        ..
    } = ctx;
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
            pipeline.emit_wait_progress(dashboard, orch_datastream, run_id, node_id);
        }
        drain_observations_with_exit(
            obs_rx,
            dashboard,
            orch_datastream,
            &provider,
            |_, status| format!("node exited: {status:?}"),
        )?;
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

fn drain_observations_with_exit(
    obs_rx: &mpsc::Receiver<PluginObservation>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    provider: &ProviderKind,
    exit_message: impl Fn(u64, Option<i32>) -> String,
) -> Result<(), String> {
    while let Ok(observation) = obs_rx.try_recv() {
        emit_plugin_observation(orch_datastream, dashboard, provider, &observation);
        match observation {
            PluginObservation::Failed { reason, .. } => return Err(reason),
            PluginObservation::Exited {
                node_id, status, ..
            } => return Err(exit_message(node_id, status)),
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
    provider: &ProviderKind,
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
        archive_collected_frame(collected, dashboard, orch_datastream);
    }
}

fn drain_frames_with_load_progress(
    frame_rx: &mpsc::Receiver<CollectedDatastreamFrame>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    progress: &mut BTreeMap<u64, StageLoadProgress>,
) {
    while let Ok(collected) = frame_rx.try_recv() {
        update_load_progress_from_frame(progress, &collected, Instant::now());
        archive_collected_frame(collected, dashboard, orch_datastream);
    }
}

fn archive_collected_frame(
    collected: CollectedDatastreamFrame,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
) {
    ingest_dashboard_frame(
        dashboard,
        &collected.stream,
        &collected.channel_name,
        &collected.frame,
    );
    orch_datastream.archive_frame(
        "node",
        &collected.stream,
        &collected.channel_name,
        &collected.frame,
    );
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
        let peer = format_dist_node_id(transition.peer);
        let from = transition.from.map(|state| format!("{:?}", state));
        let to = format!("{:?}", transition.to);
        let member_state = stack
            .member_state(transition.peer)
            .map(|state| format!("{:?}", state));
        let last_ack_age_ms = transition.last_ack_age.map(duration_ms_u64);
        let consecutive_timeouts = transition.consecutive_timeouts;
        let recent_probe_targets = swim_recent_probe_targets(stack);
        orch_datastream.emit_bootstrap_to_channel(
            dashboard,
            MVP_SWIM_MEMBERSHIP,
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
        orch_datastream.emit_record(
            dashboard,
            &MembershipTransition {
                peer,
                from: from.unwrap_or_default(),
                to,
                reason: transition.reason.to_owned(),
                last_ack_age_ms,
                consecutive_timeouts,
                recent_probe_targets,
                member_state,
            },
        );
    }
}

fn emit_swim_probe_events(
    orch_datastream: &mut OrchDatastream,
    dashboard: Option<&DashboardSupport>,
    stack: &DistributionRuntimeStack,
    local_phase: &str,
) {
    for event in stack.drain_swim_probe_events() {
        let record = swim_probe_event_record(stack, event, local_phase);
        orch_datastream.emit_record(dashboard, &record);
    }
}

fn swim_probe_event_record(
    stack: &DistributionRuntimeStack,
    event: ObservedProbeEvent,
    local_phase: &str,
) -> SwimProbeEvent {
    let config = &stack.swim_config;
    let budget_ms = event.budget_ms;
    SwimProbeEvent {
        event: event.event.to_owned(),
        target: format_dist_node_id(event.target),
        sequence: event.sequence,
        kind: event.kind.to_owned(),
        rtt_ms: event.rtt_ms,
        budget_ms,
        budget_ticks: budget_ms,
        last_ack_age_ms: event.last_ack_age.map(duration_ms_u64),
        consecutive_timeouts: event.consecutive_timeouts,
        recent_probe_targets: swim_recent_probe_targets(stack),
        member_state: stack
            .member_state(event.target)
            .map(|state| format!("{:?}", state)),
        local_phase: local_phase.to_owned(),
        probe_interval_ms: duration_ms_u64(config.probe_interval),
        probe_timeout_ms: duration_ms_u64(config.probe_timeout),
        indirect_probes: u32::try_from(config.indirect_probes).unwrap_or(u32::MAX),
        suspicion_timeout_ms: duration_ms_u64(config.suspicion_timeout),
        dead_reprobe_interval_ms: duration_ms_u64(config.dead_reprobe_interval),
        probe_mode: format!("{:?}", config.probe_mode),
        lifeguard_enabled: config.lifeguard.is_some(),
    }
}

fn swim_recent_probe_targets(stack: &DistributionRuntimeStack) -> Vec<String> {
    stack
        .swim_telemetry
        .recent_targets()
        .into_iter()
        .map(format_dist_node_id)
        .collect()
}

fn format_dist_node_id(node_id: DistNodeId) -> String {
    format!("{:?}", node_id)
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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

fn local_tinygrad_worker_env(provider: &ProviderKind) -> Option<(String, String)> {
    optional_env("MVP_TINYGRAD_WORKER").or_else(|| {
        if provider != &provider_kind::process() {
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
