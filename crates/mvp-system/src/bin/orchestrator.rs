use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use datastream::{ChannelId, DatastreamSink, Frame, Lifetime, Mux, NodeId, StreamId};
use distribution::node::DistributedNodeConfig;
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{NodeAgentMsg, StageProvisionWire};
use mvp_system::actors::orchestrator::{OrchestratorActor, OrchestratorReport};
use mvp_system::actors::register_mvp_actor_codecs;
use mvp_system::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
#[cfg(feature = "local-e2e")]
use mvp_system::dashboard_view::MvpClusterDashboardView;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::node_provisioning::ProviderKind;
use mvp_system::orchestrator_run_fsm::{RunConfig, RunId};
use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, read_submit_prompt, write_json_line};
use mvp_system::provisioning::{
    LocalDockerPlugin, NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink,
    ProviderMount, ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream,
    ProvisionPlugin,
};
#[cfg(test)]
use mvp_system::relay_provisioning::relay_runtime_config_from_env;
use mvp_system::relay_provisioning::{
    MVP_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV, relay_mode_env_value,
    relay_runtime_config_from_settings,
};
use mvp_system::run_plan::{GgufSource, TokenizerSource};
use mvp_system::telemetry::{
    MVP_PROVISIONING_EVENTS, MvpProvisionEventRecord, MvpProvisionLogRecord,
    mvp_provision_log_channel,
};
use mvp_system::vastai_provisioning::{
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

const DEFAULT_IMAGE: &str = "swactor-mvp-node:latest";
const MVP_RUNTIME_CONFIG_ENV: &str = "MVP_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MVP_CACHED_MODEL_HOST_PATH";
const CACHED_MODEL_CONTAINER_DIR: &str = "/models/cached";
const DEFAULT_RPC_BIND: &str = "127.0.0.1:19777";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_MAX_TOKENS: u32 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const MVP_ORCH_BOOTSTRAP: &str = "mvp.orch.bootstrap";
const MVP_ORCH_PROMPT: &str = "mvp.orch.prompt";
const DATASTREAM_FRAME_LOG_ENV: &str = "MVP_DATASTREAM_FRAME_LOG";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-orchestrator: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let mut config = Config::from_defaults_toml_env_args(std::env::args().skip(1))?;
    config.prepare_vastai_ssh_key()?;
    let orch_stdio_rx = install_orch_stdio_capture()?;
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
            "layer_end_exclusive":config.layer_end_exclusive,
            "relay_mode":format!("{:?}", config.relay.mode),
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
            additional_alpns: vec![],
        },
    ) {
        Ok(driver) => {
            orch_datastream.emit_bootstrap(
                None,
                config.run_id,
                config.node_id,
                "iroh_driver",
                "ready",
                json!({"relay_mode":format!("{:?}", config.relay.mode)}),
            );
            driver
        }
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

    let (frame_tx, frame_rx) = mpsc::channel::<(StreamId, Frame)>();
    let datastream_sink = match stack
        .runtime
        .spawn(DatastreamSink::new(move |stream, frame| {
            let _ = frame_tx.send((stream, frame));
        })) {
        Ok(actor) => actor,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                None,
                config.run_id,
                config.node_id,
                "datastream_sink",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("spawn datastream sink: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(datastream_sink, 1));
    orch_datastream.emit_bootstrap(
        None,
        config.run_id,
        config.node_id,
        "datastream_sink",
        "ready",
        json!({"actor":datastream_sink,"channel":"local_mpsc"}),
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

    let (work_tx, work_rx) = mpsc::channel::<PromptWork>();
    let stop_rx = spawn_stop_listener();

    let mut provisioner = config.build_provisioner(Arc::clone(&stack.runtime))?;
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
    let node_spec =
        config.node_spec(driver.endpoint_addr(), datastream_sink, orchestrator_actor)?;
    orch_datastream.emit_event(
        dashboard.as_ref(),
        ProvisionEvent {
            run_id: config.run_id,
            node_id: config.node_id,
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
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "node_spec",
        "ready",
        json!({
            "provider":config.provider.as_str(),
            "image":&config.image,
            "relay_mode":relay_mode_env_value(&config.relay.mode),
            "docker_gpus":if config.provider == ProviderKind::Docker { Some(config.docker_gpus.as_str()) } else { None },
            "provider_config":config.provider_datastream_detail(),
            "env_keys":config.node_spec_env_keys(),
        }),
    );
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "provider_start",
        "started",
        json!({
            "provider":config.provider.as_str(),
            "image":&config.image,
            "node_id":config.node_id,
            "stage_index":config.stage_index,
        }),
    );
    let (returned_provisioner, handle_result) = start_node_with_stdio_capture(
        provisioner,
        node_spec,
        sink,
        orch_stdio_rx.as_ref(),
        dashboard.as_ref(),
        &mut orch_datastream,
        config.run_id,
        config.node_id,
    );
    provisioner = returned_provisioner;
    let handle = match handle_result {
        Ok(handle) => handle,
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "provider_start",
                "failed",
                json!({"provider":config.provider.as_str(),"error":error}),
            );
            drain_orch_stdio_capture(
                orch_stdio_rx.as_ref(),
                &mut orch_datastream,
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
            );
            return Err(error);
        }
    };
    let mut provisioned_node = ProvisionedNodeGuard::new(&mut *provisioner, handle);
    drain_orch_stdio_capture(
        orch_stdio_rx.as_ref(),
        &mut orch_datastream,
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
    );

    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "node_runtime_ready",
        "started",
        json!({}),
    );
    let ready = match wait_for_runtime_ready(
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        &orchestrator_reports,
        &stop_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
        orch_stdio_rx.as_ref(),
        config.run_id,
        config.node_id,
        config.provider,
    ) {
        Ok(ready) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "node_runtime_ready",
                "ready",
                json!({"endpoint":&ready.endpoint,"node_actor":ready.node_actor,"stage_index":ready.stage_index}),
            );
            drain_orch_stdio_capture(
                orch_stdio_rx.as_ref(),
                &mut orch_datastream,
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
            );
            ready
        }
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "node_runtime_ready",
                "failed",
                json!({"error":error}),
            );
            drain_orch_stdio_capture(
                orch_stdio_rx.as_ref(),
                &mut orch_datastream,
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
            );
            return Err(error);
        }
    };
    provisioned_node.complete_bootstrap()?;
    driver.join(std::slice::from_ref(&ready.endpoint));
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "node_join",
        "ready",
        json!({"endpoint":&ready.endpoint}),
    );
    match wait_for_route(&mut driver, &stack, ready.node_actor, &stop_rx) {
        Ok(()) => orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "node_route",
            "ready",
            json!({"node_actor":ready.node_actor}),
        ),
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "node_route",
                "failed",
                json!({"node_actor":ready.node_actor,"error":error}),
            );
            return Err(error);
        }
    }
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "stage_provision",
        "started",
        json!({
            "run_id":config.run_id,
            "node_id":config.node_id,
            "stage_index":config.stage_index,
            "stage_count":1,
            "layer_range":{"start":0,"end_exclusive":config.layer_end_exclusive},
            "model_id":&config.model_id,
        }),
    );
    match provision_stage(&stack, ready.node_actor, &config) {
        Ok(()) => {}
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "stage_provision",
                "failed",
                json!({"error":error}),
            );
            return Err(error);
        }
    }
    orch_datastream.emit_bootstrap(
        dashboard.as_ref(),
        config.run_id,
        config.node_id,
        "weights_loaded",
        "started",
        json!({"model_id":&config.model_id}),
    );
    match wait_for_weights_loaded(
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        &stop_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
        orch_stdio_rx.as_ref(),
        config.run_id,
        config.node_id,
        config.provider,
    ) {
        Ok(()) => orch_datastream.emit_bootstrap(
            dashboard.as_ref(),
            config.run_id,
            config.node_id,
            "weights_loaded",
            "ready",
            json!({"source_channel":"mvp.worker.weights","model_id":&config.model_id}),
        ),
        Err(error) => {
            orch_datastream.emit_bootstrap(
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
                "weights_loaded",
                "failed",
                json!({"error":error}),
            );
            drain_orch_stdio_capture(
                orch_stdio_rx.as_ref(),
                &mut orch_datastream,
                dashboard.as_ref(),
                config.run_id,
                config.node_id,
            );
            return Err(error);
        }
    }

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
        json!({"addr":rpc_addr.to_string(),"node_actor":ready.node_actor}),
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
        &work_rx,
        &prompt_events,
        &stop_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
        orch_stdio_rx.as_ref(),
        config.run_id,
        config.node_id,
        ready.node_actor,
        prompt_reply_actor,
        config.provider,
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
    let stop_result = provisioned_node.stop();
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
            "min_down_mbps": self.provisioning.selection.min_down_mbps,
            "min_up_mbps": self.provisioning.selection.min_up_mbps,
            "min_reliability": self.provisioning.selection.min_reliability,
            "require_verified": self.provisioning.selection.require_verified,
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
            Self::Local => ProviderKind::Docker,
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
        if provider != ProviderKind::Docker {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} is a host-local cache path and requires provider=docker"
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
    layer_end_exclusive: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    max_context: Option<u32>,
    relay: RelayRuntimeConfig,
    vastai: Option<VastAiRuntimeConfig>,
    cached_model: Option<CachedModelConfig>,
    datastream_frame_log: Option<PathBuf>,
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
    layer_end_exclusive: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    max_context: Option<u32>,
    relay_mode: Option<String>,
    relay_url: Option<String>,
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
    vastai_min_up_mbps_raw: Option<String>,
    vastai_min_reliability: Option<f64>,
    vastai_min_reliability_raw: Option<String>,
    vastai_require_verified: Option<bool>,
    vastai_require_verified_raw: Option<String>,
    vastai_poll_interval_secs: Option<u64>,
    vastai_poll_interval_secs_raw: Option<String>,
    cached_model_host_path: Option<PathBuf>,
    datastream_frame_log: Option<PathBuf>,
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
            layer_end_exclusive: 16,
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
            self.layer_end_exclusive = layer_end_exclusive;
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
            self.layer_end_exclusive =
                Self::parse_value("MVP_LAYER_END_EXCLUSIVE", &layer_end_exclusive)?;
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
        if let Some(api_key) =
            env_optional("MVP_VASTAI_API_KEY").or_else(|| env_optional("VASTAI_API_KEY"))
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
                    self.layer_end_exclusive = parse_next(&mut args, "--layer-end-exclusive")?
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
        let cached_model = self
            .cached_model_host_path
            .clone()
            .map(|path| CachedModelConfig::from_host_path(provider, path))
            .transpose()?;
        let mut gguf_source = self.gguf_source.clone();
        if let Some(cached_model) = &cached_model {
            gguf_source = GgufSource::LocalPath(cached_model.container_path.clone());
        }
        let relay = relay_runtime_config_from_settings(
            self.run_id,
            self.relay_mode.as_deref(),
            self.relay_url.as_deref(),
        )?;
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
            model_id: self.model_id,
            gguf_source,
            tokenizer: self.tokenizer,
            default_max_tokens: self.default_max_tokens,
            dashboard: self.dashboard,
            max_context: self.max_context,
            relay,
            vastai,
            cached_model,
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

    fn provider_datastream_detail(&self) -> Value {
        match self.provider {
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

    fn prepare_vastai_ssh_key(&mut self) -> Result<(), String> {
        if self.provider != ProviderKind::VastAi {
            return Ok(());
        }

        let api_key = self
            .vastai
            .as_ref()
            .and_then(|vastai| vastai.api_key.as_deref())
            .ok_or_else(|| {
                "MVP_VASTAI_API_KEY or VASTAI_API_KEY is required when MVP_NODE_PROVIDER=vastai"
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
            ProviderKind::Docker => Ok(Box::new(LocalDockerPlugin::new("mvp-orchestrator"))),
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
                    "MVP_VASTAI_API_KEY or VASTAI_API_KEY is required when MVP_NODE_PROVIDER=vastai"
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
            "MVP_DATASTREAM_SINK_ACTOR",
            "MVP_ORCHESTRATOR_ACTOR",
            "MVP_MODEL_ID",
            "MVP_IROH_RELAY_MODE",
        ];
        if self.relay.url.is_some() {
            keys.push(MVP_IROH_RELAY_URL_ENV);
        }
        if self.provider == ProviderKind::Docker {
            keys.push("MVP_DOCKER_GPUS");
        }
        if std::env::var_os("MVP_TINYGRAD_TEST_MODE").is_some() {
            keys.push("MVP_TINYGRAD_TEST_MODE");
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
        datastream_sink: ActorAddress,
        orchestrator_actor: ActorAddress,
    ) -> Result<NodeProvisionSpec, String> {
        let mut env = vec![
            ("MVP_RUN_ID".to_owned(), self.run_id.to_string()),
            ("MVP_LOGICAL_NODE_ID".to_owned(), self.node_id.to_string()),
            ("MVP_STAGE_INDEX".to_owned(), self.stage_index.to_string()),
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
                "MVP_DATASTREAM_SINK_ACTOR".to_owned(),
                serde_json::to_string(&datastream_sink)
                    .map_err(|e| format!("serialize datastream sink actor: {e}"))?,
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
        env.extend(optional_env("MVP_TINYGRAD_TEST_MODE"));
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
            ProviderKind::Docker => Vec::new(),
            ProviderKind::Mock => {
                return Err("mvp-orchestrator does not support mock provider".to_owned());
            }
        };
        let mounts = if let Some(cached_model) = &self.cached_model {
            vec![ProviderMount {
                host_path: cached_model.host_path.to_string_lossy().to_string(),
                container_path: cached_model.container_path.clone(),
                readonly: false,
            }]
        } else {
            Vec::new()
        };
        Ok(NodeProvisionSpec {
            run_id: self.run_id,
            node_id: self.node_id,
            stage_index: Some(self.stage_index),
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
    stage_index: u32,
}

struct ProvisionedNodeGuard<'a> {
    provisioner: &'a mut dyn ProvisionPlugin,
    handle: Option<mvp_system::provisioning::PluginNodeHandle>,
}

impl<'a> ProvisionedNodeGuard<'a> {
    fn new(
        provisioner: &'a mut dyn ProvisionPlugin,
        handle: mvp_system::provisioning::PluginNodeHandle,
    ) -> Self {
        Self {
            provisioner,
            handle: Some(handle),
        }
    }

    fn complete_bootstrap(&mut self) -> Result<(), String> {
        let Some(handle) = self.handle.as_ref() else {
            return Ok(());
        };
        self.provisioner.complete_bootstrap(handle)
    }

    fn stop(&mut self) -> Result<(), String> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        self.provisioner.stop_node(&handle)
    }
}

impl Drop for ProvisionedNodeGuard<'_> {
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
    Result<mvp_system::provisioning::PluginNodeHandle, String>,
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

struct FailedProvisionPlugin;

impl ProvisionPlugin for FailedProvisionPlugin {
    fn start_node(
        &mut self,
        _spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<mvp_system::provisioning::PluginNodeHandle, String> {
        Err("provider start worker disconnected".to_owned())
    }

    fn complete_bootstrap(
        &mut self,
        _handle: &mvp_system::provisioning::PluginNodeHandle,
    ) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(
        &mut self,
        _handle: &mvp_system::provisioning::PluginNodeHandle,
    ) -> Result<(), String> {
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

    fn record(&mut self, source: &str, stream: &StreamId, frame: &Frame) {
        let payload = match std::str::from_utf8(&frame.payload) {
            Ok(text) => json!({"encoding":"utf8","value":text}),
            Err(_) => json!({"encoding":"bytes","value":frame.payload}),
        };
        let record = json!({
            "arrival_seq":self.next_seq,
            "source":source,
            "stream":stream.to_string(),
            "channel":frame.channel.as_str(),
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
    mux: Mux,
    archive: Option<FrameArchive>,
}

impl OrchDatastream {
    fn new(run_id: u64, frame_log: Option<&Path>) -> Result<Self, String> {
        let stream = StreamId::new(NodeId::new("mvp-orchestrator"), Lifetime(run_id));
        Ok(Self {
            stream: stream.clone(),
            mux: Mux::unbounded(stream),
            archive: frame_log.map(FrameArchive::open).transpose()?,
        })
    }

    fn emit_event(&mut self, dashboard: Option<&DashboardSupport>, event: ProvisionEvent) {
        let payload = serde_json::to_vec(&MvpProvisionEventRecord::new(event))
            .expect("serialize provisioning event");
        self.emit_bytes(dashboard, ChannelId::new(MVP_PROVISIONING_EVENTS), payload);
    }

    fn emit_log(&mut self, dashboard: Option<&DashboardSupport>, line: ProvisionLogLine) {
        let channel = mvp_provision_log_channel(line.node_id, line.stream);
        let payload =
            serde_json::to_vec(&MvpProvisionLogRecord::new(line)).expect("serialize provision log");
        self.emit_bytes(dashboard, channel, payload);
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
        let payload = serde_json::to_vec(&json!({
            "type":"OrchBootstrap",
            "phase":phase,
            "status":status,
            "run_id":run_id,
            "node_id":node_id,
            "detail":detail,
        }))
        .expect("serialize orch bootstrap event");
        self.emit_bytes(dashboard, ChannelId::new(MVP_ORCH_BOOTSTRAP), payload);
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
            "detail":detail,
        }))
        .expect("serialize orch prompt event");
        self.emit_bytes(dashboard, ChannelId::new(MVP_ORCH_PROMPT), payload);
    }

    fn emit_bytes(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: ChannelId,
        payload: Vec<u8>,
    ) {
        self.emit_bytes_from(dashboard, channel, payload, "orchestrator");
    }

    fn emit_bytes_from(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: ChannelId,
        payload: Vec<u8>,
        source: &str,
    ) {
        self.mux.submit(channel, payload);
        self.flush(dashboard, source);
    }

    fn flush(&mut self, dashboard: Option<&DashboardSupport>, source: &str) {
        for frame in self.mux.drain() {
            ingest_dashboard_frame(dashboard, &self.stream, &frame);
            self.archive_frame(source, &self.stream.clone(), &frame);
        }
    }

    fn archive_frame(&mut self, source: &str, stream: &StreamId, frame: &Frame) {
        if let Some(archive) = &mut self.archive {
            archive.record(source, stream, frame);
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

#[cfg(feature = "local-e2e")]
struct DashboardSupport {
    handle: dashboard::DashboardHandle,
}

#[cfg(feature = "local-e2e")]
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
        let handle = dashboard::start_dashboard(config);
        handle.register_view(Arc::new(MvpClusterDashboardView::new()));
        handle.start_http_standalone();
        Ok(Some(Self { handle }))
    }

    fn ingest(&self, stream: &StreamId, frame: &Frame) {
        self.handle.ingest(stream, frame);
    }
}

#[cfg(not(feature = "local-e2e"))]
struct DashboardSupport;

#[cfg(not(feature = "local-e2e"))]
impl DashboardSupport {
    fn start(enabled: bool) -> Result<Option<Self>, String> {
        if enabled {
            return Err(
                "MVP_DASHBOARD requires building mvp-system with feature local-e2e".to_owned(),
            );
        }
        Ok(None)
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame) {}
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
    frame_rx: &mpsc::Receiver<(StreamId, Frame)>,
    orchestrator_reports: &swactor::runtime::Inbox<OrchestratorReport>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    provider: ProviderKind,
) -> Result<RuntimeReady, String> {
    loop {
        pump(driver, stack);
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
            } = report
            {
                if report_run_id == run_id && report_node_id == node_id {
                    return Ok(RuntimeReady {
                        endpoint,
                        node_actor,
                        stage_index,
                    });
                }
            }
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn wait_for_route(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    actor: ActorAddress,
    stop_rx: &mpsc::Receiver<()>,
) -> Result<(), String> {
    loop {
        pump(driver, stack);
        if stop_requested(stop_rx) {
            return Err("shutdown requested while waiting for node route".to_owned());
        }
        let ready = stack
            .route_view
            .read()
            .map(|view| view.contains_key(&actor))
            .unwrap_or(false);
        if ready {
            return Ok(());
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
            NodeAgentMsg::ProvisionStage(StageProvisionWire {
                run_id: config.run_id,
                authorized_orchestrator: 0,
                node_id: config.node_id,
                stage_index: config.stage_index,
                stage_count: 1,
                layer_start: 0,
                layer_end_exclusive: config.layer_end_exclusive,
                inbound_edge_id: 1,
                outbound_edge_id: 2,
                model_id: config.model_id.clone(),
                gguf_source: config.gguf_source.clone(),
                tokenizer: config.tokenizer.clone(),
            }),
        )
        .map_err(|e| format!("send stage provision: {e}"))
}

fn wait_for_weights_loaded(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<(StreamId, Frame)>,
    stop_rx: &mpsc::Receiver<()>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
    orch_stdio_rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    node_id: u64,
    provider: ProviderKind,
) -> Result<(), String> {
    loop {
        pump(driver, stack);
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
        while let Ok((stream, frame)) = frame_rx.try_recv() {
            ingest_dashboard_frame(dashboard, &stream, &frame);
            orch_datastream.archive_frame("node_cluster", &stream, &frame);
            let payload = String::from_utf8_lossy(&frame.payload);
            if frame.channel == ChannelId::new("mvp.worker.weights")
                && json_type_is(&payload, "WeightsLoaded")
            {
                return Ok(());
            }
            if json_type_is(&payload, "WorkerFatal") {
                return Err(format!("worker fatal while loading weights: {payload}"));
            }
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn serve_prompts(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    obs_rx: &mpsc::Receiver<PluginObservation>,
    frame_rx: &mpsc::Receiver<(StreamId, Frame)>,
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
    provider: ProviderKind,
) -> Result<(), String> {
    let mut active: Option<ActivePrompt> = None;
    loop {
        pump(driver, stack);
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
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            let trimmed = line.trim();
            if trimmed.eq_ignore_ascii_case("stop")
                || trimmed.eq_ignore_ascii_case("shutdown")
                || trimmed.eq_ignore_ascii_case("quit")
            {
                let _ = tx.send(());
                break;
            }
        }
    });
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
            ChannelId::new(channel),
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
    frame_rx: &mpsc::Receiver<(StreamId, Frame)>,
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
) {
    while let Ok((stream, frame)) = frame_rx.try_recv() {
        ingest_dashboard_frame(dashboard, &stream, &frame);
        orch_datastream.archive_frame("node_cluster", &stream, &frame);
    }
}

fn ingest_dashboard_frame(dashboard: Option<&DashboardSupport>, stream: &StreamId, frame: &Frame) {
    if let Some(dashboard) = dashboard {
        dashboard.ingest(stream, frame);
    }
}

fn pump(driver: &mut IrohDriver, stack: &DistributionRuntimeStack) {
    stack.tick_protocol_actors(Instant::now());
    driver.pump_inbound_to_actors();
    stack.pump_runtime_once();
    driver.drain_outbox(&stack.outbox);
}

fn json_type_is(payload: &str, expected: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| value.get("type").and_then(Value::as_str).map(str::to_owned))
        .as_deref()
        == Some(expected)
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_env(name: &str) -> Option<(String, String)> {
    env_optional(name).map(|value| (name.to_owned(), value))
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
        "MVP_STAGE_INDEX",
        "MVP_TOKEN_PROGRESS_EVERY",
        "MVP_TINYGRAD_TEST_MODE",
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
            let datastream_sink = ActorAddress([11; 32]);
            let orchestrator_actor = ActorAddress([12; 32]);
            config
                .node_spec(coordinator, datastream_sink, orchestrator_actor)
                .expect("node spec builds")
                .env
        })
    }

    fn env_value<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
        env.iter()
            .find(|(env_key, _)| env_key == key)
            .map(|(_, value)| value.as_str())
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

    struct TempModelFile {
        root: PathBuf,
        raw_path: PathBuf,
        canonical_path: PathBuf,
    }

    impl TempModelFile {
        fn new(file_name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "mvp-cached-model-test-{}-{}",
                std::process::id(),
                std::thread::current().name().unwrap_or("unnamed")
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(root.join("nested")).expect("create temp model dir");
            let canonical_path = root.join(file_name);
            std::fs::write(&canonical_path, b"fake gguf bytes").expect("write temp model file");
            let raw_path = root.join("nested").join("..").join(file_name);
            Self {
                root,
                raw_path,
                canonical_path: canonical_path
                    .canonicalize()
                    .expect("canonicalize temp model file"),
            }
        }
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
            &Frame::new(
                "stdout",
                datastream::Position(7),
                b"hello \xce\xbb".to_vec(),
            ),
        );
        archive.record(
            "orchestrator",
            &stream,
            &Frame::new("stderr", datastream::Position(8), vec![0xff, 0x00, b'A']),
        );
        drop(archive);

        let contents = std::fs::read_to_string(&path).expect("read frame archive jsonl");
        let records = contents
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("archive line is json"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            records,
            vec![
                json!({
                    "arrival_seq":0,
                    "source":"orchestrator",
                    "stream":"test-node#42",
                    "channel":"stdout",
                    "position":7,
                    "payload":{"encoding":"utf8","value":"hello λ"},
                }),
                json!({
                    "arrival_seq":1,
                    "source":"orchestrator",
                    "stream":"test-node#42",
                    "channel":"stderr",
                    "position":8,
                    "payload":{"encoding":"bytes","value":[255,0,65]},
                }),
            ]
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
        assert_eq!(config.layer_end_exclusive, 24);
        assert!(config.dashboard);
        assert_eq!(config.max_context, Some(768));
        assert!(matches!(config.relay.mode, iroh::RelayMode::Default));
        assert!(config.vastai.is_none());
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

        assert_eq!(config.provider, ProviderKind::Docker);
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
        let datastream_sink = ActorAddress([17; 32]);
        let orchestrator_actor = ActorAddress([18; 32]);
        let spec = config
            .node_spec(coordinator, datastream_sink, orchestrator_actor)
            .expect("node spec builds");

        assert_eq!(env_value(&spec.env, "MVP_MAX_CONTEXT"), Some("256"));
    }

    #[test]
    fn runtime_profile_selects_provider_and_node_provider_takes_precedence() {
        assert_eq!(
            selected_provider(&[("MVP_RUNTIME_CONFIG", "local")]),
            ProviderKind::Docker
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
    fn docker_cached_model_builds_local_gguf_env_and_writable_file_mount_from_canonical_host_path()
    {
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
        let datastream_sink = ActorAddress([13; 32]);
        let orchestrator_actor = ActorAddress([14; 32]);
        let spec = config
            .node_spec(coordinator, datastream_sink, orchestrator_actor)
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
                readonly: false,
            }]
        );
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
                "MVP_CACHED_MODEL_HOST_PATH is a host-local cache path and requires provider=docker"
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
        ) -> Result<mvp_system::provisioning::PluginNodeHandle, String> {
            unreachable!("guard tests construct handles directly")
        }

        fn complete_bootstrap(
            &mut self,
            _handle: &mvp_system::provisioning::PluginNodeHandle,
        ) -> Result<(), String> {
            Ok(())
        }

        fn stop_node(
            &mut self,
            handle: &mvp_system::provisioning::PluginNodeHandle,
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
                mvp_system::provisioning::PluginNodeHandle {
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
                mvp_system::provisioning::PluginNodeHandle {
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
}
