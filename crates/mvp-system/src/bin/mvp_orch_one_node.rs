use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use datastream::{ChannelId, DatastreamSink, Frame, Lifetime, NodeId, Position, StreamId};
use distribution::node::DistributedNodeConfig;
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{NodeAgentMsg, StageProvisionWire};
use mvp_system::actors::register_mvp_actor_codecs;
#[cfg(feature = "local-e2e")]
use mvp_system::dashboard_view::MvpClusterDashboardView;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::prompt_rpc::{PromptEvent, SubmitPrompt, read_submit_prompt, write_json_line};
use mvp_system::provisioning::{
    LocalDockerPlugin, NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink,
    ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream, ProvisionPlugin,
};
use mvp_system::run_plan::{GgufSource, TokenizerSource};
use mvp_system::telemetry::{
    MVP_PROVISIONING_EVENTS, MvpProvisionEventRecord, MvpProvisionLogRecord,
    mvp_provision_log_channel,
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

const DEFAULT_IMAGE: &str = "swactor-mvp-node:latest";
const DEFAULT_RPC_BIND: &str = "127.0.0.1:19777";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_MAX_TOKENS: u32 = 64;
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const BOOT_TIMEOUT: Duration = Duration::from_secs(180);
const ROUTE_TIMEOUT: Duration = Duration::from_secs(30);
const WEIGHT_TIMEOUT: Duration = Duration::from_secs(900);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-orch-one-node: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_env_and_args()?;
    let tokio = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
    let mut driver = IrohDriver::with_handle(
        tokio.handle().clone(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: config.relay_mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    )
    .map_err(|e| format!("create iroh driver: {e}"))?;
    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        |registry| {
            register_mvp_actor_codecs(registry);
            datastream::wire::register_datastream_codec(registry);
        },
    );
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );

    let (frame_tx, frame_rx) = mpsc::channel::<(StreamId, Frame)>();
    let datastream_sink = stack
        .runtime
        .spawn(DatastreamSink::new(move |stream, frame| {
            let _ = frame_tx.send((stream, frame));
        }))
        .map_err(|e| format!("spawn datastream sink: {e}"))?;
    stack.register_local_actor(driver.register_actor(datastream_sink, 1));
    let dashboard = DashboardSupport::start_from_env()?;
    let mut orch_datastream = OrchDatastream::new(config.run_id);

    let prompt_events = stack
        .runtime
        .new_inbox::<PromptEvent>()
        .map_err(|e| format!("prompt event inbox: {e}"))?;
    let prompt_reply_actor = *prompt_events.addr();
    stack.register_local_actor(driver.register_actor(prompt_reply_actor, 1));

    let (work_tx, work_rx) = mpsc::channel::<PromptWork>();
    let rpc_addr = spawn_prompt_rpc(
        config.rpc_bind,
        work_tx,
        config.default_max_tokens,
        config.default_timeout_ms,
    )?;
    println!(
        "{}",
        json!({"type":"prompt_rpc_ready","addr":rpc_addr.to_string()})
    );

    let mut docker = LocalDockerPlugin::new("mvp-orch-one-node");
    let (obs_tx, obs_rx) = mpsc::channel::<PluginObservation>();
    let sink = PluginSink::new(Arc::new(ChannelObservationSink {
        tx: Mutex::new(obs_tx),
    }));
    orch_datastream.emit_event(
        dashboard.as_ref(),
        ProvisionEvent {
            run_id: config.run_id,
            node_id: config.node_id,
            kind: ProvisionEventKind::ProvisionStart,
            message: Some(format!("starting Docker image {}", config.image)),
        },
    );
    let handle = docker.start_node(
        config.node_spec(driver.endpoint_addr(), datastream_sink)?,
        sink,
    )?;

    let ready = wait_for_runtime_ready(
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
    )?;
    driver.join(std::slice::from_ref(&ready.endpoint));
    wait_for_route(&mut driver, &stack, ready.node_actor)?;
    provision_stage(&stack, ready.node_actor, &config)?;
    wait_for_weights_loaded(
        &mut driver,
        &stack,
        &obs_rx,
        &frame_rx,
        dashboard.as_ref(),
        &mut orch_datastream,
    )?;

    println!(
        "{}",
        json!({"type":"prompt_loop_ready","addr":rpc_addr.to_string(),"node_actor":ready.node_actor})
    );

    let stop_rx = spawn_stop_listener();
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
        ready.node_actor,
        prompt_reply_actor,
    );
    let stop_result = docker.stop_node(&handle);
    result.and(stop_result)
}

#[derive(Clone)]
struct Config {
    image: String,
    docker_gpus: String,
    rpc_bind: SocketAddr,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    default_timeout_ms: u64,
    relay_mode: iroh::RelayMode,
}

impl Config {
    fn from_env_and_args() -> Result<Self, String> {
        let mut args = std::env::args().skip(1);
        let mut config = Self {
            image: env_string("MVP_NODE_IMAGE", DEFAULT_IMAGE),
            docker_gpus: env_string("MVP_DOCKER_GPUS", "all"),
            rpc_bind: env_string("MVP_PROMPT_RPC_BIND", DEFAULT_RPC_BIND)
                .parse()
                .map_err(|e| format!("invalid MVP_PROMPT_RPC_BIND: {e}"))?,
            run_id: env_u64("MVP_RUN_ID", 1)?,
            node_id: env_u64("MVP_LOGICAL_NODE_ID", 1)?,
            stage_index: env_u32("MVP_STAGE_INDEX", 0)?,
            layer_end_exclusive: env_u32("MVP_LAYER_END_EXCLUSIVE", 16)?,
            model_id: env_string("MVP_MODEL_ID", DEFAULT_MODEL_ID),
            gguf_source: gguf_source_from_env(),
            tokenizer: tokenizer_from_env(),
            default_max_tokens: env_u32("MVP_PROMPT_MAX_TOKENS", DEFAULT_MAX_TOKENS)?,
            default_timeout_ms: env_u64("MVP_PROMPT_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?,
            relay_mode: relay_mode_from_env()?,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--image" => config.image = next_arg(&mut args, "--image")?,
                "--gpus" => config.docker_gpus = next_arg(&mut args, "--gpus")?,
                "--rpc-bind" => {
                    config.rpc_bind = next_arg(&mut args, "--rpc-bind")?
                        .parse()
                        .map_err(|e| format!("invalid --rpc-bind: {e}"))?
                }
                "--run-id" => config.run_id = parse_next(&mut args, "--run-id")?,
                "--node-id" => config.node_id = parse_next(&mut args, "--node-id")?,
                "--max-tokens" => {
                    config.default_max_tokens = parse_next(&mut args, "--max-tokens")?
                }
                "--timeout-ms" => {
                    config.default_timeout_ms = parse_next(&mut args, "--timeout-ms")?
                }
                "--model-id" => config.model_id = next_arg(&mut args, "--model-id")?,
                "--gguf-local-path" => {
                    config.gguf_source =
                        GgufSource::LocalPath(next_arg(&mut args, "--gguf-local-path")?)
                }
                "--gguf-repo" => {
                    let repo = next_arg(&mut args, "--gguf-repo")?;
                    config.gguf_source = match config.gguf_source {
                        GgufSource::HuggingFaceGguf { file, revision, .. } => {
                            GgufSource::HuggingFaceGguf {
                                repo,
                                file,
                                revision,
                            }
                        }
                        GgufSource::LocalPath(_) => GgufSource::HuggingFaceGguf {
                            repo,
                            file: env_string("MVP_GGUF_FILE", DEFAULT_HF_FILE),
                            revision: env_optional("MVP_GGUF_REVISION"),
                        },
                    };
                }
                "--gguf-file" => {
                    let file = next_arg(&mut args, "--gguf-file")?;
                    config.gguf_source = match config.gguf_source {
                        GgufSource::HuggingFaceGguf { repo, revision, .. } => {
                            GgufSource::HuggingFaceGguf {
                                repo,
                                file,
                                revision,
                            }
                        }
                        GgufSource::LocalPath(_) => GgufSource::HuggingFaceGguf {
                            repo: env_string("MVP_GGUF_REPO", DEFAULT_HF_REPO),
                            file,
                            revision: env_optional("MVP_GGUF_REVISION"),
                        },
                    };
                }
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        Ok(config)
    }

    fn node_spec(
        &self,
        coordinator: EndpointAddr,
        datastream_sink: ActorAddress,
    ) -> Result<NodeProvisionSpec, String> {
        let mut env = vec![
            ("MVP_RUN_ID".to_owned(), self.run_id.to_string()),
            ("MVP_LOGICAL_NODE_ID".to_owned(), self.node_id.to_string()),
            ("MVP_STAGE_INDEX".to_owned(), self.stage_index.to_string()),
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
            ("MVP_MODEL_ID".to_owned(), self.model_id.clone()),
            ("MVP_NODE_MAX_RUNTIME_SECS".to_owned(), "0".to_owned()),
            ("MVP_DOCKER_GPUS".to_owned(), self.docker_gpus.clone()),
        ];
        env.extend(optional_env("MVP_TINYGRAD_TEST_MODE"));
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
        Ok(NodeProvisionSpec {
            run_id: self.run_id,
            node_id: self.node_id,
            stage_index: Some(self.stage_index),
            image: self.image.clone(),
            env,
            args: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct RuntimeReady {
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
}

struct PromptWork {
    request: SubmitPrompt,
    events: mpsc::Sender<PromptEvent>,
}

struct ActivePrompt {
    request: SubmitPrompt,
    events: mpsc::Sender<PromptEvent>,
    deadline: Instant,
}

struct OrchDatastream {
    stream: StreamId,
    next_position: u64,
}

impl OrchDatastream {
    fn new(run_id: u64) -> Self {
        Self {
            stream: StreamId::new(NodeId::new("mvp-orchestrator"), Lifetime(run_id)),
            next_position: 0,
        }
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

    fn emit_bytes(
        &mut self,
        dashboard: Option<&DashboardSupport>,
        channel: ChannelId,
        payload: Vec<u8>,
    ) {
        let frame = Frame::new(channel, Position(self.next_position), payload);
        self.next_position += 1;
        ingest_dashboard_frame(dashboard, &self.stream, &frame);
        eprintln!(
            "mvp-orch-one-node: datastream {} {}",
            frame.channel.as_str(),
            String::from_utf8_lossy(&frame.payload)
        );
    }
}

#[cfg(feature = "local-e2e")]
struct DashboardSupport {
    handle: dashboard::DashboardHandle,
}

#[cfg(feature = "local-e2e")]
impl DashboardSupport {
    fn start_from_env() -> Result<Option<Self>, String> {
        if !env_bool("MVP_DASHBOARD", false)? {
            return Ok(None);
        }
        let mut config = dashboard::DashboardConfig::default();
        if let Some(port) = env_optional("MVP_DASHBOARD_PORT") {
            config.port = port
                .parse::<u16>()
                .map_err(|e| format!("invalid MVP_DASHBOARD_PORT={port:?}: {e}"))?;
        }
        let url = format!("http://127.0.0.1:{}/view/datastream/live", config.port);
        let handle = dashboard::start_dashboard(config);
        handle.register_view(Arc::new(MvpClusterDashboardView::new()));
        handle.start_http_standalone();
        println!("{}", json!({"type":"dashboard_ready","url":url}));
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
    fn start_from_env() -> Result<Option<Self>, String> {
        if env_bool("MVP_DASHBOARD", false)? {
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
    default_timeout_ms: u64,
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
                        if let Err(error) = handle_prompt_connection(
                            stream,
                            tx,
                            default_max_tokens,
                            default_timeout_ms,
                        ) {
                            eprintln!("mvp-orch-one-node: prompt connection closed: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("mvp-orch-one-node: accept prompt RPC: {error}"),
            }
        }
    });
    Ok(addr)
}

fn handle_prompt_connection(
    stream: TcpStream,
    work_tx: mpsc::Sender<PromptWork>,
    default_max_tokens: u32,
    default_timeout_ms: u64,
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
        let request = request.with_defaults(default_max_tokens, default_timeout_ms);
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
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
) -> Result<RuntimeReady, String> {
    let start = Instant::now();
    loop {
        pump(driver, stack);
        drain_frames(frame_rx, dashboard);
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, &observation);
            match observation {
                PluginObservation::RuntimeReady {
                    endpoint,
                    node_actor,
                    ..
                } => {
                    return Ok(RuntimeReady {
                        endpoint,
                        node_actor,
                    });
                }
                PluginObservation::ProviderLine { line, .. }
                | PluginObservation::StdoutLine { line, .. }
                | PluginObservation::StderrLine { line, .. } => {
                    eprintln!("mvp-orch-one-node: node: {line}");
                }
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited { status, .. } => {
                    return Err(format!("node exited before ready: {status:?}"));
                }
            }
        }
        if start.elapsed() > BOOT_TIMEOUT {
            return Err("timed out waiting for node ready".to_owned());
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn wait_for_route(
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
    actor: ActorAddress,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() <= ROUTE_TIMEOUT {
        pump(driver, stack);
        if stack
            .route_view
            .read()
            .expect("route view poisoned")
            .contains_key(&actor)
        {
            return Ok(());
        }
        thread::sleep(PUMP_INTERVAL);
    }
    Err(format!(
        "timed out waiting for route to node actor {actor:?}"
    ))
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
    dashboard: Option<&DashboardSupport>,
    orch_datastream: &mut OrchDatastream,
) -> Result<(), String> {
    let start = Instant::now();
    loop {
        pump(driver, stack);
        while let Ok(observation) = obs_rx.try_recv() {
            emit_plugin_observation(orch_datastream, dashboard, &observation);
            match observation {
                PluginObservation::Failed { reason, .. } => return Err(reason),
                PluginObservation::Exited { status, .. } => {
                    return Err(format!("node exited while loading weights: {status:?}"));
                }
                PluginObservation::ProviderLine { line, .. }
                | PluginObservation::StdoutLine { line, .. }
                | PluginObservation::StderrLine { line, .. } => {
                    eprintln!("mvp-orch-one-node: node: {line}");
                }
                PluginObservation::RuntimeReady { .. } => {}
            }
        }
        while let Ok((stream, frame)) = frame_rx.try_recv() {
            ingest_dashboard_frame(dashboard, &stream, &frame);
            let payload = String::from_utf8_lossy(&frame.payload);
            eprintln!(
                "mvp-orch-one-node: datastream {} {}",
                frame.channel.as_str(),
                payload
            );
            if frame.channel == ChannelId::new("mvp.worker.weights")
                && json_type_is(&payload, "WeightsLoaded")
            {
                return Ok(());
            }
            if json_type_is(&payload, "WorkerFatal") {
                return Err(format!("worker fatal while loading weights: {payload}"));
            }
        }
        if start.elapsed() > WEIGHT_TIMEOUT {
            return Err("timed out waiting for weights loaded".to_owned());
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
    node_actor: ActorAddress,
    reply_to: ActorAddress,
) -> Result<(), String> {
    let mut active: Option<ActivePrompt> = None;
    loop {
        pump(driver, stack);
        drain_observations(obs_rx, dashboard, orch_datastream)?;
        drain_frames(frame_rx, dashboard);
        if stop_rx.try_recv().is_ok() {
            eprintln!("mvp-orch-one-node: stop requested");
            return Ok(());
        }

        if active.is_none() {
            if let Ok(work) = work_rx.try_recv() {
                let request = work.request;
                stack
                    .runtime
                    .send_to(
                        node_actor,
                        NodeAgentMsg::InferPrompt {
                            request_id: request.request_id,
                            prompt: request.prompt_text.clone(),
                            max_tokens: request.max_tokens,
                            reply_to,
                        },
                    )
                    .map_err(|e| format!("send prompt request: {e}"))?;
                active = Some(ActivePrompt {
                    deadline: Instant::now() + Duration::from_millis(request.timeout_ms),
                    request,
                    events: work.events,
                });
            }
        }

        while let Some(event) = prompt_events.try_recv() {
            if let Some(current) = active.as_ref() {
                if event.request_id() == current.request.request_id {
                    let terminal = event.is_terminal();
                    let _ = current.events.send(event);
                    if terminal {
                        active = None;
                    }
                }
            }
        }

        if let Some(current) = active.as_ref()
            && Instant::now() >= current.deadline
        {
            let _ = current.events.send(PromptEvent::Fault {
                request_id: current.request.request_id,
                error: "prompt timed out".to_owned(),
            });
            active = None;
        }

        thread::sleep(PUMP_INTERVAL);
    }
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
) -> Result<(), String> {
    while let Ok(observation) = obs_rx.try_recv() {
        emit_plugin_observation(orch_datastream, dashboard, &observation);
        match observation {
            PluginObservation::Failed { reason, .. } => return Err(reason),
            PluginObservation::Exited { status, .. } => {
                return Err(format!("node exited: {status:?}"));
            }
            PluginObservation::ProviderLine { line, .. }
            | PluginObservation::StdoutLine { line, .. }
            | PluginObservation::StderrLine { line, .. } => {
                eprintln!("mvp-orch-one-node: node: {line}");
            }
            PluginObservation::RuntimeReady { .. } => {}
        }
    }
    Ok(())
}

fn emit_plugin_observation(
    orch_datastream: &mut OrchDatastream,
    dashboard: Option<&DashboardSupport>,
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
        PluginObservation::RuntimeReady {
            run_id, node_id, ..
        } => orch_datastream.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: ProvisionEventKind::NodeLive,
                message: None,
            },
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
                message: Some(reason.clone()),
            },
        ),
    }
}

fn ingest_dashboard_frame(dashboard: Option<&DashboardSupport>, stream: &StreamId, frame: &Frame) {
    if let Some(dashboard) = dashboard {
        dashboard.ingest(stream, frame);
    }
}

fn drain_frames(
    frame_rx: &mpsc::Receiver<(StreamId, Frame)>,
    dashboard: Option<&DashboardSupport>,
) {
    while let Ok((stream, frame)) = frame_rx.try_recv() {
        ingest_dashboard_frame(dashboard, &stream, &frame);
        eprintln!(
            "mvp-orch-one-node: datastream {} {}",
            frame.channel.as_str(),
            String::from_utf8_lossy(&frame.payload)
        );
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

fn env_string(name: &str, default: &str) -> String {
    env_optional(name).unwrap_or_else(|| default.to_owned())
}

fn env_bool(name: &str, default: bool) -> Result<bool, String> {
    match env_optional(name) {
        None => Ok(default),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
            )),
        },
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match env_optional(name) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32, String> {
    match env_optional(name) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}")),
        None => Ok(default),
    }
}

fn gguf_source_from_env() -> GgufSource {
    if let Some(path) = env_optional("MVP_GGUF_LOCAL_PATH") {
        return GgufSource::LocalPath(path);
    }
    GgufSource::HuggingFaceGguf {
        repo: env_string("MVP_GGUF_REPO", DEFAULT_HF_REPO),
        file: env_string("MVP_GGUF_FILE", DEFAULT_HF_FILE),
        revision: env_optional("MVP_GGUF_REVISION"),
    }
}

fn tokenizer_from_env() -> TokenizerSource {
    env_optional("MVP_TOKENIZER_LOCAL_PATH")
        .map(TokenizerSource::LocalPath)
        .unwrap_or(TokenizerSource::EmbeddedGguf)
}

fn relay_mode_from_env() -> Result<iroh::RelayMode, String> {
    match env_string("MVP_IROH_RELAY_MODE", "disabled").as_str() {
        "disabled" => Ok(iroh::RelayMode::Disabled),
        "default" => Ok(iroh::RelayMode::Default),
        other => Err(format!(
            "unsupported MVP_IROH_RELAY_MODE={other:?}; use disabled or default"
        )),
    }
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
