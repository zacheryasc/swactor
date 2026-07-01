use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::{
    Arc, OnceLock,
    mpsc::{self, Receiver},
};
use std::thread;
use std::time::{Duration, Instant};

use datastream::emit::{ClusterFrameSink, DatastreamEmitter, EmitterConfig, FrameSink, NoopSink};
use datastream::{ChannelId, DATASTREAM_SINK_NAME};

use distribution::node::DistributedNodeConfig;
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire,
};
use mvp_system::actors::register_mvp_actor_codecs;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::prompt_rpc::PromptEvent;
use mvp_system::run_plan::{GgufSource, TokenizerSource};
use mvp_system::stage_controller as stage;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

const DEFAULT_WORKER_SCRIPT: &str = "/usr/local/share/mvp/tinygrad_worker.py";
const DEFAULT_DEVICE: &str = "CUDA";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const PUMP_INTERVAL: Duration = Duration::from_millis(10);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-node: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let config = DeploymentConfig::from_env()?;
    eprintln!(
        "mvp-node: boot run={} logical_node={} stage={} worker={} device={}",
        config.run_id,
        config.logical_node_id,
        config.stage_index,
        config.worker_script,
        config.device
    );

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
    if let Some(coordinator) = &config.coordinator_endpoint {
        eprintln!("mvp-node: joining coordinator {coordinator:?}");
        driver.join(std::slice::from_ref(coordinator));
    } else {
        eprintln!("mvp-node: no MVP_COORDINATOR_ENDPOINT set; running standalone until joined");
    }

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

    let mut datastream = node_datastream(&config, &stack);

    let reports = stack
        .runtime
        .new_inbox::<NodeAgentReport>()
        .map_err(|e| format!("node report inbox: {e}"))?;
    let orchestrator = config
        .orchestrator_actor
        .unwrap_or_else(ActorAddress::new_random);
    let node_actor = stack
        .runtime
        .spawn(NodeAgentActor::new(
            stage::NodeId(config.logical_node_id),
            orchestrator,
            Some(*reports.addr()),
        ))
        .map_err(|e| format!("spawn node agent: {e}"))?;
    stack.register_local_actor(driver.register_actor(node_actor, 1));

    let mut worker = TinygradWorker::spawn(&config)?;
    let mut initial_pump = || {};
    worker.initialize(&config.device, &mut datastream, &mut initial_pump)?;
    stack
        .runtime
        .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
        .map_err(|e| format!("mark initialized worker ready: {e}"))?;

    let ready = json!({
        "type":"ready",
        "role":"node",
        "endpoint": driver.endpoint_addr(),
        "node_actor": node_actor,
        "logical_node_id": config.logical_node_id,
        "stage_index": config.stage_index,
    });
    println!("{ready}");
    datastream.submit_text(ChannelId::new("mvp.node.ready"), ready.to_string());
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush ready line: {e}"))?;

    if let Some(prompt) = &config.self_test_prompt {
        run_self_test(&mut worker, &config, prompt, &mut datastream)?;
    }

    let shutdown_rx = spawn_stdin_shutdown_listener();
    let started = Instant::now();
    loop {
        pump_network(&mut driver, &stack);
        datastream.tick();
        while let Some(report) = reports.try_recv() {
            handle_node_report(
                report,
                &stack,
                &mut driver,
                node_actor,
                &mut worker,
                &mut datastream,
            )?;
        }
        if shutdown_rx.try_recv().is_ok() {
            eprintln!("mvp-node: shutdown requested on stdin");
            let mut pump = || pump_network(&mut driver, &stack);
            let _ = worker.shutdown(&mut datastream, &mut pump);
            return Ok(());
        }
        if let Some(status) = worker.try_wait()? {
            let _ = stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerCrashed);
            return Err(format!("tinygrad helper exited with {status}"));
        }
        if started.elapsed() > config.max_runtime && config.max_runtime != Duration::ZERO {
            eprintln!("mvp-node: max runtime elapsed; shutting down cleanly");
            let mut pump = || pump_network(&mut driver, &stack);
            let _ = worker.shutdown(&mut datastream, &mut pump);
            return Ok(());
        }
        thread::sleep(PUMP_INTERVAL);
    }
}

fn pump_network(driver: &mut IrohDriver, stack: &DistributionRuntimeStack) {
    stack.tick_protocol_actors(Instant::now());
    driver.pump_inbound_to_actors();
    stack.pump_runtime_once();
    driver.drain_outbox(&stack.outbox);
}

fn node_datastream(
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
) -> DatastreamEmitter {
    let mut sinks: Vec<Box<dyn FrameSink>> = Vec::new();
    if let Some(actor) = config.datastream_sink_actor {
        let sink_addr = Arc::new(OnceLock::new());
        let _ = sink_addr.set(actor);
        eprintln!("mvp-node: native datastream targeting {DATASTREAM_SINK_NAME} actor {actor:?}");
        sinks.push(Box::new(ClusterFrameSink::new(
            stack.runtime.clone(),
            sink_addr,
        )));
    }
    if let Some(path) = &config.datastream_frame_log {
        match JsonlFrameSink::open(path) {
            Ok(sink) => {
                eprintln!("mvp-node: native datastream frame log {path}");
                sinks.push(Box::new(sink));
            }
            Err(error) => {
                eprintln!("mvp-node: failed to open MVP_DATASTREAM_FRAME_LOG={path:?}: {error}");
            }
        }
    }
    if sinks.is_empty() {
        eprintln!("mvp-node: no datastream sink configured; native datastream drops locally");
        sinks.push(Box::new(NoopSink));
    }
    let sink: Box<dyn FrameSink> = if sinks.len() == 1 {
        sinks.pop().expect("one sink")
    } else {
        Box::new(TeeFrameSink { sinks })
    };
    DatastreamEmitter::new(
        EmitterConfig {
            node_hex: config.logical_node_id.to_string(),
            life: config.run_id,
            mux_capacity: 256,
        },
        sink,
    )
}

struct TeeFrameSink {
    sinks: Vec<Box<dyn FrameSink>>,
}

impl FrameSink for TeeFrameSink {
    fn ship(&mut self, stream: &datastream::StreamId, frame: &datastream::Frame) {
        for sink in &mut self.sinks {
            sink.ship(stream, frame);
        }
    }
}

struct JsonlFrameSink {
    file: File,
}

impl JsonlFrameSink {
    fn open(path: &str) -> std::io::Result<Self> {
        Ok(Self {
            file: OpenOptions::new().create(true).append(true).open(path)?,
        })
    }
}

impl FrameSink for JsonlFrameSink {
    fn ship(&mut self, stream: &datastream::StreamId, frame: &datastream::Frame) {
        let record = json!({
            "stream":stream.to_string(),
            "channel":frame.channel.as_str(),
            "position":frame.position.0,
            "payload":String::from_utf8_lossy(&frame.payload),
        });
        let _ = serde_json::to_writer(&mut self.file, &record);
        let _ = writeln!(self.file);
        let _ = self.file.flush();
    }
}

fn handle_node_report(
    report: NodeAgentReport,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    match report {
        NodeAgentReport::Command(command) => {
            handle_stage_command(command, stack, driver, node_actor, worker, datastream)
        }
        NodeAgentReport::Lifecycle(event) => {
            let record = json!({"type":"node_lifecycle","event":format!("{event:?}")});
            println!("{record}");
            datastream.submit_text(ChannelId::new("mvp.node.lifecycle"), record.to_string());
            std::io::stdout()
                .flush()
                .map_err(|e| format!("flush lifecycle: {e}"))
        }
        NodeAgentReport::PromptRequested {
            request_id,
            prompt,
            max_tokens,
            reply_to,
        } => handle_prompt_request(
            request_id, prompt, max_tokens, reply_to, stack, driver, worker, datastream,
        ),
        NodeAgentReport::Snapshot { .. } => Ok(()),
    }
}

fn handle_prompt_request(
    request_id: u64,
    prompt: String,
    max_tokens: u32,
    reply_to: ActorAddress,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    let started = Instant::now();
    let mut pump = || pump_network(driver, stack);
    match worker.infer_prompt(&prompt, max_tokens, datastream, &mut pump) {
        Ok(result) => {
            let text = result
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let tokens_generated = result
                .get("generated_tokens")
                .and_then(Value::as_array)
                .map_or(0, |tokens| tokens.len() as u32);
            if !text.is_empty() {
                stack
                    .runtime
                    .send_to(
                        reply_to,
                        PromptEvent::TextDelta {
                            request_id,
                            text: text.clone(),
                        },
                    )
                    .map_err(|e| format!("send prompt text delta: {e}"))?;
            }
            stack
                .runtime
                .send_to(
                    reply_to,
                    PromptEvent::Done {
                        request_id,
                        final_text: text,
                        tokens_generated,
                        elapsed_ms: result
                            .get("elapsed_ms")
                            .and_then(Value::as_u64)
                            .unwrap_or_else(|| started.elapsed().as_millis() as u64),
                    },
                )
                .map_err(|e| format!("send prompt done: {e}"))
        }
        Err(error) => stack
            .runtime
            .send_to(reply_to, PromptEvent::Fault { request_id, error })
            .map_err(|e| format!("send prompt fault: {e}")),
    }
}

fn handle_stage_command(
    command: StageCommandWire,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    match command {
        StageCommandWire::ConfigureWorkerRole {
            run_id,
            stage_index,
            layer_start,
            layer_end_exclusive,
        } => {
            let mut pump = || pump_network(driver, stack);
            worker.configure_role(
                run_id,
                stage_index,
                layer_start,
                layer_end_exclusive,
                datastream,
                &mut pump,
            )?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
                .map_err(|e| format!("mark worker ready: {e}"))
        }
        StageCommandWire::LoadWeights {
            model_id,
            gguf_source,
            tokenizer,
            layer_start,
            layer_end_exclusive,
        } => {
            let mut pump = || pump_network(driver, stack);
            worker.load_weights(
                model_id,
                gguf_source,
                tokenizer,
                layer_start,
                layer_end_exclusive,
                datastream,
                &mut pump,
            )?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkWeightsReady)
                .map_err(|e| format!("mark weights ready: {e}"))
        }
        StageCommandWire::StopLocalEdges { run_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::LocalEdgesStopped { run_id })
                .map_err(|e| format!("mark local edges stopped: {e}"))?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRingsQuiesced { run_id })
                .map_err(|e| format!("mark worker rings quiesced: {e}"))
        }
        StageCommandWire::ReleaseRunDeviceObjects { run_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::DeviceObjectsReleased { run_id })
                .map_err(|e| format!("mark device objects released: {e}"))?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRoleReset { run_id })
                .map_err(|e| format!("mark worker role reset: {e}"))
        }
        StageCommandWire::EstablishInboundEdge { edge_id } => stack
            .runtime
            .send_to(node_actor, NodeAgentMsg::MarkInboundEdgeReady { edge_id })
            .map_err(|e| format!("mark inbound edge ready: {e}")),
        StageCommandWire::EstablishOutboundEdge { edge_id } => stack
            .runtime
            .send_to(node_actor, NodeAgentMsg::MarkOutboundEdgeReady { edge_id })
            .map_err(|e| format!("mark outbound edge ready: {e}")),
        StageCommandWire::RewireEdge { .. } | StageCommandWire::ReleaseInputHandle { .. } => Ok(()),
        StageCommandWire::ExecuteStep { .. } => Err(format!(
            "mvp-node image path does not carry ring payload commands yet: {command:?}"
        )),
    }
}

fn run_self_test(
    worker: &mut TinygradWorker,
    config: &DeploymentConfig,
    prompt: &str,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    eprintln!("mvp-node: running self-test prompt");
    let mut pump = || {};
    worker.configure_role(
        config.run_id,
        config.stage_index,
        0,
        config.self_test_layer_end,
        datastream,
        &mut pump,
    )?;
    worker.load_weights(
        config.model_id.clone(),
        config.gguf_source.clone(),
        config.tokenizer.clone(),
        0,
        config.self_test_layer_end,
        datastream,
        &mut pump,
    )?;
    let result = worker.infer_prompt(prompt, config.self_test_max_tokens, datastream, &mut pump)?;
    let record = json!({
        "type":"self_test_completed",
        "prompt":prompt,
        "result":result,
    });
    println!("{record}");
    datastream.submit_text(ChannelId::new("mvp.node.self_test"), record.to_string());
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush self-test: {e}"))
}

#[derive(Clone)]
struct DeploymentConfig {
    run_id: u64,
    logical_node_id: u64,
    stage_index: u32,
    coordinator_endpoint: Option<EndpointAddr>,
    orchestrator_actor: Option<ActorAddress>,
    datastream_sink_actor: Option<ActorAddress>,
    datastream_frame_log: Option<String>,
    relay_mode: iroh::RelayMode,
    worker_script: String,
    device: String,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    self_test_prompt: Option<String>,
    self_test_layer_end: u32,
    self_test_max_tokens: u32,
    max_runtime: Duration,
}

impl DeploymentConfig {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            run_id: env_u64("MVP_RUN_ID", 1)?,
            logical_node_id: env_u64("MVP_LOGICAL_NODE_ID", 1)?,
            stage_index: env_u32("MVP_STAGE_INDEX", 0)?,
            coordinator_endpoint: env_json("MVP_COORDINATOR_ENDPOINT")?,
            orchestrator_actor: env_json("MVP_ORCHESTRATOR_ACTOR")?,
            datastream_sink_actor: env_json("MVP_DATASTREAM_SINK_ACTOR")?,
            datastream_frame_log: env_optional("MVP_DATASTREAM_FRAME_LOG"),
            relay_mode: relay_mode_from_env()?,
            worker_script: env_string("MVP_TINYGRAD_WORKER", DEFAULT_WORKER_SCRIPT),
            device: env_string("DEV", DEFAULT_DEVICE),
            model_id: env_string("MVP_MODEL_ID", DEFAULT_MODEL_ID),
            gguf_source: gguf_source_from_env(),
            tokenizer: tokenizer_from_env(),
            self_test_prompt: env_optional("MVP_NODE_SELF_TEST_PROMPT"),
            self_test_layer_end: env_u32("MVP_SELF_TEST_LAYER_END", 16)?,
            self_test_max_tokens: env_u32("MVP_SELF_TEST_MAX_TOKENS", 1)?,
            max_runtime: Duration::from_secs(env_u64("MVP_NODE_MAX_RUNTIME_SECS", 0)?),
        })
    }
}

struct TinygradWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl TinygradWorker {
    fn spawn(config: &DeploymentConfig) -> Result<Self, String> {
        let mut child = Command::new("python3")
            .arg(&config.worker_script)
            .env("DEV", &config.device)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn tinygrad helper {}: {e}", config.worker_script))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "tinygrad helper stdin missing".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "tinygrad helper stdout missing".to_owned())?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    fn initialize(
        &mut self,
        device: &str,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":device}}),
            "WorkerReady",
            datastream,
            ChannelId::new("mvp.worker.initialize"),
            pump,
        )
        .map(|_| ())
    }

    fn configure_role(
        &mut self,
        run_id: u64,
        stage_index: u32,
        layer_start: u32,
        layer_end_exclusive: u32,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({
                "type":"ConfigureRole",
                "role_id":stage_index + 1,
                "config":{
                    "run_id":run_id,
                    "stage_index":stage_index,
                    "layer_start":layer_start,
                    "layer_end_exclusive":layer_end_exclusive,
                }
            }),
            "RoleConfigured",
            datastream,
            ChannelId::new("mvp.worker.role"),
            pump,
        )
        .map(|_| ())
    }

    fn load_weights(
        &mut self,
        model_id: String,
        gguf_source: GgufSource,
        tokenizer: TokenizerSource,
        layer_start: u32,
        layer_end_exclusive: u32,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({
                "type":"LoadWeights",
                "model_id":model_id,
                "gguf_source":gguf_source,
                "tokenizer":tokenizer,
                "layer_start":layer_start,
                "layer_end_exclusive":layer_end_exclusive,
            }),
            "WeightsLoaded",
            datastream,
            ChannelId::new("mvp.worker.weights"),
            pump,
        )
        .map(|_| ())
    }

    fn infer_prompt(
        &mut self,
        prompt: &str,
        max_tokens: u32,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        self.command(
            json!({"type":"InferPrompt","prompt":prompt,"max_tokens":max_tokens}),
            "PromptCompleted",
            datastream,
            ChannelId::new("mvp.worker.prompt"),
            pump,
        )
    }

    fn shutdown(
        &mut self,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ShutdownWorker"}),
            "WorkerStopped",
            datastream,
            ChannelId::new("mvp.worker.shutdown"),
            pump,
        )
        .map(|_| ())
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        self.child
            .try_wait()
            .map_err(|e| format!("poll tinygrad helper: {e}"))
    }

    fn command(
        &mut self,
        command: Value,
        expected: &str,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        writeln!(self.stdin, "{command}").map_err(|e| format!("write helper command: {e}"))?;
        self.stdin
            .flush()
            .map_err(|e| format!("flush helper command: {e}"))?;
        self.expect_event(expected, datastream, channel, pump)
    }

    fn expect_event(
        &mut self,
        expected: &str,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        loop {
            let mut line = String::new();
            let n = self
                .stdout
                .read_line(&mut line)
                .map_err(|e| format!("read helper stdout: {e}"))?;
            if n == 0 {
                return Err(format!(
                    "tinygrad helper stdout closed while waiting for {expected}"
                ));
            }
            let value: Value = serde_json::from_str(&line)
                .map_err(|e| format!("parse helper stdout {line:?}: {e}"))?;
            datastream.submit_text(channel.clone(), value.to_string());
            datastream.tick();
            pump();
            if value.get("type").and_then(Value::as_str) == Some(expected) {
                return Ok(value);
            }
            println!("{}", json!({"type":"tinygrad_event","event":value}));
            std::io::stdout()
                .flush()
                .map_err(|e| format!("flush helper passthrough: {e}"))?;
        }
    }
}

impl Drop for TinygradWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_stdin_shutdown_listener() -> Receiver<()> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines().map_while(Result::ok) {
            if line.trim().eq_ignore_ascii_case("shutdown") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx
}

fn env_string(name: &str, default: &str) -> String {
    env_optional(name).unwrap_or_else(|| default.to_owned())
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
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

fn env_json<T>(name: &str) -> Result<Option<T>, String>
where
    T: serde::de::DeserializeOwned,
{
    env_optional(name)
        .map(|value| serde_json::from_str(&value).map_err(|e| format!("invalid {name} JSON: {e}")))
        .transpose()
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
