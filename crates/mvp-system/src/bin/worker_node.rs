use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::{
    Arc, OnceLock,
    mpsc::{self, Receiver},
};
use std::thread;
use std::time::{Duration, Instant};

use datastream::ChannelId;
use datastream::emit::{
    ClusterFrameSink, DatastreamEmitter, DatastreamEventSink, EmitterConfig, FrameSink, NoopSink,
};

use distribution::node::DistributedNodeConfig;
use iroh::EndpointAddr;
use iroh_driver::{IrohDriver, IrohDriverConfig};
use mvp_system::actors::node_agent::{
    NodeAgentActor, NodeAgentMsg, NodeAgentReport, StageCommandWire,
};
use mvp_system::actors::register_mvp_actor_codecs;
use mvp_system::arena_manager as arena;
use mvp_system::distribution_stack::DistributionRuntimeStack;
use mvp_system::prompt_rpc::PromptEvent;
use mvp_system::relay_provisioning::relay_runtime_config_from_env;
use mvp_system::run_plan::{GgufSource, TokenizerSource};
use mvp_system::stage_controller as stage;
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::ActorAddress;

const DEFAULT_WORKER_SCRIPT: &str = "/usr/local/share/mvp/tinygrad_worker.py";
const DEFAULT_DEVICE: &str = "CUDA";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_ARENA_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_ARENA_ALIGNMENT: u64 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const NODE_BOOTSTRAP_CHANNEL: &str = "mvp.node.bootstrap";
const NODE_RUNTIME_CHANNEL: &str = "mvp.node.runtime";
const NODE_STAGE_CHANNEL: &str = "mvp.node.stage";
const NODE_WORKER_CHANNEL: &str = "mvp.node.worker";
const NODE_PROMPT_CHANNEL: &str = "mvp.node.prompt";
const NODE_SHUTDOWN_CHANNEL: &str = "mvp.node.shutdown";

fn node_event_payload(
    config: &DeploymentConfig,
    phase: &str,
    status: &str,
    detail: Value,
) -> Value {
    json!({
        "type":"NodeEvent",
        "phase":phase,
        "status":status,
        "run_id":config.run_id,
        "node_id":config.logical_node_id,
        "stage_index":config.stage_index,
        "detail":detail,
    })
}

fn emit_stdio_node_event(
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) -> Result<(), String> {
    println!(
        "{}",
        json!({
            "mvp_stdio_event":1,
            "kind":"datastream_frame",
            "channel":channel,
            "payload":node_event_payload(config, phase, status, detail),
        })
    );
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush stdio node event: {e}"))
}

fn emit_node_event(
    datastream: &mut DatastreamEmitter,
    config: &DeploymentConfig,
    channel: &str,
    phase: &str,
    status: &str,
    detail: Value,
) {
    datastream.submit_text(
        ChannelId::new(channel),
        node_event_payload(config, phase, status, detail).to_string(),
    );
    datastream.tick();
}

fn spawn_host_gpu_sampler(handle: tokio::runtime::Handle, sink: DatastreamEventSink) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval = tokio::time::interval(datastream::hardware::gpu::GPU_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample_seq = seq;
            let sample = match tokio::task::spawn_blocking(move || {
                datastream::hardware::gpu::sample(sample_seq)
            })
            .await
            {
                Ok(sample) => sample,
                Err(error) => datastream::hardware::gpu::HostGpuSample::error(
                    sample_seq,
                    format!("gpu sampler task failed: {error}"),
                ),
            };

            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn spawn_host_cpu_sampler(
    handle: tokio::runtime::Handle,
    sink: DatastreamEventSink,
    watched_pids: Vec<u32>,
) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut sampler = datastream::hardware::cpu::CpuSampler::new(watched_pids);
        let mut interval = tokio::time::interval(datastream::hardware::cpu::CPU_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample = sampler.sample(seq);
            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}
fn spawn_host_net_sampler(handle: tokio::runtime::Handle, sink: DatastreamEventSink) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval =
            tokio::time::interval(datastream::hardware::net::HOST_NET_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample_seq = seq;
            let sample = match tokio::task::spawn_blocking(move || {
                datastream::hardware::net::sample(sample_seq)
            })
            .await
            {
                Ok(sample) => sample,
                Err(error) => datastream::hardware::net::HostNetSample::error(
                    sample_seq,
                    format!("network sampler task failed: {error}"),
                ),
            };

            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn spawn_arena_sampler(
    handle: tokio::runtime::Handle,
    sink: DatastreamEventSink,
    arena_manager: Arc<Mutex<arena::ArenaManager>>,
) {
    handle.spawn(async move {
        let mut seq = 0_u64;
        let mut interval = tokio::time::interval(arena::ARENA_SAMPLE_INTERVAL);

        loop {
            interval.tick().await;

            let sample = arena_manager.lock().sample(seq);
            seq = seq.saturating_add(1);
            sink.submit_record(&sample);
        }
    });
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mvp-worker-node: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), String> {
    let config = DeploymentConfig::from_env()?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "config",
        "ready",
        json!({
            "worker_script":&config.worker_script,
            "device":&config.device,
            "model_id":&config.model_id,
            "has_coordinator_endpoint":config.coordinator_endpoint.is_some(),
            "has_orchestrator_actor":config.orchestrator_actor.is_some(),
            "has_datastream_sink_actor":config.datastream_sink_actor.is_some(),
            "self_test_enabled":config.self_test_prompt.is_some(),
            "arena_bytes":config.arena_bytes,
            "arena_alignment":config.arena_alignment,
        }),
    )?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "process",
        "started",
        json!({"binary":"mvp-worker-node","pid":std::process::id()}),
    )?;

    let tokio = match tokio::runtime::Runtime::new() {
        Ok(runtime) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "tokio_runtime",
                "ready",
                json!({"runtime":"tokio"}),
            )?;
            runtime
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "tokio_runtime",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("tokio runtime: {error}"));
        }
    };
    let mut driver = match IrohDriver::with_handle(
        tokio.handle().clone(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: config.relay_mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    ) {
        Ok(driver) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "iroh_driver",
                "ready",
                json!({"endpoint":driver.endpoint_addr(),"relay_mode":format!("{:?}", config.relay_mode)}),
            )?;
            driver
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "iroh_driver",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("create iroh driver: {error}"));
        }
    };
    if let Some(coordinator) = &config.coordinator_endpoint {
        driver.join(std::slice::from_ref(coordinator));
        emit_stdio_node_event(
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            "coordinator_join",
            "started",
            json!({"endpoint":coordinator}),
        )?;
    } else {
        emit_stdio_node_event(
            &config,
            NODE_BOOTSTRAP_CHANNEL,
            "coordinator_join",
            "skipped",
            json!({"reason":"MVP_COORDINATOR_ENDPOINT not set","mode":"standalone"}),
        )?;
    }

    let stack = DistributionRuntimeStack::new_with_codecs(
        driver.node_id(),
        DistributedNodeConfig::default(),
        |registry| {
            register_mvp_actor_codecs(registry);
            datastream::wire::register_datastream_codec(registry);
        },
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "distribution_stack",
        "ready",
        json!({"actors":"initialized","route_view":"initialized","swim":"initialized","outbox":"initialized"}),
    )?;
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "codecs",
        "ready",
        json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","datastream"]}),
    )?;
    driver.enable_actor_bridge(
        stack.runtime.clone(),
        stack.codec.clone(),
        stack.actor_bridge_routes(),
        stack.actors.swim,
        stack.relay_mirror.clone(),
        stack.route_view.clone(),
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "actor_bridge",
        "ready",
        json!({"transport":"iroh","routes":"attached"}),
    )?;

    let arena_manager = match arena::ArenaManager::boot(arena::ArenaConfig {
        node_id: arena::NodeId(config.logical_node_id),
        reservation_ceiling: config.arena_bytes,
        base_alignment: config.arena_alignment,
    }) {
        Ok(manager) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "arena_manager",
                "ready",
                json!({
                    "arena_bytes":config.arena_bytes,
                    "arena_alignment":config.arena_alignment,
                }),
            )?;
            Arc::new(Mutex::new(manager))
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "arena_manager",
                "failed",
                json!({"error":format!("{error:?}")}),
            )?;
            return Err(format!("boot arena manager: {error:?}"));
        }
    };

    let mut datastream = node_datastream(&config, &stack);
    spawn_host_gpu_sampler(tokio.handle().clone(), datastream.event_sink());
    spawn_host_net_sampler(tokio.handle().clone(), datastream.event_sink());
    spawn_arena_sampler(
        tokio.handle().clone(),
        datastream.event_sink(),
        Arc::clone(&arena_manager),
    );
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "datastream_emitter",
        "ready",
        config.datastream_sink_detail(),
    )?;

    let reports = match stack.runtime.new_inbox::<NodeAgentReport>() {
        Ok(inbox) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_report_inbox",
                "ready",
                json!({"actor":inbox.addr()}),
            )?;
            inbox
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_report_inbox",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("node report inbox: {error}"));
        }
    };
    let (orchestrator, orchestrator_source) = match config.orchestrator_actor {
        Some(actor) => (actor, "env"),
        None => (ActorAddress::new_random(), "generated_fallback"),
    };
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "orchestrator_actor",
        "ready",
        json!({"actor":orchestrator,"source":orchestrator_source}),
    )?;
    let node_agent = NodeAgentActor::new(
        stage::NodeId(config.logical_node_id),
        orchestrator,
        Some(*reports.addr()),
    );
    let node_actor = match stack.runtime.spawn(node_agent) {
        Ok(actor) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_agent",
                "ready",
                json!({"node_actor":actor,"source":"generated"}),
            )?;
            actor
        }
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_BOOTSTRAP_CHANNEL,
                "node_agent",
                "failed",
                json!({"error":error.to_string(),"source":"generated"}),
            )?;
            return Err(format!("spawn node agent: {error}"));
        }
    };
    stack.register_local_actor(driver.register_actor(node_actor, 1));
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "node_actor_registration",
        "ready",
        json!({"node_actor":node_actor,"network_reachable":true}),
    )?;

    emit_stdio_node_event(
        &config,
        NODE_WORKER_CHANNEL,
        "worker_process",
        "started",
        json!({
            "program":"python3",
            "script":&config.worker_script,
            "device":&config.device,
            "stdin":"piped",
            "stdout":"piped",
            "stderr":"piped",
        }),
    )?;
    let mut worker = match TinygradWorker::spawn(&config) {
        Ok(worker) => worker,
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_WORKER_CHANNEL,
                "worker_process",
                "failed",
                json!({"error":error}),
            )?;
            return Err(error);
        }
    };
    spawn_host_cpu_sampler(
        tokio.handle().clone(),
        datastream.event_sink(),
        vec![std::process::id(), worker.pid()],
    );
    emit_stdio_node_event(
        &config,
        NODE_WORKER_CHANNEL,
        "worker_initialize",
        "started",
        json!({"command":"InitializeWorker","helper_abi_version":1,"device":&config.device}),
    )?;
    let mut initial_pump = || {};
    match worker.initialize(&config.device, &config, &mut datastream, &mut initial_pump) {
        Ok(()) => emit_stdio_node_event(
            &config,
            NODE_WORKER_CHANNEL,
            "worker_initialize",
            "ready",
            json!({"worker_event_type":"WorkerReady"}),
        )?,
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_WORKER_CHANNEL,
                "worker_initialize",
                "failed",
                json!({"error":error}),
            )?;
            return Err(error);
        }
    }
    match stack
        .runtime
        .send_to(node_actor, NodeAgentMsg::MarkWorkerReady)
    {
        Ok(()) => emit_stdio_node_event(
            &config,
            NODE_RUNTIME_CHANNEL,
            "mark_worker_ready",
            "ready",
            json!({"sent":"NodeAgentMsg::MarkWorkerReady","node_actor":node_actor}),
        )?,
        Err(error) => {
            emit_stdio_node_event(
                &config,
                NODE_RUNTIME_CHANNEL,
                "mark_worker_ready",
                "failed",
                json!({"error":error.to_string()}),
            )?;
            return Err(format!("mark initialized worker ready: {error}"));
        }
    }

    let ready = json!({
        "type":"ready",
        "role":"node",
        "endpoint": driver.endpoint_addr(),
        "node_actor": node_actor,
        "logical_node_id": config.logical_node_id,
        "stage_index": config.stage_index,
    });
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "runtime_ready",
        "ready",
        json!({
            "endpoint":driver.endpoint_addr(),
            "node_actor":node_actor,
            "logical_node_id":config.logical_node_id,
            "stage_index":config.stage_index,
            "stdio_ready_line_emitted":true,
        }),
    )?;
    println!("{ready}");
    datastream.submit_text(ChannelId::new("mvp.node.ready"), ready.to_string());
    emit_stdio_node_event(
        &config,
        NODE_BOOTSTRAP_CHANNEL,
        "datastream_handoff",
        "ready",
        json!({"from":"stdio_envelope","to":"cluster_datastream","channel":NODE_BOOTSTRAP_CHANNEL}),
    )?;
    std::io::stdout()
        .flush()
        .map_err(|e| format!("flush ready line: {e}"))?;

    if let Some(prompt) = &config.self_test_prompt {
        run_self_test(&mut worker, &config, prompt, &mut datastream, &mut driver, &stack)?;
    }

    let shutdown_rx = spawn_stdin_shutdown_listener();
    emit_node_event(
        &mut datastream,
        &config,
        NODE_RUNTIME_CHANNEL,
        "stdin_shutdown_listener",
        "ready",
        json!({"command":"shutdown"}),
    );
    emit_node_event(
        &mut datastream,
        &config,
        NODE_RUNTIME_CHANNEL,
        "main_loop",
        "started",
        json!({
            "poll_interval_ms":PUMP_INTERVAL.as_millis(),
            "checks":["network","datastream","node_reports","stdin_shutdown","worker_health"],
        }),
    );
    loop {
        pump_network(&mut driver, &stack);
        datastream.tick();
        worker.drain_stderr(&config, &mut datastream);
        while let Some(report) = reports.try_recv() {
            handle_node_report(
                report,
                &config,
                &stack,
                &mut driver,
                node_actor,
                &mut worker,
                &mut datastream,
            )?;
        }
        if shutdown_rx.try_recv().is_ok() {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_SHUTDOWN_CHANNEL,
                "shutdown",
                "started",
                json!({"source":"stdin","command":"shutdown"}),
            );
            let mut pump = || pump_network(&mut driver, &stack);
            match worker.shutdown(&config, &mut datastream, &mut pump) {
                Ok(()) => {
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_SHUTDOWN_CHANNEL,
                        "worker_shutdown",
                        "ready",
                        json!({"worker_event_type":"WorkerStopped"}),
                    );
                    emit_node_event(
                        &mut datastream,
                        &config,
                        NODE_SHUTDOWN_CHANNEL,
                        "node_exit",
                        "ready",
                        json!({"result":"ok"}),
                    );
                }
                Err(error) => emit_node_event(
                    &mut datastream,
                    &config,
                    NODE_SHUTDOWN_CHANNEL,
                    "worker_shutdown",
                    "failed",
                    json!({"error":error}),
                ),
            }
            return Ok(());
        }
        if let Some(status) = worker.try_wait()? {
            emit_node_event(
                &mut datastream,
                &config,
                NODE_SHUTDOWN_CHANNEL,
                "worker_process",
                "failed",
                json!({"exit_status":status.to_string()}),
            );
            let _ = stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerCrashed);
            return Err(format!("tinygrad helper exited with {status}"));
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
        sinks.push(Box::new(ClusterFrameSink::new(
            stack.runtime.clone(),
            sink_addr,
        )));
    }
    if let Some(path) = &config.datastream_frame_log {
        match JsonlFrameSink::open(path) {
            Ok(sink) => {
                sinks.push(Box::new(sink));
            }
            Err(_error) => {}
        }
    }
    if sinks.is_empty() {
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
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    node_actor: ActorAddress,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    let kind = match &report {
        NodeAgentReport::Command(_) => "Command",
        NodeAgentReport::Lifecycle(_) => "Lifecycle",
        NodeAgentReport::PromptRequested { .. } => "PromptRequested",
        NodeAgentReport::Snapshot { .. } => "Snapshot",
    };
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "node_report",
        "observed",
        json!({"kind":kind}),
    );
    match report {
        NodeAgentReport::Command(command) => handle_stage_command(
            command, config, stack, driver, node_actor, worker, datastream,
        ),
        NodeAgentReport::Lifecycle(event) => {
            let event = format!("{event:?}");
            datastream.submit_text(
                ChannelId::new("mvp.node.lifecycle"),
                json!({"type":"node_lifecycle","event":event}).to_string(),
            );
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "lifecycle",
                "observed",
                json!({"event":event}),
            );
            Ok(())
        }
        NodeAgentReport::PromptRequested {
            request_id,
            prompt,
            max_tokens,
            reply_to,
        } => handle_prompt_request(
            request_id, prompt, max_tokens, reply_to, config, stack, driver, worker, datastream,
        ),
        NodeAgentReport::Snapshot { .. } => Ok(()),
    }
}

fn handle_prompt_request(
    request_id: u64,
    prompt: String,
    max_tokens: u32,
    reply_to: ActorAddress,
    config: &DeploymentConfig,
    stack: &DistributionRuntimeStack,
    driver: &mut IrohDriver,
    worker: &mut TinygradWorker,
    datastream: &mut DatastreamEmitter,
) -> Result<(), String> {
    let started = Instant::now();
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "prompt_requested",
        "started",
        json!({"request_id":request_id,"max_tokens":max_tokens,"reply_to":reply_to,"prompt_bytes":prompt.len()}),
    );
    emit_node_event(
        datastream,
        config,
        NODE_PROMPT_CHANNEL,
        "infer_prompt",
        "started",
        json!({"request_id":request_id,"command":"InferPrompt","max_tokens":max_tokens}),
    );
    let mut pump = || pump_network(driver, stack);
    match worker.infer_prompt(
        request_id, &prompt, max_tokens, config, datastream, &mut pump,
    ) {
        Ok(result) => {
            let text = result
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let text_bytes = text.len();
            let worker_result_payload_bytes = result.to_string().len();
            let prompt_tokens = result
                .get("prompt_tokens")
                .and_then(Value::as_array)
                .map_or(0, |tokens| tokens.len() as u32);
            let tokens_generated = result
                .get("generated_tokens")
                .and_then(Value::as_array)
                .map_or(0, |tokens| tokens.len() as u32);
            let elapsed_ms = result
                .get("elapsed_ms")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| started.elapsed().as_millis() as u64);
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "infer_prompt",
                "ready",
                json!({"request_id":request_id,"worker_event_type":"PromptCompleted","prompt_tokens":prompt_tokens,"tokens_generated":tokens_generated,"elapsed_ms":elapsed_ms,"text_bytes":text_bytes,"worker_result_payload_bytes":worker_result_payload_bytes}),
            );
            if !text.is_empty() {
                match stack.runtime.send_to(
                    reply_to,
                    PromptEvent::TextDelta {
                        request_id,
                        text: text.clone(),
                    },
                ) {
                    Ok(()) => emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"TextDelta","bytes":text_bytes,"reply_to":reply_to}),
                    ),
                    Err(error) => {
                        emit_node_event(
                            datastream,
                            config,
                            NODE_PROMPT_CHANNEL,
                            "prompt_response",
                            "failed",
                            json!({"request_id":request_id,"event":"TextDelta","error":error.to_string()}),
                        );
                        return Err(format!("send prompt text delta: {error}"));
                    }
                }
            }
            match stack.runtime.send_to(
                reply_to,
                PromptEvent::Done {
                    request_id,
                    final_text: text,
                    tokens_generated,
                    elapsed_ms,
                },
            ) {
                Ok(()) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Done","tokens_generated":tokens_generated,"elapsed_ms":elapsed_ms,"final_text_bytes":text_bytes,"reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "failed",
                        json!({"request_id":request_id,"event":"Done","error":error.to_string()}),
                    );
                    Err(format!("send prompt done: {error}"))
                }
            }
        }
        Err(error) => {
            emit_node_event(
                datastream,
                config,
                NODE_PROMPT_CHANNEL,
                "infer_prompt",
                "failed",
                json!({"request_id":request_id,"error":error}),
            );
            match stack.runtime.send_to(
                reply_to,
                PromptEvent::Fault {
                    request_id,
                    error: error.clone(),
                },
            ) {
                Ok(()) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "ready",
                        json!({"request_id":request_id,"event":"Fault","reply_to":reply_to}),
                    );
                    Ok(())
                }
                Err(send_error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_PROMPT_CHANNEL,
                        "prompt_response",
                        "failed",
                        json!({"request_id":request_id,"event":"Fault","error":send_error.to_string()}),
                    );
                    Err(format!("send prompt fault: {send_error}"))
                }
            }
        }
    }
}

fn handle_stage_command(
    command: StageCommandWire,
    config: &DeploymentConfig,
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
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "configure_worker_role",
                "started",
                json!({"run_id":run_id,"stage_index":stage_index,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            let mut pump = || pump_network(driver, stack);
            match worker.configure_role(
                run_id,
                stage_index,
                layer_start,
                layer_end_exclusive,
                config,
                datastream,
                &mut pump,
            ) {
                Ok(()) => emit_node_event(
                    datastream,
                    config,
                    NODE_STAGE_CHANNEL,
                    "configure_worker_role",
                    "ready",
                    json!({"worker_event_type":"RoleConfigured"}),
                ),
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "configure_worker_role",
                        "failed",
                        json!({"error":error}),
                    );
                    return Err(error);
                }
            }
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
            let gguf_source_kind = match &gguf_source {
                GgufSource::LocalPath(_) => "local_path",
                GgufSource::HuggingFaceGguf { .. } => "huggingface",
            };
            let tokenizer_kind = match &tokenizer {
                TokenizerSource::EmbeddedGguf => "gguf",
                TokenizerSource::LocalPath(_) => "local_path",
            };
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "load_weights",
                "started",
                json!({"model_id":&model_id,"gguf_source":gguf_source_kind,"tokenizer":tokenizer_kind,"layer_range":{"start":layer_start,"end_exclusive":layer_end_exclusive}}),
            );
            let mut pump = || pump_network(driver, stack);
            match worker.load_weights(
                model_id.clone(),
                gguf_source,
                tokenizer,
                layer_start,
                layer_end_exclusive,
                config,
                datastream,
                &mut pump,
            ) {
                Ok(()) => emit_node_event(
                    datastream,
                    config,
                    NODE_STAGE_CHANNEL,
                    "load_weights",
                    "ready",
                    json!({"worker_event_type":"WeightsLoaded","model_id":model_id}),
                ),
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_STAGE_CHANNEL,
                        "load_weights",
                        "failed",
                        json!({"error":error}),
                    );
                    return Err(error);
                }
            }
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
                .map_err(|e| format!("mark worker rings quiesced: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "local_edges",
                "ready",
                json!({"run_id":run_id,"sent":["LocalEdgesStopped","WorkerRingsQuiesced"]}),
            );
            Ok(())
        }
        StageCommandWire::ReleaseRunDeviceObjects { run_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::DeviceObjectsReleased { run_id })
                .map_err(|e| format!("mark device objects released: {e}"))?;
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::WorkerRoleReset { run_id })
                .map_err(|e| format!("mark worker role reset: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "device_objects",
                "ready",
                json!({"run_id":run_id,"sent":["DeviceObjectsReleased","WorkerRoleReset"]}),
            );
            Ok(())
        }
        StageCommandWire::EstablishInboundEdge { edge_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkInboundEdgeReady { edge_id })
                .map_err(|e| format!("mark inbound edge ready: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "inbound_edge",
                "ready",
                json!({"edge_id":format!("{edge_id:?}")}),
            );
            Ok(())
        }
        StageCommandWire::EstablishOutboundEdge { edge_id } => {
            stack
                .runtime
                .send_to(node_actor, NodeAgentMsg::MarkOutboundEdgeReady { edge_id })
                .map_err(|e| format!("mark outbound edge ready: {e}"))?;
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "outbound_edge",
                "ready",
                json!({"edge_id":format!("{edge_id:?}")}),
            );
            Ok(())
        }
        StageCommandWire::RewireEdge { .. } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "rewire_edge",
                "skipped",
                json!({"reason":"not implemented in mvp-worker-node image path"}),
            );
            Ok(())
        }
        StageCommandWire::ReleaseInputHandle { .. } => {
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "release_input_handle",
                "skipped",
                json!({"reason":"not implemented in mvp-worker-node image path"}),
            );
            Ok(())
        }
        StageCommandWire::ExecuteStep { .. } => {
            let error = "mvp-worker-node image path does not carry ring payload commands yet";
            emit_node_event(
                datastream,
                config,
                NODE_STAGE_CHANNEL,
                "execute_step",
                "failed",
                json!({"error":error}),
            );
            Err(format!("{error}: {command:?}"))
        }
    }
}

fn run_self_test(
    worker: &mut TinygradWorker,
    config: &DeploymentConfig,
    prompt: &str,
    datastream: &mut DatastreamEmitter,
    driver: &mut IrohDriver,
    stack: &DistributionRuntimeStack,
) -> Result<(), String> {
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "started",
        json!({"prompt_bytes":prompt.len()}),
    );
    let mut pump = || pump_network(driver, stack);
    worker.configure_role(
        config.run_id,
        config.stage_index,
        0,
        config.self_test_layer_end,
        config,
        datastream,
        &mut pump,
    )?;
    worker.load_weights(
        config.model_id.clone(),
        config.gguf_source.clone(),
        config.tokenizer.clone(),
        0,
        config.self_test_layer_end,
        config,
        datastream,
        &mut pump,
    )?;
    let result = worker.infer_prompt(
        0,
        prompt,
        config.self_test_max_tokens,
        config,
        datastream,
        &mut pump,
    )?;
    let record = json!({"type":"self_test_completed","prompt_bytes":prompt.len(),"result":result});
    datastream.submit_text(ChannelId::new("mvp.node.self_test"), record.to_string());
    emit_node_event(
        datastream,
        config,
        NODE_RUNTIME_CHANNEL,
        "self_test",
        "ready",
        json!({"prompt_bytes":prompt.len()}),
    );
    Ok(())
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
    arena_bytes: u64,
    arena_alignment: u64,
}

impl DeploymentConfig {
    fn from_env() -> Result<Self, String> {
        let run_id = env_u64("MVP_RUN_ID", 1)?;
        let relay = relay_runtime_config_from_env(run_id)?;
        Ok(Self {
            run_id,
            logical_node_id: env_u64("MVP_LOGICAL_NODE_ID", 1)?,
            stage_index: env_u32("MVP_STAGE_INDEX", 0)?,
            coordinator_endpoint: env_json("MVP_COORDINATOR_ENDPOINT")?,
            orchestrator_actor: env_json("MVP_ORCHESTRATOR_ACTOR")?,
            datastream_sink_actor: env_json("MVP_DATASTREAM_SINK_ACTOR")?,
            datastream_frame_log: env_optional("MVP_DATASTREAM_FRAME_LOG"),
            relay_mode: relay.mode,
            worker_script: env_string("MVP_TINYGRAD_WORKER", DEFAULT_WORKER_SCRIPT),
            device: env_string("DEV", DEFAULT_DEVICE),
            model_id: env_string("MVP_MODEL_ID", DEFAULT_MODEL_ID),
            gguf_source: gguf_source_from_env(),
            tokenizer: tokenizer_from_env(),
            self_test_prompt: env_optional("MVP_NODE_SELF_TEST_PROMPT"),
            self_test_layer_end: env_u32("MVP_SELF_TEST_LAYER_END", 16)?,
            self_test_max_tokens: env_u32("MVP_SELF_TEST_MAX_TOKENS", 1)?,
            arena_bytes: env_u64("MVP_ARENA_BYTES", DEFAULT_ARENA_BYTES)?,
            arena_alignment: env_u64("MVP_ARENA_ALIGNMENT", DEFAULT_ARENA_ALIGNMENT)?,
        })
    }

    fn datastream_sink_detail(&self) -> Value {
        let sink = match (
            self.datastream_sink_actor.is_some(),
            self.datastream_frame_log.is_some(),
        ) {
            (true, true) => "tee",
            (true, false) => "cluster_actor",
            (false, true) => "frame_log",
            (false, false) => "noop",
        };
        json!({
            "sink":sink,
            "cluster_actor":self.datastream_sink_actor,
            "frame_log":self.datastream_frame_log,
        })
    }
}

struct TinygradWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_rx: Receiver<String>,
}

impl TinygradWorker {
    fn spawn(config: &DeploymentConfig) -> Result<Self, String> {
        let mut child = Command::new("python3")
            .arg(&config.worker_script)
            .env("DEV", &config.device)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "tinygrad helper stderr missing".to_owned())?;
        let (stderr_tx, stderr_rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if stderr_tx.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_rx,
        })
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn initialize(
        &mut self,
        device: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"InitializeWorker","helper_abi_version":1,"backend":{"device":device}}),
            "WorkerReady",
            config,
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
        config: &DeploymentConfig,
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
            config,
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
        config: &DeploymentConfig,
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
            config,
            datastream,
            ChannelId::new("mvp.worker.weights"),
            pump,
        )
        .map(|_| ())
    }

    fn infer_prompt(
        &mut self,
        request_id: u64,
        prompt: &str,
        max_tokens: u32,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        self.command(
            json!({"type":"InferPrompt","request_id":request_id,"prompt":prompt,"max_tokens":max_tokens}),
            "PromptCompleted",
            config,
            datastream,
            ChannelId::new("mvp.worker.prompt"),
            pump,
        )
    }

    fn shutdown(
        &mut self,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        pump: &mut dyn FnMut(),
    ) -> Result<(), String> {
        self.command(
            json!({"type":"ShutdownWorker"}),
            "WorkerStopped",
            config,
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

    fn drain_stderr(&mut self, config: &DeploymentConfig, datastream: &mut DatastreamEmitter) {
        let mut emitted = false;
        while let Ok(line) = self.stderr_rx.try_recv() {
            let payload =
                node_event_payload(config, "worker_stderr", "observed", json!({"line":line}));
            datastream.submit_text(ChannelId::new("mvp.worker.stderr"), payload.to_string());
            emitted = true;
        }
        if emitted {
            datastream.tick();
        }
    }

    fn command(
        &mut self,
        command: Value,
        expected: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        let command_type = command
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let command_text = command.to_string();
        let command_bytes = command_text.len() + 1;
        emit_node_event(
            datastream,
            config,
            NODE_WORKER_CHANNEL,
            "worker_command_write",
            "started",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        if let Err(error) = writeln!(self.stdin, "{command_text}") {
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("write helper command: {error}"));
        }
        if let Err(error) = self.stdin.flush() {
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_command_write",
                "failed",
                json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes,"error":error.to_string()}),
            );
            return Err(format!("flush helper command: {error}"));
        }
        emit_node_event(
            datastream,
            config,
            NODE_WORKER_CHANNEL,
            "worker_command_write",
            "ready",
            json!({"command_type":command_type.as_str(),"expected_event_type":expected,"command_bytes":command_bytes}),
        );
        self.expect_event(expected, config, datastream, channel, pump)
    }

    fn expect_event(
        &mut self,
        expected: &str,
        config: &DeploymentConfig,
        datastream: &mut DatastreamEmitter,
        channel: ChannelId,
        pump: &mut dyn FnMut(),
    ) -> Result<Value, String> {
        loop {
            let mut line = String::new();
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_read",
                "started",
                json!({"expected_event_type":expected,"channel":channel.as_str()}),
            );
            let n = match self.stdout.read_line(&mut line) {
                Ok(n) => n,
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_WORKER_CHANNEL,
                        "worker_stdout_read",
                        "failed",
                        json!({"expected_event_type":expected,"channel":channel.as_str(),"error":error.to_string()}),
                    );
                    return Err(format!("read helper stdout: {error}"));
                }
            };
            self.drain_stderr(config, datastream);
            if n == 0 {
                emit_node_event(
                    datastream,
                    config,
                    NODE_WORKER_CHANNEL,
                    "worker_stdout_read",
                    "failed",
                    json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":0,"error":"stdout closed"}),
                );
                return Err(format!(
                    "tinygrad helper stdout closed while waiting for {expected}"
                ));
            }
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_read",
                "ready",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n}),
            );
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_parse",
                "started",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n}),
            );
            let value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(error) => {
                    emit_node_event(
                        datastream,
                        config,
                        NODE_WORKER_CHANNEL,
                        "worker_stdout_parse",
                        "failed",
                        json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n,"error":error.to_string()}),
                    );
                    return Err(format!("parse helper stdout {line:?}: {error}"));
                }
            };
            let worker_event_type = value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_stdout_parse",
                "ready",
                json!({"expected_event_type":expected,"channel":channel.as_str(),"line_bytes":n,"worker_event_type":worker_event_type}),
            );
            datastream.submit_text(channel.clone(), value.to_string());
            datastream.tick();
            pump();
            if value.get("type").and_then(Value::as_str) == Some(expected) {
                return Ok(value);
            }
            emit_node_event(
                datastream,
                config,
                NODE_WORKER_CHANNEL,
                "worker_event",
                "observed",
                json!({"command_waiting_for":expected,"worker_event_type":worker_event_type,"event":value}),
            );
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
