use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::RawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use datastream::emit::{DatastreamEmitter, EmitterConfig, FrameSink};
use datastream::{Frame, StreamId};
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime, RuntimeConfig};

const IMAGE: &str = "swactor-mvp-gpu-worker-node-e2e:latest";
const WORKER_EVENTS_CHANNEL: &str = "mvp.worker.events";
const ARENA_BYTES: usize = 8192;
const INGRESS_RING_ID: u64 = 8001;
const EGRESS_RING_ID: u64 = 8002;
const INGRESS_EDGE_ID: u64 = 7001;
const EGRESS_EDGE_ID: u64 = 7002;
const INGRESS_BASE: usize = 0;
const EGRESS_BASE: usize = 4096;
const RING_BYTES: usize = 1024;
const HEADER_LEN: usize = 48;
const PREFLIGHT_WATCHDOG: Duration = Duration::from_secs(45);
const EVENT_WATCHDOG: Duration = Duration::from_secs(60);

#[test]
fn gpu_worker_node_e2e_cuda() {
    if std::env::var_os("MVP_SYSTEM_CUDA_E2E_IN_CONTAINER").is_some() {
        run_integrated_node_harness();
    } else if std::env::var_os("MVP_SYSTEM_CUDA_E2E").is_some() {
        build_and_run_docker_fixture();
    } else {
        eprintln!("skipping; set MVP_SYSTEM_CUDA_E2E=1 to run docker CUDA e2e");
    }
}

fn build_and_run_docker_fixture() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .canonicalize()
        .expect("canonical workspace root");
    let context =
        std::env::temp_dir().join(format!("mvp-system-docker-context-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&context);
    copy_workspace_context(&workspace, &context);
    let dockerfile = context.join("crates/mvp-system/tests/gpu_worker_node_e2e/Dockerfile");

    phase("building CUDA Docker fixture image");
    let build = Command::new("docker")
        .args(["build", "-f"])
        .arg(&dockerfile)
        .args(["-t", IMAGE])
        .arg(&context)
        .status()
        .expect("run docker build");
    assert!(build.success(), "docker build failed with status {build}");

    phase("running CUDA Docker fixture");
    let run = Command::new("docker")
        .args(["run", "--rm", "--gpus"])
        .arg(std::env::var("MVP_CUDA_GPUS").unwrap_or_else(|_| "all".to_owned()))
        .args(["-e", "MVP_SYSTEM_CUDA_E2E_IN_CONTAINER=1"])
        .args(["-e", "CARGO_TARGET_DIR=/tmp/mvp-system-target"])
        .arg(IMAGE)
        .status()
        .expect("run docker CUDA fixture");
    assert!(
        run.success(),
        "docker CUDA fixture failed with status {run}"
    );
}

fn copy_workspace_context(source: &Path, dest: &Path) {
    std::fs::create_dir_all(dest).expect("create docker context");
    for entry in std::fs::read_dir(source).expect("read workspace") {
        let entry = entry.expect("read workspace entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(name.as_ref(), ".git" | "target" | ".dockerignore") {
            continue;
        }
        copy_context_entry(&entry.path(), &dest.join(name.as_ref()));
    }
}

fn copy_context_entry(source: &Path, dest: &Path) {
    let metadata = std::fs::symlink_metadata(source).expect("context metadata");
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(dest).expect("create context dir");
        for entry in std::fs::read_dir(source).expect("read context dir") {
            let entry = entry.expect("read context entry");
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(name.as_ref(), ".git" | "target" | ".dockerignore") {
                continue;
            }
            copy_context_entry(&entry.path(), &dest.join(name.as_ref()));
        }
    } else if metadata.is_file() {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).expect("create context parent");
        }
        std::fs::copy(source, dest).expect("copy context file");
    }
}

fn phase(message: &str) {
    eprintln!("gpu-worker-node-e2e: {message}");
}

fn run_cuda_preflight() {
    let script = r#"
import os
print("cuda preflight: importing tinygrad", flush=True)
from tinygrad import Tensor, dtypes
print(f"cuda preflight: DEV={os.environ.get('DEV')}", flush=True)
print("cuda preflight: realizing Tensor([1])", flush=True)
value = Tensor([1], dtype=dtypes.int32).realize().numpy().tolist()
print(f"cuda preflight: ok {value}", flush=True)
"#;
    let child = Command::new("python3")
        .arg("-c")
        .arg(script)
        .env("DEV", "CUDA")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn CUDA preflight");
    let pid = child.id();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });

    match rx.recv_timeout(PREFLIGHT_WATCHDOG) {
        Ok(output) => {
            let output = output.expect("wait CUDA preflight");
            eprintln!(
                "gpu-worker-node-e2e: CUDA preflight stdout:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
            assert!(
                output.status.success(),
                "CUDA preflight failed with status {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
            let output = rx
                .recv_timeout(Duration::from_secs(5))
                .ok()
                .and_then(Result::ok);
            let (stdout, stderr) = output
                .as_ref()
                .map(|output| {
                    (
                        String::from_utf8_lossy(&output.stdout).into_owned(),
                        String::from_utf8_lossy(&output.stderr).into_owned(),
                    )
                })
                .unwrap_or_else(|| ("<unavailable>".to_owned(), "<unavailable>".to_owned()));
            panic!(
                "CUDA preflight test watchdog after {:?}; killed pid {pid}\nstdout:\n{stdout}\nstderr:\n{stderr}",
                PREFLIGHT_WATCHDOG
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("CUDA preflight waiter disconnected"),
    }
}

fn run_integrated_node_harness() {
    phase("running CUDA tinygrad preflight");
    run_cuda_preflight();

    phase("creating shared arena and telemetry socket");
    let arena_fd = create_arena(ARENA_BYTES);
    let worker_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/gpu_worker_node_e2e/mvp_tinygrad_worker.py");
    let socket_path =
        std::env::temp_dir().join(format!("mvp-worker-events-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket_path);

    let (frame_tx, frame_rx) = mpsc::channel();
    let mut emitter = DatastreamEmitter::new(
        EmitterConfig {
            node_hex: "node-11".to_owned(),
            life: 1,
            mux_capacity: 256,
        },
        Box::new(ChannelFrameSink { tx: frame_tx }),
    );
    let ingest_alive = Arc::new(AtomicBool::new(true));
    let ingest_thread = spawn_worker_event_ingest(
        socket_path.clone(),
        emitter.event_sink(),
        Arc::clone(&ingest_alive),
    );

    phase("spawning Rust worker node actor");
    let rt = Runtime::new(RuntimeConfig::default());
    let reports = rt.new_inbox::<HarnessReport>().expect("report inbox");
    let node = rt
        .spawn(GpuWorkerNodeActor::new(rt.create_sender(), *reports.addr()))
        .expect("spawn gpu worker node actor");

    phase("spawning tinygrad worker process");
    rt.send_to(
        node,
        NodeMsg::Start(StartWorker {
            worker_path,
            socket_path: socket_path.clone(),
            arena_fd,
            arena_bytes: ARENA_BYTES as u64,
        }),
    )
    .expect("send start");
    wait_for_report(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "worker process start",
        |report| matches!(report, HarnessReport::ProcessStarted),
    );

    phase("initializing CUDA backend");
    send_command(
        &rt,
        node,
        json!({"type":"InitializeWorker","helper_abi_version":1}),
    );
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "WorkerReady",
        "worker ready",
    );

    phase("installing ingress and egress rings");
    send_command(
        &rt,
        node,
        install_ring_command(
            INGRESS_RING_ID,
            INGRESS_EDGE_ID,
            "in",
            "ingress",
            INGRESS_BASE,
        ),
    );
    send_command(
        &rt,
        node,
        install_ring_command(EGRESS_RING_ID, EGRESS_EDGE_ID, "out", "egress", EGRESS_BASE),
    );
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "RingInstalled",
        "ingress ring installed",
    );
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "RingInstalled",
        "egress ring installed",
    );

    phase("configuring worker role");
    send_command(&rt, node, json!({"type":"ConfigureRole","role_id":1}));
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "RoleLoaded",
        "role loaded",
    );

    phase("copying ingress object into CUDA tensor");
    let input_record = object_record(9000, 0, &[1, 2, 3, 4]);
    pwrite_all(arena_fd, INGRESS_BASE, &input_record);
    send_command(
        &rt,
        node,
        json!({"type":"RingReadable","ring_id":INGRESS_RING_ID,"committed_bytes":input_record.len()}),
    );
    let loaded = wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "ObjectLoaded",
        "object loaded",
    );
    let handle = loaded["handle"]["id"].as_u64().expect("device handle id");

    phase("executing CUDA step and writing egress object");
    send_command(
        &rt,
        node,
        json!({
            "type":"ExecuteStep",
            "step_id":9001,
            "input_handle":handle,
            "egress_ring_id":EGRESS_RING_ID,
            "output_object_id":9001
        }),
    );
    let produced = wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "ObjectProduced",
        "object produced",
    );
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "StepCompleted",
        "step completed",
    );
    let committed = produced["committed_bytes"]
        .as_u64()
        .expect("committed bytes") as usize;
    let mut egress = vec![0u8; committed];
    pread_exact(arena_fd, EGRESS_BASE, &mut egress);
    assert_eq!(decode_payload_words(&egress), vec![2, 4, 6, 8]);

    phase("releasing device object");
    send_command(
        &rt,
        node,
        json!({"type":"ReleaseDeviceObject","handle":handle}),
    );
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "DeviceObjectReleased",
        "device object released",
    );

    phase("shutting down worker process");
    send_command(&rt, node, json!({"type":"ShutdownWorker"}));
    wait_for_control(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "WorkerStopped",
        "worker stopped",
    );
    wait_for_report(
        &rt,
        node,
        &mut emitter,
        &frame_rx,
        &reports,
        "worker process exit",
        |report| matches!(report, HarnessReport::ProcessExited(0)),
    );

    phase("asserting worker telemetry");
    let telemetry = collect_telemetry(&mut emitter, &frame_rx, Duration::from_secs(1));
    assert_has_worker_event(&telemetry, "importing_tinygrad");
    assert_has_worker_event(&telemetry, "tinygrad_imported");
    assert_has_worker_event(&telemetry, "realizing_cuda_probe");
    assert_has_worker_event(&telemetry, "backend_initialized");
    assert_has_worker_event(&telemetry, "worker_ready");
    assert_has_worker_event(&telemetry, "ring_installed");
    assert_has_worker_event(&telemetry, "role_loaded");
    assert_has_worker_event(&telemetry, "object_copy_started");
    assert_has_worker_event_with(&telemetry, "object_loaded", |event| {
        event["object_id"] == 9000 && event["sequence"] == 0 && event["device_sum"] == 10
    });
    assert_has_worker_event(&telemetry, "execute_step_started");
    assert_has_worker_event_with(&telemetry, "object_produced", |event| {
        event["object_id"] == 9001 && event["sequence"] == 0 && event["device_sum"] == 20
    });
    assert_has_worker_event(&telemetry, "step_completed");
    assert_has_worker_event(&telemetry, "device_object_released");
    assert_has_worker_event(&telemetry, "worker_stopped");

    ingest_alive.store(false, Ordering::SeqCst);
    let _ = ingest_thread.join();
    let _ = std::fs::remove_file(&socket_path);
    unsafe {
        libc::close(arena_fd);
    }
}

struct ChannelFrameSink {
    tx: mpsc::Sender<Frame>,
}

impl FrameSink for ChannelFrameSink {
    fn ship(&mut self, _stream: &StreamId, frame: &Frame) {
        self.tx.send(frame.clone()).expect("ship datastream frame");
    }
}

#[derive(Clone)]
struct StartWorker {
    worker_path: PathBuf,
    socket_path: PathBuf,
    arena_fd: RawFd,
    arena_bytes: u64,
}

#[derive(Clone)]
enum NodeMsg {
    Start(StartWorker),
    SendCommand(Value),
    ControlLine(String),
    StderrLine(String),
    ProcessExited(i32),
    KillWorker(String),
}

#[derive(Clone, Debug)]
enum HarnessReport {
    ProcessStarted,
    ControlEvent(Value),
    StderrLine(String),
    ProcessExited(i32),
}

struct GpuWorkerNodeActor {
    sender: ExternalSender,
    report_to: ActorAddress,
    stdin: Option<std::process::ChildStdin>,
    child_pid: Option<u32>,
}

impl GpuWorkerNodeActor {
    fn new(sender: ExternalSender, report_to: ActorAddress) -> Self {
        Self {
            sender,
            report_to,
            stdin: None,
            child_pid: None,
        }
    }
}

impl ActorInterface for GpuWorkerNodeActor {
    type Incoming = NodeMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: NodeMsg) {
        match msg {
            NodeMsg::Start(start) => self.start_worker(ctx, start),
            NodeMsg::SendCommand(value) => {
                let stdin = self.stdin.as_mut().expect("worker stdin");
                writeln!(stdin, "{}", value).expect("write worker command");
                stdin.flush().expect("flush worker command");
            }
            NodeMsg::ControlLine(line) => {
                let value: Value = serde_json::from_str(&line).expect("control JSON");
                ctx.send(self.report_to, HarnessReport::ControlEvent(value))
                    .expect("send control report");
            }
            NodeMsg::StderrLine(line) => {
                ctx.send(self.report_to, HarnessReport::StderrLine(line))
                    .expect("send stderr report");
            }
            NodeMsg::ProcessExited(code) => {
                self.child_pid = None;
                ctx.send(self.report_to, HarnessReport::ProcessExited(code))
                    .expect("send exit report");
            }
            NodeMsg::KillWorker(reason) => {
                self.kill_worker(&reason);
            }
        }
    }
}

impl GpuWorkerNodeActor {
    fn start_worker(&mut self, ctx: &Ctx, start: StartWorker) {
        let mut command = Command::new(start.worker_path);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env("SWACTOR_ARENA_FD", start.arena_fd.to_string())
            .env("SWACTOR_ARENA_BYTES", start.arena_bytes.to_string())
            .env("SWACTOR_WORKER_EVENT_SOCK", &start.socket_path)
            .env("SWACTOR_NODE_ID", "11")
            .env("SWACTOR_RUN_ID", "77")
            .env("SWACTOR_STAGE_INDEX", "0")
            .env("DEV", "CUDA");
        unsafe {
            command.pre_exec(|| Ok(()));
        }
        let mut child = command.spawn().expect("spawn tinygrad worker");
        let stdout = child.stdout.take().expect("worker stdout");
        let stderr = child.stderr.take().expect("worker stderr");
        self.stdin = Some(child.stdin.take().expect("worker stdin"));
        self.child_pid = Some(child.id());

        let target = ctx.self_addr();
        let sender = self.sender.clone();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if sender.send_to(target, NodeMsg::ControlLine(line)).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let target = ctx.self_addr();
        let sender = self.sender.clone();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                match line {
                    Ok(line) => {
                        eprintln!("gpu-worker-node-e2e worker stderr: {line}");
                        if sender.send_to(target, NodeMsg::StderrLine(line)).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let target = ctx.self_addr();
        let sender = self.sender.clone();
        thread::spawn(move || {
            let code = child
                .wait()
                .ok()
                .and_then(|status| status.code())
                .unwrap_or(-1);
            let _ = sender.send_to(target, NodeMsg::ProcessExited(code));
        });

        ctx.send(self.report_to, HarnessReport::ProcessStarted)
            .expect("send started report");
    }

    fn kill_worker(&mut self, reason: &str) {
        if let Some(pid) = self.child_pid.take() {
            eprintln!("gpu-worker-node-e2e: killing worker pid {pid}: {reason}");
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

fn spawn_worker_event_ingest(
    socket_path: PathBuf,
    sink: datastream::emit::DatastreamEventSink,
    alive: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let (ready_tx, ready_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let socket = std::os::unix::net::UnixDatagram::bind(&socket_path).expect("bind UDS ingest");
        ready_tx.send(()).expect("signal UDS ingest ready");
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set UDS read watchdog");
        let mut buf = vec![0u8; 8192];
        while alive.load(Ordering::SeqCst) {
            match socket.recv(&mut buf) {
                Ok(len) => {
                    sink.submit_bytes(WORKER_EVENTS_CHANNEL, buf[..len].to_vec());
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => break,
            }
        }
    });
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("UDS ingest socket ready");
    handle
}

fn send_command(rt: &Runtime, node: ActorAddress, command: Value) {
    rt.send_to(node, NodeMsg::SendCommand(command))
        .expect("send node command");
}

fn wait_for_control(
    rt: &Runtime,
    node: ActorAddress,
    emitter: &mut DatastreamEmitter,
    frame_rx: &mpsc::Receiver<Frame>,
    reports: &swactor::runtime::Inbox<HarnessReport>,
    kind: &str,
    phase_name: &str,
) -> Value {
    match wait_for_report(
        rt,
        node,
        emitter,
        frame_rx,
        reports,
        phase_name,
        |report| matches!(report, HarnessReport::ControlEvent(value) if value["type"] == kind),
    ) {
        HarnessReport::ControlEvent(value) => value,
        other => panic!("unexpected report for {kind}: {other:?}"),
    }
}

fn wait_for_report(
    rt: &Runtime,
    node: ActorAddress,
    emitter: &mut DatastreamEmitter,
    _frame_rx: &mpsc::Receiver<Frame>,
    reports: &swactor::runtime::Inbox<HarnessReport>,
    phase_name: &str,
    mut predicate: impl FnMut(&HarnessReport) -> bool,
) -> HarnessReport {
    let started = Instant::now();
    let mut stderr_lines = Vec::new();
    while started.elapsed() < EVENT_WATCHDOG {
        rt.tick();
        emitter.tick();
        while let Some(report) = reports.try_recv() {
            if predicate(&report) {
                return report;
            }
            match &report {
                HarnessReport::StderrLine(line) => stderr_lines.push(line.clone()),
                HarnessReport::ControlEvent(value) if value["type"] == "WorkerFatal" => {
                    rt.send_to(
                        node,
                        NodeMsg::KillWorker(format!("fatal while waiting for {phase_name}")),
                    )
                    .expect("send kill after fatal");
                    rt.tick();
                    panic!(
                        "worker fatal while waiting for {phase_name}: {value}\nstderr={stderr_lines:?}"
                    );
                }
                HarnessReport::ProcessExited(code) if *code != 0 => {
                    panic!(
                        "worker exited with {code} while waiting for {phase_name}; stderr={stderr_lines:?}"
                    );
                }
                _ => {}
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "test watchdog after {:?} waiting for {phase_name}; stderr={stderr_lines:?}",
        EVENT_WATCHDOG
    );
}

fn collect_telemetry(
    emitter: &mut DatastreamEmitter,
    frame_rx: &mpsc::Receiver<Frame>,
    duration: Duration,
) -> Vec<Value> {
    let started = Instant::now();
    let mut frames = Vec::new();
    while started.elapsed() < duration {
        emitter.tick();
        while let Ok(frame) = frame_rx.try_recv() {
            frames.push(frame);
        }
        thread::sleep(Duration::from_millis(10));
    }
    frames
        .into_iter()
        .filter(|frame| frame.channel.as_str() == WORKER_EVENTS_CHANNEL)
        .map(|frame| serde_json::from_slice::<Value>(&frame.payload).expect("telemetry JSON"))
        .collect()
}

fn assert_has_worker_event(events: &[Value], kind: &str) {
    assert_has_worker_event_with(events, kind, |_| true);
}

fn assert_has_worker_event_with(events: &[Value], kind: &str, extra: impl Fn(&Value) -> bool) {
    assert!(
        events.iter().any(|event| {
            event["schema"] == "mvp.worker.event.v1"
                && event["kind"] == kind
                && event["node_id"] == 11
                && event["run_id"] == 77
                && event["stage_index"] == 0
                && event["worker_generation"] == 1
                && extra(event)
        }),
        "missing worker event {kind}; events={events:#?}"
    );
}

fn install_ring_command(
    ring_id: u64,
    edge_id: u64,
    port_id: &str,
    direction: &str,
    base: usize,
) -> Value {
    json!({
        "type":"InstallRing",
        "ring_id": ring_id,
        "edge_id": edge_id,
        "port_id": port_id,
        "direction": direction,
        "base": base,
        "bytes": RING_BYTES,
        "object_spec": {
            "max_extent": 16,
            "alignment": 4,
            "layout": "token"
        }
    })
}

fn object_record(object_id: u64, sequence: u64, words: &[i32]) -> Vec<u8> {
    let mut record = vec![0u8; HEADER_LEN];
    record[0..4].copy_from_slice(b"MO01");
    record[4] = 1;
    record[5] = HEADER_LEN as u8;
    record[8..16].copy_from_slice(&object_id.to_le_bytes());
    record[16..24].copy_from_slice(&sequence.to_le_bytes());
    record[24..32].copy_from_slice(&((words.len() * 4) as u64).to_le_bytes());
    record[32..40].copy_from_slice(&16u64.to_le_bytes());
    record[40..48].copy_from_slice(&4u64.to_le_bytes());
    for word in words {
        record.extend_from_slice(&word.to_le_bytes());
    }
    record
}

fn decode_payload_words(record: &[u8]) -> Vec<i32> {
    assert_eq!(&record[0..4], b"MO01");
    let extent = u64::from_le_bytes(record[24..32].try_into().unwrap()) as usize;
    record[HEADER_LEN..HEADER_LEN + extent]
        .chunks_exact(4)
        .map(|chunk| i32::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn create_arena(bytes: usize) -> RawFd {
    let name = CString::new("mvp-system-gpu-worker-node-e2e").expect("memfd name");
    let fd = unsafe { libc::memfd_create(name.as_ptr(), 0) };
    assert!(
        fd >= 0,
        "memfd_create failed: {}",
        std::io::Error::last_os_error()
    );
    let truncate = unsafe { libc::ftruncate(fd, bytes as libc::off_t) };
    assert_eq!(
        truncate,
        0,
        "ftruncate failed: {}",
        std::io::Error::last_os_error()
    );
    fd
}

fn pwrite_all(fd: RawFd, offset: usize, bytes: &[u8]) {
    let written = unsafe {
        libc::pwrite(
            fd,
            bytes.as_ptr().cast(),
            bytes.len(),
            offset as libc::off_t,
        )
    };
    assert_eq!(
        written,
        bytes.len() as isize,
        "pwrite failed: {}",
        std::io::Error::last_os_error()
    );
}

fn pread_exact(fd: RawFd, offset: usize, bytes: &mut [u8]) {
    let read = unsafe {
        libc::pread(
            fd,
            bytes.as_mut_ptr().cast(),
            bytes.len(),
            offset as libc::off_t,
        )
    };
    assert_eq!(
        read,
        bytes.len() as isize,
        "pread failed: {}",
        std::io::Error::last_os_error()
    );
}
