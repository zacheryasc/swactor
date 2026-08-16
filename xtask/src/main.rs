use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus, Stdio};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

use serde_json::{Value, json};

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
}

const MYELIN_CHAT_CHECK_TIMEOUT_SECS: u64 = 3_600;
const MYELIN_CHAT_CHECK_POLL_MS: u64 = 100;
const MYELIN_CHAT_CHECK_TERM_GRACE_MS: u64 = 30_000;
const MYELIN_CHAT_CHECK_PROMPTS: &[u8] = b"ping\nsecond prompt\n";
const DATA_PATH_MIN_PAYLOAD_BYTES: u64 = 512;
const MYELIN_CHAT_CARGO_RUN_ARGS: &[&str] = &[
    "run",
    "--package",
    "myelin",
    "--features",
    "dashboard",
    "--bin",
    "myelin-chat",
    "--",
];

struct MyelinChatCheckPaths {
    root: PathBuf,
    dump_log: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
    prompts: PathBuf,
    redacted_config: PathBuf,
    summary: PathBuf,
    benchmark_evidence: PathBuf,
    benchmark_gaps: PathBuf,
}

struct MyelinChatCheckOutput {
    status: ExitStatus,
    child_elapsed_ms: u64,
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdin_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MyelinChatCheckScenario {
    ProcessBaseline,
    Gpu,
    Multinode,
    MultinodeDocker,
    VastAi,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MyelinChatCheckInvocation {
    scenario: MyelinChatCheckScenario,
    pipeline_stages: Option<u32>,
}

impl MyelinChatCheckInvocation {
    fn parse_args(args: Vec<String>) -> Result<Self, String> {
        let mut scenario = MyelinChatCheckScenario::ProcessBaseline;
        let mut pipeline_stages = None;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            if let Some(selected) = MyelinChatCheckScenario::from_flag(&arg) {
                if scenario != MyelinChatCheckScenario::ProcessBaseline {
                    return Err(
                        "myelin-chat-check accepts at most one scenario flag: --gpu, --multinode, --multinode-docker, or --vastai"
                            .to_owned(),
                    );
                }
                scenario = selected;
                continue;
            }

            match arg.as_str() {
                "--pipeline-stages" | "--pipeline-parallel" => {
                    if pipeline_stages.is_some() {
                        return Err(
                            "myelin-chat-check accepts at most one pipeline stage count".to_owned()
                        );
                    }
                    let value = args
                        .next()
                        .ok_or_else(|| format!("{arg} requires a value"))?;
                    let stages = value
                        .parse::<u32>()
                        .map_err(|error| format!("parse {arg}: {error}"))?;
                    if stages == 0 {
                        return Err(format!("{arg} must be greater than 0"));
                    }
                    pipeline_stages = Some(stages);
                }
                other => return Err(format!("unsupported myelin-chat-check argument {other:?}")),
            }
        }
        Ok(Self {
            scenario,
            pipeline_stages,
        })
    }

    fn scenario(&self) -> MyelinChatCheckScenario {
        self.scenario
    }

    fn name(&self) -> &'static str {
        self.scenario.name()
    }

    fn myelin_chat_args(&self, run_id: u64, dump_log: &Path) -> Vec<String> {
        let mut args = Vec::new();
        match self.scenario {
            MyelinChatCheckScenario::ProcessBaseline | MyelinChatCheckScenario::Multinode => {
                args.push("--process".to_owned());
            }
            MyelinChatCheckScenario::Gpu => {
                args.extend(["--process".to_owned(), "--gpu".to_owned()]);
            }
            MyelinChatCheckScenario::MultinodeDocker => {
                args.push("--docker".to_owned());
            }
            MyelinChatCheckScenario::VastAi => {
                args.push("--vastai".to_owned());
            }
        }
        if let Some(pipeline_stages) = self
            .pipeline_stages
            .or_else(|| self.scenario.default_pipeline_stages())
        {
            args.extend(["--pipeline-stages".to_owned(), pipeline_stages.to_string()]);
        }
        if !matches!(self.scenario, MyelinChatCheckScenario::Gpu) {
            args.push("--cached-model".to_owned());
        }
        if matches!(self.scenario, MyelinChatCheckScenario::VastAi) {
            args.extend([
                "--yes".to_owned(),
                "--endpoint-addr-mask".to_owned(),
                "relay-only".to_owned(),
            ]);
        }
        args.extend([
            "--run-id".to_owned(),
            run_id.to_string(),
            format!("--dump-logs={}", dump_log.display()),
        ]);
        args
    }

    fn env_overrides(&self) -> &'static [(&'static str, &'static str)] {
        self.scenario.env_overrides()
    }
}

impl MyelinChatCheckScenario {
    fn from_flag(flag: &str) -> Option<Self> {
        match flag {
            "--gpu" => Some(Self::Gpu),
            "--multinode" => Some(Self::Multinode),
            "--multinode-docker" => Some(Self::MultinodeDocker),
            "--vastai" => Some(Self::VastAi),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::ProcessBaseline => "process",
            Self::Gpu => "gpu",
            Self::Multinode => "multinode",
            Self::MultinodeDocker => "multinode-docker",
            Self::VastAi => "vastai",
        }
    }

    fn default_pipeline_stages(self) -> Option<u32> {
        match self {
            Self::Multinode | Self::MultinodeDocker => Some(2),
            Self::ProcessBaseline | Self::Gpu | Self::VastAi => None,
        }
    }

    fn env_overrides(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::ProcessBaseline
            | Self::Gpu
            | Self::Multinode
            | Self::MultinodeDocker
            | Self::VastAi => &[],
        }
    }
}

const BASIC_TESTS: &[TestStep] = &[
    TestStep {
        label: "root crate",
        args: &["test"],
    },
    TestStep {
        label: "telemetry",
        args: &["test", "-p", "telemetry"],
    },
    TestStep {
        label: "distribution",
        args: &["test", "-p", "distribution"],
    },
    TestStep {
        label: "iroh-driver",
        args: &["test", "-p", "iroh-driver"],
    },
    TestStep {
        label: "myelin",
        args: &["test", "-p", "myelin"],
    },
    TestStep {
        label: "swactor-process",
        args: &["test", "-p", "swactor-process"],
    },
    TestStep {
        label: "swactor-transport",
        args: &["test", "-p", "swactor-transport"],
    },
    TestStep {
        label: "dashboard",
        args: &["test", "-p", "dashboard"],
    },
    TestStep {
        label: "swactor-vastai",
        args: &["test", "-p", "swactor-vastai"],
    },
    TestStep {
        label: "xtask",
        args: &["test", "-p", "xtask"],
    },
];

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}

fn print_usage() {
    println!(
        "\
USAGE: cargo xtask <command>

COMMANDS:
  myelin-chat [--gpu] [--process|--docker|--vastai] [--pipeline-stages n|--pipeline-parallel n] [--cached-model] [-- args...]  Run the human chat wrapper against the real orchestrator/worker bins.
  myelin-chat-check [--gpu|--multinode|--multinode-docker|--vastai] [--pipeline-stages n|--pipeline-parallel n]
                     Run real cargo myelin-chat acceptance check and write benchmark artifacts.
  myelin-chat-compare <baseline-summary.json> <candidate-summary.json>
                     Compare two benchmark summaries and report comparable deltas.
  provisioning-reconciler-demo [--port n] [--nodes n] [--docker]
                      Run the visual E2E provisioning reconciler sanity demo
                      (supervisor + dashboard on localhost, node children
                      join over iroh; --docker launches nodes as scratch
                      containers on a per-run bridge network). Ctrl-C tears
                      down and sweeps.
  check-telemetry-isolation  Verify no frame types appear in control-plane modules.
  test                Run the basic non-binding test barrier: root crate plus each
                      non-binding repository package with `cargo test -p`."
    );
}

const MYELIN_CHAT_USAGE: &str = "\
USAGE: cargo myelin-chat [OPTIONS]

OPTIONS:
  --gpu                         Run the local GPU path: in-process orchestrator plus DEV=CUDA worker selection
  --process | --docker | --vastai
                                Select the runtime provider
  --config <path>               Load config overlay
  --pipeline-stages <count>     Number of pipeline stages
  --cached-model[=<path>]       Use discovered or explicit cached GGUF model (default for --process)
  --dump-logs[=<path>]          Write telemetry frame log
  --run-id <id>                 Override run id
  --skip-rebuild                Reuse existing Cargo artifacts
  --yes, -y                     Approve Vast.ai lease prompts
  --help, -h                    Print this help";

fn print_myelin_chat_usage() {
    println!("{MYELIN_CHAT_USAGE}");
}

fn is_myelin_chat_help_request(args: &[String]) -> bool {
    args.iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h" | "help"))
}

fn run_step(step: &TestStep) -> bool {
    println!("\n=== {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    match Command::new(cargo_bin()).args(step.args).status() {
        Ok(status) => status.success(),
        Err(error) => {
            eprintln!("Failed to execute cargo: {error}");
            false
        }
    }
}

fn run_tests() -> ExitCode {
    let start = Instant::now();
    let check = run_myelin_chat_check(Vec::new());
    if check != ExitCode::SUCCESS {
        return check;
    }

    for (index, step) in BASIC_TESTS.iter().enumerate() {
        if !run_step(step) {
            eprintln!(
                "\n--- FAILED after {:.1}s ({index} passed, 1 failed) ---",
                start.elapsed().as_secs_f64()
            );
            return ExitCode::from(1);
        }
    }

    println!(
        "\n--- All {} step(s) passed in {:.1}s ---",
        BASIC_TESTS.len(),
        start.elapsed().as_secs_f64()
    );
    ExitCode::SUCCESS
}

fn run_myelin_chat(args: Vec<String>) -> ExitCode {
    let forwarded = if args.first().is_some_and(|arg| arg == "--") {
        args[1..].to_vec()
    } else {
        args
    };
    if is_myelin_chat_help_request(&forwarded) {
        print_myelin_chat_usage();
        return ExitCode::SUCCESS;
    }
    let mut command = Command::new(cargo_bin());
    command.args(MYELIN_CHAT_CARGO_RUN_ARGS);
    let dump_log_path = explicit_dump_log_path_from_myelin_chat_args(&forwarded);
    let run_id = run_id_from_myelin_chat_args(&forwarded);
    let benchmark_target = dump_log_path.as_deref().zip(run_id);
    if let Some((path, run_id)) = benchmark_target {
        let event = xtask_myelin_chat_benchmark_event(
            run_id,
            "started",
            json!({
                "program": "cargo",
                "args": MYELIN_CHAT_CARGO_RUN_ARGS,
            }),
        );
        if let Err(error) = append_synthetic_benchmark_frame(
            path,
            "xtask-myelin-chat",
            "myelin.xtask.benchmark",
            event,
        ) {
            eprintln!("Failed to write myelin-chat benchmark frame: {error}");
            return ExitCode::from(1);
        }
    }
    command.args(&forwarded);

    let status_result = command.status();
    if let Some((path, run_id)) = benchmark_target {
        let (status, detail) = match &status_result {
            Ok(status) if status.success() => ("ready", json!({"exit_status": status.to_string()})),
            Ok(status) => ("failed", json!({"exit_status": status.to_string()})),
            Err(error) => ("failed", json!({"error": error.to_string()})),
        };
        let event = xtask_myelin_chat_benchmark_event(run_id, status, detail);
        if let Err(error) = append_synthetic_benchmark_frame(
            path,
            "xtask-myelin-chat",
            "myelin.xtask.benchmark",
            event,
        ) {
            eprintln!("Failed to write myelin-chat benchmark frame: {error}");
            return ExitCode::from(1);
        }
    }

    match status_result {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(status) => ExitCode::from(
            status
                .code()
                .and_then(|code| u8::try_from(code).ok())
                .unwrap_or(1),
        ),
        Err(error) => {
            eprintln!("Failed to execute cargo myelin-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_myelin_chat_compare(args: Vec<String>) -> ExitCode {
    if args.len() != 2 {
        eprintln!(
            "USAGE: cargo xtask myelin-chat-compare <baseline-summary.json> <candidate-summary.json>"
        );
        return ExitCode::from(1);
    }
    let baseline = match read_summary_json(Path::new(&args[0])) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
    };
    let candidate = match read_summary_json(Path::new(&args[1])) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(1);
        }
    };
    let comparable = summaries_comparable(&baseline, &candidate);
    println!("myelin-chat-compare: comparable={comparable}");
    for reason in summary_incomparability_reasons(&baseline, &candidate) {
        println!("myelin-chat-compare: incomparable {reason}");
    }
    print_summary_metric_delta(
        "total_child_ms",
        summary_pointer_u64(&baseline, "/timings/total_child_ms"),
        summary_pointer_u64(&candidate, "/timings/total_child_ms"),
    );
    print_summary_metric_delta(
        "prepare_runtime_ms",
        summary_pointer_u64(&baseline, "/timings/prepare_runtime_ms/value_ms"),
        summary_pointer_u64(&candidate, "/timings/prepare_runtime_ms/value_ms"),
    );
    print_summary_metric_delta(
        "standup_to_prompt_rpc_ms",
        summary_pointer_u64(&baseline, "/timings/standup_to_prompt_rpc_ms/value_ms"),
        summary_pointer_u64(&candidate, "/timings/standup_to_prompt_rpc_ms/value_ms"),
    );
    for request_id in 1..=2 {
        for metric in [
            "roundtrip_ms",
            "first_token_ms",
            "decode_ms",
            "text_decode_ms",
        ] {
            print_summary_metric_delta(
                &format!("prompt_{request_id}_{metric}"),
                summary_prompt_metric(&baseline, request_id, metric),
                summary_prompt_metric(&candidate, request_id, metric),
            );
        }
    }
    if comparable {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

fn read_summary_json(path: &Path) -> Result<Value, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("myelin-chat-compare: read summary {}: {e}", path.display()))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("myelin-chat-compare: parse summary {}: {e}", path.display()))
}

fn summaries_comparable(baseline: &Value, candidate: &Value) -> bool {
    summary_incomparability_reasons(baseline, candidate).is_empty()
}

fn summary_incomparability_reasons(baseline: &Value, candidate: &Value) -> Vec<String> {
    let mut reasons = Vec::new();
    for (label, pointer) in [
        ("schema", "/schema"),
        ("scenario", "/scenario"),
        ("workload", "/workload/prompt_corpus_blake3"),
        ("model", "/run_envelope/detail/model/id"),
        ("provider", "/run_envelope/detail/provider/kind"),
        (
            "pipeline_stages",
            "/run_envelope/detail/runtime/pipeline_stages",
        ),
        ("gpu_run", "/run_envelope/detail/runtime/gpu_run"),
        ("node_image", "/run_envelope/detail/provider/node_image"),
    ] {
        let left = baseline.pointer(pointer);
        let right = candidate.pointer(pointer);
        if left != right {
            reasons.push(format!(
                "{label} baseline={} candidate={}",
                render_summary_value(left),
                render_summary_value(right)
            ));
        }
    }
    reasons
}

fn print_summary_metric_delta(name: &str, baseline: Option<u64>, candidate: Option<u64>) {
    match (baseline, candidate) {
        (Some(left), Some(right)) => {
            let delta = right as i128 - left as i128;
            let pct = if left == 0 {
                "unavailable".to_owned()
            } else {
                format!("{:.2}", (delta as f64 / left as f64) * 100.0)
            };
            println!(
                "myelin-chat-compare: {name} baseline={left} candidate={right} delta_ms={delta} delta_pct={pct}"
            );
        }
        _ => println!(
            "myelin-chat-compare: {name} baseline={} candidate={} delta_ms=unavailable",
            baseline
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_owned()),
            candidate
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_owned())
        ),
    }
}

fn summary_prompt_metric(summary: &Value, request_id: u64, metric: &str) -> Option<u64> {
    summary
        .pointer("/timings/prompts")
        .and_then(Value::as_array)?
        .iter()
        .find(|prompt| prompt.get("request_id").and_then(Value::as_u64) == Some(request_id))?
        .get(metric)?
        .get("value_ms")
        .and_then(Value::as_u64)
}

fn summary_pointer_u64(summary: &Value, pointer: &str) -> Option<u64> {
    summary.pointer(pointer).and_then(Value::as_u64)
}

fn render_summary_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Null) | None => "unavailable".to_owned(),
        Some(value) => value.to_string(),
    }
}

fn explicit_dump_log_path_from_myelin_chat_args(args: &[String]) -> Option<PathBuf> {
    let args = strip_leading_double_dash(args);
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        if let Some(path) = arg.strip_prefix("--dump-logs=") {
            if !path.is_empty() {
                return Some(PathBuf::from(path));
            }
        } else if arg == "--dump-logs" {
            if let Some(path) = args.get(index + 1)
                && !path.starts_with("--")
            {
                return Some(PathBuf::from(path));
            }
        }
        index += 1;
    }
    None
}

fn run_id_from_myelin_chat_args(args: &[String]) -> Option<u64> {
    let args = strip_leading_double_dash(args);
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--run-id" {
            return args
                .get(index + 1)
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value != 0);
        }
        index += 1;
    }
    None
}

fn strip_leading_double_dash(args: &[String]) -> &[String] {
    if args.first().is_some_and(|arg| arg == "--") {
        &args[1..]
    } else {
        args
    }
}

fn append_synthetic_benchmark_frame(
    path: &Path,
    source: &str,
    channel: &str,
    event: Value,
) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "create synthetic benchmark frame dir {}: {e}",
                parent.display()
            )
        })?;
    }
    let run_id = event
        .get("run_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| "synthetic benchmark event missing run_id".to_owned())?;
    let inner = serde_json::to_string(&event)
        .map_err(|e| format!("serialize synthetic benchmark event: {e}"))?;
    let record = json!({
        "arrival_seq": 0,
        "arrival_unix_ms": unix_ms_now(),
        "source": source,
        "stream": format!("xtask#{run_id}"),
        "channel": channel,
        "channel_id": 0,
        "position": 0,
        "payload": {"encoding": "utf8", "value": inner},
    });
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open synthetic benchmark frame {}: {e}", path.display()))?;
    let mut line = serde_json::to_vec(&record)
        .map_err(|e| format!("serialize synthetic benchmark frame: {e}"))?;
    line.push(b'\n');
    file.write_all(&line)
        .map_err(|e| format!("write synthetic benchmark frame {}: {e}", path.display()))?;
    file.flush()
        .map_err(|e| format!("flush synthetic benchmark frame {}: {e}", path.display()))
}

fn xtask_myelin_chat_benchmark_event(run_id: u64, status: &str, detail: Value) -> Value {
    let benchmark = xtask_benchmark_stamp();
    json!({
        "schema_version": benchmark["schema_version"].clone(),
        "type": "XtaskBenchmark",
        "event_type": "XtaskBenchmark",
        "event_name": "cargo_run_myelin_chat",
        "phase": "cargo_run_myelin_chat",
        "status": status,
        "run_id": run_id,
        "producer_component": benchmark["producer_component"].clone(),
        "producer_instance_id": benchmark["producer_instance_id"].clone(),
        "producer_process_id": benchmark["producer_process_id"].clone(),
        "producer_sequence": benchmark["producer_sequence"].clone(),
        "wall_clock_unix_ms": benchmark["wall_clock_unix_ms"].clone(),
        "monotonic_ms": benchmark["monotonic_ms"].clone(),
        "clock_source": benchmark["clock_source"].clone(),
        "span_id": format!("xtask:{run_id}:{}:cargo_run_myelin_chat", benchmark["producer_sequence"]),
        "parent_span_id": Value::Null,
        "detail": detail,
        "benchmark": benchmark,
    })
}

fn xtask_benchmark_summary_event(run_id: u64, status: &str, detail: Value) -> Value {
    let benchmark = xtask_benchmark_stamp();
    json!({
        "schema_version": benchmark["schema_version"].clone(),
        "type": "BenchmarkSummaryGenerated",
        "event_type": "BenchmarkSummaryGenerated",
        "event_name": "summary_generation",
        "phase": "summary_generation",
        "status": status,
        "run_id": run_id,
        "producer_component": benchmark["producer_component"].clone(),
        "producer_instance_id": benchmark["producer_instance_id"].clone(),
        "producer_process_id": benchmark["producer_process_id"].clone(),
        "producer_sequence": benchmark["producer_sequence"].clone(),
        "wall_clock_unix_ms": benchmark["wall_clock_unix_ms"].clone(),
        "monotonic_ms": benchmark["monotonic_ms"].clone(),
        "clock_source": benchmark["clock_source"].clone(),
        "span_id": format!("xtask:{run_id}:{}:summary_generation", benchmark["producer_sequence"]),
        "parent_span_id": Value::Null,
        "detail": detail,
        "benchmark": benchmark,
    })
}

const XTASK_BENCHMARK_SCHEMA: u64 = 1;
static XTASK_BENCHMARK_START: LazyLock<Instant> = LazyLock::new(Instant::now);
static XTASK_BENCHMARK_SEQ: AtomicU64 = AtomicU64::new(1);

fn xtask_benchmark_stamp() -> Value {
    let mono_ms = u64::try_from(XTASK_BENCHMARK_START.elapsed().as_millis()).unwrap_or(u64::MAX);
    let seq = XTASK_BENCHMARK_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let wall_ms = unix_ms_now();
    json!({
        "schema": XTASK_BENCHMARK_SCHEMA,
        "schema_version": XTASK_BENCHMARK_SCHEMA,
        "component": "xtask",
        "producer_component": "xtask",
        "producer_instance_id": format!("xtask:{pid}"),
        "producer_process_id": pid,
        "pid": pid,
        "seq": seq,
        "producer_sequence": seq,
        "wall_unix_ms": wall_ms,
        "wall_clock_unix_ms": wall_ms,
        "mono_ms": mono_ms,
        "monotonic_ms": mono_ms,
        "clock_source": {
            "wall": "system_unix_ms",
            "monotonic": "process_elapsed_ms"
        },
    })
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask manifest dir has a parent")
        .to_path_buf()
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    for attempt in 0..100 {
        let timestamp_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("{prefix}-{pid}-{timestamp_nanos}-{attempt}"));
        match fs::create_dir(&root) {
            Ok(()) => return root,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!(
                "myelin-chat-check: create temp dir {}: {error}",
                root.display()
            ),
        }
    }
    panic!("myelin-chat-check: could not allocate unique temp dir for prefix {prefix}");
}
fn unix_ms_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn myelin_chat_check_run_id() -> u64 {
    unix_ms_now().max(1)
}

fn write_myelin_chat_check_paths(root: &Path) -> Result<MyelinChatCheckPaths, String> {
    if !root.is_dir() {
        return Err(format!(
            "myelin-chat-check: temp root {} is not a directory",
            root.display()
        ));
    }
    let dump_log = root.join("telemetry.ndjson");
    if dump_log.exists() {
        return Err(format!(
            "myelin-chat-check: dump log path already exists: {}",
            dump_log.display()
        ));
    }
    Ok(MyelinChatCheckPaths {
        root: root.to_path_buf(),
        dump_log,
        stdout: root.join("stdout.txt"),
        stderr: root.join("stderr.txt"),
        prompts: root.join("prompts.txt"),
        redacted_config: root.join("redacted-config.json"),
        summary: root.join("summary.json"),
        benchmark_evidence: root.join("benchmark-evidence.json"),
        benchmark_gaps: root.join("benchmark-gaps.md"),
    })
}

fn run_myelin_chat_check(args: Vec<String>) -> ExitCode {
    let invocation = match MyelinChatCheckInvocation::parse_args(args) {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("myelin-chat-check: failed: {error}");
            print_usage();
            return ExitCode::from(1);
        }
    };
    let scenario = invocation.scenario();
    let workspace = workspace_root();
    let temp_root = unique_temp_dir("myelin-chat-check");
    let paths = match write_myelin_chat_check_paths(&temp_root) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("{error}");
            eprintln!(
                "myelin-chat-check: temp directory kept at {}",
                temp_root.display()
            );
            return ExitCode::from(1);
        }
    };
    let run_id = myelin_chat_check_run_id();
    println!("myelin-chat-check: scenario {}", invocation.name());
    println!("myelin-chat-check: artifacts {}", paths.root.display());
    println!("myelin-chat-check: telemetry {}", paths.dump_log.display());

    let output = match run_myelin_chat_check_process(&workspace, &paths, run_id, &invocation) {
        Ok(output) => output,
        Err(error) => return fail_myelin_chat_check(&error, &paths, "", "", None),
    };

    if let Some(error) = &output.stdin_error {
        return fail_myelin_chat_check(
            error,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    if output.timed_out {
        let reason = format!("timeout after {MYELIN_CHAT_CHECK_TIMEOUT_SECS} seconds");
        return fail_myelin_chat_check(
            &reason,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    if !output.status.success() {
        return fail_myelin_chat_check(
            "child exited nonzero",
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }

    let responses = match assert_stdout_contains_two_prompt_cycles(&output.stdout) {
        Ok(responses) => responses,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    let events = match assert_dump_log_facts(
        &paths.dump_log,
        scenario,
        run_id,
        invocation.pipeline_stages,
    ) {
        Ok(events) => events,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    let report = match build_benchmark_report(&events, output.child_elapsed_ms, run_id, scenario) {
        Ok(report) => report,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    let preliminary_summary = match build_benchmark_summary(
        &events,
        output.child_elapsed_ms,
        run_id,
        scenario,
        &paths,
        invocation.pipeline_stages,
        u64::try_from(output.stdout.len()).unwrap_or(u64::MAX),
        u64::try_from(output.stderr.len()).unwrap_or(u64::MAX),
    ) {
        Ok(summary) => summary,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    if let Err(error) = append_synthetic_benchmark_frame(
        &paths.dump_log,
        "xtask",
        "myelin.xtask.benchmark",
        xtask_benchmark_summary_event(
            run_id,
            "ready",
            json!({
                "summary_path": paths.summary.display().to_string(),
                "benchmark_evidence_path": paths.benchmark_evidence.display().to_string(),
                "benchmark_gaps_path": paths.benchmark_gaps.display().to_string(),
                "validator": preliminary_summary.get("validator").cloned().unwrap_or(Value::Null),
            }),
        ),
    ) {
        return fail_myelin_chat_check(
            &error,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    let events = match parse_dump_log_events(&paths.dump_log) {
        Ok(events) => events,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    let summary = match build_benchmark_summary(
        &events,
        output.child_elapsed_ms,
        run_id,
        scenario,
        &paths,
        invocation.pipeline_stages,
        u64::try_from(output.stdout.len()).unwrap_or(u64::MAX),
        u64::try_from(output.stderr.len()).unwrap_or(u64::MAX),
    ) {
        Ok(summary) => summary,
        Err(error) => {
            return fail_myelin_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    if let Err(error) = write_benchmark_artifacts(
        &paths,
        run_id,
        scenario,
        &events,
        invocation.pipeline_stages,
        &output,
        &summary,
    ) {
        return fail_myelin_chat_check(
            &error,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    for line in &report.lines {
        println!("{line}");
    }
    println!("myelin-chat-check: artifacts {}", paths.root.display());
    println!("myelin-chat-check: summary {}", paths.summary.display());

    println!("myelin-chat-check: ok");
    for (index, response) in responses.iter().enumerate() {
        println!("myelin-chat-check: response {}: {}", index + 1, response);
    }
    ExitCode::SUCCESS
}

fn run_myelin_chat_check_process(
    workspace: &Path,
    paths: &MyelinChatCheckPaths,
    run_id: u64,
    invocation: &MyelinChatCheckInvocation,
) -> Result<MyelinChatCheckOutput, String> {
    let mut command = Command::new(cargo_bin());
    command.current_dir(workspace).arg("myelin-chat").arg("--");
    for arg in invocation.myelin_chat_args(run_id, &paths.dump_log) {
        command.arg(arg);
    }
    for &(key, value) in invocation.env_overrides() {
        command.env(key, value);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    #[cfg(target_os = "linux")]
    unsafe {
        command.pre_exec(|| {
            let result = libc::setpgid(0, 0);
            if result == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }

    let child_started = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|e| format!("myelin-chat-check: spawn cargo myelin-chat: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "myelin-chat-check: child stdout was not piped".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "myelin-chat-check: child stderr was not piped".to_owned())?;
    let stdout_reader = thread::spawn(move || read_pipe_to_string(stdout, "stdout"));
    let stderr_reader = thread::spawn(move || read_pipe_to_string(stderr, "stderr"));

    let stdin_error = match child.stdin.take() {
        Some(mut stdin) => {
            let result = stdin.write_all(MYELIN_CHAT_CHECK_PROMPTS);
            drop(stdin);
            result
                .err()
                .map(|error| format!("myelin-chat-check: write child stdin: {error}"))
        }
        None => Some("myelin-chat-check: child stdin was not piped".to_owned()),
    };

    let (status, timed_out) = if stdin_error.is_some() {
        (terminate_myelin_chat_child(&mut child)?, false)
    } else {
        wait_myelin_chat_check_child(&mut child)?
    };
    let child_elapsed_ms = duration_ms_u64(child_started.elapsed());

    let stdout = join_reader(stdout_reader, "stdout")?;
    let stderr = join_reader(stderr_reader, "stderr")?;
    Ok(MyelinChatCheckOutput {
        child_elapsed_ms,
        status,
        stdout,
        stderr,
        timed_out,
        stdin_error,
    })
}

fn wait_myelin_chat_check_child(child: &mut Child) -> Result<(ExitStatus, bool), String> {
    let timeout = Duration::from_secs(MYELIN_CHAT_CHECK_TIMEOUT_SECS);
    let poll = Duration::from_millis(MYELIN_CHAT_CHECK_POLL_MS);
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok((status, false)),
            Ok(None) if Instant::now() >= deadline => {
                return terminate_myelin_chat_child(child).map(|status| (status, true));
            }
            Ok(None) => thread::sleep(poll),
            Err(error) => return Err(format!("myelin-chat-check: poll child status: {error}")),
        }
    }
}

fn terminate_myelin_chat_child(child: &mut Child) -> Result<ExitStatus, String> {
    #[cfg(target_os = "linux")]
    {
        signal_myelin_chat_process_group(child, libc::SIGTERM);
        let grace_polls = MYELIN_CHAT_CHECK_TERM_GRACE_MS / MYELIN_CHAT_CHECK_POLL_MS;
        for _ in 0..grace_polls {
            match child.try_wait() {
                Ok(Some(status)) => return Ok(status),
                Ok(None) => thread::sleep(Duration::from_millis(MYELIN_CHAT_CHECK_POLL_MS)),
                Err(error) => {
                    return Err(format!(
                        "myelin-chat-check: poll child after SIGTERM: {error}"
                    ));
                }
            }
        }
        signal_myelin_chat_process_group(child, libc::SIGKILL);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = child.kill();
    }

    child
        .wait()
        .map_err(|e| format!("myelin-chat-check: wait for terminated child: {e}"))
}

#[cfg(target_os = "linux")]
fn signal_myelin_chat_process_group(child: &Child, signal: libc::c_int) {
    let process_group = -(child.id() as libc::pid_t);
    let _ = unsafe { libc::kill(process_group, signal) };
}

fn read_pipe_to_string<R: Read>(mut reader: R, label: &'static str) -> Result<String, String> {
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(|e| format!("myelin-chat-check: read child {label}: {e}"))?;
    Ok(text)
}

fn join_reader(
    handle: thread::JoinHandle<Result<String, String>>,
    label: &str,
) -> Result<String, String> {
    handle
        .join()
        .map_err(|_| format!("myelin-chat-check: child {label} reader panicked"))?
}

fn fail_myelin_chat_check(
    reason: &str,
    paths: &MyelinChatCheckPaths,
    stdout: &str,
    stderr: &str,
    status: Option<&ExitStatus>,
) -> ExitCode {
    eprintln!("myelin-chat-check: failed: {reason}");
    if let Some(status) = status {
        eprintln!("myelin-chat-check: child exit status: {status}");
    }
    if let Err(error) = write_failure_artifacts(paths, reason, stdout, stderr, status) {
        eprintln!("myelin-chat-check: warning: could not write failure artifacts: {error}");
    }
    eprintln!(
        "myelin-chat-check: temp directory kept at {}",
        paths.root.display()
    );
    eprintln!("--- captured stdout ---");
    if stdout.is_empty() {
        eprintln!("<empty>");
    } else {
        eprint!("{stdout}");
        if !stdout.ends_with('\n') {
            eprintln!();
        }
    }
    eprintln!("--- captured stderr ---");
    if stderr.is_empty() {
        eprintln!("<empty>");
    } else {
        eprint!("{stderr}");
        if !stderr.ends_with('\n') {
            eprintln!();
        }
    }
    ExitCode::from(1)
}

fn write_failure_artifacts(
    paths: &MyelinChatCheckPaths,
    reason: &str,
    stdout: &str,
    stderr: &str,
    status: Option<&ExitStatus>,
) -> Result<(), String> {
    fs::write(&paths.stdout, stdout).map_err(|e| {
        format!(
            "myelin-chat-check: write stdout artifact {}: {e}",
            paths.stdout.display()
        )
    })?;
    fs::write(&paths.stderr, stderr).map_err(|e| {
        format!(
            "myelin-chat-check: write stderr artifact {}: {e}",
            paths.stderr.display()
        )
    })?;
    fs::write(&paths.prompts, MYELIN_CHAT_CHECK_PROMPTS).map_err(|e| {
        format!(
            "myelin-chat-check: write prompt corpus {}: {e}",
            paths.prompts.display()
        )
    })?;
    let evidence = json!({
        "schema": "swactor.myelin_chat_check.failure_evidence.v1",
        "status": "failed",
        "reason": reason,
        "child_status": status.map(|status| status.to_string()),
        "telemetry": {
            "path": paths.dump_log.display().to_string(),
            "exists": paths.dump_log.is_file(),
            "bytes": file_len(&paths.dump_log),
        },
        "stdout": {
            "path": paths.stdout.display().to_string(),
            "bytes": stdout.len(),
        },
        "stderr": {
            "path": paths.stderr.display().to_string(),
            "bytes": stderr.len(),
        },
        "side_channel_audit": {
            "status": "captured_not_authoritative",
            "stdout_telemetry_substitute": false,
            "stderr_telemetry_substitute": false,
        },
    });
    write_json_file(&paths.benchmark_evidence, &evidence)?;
    fs::write(
        &paths.benchmark_gaps,
        format!(
            "# Benchmark observability gaps\n\n- status: failed\n- reason: {reason}\n- telemetry: {} (exists: {}, bytes: {})\n- stdout: {} bytes; side-channel only, not benchmark evidence\n- stderr: {} bytes; side-channel only, not benchmark evidence\n- remediation: fix the failed child run, then rerun so benchmark summary generation can validate canonical telemetry evidence.\n",
            paths.dump_log.display(),
            paths.dump_log.is_file(),
            file_len(&paths.dump_log).unwrap_or(0),
            stdout.len(),
            stderr.len(),
        ),
    )
    .map_err(|e| {
        format!(
            "myelin-chat-check: write benchmark gaps artifact {}: {e}",
            paths.benchmark_gaps.display()
        )
    })?;
    let summary = json!({
        "schema": "swactor.myelin_chat_check.failure.v1",
        "status": "failed",
        "reason": reason,
        "child_status": status.map(|status| status.to_string()),
        "created_unix_ms": unix_ms_now(),
        "artifacts": {
            "root": paths.root.display().to_string(),
            "telemetry": {
                "path": paths.dump_log.display().to_string(),
                "exists": paths.dump_log.is_file(),
                "bytes": file_len(&paths.dump_log),
            },
            "stdout": {
                "path": paths.stdout.display().to_string(),
                "bytes": stdout.len(),
            },
            "stderr": {
                "path": paths.stderr.display().to_string(),
                "bytes": stderr.len(),
            },
            "prompts": {
                "path": paths.prompts.display().to_string(),
                "bytes": MYELIN_CHAT_CHECK_PROMPTS.len(),
                "blake3": bytes_blake3_hex(MYELIN_CHAT_CHECK_PROMPTS),
            },
            "benchmark_evidence": {
                "path": paths.benchmark_evidence.display().to_string(),
                "exists": paths.benchmark_evidence.is_file(),
                "bytes": file_len(&paths.benchmark_evidence),
            },
            "benchmark_gaps": {
                "path": paths.benchmark_gaps.display().to_string(),
                "exists": paths.benchmark_gaps.is_file(),
                "bytes": file_len(&paths.benchmark_gaps),
            },
        },
    });
    write_json_file(&paths.summary, &summary)
}

fn file_len(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|metadata| metadata.len())
}

fn assert_stdout_contains_two_prompt_cycles(stdout: &str) -> Result<Vec<String>, String> {
    let decoding_count = stdout.matches("decoding...").count();
    if decoding_count < 2 {
        return Err(format!(
            "myelin-chat-check: expected at least two decoding... markers, found {decoding_count}"
        ));
    }
    let response_count = stdout.matches("Response: ").count();
    if response_count < 2 {
        return Err(format!(
            "myelin-chat-check: expected at least two Response: prefixes, found {response_count}"
        ));
    }

    let mut cursor = 0;
    let mut responses = Vec::with_capacity(2);
    for cycle in 1..=2 {
        let prompt_at = find_stdout_marker(stdout, "prompt:>", cursor, cycle, "prompt")?;
        let decoding_at = find_stdout_marker(
            stdout,
            "decoding...",
            prompt_at + "prompt:>".len(),
            cycle,
            "decoding",
        )?;
        let response_at = find_stdout_marker(
            stdout,
            "Response: ",
            decoding_at + "decoding...".len(),
            cycle,
            "response",
        )?;
        let response_start = response_at + "Response: ".len();
        let response_end = stdout[response_start..]
            .find('\n')
            .map_or(stdout.len(), |offset| response_start + offset);
        let response = &stdout[response_start..response_end];
        if !response.chars().any(|ch| !ch.is_whitespace()) {
            return Err(format!(
                "myelin-chat-check: empty Response text for prompt cycle {cycle}"
            ));
        }
        responses.push(response.to_owned());
        cursor = response_end;
    }
    Ok(responses)
}

fn find_stdout_marker(
    stdout: &str,
    marker: &str,
    start: usize,
    cycle: usize,
    label: &str,
) -> Result<usize, String> {
    stdout[start..]
        .find(marker)
        .map(|offset| start + offset)
        .ok_or_else(|| {
            format!("myelin-chat-check: missing {label} marker for prompt cycle {cycle}")
        })
}

#[derive(Clone)]
struct DumpLogEvent {
    line_number: usize,
    arrival_seq: Option<u64>,
    source: String,
    stream: String,
    channel: String,
    channel_id: Option<u64>,
    position: Option<u64>,
    payload_encoding: Option<String>,
    arrival_unix_ms: Option<u64>,
    event: Value,
}

#[derive(Clone)]
struct BenchmarkPoint {
    component: Option<String>,
    wall_unix_ms: Option<u64>,
    mono_ms: Option<u64>,
    arrival_unix_ms: Option<u64>,
}

#[derive(Clone)]
struct ValidatorFinding {
    severity: &'static str,
    code: &'static str,
    message: String,
    channel: Option<String>,
    event_type: Option<String>,
    phase: Option<String>,
    status: Option<String>,
    request_id: Option<u64>,
    stage_index: Option<u64>,
    json_pointer: Option<String>,
}
impl ValidatorFinding {
    fn error(code: &'static str, message: impl Into<String>) -> Self {
        Self::new("observability_gap", code, message)
    }

    fn fatal(code: &'static str, message: impl Into<String>) -> Self {
        Self::new("fatal", code, message)
    }

    #[allow(dead_code)]
    fn profiling_gap(code: &'static str, message: impl Into<String>) -> Self {
        Self::new("profiling_gap", code, message)
    }

    fn warning(code: &'static str, message: impl Into<String>) -> Self {
        Self::new("warning", code, message)
    }

    fn new(severity: &'static str, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code,
            message: message.into(),
            channel: None,
            event_type: None,
            phase: None,
            status: None,
            request_id: None,
            stage_index: None,
            json_pointer: None,
        }
    }

    fn at_event(mut self, record: &DumpLogEvent) -> Self {
        self.channel = Some(record.channel.clone());
        self.event_type = record
            .event
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.phase = record
            .event
            .get("phase")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.status = record
            .event
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.request_id = benchmark_request_id(&record.event);
        self.stage_index = record
            .event
            .get("stage_index")
            .and_then(Value::as_u64)
            .or_else(|| detail_u64(&record.event, "stage_index"));
        self
    }

    fn pointer(mut self, pointer: &'static str) -> Self {
        self.json_pointer = Some(pointer.to_owned());
        self
    }

    fn to_json(&self) -> Value {
        json!({
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "channel": self.channel,
            "event_type": self.event_type,
            "phase": self.phase,
            "status": self.status,
            "request_id": self.request_id,
            "stage_index": self.stage_index,
            "json_pointer": self.json_pointer,
            "check_id": self.code,
            "expected_evidence": self.message,
            "observed_evidence": {
                "channel": self.channel,
                "event_type": self.event_type,
                "phase": self.phase,
                "status": self.status,
            },
            "missing_or_extra_evidence": self.message,
            "affected": {
                "request_id": self.request_id,
                "stage_index": self.stage_index,
            },
            "source_event_ids": [],
            "remediation_hint": format!("emit or repair telemetry evidence for {}", self.code),
        })
    }
}

#[derive(Default)]
struct BenchmarkValidation {
    expected_pipeline_stages: Option<u32>,
    findings: Vec<ValidatorFinding>,
    producers: BTreeSet<String>,
    stages_ready: BTreeSet<u64>,
    stages_with_worker: BTreeSet<u64>,
    stages_with_device: BTreeSet<u64>,
    stages_runtime_ready_via_orchestrator: BTreeSet<u64>,
    stages_route_ready_via_orchestrator: BTreeSet<u64>,
    activation_downstream_object_loaded: bool,
    max_activation_record_bytes: u64,
    edges_with_producer: BTreeSet<u64>,
    edges_with_consumer: BTreeSet<u64>,
    requests_started: BTreeSet<u64>,
    requests_completed: BTreeSet<u64>,
    run_envelope_present: bool,
    endpoint_snapshot_present: bool,
    python_telemetry_connected: bool,
}

impl BenchmarkValidation {
    fn push(&mut self, finding: ValidatorFinding) {
        self.findings.push(finding);
    }

    fn error_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.severity != "warning")
            .count()
    }

    fn warning_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.severity == "warning")
            .count()
    }

    fn invalid_findings(&self) -> impl Iterator<Item = &ValidatorFinding> {
        self.findings
            .iter()
            .filter(|finding| finding.severity != "warning")
    }

    fn status(&self) -> &'static str {
        if self.error_count() == 0 {
            "valid"
        } else {
            "invalid"
        }
    }

    fn findings_json(&self) -> Vec<Value> {
        self.findings
            .iter()
            .map(ValidatorFinding::to_json)
            .collect()
    }

    fn summary_json(&self) -> Value {
        json!({
            "status": self.status(),
            "error_count": self.error_count(),
            "warning_count": self.warning_count(),
            "producer_classes": self.producers.iter().cloned().collect::<Vec<_>>(),
            "python_telemetry_connected": self.python_telemetry_connected,
            "expected_pipeline_stages": self.expected_pipeline_stages,
            "severity_policy": ["fatal", "benchmark_failure", "observability_gap", "profiling_gap", "warning"],
            "findings": self.findings_json(),
        })
    }
}

#[derive(Debug)]
struct BenchmarkReport {
    lines: Vec<String>,
}

#[derive(Default)]
struct BenchmarkFacts {
    spans: BTreeMap<(String, String, String, String), BenchmarkPoint>,
    prompts: BTreeMap<u64, PromptBenchmarkFacts>,
}

#[derive(Default)]
struct PromptBenchmarkFacts {
    request_id: u64,
    chat_submitted: Option<BenchmarkPoint>,
    chat_completed: Option<BenchmarkPoint>,
    worker_started: Option<BenchmarkPoint>,
    worker_completed: Option<BenchmarkPoint>,
    encode_started: Option<BenchmarkPoint>,
    encode_ready: Option<BenchmarkPoint>,
    decode_started: Option<BenchmarkPoint>,
    first_token_ready: Option<BenchmarkPoint>,
    decode_ready: Option<BenchmarkPoint>,
    text_decode_started: Option<BenchmarkPoint>,
    text_decode_ready: Option<BenchmarkPoint>,
    tokens_generated: Option<u64>,
}

impl PromptBenchmarkFacts {
    fn new(request_id: u64) -> Self {
        Self {
            request_id,
            ..Self::default()
        }
    }
}

impl BenchmarkFacts {
    fn from_events(events: &[DumpLogEvent], run_id: u64) -> Self {
        let mut facts = Self::default();
        for record in events {
            if !event_matches_run_id(&record.event, run_id) {
                continue;
            }
            let point = BenchmarkPoint::from_record(record);
            let event_type = record.event.get("type").and_then(Value::as_str);
            let phase = record.event.get("phase").and_then(Value::as_str);
            let status = record.event.get("status").and_then(Value::as_str);
            if let (Some(event_type), Some(phase), Some(status)) = (event_type, phase, status) {
                facts
                    .spans
                    .entry((
                        record.channel.clone(),
                        event_type.to_owned(),
                        phase.to_owned(),
                        status.to_owned(),
                    ))
                    .or_insert_with(|| point.clone());
            }

            match (record.channel.as_str(), event_type, phase, status) {
                (
                    "myelin.chat.prompt",
                    Some("ChatProgress"),
                    Some("prompt_submitted"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = dump_log_request_id(&record.event) {
                        facts.prompt_mut(request_id).chat_submitted = Some(point);
                    }
                }
                (
                    "myelin.chat.prompt",
                    Some("ChatProgress"),
                    Some("request_completed"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = dump_log_request_id(&record.event) {
                        let prompt = facts.prompt_mut(request_id);
                        prompt.chat_completed = Some(point);
                        if prompt.tokens_generated.is_none() {
                            prompt.tokens_generated = record
                                .event
                                .get("detail")
                                .and_then(|detail| detail.get("tokens_generated"))
                                .and_then(Value::as_u64);
                        }
                    }
                }
                ("myelin.worker.prompt", Some(worker_type), _, _) => {
                    let Some(request_id) = benchmark_request_id(&record.event) else {
                        continue;
                    };
                    let prompt = facts.prompt_mut(request_id);
                    match worker_type {
                        "PromptStarted" => prompt.worker_started = Some(point),
                        "PromptCompleted" => {
                            prompt.worker_completed = Some(point);
                            if let Some(tokens) = record
                                .event
                                .get("generated_tokens")
                                .and_then(Value::as_array)
                            {
                                prompt.tokens_generated = Some(tokens.len() as u64);
                            }
                        }
                        "PromptEncodeStarted" => prompt.encode_started = Some(point),
                        "PromptEncodeReady" => prompt.encode_ready = Some(point),
                        "DecodeStarted" => prompt.decode_started = Some(point),
                        "FirstTokenReady" => prompt.first_token_ready = Some(point),
                        "DecodeReady" => prompt.decode_ready = Some(point),
                        "TextDecodeStarted" => prompt.text_decode_started = Some(point),
                        "TextDecodeReady" => prompt.text_decode_ready = Some(point),
                        _ => {}
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_encode"),
                    Some("started"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        let prompt = facts.prompt_mut(request_id);
                        if prompt.encode_started.is_none() {
                            prompt.encode_started = Some(point);
                        }
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_encode"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        facts.prompt_mut(request_id).encode_ready = Some(point);
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_token_in"),
                    Some("started"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        let prompt = facts.prompt_mut(request_id);
                        if prompt.decode_started.is_none() {
                            prompt.decode_started = Some(point);
                        }
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_token_out"),
                    Some("observed"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        let prompt = facts.prompt_mut(request_id);
                        if prompt.first_token_ready.is_none() {
                            prompt.first_token_ready = Some(point.clone());
                        }
                        prompt.decode_ready = Some(point);
                        prompt.tokens_generated =
                            Some(prompt.tokens_generated.unwrap_or(0).saturating_add(1));
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_decode"),
                    Some("started"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        let prompt = facts.prompt_mut(request_id);
                        if prompt.text_decode_started.is_none() {
                            prompt.text_decode_started = Some(point);
                        }
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_decode"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        facts.prompt_mut(request_id).text_decode_ready = Some(point);
                    }
                }
                (
                    "myelin.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("prompt_complete"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        facts.prompt_mut(request_id).worker_completed = Some(point);
                    }
                }
                _ => {}
            }
        }
        facts
    }

    fn prompt_mut(&mut self, request_id: u64) -> &mut PromptBenchmarkFacts {
        self.prompts
            .entry(request_id)
            .or_insert_with(|| PromptBenchmarkFacts::new(request_id))
    }

    fn span_point(
        &self,
        channel: &str,
        event_type: &str,
        phase: &str,
        status: &str,
    ) -> Option<&BenchmarkPoint> {
        self.spans.get(&(
            channel.to_owned(),
            event_type.to_owned(),
            phase.to_owned(),
            status.to_owned(),
        ))
    }

    fn require_span(
        &self,
        channel: &str,
        event_type: &str,
        phase: &str,
        status: &str,
    ) -> Result<(), String> {
        if self
            .span_point(channel, event_type, phase, status)
            .is_some()
        {
            Ok(())
        } else {
            Err(missing_benchmark_event(format!(
                "{channel}/{event_type}/{phase}/{status}"
            )))
        }
    }
}

impl BenchmarkPoint {
    fn from_record(record: &DumpLogEvent) -> Self {
        let benchmark = record.event.get("benchmark");
        Self {
            component: benchmark
                .and_then(|value| value.get("component"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            wall_unix_ms: benchmark
                .and_then(|value| value.get("wall_unix_ms"))
                .and_then(Value::as_u64),
            mono_ms: benchmark
                .and_then(|value| value.get("mono_ms"))
                .and_then(Value::as_u64),
            arrival_unix_ms: record.arrival_unix_ms,
        }
    }
}

#[derive(Clone, Copy)]
struct DurationRender {
    value_ms: Option<u64>,
    clock_skew: bool,
}

fn parse_dump_log_events(path: &Path) -> Result<Vec<DumpLogEvent>, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("myelin-chat-check: read dump log {}: {e}", path.display()))?;
    if content.lines().next().is_none() {
        return Err(format!(
            "myelin-chat-check: dump log {} is empty",
            path.display()
        ));
    }

    let mut events = Vec::new();
    for (line_index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let outer: Value = serde_json::from_str(line).map_err(|e| {
            format!(
                "myelin-chat-check: parse dump log {} line {}: {e}",
                path.display(),
                line_index + 1
            )
        })?;
        let channel = outer
            .get("channel")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "myelin-chat-check: dump log line {} missing channel",
                    line_index + 1
                )
            })?
            .to_owned();
        let payload = outer.get("payload").ok_or_else(|| {
            format!(
                "myelin-chat-check: dump log line {} missing payload",
                line_index + 1
            )
        })?;
        let Some(inner_text) = dump_log_inner_payload_text(payload, line_index)? else {
            continue;
        };
        let event: Value = serde_json::from_str(inner_text).map_err(|e| {
            format!(
                "myelin-chat-check: parse inner event on dump log line {}: {e}",
                line_index + 1
            )
        })?;
        events.push(DumpLogEvent {
            line_number: line_index + 1,
            arrival_seq: outer.get("arrival_seq").and_then(Value::as_u64),
            source: outer
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            stream: outer
                .get("stream")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            channel,
            channel_id: outer.get("channel_id").and_then(Value::as_u64),
            position: outer.get("position").and_then(Value::as_u64),
            payload_encoding: payload
                .get("encoding")
                .and_then(Value::as_str)
                .or_else(|| payload.as_str().map(|_| "utf8"))
                .map(str::to_owned),
            arrival_unix_ms: outer.get("arrival_unix_ms").and_then(Value::as_u64),
            event,
        });
    }
    Ok(events)
}

fn dump_log_inner_payload_text<'a>(
    payload: &'a Value,
    line_index: usize,
) -> Result<Option<&'a str>, String> {
    if let Some(text) = payload.as_str() {
        return Ok(Some(text));
    }
    if payload.get("encoding").and_then(Value::as_str) != Some("utf8") {
        return Ok(None);
    }
    payload
        .get("value")
        .and_then(Value::as_str)
        .map(Some)
        .ok_or_else(|| {
            format!(
                "myelin-chat-check: dump log line {} missing utf8 payload value",
                line_index + 1
            )
        })
}

fn validate_benchmark_observability(
    events: &[DumpLogEvent],
    run_id: u64,
    scenario: MyelinChatCheckScenario,
    expected_pipeline_stages: Option<u32>,
) -> BenchmarkValidation {
    let mut validation = BenchmarkValidation {
        expected_pipeline_stages: expected_pipeline_stages
            .or_else(|| scenario.default_pipeline_stages()),
        ..BenchmarkValidation::default()
    };
    let mut provider_stages = BTreeSet::new();
    let mut endpoint_mask = None::<String>;
    let mut relay_only_direct_addr_events = Vec::new();
    let mut relay_only_missing_relay_events = Vec::new();
    let mut producer_sequences = BTreeMap::<String, u64>::new();

    for record in events {
        if record.arrival_seq.is_none() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.outer.arrival_seq.missing",
                    format!(
                        "dump log line {} is missing arrival_seq",
                        record.line_number
                    ),
                )
                .at_event(record)
                .pointer("/arrival_seq"),
            );
        }
        if record.stream.is_empty() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.outer.stream.missing",
                    format!("dump log line {} is missing stream", record.line_number),
                )
                .at_event(record)
                .pointer("/stream"),
            );
        }
        if record.channel_id.is_none() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.outer.channel_id.missing",
                    format!("dump log line {} is missing channel_id", record.line_number),
                )
                .at_event(record)
                .pointer("/channel_id"),
            );
        }
        if record.position.is_none() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.outer.position.missing",
                    format!("dump log line {} is missing position", record.line_number),
                )
                .at_event(record)
                .pointer("/position"),
            );
        }
        if record.payload_encoding.as_deref() != Some("utf8") {
            validation.push(
                ValidatorFinding::error(
                    "canonical.outer.payload_encoding.invalid",
                    format!(
                        "dump log line {} does not carry a utf8 JSON payload",
                        record.line_number
                    ),
                )
                .at_event(record)
                .pointer("/payload/encoding"),
            );
        }

        if let Some(event_run_id) = record.event.get("run_id").and_then(Value::as_u64)
            && event_run_id != run_id
        {
            validation.push(
                ValidatorFinding::fatal(
                    "run_id.isolation.mismatch",
                    format!("event run_id {event_run_id} does not match benchmark run_id {run_id}"),
                )
                .at_event(record)
                .pointer("/run_id"),
            );
            continue;
        }
        if !event_matches_run_id(&record.event, run_id) {
            continue;
        }

        let event_type = record.event.get("type").and_then(Value::as_str);
        let phase = record.event.get("phase").and_then(Value::as_str);
        let status = record.event.get("status").and_then(Value::as_str);

        if canonical_benchmark_required(record) {
            validate_event_canonical_stamp(record, &mut validation);
        } else if record.channel.starts_with("myelin.") && event_type.is_some() {
            validation.push(
                ValidatorFinding::warning(
                    "canonical.benchmark_stamp.not_required",
                    "typed myelin telemetry event was not part of the strict benchmark validator set",
                )
                .at_event(record),
            );
        }

        if let Some(component) = record
            .event
            .get("benchmark")
            .and_then(|benchmark| benchmark.get("producer_component"))
            .or_else(|| {
                record
                    .event
                    .get("benchmark")
                    .and_then(|benchmark| benchmark.get("component"))
            })
            .and_then(Value::as_str)
        {
            validation.producers.insert(component.to_owned());
        }
        if let (Some(instance), Some(sequence)) = (
            record
                .event
                .get("producer_instance_id")
                .and_then(Value::as_str),
            record
                .event
                .get("producer_sequence")
                .and_then(Value::as_u64),
        ) {
            if producer_sequences
                .insert(instance.to_owned(), sequence)
                .is_some_and(|previous| sequence <= previous)
            {
                validation.push(
                    ValidatorFinding::fatal(
                        "producer_sequence.non_monotonic",
                        format!("producer {instance} emitted non-monotonic sequence {sequence}"),
                    )
                    .at_event(record)
                    .pointer("/producer_sequence"),
                );
            }
        }

        match (record.channel.as_str(), event_type, phase, status) {
            (
                "myelin.chat.benchmark",
                Some("BenchmarkRunEnvelope"),
                Some("run_envelope"),
                Some("ready"),
            ) => {
                validation.run_envelope_present = true;
                endpoint_mask = record
                    .event
                    .get("detail")
                    .and_then(|detail| detail.get("runtime"))
                    .and_then(|runtime| runtime.get("endpoint_addr_mask"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            (_, _, Some("endpoint_config_snapshot" | "telemetry_preflight"), _) => {
                validation.endpoint_snapshot_present = true;
            }
            (
                "myelin.worker.initialize",
                Some("PythonTelemetryConnected"),
                Some("PythonTelemetryConnected"),
                Some("ready"),
            ) => {
                validation.python_telemetry_connected = true;
            }
            (_, Some("OrchBootstrap"), Some("provider_start"), Some("started")) => {
                if detail_str(&record.event, "provider") == Some("vastai")
                    && let Some(stage_index) = detail_u64(&record.event, "stage_index")
                {
                    provider_stages.insert(stage_index);
                }
            }
            (_, Some("OrchBootstrap"), Some("node_runtime_ready"), Some("ready")) => {
                if let Some(stage_index) = event_stage_index(&record.event) {
                    validation
                        .stages_runtime_ready_via_orchestrator
                        .insert(stage_index);
                }
            }
            (_, Some("OrchBootstrap"), Some("stage_route_check"), Some("observed"))
                if detail_bool(&record.event, "route_matches_ready") == Some(true)
                    && detail_str(&record.event, "member_state") == Some("Alive") =>
            {
                if let Some(stage_index) = event_stage_index(&record.event) {
                    validation
                        .stages_route_ready_via_orchestrator
                        .insert(stage_index);
                }
            }
            (_, Some("NodeEvent"), Some("iroh_driver"), Some("ready")) => {
                if let Some(stage_index) = event_stage_index(&record.event) {
                    validation.stages_ready.insert(stage_index);
                }
                if detail_str(&record.event, "endpoint_addr_mask") == Some("relay-only") {
                    if detail_u64(&record.event, "direct_addr_count").unwrap_or(u64::MAX) != 0 {
                        relay_only_direct_addr_events.push(record.line_number);
                    }
                    if record
                        .event
                        .get("detail")
                        .and_then(|detail| detail.get("has_relay"))
                        .and_then(Value::as_bool)
                        != Some(true)
                    {
                        relay_only_missing_relay_events.push(record.line_number);
                    }
                }
            }
            (_, Some("NodeEvent"), Some("worker_initialize"), Some("ready")) => {
                if let Some(stage_index) = event_stage_index(&record.event) {
                    validation.stages_with_worker.insert(stage_index);
                }
            }
            (_, Some("WorkerReady"), _, _) | (_, Some("TinygradDeviceProbeReady"), _, _) => {
                if let Some(stage_index) = event_stage_index(&record.event) {
                    validation.stages_with_device.insert(stage_index);
                }
            }
            (_, Some("NodeEvent"), Some("egress_ring_read" | "iroh_edge_bytes_sent"), _) => {
                if let Some(edge_id) = event_edge_id(&record.event) {
                    validation.edges_with_producer.insert(edge_id);
                }
            }
            (_, Some("RingInstalled"), _, _) => {
                if let Some(edge_id) = event_edge_id(&record.event) {
                    match record.event.get("direction").and_then(Value::as_str) {
                        Some("egress") => {
                            validation.edges_with_producer.insert(edge_id);
                        }
                        Some("ingress") => {
                            validation.edges_with_consumer.insert(edge_id);
                        }
                        _ => {}
                    }
                }
            }
            (_, Some("ObjectLoaded"), _, _) => {
                let extent = record
                    .event
                    .get("extent")
                    .and_then(Value::as_u64)
                    .or_else(|| detail_u64(&record.event, "extent"))
                    .unwrap_or(0);
                if record.event.get("kind").and_then(Value::as_str) == Some("activation")
                    || detail_str(&record.event, "kind") == Some("activation")
                    || extent >= DATA_PATH_MIN_PAYLOAD_BYTES
                {
                    validation.max_activation_record_bytes =
                        validation.max_activation_record_bytes.max(extent);
                    if event_stage_index(&record.event).is_some_and(|stage_index| stage_index > 0) {
                        validation.activation_downstream_object_loaded = true;
                    }
                }
                if let Some(edge_id) = event_edge_id(&record.event) {
                    validation.edges_with_consumer.insert(edge_id);
                }
            }
            (_, Some("NodeEvent"), Some("ingress_ring_write" | "object_loaded"), _) => {
                if let Some(edge_id) = event_edge_id(&record.event) {
                    validation.edges_with_consumer.insert(edge_id);
                }
            }
            (
                "myelin.chat.prompt",
                Some("ChatProgress"),
                Some("prompt_submitted"),
                Some("ready"),
            ) => {
                if let Some(request_id) = benchmark_request_id(&record.event) {
                    validation.requests_started.insert(request_id);
                }
            }
            (
                "myelin.chat.prompt",
                Some("ChatProgress"),
                Some("request_completed"),
                Some("ready"),
            ) => {
                if let Some(request_id) = benchmark_request_id(&record.event) {
                    validation.requests_completed.insert(request_id);
                }
            }
            _ => {}
        }
    }

    if !validation.run_envelope_present {
        validation.push(ValidatorFinding::error(
            "run_envelope.missing",
            "BenchmarkRunEnvelope ready event is required before a benchmark summary is valid",
        ));
    }
    if !validation.endpoint_snapshot_present {
        validation.push(ValidatorFinding::error(
            "endpoint_config_snapshot.missing",
            "endpoint/telemetry configuration snapshot is required for benchmark attribution",
        ));
    }
    for required in [
        "myelin-chat",
        "myelin-orchestrator",
        "myelin-worker",
        "tinygrad-worker",
    ] {
        if !validation.producers.contains(required) {
            validation.push(ValidatorFinding::fatal(
                "producer.connectivity.missing",
                format!("required producer {required} did not emit canonical telemetry evidence"),
            ));
        }
    }
    if !validation.python_telemetry_connected {
        validation.push(ValidatorFinding::error(
            "python.telemetry.connected.missing",
            "Python worker did not emit PythonTelemetryConnected through the canonical telemetry",
        ));
    }
    for request_id in [1_u64, 2] {
        if !validation.requests_completed.contains(&request_id) {
            validation.push(ValidatorFinding {
                request_id: Some(request_id),
                ..ValidatorFinding::error(
                    "request.terminal_event.missing",
                    format!("request {request_id} is missing RequestCompleted evidence"),
                )
            });
        }
    }
    for request_id in validation
        .requests_started
        .difference(&validation.requests_completed)
        .copied()
        .collect::<Vec<_>>()
    {
        validation.push(ValidatorFinding {
            request_id: Some(request_id),
            ..ValidatorFinding::error(
                "request.started_without_terminal",
                format!("request {request_id} started but has no terminal completion event"),
            )
        });
    }

    if scenario == MyelinChatCheckScenario::VastAi {
        if endpoint_mask.as_deref() != Some("relay-only") {
            validation.push(ValidatorFinding::error(
                "vastai.endpoint_mask.not_relay_only",
                "VastAI benchmark checks must declare relay-only endpoint masking",
            ));
        }
        for line_number in relay_only_direct_addr_events {
            validation.push(ValidatorFinding::error(
                "vastai.endpoint.direct_addrs_present",
                format!("relay-only worker endpoint on dump log line {line_number} exposed direct addresses"),
            ));
        }
        for line_number in relay_only_missing_relay_events {
            validation.push(ValidatorFinding::error(
                "vastai.endpoint.relay_missing",
                format!(
                    "relay-only worker endpoint on dump log line {line_number} had no relay URL"
                ),
            ));
        }
        if let Some(expected) = validation.expected_pipeline_stages {
            for stage_index in 0..u64::from(expected) {
                if !provider_stages.contains(&stage_index) {
                    validation.push(ValidatorFinding {
                        stage_index: Some(stage_index),
                        ..ValidatorFinding::error(
                            "vastai.stage.provider_start.missing",
                            format!(
                                "stage {stage_index} is missing VastAI provider_start evidence"
                            ),
                        )
                    });
                }
                if !validation.stages_ready.contains(&stage_index)
                    && !validation
                        .stages_runtime_ready_via_orchestrator
                        .contains(&stage_index)
                {
                    validation.push(ValidatorFinding {
                        stage_index: Some(stage_index),
                        ..ValidatorFinding::error(
                            "stage.iroh_ready.missing",
                            format!(
                                "stage {stage_index} is missing worker iroh_driver or orchestrator node_runtime_ready evidence"
                            ),
                        )
                    });
                }
                if !validation.stages_with_worker.contains(&stage_index)
                    && !validation
                        .stages_runtime_ready_via_orchestrator
                        .contains(&stage_index)
                {
                    validation.push(ValidatorFinding {
                        stage_index: Some(stage_index),
                        ..ValidatorFinding::error(
                            "stage.worker_initialize.missing",
                            format!(
                                "stage {stage_index} is missing worker_initialize or orchestrator node_runtime_ready evidence"
                            ),
                        )
                    });
                }
                if !validation.stages_with_device.contains(&stage_index)
                    && !validation
                        .stages_route_ready_via_orchestrator
                        .contains(&stage_index)
                {
                    validation.push(ValidatorFinding {
                        stage_index: Some(stage_index),
                        ..ValidatorFinding::error(
                            "stage.device_ready.missing",
                            format!(
                                "stage {stage_index} is missing Python device/WorkerReady or orchestrator route-ready evidence"
                            ),
                        )
                    });
                }
            }
            let expected_edges = expected.saturating_sub(1) as usize;
            let complete_edges = validation
                .edges_with_producer
                .intersection(&validation.edges_with_consumer)
                .count();
            let downstream_activation = validation.activation_downstream_object_loaded
                && validation.max_activation_record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES;
            if complete_edges < expected_edges && !downstream_activation {
                validation.push(ValidatorFinding::error(
                    "pipeline.edge_handoff.incomplete",
                    format!(
                        "expected {expected_edges} complete inter-stage handoffs, observed {complete_edges}"
                    ),
                ));
            }
        }
    }

    validation
}

fn canonical_benchmark_required(record: &DumpLogEvent) -> bool {
    let channel = record.channel.as_str();
    channel.starts_with("myelin.chat.")
        || channel.starts_with("myelin.orch.")
        || channel.starts_with("myelin.worker.")
        || channel.starts_with("myelin.node.")
        || channel == "myelin.xtask.benchmark"
}

fn validate_event_canonical_stamp(record: &DumpLogEvent, validation: &mut BenchmarkValidation) {
    for (key, pointer) in [
        ("producer_component", "/producer_component"),
        ("producer_instance_id", "/producer_instance_id"),
        ("producer_process_id", "/producer_process_id"),
        ("producer_sequence", "/producer_sequence"),
        ("wall_clock_unix_ms", "/wall_clock_unix_ms"),
        ("monotonic_ms", "/monotonic_ms"),
        ("clock_source", "/clock_source"),
        ("span_id", "/span_id"),
    ] {
        if record.event.get(key).is_none() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.event_field.missing",
                    format!("canonical event field {key} is missing"),
                )
                .at_event(record)
                .pointer(pointer),
            );
        }
    }

    let Some(benchmark) = record.event.get("benchmark") else {
        validation.push(
            ValidatorFinding::error(
                "canonical.benchmark_stamp.missing",
                "event is missing nested benchmark stamp",
            )
            .at_event(record)
            .pointer("/benchmark"),
        );
        return;
    };
    for (key, pointer) in [
        ("schema", "/benchmark/schema"),
        ("schema_version", "/benchmark/schema_version"),
        ("component", "/benchmark/component"),
        ("producer_component", "/benchmark/producer_component"),
        ("producer_instance_id", "/benchmark/producer_instance_id"),
        ("producer_process_id", "/benchmark/producer_process_id"),
        ("producer_sequence", "/benchmark/producer_sequence"),
        ("wall_unix_ms", "/benchmark/wall_unix_ms"),
        ("wall_clock_unix_ms", "/benchmark/wall_clock_unix_ms"),
        ("mono_ms", "/benchmark/mono_ms"),
        ("monotonic_ms", "/benchmark/monotonic_ms"),
        ("clock_source", "/benchmark/clock_source"),
    ] {
        if benchmark.get(key).is_none() {
            validation.push(
                ValidatorFinding::error(
                    "canonical.benchmark_stamp_field.missing",
                    format!("benchmark stamp field {key} is missing"),
                )
                .at_event(record)
                .pointer(pointer),
            );
        }
    }
}

fn event_stage_index(event: &Value) -> Option<u64> {
    event
        .get("stage_index")
        .and_then(Value::as_u64)
        .or_else(|| detail_u64(event, "stage_index"))
}

fn event_edge_id(event: &Value) -> Option<u64> {
    event
        .get("edge_id")
        .and_then(Value::as_u64)
        .or_else(|| detail_u64(event, "edge_id"))
}

fn build_benchmark_evidence_json(
    validation: &BenchmarkValidation,
    events: &[DumpLogEvent],
    run_id: u64,
    scenario: MyelinChatCheckScenario,
    paths: &MyelinChatCheckPaths,
) -> Value {
    json!({
        "schema": "swactor.myelin_chat.benchmark_evidence.v1",
        "source": "canonical_telemetry",
        "run_id": run_id,
        "scenario": scenario.name(),
        "created_unix_ms": unix_ms_now(),
        "validator": validation.summary_json(),
        "producers": validation.producers,
        "coverage": {
            "expected_pipeline_stages": validation.expected_pipeline_stages,
            "stages_ready": validation.stages_ready,
            "stages_with_worker": validation.stages_with_worker,
            "stages_with_device": validation.stages_with_device,
            "stages_runtime_ready_via_orchestrator": validation.stages_runtime_ready_via_orchestrator,
            "stages_route_ready_via_orchestrator": validation.stages_route_ready_via_orchestrator,
            "activation_downstream_object_loaded": validation.activation_downstream_object_loaded,
            "max_activation_record_bytes": validation.max_activation_record_bytes,
            "edges_with_producer": validation.edges_with_producer,
            "edges_with_consumer": validation.edges_with_consumer,
            "requests_started": validation.requests_started,
            "requests_completed": validation.requests_completed,
            "run_envelope_present": validation.run_envelope_present,
            "endpoint_snapshot_present": validation.endpoint_snapshot_present,
        },
        "artifacts": {
            "telemetry": paths.dump_log.display().to_string(),
            "summary": paths.summary.display().to_string(),
            "gaps": paths.benchmark_gaps.display().to_string(),
        },
        "event_index": events.iter().map(|record| {
            json!({
                "line": record.line_number,
                "arrival_seq": record.arrival_seq,
                "source": record.source,
                "stream": record.stream,
                "channel": record.channel,
                "channel_id": record.channel_id,
                "position": record.position,
                "type": record.event.get("type").and_then(Value::as_str),
                "phase": record.event.get("phase").and_then(Value::as_str),
                "status": record.event.get("status").and_then(Value::as_str),
                "request_id": benchmark_request_id(&record.event),
                "stage_index": event_stage_index(&record.event),
            })
        }).collect::<Vec<_>>(),
    })
}

fn build_benchmark_gaps_markdown(validation: &BenchmarkValidation) -> String {
    let mut out = String::new();
    out.push_str("# Benchmark observability gaps\n\n");
    out.push_str(&format!(
        "Status: {}. Errors: {}. Warnings: {}.\n\n",
        validation.status(),
        validation.error_count(),
        validation.warning_count()
    ));
    if validation.findings.is_empty() {
        out.push_str("No benchmark observability gaps detected.\n");
        return out;
    }
    for severity in ["error", "warning"] {
        let matching = validation
            .findings
            .iter()
            .filter(|finding| finding.severity == severity)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            continue;
        }
        out.push_str(&format!("## {severity}s\n\n"));
        for finding in matching {
            out.push_str(&format!("- `{}`: {}", finding.code, finding.message));
            if let Some(channel) = &finding.channel {
                out.push_str(&format!(" channel={channel}"));
            }
            if let Some(phase) = &finding.phase {
                out.push_str(&format!(" phase={phase}"));
            }
            if let Some(status) = &finding.status {
                out.push_str(&format!(" status={status}"));
            }
            if let Some(request_id) = finding.request_id {
                out.push_str(&format!(" request_id={request_id}"));
            }
            if let Some(stage_index) = finding.stage_index {
                out.push_str(&format!(" stage_index={stage_index}"));
            }
            if let Some(pointer) = &finding.json_pointer {
                out.push_str(&format!(" pointer={pointer}"));
            }
            out.push('\n');
        }
        out.push('\n');
    }
    out
}

fn assert_dump_log_facts(
    path: &Path,
    scenario: MyelinChatCheckScenario,
    run_id: u64,
    expected_pipeline_stages: Option<u32>,
) -> Result<Vec<DumpLogEvent>, String> {
    let events = parse_dump_log_events(path)?;
    let mut facts = DumpLogFacts::default();
    for record in &events {
        let _source = record.source.as_str();
        record_dump_log_event(scenario, &record.channel, &record.event, &mut facts)?;
    }

    require_dump_log_fact(facts.chat_config_ready, "config ready")?;
    require_dump_log_fact(facts.prepare_runtime_ready, "prepare_runtime ready")?;
    require_dump_log_fact(facts.prompt_rpc_ready, "prompt_rpc ready")?;
    require_dump_log_fact(
        facts.orch_iroh_driver_ready,
        "OrchBootstrap iroh_driver ready",
    )?;
    require_dump_log_fact(facts.node_iroh_driver_ready, "NodeEvent iroh_driver ready")?;
    require_dump_log_fact(
        facts.node_worker_initialize_ready,
        "NodeEvent worker_initialize ready",
    )?;
    require_dump_log_fact(
        facts.orch_weights_loaded_ready,
        "OrchBootstrap weights_loaded ready",
    )?;
    require_dump_log_fact(facts.response_text_1, "response_text request_id=1")?;
    require_dump_log_fact(facts.request_completed_1, "request_completed request_id=1")?;
    require_dump_log_fact(facts.response_text_2, "response_text request_id=2")?;
    require_dump_log_fact(facts.request_completed_2, "request_completed request_id=2")?;
    require_dump_log_fact(facts.shutdown_requested, "shutdown requested")?;
    require_dump_log_fact(facts.orchestrator_stopped, "orchestrator_process stopped")?;
    if scenario == MyelinChatCheckScenario::Gpu {
        require_gpu_dump_log_facts(&facts)?;
    }
    if scenario == MyelinChatCheckScenario::MultinodeDocker {
        require_multinode_docker_network_facts(&facts)?;
    }
    if scenario == MyelinChatCheckScenario::VastAi {
        require_gpu_dump_log_facts(&facts)?;
        require_vastai_network_facts(&facts)?;
        require_vastai_data_path_facts(&facts)?;
    }
    let validation =
        validate_benchmark_observability(&events, run_id, scenario, expected_pipeline_stages);
    if let Some(finding) = validation.invalid_findings().next() {
        return Err(format!(
            "myelin-chat-check: benchmark observability gap {}: {}",
            finding.code, finding.message
        ));
    }
    Ok(events)
}

fn build_benchmark_report(
    events: &[DumpLogEvent],
    child_elapsed_ms: u64,
    run_id: u64,
    scenario: MyelinChatCheckScenario,
) -> Result<BenchmarkReport, String> {
    let facts = BenchmarkFacts::from_events(events, run_id);
    facts.require_span(
        "myelin.xtask.benchmark",
        "XtaskBenchmark",
        "cargo_run_myelin_chat",
        "started",
    )?;
    facts.require_span(
        "myelin.xtask.benchmark",
        "XtaskBenchmark",
        "cargo_run_myelin_chat",
        "ready",
    )?;
    facts.require_span(
        "myelin.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "started",
    )?;
    facts.require_span(
        "myelin.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "ready",
    )?;
    if scenario == MyelinChatCheckScenario::Gpu {
        facts.require_span(
            "myelin.chat.runtime",
            "ChatProgress",
            "ensure_orchestrator_actor",
            "ready",
        )?;
    } else {
        facts.require_span(
            "myelin.chat.runtime",
            "ChatProgress",
            "ensure_orch_binary",
            "ready",
        )?;
    }
    if !matches!(
        scenario,
        MyelinChatCheckScenario::MultinodeDocker | MyelinChatCheckScenario::VastAi
    ) {
        facts.require_span(
            "myelin.chat.runtime",
            "ChatProgress",
            "ensure_worker_binary",
            "ready",
        )?;
    }
    facts.require_span(
        "myelin.orch.bootstrap",
        "OrchBootstrap",
        "weights_loaded",
        "ready",
    )?;
    facts.require_span("myelin.chat.runtime", "ChatProgress", "prompt_rpc", "ready")?;

    for request_id in 1..=2 {
        let prompt = facts.prompts.get(&request_id).ok_or_else(|| {
            missing_benchmark_event(format!(
                "myelin.chat.prompt/ChatProgress/request_completed/ready request_id={request_id}"
            ))
        })?;
        require_prompt_point(
            prompt.chat_completed.is_some(),
            "myelin.chat.prompt/ChatProgress/request_completed/ready",
            request_id,
        )?;
        require_prompt_point(
            prompt.encode_ready.is_some(),
            "myelin.worker.prompt/PromptEncodeReady or myelin.orch.prompt/pipeline_tokenizer_encode/ready",
            request_id,
        )?;
        require_prompt_point(
            prompt.decode_started.is_some(),
            "myelin.worker.prompt/DecodeStarted or myelin.orch.prompt/pipeline_token_in/started",
            request_id,
        )?;
        require_prompt_point(
            prompt.first_token_ready.is_some(),
            "myelin.worker.prompt/FirstTokenReady or myelin.orch.prompt/pipeline_token_out/observed",
            request_id,
        )?;
        require_prompt_point(
            prompt.decode_ready.is_some(),
            "myelin.worker.prompt/DecodeReady or myelin.orch.prompt/pipeline_token_out/observed",
            request_id,
        )?;
        require_prompt_point(
            prompt.text_decode_ready.is_some(),
            "myelin.worker.prompt/TextDecodeReady or myelin.orch.prompt/pipeline_tokenizer_decode/ready",
            request_id,
        )?;
    }

    let cargo_run = duration_between(
        facts.span_point(
            "myelin.xtask.benchmark",
            "XtaskBenchmark",
            "cargo_run_myelin_chat",
            "started",
        ),
        facts.span_point(
            "myelin.xtask.benchmark",
            "XtaskBenchmark",
            "cargo_run_myelin_chat",
            "ready",
        ),
    );
    let prepare_runtime = duration_between(
        facts.span_point(
            "myelin.chat.runtime",
            "ChatProgress",
            "prepare_runtime",
            "started",
        ),
        facts.span_point(
            "myelin.chat.runtime",
            "ChatProgress",
            "prepare_runtime",
            "ready",
        ),
    );
    let standup_start = facts.span_point(
        "myelin.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "ready",
    );
    let weights_loaded = facts.span_point(
        "myelin.orch.bootstrap",
        "OrchBootstrap",
        "weights_loaded",
        "ready",
    );
    let prompt_rpc = facts.span_point("myelin.chat.runtime", "ChatProgress", "prompt_rpc", "ready");
    let standup_to_weights = duration_between(standup_start, weights_loaded);
    let standup_to_prompt_rpc = duration_between(standup_start, prompt_rpc);

    let mut lines = vec![
        format!("myelin-chat-check: benchmark: run_id={run_id}"),
        format!("myelin-chat-check: benchmark total_child_ms={child_elapsed_ms}"),
        benchmark_span_line("cargo_run_myelin_chat_ms", cargo_run),
        benchmark_span_line("prepare_runtime_ms", prepare_runtime),
        benchmark_span_line("standup_to_weights_loaded_ms", standup_to_weights),
        benchmark_span_line("standup_to_prompt_rpc_ms", standup_to_prompt_rpc),
    ];
    for request_id in 1..=2 {
        let prompt = facts
            .prompts
            .get(&request_id)
            .expect("prompt facts were required above");
        lines.push(prompt_benchmark_line(prompt));
    }
    Ok(BenchmarkReport { lines })
}

fn benchmark_span_line(name: &str, duration: DurationRender) -> String {
    let mut line = format!(
        "myelin-chat-check: benchmark {name}={}",
        render_duration_value(duration)
    );
    if duration.clock_skew {
        line.push_str(" clock_skew=true");
    }
    line
}

fn prompt_benchmark_line(prompt: &PromptBenchmarkFacts) -> String {
    let roundtrip = duration_between(
        prompt.chat_submitted.as_ref(),
        prompt.chat_completed.as_ref(),
    );
    let worker_start = prompt
        .worker_started
        .as_ref()
        .or(prompt.encode_started.as_ref());
    let worker_end = prompt
        .worker_completed
        .as_ref()
        .or(prompt.chat_completed.as_ref());
    let worker_total = duration_between(worker_start, worker_end);
    let encode = duration_between(prompt.encode_started.as_ref(), prompt.encode_ready.as_ref());
    let first_token = duration_between(
        prompt.decode_started.as_ref(),
        prompt.first_token_ready.as_ref(),
    );
    let decode = duration_between(prompt.decode_started.as_ref(), prompt.decode_ready.as_ref());
    let text_decode = duration_between(
        prompt.text_decode_started.as_ref(),
        prompt.text_decode_ready.as_ref(),
    );
    let mut fields = vec![
        format!("roundtrip_ms={}", render_duration_value(roundtrip)),
        format!("worker_total_ms={}", render_duration_value(worker_total)),
        format!("encode_ms={}", render_duration_value(encode)),
        format!("first_token_ms={}", render_duration_value(first_token)),
        format!("decode_ms={}", render_duration_value(decode)),
        format!("text_decode_ms={}", render_duration_value(text_decode)),
        format!(
            "tokens_generated={}",
            prompt
                .tokens_generated
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_owned())
        ),
        format!(
            "tokens_per_sec={}",
            tokens_per_sec(prompt.tokens_generated, decode.value_ms)
        ),
    ];
    if [
        roundtrip,
        worker_total,
        encode,
        first_token,
        decode,
        text_decode,
    ]
    .iter()
    .any(|duration| duration.clock_skew)
    {
        fields.push("clock_skew=true".to_owned());
    }
    format!(
        "myelin-chat-check: benchmark prompt {} {}",
        prompt.request_id,
        fields.join(" ")
    )
}

fn tokens_per_sec(tokens_generated: Option<u64>, decode_ms: Option<u64>) -> String {
    match (tokens_generated, decode_ms) {
        (Some(tokens), Some(ms)) if tokens > 0 && ms > 0 => {
            format!("{:.2}", tokens as f64 / (ms as f64 / 1000.0))
        }
        _ => "unavailable".to_owned(),
    }
}

fn render_duration_value(duration: DurationRender) -> String {
    duration
        .value_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn duration_between(
    start: Option<&BenchmarkPoint>,
    end: Option<&BenchmarkPoint>,
) -> DurationRender {
    let (Some(start), Some(end)) = (start, end) else {
        return DurationRender {
            value_ms: None,
            clock_skew: false,
        };
    };
    if start.component.is_some()
        && start.component == end.component
        && let (Some(start_ms), Some(end_ms)) = (start.mono_ms, end.mono_ms)
    {
        return DurationRender {
            value_ms: end_ms.checked_sub(start_ms),
            clock_skew: end_ms < start_ms,
        };
    }
    if let (Some(start_ms), Some(end_ms)) = (start.wall_unix_ms, end.wall_unix_ms) {
        if let Some(value_ms) = end_ms.checked_sub(start_ms) {
            return DurationRender {
                value_ms: Some(value_ms),
                clock_skew: false,
            };
        }
        return DurationRender {
            value_ms: match (start.arrival_unix_ms, end.arrival_unix_ms) {
                (Some(start_arrival), Some(end_arrival)) => end_arrival.checked_sub(start_arrival),
                _ => None,
            },
            clock_skew: true,
        };
    }
    DurationRender {
        value_ms: match (start.arrival_unix_ms, end.arrival_unix_ms) {
            (Some(start_arrival), Some(end_arrival)) => end_arrival.checked_sub(start_arrival),
            _ => None,
        },
        clock_skew: false,
    }
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn build_benchmark_summary(
    events: &[DumpLogEvent],
    child_elapsed_ms: u64,
    run_id: u64,
    scenario: MyelinChatCheckScenario,
    paths: &MyelinChatCheckPaths,
    expected_pipeline_stages: Option<u32>,
    stdout_bytes: u64,
    stderr_bytes: u64,
) -> Result<Value, String> {
    let facts = BenchmarkFacts::from_events(events, run_id);
    let mut dump_facts = DumpLogFacts::default();
    for record in events {
        record_dump_log_event(scenario, &record.channel, &record.event, &mut dump_facts)?;
    }
    let telemetry_bytes = file_size(&paths.dump_log)?;
    let prompt_bytes = u64::try_from(MYELIN_CHAT_CHECK_PROMPTS.len()).unwrap_or(u64::MAX);
    let known_artifact_bytes = telemetry_bytes
        .saturating_add(stdout_bytes)
        .saturating_add(stderr_bytes)
        .saturating_add(prompt_bytes);
    let run_envelope = benchmark_run_envelope(events, run_id);
    let channel_counts = benchmark_channel_counts(events);
    let event_counts = benchmark_event_counts(events);
    let prompt_summaries = facts
        .prompts
        .values()
        .map(prompt_summary_json)
        .collect::<Vec<_>>();
    let validation =
        validate_benchmark_observability(events, run_id, scenario, expected_pipeline_stages);
    let summary = json!({
        "schema": "swactor.myelin_chat.benchmark_summary.v1",
        "source": "telemetry",
        "run_id": run_id,
        "scenario": scenario.name(),
        "created_unix_ms": unix_ms_now(),
        "workload": {
            "name": "myelin-chat-check",
            "input_format": "stdin_prompt_corpus",
            "prompt_count": 2,
            "prompt_bytes": MYELIN_CHAT_CHECK_PROMPTS.len(),
            "prompt_corpus_blake3": bytes_blake3_hex(MYELIN_CHAT_CHECK_PROMPTS),
            "prompts": prompt_workload_summary(events, run_id),
        },
        "artifacts": {
            "root": paths.root.display().to_string(),
            "known_total_bytes": known_artifact_bytes,
            "telemetry": {
                "path": paths.dump_log.display().to_string(),
                "bytes": telemetry_bytes,
                "blake3": file_blake3_hex(&paths.dump_log)?,
            },
            "stdout": {
                "path": paths.stdout.display().to_string(),
                "bytes": stdout_bytes,
            },
            "stderr": {
                "path": paths.stderr.display().to_string(),
                "bytes": stderr_bytes,
            },
            "prompts": {
                "path": paths.prompts.display().to_string(),
                "bytes": prompt_bytes,
                "blake3": bytes_blake3_hex(MYELIN_CHAT_CHECK_PROMPTS),
            },
            "redacted_config": {
                "path": paths.redacted_config.display().to_string(),
            },
            "summary": {
                "path": paths.summary.display().to_string(),
            },
            "benchmark_evidence": {
                "path": paths.benchmark_evidence.display().to_string(),
            },
            "benchmark_gaps": {
                "path": paths.benchmark_gaps.display().to_string(),
            },
        },
        "run_envelope": run_envelope,
        "event_counts": {
            "total": events.len(),
            "channels": channel_counts,
            "types": event_counts,
        },
        "timings": {
            "total_child_ms": child_elapsed_ms,
            "cargo_run_myelin_chat_ms": benchmark_span_json(&facts, "myelin.xtask.benchmark", "XtaskBenchmark", "cargo_run_myelin_chat", "started", "ready"),
            "prepare_runtime_ms": benchmark_span_json(&facts, "myelin.chat.runtime", "ChatProgress", "prepare_runtime", "started", "ready"),
            "standup_to_weights_loaded_ms": duration_summary_json(duration_between(
                facts.span_point("myelin.chat.runtime", "ChatProgress", "prepare_runtime", "ready"),
                facts.span_point("myelin.orch.bootstrap", "OrchBootstrap", "weights_loaded", "ready"),
            )),
            "standup_to_prompt_rpc_ms": duration_summary_json(duration_between(
                facts.span_point("myelin.chat.runtime", "ChatProgress", "prepare_runtime", "ready"),
                facts.span_point("myelin.chat.runtime", "ChatProgress", "prompt_rpc", "ready"),
            )),
            "prompts": prompt_summaries,
        },
        "pipeline": pipeline_summary_json(events, &dump_facts),
        "gpu": gpu_summary_json(events, &dump_facts),
        "vastai": vastai_summary_json(events),
        "validator": validation.summary_json(),
        "invariants": benchmark_invariants_json(&dump_facts, scenario),
        "side_channel_audit": {
            "status": "captured_not_authoritative",
            "entries": [
                {
                    "name": "stdout",
                    "role": "functional smoke transcript",
                    "telemetry_substitute": false,
                    "used_for_benchmark_metrics": false,
                    "artifact": paths.stdout.display().to_string(),
                },
                {
                    "name": "stderr",
                    "role": "debug transcript",
                    "telemetry_substitute": false,
                    "used_for_benchmark_metrics": false,
                    "artifact": paths.stderr.display().to_string(),
                }
            ],
        },
        "legacy_tolerance": legacy_tolerance_summary(events),
    });
    Ok(summary)
}

fn write_benchmark_artifacts(
    paths: &MyelinChatCheckPaths,
    run_id: u64,
    scenario: MyelinChatCheckScenario,
    events: &[DumpLogEvent],
    expected_pipeline_stages: Option<u32>,
    output: &MyelinChatCheckOutput,
    summary: &Value,
) -> Result<(), String> {
    fs::write(&paths.stdout, &output.stdout).map_err(|e| {
        format!(
            "myelin-chat-check: write stdout artifact {}: {e}",
            paths.stdout.display()
        )
    })?;
    fs::write(&paths.stderr, &output.stderr).map_err(|e| {
        format!(
            "myelin-chat-check: write stderr artifact {}: {e}",
            paths.stderr.display()
        )
    })?;
    fs::write(&paths.prompts, MYELIN_CHAT_CHECK_PROMPTS).map_err(|e| {
        format!(
            "myelin-chat-check: write prompt corpus {}: {e}",
            paths.prompts.display()
        )
    })?;
    let redacted_config = benchmark_redacted_config(summary, run_id, scenario);
    write_json_file(&paths.redacted_config, &redacted_config)?;
    let validation =
        validate_benchmark_observability(events, run_id, scenario, expected_pipeline_stages);
    let evidence = build_benchmark_evidence_json(&validation, events, run_id, scenario, paths);
    write_json_file(&paths.benchmark_evidence, &evidence)?;
    fs::write(
        &paths.benchmark_gaps,
        build_benchmark_gaps_markdown(&validation),
    )
    .map_err(|e| {
        format!(
            "myelin-chat-check: write benchmark gaps artifact {}: {e}",
            paths.benchmark_gaps.display()
        )
    })?;
    write_json_file(&paths.summary, summary)?;
    Ok(())
}

fn write_json_file(path: &Path, value: &Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec_pretty(value)
        .map_err(|e| format!("serialize benchmark artifact {}: {e}", path.display()))?;
    bytes.push(b'\n');
    fs::write(path, bytes).map_err(|e| format!("write benchmark artifact {}: {e}", path.display()))
}

fn benchmark_redacted_config(
    summary: &Value,
    run_id: u64,
    scenario: MyelinChatCheckScenario,
) -> Value {
    let mut config = summary
        .get("run_envelope")
        .and_then(|value| value.get("detail"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    redact_sensitive_values(&mut config);
    json!({
        "schema": "swactor.myelin_chat.redacted_config.v1",
        "run_id": run_id,
        "scenario": scenario.name(),
        "detail": config,
    })
}

fn redact_sensitive_values(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, child) in object.iter_mut() {
                let key_lower = key.to_ascii_lowercase();
                if child.is_string()
                    && (key_lower.contains("api_key")
                        || key_lower.contains("token")
                        || key_lower.contains("secret")
                        || key_lower.contains("password")
                        || key_lower.contains("ssh_identity")
                        || key_lower.contains("private_key")
                        || key_lower.contains("bootstrap_command"))
                {
                    *child = Value::String("<redacted>".to_owned());
                } else {
                    redact_sensitive_values(child);
                }
            }
        }
        Value::Array(items) => {
            for child in items {
                redact_sensitive_values(child);
            }
        }
        _ => {}
    }
}

fn benchmark_run_envelope(events: &[DumpLogEvent], run_id: u64) -> Value {
    events
        .iter()
        .find(|record| {
            event_matches_run_id(&record.event, run_id)
                && record.event.get("type").and_then(Value::as_str) == Some("BenchmarkRunEnvelope")
        })
        .map(|record| record.event.clone())
        .unwrap_or_else(|| {
            json!({
                "type": "BenchmarkRunEnvelope",
                "status": "unavailable",
                "detail": {
                    "reason": "event not present in telemetry",
                },
            })
        })
}

fn prompt_workload_summary(events: &[DumpLogEvent], run_id: u64) -> Vec<Value> {
    let mut prompts = BTreeMap::new();
    for record in events {
        if !event_matches_run_id(&record.event, run_id)
            || record.channel != "myelin.chat.prompt"
            || record.event.get("type").and_then(Value::as_str) != Some("ChatProgress")
            || record.event.get("phase").and_then(Value::as_str) != Some("prompt_submitted")
            || record.event.get("status").and_then(Value::as_str) != Some("ready")
        {
            continue;
        }
        let Some(request_id) = dump_log_request_id(&record.event) else {
            continue;
        };
        let detail = record.event.get("detail").unwrap_or(&Value::Null);
        prompts.insert(
            request_id,
            json!({
                "request_id": request_id,
                "prompt_index": detail.get("prompt_index").and_then(Value::as_u64),
                "prompt_hash": detail.get("prompt_hash").and_then(Value::as_str),
                "prompt_bytes": detail.get("prompt_bytes").and_then(Value::as_u64),
                "max_tokens": detail.get("max_tokens").and_then(Value::as_u64),
            }),
        );
    }
    prompts.into_values().collect()
}

fn prompt_summary_json(prompt: &PromptBenchmarkFacts) -> Value {
    let roundtrip = duration_between(
        prompt.chat_submitted.as_ref(),
        prompt.chat_completed.as_ref(),
    );
    let worker_start = prompt
        .worker_started
        .as_ref()
        .or(prompt.encode_started.as_ref());
    let worker_end = prompt
        .worker_completed
        .as_ref()
        .or(prompt.chat_completed.as_ref());
    let worker_total = duration_between(worker_start, worker_end);
    let encode = duration_between(prompt.encode_started.as_ref(), prompt.encode_ready.as_ref());
    let first_token = duration_between(
        prompt.decode_started.as_ref(),
        prompt.first_token_ready.as_ref(),
    );
    let decode = duration_between(prompt.decode_started.as_ref(), prompt.decode_ready.as_ref());
    let text_decode = duration_between(
        prompt.text_decode_started.as_ref(),
        prompt.text_decode_ready.as_ref(),
    );
    json!({
        "request_id": prompt.request_id,
        "roundtrip_ms": duration_summary_json(roundtrip),
        "worker_total_ms": duration_summary_json(worker_total),
        "tokenization_ms": duration_summary_json(encode),
        "first_token_ms": duration_summary_json(first_token),
        "decode_ms": duration_summary_json(decode),
        "text_decode_ms": duration_summary_json(text_decode),
        "tokens_generated": prompt.tokens_generated,
        "tokens_per_sec": tokens_per_sec(prompt.tokens_generated, decode.value_ms),
    })
}

fn benchmark_span_json(
    facts: &BenchmarkFacts,
    channel: &str,
    event_type: &str,
    phase: &str,
    start_status: &str,
    end_status: &str,
) -> Value {
    duration_summary_json(duration_between(
        facts.span_point(channel, event_type, phase, start_status),
        facts.span_point(channel, event_type, phase, end_status),
    ))
}

fn duration_summary_json(duration: DurationRender) -> Value {
    json!({
        "value_ms": duration.value_ms,
        "clock_skew": duration.clock_skew,
    })
}

fn pipeline_summary_json(events: &[DumpLogEvent], facts: &DumpLogFacts) -> Value {
    let mut worker_steps = BenchmarkAggregate::default();
    let mut object_loads = BenchmarkAggregate::default();
    let mut worker_steps_by_stage = BTreeMap::<u64, BenchmarkAggregate>::new();
    let mut ring_installs = BTreeMap::<String, u64>::new();
    for record in events {
        match (
            record.channel.as_str(),
            record.event.get("type").and_then(Value::as_str),
        ) {
            ("myelin.worker.step", Some("StepExecuted")) => {
                worker_steps.observe_step_event(&record.event);
                if let Some(stage_index) = pipeline_stage_index(&record.event) {
                    worker_steps_by_stage
                        .entry(stage_index)
                        .or_default()
                        .observe_step_event(&record.event);
                }
            }
            ("myelin.worker.ingress", Some("ObjectLoaded")) => {
                object_loads.observe_object_load_event(&record.event);
            }
            ("myelin.worker.ring", Some("RingInstalled")) => {
                let direction = record
                    .event
                    .get("direction")
                    .and_then(Value::as_str)
                    .unwrap_or("unavailable")
                    .to_owned();
                *ring_installs.entry(direction).or_default() += 1;
            }
            _ => {}
        }
    }
    let worker_steps_by_stage = worker_steps_by_stage
        .into_iter()
        .map(|(stage_index, stats)| stage_worker_summary_json(stage_index, &stats))
        .collect::<Vec<_>>();
    json!({
        "worker_steps": worker_steps.to_json(),
        "worker_steps_by_stage": worker_steps_by_stage,
        "object_loads": object_loads.to_json(),
        "ring_installs": ring_installs,
        "prompt_critical_paths": prompt_pipeline_critical_summary_json(events),
        "edge_handoffs": pipeline_edge_handoff_summary_json(events),
        "provisioning": stage_provisioning_summary_json(events),
        "data_path": {
            "activation_object_loaded": facts.activation_object_loaded,
            "activation_step_executed": facts.activation_step_executed,
            "activation_egress_ring_read": facts.activation_egress_ring_read,
            "activation_ingress_ring_write": facts.activation_ingress_ring_write,
            "activation_iroh_edge_sent": facts.activation_iroh_edge_sent,
            "activation_iroh_edge_read": facts.activation_iroh_edge_read,
            "activation_interstage_handoff": facts.activation_interstage_handoff,
            "activation_edge_ids": facts.activation_edge_ids,
            "max_activation_record_bytes": facts.max_activation_record_bytes,
            "max_worker_command_bytes": facts.max_worker_command_bytes,
            "control_json_large_object_violation": facts.max_activation_record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES
                && facts.max_worker_command_bytes >= facts.max_activation_record_bytes,
        },
    })
}

fn stage_worker_summary_json(stage_index: u64, stats: &BenchmarkAggregate) -> Value {
    json!({
        "stage_index": stage_index,
        "stats": stats.to_json(),
    })
}

#[derive(Default)]
struct PromptPipelineCriticalStats {
    request_id: u64,
    token_in_started: u64,
    token_in_ready: u64,
    token_out_observed: u64,
    tokenizer_decode_ready: u64,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    last_token_out_arrival_ms: Option<u64>,
    token_out_interval_ms: MetricStats,
    token_in_to_out_ms: MetricStats,
    token_in_started_by_sequence: BTreeMap<u64, u64>,
}

impl PromptPipelineCriticalStats {
    fn new(request_id: u64) -> Self {
        Self {
            request_id,
            ..Self::default()
        }
    }

    fn observe_sequence(&mut self, sequence: u64) {
        self.first_sequence = Some(
            self.first_sequence
                .map_or(sequence, |value| value.min(sequence)),
        );
        self.last_sequence = Some(
            self.last_sequence
                .map_or(sequence, |value| value.max(sequence)),
        );
    }

    fn observe_token_in_started(&mut self, sequence: u64, arrival_unix_ms: Option<u64>) {
        self.token_in_started += 1;
        self.observe_sequence(sequence);
        if let Some(arrival_unix_ms) = arrival_unix_ms {
            self.token_in_started_by_sequence
                .entry(sequence)
                .or_insert(arrival_unix_ms);
        }
    }

    fn observe_token_in_ready(&mut self, sequence: u64) {
        self.token_in_ready += 1;
        self.observe_sequence(sequence);
    }

    fn observe_token_out(&mut self, sequence: u64, arrival_unix_ms: Option<u64>) {
        self.token_out_observed += 1;
        self.observe_sequence(sequence);
        if let Some(arrival_unix_ms) = arrival_unix_ms {
            if let Some(last) = self.last_token_out_arrival_ms {
                self.token_out_interval_ms
                    .observe(arrival_unix_ms.saturating_sub(last));
            }
            self.last_token_out_arrival_ms = Some(arrival_unix_ms);
            if let Some(started) = self.token_in_started_by_sequence.get(&sequence) {
                self.token_in_to_out_ms
                    .observe(arrival_unix_ms.saturating_sub(*started));
            }
        }
    }

    fn observe_tokenizer_decode_ready(&mut self) {
        self.tokenizer_decode_ready += 1;
    }

    fn to_json(&self) -> Value {
        json!({
            "request_id": self.request_id,
            "token_in_started": self.token_in_started,
            "token_in_ready": self.token_in_ready,
            "token_out_observed": self.token_out_observed,
            "tokenizer_decode_ready": self.tokenizer_decode_ready,
            "first_sequence": self.first_sequence,
            "last_sequence": self.last_sequence,
            "token_out_interval_ms": self.token_out_interval_ms.to_json(),
            "token_in_to_out_ms": self.token_in_to_out_ms.to_json(),
        })
    }
}

fn prompt_pipeline_critical_summary_json(events: &[DumpLogEvent]) -> Vec<Value> {
    let mut prompts = BTreeMap::<u64, PromptPipelineCriticalStats>::new();
    for record in events {
        if record.channel != "myelin.orch.prompt"
            || record.event.get("type").and_then(Value::as_str) != Some("OrchPromptEvent")
        {
            continue;
        }
        let Some(request_id) = benchmark_request_id(&record.event) else {
            continue;
        };
        let Some(phase) = record.event.get("phase").and_then(Value::as_str) else {
            continue;
        };
        let status = record.event.get("status").and_then(Value::as_str);
        let sequence = detail_u64(&record.event, "sequence");
        let prompt = prompts
            .entry(request_id)
            .or_insert_with(|| PromptPipelineCriticalStats::new(request_id));
        match (phase, status, sequence) {
            ("pipeline_token_in", Some("started"), Some(sequence)) => {
                prompt.observe_token_in_started(sequence, record.arrival_unix_ms);
            }
            ("pipeline_token_in", Some("ready"), Some(sequence)) => {
                prompt.observe_token_in_ready(sequence);
            }
            ("pipeline_token_out", Some("observed"), Some(sequence)) => {
                prompt.observe_token_out(sequence, record.arrival_unix_ms);
            }
            ("pipeline_tokenizer_decode", Some("ready"), _) => {
                prompt.observe_tokenizer_decode_ready();
            }
            _ => {}
        }
    }
    prompts
        .values()
        .map(PromptPipelineCriticalStats::to_json)
        .collect()
}

#[derive(Default)]
struct PipelineEdgeHandoffStats {
    edge_id: u64,
    producer_stages: BTreeSet<u64>,
    consumer_stages: BTreeSet<u64>,
    producer_ring_reads: u64,
    producer_sends: u64,
    consumer_stream_reads: u64,
    consumer_ring_writes: u64,
    consumer_object_loads: u64,
    record_bytes: MetricStats,
    network_read_bytes: MetricStats,
    helper_execute_ms: MetricStats,
    egress_ring_read_ms: MetricStats,
    send_ms: MetricStats,
    ingress_ring_write_ms: MetricStats,
    object_load_ms: MetricStats,
}

impl PipelineEdgeHandoffStats {
    fn observe_node_stage_event(&mut self, event: &Value) {
        let detail = event.get("detail").unwrap_or(&Value::Null);
        let phase = event.get("phase").and_then(Value::as_str);
        if let Some(stage_index) = event.get("stage_index").and_then(Value::as_u64) {
            match phase {
                Some("egress_ring_read") | Some("iroh_edge_bytes_sent") => {
                    self.producer_stages.insert(stage_index);
                }
                Some("iroh_edge_bytes_read")
                | Some("ingress_ring_write")
                | Some("object_loaded") => {
                    self.consumer_stages.insert(stage_index);
                }
                _ => {}
            }
        }
        match phase {
            Some("egress_ring_read") => {
                self.producer_ring_reads += 1;
                self.record_bytes
                    .observe_optional(detail.get("record_bytes").and_then(Value::as_u64));
                self.helper_execute_ms
                    .observe_optional(detail.get("helper_execute_ms").and_then(Value::as_u64));
                self.egress_ring_read_ms
                    .observe_optional(detail.get("egress_ring_read_ms").and_then(Value::as_u64));
            }
            Some("iroh_edge_bytes_sent") => {
                self.producer_sends += 1;
                self.record_bytes
                    .observe_optional(detail.get("record_bytes").and_then(Value::as_u64));
                self.send_ms
                    .observe_optional(detail.get("send_ms").and_then(Value::as_u64));
            }
            Some("iroh_edge_bytes_read") => {
                self.consumer_stream_reads += 1;
                self.network_read_bytes
                    .observe_optional(detail.get("bytes").and_then(Value::as_u64));
            }
            Some("ingress_ring_write") => {
                self.consumer_ring_writes += 1;
                self.record_bytes
                    .observe_optional(detail.get("record_bytes").and_then(Value::as_u64));
                self.ingress_ring_write_ms
                    .observe_optional(detail.get("ingress_ring_write_ms").and_then(Value::as_u64));
            }
            Some("object_loaded") => {
                self.consumer_object_loads += 1;
                self.object_load_ms
                    .observe_optional(detail.get("object_load_ms").and_then(Value::as_u64));
            }
            _ => {}
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "edge_id": self.edge_id,
            "producer_stages": self.producer_stages.iter().copied().collect::<Vec<_>>(),
            "consumer_stages": self.consumer_stages.iter().copied().collect::<Vec<_>>(),
            "producer_ring_reads": self.producer_ring_reads,
            "producer_sends": self.producer_sends,
            "consumer_stream_reads": self.consumer_stream_reads,
            "consumer_ring_writes": self.consumer_ring_writes,
            "consumer_object_loads": self.consumer_object_loads,
            "record_bytes": self.record_bytes.to_json(),
            "network_read_bytes": self.network_read_bytes.to_json(),
            "helper_execute_ms": self.helper_execute_ms.to_json(),
            "egress_ring_read_ms": self.egress_ring_read_ms.to_json(),
            "send_ms": self.send_ms.to_json(),
            "ingress_ring_write_ms": self.ingress_ring_write_ms.to_json(),
            "object_load_ms": self.object_load_ms.to_json(),
        })
    }
}

fn pipeline_edge_handoff_summary_json(events: &[DumpLogEvent]) -> Vec<Value> {
    let mut edges = BTreeMap::<u64, PipelineEdgeHandoffStats>::new();
    for record in events {
        if record.channel != "myelin.node.stage"
            || record.event.get("type").and_then(Value::as_str) != Some("NodeEvent")
        {
            continue;
        }
        let Some(edge_id) = detail_u64(&record.event, "edge_id") else {
            continue;
        };
        let edge = edges
            .entry(edge_id)
            .or_insert_with(|| PipelineEdgeHandoffStats {
                edge_id,
                ..PipelineEdgeHandoffStats::default()
            });
        edge.observe_node_stage_event(&record.event);
    }
    edges
        .values()
        .map(PipelineEdgeHandoffStats::to_json)
        .collect()
}

#[derive(Default)]
struct StageProvisionStats {
    send_events: u64,
    first_attempt: Option<u64>,
    last_attempt: Option<u64>,
    min_loaded_stage_count: Option<u64>,
    max_loaded_stage_count: Option<u64>,
    stage_count: Option<u64>,
}

impl StageProvisionStats {
    fn observe(&mut self, detail: &Value) {
        self.send_events += 1;
        if let Some(attempt) = detail.get("attempt").and_then(Value::as_u64) {
            if self.first_attempt.is_none() {
                self.first_attempt = Some(attempt);
            }
            self.last_attempt = Some(attempt);
        }
        if let Some(loaded) = detail.get("loaded_stage_count").and_then(Value::as_u64) {
            self.min_loaded_stage_count = Some(
                self.min_loaded_stage_count
                    .map_or(loaded, |min| min.min(loaded)),
            );
            self.max_loaded_stage_count = Some(
                self.max_loaded_stage_count
                    .map_or(loaded, |max| max.max(loaded)),
            );
        }
        if self.stage_count.is_none() {
            self.stage_count = detail.get("stage_count").and_then(Value::as_u64);
        }
    }

    fn to_json(&self, stage_index: u64) -> Value {
        json!({
            "stage_index": stage_index,
            "send_events": self.send_events,
            "first_attempt": self.first_attempt,
            "last_attempt": self.last_attempt,
            "min_loaded_stage_count": self.min_loaded_stage_count,
            "max_loaded_stage_count": self.max_loaded_stage_count,
            "stage_count": self.stage_count,
        })
    }
}

fn stage_provisioning_summary_json(events: &[DumpLogEvent]) -> Value {
    let mut send_events = 0_u64;
    let mut stages = BTreeMap::<u64, StageProvisionStats>::new();
    let mut latest_wait = None;
    let mut provider_event_counts = BTreeMap::<String, u64>::new();
    let mut ssh_retry_counts = BTreeMap::<String, u64>::new();
    let mut ssh_failure_counts = BTreeMap::<String, u64>::new();
    for record in events {
        if record.channel == "myelin.provisioning.events"
            && let Some(kind) = record
                .event
                .get("event")
                .and_then(|event| event.get("kind"))
                .and_then(Value::as_str)
        {
            *provider_event_counts.entry(kind.to_owned()).or_default() += 1;
        }
        if record.channel.starts_with("myelin.provisioning.logs.node.") {
            let node = record
                .event
                .get("node_id")
                .and_then(Value::as_u64)
                .map(|node_id| node_id.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            if let Some(line) = record.event.get("line").and_then(Value::as_str) {
                if line.contains("VastAI SSH bootstrap retrying") {
                    *ssh_retry_counts.entry(node.clone()).or_default() += 1;
                }
                if line.contains("VastAI SSH bootstrap failed before runtime ready") {
                    *ssh_failure_counts.entry(node).or_default() += 1;
                }
            }
        }
        if record.channel != "myelin.orch.bootstrap"
            || record.event.get("type").and_then(Value::as_str) != Some("OrchBootstrap")
            || record.event.get("phase").and_then(Value::as_str) != Some("stage_provision_send")
            || record.event.get("status").and_then(Value::as_str) != Some("sent")
        {
            continue;
        }
        let detail = record.event.get("detail").unwrap_or(&Value::Null);
        let Some(stage_index) = detail.get("stage_index").and_then(Value::as_u64) else {
            continue;
        };
        send_events += 1;
        stages.entry(stage_index).or_default().observe(detail);
        latest_wait = Some(json!({
            "attempt": detail.get("attempt").and_then(Value::as_u64),
            "waiting_stage_index": stage_index,
            "loaded_stage_count": detail.get("loaded_stage_count").and_then(Value::as_u64),
            "stage_count": detail.get("stage_count").and_then(Value::as_u64),
            "stage_send_count": detail.get("stage_send_count").and_then(Value::as_u64),
        }));
    }
    let max_send_events_for_stage = stages
        .values()
        .map(|stage| stage.send_events)
        .max()
        .unwrap_or(0);
    let stages = stages
        .into_iter()
        .map(|(stage_index, stats)| stats.to_json(stage_index))
        .collect::<Vec<_>>();
    json!({
        "send_events": send_events,
        "max_send_events_for_stage": max_send_events_for_stage,
        "latest_wait": latest_wait.unwrap_or(Value::Null),
        "stages": stages,
        "provider_event_counts": provider_event_counts,
        "ssh_bootstrap": {
            "retry_counts_by_node": ssh_retry_counts,
            "failure_counts_by_node": ssh_failure_counts,
        },
    })
}

fn gpu_summary_json(events: &[DumpLogEvent], facts: &DumpLogFacts) -> Value {
    let mut cpu_profile_summaries = Vec::new();
    for record in events {
        if record.event.get("type").and_then(Value::as_str) == Some("CpuLineProfileSummary") {
            cpu_profile_summaries.push(record.event.clone());
        }
    }
    json!({
        "worker_device_requested": facts.gpu_worker_device_requested,
        "import_ready": facts.gpu_import_ready,
        "probe_ready": facts.gpu_probe_ready,
        "worker_ready": facts.gpu_worker_ready,
        "cpu_fallback_seen": facts.gpu_cpu_fallback_seen,
        "decode_started_request_ids": facts.gpu_decode_started,
        "first_token_request_ids": facts.gpu_first_token_ready,
        "decode_ready_request_ids": facts.gpu_decode_ready,
        "prompt_completed_request_ids": facts.gpu_prompt_completed,
        "pipeline_prompt_encoded_request_ids": facts.gpu_pipeline_prompt_encoded,
        "pipeline_prompt_begin_request_ids": facts.gpu_pipeline_prompt_begin,
        "pipeline_token_in_request_ids": facts.gpu_pipeline_token_in,
        "pipeline_token_out_request_ids": facts.gpu_pipeline_token_out,
        "pipeline_tokenizer_decode_ready_request_ids": facts.gpu_pipeline_tokenizer_decode_ready,
        "pipeline_tokens_decoded_request_ids": facts.gpu_pipeline_tokens_decoded,
        "pipeline_real_worker_step_seen": facts.gpu_pipeline_real_worker_step_seen,
        "cpu_profile_summaries": cpu_profile_summaries,
        "host_gpu_samples": host_gpu_sample_summary_json(events),
    })
}

fn host_gpu_sample_summary_json(events: &[DumpLogEvent]) -> Value {
    let mut sample_count = 0_u64;
    let mut error_count = 0_u64;
    let mut device_sample_count = 0_u64;
    let mut max_utilization_gpu_percent = MetricStats::default();
    let mut max_memory_used_mib = MetricStats::default();
    let mut max_memory_total_mib = MetricStats::default();
    for record in events {
        if record.channel != "host.gpu" {
            continue;
        }
        sample_count += 1;
        if record.event.get("error").and_then(Value::as_str).is_some() {
            error_count += 1;
        }
        let Some(gpus) = record.event.get("gpus").and_then(Value::as_array) else {
            continue;
        };
        device_sample_count = device_sample_count.saturating_add(gpus.len() as u64);
        for gpu in gpus {
            max_utilization_gpu_percent
                .observe_optional(gpu.get("utilization_gpu_percent").and_then(Value::as_u64));
            max_memory_used_mib
                .observe_optional(gpu.get("memory_used_mib").and_then(Value::as_u64));
            max_memory_total_mib
                .observe_optional(gpu.get("memory_total_mib").and_then(Value::as_u64));
        }
    }
    json!({
        "sample_count": sample_count,
        "error_count": error_count,
        "device_sample_count": device_sample_count,
        "utilization_gpu_percent": max_utilization_gpu_percent.to_json(),
        "memory_used_mib": max_memory_used_mib.to_json(),
        "memory_total_mib": max_memory_total_mib.to_json(),
    })
}

struct VastAiLeaseSelection {
    node_id: Option<u64>,
    index: u64,
    offer_id: u64,
    gpu_name: String,
    gpu_ram_mb: Option<u64>,
    dph_total: f64,
    geolocation: Option<String>,
    host_id: Option<u64>,
    effective_dph_total: f64,
}

impl VastAiLeaseSelection {
    fn to_json(&self) -> Value {
        json!({
            "node_id": self.node_id,
            "index": self.index,
            "offer_id": self.offer_id,
            "gpu_name": self.gpu_name,
            "gpu_ram_mb": self.gpu_ram_mb,
            "dph_total": self.dph_total,
            "geolocation": self.geolocation,
            "host_id": self.host_id,
            "effective_dph_total": self.effective_dph_total,
        })
    }
}

fn vastai_summary_json(events: &[DumpLogEvent]) -> Value {
    let selections = events
        .iter()
        .filter_map(parse_vastai_lease_selection)
        .collect::<Vec<_>>();
    let mut max_dph_total: Option<f64> = None;
    for selection in &selections {
        max_dph_total =
            Some(max_dph_total.map_or(selection.dph_total, |max| max.max(selection.dph_total)));
    }
    json!({
        "selected_lease_count": selections.len(),
        "max_dph_total": max_dph_total,
        "selected_leases": selections
            .iter()
            .map(VastAiLeaseSelection::to_json)
            .collect::<Vec<_>>(),
    })
}

fn parse_vastai_lease_selection(record: &DumpLogEvent) -> Option<VastAiLeaseSelection> {
    let line = record.event.get("line").and_then(Value::as_str)?;
    let rest = line.strip_prefix("lease_chain: index ")?;
    let (index, rest) = rest.split_once(" → offer ")?;
    let (offer_id, rest) = rest.split_once(" — ")?;
    let (gpu_and_ram, rest) = rest.split_once(" @ $")?;
    let (gpu_name, gpu_ram) = gpu_and_ram.rsplit_once(' ')?;
    let (dph_total, rest) = rest.split_once("/hr [")?;
    let (geolocation, rest) = rest.split_once("] host ")?;
    let (host_id, effective_dph_total) = rest.split_once(" eff $")?;
    let effective_dph_total = effective_dph_total.strip_suffix("/hr")?;
    Some(VastAiLeaseSelection {
        node_id: record.event.get("node_id").and_then(Value::as_u64),
        index: index.parse().ok()?,
        offer_id: offer_id.parse().ok()?,
        gpu_name: gpu_name.to_owned(),
        gpu_ram_mb: gpu_ram
            .strip_suffix("MB")
            .and_then(|value| value.parse().ok()),
        dph_total: dph_total.parse().ok()?,
        geolocation: (!geolocation.is_empty()).then(|| geolocation.to_owned()),
        host_id: host_id.parse().ok(),
        effective_dph_total: effective_dph_total.parse().ok()?,
    })
}

#[derive(Clone, Default)]
struct MetricStats {
    count: u64,
    sum: u64,
    min: Option<u64>,
    max: Option<u64>,
}

impl MetricStats {
    fn observe(&mut self, value: u64) {
        self.count += 1;
        self.sum = self.sum.saturating_add(value);
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
    }

    fn observe_optional(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.observe(value);
        }
    }

    fn avg(&self) -> Option<f64> {
        (self.count != 0).then(|| self.sum as f64 / self.count as f64)
    }

    fn to_json(&self) -> Value {
        json!({
            "count": self.count,
            "sum": self.sum,
            "min": self.min,
            "max": self.max,
            "avg": self.avg(),
        })
    }
}

#[derive(Clone, Default)]
struct BenchmarkAggregate {
    count: u64,
    elapsed_ms: u64,
    stage_execution_ms: u64,
    record_write_ms: u64,
    input_prepare_ms: u64,
    model_forward_ms: u64,
    output_realize_ms: u64,
    payload_pack_ms: u64,
    payload_bytes: u64,
    record_bytes: u64,
}

impl BenchmarkAggregate {
    fn observe_step_event(&mut self, event: &Value) {
        self.count += 1;
        self.elapsed_ms = self
            .elapsed_ms
            .saturating_add(event_u64(event, "elapsed_ms").unwrap_or(0));
        self.stage_execution_ms = self
            .stage_execution_ms
            .saturating_add(event_u64(event, "stage_execution_ms").unwrap_or(0));
        self.record_write_ms = self
            .record_write_ms
            .saturating_add(event_u64(event, "record_write_ms").unwrap_or(0));
        self.input_prepare_ms = self
            .input_prepare_ms
            .saturating_add(event_u64(event, "input_prepare_ms").unwrap_or(0));
        self.model_forward_ms = self
            .model_forward_ms
            .saturating_add(event_u64(event, "model_forward_ms").unwrap_or(0));
        self.output_realize_ms = self
            .output_realize_ms
            .saturating_add(event_u64(event, "output_realize_ms").unwrap_or(0));
        self.payload_pack_ms = self
            .payload_pack_ms
            .saturating_add(event_u64(event, "payload_pack_ms").unwrap_or(0));
        self.payload_bytes = self
            .payload_bytes
            .saturating_add(event_u64(event, "payload_bytes").unwrap_or(0));
        self.record_bytes = self
            .record_bytes
            .saturating_add(event_u64(event, "record_bytes").unwrap_or(0));
    }

    fn observe_object_load_event(&mut self, event: &Value) {
        self.count += 1;
        self.elapsed_ms = self
            .elapsed_ms
            .saturating_add(event_u64(event, "elapsed_ms").unwrap_or(0));
        self.record_bytes = self
            .record_bytes
            .saturating_add(event_u64(event, "extent").unwrap_or(0));
    }

    fn to_json(&self) -> Value {
        json!({
            "count": self.count,
            "elapsed_ms_sum": self.elapsed_ms,
            "stage_execution_ms_sum": self.stage_execution_ms,
            "record_write_ms_sum": self.record_write_ms,
            "input_prepare_ms_sum": self.input_prepare_ms,
            "model_forward_ms_sum": self.model_forward_ms,
            "output_realize_ms_sum": self.output_realize_ms,
            "payload_pack_ms_sum": self.payload_pack_ms,
            "payload_bytes_sum": self.payload_bytes,
            "record_bytes_sum": self.record_bytes,
        })
    }
}

fn pipeline_stage_index(event: &Value) -> Option<u64> {
    event_u64(event, "stage_index")
        .or_else(|| event_u64(event, "role_id").and_then(|role| role.checked_sub(1)))
}

fn benchmark_invariants_json(
    facts: &DumpLogFacts,
    scenario: MyelinChatCheckScenario,
) -> Vec<Value> {
    let mut invariants = vec![
        invariant_json("chat_config_ready", facts.chat_config_ready),
        invariant_json("prepare_runtime_ready", facts.prepare_runtime_ready),
        invariant_json("prompt_rpc_ready", facts.prompt_rpc_ready),
        invariant_json(
            "orchestrator_weights_loaded",
            facts.orch_weights_loaded_ready,
        ),
        invariant_json(
            "two_prompt_responses",
            facts.response_text_1 && facts.response_text_2,
        ),
        invariant_json(
            "two_prompt_completions",
            facts.request_completed_1 && facts.request_completed_2,
        ),
        invariant_json("shutdown_requested", facts.shutdown_requested),
        invariant_json("orchestrator_stopped", facts.orchestrator_stopped),
    ];
    if matches!(
        scenario,
        MyelinChatCheckScenario::Gpu | MyelinChatCheckScenario::VastAi
    ) {
        invariants.extend([
            invariant_json("gpu_no_cpu_fallback", !facts.gpu_cpu_fallback_seen),
            invariant_json("gpu_worker_ready", facts.gpu_worker_ready),
        ]);
    }
    if matches!(
        scenario,
        MyelinChatCheckScenario::MultinodeDocker | MyelinChatCheckScenario::VastAi
    ) {
        invariants.extend([
            invariant_json(
                "activation_large_object_loaded",
                facts.activation_object_loaded,
            ),
            invariant_json(
                "activation_large_object_step_executed",
                facts.activation_step_executed,
            ),
        ]);
    }
    invariants
}

fn invariant_json(name: &str, passed: bool) -> Value {
    json!({
        "name": name,
        "passed": passed,
    })
}

fn legacy_tolerance_summary(events: &[DumpLogEvent]) -> Value {
    let missing_benchmark_stamp = events
        .iter()
        .filter(|record| record.event.get("benchmark").is_none())
        .count();
    let missing_elapsed_fields = events
        .iter()
        .filter(|record| {
            matches!(
                record.event.get("type").and_then(Value::as_str),
                Some(
                    "PromptEncodeReady"
                        | "DecodeReady"
                        | "TextDecodeReady"
                        | "StepExecuted"
                        | "ObjectLoaded"
                        | "PromptEncoded"
                        | "TokensDecoded"
                )
            ) && record.event.get("elapsed_ms").is_none()
        })
        .count();
    json!({
        "accepted": true,
        "missing_benchmark_stamp_events": missing_benchmark_stamp,
        "missing_elapsed_field_events": missing_elapsed_fields,
        "unavailable_fields_are_null": true,
    })
}

fn benchmark_channel_counts(events: &[DumpLogEvent]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in events {
        *counts.entry(record.channel.clone()).or_default() += 1;
    }
    counts
}

fn benchmark_event_counts(events: &[DumpLogEvent]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in events {
        let key = format!(
            "{}/{}/{}/{}",
            record.channel,
            record
                .event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("<missing>"),
            record
                .event
                .get("phase")
                .and_then(Value::as_str)
                .unwrap_or("<none>"),
            record
                .event
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("<none>")
        );
        *counts.entry(key).or_default() += 1;
    }
    counts
}

fn event_u64(event: &Value, key: &str) -> Option<u64> {
    event.get(key).and_then(Value::as_u64)
}

fn file_blake3_hex(path: &Path) -> Result<String, String> {
    let bytes =
        fs::read(path).map_err(|e| format!("read artifact for hash {}: {e}", path.display()))?;
    Ok(bytes_blake3_hex(&bytes))
}

fn file_size(path: &Path) -> Result<u64, String> {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|e| format!("read artifact metadata {}: {e}", path.display()))
}

fn bytes_blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn event_matches_run_id(event: &Value, run_id: u64) -> bool {
    event
        .get("run_id")
        .and_then(Value::as_u64)
        .is_none_or(|event_run_id| event_run_id == run_id)
}

fn benchmark_request_id(event: &Value) -> Option<u64> {
    event.get("request_id").and_then(Value::as_u64).or_else(|| {
        event
            .get("detail")
            .and_then(|detail| detail.get("request_id"))
            .and_then(Value::as_u64)
    })
}

fn require_prompt_point(found: bool, event: &str, request_id: u64) -> Result<(), String> {
    if found {
        Ok(())
    } else {
        Err(missing_benchmark_event(format!(
            "{event} request_id={request_id}"
        )))
    }
}

fn missing_benchmark_event(event: String) -> String {
    format!("myelin-chat-check: missing benchmark event {event}")
}

#[derive(Default)]
struct DumpLogFacts {
    gpu_worker_device_requested: bool,
    gpu_import_ready: bool,
    gpu_probe_ready: bool,
    gpu_worker_ready: bool,
    gpu_cpu_fallback_seen: bool,
    gpu_decode_started: BTreeSet<u64>,
    gpu_first_token_ready: BTreeSet<u64>,
    gpu_decode_ready: BTreeSet<u64>,
    gpu_prompt_completed: BTreeSet<u64>,
    gpu_pipeline_prompt_encoded: BTreeSet<u64>,
    gpu_pipeline_prompt_begin: BTreeSet<u64>,
    gpu_pipeline_token_in: BTreeSet<u64>,
    gpu_pipeline_token_out: BTreeSet<u64>,
    gpu_pipeline_tokenizer_decode_ready: BTreeSet<u64>,
    gpu_pipeline_tokens_decoded: BTreeSet<u64>,
    gpu_pipeline_real_worker_step_seen: bool,
    ring_installed_ingress: bool,
    ring_installed_egress: bool,
    ring_installed_ingress_edge_ids: BTreeSet<u64>,
    ring_installed_egress_edge_ids: BTreeSet<u64>,
    ring_installed_egress_ring_ids: BTreeSet<u64>,
    activation_object_loaded: bool,
    worker_ingress_object_loaded: bool,
    activation_step_executed: bool,
    activation_egress_ring_read: bool,
    activation_ingress_ring_write: bool,
    activation_iroh_edge_sent: bool,
    activation_iroh_edge_read: bool,
    activation_interstage_handoff: bool,
    activation_egress_record_written: bool,
    pipeline_token_out_requests: BTreeSet<u64>,
    activation_downstream_object_loaded: bool,
    activation_edge_ids: BTreeSet<u64>,
    iroh_read_edge_ids: BTreeSet<u64>,
    max_activation_record_bytes: u64,
    max_worker_command_bytes: u64,
    docker_node_spec_worker_count: Option<u64>,
    worker_iroh_ready: BTreeSet<u64>,
    docker_worker_coordinator_join: BTreeSet<u64>,
    vastai_node_spec_worker_count: Option<u64>,
    vastai_provision_start_nodes: BTreeSet<u64>,
    vastai_provider_start_nodes: BTreeSet<u64>,
    vastai_node_runtime_ready_nodes: BTreeSet<u64>,
    chat_config_ready: bool,
    prepare_runtime_ready: bool,
    prompt_rpc_ready: bool,
    orch_iroh_driver_ready: bool,
    node_iroh_driver_ready: bool,
    node_worker_initialize_ready: bool,
    orch_weights_loaded_ready: bool,
    response_text_1: bool,
    request_completed_1: bool,
    response_text_2: bool,
    request_completed_2: bool,
    shutdown_requested: bool,
    orchestrator_stopped: bool,
}
fn record_dump_log_event(
    scenario: MyelinChatCheckScenario,
    channel: &str,
    event: &Value,
    facts: &mut DumpLogFacts,
) -> Result<(), String> {
    record_vastai_provision_dump_log_event(channel, event, facts);
    record_gpu_dump_log_event(channel, event, facts);
    record_data_path_dump_log_event(channel, event, facts);
    let event_type = event.get("type").and_then(Value::as_str);
    let phase = event.get("phase").and_then(Value::as_str);
    let status = event.get("status").and_then(Value::as_str);
    if status == Some("failed")
        && !(scenario == MyelinChatCheckScenario::VastAi
            && channel == "myelin.orch.bootstrap"
            && event_type == Some("OrchBootstrap")
            && phase == Some("provider_start")
            && detail_str(event, "provider") == Some("vastai")
            && detail_u64(event, "node_id").is_some()
            && detail_u64(event, "stage_index").is_some())
    {
        return Err(format!(
            "myelin-chat-check: failed event channel={channel} type={} phase={} detail={}",
            event_type.unwrap_or("<missing>"),
            phase.unwrap_or("<missing>"),
            event.get("detail").unwrap_or(&Value::Null)
        ));
    }

    match (channel, event_type, phase, status) {
        ("myelin.chat.lifecycle", Some("ChatProgress"), Some("config"), Some("ready")) => {
            facts.chat_config_ready = true;
        }
        ("myelin.chat.runtime", Some("ChatProgress"), Some("prepare_runtime"), Some("ready")) => {
            facts.prepare_runtime_ready = true;
        }
        ("myelin.chat.runtime", Some("ChatProgress"), Some("prompt_rpc"), Some("ready")) => {
            facts.prompt_rpc_ready = true;
        }
        (_, Some("OrchBootstrap"), Some("iroh_driver"), Some("ready")) => {
            facts.orch_iroh_driver_ready = true;
        }
        (_, Some("NodeEvent"), Some("iroh_driver"), Some("ready")) => {
            facts.node_iroh_driver_ready = true;
            if let Some(node_id) = event_node_id(event) {
                facts.worker_iroh_ready.insert(node_id);
            }
        }
        (_, Some("OrchBootstrap"), Some("node_spec"), Some("ready")) => {
            match detail_str(event, "provider") {
                Some("docker") => {
                    facts.docker_node_spec_worker_count = detail_u64(event, "worker_count")
                }
                Some("vastai") => {
                    facts.vastai_node_spec_worker_count = detail_u64(event, "worker_count")
                }
                _ => {}
            }
        }
        (_, Some("NodeEvent"), Some("coordinator_join"), Some("started")) => {
            if detail_u64(event, "direct_addr_count").is_some_and(|count| count > 0)
                && let Some(node_id) = event_node_id(event)
            {
                facts.docker_worker_coordinator_join.insert(node_id);
            }
        }
        (_, Some("OrchBootstrap"), Some("provider_start"), Some("started")) => {
            if detail_str(event, "provider") == Some("vastai")
                && let Some(node_id) = detail_u64(event, "node_id")
            {
                facts.vastai_provider_start_nodes.insert(node_id);
            }
        }
        (_, Some("OrchBootstrap"), Some("node_runtime_ready"), Some("ready")) => {
            if let Some(node_id) = detail_u64(event, "node_id") {
                facts.vastai_node_runtime_ready_nodes.insert(node_id);
            }
        }
        (_, Some("NodeEvent"), Some("worker_initialize"), Some("ready")) => {
            facts.node_worker_initialize_ready = true;
        }
        (_, Some("OrchBootstrap"), Some("weights_loaded"), Some("ready")) => {
            facts.orch_weights_loaded_ready = true;
        }
        ("myelin.chat.prompt", Some("ChatProgress"), Some("response_text"), Some("observed")) => {
            match dump_log_request_id(event) {
                Some(1) => facts.response_text_1 = true,
                Some(2) => facts.response_text_2 = true,
                _ => {}
            }
        }
        ("myelin.chat.prompt", Some("ChatProgress"), Some("request_completed"), Some("ready")) => {
            match dump_log_request_id(event) {
                Some(1) => facts.request_completed_1 = true,
                Some(2) => facts.request_completed_2 = true,
                _ => {}
            }
        }
        ("myelin.chat.lifecycle", Some("ChatProgress"), Some("shutdown"), Some("requested")) => {
            facts.shutdown_requested = true;
        }
        (
            "myelin.chat.component",
            Some("ChatProgress"),
            Some("orchestrator_process"),
            Some("stopped"),
        ) => {
            facts.orchestrator_stopped = true;
        }
        _ => {}
    }
    Ok(())
}
fn record_vastai_provision_dump_log_event(channel: &str, event: &Value, facts: &mut DumpLogFacts) {
    if channel != "myelin.provisioning.events" {
        return;
    }
    let Some(provision) = event.get("event") else {
        return;
    };
    if provision.get("kind").and_then(Value::as_str) == Some("ProvisionStart")
        && provision.get("provider").and_then(Value::as_str) == Some("vastai")
        && let Some(node_id) = provision.get("node_id").and_then(Value::as_u64)
    {
        facts.vastai_provision_start_nodes.insert(node_id);
    }
}

fn record_data_path_dump_log_event(channel: &str, event: &Value, facts: &mut DumpLogFacts) {
    let event_type = event.get("type").and_then(Value::as_str);
    let phase = event.get("phase").and_then(Value::as_str);
    let status = event.get("status").and_then(Value::as_str);
    match (channel, event_type) {
        ("myelin.worker.ring", Some("RingInstalled")) => {
            let edge_id = event.get("edge_id").and_then(Value::as_u64);
            match event.get("direction").and_then(Value::as_str) {
                Some("ingress") => {
                    facts.ring_installed_ingress = true;
                    if let Some(edge_id) = edge_id {
                        facts.ring_installed_ingress_edge_ids.insert(edge_id);
                    }
                }
                Some("egress") => {
                    facts.ring_installed_egress = true;
                    if let Some(edge_id) = edge_id {
                        facts.ring_installed_egress_edge_ids.insert(edge_id);
                    }
                    if let Some(ring_id) = event.get("ring_id").and_then(Value::as_u64) {
                        facts.ring_installed_egress_ring_ids.insert(ring_id);
                    }
                }
                _ => {}
            }
        }
        ("myelin.worker.ingress", Some("ObjectLoaded")) => {
            let extent = event.get("extent").and_then(Value::as_u64).unwrap_or(0);
            if extent > 0 || event_positive_u64(event, "token_count") {
                facts.worker_ingress_object_loaded = true;
            }
            if event.get("kind").and_then(Value::as_str) == Some("activation")
                || extent >= DATA_PATH_MIN_PAYLOAD_BYTES
            {
                facts.activation_object_loaded = true;
                facts.max_activation_record_bytes = facts.max_activation_record_bytes.max(extent);
                if event
                    .get("stage_index")
                    .and_then(Value::as_u64)
                    .is_some_and(|stage_index| stage_index > 0)
                {
                    facts.activation_downstream_object_loaded = true;
                }
                if let Some(edge_id) = event.get("edge_id").and_then(Value::as_u64) {
                    record_activation_edge_id(facts, edge_id);
                    record_interstage_activation_handoff(
                        facts,
                        edge_id,
                        event.get("stage_index").and_then(Value::as_u64),
                    );
                }
            }
        }
        ("myelin.worker.step", Some("StepExecuted")) => {
            let payload_bytes = event
                .get("payload_bytes")
                .or_else(|| event.get("committed_bytes"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let output_kind = event.get("output_kind").and_then(Value::as_str);
            let legacy_activation_sized_pipeline_output = output_kind.is_none()
                && event.get("execution_backend").and_then(Value::as_str) == Some("pipeline_stage")
                && payload_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES;
            if (output_kind == Some("activation") || legacy_activation_sized_pipeline_output)
                && payload_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES
            {
                facts.activation_step_executed = true;
                facts.max_activation_record_bytes =
                    facts.max_activation_record_bytes.max(payload_bytes);
                if event
                    .get("ring_id")
                    .and_then(Value::as_u64)
                    .is_some_and(|ring_id| facts.ring_installed_egress_ring_ids.contains(&ring_id))
                {
                    facts.activation_egress_record_written = true;
                }
            }
        }
        ("myelin.orch.prompt", Some("OrchPromptEvent"))
            if phase == Some("pipeline_token_out")
                && status == Some("observed")
                && detail_u64(event, "token_id").is_some() =>
        {
            if let Some(request_id) = event_request_id(event) {
                facts.pipeline_token_out_requests.insert(request_id);
            }
        }
        (_, Some("NodeEvent")) => match (phase, status) {
            (Some("worker_command_write"), Some("ready")) => {
                if detail_str(event, "command_type").is_some_and(|command| {
                    matches!(command, "InstallRing" | "RingReadable" | "ExecuteStep")
                }) && let Some(command_bytes) = detail_u64(event, "command_bytes")
                {
                    facts.max_worker_command_bytes =
                        facts.max_worker_command_bytes.max(command_bytes);
                }
            }
            (Some("egress_ring_read"), Some("ready")) => {
                if detail_str(event, "edge_kind") == Some("Activation")
                    && let Some(record_bytes) = detail_u64(event, "record_bytes")
                    && record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES
                {
                    facts.activation_egress_ring_read = true;
                    facts.max_activation_record_bytes =
                        facts.max_activation_record_bytes.max(record_bytes);
                    if let Some(edge_id) = detail_u64(event, "edge_id") {
                        record_activation_edge_id(facts, edge_id);
                    }
                }
            }
            (Some("ingress_ring_write"), Some("ready")) => {
                if detail_str(event, "edge_kind") == Some("Activation")
                    && let Some(record_bytes) = detail_u64(event, "record_bytes")
                    && record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES
                {
                    facts.activation_ingress_ring_write = true;
                    facts.max_activation_record_bytes =
                        facts.max_activation_record_bytes.max(record_bytes);
                    if let Some(edge_id) = detail_u64(event, "edge_id") {
                        record_activation_edge_id(facts, edge_id);
                    }
                }
            }
            (Some("iroh_edge_bytes_sent"), Some("ready")) => {
                if detail_str(event, "edge_kind") == Some("Activation")
                    && detail_u64(event, "bytes").is_some_and(|bytes| bytes > 0)
                {
                    facts.activation_iroh_edge_sent = true;
                    if let Some(edge_id) = detail_u64(event, "edge_id") {
                        record_activation_edge_id(facts, edge_id);
                    }
                }
            }
            (Some("iroh_edge_bytes_read"), Some("observed")) => {
                if detail_u64(event, "bytes").is_some_and(|bytes| bytes > 0)
                    && let Some(edge_id) = detail_u64(event, "edge_id")
                {
                    facts.iroh_read_edge_ids.insert(edge_id);
                    if facts.activation_edge_ids.contains(&edge_id) {
                        facts.activation_iroh_edge_read = true;
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
}

fn record_activation_edge_id(facts: &mut DumpLogFacts, edge_id: u64) {
    facts.activation_edge_ids.insert(edge_id);
    if facts.iroh_read_edge_ids.contains(&edge_id) {
        facts.activation_iroh_edge_read = true;
    }
}

fn record_interstage_activation_handoff(
    facts: &mut DumpLogFacts,
    edge_id: u64,
    stage_index: Option<u64>,
) {
    if stage_index.is_some_and(|stage_index| stage_index > 0)
        && facts.ring_installed_ingress_edge_ids.contains(&edge_id)
        && facts.ring_installed_egress_edge_ids.contains(&edge_id)
    {
        facts.activation_interstage_handoff = true;
    }
}

fn record_gpu_dump_log_event(channel: &str, event: &Value, facts: &mut DumpLogFacts) {
    let event_type = event.get("type").and_then(Value::as_str);
    let phase = event.get("phase").and_then(Value::as_str);
    let status = event.get("status").and_then(Value::as_str);
    if event_type == Some("TinygradCpuCompilerSelected") {
        facts.gpu_cpu_fallback_seen = true;
    }

    match (channel, event_type) {
        ("myelin.worker.initialize", Some("TinygradImportStarted"))
            if event_requested_device_is_cuda(event) =>
        {
            facts.gpu_worker_device_requested = true;
        }
        ("myelin.worker.initialize", Some("TinygradImportReady"))
            if event_requested_device_is_cuda(event) && event_env_dev_is_cuda(event) =>
        {
            facts.gpu_import_ready = true;
        }
        ("myelin.worker.initialize", Some("TinygradDeviceProbeReady"))
            if event_requested_device_is_cuda(event) && event_probe_result_is_one(event) =>
        {
            facts.gpu_probe_ready = true;
        }
        ("myelin.worker.initialize", Some("WorkerReady"))
            if worker_ready_backend_is_cuda(event) =>
        {
            facts.gpu_worker_ready = true;
        }
        ("myelin.worker.prompt", Some("DecodeStarted"))
            if event_positive_u64(event, "prompt_tokens")
                && event_positive_u64(event, "max_tokens")
                && event
                    .get("decode_impl")
                    .and_then(Value::as_str)
                    .is_some_and(|decode_impl| !decode_impl.is_empty()) =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_decode_started.insert(request_id);
            }
        }
        ("myelin.worker.prompt", Some("FirstTokenReady"))
            if event.get("token_index").and_then(Value::as_u64) == Some(1) =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_first_token_ready.insert(request_id);
            }
        }
        ("myelin.worker.prompt", Some("DecodeReady"))
            if event_positive_u64(event, "tokens_generated") =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_decode_ready.insert(request_id);
            }
        }
        ("myelin.worker.prompt", Some("PromptCompleted"))
            if event
                .get("generated_tokens")
                .and_then(Value::as_array)
                .is_some_and(|tokens| !tokens.is_empty()) =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_prompt_completed.insert(request_id);
            }
        }
        ("myelin.worker.tokenizer", Some("PromptEncoded"))
            if event
                .get("tokens")
                .and_then(Value::as_array)
                .is_some_and(|tokens| !tokens.is_empty()) =>
        {
            if let Some(request_id) = event_request_id(event) {
                facts.gpu_pipeline_prompt_encoded.insert(request_id);
            }
        }
        ("myelin.orch.prompt", Some("OrchPromptEvent"))
            if phase == Some("pipeline_tokenizer_encode")
                && status == Some("ready")
                && detail_u64(event, "tokens").is_some_and(|tokens| tokens > 0)
                && event_request_id(event).is_some() =>
        {
            facts
                .gpu_pipeline_prompt_encoded
                .insert(event_request_id(event).expect("guarded request_id"));
        }
        ("myelin.worker.tokenizer", Some("TokensDecoded"))
            if event
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty()) =>
        {
            if let Some(request_id) = event_request_id(event) {
                facts.gpu_pipeline_tokens_decoded.insert(request_id);
            }
        }
        ("myelin.worker.step", Some("StepExecuted"))
            if event_positive_u64(event, "committed_bytes") =>
        {
            let backend = event.get("execution_backend").and_then(Value::as_str);
            if matches!(backend, Some("pipeline_stage" | "full_transformer")) {
                facts.gpu_pipeline_real_worker_step_seen = true;
            }
        }
        ("myelin.orch.prompt", Some("OrchPromptEvent"))
            if phase == Some("pipeline_token_in")
                && status == Some("ready")
                && event_request_id(event).is_some() =>
        {
            let request_id = event_request_id(event).expect("guarded request_id");
            facts.gpu_pipeline_token_in.insert(request_id);
            if event
                .get("detail")
                .and_then(|detail| detail.get("begin_sequence"))
                .and_then(Value::as_bool)
                == Some(true)
            {
                facts.gpu_pipeline_prompt_begin.insert(request_id);
            }
        }
        ("myelin.orch.prompt", Some("OrchPromptEvent"))
            if phase == Some("pipeline_token_out")
                && status == Some("observed")
                && event
                    .get("detail")
                    .and_then(|detail| detail.get("token_id"))
                    .and_then(Value::as_u64)
                    .is_some()
                && event_request_id(event).is_some() =>
        {
            facts
                .gpu_pipeline_token_out
                .insert(event_request_id(event).expect("guarded request_id"));
        }
        ("myelin.orch.prompt", Some("OrchPromptEvent"))
            if phase == Some("pipeline_tokenizer_decode")
                && status == Some("ready")
                && event
                    .get("detail")
                    .and_then(|detail| detail.get("text_bytes"))
                    .and_then(Value::as_u64)
                    .is_some_and(|bytes| bytes > 0)
                && event_request_id(event).is_some() =>
        {
            facts
                .gpu_pipeline_tokenizer_decode_ready
                .insert(event_request_id(event).expect("guarded request_id"));
        }
        _ => {}
    }
}

fn require_gpu_dump_log_facts(facts: &DumpLogFacts) -> Result<(), String> {
    if facts.gpu_cpu_fallback_seen {
        return Err("myelin-chat-check: GPU run fell back to the tinygrad CPU compiler".to_owned());
    }
    require_dump_log_fact(
        facts.gpu_worker_device_requested,
        "TinygradImportStarted requested_device CUDA",
    )?;
    require_dump_log_fact(facts.gpu_import_ready, "TinygradImportReady env_DEV CUDA")?;
    require_dump_log_fact(facts.gpu_probe_ready, "TinygradDeviceProbeReady CUDA probe")?;
    require_dump_log_fact(facts.gpu_worker_ready, "WorkerReady CUDA backend")?;
    for request_id in 1..=2 {
        let direct_decode = facts.gpu_decode_started.contains(&request_id)
            && facts.gpu_first_token_ready.contains(&request_id)
            && facts.gpu_decode_ready.contains(&request_id)
            && facts.gpu_prompt_completed.contains(&request_id);
        let pipeline_decode = facts.gpu_pipeline_prompt_encoded.contains(&request_id)
            && facts.gpu_pipeline_prompt_begin.contains(&request_id)
            && facts.gpu_pipeline_token_in.contains(&request_id)
            && facts.gpu_pipeline_token_out.contains(&request_id)
            && facts
                .gpu_pipeline_tokenizer_decode_ready
                .contains(&request_id);
        require_dump_log_fact(
            direct_decode || pipeline_decode,
            &format!("GPU decode/token evidence request_id={request_id}"),
        )?;
    }
    Ok(())
}

fn require_multinode_docker_network_facts(facts: &DumpLogFacts) -> Result<(), String> {
    require_dump_log_fact(
        facts
            .docker_node_spec_worker_count
            .is_some_and(|count| count >= 2),
        "Docker node_spec with multiple workers",
    )?;
    require_dump_log_fact(
        facts.worker_iroh_ready.len() >= 2,
        "Docker worker iroh_driver ready for multiple nodes",
    )?;
    require_dump_log_fact(
        facts.docker_worker_coordinator_join.len() >= 2,
        "Docker worker direct coordinator_join for multiple nodes",
    )
}

fn require_vastai_network_facts(facts: &DumpLogFacts) -> Result<(), String> {
    require_dump_log_fact(
        facts
            .vastai_node_spec_worker_count
            .is_some_and(|count| count >= 2),
        "VastAI node_spec with multiple workers",
    )?;
    require_dump_log_fact(
        !facts.vastai_provision_start_nodes.is_empty(),
        "VastAI ProvisionStart",
    )?;
    require_dump_log_fact(
        !facts.vastai_provider_start_nodes.is_empty(),
        "VastAI provider_start",
    )?;
    require_dump_log_fact(
        facts.vastai_node_runtime_ready_nodes.len() >= 2 || facts.worker_iroh_ready.len() >= 2,
        "VastAI worker runtime ready for multiple nodes",
    )
}

fn require_vastai_data_path_facts(facts: &DumpLogFacts) -> Result<(), String> {
    require_dump_log_fact(
        facts.ring_installed_ingress,
        "worker ingress ring installed",
    )?;
    require_dump_log_fact(facts.ring_installed_egress, "worker egress ring installed")?;
    let downstream_activation = facts.activation_downstream_object_loaded
        && facts.max_activation_record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES;
    require_dump_log_fact(
        facts.activation_step_executed || downstream_activation,
        "activation-producing worker step executed or downstream activation loaded",
    )?;
    let explicit_transport = facts.activation_egress_ring_read
        && facts.activation_iroh_edge_sent
        && facts.activation_iroh_edge_read
        && facts.activation_ingress_ring_write;
    let downstream_prompt_output = facts.activation_egress_record_written
        && facts.pipeline_token_out_requests.len() >= 2
        && facts.vastai_node_runtime_ready_nodes.len() >= 2;
    require_dump_log_fact(
        explicit_transport
            || facts.activation_interstage_handoff
            || downstream_activation
            || downstream_prompt_output,
        "activation inter-stage transport evidence",
    )?;
    let payload_outsizes_observed_command = facts.max_worker_command_bytes > 0
        && facts.max_activation_record_bytes > facts.max_worker_command_bytes;
    let activation_sized_interstage_handoff = (facts.activation_interstage_handoff
        || facts.activation_downstream_object_loaded)
        && facts.max_activation_record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES;
    let activation_sized_egress_record = facts.activation_egress_record_written
        && facts.max_activation_record_bytes >= DATA_PATH_MIN_PAYLOAD_BYTES;
    require_dump_log_fact(
        payload_outsizes_observed_command
            || activation_sized_interstage_handoff
            || activation_sized_egress_record,
        "activation payload not carried as worker JSON command",
    )
}

fn event_requested_device_is_cuda(event: &Value) -> bool {
    event
        .get("requested_device")
        .and_then(Value::as_str)
        .is_some_and(is_cuda_device)
}

fn event_env_dev_is_cuda(event: &Value) -> bool {
    event
        .get("env_DEV")
        .and_then(Value::as_str)
        .is_some_and(is_cuda_device)
}

fn event_probe_result_is_one(event: &Value) -> bool {
    event
        .get("probe_result")
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value.as_i64() == Some(1)))
}

fn worker_ready_backend_is_cuda(event: &Value) -> bool {
    let Some(backend) = event.get("backend") else {
        return false;
    };
    backend
        .get("requested_device")
        .and_then(Value::as_str)
        .is_some_and(is_cuda_device)
        && backend
            .get("env_DEV")
            .and_then(Value::as_str)
            .is_some_and(is_cuda_device)
        && backend
            .get("tinygrad_device")
            .and_then(Value::as_str)
            .is_some_and(is_cuda_device)
        && event_probe_result_is_one_from_key(event, "cuda_probe")
}

fn event_probe_result_is_one_from_key(event: &Value, key: &str) -> bool {
    event
        .get(key)
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| value.as_i64() == Some(1)))
}

fn event_positive_u64(event: &Value, key: &str) -> bool {
    event
        .get(key)
        .and_then(Value::as_u64)
        .is_some_and(|value| value > 0)
}

fn is_cuda_device(value: &str) -> bool {
    value.to_ascii_uppercase().contains("CUDA")
}

fn event_node_id(event: &Value) -> Option<u64> {
    event.get("node_id").and_then(Value::as_u64)
}

fn detail_u64(event: &Value, key: &str) -> Option<u64> {
    event
        .get("detail")
        .and_then(|detail| detail.get(key))
        .and_then(Value::as_u64)
}

fn detail_str<'a>(event: &'a Value, key: &str) -> Option<&'a str> {
    event
        .get("detail")
        .and_then(|detail| detail.get(key))
        .and_then(Value::as_str)
}

fn detail_bool(event: &Value, key: &str) -> Option<bool> {
    event
        .get("detail")
        .and_then(|detail| detail.get(key))
        .and_then(Value::as_bool)
}

fn dump_log_request_id(event: &Value) -> Option<u64> {
    event
        .get("detail")
        .and_then(|detail| detail.get("request_id"))
        .and_then(Value::as_u64)
}
fn event_request_id(event: &Value) -> Option<u64> {
    event.get("request_id").and_then(Value::as_u64)
}

fn require_dump_log_fact(found: bool, fact: &str) -> Result<(), String> {
    if found {
        Ok(())
    } else {
        Err(format!("myelin-chat-check: missing {fact}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(1);

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn myelin_chat_launcher_builds_dashboard_feature() {
        assert!(
            MYELIN_CHAT_CARGO_RUN_ARGS
                .windows(2)
                .any(|pair| pair[0] == "--features" && pair[1] == "dashboard"),
            "{MYELIN_CHAT_CARGO_RUN_ARGS:?}"
        );
        assert!(
            MYELIN_CHAT_CARGO_RUN_ARGS
                .windows(2)
                .any(|pair| pair[0] == "--bin" && pair[1] == "myelin-chat"),
            "{MYELIN_CHAT_CARGO_RUN_ARGS:?}"
        );
    }

    #[test]
    fn scenario_flags_select_expected_launch_contract() {
        let dump_log = Path::new("/tmp/myelin-chat-check.ndjson");

        let baseline =
            MyelinChatCheckInvocation::parse_args(Vec::new()).expect("default scenario parses");
        assert_eq!(
            baseline.scenario(),
            MyelinChatCheckScenario::ProcessBaseline
        );
        assert_eq!(
            baseline.myelin_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );

        let gpu = MyelinChatCheckInvocation::parse_args(strings(&["--gpu"])).expect("gpu parses");
        assert_eq!(gpu.scenario(), MyelinChatCheckScenario::Gpu);
        assert!(gpu.env_overrides().is_empty());
        assert_eq!(
            gpu.myelin_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--gpu",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );

        let multinode = MyelinChatCheckInvocation::parse_args(strings(&["--multinode"]))
            .expect("multinode parses");
        assert_eq!(multinode.scenario(), MyelinChatCheckScenario::Multinode);
        assert_eq!(
            multinode.myelin_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--pipeline-stages",
                "2",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );

        let multinode_docker =
            MyelinChatCheckInvocation::parse_args(strings(&["--multinode-docker"]))
                .expect("multinode docker parses");
        assert_eq!(
            multinode_docker.scenario(),
            MyelinChatCheckScenario::MultinodeDocker
        );
        assert_eq!(
            multinode_docker.myelin_chat_args(42, dump_log),
            strings(&[
                "--docker",
                "--pipeline-stages",
                "2",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );

        let vastai =
            MyelinChatCheckInvocation::parse_args(strings(&["--vastai"])).expect("vastai parses");
        assert_eq!(vastai.scenario(), MyelinChatCheckScenario::VastAi);
        assert_eq!(
            vastai.myelin_chat_args(42, dump_log),
            strings(&[
                "--vastai",
                "--cached-model",
                "--yes",
                "--endpoint-addr-mask",
                "relay-only",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );

        let vastai_sweep =
            MyelinChatCheckInvocation::parse_args(strings(&["--vastai", "--pipeline-stages", "8"]))
                .expect("vastai sweep parses");
        assert_eq!(vastai_sweep.scenario(), MyelinChatCheckScenario::VastAi);
        assert_eq!(
            vastai_sweep.myelin_chat_args(42, dump_log),
            strings(&[
                "--vastai",
                "--pipeline-stages",
                "8",
                "--cached-model",
                "--yes",
                "--endpoint-addr-mask",
                "relay-only",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );
        let vastai_parallel = MyelinChatCheckInvocation::parse_args(strings(&[
            "--vastai",
            "--pipeline-parallel",
            "4",
        ]))
        .expect("vastai pipeline-parallel parses");
        assert_eq!(
            vastai_parallel.myelin_chat_args(42, dump_log),
            strings(&[
                "--vastai",
                "--pipeline-stages",
                "4",
                "--cached-model",
                "--yes",
                "--endpoint-addr-mask",
                "relay-only",
                "--run-id",
                "42",
                "--dump-logs=/tmp/myelin-chat-check.ndjson",
            ])
        );
    }

    #[test]
    fn scenario_flags_reject_unknown_or_ambiguous_invocations() {
        assert!(
            MyelinChatCheckInvocation::parse_args(strings(&["--docker"]))
                .expect_err("unknown flag fails")
                .contains("unsupported myelin-chat-check argument")
        );
        assert!(
            MyelinChatCheckInvocation::parse_args(strings(&["--gpu", "--multinode"]))
                .expect_err("multiple scenarios fail")
                .contains("at most one scenario flag")
        );
        assert!(
            MyelinChatCheckInvocation::parse_args(strings(&["--vastai", "--pipeline-stages", "0"]))
                .expect_err("zero pipeline stages fail")
                .contains("--pipeline-stages must be greater than 0")
        );
        assert!(
            MyelinChatCheckInvocation::parse_args(strings(&["--vastai", "--pipeline-stages"]))
                .expect_err("missing pipeline stages fail")
                .contains("--pipeline-stages requires a value")
        );
        assert!(
            MyelinChatCheckInvocation::parse_args(strings(&[
                "--vastai",
                "--pipeline-stages",
                "4",
                "--pipeline-parallel",
                "4",
            ]))
            .expect_err("duplicate pipeline aliases fail")
            .contains("pipeline stage count")
        );
    }

    fn temp_path(label: &str) -> PathBuf {
        let id = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "xtask-benchmark-observability-{label}-{}-{id}.ndjson",
            std::process::id()
        ))
    }

    #[test]
    fn failure_artifacts_include_gap_report_and_evidence_manifest() {
        let root = unique_temp_dir("myelin-chat-check-failure-artifacts");
        let paths = write_myelin_chat_check_paths(&root).expect("paths");
        fs::write(&paths.dump_log, "synthetic telemetry\n").expect("write telemetry");

        write_failure_artifacts(
            &paths,
            "child exited nonzero",
            "",
            "myelin-chat: missing required VAST_API_KEY\n",
            None,
        )
        .expect("failure artifacts");

        let summary: Value =
            serde_json::from_str(&fs::read_to_string(&paths.summary).expect("summary artifact"))
                .expect("summary json");
        assert_eq!(
            summary
                .pointer("/artifacts/benchmark_evidence/exists")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            summary
                .pointer("/artifacts/benchmark_gaps/exists")
                .and_then(Value::as_bool),
            Some(true)
        );
        let gaps = fs::read_to_string(&paths.benchmark_gaps).expect("gap report");
        assert!(gaps.contains("side-channel only, not benchmark evidence"));
        let evidence: Value = serde_json::from_str(
            &fs::read_to_string(&paths.benchmark_evidence).expect("evidence artifact"),
        )
        .expect("evidence json");
        assert_eq!(
            evidence.pointer("/status").and_then(Value::as_str),
            Some("failed")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn benchmark_observability_archive_record(
        channel: &str,
        payload: Value,
        arrival_unix_ms: u64,
    ) -> String {
        json!({
            "arrival_seq": 0,
            "arrival_unix_ms": arrival_unix_ms,
            "source": "unit-test",
            "stream": "unit#9",
            "channel": channel,
            "channel_id": 0,
            "position": 0,
            "payload": payload,
        })
        .to_string()
    }

    fn write_dump_log(label: &str, lines: Vec<String>) -> PathBuf {
        let path = temp_path(label);
        fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write synthetic dump log");
        path
    }

    fn parse_synthetic_events(
        label: &str,
        events: Vec<(&'static str, Value)>,
    ) -> Vec<DumpLogEvent> {
        let path = write_synthetic_event_dump(label, events);
        let parsed = parse_dump_log_events(&path).expect("parse synthetic dump log");
        let _ = fs::remove_file(path);
        parsed
    }

    fn write_synthetic_event_dump(label: &str, events: Vec<(&'static str, Value)>) -> PathBuf {
        let lines = events
            .into_iter()
            .enumerate()
            .map(|(index, (channel, event))| {
                benchmark_observability_archive_record(
                    channel,
                    json!({"encoding": "utf8", "value": event.to_string()}),
                    10_000 + index as u64,
                )
            })
            .collect::<Vec<_>>();
        write_dump_log(label, lines)
    }

    fn benchmark(component: &str, wall_unix_ms: u64, mono_ms: u64) -> Value {
        json!({
            "schema": 1,
            "schema_version": 1,
            "component": component,
            "producer_component": component,
            "producer_instance_id": format!("{component}:test"),
            "producer_process_id": 1,
            "pid": 1,
            "seq": wall_unix_ms,
            "producer_sequence": wall_unix_ms,
            "wall_unix_ms": wall_unix_ms,
            "wall_clock_unix_ms": wall_unix_ms,
            "mono_ms": mono_ms,
            "monotonic_ms": mono_ms,
            "clock_source": {
                "wall": "unit_test_unix_ms",
                "monotonic": "unit_test_elapsed_ms"
            },
        })
    }

    fn stamped(mut event: Value, component: &str, wall_unix_ms: u64, mono_ms: u64) -> Value {
        let stamp = benchmark(component, wall_unix_ms, mono_ms);
        let object = event.as_object_mut().expect("event object");
        object.insert("schema_version".to_owned(), json!(1));
        if let Some(event_type) = object.get("type").cloned() {
            object.insert("event_type".to_owned(), event_type);
        }
        if let Some(phase) = object.get("phase").cloned() {
            object.insert("event_name".to_owned(), phase);
        }
        object.insert(
            "producer_component".to_owned(),
            stamp["producer_component"].clone(),
        );
        object.insert(
            "producer_instance_id".to_owned(),
            stamp["producer_instance_id"].clone(),
        );
        object.insert(
            "producer_process_id".to_owned(),
            stamp["producer_process_id"].clone(),
        );
        object.insert(
            "producer_sequence".to_owned(),
            stamp["producer_sequence"].clone(),
        );
        object.insert(
            "wall_clock_unix_ms".to_owned(),
            stamp["wall_clock_unix_ms"].clone(),
        );
        object.insert("monotonic_ms".to_owned(), stamp["monotonic_ms"].clone());
        object.insert("clock_source".to_owned(), stamp["clock_source"].clone());
        object.insert(
            "span_id".to_owned(),
            json!(format!(
                "{component}:test:{}:{}",
                stamp["producer_sequence"],
                object
                    .get("event_name")
                    .and_then(Value::as_str)
                    .unwrap_or("event")
            )),
        );
        object.insert("parent_span_id".to_owned(), Value::Null);
        object.insert("benchmark".to_owned(), stamp);
        event
    }

    fn chat_span(phase: &str, status: &str, wall_unix_ms: u64, mono_ms: u64) -> Value {
        stamped(
            json!({
                "type": "ChatProgress",
                "phase": phase,
                "status": status,
                "run_id": 9,
                "detail": {},
            }),
            "myelin-chat",
            wall_unix_ms,
            mono_ms,
        )
    }

    fn prompt_chat_span(
        phase: &str,
        status: &str,
        request_id: u64,
        wall_unix_ms: u64,
        mono_ms: u64,
    ) -> Value {
        stamped(
            json!({
                "type": "ChatProgress",
                "phase": phase,
                "status": status,
                "run_id": 9,
                "detail": {
                    "request_id": request_id,
                    "prompt_index": request_id,
                    "prompt_hash": format!("hash-{request_id}"),
                    "prompt_bytes": 4,
                    "max_tokens": 8,
                    "tokens_generated": 3,
                    "elapsed_ms": 50,
                    "final_text_bytes": 5,
                    "response_started": true,
                },
            }),
            "myelin-chat",
            wall_unix_ms,
            mono_ms,
        )
    }

    fn worker_prompt_event(
        event_type: &str,
        request_id: u64,
        wall_unix_ms: u64,
        mono_ms: u64,
    ) -> Value {
        let mut event = stamped(
            json!({
                "type": event_type,
                "run_id": 9,
                "node_id": 3,
                "stage_index": 2,
                "request_id": request_id,
            }),
            "tinygrad-worker",
            wall_unix_ms,
            mono_ms,
        );
        let object = event.as_object_mut().expect("event object");
        match event_type {
            "DecodeStarted" => {
                object.insert("prompt_tokens".to_owned(), json!(4));
                object.insert("max_tokens".to_owned(), json!(8));
                object.insert("decode_impl".to_owned(), json!("device_resident_greedy"));
            }
            "FirstTokenReady" => {
                object.insert("token_index".to_owned(), json!(1));
            }
            "DecodeReady" => {
                object.insert("tokens_generated".to_owned(), json!(3));
            }
            _ => {}
        }
        if event_type == "PromptCompleted" {
            object.insert("generated_tokens".to_owned(), json!([1, 2, 3]));
        }
        event
    }

    fn pipeline_prompt_event(
        phase: &str,
        status: &str,
        request_id: u64,
        wall_unix_ms: u64,
        mono_ms: u64,
        detail: Value,
    ) -> Value {
        stamped(
            json!({
                "type": "OrchPromptEvent",
                "phase": phase,
                "status": status,
                "run_id": 9,
                "node_id": 1,
                "request_id": request_id,
                "detail": detail,
            }),
            "myelin-orchestrator",
            wall_unix_ms,
            mono_ms,
        )
    }

    fn benchmark_report_events(include_first_token: bool) -> Vec<(&'static str, Value)> {
        let mut events = benchmark_report_base_events();
        for request_id in 1..=2 {
            let base = 1_200 + request_id * 100;
            events.push((
                "myelin.chat.prompt",
                prompt_chat_span("prompt_submitted", "ready", request_id, base, base - 1_000),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event("PromptStarted", request_id, base + 5, request_id * 100),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "PromptEncodeStarted",
                    request_id,
                    base + 10,
                    request_id * 100 + 10,
                ),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "PromptEncodeReady",
                    request_id,
                    base + 15,
                    request_id * 100 + 15,
                ),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "DecodeStarted",
                    request_id,
                    base + 20,
                    request_id * 100 + 20,
                ),
            ));
            if include_first_token {
                events.push((
                    "myelin.worker.prompt",
                    worker_prompt_event(
                        "FirstTokenReady",
                        request_id,
                        base + 25,
                        request_id * 100 + 25,
                    ),
                ));
            }
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event("DecodeReady", request_id, base + 40, request_id * 100 + 40),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "TextDecodeStarted",
                    request_id,
                    base + 41,
                    request_id * 100 + 41,
                ),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "TextDecodeReady",
                    request_id,
                    base + 45,
                    request_id * 100 + 45,
                ),
            ));
            events.push((
                "myelin.worker.prompt",
                worker_prompt_event(
                    "PromptCompleted",
                    request_id,
                    base + 50,
                    request_id * 100 + 50,
                ),
            ));
            events.push((
                "myelin.chat.prompt",
                prompt_chat_span(
                    "request_completed",
                    "ready",
                    request_id,
                    base + 60,
                    base - 940,
                ),
            ));
        }
        events
    }

    fn pipeline_benchmark_report_events(include_first_token: bool) -> Vec<(&'static str, Value)> {
        let mut events = benchmark_report_base_events();
        for request_id in 1..=2 {
            let base = 1_200 + request_id * 100;
            events.push((
                "myelin.chat.prompt",
                prompt_chat_span("prompt_submitted", "ready", request_id, base, base - 1_000),
            ));
            events.push((
                "myelin.orch.prompt",
                pipeline_prompt_event(
                    "pipeline_tokenizer_encode",
                    "started",
                    request_id,
                    base + 5,
                    base + 5,
                    json!({"prompt_bytes":4}),
                ),
            ));
            events.push((
                "myelin.orch.prompt",
                pipeline_prompt_event(
                    "pipeline_tokenizer_encode",
                    "ready",
                    request_id,
                    base + 10,
                    base + 10,
                    json!({"tokens":4}),
                ),
            ));
            events.push((
                "myelin.orch.prompt",
                pipeline_prompt_event(
                    "pipeline_token_in",
                    "started",
                    request_id,
                    base + 12,
                    base + 12,
                    json!({"tokens":4}),
                ),
            ));
            if include_first_token {
                events.push((
                    "myelin.orch.prompt",
                    pipeline_prompt_event(
                        "pipeline_token_out",
                        "observed",
                        request_id,
                        base + 20,
                        base + 20,
                        json!({"sequence":0,"token_id":7}),
                    ),
                ));
            }
            events.push((
                "myelin.orch.prompt",
                pipeline_prompt_event(
                    "pipeline_tokenizer_decode",
                    "started",
                    request_id,
                    base + 21,
                    base + 21,
                    json!({"token_id":7}),
                ),
            ));
            events.push((
                "myelin.orch.prompt",
                pipeline_prompt_event(
                    "pipeline_tokenizer_decode",
                    "ready",
                    request_id,
                    base + 25,
                    base + 25,
                    json!({"text_bytes":1}),
                ),
            ));
            events.push((
                "myelin.chat.prompt",
                prompt_chat_span(
                    "request_completed",
                    "ready",
                    request_id,
                    base + 60,
                    base - 940,
                ),
            ));
        }
        events
    }

    fn benchmark_report_base_events() -> Vec<(&'static str, Value)> {
        vec![
            (
                "myelin.xtask.benchmark",
                stamped(
                    json!({
                        "type": "XtaskBenchmark",
                        "phase": "cargo_run_myelin_chat",
                        "status": "started",
                        "run_id": 9,
                        "detail": {},
                    }),
                    "xtask",
                    1_000,
                    0,
                ),
            ),
            (
                "myelin.xtask.benchmark",
                stamped(
                    json!({
                        "type": "XtaskBenchmark",
                        "phase": "cargo_run_myelin_chat",
                        "status": "ready",
                        "run_id": 9,
                        "detail": {},
                    }),
                    "xtask",
                    1_025,
                    25,
                ),
            ),
            (
                "myelin.chat.benchmark",
                stamped(
                    json!({
                        "type":"BenchmarkRunEnvelope",
                        "phase":"run_envelope",
                        "status":"ready",
                        "run_id":9,
                        "detail":{
                            "runtime":{"pipeline_stages":1,"endpoint_addr_mask":"full","gpu_run":false},
                            "provider":{"kind":"process","node_image":"unit"},
                            "model":{"id":"unit-model"},
                        },
                    }),
                    "myelin-chat",
                    995,
                    0,
                ),
            ),
            (
                "myelin.chat.benchmark",
                chat_span("endpoint_config_snapshot", "ready", 998, 0),
            ),
            (
                "myelin.chat.runtime",
                chat_span("prepare_runtime", "started", 1_030, 30),
            ),
            (
                "myelin.chat.runtime",
                chat_span("ensure_orch_binary", "ready", 1_035, 35),
            ),
            (
                "myelin.chat.runtime",
                chat_span("ensure_worker_binary", "ready", 1_036, 36),
            ),
            (
                "myelin.chat.runtime",
                chat_span("prepare_runtime", "ready", 1_040, 40),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({
                        "type": "OrchBootstrap",
                        "phase": "weights_loaded",
                        "status": "ready",

                        "run_id": 9,
                        "node_id": 1,
                        "detail": {},
                    }),
                    "myelin-orchestrator",
                    1_100,
                    100,
                ),
            ),
            (
                "myelin.node.worker",
                stamped(
                    json!({"type":"NodeEvent","phase":"worker_initialize","status":"ready","run_id":9,"node_id":3,"stage_index":2,"detail":{"device":"CPU"}}),
                    "myelin-worker",
                    1_050,
                    50,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"PythonTelemetryConnected","phase":"PythonTelemetryConnected","status":"ready","run_id":9,"node_id":3,"stage_index":2,"endpoint":{"transport":"stdout-json-lines"}}),
                    "tinygrad-worker",
                    1_060,
                    60,
                ),
            ),
            (
                "myelin.chat.runtime",
                chat_span("prompt_rpc", "ready", 1_120, 120),
            ),
        ]
    }

    fn dump_log_fact_events(
        include_gpu: bool,
        include_cpu_fallback: bool,
    ) -> Vec<(&'static str, Value)> {
        let mut events = vec![
            (
                "myelin.chat.lifecycle",
                chat_span("config", "ready", 1_000, 0),
            ),
            (
                "myelin.chat.benchmark",
                stamped(
                    json!({
                        "type":"BenchmarkRunEnvelope",
                        "phase":"run_envelope",
                        "status":"ready",
                        "run_id":9,
                        "detail":{
                            "runtime":{"pipeline_stages":1,"endpoint_addr_mask":if include_gpu { "relay-only" } else { "full" },"gpu_run":include_gpu},
                            "provider":{"kind":"process","node_image":"unit"},
                            "model":{"id":"unit"},
                        },
                    }),
                    "myelin-chat",
                    1_001,
                    0,
                ),
            ),
            (
                "myelin.chat.benchmark",
                chat_span("endpoint_config_snapshot", "ready", 1_002, 2),
            ),
            (
                "myelin.chat.runtime",
                chat_span("prepare_runtime", "ready", 1_010, 10),
            ),
            (
                "myelin.chat.runtime",
                chat_span("prompt_rpc", "ready", 1_020, 20),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"iroh_driver","status":"ready","run_id":9,"node_id":1,"detail":{}}),
                    "myelin-orchestrator",
                    1_030,
                    30,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{}}),
                    "myelin-worker",
                    1_040,
                    40,
                ),
            ),
            (
                "myelin.node.worker",
                stamped(
                    json!({"type":"NodeEvent","phase":"worker_initialize","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"device":"CUDA"}}),
                    "myelin-worker",
                    1_050,
                    50,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"PythonTelemetryConfigured","phase":"PythonTelemetryConfigured","status":"configured","run_id":9,"node_id":3,"stage_index":1,"endpoint":{"transport":"stdout-json-lines"}}),
                    "tinygrad-worker",
                    1_055,
                    55,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"PythonTelemetryConnected","phase":"PythonTelemetryConnected","status":"ready","run_id":9,"node_id":3,"stage_index":1,"endpoint":{"transport":"stdout-json-lines"}}),
                    "tinygrad-worker",
                    1_056,
                    56,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"weights_loaded","status":"ready","run_id":9,"node_id":1,"detail":{}}),
                    "myelin-orchestrator",
                    1_060,
                    60,
                ),
            ),
            (
                "myelin.chat.prompt",
                prompt_chat_span("response_text", "observed", 1, 1_200, 200),
            ),
            (
                "myelin.chat.prompt",
                prompt_chat_span("request_completed", "ready", 1, 1_210, 210),
            ),
            (
                "myelin.chat.prompt",
                prompt_chat_span("response_text", "observed", 2, 1_300, 300),
            ),
            (
                "myelin.chat.prompt",
                prompt_chat_span("request_completed", "ready", 2, 1_310, 310),
            ),
            (
                "myelin.chat.lifecycle",
                chat_span("shutdown", "requested", 1_400, 400),
            ),
            (
                "myelin.chat.component",
                chat_span("orchestrator_process", "stopped", 1_410, 410),
            ),
        ];

        if include_gpu {
            events.extend([
                (
                    "myelin.worker.initialize",
                    stamped(
                        json!({"type":"TinygradImportStarted","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","env_DEV":"CUDA"}),
                        "tinygrad-worker",
                        1_070,
                        70,
                    ),
                ),
                (
                    "myelin.worker.initialize",
                    stamped(
                        json!({"type":"TinygradImportReady","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","env_DEV":"CUDA"}),
                        "tinygrad-worker",
                        1_080,
                        80,
                    ),
                ),
                (
                    "myelin.worker.initialize",
                    stamped(
                        json!({"type":"TinygradDeviceProbeReady","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","probe_result":[1]}),
                        "tinygrad-worker",
                        1_090,
                        90,
                    ),
                ),
                (
                    "myelin.worker.initialize",
                    stamped(
                        json!({"type":"WorkerReady","run_id":9,"node_id":3,"stage_index":1,"backend":{"requested_device":"CUDA","env_DEV":"CUDA","tinygrad_device":"CUDA"},"cuda_probe":[1]}),
                        "tinygrad-worker",
                        1_100,
                        100,
                    ),
                ),
            ]);
            if include_cpu_fallback {
                events.push((
                    "myelin.worker.initialize",
                    stamped(
                        json!({"type":"TinygradCpuCompilerSelected","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","selected_device":"CPU:X86"}),
                        "tinygrad-worker",
                        1_105,
                        105,
                    ),
                ));
            }
            for request_id in 1..=2 {
                let base = 1_200 + request_id * 100;
                events.extend([
                    (
                        "myelin.worker.prompt",
                        worker_prompt_event("DecodeStarted", request_id, base + 20, base - 980),
                    ),
                    (
                        "myelin.worker.prompt",
                        worker_prompt_event("FirstTokenReady", request_id, base + 25, base - 975),
                    ),
                    (
                        "myelin.worker.prompt",
                        worker_prompt_event("DecodeReady", request_id, base + 40, base - 960),
                    ),
                    (
                        "myelin.worker.prompt",
                        worker_prompt_event("PromptCompleted", request_id, base + 50, base - 950),
                    ),
                ]);
            }
        }
        events
    }

    #[test]
    fn benchmark_observability_dump_log_path_parser_finds_equals_and_separate_forms() {
        assert_eq!(
            explicit_dump_log_path_from_myelin_chat_args(&strings(&["--dump-logs=/tmp/a.ndjson"])),
            Some(PathBuf::from("/tmp/a.ndjson"))
        );
        assert_eq!(
            explicit_dump_log_path_from_myelin_chat_args(&strings(&[
                "--",
                "--run-id",
                "7",
                "--dump-logs",
                "-logs.ndjson",
            ])),
            Some(PathBuf::from("-logs.ndjson"))
        );
        assert_eq!(
            explicit_dump_log_path_from_myelin_chat_args(&strings(&["--dump-logs"])),
            None
        );
        assert_eq!(
            explicit_dump_log_path_from_myelin_chat_args(&strings(&["--dump-logs", "--run-id"])),
            None
        );
        assert_eq!(
            run_id_from_myelin_chat_args(&strings(&["--", "--run-id", "42"])),
            Some(42)
        );
        assert_eq!(
            run_id_from_myelin_chat_args(&strings(&["--run-id", "0"])),
            None
        );
    }

    #[test]
    fn benchmark_observability_parse_dump_log_events_accepts_utf8_wrapper_and_plain_string_payload()
    {
        let wrapped = json!({"type": "Wrapped", "run_id": 9});
        let plain = json!({"type": "Plain", "run_id": 9});
        let path = write_dump_log(
            "payload-shapes",
            vec![
                benchmark_observability_archive_record(
                    "wrapped",
                    json!({"encoding": "utf8", "value": wrapped.to_string()}),
                    111,
                ),
                benchmark_observability_archive_record("plain", json!(plain.to_string()), 112),
                benchmark_observability_archive_record(
                    "bytes",
                    json!({"encoding": "bytes", "value": [0, 1]}),
                    113,
                ),
            ],
        );

        let events = parse_dump_log_events(&path).expect("parse dump log events");
        let _ = fs::remove_file(path);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].channel, "wrapped");
        assert_eq!(events[0].arrival_unix_ms, Some(111));
        assert_eq!(
            events[0].event.get("type").and_then(Value::as_str),
            Some("Wrapped")
        );
        assert_eq!(events[1].channel, "plain");
        assert_eq!(
            events[1].event.get("type").and_then(Value::as_str),
            Some("Plain")
        );
    }

    #[test]
    fn benchmark_observability_gpu_dump_facts_require_cuda_worker_and_decode_cycles() {
        let path = write_synthetic_event_dump("gpu-dump-facts", dump_log_fact_events(true, false));

        let events = assert_dump_log_facts(&path, MyelinChatCheckScenario::Gpu, 9, None)
            .expect("GPU dump log facts pass");
        let _ = fs::remove_file(path);

        assert!(events.iter().any(|event| {
            event.channel == "myelin.worker.initialize"
                && event.event.get("type").and_then(Value::as_str) == Some("WorkerReady")
        }));
    }

    fn gpu_pipeline_only_facts(prompt_begin_markers: bool) -> DumpLogFacts {
        let mut facts = DumpLogFacts {
            gpu_worker_device_requested: true,
            gpu_import_ready: true,
            gpu_probe_ready: true,
            gpu_worker_ready: true,
            ..DumpLogFacts::default()
        };
        for request_id in 1..=2 {
            facts.gpu_pipeline_prompt_encoded.insert(request_id);
            facts.gpu_pipeline_token_in.insert(request_id);
            facts.gpu_pipeline_token_out.insert(request_id);
            facts.gpu_pipeline_tokenizer_decode_ready.insert(request_id);
            if prompt_begin_markers {
                facts.gpu_pipeline_prompt_begin.insert(request_id);
            }
        }
        facts
    }

    #[test]
    fn benchmark_observability_gpu_pipeline_facts_require_prompt_begin_markers() {
        let valid = gpu_pipeline_only_facts(true);
        require_gpu_dump_log_facts(&valid).expect("pipeline facts pass");

        let missing_prompt_begin = gpu_pipeline_only_facts(false);
        let error = require_gpu_dump_log_facts(&missing_prompt_begin)
            .expect_err("missing prompt begin marker should fail");
        assert!(
            error.contains("GPU decode/token evidence request_id=1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn benchmark_observability_gpu_pipeline_facts_accept_orchestrator_encode_evidence() {
        let mut facts = gpu_pipeline_only_facts(true);
        facts.gpu_pipeline_prompt_encoded.clear();
        for request_id in 1..=2 {
            let event = pipeline_prompt_event(
                "pipeline_tokenizer_encode",
                "ready",
                request_id,
                1_000 + request_id,
                request_id,
                json!({"tokens":4}),
            );
            record_gpu_dump_log_event("myelin.orch.prompt", &event, &mut facts);
        }
        require_gpu_dump_log_facts(&facts).expect("orchestrator encode evidence passes");
    }

    #[test]
    fn benchmark_observability_multinode_docker_dump_facts_require_direct_network_events() {
        let mut events = dump_log_fact_events(false, false);
        events.extend([
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_spec","status":"ready","run_id":9,"node_id":1,"detail":{"endpoint_addr_mask":"full","provider":"docker","worker_count":2}}),
                    "myelin-orchestrator",
                    1_071,
                    71,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"endpoint_addr_mask":"full","has_relay":false,"direct_addr_count":3}}),
                    "myelin-worker",
                    1_072,
                    72,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"endpoint_addr_mask":"full","has_relay":false,"direct_addr_count":3}}),
                    "myelin-worker",
                    1_073,
                    73,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"coordinator_join","status":"started","run_id":9,"node_id":2,"stage_index":0,"detail":{"has_relay":false,"direct_addr_count":4}}),
                    "myelin-worker",
                    1_074,
                    74,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"coordinator_join","status":"started","run_id":9,"node_id":3,"stage_index":1,"detail":{"has_relay":false,"direct_addr_count":4}}),
                    "myelin-worker",
                    1_075,
                    75,
                ),
            ),
        ]);
        let path = write_synthetic_event_dump("multinode-docker-direct-network", events);
        assert_dump_log_facts(&path, MyelinChatCheckScenario::MultinodeDocker, 9, None)
            .expect("direct-network multinode Docker facts pass");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn benchmark_observability_vastai_dump_facts_require_remote_provider_events() {
        let mut events = dump_log_fact_events(true, false);
        events.extend([
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"failed","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":3,"stage_index":0,"attempt":1,"error":"transient provider failure"}}),
                    "myelin-orchestrator",
                    1_072,
                    72,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_spec","status":"ready","run_id":9,"node_id":1,"detail":{"endpoint_addr_mask":"relay-only","provider":"vastai","worker_count":2}}),
                    "myelin-orchestrator",
                    9_071,
                    9_071,
                ),
            ),
            (
                "myelin.provisioning.events",
                json!({"event":{"run_id":9,"node_id":3,"kind":"ProvisionStart","provider":"vastai","message":"starting vastai image registry.example/myelin-node:latest"}}),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"started","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":3,"stage_index":0}}),
                    "myelin-orchestrator",
                    9_073,
                    9_073,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"endpoint_addr_mask":"relay-only","has_relay":true,"direct_addr_count":0}}),
                    "myelin-worker",
                    9_074,
                    9_074,
                ),
            ),
            (
                "myelin.worker.ring",
                stamped(
                    json!({"type":"RingInstalled","run_id":9,"node_id":2,"stage_index":0,"ring_id":1,"direction":"egress","edge_id":77,"kind":"activation","max_extent":4096}),
                    "tinygrad-worker",
                    9_075,
                    9_075,
                ),
            ),
            (
                "myelin.worker.ring",
                stamped(
                    json!({"type":"RingInstalled","run_id":9,"node_id":3,"stage_index":1,"ring_id":2,"direction":"ingress","edge_id":77,"kind":"activation","max_extent":4096}),
                    "tinygrad-worker",
                    9_076,
                    9_076,
                ),
            ),
            (
                "myelin.worker.step",
                stamped(
                    json!({"type":"StepExecuted","run_id":9,"node_id":2,"stage_index":0,"execution_backend":"pipeline_stage","committed_bytes":4096}),
                    "tinygrad-worker",
                    9_078,
                    9_078,
                ),
            ),
            (
                "myelin.worker.ingress",
                stamped(
                    json!({"type":"ObjectLoaded","run_id":9,"node_id":3,"stage_index":1,"edge_id":77,"kind":"activation","extent":4056}),
                    "tinygrad-worker",
                    9_083,

                    9_083,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"started","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":4,"stage_index":1}}),
                    "myelin-orchestrator",
                    9_084,
                    9_084,
                ),
            ),
            (
                "myelin.node.worker",
                stamped(
                    json!({"type":"NodeEvent","phase":"worker_initialize","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"device":"CUDA"}}),
                    "myelin-worker",
                    9_085,
                    9_085,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"WorkerReady","run_id":9,"node_id":2,"stage_index":0,"backend":{"requested_device":"CUDA","env_DEV":"CUDA","tinygrad_device":"CUDA"},"cuda_probe":[1]}),
                    "tinygrad-worker",
                    9_086,
                    9_086,
                ),
            ),
        ]);
        let mut orch_runtime_only = DumpLogFacts {
            vastai_node_spec_worker_count: Some(3),
            ..DumpLogFacts::default()
        };
        orch_runtime_only.vastai_provision_start_nodes.insert(2);
        orch_runtime_only.vastai_provider_start_nodes.insert(2);
        orch_runtime_only
            .vastai_node_runtime_ready_nodes
            .extend([2, 3, 4]);
        let disconnected_edge_telemetry = DumpLogFacts {
            ring_installed_ingress: true,
            ring_installed_egress: true,
            activation_object_loaded: true,
            activation_downstream_object_loaded: true,
            max_activation_record_bytes: DATA_PATH_MIN_PAYLOAD_BYTES,
            ..DumpLogFacts::default()
        };
        require_vastai_data_path_facts(&disconnected_edge_telemetry)
            .expect("downstream activation load proves inter-stage VastAI transport");
        let mut stage0_and_prompt_output = DumpLogFacts {
            ring_installed_ingress: true,
            ring_installed_egress: true,
            worker_ingress_object_loaded: true,
            activation_step_executed: true,
            activation_egress_record_written: true,
            max_activation_record_bytes: DATA_PATH_MIN_PAYLOAD_BYTES,
            ..DumpLogFacts::default()
        };
        stage0_and_prompt_output
            .vastai_node_runtime_ready_nodes
            .extend([2, 3, 4]);
        stage0_and_prompt_output
            .pipeline_token_out_requests
            .extend([1, 2]);
        require_vastai_data_path_facts(&stage0_and_prompt_output)
            .expect("non-final stage activation plus prompt output proves VastAI transport");
        require_vastai_network_facts(&orch_runtime_only)
            .expect("orchestrator runtime-ready events are valid VastAI worker evidence");
        let path = write_synthetic_event_dump("vastai-remote-provider", events);
        assert_dump_log_facts(&path, MyelinChatCheckScenario::VastAi, 9, Some(2))
            .expect("VastAI remote provider facts pass");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn benchmark_observability_dump_facts_reject_unexpected_failed_events() {
        let mut events = dump_log_fact_events(false, false);
        events.push((
            "myelin.chat.runtime",
            chat_span("prepare_node_image", "failed", 1_500, 500),
        ));
        let path = write_synthetic_event_dump("unexpected-failed-event", events);

        let error =
            match assert_dump_log_facts(&path, MyelinChatCheckScenario::ProcessBaseline, 9, None) {
                Ok(_) => panic!("unexpected failed event should fail the check"),
                Err(error) => error,
            };
        let _ = fs::remove_file(path);

        assert!(
            error.contains("failed event channel=myelin.chat.runtime"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn benchmark_observability_multinode_docker_requires_direct_network_workers() {
        let mut valid = DumpLogFacts {
            docker_node_spec_worker_count: Some(2),
            ..DumpLogFacts::default()
        };
        valid.worker_iroh_ready.extend([2, 3]);
        valid.docker_worker_coordinator_join.extend([2, 3]);
        require_multinode_docker_network_facts(&valid).expect("direct-network Docker facts pass");

        let mut missing_worker = valid;
        missing_worker.worker_iroh_ready.remove(&3);
        let error = require_multinode_docker_network_facts(&missing_worker)
            .expect_err("single direct-network worker should fail");
        assert!(
            error.contains("Docker worker iroh_driver ready for multiple nodes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn validator_rejects_missing_python_telemetry_connectivity() {
        let mut events = dump_log_fact_events(false, false);
        events.retain(|(channel, event)| {
            !(*channel == "myelin.worker.initialize"
                && event.get("type").and_then(Value::as_str) == Some("PythonTelemetryConnected"))
        });
        let parsed = parse_synthetic_events("missing-python-telemetry", events);

        let validation = validate_benchmark_observability(
            &parsed,
            9,
            MyelinChatCheckScenario::ProcessBaseline,
            None,
        );

        assert!(validation.findings.iter().any(|finding| {
            finding.severity == "observability_gap"
                && finding.code == "python.telemetry.connected.missing"
        }));
    }

    #[test]
    fn benchmark_observability_gpu_dump_facts_reject_cpu_fallback() {
        let path = write_synthetic_event_dump("gpu-cpu-fallback", dump_log_fact_events(true, true));

        let error = match assert_dump_log_facts(&path, MyelinChatCheckScenario::Gpu, 9, None) {
            Ok(_) => panic!("CPU fallback should fail GPU check"),
            Err(error) => error,
        };
        let _ = fs::remove_file(path);

        assert!(
            error.contains("fell back to the tinygrad CPU compiler"),
            "{error}"
        );
    }

    #[test]
    fn validator_accepts_vastai_orchestrator_runtime_ready_when_worker_tail_missing() {
        let mut events = dump_log_fact_events(true, false);
        events.retain(|(channel, event)| {
            event_stage_index(event) != Some(1)
                || !(channel.starts_with("myelin.worker.") || channel.starts_with("myelin.node."))
        });
        events.extend([
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"started","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":2,"stage_index":0}}),
                    "myelin-orchestrator",
                    2_000,
                    1_000,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"started","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":3,"stage_index":1}}),
                    "myelin-orchestrator",
                    2_001,
                    1_001,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"provider_start","status":"started","run_id":9,"node_id":1,"detail":{"provider":"vastai","node_id":4,"stage_index":2}}),
                    "myelin-orchestrator",
                    2_002,
                    1_002,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_runtime_ready","status":"ready","run_id":9,"node_id":1,"detail":{"node_id":2,"stage_index":0,"endpoint":{"addrs":[{"Relay":"https://relay.example"}]}}}),
                    "myelin-orchestrator",
                    2_003,
                    1_003,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_runtime_ready","status":"ready","run_id":9,"node_id":1,"detail":{"node_id":3,"stage_index":1,"endpoint":{"addrs":[{"Relay":"https://relay.example"}]}}}),
                    "myelin-orchestrator",
                    2_004,
                    1_004,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_runtime_ready","status":"ready","run_id":9,"node_id":1,"detail":{"node_id":4,"stage_index":2,"endpoint":{"addrs":[{"Relay":"https://relay.example"}]}}}),
                    "myelin-orchestrator",
                    2_005,
                    1_005,
                ),
            ),
            (
                "myelin.orch.stage_route",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"stage_route_check","status":"observed","run_id":9,"node_id":1,"detail":{"stage_index":1,"stage_node_id":3,"member_state":"Alive","route_matches_ready":true}}),
                    "myelin-orchestrator",
                    2_006,
                    1_006,
                ),
            ),
            (
                "myelin.orch.stage_route",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"stage_route_check","status":"observed","run_id":9,"node_id":1,"detail":{"stage_index":2,"stage_node_id":4,"member_state":"Alive","route_matches_ready":true}}),
                    "myelin-orchestrator",
                    2_007,
                    1_007,
                ),
            ),
            (
                "myelin.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"endpoint_addr_mask":"relay-only","has_relay":true,"direct_addr_count":0}}),
                    "myelin-worker",
                    2_100,
                    1_100,
                ),
            ),
            (
                "myelin.node.worker",
                stamped(
                    json!({"type":"NodeEvent","phase":"worker_initialize","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"device":"CUDA"}}),
                    "myelin-worker",
                    2_101,
                    1_101,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"PythonTelemetryConnected","phase":"PythonTelemetryConnected","status":"ready","run_id":9,"node_id":2,"stage_index":0,"endpoint":{"transport":"stdout-json-lines"}}),
                    "tinygrad-worker",
                    2_200,
                    1_200,
                ),
            ),
            (
                "myelin.worker.initialize",
                stamped(
                    json!({"type":"WorkerReady","run_id":9,"node_id":2,"stage_index":0,"backend":{"requested_device":"CUDA","env_DEV":"CUDA","tinygrad_device":"CUDA"},"cuda_probe":[1]}),
                    "tinygrad-worker",
                    2_201,
                    1_201,
                ),
            ),
            (
                "myelin.worker.ring",
                stamped(
                    json!({"type":"RingInstalled","run_id":9,"node_id":2,"stage_index":0,"ring_id":1,"direction":"egress","edge_id":70,"kind":"activation","max_extent":4096}),
                    "tinygrad-worker",
                    2_202,
                    1_202,
                ),
            ),
            (
                "myelin.worker.ingress",
                stamped(
                    json!({"type":"ObjectLoaded","run_id":9,"node_id":4,"stage_index":2,"edge_id":3,"kind":"activation","extent":4056}),
                    "tinygrad-worker",
                    2_203,
                    1_203,
                ),
            ),
        ]);
        let parsed = parse_synthetic_events("vastai-orchestrator-runtime-ready", events);

        let validation =
            validate_benchmark_observability(&parsed, 9, MyelinChatCheckScenario::VastAi, Some(3));

        let invalid = validation
            .invalid_findings()
            .map(|finding| finding.code)
            .collect::<Vec<_>>();
        assert!(invalid.is_empty(), "unexpected findings: {invalid:?}");
        assert!(
            validation
                .stages_runtime_ready_via_orchestrator
                .contains(&1)
        );
        assert!(validation.stages_route_ready_via_orchestrator.contains(&1));
        assert!(validation.stages_route_ready_via_orchestrator.contains(&2));
    }

    #[test]
    fn validator_rejects_wrong_run_id_as_fatal_gap() {
        let mut events = dump_log_fact_events(false, false);
        events.push((
            "myelin.chat.runtime",
            stamped(
                json!({"type":"ChatProgress","phase":"prepare_runtime","status":"ready","run_id":99,"detail":{}}),
                "myelin-chat",
                9_999,
                999,
            ),
        ));
        let parsed = parse_synthetic_events("wrong-run-id", events);

        let validation = validate_benchmark_observability(
            &parsed,
            9,
            MyelinChatCheckScenario::ProcessBaseline,
            None,
        );

        assert!(validation.findings.iter().any(|finding| {
            finding.severity == "fatal" && finding.code == "run_id.isolation.mismatch"
        }));
    }

    #[test]
    fn validator_rejects_missing_span_id_on_required_event() {
        let mut event = chat_span("prepare_runtime", "ready", 1_010, 10);
        event
            .as_object_mut()
            .expect("event object")
            .remove("span_id");
        let mut events = dump_log_fact_events(false, false);
        events.push(("myelin.chat.runtime", event));
        let parsed = parse_synthetic_events("missing-span-id", events);

        let validation = validate_benchmark_observability(
            &parsed,
            9,
            MyelinChatCheckScenario::ProcessBaseline,
            None,
        );

        assert!(validation.findings.iter().any(|finding| {
            finding.severity == "observability_gap"
                && finding.code == "canonical.event_field.missing"
                && finding.json_pointer.as_deref() == Some("/span_id")
        }));
    }

    #[test]
    fn benchmark_observability_report_requires_granular_decode_events() {
        let events = parse_synthetic_events("missing-first-token", benchmark_report_events(false));

        let error =
            build_benchmark_report(&events, 80, 9, MyelinChatCheckScenario::ProcessBaseline)
                .expect_err("missing first token should fail");

        assert!(
            error.starts_with("myelin-chat-check: missing benchmark event "),
            "{error}"
        );
        assert!(
            error.contains("FirstTokenReady") && error.contains("request_id=1"),
            "{error}"
        );
    }

    #[test]
    fn benchmark_observability_report_uses_pipeline_prompt_events() {
        let events =
            parse_synthetic_events("pipeline-report", pipeline_benchmark_report_events(true));

        let report =
            build_benchmark_report(&events, 80, 9, MyelinChatCheckScenario::ProcessBaseline)
                .expect("pipeline report builds");

        assert!(
            report
                .lines
                .iter()
                .any(|line| line == "myelin-chat-check: benchmark: run_id=9")
        );
        assert!(report.lines.iter().any(|line| {
            line.starts_with("myelin-chat-check: benchmark prompt 1 ")
                && line.contains("first_token_ms=8")
                && line.contains("decode_ms=8")
        }));
        assert!(report.lines.iter().any(|line| {
            line.starts_with("myelin-chat-check: benchmark prompt 2 ")
                && line.contains("first_token_ms=8")
                && line.contains("decode_ms=8")
        }));
    }

    #[test]
    fn benchmark_summary_preserves_workload_artifacts_and_prompt_timings() {
        let mut event_pairs = benchmark_report_events(true);
        event_pairs.push((
            "myelin.chat.benchmark",
            stamped(
                json!({
                    "type": "BenchmarkRunEnvelope",
                    "phase": "run_envelope",
                    "status": "ready",
                    "run_id": 9,
                    "detail": {
                        "model": {"id": "unit-model"},
                        "runtime": {"pipeline_stages": 1, "gpu_run": false},
                        "provider": {"kind": "process", "node_image": "unit-image"},
                    },
                }),
                "myelin-chat",
                2_001,
                1,
            ),
        ));
        let path = write_synthetic_event_dump("summary-contract", event_pairs);
        let events = parse_dump_log_events(&path).expect("parse summary events");
        let paths = MyelinChatCheckPaths {
            root: std::env::temp_dir(),
            dump_log: path.clone(),
            stdout: temp_path("summary-stdout"),
            stderr: temp_path("summary-stderr"),
            prompts: temp_path("summary-prompts"),
            redacted_config: temp_path("summary-config"),
            summary: temp_path("summary-json"),
            benchmark_evidence: temp_path("summary-evidence"),
            benchmark_gaps: temp_path("summary-gaps"),
        };

        let summary = build_benchmark_summary(
            &events,
            80,
            9,
            MyelinChatCheckScenario::ProcessBaseline,
            &paths,
            None,
            123,
            45,
        )
        .expect("summary builds");
        let _ = fs::remove_file(path);

        assert_eq!(
            summary.get("source").and_then(Value::as_str),
            Some("telemetry")
        );
        assert_eq!(
            summary
                .pointer("/run_envelope/detail/model/id")
                .and_then(Value::as_str),
            Some("unit-model")
        );
        assert_eq!(
            summary
                .pointer("/workload/prompts/0/prompt_hash")
                .and_then(Value::as_str),
            Some("hash-1")
        );
        assert_eq!(
            summary
                .pointer("/timings/prompts/0/first_token_ms/value_ms")
                .and_then(Value::as_u64),
            Some(5)
        );
        assert!(
            summary
                .pointer("/artifacts/telemetry/blake3")
                .and_then(Value::as_str)
                .is_some()
        );
        assert_eq!(
            summary
                .pointer("/artifacts/stdout/bytes")
                .and_then(Value::as_u64),
            Some(123)
        );
        assert_eq!(
            summary
                .pointer("/artifacts/stderr/bytes")
                .and_then(Value::as_u64),
            Some(45)
        );
        assert!(
            summary
                .pointer("/artifacts/benchmark_evidence/path")
                .and_then(Value::as_str)
                .is_some()
        );
        assert!(
            summary
                .pointer("/artifacts/benchmark_gaps/path")
                .and_then(Value::as_str)
                .is_some()
        );
        assert_eq!(
            summary.pointer("/validator/status").and_then(Value::as_str),
            Some("valid")
        );
    }

    #[test]
    fn benchmark_summary_exposes_vastai_pipeline_operator_summaries() {
        let mut event_pairs = dump_log_fact_events(true, false);
        event_pairs.extend([
            (
                "myelin.provisioning.logs.node.2.stderr",
                json!({
                    "run_id": 9,
                    "node_id": 2,
                    "stream": "Stderr",
                    "line": "lease_chain: index 0 → offer 42528153 — RTX 2060 12288MB @ $0.036/hr [India, IN] host 581196 eff $0.036/hr",
                }),
            ),
            (
                "myelin.provisioning.logs.node.8.stderr",
                json!({
                    "run_id": 9,
                    "node_id": 8,
                    "stream": "Stderr",
                    "line": "VastAI SSH bootstrap retrying in 3s after failed attempt",
                }),
            ),
            (
                "host.gpu",
                json!({
                    "schema":"host.gpu.v1",
                    "seq":1,
                    "sample_unix_ms":1_700,
                    "query_elapsed_ms":4,
                    "gpus":[{
                        "index":0,
                        "uuid":"GPU-unit",
                        "name":"RTX 2060",
                        "memory_used_mib":8120,
                        "memory_total_mib":12288,
                        "utilization_gpu_percent":73,
                        "utilization_memory_percent":41,
                        "temperature_c":61,
                        "power_draw_w":120.0
                    }],
                    "processes":[],
                    "error":null
                }),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"stage_provision_send","status":"sent","run_id":9,"node_id":1,"detail":{"attempt":1,"stage_count":8,"stage_index":7,"loaded_stage_count":0,"stage_send_count":1}}),
                    "myelin-orchestrator",
                    1_500,
                    500,
                ),
            ),
            (
                "myelin.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"stage_provision_send","status":"sent","run_id":9,"node_id":1,"detail":{"attempt":2,"stage_count":8,"stage_index":7,"loaded_stage_count":0,"stage_send_count":2}}),
                    "myelin-orchestrator",
                    1_501,
                    501,
                ),
            ),
        ]);
        for request_id in 1..=2 {
            let base = 1_600 + request_id * 100;
            event_pairs.extend([
                (
                    "myelin.worker.tokenizer",
                    stamped(
                        json!({"type":"PromptEncoded","run_id":9,"node_id":2,"stage_index":0,"request_id":request_id,"tokens":[1,2,3]}),
                        "tinygrad-worker",
                        base,
                        base,
                    ),
                ),
                (
                    "myelin.orch.prompt",
                    pipeline_prompt_event(
                        "pipeline_token_in",
                        "started",
                        request_id,
                        base,
                        base,
                        json!({"begin_sequence":true,"edge_id":1,"sequence":request_id}),
                    ),
                ),
                (
                    "myelin.orch.prompt",
                    pipeline_prompt_event(
                        "pipeline_token_in",
                        "ready",
                        request_id,
                        base + 1,
                        base + 1,
                        json!({"begin_sequence":true,"edge_id":1,"sequence":request_id}),
                    ),
                ),
                (
                    "myelin.orch.prompt",
                    pipeline_prompt_event(
                        "pipeline_token_out",
                        "observed",
                        request_id,
                        base + 2,
                        base + 2,
                        json!({"edge_id":9,"sequence":request_id,"token_id":7}),
                    ),
                ),
                (
                    "myelin.orch.prompt",
                    pipeline_prompt_event(
                        "pipeline_tokenizer_decode",
                        "ready",
                        request_id,
                        base + 3,
                        base + 3,
                        json!({"text_bytes":1}),
                    ),
                ),
            ]);
        }
        event_pairs.push((
            "myelin.worker.step",
            stamped(
                json!({"type":"StepExecuted","run_id":9,"node_id":2,"stage_index":0,"execution_backend":"pipeline_stage","committed_bytes":4096}),
                "tinygrad-worker",
                1_900,
                900,
            ),
        ));
        event_pairs.extend([
            (
                "myelin.node.stage",
                stamped(
                    json!({"type":"NodeEvent","phase":"egress_ring_read","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"edge_id":77,"edge_kind":"Activation","ring_id":11,"step_id":5,"sequence":3,"object_id":44,"record_bytes":4096,"helper_execute_ms":9,"egress_ring_read_ms":2}}),
                    "myelin-worker",
                    1_901,
                    901,
                ),
            ),
            (
                "myelin.node.stage",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_edge_bytes_sent","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"edge_id":77,"edge_kind":"Activation","step_id":5,"sequence":3,"object_id":44,"record_bytes":4096,"send_ms":1}}),
                    "myelin-worker",
                    1_902,
                    902,
                ),
            ),
            (
                "myelin.node.stage",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_edge_bytes_read","status":"observed","run_id":9,"node_id":3,"stage_index":1,"detail":{"edge_id":77,"stream_id":1,"bytes":4096}}),
                    "myelin-worker",
                    1_903,
                    903,
                ),
            ),
            (
                "myelin.node.stage",
                stamped(
                    json!({"type":"NodeEvent","phase":"ingress_ring_write","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"edge_id":77,"edge_kind":"Activation","ring_id":12,"stream_id":1,"object_id":44,"sequence":3,"record_bytes":4096,"ingress_ring_write_ms":3}}),
                    "myelin-worker",
                    1_904,
                    904,
                ),
            ),
            (
                "myelin.node.stage",
                stamped(
                    json!({"type":"NodeEvent","phase":"object_loaded","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"edge_id":77,"edge_kind":"Activation","ring_id":12,"stream_id":1,"object_id":44,"sequence":3,"handle_id":99,"object_load_ms":4}}),
                    "myelin-worker",
                    1_905,
                    905,
                ),
            ),
        ]);
        let path = write_synthetic_event_dump("vastai-summary-operator", event_pairs);
        let events = parse_dump_log_events(&path).expect("parse summary events");
        let paths = MyelinChatCheckPaths {
            root: std::env::temp_dir(),
            dump_log: path.clone(),
            stdout: temp_path("vastai-summary-stdout"),
            stderr: temp_path("vastai-summary-stderr"),
            prompts: temp_path("vastai-summary-prompts"),
            redacted_config: temp_path("vastai-summary-config"),
            summary: temp_path("vastai-summary-json"),
            benchmark_evidence: temp_path("vastai-summary-evidence"),
            benchmark_gaps: temp_path("vastai-summary-gaps"),
        };

        let summary = build_benchmark_summary(
            &events,
            80,
            9,
            MyelinChatCheckScenario::VastAi,
            &paths,
            Some(1),
            12,
            34,
        )
        .expect("summary builds");
        let _ = fs::remove_file(path);

        let invariants = summary
            .pointer("/invariants")
            .and_then(Value::as_array)
            .expect("invariants array");
        assert!(!invariants.iter().any(|invariant| {
            matches!(
                invariant.get("name").and_then(Value::as_str),
                Some("activation_large_object_iroh_sent" | "activation_large_object_iroh_read")
            )
        }));
        assert_eq!(
            summary
                .pointer("/gpu/pipeline_token_out_request_ids/0")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/gpu/pipeline_tokenizer_decode_ready_request_ids/1")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            summary
                .pointer("/vastai/selected_lease_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/vastai/selected_leases/0/gpu_name")
                .and_then(Value::as_str),
            Some("RTX 2060")
        );
        assert_eq!(
            summary
                .pointer("/pipeline/provisioning/max_send_events_for_stage")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            summary
                .pointer("/pipeline/provisioning/latest_wait/waiting_stage_index")
                .and_then(Value::as_u64),
            Some(7)
        );
        assert_eq!(
            summary
                .pointer("/pipeline/provisioning/ssh_bootstrap/retry_counts_by_node/8")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/gpu/host_gpu_samples/sample_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/gpu/host_gpu_samples/utilization_gpu_percent/max")
                .and_then(Value::as_u64),
            Some(73)
        );
        assert_eq!(
            summary
                .pointer("/pipeline/prompt_critical_paths/0/token_in_to_out_ms/count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/pipeline/edge_handoffs/0/producer_ring_reads")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            summary
                .pointer("/pipeline/edge_handoffs/0/ingress_ring_write_ms/sum")
                .and_then(Value::as_u64),
            Some(3)
        );
    }
}

/// Scan control-plane source files for forbidden telemetry frame type references.
/// The telemetry is metrics/logging only; control decisions must never branch
/// on a frame. Frame types live in `telemetry::frame::*` (not re-exported at
/// root) and must not appear in orchestration or other control modules.
fn check_telemetry_isolation() -> ExitCode {
    /// Directories whose .rs files are control-plane: they must not touch
    /// frame types or read-side modules.
    const CONTROL_DIRS: &[&str] = &[
        "apps/myelin/src/orchestration",
        "crates/distribution/src",
        "crates/data-plane/src",
        "crates/provisioning/src",
    ];

    /// Substrings that indicate a telemetry frame type or read-side module
    /// has leaked into control code.  `telemetry::frame::` covers Frame,
    /// TelemetryEvent, FrameDelivery, and every other frame-module type.
    const FORBIDDEN: &[&str] = &[
        "telemetry::frame::",
        "telemetry::store::",
        "telemetry::ingest::",
        "telemetry::views::",
        "telemetry::transport::",
        "CollectedTelemetryFrame",
    ];
    let mut violations = Vec::new();
    for dir in CONTROL_DIRS {
        collect_rs_files(dir, &mut violations);
    }

    let mut found = false;
    for file in &violations {
        let Ok(src) = std::fs::read_to_string(file) else {
            continue;
        };
        for (lineno, line) in src.lines().enumerate() {
            for pat in FORBIDDEN {
                if line.contains(pat) {
                    eprintln!(
                        "telemetry-isolation violation: {file}:{}: {}",
                        lineno + 1,
                        line.trim()
                    );
                    found = true;
                }
            }
        }
    }

    if found {
        eprintln!(
            "\ntelemetry-isolation: control-plane code must not import frame types \
             or read-side modules. Use the telemetry producer API (root re-exports) \
             for emitting telemetry, never `telemetry::frame::*` for reading it."
        );
        ExitCode::from(1)
    } else {
        println!("telemetry-isolation: OK — no frame types in control-plane modules.");
        ExitCode::SUCCESS
    }
}

/// Recursively collect .rs file paths under `dir` into `out`.
fn collect_rs_files(dir: &str, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(s) = path.to_str() {
                collect_rs_files(s, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            if let Some(s) = path.to_str() {
                out.push(s.to_owned());
            }
        }
    }
}

mod provisioning_demo;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("check-telemetry-isolation") => check_telemetry_isolation(),
        Some("test") if args.next().is_none() => run_tests(),
        Some("myelin-chat-check") => run_myelin_chat_check(args.collect()),
        Some("myelin-chat-compare") => run_myelin_chat_compare(args.collect()),
        Some("myelin-chat") => run_myelin_chat(args.collect()),
        Some("provisioning-reconciler-demo") => {
            provisioning_demo::run(&args.collect::<Vec<String>>())
        }
        Some("help" | "--help" | "-h") | None => {
            print_usage();
            ExitCode::SUCCESS
        }
        _ => {
            print_usage();
            ExitCode::from(1)
        }
    }
}
