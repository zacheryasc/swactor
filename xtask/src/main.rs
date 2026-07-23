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

const MVP_CHAT_CHECK_TIMEOUT_SECS: u64 = 900;
const MVP_CHAT_CHECK_POLL_MS: u64 = 100;
const MVP_CHAT_CHECK_TERM_GRACE_MS: u64 = 2_000;
const MVP_CHAT_CHECK_PROMPTS: &[u8] = b"ping\nsecond prompt\n";

struct MvpChatCheckPaths {
    root: PathBuf,
    dump_log: PathBuf,
}

struct MvpChatCheckOutput {
    status: ExitStatus,
    child_elapsed_ms: u64,
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdin_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MvpChatCheckScenario {
    ProcessBaseline,
    Gpu,
    Multinode,
    MultinodeDocker,
}

impl MvpChatCheckScenario {
    fn parse_args(args: Vec<String>) -> Result<Self, String> {
        let mut scenario = Self::ProcessBaseline;
        for arg in args {
            let selected = match arg.as_str() {
                "--gpu" => Self::Gpu,
                "--multinode" => Self::Multinode,
                "--multinode-docker" => Self::MultinodeDocker,
                other => return Err(format!("unsupported mvp-chat-check argument {other:?}")),
            };
            if scenario != Self::ProcessBaseline {
                return Err(
                    "mvp-chat-check accepts at most one scenario flag: --gpu, --multinode, or --multinode-docker"
                        .to_owned(),
                );
            }
            scenario = selected;
        }
        Ok(scenario)
    }

    fn name(self) -> &'static str {
        match self {
            Self::ProcessBaseline => "process",
            Self::Gpu => "gpu",
            Self::Multinode => "multinode",
            Self::MultinodeDocker => "multinode-docker",
        }
    }

    fn mvp_chat_args(self, run_id: u64, dump_log: &Path) -> Vec<String> {
        let mut args = Vec::new();
        match self {
            Self::ProcessBaseline | Self::Multinode => {
                args.push("--process".to_owned());
            }
            Self::Gpu => {
                args.extend(["--process".to_owned(), "--gpu".to_owned()]);
            }
            Self::MultinodeDocker => {
                args.push("--docker".to_owned());
                args.extend([
                    "--relay-mode".to_owned(),
                    "default".to_owned(),
                    "--endpoint-addr-mask".to_owned(),
                    "relay-only".to_owned(),
                ]);
            }
        }
        if matches!(self, Self::Multinode | Self::MultinodeDocker) {
            args.extend(["--pipeline-stages".to_owned(), "2".to_owned()]);
        }
        if !matches!(self, Self::Gpu) {
            args.push("--cached-model".to_owned());
        }
        args.extend([
            "--run-id".to_owned(),
            run_id.to_string(),
            format!("--dump-logs={}", dump_log.display()),
        ]);
        args
    }

    fn env_overrides(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::ProcessBaseline | Self::Gpu | Self::Multinode | Self::MultinodeDocker => &[],
        }
    }
}

const BASIC_TESTS: &[TestStep] = &[
    TestStep {
        label: "root crate",
        args: &["test"],
    },
    TestStep {
        label: "datastream",
        args: &["test", "-p", "datastream"],
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
        label: "mvp-system",
        args: &["test", "-p", "mvp-system"],
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
  mvp-chat [--gpu] [--process|--docker|--vastai] [--pipeline-stages n] [--cached-model] [-- args...]  Run the human chat wrapper against the real orchestrator/worker bins.
  mvp-chat-check [--gpu|--multinode|--multinode-docker]
                     Run real cargo mvp-chat acceptance check for one explicit scenario.
  test                Run the basic non-binding test barrier: root crate plus each
                      non-binding repository package with `cargo test -p`."
    );
}

const MVP_CHAT_USAGE: &str = "\
USAGE: cargo mvp-chat [OPTIONS]

OPTIONS:
  --gpu                         Run the local GPU path: in-process orchestrator plus DEV=CUDA worker selection
  --process | --docker | --vastai
                                Select the runtime provider
  --config <path>               Load config overlay
  --pipeline-stages <count>     Number of pipeline stages
  --cached-model[=<path>]       Use discovered or explicit cached GGUF model
  --dump-logs[=<path>]          Write datastream frame log
  --run-id <id>                 Override run id
  --skip-rebuild                Reuse existing Cargo artifacts
  --yes, -y                     Approve Vast.ai lease prompts
  --help, -h                    Print this help";

fn print_mvp_chat_usage() {
    println!("{MVP_CHAT_USAGE}");
}

fn is_mvp_chat_help_request(args: &[String]) -> bool {
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
    let check = run_mvp_chat_check(Vec::new());
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

fn run_mvp_chat(args: Vec<String>) -> ExitCode {
    let forwarded = if args.first().is_some_and(|arg| arg == "--") {
        args[1..].to_vec()
    } else {
        args
    };
    if is_mvp_chat_help_request(&forwarded) {
        print_mvp_chat_usage();
        return ExitCode::SUCCESS;
    }
    let mut command = Command::new(cargo_bin());
    command.args(["run", "--package", "mvp-system", "--bin", "mvp-chat", "--"]);
    let dump_log_path = explicit_dump_log_path_from_mvp_chat_args(&forwarded);
    let run_id = run_id_from_mvp_chat_args(&forwarded);
    let benchmark_target = dump_log_path.as_deref().zip(run_id);
    if let Some((path, run_id)) = benchmark_target {
        let event = xtask_mvp_chat_benchmark_event(
            run_id,
            "started",
            json!({
                "program": "cargo",
                "args": ["run", "--package", "mvp-system", "--bin", "mvp-chat", "--"],
            }),
        );
        if let Err(error) =
            append_synthetic_benchmark_frame(path, "xtask-mvp-chat", "mvp.xtask.benchmark", event)
        {
            eprintln!("Failed to write mvp-chat benchmark frame: {error}");
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
        let event = xtask_mvp_chat_benchmark_event(run_id, status, detail);
        if let Err(error) =
            append_synthetic_benchmark_frame(path, "xtask-mvp-chat", "mvp.xtask.benchmark", event)
        {
            eprintln!("Failed to write mvp-chat benchmark frame: {error}");
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
            eprintln!("Failed to execute cargo mvp-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn explicit_dump_log_path_from_mvp_chat_args(args: &[String]) -> Option<PathBuf> {
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

fn run_id_from_mvp_chat_args(args: &[String]) -> Option<u64> {
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

fn xtask_mvp_chat_benchmark_event(run_id: u64, status: &str, detail: Value) -> Value {
    json!({
        "type": "XtaskBenchmark",
        "phase": "cargo_run_mvp_chat",
        "status": status,
        "run_id": run_id,
        "detail": detail,
        "benchmark": xtask_benchmark_stamp(),
    })
}

const XTASK_BENCHMARK_SCHEMA: u64 = 1;
static XTASK_BENCHMARK_START: LazyLock<Instant> = LazyLock::new(Instant::now);
static XTASK_BENCHMARK_SEQ: AtomicU64 = AtomicU64::new(1);

fn xtask_benchmark_stamp() -> Value {
    let mono_ms = u64::try_from(XTASK_BENCHMARK_START.elapsed().as_millis()).unwrap_or(u64::MAX);
    let seq = XTASK_BENCHMARK_SEQ.fetch_add(1, Ordering::Relaxed);
    json!({
        "schema": XTASK_BENCHMARK_SCHEMA,
        "component": "xtask",
        "pid": std::process::id(),
        "seq": seq,
        "wall_unix_ms": unix_ms_now(),
        "mono_ms": mono_ms,
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
                "mvp-chat-check: create temp dir {}: {error}",
                root.display()
            ),
        }
    }
    panic!("mvp-chat-check: could not allocate unique temp dir for prefix {prefix}");
}
fn unix_ms_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn mvp_chat_check_run_id() -> u64 {
    unix_ms_now().max(1)
}

fn write_mvp_chat_check_paths(root: &Path) -> Result<MvpChatCheckPaths, String> {
    if !root.is_dir() {
        return Err(format!(
            "mvp-chat-check: temp root {} is not a directory",
            root.display()
        ));
    }
    let dump_log = root.join("mvp-chat.ndjson");
    if dump_log.exists() {
        return Err(format!(
            "mvp-chat-check: dump log path already exists: {}",
            dump_log.display()
        ));
    }
    Ok(MvpChatCheckPaths {
        root: root.to_path_buf(),
        dump_log,
    })
}

fn run_mvp_chat_check(args: Vec<String>) -> ExitCode {
    let scenario = match MvpChatCheckScenario::parse_args(args) {
        Ok(scenario) => scenario,
        Err(error) => {
            eprintln!("mvp-chat-check: failed: {error}");
            print_usage();
            return ExitCode::from(1);
        }
    };
    let workspace = workspace_root();
    let temp_root = unique_temp_dir("mvp-chat-check");
    let paths = match write_mvp_chat_check_paths(&temp_root) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("{error}");
            eprintln!(
                "mvp-chat-check: temp directory kept at {}",
                temp_root.display()
            );
            return ExitCode::from(1);
        }
    };
    let run_id = mvp_chat_check_run_id();
    println!("mvp-chat-check: scenario {}", scenario.name());

    let output = match run_mvp_chat_check_process(&workspace, &paths, run_id, scenario) {
        Ok(output) => output,
        Err(error) => return fail_mvp_chat_check(&error, &paths, "", "", None),
    };

    if let Some(error) = &output.stdin_error {
        return fail_mvp_chat_check(
            error,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    if output.timed_out {
        let reason = format!("timeout after {MVP_CHAT_CHECK_TIMEOUT_SECS} seconds");
        return fail_mvp_chat_check(
            &reason,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
    }
    if !output.status.success() {
        return fail_mvp_chat_check(
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
            return fail_mvp_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    let events = match assert_dump_log_facts(&paths.dump_log, scenario) {
        Ok(events) => events,
        Err(error) => {
            return fail_mvp_chat_check(
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
            return fail_mvp_chat_check(
                &error,
                &paths,
                &output.stdout,
                &output.stderr,
                Some(&output.status),
            );
        }
    };
    for line in &report.lines {
        println!("{line}");
    }

    if let Err(error) = fs::remove_dir_all(&paths.root) {
        eprintln!(
            "mvp-chat-check: remove temp directory {}: {error}",
            paths.root.display()
        );
        return ExitCode::from(1);
    }

    println!("mvp-chat-check: ok");
    for (index, response) in responses.iter().enumerate() {
        println!("mvp-chat-check: response {}: {}", index + 1, response);
    }
    ExitCode::SUCCESS
}

fn run_mvp_chat_check_process(
    workspace: &Path,
    paths: &MvpChatCheckPaths,
    run_id: u64,
    scenario: MvpChatCheckScenario,
) -> Result<MvpChatCheckOutput, String> {
    let mut command = Command::new(cargo_bin());
    command.current_dir(workspace).arg("mvp-chat").arg("--");
    for arg in scenario.mvp_chat_args(run_id, &paths.dump_log) {
        command.arg(arg);
    }
    for &(key, value) in scenario.env_overrides() {
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
        .map_err(|e| format!("mvp-chat-check: spawn cargo mvp-chat: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "mvp-chat-check: child stdout was not piped".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "mvp-chat-check: child stderr was not piped".to_owned())?;
    let stdout_reader = thread::spawn(move || read_pipe_to_string(stdout, "stdout"));
    let stderr_reader = thread::spawn(move || read_pipe_to_string(stderr, "stderr"));

    let stdin_error = match child.stdin.take() {
        Some(mut stdin) => {
            let result = stdin.write_all(MVP_CHAT_CHECK_PROMPTS);
            drop(stdin);
            result
                .err()
                .map(|error| format!("mvp-chat-check: write child stdin: {error}"))
        }
        None => Some("mvp-chat-check: child stdin was not piped".to_owned()),
    };

    let (status, timed_out) = if stdin_error.is_some() {
        (terminate_mvp_chat_child(&mut child)?, false)
    } else {
        wait_mvp_chat_check_child(&mut child)?
    };
    let child_elapsed_ms = duration_ms_u64(child_started.elapsed());

    let stdout = join_reader(stdout_reader, "stdout")?;
    let stderr = join_reader(stderr_reader, "stderr")?;
    Ok(MvpChatCheckOutput {
        child_elapsed_ms,
        status,
        stdout,
        stderr,
        timed_out,
        stdin_error,
    })
}

fn wait_mvp_chat_check_child(child: &mut Child) -> Result<(ExitStatus, bool), String> {
    let timeout = Duration::from_secs(MVP_CHAT_CHECK_TIMEOUT_SECS);
    let poll = Duration::from_millis(MVP_CHAT_CHECK_POLL_MS);
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok((status, false)),
            Ok(None) if Instant::now() >= deadline => {
                return terminate_mvp_chat_child(child).map(|status| (status, true));
            }
            Ok(None) => thread::sleep(poll),
            Err(error) => return Err(format!("mvp-chat-check: poll child status: {error}")),
        }
    }
}

fn terminate_mvp_chat_child(child: &mut Child) -> Result<ExitStatus, String> {
    #[cfg(target_os = "linux")]
    {
        signal_mvp_chat_process_group(child, libc::SIGTERM);
        let grace_polls = MVP_CHAT_CHECK_TERM_GRACE_MS / MVP_CHAT_CHECK_POLL_MS;
        for _ in 0..grace_polls {
            match child.try_wait() {
                Ok(Some(status)) => return Ok(status),
                Ok(None) => thread::sleep(Duration::from_millis(MVP_CHAT_CHECK_POLL_MS)),
                Err(error) => {
                    return Err(format!("mvp-chat-check: poll child after SIGTERM: {error}"));
                }
            }
        }
        signal_mvp_chat_process_group(child, libc::SIGKILL);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = child.kill();
    }

    child
        .wait()
        .map_err(|e| format!("mvp-chat-check: wait for terminated child: {e}"))
}

#[cfg(target_os = "linux")]
fn signal_mvp_chat_process_group(child: &Child, signal: libc::c_int) {
    let process_group = -(child.id() as libc::pid_t);
    let _ = unsafe { libc::kill(process_group, signal) };
}

fn read_pipe_to_string<R: Read>(mut reader: R, label: &'static str) -> Result<String, String> {
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(|e| format!("mvp-chat-check: read child {label}: {e}"))?;
    Ok(text)
}

fn join_reader(
    handle: thread::JoinHandle<Result<String, String>>,
    label: &str,
) -> Result<String, String> {
    handle
        .join()
        .map_err(|_| format!("mvp-chat-check: child {label} reader panicked"))?
}

fn fail_mvp_chat_check(
    reason: &str,
    paths: &MvpChatCheckPaths,
    stdout: &str,
    stderr: &str,
    status: Option<&ExitStatus>,
) -> ExitCode {
    eprintln!("mvp-chat-check: failed: {reason}");
    if let Some(status) = status {
        eprintln!("mvp-chat-check: child exit status: {status}");
    }
    eprintln!(
        "mvp-chat-check: temp directory kept at {}",
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

fn assert_stdout_contains_two_prompt_cycles(stdout: &str) -> Result<Vec<String>, String> {
    let decoding_count = stdout.matches("decoding...").count();
    if decoding_count < 2 {
        return Err(format!(
            "mvp-chat-check: expected at least two decoding... markers, found {decoding_count}"
        ));
    }
    let response_count = stdout.matches("Response: ").count();
    if response_count < 2 {
        return Err(format!(
            "mvp-chat-check: expected at least two Response: prefixes, found {response_count}"
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
                "mvp-chat-check: empty Response text for prompt cycle {cycle}"
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
        .ok_or_else(|| format!("mvp-chat-check: missing {label} marker for prompt cycle {cycle}"))
}

#[derive(Clone)]
struct DumpLogEvent {
    source: String,
    channel: String,
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
                    "mvp.chat.prompt",
                    Some("ChatProgress"),
                    Some("prompt_submitted"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = dump_log_request_id(&record.event) {
                        facts.prompt_mut(request_id).chat_submitted = Some(point);
                    }
                }
                (
                    "mvp.chat.prompt",
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
                ("mvp.worker.prompt", Some(worker_type), _, _) => {
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
                    "mvp.orch.prompt",
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
                    "mvp.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_encode"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        facts.prompt_mut(request_id).encode_ready = Some(point);
                    }
                }
                (
                    "mvp.orch.prompt",
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
                    "mvp.orch.prompt",
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
                    "mvp.orch.prompt",
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
                    "mvp.orch.prompt",
                    Some("OrchPromptEvent"),
                    Some("pipeline_tokenizer_decode"),
                    Some("ready"),
                ) => {
                    if let Some(request_id) = benchmark_request_id(&record.event) {
                        facts.prompt_mut(request_id).text_decode_ready = Some(point);
                    }
                }
                (
                    "mvp.orch.prompt",
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
        .map_err(|e| format!("mvp-chat-check: read dump log {}: {e}", path.display()))?;
    if content.lines().next().is_none() {
        return Err(format!(
            "mvp-chat-check: dump log {} is empty",
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
                "mvp-chat-check: parse dump log {} line {}: {e}",
                path.display(),
                line_index + 1
            )
        })?;
        let channel = outer
            .get("channel")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "mvp-chat-check: dump log line {} missing channel",
                    line_index + 1
                )
            })?
            .to_owned();
        let payload = outer.get("payload").ok_or_else(|| {
            format!(
                "mvp-chat-check: dump log line {} missing payload",
                line_index + 1
            )
        })?;
        let Some(inner_text) = dump_log_inner_payload_text(payload, line_index)? else {
            continue;
        };
        let event: Value = serde_json::from_str(inner_text).map_err(|e| {
            format!(
                "mvp-chat-check: parse inner event on dump log line {}: {e}",
                line_index + 1
            )
        })?;
        events.push(DumpLogEvent {
            source: outer
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            channel,
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
                "mvp-chat-check: dump log line {} missing utf8 payload value",
                line_index + 1
            )
        })
}

fn assert_dump_log_facts(
    path: &Path,
    scenario: MvpChatCheckScenario,
) -> Result<Vec<DumpLogEvent>, String> {
    let events = parse_dump_log_events(path)?;
    let mut facts = DumpLogFacts::default();
    for record in &events {
        let _source = record.source.as_str();
        record_dump_log_event(&record.channel, &record.event, &mut facts)?;
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
    if scenario == MvpChatCheckScenario::Gpu {
        require_gpu_dump_log_facts(&facts)?;
    }
    if scenario == MvpChatCheckScenario::MultinodeDocker {
        require_multinode_docker_relay_facts(&facts)?;
    }
    Ok(events)
}

fn build_benchmark_report(
    events: &[DumpLogEvent],
    child_elapsed_ms: u64,
    run_id: u64,
    scenario: MvpChatCheckScenario,
) -> Result<BenchmarkReport, String> {
    let facts = BenchmarkFacts::from_events(events, run_id);
    facts.require_span(
        "mvp.xtask.benchmark",
        "XtaskBenchmark",
        "cargo_run_mvp_chat",
        "started",
    )?;
    facts.require_span(
        "mvp.xtask.benchmark",
        "XtaskBenchmark",
        "cargo_run_mvp_chat",
        "ready",
    )?;
    facts.require_span(
        "mvp.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "started",
    )?;
    facts.require_span(
        "mvp.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "ready",
    )?;
    if scenario == MvpChatCheckScenario::Gpu {
        facts.require_span(
            "mvp.chat.runtime",
            "ChatProgress",
            "ensure_orchestrator_actor",
            "ready",
        )?;
    } else {
        facts.require_span(
            "mvp.chat.runtime",
            "ChatProgress",
            "ensure_orch_binary",
            "ready",
        )?;
    }
    facts.require_span(
        "mvp.chat.runtime",
        "ChatProgress",
        "ensure_worker_binary",
        "ready",
    )?;
    facts.require_span(
        "mvp.orch.bootstrap",
        "OrchBootstrap",
        "weights_loaded",
        "ready",
    )?;
    facts.require_span("mvp.chat.runtime", "ChatProgress", "prompt_rpc", "ready")?;

    for request_id in 1..=2 {
        let prompt = facts.prompts.get(&request_id).ok_or_else(|| {
            missing_benchmark_event(format!(
                "mvp.chat.prompt/ChatProgress/request_completed/ready request_id={request_id}"
            ))
        })?;
        require_prompt_point(
            prompt.chat_completed.is_some(),
            "mvp.chat.prompt/ChatProgress/request_completed/ready",
            request_id,
        )?;
        require_prompt_point(
            prompt.encode_ready.is_some(),
            "mvp.worker.prompt/PromptEncodeReady or mvp.orch.prompt/pipeline_tokenizer_encode/ready",
            request_id,
        )?;
        require_prompt_point(
            prompt.decode_started.is_some(),
            "mvp.worker.prompt/DecodeStarted or mvp.orch.prompt/pipeline_token_in/started",
            request_id,
        )?;
        require_prompt_point(
            prompt.first_token_ready.is_some(),
            "mvp.worker.prompt/FirstTokenReady or mvp.orch.prompt/pipeline_token_out/observed",
            request_id,
        )?;
        require_prompt_point(
            prompt.decode_ready.is_some(),
            "mvp.worker.prompt/DecodeReady or mvp.orch.prompt/pipeline_token_out/observed",
            request_id,
        )?;
        require_prompt_point(
            prompt.text_decode_ready.is_some(),
            "mvp.worker.prompt/TextDecodeReady or mvp.orch.prompt/pipeline_tokenizer_decode/ready",
            request_id,
        )?;
    }

    let cargo_run = duration_between(
        facts.span_point(
            "mvp.xtask.benchmark",
            "XtaskBenchmark",
            "cargo_run_mvp_chat",
            "started",
        ),
        facts.span_point(
            "mvp.xtask.benchmark",
            "XtaskBenchmark",
            "cargo_run_mvp_chat",
            "ready",
        ),
    );
    let prepare_runtime = duration_between(
        facts.span_point(
            "mvp.chat.runtime",
            "ChatProgress",
            "prepare_runtime",
            "started",
        ),
        facts.span_point(
            "mvp.chat.runtime",
            "ChatProgress",
            "prepare_runtime",
            "ready",
        ),
    );
    let standup_start = facts.span_point(
        "mvp.chat.runtime",
        "ChatProgress",
        "prepare_runtime",
        "ready",
    );
    let weights_loaded = facts.span_point(
        "mvp.orch.bootstrap",
        "OrchBootstrap",
        "weights_loaded",
        "ready",
    );
    let prompt_rpc = facts.span_point("mvp.chat.runtime", "ChatProgress", "prompt_rpc", "ready");
    let standup_to_weights = duration_between(standup_start, weights_loaded);
    let standup_to_prompt_rpc = duration_between(standup_start, prompt_rpc);

    let mut lines = vec![
        format!("mvp-chat-check: benchmark: run_id={run_id}"),
        format!("mvp-chat-check: benchmark total_child_ms={child_elapsed_ms}"),
        benchmark_span_line("cargo_run_mvp_chat_ms", cargo_run),
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
        "mvp-chat-check: benchmark {name}={}",
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
        "mvp-chat-check: benchmark prompt {} {}",
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
    format!("mvp-chat-check: missing benchmark event {event}")
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
    relay_masked_orchestrator_ready: bool,
    relay_masked_node_spec_worker_count: Option<u64>,
    relay_masked_worker_iroh_ready: BTreeSet<u64>,
    relay_masked_worker_coordinator_join: BTreeSet<u64>,
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
    channel: &str,
    event: &Value,
    facts: &mut DumpLogFacts,
) -> Result<(), String> {
    record_gpu_dump_log_event(channel, event, facts);
    let event_type = event.get("type").and_then(Value::as_str);
    let phase = event.get("phase").and_then(Value::as_str);
    let status = event.get("status").and_then(Value::as_str);
    if status == Some("failed") {
        return Err(format!(
            "mvp-chat-check: failed event channel={channel} type={} phase={} detail={}",
            event_type.unwrap_or("<missing>"),
            phase.unwrap_or("<missing>"),
            event.get("detail").unwrap_or(&Value::Null)
        ));
    }

    match (channel, event_type, phase, status) {
        ("mvp.chat.lifecycle", Some("ChatProgress"), Some("config"), Some("ready")) => {
            facts.chat_config_ready = true;
        }
        ("mvp.chat.runtime", Some("ChatProgress"), Some("prepare_runtime"), Some("ready")) => {
            facts.prepare_runtime_ready = true;
        }
        ("mvp.chat.runtime", Some("ChatProgress"), Some("prompt_rpc"), Some("ready")) => {
            facts.prompt_rpc_ready = true;
        }
        (_, Some("OrchBootstrap"), Some("iroh_driver"), Some("ready")) => {
            facts.orch_iroh_driver_ready = true;
            if detail_relay_only_advertisement(event) {
                facts.relay_masked_orchestrator_ready = true;
            }
        }
        (_, Some("NodeEvent"), Some("iroh_driver"), Some("ready")) => {
            facts.node_iroh_driver_ready = true;
            if detail_relay_only_advertisement(event)
                && let Some(node_id) = event_node_id(event)
            {
                facts.relay_masked_worker_iroh_ready.insert(node_id);
            }
        }
        (_, Some("OrchBootstrap"), Some("node_spec"), Some("ready")) => {
            if detail_str(event, "endpoint_addr_mask") == Some("relay-only")
                && detail_str(event, "relay_mode") == Some("default")
            {
                facts.relay_masked_node_spec_worker_count = detail_u64(event, "worker_count");
            }
        }
        (_, Some("NodeEvent"), Some("coordinator_join"), Some("started")) => {
            if detail_bool(event, "has_relay") == Some(true)
                && detail_u64(event, "direct_addr_count") == Some(0)
                && let Some(node_id) = event_node_id(event)
            {
                facts.relay_masked_worker_coordinator_join.insert(node_id);
            }
        }
        (_, Some("NodeEvent"), Some("worker_initialize"), Some("ready")) => {
            facts.node_worker_initialize_ready = true;
        }
        (_, Some("OrchBootstrap"), Some("weights_loaded"), Some("ready")) => {
            facts.orch_weights_loaded_ready = true;
        }
        ("mvp.chat.prompt", Some("ChatProgress"), Some("response_text"), Some("observed")) => {
            match dump_log_request_id(event) {
                Some(1) => facts.response_text_1 = true,
                Some(2) => facts.response_text_2 = true,
                _ => {}
            }
        }
        ("mvp.chat.prompt", Some("ChatProgress"), Some("request_completed"), Some("ready")) => {
            match dump_log_request_id(event) {
                Some(1) => facts.request_completed_1 = true,
                Some(2) => facts.request_completed_2 = true,
                _ => {}
            }
        }
        ("mvp.chat.lifecycle", Some("ChatProgress"), Some("shutdown"), Some("requested")) => {
            facts.shutdown_requested = true;
        }
        (
            "mvp.chat.component",
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

fn record_gpu_dump_log_event(channel: &str, event: &Value, facts: &mut DumpLogFacts) {
    let event_type = event.get("type").and_then(Value::as_str);
    let phase = event.get("phase").and_then(Value::as_str);
    let status = event.get("status").and_then(Value::as_str);
    if event_type == Some("TinygradCpuCompilerSelected") {
        facts.gpu_cpu_fallback_seen = true;
    }

    match (channel, event_type) {
        ("mvp.worker.initialize", Some("TinygradImportStarted"))
            if event_requested_device_is_cuda(event) =>
        {
            facts.gpu_worker_device_requested = true;
        }
        ("mvp.worker.initialize", Some("TinygradImportReady"))
            if event_requested_device_is_cuda(event) && event_env_dev_is_cuda(event) =>
        {
            facts.gpu_import_ready = true;
        }
        ("mvp.worker.initialize", Some("TinygradDeviceProbeReady"))
            if event_requested_device_is_cuda(event) && event_probe_result_is_one(event) =>
        {
            facts.gpu_probe_ready = true;
        }
        ("mvp.worker.initialize", Some("WorkerReady")) if worker_ready_backend_is_cuda(event) => {
            facts.gpu_worker_ready = true;
        }
        ("mvp.worker.prompt", Some("DecodeStarted"))
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
        ("mvp.worker.prompt", Some("FirstTokenReady"))
            if event.get("token_index").and_then(Value::as_u64) == Some(1) =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_first_token_ready.insert(request_id);
            }
        }
        ("mvp.worker.prompt", Some("DecodeReady"))
            if event_positive_u64(event, "tokens_generated") =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_decode_ready.insert(request_id);
            }
        }
        ("mvp.worker.prompt", Some("PromptCompleted"))
            if event
                .get("generated_tokens")
                .and_then(Value::as_array)
                .is_some_and(|tokens| !tokens.is_empty()) =>
        {
            if let Some(request_id) = event.get("request_id").and_then(Value::as_u64) {
                facts.gpu_prompt_completed.insert(request_id);
            }
        }
        ("mvp.worker.tokenizer", Some("PromptEncoded"))
            if event
                .get("tokens")
                .and_then(Value::as_array)
                .is_some_and(|tokens| !tokens.is_empty()) =>
        {
            if let Some(request_id) = event_request_id(event) {
                facts.gpu_pipeline_prompt_encoded.insert(request_id);
            }
        }
        ("mvp.worker.tokenizer", Some("TokensDecoded"))
            if event
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty()) =>
        {
            if let Some(request_id) = event_request_id(event) {
                facts.gpu_pipeline_tokens_decoded.insert(request_id);
            }
        }
        ("mvp.worker.step", Some("StepExecuted"))
            if event_positive_u64(event, "committed_bytes") =>
        {
            let backend = event.get("execution_backend").and_then(Value::as_str);
            if matches!(backend, Some("pipeline_stage" | "full_transformer")) {
                facts.gpu_pipeline_real_worker_step_seen = true;
            }
        }
        ("mvp.orch.prompt", Some("OrchPromptEvent"))
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
        ("mvp.orch.prompt", Some("OrchPromptEvent"))
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
        ("mvp.orch.prompt", Some("OrchPromptEvent"))
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
        return Err("mvp-chat-check: GPU run fell back to the tinygrad CPU compiler".to_owned());
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
        let pipeline_decode = facts.gpu_pipeline_real_worker_step_seen
            && facts.gpu_pipeline_prompt_encoded.contains(&request_id)
            && facts.gpu_pipeline_prompt_begin.contains(&request_id)
            && facts.gpu_pipeline_token_in.contains(&request_id)
            && facts.gpu_pipeline_token_out.contains(&request_id)
            && facts
                .gpu_pipeline_tokenizer_decode_ready
                .contains(&request_id)
            && facts.gpu_pipeline_tokens_decoded.contains(&request_id);
        require_dump_log_fact(
            direct_decode || pipeline_decode,
            &format!("GPU decode/token evidence request_id={request_id}"),
        )?;
    }
    Ok(())
}

fn require_multinode_docker_relay_facts(facts: &DumpLogFacts) -> Result<(), String> {
    require_dump_log_fact(
        facts.relay_masked_orchestrator_ready,
        "relay-masked orchestrator endpoint",
    )?;
    require_dump_log_fact(
        facts
            .relay_masked_node_spec_worker_count
            .is_some_and(|count| count >= 2),
        "relay-masked Docker node_spec with multiple workers",
    )?;
    require_dump_log_fact(
        facts.relay_masked_worker_iroh_ready.len() >= 2,
        "relay-masked worker iroh_driver ready for multiple nodes",
    )?;
    require_dump_log_fact(
        facts.relay_masked_worker_coordinator_join.len() >= 2,
        "relay-masked worker coordinator_join for multiple nodes",
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

fn detail_bool(event: &Value, key: &str) -> Option<bool> {
    event
        .get("detail")
        .and_then(|detail| detail.get(key))
        .and_then(Value::as_bool)
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

fn detail_relay_only_advertisement(event: &Value) -> bool {
    detail_str(event, "endpoint_addr_mask") == Some("relay-only")
        && detail_bool(event, "has_relay") == Some(true)
        && detail_u64(event, "direct_addr_count") == Some(0)
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
        Err(format!("mvp-chat-check: missing {fact}"))
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
    fn scenario_flags_select_expected_launch_contract() {
        let dump_log = Path::new("/tmp/mvp-chat-check.ndjson");

        let baseline =
            MvpChatCheckScenario::parse_args(Vec::new()).expect("default scenario parses");
        assert_eq!(baseline, MvpChatCheckScenario::ProcessBaseline);
        assert_eq!(
            baseline.mvp_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/mvp-chat-check.ndjson",
            ])
        );

        let gpu = MvpChatCheckScenario::parse_args(strings(&["--gpu"])).expect("gpu parses");
        assert_eq!(gpu, MvpChatCheckScenario::Gpu);
        assert!(gpu.env_overrides().is_empty());
        assert_eq!(
            gpu.mvp_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--gpu",
                "--run-id",
                "42",
                "--dump-logs=/tmp/mvp-chat-check.ndjson",
            ])
        );

        let multinode =
            MvpChatCheckScenario::parse_args(strings(&["--multinode"])).expect("multinode parses");
        assert_eq!(multinode, MvpChatCheckScenario::Multinode);
        assert_eq!(
            multinode.mvp_chat_args(42, dump_log),
            strings(&[
                "--process",
                "--pipeline-stages",
                "2",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/mvp-chat-check.ndjson",
            ])
        );

        let multinode_docker = MvpChatCheckScenario::parse_args(strings(&["--multinode-docker"]))
            .expect("multinode docker parses");
        assert_eq!(multinode_docker, MvpChatCheckScenario::MultinodeDocker);
        assert_eq!(
            multinode_docker.mvp_chat_args(42, dump_log),
            strings(&[
                "--docker",
                "--relay-mode",
                "default",
                "--endpoint-addr-mask",
                "relay-only",
                "--pipeline-stages",
                "2",
                "--cached-model",
                "--run-id",
                "42",
                "--dump-logs=/tmp/mvp-chat-check.ndjson",
            ])
        );
    }

    #[test]
    fn scenario_flags_reject_unknown_or_ambiguous_invocations() {
        assert!(
            MvpChatCheckScenario::parse_args(strings(&["--docker"]))
                .expect_err("unknown flag fails")
                .contains("unsupported mvp-chat-check argument")
        );
        assert!(
            MvpChatCheckScenario::parse_args(strings(&["--gpu", "--multinode"]))
                .expect_err("multiple scenarios fail")
                .contains("at most one scenario flag")
        );
    }

    fn temp_path(label: &str) -> PathBuf {
        let id = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "xtask-benchmark-observability-{label}-{}-{id}.ndjson",
            std::process::id()
        ))
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
            "component": component,
            "pid": 1,
            "seq": wall_unix_ms,
            "wall_unix_ms": wall_unix_ms,
            "mono_ms": mono_ms,
        })
    }

    fn stamped(mut event: Value, component: &str, wall_unix_ms: u64, mono_ms: u64) -> Value {
        let object = event.as_object_mut().expect("event object");
        object.insert(
            "benchmark".to_owned(),
            benchmark(component, wall_unix_ms, mono_ms),
        );
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
            "mvp-chat",
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
                    "tokens_generated": 3,
                    "elapsed_ms": 50,
                    "final_text_bytes": 5,
                    "response_started": true,
                },
            }),
            "mvp-chat",
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
            "mvp-orchestrator",
            wall_unix_ms,
            mono_ms,
        )
    }

    fn benchmark_report_events(include_first_token: bool) -> Vec<(&'static str, Value)> {
        let mut events = benchmark_report_base_events();
        for request_id in 1..=2 {
            let base = 1_200 + request_id * 100;
            events.push((
                "mvp.chat.prompt",
                prompt_chat_span("prompt_submitted", "ready", request_id, base, base - 1_000),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event("PromptStarted", request_id, base + 5, request_id * 100),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "PromptEncodeStarted",
                    request_id,
                    base + 10,
                    request_id * 100 + 10,
                ),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "PromptEncodeReady",
                    request_id,
                    base + 15,
                    request_id * 100 + 15,
                ),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "DecodeStarted",
                    request_id,
                    base + 20,
                    request_id * 100 + 20,
                ),
            ));
            if include_first_token {
                events.push((
                    "mvp.worker.prompt",
                    worker_prompt_event(
                        "FirstTokenReady",
                        request_id,
                        base + 25,
                        request_id * 100 + 25,
                    ),
                ));
            }
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event("DecodeReady", request_id, base + 40, request_id * 100 + 40),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "TextDecodeStarted",
                    request_id,
                    base + 41,
                    request_id * 100 + 41,
                ),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "TextDecodeReady",
                    request_id,
                    base + 45,
                    request_id * 100 + 45,
                ),
            ));
            events.push((
                "mvp.worker.prompt",
                worker_prompt_event(
                    "PromptCompleted",
                    request_id,
                    base + 50,
                    request_id * 100 + 50,
                ),
            ));
            events.push((
                "mvp.chat.prompt",
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
                "mvp.chat.prompt",
                prompt_chat_span("prompt_submitted", "ready", request_id, base, base - 1_000),
            ));
            events.push((
                "mvp.orch.prompt",
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
                "mvp.orch.prompt",
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
                "mvp.orch.prompt",
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
                    "mvp.orch.prompt",
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
                "mvp.orch.prompt",
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
                "mvp.orch.prompt",
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
                "mvp.chat.prompt",
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
                "mvp.xtask.benchmark",
                stamped(
                    json!({
                        "type": "XtaskBenchmark",
                        "phase": "cargo_run_mvp_chat",
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
                "mvp.xtask.benchmark",
                stamped(
                    json!({
                        "type": "XtaskBenchmark",
                        "phase": "cargo_run_mvp_chat",
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
                "mvp.chat.runtime",
                chat_span("prepare_runtime", "started", 1_030, 30),
            ),
            (
                "mvp.chat.runtime",
                chat_span("ensure_orch_binary", "ready", 1_035, 35),
            ),
            (
                "mvp.chat.runtime",
                chat_span("ensure_worker_binary", "ready", 1_036, 36),
            ),
            (
                "mvp.chat.runtime",
                chat_span("prepare_runtime", "ready", 1_040, 40),
            ),
            (
                "mvp.orch.bootstrap",
                stamped(
                    json!({
                        "type": "OrchBootstrap",
                        "phase": "weights_loaded",
                        "status": "ready",

                        "run_id": 9,
                        "node_id": 1,
                        "detail": {},
                    }),
                    "mvp-orchestrator",
                    1_100,
                    100,
                ),
            ),
            (
                "mvp.chat.runtime",
                chat_span("prompt_rpc", "ready", 1_120, 120),
            ),
        ]
    }

    fn dump_log_fact_events(
        include_gpu: bool,
        include_cpu_fallback: bool,
    ) -> Vec<(&'static str, Value)> {
        let mut events = vec![
            ("mvp.chat.lifecycle", chat_span("config", "ready", 1_000, 0)),
            (
                "mvp.chat.runtime",
                chat_span("prepare_runtime", "ready", 1_010, 10),
            ),
            (
                "mvp.chat.runtime",
                chat_span("prompt_rpc", "ready", 1_020, 20),
            ),
            (
                "mvp.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"iroh_driver","status":"ready","run_id":9,"node_id":1,"detail":{}}),
                    "mvp-orchestrator",
                    1_030,
                    30,
                ),
            ),
            (
                "mvp.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{}}),
                    "mvp-worker-node",
                    1_040,
                    40,
                ),
            ),
            (
                "mvp.node.worker",
                stamped(
                    json!({"type":"NodeEvent","phase":"worker_initialize","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"device":"CUDA"}}),
                    "mvp-worker-node",
                    1_050,
                    50,
                ),
            ),
            (
                "mvp.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"weights_loaded","status":"ready","run_id":9,"node_id":1,"detail":{}}),
                    "mvp-orchestrator",
                    1_060,
                    60,
                ),
            ),
            (
                "mvp.chat.prompt",
                prompt_chat_span("response_text", "observed", 1, 1_200, 200),
            ),
            (
                "mvp.chat.prompt",
                prompt_chat_span("request_completed", "ready", 1, 1_210, 210),
            ),
            (
                "mvp.chat.prompt",
                prompt_chat_span("response_text", "observed", 2, 1_300, 300),
            ),
            (
                "mvp.chat.prompt",
                prompt_chat_span("request_completed", "ready", 2, 1_310, 310),
            ),
            (
                "mvp.chat.lifecycle",
                chat_span("shutdown", "requested", 1_400, 400),
            ),
            (
                "mvp.chat.component",
                chat_span("orchestrator_process", "stopped", 1_410, 410),
            ),
        ];

        if include_gpu {
            events.extend([
                (
                    "mvp.worker.initialize",
                    stamped(
                        json!({"type":"TinygradImportStarted","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","env_DEV":"CUDA"}),
                        "tinygrad-worker",
                        1_070,
                        70,
                    ),
                ),
                (
                    "mvp.worker.initialize",
                    stamped(
                        json!({"type":"TinygradImportReady","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","env_DEV":"CUDA"}),
                        "tinygrad-worker",
                        1_080,
                        80,
                    ),
                ),
                (
                    "mvp.worker.initialize",
                    stamped(
                        json!({"type":"TinygradDeviceProbeReady","run_id":9,"node_id":3,"stage_index":1,"requested_device":"CUDA","probe_result":[1]}),
                        "tinygrad-worker",
                        1_090,
                        90,
                    ),
                ),
                (
                    "mvp.worker.initialize",
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
                    "mvp.worker.initialize",
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
                        "mvp.worker.prompt",
                        worker_prompt_event("DecodeStarted", request_id, base + 20, base - 980),
                    ),
                    (
                        "mvp.worker.prompt",
                        worker_prompt_event("FirstTokenReady", request_id, base + 25, base - 975),
                    ),
                    (
                        "mvp.worker.prompt",
                        worker_prompt_event("DecodeReady", request_id, base + 40, base - 960),
                    ),
                    (
                        "mvp.worker.prompt",
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
            explicit_dump_log_path_from_mvp_chat_args(&strings(&["--dump-logs=/tmp/a.ndjson"])),
            Some(PathBuf::from("/tmp/a.ndjson"))
        );
        assert_eq!(
            explicit_dump_log_path_from_mvp_chat_args(&strings(&[
                "--",
                "--run-id",
                "7",
                "--dump-logs",
                "-logs.ndjson",
            ])),
            Some(PathBuf::from("-logs.ndjson"))
        );
        assert_eq!(
            explicit_dump_log_path_from_mvp_chat_args(&strings(&["--dump-logs"])),
            None
        );
        assert_eq!(
            explicit_dump_log_path_from_mvp_chat_args(&strings(&["--dump-logs", "--run-id"])),
            None
        );
        assert_eq!(
            run_id_from_mvp_chat_args(&strings(&["--", "--run-id", "42"])),
            Some(42)
        );
        assert_eq!(
            run_id_from_mvp_chat_args(&strings(&["--run-id", "0"])),
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

        let events = assert_dump_log_facts(&path, MvpChatCheckScenario::Gpu)
            .expect("GPU dump log facts pass");
        let _ = fs::remove_file(path);

        assert!(events.iter().any(|event| {
            event.channel == "mvp.worker.initialize"
                && event.event.get("type").and_then(Value::as_str) == Some("WorkerReady")
        }));
    }

    fn gpu_pipeline_only_facts(
        real_worker_backend: bool,
        prompt_begin_markers: bool,
    ) -> DumpLogFacts {
        let mut facts = DumpLogFacts {
            gpu_worker_device_requested: true,
            gpu_import_ready: true,
            gpu_probe_ready: true,
            gpu_worker_ready: true,
            ..DumpLogFacts::default()
        };
        if real_worker_backend {
            facts.gpu_pipeline_real_worker_step_seen = true;
        }
        for request_id in 1..=2 {
            facts.gpu_pipeline_prompt_encoded.insert(request_id);
            facts.gpu_pipeline_token_in.insert(request_id);
            facts.gpu_pipeline_token_out.insert(request_id);
            facts.gpu_pipeline_tokenizer_decode_ready.insert(request_id);
            facts.gpu_pipeline_tokens_decoded.insert(request_id);
            if prompt_begin_markers {
                facts.gpu_pipeline_prompt_begin.insert(request_id);
            }
        }
        facts
    }

    #[test]
    fn benchmark_observability_gpu_pipeline_facts_require_real_steps_and_prompt_begin_markers() {
        let valid = gpu_pipeline_only_facts(true, true);
        require_gpu_dump_log_facts(&valid).expect("real pipeline facts pass");

        let missing_real_backend = gpu_pipeline_only_facts(false, true);
        let error = require_gpu_dump_log_facts(&missing_real_backend)
            .expect_err("missing real worker backend should fail");
        assert!(
            error.contains("GPU decode/token evidence request_id=1"),
            "unexpected error: {error}"
        );

        let missing_prompt_begin = gpu_pipeline_only_facts(true, false);
        let error = require_gpu_dump_log_facts(&missing_prompt_begin)
            .expect_err("missing prompt begin marker should fail");
        assert!(
            error.contains("GPU decode/token evidence request_id=1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn benchmark_observability_multinode_docker_dump_facts_require_relay_masked_events() {
        let mut events = dump_log_fact_events(false, false);
        events.extend([
            (
                "mvp.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"iroh_driver","status":"ready","run_id":9,"node_id":1,"detail":{"endpoint_addr_mask":"relay-only","relay_mode":"Default","has_relay":true,"direct_addr_count":0}}),
                    "mvp-orchestrator",
                    1_070,
                    70,
                ),
            ),
            (
                "mvp.orch.bootstrap",
                stamped(
                    json!({"type":"OrchBootstrap","phase":"node_spec","status":"ready","run_id":9,"node_id":1,"detail":{"endpoint_addr_mask":"relay-only","relay_mode":"default","worker_count":2}}),
                    "mvp-orchestrator",
                    1_071,
                    71,
                ),
            ),
            (
                "mvp.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":2,"stage_index":0,"detail":{"endpoint_addr_mask":"relay-only","has_relay":true,"direct_addr_count":0}}),
                    "mvp-worker-node",
                    1_072,
                    72,
                ),
            ),
            (
                "mvp.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"iroh_driver","status":"ready","run_id":9,"node_id":3,"stage_index":1,"detail":{"endpoint_addr_mask":"relay-only","has_relay":true,"direct_addr_count":0}}),
                    "mvp-worker-node",
                    1_073,
                    73,
                ),
            ),
            (
                "mvp.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"coordinator_join","status":"started","run_id":9,"node_id":2,"stage_index":0,"detail":{"has_relay":true,"direct_addr_count":0}}),
                    "mvp-worker-node",
                    1_074,
                    74,
                ),
            ),
            (
                "mvp.node.bootstrap",
                stamped(
                    json!({"type":"NodeEvent","phase":"coordinator_join","status":"started","run_id":9,"node_id":3,"stage_index":1,"detail":{"has_relay":true,"direct_addr_count":0}}),
                    "mvp-worker-node",
                    1_075,
                    75,
                ),
            ),
        ]);
        let path = write_synthetic_event_dump("multinode-docker-relay-mask", events);
        assert_dump_log_facts(&path, MvpChatCheckScenario::MultinodeDocker)
            .expect("relay-masked multinode Docker facts pass");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn benchmark_observability_multinode_docker_requires_relay_masked_workers() {
        let mut valid = DumpLogFacts {
            relay_masked_orchestrator_ready: true,
            relay_masked_node_spec_worker_count: Some(2),
            ..DumpLogFacts::default()
        };
        valid.relay_masked_worker_iroh_ready.extend([2, 3]);
        valid.relay_masked_worker_coordinator_join.extend([2, 3]);
        require_multinode_docker_relay_facts(&valid).expect("relay-masked Docker facts pass");

        let mut missing_worker = valid;
        missing_worker.relay_masked_worker_iroh_ready.remove(&3);
        let error = require_multinode_docker_relay_facts(&missing_worker)
            .expect_err("single relay-masked worker should fail");
        assert!(
            error.contains("relay-masked worker iroh_driver ready for multiple nodes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn benchmark_observability_gpu_dump_facts_reject_cpu_fallback() {
        let path = write_synthetic_event_dump("gpu-cpu-fallback", dump_log_fact_events(true, true));

        let error = match assert_dump_log_facts(&path, MvpChatCheckScenario::Gpu) {
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
    fn benchmark_observability_report_requires_granular_decode_events() {
        let events = parse_synthetic_events("missing-first-token", benchmark_report_events(false));

        let error = build_benchmark_report(&events, 80, 9, MvpChatCheckScenario::ProcessBaseline)
            .expect_err("missing first token should fail");

        assert!(
            error.starts_with("mvp-chat-check: missing benchmark event "),
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

        let report = build_benchmark_report(&events, 80, 9, MvpChatCheckScenario::ProcessBaseline)
            .expect("pipeline report builds");

        assert!(
            report
                .lines
                .iter()
                .any(|line| line == "mvp-chat-check: benchmark: run_id=9")
        );
        assert!(report.lines.iter().any(|line| {
            line.starts_with("mvp-chat-check: benchmark prompt 1 ")
                && line.contains("first_token_ms=8")
                && line.contains("decode_ms=8")
        }));
        assert!(report.lines.iter().any(|line| {
            line.starts_with("mvp-chat-check: benchmark prompt 2 ")
                && line.contains("first_token_ms=8")
                && line.contains("decode_ms=8")
        }));
    }
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("test") if args.next().is_none() => run_tests(),
        Some("mvp-chat-check") => run_mvp_chat_check(args.collect()),
        Some("mvp-chat") => run_mvp_chat(args.collect()),
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
