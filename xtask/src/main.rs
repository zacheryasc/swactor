use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

use serde_json::Value;

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
    stdout: String,
    stderr: String,
    timed_out: bool,
    stdin_error: Option<String>,
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
  mvp-chat [--process|--docker|--vastai] [--pipeline-stages n] [--cached-model] [-- args...]  Run the human chat wrapper against the real orchestrator/worker bins.
  mvp-chat-check      Run real cargo mvp-chat acceptance check.
  test                Run the basic non-binding test barrier: root crate plus each
                      non-binding repository package with `cargo test -p`."
    );
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
    let check = run_mvp_chat_check();
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
    let mut command = Command::new(cargo_bin());
    command.args(["run", "--package", "mvp-system", "--bin", "mvp-chat", "--"]);
    let forwarded = if args.first().is_some_and(|arg| arg == "--") {
        args[1..].to_vec()
    } else {
        args
    };
    command.args(forwarded);

    match command.status() {
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
        let root =
            std::env::temp_dir().join(format!("{prefix}-{pid}-{timestamp_nanos}-{attempt}"));
        match fs::create_dir(&root) {
            Ok(()) => return root,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("mvp-chat-check: create temp dir {}: {error}", root.display()),
        }
    }
    panic!("mvp-chat-check: could not allocate unique temp dir for prefix {prefix}");
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

fn run_mvp_chat_check() -> ExitCode {
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

    let output = match run_mvp_chat_check_process(&workspace, &paths) {
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
    if let Err(error) = assert_dump_log_facts(&paths.dump_log) {
        return fail_mvp_chat_check(
            &error,
            &paths,
            &output.stdout,
            &output.stderr,
            Some(&output.status),
        );
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
) -> Result<MvpChatCheckOutput, String> {
    let mut command = Command::new(cargo_bin());
    command
        .current_dir(workspace)
        .args(["mvp-chat", "--", "--cached-model"])
        .arg(format!("--dump-logs={}", paths.dump_log.display()))
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

    let stdout = join_reader(stdout_reader, "stdout")?;
    let stderr = join_reader(stderr_reader, "stderr")?;
    Ok(MvpChatCheckOutput {
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
                    return Err(format!(
                        "mvp-chat-check: poll child after SIGTERM: {error}"
                    ));
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
        .ok_or_else(|| {
            format!("mvp-chat-check: missing {label} marker for prompt cycle {cycle}")
        })
}

#[derive(Default)]
struct DumpLogFacts {
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

fn assert_dump_log_facts(path: &Path) -> Result<(), String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("mvp-chat-check: read dump log {}: {e}", path.display()))?;
    if content.lines().next().is_none() {
        return Err(format!(
            "mvp-chat-check: dump log {} is empty",
            path.display()
        ));
    }

    let mut facts = DumpLogFacts::default();
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
            })?;
        let payload = outer.get("payload").ok_or_else(|| {
            format!(
                "mvp-chat-check: dump log line {} missing payload",
                line_index + 1
            )
        })?;
        if payload.get("encoding").and_then(Value::as_str) != Some("utf8") {
            continue;
        }
        let inner_text = payload.get("value").and_then(Value::as_str).ok_or_else(|| {
            format!(
                "mvp-chat-check: dump log line {} missing utf8 payload value",
                line_index + 1
            )
        })?;
        let event: Value = serde_json::from_str(inner_text).map_err(|e| {
            format!(
                "mvp-chat-check: parse inner event on dump log line {}: {e}",
                line_index + 1
            )
        })?;
        record_dump_log_event(channel, &event, &mut facts)?;
    }

    require_dump_log_fact(facts.chat_config_ready, "config ready")?;
    require_dump_log_fact(facts.prepare_runtime_ready, "prepare_runtime ready")?;
    require_dump_log_fact(facts.prompt_rpc_ready, "prompt_rpc ready")?;
    require_dump_log_fact(facts.orch_iroh_driver_ready, "OrchBootstrap iroh_driver ready")?;
    require_dump_log_fact(facts.node_iroh_driver_ready, "NodeEvent iroh_driver ready")?;
    require_dump_log_fact(
        facts.node_worker_initialize_ready,
        "NodeEvent worker_initialize ready",
    )?;
    require_dump_log_fact(facts.orch_weights_loaded_ready, "OrchBootstrap weights_loaded ready")?;
    require_dump_log_fact(facts.response_text_1, "response_text request_id=1")?;
    require_dump_log_fact(facts.request_completed_1, "request_completed request_id=1")?;
    require_dump_log_fact(facts.response_text_2, "response_text request_id=2")?;
    require_dump_log_fact(facts.request_completed_2, "request_completed request_id=2")?;
    require_dump_log_fact(facts.shutdown_requested, "shutdown requested")?;
    require_dump_log_fact(facts.orchestrator_stopped, "orchestrator_process stopped")
}

fn record_dump_log_event(
    channel: &str,
    event: &Value,
    facts: &mut DumpLogFacts,
) -> Result<(), String> {
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
        }
        (_, Some("NodeEvent"), Some("iroh_driver"), Some("ready")) => {
            facts.node_iroh_driver_ready = true;
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

fn dump_log_request_id(event: &Value) -> Option<u64> {
    event
        .get("detail")
        .and_then(|detail| detail.get("request_id"))
        .and_then(Value::as_u64)
}

fn require_dump_log_fact(found: bool, fact: &str) -> Result<(), String> {
    if found {
        Ok(())
    } else {
        Err(format!("mvp-chat-check: missing {fact}"))
    }
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("test") if args.next().is_none() => run_tests(),
        Some("mvp-chat-check") if args.next().is_none() => run_mvp_chat_check(),
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
