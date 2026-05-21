use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::Instant;

use clap::{Parser, Subcommand};

// ── CLI ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "xtask", about = "Development task runner")]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,

    /// (Legacy) Test group to run directly without subcommand
    #[arg(hide = true)]
    group: Option<String>,

    /// Show all groups and the cargo commands they run
    #[arg(long)]
    list: bool,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a local swactor node with datastore + distribution on localhost
    Node {
        /// Dashboard HTTP port
        #[arg(long, default_value = "9090")]
        dashboard_port: u16,

        /// Pass extra args to the swactor binary
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Run the simulator parity-bar check suite.
    ///
    /// Drives the full TESTING_SPEC §2–§12 check list:
    ///   * lock check
    ///   * banned-API + parity-bar hygiene lints
    ///   * both feature builds (facade-prod / facade-sim, plus the
    ///     negative both-features / neither-feature builds)
    ///   * runtime-facade surface fingerprint
    ///   * detector binary in both configurations
    ///   * every parity-bar test
    ///
    /// With `--phase 1` accepts engine-side `NotImplemented` failures
    /// (and the other items in `expected_failures.txt`) as expected
    /// pre-engine outcomes.
    ParityBar {
        /// Restrict acceptance to a phase (1 = pre-engine, 2 = full).
        #[arg(long)]
        phase: Option<u8>,

        /// Filter to a single test binary (e.g. `t_determinism`).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        filter: Vec<String>,
    },
}

// ── Test infrastructure ─────────────────────────────────────────────

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
}

struct Group {
    name: &'static str,
    description: &'static str,
    steps: &'static [TestStep],
}

const CORE: Group = Group {
    name: "core",
    description: "Actor runtime, message delivery, property tests",
    steps: &[TestStep {
        label: "actor runtime",
        args: &["test", "-p", "swactor", "--features", "transport"],
    }],
};

const SIMULATION: Group = Group {
    name: "simulation",
    description: "Deterministic cluster simulations",
    steps: &[TestStep {
        label: "cluster simulations",
        args: &["test", "-p", "simulation"],
    }],
};

const FORMAL_VERIFICATION: Group = Group {
    name: "formal-verification",
    description: "Kani proofs + Stateright model checking",
    steps: &[
        TestStep {
            label: "kani proofs",
            args: &["kani", "-p", "swactor-datastore"],
        },
        TestStep {
            label: "gateway dispatch model check",
            args: &[
                "test",
                "-p",
                "swactor-datastore",
                "--test",
                "gateway_model_check",
            ],
        },
    ],
};

const GROUPS: &[&Group] = &[&CORE, &SIMULATION, &FORMAL_VERIFICATION];

fn groups_for(name: &str) -> Option<Vec<&'static Group>> {
    match name {
        "core" => Some(vec![&CORE]),
        "simulation" => Some(vec![&SIMULATION]),
        "formal-verification" => Some(vec![&FORMAL_VERIFICATION]),
        "essential" => Some(vec![&CORE, &SIMULATION]),
        "all" => Some(vec![&CORE, &SIMULATION, &FORMAL_VERIFICATION]),
        _ => None,
    }
}

fn run_step(group_name: &str, step: &TestStep) -> bool {
    println!("\n=== {group_name}: {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    let status = Command::new(cargo_bin()).args(step.args).status();

    match status {
        Ok(s) => s.success(),
        Err(e) => {
            eprintln!("Failed to execute cargo: {e}");
            false
        }
    }
}

fn print_usage() {
    println!(
        "\
USAGE: cargo xtask <COMMAND|GROUP>

COMMANDS:
  node                 Run a local swactor node (datastore + distribution, localhost)
  parity-bar           Run the simulator parity-bar check suite

TEST GROUPS:
  core                 Actor runtime, message delivery, property tests
  simulation           Deterministic cluster simulations
  formal-verification  Kani proofs + Stateright model checking
  essential            core + simulation
  all                  Every test group

FLAGS:
  --list               Show all groups and the cargo commands they run"
    );
}

fn print_list() {
    println!("Available test groups:\n");

    for group in GROUPS {
        println!("  {:<22}{}", group.name, group.description);
        for step in group.steps {
            println!("                        → cargo {}", step.args.join(" "));
        }
        println!();
    }

    println!("  {:<22}core + simulation", "essential");
    println!("  {:<22}Every test group", "all");
}

// ── Node runner ─────────────────────────────────────────────────────

fn run_node(dashboard_port: u16, extra_args: &[String]) {
    eprintln!("Building swactor node...");
    let build = Command::new(cargo_bin())
        .args(["build", "-p", "node"])
        .status();

    match build {
        Ok(s) if s.success() => {}
        Ok(s) => {
            eprintln!("Build failed");
            std::process::exit(s.code().unwrap_or(1));
        }
        Err(e) => {
            eprintln!("Failed to execute cargo build: {e}");
            std::process::exit(1);
        }
    }

    let tmp = std::env::temp_dir().join("swactor-dev-node");
    std::fs::create_dir_all(&tmp).expect("failed to create temp dir");

    let identity_dir = tmp.join("identity");
    let auth_dir = tmp.join("auth");

    eprintln!();

    let mut cmd = Command::new("target/debug/swactor");
    cmd.args([
        "--transport",
        "iroh",
        "--dashboard-port",
        &dashboard_port.to_string(),
        "--identity-dir",
        &identity_dir.to_string_lossy(),
        "--auth-dir",
        &auth_dir.to_string_lossy(),
        "--no-relay",
    ]);
    cmd.env("HOME", tmp.to_string_lossy().as_ref());
    cmd.args(extra_args);

    let status = cmd.status();
    match status {
        Ok(s) => std::process::exit(s.code().unwrap_or(0)),
        Err(e) => {
            eprintln!("Failed to execute swactor: {e}");
            std::process::exit(1);
        }
    }
}

// ── parity-bar ──────────────────────────────────────────────────────

const PARITY_TEST_BINARIES: &[&str] = &[
    "t_determinism",
    "t_replay",
    "t_facade",
    "t_same_binary",
    "t_schema_coverage",
    "t_round_trip",
    "t_causality",
    "t_equivariance",
    "t_lifecycle",
    "t_detector",
];

#[derive(Debug)]
struct CheckResult {
    label: String,
    passed: bool,
    detail: String,
}

impl CheckResult {
    fn pass(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            passed: true,
            detail: detail.into(),
        }
    }
    fn fail(label: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            passed: false,
            detail: detail.into(),
        }
    }
}

fn run_parity_bar(phase: Option<u8>, filter: &[String]) -> ExitCode {
    let start = Instant::now();
    let workspace = workspace_root();

    println!(
        "parity-bar: phase={} filter={}",
        phase.map(|p| p.to_string()).unwrap_or_else(|| "any".into()),
        if filter.is_empty() {
            "(all)".into()
        } else {
            filter.join(" ")
        }
    );

    let mut results: Vec<CheckResult> = Vec::new();

    // The infrastructure checks all need to pass in any phase.
    results.push(check_lock(&workspace));
    results.push(check_banned_api_lint(&workspace));
    results.push(check_parity_bar_lint(&workspace));
    results.push(check_feature_build(&workspace, "facade-prod"));
    results.push(check_feature_build(&workspace, "facade-sim"));
    results.push(check_negative_build(
        &workspace,
        &["build", "--workspace", "--features", "facade-prod,facade-sim"],
        "both-features rejected",
    ));
    results.push(check_negative_build(
        &workspace,
        &["build", "--workspace", "--no-default-features"],
        "neither-feature rejected",
    ));
    results.push(check_surface_fingerprint(&workspace));
    results.push(check_detector_build(&workspace, "facade-prod"));
    results.push(check_detector_build(&workspace, "facade-sim"));

    // Parity-bar tests last (most expensive). Skip if a prerequisite
    // already failed — running the tests with a broken lint or lock is
    // not informative.
    if results.iter().all(|r| r.passed) {
        results.push(check_parity_tests(&workspace, phase, filter));
    } else {
        results.push(CheckResult::fail(
            "parity-bar tests",
            "skipped (prerequisite check failed)".to_string(),
        ));
    }

    let elapsed = start.elapsed();
    let mut failed = 0;
    println!("\n── parity-bar summary ─────────────────────────────────");
    for r in &results {
        let tag = if r.passed { "ok  " } else { "FAIL" };
        println!("{tag}  {:<32}  {}", r.label, r.detail);
        if !r.passed {
            failed += 1;
        }
    }
    println!(
        "── {} check(s), {failed} failed, {:.1}s ────────────",
        results.len(),
        elapsed.as_secs_f64()
    );

    if failed > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

// ── parity-bar checks ───────────────────────────────────────────────

fn check_lock(workspace: &Path) -> CheckResult {
    let script = workspace.join("scripts/check-parity-lock.sh");
    let output = Command::new("bash")
        .arg(&script)
        .current_dir(workspace)
        .output();
    match output {
        Ok(out) if out.status.success() => {
            CheckResult::pass("parity-lock check", "hash matches")
        }
        Ok(out) => CheckResult::fail(
            "parity-lock check",
            String::from_utf8_lossy(&out.stderr).trim().to_string(),
        ),
        Err(e) => CheckResult::fail("parity-lock check", e.to_string()),
    }
}

fn check_banned_api_lint(workspace: &Path) -> CheckResult {
    let out = Command::new(cargo_bin())
        .args([
            "run",
            "-p",
            "simulation",
            "--bin",
            "lint-deterministic",
            "--quiet",
            "--",
            "--workspace-root",
            &workspace.display().to_string(),
            "--quiet",
        ])
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => {
            CheckResult::pass("banned-API lint", "no violations")
        }
        Ok(o) => CheckResult::fail(
            "banned-API lint",
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .chain(String::from_utf8_lossy(&o.stderr).lines())
                .take(8)
                .collect::<Vec<_>>()
                .join("; "),
        ),
        Err(e) => CheckResult::fail("banned-API lint", e.to_string()),
    }
}

fn check_parity_bar_lint(workspace: &Path) -> CheckResult {
    let out = Command::new(cargo_bin())
        .args([
            "run",
            "-p",
            "simulation",
            "--bin",
            "lint-deterministic",
            "--quiet",
            "--",
            "--workspace-root",
            &workspace.display().to_string(),
            "--check-parity-bar",
            "--quiet",
        ])
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => {
            CheckResult::pass("parity-bar hygiene", "no violations")
        }
        Ok(o) => CheckResult::fail(
            "parity-bar hygiene",
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .chain(String::from_utf8_lossy(&o.stderr).lines())
                .take(8)
                .collect::<Vec<_>>()
                .join("; "),
        ),
        Err(e) => CheckResult::fail("parity-bar hygiene", e.to_string()),
    }
}

fn check_feature_build(workspace: &Path, feature: &str) -> CheckResult {
    let out = Command::new(cargo_bin())
        .args([
            "build",
            "--workspace",
            "--features",
            feature,
            "--quiet",
        ])
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => CheckResult::pass(
            format!("build --features {feature}"),
            "compiled cleanly",
        ),
        Ok(o) => CheckResult::fail(
            format!("build --features {feature}"),
            String::from_utf8_lossy(&o.stderr)
                .lines()
                .filter(|l| l.contains("error"))
                .take(6)
                .collect::<Vec<_>>()
                .join("; "),
        ),
        Err(e) => CheckResult::fail(format!("build --features {feature}"), e.to_string()),
    }
}

fn check_negative_build(workspace: &Path, args: &[&str], label: &str) -> CheckResult {
    // The build is expected to fail. Per the Stage 1 gate (and
    // TESTING_SPEC §4.2) the failure message must not contain the
    // literal substring "compile_error" — we use `const _: () = panic!`
    // for the diagnostic. Pass iff the failure is via the const-panic
    // mechanism (or any non-compile_error path), fail iff the build
    // succeeded or the diagnostic mentioned "compile_error".
    let out = Command::new(cargo_bin())
        .args(args)
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => {
            CheckResult::fail(label, "build unexpectedly succeeded")
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            if stderr.contains("compile_error") {
                CheckResult::fail(
                    label,
                    "diagnostic mentioned `compile_error` — use const-panic",
                )
            } else {
                CheckResult::pass(label, "rejected by const-panic")
            }
        }
        Err(e) => CheckResult::fail(label, e.to_string()),
    }
}

fn check_surface_fingerprint(workspace: &Path) -> CheckResult {
    let out = Command::new(cargo_bin())
        .args(["test", "-p", "simulation", "--test", "surface_locked", "--quiet"])
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => {
            CheckResult::pass("facade surface lock", "fingerprint matches")
        }
        Ok(o) => CheckResult::fail(
            "facade surface lock",
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .chain(String::from_utf8_lossy(&o.stderr).lines())
                .filter(|l| l.contains("FAIL") || l.contains("error"))
                .take(6)
                .collect::<Vec<_>>()
                .join("; "),
        ),
        Err(e) => CheckResult::fail("facade surface lock", e.to_string()),
    }
}

fn check_detector_build(workspace: &Path, feature: &str) -> CheckResult {
    let out = Command::new(cargo_bin())
        .args([
            "build",
            "-p",
            "simulation",
            "--bin",
            "sim-detector",
            "--features",
            feature,
            "--quiet",
        ])
        .current_dir(workspace)
        .output();
    match out {
        Ok(o) if o.status.success() => CheckResult::pass(
            format!("sim-detector {feature}"),
            "binary compiles",
        ),
        Ok(o) => CheckResult::fail(
            format!("sim-detector {feature}"),
            String::from_utf8_lossy(&o.stderr)
                .lines()
                .filter(|l| l.contains("error"))
                .take(6)
                .collect::<Vec<_>>()
                .join("; "),
        ),
        Err(e) => CheckResult::fail(format!("sim-detector {feature}"), e.to_string()),
    }
}

fn check_parity_tests(workspace: &Path, phase: Option<u8>, filter: &[String]) -> CheckResult {
    let expected = load_expected_failures(workspace);
    let mut runs: Vec<TestRun> = Vec::new();

    for binary in PARITY_TEST_BINARIES {
        if !filter.is_empty()
            && !filter
                .iter()
                .any(|f| binary == &f.as_str() || binary.contains(f.as_str()))
        {
            continue;
        }
        runs.push(run_one_test_binary(workspace, binary));
    }

    let mut all_failures: Vec<String> = Vec::new();
    let mut accepted_failures: Vec<String> = Vec::new();
    let mut passes: usize = 0;

    for run in &runs {
        passes += run.passed.len();
        for test in &run.failed {
            let qualified = format!("{}::{}", run.binary, test);
            if expected.contains(&qualified) {
                accepted_failures.push(qualified);
            } else {
                all_failures.push(qualified);
            }
        }
    }

    let label = "parity-bar tests";
    let detail = format!(
        "{passes} pass, {} expected-fail, {} unexpected-fail",
        accepted_failures.len(),
        all_failures.len()
    );

    let phase_one_ok = phase == Some(1) && all_failures.is_empty();
    let phase_two_ok = phase != Some(1) && all_failures.is_empty() && accepted_failures.is_empty();

    if phase_one_ok || phase_two_ok {
        CheckResult::pass(label, detail)
    } else {
        let head: Vec<String> = all_failures.iter().take(10).cloned().collect();
        CheckResult::fail(
            label,
            format!(
                "{detail}; unexpected: {}",
                if head.is_empty() {
                    "(none — but accepted-fail count > 0 in phase ≥ 2)".to_string()
                } else {
                    head.join(", ")
                }
            ),
        )
    }
}

#[derive(Debug)]
struct TestRun {
    binary: String,
    passed: Vec<String>,
    failed: Vec<String>,
}

fn run_one_test_binary(workspace: &Path, binary: &str) -> TestRun {
    let out = Command::new(cargo_bin())
        .args([
            "test",
            "-p",
            "simulation",
            "--test",
            binary,
            "--no-fail-fast",
            "--",
            "--test-threads=1",
        ])
        .current_dir(workspace)
        .output()
        .unwrap_or_else(|e| panic!("cargo test --test {binary}: {e}"));

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let mut passed = Vec::new();
    let mut failed = Vec::new();
    for source in [stdout.as_ref(), stderr.as_ref()] {
        for line in source.lines() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("test ") {
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix("test ") {
                if let Some(name) = rest.strip_suffix(" ... ok") {
                    if !passed.contains(&name.to_string()) {
                        passed.push(name.to_string());
                    }
                } else if let Some(name) = rest.strip_suffix(" ... FAILED") {
                    if !failed.contains(&name.to_string()) {
                        failed.push(name.to_string());
                    }
                }
            }
        }
    }
    TestRun {
        binary: binary.to_string(),
        passed,
        failed,
    }
}

fn load_expected_failures(workspace: &Path) -> BTreeSet<String> {
    let path = workspace
        .join("crates/simulation/tests/parity-bar/expected_failures.txt");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(_) => return BTreeSet::new(),
    };
    let mut out = BTreeSet::new();
    for raw in text.lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        out.insert(line.to_string());
    }
    out
}

fn workspace_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(manifest_dir))
}

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}

// ── Dispatch ────────────────────────────────────────────────────────

fn run_test_groups(group_name: &str) {
    let groups = match groups_for(group_name) {
        Some(g) => g,
        None => {
            eprintln!("Unknown command or test group: {group_name}\n");
            print_usage();
            std::process::exit(1);
        }
    };

    let start = Instant::now();
    let mut passed = 0usize;
    let mut failed = 0usize;

    for group in &groups {
        for step in group.steps {
            if run_step(group.name, step) {
                passed += 1;
            } else {
                failed += 1;
                let elapsed = start.elapsed();
                println!(
                    "\n--- FAILED after {:.1}s ({passed} passed, {failed} failed) ---",
                    elapsed.as_secs_f64()
                );
                std::process::exit(1);
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\n--- All {passed} step(s) passed in {:.1}s ---",
        elapsed.as_secs_f64()
    );
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if cli.list {
        print_list();
        return ExitCode::SUCCESS;
    }

    if let Some(cmd) = cli.command {
        return match cmd {
            Cmd::Node {
                dashboard_port,
                args,
            } => {
                run_node(dashboard_port, &args);
                ExitCode::SUCCESS
            }
            Cmd::ParityBar { phase, filter } => run_parity_bar(phase, &filter),
        };
    }

    match cli.group {
        Some(name) => {
            run_test_groups(&name);
            ExitCode::SUCCESS
        }
        None => {
            print_usage();
            ExitCode::from(1)
        }
    }
}
