use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Instant;

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
}

const TEST_STEPS: &[TestStep] = &[
    TestStep {
        label: "strict workspace lint",
        args: &["lint"],
    },
    TestStep {
        label: "all Rust tests (60s per-test timeout)",
        args: &["nextest", "run", "--workspace", "--all-features"],
    },
    TestStep {
        label: "all Rust doctests",
        args: &[
            "test",
            "--workspace",
            "--all-features",
            "--doc",
            "--exclude",
            "python",
            "--exclude",
            "wasm-runtime",
        ],
    },
];

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}

fn remove_inherited_build_context(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        let name_text = name.to_string_lossy();
        let package_scoped = name_text.starts_with("CARGO_PKG_")
            || name_text.starts_with("CARGO_FEATURE_")
            || name_text.starts_with("CARGO_CFG_")
            || name_text.starts_with("DEP_");
        let build_scoped = matches!(
            name_text.as_ref(),
            "CARGO_BIN_NAME"
                | "CARGO_CRATE_NAME"
                | "CARGO_MANIFEST_DIR"
                | "CARGO_MANIFEST_PATH"
                | "CARGO_PRIMARY_PACKAGE"
                | "DEBUG"
                | "HOST"
                | "NUM_JOBS"
                | "OPT_LEVEL"
                | "OUT_DIR"
                | "PROFILE"
                | "PYO3_ENVIRONMENT_SIGNATURE"
                | "TARGET"
        );
        if package_scoped || build_scoped {
            command.env_remove(name);
        }
    }
}

fn cargo_command() -> Command {
    let mut command = Command::new(cargo_bin());
    remove_inherited_build_context(&mut command);
    command
}

fn print_usage() {
    println!(
        "\
USAGE: cargo xtask <command>

COMMANDS:
  demo [--port n] [--nodes n] [--docker]
                      Run the visual provisioning-reconciler demo.
  check-telemetry-isolation
                      Verify no frame types appear in control-plane modules.
  test                Run strict lint plus every Rust and Python test."
    );
}

fn run_step(step: &TestStep, python: &Path) -> bool {
    println!("\n=== {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    match swactor_process::command_status(
        cargo_command().args(step.args).env("PYO3_PYTHON", python),
    ) {
        Ok(status) => status.success(),
        Err(error) => {
            eprintln!("Failed to execute cargo: {error}");
            false
        }
    }
}

fn run_tests() -> ExitCode {
    let start = Instant::now();
    let Some(python) = PythonTestTools::discover() else {
        return ExitCode::from(1);
    };
    if !nextest_available() {
        return ExitCode::from(1);
    }
    if !check_telemetry_isolation() {
        return ExitCode::from(1);
    }
    for (index, step) in TEST_STEPS.iter().enumerate() {
        if !run_step(step, &python.python) {
            eprintln!(
                "\n--- FAILED after {:.1}s ({index} passed, 1 failed) ---",
                start.elapsed().as_secs_f64()
            );
            return ExitCode::from(1);
        }
    }
    if !python.run() {
        eprintln!(
            "\n--- FAILED after {:.1}s (Python tests failed) ---",
            start.elapsed().as_secs_f64()
        );
        return ExitCode::from(1);
    }

    println!(
        "\n--- All {} step(s) passed in {:.1}s ---",
        TEST_STEPS.len() + 1,
        start.elapsed().as_secs_f64()
    );
    ExitCode::SUCCESS
}

fn nextest_available() -> bool {
    let available = swactor_process::command_status(
        cargo_command()
            .args(["nextest", "--version"])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .is_ok_and(|status| status.success());
    if !available {
        eprintln!("cargo-nextest is required; install it from https://nexte.st/docs/installation/");
    }
    available
}

struct PythonTestTools {
    directory: PathBuf,
    maturin: PathBuf,
    python: PathBuf,
}

impl PythonTestTools {
    fn discover() -> Option<Self> {
        let directory = workspace_root().join("crates/bindings/python");
        let tools = Self {
            maturin: directory.join(".venv/bin/maturin"),
            python: directory.join(".venv/bin/python"),
            directory,
        };
        if tools.maturin.is_file() && tools.python.is_file() {
            Some(tools)
        } else {
            eprintln!(
                "Python test environment is missing; run `uv sync --project {}` first",
                tools.directory.display()
            );
            None
        }
    }

    fn run(&self) -> bool {
        println!("\n=== all Python tests ===");
        let mut build_command = Command::new(&self.maturin);
        remove_inherited_build_context(&mut build_command);
        let built = swactor_process::command_status(
            build_command
                .current_dir(&self.directory)
                .arg("develop")
                .env("PYO3_PYTHON", &self.python),
        )
        .is_ok_and(|status| status.success());
        if !built {
            return false;
        }
        swactor_process::command_status(
            Command::new(&self.python)
                .current_dir(&self.directory)
                .args([
                    "-m",
                    "pytest",
                    "-q",
                    "--timeout=60",
                    "tests/test_bootstrap.py",
                    "../../../tests/test_python.py",
                ]),
        )
        .is_ok_and(|status| status.success())
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must live directly under the workspace root")
        .to_path_buf()
}

/// Verify that control-plane modules never import telemetry frame/read-side
/// types. They may emit through the producer API only.
fn check_telemetry_isolation() -> bool {
    const CONTROL_DIRS: &[&str] = &[
        "apps/myelin/src/orchestration",
        "crates/distribution/src",
        "crates/data-plane/src",
        "crates/provisioning/src",
    ];
    const FORBIDDEN: &[&str] = &[
        "telemetry::frame::",
        "telemetry::store::",
        "telemetry::ingest::",
        "telemetry::views::",
        "telemetry::transport::",
        "CollectedTelemetryFrame",
    ];

    let mut files = Vec::new();
    for dir in CONTROL_DIRS {
        collect_rs_files(dir, &mut files);
    }

    let mut found = false;
    for file in &files {
        let Ok(src) = std::fs::read_to_string(file) else {
            continue;
        };
        for (lineno, line) in src.lines().enumerate() {
            for pattern in FORBIDDEN {
                if line.contains(pattern) {
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
             or read-side modules. Use the telemetry producer API for emission."
        );
        false
    } else {
        println!("telemetry-isolation: OK — no frame types in control-plane modules.");
        true
    }
}

fn collect_rs_files(dir: &str, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(path) = path.to_str() {
                collect_rs_files(path, out);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && let Some(path) = path.to_str()
        {
            out.push(path.to_owned());
        }
    }
}

mod demo;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("check-telemetry-isolation") => {
            if check_telemetry_isolation() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Some("test") if args.next().is_none() => run_tests(),
        Some("demo") => demo::run(&args.collect::<Vec<_>>()),
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
