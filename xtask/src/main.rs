use std::process::{Command, ExitCode};
use std::time::Instant;

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
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
  demo [--port n] [--nodes n] [--docker]
                      Run the visual provisioning-reconciler demo.
  check-telemetry-isolation
                      Verify no frame types appear in control-plane modules.
  test                Run the non-binding repository test barrier."
    );
}

fn run_step(step: &TestStep) -> bool {
    println!("\n=== {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    match swactor_process::command_status(Command::new(cargo_bin()).args(step.args)) {
        Ok(status) => status.success(),
        Err(error) => {
            eprintln!("Failed to execute cargo: {error}");
            false
        }
    }
}

fn run_tests() -> ExitCode {
    let start = Instant::now();
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

/// Verify that control-plane modules never import telemetry frame/read-side
/// types. They may emit through the producer API only.
fn check_telemetry_isolation() -> ExitCode {
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
        ExitCode::from(1)
    } else {
        println!("telemetry-isolation: OK — no frame types in control-plane modules.");
        ExitCode::SUCCESS
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
        Some("check-telemetry-isolation") => check_telemetry_isolation(),
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
