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
  test                Run all basic non-binding tests. This includes the root crate with
                      `cargo test` plus each non-binding repository package with `cargo test -p`.
                      Feature-gated E2E/bin tests are intentionally excluded."
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
    command.args(args);

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

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("test") if args.next().is_none() => run_tests(),
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
