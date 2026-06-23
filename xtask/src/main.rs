use std::process::{Command, ExitCode};
use std::time::Instant;

use clap::Parser;

// ── CLI ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "xtask", about = "Development task runner")]
struct Cli {
    /// (Legacy) Test group to run directly without subcommand
    #[arg(hide = true)]
    group: Option<String>,

    /// Show all groups and the cargo commands they run
    #[arg(long)]
    list: bool,
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

const GROUPS: &[&Group] = &[&CORE];

fn groups_for(name: &str) -> Option<Vec<&'static Group>> {
    match name {
        "core" | "essential" | "all" => Some(vec![&CORE]),
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
USAGE: cargo xtask <GROUP>

TEST GROUPS:
  core                 Actor runtime, message delivery, property tests
  essential            core
  all                  core

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

    println!("  {:<22}core", "essential");
    println!("  {:<22}Every test group", "all");
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
