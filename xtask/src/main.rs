use std::process::Command;
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

    let status = Command::new("cargo").args(step.args).status();

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
    // Build the node binary first
    eprintln!("Building swactor node...");
    let build = Command::new("cargo")
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

    // Use a temp directory for identity/auth so we don't pollute the repo
    let tmp = std::env::temp_dir().join("swactor-dev-node");
    std::fs::create_dir_all(&tmp).expect("failed to create temp dir");

    let identity_dir = tmp.join("identity");
    let auth_dir = tmp.join("auth");

    eprintln!();

    let mut cmd = Command::new("target/debug/swactor");
    cmd.args([
        "--transport", "iroh",
        "--dashboard-port", &dashboard_port.to_string(),
        "--identity-dir", &identity_dir.to_string_lossy(),
        "--auth-dir", &auth_dir.to_string_lossy(),
        "--no-relay",
    ]);
    // Don't pass --config so it doesn't touch ~/.swactor
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

fn main() {
    let cli = Cli::parse();

    if cli.list {
        print_list();
        return;
    }

    // Explicit subcommand takes priority
    if let Some(cmd) = cli.command {
        match cmd {
            Cmd::Node { dashboard_port, args } => run_node(dashboard_port, &args),
        }
        return;
    }

    // Fall back to positional group name (backward compat: `cargo xtask essential`)
    match cli.group {
        Some(name) => run_test_groups(&name),
        None => {
            print_usage();
            std::process::exit(1);
        }
    }
}
