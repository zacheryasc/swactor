use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use clap::{Parser, Subcommand};
use serde::Deserialize;

// ── Signal handling ─────────────────────────────────────────────────

/// Ignore SIGINT in this process so the child handles Ctrl-C.
/// Without this, xtask dies immediately on Ctrl-C and the shell
/// shows a prompt before the child's shutdown messages finish.
#[cfg(unix)]
fn ignore_sigint() {
    unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN); }
}

#[cfg(not(unix))]
fn ignore_sigint() {}

// ── CLI ─────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "xtask", about = "Development task runner")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run test groups
    Test {
        /// Test group to run (core, distribution, cluster-sims, integrated, kani, stateright, essential, all)
        group: Option<String>,

        /// Show all groups and the cargo commands they run
        #[arg(long)]
        list: bool,
    },

    /// Start a datastore node
    #[command(trailing_var_arg = true)]
    Node {
        /// Port for the node
        #[arg(long)]
        port: Option<u16>,

        /// Storage path
        #[arg(long)]
        storage_path: Option<String>,

        /// Enable auth (bare --auth → true, --auth=false → false)
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        auth: Option<bool>,

        /// Auth directory
        #[arg(long)]
        auth_dir: Option<String>,

        /// Extra arguments forwarded to the underlying binary
        #[arg(allow_hyphen_values = true)]
        extra: Vec<String>,
    },

    /// Build the crypto WASM module
    Wasm,

    /// Run a datastore CLI command
    #[command(trailing_var_arg = true)]
    Cli {
        /// Node URL
        #[arg(long)]
        url: Option<String>,

        /// Path to key file
        #[arg(long)]
        key: Option<String>,

        /// Extra arguments forwarded to the underlying binary
        #[arg(allow_hyphen_values = true)]
        extra: Vec<String>,
    },
}

// ── Config file ─────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct Config {
    #[serde(default)]
    node: NodeConfig,
    #[serde(default)]
    cli: CliConfig,
}

#[derive(Deserialize, Default)]
struct NodeConfig {
    port: Option<u16>,
    storage_path: Option<String>,
    auth: Option<bool>,
    auth_dir: Option<String>,
}

#[derive(Deserialize, Default)]
struct CliConfig {
    url: Option<String>,
    key: Option<String>,
}

fn load_config(root: &Path) -> Config {
    let path = root.join("xtask/config.toml");
    match std::fs::read_to_string(&path) {
        Ok(content) => toml::from_str(&content).unwrap_or_else(|e| {
            eprintln!("Warning: failed to parse {}: {e}", path.display());
            Config::default()
        }),
        Err(_) => Config::default(),
    }
}

// ── Workspace root ──────────────────────────────────────────────────

fn workspace_root() -> PathBuf {
    let mut dir = std::env::current_dir().expect("cannot determine current directory");
    loop {
        if dir.join("Cargo.toml").exists() && dir.join("xtask").is_dir() {
            return dir;
        }
        if !dir.pop() {
            panic!("could not find workspace root (Cargo.toml + xtask/ dir)");
        }
    }
}

// ── Test infrastructure (unchanged) ─────────────────────────────────

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

const DISTRIBUTION: Group = Group {
    name: "distribution",
    description: "Distribution protocol + datastore",
    steps: &[
        TestStep {
            label: "distribution protocol",
            args: &["test", "-p", "distribution"],
        },
        TestStep {
            label: "datastore",
            args: &["test", "-p", "swactor-datastore"],
        },
    ],
};

const CLUSTER_SIMS: Group = Group {
    name: "cluster-sims",
    description: "Deterministic cluster simulations",
    steps: &[TestStep {
        label: "cluster simulations",
        args: &["test", "-p", "simulation"],
    }],
};

const KANI: Group = Group {
    name: "kani",
    description: "Kani formal verification proofs (requires cargo-kani)",
    steps: &[TestStep {
        label: "authz engine proofs",
        args: &["kani", "-p", "swactor-datastore"],
    }],
};

const STATERIGHT: Group = Group {
    name: "stateright",
    description: "Stateright model checking (gateway dispatch)",
    steps: &[TestStep {
        label: "gateway dispatch model check",
        args: &["test", "-p", "swactor-datastore", "--test", "gateway_model_check"],
    }],
};

const INTEGRATED: Group = Group {
    name: "integrated",
    description: "HTTP API + dashboard end-to-end tests",
    steps: &[
        TestStep {
            label: "datastore integration (node features)",
            args: &[
                "test",
                "-p",
                "swactor-datastore",
                "--features",
                "node",
                "--test",
                "api_integration_test",
                "--test",
                "dashboard_integration_test",
            ],
        },
        TestStep {
            label: "runtime dashboard",
            args: &["test", "-p", "runtime-dashboard"],
        },
    ],
};

fn groups_for(name: &str) -> Option<Vec<&'static Group>> {
    match name {
        "core" => Some(vec![&CORE]),
        "distribution" => Some(vec![&DISTRIBUTION]),
        "cluster-sims" => Some(vec![&CLUSTER_SIMS]),
        "integrated" => Some(vec![&INTEGRATED]),
        "kani" => Some(vec![&KANI]),
        "stateright" => Some(vec![&STATERIGHT]),
        "essential" => Some(vec![&CORE, &DISTRIBUTION, &INTEGRATED]),
        "all" => Some(vec![&CORE, &DISTRIBUTION, &CLUSTER_SIMS, &INTEGRATED]),
        _ => None,
    }
}

fn run_step(group_name: &str, step: &TestStep) -> bool {
    println!("\n=== {group_name}: {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    let status = Command::new("cargo")
        .args(step.args)
        .status();

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
USAGE: cargo xtask test <GROUP>

GROUPS:
  core          Actor runtime, message delivery, property tests
  distribution  Distribution protocol + datastore
  cluster-sims  Deterministic cluster simulations
  integrated    HTTP API + dashboard end-to-end tests
  kani          Kani formal verification proofs (requires cargo-kani)
  stateright    Stateright model checking (gateway dispatch)
  essential     core + distribution + integrated (merge gate)
  all           Every test group

FLAGS:
  --list        Show all groups and the cargo commands they run"
    );
}

fn print_list() {
    let all_groups: &[(&[&str], &Group)] = &[
        (&[], &CORE),
        (&[], &DISTRIBUTION),
        (&[], &CLUSTER_SIMS),
        (&[], &INTEGRATED),
        (&[], &KANI),
        (&[], &STATERIGHT),
    ];

    println!("Available test groups:\n");

    for &(_, group) in all_groups {
        println!("  {:<14}{}", group.name, group.description);
        for step in group.steps {
            println!("                → cargo {}", step.args.join(" "));
        }
        println!();
    }

    println!("  {:<14}core + distribution + integrated (merge gate)", "essential");
    println!("  {:<14}Every test group", "all");
}

// ── Dispatch ────────────────────────────────────────────────────────

fn run_test(group: Option<String>, list: bool) {
    if list {
        print_list();
        return;
    }

    let group_name = match group {
        Some(g) => g,
        None => {
            print_usage();
            std::process::exit(1);
        }
    };

    let groups = match groups_for(&group_name) {
        Some(g) => g,
        None => {
            eprintln!("Unknown test group: {group_name}\n");
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

fn run_node(
    port: Option<u16>,
    storage_path: Option<String>,
    auth: Option<bool>,
    auth_dir: Option<String>,
    extra: Vec<String>,
    cfg: &NodeConfig,
) {
    ignore_sigint();
    let port = port.or(cfg.port).unwrap_or(9091);
    let storage_path = storage_path
        .or_else(|| cfg.storage_path.clone())
        .unwrap_or_else(|| "./datastore".into());
    let auth_enabled = auth.or(cfg.auth).unwrap_or(true);
    let auth_dir = auth_dir
        .or_else(|| cfg.auth_dir.clone())
        .unwrap_or_else(|| "./auth".into());

    // Build first, then run the binary directly (not via `cargo run`).
    // This avoids cargo sitting in the middle of the process chain and
    // dying from SIGINT before the node finishes its shutdown.
    let build_status = Command::new("cargo")
        .args([
            "build", "-p", "swactor-datastore", "--features", "node",
            "--bin", "swactor-store-node",
        ])
        .status();
    match build_status {
        Ok(s) if !s.success() => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("Failed to execute cargo build: {e}");
            std::process::exit(1);
        }
        _ => {}
    }

    // Locate the built binary
    let root = workspace_root();
    let binary = root.join("target/debug/swactor-store-node");
    if !binary.exists() {
        eprintln!("Binary not found at {}", binary.display());
        std::process::exit(1);
    }

    let mut bin_args: Vec<String> = vec![
        "--port".into(),
        port.to_string(),
        "--storage-path".into(),
        storage_path,
    ];

    if auth_enabled {
        bin_args.push("--auth".into());
        bin_args.push("--auth-dir".into());
        bin_args.push(auth_dir);
    }

    bin_args.extend(extra);

    let status = Command::new(&binary).args(&bin_args).status();
    match status {
        Ok(s) if !s.success() => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("Failed to execute {}: {e}", binary.display());
            std::process::exit(1);
        }
        _ => {}
    }
}

fn run_cli(
    url: Option<String>,
    key: Option<String>,
    extra: Vec<String>,
    cfg: &CliConfig,
) {
    ignore_sigint();
    let url = url
        .or_else(|| cfg.url.clone())
        .unwrap_or_else(|| "http://localhost:9091".into());
    let key = key.or_else(|| cfg.key.clone()).or_else(|| {
        // Only default to owner.key.json if the file exists
        let default_path = "./auth/owner.key.json";
        if Path::new(default_path).exists() {
            Some(default_path.into())
        } else {
            None
        }
    });

    // Build first, then run the binary directly.
    let build_status = Command::new("cargo")
        .args([
            "build", "-p", "swactor-datastore", "--features", "cli",
            "--bin", "swactor-store",
        ])
        .status();
    match build_status {
        Ok(s) if !s.success() => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("Failed to execute cargo build: {e}");
            std::process::exit(1);
        }
        _ => {}
    }

    let root = workspace_root();
    let binary = root.join("target/debug/swactor-store");

    let mut bin_args: Vec<String> = vec![
        "--url".into(),
        url,
    ];

    if let Some(key) = key {
        bin_args.push("--key".into());
        bin_args.push(key);
    }

    bin_args.extend(extra);

    let status = Command::new(&binary).args(&bin_args).status();
    match status {
        Ok(s) if !s.success() => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("Failed to execute {}: {e}", binary.display());
            std::process::exit(1);
        }
        _ => {}
    }
}

fn run_wasm() {
    let root = workspace_root();

    println!("Building crypto WASM module...");
    let status = Command::new("cargo")
        .args([
            "build",
            "--target", "wasm32-unknown-unknown",
            "--release",
            "-p", "swactor-crypto-wasm",
        ])
        .status();

    match status {
        Ok(s) if !s.success() => {
            eprintln!("WASM build failed");
            std::process::exit(s.code().unwrap_or(1));
        }
        Err(e) => {
            eprintln!("Failed to execute cargo build: {e}");
            std::process::exit(1);
        }
        _ => {}
    }

    let src = root.join("target/wasm32-unknown-unknown/release/swactor_crypto_wasm.wasm");
    let dst = root.join("crates/datastore/src/crypto_wasm.wasm");

    std::fs::copy(&src, &dst).unwrap_or_else(|e| {
        eprintln!("Failed to copy {} → {}: {e}", src.display(), dst.display());
        std::process::exit(1);
    });

    let size = std::fs::metadata(&dst).map(|m| m.len()).unwrap_or(0);
    println!("Copied {} ({} bytes)", dst.display(), size);

    // Try wasm-strip if available (optional optimization)
    if Command::new("wasm-strip").arg(&dst).status().is_ok() {
        let stripped_size = std::fs::metadata(&dst).map(|m| m.len()).unwrap_or(0);
        println!("Stripped to {} bytes", stripped_size);
    }
}

fn main() {
    let cli = Cli::parse();
    let root = workspace_root();
    let config = load_config(&root);

    match cli.command {
        Cmd::Test { group, list } => run_test(group, list),
        Cmd::Wasm => run_wasm(),
        Cmd::Node {
            port,
            storage_path,
            auth,
            auth_dir,
            extra,
        } => run_node(port, storage_path, auth, auth_dir, extra, &config.node),
        Cmd::Cli {
            url,
            key,
            extra,
        } => run_cli(url, key, extra, &config.cli),
    }
}
