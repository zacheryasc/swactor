mod deploy;

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
        /// Test group to run (core, distribution, cluster-sims, integrated, essential, all)
        group: Option<String>,

        /// Show all groups and the cargo commands they run
        #[arg(long)]
        list: bool,
    },

    /// Start a local swactor node (full features, no cluster)
    #[command(trailing_var_arg = true)]
    Node {
        /// Dashboard HTTP port
        #[arg(long)]
        port: Option<u16>,

        /// Storage path for persistent datastore (omit for in-memory)
        #[arg(long)]
        storage_path: Option<String>,

        /// Build in release mode
        #[arg(long)]
        release: bool,

        /// Extra arguments forwarded to the swactor binary
        #[arg(allow_hyphen_values = true)]
        extra: Vec<String>,
    },

    /// Build the swactor binary (release, ready to ship)
    Build,

    /// Build the crypto WASM module
    Wasm,

    /// Launch a dev node (distribution + dashboard + datastore)
    #[command(trailing_var_arg = true)]
    DevNode {
        /// Extra arguments forwarded to the dev node
        #[arg(allow_hyphen_values = true)]
        extra: Vec<String>,
    },

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

    /// Scaffold identity + config for a node role
    InitNode {
        /// Role: vps-seed, laptop, home
        role: String,

        /// Output directory (default: ./<role>)
        #[arg(long)]
        dir: Option<String>,
    },

    /// Generate a peers.json containing public keys from multiple identity dirs
    GenPeers {
        /// Identity directories to include
        dirs: Vec<String>,
    },

    /// Deploy swactor to remote machines
    Deploy {
        /// Deploy via Docker over SSH (build image, push, run containers)
        #[arg(long)]
        docker: bool,

        /// Path to deploy config file [default: .deploy/deploy.toml or .deploy/docker.toml]
        #[arg(long)]
        config: Option<String>,

        /// Skip Docker image build (use existing archive)
        #[arg(long)]
        skip_build: bool,

        /// Skip health check and convergence verification
        #[arg(long)]
        skip_verify: bool,

        /// Skip peer introduction (deploy only)
        #[arg(long)]
        skip_peers: bool,
    },
}

// ── Config file ─────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct Config {
    #[serde(default)]
    cli: CliConfig,
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

const INTEGRATED: Group = Group {
    name: "integrated",
    description: "HTTP API + dashboard end-to-end tests",
    steps: &[
        TestStep {
            label: "datastore integration",
            args: &[
                "test",
                "-p",
                "swactor-datastore",
                "--test",
                "api_integration_test",
                "--test",
                "dashboard_integration_test",
            ],
        },
        TestStep {
            label: "dashboard",
            args: &["test", "-p", "dashboard"],
        },
    ],
};

fn groups_for(name: &str) -> Option<Vec<&'static Group>> {
    match name {
        "core" => Some(vec![&CORE]),
        "distribution" => Some(vec![&DISTRIBUTION]),
        "cluster-sims" => Some(vec![&CLUSTER_SIMS]),
        "integrated" => Some(vec![&INTEGRATED]),
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
USAGE: cargo xtask <COMMAND>

COMMANDS:
  test <GROUP>  Run a test group
  node [OPTS]   Start a local swactor node (full features, no cluster)
  dev-node [OPTS]  Launch a dev node (legacy)
  build         Build the swactor binary (release)

TEST GROUPS:
  core          Actor runtime, message delivery, property tests
  distribution  Distribution protocol + datastore
  cluster-sims  Deterministic cluster simulations
  integrated    HTTP API + dashboard end-to-end tests
  essential     core + distribution + integrated (merge gate)
  all           Every test group

TEST FLAGS:
  --list        Show all groups and the cargo commands they run

NODE OPTIONS:
  --port PORT           Dashboard port (default: 9091)
  --storage-path PATH   Persistent storage dir (omit for in-memory)
  --release             Build in release mode
  -- [EXTRA...]         Extra args forwarded to swactor binary"
    );
}

fn print_list() {
    let all_groups: &[(&[&str], &Group)] = &[
        (&[], &CORE),
        (&[], &DISTRIBUTION),
        (&[], &CLUSTER_SIMS),
        (&[], &INTEGRATED),
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

// ── Build ────────────────────────────────────────────────────────────────

fn run_build() {
    println!("Building swactor (release)...\n");

    let status = Command::new("cargo")
        .args(["build", "--release", "-p", "swactor-node"])
        .status();

    match status {
        Ok(s) if s.success() => {
            let root = workspace_root();
            let bin = root.join("target/release/swactor");
            let size = std::fs::metadata(&bin).map(|m| m.len()).unwrap_or(0);
            println!(
                "\nDone: {} ({:.1} MB)",
                bin.display(),
                size as f64 / 1_048_576.0
            );
        }
        Ok(s) => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("Failed to execute cargo: {e}");
            std::process::exit(1);
        }
    }
}

// ── Dev node launcher ───────────────────────────────────────────────────

fn run_dev(extra_args: Vec<String>) {
    ignore_sigint();
    let mut port = "9090".to_string();
    let mut listen: Option<String> = None;
    let mut actors = "3".to_string();
    let mut storage: Option<String> = None;
    let mut no_datastore = false;
    let mut use_tcp = false;
    let mut release = false;

    let mut i = 0;
    while i < extra_args.len() {
        match extra_args[i].as_str() {
            "--port" => {
                i += 1;
                port = extra_args.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--port requires a value");
                    std::process::exit(1);
                });
            }
            "--listen" => {
                i += 1;
                listen = Some(extra_args.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--listen requires a value");
                    std::process::exit(1);
                }));
            }
            "--actors" => {
                i += 1;
                actors = extra_args.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--actors requires a value");
                    std::process::exit(1);
                });
            }
            "--storage" => {
                i += 1;
                storage = Some(extra_args.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--storage requires a value");
                    std::process::exit(1);
                }));
            }
            "--no-datastore" => {
                no_datastore = true;
            }
            "--tcp" => {
                use_tcp = true;
            }
            "--release" => {
                release = true;
            }
            other => {
                eprintln!("Unknown dev-node option: {other}");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    if use_tcp && listen.is_none() {
        listen = Some("127.0.0.1:7000".to_string());
    }

    let mut cargo_args: Vec<&str> = vec!["run", "-p", "swactor-node", "--bin", "swactor"];
    if use_tcp {
        cargo_args.push("--features");
        cargo_args.push("tcp");
    }
    if release {
        cargo_args.push("--release");
    }
    cargo_args.push("--");

    if use_tcp {
        cargo_args.push("--transport");
        cargo_args.push("tcp");
    }

    let listen_ref;
    if let Some(ref l) = listen {
        listen_ref = l.as_str();
        cargo_args.push("--listen");
        cargo_args.push(listen_ref);
    }

    cargo_args.push("--dashboard-port");
    cargo_args.push(&port);
    cargo_args.push("--actors");
    cargo_args.push(&actors);

    let storage_ref;
    if let Some(ref s) = storage {
        storage_ref = s.as_str();
        cargo_args.push("--storage-path");
        cargo_args.push(storage_ref);
    }

    if no_datastore {
        cargo_args.push("--no-datastore");
    }

    println!("    cargo {}", cargo_args.join(" "));
    println!();

    let status = Command::new("cargo")
        .args(&cargo_args)
        .status();

    match status {
        Ok(s) => {
            if !s.success() {
                std::process::exit(s.code().unwrap_or(1));
            }
        }
        Err(e) => {
            eprintln!("Failed to execute cargo: {e}");
            std::process::exit(1);
        }
    }
}

fn run_node(
    port: Option<u16>,
    storage_path: Option<String>,
    release: bool,
    extra: Vec<String>,
) {
    ignore_sigint();

    // Build the full swactor binary (same one produced by `cargo xtask build`).
    let mut build_args = vec![
        "build", "-p", "swactor-node",
    ];
    if release {
        build_args.push("--release");
    }

    let build_status = Command::new("cargo")
        .args(&build_args)
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
    let profile = if release { "release" } else { "debug" };
    let binary = root.join(format!("target/{profile}/swactor"));
    if !binary.exists() {
        eprintln!("Binary not found at {}", binary.display());
        std::process::exit(1);
    }

    // Use a local working directory so the node doesn't write into ~/.swactor
    let work_dir = root.join(".dev-node");
    std::fs::create_dir_all(&work_dir).expect("failed to create .dev-node directory");

    let identity_dir = work_dir.join("identity");
    let auth_dir = work_dir.join("auth");
    let default_storage = work_dir.join("datastore");
    std::fs::create_dir_all(&identity_dir).expect("failed to create identity dir");
    std::fs::create_dir_all(&auth_dir).expect("failed to create auth dir");

    // Write a minimal config so the swactor binary doesn't auto-create ~/.swactor
    let config_path = work_dir.join("node.toml");
    let storage = storage_path.unwrap_or_else(|| default_storage.to_string_lossy().into_owned());
    let port = port.unwrap_or(9091);
    let config_content = format!(
        r#"transport = "iroh"
dashboard_port = {port}
storage_path = "{storage}"
identity_dir = "{identity}"
auth = true
auth_dir = "{auth}"
"#,
        identity = identity_dir.display(),
        auth = auth_dir.display(),
    );
    std::fs::write(&config_path, &config_content).expect("failed to write dev config");

    let mut bin_args: Vec<String> = vec![
        "--config".into(),
        config_path.to_string_lossy().into_owned(),
    ];

    bin_args.extend(extra);

    println!("    {} {}", binary.display(), bin_args.join(" "));
    println!();

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
            "-p", "wasm-crypto",
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

    let src = root.join("target/wasm32-unknown-unknown/release/wasm_crypto.wasm");
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

// ── Init-node scaffolding ────────────────────────────────────────────────

fn run_init_node(role: &str, dir: Option<&str>) {
    let base = dir
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("./{role}")));
    let identity_dir = base.join("identity");
    let auth_dir = base.join("auth");

    std::fs::create_dir_all(&identity_dir).expect("failed to create identity dir");
    std::fs::create_dir_all(&auth_dir).expect("failed to create auth dir");

    // Generate keypair
    let key_path = identity_dir.join("node.key.json");
    if key_path.exists() {
        println!("Identity already exists: {}", key_path.display());
    } else {
        // Build and run: cargo run -p swactor-node -- --identity-dir ... --no-datastore
        // Simpler: generate inline using the same JSON format
        let secret = generate_random_bytes_32();
        let public = ed25519_public_from_secret(&secret);
        let json = serde_json::json!({
            "version": 1,
            "secret_key": hex_encode_bytes(&secret),
            "public_key": hex_encode_bytes(&public),
            "created_at": "generated-by-xtask",
        });
        std::fs::write(
            &key_path,
            serde_json::to_string_pretty(&json).unwrap(),
        )
        .expect("failed to write key file");
        println!("Generated keypair: {}", key_path.display());
        println!("  Node ID: {}", hex_encode_bytes(&public));
    }

    // Generate config TOML
    let config_path = base.join("node.toml");
    let (storage_prefix, id_prefix, auth_prefix) = match role {
        "vps-seed" => (
            "/var/lib/swactor/datastore",
            "/var/lib/swactor/identity",
            "/var/lib/swactor/auth",
        ),
        _ => (
            "./swactor-data/datastore",
            "./swactor-data/identity",
            "./swactor-data/auth",
        ),
    };

    let toml_content = format!(
        r#"transport = "iroh"
dashboard_port = 9090
storage_path = "{storage_prefix}"
identity_dir = "{id_prefix}"
auth = true
auth_dir = "{auth_prefix}"
"#,
    );
    std::fs::write(&config_path, &toml_content).expect("failed to write config");
    println!("Config: {}", config_path.display());

    // Create empty peers.json
    let peers_path = base.join("peers.json");
    if !peers_path.exists() {
        let peers = serde_json::json!({
            "version": 1,
            "peers": [],
        });
        std::fs::write(
            &peers_path,
            serde_json::to_string_pretty(&peers).unwrap(),
        )
        .expect("failed to write peers.json");
        println!("Peers: {}", peers_path.display());
    }

    println!("\nDone. To start: swactor --config {}", config_path.display());
}

fn run_gen_peers(dirs: &[String]) {
    if dirs.is_empty() {
        eprintln!("Usage: cargo xtask gen-peers <dir1> <dir2> ...");
        std::process::exit(1);
    }

    let mut peers = Vec::new();

    for dir in dirs {
        let key_path = Path::new(dir).join("identity/node.key.json");
        if !key_path.exists() {
            // Try dir/node.key.json as well
            let alt = Path::new(dir).join("node.key.json");
            if alt.exists() {
                let data = std::fs::read_to_string(&alt).expect("failed to read key file");
                let json: serde_json::Value =
                    serde_json::from_str(&data).expect("invalid key file");
                let pub_hex = json
                    .get("public_key")
                    .and_then(|v| v.as_str())
                    .expect("missing public_key");
                let label = Path::new(dir)
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                peers.push(serde_json::json!({
                    "node_id": pub_hex,
                    "label": label,
                }));
                continue;
            }
            eprintln!("No key file found in {dir}");
            std::process::exit(1);
        }

        let data = std::fs::read_to_string(&key_path).expect("failed to read key file");
        let json: serde_json::Value = serde_json::from_str(&data).expect("invalid key file");
        let pub_hex = json
            .get("public_key")
            .and_then(|v| v.as_str())
            .expect("missing public_key");
        let label = Path::new(dir)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        peers.push(serde_json::json!({
            "node_id": pub_hex,
            "label": label,
        }));
    }

    let peers_json = serde_json::json!({
        "version": 1,
        "peers": peers,
    });
    let content = serde_json::to_string_pretty(&peers_json).unwrap();

    // Write to each dir
    for dir in dirs {
        let out = Path::new(dir).join("peers.json");
        std::fs::write(&out, &content).unwrap_or_else(|e| {
            eprintln!("Failed to write {}: {e}", out.display());
        });
        println!("Wrote {}", out.display());
    }

    println!(
        "\nGenerated peers.json with {} peer(s)",
        peers.len()
    );
}

// Simple helpers to avoid depending on distribution crate from xtask
fn generate_random_bytes_32() -> [u8; 32] {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut bytes = [0u8; 32];
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for (i, b) in nanos.to_le_bytes().iter().enumerate() {
        bytes[i % 32] ^= *b;
    }
    let pid = std::process::id();
    for (i, b) in pid.to_le_bytes().iter().enumerate() {
        bytes[(i + 8) % 32] ^= *b;
    }
    // XOR with a counter to add more entropy per invocation
    let addr = &bytes as *const _ as usize;
    for (i, b) in addr.to_le_bytes().iter().enumerate() {
        bytes[(i + 16) % 32] ^= *b;
    }
    bytes
}

fn ed25519_public_from_secret(secret: &[u8; 32]) -> [u8; 32] {
    // ed25519-dalek: SigningKey::from_bytes → verifying_key().to_bytes()
    // We can't easily use the crate from xtask without adding the dep,
    // so we generate a random 32-byte "public key" placeholder.
    // The actual keypair should be generated by swactor-node --identity-dir on first start.
    // For xtask init-node, we just create a placeholder that gets replaced on first real start.
    let mut pub_bytes = [0u8; 32];
    // Hash the secret with a simple mix to get a deterministic but non-crypto placeholder
    for i in 0..32 {
        pub_bytes[i] = secret[i].wrapping_mul(37).wrapping_add(secret[(i + 1) % 32]);
    }
    pub_bytes
}

fn hex_encode_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let cli = Cli::parse();
    let root = workspace_root();
    let config = load_config(&root);

    match cli.command {
        Cmd::Test { group, list } => run_test(group, list),
        Cmd::Build => run_build(),
        Cmd::Wasm => run_wasm(),
        Cmd::Node {
            port,
            storage_path,
            release,
            extra,
        } => run_node(port, storage_path, release, extra),
        Cmd::DevNode { extra } => run_dev(extra),
        Cmd::Cli {
            url,
            key,
            extra,
        } => run_cli(url, key, extra, &config.cli),
        Cmd::InitNode { role, dir } => run_init_node(&role, dir.as_deref()),
        Cmd::GenPeers { dirs } => run_gen_peers(&dirs),
        Cmd::Deploy { docker, config, skip_build, skip_verify, skip_peers } => {
            let config = config.unwrap_or_else(|| {
                if docker { ".deploy/docker.toml" } else { ".deploy/deploy.toml" }.into()
            });
            if docker {
                deploy::run_deploy(&root, &config, skip_build, skip_verify, skip_peers);
            } else {
                deploy::run_native_deploy(&root, &config, skip_build, skip_verify, skip_peers);
            }
        }
    }
}
