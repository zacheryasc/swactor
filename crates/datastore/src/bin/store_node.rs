//! swactor-store-node — standalone datastore node with HTTP API.
//!
//! Starts the actor runtime, spawns datastore actors, and serves
//! a REST API for external tools (the `swactor-store` CLI).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::Parser;
use serde::Deserialize;

use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use swactor_datastore::actors::{BlobStoreActor, DatastoreNode, GatewayActor, MetadataActor};
use swactor_datastore::api::start_api_server;
use swactor_datastore::auth::{AccessControlList, AuthzEngine};
use swactor_datastore::messages::{GatewayMsg, MetadataMsg};
use swactor_datastore::metrics::DatastoreMetrics;
use swactor_datastore::storage::{FilesystemBackend, InMemoryBackend};
use swactor_datastore::DatastoreConfig;

use distribution::crypto::Keypair;
use distribution::types::NodeId;

#[derive(Parser)]
#[command(name = "swactor-store-node", about = "Swactor distributed datastore node")]
struct Args {
    /// Path to a TOML config file
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// HTTP API port
    #[arg(long)]
    port: Option<u16>,

    /// Storage directory (omit for in-memory)
    #[arg(long)]
    storage_path: Option<String>,

    /// Dashboard HTTP port (omit to disable dashboard)
    #[arg(long)]
    dashboard_port: Option<u16>,

    /// Chunk size in bytes
    #[arg(long)]
    chunk_size: Option<u32>,

    /// GC interval in ticks (each tick is ~100ms)
    #[arg(long)]
    gc_interval: Option<u64>,

    /// Dissemination interval in ticks
    #[arg(long)]
    disseminate_interval: Option<u64>,

    /// Enable auth (generates owner keypair if needed)
    #[arg(long)]
    auth: bool,

    /// Directory for owner.key.json + acl.json (default: "auth")
    #[arg(long, default_value = "auth")]
    auth_dir: String,
}

#[derive(Deserialize, Default)]
struct NodeConfig {
    port: Option<u16>,
    storage_path: Option<String>,
    dashboard_port: Option<u16>,
    chunk_size: Option<u32>,
    gc_interval: Option<u64>,
    disseminate_interval: Option<u64>,
}

/// Resolved configuration with CLI > config file > defaults applied.
struct ResolvedConfig {
    port: u16,
    storage_path: Option<String>,
    dashboard_port: Option<u16>,
    chunk_size: u32,
    gc_interval: u64,
    disseminate_interval: u64,
}

// ── Key file helpers ────────────────────────────────────────────────────────

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Some(bytes)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn load_or_generate_keypair(path: &std::path::Path) -> Keypair {
    if path.exists() {
        let data = std::fs::read_to_string(path).expect("failed to read key file");
        let json: serde_json::Value = serde_json::from_str(&data).expect("invalid key file JSON");
        let secret_hex = json
            .get("secret_key")
            .and_then(|v| v.as_str())
            .expect("key file missing secret_key");
        let secret_bytes = hex_decode(secret_hex).expect("invalid secret_key hex");
        let secret: [u8; 32] = secret_bytes
            .try_into()
            .expect("secret_key must be 32 bytes");
        Keypair::from_bytes(&secret)
    } else {
        let keypair = Keypair::generate();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let json = serde_json::json!({
            "version": 1,
            "secret_key": hex_encode(&keypair.secret_bytes()),
            "public_key": hex_encode(&keypair.node_id().0),
            "created_at": format_timestamp(now),
        });
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create key file directory");
        }
        std::fs::write(path, serde_json::to_string_pretty(&json).unwrap())
            .expect("failed to write key file");
        keypair
    }
}

fn format_timestamp(secs: u64) -> String {
    // Simple ISO-8601 UTC timestamp
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    // Days since epoch to Y-M-D (simplified)
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn resolve_config(args: &Args) -> ResolvedConfig {
    let file_cfg = match &args.config {
        Some(path) => {
            let contents = std::fs::read_to_string(path)
                .unwrap_or_else(|e| panic!("failed to read config file {}: {e}", path.display()));
            toml::from_str::<NodeConfig>(&contents)
                .unwrap_or_else(|e| panic!("failed to parse config file {}: {e}", path.display()))
        }
        None => NodeConfig::default(),
    };

    ResolvedConfig {
        port: args.port.or(file_cfg.port).unwrap_or(9091),
        storage_path: args.storage_path.clone().or(file_cfg.storage_path),
        dashboard_port: args.dashboard_port.or(file_cfg.dashboard_port),
        chunk_size: args.chunk_size.or(file_cfg.chunk_size).unwrap_or(1_048_576),
        gc_interval: args.gc_interval.or(file_cfg.gc_interval).unwrap_or(1000),
        disseminate_interval: args.disseminate_interval.or(file_cfg.disseminate_interval).unwrap_or(50),
    }
}

fn main() {
    let args = Args::parse();
    let cfg = resolve_config(&args);
    let stop = Arc::new(AtomicBool::new(false));

    // Signal handler — double Ctrl-C forces immediate exit
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            if stop.load(Ordering::Relaxed) {
                eprintln!("\nForced exit.");
                std::process::exit(1);
            }
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set signal handler");
    }

    // Optionally start dashboard
    let dash = cfg.dashboard_port.map(|port| {
        let d = runtime_dashboard::start_dashboard(runtime_dashboard::DashboardConfig {
            port,
            ..Default::default()
        });
        d.install_tracing();
        d
    });

    // Create runtime
    let num_threads = 2;
    let collector = runtime_dashboard::collector::StatsCollector::new(num_threads);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    // Generate or load node identity
    let (node_id, owner_keypair) = if args.auth {
        let auth_dir = std::path::PathBuf::from(&args.auth_dir);
        std::fs::create_dir_all(&auth_dir).expect("failed to create auth directory");
        let key_path = auth_dir.join("owner.key.json");
        let keypair = load_or_generate_keypair(&key_path);
        let nid = keypair.node_id();
        eprintln!(
            "Auth enabled — owner key: {}",
            hex_encode(&nid.0)
        );
        eprintln!("Key file: {}", key_path.display());
        (nid, Some((keypair, auth_dir)))
    } else {
        let node_id = {
            let mut bytes = [0u8; 32];
            for (i, b) in std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_le_bytes()
                .iter()
                .enumerate()
            {
                bytes[i % 32] ^= *b;
            }
            // Mix in process id for uniqueness
            let pid = std::process::id();
            for (i, b) in pid.to_le_bytes().iter().enumerate() {
                bytes[i + 16] ^= *b;
            }
            NodeId(bytes)
        };
        (node_id, None)
    };

    // Datastore config
    let config = DatastoreConfig {
        chunk_size: cfg.chunk_size,
        storage_path: cfg
            .storage_path
            .as_ref()
            .map(|s| s.into())
            .unwrap_or_else(|| "datastore".into()),
        gc_interval: cfg.gc_interval,
        ..Default::default()
    };

    // Create storage backend
    let backend: Box<dyn swactor_datastore::StorageBackend> = match &cfg.storage_path {
        Some(path) => {
            let p = std::path::PathBuf::from(path);
            std::fs::create_dir_all(&p).expect("failed to create storage directory");
            Box::new(FilesystemBackend::new(p))
        }
        None => Box::new(InMemoryBackend::new()),
    };

    // Spawn actors before starting runtime threads
    let blob_store_addr = rt
        .spawn(BlobStoreActor::new(backend))
        .expect("failed to spawn BlobStoreActor");

    let mut metadata = MetadataActor::new(node_id, &config);
    metadata.set_blob_store(blob_store_addr);
    let metadata_addr = rt
        .spawn(metadata)
        .expect("failed to spawn MetadataActor");

    let datastore_node = DatastoreNode::new(node_id, blob_store_addr, metadata_addr, config);
    let datastore_addr = rt
        .spawn(datastore_node)
        .expect("failed to spawn DatastoreNode");

    // Spawn GatewayActor if auth is enabled
    let gateway_addr = if let Some((_, ref auth_dir)) = owner_keypair {
        let acl_path = auth_dir.join("acl.json");
        let acl = AccessControlList::load_or_create(&acl_path, node_id)
            .expect("failed to load/create ACL");
        let engine = AuthzEngine::new(acl);
        let gateway = GatewayActor::new(engine, datastore_addr, Some(acl_path));
        let addr = rt
            .spawn(gateway)
            .expect("failed to spawn GatewayActor");
        Some(addr)
    } else {
        None
    };

    // Start runtime
    let handle = rt.run().expect("failed to start runtime");

    // Load persisted entries from storage
    {
        let inbox = handle
            .runtime
            .new_inbox::<swactor_datastore::DatastoreResponse>()
            .expect("failed to create inbox");
        let _ = handle.runtime.send_to(
            blob_store_addr,
            swactor_datastore::BlobStoreMsg::LoadAll {
                reply_to: *inbox.addr(),
            },
        );
        // Poll for response (up to 5 seconds)
        let start = std::time::Instant::now();
        let mut loaded = false;
        while start.elapsed() < Duration::from_secs(5) {
            if let Some(resp) = inbox.try_recv() {
                match resp {
                    swactor_datastore::DatastoreResponse::LoadedAll { entries } => {
                        let n = entries.len();
                        let _ = handle.runtime.send_to(
                            metadata_addr,
                            swactor_datastore::MetadataMsg::BulkLoad { entries },
                        );
                        if n > 0 {
                            eprintln!("Loaded {n} entries from storage");
                        }
                        loaded = true;
                    }
                    swactor_datastore::DatastoreResponse::Error { reason } => {
                        eprintln!("Warning: failed to load entries: {reason}");
                        loaded = true;
                    }
                    _ => {}
                }
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        if !loaded {
            eprintln!("Warning: timed out loading entries from storage");
        }
    }

    // Create datastore metrics
    let node_hex: String = node_id.0.iter().map(|b| format!("{b:02x}")).collect();
    let metrics = Arc::new(DatastoreMetrics::new());
    metrics.set_node_id(node_hex.clone());

    if let Some(ref d) = dash {
        d.set_runtime(handle.runtime.clone(), collector);
        d.set_datastore(Arc::clone(&metrics) as Arc<dyn runtime_dashboard::datastore_collector::DatastoreStatsProvider>);
    }

    // Start HTTP API
    let (api_shutdown, _peers) = start_api_server(
        handle.runtime.clone(),
        datastore_addr,
        metadata_addr,
        blob_store_addr,
        gateway_addr,
        cfg.port,
        Arc::clone(&metrics),
    );

    eprintln!("──────────────────────────────────────");
    eprintln!("  swactor-store node {}", &node_hex[..16]);
    eprintln!("  API:     http://0.0.0.0:{}", cfg.port);
    if let Some(port) = cfg.dashboard_port {
        eprintln!("  Dashboard: http://0.0.0.0:{port}");
    }
    if cfg.storage_path.is_some() {
        eprintln!("  Storage: {} (filesystem)", cfg.storage_path.as_ref().unwrap());
    } else {
        eprintln!("  Storage: in-memory");
    }
    if owner_keypair.is_some() {
        eprintln!("  Auth:    enabled (owner {})", &node_hex[..16]);
    } else {
        eprintln!("  Auth:    disabled");
    }
    eprintln!("──────────────────────────────────────");

    // Main loop
    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        round += 1;

        if round % cfg.gc_interval == 0 {
            let _ = handle
                .runtime
                .send_to(metadata_addr, MetadataMsg::GcTick);

            if let Some(gw) = gateway_addr {
                let _ = handle.runtime.send_to(gw, GatewayMsg::NonceGcTick);
            }
        }

        if round % cfg.disseminate_interval == 0 {
            let _ = handle
                .runtime
                .send_to(metadata_addr, MetadataMsg::DisseminateTick);
        }

        thread::sleep(Duration::from_millis(100));
    }

    eprintln!("\nShutting down...");
    api_shutdown.store(true, Ordering::Relaxed);
    handle.shutdown();
    if let Some(d) = dash {
        d.shutdown();
    }
    // Brief pause for threads to flush I/O, then exit.
    // No join — cargo run already died from SIGINT so there's
    // no parent waiting on us; just exit cleanly.
    thread::sleep(Duration::from_millis(50));
    eprintln!("Shutdown complete.");
    std::process::exit(0);
}
