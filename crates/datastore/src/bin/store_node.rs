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

use swactor_datastore::actors::{BlobStoreActor, DatastoreNode, MetadataActor};
use swactor_datastore::api::start_api_server;
use swactor_datastore::messages::MetadataMsg;
use swactor_datastore::metrics::DatastoreMetrics;
use swactor_datastore::storage::{FilesystemBackend, InMemoryBackend};
use swactor_datastore::DatastoreConfig;

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

    // Signal handler
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
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

    // Generate node ID from random bytes
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

    // Start runtime
    let handle = rt.run().expect("failed to start runtime");

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
        cfg.port,
        Arc::clone(&metrics),
    );

    eprintln!("Node {} started", &node_hex[..8]);
    eprintln!("API at http://0.0.0.0:{}", cfg.port);
    if let Some(port) = cfg.dashboard_port {
        eprintln!("Dashboard at http://0.0.0.0:{port}");
    }
    if cfg.storage_path.is_some() {
        eprintln!("Storage: {}", cfg.storage_path.as_ref().unwrap());
    } else {
        eprintln!("Storage: in-memory");
    }

    // Main loop
    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        round += 1;

        if round % cfg.gc_interval == 0 {
            let _ = handle
                .runtime
                .send_to(metadata_addr, MetadataMsg::GcTick);
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
    handle.join();
}
