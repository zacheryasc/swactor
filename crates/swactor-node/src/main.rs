//! swactor-node — unified distributed node with dashboard and datastore.
//!
//! Combines distribution, runtime dashboard, and content-addressed datastore
//! into a single batteries-included binary. Datastore is on by default
//! (in-memory) and can be disabled via `--no-datastore`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use clap::{Parser, Subcommand};

use swactor::actor::{ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use distribution::crypto::Keypair;
use distribution::identity::{base58_encode, base58_decode, hex_encode, load_or_generate_keypair};
use distribution::node::DistributedNodeConfig;
use distribution::peer_auth::PeerAllowList;
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::probe::SwimConfig;
use distribution::types::NodeId;

use dashboard::collector::StatsCollector;
use dashboard::distribution_collector::DistributionStatsProvider;
use dashboard::{start_dashboard, DashboardConfig};

use swactor_datastore::bridge::DatastoreNodeFactory;
use swactor_datastore::{DatastoreAuthConfig, DatastoreGroup, DatastoreGroupConfig};

mod config;
mod install;
mod names;
#[cfg(feature = "relay")]
mod relay;

// ── CLI ──────────────────────────────────────────────────────────────────

const VERSION: &str = concat!(
    env!("SWACTOR_GIT_BRANCH"),
    " @ ",
    env!("SWACTOR_GIT_HASH"),
);

#[derive(Subcommand)]
enum Subcmd {
    /// Install swactor as a system service
    Install,
    /// Remove swactor system service
    Uninstall,
    /// View or set the node name
    Name {
        /// New name to set (omit to print current name)
        name: Option<String>,
    },
    /// Print an invite code for this node
    Invite,
    /// Join a peer using their invite code
    Join {
        /// Invite code (base58-encoded node ID)
        code: String,
    },
}

#[derive(Parser)]
#[command(
    name = "swactor",
    version = VERSION,
    about = "Unified swactor node: distribution + dashboard + datastore"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Subcmd>,

    /// Path to TOML config file
    #[arg(long)]
    config: Option<std::path::PathBuf>,

    /// Transport to use: iroh or tcp
    #[arg(long, default_value = "iroh")]
    transport: String,

    /// Address to listen on for TCP transport (e.g. 10.0.1.10:7000)
    #[arg(long)]
    listen: Option<std::net::SocketAddr>,

    /// Seed node address to join (TCP mode: host:port)
    #[arg(long)]
    seed: Option<String>,

    /// Seed node's iroh public key (iroh mode: hex-encoded 32-byte key)
    #[arg(long)]
    seed_node_id: Option<String>,

    /// Dashboard HTTP port
    #[arg(long, default_value = "9090")]
    dashboard_port: u16,

    /// Number of dummy heartbeat actors to register
    #[arg(long, default_value = "0")]
    actors: usize,

    /// Storage directory for persistent datastore (omit for in-memory)
    #[arg(long)]
    storage_path: Option<String>,

    /// Disable the datastore entirely
    #[arg(long)]
    no_datastore: bool,

    /// Chunk size in bytes
    #[arg(long, default_value = "1048576")]
    chunk_size: u32,

    /// GC interval in ticks (each tick is ~100ms)
    #[arg(long, default_value = "1000")]
    gc_interval: u64,

    /// Dissemination interval in ticks
    #[arg(long, default_value = "50")]
    disseminate_interval: u64,

    /// Directory for persistent node identity keypair
    #[arg(long, default_value = "./identity")]
    identity_dir: String,

    /// Path to peers.json for peer allow-list (omit for open mode)
    #[arg(long)]
    peers_file: Option<String>,

    /// Enable datastore auth (GatewayActor)
    #[arg(long)]
    auth: bool,

    /// Directory for auth files (ACL, owner key)
    #[arg(long, default_value = "./auth")]
    auth_dir: String,

    /// Human-readable node name (e.g. "swift-falcon")
    #[arg(long)]
    node_name: Option<String>,

    /// Disable embedded relay server
    #[arg(long)]
    no_relay: bool,

    /// Port for embedded relay server
    #[arg(long, default_value = "3340")]
    relay_port: u16,

    /// Bind address for embedded relay server
    #[arg(long, default_value = "0.0.0.0")]
    relay_bind: String,
}

// ── Dummy actor ──────────────────────────────────────────────────────────

#[derive(Clone)]
struct Heartbeat;

struct HeartbeatActor;

impl ActorInterface for HeartbeatActor {
    type Incoming = Heartbeat;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: Heartbeat) {}
}

// ── Snapshot provider ────────────────────────────────────────────────────

struct SnapshotProvider {
    snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
}

impl DistributionStatsProvider for SnapshotProvider {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }
}

// ── Main ─────────────────────────────────────────────────────────────────

fn default_config_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| {
        eprintln!("$HOME is not set");
        std::process::exit(1);
    });
    std::path::PathBuf::from(home).join(".swactor")
}

fn generate_default_config(config_dir: &std::path::Path) -> std::path::PathBuf {
    let config_path = config_dir.join("node.toml");

    // Create directory tree
    for sub in &["identity", "datastore", "auth"] {
        std::fs::create_dir_all(config_dir.join(sub)).unwrap_or_else(|e| {
            eprintln!(
                "Failed to create {}: {e}",
                config_dir.join(sub).display()
            );
            std::process::exit(1);
        });
    }

    // Write default config with absolute paths
    let contents = format!(
        r#"transport = "iroh"
dashboard_port = 9090
storage_path = "{dir}/datastore"
identity_dir = "{dir}/identity"
peers_file = "{dir}/peers.json"
auth = true
auth_dir = "{dir}/auth"
relay = true
relay_port = 3340
"#,
        dir = config_dir.display(),
    );
    std::fs::write(&config_path, &contents).unwrap_or_else(|e| {
        eprintln!("Failed to write {}: {e}", config_path.display());
        std::process::exit(1);
    });

    // Write empty peers.json
    let peers_path = config_dir.join("peers.json");
    if !peers_path.exists() {
        std::fs::write(&peers_path, "{\"version\":1,\"peers\":[]}").unwrap_or_else(|e| {
            eprintln!("Failed to write {}: {e}", peers_path.display());
            std::process::exit(1);
        });
    }

    eprintln!("Generated default config: {}", config_path.display());
    config_path
}

fn main() {
    let args = Args::parse();

    if let Some(subcmd) = &args.command {
        match subcmd {
            Subcmd::Install => {
                install::install();
                return;
            }
            Subcmd::Uninstall => {
                install::uninstall();
                return;
            }
            Subcmd::Name { .. } | Subcmd::Invite | Subcmd::Join { .. } => {
                /* handled after config/identity is loaded */
            }
        }
    }

    let stop = Arc::new(AtomicBool::new(false));

    // Resolve config path: explicit --config, or auto-detect ~/.swactor/node.toml
    let resolved_config_path = match &args.config {
        Some(path) => Some(path.clone()),
        None => {
            let default_path = default_config_dir().join("node.toml");
            if default_path.exists() {
                Some(default_path)
            } else {
                Some(generate_default_config(&default_config_dir()))
            }
        }
    };

    // Load config file (CLI flags override config values)
    let cfg = match &resolved_config_path {
        Some(path) => match config::SwactorNodeConfig::load(path) {
            Ok(c) => {
                eprintln!("Config: {}", path.display());
                c
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        },
        None => config::SwactorNodeConfig::default(),
    };

    // Layer: CLI > config > defaults
    // For args with default values, we check if the user explicitly provided
    // the CLI flag; if not, we fall back to config, then to the default.
    let transport = if args.transport != "iroh" {
        args.transport.clone()
    } else {
        cfg.transport.unwrap_or_else(|| args.transport.clone())
    };
    let dashboard_port = if args.dashboard_port != 9090 {
        args.dashboard_port
    } else {
        cfg.dashboard_port.unwrap_or(args.dashboard_port)
    };
    let identity_dir_str = if args.identity_dir != "./identity" {
        args.identity_dir.clone()
    } else {
        cfg.identity_dir.unwrap_or_else(|| args.identity_dir.clone())
    };
    let auth_dir_str = if args.auth_dir != "./auth" {
        args.auth_dir.clone()
    } else {
        cfg.auth_dir.unwrap_or_else(|| args.auth_dir.clone())
    };
    let storage_path = args.storage_path.clone().or(cfg.storage_path);
    let peers_file = args.peers_file.clone().or(cfg.peers_file);
    let seed_node_id = args.seed_node_id.clone().or(cfg.seed_node_id);
    #[cfg(feature = "tcp")]
    let seed = args.seed.clone().or(cfg.seed);
    let auth_enabled = args.auth || cfg.auth.unwrap_or(false);
    let actors = if args.actors != 0 {
        args.actors
    } else {
        cfg.actors.unwrap_or(args.actors)
    };
    let chunk_size = if args.chunk_size != 1048576 {
        args.chunk_size
    } else {
        cfg.chunk_size.unwrap_or(args.chunk_size)
    };
    let gc_interval = if args.gc_interval != 1000 {
        args.gc_interval
    } else {
        cfg.gc_interval.unwrap_or(args.gc_interval)
    };
    let disseminate_interval = if args.disseminate_interval != 50 {
        args.disseminate_interval
    } else {
        cfg.disseminate_interval.unwrap_or(args.disseminate_interval)
    };
    let no_datastore = args.no_datastore || cfg.no_datastore.unwrap_or(false);
    let relay_enabled = !args.no_relay && cfg.relay.unwrap_or(true);
    let relay_port = if args.relay_port != 3340 {
        args.relay_port
    } else {
        cfg.relay_port.unwrap_or(args.relay_port)
    };
    let relay_bind = if args.relay_bind != "0.0.0.0" {
        args.relay_bind.clone()
    } else {
        cfg.relay_bind.unwrap_or_else(|| args.relay_bind.clone())
    };
    let relay_hosts = cfg.relay_hosts.unwrap_or_default();
    #[cfg(feature = "tcp")]
    let listen = args.listen.or_else(|| {
        cfg.listen.as_ref().and_then(|s| s.parse().ok())
    });

    // Signal handler
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set signal handler");
    }

    // Load or generate persistent identity
    let identity_dir = std::path::PathBuf::from(&identity_dir_str);
    std::fs::create_dir_all(&identity_dir).expect("failed to create identity directory");
    let key_path = identity_dir.join("node.key.json");
    let keypair = load_or_generate_keypair(&key_path);
    let node_id = keypair.node_id();
    let node_hex = hex_encode(&node_id.0);
    let invite_code = base58_encode(&node_id.0);

    eprintln!("Node ID: {} ({})", invite_code, &node_hex[..8]);
    eprintln!("Identity: {}", key_path.display());

    // Handle subcommands that need identity but not the full runtime
    match &args.command {
        Some(Subcmd::Name { name }) => {
            let config_path = resolved_config_path
                .as_ref()
                .expect("config path must be resolved");
            match name {
                Some(new_name) => {
                    persist_node_name(config_path, new_name);
                    eprintln!("Node name set to: {new_name}");
                }
                None => {
                    let current = cfg
                        .node_name
                        .clone()
                        .unwrap_or_else(|| names::generate_name(&keypair));
                    println!("{current}");
                }
            }
            return;
        }
        Some(Subcmd::Invite) => {
            println!("{invite_code}");
            return;
        }
        Some(Subcmd::Join { code }) => {
            let peer_bytes = base58_decode(code).unwrap_or_else(|| {
                eprintln!("Invalid invite code (expected base58-encoded 32-byte key)");
                std::process::exit(1);
            });
            let peer_node_id = NodeId(peer_bytes);
            let peer_hex = hex_encode(&peer_bytes);

            // Load peers file and add the peer
            let peers_path = peers_file
                .as_ref()
                .cloned()
                .unwrap_or_else(|| {
                    default_config_dir().join("peers.json").to_string_lossy().into_owned()
                });
            let p = std::path::Path::new(&peers_path);
            let mut peer_auth = PeerAllowList::from_file(p).unwrap_or_else(|e| {
                eprintln!("Failed to load peers file {peers_path}: {e}");
                std::process::exit(1);
            });
            peer_auth.add_peer(peer_node_id, code.clone());
            peer_auth.save().unwrap_or_else(|e| {
                eprintln!("Failed to save peers file: {e}");
                std::process::exit(1);
            });

            // Persist seed_node_id into config so the next startup auto-joins
            if let Some(config_path) = &resolved_config_path {
                persist_config_key(config_path, "seed_node_id", &peer_hex);
            }

            eprintln!("Peer added: {} ({})", code, &peer_hex[..8]);
            eprintln!("Seed node ID set — will auto-join on next startup.");
            return;
        }
        _ => {}
    }

    // Resolve node name: CLI > config > generate from keypair
    let node_name = args
        .node_name
        .clone()
        .or(cfg.node_name.clone())
        .unwrap_or_else(|| names::generate_name(&keypair));

    // Persist to config if not already set
    if cfg.node_name.is_none() {
        if let Some(config_path) = &resolved_config_path {
            persist_node_name(config_path, &node_name);
        }
    }

    eprintln!("Node name: {node_name}");

    // Load peer allow-list
    let peer_auth = match &peers_file {
        Some(path) => {
            let p = std::path::Path::new(path);
            match PeerAllowList::from_file(p) {
                Ok(auth) => {
                    eprintln!(
                        "Peer auth: allow-list ({} peers from {})",
                        auth.list_peers().len(),
                        path
                    );
                    auth
                }
                Err(e) => {
                    eprintln!("Failed to load peers file {path}: {e}");
                    std::process::exit(1);
                }
            }
        }
        None => {
            eprintln!("Peer auth: open (no allow-list)");
            PeerAllowList::open()
        }
    };
    let peer_auth = Arc::new(Mutex::new(peer_auth));

    // Start dashboard
    let dash = start_dashboard(DashboardConfig {
        port: dashboard_port,
        ..Default::default()
    });
    dash.install_tracing();
    dash.set_peer_auth(Arc::clone(&peer_auth));

    // Create actor runtime
    let num_threads = 2;
    let collector = StatsCollector::new(num_threads);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    })
    .with_extension(Arc::new(swactor_std::StdExtension::new()));
    rt.set_stats_hook(collector.clone());

    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(handle.runtime.clone(), collector);

    // Datastore setup
    let ds_group = if !no_datastore {
        let group = DatastoreGroup::spawn(
            handle.runtime.clone(),
            DatastoreGroupConfig {
                node_id,
                node_id_hex: node_hex.clone(),
                chunk_size,
                storage_path: storage_path.clone(),
                auth: if auth_enabled {
                    Some(DatastoreAuthConfig {
                        auth_dir: auth_dir_str.into(),
                    })
                } else {
                    None
                },
                gc_interval,
                disseminate_interval,
            },
        )
        .expect("failed to start datastore");
        dash.set_datastore(group.bridge().clone());
        Some(group)
    } else {
        eprintln!("Datastore: disabled");
        None
    };

    // Always wire up the factory so the UI can start/stop datastore
    let factory = DatastoreNodeFactory::new(Arc::clone(&handle.runtime), chunk_size);
    dash.set_datastore_factory(Arc::new(factory));

    // Distribution config
    let swim_config = SwimConfig {
        probe_interval: 5,
        probe_timeout: 6,       // 600ms — allows relay round-trip
        indirect_probes: 2,
        suspicion_timeout: 40,   // 4s — gives refutation time to piggyback
        dead_reprobe_interval: 50,
    };
    let node_config = DistributedNodeConfig {
        swim: swim_config,
        cache_capacity: 1000,
        republish_interval: 500,
        ..Default::default()
    };

    // Channel for triggering SWIM joins (fed by dashboard "Add Peer")
    let (join_tx, join_rx) = std::sync::mpsc::channel::<dashboard::JoinPeerInfo>();
    dash.set_join_sender(join_tx);

    match transport.as_str() {
        #[cfg(feature = "iroh")]
        "iroh" => run_iroh(
            seed_node_id,
            dashboard_port,
            actors,
            node_config,
            keypair,
            Arc::clone(&peer_auth),
            &handle,
            &dash,
            &stop,
            &ds_group,
            node_name,
            invite_code,
            join_rx,
            relay_enabled,
            &relay_bind,
            relay_port,
            relay_hosts,
        ),
        #[cfg(feature = "tcp")]
        "tcp" => run_tcp(
            listen,
            seed,
            dashboard_port,
            actors,
            node_config,
            keypair,
            Arc::clone(&peer_auth),
            &handle,
            &dash,
            &stop,
            &ds_group,
            node_name,
            invite_code,
            join_rx,
        ),
        other => {
            eprintln!("Unknown or unavailable transport: {other}");
            eprintln!("Available transports:");
            #[cfg(feature = "iroh")]
            eprintln!("  iroh");
            #[cfg(feature = "tcp")]
            eprintln!("  tcp");
            std::process::exit(1);
        }
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    dash.shutdown();
    handle.join();
}

// ── TCP transport ────────────────────────────────────────────────────────

#[cfg(feature = "tcp")]
fn run_tcp(
    listen: Option<std::net::SocketAddr>,
    seed: Option<String>,
    dashboard_port: u16,
    actors: usize,
    node_config: DistributedNodeConfig,
    keypair: Keypair,
    peer_auth: Arc<Mutex<PeerAllowList>>,
    handle: &swactor::runtime::RuntimeHandle,
    dash: &dashboard::DashboardHandle,
    stop: &Arc<AtomicBool>,
    ds_group: &Option<DatastoreGroup>,
    node_name: String,
    invite_code: String,
    _join_rx: std::sync::mpsc::Receiver<dashboard::JoinPeerInfo>,
) {
    use distribution::driver::NodeDriver;

    let listen_addr = listen.expect("--listen is required for TCP mode");
    let mut driver = NodeDriver::with_keypair(listen_addr, keypair, node_config)
        .expect("failed to create node driver");

    eprintln!(
        "Node {} listening on {} (TCP)",
        hex(&driver.node_id().0[..4]),
        driver.listen_addr(),
    );

    // Join seed if provided
    if let Some(seed) = seed {
        let seed_addr: std::net::SocketAddr = seed.parse().expect("invalid seed address");
        eprintln!("Joining cluster via seed {seed_addr}");
        driver.join(&[seed_addr]);
    }

    // Spawn and register actors
    let actor_addrs = spawn_actors(actors, handle, driver.node_mut());

    // Wire distribution snapshot to dashboard
    let mut snap = driver.snapshot();
    snap.node_name = Some(node_name.clone());
    snap.invite_code = Some(invite_code.clone());
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(snap)));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    // Start dashboard HTTP on a standalone tokio runtime (no iroh runtime in TCP mode)
    dash.start_http_standalone();
    eprintln!("Dashboard at http://0.0.0.0:{dashboard_port}");

    // Main loop
    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        round += 1;

        driver.recv_with_auth(&peer_auth);
        driver.tick();

        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        let mut snap = driver.snapshot();
        snap.node_name = Some(node_name.clone());
        snap.invite_code = Some(invite_code.clone());
        *cached_snapshot.lock().unwrap() = Some(snap);

        // Datastore ticks
        if let Some(group) = ds_group {
            group.tick(round);
        }

        thread::sleep(Duration::from_millis(100));
    }
}

// ── iroh transport ───────────────────────────────────────────────────────

#[cfg(feature = "iroh")]
fn run_iroh(
    seed_node_id: Option<String>,
    dashboard_port: u16,
    actors: usize,
    node_config: DistributedNodeConfig,
    keypair: Keypair,
    peer_auth: Arc<Mutex<PeerAllowList>>,
    handle: &swactor::runtime::RuntimeHandle,
    dash: &dashboard::DashboardHandle,
    stop: &Arc<AtomicBool>,
    ds_group: &Option<DatastoreGroup>,
    node_name: String,
    invite_code: String,
    join_rx: std::sync::mpsc::Receiver<dashboard::JoinPeerInfo>,
    relay_enabled: bool,
    relay_bind: &str,
    relay_port: u16,
    relay_hosts: Vec<String>,
) {
    use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
    use iroh::{RelayMode, SecretKey};
    use swactor_std::RuntimeNaming;

    // Evaluate relay candidacy and determine embedded relay bind address
    #[cfg(feature = "relay")]
    let (embedded_relay_bind, relay_public_ip) = if relay_enabled {
        let bind_addr: std::net::SocketAddr = format!("{relay_bind}:{relay_port}")
            .parse()
            .unwrap_or_else(|e| {
                eprintln!("Invalid relay bind address: {e}");
                std::process::exit(1);
            });

        let rules: Vec<Box<dyn relay::CandidacyRule>> = vec![
            Box::new(relay::PublicIpRule),
            Box::new(relay::PortBindRule::new(bind_addr)),
        ];
        match relay::evaluate_candidacy(&rules) {
            Ok(()) => {
                let public_ip = relay::PublicIpRule::outbound_ip();
                if let Some(ip) = &public_ip {
                    eprintln!("Relay: candidacy passed, will start on {bind_addr} (public IP: {ip})");
                } else {
                    eprintln!("Relay: candidacy passed, will start on {bind_addr} (no public IP detected)");
                }
                (Some(bind_addr), public_ip)
            }
            Err(reason) => {
                eprintln!("Relay: not eligible — {reason}");
                (None, None)
            }
        }
    } else {
        eprintln!("Relay: disabled");
        (None, None)
    };

    #[cfg(not(feature = "relay"))]
    let _ = (relay_enabled, relay_bind, relay_port);

    // Compute relay mode from known relay hosts (if any)
    let relay_mode = {
        let urls: Vec<iroh::RelayUrl> = relay_hosts.iter()
            .filter_map(|host| {
                let url_str = format!("http://{host}:{relay_port}/");
                match url_str.parse::<iroh::RelayUrl>() {
                    Ok(u) => Some(u),
                    Err(e) => { eprintln!("Relay: bad host {host}: {e}"); None }
                }
            })
            .collect();
        if urls.is_empty() {
            RelayMode::Disabled
        } else {
            eprintln!("Relay: using {} known relay(s)", urls.len());
            RelayMode::Custom(urls.into_iter().collect::<iroh::RelayMap>())
        }
    };

    let iroh_config = IrohDriverConfig {
        secret_key: Some(SecretKey::from_bytes(&keypair.secret_bytes())),
        relay_mode,
        node: node_config,
        peer_auth: Some(peer_auth.clone()),
        additional_alpns: vec![swactor_streams::ALPN.to_vec()],
        #[cfg(feature = "relay")]
        embedded_relay_bind,
        #[cfg(feature = "relay")]
        relay_public_ip,
    };
    let mut driver = IrohDriver::new(iroh_config).expect("failed to create iroh driver");

    // Spawn StreamManager actor
    let stream_mgr = swactor_streams::StreamManager::new(
        driver.endpoint().clone(),
        driver.tokio_handle(),
        Arc::clone(&handle.runtime),
    );
    let stream_mgr_addr = handle
        .runtime
        .spawn(stream_mgr)
        .expect("spawn StreamManager");
    handle
        .runtime
        .register_name(swactor_streams::STREAM_MANAGER_NAME, stream_mgr_addr)
        .expect("register StreamManager");

    // Wire streams into the datastore
    if let Some(group) = ds_group {
        group.configure_streams(stream_mgr_addr, driver.tokio_handle());
    }

    eprintln!("Node {} started (iroh)", hex(&driver.node_id().0[..4]));

    // Join seed if provided (accepts hex or base58)
    if let Some(seed_str) = seed_node_id {
        let seed_bytes = parse_node_id_str(&seed_str).expect("invalid seed node ID (expected hex or base58)");
        let seed_key =
            iroh::PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");
        let mut seed_addr = iroh::EndpointAddr::from(seed_key);
        // Include relay URLs so iroh can locate the seed through the relay
        for host in &relay_hosts {
            let url_str = format!("http://{host}:{relay_port}/");
            if let Ok(url) = url_str.parse::<iroh::RelayUrl>() {
                seed_addr = seed_addr.with_relay_url(url);
            }
        }
        eprintln!("Joining cluster via seed {}", base58_encode(&seed_bytes));
        driver.join(&[seed_addr]);
    }

    // Spawn and register actors
    let actor_addrs = spawn_actors(actors, handle, driver.node_mut());

    // Announce relay URL to cluster gossip
    let mut home_relay_set = if let Some(url) = driver.relay_url().map(|u| u.to_string()) {
        driver.node_mut().set_relay_url(Some(url));
        true
    } else {
        false
    };

    // Wire distribution snapshot to dashboard
    let mut snap = driver.snapshot();
    snap.node_name = Some(node_name.clone());
    snap.invite_code = Some(invite_code.clone());
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(snap)));
    let provider = SnapshotProvider {
        snapshot: Arc::clone(&cached_snapshot),
    };
    dash.set_distribution(Arc::new(provider));

    // Start dashboard HTTP on IrohDriver's tokio runtime
    dash.start_http(driver.tokio_handle());
    eprintln!("Dashboard at http://0.0.0.0:{dashboard_port}");

    // Main loop
    let mut round: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        round += 1;

        driver.recv();
        driver.tick();

        // Forward incoming stream connections to StreamManager
        for (node_id, conn) in driver.drain_other_connections() {
            let rt_clone = Arc::clone(&handle.runtime);
            let mgr_addr = stream_mgr_addr;
            let node_bytes = node_id.0;
            driver.tokio_handle().spawn(async move {
                match swactor_streams::accept::handle_incoming(
                    node_bytes, conn, &rt_clone, mgr_addr,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("stream accept: failed to handle incoming: {e}");
                    }
                }
            });
        }

        // Drain discovered peers (dashboard "Add Peer") and auto-join them
        {
            let mut new_peers = Vec::new();
            while let Ok(info) = join_rx.try_recv() {
                new_peers.push(info);
            }
            if !new_peers.is_empty() {
                let own_id = driver.node_id().0;
                let addrs: Vec<iroh::EndpointAddr> = new_peers
                    .iter()
                    .filter(|(bytes, _)| *bytes != own_id)
                    .filter_map(|(bytes, relay_url)| {
                        iroh::PublicKey::from_bytes(bytes).ok().map(|k| {
                            let mut addr = iroh::EndpointAddr::from(k);
                            if let Some(url_str) = relay_url {
                                match url_str.parse::<iroh::RelayUrl>() {
                                    Ok(url) => {
                                        eprintln!("Auto-joining peer {} via relay {}", base58_encode(bytes), url);
                                        addr = addr.with_relay_url(url);
                                    }
                                    Err(e) => {
                                        eprintln!("Auto-joining peer {} (bad relay URL {}: {e})", base58_encode(bytes), url_str);
                                    }
                                }
                            } else {
                                eprintln!("Auto-joining peer {} (no relay URL)", base58_encode(bytes));
                            }
                            addr
                        })
                    })
                    .collect();
                if !addrs.is_empty() {
                    driver.join(&addrs);
                }
            }
        }

        // Lazily pick up home relay URL once the endpoint connects
        if !home_relay_set {
            if let Some(url) = driver.home_relay_url() {
                eprintln!("Relay URL (home): {url}");
                driver.node_mut().set_relay_url(Some(url.to_string()));
                home_relay_set = true;
            }
        }

        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        let mut snap = driver.snapshot();
        snap.node_name = Some(node_name.clone());
        snap.invite_code = Some(invite_code.clone());
        *cached_snapshot.lock().unwrap() = Some(snap);

        // Datastore ticks
        if let Some(group) = ds_group {
            group.tick(round);
        }

        thread::sleep(Duration::from_millis(100));
    }

    // Driver shutdown handles embedded relay cleanup automatically
    driver.shutdown();
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn spawn_actors(
    count: usize,
    handle: &swactor::runtime::RuntimeHandle,
    node: &mut distribution::node::DistributedNode,
) -> Vec<swactor::actor::ActorAddress> {
    let mut addrs = Vec::new();
    for _ in 0..count {
        match handle.runtime.spawn(HeartbeatActor) {
            Ok(addr) => {
                node.register_actor(addr, 1);
                addrs.push(addr);
            }
            Err(e) => eprintln!("failed to spawn actor: {e}"),
        }
    }
    if !addrs.is_empty() {
        eprintln!("Registered {} actors", addrs.len());
    }
    addrs
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Persist `node_name` into an existing TOML config file.
///
/// If the file already contains a `node_name` line it is replaced in-place;
/// otherwise the key is appended.
fn persist_node_name(path: &std::path::Path, name: &str) {
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    let new_line = format!("node_name = \"{}\"", name);

    let updated = if contents.contains("node_name") {
        // Replace existing line
        contents
            .lines()
            .map(|line| {
                if line.trim_start().starts_with("node_name") {
                    new_line.as_str()
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    } else {
        // Append
        let mut s = contents;
        if !s.ends_with('\n') && !s.is_empty() {
            s.push('\n');
        }
        s.push_str(&new_line);
        s.push('\n');
        s
    };

    if let Err(e) = std::fs::write(path, &updated) {
        eprintln!("Warning: could not persist node_name to {}: {e}", path.display());
    }
}

/// Persist a key-value pair into an existing TOML config file.
fn persist_config_key(path: &std::path::Path, key: &str, value: &str) {
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    let new_line = format!("{key} = \"{value}\"");

    let updated = if contents.contains(key) {
        contents
            .lines()
            .map(|line| {
                if line.trim_start().starts_with(key) {
                    new_line.as_str()
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    } else {
        let mut s = contents;
        if !s.ends_with('\n') && !s.is_empty() {
            s.push('\n');
        }
        s.push_str(&new_line);
        s.push('\n');
        s
    };

    if let Err(e) = std::fs::write(path, &updated) {
        eprintln!("Warning: could not persist {key} to {}: {e}", path.display());
    }
}

/// Parse a node ID from either hex (64 chars) or base58 (~44 chars).
fn parse_node_id_str(s: &str) -> Option<[u8; 32]> {
    if s.len() == 64 {
        // Hex
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(bytes)
    } else {
        // Try base58
        base58_decode(s)
    }
}
