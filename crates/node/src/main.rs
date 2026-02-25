//! Swactor node — unified distributed node with dashboard and datastore.
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
use swactor::transport::NodeId;
use swactor_transport::crypto::Keypair;
use swactor_transport::identity::{base58_encode, base58_decode, hex_encode, load_or_generate_keypair};
use distribution::node::DistributedNodeConfig;
use distribution::peer_auth::PeerAllowList;
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::probe::SwimConfig;

use dashboard::collector::StatsCollector;
use dashboard::{start_dashboard, DashboardConfig};

use swactor_datastore::{DatastoreAuthConfig, DatastoreGroup, DatastoreGroupConfig};

use swactor_node::{config, install, names, plugins};
#[cfg(feature = "relay")]
use swactor_node::relay;

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

    /// Seed node's iroh public key (hex-encoded 32-byte key)
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

    // Auto-detect public IP for relay_hosts
    let relay_hosts_line = detect_public_ip_for_config();

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
{relay_hosts}"#,
        dir = config_dir.display(),
        relay_hosts = relay_hosts_line,
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

/// Detect outbound IP; if public, return a `relay_hosts = ["<ip>"]` TOML line.
fn detect_public_ip_for_config() -> String {
    let public_ip = (|| -> Option<std::net::IpAddr> {
        let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
        sock.connect("192.0.2.1:80").ok()?; // RFC 5737 TEST-NET-1
        let ip = sock.local_addr().ok()?.ip();
        match ip {
            std::net::IpAddr::V4(v4) => {
                if !v4.is_loopback() && !v4.is_private()
                    && !v4.is_link_local() && !v4.is_unspecified()
                {
                    Some(ip)
                } else {
                    None
                }
            }
            std::net::IpAddr::V6(v6) => {
                if !v6.is_loopback() && !v6.is_unspecified() {
                    Some(ip)
                } else {
                    None
                }
            }
        }
    })();

    match public_ip {
        Some(ip) => {
            eprintln!("Detected public IP {ip} — adding to relay_hosts");
            format!("relay_hosts = [\"{ip}\"]")
        }
        None => String::new(),
    }
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
    // Signal handler — second Ctrl+C forces immediate exit
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            if stop.load(Ordering::Relaxed) {
                eprintln!("\nForce exit.");
                std::process::exit(1);
            }
            eprintln!("\nShutting down (press Ctrl+C again to force)...");
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
            let rich = if let Some(host) = relay_hosts.first() {
                format!("{invite_code}@http://{host}:{relay_port}/")
            } else {
                invite_code.clone()
            };
            println!("{rich}");
            return;
        }
        Some(Subcmd::Join { code }) => {
            // Parse rich invite code: <base58>#<addrs>@<relay>
            let (node_id_str, _direct_addrs_str, relay_str) = parse_rich_invite(code);
            let peer_bytes = base58_decode(&node_id_str).unwrap_or_else(|| {
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

            // Persist seed_node_id and relay host into config
            if let Some(config_path) = &resolved_config_path {
                persist_config_key(config_path, "seed_node_id", &peer_hex);

                // Extract relay host from invite URL and save to config
                if let Some(ref relay_url) = relay_str {
                    if let Some(host) = extract_relay_host(relay_url) {
                        persist_config_array_key(config_path, "relay_hosts", &[&host]);
                        eprintln!("Relay host saved: {host}");
                    }
                }
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
    if cfg.node_name.is_none()
        && let Some(config_path) = &resolved_config_path {
            persist_node_name(config_path, &node_name);
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

    // Create actor runtime
    let num_threads = 2;
    let collector = StatsCollector::new(num_threads);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    })
    .with_extension(Arc::new(swactor::std::StdExtension::new()));
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
        // Register datastore plugin
        let ds_plugin = plugins::datastore::DatastorePlugin::from_group(
            &group,
            Arc::clone(&handle.runtime),
            chunk_size,
        );
        dash.register_plugin(Arc::new(ds_plugin));
        Some(group)
    } else {
        eprintln!("Datastore: disabled");
        // Register stopped datastore plugin (allows starting from dashboard)
        let ds_plugin = plugins::datastore::DatastorePlugin::stopped(
            Arc::clone(&handle.runtime),
            chunk_size,
        );
        dash.register_plugin(Arc::new(ds_plugin));
        None
    };

    // Distribution config
    eprintln!("Distribution: SWIM (transport: iroh, mode: reactive)");
    let swim_config = SwimConfig {
        probe_interval: 10,       // unused in Reactive mode, kept for compat
        probe_timeout: 15,        // 1.5s — generous for relay roundtrips
        indirect_probes: 2,
        suspicion_timeout: 80,    // 8s — gives refutation time to gossip back
        dead_reprobe_interval: 100,
        probe_mode: distribution::swim::probe::ProbeMode::Reactive {
            safety_sweep_interval: 3000, // 5 minutes at 100ms/tick
        },
    };
    let node_config = DistributedNodeConfig {
        swim: swim_config,
        cache_capacity: 1000,
        republish_interval: 500,
        ..Default::default()
    };

    // Channel for triggering SWIM joins (fed by peers plugin "Add Peer" and distribution "Re-peer")
    let (join_tx, join_rx) = std::sync::mpsc::channel::<dashboard::JoinPeerInfo>();
    let join_tx_dist = join_tx.clone();

    // Register peers plugin
    let peers_plugin = plugins::peers::PeersPlugin::new(
        Arc::clone(&peer_auth),
        Some(join_tx),
    );
    dash.register_plugin(Arc::new(peers_plugin));

    #[cfg(feature = "iroh")]
    run_iroh(
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
        join_tx_dist,
        relay_enabled,
        &relay_bind,
        relay_port,
        relay_hosts,
    );

    #[cfg(not(feature = "iroh"))]
    {
        eprintln!("iroh feature is required but not enabled");
        std::process::exit(1);
    }

    handle.shutdown();
    dash.shutdown();
    handle.join();
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
    join_tx_dist: std::sync::mpsc::Sender<dashboard::JoinPeerInfo>,
    relay_enabled: bool,
    relay_bind: &str,
    relay_port: u16,
    relay_hosts: Vec<String>,
) {
    use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
    use iroh::{RelayMode, SecretKey};
    use swactor::std::RuntimeNaming;

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
            if relay_enabled {
                eprintln!("Relay: no hosts configured, using iroh default relays");
                RelayMode::Default
            } else {
                RelayMode::Disabled
            }
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
        additional_alpns: vec![swactor_datastore::streams::ALPN.to_vec()],
        #[cfg(feature = "relay")]
        embedded_relay_bind,
        #[cfg(feature = "relay")]
        relay_public_ip,
    };
    let mut driver = IrohDriver::new(iroh_config).expect("failed to create iroh driver");

    // Spawn StreamManager actor
    let stream_mgr = swactor_datastore::streams::StreamManager::new(
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
        .register_name(swactor_datastore::streams::STREAM_MANAGER_NAME, stream_mgr_addr)
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

    // Announce node name and relay URL to cluster gossip
    driver.node_mut().set_node_name(node_name.clone());
    let mut home_relay_set = if let Some(url) = driver.relay_url().map(|u| u.to_string()) {
        driver.node_mut().set_relay_url(Some(url));
        true
    } else {
        false
    };

    // Wire distribution snapshot to dashboard via plugin
    let mut snap = driver.snapshot();
    snap.node_name = Some(node_name.clone());
    snap.invite_code = Some(invite_code.clone());
    snap.version = Some(VERSION.to_string());
    let cached_snapshot: Arc<Mutex<Option<DistributionNodeSnapshot>>> =
        Arc::new(Mutex::new(Some(snap)));
    let dist_plugin = plugins::distribution::DistributionPlugin::new(
        Arc::clone(&cached_snapshot),
        Some(join_tx_dist),
    );
    let dismissed_statuses = dist_plugin.dismissed_statuses();
    dash.register_plugin(Arc::new(dist_plugin));

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
                match swactor_datastore::streams::accept::handle_incoming(
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
            let mut new_peers: Vec<dashboard::JoinPeerInfo> = Vec::new();
            while let Ok(info) = join_rx.try_recv() {
                new_peers.push(info);
            }
            if !new_peers.is_empty() {
                let own_id = driver.node_id().0;
                let addrs: Vec<iroh::EndpointAddr> = new_peers
                    .iter()
                    .filter(|info| info.node_id != own_id)
                    .filter_map(|info| {
                        iroh::PublicKey::from_bytes(&info.node_id).ok().map(|k| {
                            let mut addr = iroh::EndpointAddr::from(k);
                            if let Some(url_str) = &info.relay_url {
                                match url_str.parse::<iroh::RelayUrl>() {
                                    Ok(url) => {
                                        eprintln!("Auto-joining peer {} via relay {}", base58_encode(&info.node_id), url);
                                        addr = addr.with_relay_url(url);
                                    }
                                    Err(e) => {
                                        eprintln!("Auto-joining peer {} (bad relay URL {}: {e})", base58_encode(&info.node_id), url_str);
                                    }
                                }
                            } else {
                                eprintln!("Auto-joining peer {} (no relay URL)", base58_encode(&info.node_id));
                            }
                            for sa in &info.direct_addrs {
                                addr = addr.with_ip_addr(*sa);
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
        if !home_relay_set
            && let Some(url) = driver.home_relay_url() {
                eprintln!("Relay URL (home): {url}");
                driver.node_mut().set_relay_url(Some(url.to_string()));
                home_relay_set = true;
            }

        for addr in &actor_addrs {
            let _ = handle.runtime.send_to(*addr, Heartbeat);
        }

        let mut snap = driver.snapshot();
        snap.node_name = Some(node_name.clone());

        // Build rich invite code: <base58>#<addr1>,<addr2>@<relay_url>
        {
            let direct_addrs = driver.direct_addresses();
            let addrs_part = if direct_addrs.is_empty() {
                String::new()
            } else {
                let addrs_str: Vec<String> = direct_addrs.iter().map(|a| a.to_string()).collect();
                format!("#{}", addrs_str.join(","))
            };
            let relay_part = match &snap.relay_url {
                Some(relay) => format!("@{}", relay),
                None => String::new(),
            };
            snap.invite_code = Some(format!("{}{}{}", invite_code, addrs_part, relay_part));
        }

        // Drain dismissed join statuses from the dashboard
        {
            let mut dismissed = dismissed_statuses.lock().unwrap();
            for bytes in dismissed.drain(..) {
                driver.clear_join_status(&swactor::transport::NodeId(bytes));
            }
        }

        // Populate join statuses, auto-clearing alive peers
        {
            use distribution::iroh_driver::JoinPhase;
            use distribution::snapshot::JoinStatusInfo;

            let statuses = driver.join_statuses();
            let alive_node_ids: Vec<swactor::transport::NodeId> = snap.members.iter()
                .filter(|m| m.state == "alive")
                .filter_map(|m| {
                    let mut bytes = [0u8; 32];
                    if m.node_id.len() == 64 {
                        for i in 0..32 {
                            bytes[i] = u8::from_str_radix(&m.node_id[i*2..i*2+2], 16).unwrap_or(0);
                        }
                        Some(swactor::transport::NodeId(bytes))
                    } else {
                        None
                    }
                })
                .collect();

            // Clear statuses for alive peers
            if !alive_node_ids.is_empty() {
                driver.clear_join_statuses(&alive_node_ids);
            }

            // Convert remaining statuses to snapshot format
            snap.join_statuses = statuses.iter()
                .filter(|(nid, _)| !alive_node_ids.contains(nid))
                .map(|(nid, status)| {
                    let node_id_hex: String = nid.0.iter().map(|b| format!("{:02x}", b)).collect();
                    let (phase_str, detail) = match &status.phase {
                        JoinPhase::Connecting { attempt, max_attempts } =>
                            ("connecting".into(), Some(format!("{}/{}", attempt, max_attempts))),
                        JoinPhase::Sending { attempt, max_attempts } =>
                            ("sending".into(), Some(format!("{}/{}", attempt, max_attempts))),
                        JoinPhase::Sent => ("sent".into(), None),
                        JoinPhase::Failed { error } => ("failed".into(), Some(error.clone())),
                    };
                    JoinStatusInfo {
                        node_id: node_id_hex,
                        phase: phase_str,
                        detail,
                        has_relay: status.has_relay,
                        has_direct: status.has_direct,
                        direct_addr_count: status.direct_addr_count,
                    }
                })
                .collect();
        }

        snap.version = Some(VERSION.to_string());
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

/// Extract hostname from a relay URL like `http://167.71.x.x:3340/`.
fn extract_relay_host(url: &str) -> Option<String> {
    let stripped = url.strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    let host_port = stripped.trim_end_matches('/');
    // Handle bracket-enclosed IPv6: [::1]:3340
    if host_port.starts_with('[') {
        let end = host_port.find(']')?;
        Some(host_port[1..end].to_string())
    } else {
        let host = match host_port.rfind(':') {
            Some(idx) => &host_port[..idx],
            None => host_port,
        };
        if host.is_empty() { None } else { Some(host.to_string()) }
    }
}

/// Persist a TOML array key into an existing config file.
fn persist_config_array_key(path: &std::path::Path, key: &str, values: &[&str]) {
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    let array_str = values
        .iter()
        .map(|v| format!("\"{v}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let new_line = format!("{key} = [{array_str}]");

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

/// Parse a rich invite code: `<base58>#<addr1>,<addr2>@<relay_url>`
///
/// Returns (node_id_str, optional_direct_addrs_csv, optional_relay_url).
fn parse_rich_invite(raw: &str) -> (String, Option<String>, Option<String>) {
    // Split on last '@' for relay
    let (left, relay) = match raw.rfind('@') {
        Some(idx) => (&raw[..idx], Some(raw[idx + 1..].to_string())),
        None => (raw, None),
    };
    // Split on '#' for direct addrs
    let (node_id, addrs) = match left.find('#') {
        Some(idx) => (&left[..idx], Some(left[idx + 1..].to_string())),
        None => (left, None),
    };
    (node_id.to_string(), addrs, relay)
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
