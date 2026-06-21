//! Swactor node — unified distributed node with dashboard and datastore.
//!
//! Combines distribution, runtime dashboard, and content-addressed datastore
//! into a single batteries-included binary. Datastore is on by default
//! (in-memory) and can be disabled via `--no-datastore`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use swactor::actor::{ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor::stats::StatsHook;
use swactor_transport::NodeId;
use swactor_transport::crypto::Keypair;
use swactor_transport::identity::{base58_encode, base58_decode, hex_encode, load_or_generate_keypair};
use distribution::node::DistributedNodeConfig;
use distribution::peer_auth::PeerAllowList;
use distribution::swim::probe::SwimConfig;

use datastream::catalog::{
    self, ActorRec, ActorRuntimeDetail, DatastoreState, DistributionState, IdentityRecord,
    RuntimeStats as DsRuntimeStats,
};
use datastream::emit::{
    DatastreamEmitter, DatastreamEventSink, EmitterConfig, FrameSink, TickInput,
};
use datastream::frame::{Frame, StreamId};

use dashboard::collector::StatsCollector;
use dashboard::datastream_source::FleetView;
use dashboard::{start_dashboard, DashboardConfig};

use swactor_datastore::metrics::{DatastoreEventObserver, DatastoreMetrics};
use swactor_datastore::{DatastoreAuthConfig, DatastoreGroup, DatastoreGroupConfig};

use swactor_node::{config, install, names, plugins};
#[cfg(feature = "relay")]
use swactor_node::relay;

/// Extra swactor ticks run after `stop` so in-flight messages and stopping
/// actors drain before teardown.
const DRAIN_TICKS: usize = 100;

/// How often the unified driver loop rebuilds the dashboard snapshot. This is a
/// UI-refresh throttle (the snapshot read/serialize is comparatively expensive),
/// not a protocol cadence — SWIM and the actors advance every loop iteration.
const SNAPSHOT_REFRESH: Duration = Duration::from_millis(200);

/// tokio worker-thread count. One worker is permanently consumed by the
/// busy-spinning swactor driver loop (the mailbox has no waker, by design), so
/// reserve it and leave the rest for iroh I/O + the dashboard; floor at 2 so a
/// small box still makes progress.
fn worker_count(cpus: usize) -> usize {
    cpus.saturating_sub(1).max(2)
}

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

// ── Membership fanout ──────────────────────────────────────────────────────
// Adapts the SwimActor's `MembershipChanged` stream (its sole observable) into
// the registry/metadata actors' `Membership` control messages, and folds it into
// a mirror the dashboard snapshot reads. This is the seam by which the directory,
// registry, and metadata layers "live beside" SWIM yet react to its membership.
struct MembershipFanout {
    registry: swactor::actor::ActorAddress,
    metadata: swactor::actor::ActorAddress,
    directory: swactor::actor::ActorAddress,
    mirror: Arc<Mutex<distribution::swim::member_list::MemberList>>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = distribution::swim::actor::MembershipChanged;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, m: Self::Incoming) {
        self.mirror
            .lock()
            .unwrap()
            .apply(m.node_id, m.state, m.incarnation);
        let _ = ctx.send(
            self.registry,
            distribution::registry_actor::RegistryIn::Membership(m.clone()),
        );
        let _ = ctx.send(
            self.metadata,
            distribution::node_metadata_actor::MetadataIn::Membership(m.clone()),
        );
        let _ = ctx.send(
            self.directory,
            distribution::directory_actor::DirectoryIn::Membership(m),
        );
    }
}

// ── In-process datastream consumer (node-local dashboard) ───────────────────
// Single source: the node's emitter ships frames here. We fold them through the
// SAME `FleetView` the fleet dashboard uses and render the node-local dashboard
// from the result, so the node renders its own telemetry exactly as a remote
// observer would. (Cross-node export rides the cluster transport via a
// `ClusterFrameSink`, not a dedicated UDP channel.)
struct LocalRenderSink {
    view: FleetView,
    dist_cache: Arc<Mutex<Option<String>>>,
    datastore_cache: Arc<Mutex<Option<String>>>,
    fleet_cache: Arc<Mutex<Option<String>>>,
    stats_slot: Arc<Mutex<Option<swactor::stats::RuntimeStats>>>,
    /// Node-local overlays the datastream view does not carry directly: the
    /// interactive invite code / join statuses, and the node's authoritative
    /// SWIM membership (`{members, *_count}`) for its own single-node view.
    invite_code: Arc<Mutex<Option<String>>>,
    join_statuses: Arc<Mutex<Option<String>>>,
    members: Arc<Mutex<Option<String>>>,
}

impl FrameSink for LocalRenderSink {
    fn ship(&mut self, stream: &StreamId, frame: &Frame) {
        let update = self.view.ingest(stream, frame);
        for (is_warn, m) in update.logs {
            if is_warn {
                tracing::warn!(target: "datastream", "{m}");
            } else {
                tracing::info!(target: "datastream", "{m}");
            }
        }
        if let Some(stats) = update.stats {
            *self.stats_slot.lock().unwrap() = Some(stats);
        }
        *self.fleet_cache.lock().unwrap() = Some(update.fleet_json);
        if let Some(dist_json) = update.dist_json {
            *self.dist_cache.lock().unwrap() = Some(overlay_dist(
                dist_json,
                &self.invite_code,
                &self.join_statuses,
                &self.members,
            ));
        }
        if let Some(ds_json) = update.datastore_json {
            *self.datastore_cache.lock().unwrap() =
                Some(format!(r#"{{"is_running":true,"snapshot":{ds_json}}}"#));
        }
    }
}

/// Overlay node-local state onto the reconstructed distribution JSON before the
/// node-local distribution page serves it: the authoritative SWIM membership
/// (`{members, *_count}`), the interactive invite code, and the real-time join
/// statuses — all things the single-node datastream view cannot supply itself.
fn overlay_dist(
    dist_json: String,
    invite_code: &Arc<Mutex<Option<String>>>,
    join_statuses: &Arc<Mutex<Option<String>>>,
    members: &Arc<Mutex<Option<String>>>,
) -> String {
    let mut v: serde_json::Value = match serde_json::from_str(&dist_json) {
        Ok(v) => v,
        Err(_) => return dist_json,
    };
    if let Some(obj) = v.as_object_mut() {
        // Authoritative membership: merge `members` + the alive/suspect/dead counts.
        if let Some(m) = members.lock().unwrap().clone() {
            if let Ok(serde_json::Value::Object(mo)) = serde_json::from_str(&m) {
                for (k, val) in mo {
                    obj.insert(k, val);
                }
            }
        }
        if let Some(code) = invite_code.lock().unwrap().clone() {
            obj.insert("invite_code".into(), serde_json::Value::String(code));
        }
        if let Some(js) = join_statuses.lock().unwrap().clone() {
            if let Ok(arr) = serde_json::from_str::<serde_json::Value>(&js) {
                obj.insert("join_statuses".into(), arr);
            }
        }
    }
    v.to_string()
}

/// Bridges datastore operation events onto the node's datastream. The datastore
/// defines the observer trait (it owns the events); this node-side adapter holds
/// the emitter's thread-safe event sink and frames each op as it fires.
struct NodeDatastoreObserver {
    sink: DatastreamEventSink,
}

impl DatastoreEventObserver for NodeDatastoreObserver {
    fn on_event(
        &self,
        timestamp_ms: u64,
        kind: &str,
        hash: &str,
        name: Option<&str>,
        size_bytes: u64,
    ) {
        self.sink
            .datastore_event(timestamp_ms, kind, hash, name, size_bytes);
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

    // The node's one tokio pool and one swactor runtime. iroh I/O, the SWIM
    // state machine, dashboard HTTP, and the actor runtime all live on this one
    // pool; a single driver loop ticks the lone swactor runtime (Arc-shared).
    // No second engine, no per-core islands.
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_count(cpus))
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");
    let collector = StatsCollector::new(1);
    let mut swactor_rt = Runtime::new(RuntimeConfig {
        num_threads: 1,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    })
    .with_extension(Arc::new(swactor::std::StdExtension::new()));
    swactor_rt.set_stats_hook(collector.clone() as Arc<dyn StatsHook>);
    // Actor transport: install the actor codec registry (wire `type_tag` ⇄ actor
    // `Incoming` variant) and a transport router so the protocol actors can reach
    // remote peers with a plain `ctx.send` (→ iroh egress). Must be installed
    // before the runtime is shared behind an `Arc`.
    let actor_codec = Arc::new(distribution::messages::actor_codec_registry());
    let transport_router = Arc::new(swactor_transport::TransportRouter::new());
    swactor_rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
        Arc::clone(&actor_codec),
        Arc::clone(&transport_router),
    )));
    let rt: Arc<Runtime> = Arc::new(swactor_rt);
    // Single source: the node-local dashboard renders from the node's OWN
    // datastream frames. Runtime stats are pushed via `set_stats` from the
    // in-process frame consumer (built in `run_iroh`), not read from a live
    // runtime handle — so we do NOT call `dash.set_runtime`. The `collector`
    // stays as the per-actor stats *producer* feeding the `runtime.actors`
    // record, and is handed to `run_iroh`.

    // Datastream-reconstructed datastore telemetry JSON, shared between the
    // node's frame consumer (writer) and the datastore plugin (reader).
    let datastore_cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    // Datastore setup
    let ds_group = if !no_datastore {
        let group = DatastoreGroup::spawn(
            Arc::clone(&rt),
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
            Arc::clone(&rt),
            chunk_size,
            Arc::clone(&datastore_cache),
        );
        dash.register_plugin(Arc::new(ds_plugin));
        Some(group)
    } else {
        eprintln!("Datastore: disabled");
        // Register stopped datastore plugin (allows starting from dashboard)
        let ds_plugin = plugins::datastore::DatastorePlugin::stopped(
            Arc::clone(&rt),
            chunk_size,
            Arc::clone(&datastore_cache),
        );
        dash.register_plugin(Arc::new(ds_plugin));
        None
    };
    // The datastore's metrics accumulator (the single source the plugin's CRUD
    // records to) — cloned out before `ds_group` moves into `run_iroh`, so the
    // emitter can frame its readout and stream its op events.
    let ds_metrics = ds_group.as_ref().map(|g| Arc::clone(g.metrics()));

    // Distribution config
    eprintln!("Distribution: SWIM (transport: iroh, mode: reactive)");
    let swim_config = SwimConfig {
        probe_interval: Duration::from_secs(1), // unused in Reactive mode, kept for compat
        probe_timeout: Duration::from_millis(1500), // generous for relay roundtrips
        indirect_probes: 2,
        suspicion_timeout: Duration::from_secs(8), // gives refutation time to gossip back
        dead_reprobe_interval: Duration::from_secs(10),
        probe_mode: distribution::swim::probe::ProbeMode::Reactive {
            safety_sweep_interval: Duration::from_secs(300), // 5 minutes
        },
        lifeguard: None,
    };
    let node_config = DistributedNodeConfig {
        swim: swim_config,
        cache_capacity: 1000,
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
        &tokio,
        rt,
        &dash,
        &stop,
        ds_group,
        node_name,
        invite_code,
        join_rx,
        join_tx_dist,
        relay_enabled,
        &relay_bind,
        relay_port,
        relay_hosts,
        actor_codec,
        transport_router,
        collector,
        ds_metrics,
        datastore_cache,
    );

    #[cfg(not(feature = "iroh"))]
    {
        let _ = (collector, ds_metrics, datastore_cache);
        eprintln!("iroh feature is required but not enabled");
        std::process::exit(1);
    }

    tokio.shutdown_timeout(Duration::from_secs(5));
    dash.shutdown();
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
    tokio: &tokio::runtime::Runtime,
    rt: Arc<Runtime>,
    dash: &dashboard::DashboardHandle,
    stop: &Arc<AtomicBool>,
    ds_group: Option<DatastoreGroup>,
    node_name: String,
    invite_code: String,
    join_rx: std::sync::mpsc::Receiver<dashboard::JoinPeerInfo>,
    join_tx_dist: std::sync::mpsc::Sender<dashboard::JoinPeerInfo>,
    relay_enabled: bool,
    relay_bind: &str,
    relay_port: u16,
    relay_hosts: Vec<String>,
    actor_codec: Arc<swactor_transport::CodecRegistry>,
    transport_router: Arc<swactor_transport::TransportRouter>,
    collector: Arc<StatsCollector>,
    ds_metrics: Option<Arc<DatastoreMetrics>>,
    datastore_cache: Arc<Mutex<Option<String>>>,
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

    // Pull the protocol-actor configs out before `node_config` moves into the
    // (now transport-only) driver.
    let swim_config = node_config.swim.clone();
    let registry_config = node_config.registry.clone();
    let metadata_lambda = node_config.metadata_lambda;

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
    // Build the driver on the node's one tokio pool. Construction and the
    // synchronous setup below run on the main thread, so the driver's
    // constructor block_on is a legal sync→async bridge. The driver loop that
    // follows is async and uses only the pure-sync recv()/tick().
    let mut driver = IrohDriver::with_handle(tokio.handle().clone(), iroh_config)
        .expect("failed to create iroh driver");

    // ── Actorized distribution protocol ──
    // SWIM membership, the cluster name registry, and node-metadata dissemination
    // now run as actors on the swactor runtime; the driver is reduced to a
    // transport bridge (decode inbound → actor mailbox; actor outbound → iroh).
    use distribution::directory_actor::{DirectoryActor, DirectoryIn};
    use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
    use distribution::registry_actor::{RegistryActor, RegistryIn, RegistryView};
    use distribution::swim::actor::{SwimActor, SwimIn};
    use distribution::transport_bridge::{
        IrohPeerDirectory, IrohRouteBinder, Outbox, RelayMirror, RouteView, RouteViewTransport,
    };

    let node_id = driver.node_id();
    let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
    let relay_mirror: RelayMirror =
        Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let route_view: RouteView =
        Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    // Read-mirror of the cluster registry the telemetry tick reads to fill the
    // `dist.state` registry fields (size / tombstones / entries).
    let registry_view: RegistryView = Arc::new(std::sync::RwLock::new(Default::default()));
    // Production SWIM observer: reconstructs probe RTT, recent probe targets, and
    // membership transitions (with cause) from the SWIM observation stream.
    let swim_telemetry = distribution::swim::telemetry::SwimTelemetry::new();
    let peer_directory = Arc::new(IrohPeerDirectory::new(
        Arc::clone(&transport_router),
        Arc::clone(&outbox),
    ));

    let swim_addr = rt
        .spawn(
            SwimActor::new(node_id, swim_config, Instant::now(), peer_directory.clone())
                .with_observer(Box::new(Arc::clone(&swim_telemetry))),
        )
        .expect("spawn SwimActor");
    let registry_addr = rt
        .spawn(
            RegistryActor::new(node_id, registry_config, peer_directory.clone())
                .with_view(Arc::clone(&registry_view)),
        )
        .expect("spawn RegistryActor");
    let metadata_addr = rt
        .spawn(MetadataActor::new(
            node_id,
            metadata_lambda,
            peer_directory.clone(),
            Arc::clone(&relay_mirror),
        ))
        .expect("spawn MetadataActor");
    // §5 egress: a shared transport that resolves an actor's host from the route
    // view at send time, plus the binder the DirectoryActor uses to register a
    // route for each actor it learns lives on a peer.
    let route_view_transport = Arc::new(RouteViewTransport::new(
        Arc::clone(&route_view),
        Arc::clone(&outbox),
    ));
    let route_binder = Arc::new(IrohRouteBinder::new(
        Arc::clone(&transport_router),
        Arc::clone(&route_view_transport),
    ));
    let directory_addr = rt
        .spawn(DirectoryActor::new(
            node_id,
            peer_directory.clone(),
            Arc::clone(&route_view),
            route_binder,
        ))
        .expect("spawn DirectoryActor");

    // Fan SWIM's MembershipChanged stream out to the registry + metadata actors
    // and into a mirror the dashboard snapshot reads. The sentinel self-id means
    // every real node is stored in the mirror (a MemberList never stores self).
    let membership_mirror = Arc::new(Mutex::new(
        distribution::swim::member_list::MemberList::new(distribution::types::NodeId([0xFF; 32])),
    ));
    let fanout_addr = rt
        .spawn(MembershipFanout {
            registry: registry_addr,
            metadata: metadata_addr,
            directory: directory_addr,
            mirror: Arc::clone(&membership_mirror),
        })
        .expect("spawn MembershipFanout");
    rt.send_to(swim_addr, SwimIn::Subscribe { observer: fanout_addr })
        .expect("subscribe membership fanout");

    // Ingress routing table: which local actor owns each inbound wire tag.
    let mut routes: std::collections::HashMap<String, swactor::actor::ActorAddress> =
        std::collections::HashMap::new();
    for tag in [
        "swactor_dist::Ping",
        "swactor_dist::Ack",
        "swactor_dist::PingReq",
        "swactor_dist::IndirectAck",
        "swactor_dist::JoinRequest",
        "swactor_dist::JoinResponse",
    ] {
        routes.insert(tag.to_string(), swim_addr);
    }
    routes.insert("swactor_dist::RegistryGossip".to_string(), registry_addr);
    routes.insert("swactor_dist::MetadataGossip".to_string(), metadata_addr);
    routes.insert("swactor_dist::DirectoryGossip".to_string(), directory_addr);
    driver.enable_actor_bridge(
        Arc::clone(&rt),
        Arc::clone(&actor_codec),
        routes,
        swim_addr,
        Arc::clone(&relay_mirror),
        Arc::clone(&route_view),
    );

    // Spawn StreamManager actor on the swactor runtime
    let stream_mgr = swactor_datastore::streams::StreamManager::new(
        driver.endpoint().clone(),
        driver.tokio_handle(),
        Arc::clone(&rt),
    );
    let stream_mgr_addr = rt.spawn(stream_mgr).expect("spawn StreamManager");
    rt.register_name(swactor_datastore::streams::STREAM_MANAGER_NAME, stream_mgr_addr)
        .expect("register StreamManager");

    // Wire streams into the datastore
    if let Some(group) = &ds_group {
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
    let actor_addrs = spawn_actors(actors, &rt, directory_addr, &driver);

    // Announce node name and relay URL to cluster gossip (via the MetadataActor).
    rt.send_to(metadata_addr, MetadataIn::SetNodeName { name: node_name.clone() })
        .ok();
    let home_relay_set = if let Some(url) = driver.relay_url().map(|u| u.to_string()) {
        rt.send_to(metadata_addr, MetadataIn::SetRelayUrl { url: Some(url) })
            .ok();
        true
    } else {
        false
    };

    // Single-source dashboard wiring: the node-local dashboard renders from the
    // node's OWN datastream frames, folded by an in-process `FleetView`. These
    // shared slots carry the reconstructed views from the frame consumer (the
    // writer, in the emitter's sink) to the plugins / `set_stats` (the readers).
    let dist_cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let fleet_cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let stats_slot: Arc<Mutex<Option<swactor::stats::RuntimeStats>>> = Arc::new(Mutex::new(None));
    // Live node-interactive overlays the node-local distribution page layers
    // onto the reconstructed JSON: the rich invite code and real-time join
    // statuses (interactive, not collection telemetry), plus the node's own
    // authoritative SWIM membership — the node ran SWIM, so its membership
    // mirror is the source of truth for its single-node view (the fleet
    // consumer's cross-node liveness filter would otherwise hide every peer).
    let invite_overlay: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let join_overlay: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let members_overlay: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let dist_plugin = plugins::distribution::DistributionPlugin::new(
        Arc::clone(&dist_cache),
        Some(join_tx_dist),
    );
    let dismissed_statuses = dist_plugin.dismissed_statuses();
    dash.register_plugin(Arc::new(dist_plugin));

    // Start dashboard HTTP on IrohDriver's tokio runtime
    dash.start_http(driver.tokio_handle());
    eprintln!("Dashboard at http://0.0.0.0:{dashboard_port}");

    // ── Periodic side-work: ordinary async tasks on tokio::time::interval ──
    // (NOT a thread::sleep pump). The datastore keeps its round-based cadence —
    // one interval tick == one round — preserving the old 100ms/round semantics
    // with zero datastore-crate change.
    if let Some(group) = ds_group {
        let stop = Arc::clone(stop);
        tokio.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let mut round: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                interval.tick().await;
                round += 1;
                group.tick(round);
            }
        });
    }

    // Demo heartbeat: route a Heartbeat to each demo actor on a fixed cadence.
    {
        let rt = Arc::clone(&rt);
        let stop = Arc::clone(stop);
        let addrs = actor_addrs;
        tokio.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            while !stop.load(Ordering::Relaxed) {
                interval.tick().await;
                for addr in &addrs {
                    let _ = rt.send_to(*addr, Heartbeat);
                }
            }
        });
    }

    // ── SWIM / gossip clock ──
    // An external interval restamps wall-clock `now` each fire (swactor's
    // tick-counted timer can't) and injects `Tick` into each protocol actor.
    // 100 ms ≪ the probe/suspicion timeouts, so detection resolution is ample.
    {
        let rt = Arc::clone(&rt);
        let stop = Arc::clone(stop);
        tokio.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            while !stop.load(Ordering::Relaxed) {
                interval.tick().await;
                let _ = rt.send_to(swim_addr, SwimIn::Tick { now: Instant::now() });
                let _ = rt.send_to(registry_addr, RegistryIn::Tick);
                let _ = rt.send_to(metadata_addr, MetadataIn::Tick);
                let _ = rt.send_to(directory_addr, DirectoryIn::Tick);
            }
        });
    }

    // ── THE unified driver loop ──
    // One task ticks the lone swactor runtime AND the SWIM state machine (now
    // wall-clock, so it advances every iteration) and drains iroh I/O — all
    // pure-sync, no block_on. It runs as the block_on future on the main thread
    // (so `driver`, which is !Sync, needs no Send bound) and busy-spins one
    // worker (the mailbox has no waker, by design).
    let stop_loop = Arc::clone(stop);
    // Per-node telemetry datastream (single source, always-on): build the shared
    // emitter and install its process-output observer so every process this node
    // spawns is captured as `proc.<label>.*`. The emitter ships frames to an
    // in-process consumer that renders the node-local dashboard. There is no
    // dedicated UDP collector — cross-node telemetry rides the cluster transport.
    let node_hex = hex_encode(&driver.node_id().0);
    let life = std::env::var("SWACTOR_LIFETIME")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let sink = LocalRenderSink {
        view: FleetView::new(Some(node_hex.clone())),
        dist_cache: Arc::clone(&dist_cache),
        datastore_cache: Arc::clone(&datastore_cache),
        fleet_cache: Arc::clone(&fleet_cache),
        stats_slot: Arc::clone(&stats_slot),
        invite_code: Arc::clone(&invite_overlay),
        join_statuses: Arc::clone(&join_overlay),
        members: Arc::clone(&members_overlay),
    };
    let mut emitter = DatastreamEmitter::new(
        EmitterConfig {
            node_hex: node_hex.clone(),
            life,
            mux_capacity: 4096,
        },
        Box::new(sink),
    );
    // The node drives `membership` from the SWIM observer (real transitions with a
    // cause), so turn off the emitter's member-list diff to avoid duplicate,
    // reason-less transitions.
    emitter.use_external_membership();
    rt.set_process_output_observer(emitter.process_observer());
    // Stream datastore op events onto the node's own datastream.
    if let Some(metrics) = &ds_metrics {
        metrics.set_event_observer(Arc::new(NodeDatastoreObserver {
            sink: emitter.event_sink(),
        }));
    }
    // Seed identity with the descriptive fields known at startup; the relay URL
    // and listen addr are learned lazily and re-emitted from the loop below.
    emitter.update_identity(&IdentityRecord {
        node: node_hex.clone(),
        life,
        node_name: node_name.clone(),
        listen_addr: driver.listen_addr(),
        relay_url: driver.relay_url().map(|u| u.to_string()).unwrap_or_default(),
        version: VERSION.to_string(),
    });

    tokio.block_on(async move {
        let mut home_relay_set = home_relay_set;
        let mut last_snapshot = Instant::now();
        while !stop_loop.load(Ordering::Relaxed) {
            tokio::task::yield_now().await; // run iroh reader/writer/accept/dial tasks + reactor
            driver.pump_inbound_to_actors(); // decode received frames → actor mailboxes
            rt.tick(); // advance the actors: consume inbound + Tick, enqueue outbound
            driver.drain_outbox(&outbox); // actor-produced frames → iroh writes

            // Forward incoming stream connections to StreamManager.
            for (node_id, conn) in driver.drain_other_connections() {
                let rt_clone = Arc::clone(&rt);
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

            // Drain discovered peers (dashboard "Add Peer") and auto-join them.
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

            // Lazily pick up home relay URL once the endpoint connects.
            if !home_relay_set
                && let Some(url) = driver.home_relay_url() {
                    eprintln!("Relay URL (home): {url}");
                    rt.send_to(metadata_addr, MetadataIn::SetRelayUrl { url: Some(url.to_string()) })
                        .ok();
                    home_relay_set = true;
                }

            // Dashboard snapshot — throttled (UI refresh, not protocol cadence:
            // SWIM and the actors advance every iteration above).
            if last_snapshot.elapsed() >= SNAPSHOT_REFRESH {
                last_snapshot = Instant::now();
                // Members and relay-peer count from the MembershipChanged-fed
                // mirror (SWIM runs in the actor now) and the metadata relay
                // mirror. We also stage the authoritative member list + counts as
                // the node-local overlay (the node's own SWIM view drives its
                // single-node distribution graph).
                let (members, relay_peers) = {
                    use distribution::types::MemberState;
                    let ml = membership_mirror.lock().unwrap();
                    let relay = relay_mirror.read().unwrap();
                    let mut members: Vec<(String, String)> = Vec::new();
                    let mut members_json: Vec<serde_json::Value> = Vec::new();
                    let mut relay_peers = 0u32;
                    let (mut alive, mut suspect, mut dead) = (0usize, 0usize, 0usize);
                    for e in ml.all_members() {
                        let id: String =
                            e.node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
                        let state = match e.state {
                            MemberState::Alive => {
                                alive += 1;
                                "alive"
                            }
                            MemberState::Suspect => {
                                suspect += 1;
                                "suspect"
                            }
                            MemberState::Dead => {
                                dead += 1;
                                "dead"
                            }
                        }
                        .to_string();
                        let r = relay.get(&e.node_id).cloned();
                        if r.is_some() {
                            relay_peers += 1;
                        }
                        members_json.push(serde_json::json!({
                            "node_id": id.clone(),
                            "addr": serde_json::Value::Null,
                            "state": state.clone(),
                            "incarnation": e.incarnation,
                            "relay_url": r,
                        }));
                        members.push((id, state));
                    }
                    *members_overlay.lock().unwrap() = Some(
                        serde_json::json!({
                            "members": members_json,
                            "alive_count": alive,
                            "suspect_count": suspect,
                            "dead_count": dead,
                        })
                        .to_string(),
                    );
                    (members, relay_peers)
                };

                // Consolidated distribution-subsystem state. Directory route count,
                // peer-auth, the cluster registry (via `registry_view`), and the
                // location cache (remote routes off the directory's RouteView) are
                // live; probe targets still come from the actors and remain empty
                // until the SWIM observer mirror lands.
                {
                    let (mode, count) = {
                        let pa = peer_auth.lock().unwrap();
                        if pa.is_open() {
                            ("open".to_string(), 0)
                        } else {
                            ("allow-list".to_string(), pa.list_peers().len() as u32)
                        }
                    };
                    let registry = registry_view.read().unwrap();
                    let registry_entries = registry
                        .entries
                        .iter()
                        .map(|e| catalog::RegistryEntryRec {
                            name: e.name.clone(),
                            actor_addr: hex(&e.actor_addr.0),
                            node_id: hex(&e.node_id.0),
                            tombstone: e.tombstone,
                        })
                        .collect();
                    let cache = driver.location_cache_entries();
                    let cache_entries = cache
                        .iter()
                        .map(|(addr, host)| catalog::CacheEntryRec {
                            actor_addr: hex(&addr.0),
                            node_id: hex(&host.0),
                        })
                        .collect();
                    let recent_probe_targets = swim_telemetry
                        .recent_targets()
                        .iter()
                        .map(|t| hex(&t.0))
                        .collect();
                    emitter.submit_dist_state(&DistributionState {
                        directory_route_count: driver.directory_route_count() as u32,
                        peer_auth_mode: mode,
                        authorized_peer_count: count,
                        registry_size: registry.size as u32,
                        registry_tombstones: registry.tombstones as u32,
                        registry_entries,
                        cache_size: cache.len() as u32,
                        cache_entries,
                        recent_probe_targets,
                        ..Default::default()
                    });
                }

                // Consolidated datastore steady metrics from the shared accumulator.
                if let Some(metrics) = &ds_metrics {
                    let r = metrics.readout();
                    emitter.submit_datastore_state(&DatastoreState {
                        object_count: r.object_count,
                        total_bytes: r.total_bytes,
                        put_ops: r.put_ops,
                        get_ops: r.get_ops,
                        delete_ops: r.delete_ops,
                        objects: r
                            .objects
                            .iter()
                            .map(|o| catalog::ObjectRec {
                                hash: o.hash.clone(),
                                name: o.name.clone(),
                                size_bytes: o.size_bytes,
                            })
                            .collect(),
                        active_transfers: r
                            .active_transfers
                            .iter()
                            .map(|t| catalog::TransferRec {
                                hash: t.hash.clone(),
                                chunks_received: t.chunks_received as u64,
                                chunks_total: t.chunks_total as u64,
                            })
                            .collect(),
                    });
                }

                // Per-actor runtime detail (the real actor table).
                {
                    let mut rs = rt.stats();
                    collector.enrich(&mut rs);
                    dashboard::collector::enrich_names(&mut rs, &rt);
                    let actors = rs
                        .actor_details
                        .iter()
                        .map(|a| ActorRec {
                            address: a.address.0.iter().map(|b| format!("{:02x}", b)).collect(),
                            name: a.name.clone().unwrap_or_default(),
                            mailbox_depth: a.mailbox_depth as u32,
                            messages_processed: a.messages_processed,
                            last_msg_type: a.last_msg_type.clone().unwrap_or_default(),
                            poisoned: a.poisoned,
                            message_type_counts: a.message_type_counts.clone(),
                        })
                        .collect();
                    emitter.submit_actor_detail(&ActorRuntimeDetail { actors });

                    // Worker-runtime counters (W7): the routing/error tallies and
                    // tick timing the runtime keeps per worker — live in-process but
                    // never on the pipe until now. Aggregated across workers from the
                    // same snapshot.
                    let mut wc = catalog::WorkerCounters {
                        num_workers: rs.workers.len() as u32,
                        ..Default::default()
                    };
                    for w in &rs.workers {
                        wc.scheduled_tasks += w.num_actors as u32;
                        wc.local_sends += w.local_sends;
                        wc.cross_sends += w.cross_sends;
                        wc.inbox_sends += w.inbox_sends;
                        wc.type_mismatches += w.type_mismatches;
                        wc.panics += w.panics;
                        wc.messages_dropped += w.messages_dropped;
                        wc.restarts += w.restarts;
                        wc.stops += w.stops;
                        wc.messages_processed += w.messages_processed;
                    }
                    let mut tick_us: Vec<u64> = rs
                        .tick_timings
                        .iter()
                        .flatten()
                        .map(|t| t.phase_us.iter().sum())
                        .collect();
                    tick_us.sort_unstable();
                    wc.tick_p50_us = tick_us.get(tick_us.len() / 2).copied().unwrap_or(0);
                    emitter.submit_worker_counters(&wc);
                }

                // Real membership transitions (with cause) from the SWIM observer,
                // submitted before the tick so they drain on this iteration. Replaces
                // the emitter's reason-less member-list diff (`use_external_membership`).
                for t in swim_telemetry.drain_transitions() {
                    emitter.submit_membership(&datastream::catalog::MembershipTransition {
                        peer: hex(&t.peer.0),
                        from: t.from.map(member_state_str).unwrap_or("unknown").to_string(),
                        to: member_state_str(t.to).to_string(),
                        reason: t.reason.to_string(),
                    });
                }

                // Periodic host/runtime/transport samples, then drain — the sink
                // renders the node-local dashboard. `rtt_ms_p50` is the SWIM
                // observer's real probe round-trip median (0 until a probe completes).
                {
                    let rs = rt.stats();
                    emitter.tick(
                        TickInput {
                            members: &members,
                            runtime: DsRuntimeStats {
                                actors_live: rs.actors.len() as u32,
                                mailbox_depth: rs.workers.iter().map(|w| w.mailbox_depth as u32).sum(),
                                scheduled_tasks: rs.workers.iter().map(|w| w.num_actors as u32).sum(),
                            },
                            relay_connected: driver.home_relay_url().is_some(),
                            relay_peers,
                            rtt_ms_p50: swim_telemetry.rtt_ms_p50(),
                        },
                        true,
                    );
                }

                // Push the synthesized runtime stats to the Overview/Actors page.
                if let Some(stats) = stats_slot.lock().unwrap().take() {
                    dash.set_stats(stats);
                }

                // ── Interactive overlays (node-local only; not collection
                // telemetry, so not on the datastream). The node-local
                // distribution page overlays these onto the reconstructed JSON.
                // Rich invite code: <base58>#<addr1>,<addr2>@<relay_url>.
                {
                    let self_relay = relay_mirror.read().unwrap().get(&node_id).cloned();
                    let direct_addrs = driver.direct_addresses();
                    let addrs_part = if direct_addrs.is_empty() {
                        String::new()
                    } else {
                        let addrs_str: Vec<String> =
                            direct_addrs.iter().map(|a| a.to_string()).collect();
                        format!("#{}", addrs_str.join(","))
                    };
                    let relay_part = match self_relay {
                        Some(relay) => format!("@{relay}"),
                        None => String::new(),
                    };
                    *invite_overlay.lock().unwrap() =
                        Some(format!("{invite_code}{addrs_part}{relay_part}"));
                }

                // Drain dismissed join statuses from the dashboard.
                {
                    let mut dismissed = dismissed_statuses.lock().unwrap();
                    for bytes in dismissed.drain(..) {
                        driver.clear_join_status(&swactor_transport::NodeId(bytes));
                    }
                }

                // Real-time join statuses, auto-clearing alive peers.
                {
                    use distribution::iroh_driver::JoinPhase;
                    let statuses = driver.join_statuses();
                    let alive_node_ids: Vec<swactor_transport::NodeId> = members
                        .iter()
                        .filter(|(_, s)| s == "alive")
                        .filter_map(|(id, _)| {
                            if id.len() == 64 {
                                let mut bytes = [0u8; 32];
                                for i in 0..32 {
                                    bytes[i] =
                                        u8::from_str_radix(&id[i * 2..i * 2 + 2], 16).unwrap_or(0);
                                }
                                Some(swactor_transport::NodeId(bytes))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !alive_node_ids.is_empty() {
                        driver.clear_join_statuses(&alive_node_ids);
                    }
                    let js: Vec<serde_json::Value> = statuses
                        .iter()
                        .filter(|(nid, _)| !alive_node_ids.contains(nid))
                        .map(|(nid, status)| {
                            let node_id_hex: String =
                                nid.0.iter().map(|b| format!("{:02x}", b)).collect();
                            let (phase, detail) = match &status.phase {
                                JoinPhase::Connecting { attempt, max_attempts } => {
                                    ("connecting", Some(format!("{attempt}/{max_attempts}")))
                                }
                                JoinPhase::Sending { attempt, max_attempts } => {
                                    ("sending", Some(format!("{attempt}/{max_attempts}")))
                                }
                                JoinPhase::Sent => ("sent", None),
                                JoinPhase::Failed { error } => ("failed", Some(error.clone())),
                            };
                            serde_json::json!({
                                "node_id": node_id_hex,
                                "phase": phase,
                                "detail": detail,
                                "has_relay": status.has_relay,
                                "has_direct": status.has_direct,
                                "direct_addr_count": status.direct_addr_count,
                            })
                        })
                        .collect();
                    *join_overlay.lock().unwrap() = Some(serde_json::Value::Array(js).to_string());
                }
            }
        }

        // Graceful drain: stop the swactor runtime, close iroh (awaited, not
        // block_on — we're on a worker), then tick a bounded number of times so
        // stopping actors and in-flight messages clean up.
        rt.shutdown();
        driver.close().await;
        for _ in 0..DRAIN_TICKS {
            rt.tick();
        }
    });
}

// ── Helpers ──────────────────────────────────────────────────────────────

#[cfg(feature = "iroh")]
fn spawn_actors(
    count: usize,
    rt: &Arc<Runtime>,
    directory_addr: swactor::actor::ActorAddress,
    driver: &distribution::iroh_driver::IrohDriver,
) -> Vec<swactor::actor::ActorAddress> {
    use distribution::directory_actor::DirectoryIn;
    let mut addrs = Vec::new();
    for _ in 0..count {
        // All application actors live on the one swactor runtime.
        match rt.spawn(HeartbeatActor) {
            Ok(addr) => {
                // Sign a host claim for the actor and hand it to the DirectoryActor
                // to disseminate, so peers learn where to route this actor.
                let entry = driver.register_actor(addr, 1);
                let _ = rt.send_to(directory_addr, DirectoryIn::Register(entry));
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

/// The `membership` channel's state strings, matching the member-list diff's
/// convention so the consumer renders observer-driven and diffed transitions alike.
fn member_state_str(state: distribution::types::MemberState) -> &'static str {
    use distribution::types::MemberState;
    match state {
        MemberState::Alive => "alive",
        MemberState::Suspect => "suspect",
        MemberState::Dead => "dead",
    }
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
