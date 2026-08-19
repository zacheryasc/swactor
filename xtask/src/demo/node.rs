//! Node role: a real swactor runtime that joins the supervisor over iroh.
//!
//! Re-exec'd from the xtask binary by the demo provider (local process kind)
//! or launched as the entrypoint of the demo docker image. Builds an engine +
//! `IrohDriver` (relay disabled), joins the supervisor's endpoint, and then
//! **announces itself over the control plane**: a tagged gossip frame
//! ([`ANNOUNCE_TAG`]) carrying the provision attempt token, its iroh node
//! key, and its advertised endpoint address. The supervisor's announce relay
//! routes that frame by attempt to the owning bootstrap actor — the wire
//! announce *is* the readiness signal (no shared filesystem, no stdout
//! capture), and it repeats every [`HEARTBEAT_PERIOD`] as a liveness
//! heartbeat.
//!
//! Telemetry: the node owns a real [`TelemetryEndpoint`] whose stream id is
//! its transport identity (iroh node key). The runtime stats hook feeds
//! `runtime.actors` rosters, a `node.status` channel carries liveness
//! records, and beat actors keep the roster visibly alive. The supervisor
//! pulls this endpoint over `TELEMETRY_ALPN`
//! ([`iroh_driver::spawn_pull_server`]) — the node never dials out for
//! telemetry.

use std::sync::Arc;
use std::time::Duration;

use iroh::RelayMode;
use serde_json::json;
use swactor::actor::{ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use distribution::node::DistributedNodeConfig;
use iroh_driver::{IrohDriver, IrohDriverConfig, TELEMETRY_ALPN, spawn_pull_server};
use telemetry::{ChannelContent, TelemetryEndpoint, TelemetryProducer};

use crate::demo::HEARTBEAT_PERIOD;
use crate::demo::edge;

/// Wire tag of the announce gossip frame (`CodecRegistry` decode key on the
/// supervisor side; raw tag bytes on the node side).
pub const ANNOUNCE_TAG: &str = "xtask_demo/NodeAnnounce/1";

/// A node's self-introduction on the supervisor's control plane. Sent right
/// after joining and then every heartbeat period; the supervisor routes it
/// by `attempt` to the owning bootstrap actor (first delivery = readiness,
/// later deliveries = liveness).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NodeAnnounce {
    /// Provision attempt token (`--demo-attempt`); correlates the announce
    /// with the supervisor-side bootstrap actor that launched this node.
    pub attempt: u64,
    /// Logical node name (slot id) for dashboard labeling.
    pub logical_node: String,
    /// The node's transport key (iroh node id, hex).
    pub key_hex: String,
    /// Serde-serialized `iroh::EndpointAddr` the supervisor dials for
    /// telemetry pulls.
    pub endpoint_addr_json: String,
    pub at_ms: u64,
}

/// How often the node polls its driver for accepted telemetry pull
/// connections.
const PULL_POLL_PERIOD: Duration = Duration::from_millis(200);

/// Idle sleep for the pull answer writer between subscription batches.
const WRITER_IDLE: Duration = Duration::from_millis(100);

/// Number of beat actors on the node — real actors whose message flow keeps
/// the `runtime.actors` roster visibly alive.
const BEAT_ACTORS: u64 = 2;

/// Run the node role. `supervisor_addr_json` is a serde-serialized
/// `iroh::EndpointAddr` of the supervisor's iroh endpoint; `attempt` is the
/// provision attempt token echoed back in every announce.
pub fn run_node_role(supervisor_addr_json: &str, attempt: u64) -> Result<(), String> {
    install_parent_death_signal()?;
    let supervisor_addr: iroh::EndpointAddr = serde_json::from_str(supervisor_addr_json)
        .map_err(|error| format!("invalid supervisor endpoint address: {error}"))?;
    let logical_node = std::env::var("DEMO_NODE_ID").unwrap_or_else(|_| "node".to_owned());

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).map_err(|error| format!("backend: {error}"))?,
    )
    .map_err(|error| format!("engine: {error}"))?;

    // Bind the driver, then keep it alive for the process lifetime: dropping
    // it closes the endpoint. The endpoint must advertise TELEMETRY_ALPN so
    // the supervisor's pull connection can negotiate it, and EDGE_ALPN so
    // the supervisor's data-plane edges can dial in.
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![TELEMETRY_ALPN.to_vec(), iroh_driver::EDGE_ALPN.to_vec()],
        },
    )
    .map_err(|error| format!("iroh driver: {error}"))?;
    let node_hex = swactor_transport::hex_encode(&driver.node_id().0);

    // Telemetry endpoint: stream identity is the transport node key, so
    // every provisioned process lands on its own dashboard stream.
    let stream = telemetry::frame::StreamId::new(
        telemetry::frame::NodeId::new(&node_hex),
        telemetry::frame::Lifetime(1),
    );
    let endpoint = TelemetryEndpoint::with_descriptor(
        telemetry::frame::StreamDescriptor {
            stream,
            label: Some(format!("demo node {logical_node} runtime")),
            origin: telemetry::frame::StreamOrigin::RemoteNode,
        },
        256,
        16,
    );
    let producer = endpoint.producer();
    let actors_channel = endpoint.register_channel(
        "runtime.actors",
        ChannelContent::JsonRecord {
            schema: Some("runtime.actors".to_owned()),
        },
    );
    let status_channel = endpoint.register_channel(
        "node.status",
        ChannelContent::JsonRecord {
            schema: Some("demo.node.status.v1".to_owned()),
        },
    );
    let beats_channel = endpoint.register_channel(
        "node.beat",
        ChannelContent::JsonRecord {
            schema: Some("demo.node.beat.v1".to_owned()),
        },
    );
    runtime.set_stats_hook(producer.stats_hook_on(actors_channel));

    // Data-plane edge agent: owns this node's (single) inbound edge. The
    // supervisor provisions it over the control plane (tagged gossip
    // decoded by the actor bridge below); observations are mirrored onto
    // the `node.edge` telemetry channel (render-only), and readiness
    // acks travel back as gossip — never through telemetry.
    let edge_channel = endpoint.register_channel(
        edge::NODE_EDGE_CHANNEL,
        ChannelContent::JsonRecord {
            schema: Some("demo.node.edge.v1".to_owned()),
        },
    );
    let driver_slot: Arc<std::sync::OnceLock<Arc<IrohDriver>>> =
        Arc::new(std::sync::OnceLock::new());
    let edge_agent = runtime
        .spawn(edge::NodeEdgeAgent::new(
            attempt,
            logical_node.clone(),
            supervisor_addr.clone(),
            driver_slot.clone(),
            producer.clone(),
            edge_channel,
        ))
        .map_err(|error| format!("spawn edge agent: {error}"))?;
    {
        use distribution::transport_bridge::{Outbox, RelayMirror, RouteView};
        use swactor_transport::CodecRegistry;
        let mut codec = CodecRegistry::new();
        codec.register_decoder::<edge::NodeEdgeMsg>(edge::EDGE_PROVISION_TAG, |bytes| {
            let provision: edge::EdgeProvision = serde_json::from_slice(bytes)
                .map_err(|e| swactor::Error::from(format!("edge provision decode: {e}")))?;
            Ok(edge::NodeEdgeMsg::Provision(provision))
        });
        let mut routes = std::collections::HashMap::new();
        routes.insert(edge::EDGE_PROVISION_TAG.to_owned(), edge_agent);
        let relay_mirror: RelayMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let route_view: RouteView =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let outbox: Outbox = Arc::new(std::sync::Mutex::new(Vec::new()));
        driver.enable_actor_bridge(
            runtime.clone(),
            Arc::new(codec),
            routes,
            edge_agent,
            relay_mirror,
            route_view,
            outbox,
        );
        driver.install_actor_bridge_pump(Duration::from_millis(250));
    }
    let driver = Arc::new(driver);
    let _ = driver_slot.set(Arc::clone(&driver));
    // The node serves telemetry pulls itself (spawn_pull_server): keep
    // TELEMETRY_ALPN connections out of the driver-owned ingress so the
    // serve loop below can drain them.
    driver.retain_telemetry_connections();
    {
        // Edge agent tick: poll the edge runtime on the supervisor's
        // session cadence.
        let sender = runtime.create_sender();
        let edge_engine = engine.handle();
        edge_engine.clone().spawn(async move {
            let mut interval = edge_engine.interval(Duration::from_millis(250));
            loop {
                (&mut interval).await;
                let _ = sender.send_to(edge_agent, edge::NodeEdgeMsg::Tick);
            }
        });
    }

    // Join, then announce identity + advertised address to the supervisor's
    // bootstrap actor over the control plane (readiness + telemetry dial).
    driver.join(&[supervisor_addr.clone()]);
    let addr_json =
        serde_json::to_string(&driver.endpoint_addr()).map_err(|e| format!("addr: {e}"))?;
    let announce_driver = Arc::clone(&driver);
    let announce_logical = logical_node.clone();
    let announce_key = node_hex.clone();
    let announce_engine = engine.handle();
    announce_engine.clone().spawn(async move {
        let mut interval = announce_engine.interval(HEARTBEAT_PERIOD);
        loop {
            (&mut interval).await;
            let announce = NodeAnnounce {
                attempt,
                logical_node: announce_logical.clone(),
                key_hex: announce_key.clone(),
                endpoint_addr_json: addr_json.clone(),
                at_ms: unix_ms(),
            };
            let Ok(bytes) = serde_json::to_vec(&announce) else {
                continue;
            };
            announce_driver.send_tagged_gossip(
                supervisor_addr.clone(),
                ANNOUNCE_TAG.as_bytes(),
                bytes,
            );
        }
    });

    // Beat actors: real actors with real message flow, so the node's
    // runtime.actors roster (pulled by the supervisor) is visibly alive.
    let sender = runtime.create_sender();
    let mut beat_addrs = Vec::new();
    for index in 0..BEAT_ACTORS {
        let name = format!("beat-{index}");
        let producer = producer.clone();
        let addr = runtime
            .spawn(BeatActor {
                name: name.clone(),
                producer,
                channel: beats_channel,
            })
            .map_err(|error| format!("spawn beat actor: {error}"))?;
        beat_addrs.push((addr, name));
    }

    // Heartbeats: a node.status telemetry record (the wire announce above is
    // the supervisor-facing liveness channel).
    let heartbeat_producer = producer.clone();
    let heartbeat_logical = logical_node.clone();
    let heartbeat_key = node_hex.clone();
    let interval_engine = engine.handle();
    interval_engine.clone().spawn(async move {
        let mut interval = interval_engine.interval(HEARTBEAT_PERIOD);
        let mut seq: u64 = 0;
        loop {
            (&mut interval).await;
            seq += 1;
            let payload = json!({
                "at_ms": unix_ms(),
                "node": heartbeat_logical,
                "key": heartbeat_key,
                "seq": seq,
                "alive": true,
                "pid": std::process::id(),
                "beats": BEAT_ACTORS,
            });
            let bytes = serde_json::to_vec(&payload).expect("status serializes");
            heartbeat_producer.submit_bytes(status_channel, bytes);
        }
    });

    // Beat driver: engine intervals poking the beat actors.
    for (index, (addr, _name)) in beat_addrs.iter().enumerate() {
        let addr = *addr;
        let beat_sender = sender.clone();
        let beat_engine = engine.handle();
        // Stagger the beat periods so roster entries tick at different rates.
        let period = Duration::from_millis(1000 + 500 * index as u64);
        beat_engine.clone().spawn(async move {
            let mut interval = beat_engine.interval(period);
            loop {
                (&mut interval).await;
                let _ = beat_sender.send_to(addr, BeatMsg::Wake);
            }
        });
    }

    // Serve telemetry pulls: accepted TELEMETRY_ALPN connections answer with
    // this endpoint's subscription stream.
    let telemetry_endpoint = Arc::new(endpoint);
    let serve_engine = engine.handle();
    let serve_driver = Arc::clone(&driver);
    serve_engine.clone().spawn(async move {
        let mut interval = serve_engine.interval(PULL_POLL_PERIOD);
        loop {
            (&mut interval).await;
            // Drain the mux into the endpoint fanout so producer frames
            // reach the pull subscription; without this tick the mux fills
            // and nothing is ever streamed.
            let _ = telemetry_endpoint.tick();
            for (_node, conn) in serve_driver.drain_accepted_for_alpn(TELEMETRY_ALPN) {
                spawn_pull_server(
                    &serve_engine,
                    conn,
                    Arc::clone(&telemetry_endpoint),
                    WRITER_IDLE,
                );
            }
        }
    });

    // The engine owns progression; park this thread until killed. `driver`
    // stays alive until process exit.
    let _keep_driver = driver;
    loop {
        std::thread::park();
    }
}

/// Message that keeps a beat actor's mailbox flowing (and therefore its
/// `runtime.actors` stats current).
#[derive(Clone, Debug)]
pub enum BeatMsg {
    Wake,
}

/// A deliberately trivial actor whose only job is existing: every Wake it
/// submits one `node.beat` record, and the runtime stats hook records its
/// productive tick on `runtime.actors`.
pub struct BeatActor {
    name: String,
    producer: TelemetryProducer,
    channel: telemetry::ChannelId,
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

impl ActorInterface for BeatActor {
    type Incoming = BeatMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: BeatMsg) {
        let payload = json!({
            "at_ms": unix_ms(),
            "actor": self.name,
        });
        let bytes = serde_json::to_vec(&payload).expect("beat serializes");
        self.producer.submit_bytes(self.channel, bytes);
    }
}

/// Arm the kernel's parent-death signal so a hard-killed supervisor
/// (SIGKILL, crash, wedged teardown) cannot leave this node orphaned and
/// parked forever. The `parent_id` check closes the fork/prctl race: if the
/// supervisor died before the prctl landed, the signal would never fire.
/// Inside a container the node is PID 1 (parent 0), so the check passes and
/// the prctl is a harmless no-op — the docker bootstrap's `docker rm -f`
/// owns termination there.
#[cfg(target_os = "linux")]
fn install_parent_death_signal() -> Result<(), String> {
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
            return Err(format!(
                "prctl(PR_SET_PDEATHSIG): {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    if std::os::unix::process::parent_id() == 1 {
        return Err("supervisor exited before node startup".to_owned());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn install_parent_death_signal() -> Result<(), String> {
    // Non-Linux builds rely on the supervisor's graceful teardown path.
    Ok(())
}
