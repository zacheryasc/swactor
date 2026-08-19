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
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::config::RuntimeConfig;
use swactor::runtime::{ExternalSender, RuntimeParts};
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};

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

    // Join, then announce identity + advertised address to the supervisor's
    // bootstrap actor over the control plane (readiness + telemetry dial).
    driver.join(&[supervisor_addr.clone()]);
    let addr_json =
        serde_json::to_string(&driver.endpoint_addr()).map_err(|e| format!("addr: {e}"))?;

    // Beat actors: real actors with real message flow, so the node's
    // runtime.actors roster (pulled by the supervisor) is visibly alive.
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
        beat_addrs.push(addr);
    }

    // Serve telemetry pulls: accepted TELEMETRY_ALPN connections answer with
    // this endpoint's subscription stream.
    let telemetry_endpoint = Arc::new(endpoint);
    let completion = ActorCompletion::new();
    let runtime_actor = NodeRuntimeActor {
        engine: engine.handle(),
        sender: runtime.create_sender(),
        edge_agent,
        driver: Arc::clone(&driver),
        attempt,
        supervisor_addr,
        logical_node,
        node_hex,
        endpoint_addr_json: addr_json,
        endpoint: Arc::clone(&telemetry_endpoint),
        producer,
        status_channel,
        beats: beat_addrs
            .into_iter()
            .enumerate()
            .map(|(index, actor)| (actor, Duration::from_millis(1000 + 500 * index as u64)))
            .collect(),
        heartbeat_seq: 0,
        completion: completion.clone(),
    };
    let runtime_actor = runtime
        .spawn(runtime_actor)
        .map_err(|error| format!("spawn node runtime actor: {error}"))?;
    let stop_actor = runtime
        .spawn(NodeStopForwarder {
            sender: runtime.create_sender(),
            runtime_actor,
        })
        .map_err(|error| format!("spawn node stop actor: {error}"))?;
    #[cfg(target_os = "linux")]
    swactor_process::spawn_os_stop_signal_wait(runtime.create_sender(), stop_actor);

    // The actor owns lifecycle; the entrypoint waits only for its terminal signal.
    completion.wait();
    Ok(())
}

#[derive(Clone, Debug)]
enum NodeRuntimeMsg {
    EdgeTick,
    Announce,
    Heartbeat,
    Beat(usize),
    PullTelemetry,
    Stop,
}

struct NodeRuntimeActor {
    engine: EngineHandle,
    sender: ExternalSender,
    attempt: u64,
    edge_agent: ActorAddress,
    driver: Arc<IrohDriver>,
    supervisor_addr: iroh::EndpointAddr,
    logical_node: String,
    node_hex: String,
    endpoint_addr_json: String,
    endpoint: Arc<TelemetryEndpoint>,
    producer: TelemetryProducer,
    status_channel: telemetry::ChannelId,
    beats: Vec<(ActorAddress, Duration)>,
    heartbeat_seq: u64,
    completion: ActorCompletion<()>,
}

impl NodeRuntimeActor {
    fn schedule(&self, ctx: &Ctx, delay: Duration, message: NodeRuntimeMsg) {
        self.engine
            .send_after(delay, self.sender.clone(), ctx.self_addr(), message);
    }
}

impl ActorInterface for NodeRuntimeActor {
    type Incoming = NodeRuntimeMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.schedule(ctx, Duration::from_millis(250), NodeRuntimeMsg::EdgeTick);
        self.schedule(ctx, Duration::ZERO, NodeRuntimeMsg::Announce);
        self.schedule(ctx, HEARTBEAT_PERIOD, NodeRuntimeMsg::Heartbeat);
        self.schedule(ctx, PULL_POLL_PERIOD, NodeRuntimeMsg::PullTelemetry);
        for (index, (_, period)) in self.beats.iter().enumerate() {
            self.schedule(ctx, *period, NodeRuntimeMsg::Beat(index));
        }
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        match message {
            NodeRuntimeMsg::EdgeTick => {
                let _ = self
                    .sender
                    .send_to(self.edge_agent, edge::NodeEdgeMsg::Tick);
                self.schedule(ctx, Duration::from_millis(250), NodeRuntimeMsg::EdgeTick);
            }
            NodeRuntimeMsg::Announce => {
                let announce = NodeAnnounce {
                    attempt: self.attempt,
                    logical_node: self.logical_node.clone(),
                    key_hex: self.node_hex.clone(),
                    endpoint_addr_json: self.endpoint_addr_json.clone(),
                    at_ms: unix_ms(),
                };
                if let Ok(bytes) = serde_json::to_vec(&announce) {
                    self.driver.send_tagged_gossip(
                        self.supervisor_addr.clone(),
                        ANNOUNCE_TAG.as_bytes(),
                        bytes,
                    );
                }
                self.schedule(ctx, HEARTBEAT_PERIOD, NodeRuntimeMsg::Announce);
            }
            NodeRuntimeMsg::Heartbeat => {
                self.heartbeat_seq = self.heartbeat_seq.saturating_add(1);
                let payload = json!({
                    "at_ms": unix_ms(),
                    "node": self.logical_node,
                    "key": self.node_hex,
                    "seq": self.heartbeat_seq,
                    "alive": true,
                    "pid": std::process::id(),
                    "beats": BEAT_ACTORS,
                });
                let bytes = serde_json::to_vec(&payload).expect("status serializes");
                self.producer.submit_bytes(self.status_channel, bytes);
                self.schedule(ctx, HEARTBEAT_PERIOD, NodeRuntimeMsg::Heartbeat);
            }
            NodeRuntimeMsg::Beat(index) => {
                if let Some((actor, period)) = self.beats.get(index).copied() {
                    let _ = self.sender.send_to(actor, BeatMsg::Wake);
                    self.schedule(ctx, period, NodeRuntimeMsg::Beat(index));
                }
            }
            NodeRuntimeMsg::PullTelemetry => {
                let _ = self.endpoint.tick();
                for (_node, connection) in self.driver.drain_accepted_for_alpn(TELEMETRY_ALPN) {
                    spawn_pull_server(
                        &self.engine,
                        connection,
                        Arc::clone(&self.endpoint),
                        WRITER_IDLE,
                    );
                }
                self.schedule(ctx, PULL_POLL_PERIOD, NodeRuntimeMsg::PullTelemetry);
            }
            NodeRuntimeMsg::Stop => {
                assert!(
                    self.completion.complete(()).is_ok(),
                    "demo node completed twice"
                );
                ctx.stop_self();
            }
        }
    }
}

struct NodeStopForwarder {
    sender: ExternalSender,
    runtime_actor: ActorAddress,
}

impl ActorInterface for NodeStopForwarder {
    type Incoming = swactor_process::ProcessStopSignal;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        let _ = self
            .sender
            .send_to(self.runtime_actor, NodeRuntimeMsg::Stop);
        ctx.stop_self();
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

#[cfg(test)]
mod properties {
    use proptest::prelude::*;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{ActorCompletion, Engine, SteppingBackend};

    use super::*;

    #[derive(Clone, Debug)]
    enum NodeAction {
        Announce,
        Heartbeat,
        EdgeTick,
        PullTelemetry,
        Stop,
    }

    fn node_actions() -> impl Strategy<Value = Vec<NodeAction>> {
        prop::collection::vec(
            prop_oneof![
                3 => Just(NodeAction::Announce),
                4 => Just(NodeAction::Heartbeat),
                2 => Just(NodeAction::EdgeTick),
                1 => Just(NodeAction::PullTelemetry),
            ],
            0..=28,
        )
        .prop_map(|mut generated| {
            let mut actions = vec![NodeAction::Announce, NodeAction::Heartbeat];
            actions.append(&mut generated);
            actions.extend([NodeAction::Stop, NodeAction::Stop]);
            actions
        })
    }

    fn drive(backend: &SteppingBackend, steps: usize) {
        for _ in 0..steps {
            backend.step();
        }
    }

    fn check_node_invariants(
        expected_heartbeats: usize,
        observed_heartbeats: usize,
        active_actors: usize,
        final_actors: usize,
        worker_panics: u64,
    ) -> Result<(), String> {
        if observed_heartbeats != expected_heartbeats {
            return Err(format!(
                "heartbeat mismatch: expected={expected_heartbeats} observed={observed_heartbeats}"
            ));
        }
        if active_actors != 2 {
            return Err(format!(
                "one node identity must own runtime+stop actors: observed={active_actors}"
            ));
        }
        if final_actors != 0 {
            return Err(format!(
                "node actors did not return to baseline: observed={final_actors}"
            ));
        }
        if worker_panics != 0 {
            return Err(format!(
                "node actor worker panicked {worker_panics} time(s)"
            ));
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_node_runtime_transitions_emit_heartbeats_and_stop_once(
            actions in node_actions(),
            attempt in any::<u64>(),
        ) {
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine =
                Engine::new(parts, backend.clone()).expect("one-worker node stepping engine");
            let driver = crate::demo::shared_test_driver();
            let supervisor_addr = driver.endpoint_addr();
            let endpoint = Arc::new(TelemetryEndpoint::with_descriptor(
                telemetry::frame::StreamDescriptor {
                    stream: telemetry::frame::StreamId::new(
                        telemetry::frame::NodeId::new(format!("generated-node-{attempt}")),
                        telemetry::frame::Lifetime(1),
                    ),
                    label: Some("generated demo node".to_owned()),
                    origin: telemetry::frame::StreamOrigin::RemoteNode,
                },
                128,
                64,
            ));
            let producer = endpoint.producer();
            let status_channel = endpoint.register_channel(
                "node.status",
                ChannelContent::JsonRecord {
                    schema: Some("demo.node.status.v1".to_owned()),
                },
            );
            let status_frames = endpoint.subscribe_all("generated-node-property");
            let edge_inbox = runtime
                .new_inbox::<edge::NodeEdgeMsg>()
                .expect("create generated node edge inbox");
            let completion = ActorCompletion::new();
            let runtime_actor = runtime
                .spawn(NodeRuntimeActor {
                    engine: engine.handle(),
                    sender: runtime.create_sender(),
                    attempt,
                    edge_agent: *edge_inbox.addr(),
                    driver: Arc::clone(&driver),
                    supervisor_addr,
                    logical_node: format!("node-{attempt}"),
                    node_hex: format!("{attempt:016x}"),
                    endpoint_addr_json: serde_json::to_string(&driver.endpoint_addr())
                        .expect("serialize generated node endpoint"),
                    endpoint: Arc::clone(&endpoint),
                    producer,
                    status_channel,
                    beats: Vec::new(),
                    heartbeat_seq: 0,
                    completion: completion.clone(),
                })
                .expect("spawn generated node runtime actor");
            let stop_actor = runtime
                .spawn(NodeStopForwarder {
                    sender: runtime.create_sender(),
                    runtime_actor,
                })
                .expect("spawn generated node stop actor");
            drive(&backend, 8);
            let active_stats = runtime.stats();

            let mut expected_heartbeats = 0;
            for action in actions
                .iter()
                .take(actions.len().saturating_sub(2))
            {
                let message = match action {
                    NodeAction::Announce => NodeRuntimeMsg::Announce,
                    NodeAction::Heartbeat => {
                        expected_heartbeats += 1;
                        NodeRuntimeMsg::Heartbeat
                    }
                    NodeAction::EdgeTick => NodeRuntimeMsg::EdgeTick,
                    NodeAction::PullTelemetry => NodeRuntimeMsg::PullTelemetry,
                    NodeAction::Stop => unreachable!("stop actions are the bounded suffix"),
                };
                runtime
                    .send_to(runtime_actor, message)
                    .expect("send generated node action");
                drive(&backend, 4);
            }

            runtime
                .send_to(stop_actor, swactor_process::ProcessStopSignal)
                .expect("send first generated OS stop signal");
            runtime
                .send_to(stop_actor, swactor_process::ProcessStopSignal)
                .expect("send repeated generated OS stop signal");
            drive(&backend, 32);
            prop_assert!(
                completion.complete(()).is_err(),
                "node completion remained pending; actions={actions:?} active={active_stats:?}"
            );
            completion.wait();

            endpoint.tick();
            let observed_heartbeats = status_frames
                .drain_available()
                .into_iter()
                .filter(|event| {
                    matches!(
                        event,
                        telemetry::frame::TelemetryEvent::Frame(delivery)
                            if delivery.channel.channel == status_channel
                    )
                })
                .count();
            let final_stats = runtime.stats();
            let worker_panics = final_stats
                .workers
                .iter()
                .map(|worker| worker.panics)
                .sum::<u64>();
            prop_assert!(
                check_node_invariants(
                    expected_heartbeats,
                    observed_heartbeats,
                    active_stats.actors.len(),
                    final_stats.actors.len(),
                    worker_panics,
                )
                .is_ok(),
                "node transition invariant failed; actions={actions:?} attempt={attempt} \
                 expected_heartbeats={expected_heartbeats} observed_heartbeats={observed_heartbeats} \
                 active={active_stats:?} final={final_stats:?}"
            );
        }
    }

    #[test]
    fn node_transition_oracle_rejects_duplicate_resources() {
        let rejected = check_node_invariants(3, 3, 3, 0, 0);
        assert!(
            rejected.is_err(),
            "node property oracle accepted a controlled duplicate actor resource"
        );
    }
}
