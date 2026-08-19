//! Demo data-plane edges: real `EdgeRuntime` sessions over `EDGE_ALPN`.
//!
//! Supervisor side: one [`EdgeSession`] per established edge — an
//! `EdgeRuntime` (outbound) with its own arena and a recorder worker,
//! polled on the supervisor tick. Node side: [`NodeEdgeAgent`] — the actor
//! the node's actor bridge routes `EdgeProvision` gossip to; it provisions
//! the node's (single) inbound edge, polls it, mirrors observations onto
//! the `node.edge` telemetry channel, and answers control-plane
//! [`EdgeAck`] gossip which terminates the supervisor's provision
//! retries. No control flow reads telemetry: establishment decisions come
//! from the supervisor-local FSM plus the gossip ack path. `node.edge`
//! records are render-only dashboard material.
//!
//! Edge ids are supervisor-allocated and travel in the provision message,
//! so both sides agree on the wire preamble tag. Provision gossip is
//! retried by the supervisor until an ack lands (the node re-acks on
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data_plane::arena::{ArenaConfig, ArenaManager};
use data_plane::edge_lifecycle::{
    DType, NodeId, ObjectKind, ObjectSpec as EdgeObjectSpec, ProvisionRx, ProvisionTx, RingSpec,
};
pub use data_plane::edge_runtime::Observation;
use data_plane::edge_runtime::{EdgeRuntime, LoadedObject, WorkerPort};
use data_plane::edge_wire::EdgeTransport;
use data_plane::ids::{EdgeId, RunId};
use data_plane::object_record::{self, ObjectRecord};
use iroh::EndpointAddr;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use telemetry::TelemetryProducer;

/// Wire tag of the edge-provision gossip frame (supervisor → node).
pub const EDGE_PROVISION_TAG: &str = "xtask_demo/EdgeProvision/1";
/// Wire tag of the edge-ack gossip frame (node → supervisor).
pub const EDGE_ACK_TAG: &str = "xtask_demo/EdgeAck/1";

/// Telemetry channel carrying node-side edge observations (render-only).
pub const NODE_EDGE_CHANNEL: &str = "node.edge";
/// How long a supervisor session waits for an ack before faulting.
pub const PROVISION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound on one edge connect handshake before the session faults.
pub const EDGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Provision gossip retry period.
pub const PROVISION_RETRY_PERIOD: Duration = Duration::from_secs(1);

/// Supervisor → node: arm your inbound edge `edge_id`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EdgeProvision {
    pub attempt: u64,
    pub edge_id: u64,
    pub at_ms: u64,
}

/// Node → supervisor: my inbound `edge_id` reached `outcome`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EdgeAck {
    pub attempt: u64,
    pub edge_id: u64,
    /// `"ready"` or `"fault:<detail>"`.
    pub outcome: String,
    pub at_ms: u64,
}

// Both binaries are the same crate, so the demo edge specs are shared
// constants — the provision message only carries identity, never specs.
pub fn demo_object_spec() -> EdgeObjectSpec {
    EdgeObjectSpec {
        kind: ObjectKind::Activation,
        dtype: DType::F16,
        max_extent_bytes: 4096,
    }
}

pub fn demo_ring_spec() -> RingSpec {
    RingSpec {
        header_bytes: 0,
        data_bytes: 8192,
        alignment: 64,
    }
}

pub fn demo_parse_spec() -> object_record::ObjectSpec {
    object_record::ObjectSpec {
        max_extent: 4096,
        alignment: 16,
        layout: object_record::ObjectLayout::Token,
    }
}

fn boot_arena(node_id: NodeId) -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id,
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("demo arena boots")
}

pub fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

// ─── Shared transport ───────────────────────────────────────────────────────

/// `EdgeTransport` over a shared driver: outbound writers are driver-owned
/// send pumps (`&self` through the Arc); inbound events drain the shared
/// edge-event queue. Supervisor sessions (all outbound) and the node agent
/// (single inbound) both use this — outbound-only pollers never race on
/// `drain_events` because inbound events only exist where edges are
/// accepted.
pub struct DriverTransport(pub Arc<iroh_driver::IrohDriver>);
impl EdgeTransport for DriverTransport {
    type Writer = iroh_driver::EdgeSendHandle;
    type PeerAddr = EndpointAddr;

    fn open_writer(
        &mut self,
        edge_id: EdgeId,
        peer: &EndpointAddr,
    ) -> Result<Self::Writer, String> {
        self.0
            .spawn_edge_send_pump_timeout(peer.clone(), edge_id.0, EDGE_CONNECT_TIMEOUT)
    }

    fn drain_events(&mut self) -> Vec<data_plane::edge_wire::WireEvent> {
        self.0.drain_edge_events()
    }
}

// ─── Recorder worker ────────────────────────────────────────────────────────

/// A `WorkerPort` that records effects instead of touching a GPU: the demo
/// has no worker, but the real edge lifecycle leases and installs rings on
/// both sides, and those effects must succeed for the FSM to reach Ready.
#[derive(Default)]
pub struct DemoWorkerPort {
    pub installed: Vec<(EdgeId, data_plane::ids::RingId)>,
}

impl WorkerPort for DemoWorkerPort {
    fn install_ring(
        &mut self,
        edge_id: EdgeId,
        ring_id: data_plane::ids::RingId,
        _direction: data_plane::edge_lifecycle::RingDirection,
        _layout: &data_plane::arena::RingLayout,
        _object_spec: &EdgeObjectSpec,
    ) -> Result<(), String> {
        self.installed.push((edge_id, ring_id));
        Ok(())
    }

    fn uninstall_ring(&mut self, _ring_id: data_plane::ids::RingId) -> Result<(), String> {
        Ok(())
    }

    fn load_object(
        &mut self,
        _edge_id: EdgeId,
        _ring_id: data_plane::ids::RingId,
        record: &ObjectRecord,
        _spec: &object_record::ObjectSpec,
    ) -> Result<LoadedObject, String> {
        Ok(LoadedObject {
            object_id: record.object_id.0,
            sequence: record.sequence,
            handle_generation: 1,
            handle_id: record.object_id.0,
        })
    }
}

/// Supervisor-side state for one outbound edge toward a node.
pub struct EdgeSession {
    pub edge_id: EdgeId,
    pub attempt: u64,
    pub logical_node: String,
    pub peer: EndpointAddr,
    runtime: EdgeRuntime<DriverTransport>,
    arena: ArenaManager,
    worker: DemoWorkerPort,
    /// Node ack outcome, once its gossip landed.
    pub acked: Option<String>,
    /// Local FSM observed outbound readiness.
    pub local_ready: bool,
    pub faulted: bool,
    /// First provision send; drives retry + timeout.
    pub started: SystemTime,
    pub last_send: SystemTime,
}

impl EdgeSession {
    pub fn new(edge_id: EdgeId, attempt: u64, logical_node: String, peer: EndpointAddr) -> Self {
        let mut runtime = EdgeRuntime::new(NodeId(0));
        runtime.establish_outbound(
            ProvisionTx {
                run_id: RunId(1),
                edge_id,
                local_node_id: NodeId(0),
                consumer_node_id: NodeId(attempt),
                object_spec: demo_object_spec(),
                ring_spec: demo_ring_spec(),
            },
            peer.clone(),
        );
        Self {
            edge_id,
            attempt,
            logical_node,
            peer,
            runtime,
            arena: boot_arena(NodeId(0)),
            worker: DemoWorkerPort::default(),
            acked: None,
            local_ready: false,
            faulted: false,
            started: SystemTime::now(),
            last_send: SystemTime::now(),
        }
    }

    /// One poll: drive the lifecycle; return new observations.
    pub fn poll(
        &mut self,
        driver: &Arc<iroh_driver::IrohDriver>,
    ) -> (Vec<Observation>, Option<String>) {
        let mut transport = DriverTransport(Arc::clone(driver));
        let result = self
            .runtime
            .poll(&mut transport, &mut self.arena, &mut self.worker);
        let observations = self.runtime.take_observations();
        for observation in &observations {
            if matches!(observation, Observation::EdgeReady { .. }) {
                self.local_ready = true;
            }
            if matches!(observation, Observation::EdgeFaulted { .. }) {
                self.faulted = true;
            }
        }
        if result.is_err() {
            self.faulted = true;
        }
        (observations, result.err())
    }

    /// Provision gossip payload for this session.
    pub fn provision(&self) -> EdgeProvision {
        EdgeProvision {
            attempt: self.attempt,
            edge_id: self.edge_id.0,
            at_ms: unix_ms(SystemTime::now()),
        }
    }

    /// Display state for the dashboard snapshot.
    pub fn state(&self) -> &'static str {
        if self.faulted {
            "faulted"
        } else if self.acked.as_deref() == Some("ready") && self.local_ready {
            "ready"
        } else {
            "provisioning"
        }
    }

    pub fn ring_id(&self) -> Option<u64> {
        self.runtime.outbound_ring_id().map(|ring| ring.0)
    }
}

pub enum EdgePumpCmd {
    Establish(Box<EdgeSession>),
    Ack(EdgeAck),
    LiveAttempts(Vec<u64>),
    DropAll,
    Tick,
}

#[derive(Clone)]
pub struct EdgePumpMessage(Arc<std::sync::Mutex<Option<EdgePumpCmd>>>);

impl EdgePumpMessage {
    pub fn new(command: EdgePumpCmd) -> Self {
        Self(Arc::new(std::sync::Mutex::new(Some(command))))
    }
}

/// One pump→actor update: feed lines plus the full edge-state mirror for
/// the dashboard snapshot.
#[derive(Clone, Debug, Default)]
pub struct EdgePumpUpdate {
    pub feed: Vec<(String, String)>,
    pub states: Vec<serde_json::Value>,
}

struct EdgePumpActor {
    engine: EngineHandle,
    sender: ExternalSender,
    driver: Arc<iroh_driver::IrohDriver>,
    supervisor: Arc<std::sync::OnceLock<ActorAddress>>,
    sessions: Vec<EdgeSession>,
}

impl EdgePumpActor {
    fn schedule(&self, ctx: &Ctx) {
        self.engine.send_after(
            Duration::from_millis(250),
            self.sender.clone(),
            ctx.self_addr(),
            EdgePumpMessage::new(EdgePumpCmd::Tick),
        );
    }
}

impl ActorInterface for EdgePumpActor {
    type Incoming = EdgePumpMessage;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.schedule(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        let Some(command) = message
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            return;
        };
        match command {
            EdgePumpCmd::Establish(session) => {
                let attempt = session.attempt;
                self.sessions.retain(|existing| existing.attempt != attempt);
                self.sessions.push(*session);
            }
            EdgePumpCmd::Ack(ack) => {
                if let Some(session) = self
                    .sessions
                    .iter_mut()
                    .find(|session| session.edge_id.0 == ack.edge_id)
                {
                    session.acked = Some(ack.outcome);
                }
            }
            EdgePumpCmd::LiveAttempts(live) => {
                let mut index = 0;
                while index < self.sessions.len() {
                    if live.contains(&self.sessions[index].attempt) {
                        index += 1;
                    } else {
                        let dead = self.sessions.remove(index);
                        eprintln!("demo: edge {} torn down (node gone)", dead.edge_id.0);
                    }
                }
            }
            EdgePumpCmd::DropAll => self.sessions.clear(),
            EdgePumpCmd::Tick => {
                let mut update = EdgePumpUpdate::default();
                for session in &mut self.sessions {
                    pump_one(session, &self.driver, &mut update);
                }
                update.states = self.sessions.iter().map(session_state_json).collect();
                if let Some(supervisor) = self.supervisor.get() {
                    let _ = self.sender.send_to(
                        *supervisor,
                        crate::demo::feed::SupervisorMsg::EdgeUpdate(update),
                    );
                }
                self.schedule(ctx);
            }
        }
    }
}

pub fn start_edge_pump(
    runtime: &Runtime,
    engine: EngineHandle,
    driver: Arc<iroh_driver::IrohDriver>,
    sender: ExternalSender,
    supervisor: Arc<std::sync::OnceLock<ActorAddress>>,
) -> Result<ActorAddress, String> {
    runtime
        .spawn(EdgePumpActor {
            engine,
            sender,
            driver,
            supervisor,
            sessions: Vec::new(),
        })
        .map_err(|error| format!("spawn edge pump actor: {error}"))
}

fn session_state_json(session: &EdgeSession) -> serde_json::Value {
    serde_json::json!({
        "edge": session.edge_id.0,
        "node": session.logical_node,
        "state": session.state(),
        "acked": session.acked.is_some(),
        "ring": session.ring_id(),
    })
}

fn pump_one(
    session: &mut EdgeSession,
    driver: &Arc<iroh_driver::IrohDriver>,
    update: &mut EdgePumpUpdate,
) {
    // Retry the provision gossip until the node acks (the node re-acks
    // duplicates, so retries are safe).
    if session.acked.is_none() && !session.faulted {
        let since_send = session
            .last_send
            .elapsed()
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let since_start = session
            .started
            .elapsed()
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if since_start > PROVISION_ACK_TIMEOUT.as_millis() as u64 {
            session.faulted = true;
            update.feed.push((
                session.logical_node.clone(),
                format!(
                    "edge {}: faulted (provision ack timeout)",
                    session.edge_id.0
                ),
            ));
        } else if since_send >= PROVISION_RETRY_PERIOD.as_millis() as u64 {
            if let Ok(bytes) = serde_json::to_vec(&session.provision()) {
                driver.send_tagged_gossip(
                    session.peer.clone(),
                    EDGE_PROVISION_TAG.as_bytes(),
                    bytes,
                );
            }
            session.last_send = SystemTime::now();
        }
    }
    let (observations, error) = session.poll(driver);
    for observation in &observations {
        match observation {
            Observation::EdgeReady { .. } => update.feed.push((
                session.logical_node.clone(),
                format!("edge {}: outbound ready", session.edge_id.0),
            )),
            Observation::EdgeFaulted { reason, .. } => update.feed.push((
                session.logical_node.clone(),
                format!("edge {}: faulted ({reason:?})", session.edge_id.0),
            )),
            _ => {}
        }
    }
    if let Some(error) = error {
        update.feed.push((
            session.logical_node.clone(),
            format!("edge {}: faulted ({error})", session.edge_id.0),
        ));
    }
}

// ─── Node-side agent ────────────────────────────────────────────────────────

/// Messages into the node edge agent.
#[derive(Clone, Debug)]
pub enum NodeEdgeMsg {
    /// Decoded `EdgeProvision` gossip from the supervisor.
    Provision(EdgeProvision),
    /// Poll tick from the engine interval.
    Tick,
}

/// Node-side owner of the (single) inbound edge. Provisions on gossip,
/// polls the real `EdgeRuntime` lifecycle, mirrors observations onto the
/// `node.edge` telemetry channel, and acks readiness/fault on the control
/// plane. A provision for a different edge id rebuilds the inbound edge
/// (the supervisor replaces sessions); a duplicate for the current edge id
/// only re-acks.
pub struct NodeEdgeAgent {
    attempt: u64,
    logical_node: String,
    supervisor_addr: EndpointAddr,
    /// Filled right after the driver is Arc-wrapped (the bridge setup
    /// needs `&mut` on the driver first; same pattern as the supervisor
    /// slot).
    driver_slot: Arc<std::sync::OnceLock<Arc<iroh_driver::IrohDriver>>>,
    producer: TelemetryProducer,
    edge_channel: telemetry::ChannelId,
    runtime: EdgeRuntime<DriverTransport>,
    arena: ArenaManager,
    worker: DemoWorkerPort,
    edge_id: Option<EdgeId>,
    acked_ready_sent: bool,
}

impl NodeEdgeAgent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt: u64,
        logical_node: String,
        supervisor_addr: EndpointAddr,
        driver_slot: Arc<std::sync::OnceLock<Arc<iroh_driver::IrohDriver>>>,
        producer: TelemetryProducer,
        edge_channel: telemetry::ChannelId,
    ) -> Self {
        Self {
            attempt,
            logical_node,
            supervisor_addr,
            driver_slot,
            producer,
            edge_channel,
            runtime: EdgeRuntime::new(NodeId(attempt)),
            arena: boot_arena(NodeId(attempt)),
            worker: DemoWorkerPort::default(),
            edge_id: None,
            acked_ready_sent: false,
        }
    }

    fn send_ack(&self, outcome: &str) {
        let Some(driver) = self.driver_slot.get() else {
            return;
        };
        let ack = EdgeAck {
            attempt: self.attempt,
            edge_id: self.edge_id.map(|edge| edge.0).unwrap_or(0),
            outcome: outcome.to_owned(),
            at_ms: unix_ms(SystemTime::now()),
        };
        let Ok(bytes) = serde_json::to_vec(&ack) else {
            return;
        };
        driver.send_tagged_gossip(self.supervisor_addr.clone(), EDGE_ACK_TAG.as_bytes(), bytes);
    }

    fn submit_observation(&self, observation: &Observation) {
        let payload = self.observation_json(observation);
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            self.producer.submit_bytes(self.edge_channel, bytes);
        }
    }

    fn observation_json(&self, observation: &Observation) -> serde_json::Value {
        use serde_json::json;
        let base = |kind: &str| {
            json!({
                "at_ms": unix_ms(SystemTime::now()),
                "node": self.logical_node,
                "attempt": self.attempt,
                "edge": self.edge_id.map(|edge| edge.0),
                "kind": kind,
            })
        };
        match observation {
            Observation::StreamArrived { edge_id, stream_id } => {
                let mut value = base("stream-arrived");
                value["edge"] = json!(edge_id.0);
                value["stream"] = json!(stream_id.0);
                value
            }
            Observation::BytesRead {
                edge_id,
                stream_id,
                byte_count,
            } => {
                let mut value = base("bytes");
                value["edge"] = json!(edge_id.0);
                value["stream"] = json!(stream_id.0);
                value["bytes"] = json!(byte_count);
                value
            }
            Observation::IngressRingWrite {
                edge_id,
                ring_id,
                object_id,
                sequence,
                extent,
                ..
            } => {
                let mut value = base("ingress-write");
                value["edge"] = json!(edge_id.0);
                value["ring"] = json!(ring_id.0);
                value["object"] = json!(object_id);
                value["sequence"] = json!(sequence);
                value["extent"] = json!(extent);
                value
            }
            Observation::ObjectLoaded {
                edge_id,
                object: loaded,
                ..
            } => {
                let mut value = base("object-loaded");
                value["edge"] = json!(edge_id.0);
                value["object"] = json!(loaded.object_id);
                value["sequence"] = json!(loaded.sequence);
                value
            }
            Observation::ObjectFailed { edge_id, object_id } => {
                let mut value = base("object-failed");
                value["edge"] = json!(edge_id.0);
                if let Some(object) = object_id {
                    value["object"] = json!(object);
                }
                value
            }
            Observation::EdgeReady { edge_id, direction } => {
                let mut value = base("edge-ready");
                value["edge"] = json!(edge_id.0);
                value["direction"] = json!(format!("{direction:?}"));
                value
            }
            Observation::EdgeFaulted { edge_id, reason } => {
                let mut value = base("edge-faulted");
                value["edge"] = json!(edge_id.0);
                value["reason"] = json!(format!("{reason:?}"));
                value
            }
            Observation::EdgeStopped { edge_id } => {
                let mut value = base("edge-stopped");
                value["edge"] = json!(edge_id.0);
                value
            }
        }
    }

    /// One poll pass: drive the runtime, mirror observations, ack
    /// readiness/fault once (or again if the runtime was rebuilt).
    fn pump(&mut self) {
        let Some(driver) = self.driver_slot.get().cloned() else {
            return;
        };
        if self.edge_id.is_none() {
            return;
        }
        let mut transport = DriverTransport(driver);
        let result = self
            .runtime
            .poll(&mut transport, &mut self.arena, &mut self.worker);
        let observations = self.runtime.take_observations();
        let mut ready = false;
        let mut fault: Option<String> = result.err();
        for observation in &observations {
            self.submit_observation(observation);
            match observation {
                Observation::EdgeReady { .. } => ready = true,
                Observation::EdgeFaulted { reason, .. } => {
                    fault = Some(format!("edge faulted: {reason:?}"));
                }
                _ => {}
            }
        }
        if ready && !self.acked_ready_sent {
            self.acked_ready_sent = true;
            self.send_ack("ready");
        }
        if let Some(fault) = fault {
            self.send_ack(&format!("fault:{fault}"));
        }
    }
}

impl swactor::actor::ActorInterface for NodeEdgeAgent {
    type Incoming = NodeEdgeMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &swactor::actor::Ctx, msg: NodeEdgeMsg) {
        match msg {
            NodeEdgeMsg::Provision(provision) => {
                if self.attempt != provision.attempt {
                    // Stale provision (replaced attempt): let it drop.
                    return;
                }
                match self.edge_id {
                    Some(current) if current.0 == provision.edge_id => {
                        // Duplicate retry: idempotent re-ack.
                        if self.acked_ready_sent {
                            self.send_ack("ready");
                        }
                    }
                    Some(current) => {
                        // Supervisor replaced the session: rebuild inbound.
                        self.submit_observation(&Observation::EdgeStopped { edge_id: current });
                        self.edge_id = Some(EdgeId(provision.edge_id));
                        self.runtime = EdgeRuntime::new(NodeId(self.attempt));
                        self.arena = boot_arena(NodeId(self.attempt));
                        self.worker = DemoWorkerPort::default();
                        self.acked_ready_sent = false;
                        self.runtime.establish_inbound(
                            ProvisionRx {
                                run_id: RunId(1),
                                edge_id: EdgeId(provision.edge_id),
                                local_node_id: NodeId(self.attempt),
                                object_spec: demo_object_spec(),
                                ring_spec: demo_ring_spec(),
                            },
                            demo_parse_spec(),
                        );
                        self.pump();
                    }
                    None => {
                        self.edge_id = Some(EdgeId(provision.edge_id));
                        self.runtime.establish_inbound(
                            ProvisionRx {
                                run_id: RunId(1),
                                edge_id: EdgeId(provision.edge_id),
                                local_node_id: NodeId(self.attempt),
                                object_spec: demo_object_spec(),
                                ring_spec: demo_ring_spec(),
                            },
                            demo_parse_spec(),
                        );
                        self.pump();
                    }
                }
            }
            NodeEdgeMsg::Tick => self.pump(),
        }
    }
}

// ─── Supervisor-side ack relay ──────────────────────────────────────────────

/// Decodes `EdgeAck` gossip (via the supervisor's actor bridge) and
/// forwards it to the supervisor actor. Same pattern as the announce
/// relay.
pub struct EdgeAckRelay {
    sender: swactor::runtime::ExternalSender,
    supervisor: Arc<std::sync::OnceLock<swactor::actor::ActorAddress>>,
}

impl EdgeAckRelay {
    pub fn new(
        sender: swactor::runtime::ExternalSender,
        supervisor: Arc<std::sync::OnceLock<swactor::actor::ActorAddress>>,
    ) -> Self {
        Self { sender, supervisor }
    }
}

impl swactor::actor::ActorInterface for EdgeAckRelay {
    type Incoming = EdgeAck;
    type Response = ();

    fn handle(&mut self, _ctx: &swactor::actor::Ctx, ack: EdgeAck) {
        if let Some(addr) = self.supervisor.get() {
            let _ = self
                .sender
                .send_to(addr.clone(), crate::demo::feed::SupervisorMsg::EdgeAck(ack));
        }
    }
}
