//! SWIM host adapter (SIM_SPEC §6.2).
//!
//! Wraps `distribution::swim::SwimNode` and presents the simulator's
//! `Host` trait. Constructs the production state machine from the
//! scenario's `kind_config`, installs a diagnostics emitter shim that
//! turns production `Event`s into `RecordEvent` actions, installs the
//! tier-2 `SwimIntrospect`, and translates the production `NodeAction`
//! enum into the simulator's `Action` enum.
//!
//! The adapter does *not* substitute for any production logic. Every
//! state change, message decode, and snapshot capture goes through the
//! production code; the adapter is pure translation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use distribution::diagnostics::sink::{DynEmitter, EventEmitter};
use distribution::diagnostics::{Event as DiagEvent, SwimIntrospect};
use distribution::swim::node::{NodeAction, SwimNode};
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};
use serde_json::json;

use crate::host::{Action, Host, HostFactory, HostId, HostMessage, KindTag, SnapshotBytes};
use crate::swim_codec::SwimMessage;

// ──────────────────────────────────────────────────────────────────────
// Factory
// ──────────────────────────────────────────────────────────────────────

/// `HostFactory` impl for the `swim` kind. Builds a `SwimHost` from
/// the scenario's `kind_config` and the cluster roster.
pub struct SwimHostFactory;

impl HostFactory for SwimHostFactory {
    fn kind_tag(&self) -> KindTag {
        "swim"
    }
    fn build(
        &self,
        host_id: &str,
        kind_config: &toml::value::Table,
        peers: &[String],
        tick_period_ns: u64,
    ) -> Box<dyn Host> {
        let cfg = SwimHost::config_from_kind(kind_config, tick_period_ns);
        Box::new(SwimHost::new(host_id, peers, cfg))
    }
}

// The §8 host-kind validator for "swim" already lives in
// `scenario::SwimHostKindValidator`; we just need the config
// translation helper here.

// ──────────────────────────────────────────────────────────────────────
// HostId ↔ NodeId mapping
// ──────────────────────────────────────────────────────────────────────

/// Derive a deterministic NodeId from a `HostId` string. SHA-256 of
/// the UTF-8 bytes — cross-architecture-stable, collision-resistant
/// for the cluster sizes the simulator runs at.
pub fn node_id_for(host_id: &str) -> NodeId {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(host_id.as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&digest);
    NodeId(bytes)
}

// ──────────────────────────────────────────────────────────────────────
// Diagnostics emitter shim
// ──────────────────────────────────────────────────────────────────────

/// Buffers `Event`s into a shared `Vec` so the adapter can drain them
/// into `Action::RecordEvent` after each `tick` / `recv` call.
#[derive(Default)]
struct BufferingEmitter {
    buf: Mutex<Vec<DiagEvent>>,
}

impl EventEmitter for BufferingEmitter {
    fn emit_event(&self, event: DiagEvent) {
        self.buf.lock().unwrap().push(event);
    }
}

impl BufferingEmitter {
    fn drain(&self) -> Vec<DiagEvent> {
        std::mem::take(&mut *self.buf.lock().unwrap())
    }
}

// ──────────────────────────────────────────────────────────────────────
// SwimHost
// ──────────────────────────────────────────────────────────────────────

pub struct SwimHost {
    host_id: String,
    node_id: NodeId,
    node: SwimNode,
    introspect: Arc<SwimIntrospect>,
    emitter: Arc<BufferingEmitter>,
    /// HostId lookup for `NodeId` peers the SWIM node emits to. Built
    /// from the cluster roster at construction. `NodeId` only
    /// implements `Hash` / `Eq` upstream, so we key by it directly.
    peer_id_of: HashMap<NodeId, HostId>,
    /// Latest virtual time the host has seen (updated on every
    /// tick/recv). `snapshot()` is `&self` so we need interior
    /// mutability; `AtomicU64` is the simplest `Send`-safe option.
    /// Used to scrub wall-clock fields the production `SwimIntrospect`
    /// stamps into `Tier2SwimState` — §7.1 forbids the simulator's
    /// bundle from carrying the host wall clock.
    last_virtual_ns: AtomicU64,
}

impl SwimHost {
    /// Construct a SWIM host for `host_id` against a known `peer_ids`
    /// roster. The roster is needed so outgoing `NodeAction::*` (which
    /// names peers by `NodeId`) can be translated back into the
    /// engine's `HostId` strings.
    ///
    /// The scenario format declares the full cluster topology
    /// up-front, so the adapter bootstraps the SWIM `MemberList`
    /// with every other peer as Alive at incarnation 0 — equivalent
    /// to a join handshake having already completed. Without this
    /// the SWIM probe cycle has no targets and the host emits no
    /// network traffic.
    pub fn new(host_id: impl Into<String>, peer_ids: &[String], config: SwimConfig) -> Self {
        let host_id = host_id.into();
        let node_id = node_id_for(&host_id);
        let mut node = SwimNode::new(node_id, config);
        let introspect = node.install_introspect();
        let emitter: Arc<BufferingEmitter> = Arc::new(BufferingEmitter::default());
        let dyn_emitter: DynEmitter = emitter.clone();
        node.set_diagnostics(dyn_emitter);
        let mut peer_id_of = HashMap::new();
        for p in peer_ids {
            peer_id_of.insert(node_id_for(p), p.clone());
        }
        // Pre-populate the member list with every other declared peer.
        // We discard the returned NodeActions (the bootstrap is
        // pre-tick-0 simulator setup, not a runtime gossip event).
        let bootstrap_members: Vec<distribution::types::NodeRecord> = peer_ids
            .iter()
            .filter(|p| *p != &host_id)
            .map(|p| distribution::types::NodeRecord {
                node_id: node_id_for(p),
                state: MemberState::Alive,
                incarnation: 0,
            })
            .collect();
        if !bootstrap_members.is_empty() {
            let _ = node.handle_join_response(bootstrap_members);
        }
        // Drain anything the bootstrap may have queued into the
        // diagnostics emitter; the first real tick starts with a
        // clean slate.
        let _ = emitter.drain();
        Self {
            host_id,
            node_id,
            node,
            introspect,
            emitter,
            peer_id_of,
            last_virtual_ns: AtomicU64::new(0),
        }
    }

    /// Default SwimConfig matching the scenario `kind_config` shape.
    /// The conversion of scenario nanoseconds to SwimNode "tick" units
    /// requires the engine's tick period for this host; supply it as
    /// `tick_period_ns` so the integer ratios round consistently.
    pub fn config_from_kind(
        kind_config: &toml::value::Table,
        tick_period_ns: u64,
    ) -> SwimConfig {
        let probe_interval_ns = kind_config
            .get("probe_interval_ns")
            .and_then(|v| v.as_integer())
            .map(|n| n as u64)
            .unwrap_or(tick_period_ns);
        let suspicion_timeout_ns = kind_config
            .get("suspicion_timeout_ns")
            .and_then(|v| v.as_integer())
            .map(|n| n as u64)
            .unwrap_or(probe_interval_ns.saturating_mul(3));
        let probe_timeout_ns = kind_config
            .get("probe_timeout_ns")
            .and_then(|v| v.as_integer())
            .map(|n| n as u64)
            .unwrap_or(probe_interval_ns / 3);
        let indirect_probes = kind_config
            .get("indirect_ping_fanout")
            .and_then(|v| v.as_integer())
            .map(|n| n.max(1) as usize)
            .unwrap_or(3);
        let dead_reprobe_interval_ns = kind_config
            .get("dead_reprobe_interval_ns")
            .and_then(|v| v.as_integer())
            .map(|n| n as u64)
            .unwrap_or(0);
        let unit = tick_period_ns.max(1);
        SwimConfig {
            probe_interval: (probe_interval_ns / unit).max(1),
            probe_timeout: (probe_timeout_ns / unit).max(1),
            indirect_probes,
            suspicion_timeout: (suspicion_timeout_ns / unit).max(1),
            dead_reprobe_interval: dead_reprobe_interval_ns / unit,
            probe_mode: ProbeMode::Periodic,
        }
    }

    pub fn introspect(&self) -> &Arc<SwimIntrospect> {
        &self.introspect
    }

    fn drain_emitter(&self) -> Vec<Action> {
        self.emitter
            .drain()
            .into_iter()
            .map(|ev| Action::RecordEvent {
                kind_tag: "swim".into(),
                event: diag_event_payload(&ev, &self.peer_id_of),
            })
            .collect()
    }

    /// Convert a `NodeAction` from the production state machine into
    /// zero-or-more simulator `Action`s. A new `NodeAction` variant —
    /// say SWIM grows a `SendStateSync` — becomes a non-exhaustive
    /// match here and a compile-time failure, which is the §6.4
    /// "unknown-output is loud" property.
    fn translate(&self, action: NodeAction) -> Vec<Action> {
        match action {
            NodeAction::SendPing { to, sequence, piggyback } => self.send_swim(
                to,
                "swactor_dist::Ping",
                SwimMessage::Ping(distribution::messages::Ping {
                    from: self.node_id,
                    sequence,
                    piggyback,
                }),
            ),
            NodeAction::SendAck { to, sequence, piggyback } => self.send_swim(
                to,
                "swactor_dist::Ack",
                SwimMessage::Ack(distribution::messages::Ack {
                    from: self.node_id,
                    sequence,
                    piggyback,
                }),
            ),
            NodeAction::SendPingReq { relay, target, sequence, piggyback } => self.send_swim(
                relay,
                "swactor_dist::PingReq",
                SwimMessage::PingReq(distribution::messages::PingReq {
                    from: self.node_id,
                    target,
                    sequence,
                    piggyback,
                }),
            ),
            NodeAction::ForwardAck { to, target, sequence, piggyback } => self.send_swim(
                to,
                "swactor_dist::IndirectAck",
                SwimMessage::IndirectAck(distribution::messages::IndirectAck {
                    target,
                    sequence,
                    piggyback,
                }),
            ),
            NodeAction::SendJoinResponse { to, members } => self.send_swim(
                to,
                "swactor_dist::JoinResponse",
                SwimMessage::JoinResponse(distribution::messages::JoinResponse { members }),
            ),
            NodeAction::MembershipChanged { .. } => {
                // Per SIM_SPEC §1, §6.4, and §9.2 the simulator may not
                // emit event kinds production does not. `NodeAction::
                // MembershipChanged` is a state-machine *output* (used
                // by production to wire SWIM into Kademlia), not a
                // diagnostic event — production emits no
                // `MembershipChanged` `Event` variant. The diagnostics
                // emitter already publishes a `SwimTransition` for
                // every state change, which `drain_emitter` records
                // as a `state_transition` event in the bundle. So
                // we deliberately emit no `Action` here: the
                // membership signal is preserved via production's own
                // diagnostic. The arm is matched (rather than `_`-ed)
                // so a future `NodeAction` variant is a compile-time
                // failure here — the §6.4 "unknown-output is loud"
                // property.
                Vec::new()
            }
        }
    }

    fn send_swim(&self, to: NodeId, kind_tag: &'static str, msg: SwimMessage) -> Vec<Action> {
        let Some(host_id) = self.peer_id_of.get(&to).cloned() else {
            // Production would log the unknown peer; the simulator panics
            // because §6.4 demands no silent fallback.
            panic!(
                "SwimHost {} tried to send {kind_tag} to unregistered NodeId {:?}",
                self.host_id, to
            );
        };
        // Also emit a structured record so the assertion evaluator's
        // `message_send` schema fires; the bandwidth contract lives in
        // the `encoded` length passed to the network.
        let encoded = msg.encode();
        let bytes_len = encoded.len() as u64;
        vec![
            Action::RecordEvent {
                kind_tag: "swim".into(),
                event: serde_json::to_vec(&json!({
                    "kind": "message_send",
                    "from": self.host_id,
                    "to": host_id,
                    "message_kind": kind_tag,
                    "bytes": bytes_len,
                }))
                .unwrap(),
            },
            Action::Send { to: host_id, encoded },
        ]
    }

    fn collect_actions(&mut self, node_actions: Vec<NodeAction>) -> Vec<Action> {
        let mut out = Vec::new();
        for na in node_actions {
            out.extend(self.translate(na));
        }
        out.extend(self.drain_emitter());
        out
    }
}

impl Host for SwimHost {
    fn id(&self) -> &str {
        &self.host_id
    }
    fn kind_tag(&self) -> KindTag {
        "swim"
    }

    fn tick(&mut self, now_ns: u64) -> Vec<Action> {
        self.last_virtual_ns.store(now_ns, Ordering::Relaxed);
        let actions = self.node.tick();
        self.collect_actions(actions)
    }

    fn recv(&mut self, message: HostMessage, now_ns: u64) -> Vec<Action> {
        self.last_virtual_ns.store(now_ns, Ordering::Relaxed);
        match message {
            HostMessage::App(bytes) => {
                // The engine doesn't know the SWIM message kind; we
                // decode by sniffing for the known JSON shapes. Tag-
                // based dispatch would be cleaner but requires the
                // engine to pass the kind tag through; for the MVP
                // SwimNode handlers are tolerant enough to dispatch
                // by JSON shape.
                let actions = dispatch_swim_recv(&mut self.node, self.node_id, &bytes);
                self.collect_actions(actions)
            }
            HostMessage::TimerFired { .. } => {
                // SwimNode is tick-driven, not timer-driven. Drain any
                // pending diagnostics anyway.
                self.drain_emitter()
            }
            HostMessage::SendFailed { to, .. } => {
                let Some(node_id) = self.peer_id_of.iter().find_map(|(nid, h)| {
                    if h == &to { Some(*nid) } else { None }
                }) else {
                    return self.drain_emitter();
                };
                let actions = self.node.report_send_failure(node_id);
                self.collect_actions(actions)
            }
            // RELAY_SPEC §5.3 — the SWIM host kind has no
            // `WorkerExit` semantics. The engine guards this at
            // `dispatch_mutation` (aborting before the envelope is
            // even routed), so reaching this arm means the engine's
            // gate is broken or the host has been reused outside the
            // stage-host invariant. Panic loudly rather than silently
            // dropping; silent fallback is the bug class the
            // simulator exists to prevent.
            HostMessage::WorkerExit { .. } => {
                panic!(
                    "SWIM host kind cannot receive WorkerExit; the engine's WorkerExitOnWrongKind gate is broken"
                );
            }
        }
    }

    fn snapshot(&self) -> SnapshotBytes {
        use distribution::diagnostics::snapshot::SwimIntrospector;
        let tier2 = self.introspect.capture();
        let virtual_ns = self.last_virtual_ns.load(Ordering::Relaxed);
        serde_json::to_vec(&snapshot_payload(&tier2, &self.host_id, virtual_ns))
            .expect("Tier2SwimState serialises to JSON by construction")
    }
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

/// Project a production `Event` into the MVP evaluator schema. The
/// match is exhaustive over `DiagEvent`: a future production variant
/// is a compile-time failure here, which is the §6.4 "SWIM emits no
/// novel kinds" property — structural, not positional. Variants the
/// MVP evaluator doesn't have a schema for fall through to a
/// `diag_event` envelope that carries the production `type` tag
/// verbatim, so the bundle still records them.
fn diag_event_payload(ev: &DiagEvent, peer_id_of: &HashMap<NodeId, HostId>) -> Vec<u8> {
    // Resolve a `NodeId` to the simulator's `HostId` string so the
    // evaluator's host_id-keyed assertions can match. Falls back to
    // hex when the NodeId is not in the cluster roster — production
    // emits NodeId-hex natively, so this preserves the "honest about
    // absence" pattern (the bundle reader sees a hex string instead
    // of a name when the peer is unknown to the simulator).
    let label = |id: &NodeId| -> String {
        peer_id_of
            .get(id)
            .cloned()
            .unwrap_or_else(|| hex_node_id(id))
    };
    let v = match ev {
        DiagEvent::SwimTransition { peer, from, to, reason } => json!({
            "kind": "state_transition",
            "peer": hex_node_id(peer),
            "from": format!("{from:?}"),
            "to": format!("{to:?}"),
            "reason": reason,
        }),
        // Coverage 2.6: SWIM probe lifecycle. Dedicated `kind` strings
        // so the bundle reader (and the evaluator's
        // `no_flap_while_probes_ok` precondition) can match without
        // unpacking the generic `diag_event` envelope.
        //
        // `target` is the probed peer's `HostId` string (looked up
        // through `peer_id_of`), consistent with the simulator's
        // `message_send` convention. Production emits `NodeId`-hex
        // natively; the simulator translates at the boundary so the
        // evaluator can compare against assertion `peer` strings that
        // name peers by their scenario-declared host id. This is the
        // same translation pattern `message_send` uses — schema
        // parity per `SIM_SPEC.md §9.2` holds at the field-name level
        // (`target`, `sequence`, `probe_kind`, `budget_ticks`).
        DiagEvent::SwimProbeSent { target, sequence, kind } => json!({
            "kind": "swim_probe_sent",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
        }),
        DiagEvent::SwimProbeAcked { target, sequence, kind } => json!({
            "kind": "swim_probe_acked",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
        }),
        DiagEvent::SwimProbeTimedOut { target, sequence, kind, budget_ticks } => json!({
            "kind": "swim_probe_timed_out",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
            "budget_ticks": budget_ticks,
        }),
        // Every other production `Event` variant — iroh dial events,
        // metadata, message accounting, probes, errors, custom —
        // surfaces under one `diag_event` kind, carrying production's
        // own `type` discriminator inside the payload. The arms are
        // listed individually so a new production variant fails to
        // compile here rather than silently routing through a default
        // arm.
        DiagEvent::DialStarted { .. }
        | DiagEvent::DialOutcome { .. }
        | DiagEvent::IrohConnTypeChanged { .. }
        | DiagEvent::RelayChanged { .. }
        | DiagEvent::RelaySessionStateChanged { .. }
        | DiagEvent::RelaySessionOpened { .. }
        | DiagEvent::RelaySessionClosed { .. }
        | DiagEvent::SubprocessSpawned { .. }
        | DiagEvent::SubprocessExited { .. }
        | DiagEvent::GossipReceived { .. }
        | DiagEvent::SwimMetadataSent { .. }
        | DiagEvent::SwimMetadataReceived { .. }
        | DiagEvent::ConnectionCacheHit { .. }
        | DiagEvent::ConnectionCacheMiss { .. }
        | DiagEvent::ConnectionCacheInvalidated { .. }
        | DiagEvent::NodeMapUpdate { .. }
        | DiagEvent::MessageSent { .. }
        | DiagEvent::MessageReceived { .. }
        | DiagEvent::ProbeSent { .. }
        | DiagEvent::ProbeReceived { .. }
        | DiagEvent::InferenceResponseSent { .. }
        | DiagEvent::Error { .. }
        | DiagEvent::Custom { .. } => {
            let inner = serde_json::to_value(ev).unwrap_or(serde_json::Value::Null);
            json!({ "kind": "diag_event", "payload": inner })
        }
    };
    serde_json::to_vec(&v).unwrap()
}

fn hex_node_id(id: &NodeId) -> String {
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Try every SWIM message shape until one parses. The cluster only
/// speaks SWIM here, so a parse failure across all kinds is an unknown
/// message; we route it to a panic per §6.4 "no silent fallback".
fn dispatch_swim_recv(
    node: &mut SwimNode,
    self_node_id: NodeId,
    bytes: &[u8],
) -> Vec<NodeAction> {
    // Try the kinds in the order the engine sees them. Use serde_json
    // sniffing — production multiplexes by `type_tag` over the wire,
    // but our engine envelopes are opaque bytes, so this is the
    // cleanest way to recover the kind for the MVP.
    if let Ok(m) = serde_json::from_slice::<distribution::messages::Ping>(bytes) {
        return node.handle_ping(m.from, m.sequence, &m.piggyback);
    }
    if let Ok(m) = serde_json::from_slice::<distribution::messages::Ack>(bytes) {
        return node.handle_ack(m.from, m.sequence, &m.piggyback);
    }
    if let Ok(m) = serde_json::from_slice::<distribution::messages::PingReq>(bytes) {
        // Skip messages we routed to ourselves as relay if the target
        // is also us (a degenerate config); production wouldn't send
        // that, but the simulator can.
        return node.handle_ping_req(m.from, m.target, m.sequence, &m.piggyback);
    }
    if let Ok(m) = serde_json::from_slice::<distribution::messages::IndirectAck>(bytes) {
        return node.handle_indirect_ack(m.target, m.sequence, &m.piggyback);
    }
    if let Ok(m) = serde_json::from_slice::<distribution::messages::JoinRequest>(bytes) {
        let _ = self_node_id;
        return node.handle_join_request(m.from);
    }
    if let Ok(m) = serde_json::from_slice::<distribution::messages::JoinResponse>(bytes) {
        return node.handle_join_response(m.members);
    }
    panic!(
        "SwimHost recv: unrecognised SWIM message ({} bytes). The codec list in swim_codec.rs and the dispatch list in swim_host.rs must stay in sync.",
        bytes.len()
    );
}

/// Map a captured `Tier2SwimState` into the MVP evaluator schema
/// (`members: {peer_id: {state, incarnation}}, self_incarnation`)
/// while keeping production's tier-2 fields available under a nested
/// key. The full production payload is the source of truth; the MVP
/// schema is a projection the §10 evaluator already understands.
///
/// §7.1 compliance: `Tier2SwimState` is stamped by the production
/// `SwimIntrospect` with `wall_ms_now()` values
/// (`crates/distribution/src/diagnostics/swim_introspect.rs`). §7.1
/// forbids any read of the host wall clock from the simulator's
/// bundle path, so every wall-clock-typed field is overwritten with
/// a virtual-time value (or `null` for optional fields) before the
/// payload is serialised. The production *schema* is preserved
/// verbatim — only the polluted timestamps are replaced.
fn snapshot_payload(
    tier2: &distribution::diagnostics::snapshot::Tier2SwimState,
    self_id: &str,
    virtual_ns: u64,
) -> serde_json::Value {
    let mut members = serde_json::Map::new();
    for peer in &tier2.peers {
        members.insert(
            peer.peer_node_id_hex.clone(),
            json!({
                "state": format!("{:?}", peer.state),
                "incarnation": peer.incarnation,
            }),
        );
    }
    let virtual_ms = virtual_ns / 1_000_000;
    // Render the production tier-2 blob and then replace the
    // wall-clock-tainted fields. Doing it on the rendered Value
    // keeps the schema (key names, key order, nesting) identical to
    // production while letting us swap values.
    let mut tier2_value =
        serde_json::to_value(tier2).expect("Tier2SwimState serialises by construction");
    if let Some(obj) = tier2_value.as_object_mut() {
        obj.insert("scraped_at_ms".into(), serde_json::Value::from(virtual_ms));
        if let Some(peers) = obj.get_mut("peers").and_then(|v| v.as_array_mut()) {
            for p in peers {
                if let Some(p_obj) = p.as_object_mut() {
                    for field in [
                        "last_ping_sent_at_ms",
                        "last_ack_received_at_ms",
                        "last_ping_received_at_ms",
                        "suspect_started_at_ms",
                    ] {
                        if p_obj.contains_key(field) {
                            p_obj.insert(field.into(), serde_json::Value::Null);
                        }
                    }
                }
            }
        }
        if let Some(msgs) = obj.get_mut("recent_messages").and_then(|v| v.as_array_mut()) {
            for m in msgs {
                if let Some(m_obj) = m.as_object_mut() {
                    if m_obj.contains_key("at_ms") {
                        m_obj.insert("at_ms".into(), serde_json::Value::from(virtual_ms));
                    }
                }
            }
        }
    }
    json!({
        "self_id": self_id,
        "self_node_id_hex": tier2.self_node_id_hex,
        "members": members,
        "self_incarnation": tier2.self_incarnation,
        "tier2": tier2_value,
    })
}

