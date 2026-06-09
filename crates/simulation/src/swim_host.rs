//! SWIM host adapter (SIM_SPEC §6.2).
//!
//! Wraps `distribution::swim::SwimNode` and presents the simulator's
//! `Host` trait. Constructs the production state machine from the
//! scenario's `kind_config`, installs the production observation hook
//! ([`SwimObserver`]) that turns [`SwimObservation`]s into
//! `RecordEvent` actions, and translates the production `NodeAction`
//! enum into the simulator's `Action` enum.
//!
//! The adapter does *not* substitute for any production logic. Every
//! state change and message decode goes through the production code;
//! snapshots are built from the node's public membership state (the
//! same source the production datastream emitter polls); the adapter
//! is pure translation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use distribution::swim::lifeguard::LifeguardConfig;
use distribution::swim::node::{NodeAction, SwimNode, SwimObservation, SwimObserver};
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
// Observation shim
// ──────────────────────────────────────────────────────────────────────

/// Buffers [`SwimObservation`]s into a shared `Vec` so the adapter can
/// drain them into `Action::RecordEvent` after each `tick` / `recv` call.
#[derive(Default)]
struct BufferingObserver {
    buf: Mutex<Vec<SwimObservation>>,
}

impl BufferingObserver {
    fn drain(&self) -> Vec<SwimObservation> {
        std::mem::take(&mut *self.buf.lock().unwrap())
    }
}

/// The handle installed on the SWIM node; the host keeps the shared
/// buffer end to drain after each call into production code.
struct ObserverHandle(Arc<BufferingObserver>);

impl SwimObserver for ObserverHandle {
    fn observe(&self, observation: SwimObservation) {
        self.0.buf.lock().unwrap().push(observation);
    }
}

// ──────────────────────────────────────────────────────────────────────
// SwimHost
// ──────────────────────────────────────────────────────────────────────

pub struct SwimHost {
    host_id: String,
    node_id: NodeId,
    node: SwimNode,
    observer: Arc<BufferingObserver>,
    /// HostId lookup for `NodeId` peers the SWIM node emits to. Built
    /// from the cluster roster at construction. `NodeId` only
    /// implements `Hash` / `Eq` upstream, so we key by it directly.
    peer_id_of: HashMap<NodeId, HostId>,
    /// Anchor for synthesizing an `Instant` from the simulator's virtual
    /// nanoseconds. SWIM is wall-clock-driven (`SwimProbe::step` takes an
    /// `Instant`); the simulator advances time by passing `base + now_ns`, so
    /// the pure state machine still sees a monotonic, fully-deterministic clock
    /// (`base` cancels out of every deadline comparison).
    base: Instant,
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
        // Anchor SWIM's wall clock at this host's virtual-time base so the
        // simulation stays fully deterministic (`base` cancels out of every
        // deadline comparison: `base + now_ns >= base + interval`).
        let base = Instant::now();
        let mut node = SwimNode::new(node_id, config, base);
        let observer: Arc<BufferingObserver> = Arc::new(BufferingObserver::default());
        node.set_observer(Box::new(ObserverHandle(observer.clone())));
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
        // observer; the first real tick starts with a clean slate.
        let _ = observer.drain();
        Self {
            host_id,
            node_id,
            node,
            observer,
            peer_id_of,
            base,
        }
    }

    /// Synthesize the virtual `Instant` for a given engine timestamp.
    fn virtual_now(&self, now_ns: u64) -> Instant {
        self.base + Duration::from_nanos(now_ns)
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
        // SWIM is wall-clock now: scenario nanoseconds map straight to
        // `Duration`s (no tick-period quantization). `tick_period_ns` is still
        // the probe-interval default above.
        // Lifeguard wiring (§3.6): inherit the production default's adaptive
        // band, anchored at the scenario's suspicion timeout. The sim does not
        // currently parse a per-host lifeguard kind_config — follow-up.
        let suspicion = Duration::from_nanos(suspicion_timeout_ns.max(1));
        let lifeguard = SwimConfig::default().lifeguard.map(|cfg| LifeguardConfig {
            base_suspicion_timeout: suspicion,
            min_suspicion_timeout: suspicion,
            max_suspicion_timeout: suspicion.saturating_mul(6),
            ..cfg
        });
        SwimConfig {
            probe_interval: Duration::from_nanos(probe_interval_ns.max(1)),
            probe_timeout: Duration::from_nanos(probe_timeout_ns.max(1)),
            indirect_probes,
            suspicion_timeout: suspicion,
            dead_reprobe_interval: Duration::from_nanos(dead_reprobe_interval_ns),
            probe_mode: ProbeMode::Periodic,
            lifeguard,
        }
    }

    fn drain_observer(&self) -> Vec<Action> {
        self.observer
            .drain()
            .into_iter()
            .map(|obs| Action::RecordEvent {
                kind_tag: "swim".into(),
                event: observation_payload(&obs, &self.peer_id_of),
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
                // by production to wire SWIM into the directory actor),
                // not an observation. The production observation hook
                // already publishes a `Transition` for every state
                // change, which `drain_observer` records as a
                // `state_transition` event in the bundle. So we
                // deliberately emit no `Action` here: the membership
                // signal is preserved via production's own observation.
                // The arm is matched (rather than `_`-ed) so a future
                // `NodeAction` variant is a compile-time failure here —
                // the §6.4 "unknown-output is loud" property.
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
        out.extend(self.drain_observer());
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
        let actions = self.node.tick(self.virtual_now(now_ns));
        self.collect_actions(actions)
    }

    fn recv(&mut self, message: HostMessage, _now_ns: u64) -> Vec<Action> {
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
                // pending observations anyway.
                self.drain_observer()
            }
            HostMessage::SendFailed { to, .. } => {
                let Some(node_id) = self.peer_id_of.iter().find_map(|(nid, h)| {
                    if h == &to { Some(*nid) } else { None }
                }) else {
                    return self.drain_observer();
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
        serde_json::to_vec(&snapshot_payload(&self.node, &self.host_id, &self.peer_id_of))
            .expect("snapshot serialises to JSON by construction")
    }
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

/// Project a production [`SwimObservation`] into the MVP evaluator
/// schema. The match is exhaustive over `SwimObservation`: a future
/// production variant is a compile-time failure here, which is the
/// §6.4 "SWIM emits no novel kinds" property — structural, not
/// positional.
fn observation_payload(
    obs: &SwimObservation,
    peer_id_of: &HashMap<NodeId, HostId>,
) -> Vec<u8> {
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
    let v = match obs {
        SwimObservation::Transition { peer, from, to, reason } => json!({
            "kind": "state_transition",
            // Translate NodeId → scenario HostId (the same `label`
            // boundary the probe events below, `message_send`, and the
            // snapshot `members` view use) so the evaluator's
            // host_id-keyed assertions (`dead_peer_resurrects_within`,
            // `peer_detected_dead_within`, `no_flap_while_probes_ok`, …)
            // can match. Falls back to hex for a peer not in the roster
            // — the "honest about absence" pattern.
            "peer": label(peer),
            // `from` is `None` when the peer was previously unknown to
            // this node — rendered as "Unknown", the same string the
            // old diagnostics `PeerState::Unknown` produced.
            "from": from
                .map(|s| format!("{s:?}"))
                .unwrap_or_else(|| "Unknown".to_string()),
            "to": format!("{to:?}"),
            "reason": reason,
        }),
        // Coverage 2.6: SWIM probe lifecycle. Dedicated `kind` strings
        // so the bundle reader (and the evaluator's
        // `no_flap_while_probes_ok` precondition) can match without
        // unpacking a generic envelope.
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
        SwimObservation::ProbeSent { target, sequence, kind } => json!({
            "kind": "swim_probe_sent",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
        }),
        SwimObservation::ProbeAcked { target, sequence, kind } => json!({
            "kind": "swim_probe_acked",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
        }),
        SwimObservation::ProbeTimedOut { target, sequence, kind, budget_ticks } => json!({
            "kind": "swim_probe_timed_out",
            "target": label(target),
            "sequence": sequence,
            "probe_kind": kind,
            "budget_ticks": budget_ticks,
        }),
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

/// Build the MVP evaluator snapshot schema
/// (`members: {host_id: {state, incarnation}}, self_incarnation`)
/// from the SWIM node's *public* membership state — the same source
/// the production datastream emitter polls each tick to derive its
/// `MembershipTransition` records. No wall clock is read anywhere on
/// this path, so §7.1 (no host wall clock in the simulator's bundle)
/// holds by construction.
///
/// Members are keyed by the scenario `HostId` (translated from the
/// peer's `NodeId` through `peer_id_of`, falling back to hex for a
/// peer not in the roster) so the §10 evaluator's host_id-named
/// membership assertions (`all_alive_at`, `peer_detected_dead_within`,
/// `convergence_after`, …) can look members up — the same boundary
/// translation `state_transition` / `message_send` apply.
///
/// The old `Tier2SwimState` snapshot (with per-peer `*_at_ms`
/// timestamps and a `recent_messages` ring) had no replacement once
/// the diagnostics introspection subsystem was removed; this
/// member-derived projection carries exactly the fields the §10
/// evaluator consumes (`members`, `self_incarnation`).
fn snapshot_payload(
    node: &SwimNode,
    self_id: &str,
    peer_id_of: &HashMap<NodeId, HostId>,
) -> serde_json::Value {
    let member_list = node.members();
    let mut members = serde_json::Map::new();
    for entry in member_list.all_members() {
        let key = peer_id_of
            .get(&entry.node_id)
            .cloned()
            .unwrap_or_else(|| hex_node_id(&entry.node_id));
        members.insert(
            key,
            json!({
                "state": format!("{:?}", entry.state),
                "incarnation": entry.incarnation,
            }),
        );
    }
    json!({
        "self_id": self_id,
        "self_node_id_hex": hex_node_id(&node.self_id()),
        "members": members,
        "self_incarnation": member_list.self_incarnation(),
    })
}

