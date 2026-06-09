//! Layer 2 — behavioral tests (in-process). Goals 1, 2, 3, 6, 7 of
//! BEHAVIORAL_TEST_SPEC.md.
//!
//! These observe ONLY the boundary the spec draws: each node's membership view
//! (`members()`) and the `MembershipChanged` notification stream. They never
//! inspect probe phases, piggyback bytes, queue contents, timers, or action
//! enums in an assertion — so they survive any rewrite that keeps the boundary.
//! The harness DOES route `Ping`/`Ack`/`PingReq`/`IndirectAck`/`JoinResponse`
//! between nodes, but that is the simulated transport, not a test assertion.
//!
//! Eventuality is pinned by driving the cluster to quiescence — repeated gossip
//! rounds until several consecutive rounds produce no new `MembershipChanged` —
//! and then asserting on the settled view. No fixed round count is tuned to pass.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use distribution::swim::dissemination::{membership_update, DisseminationQueue};
use distribution::swim::member_list::MemberList;
use distribution::swim::node::{NodeAction, SwimNode};
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};

const TICK: Duration = Duration::from_millis(10);

fn t(n: u64) -> Duration {
    TICK * n as u32
}

fn id(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// A brisk Periodic config so detection completes in a manageable number of
/// rounds: detection ≈ 2·probe_timeout (direct+indirect) + suspicion_timeout.
fn behavioral_config(dead_reprobe: Duration) -> SwimConfig {
    SwimConfig {
        probe_interval: t(1),
        probe_timeout: t(2),
        indirect_probes: 2,
        suspicion_timeout: t(3),
        dead_reprobe_interval: dead_reprobe,
        probe_mode: ProbeMode::Periodic,
        lifeguard: None,
    }
}

/// A single update about `target`, encoded as a wire piggyback frame.
fn piggyback_about(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(membership_update(target, state, incarnation), 5);
    q.pack_piggyback(10)
}

type View = BTreeMap<[u8; 32], (MemberState, u64)>;

/// Fold a `MembershipChanged` stream into a view using the §7 merge rule — the
/// dominating update per node. This is the Goal-7 reconstruction.
fn fold_stream(stream: &[(NodeId, MemberState, u64)]) -> View {
    // self id is a sentinel that never appears in any stream, so every entry is
    // stored (self is never stored, §7 inv. 4).
    let mut ml = MemberList::new(id(250));
    for &(nid, st, inc) in stream {
        ml.apply(nid, st, inc);
    }
    ml.all_members().iter().map(|e| (e.node_id.0, (e.state, e.incarnation))).collect()
}

/// In-process cluster of `SwimNode`s with simulated, correct-by-construction
/// transport. Records each node's emitted `MembershipChanged` stream.
struct Cluster {
    ids: Vec<NodeId>,
    nodes: Vec<SwimNode>,
    clock: Instant,
    notifications: Vec<Vec<(NodeId, MemberState, u64)>>,
}

impl Cluster {
    /// `n` nodes; nodes 1..n join via node 0 (the seed).
    fn new(n: usize, config: SwimConfig) -> Self {
        let now = Instant::now();
        let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();
        let nodes = ids.iter().map(|&nid| SwimNode::new(nid, config.clone(), now)).collect();
        let mut c = Cluster { ids, nodes, clock: now, notifications: vec![Vec::new(); n] };
        for i in 1..n {
            let acts = c.nodes[0].handle_join_request(c.ids[i]);
            c.record(0, &acts);
            c.route(0, acts, &[]);
        }
        c
    }

    fn index_of(&self, nid: NodeId) -> Option<usize> {
        self.ids.iter().position(|x| *x == nid)
    }

    fn record(&mut self, origin: usize, acts: &[NodeAction]) {
        for a in acts {
            if let NodeAction::MembershipChanged { node_id, state, incarnation } = a {
                self.notifications[origin].push((*node_id, *state, *incarnation));
            }
        }
    }

    /// Deliver every network action `origin` produced to its target, recursively
    /// routing each response. `excluded` nodes neither send nor receive (genuine
    /// silence). MembershipChanged is captured by `record`, never delivered.
    fn route(&mut self, origin: usize, acts: Vec<NodeAction>, excluded: &[usize]) {
        if excluded.contains(&origin) {
            return;
        }
        for a in acts {
            match a {
                NodeAction::SendPing { to, sequence, piggyback } => {
                    if let Some(t) = self.index_of(to) {
                        if t != origin && !excluded.contains(&t) {
                            let from = self.ids[origin];
                            let resp = self.nodes[t].handle_ping(from, sequence, &piggyback);
                            self.record(t, &resp);
                            self.route(t, resp, excluded);
                        }
                    }
                }
                NodeAction::SendAck { to, sequence, piggyback } => {
                    if let Some(t) = self.index_of(to) {
                        if t != origin && !excluded.contains(&t) {
                            let from = self.ids[origin];
                            let resp = self.nodes[t].handle_ack(from, sequence, &piggyback);
                            self.record(t, &resp);
                            self.route(t, resp, excluded);
                        }
                    }
                }
                NodeAction::SendPingReq { relay, target, sequence, piggyback } => {
                    if let Some(t) = self.index_of(relay) {
                        if t != origin && !excluded.contains(&t) {
                            let from = self.ids[origin];
                            let resp = self.nodes[t].handle_ping_req(from, target, sequence, &piggyback);
                            self.record(t, &resp);
                            self.route(t, resp, excluded);
                        }
                    }
                }
                NodeAction::ForwardAck { to, target, sequence, piggyback } => {
                    if let Some(t) = self.index_of(to) {
                        if t != origin && !excluded.contains(&t) {
                            let resp = self.nodes[t].handle_indirect_ack(target, sequence, &piggyback);
                            self.record(t, &resp);
                            self.route(t, resp, excluded);
                        }
                    }
                }
                NodeAction::SendJoinResponse { to, members } => {
                    if let Some(t) = self.index_of(to) {
                        if t != origin && !excluded.contains(&t) {
                            let resp = self.nodes[t].handle_join_response(members);
                            self.record(t, &resp);
                            self.route(t, resp, excluded);
                        }
                    }
                }
                NodeAction::MembershipChanged { .. } => {}
            }
        }
    }

    fn gossip_round(&mut self, excluded: &[usize]) {
        self.clock += TICK;
        for i in 0..self.nodes.len() {
            if excluded.contains(&i) {
                continue;
            }
            let now = self.clock;
            let acts = self.nodes[i].tick(now);
            self.record(i, &acts);
            self.route(i, acts, excluded);
        }
    }

    /// Converge-or-timeout: drive rounds until the eventual `cond` holds, or
    /// `cap` rounds elapse. Returns whether `cond` was reached. This pins
    /// eventuality without a fixed count — and, unlike "no new notifications",
    /// it does not mistake an in-flight probe timeout for a settled cluster.
    fn run_until<F: Fn(&Cluster) -> bool>(&mut self, excluded: &[usize], cap: usize, cond: F) -> bool {
        if cond(self) {
            return true;
        }
        for _ in 0..cap {
            self.gossip_round(excluded);
            if cond(self) {
                return true;
            }
        }
        false
    }

    fn state_of(&self, observer: usize, subject: NodeId) -> Option<MemberState> {
        self.nodes[observer].members().get(&subject).map(|e| e.state)
    }

    /// Every node sees every other node as Alive (shared converged view).
    fn all_converged_alive(&self) -> bool {
        let n = self.nodes.len();
        (0..n).all(|o| {
            (0..n).all(|s| o == s || self.state_of(o, id(s as u8)) == Some(MemberState::Alive))
        })
    }

    /// Every non-excluded node sees `subject` in `state`.
    fn survivors_see(&self, excluded: &[usize], subject: NodeId, state: MemberState) -> bool {
        (0..self.nodes.len()).all(|o| excluded.contains(&o) || self.state_of(o, subject) == Some(state))
    }

    fn view(&self, observer: usize) -> View {
        self.nodes[observer]
            .members()
            .all_members()
            .iter()
            .map(|e| (e.node_id.0, (e.state, e.incarnation)))
            .collect()
    }
}

// ─── Goal 1 — convergence ────────────────────────────────────────────────────

#[test]
fn goal1_nodes_join_and_reach_a_shared_alive_view() {
    let mut c = Cluster::new(4, behavioral_config(t(0)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "cluster did not converge to a shared Alive view");
    for observer in 0..4 {
        assert_eq!(c.nodes[observer].members().alive_count(), 3, "node {observer} must see all 3 peers alive");
    }
}

// ─── Goal 2 — real detection ─────────────────────────────────────────────────

#[test]
fn goal2_a_truly_silent_node_is_detected_dead_by_survivors() {
    let mut c = Cluster::new(4, behavioral_config(t(0)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "precondition: cluster must converge");

    // Genuinely silence node 3 — it neither ticks nor sends nor receives, so a
    // survivor's probe TRULY times out (not an injected death). Poll until the
    // survivors converge on it being Dead, or time out.
    let dead = 3usize;
    let detected = c.run_until(&[dead], 1000, |c| c.survivors_see(&[dead], id(3), MemberState::Dead));
    assert!(detected, "survivors must converge on the silenced node being Dead within the detection window");
}

// ─── Goal 3 — death is provisional ───────────────────────────────────────────

#[test]
fn goal3_a_silenced_node_resurrects_when_it_answers_again() {
    // dead_reprobe enabled so the partition-heal detector (§9.8) re-probes the
    // Dead node and lets it refute back to Alive.
    let mut c = Cluster::new(4, behavioral_config(t(2)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "precondition: cluster must converge");

    let isolated = 3usize;
    let died = c.run_until(&[isolated], 1000, |c| c.survivors_see(&[isolated], id(3), MemberState::Dead));
    assert!(died, "the silenced node must first be detected Dead by the survivors");

    // Restore the node: it answers probes again, so the reprobe revives it.
    let revived = c.run_until(&[], 1000, |c| c.survivors_see(&[isolated], id(3), MemberState::Alive));
    assert!(revived, "the restored node must resurrect to Alive (death is provisional)");

    // A survivor's notification stream witnessed the full provisional arc.
    let s = if isolated == 0 { 1 } else { 0 };
    let stream = &c.notifications[s];
    let dead_at = stream.iter().position(|(n, st, _)| *n == id(isolated as u8) && *st == MemberState::Dead);
    let alive_after = stream.iter().rposition(|(n, st, _)| *n == id(isolated as u8) && *st == MemberState::Alive);
    assert!(
        matches!((dead_at, alive_after), (Some(d), Some(a)) if a > d),
        "a survivor must witness node {isolated} go Dead then back Alive"
    );
}

// ─── Goal 6 — dissemination reaches everyone ─────────────────────────────────

#[test]
fn goal6_a_single_change_known_to_one_node_infects_every_node() {
    let mut c = Cluster::new(4, behavioral_config(t(0)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "precondition: cluster must converge");

    // A change known to ONLY node 0: a phantom peer reported Dead in a single
    // gossip exchange. No other node has ever heard of this peer, and no node
    // probes a Dead member — so the others can learn it ONLY by multi-hop
    // piggyback infection, never by direct observation.
    let phantom = id(9);
    let from = c.ids[1];
    let pb = piggyback_about(phantom, MemberState::Dead, 5);
    let _ = c.nodes[0].handle_ping(from, 1, &pb);

    let infected = c.run_until(&[], 1000, |c| c.survivors_see(&[], phantom, MemberState::Dead));
    assert!(infected, "the single change must infect every node — dissemination reaches everyone, not just probe partners");
}

// ─── Goal 7 — notification contract ──────────────────────────────────────────

#[test]
fn goal7_membership_changed_stream_reconstructs_the_view() {
    // §6.3 / Move 1: `MembershipChanged` is the sole observable and the stream
    // alone must reconstruct the settled membership view. (May be red until the
    // §10.2 learn-sender notification lands — a peer learned via an inbound Ping
    // mutates the view today without emitting MembershipChanged.)
    let mut c = Cluster::new(4, behavioral_config(t(0)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "cluster did not converge");
    for observer in 0..4 {
        assert_eq!(
            fold_stream(&c.notifications[observer]),
            c.view(observer),
            "node {observer}: folding the MembershipChanged stream must reconstruct the view"
        );
    }
}

#[test]
fn goal7_stream_reconstructs_view_for_a_peer_learned_via_ping() {
    // §6.3 / §10.2: even a peer first learned by receiving its Ping must appear in
    // the notification stream. EXPECTED RED today — learning a sender mutates the
    // view without a MembershipChanged; the actor must route it through the stream.
    let now = Instant::now();
    let mut a = SwimNode::new(id(0), behavioral_config(t(0)), now);
    let mut stream: Vec<(NodeId, MemberState, u64)> = Vec::new();
    for act in a.handle_ping(id(5), 1, &[]) {
        if let NodeAction::MembershipChanged { node_id, state, incarnation } = act {
            stream.push((node_id, state, incarnation));
        }
    }
    let view: View = a
        .members()
        .all_members()
        .iter()
        .map(|e| (e.node_id.0, (e.state, e.incarnation)))
        .collect();
    assert_eq!(
        fold_stream(&stream),
        view,
        "the stream must reconstruct the view even for a peer learned via its Ping (§10.2)"
    );
}

#[test]
fn goal7_no_duplicate_or_coalesced_notifications() {
    // §6.3: one notification per real change — no dupes, no coalescing. Each
    // notification for a given peer must strictly advance (dominate) the previous
    // one for that peer; an identical repeat would be a spurious duplicate.
    let mut c = Cluster::new(4, behavioral_config(t(0)));
    assert!(c.run_until(&[], 500, |c| c.all_converged_alive()), "cluster did not converge");
    for observer in 0..4 {
        let mut last: BTreeMap<[u8; 32], (MemberState, u64)> = BTreeMap::new();
        for &(nid, st, inc) in &c.notifications[observer] {
            if let Some(&(pst, pinc)) = last.get(&nid.0) {
                let advances = inc > pinc || (inc == pinc && st.priority() > pst.priority());
                assert!(
                    advances,
                    "node {observer}: a notification ({st:?},{inc}) did not advance past ({pst:?},{pinc}) — duplicate/coalesced"
                );
            }
            last.insert(nid.0, (st, inc));
        }
    }
}
