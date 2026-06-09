//! Actor-runtime behavioral tests for `SwimActor` (SWIM_ACTOR_SPEC §2–§4).
//!
//! These drive real `SwimActor`s inside a swactor `Runtime` and observe ONLY the
//! spec boundary (§6.3): the `MembershipChanged` notification stream delivered to
//! a subscriber inbox. They never inspect probe phases, queue contents, or the
//! `NodeAction` enum — they exercise the actorization seams end to end:
//!   - the single `SwimIn` Incoming enum (§4.1),
//!   - the clock as `Tick{now}` (§4.2),
//!   - the `NodeId`→`ActorAddress` Binding (`PeerDirectory`, §3.2),
//!   - send-failure-as-message driving detection (§4.3),
//!   - `Subscribe` + `MembershipChanged` as the sole observable (§6.3).
//!
//! Eventuality is pinned by driving to a converge-or-timeout condition, never a
//! fixed tick count tuned to pass.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::std::StdExtension;

use distribution::swim::actor::{
    MembershipChanged, PeerDirectory, SharedPeerDirectory, SwimActor, SwimIn,
};
use distribution::swim::member_list::MemberList;
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};

const TICK: Duration = Duration::from_millis(10);

fn id(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// A brisk Periodic config so detection completes in a manageable number of
/// rounds: detection ≈ 2·probe_timeout (direct+indirect) + suspicion_timeout.
fn brisk_config() -> SwimConfig {
    SwimConfig {
        probe_interval: TICK,
        probe_timeout: TICK * 2,
        indirect_probes: 2,
        suspicion_timeout: TICK * 3,
        dead_reprobe_interval: Duration::ZERO,
        probe_mode: ProbeMode::Periodic,
        lifeguard: None,
    }
}

/// A cluster of `n` `SwimActor`s in one runtime, each with its own
/// `MembershipChanged` subscriber inbox, and a shared Binding.
struct ActorCluster {
    rt: Runtime,
    ids: Vec<NodeId>,
    addrs: Vec<swactor::actor::ActorAddress>,
    inboxes: Vec<swactor::runtime::Inbox<MembershipChanged>>,
    dir: SharedPeerDirectory,
    streams: Vec<Vec<MembershipChanged>>,
    clock: Instant,
    step: u64,
}

impl ActorCluster {
    /// Spawn `n` actors. Nodes `1..n` join via node 0 (the seed). Returns once
    /// the join requests have been issued (not yet converged).
    fn new(n: usize) -> Self {
        let rt = Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
        let dir = SharedPeerDirectory::new();
        let now = Instant::now();
        let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();

        let mut addrs = Vec::new();
        let mut inboxes = Vec::new();
        for &nid in &ids {
            let addr = rt
                .spawn(SwimActor::new(nid, brisk_config(), now, Arc::new(dir.clone())))
                .expect("spawn SwimActor");
            // Generation 0 binding; the actor resolves NodeId→ActorAddress here.
            dir.bind(nid, addr, 0);
            let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
            rt.send_to(addr, SwimIn::Subscribe { observer: *inbox.addr() }).unwrap();
            addrs.push(addr);
            inboxes.push(inbox);
        }
        let mut c = ActorCluster {
            rt,
            ids,
            addrs,
            inboxes,
            dir,
            streams: vec![Vec::new(); n],
            clock: now,
            step: 0,
        };
        // Settle the Subscribe messages.
        c.pump(4);
        // Nodes 1..n join via the seed (node 0).
        let seed = c.ids[0];
        for i in 1..n {
            let addr = c.addrs[i];
            c.rt.send_to(addr, SwimIn::Join { seeds: vec![seed] }).unwrap();
        }
        c.pump(8);
        c.drain();
        c
    }

    fn pump(&self, n: usize) {
        for _ in 0..n {
            self.rt.tick();
        }
    }

    /// Drain every inbox into its per-node `MembershipChanged` stream.
    fn drain(&mut self) {
        for (i, inbox) in self.inboxes.iter().enumerate() {
            while let Some(m) = inbox.try_recv() {
                self.streams[i].push(m);
            }
        }
    }

    /// Advance the clock and deliver a `Tick{now}` to every actor whose NodeId
    /// is still bound (an unbound node is unreachable — genuinely silent). Then
    /// pump the runtime so the resulting sends/acks/gossip settle, and drain.
    fn round(&mut self) {
        self.step += 1;
        self.clock += TICK;
        let now = self.clock;
        for i in 0..self.ids.len() {
            if self.dir.resolve(&self.ids[i]).is_some() {
                let _ = self.rt.send_to(self.addrs[i], SwimIn::Tick { now });
            }
        }
        // Enough internal ticks for a ping→ack and a relay chain plus the
        // notification hop to settle within one round.
        self.pump(8);
        self.drain();
    }

    fn run_until<F: Fn(&ActorCluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
        if cond(self) {
            return true;
        }
        for _ in 0..cap {
            self.round();
            if cond(self) {
                return true;
            }
        }
        false
    }

    /// Reconstruct node `observer`'s membership view by folding its
    /// `MembershipChanged` stream under the §7 merge rule (the Goal-7 fold).
    fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
        // A sentinel self-id that never appears in any stream, so every entry is
        // stored (self is never stored, §7 inv. 4).
        let mut ml = MemberList::new(id(250));
        for m in &self.streams[observer] {
            ml.apply(m.node_id, m.state, m.incarnation);
        }
        ml.all_members().iter().map(|e| (e.node_id.0, e.state)).collect()
    }

    /// Does `observer`'s folded view show `subject` in `state`?
    fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
        self.folded_view(observer).get(&subject.0) == Some(&state)
    }

    /// Every node's folded stream shows every other node Alive.
    fn all_converged_alive(&self) -> bool {
        let n = self.ids.len();
        (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
    }
}

#[test]
fn actors_converge_to_a_shared_alive_view_via_the_membership_changed_stream() {
    // §6.3 + §3.2 + §4.2: actors that join through the seed reach a shared Alive
    // view, observed solely through the MembershipChanged notification stream
    // each subscriber receives — the actorization seams carry gossip end to end.
    let mut c = ActorCluster::new(4);
    assert!(
        c.run_until(400, |c| c.all_converged_alive()),
        "actors did not converge to a shared Alive view through MembershipChanged"
    );
}

#[test]
fn an_unreachable_actor_is_detected_dead_by_survivors() {
    // §4.3 + §9: once a node becomes unreachable (its Binding is dropped, so a
    // survivor's probe genuinely never gets an Ack), the survivors must converge
    // on it being Dead — real detection driven by SendFailed + probe timeout,
    // not an injected death.
    let mut c = ActorCluster::new(4);
    assert!(c.run_until(400, |c| c.all_converged_alive()), "precondition: must converge");

    // Genuinely silence node 3: drop its binding so nobody can deliver to it, and
    // stop ticking it (round() skips unbound nodes).
    let dead = 3usize;
    c.dir.unbind(&id(dead as u8));

    let detected = c.run_until(2000, |c| {
        (0..4).all(|o| o == dead || c.sees(o, id(dead as u8), MemberState::Dead))
    });
    assert!(detected, "survivors must converge on the unreachable node being Dead");
}
