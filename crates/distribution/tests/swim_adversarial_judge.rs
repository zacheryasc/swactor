//! Adversarial judge tests — written from the spec, not the code, to probe
//! behavior the shipped suite does not pin tightly.
//!
//! These ride the SAME observable boundary the spec draws (§ behavioral doc,
//! Scope): each node's view as reconstructed from the `MembershipChanged`
//! stream, plus self-incarnation as witnessed by peers. They never match on
//! `NodeAction`/`SwimAction` variants.
//!
//! Targets:
//!   1. Goal 3 — "death is provisional": a silenced node that comes back is
//!      re-probed (§9.8) and resurrected Dead→Alive. The shipped actor test only
//!      kills; it never asserts the resolve.
//!   2. Goal 6 — "dissemination reaches everyone": a death observed by exactly
//!      one node must infect a node that never probed the dead peer, via
//!      multi-hop piggyback (§1 inv. 2, §11).
//!   3. §7.1 storm bound, end to end: under a *flood* of stale Suspect gossip a
//!      victim refutes at most once; its incarnation does not run away.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::std::StdExtension;

use distribution::swim::actor::{
    MembershipChanged, PeerDirectory, SharedPeerDirectory, SwimActor, SwimIn,
};
use distribution::swim::dissemination::{membership_update, DisseminationQueue};
use distribution::swim::member_list::MemberList;
use distribution::swim::node::SwimNode;
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};

const TICK: Duration = Duration::from_millis(10);

fn id(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// Brisk Periodic config with dead-reprobe ENABLED, so the §9.8 partition-heal
/// detector actually re-probes Dead nodes and can resurrect them.
fn reprobe_config() -> SwimConfig {
    SwimConfig {
        probe_interval: TICK,
        probe_timeout: TICK * 2,
        indirect_probes: 2,
        suspicion_timeout: TICK * 3,
        dead_reprobe_interval: TICK * 2,
        probe_mode: ProbeMode::Periodic,
        lifeguard: None,
    }
}

/// A cluster of `n` `SwimActor`s in one runtime, each with its own
/// `MembershipChanged` inbox and a shared Binding. Mirrors the shipped harness
/// but lets the test choose the config and rebind a dropped node.
struct Cluster {
    rt: Runtime,
    ids: Vec<NodeId>,
    addrs: Vec<swactor::actor::ActorAddress>,
    inboxes: Vec<swactor::runtime::Inbox<MembershipChanged>>,
    dir: SharedPeerDirectory,
    streams: Vec<Vec<MembershipChanged>>,
    clock: Instant,
}

impl Cluster {
    fn new(n: usize, config: SwimConfig) -> Self {
        let rt =
            Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
        let dir = SharedPeerDirectory::new();
        let now = Instant::now();
        let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();

        let mut addrs = Vec::new();
        let mut inboxes = Vec::new();
        for &nid in &ids {
            let addr = rt
                .spawn(SwimActor::new(nid, config.clone(), now, Arc::new(dir.clone())))
                .expect("spawn SwimActor");
            dir.bind(nid, addr, 0);
            let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
            rt.send_to(addr, SwimIn::Subscribe { observer: *inbox.addr() })
                .unwrap();
            addrs.push(addr);
            inboxes.push(inbox);
        }
        let mut c = Cluster {
            rt,
            ids,
            addrs,
            inboxes,
            dir,
            streams: vec![Vec::new(); n],
            clock: now,
        };
        c.pump(4);
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

    fn drain(&mut self) {
        for (i, inbox) in self.inboxes.iter().enumerate() {
            while let Some(m) = inbox.try_recv() {
                self.streams[i].push(m);
            }
        }
    }

    /// Advance the clock and tick every still-bound actor (unbound = silent).
    fn round(&mut self) {
        self.clock += TICK;
        let now = self.clock;
        for i in 0..self.ids.len() {
            if self.dir.resolve(&self.ids[i]).is_some() {
                let _ = self.rt.send_to(self.addrs[i], SwimIn::Tick { now });
            }
        }
        self.pump(8);
        self.drain();
    }

    fn run_until<F: Fn(&Cluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
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

    /// Fold observer's `MembershipChanged` stream under the §7 merge rule.
    fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
        let mut ml = MemberList::new(id(250));
        for m in &self.streams[observer] {
            ml.apply(m.node_id, m.state, m.incarnation);
        }
        ml.all_members()
            .iter()
            .map(|e| (e.node_id.0, e.state))
            .collect()
    }

    fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
        self.folded_view(observer).get(&subject.0) == Some(&state)
    }

    fn all_converged_alive(&self) -> bool {
        let n = self.ids.len();
        (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
    }
}

#[test]
fn a_silenced_node_that_returns_is_resurrected_dead_then_alive() {
    // Goal 3 (death is provisional). A node is genuinely silenced (binding
    // dropped, no ticks), detected Dead by survivors, then comes back. The §9.8
    // dead-reprobe must ping it, the §10.6 re-enqueue hands it its own Dead
    // record, it refutes (Alive at a higher incarnation), and that refutation
    // propagates so survivors flip Dead → Alive. The endpoint is asserted, not
    // just post-heal agreement.
    let mut c = Cluster::new(4, reprobe_config());
    assert!(
        c.run_until(400, |c| c.all_converged_alive()),
        "precondition: cluster must converge Alive"
    );

    let victim = 3usize;
    let vid = id(victim as u8);

    // Genuine silence.
    c.dir.unbind(&vid);
    let detected = c.run_until(2000, |c| {
        (0..4).all(|o| o == victim || c.sees(o, vid, MemberState::Dead))
    });
    assert!(detected, "survivors must first detect the silenced node as Dead");

    // The node returns: rebind at a higher generation, ticking resumes.
    c.dir.bind(vid, c.addrs[victim], 1);

    let resurrected = c.run_until(4000, |c| {
        (0..4).all(|o| o == victim || c.sees(o, vid, MemberState::Alive))
    });
    assert!(
        resurrected,
        "a returned node must be re-probed and resurrected Dead -> Alive (Goal 3)"
    );
}

#[test]
fn a_single_observed_death_infects_a_non_probing_witness() {
    // Goal 6 (dissemination reaches everyone). After convergence, silence one
    // node. SOME survivor's probe times out and originates the Dead. Every OTHER
    // survivor — including ones whose own probe round may never have targeted the
    // dead node before it was evicted from their alive set — must still converge
    // on Dead, which is only reachable by multi-hop piggyback infection (§11),
    // not direct observation.
    let mut c = Cluster::new(5, reprobe_config());
    assert!(c.run_until(400, |c| c.all_converged_alive()), "precondition: converge");

    let victim = 4usize;
    let vid = id(victim as u8);
    c.dir.unbind(&vid);

    let everyone_knows = c.run_until(3000, |c| {
        (0..5).all(|o| o == victim || c.sees(o, vid, MemberState::Dead))
    });
    assert!(
        everyone_knows,
        "every survivor must learn the death via gossip infection, not just the prober (Goal 6)"
    );
}

// ─── §7.1 storm bound, end to end (no actor harness needed) ──────────────────

fn flood_piggyback(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(membership_update(target, state, incarnation), 5);
    q.pack_piggyback(10)
}

#[test]
fn a_flood_of_stale_suspect_gossip_does_not_run_self_incarnation_away() {
    // §7.1, the load-bearing `>=` bound stated as a *property over a flood*: a
    // victim pelted with many Suspect-about-self updates all at the same stale
    // incarnation refutes exactly once (to inc 1) and ignores the rest. If the
    // gate were `>` or compared against the original incarnation, the count would
    // climb with the flood — the unbounded refute storm the spec calls out.
    let mut swim = SwimNode::new(id(7), SwimConfig::default(), Instant::now());
    assert_eq!(swim.members().self_incarnation(), 0);

    // 50 stale Suspect-about-self updates, all at incarnation 0.
    for seq in 0..50u64 {
        swim.handle_ping(id(1), seq, &flood_piggyback(id(7), MemberState::Suspect, 0));
    }
    assert_eq!(
        swim.members().self_incarnation(),
        1,
        "a flood at one stale incarnation must refute exactly once (storm bound)"
    );

    // Now a genuinely fresh accusation at the *current* incarnation must still
    // get through — the bound suppresses stale gossip, not legitimate news.
    swim.handle_ping(id(1), 99, &flood_piggyback(id(7), MemberState::Suspect, 1));
    assert_eq!(
        swim.members().self_incarnation(),
        2,
        "an accusation at the current incarnation must still refute"
    );
}
