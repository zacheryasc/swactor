//! Judge-authored adversarial tests, written from SWIM_ACTOR_SPEC.md alone.
//!
//! These probe load-bearing invariants the shipped suite leaves implicit:
//!   - §7.1 refute gate is `>=`, and its NEGATIVE side: a STALE accusation
//!     (incarnation strictly below ours) must NOT refute.
//!   - §7/§8 merge may import a direct Alive -> Dead jump on a higher
//!     incarnation (a peer can skip Suspect).
//!   - §7/§8 equal-incarnation merge obeys Dead > Suspect > Alive, so an Alive
//!     at the SAME incarnation never resurrects a Dead entry (only a higher
//!     incarnation does).
//!   - §9.5 still-Suspect guard: a node that refutes mid-suspicion-window
//!     (Alive at a higher incarnation arrives before the timer expires) is NOT
//!     declared Dead; the timer is dropped.

use std::time::{Duration, Instant};

use distribution::swim::dissemination::{DisseminationQueue, membership_update};
use distribution::swim::member_list::MemberList;
use distribution::swim::node::SwimNode;
use distribution::swim::probe::{ProbeMode, SwimConfig};
use distribution::types::{MemberState, NodeId};

const TICK: Duration = Duration::from_millis(10);

fn id(b: u8) -> NodeId {
    NodeId([b; 32])
}

/// One update packed exactly as it would ride a real `piggyback` field (§6.4).
fn pb(node: NodeId, state: MemberState, inc: u64) -> Vec<u8> {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(membership_update(node, state, inc), 5);
    q.pack_piggyback(10)
}

// ── §7.1 negative: a stale accusation below our incarnation must NOT refute ──

#[test]
fn a_stale_suspect_below_current_incarnation_does_not_refute() {
    // The `>=` gate is the storm bound. Its negative half is just as load-bearing:
    // once we have advanced past an incarnation, a Suspect/Dead record about self
    // at a LOWER incarnation is news we already overrode — refuting again would
    // re-open the unbounded cascade the spec warns about (§7.1).
    let mut me = SwimNode::new(id(0), SwimConfig::default(), Instant::now());

    // Drive self_incarnation to 2 via two fresh accusations (inc 0 then inc 1).
    me.handle_ping(id(1), 1, &pb(id(0), MemberState::Suspect, 0));
    me.handle_ping(id(1), 2, &pb(id(0), MemberState::Suspect, 1));
    assert_eq!(
        me.members().self_incarnation(),
        2,
        "two fresh accusations -> inc 2"
    );

    // Now pelt with STALE accusations strictly below the current incarnation.
    for seq in 0..25u64 {
        me.handle_ping(id(2), seq, &pb(id(0), MemberState::Suspect, 0));
        me.handle_ping(id(2), 100 + seq, &pb(id(0), MemberState::Dead, 1));
    }
    assert_eq!(
        me.members().self_incarnation(),
        2,
        "a stale Suspect/Dead below our incarnation must be ignored, not refuted"
    );

    // A fresh accusation AT the current incarnation still gets through (the gate
    // suppresses stale gossip, never legitimate news).
    me.handle_ping(id(2), 999, &pb(id(0), MemberState::Suspect, 2));
    assert_eq!(
        me.members().self_incarnation(),
        3,
        "accusation at current inc must refute"
    );
}

// ── §7/§8: merge imports a direct Alive -> Dead jump on a higher incarnation ──

#[test]
fn merge_imports_a_direct_alive_to_dead_jump_at_higher_incarnation() {
    // Lifecycle (suspect/declare_dead) is local-only and walks Alive->Suspect->Dead.
    // Merge is different: replicating a remote decision, it may jump straight to any
    // state on a higher incarnation (§8 "any state, incl. Alive->Dead jump").
    let mut ml = MemberList::new(id(0));
    assert!(ml.apply(id(1), MemberState::Alive, 4), "learn peer Alive@4");
    assert_eq!(ml.get(&id(1)).map(|e| e.state), Some(MemberState::Alive));

    // A higher-incarnation Dead jumps Alive -> Dead directly, skipping Suspect.
    assert!(
        ml.apply(id(1), MemberState::Dead, 5),
        "higher-inc Dead must win"
    );
    let e = ml.get(&id(1)).unwrap();
    assert_eq!(e.state, MemberState::Dead);
    assert_eq!(e.incarnation, 5);
}

// ── §7/§8: equal incarnation -> Dead>Suspect>Alive; Alive never resurrects ──

#[test]
fn equal_incarnation_alive_never_resurrects_a_dead_entry() {
    // At EQUAL incarnation the merge is priority-ordered: Dead(2) > Suspect(1) >
    // Alive(0). An Alive at the same incarnation as a Dead entry is dominated and
    // must be a no-op — only a strictly higher incarnation can bring it back.
    let mut ml = MemberList::new(id(0));
    assert!(ml.apply(id(1), MemberState::Dead, 7), "peer is Dead@7");

    // Alive at the same incarnation: dominated, no change.
    assert!(
        !ml.apply(id(1), MemberState::Alive, 7),
        "Alive@7 must not resurrect Dead@7"
    );
    assert_eq!(ml.get(&id(1)).map(|e| e.state), Some(MemberState::Dead));

    // Suspect at the same incarnation: also dominated (Suspect < Dead).
    assert!(
        !ml.apply(id(1), MemberState::Suspect, 7),
        "Suspect@7 must not lower Dead@7"
    );
    assert_eq!(ml.get(&id(1)).map(|e| e.state), Some(MemberState::Dead));

    // A strictly higher incarnation Alive DOES resurrect (the only legal path).
    assert!(
        ml.apply(id(1), MemberState::Alive, 8),
        "Alive@8 must resurrect"
    );
    assert_eq!(ml.get(&id(1)).map(|e| e.state), Some(MemberState::Alive));
}

// ── §9.5 still-Suspect guard: a mid-window refutation prevents the death ──

#[test]
fn a_peer_that_refutes_mid_window_is_not_declared_dead() {
    // §9.5: when a suspicion timer fires, declare_dead happens ONLY if the node is
    // still Suspect. If a refutation (Alive at a higher incarnation) arrived during
    // the window, the §7 merge already cleared Suspect; the still-Suspect guard must
    // honor that and drop the timer WITHOUT killing the node.
    //
    // Isolating that guard takes care. An ack does NOT un-Suspect a peer (§8: only
    // a higher-incarnation Alive merge does); and a permanently-probed peer is
    // simply re-suspected, which is correct. So we use Reactive mode (no periodic
    // probing) with probe_timeout (100 ticks) >> suspicion_timeout (5 ticks): after
    // the refutation a re-probe physically cannot re-suspect before the ORIGINAL
    // timer expires, so the only thing that could kill the peer at expiry is a
    // missing guard.
    let config = SwimConfig {
        probe_interval: TICK,
        probe_timeout: TICK * 100,
        indirect_probes: 2,
        suspicion_timeout: TICK * 5,
        dead_reprobe_interval: Duration::ZERO,
        probe_mode: ProbeMode::Reactive {
            safety_sweep_interval: TICK * 100_000,
        },
        lifeguard: None,
    };
    let t0 = Instant::now();
    let mut me = SwimNode::new(id(0), config, t0);

    // Learn one peer, id(1), Alive@0 (join_request does not enqueue a probe).
    me.handle_join_request(id(1));
    assert_eq!(
        me.members().get(&id(1)).map(|e| e.state),
        Some(MemberState::Alive)
    );

    // Kick off a single reactive probe of id(1); it never acks, so after the
    // direct + indirect phases (each 100 ticks) it is suspected. Drive ticks until
    // that happens, capped so a hang fails loudly.
    me.tick(t0 + TICK); // anchor the engine clock at a real tick
    me.report_send_failure(id(1));
    let mut k = 2u64;
    let suspected_at = loop {
        let now = t0 + TICK * (k as u32);
        me.tick(now);
        if me.members().get(&id(1)).map(|e| e.state) == Some(MemberState::Suspect) {
            break now;
        }
        k += 1;
        assert!(k < 500, "peer should have been suspected by now");
    };

    // A refutation for id(1) arrives via gossip: Alive at a higher incarnation.
    // This clears Suspect through the §7 merge. The original suspicion timer
    // (started at suspected_at, expiring 5 ticks later) is untouched by the merge.
    me.handle_ping(id(2), 7, &pb(id(1), MemberState::Alive, 1));
    assert_eq!(
        me.members().get(&id(1)).map(|e| e.state),
        Some(MemberState::Alive),
        "refutation must clear Suspect via merge"
    );

    // Drive a few ticks across the original timer's expiry (5 ticks). A re-probe
    // may launch but cannot re-suspect for 100+ ticks, so when the timer fires the
    // peer is still Alive and the still-Suspect guard must decline to kill it.
    for j in 1..8u64 {
        me.tick(suspected_at + TICK * (j as u32));
    }
    assert_eq!(
        me.members().get(&id(1)).map(|e| e.state),
        Some(MemberState::Alive),
        "a node that refuted mid-window must NOT be declared Dead (§9.5 still-Suspect guard)"
    );
}
