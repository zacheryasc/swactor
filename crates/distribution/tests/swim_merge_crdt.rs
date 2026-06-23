//! Layer 1 — §7 membership CRDT (`MemberList::apply`) + §7.1 self-refute gate.
//!
//! These pin the **wire-normative** merge contract two independent SWIM
//! implementations MUST agree on byte-for-byte (SWIM_ACTOR_SPEC §7):
//!
//!   1. monotone incarnation per node (a stored incarnation never decreases);
//!   2. `Dead > Suspect > Alive` at equal incarnation;
//!   3. merge may jump to any state on a higher incarnation (incl. Alive→Dead);
//!   4. self is never stored as a member;
//!   5. the load-bearing `>=` self-refute gate (§7.1).
//!
//! The merge rule is expressed primarily as a **property over an arbitrary
//! stream of updates**: the stored state is a function of the *dominating*
//! update alone — the unique maximum under the lexicographic order
//! `(incarnation, state-priority)` — and is therefore independent of arrival
//! order. That is the CRDT contract, not the shape of today's `match`.

use std::collections::BTreeMap;
use std::time::Instant;

use distribution::swim::dissemination::{DisseminationQueue, membership_update};
use distribution::swim::member_list::MemberList;
use distribution::swim::node::SwimNode;
use distribution::swim::probe::SwimConfig;
use distribution::types::{MemberState, NodeId};
use proptest::prelude::*;

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

/// §7 dominance: a higher incarnation wins; at equal incarnation a
/// higher-priority state (`Dead > Suspect > Alive`) wins. `(incarnation,
/// priority)` is a total order, so the dominating update of a stream is unique
/// in `(state, incarnation)`.
fn dominates(new: (MemberState, u64), old: (MemberState, u64)) -> bool {
    new.1 > old.1 || (new.1 == old.1 && new.0.priority() > old.0.priority())
}

fn arb_state() -> impl Strategy<Value = MemberState> {
    prop_oneof![
        Just(MemberState::Alive),
        Just(MemberState::Suspect),
        Just(MemberState::Dead),
    ]
}

/// A stream update about one of a few peers (small id/incarnation domains so
/// collisions — the interesting merges — actually happen).
fn arb_update() -> impl Strategy<Value = (u8, MemberState, u64)> {
    (1u8..=4, arb_state(), 0u64..5)
}

// ─── §7: merge is a CRDT — order-independent, dominated by the max update ────

proptest! {
    /// §7 invariants 1–3: after an arbitrary stream of updates the stored entry
    /// for every node equals the dominating update, regardless of the order the
    /// updates arrived in. Two different orderings of the same multiset converge.
    #[test]
    fn merge_converges_to_the_dominating_update_independent_of_order(
        updates in proptest::collection::vec(arb_update(), 1..40),
        rotate in 0usize..40,
    ) {
        let mut forward = MemberList::new(node(0));
        for &(b, s, i) in &updates {
            forward.apply(node(b), s, i);
        }

        // Same multiset, a rotated arrival order.
        let mut rotated = MemberList::new(node(0));
        let n = updates.len();
        for k in 0..n {
            let (b, s, i) = updates[(k + rotate) % n];
            rotated.apply(node(b), s, i);
        }

        for byte in 1u8..=4 {
            let id = node(byte);
            let dominating = updates
                .iter()
                .filter(|(b, _, _)| *b == byte)
                .map(|&(_, s, i)| (s, i))
                .reduce(|acc, x| if dominates(x, acc) { x } else { acc });
            let f = forward.get(&id).map(|e| (e.state, e.incarnation));
            let r = rotated.get(&id).map(|e| (e.state, e.incarnation));
            prop_assert_eq!(f, r, "merge depends on arrival order for node {}", byte);
            prop_assert_eq!(f, dominating, "stored state is not the dominating update for node {}", byte);
        }
    }

    /// §7 invariant 1: a stored entry's incarnation is monotone non-decreasing
    /// across the whole stream — it never goes backwards.
    #[test]
    fn stored_incarnation_never_decreases(
        updates in proptest::collection::vec(arb_update(), 1..40),
    ) {
        let mut ml = MemberList::new(node(0));
        let mut last: BTreeMap<u8, u64> = BTreeMap::new();
        for &(b, s, i) in &updates {
            ml.apply(node(b), s, i);
            if let Some(e) = ml.get(&node(b)) {
                if let Some(prev) = last.get(&b) {
                    prop_assert!(e.incarnation >= *prev, "incarnation regressed for node {}", b);
                }
                last.insert(b, e.incarnation);
            }
        }
    }

    /// §7: `apply` reports "changed" exactly when the update strictly dominates
    /// the stored entry (or there was none). A non-dominating update is a no-op.
    /// This is the contract the dissemination edge (§7 invariant 5) keys off of.
    #[test]
    fn apply_reports_changed_iff_update_strictly_dominates(
        updates in proptest::collection::vec(arb_update(), 1..40),
    ) {
        let mut ml = MemberList::new(node(0));
        for &(b, s, i) in &updates {
            let before = ml.get(&node(b)).map(|e| (e.state, e.incarnation));
            let changed = ml.apply(node(b), s, i);
            let after = ml.get(&node(b)).map(|e| (e.state, e.incarnation));
            match before {
                None => {
                    prop_assert!(changed, "first sighting of a node must be a change");
                    prop_assert_eq!(after, Some((s, i)));
                }
                Some(prev) if dominates((s, i), prev) => {
                    prop_assert!(changed, "a dominating update must report changed");
                    prop_assert_eq!(after, Some((s, i)));
                }
                Some(prev) => {
                    prop_assert!(!changed, "a non-dominating update must be a no-op");
                    prop_assert_eq!(after, Some(prev));
                }
            }
        }
    }

    /// §7 invariant 4 / §3.3: a node never stores an entry about itself, and an
    /// update about self never reports a change — self liveness lives only in
    /// `self_incarnation` (raised by refute, §7.1).
    #[test]
    fn self_is_never_stored(
        updates in proptest::collection::vec((arb_state(), 0u64..5), 0..20),
    ) {
        let mut ml = MemberList::new(node(7));
        for (s, i) in updates {
            let changed = ml.apply(node(7), s, i);
            prop_assert!(!changed, "an update about self must never report changed");
            prop_assert!(ml.get(&node(7)).is_none(), "self must never be stored");
            prop_assert_eq!(ml.len(), 0);
        }
    }
}

// ─── §7/§8: targeted merge-vs-lifecycle contrasts ───────────────────────────

#[test]
fn merge_jumps_alive_to_dead_on_higher_incarnation() {
    // §7 invariant 3 / §8: unlike the *local* lifecycle (which must walk
    // Alive→Suspect→Dead), a *merge* at a higher incarnation may import any
    // state directly — a peer can be learned Dead without ever being seen
    // Suspect locally.
    let mut ml = MemberList::new(node(0));
    ml.apply(node(1), MemberState::Alive, 0);
    let changed = ml.apply(node(1), MemberState::Dead, 1);
    assert!(changed);
    let e = ml.get(&node(1)).unwrap();
    assert_eq!(e.state, MemberState::Dead);
    assert_eq!(e.incarnation, 1);
}

#[test]
fn merge_obeys_dead_over_suspect_over_alive_at_equal_incarnation() {
    // §7 invariant 2: at equal incarnation, higher priority wins; lower priority
    // is ignored even though it is "newer" by arrival.
    for (winner, loser) in [
        (MemberState::Dead, MemberState::Suspect),
        (MemberState::Dead, MemberState::Alive),
        (MemberState::Suspect, MemberState::Alive),
    ] {
        let mut ml = MemberList::new(node(0));
        ml.apply(node(1), loser, 3);
        assert!(
            ml.apply(node(1), winner, 3),
            "{winner:?} must override {loser:?} at equal inc"
        );
        assert_eq!(ml.get(&node(1)).unwrap().state, winner);

        // And the reverse arrival order is a no-op — priority, not recency.
        let mut ml = MemberList::new(node(0));
        ml.apply(node(1), winner, 3);
        assert!(
            !ml.apply(node(1), loser, 3),
            "{loser:?} must not override {winner:?} at equal inc"
        );
        assert_eq!(ml.get(&node(1)).unwrap().state, winner);
    }
}

// ─── §7.1: self-refute gate (the `>=` bound) ─────────────────────────────────

/// A piggyback frame carrying a single update *about `target`* — used to feed
/// the self-refute gate through the public `handle_ping` boundary (§10.0/§10.2).
fn piggyback_about(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(membership_update(target, state, incarnation), 5);
    q.pack_piggyback(10)
}

fn fresh_node(self_byte: u8) -> SwimNode {
    SwimNode::new(node(self_byte), SwimConfig::default(), Instant::now())
}

#[test]
fn refute_fires_on_inbound_suspect_at_equal_incarnation() {
    // §7.1: the gate is `incoming incarnation >= self_incarnation`. At *equality*
    // (the `>=`, not `>`) it MUST fire — refusing to refute equal-incarnation
    // gossip would leave a node unable to clear a fresh suspicion about itself.
    let mut swim = fresh_node(0);
    assert_eq!(swim.members().self_incarnation(), 0);
    swim.handle_ping(
        node(1),
        1,
        &piggyback_about(node(0), MemberState::Suspect, 0),
    );
    assert_eq!(
        swim.members().self_incarnation(),
        1,
        "equal-incarnation Suspect about self must trigger refute"
    );
}

#[test]
fn refute_ignores_stale_suspect_below_current_incarnation() {
    // §7.1: the load-bearing bound. A Suspect/Dead at an *already-superseded*
    // incarnation is news we have already answered; refuting it again is the
    // unbounded refute storm the `>=` (vs `>`) gate exists to prevent.
    let mut swim = fresh_node(0);
    swim.handle_ping(
        node(1),
        1,
        &piggyback_about(node(0), MemberState::Suspect, 0),
    );
    assert_eq!(swim.members().self_incarnation(), 1);

    // A stale Suspect at incarnation 0 (< our current 1) must NOT bump again.
    swim.handle_ping(
        node(2),
        1,
        &piggyback_about(node(0), MemberState::Suspect, 0),
    );
    assert_eq!(
        swim.members().self_incarnation(),
        1,
        "stale Suspect (inc < self) must not refute — the storm bound"
    );
}

#[test]
fn refute_fires_again_when_incarnation_catches_up() {
    // §7.1: once we have advanced to incarnation N, a Suspect at N (>= N) is
    // current again and MUST refute. Pins that the gate compares against the
    // *current* incarnation, not the original one.
    let mut swim = fresh_node(0);
    swim.handle_ping(
        node(1),
        1,
        &piggyback_about(node(0), MemberState::Suspect, 0),
    );
    assert_eq!(swim.members().self_incarnation(), 1);
    swim.handle_ping(
        node(2),
        1,
        &piggyback_about(node(0), MemberState::Suspect, 1),
    );
    assert_eq!(
        swim.members().self_incarnation(),
        2,
        "Suspect at the current incarnation must refute"
    );
}

#[test]
fn dead_about_self_refutes_and_self_is_never_stored() {
    // §7.1 covers Dead-about-self as well as Suspect; §7 invariant 4: even a
    // Dead claim about self never becomes a stored member entry.
    let mut swim = fresh_node(0);
    swim.handle_ping(node(1), 1, &piggyback_about(node(0), MemberState::Dead, 0));
    assert!(
        swim.members().self_incarnation() > 0,
        "Dead about self must refute"
    );
    assert!(
        swim.members().get(&node(0)).is_none(),
        "self must never be stored as a member"
    );
}

#[test]
fn alive_gossip_about_self_never_refutes() {
    // §7.1: only Suspect/Dead about self pass the gate. Alive-about-self — even
    // at a high incarnation — is not an accusation and must not bump incarnation.
    let mut swim = fresh_node(0);
    swim.handle_ping(node(1), 1, &piggyback_about(node(0), MemberState::Alive, 9));
    assert_eq!(
        swim.members().self_incarnation(),
        0,
        "Alive about self is not a suspicion and must not raise incarnation"
    );
}
