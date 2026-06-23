//! Layer 1 — §11 dissemination queue, as a property/contract suite.
//!
//! The wire-normative dissemination contract (SWIM_ACTOR_SPEC §11):
//!
//!   * **at most one entry per node** — a second update for a node never
//!     duplicates; it either replaces the queued one or is dropped;
//!   * **a dominating update resets the transmit budget** — fresher news gets a
//!     full infection round, not the leftover budget of the entry it replaces;
//!   * **eviction at the `Λ·⌈log₂ n⌉` bound** — each update is gossiped exactly
//!     that many times (floor 1 on the log term) and then evicted;
//!   * **priority order on the wire** — `take` yields `Dead > Suspect > Alive`.
//!
//! These mirror the merge CRDT (§7): "dominates" here is the same
//! `(incarnation, state-priority)` order. The at-most-one and eviction facts
//! are expressed as properties over arbitrary update streams.

use std::collections::BTreeSet;

use distribution::swim::dissemination::{DisseminationQueue, membership_update};
use distribution::types::{MemberState, NodeId};
use proptest::prelude::*;

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

const LAMBDA: usize = 3;

/// The spec's transmit bound: `Λ · max(1, ⌈log₂(max(n, 2))⌉)`.
fn spec_budget(cluster_size: usize) -> usize {
    let n = cluster_size.max(2) as f64;
    let log_n = n.log2().ceil() as usize;
    LAMBDA * log_n.max(1)
}

fn arb_state() -> impl Strategy<Value = MemberState> {
    prop_oneof![
        Just(MemberState::Alive),
        Just(MemberState::Suspect),
        Just(MemberState::Dead),
    ]
}

// ─── at-most-one-per-node + priority order (properties) ──────────────────────

proptest! {
    /// §11: however many updates are enqueued for whatever nodes, the queue
    /// never holds two entries for the same node — draining it yields each node
    /// at most once.
    #[test]
    fn queue_holds_at_most_one_entry_per_node(
        updates in proptest::collection::vec((1u8..=6, arb_state(), 0u64..4), 1..40),
        cluster in 2usize..32,
    ) {
        let mut q = DisseminationQueue::new(LAMBDA);
        for (b, s, i) in &updates {
            q.enqueue(membership_update(node(*b), *s, *i), cluster);
        }
        let drained = q.take(usize::MAX);
        let mut seen = BTreeSet::new();
        for u in &drained {
            prop_assert!(seen.insert(u.node_id), "queue held two entries for one node");
        }
    }

    /// §11: `take` hands out updates in priority order — `Dead` before
    /// `Suspect` before `Alive` — so the most urgent news rides first when the
    /// piggyback budget is tight.
    #[test]
    fn take_yields_descending_priority(
        updates in proptest::collection::vec((1u8..=8, arb_state(), 0u64..3), 1..20),
    ) {
        let mut q = DisseminationQueue::new(LAMBDA);
        for (b, s, i) in &updates {
            q.enqueue(membership_update(node(*b), *s, *i), 16);
        }
        let drained = q.take(usize::MAX);
        for w in drained.windows(2) {
            prop_assert!(
                w[0].state.priority() >= w[1].state.priority(),
                "take must not place a lower-priority update before a higher one"
            );
        }
    }

    /// §11: a freshly-enqueued update is gossiped exactly `Λ·⌈log₂ n⌉` times
    /// (with a floor of 1 on the log term) and is then evicted — the standard
    /// SWIM infection bound, scaling with cluster size.
    #[test]
    fn entry_survives_exactly_the_transmit_budget(cluster in 2usize..64) {
        let mut q = DisseminationQueue::new(LAMBDA);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), cluster);
        let budget = spec_budget(cluster);
        for k in 0..budget {
            prop_assert_eq!(q.take(10).len(), 1, "entry evicted early at transmission {}", k);
        }
        prop_assert_eq!(q.take(10).len(), 0, "entry must be evicted once the budget is spent");
    }
}

// ─── eviction bound at known points (no formula echo) ────────────────────────

#[test]
fn transmit_budget_matches_spec_at_known_cluster_sizes() {
    // §11: spot-check the `Λ·⌈log₂ n⌉` bound at concrete sizes (Λ = 3) so the
    // contract is pinned independent of the float arithmetic that computes it:
    //   n=2 → 3·1 = 3,  n=4 → 3·2 = 6,  n=16 → 3·4 = 12.
    for (cluster, expected) in [(2usize, 3usize), (4, 6), (16, 12)] {
        let mut q = DisseminationQueue::new(LAMBDA);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), cluster);
        let mut transmissions = 0;
        while !q.take(10).is_empty() {
            transmissions += 1;
            assert!(
                transmissions <= expected + 1,
                "cluster {cluster}: ran past the spec budget"
            );
        }
        assert_eq!(
            transmissions, expected,
            "cluster {cluster}: wrong transmit budget"
        );
    }
}

// ─── dominating update resets the budget ─────────────────────────────────────

#[test]
fn dominating_update_replaces_entry_and_resets_budget() {
    // §11: when a dominating update arrives for an already-queued node it
    // replaces the entry *and* resets `remaining` to the full budget, so the
    // fresher state gets a complete infection round.
    let cluster = 2; // budget = 3
    let mut q = DisseminationQueue::new(LAMBDA);
    q.enqueue(membership_update(node(1), MemberState::Alive, 0), cluster);

    // Spend two of the three transmissions.
    q.take(10);
    q.take(10);

    // A dominating update (higher priority at equal incarnation) arrives.
    q.enqueue(membership_update(node(1), MemberState::Suspect, 0), cluster);
    assert_eq!(q.len(), 1, "dominating update must replace, not duplicate");

    // The reset budget (3) now carries the *new* state through a full round.
    for _ in 0..3 {
        let got = q.take(10);
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].state,
            MemberState::Suspect,
            "the replaced state must be what propagates"
        );
    }
    assert_eq!(
        q.take(10).len(),
        0,
        "entry evicted only after the reset budget is spent"
    );
}

#[test]
fn non_dominating_update_neither_replaces_nor_resets() {
    // §11 (mirror of §7's stale-ignore): an update that does not dominate the
    // queued entry is dropped — the state is unchanged and the budget keeps
    // counting down from where it was.
    let cluster = 2; // budget = 3
    let mut q = DisseminationQueue::new(LAMBDA);
    q.enqueue(membership_update(node(1), MemberState::Suspect, 5), cluster);
    q.take(10); // one of three spent → 2 remaining

    // A dominated update (lower incarnation) must not touch the entry.
    q.enqueue(membership_update(node(1), MemberState::Dead, 3), cluster);

    for _ in 0..2 {
        let got = q.take(10);
        assert_eq!(
            got[0].state,
            MemberState::Suspect,
            "non-dominating update must not replace"
        );
        assert_eq!(got[0].incarnation, 5);
    }
    assert_eq!(
        q.take(10).len(),
        0,
        "budget was not reset — entry evicts on its original schedule"
    );
}
