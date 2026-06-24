//! Pure SWIM protocol contracts.
//!
//! These tests exercise the SWIM protocol engine without relying on network drivers: membership
//! merge, dissemination, probing, lifeguard timing, handlers, and in-memory behavioral convergence.
//!
//! Behavioral/correctness guarantees:
//! - SWIM membership is a convergent CRDT: accepted updates move state monotonically under
//!   incarnation/state priority.
//! - Self-state is protected: a node never stores itself as a peer and refutes current
//!   accusations without unbounded refute storms.
//! - Membership dissemination is bounded, prioritized, deduplicated, and safe against malformed
//!   piggyback input.
//! - Probe/lifeguard timing turns real silence into Suspect/Dead while allowing timely refutation
//!   before death.
//! - Lifeguard only relaxes timing under local degradation; it never makes failure detection more
//!   aggressive than the configured floor.
//! - Protocol handlers emit exactly the wire/control actions implied by each input, with no
//!   hidden side effects.
//! - In an in-memory SWIM cluster, joins converge, silence is detected, death is provisional, and
//!   membership-change streams reconstruct the view.

mod lifeguard_timing {
    //! Lifeguard health multiplier and timeout math: degradation relaxes timing and clamps stay
    //! within configured floors/ceilings.

    use std::time::Duration;

    use distribution::swim::lifeguard::{HealthMultiplier, LifeguardConfig};

    /// Suspicion timeouts are wall-clock now; express the old integer tick-counts
    /// as milliseconds so the relative assertions carry over unchanged.
    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    // ─── Local Health Multiplier ─────────────────────────────────────────────────

    #[test]
    fn healthy_node_has_multiplier_of_one() {
        // Given: a freshly created health multiplier
        let hm = HealthMultiplier::new(LifeguardConfig::default());

        // Then: multiplier is 1 (no scaling)
        assert_eq!(hm.multiplier(), 1);
        assert_eq!(hm.score(), 0);
    }

    #[test]
    fn nacks_degrade_health_and_increase_multiplier() {
        // Given: a healthy node
        let mut hm = HealthMultiplier::new(LifeguardConfig::default());

        // When: 3 consecutive nacks occur (no acks)
        hm.record_nack();
        hm.record_nack();
        hm.record_nack();

        // Then: health score is 3, multiplier is 4
        assert_eq!(hm.score(), 3);
        assert_eq!(hm.multiplier(), 4);
    }

    #[test]
    fn acks_improve_health() {
        // Given: a degraded node (score = 3)
        let mut hm = HealthMultiplier::new(LifeguardConfig::default());
        hm.record_nack();
        hm.record_nack();
        hm.record_nack();

        // When: 2 successful acks arrive
        hm.record_ack();
        hm.record_ack();

        // Then: health improves
        assert_eq!(hm.score(), 1);
        assert_eq!(hm.multiplier(), 2);
    }

    #[test]
    fn health_score_cannot_go_below_zero() {
        // Given: a healthy node
        let mut hm = HealthMultiplier::new(LifeguardConfig::default());

        // When: acks arrive despite no prior nacks
        hm.record_ack();
        hm.record_ack();
        hm.record_ack();

        // Then: score stays at 0
        assert_eq!(hm.score(), 0);
        assert_eq!(hm.multiplier(), 1);
    }

    #[test]
    fn health_score_capped_at_max() {
        // Given: a config with max_health_score = 4
        let config = LifeguardConfig {
            max_health_score: 4,
            ..LifeguardConfig::default()
        };
        let mut hm = HealthMultiplier::new(config);

        // When: many nacks occur
        for _ in 0..20 {
            hm.record_nack();
        }

        // Then: score is capped at 4, multiplier at 5
        assert_eq!(hm.score(), 4);
        assert_eq!(hm.multiplier(), 5);
    }

    // ─── Protocol Period Scaling ─────────────────────────────────────────────────

    #[test]
    fn healthy_node_uses_base_probe_interval() {
        // Given: a healthy node
        let hm = HealthMultiplier::new(LifeguardConfig::default());

        // When: computing scaled probe interval with base = 10
        let interval = hm.scaled_probe_interval(10);

        // Then: interval is unchanged (multiplier = 1)
        assert_eq!(interval, 10);
    }

    #[test]
    fn degraded_node_stretches_probe_interval() {
        // Given: a node with health score 3 (multiplier = 4)
        let mut hm = HealthMultiplier::new(LifeguardConfig::default());
        hm.record_nack();
        hm.record_nack();
        hm.record_nack();

        // When: computing scaled probe interval with base = 10
        let interval = hm.scaled_probe_interval(10);

        // Then: interval is stretched to 40
        assert_eq!(interval, 40);
    }

    #[test]
    fn degraded_node_stretches_probe_timeout() {
        // Given: a node with health score 2
        let mut hm = HealthMultiplier::new(LifeguardConfig::default());
        hm.record_nack();
        hm.record_nack();

        // When: computing scaled probe timeout with base = 3
        let timeout = hm.scaled_probe_timeout(3);

        // Then: timeout is stretched to 9 (3 * multiplier 3)
        assert_eq!(timeout, 9);
    }

    // ─── Dynamic Suspect Timeout ─────────────────────────────────────────────────

    #[test]
    fn suspect_timeout_scales_with_cluster_size() {
        // Given: a healthy node with base_suspicion_timeout = 30ms
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(30),
            min_suspicion_timeout: ms(10),
            max_suspicion_timeout: ms(500),
            ..LifeguardConfig::default()
        };
        let hm = HealthMultiplier::new(config);

        // When: computing dynamic timeout for different cluster sizes
        let timeout_2 = hm.dynamic_suspicion_timeout(2);
        let timeout_8 = hm.dynamic_suspicion_timeout(8);
        let timeout_100 = hm.dynamic_suspicion_timeout(100);

        // Then: larger clusters get longer timeouts (log2 scaling)
        assert!(
            timeout_2 < timeout_8,
            "8-node cluster should have longer timeout than 2-node: {:?} vs {:?}",
            timeout_2,
            timeout_8
        );
        assert!(
            timeout_8 < timeout_100,
            "100-node cluster should have longer timeout than 8-node: {:?} vs {:?}",
            timeout_8,
            timeout_100
        );
    }

    #[test]
    fn suspect_timeout_is_clamped_to_min() {
        // Given: a config with min_suspicion_timeout = 50ms and a tiny cluster
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(1),
            min_suspicion_timeout: ms(50),
            max_suspicion_timeout: ms(500),
            ..LifeguardConfig::default()
        };
        let hm = HealthMultiplier::new(config);

        // When: computing for a 2-node cluster (log2(3) ≈ 2, so base*2*1 = 2ms)
        let timeout = hm.dynamic_suspicion_timeout(2);

        // Then: clamped to minimum
        assert_eq!(timeout, ms(50));
    }

    #[test]
    fn suspect_timeout_is_clamped_to_max() {
        // Given: a config with max_suspicion_timeout = 100ms and a huge cluster
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(30),
            min_suspicion_timeout: ms(10),
            max_suspicion_timeout: ms(100),
            ..LifeguardConfig::default()
        };
        let hm = HealthMultiplier::new(config);

        // When: computing for a 10000-node cluster
        let timeout = hm.dynamic_suspicion_timeout(10000);

        // Then: clamped to maximum
        assert_eq!(timeout, ms(100));
    }

    #[test]
    fn degraded_health_further_increases_suspect_timeout() {
        // Given: a config and two nodes — one healthy, one degraded
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(30),
            min_suspicion_timeout: ms(10),
            max_suspicion_timeout: ms(5000),
            ..LifeguardConfig::default()
        };
        let healthy = HealthMultiplier::new(config.clone());
        let mut degraded = HealthMultiplier::new(config);
        degraded.record_nack();
        degraded.record_nack();

        // When: both compute timeout for a 16-node cluster
        let healthy_timeout = healthy.dynamic_suspicion_timeout(16);
        let degraded_timeout = degraded.dynamic_suspicion_timeout(16);

        // Then: degraded node gives itself even more time
        assert!(
            degraded_timeout > healthy_timeout,
            "degraded node ({:?}) should have longer suspect timeout than healthy ({:?})",
            degraded_timeout,
            healthy_timeout
        );
        // Specifically: healthy = 30 * log2(17) * 1, degraded = 30 * log2(17) * 3
        assert_eq!(degraded_timeout, healthy_timeout * 3);
    }

    // ─── Stress / Scenario Tests ─────────────────────────────────────────────────

    #[test]
    fn recovery_from_worst_health_takes_max_acks() {
        // Given: a node at maximum degradation
        let config = LifeguardConfig {
            max_health_score: 8,
            nack_penalty: 1,
            ack_reward: 1,
            ..LifeguardConfig::default()
        };
        let mut hm = HealthMultiplier::new(config);
        for _ in 0..100 {
            hm.record_nack();
        }
        assert_eq!(hm.score(), 8);

        // When: exactly max_health_score acks arrive
        for _ in 0..8 {
            hm.record_ack();
        }

        // Then: fully recovered
        assert_eq!(hm.score(), 0);
        assert_eq!(hm.multiplier(), 1);
    }

    #[test]
    fn mixed_ack_nack_stream_settles_to_moderate_health() {
        // Given: a node receiving alternating acks and nacks (slightly more nacks)
        let config = LifeguardConfig {
            max_health_score: 10,
            nack_penalty: 2,
            ack_reward: 1,
            ..LifeguardConfig::default()
        };
        let mut hm = HealthMultiplier::new(config);

        // When: 100 rounds of alternating nack, ack
        for _ in 0..100 {
            hm.record_nack(); // +2
            hm.record_ack(); // -1
        }

        // Then: score settles near max (nack caps at 10, final ack brings it to 9)
        assert_eq!(hm.score(), 9);
    }

    #[test]
    fn solo_node_gets_minimal_suspect_timeout() {
        // Given: a healthy node in a cluster of size 1
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(30),
            min_suspicion_timeout: ms(15),
            max_suspicion_timeout: ms(500),
            ..LifeguardConfig::default()
        };
        let hm = HealthMultiplier::new(config);

        // When: computing timeout for cluster of 1
        let timeout = hm.dynamic_suspicion_timeout(1);

        // Then: log2(2) = 1, so 30*1*1 = 30 (above min)
        assert_eq!(timeout, ms(30));
    }

    #[test]
    fn empty_cluster_still_returns_valid_timeout() {
        // Given: edge case — cluster size 0
        let config = LifeguardConfig {
            base_suspicion_timeout: ms(30),
            min_suspicion_timeout: ms(15),
            max_suspicion_timeout: ms(500),
            ..LifeguardConfig::default()
        };
        let hm = HealthMultiplier::new(config);

        // When/Then: doesn't panic and returns clamped value
        let timeout = hm.dynamic_suspicion_timeout(0);
        assert!(timeout >= ms(15));
    }
}

mod dissemination_queue {
    //! Membership piggyback queue mechanics: coalescing, priority, transmit budget, eviction, and
    //! codec safety.

    use distribution::swim::dissemination::{DisseminationQueue, membership_update};
    use distribution::types::{MemberState, NodeId};

    fn node(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    // ─── Basic queue operations ─────────────────────────────────────────────────

    #[test]
    fn enqueue_and_take_single_update() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 5);
        assert_eq!(q.len(), 1);

        let updates = q.take(10);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].node_id, node(1));
    }

    #[test]
    fn take_respects_max_count() {
        let mut q = DisseminationQueue::new(3);
        for i in 1..=5 {
            q.enqueue(membership_update(node(i), MemberState::Alive, 0), 10);
        }
        let updates = q.take(2);
        assert_eq!(updates.len(), 2);
    }

    // ─── Priority ordering ─────────────────────────────────────────────────────

    #[test]
    fn dead_updates_are_prioritized_over_suspect_and_alive() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 10);
        q.enqueue(membership_update(node(2), MemberState::Dead, 0), 10);
        q.enqueue(membership_update(node(3), MemberState::Suspect, 0), 10);

        let updates = q.take(3);
        assert_eq!(updates[0].state, MemberState::Dead, "Dead should be first");
        assert_eq!(
            updates[1].state,
            MemberState::Suspect,
            "Suspect should be second"
        );
        assert_eq!(updates[2].state, MemberState::Alive, "Alive should be last");
    }

    // ─── Transmit budget and eviction ───────────────────────────────────────────

    #[test]
    fn entries_evicted_after_transmit_budget_exhausted() {
        // lambda=1, cluster_size=2 → budget = 1 * ceil(log2(2)) = 1
        let mut q = DisseminationQueue::new(1);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 2);

        // First take: remaining goes from 1 to 0
        let updates = q.take(10);
        assert_eq!(updates.len(), 1);

        // Entry should be evicted now
        assert_eq!(q.len(), 0);
        let updates = q.take(10);
        assert_eq!(updates.len(), 0);
    }

    #[test]
    fn larger_cluster_gives_higher_transmit_budget() {
        // lambda=2, cluster_size=16 → budget = 2 * ceil(log2(16)) = 2 * 4 = 8
        let mut q = DisseminationQueue::new(2);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 16);

        // Take 8 times — entry should survive all of them
        for i in 0..8 {
            let updates = q.take(10);
            assert_eq!(updates.len(), 1, "take #{i} should still have the entry");
        }

        // 9th take: entry should be evicted
        assert_eq!(q.len(), 0);
    }

    // ─── Dedup: newer update for same node replaces older ───────────────────────

    #[test]
    fn newer_update_for_same_node_replaces_older() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 10);
        q.enqueue(membership_update(node(1), MemberState::Suspect, 0), 10);

        assert_eq!(q.len(), 1, "should replace, not duplicate");
        let updates = q.take(10);
        assert_eq!(updates[0].state, MemberState::Suspect);
    }

    #[test]
    fn higher_incarnation_replaces_lower() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Dead, 5), 10);
        // Same node, higher incarnation, Alive (incarnation wins over state)
        q.enqueue(membership_update(node(1), MemberState::Alive, 6), 10);

        assert_eq!(q.len(), 1);
        let updates = q.take(10);
        assert_eq!(updates[0].incarnation, 6);
        assert_eq!(updates[0].state, MemberState::Alive);
    }

    #[test]
    fn lower_incarnation_is_ignored() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Alive, 5), 10);
        q.enqueue(membership_update(node(1), MemberState::Dead, 3), 10);

        let updates = q.take(10);
        assert_eq!(
            updates[0].incarnation, 5,
            "older incarnation should be ignored"
        );
        assert_eq!(updates[0].state, MemberState::Alive);
    }

    // ─── Piggyback serialization ────────────────────────────────────────────────

    #[test]
    fn pack_and_unpack_piggyback_roundtrip() {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(node(1), MemberState::Alive, 0), 10);
        q.enqueue(membership_update(node(2), MemberState::Dead, 3), 10);

        let bytes = q.pack_piggyback(10);
        assert!(!bytes.is_empty());

        let unpacked = DisseminationQueue::unpack_piggyback(&bytes);
        assert_eq!(unpacked.len(), 2);
        // Dead should be first (priority ordering from take())
        assert_eq!(unpacked[0].state, MemberState::Dead);
    }

    #[test]
    fn unpack_empty_piggyback_returns_empty() {
        let unpacked = DisseminationQueue::unpack_piggyback(&[]);
        assert!(unpacked.is_empty());
    }

    #[test]
    fn unpack_garbage_returns_empty() {
        let unpacked = DisseminationQueue::unpack_piggyback(b"not valid json");
        assert!(unpacked.is_empty());
    }
}

mod dissemination_properties {
    //! Property-level dissemination guarantees: one pending update per node, priority order, and
    //! exact budget survival.

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
}

mod membership_crdt {
    //! Membership merge CRDT and self-refute behavior at the public SwimNode boundary.

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
}

mod probe_lifecycle {
    //! Probe state-machine contract: send failures, direct/indirect windows, refutation, and
    //! suspicion-to-death progression.

    use std::time::{Duration, Instant};

    use distribution::swim::dissemination::DisseminationQueue;
    use distribution::swim::lifeguard::LifeguardConfig;
    use distribution::swim::member_list::MemberList;
    use distribution::swim::node::{NodeAction, SwimNode};
    use distribution::swim::probe::{SwimAction, SwimConfig, SwimEvent, SwimProbe};
    use distribution::types::{MemberState, NodeId};

    fn node(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    /// Synthetic per-step clock granularity (SWIM is wall-clock driven).
    const TICK: Duration = Duration::from_millis(10);

    fn ticks(n: u64) -> Duration {
        TICK * n as u32
    }

    /// Step the probe once, advancing the synthetic clock by one `TICK` first.
    fn tick_once(
        probe: &mut SwimProbe,
        members: &mut MemberList,
        now: &mut Instant,
    ) -> Vec<SwimAction> {
        *now += TICK;
        probe.step(*now, SwimEvent::Tick, members)
    }

    fn tick_n(
        probe: &mut SwimProbe,
        members: &mut MemberList,
        now: &mut Instant,
        n: u64,
    ) -> Vec<SwimAction> {
        let mut all = Vec::new();
        for _ in 0..n {
            all.extend(tick_once(probe, members, now));
        }
        all
    }

    fn has_ping_req(actions: &[SwimAction]) -> bool {
        actions
            .iter()
            .any(|a| matches!(a, SwimAction::SendPingReq { .. }))
    }

    fn has_suspect(actions: &[SwimAction]) -> bool {
        actions.iter().any(|a| matches!(a, SwimAction::Suspect(_)))
    }

    fn ping_target(actions: &[SwimAction]) -> Option<NodeId> {
        actions.iter().find_map(|a| match a {
            SwimAction::SendPing { to, .. } => Some(*to),
            _ => None,
        })
    }

    // ─── §9.6 / §10.7 — SendFailed is a reactive probe trigger ────────────────────

    #[test]
    fn send_failed_reactively_probes_a_known_live_peer_but_ignores_unknown_and_dead() {
        // §9.6: a failed send to a peer is evidence that peer may be gone, so — while
        // Idle — it triggers an *immediate directed probe* of that peer (the reactive
        // trigger of §10.7), but ONLY when the target is a known, non-Dead member.
        // An unknown peer (nothing to probe) and a peer already Dead are both no-ops.
        let config = SwimConfig {
            probe_interval: ticks(1000), // park periodic probing far away so the only
            probe_timeout: ticks(3),     // SendPing we can observe is the reactive one
            indirect_probes: 0,
            suspicion_timeout: ticks(1000),
            dead_reprobe_interval: ticks(0),
            ..SwimConfig::default()
        };
        let now = Instant::now();
        let mut probe = SwimProbe::new(config, now);
        let mut members = MemberList::new(node(0));
        members.apply(node(1), MemberState::Alive, 0); // known & live
        members.apply(node(2), MemberState::Dead, 4); // known & dead

        // Unknown peer: a SendFailed about a node we have never heard of probes nobody.
        let unknown = probe.step(now, SwimEvent::SendFailed { to: node(9) }, &mut members);
        assert!(
            ping_target(&unknown).is_none(),
            "a SendFailed for an unknown peer must not start a probe"
        );

        // Dead peer: we have already given up on it — no reactive probe.
        let dead = probe.step(now, SwimEvent::SendFailed { to: node(2) }, &mut members);
        assert!(
            ping_target(&dead).is_none(),
            "a SendFailed for a Dead peer must not start a probe"
        );

        // Known live peer, Idle: the failure reactively probes exactly that peer.
        let live = probe.step(now, SwimEvent::SendFailed { to: node(1) }, &mut members);
        assert_eq!(
            ping_target(&live),
            Some(node(1)),
            "a SendFailed for a known live peer must reactively probe that peer"
        );
    }

    // ─── §9.5 — one probe_timeout bounds BOTH the direct and indirect phase ──────

    #[test]
    fn the_same_probe_timeout_bounds_both_phases() {
        // §9.5: a probe sent at T fans out indirect probes at exactly T+probe_timeout
        // (direct phase), and — if still unanswered — suspects the target at exactly
        // T+2·probe_timeout (indirect phase). The two phases share one budget.
        let config = SwimConfig {
            probe_interval: ticks(5),
            probe_timeout: ticks(3),
            indirect_probes: 2,
            suspicion_timeout: ticks(1000), // irrelevant here
            dead_reprobe_interval: ticks(0),
            ..SwimConfig::default()
        };
        let mut now = Instant::now();
        let mut probe = SwimProbe::new(config, now);
        let mut members = MemberList::new(node(0));
        members.apply(node(1), MemberState::Alive, 0);
        members.apply(node(2), MemberState::Alive, 0);
        members.apply(node(3), MemberState::Alive, 0);

        // Tick 5 fires the probe (a SendPing).
        let fired = tick_n(&mut probe, &mut members, &mut now, 5);
        assert!(
            fired
                .iter()
                .any(|a| matches!(a, SwimAction::SendPing { .. })),
            "a probe must fire at the probe interval"
        );

        // Two ticks later: still inside the direct budget — no indirect fanout yet.
        let early = tick_n(&mut probe, &mut members, &mut now, 2);
        assert!(
            !has_ping_req(&early),
            "indirect probes must not fire before probe_timeout elapses"
        );

        // The third tick hits probe_timeout exactly — the direct phase ends here.
        let at_direct_timeout = tick_once(&mut probe, &mut members, &mut now);
        assert!(
            has_ping_req(&at_direct_timeout),
            "the direct phase must end at exactly probe_timeout"
        );
        assert!(
            !has_suspect(&at_direct_timeout),
            "the target is not suspected yet — the indirect phase just began"
        );

        // The indirect phase has its OWN probe_timeout: two more ticks, no suspicion.
        let early = tick_n(&mut probe, &mut members, &mut now, 2);
        assert!(
            !has_suspect(&early),
            "suspicion must not fire before the indirect probe_timeout elapses"
        );

        // The third tick hits the second probe_timeout — now the target is suspected.
        let at_indirect_timeout = tick_once(&mut probe, &mut members, &mut now);
        assert!(
            has_suspect(&at_indirect_timeout),
            "the indirect phase must end at another probe_timeout, suspecting the target"
        );
    }

    // ─── §9.5 — the still-Suspect guard honors a mid-window refute ───────────────

    #[test]
    fn mid_window_refute_cancels_the_pending_death() {
        // §9.5: a refutation (Alive at a higher incarnation) merged in before the
        // suspicion timer expires clears the Suspect state; the still-Suspect guard
        // must honor that and drop the timer WITHOUT declaring the node Dead.
        let config = SwimConfig {
            probe_interval: ticks(5),
            probe_timeout: ticks(3),
            indirect_probes: 0,
            suspicion_timeout: ticks(10),
            dead_reprobe_interval: ticks(0),
            ..SwimConfig::default()
        };
        let mut now = Instant::now();
        let mut probe = SwimProbe::new(config, now);
        let mut members = MemberList::new(node(0));
        members.apply(node(1), MemberState::Alive, 0);

        // Drive a genuine timeout to produce the Suspect action, and apply it like
        // the handler would (§10.6): node 1 is now Suspect with a running timer.
        tick_n(&mut probe, &mut members, &mut now, 5); // ping
        tick_n(&mut probe, &mut members, &mut now, 3); // direct timeout → indirect phase
        let suspected = tick_n(&mut probe, &mut members, &mut now, 3); // indirect timeout → Suspect
        assert!(
            has_suspect(&suspected),
            "precondition: the unanswered probe must produce Suspect"
        );
        assert!(
            members.suspect(node(1)),
            "the handler applies the Suspect transition"
        );

        // Mid-window: node 1 refutes — a higher-incarnation Alive merges in.
        members.apply(node(1), MemberState::Alive, 1);
        assert_eq!(members.get(&node(1)).unwrap().state, MemberState::Alive);

        // Tick well past the suspicion timeout. Because node 1 is no longer Suspect,
        // the guard must never declare it Dead.
        let actions = tick_n(&mut probe, &mut members, &mut now, 30);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, SwimAction::DeclareDead(_))),
            "a refuted node must not be declared Dead — the still-Suspect guard honors the refute"
        );
        assert_eq!(members.get(&node(1)).unwrap().state, MemberState::Alive);
    }

    // ─── §10.6 — a genuinely silent peer ends Suspect, then Dead (with effects) ──

    #[test]
    fn unanswered_probe_drives_member_through_suspect_then_dead_with_notifications() {
        // §9 + §10.6: the litmus from the behavioral spec — a genuinely unanswered
        // probe ends Suspect, then Dead after the suspicion window. Each transition
        // fires a MembershipChanged and is re-gossiped.
        let config = SwimConfig {
            probe_interval: ticks(5),
            probe_timeout: ticks(3),
            indirect_probes: 0,
            suspicion_timeout: ticks(10),
            dead_reprobe_interval: ticks(0),
            ..SwimConfig::default()
        };
        let mut now = Instant::now();
        let mut swim = SwimNode::new(node(0), config, now);
        swim.handle_join_request(node(1)); // node 1 is a member; it will never answer

        let mut notifications: Vec<(NodeId, MemberState, u64)> = Vec::new();
        for _ in 0..40 {
            now += TICK;
            for a in &swim.tick(now) {
                if let NodeAction::MembershipChanged {
                    node_id,
                    state,
                    incarnation,
                } = a
                {
                    notifications.push((*node_id, *state, *incarnation));
                }
            }
        }

        let suspect_at = notifications
            .iter()
            .position(|(id, s, _)| *id == node(1) && *s == MemberState::Suspect);
        let dead_at = notifications
            .iter()
            .position(|(id, s, _)| *id == node(1) && *s == MemberState::Dead);
        assert!(
            suspect_at.is_some(),
            "a silent peer must be notified Suspect, got {notifications:?}"
        );
        assert!(
            dead_at.is_some(),
            "a silent peer must then be notified Dead, got {notifications:?}"
        );
        assert!(
            suspect_at < dead_at,
            "Suspect must precede Dead in the notification stream"
        );

        // The settled membership view agrees: node 1 is Dead, no longer alive.
        assert!(
            swim.members()
                .all_members()
                .iter()
                .any(|e| e.node_id == node(1) && e.state == MemberState::Dead),
            "the settled view must show node 1 as Dead"
        );
        assert_eq!(
            swim.members().alive_count(),
            0,
            "a Dead peer is not counted alive"
        );

        // §10.6/§7 inv.5: the Dead transition was enqueued for dissemination — it
        // rides the next outgoing message. Drain the queue via a throwaway probe,
        // which is topology-independent: in a 1-peer cluster no probe fires once the
        // only member is Dead. (This learns node 9, so it runs after the view check.)
        let onward = match swim
            .handle_ping(node(9), 1, &[])
            .into_iter()
            .find_map(|a| match a {
                NodeAction::SendAck { piggyback, .. } => Some(piggyback),
                _ => None,
            }) {
            Some(pb) => DisseminationQueue::unpack_piggyback(&pb),
            None => Vec::new(),
        };
        assert!(
            onward
                .iter()
                .any(|u| u.node_id == node(1) && u.state == MemberState::Dead),
            "the Dead transition must be re-gossiped, but the queue held {onward:?}"
        );
    }

    // ─── §12 — Lifeguard can only lengthen the suspicion window, never shorten ───

    /// Count ticks from "node 1 is freshly Suspect" until `DeclareDead` fires.
    fn ticks_until_dead(config: SwimConfig) -> u64 {
        let mut now = Instant::now();
        let mut probe = SwimProbe::new(config, now);
        let mut members = MemberList::new(node(0));
        members.apply(node(1), MemberState::Alive, 0);
        tick_n(&mut probe, &mut members, &mut now, 5);
        tick_n(&mut probe, &mut members, &mut now, 3);
        let suspected = tick_n(&mut probe, &mut members, &mut now, 3);
        for a in &suspected {
            if let SwimAction::Suspect(id) = a {
                members.suspect(*id);
            }
        }
        let mut count = 0u64;
        loop {
            count += 1;
            now += TICK;
            let actions = probe.step(now, SwimEvent::Tick, &mut members);
            if actions
                .iter()
                .any(|a| matches!(a, SwimAction::DeclareDead(_)))
            {
                return count;
            }
            if count >= 10_000 {
                return count; // guard against a hang under misconfiguration
            }
        }
    }

    #[test]
    fn lifeguard_never_shortens_below_the_static_floor() {
        // §12: the effective suspicion timeout is `max(config.suspicion_timeout,
        // dynamic_…)`. When the adaptive value is SMALLER than the static floor, the
        // floor wins — a healthy small cluster must not die faster than the static
        // window. Same static floor on both sides; only the Lifeguard band differs.
        let base = SwimConfig {
            probe_interval: ticks(5),
            probe_timeout: ticks(3),
            indirect_probes: 0,
            suspicion_timeout: ticks(10), // the static floor
            dead_reprobe_interval: ticks(0),
            ..SwimConfig::default()
        };
        let static_ticks = ticks_until_dead(SwimConfig {
            lifeguard: None,
            ..base.clone()
        });
        let floored_ticks = ticks_until_dead(SwimConfig {
            // A deliberately tiny adaptive band — its dynamic timeout is far below
            // the 10-tick static floor, so the floor must dominate.
            lifeguard: Some(LifeguardConfig {
                base_suspicion_timeout: ticks(1),
                min_suspicion_timeout: ticks(1),
                max_suspicion_timeout: ticks(2),
                ..LifeguardConfig::default()
            }),
            ..base
        });
        assert_eq!(
            floored_ticks, static_ticks,
            "Lifeguard with a sub-floor band ({floored_ticks}) must not shorten the static window ({static_ticks})"
        );
    }
}

mod handler_emission {
    //! SwimNode handler boundary: each incoming protocol message emits the exact expected action
    //! set.

    use std::time::Instant;

    use distribution::messages::MembershipUpdate;
    use distribution::swim::dissemination::{DisseminationQueue, membership_update};
    use distribution::swim::node::{NodeAction, SwimNode};
    use distribution::swim::probe::SwimConfig;
    use distribution::types::{MemberState, NodeId, NodeRecord};

    fn node(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    fn fresh_node(self_byte: u8) -> SwimNode {
        SwimNode::new(node(self_byte), SwimConfig::default(), Instant::now())
    }

    /// The `(node_id, state, incarnation)` of every `MembershipChanged` in `actions`.
    fn membership_changes(actions: &[NodeAction]) -> Vec<(NodeId, MemberState, u64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                NodeAction::MembershipChanged {
                    node_id,
                    state,
                    incarnation,
                } => Some((*node_id, *state, *incarnation)),
                _ => None,
            })
            .collect()
    }

    /// A piggyback frame carrying a single update about `target`.
    fn piggyback_about(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(target, state, incarnation), 5);
        q.pack_piggyback(10)
    }

    /// Drain whatever the node currently has queued for dissemination by triggering
    /// one outgoing message and decoding its piggyback. `prober` is a throwaway
    /// peer id (learning it has no dissemination effect, §10.2), so the decoded
    /// updates are exactly what the handler-under-test enqueued.
    fn drain_dissemination(swim: &mut SwimNode, prober: NodeId) -> Vec<MembershipUpdate> {
        for a in swim.handle_ping(prober, 9999, &[]) {
            if let NodeAction::SendAck { piggyback, .. } = a {
                return DisseminationQueue::unpack_piggyback(&piggyback);
            }
        }
        Vec::new()
    }

    // ─── §10.0 — gossip ingestion ────────────────────────────────────────────────

    #[test]
    fn accepted_gossip_emits_one_change_and_re_gossips_it() {
        // §10.0 + §7 inv. 5: an accepted merge emits exactly one MembershipChanged
        // and re-enqueues that update for dissemination — the infection edge.
        let mut swim = fresh_node(0);
        let actions = swim.handle_ping(
            node(1),
            1,
            &piggyback_about(node(2), MemberState::Suspect, 3),
        );

        let changes = membership_changes(&actions);
        assert!(
            changes.contains(&(node(2), MemberState::Suspect, 3)),
            "an accepted gossip merge must notify via MembershipChanged, got {changes:?}"
        );

        // The very ack this handler emits must carry the merged update onward.
        let ack_pb = actions.iter().find_map(|a| match a {
            NodeAction::SendAck { piggyback, .. } => Some(piggyback.clone()),
            _ => None,
        });
        let onward =
            DisseminationQueue::unpack_piggyback(&ack_pb.expect("Ping must produce an Ack"));
        assert!(
            onward.iter().any(|u| u.node_id == node(2)
                && u.state == MemberState::Suspect
                && u.incarnation == 3),
            "every accepted change must be re-gossiped (§7 inv. 5)"
        );
    }

    #[test]
    fn dominated_gossip_emits_no_change() {
        // §10.0: gossip the merge rejects (stale) produces no notification and
        // nothing new to disseminate — it is a pure no-op at the boundary.
        let mut swim = fresh_node(0);
        swim.handle_ping(node(1), 1, &piggyback_about(node(2), MemberState::Dead, 5));
        // A stale, dominated update about node 2.
        let actions =
            swim.handle_ping(node(1), 2, &piggyback_about(node(2), MemberState::Alive, 1));
        assert!(
            membership_changes(&actions).is_empty(),
            "a dominated (stale) gossip update must emit no MembershipChanged"
        );
    }

    // ─── §10.2 — Ping learns its sender ──────────────────────────────────────────

    #[test]
    fn ping_acks_the_sender() {
        // §10.2: a Ping is always answered with an Ack addressed back to the sender.
        let mut swim = fresh_node(0);
        let actions = swim.handle_ping(node(1), 42, &[]);
        let ack_to = actions.iter().find_map(|a| match a {
            NodeAction::SendAck { to, sequence, .. } => Some((*to, *sequence)),
            _ => None,
        });
        assert_eq!(
            ack_to,
            Some((node(1), 42)),
            "Ping must produce exactly one Ack to the sender"
        );
    }

    #[test]
    fn ping_that_learns_a_new_sender_notifies_via_membership_changed() {
        // §10.2 (the flagged learn-sender notification): with `MembershipChanged`
        // now the sole membership channel, a Ping that first learns its sender must
        // route the event through it — consistent with Join and gossip.
        //
        // EXPECTED RED against the transcribed source, which emitted only a removed
        // diagnostic here; the actor rewrite routes it through MembershipChanged.
        let mut swim = fresh_node(0);
        let actions = swim.handle_ping(node(1), 1, &[]);
        let changes = membership_changes(&actions);
        assert!(
            changes.contains(&(node(1), MemberState::Alive, 0)),
            "learning a previously-unknown sender must notify via MembershipChanged, got {changes:?}"
        );
    }

    #[test]
    fn ping_from_known_sender_is_not_a_membership_change() {
        // §10.2: re-pinging from an already-tracked Alive sender is not a change —
        // `apply(from, Alive, 0)` does not downgrade or duplicate it.
        let mut swim = fresh_node(0);
        swim.handle_ping(node(1), 1, &[]); // first sighting
        let actions = swim.handle_ping(node(1), 2, &[]); // already known
        assert!(
            membership_changes(&actions).is_empty(),
            "a Ping from a known Alive sender must emit no MembershipChanged"
        );
    }

    // ─── §10.8 — JoinRequest (we are the seed) ───────────────────────────────────

    #[test]
    fn join_request_admits_joiner_and_replies_with_self_in_the_roster() {
        // §10.8: a new joiner is admitted (one MembershipChanged + enqueued for
        // dissemination) and answered with a JoinResponse whose roster includes the
        // responder itself.
        let mut seed = fresh_node(0);
        let actions = seed.handle_join_request(node(1));

        assert_eq!(
            membership_changes(&actions),
            vec![(node(1), MemberState::Alive, 0)],
            "admitting a new joiner must fire exactly one MembershipChanged"
        );

        let roster = actions.iter().find_map(|a| match a {
            NodeAction::SendJoinResponse { to, members } if *to == node(1) => Some(members.clone()),
            _ => None,
        });
        let roster = roster.expect("JoinRequest must produce a JoinResponse to the joiner");
        assert!(
            roster.iter().any(|r| r.node_id == node(0)),
            "§10.8: the JoinResponse roster must include the responder (self)"
        );

        // §7 inv. 5: the admitted joiner is enqueued for dissemination.
        let drained = drain_dissemination(&mut seed, node(7));
        assert!(
            drained
                .iter()
                .any(|u| u.node_id == node(1) && u.state == MemberState::Alive),
            "JoinRequest must enqueue the new member for gossip"
        );
    }

    #[test]
    fn re_join_of_known_member_emits_no_change_but_still_responds() {
        // §10.8: a JoinRequest from an already-known member is not a membership
        // change, but the seed still answers with a fresh roster.
        let mut seed = fresh_node(0);
        seed.handle_join_request(node(1));
        let actions = seed.handle_join_request(node(1));
        assert!(
            membership_changes(&actions).is_empty(),
            "re-join of a known member must emit no MembershipChanged"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, NodeAction::SendJoinResponse { to, .. } if *to == node(1))),
            "a JoinRequest must always be answered with a JoinResponse"
        );
    }

    // ─── §10.9 — JoinResponse (we are the joiner) — the asymmetry ─────────────────

    #[test]
    fn join_response_seeds_the_roster_with_one_change_per_record() {
        // §10.9: the joiner fires one MembershipChanged per newly-learned record.
        let mut joiner = fresh_node(1);
        let actions = joiner.handle_join_response(vec![
            NodeRecord {
                node_id: node(2),
                state: MemberState::Alive,
                incarnation: 0,
            },
            NodeRecord {
                node_id: node(3),
                state: MemberState::Alive,
                incarnation: 0,
            },
        ]);
        let mut changed: Vec<NodeId> = membership_changes(&actions)
            .into_iter()
            .map(|(id, ..)| id)
            .collect();
        changed.sort();
        assert_eq!(
            changed,
            vec![node(2), node(3)],
            "one MembershipChanged per newly-seeded record"
        );
    }

    #[test]
    fn join_response_does_not_re_gossip_the_bulk_snapshot() {
        // §10.9 asymmetry (the lone exception to §7 inv. 5): a JoinResponse seeds
        // the member list and fires notifications, but must NOT enqueue the bulk
        // snapshot for dissemination — re-gossiping the whole roster would be a
        // burst, and the members are confirmed by subsequent probing.
        let mut joiner = fresh_node(1);
        joiner.handle_join_response(vec![
            NodeRecord {
                node_id: node(2),
                state: MemberState::Alive,
                incarnation: 0,
            },
            NodeRecord {
                node_id: node(3),
                state: MemberState::Alive,
                incarnation: 0,
            },
            NodeRecord {
                node_id: node(4),
                state: MemberState::Dead,
                incarnation: 2,
            },
        ]);
        let drained = drain_dissemination(&mut joiner, node(9));
        assert!(
            drained.is_empty(),
            "§10.9: JoinResponse must not re-gossip the bulk snapshot, but queued {drained:?}"
        );
    }

    // ─── §10.10 — Leave ──────────────────────────────────────────────────────────

    #[test]
    fn leave_enqueues_self_dead_without_immediate_notification() {
        // §10.10: Leave gossips self as Dead at the current incarnation and fires no
        // immediate MembershipChanged — the Dead self-record propagates on later
        // piggybacks.
        let mut swim = fresh_node(0);
        let actions = swim.leave();
        assert!(
            membership_changes(&actions).is_empty(),
            "Leave must produce no immediate MembershipChanged"
        );
        let drained = drain_dissemination(&mut swim, node(9));
        assert!(
            drained
                .iter()
                .any(|u| u.node_id == node(0) && u.state == MemberState::Dead),
            "Leave must enqueue self as Dead for subsequent piggybacks"
        );
    }

    // ─── §10.4 / §10.3 / §10.5 — the relay path ──────────────────────────────────

    #[test]
    fn ping_req_relays_a_ping_to_the_target() {
        // §10.4: as the relay, a PingReq forwards a Ping to the named target with
        // the same sequence (its `from` is self on the wire).
        let mut relay = fresh_node(0);
        let actions = relay.handle_ping_req(node(1), node(2), 77, &[]);
        let forwarded = actions.iter().find_map(|a| match a {
            NodeAction::SendPing { to, sequence, .. } => Some((*to, *sequence)),
            _ => None,
        });
        assert_eq!(
            forwarded,
            Some((node(2), 77)),
            "a PingReq must forward a Ping to the target carrying the same sequence"
        );
    }

    #[test]
    fn relay_forwards_indirect_ack_to_requester_on_target_ack() {
        // §10.3: once the relayed target acks (matching the pending relay's
        // target+sequence), the relay sends an IndirectAck back to the original
        // requester — and only to it.
        let mut relay = fresh_node(0);
        relay.handle_ping_req(node(1), node(2), 88, &[]); // requester = 1, target = 2
        let actions = relay.handle_ack(node(2), 88, &[]); // the target answers
        let fwd = actions.iter().find_map(|a| match a {
            NodeAction::ForwardAck {
                to,
                target,
                sequence,
                ..
            } => Some((*to, *target, *sequence)),
            _ => None,
        });
        assert_eq!(
            fwd,
            Some((node(1), node(2), 88)),
            "a matching target Ack must be relayed home to the requester as an IndirectAck"
        );
    }

    #[test]
    fn unmatched_ack_does_not_forward_an_indirect_ack() {
        // §10.3: an Ack that matches no pending relay produces no IndirectAck.
        let mut relay = fresh_node(0);
        relay.handle_ping_req(node(1), node(2), 88, &[]);
        let actions = relay.handle_ack(node(2), 999, &[]); // wrong sequence
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, NodeAction::ForwardAck { .. })),
            "an Ack with no matching pending relay must not be forwarded"
        );
    }

    #[test]
    fn indirect_ack_applies_its_piggyback_gossip() {
        // §10.5: as the original prober, an IndirectAck's piggyback is merged like
        // any other gossip — the indirectly-probed peer's news rides home on it.
        let mut prober = fresh_node(0);
        let actions = prober.handle_indirect_ack(
            node(2),
            5,
            &piggyback_about(node(3), MemberState::Suspect, 4),
        );
        assert!(
            membership_changes(&actions).contains(&(node(3), MemberState::Suspect, 4)),
            "an IndirectAck must apply the gossip carried in its piggyback"
        );
    }
}

mod node_behavior {
    //! In-memory SWIM cluster behavior: join convergence, silence detection, resurrection,
    //! dissemination, and notification reconstruction.

    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use distribution::swim::dissemination::{DisseminationQueue, membership_update};
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
        ml.all_members()
            .iter()
            .map(|e| (e.node_id.0, (e.state, e.incarnation)))
            .collect()
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
            let nodes = ids
                .iter()
                .map(|&nid| SwimNode::new(nid, config.clone(), now))
                .collect();
            let mut c = Cluster {
                ids,
                nodes,
                clock: now,
                notifications: vec![Vec::new(); n],
            };
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
                if let NodeAction::MembershipChanged {
                    node_id,
                    state,
                    incarnation,
                } = a
                {
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
                    NodeAction::SendPing {
                        to,
                        sequence,
                        piggyback,
                    } => {
                        if let Some(t) = self.index_of(to) {
                            if t != origin && !excluded.contains(&t) {
                                let from = self.ids[origin];
                                let resp = self.nodes[t].handle_ping(from, sequence, &piggyback);
                                self.record(t, &resp);
                                self.route(t, resp, excluded);
                            }
                        }
                    }
                    NodeAction::SendAck {
                        to,
                        sequence,
                        piggyback,
                    } => {
                        if let Some(t) = self.index_of(to) {
                            if t != origin && !excluded.contains(&t) {
                                let from = self.ids[origin];
                                let resp = self.nodes[t].handle_ack(from, sequence, &piggyback);
                                self.record(t, &resp);
                                self.route(t, resp, excluded);
                            }
                        }
                    }
                    NodeAction::SendPingReq {
                        relay,
                        target,
                        sequence,
                        piggyback,
                    } => {
                        if let Some(t) = self.index_of(relay) {
                            if t != origin && !excluded.contains(&t) {
                                let from = self.ids[origin];
                                let resp = self.nodes[t]
                                    .handle_ping_req(from, target, sequence, &piggyback);
                                self.record(t, &resp);
                                self.route(t, resp, excluded);
                            }
                        }
                    }
                    NodeAction::ForwardAck {
                        to,
                        target,
                        sequence,
                        piggyback,
                    } => {
                        if let Some(t) = self.index_of(to) {
                            if t != origin && !excluded.contains(&t) {
                                let resp =
                                    self.nodes[t].handle_indirect_ack(target, sequence, &piggyback);
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
        fn run_until<F: Fn(&Cluster) -> bool>(
            &mut self,
            excluded: &[usize],
            cap: usize,
            cond: F,
        ) -> bool {
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
            self.nodes[observer]
                .members()
                .get(&subject)
                .map(|e| e.state)
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
            (0..self.nodes.len())
                .all(|o| excluded.contains(&o) || self.state_of(o, subject) == Some(state))
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
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "cluster did not converge to a shared Alive view"
        );
        for observer in 0..4 {
            assert_eq!(
                c.nodes[observer].members().alive_count(),
                3,
                "node {observer} must see all 3 peers alive"
            );
        }
    }

    // ─── Goal 2 — real detection ─────────────────────────────────────────────────

    #[test]
    fn goal2_a_truly_silent_node_is_detected_dead_by_survivors() {
        let mut c = Cluster::new(4, behavioral_config(t(0)));
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "precondition: cluster must converge"
        );

        // Genuinely silence node 3 — it neither ticks nor sends nor receives, so a
        // survivor's probe TRULY times out (not an injected death). Poll until the
        // survivors converge on it being Dead, or time out.
        let dead = 3usize;
        let detected = c.run_until(&[dead], 1000, |c| {
            c.survivors_see(&[dead], id(3), MemberState::Dead)
        });
        assert!(
            detected,
            "survivors must converge on the silenced node being Dead within the detection window"
        );
    }

    // ─── Goal 3 — death is provisional ───────────────────────────────────────────

    #[test]
    fn goal3_a_silenced_node_resurrects_when_it_answers_again() {
        // dead_reprobe enabled so the partition-heal detector (§9.8) re-probes the
        // Dead node and lets it refute back to Alive.
        let mut c = Cluster::new(4, behavioral_config(t(2)));
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "precondition: cluster must converge"
        );

        let isolated = 3usize;
        let died = c.run_until(&[isolated], 1000, |c| {
            c.survivors_see(&[isolated], id(3), MemberState::Dead)
        });
        assert!(
            died,
            "the silenced node must first be detected Dead by the survivors"
        );

        // Restore the node: it answers probes again, so the reprobe revives it.
        let revived = c.run_until(&[], 1000, |c| {
            c.survivors_see(&[isolated], id(3), MemberState::Alive)
        });
        assert!(
            revived,
            "the restored node must resurrect to Alive (death is provisional)"
        );

        // A survivor's notification stream witnessed the full provisional arc.
        let s = if isolated == 0 { 1 } else { 0 };
        let stream = &c.notifications[s];
        let dead_at = stream
            .iter()
            .position(|(n, st, _)| *n == id(isolated as u8) && *st == MemberState::Dead);
        let alive_after = stream
            .iter()
            .rposition(|(n, st, _)| *n == id(isolated as u8) && *st == MemberState::Alive);
        assert!(
            matches!((dead_at, alive_after), (Some(d), Some(a)) if a > d),
            "a survivor must witness node {isolated} go Dead then back Alive"
        );
    }

    // ─── Goal 6 — dissemination reaches everyone ─────────────────────────────────

    #[test]
    fn goal6_a_single_change_known_to_one_node_infects_every_node() {
        let mut c = Cluster::new(4, behavioral_config(t(0)));
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "precondition: cluster must converge"
        );

        // A change known to ONLY node 0: a phantom peer reported Dead in a single
        // gossip exchange. No other node has ever heard of this peer, and no node
        // probes a Dead member — so the others can learn it ONLY by multi-hop
        // piggyback infection, never by direct observation.
        let phantom = id(9);
        let from = c.ids[1];
        let pb = piggyback_about(phantom, MemberState::Dead, 5);
        let _ = c.nodes[0].handle_ping(from, 1, &pb);

        let infected = c.run_until(&[], 1000, |c| {
            c.survivors_see(&[], phantom, MemberState::Dead)
        });
        assert!(
            infected,
            "the single change must infect every node — dissemination reaches everyone, not just probe partners"
        );
    }

    // ─── Goal 7 — notification contract ──────────────────────────────────────────

    #[test]
    fn goal7_membership_changed_stream_reconstructs_the_view() {
        // §6.3 / Move 1: `MembershipChanged` is the sole observable and the stream
        // alone must reconstruct the settled membership view. (May be red until the
        // §10.2 learn-sender notification lands — a peer learned via an inbound Ping
        // mutates the view today without emitting MembershipChanged.)
        let mut c = Cluster::new(4, behavioral_config(t(0)));
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "cluster did not converge"
        );
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
            if let NodeAction::MembershipChanged {
                node_id,
                state,
                incarnation,
            } = act
            {
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
        assert!(
            c.run_until(&[], 500, |c| c.all_converged_alive()),
            "cluster did not converge"
        );
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
}

mod membership_safety_edges {
    //! Additional generic SWIM safety edges folded into core guarantees: stale accusations, direct
    //! Dead imports, equal-incarnation priority, and mid-window refutation.

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
}
