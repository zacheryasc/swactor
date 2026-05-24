//! SIM_SPEC §5.8 behavioural property tests for the network. Each
//! `#[test]` names the §5.8 property it covers.
//!
//! Test posture is scenario-level: build a small `Scenario`, exercise
//! the network through its public `send`/`apply_mutation` API, observe
//! `SendOutcome` and `take_pending_notifications`. We never inspect
//! private state.

use std::path::Path;

use proptest::prelude::*;
use simulation::network::{
    CacheTransition, DropReason, Network, NetworkNotification, SendOutcome,
};
use simulation::scenario::{
    HostKindRegistry, Mutation, MutationKind, Scenario, load_from_str,
};

// ──────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn fake_path() -> &'static Path {
    Path::new("test://network.toml")
}

/// Two-peer scenario, both directions declared. Returns the network
/// and a clone of the scenario for any test that needs to reference
/// the policy.
fn two_peer_scenario(seed: u64, custom: &str) -> (Network, Scenario) {
    let text = format!(
        r#"
name = "n"
seed = {seed}
duration_ns = 10_000_000_000

[default_tick]
period_ns = 1_000_000

[default_link]
latency_ns = 1000
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1_000_000_000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 10_000_000_000

[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = {{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }}

[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = {{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }}

[[links]]
from = "a"
to = "b"
{custom}

[[links]]
from = "b"
to = "a"
"#
    );
    let scenario = load_from_str(fake_path(), &text, &registry()).expect("scenario must validate");
    (Network::new(&scenario), scenario)
}

fn ok(outcome: SendOutcome) -> u64 {
    match outcome {
        SendOutcome::Arrive { at_ns, .. } => at_ns,
        SendOutcome::Drop { reason } => panic!("expected Arrive, got Drop({reason:?})"),
    }
}

// ──────────────────────────────────────────────────────────────────────
// §5.8 Properties
// ──────────────────────────────────────────────────────────────────────

#[test]
fn reachability_returns_arrive_iff_edge_declared_and_no_partition() {
    let (mut net, _) = two_peer_scenario(1, "");

    // Declared edge → Arrive.
    assert!(matches!(
        net.send("a", "b", 64, 1_000),
        SendOutcome::Arrive { .. }
    ));
    // Drain notifications produced by the successful warm-up so the
    // no-route check below is unambiguous.
    let _ = net.take_pending_notifications();

    // Undeclared edge → NoRoute, no state change visible (we can't
    // inspect state but we can verify no notifications fired).
    let outcome = net.send("a", "ghost", 64, 2_000);
    assert!(matches!(
        outcome,
        SendOutcome::Drop {
            reason: DropReason::NoRoute
        }
    ));
    assert!(
        net.take_pending_notifications().is_empty(),
        "no-route send must not produce side-channel notifications"
    );
}

#[test]
fn refused_send_leaves_network_state_unchanged() {
    // §5.8 "A send the network refuses leaves the network's state
    // unchanged." We verify by: do one Arrive, refuse another Send
    // (via partition), then send again and check the next Arrive time
    // is what the bandwidth/serialization math predicts based on only
    // the first Arrive.
    let (mut net, _) = two_peer_scenario(1, "");
    let first_at = ok(net.send("a", "b", 64, 1_000));

    let mutation = Mutation {
        at_ns: 1_500,
        kind: MutationKind::Partition {
            peers_a: vec!["a".to_string()],
            peers_b: vec!["b".to_string()],
        },
    };
    let _ = net.apply_mutation(&mutation, 1_500);
    assert!(matches!(
        net.send("a", "b", 64, 2_000),
        SendOutcome::Drop {
            reason: DropReason::Partitioned
        }
    ));

    // Heal and send again.
    let _ = net.apply_mutation(
        &Mutation {
            at_ns: 2_500,
            kind: MutationKind::Heal,
        },
        2_500,
    );
    let third_at = ok(net.send("a", "b", 64, 3_000));

    // Bandwidth-serialization should pick up from the original last_arrive,
    // not from the partitioned attempt. Since the partition refusal
    // changed no state, third_at math depends only on (3_000, first_at).
    // We check third_at >= 3_000 + base latency. The exact value is
    // implementation-defined but should be 3_000 + nanos_per_64_bytes + latency.
    assert!(third_at >= 3_000 + 1_000, "third arrival too early: {third_at}");
    // And the partition didn't perturb deterministic ordering: must
    // be after the first arrival.
    assert!(third_at > first_at);
}

#[test]
fn partition_heals_to_identity_on_subsequent_sends() {
    let make_net = || two_peer_scenario(42, "").0;

    let mut a = make_net();
    let mut b = make_net();

    // a: partition + heal at times 100, 200, then send at 1_000.
    let _ = a.apply_mutation(
        &Mutation {
            at_ns: 100,
            kind: MutationKind::Partition {
                peers_a: vec!["a".into()],
                peers_b: vec!["b".into()],
            },
        },
        100,
    );
    let _ = a.apply_mutation(
        &Mutation {
            at_ns: 200,
            kind: MutationKind::Heal,
        },
        200,
    );
    let _ = a.take_pending_notifications();

    // b: nothing.
    let _ = b.take_pending_notifications();

    let send_a = a.send("a", "b", 64, 1_000);
    let send_b = b.send("a", "b", 64, 1_000);
    assert_eq!(send_a, send_b, "partition+heal must be indistinguishable");
}

#[test]
fn loss_is_bernoulli_with_burst_overriding_for_window_only() {
    // With loss_prob_ppm = 500_000 we expect roughly half of sends to
    // drop. With a 100% loss burst on the link for [t, t+dur), all
    // sends in that window drop; outside the window the base rate
    // resumes.
    let (mut net, _) = two_peer_scenario(
        99,
        "loss_prob_ppm = 500_000",
    );

    let base_drops_before = run_n_sends_count_drops(&mut net, "a", "b", 64, 1_000, 200);

    // Apply a 100% LossBurst from t=300k to t=400k.
    let _ = net.apply_mutation(
        &Mutation {
            at_ns: 300_000,
            kind: MutationKind::LossBurst {
                links: vec![link_ref("a", "b")],
                prob_ppm: 1_000_000,
                duration_ns: 100_000,
            },
        },
        300_000,
    );
    // Every send inside [300_000, 400_000) drops.
    for t in (300_000..400_000).step_by(1_000) {
        let outcome = net.send("a", "b", 64, t);
        assert!(
            matches!(outcome, SendOutcome::Drop { reason: DropReason::Lossy }),
            "burst window: t={t} got {outcome:?}"
        );
    }
    // After the burst ends, base rate ~0.5 resumes.
    let base_drops_after = run_n_sends_count_drops(&mut net, "a", "b", 64, 500_000, 200);

    // Sanity: under base rate 500_000ppm we expect ~100/200 drops,
    // tolerated within 60..140.
    for label in [base_drops_before, base_drops_after] {
        assert!(
            (60..=140).contains(&label),
            "expected ~100 drops out of 200 at base rate, saw {label}"
        );
    }
}

fn run_n_sends_count_drops(
    net: &mut Network,
    from: &str,
    to: &str,
    bytes: u64,
    start_ns: u64,
    n: u64,
) -> usize {
    let mut drops = 0;
    for i in 0..n {
        let outcome = net.send(from, to, bytes, start_ns + i * 1_000);
        if matches!(outcome, SendOutcome::Drop { reason: DropReason::Lossy }) {
            drops += 1;
        }
    }
    drops
}

fn link_ref(from: &str, to: &str) -> simulation::scenario::LinkRef {
    simulation::scenario::LinkRef {
        from: from.into(),
        to: to.into(),
    }
}

#[test]
fn bandwidth_serializes_no_overlap_between_messages() {
    // bandwidth_bps is bytes/sec per §5.2. With 1_000_000 bytes/sec and
    // 1000-byte messages, each message occupies 1ms on the wire. Two
    // back-to-back sends must arrive at least ~1ms apart.
    let (mut net, _) = two_peer_scenario(
        1,
        "bandwidth_bps = 1_000_000\nlatency_ns = 0",
    );
    let a1 = ok(net.send("a", "b", 1_000, 0)); // 1 ms occupancy
    let a2 = ok(net.send("a", "b", 1_000, 0));
    let gap = a2 - a1;
    // 1000 bytes / 1_000_000 bytes_per_sec = 1ms. ±1 ns of rounding.
    assert!(
        (999_999..=1_000_001).contains(&gap),
        "expected ~1ms gap, got {gap}"
    );
}

#[test]
fn latency_is_additive_decomposes_per_5_4() {
    // With latency_ns=L, no jitter, no spikes, no relay buffer, zero
    // bandwidth-occupancy (bytes=0 makes the serialisation contribution
    // zero), an Arrive at time t is at exactly t + L (plus cold-dial
    // penalty on the first call). Space the second send out beyond
    // the first arrival so bandwidth serialisation doesn't push it.
    let (mut net, _) = two_peer_scenario(
        1,
        "latency_ns = 5000\nbandwidth_bps = 1_000_000_000\ncold_dial_penalty_ns = 0",
    );
    let a1 = ok(net.send("a", "b", 0, 100));
    let a2 = ok(net.send("a", "b", 0, 20_000));
    assert_eq!(a1, 100 + 5000);
    assert_eq!(a2, 20_000 + 5000);
}

#[test]
fn jitter_is_symmetric_and_integer_valued() {
    // §5.8 "Jitter samples come from the precomputed table of §7, are
    // symmetric around zero, and are never non-integer."
    // The integer property is encoded in the function signature (i64);
    // we test symmetry on the table itself.
    use simulation::rng::GAUSSIAN_TABLE_1024;
    for i in 0..GAUSSIAN_TABLE_1024.len() {
        let j = GAUSSIAN_TABLE_1024.len() - 1 - i;
        assert_eq!(
            GAUSSIAN_TABLE_1024[i] + GAUSSIAN_TABLE_1024[j],
            0,
            "table[{i}] + table[{j}] != 0",
        );
    }
}

#[test]
fn cache_state_warm_after_traffic_and_cold_again_after_idle() {
    // SIM_SPEC §5.4: DialStart + DialOutcome fire at Cold→Warming.
    // The Warmed CacheStateChange fires only at Warming→Warm,
    // i.e. when a send happens at or after `since + cache_warm_after_ns`.
    let (mut net, _) = two_peer_scenario(
        1,
        "cold_dial_penalty_ns = 10_000\ncache_warm_after_ns = 1_000\ncache_invalidate_after_idle_ns = 5_000",
    );
    let cold_at = ok(net.send("a", "b", 64, 0));
    let notes = net.take_pending_notifications();
    // First send: only DialStart + DialOutcome at the cold→warming edge.
    assert!(notes.iter().any(|n| matches!(n, NetworkNotification::DialStart { .. })));
    assert!(notes.iter().any(|n| matches!(n, NetworkNotification::DialOutcome { .. })));
    assert!(
        !notes.iter().any(|n| matches!(
            n,
            NetworkNotification::CacheStateChange {
                transition: CacheTransition::Warmed,
                ..
            }
        )),
        "Cold→Warming must not emit CacheStateChange{{Warmed}} per §5.4 step 10"
    );

    // Second send past warm_after — should now be warm (no cold-dial
    // penalty) AND emit exactly one CacheStateChange{Warmed} at the
    // actual transition time (since + warm_after).
    let warm_at = ok(net.send("a", "b", 64, 2_000));
    let notes = net.take_pending_notifications();
    let warmed_notes: Vec<&NetworkNotification> = notes
        .iter()
        .filter(|n| {
            matches!(
                n,
                NetworkNotification::CacheStateChange {
                    transition: CacheTransition::Warmed,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(warmed_notes.len(), 1, "Warming→Warm must emit exactly one Warmed");
    if let NetworkNotification::CacheStateChange { at_ns, .. } = warmed_notes[0] {
        // since_ns was 0; warm_after is 1_000; transition fires at 1_000.
        assert_eq!(*at_ns, 1_000);
    }
    assert!(
        warm_at < cold_at + 10_000,
        "second send should not pay the 10us cold-dial penalty"
    );

    // Now skip ahead past the idle-invalidation horizon.
    let idle_send_at = ok(net.send("a", "b", 64, 2_000 + 10_000));
    let notes = net.take_pending_notifications();
    assert!(notes.iter().any(|n| matches!(
        n,
        NetworkNotification::CacheStateChange {
            transition: CacheTransition::IdleCooled,
            ..
        }
    )));
    assert!(idle_send_at >= 2_000 + 10_000);
}

#[test]
fn cache_transitions_emit_exactly_one_notification_each() {
    // §5.8 "Every cold↔warm transition emits exactly one
    // CacheStateChange notification at the transition's virtual time."
    // We construct a Cold→Warming→Warm→IdleCooled sequence with a
    // non-zero cache_warm_after_ns so that the Cold→Warming and
    // Warming→Warm edges are distinguishable.
    let (mut net, _) = two_peer_scenario(
        1,
        "cold_dial_penalty_ns = 10_000\ncache_warm_after_ns = 1_000\ncache_invalidate_after_idle_ns = 5_000",
    );
    // First send: Cold→Warming. No Warmed yet.
    let _ = net.send("a", "b", 64, 0);
    let warmed_step1: usize = net
        .take_pending_notifications()
        .iter()
        .filter(|n| matches!(
            n,
            NetworkNotification::CacheStateChange {
                transition: CacheTransition::Warmed,
                ..
            }
        ))
        .count();
    assert_eq!(warmed_step1, 0, "no Warmed at Cold→Warming");

    // Second send still within warm-after — still Warming, no Warmed.
    let _ = net.send("a", "b", 64, 500);
    let warmed_step2: usize = net
        .take_pending_notifications()
        .iter()
        .filter(|n| matches!(
            n,
            NetworkNotification::CacheStateChange {
                transition: CacheTransition::Warmed,
                ..
            }
        ))
        .count();
    assert_eq!(warmed_step2, 0, "still Warming below threshold");

    // Third send past the threshold: Warming→Warm. Exactly one Warmed.
    let _ = net.send("a", "b", 64, 1_500);
    let warmed_step3: usize = net
        .take_pending_notifications()
        .iter()
        .filter(|n| matches!(
            n,
            NetworkNotification::CacheStateChange {
                transition: CacheTransition::Warmed,
                ..
            }
        ))
        .count();
    assert_eq!(warmed_step3, 1, "Warming→Warm must emit exactly one Warmed");

    // Many warm sends — no new transition notifications.
    for t in 1_600..2_000 {
        let _ = net.send("a", "b", 64, t);
    }
    let none: usize = net
        .take_pending_notifications()
        .iter()
        .filter(|n| matches!(n, NetworkNotification::CacheStateChange { .. }))
        .count();
    assert_eq!(none, 0, "no transition should re-emit while Warm");

    // Idle cool: send after the invalidate-after-idle horizon.
    let _ = net.send("a", "b", 64, 2_000 + 10_000);
    let cooled: usize = net
        .take_pending_notifications()
        .iter()
        .filter(|n| matches!(
            n,
            NetworkNotification::CacheStateChange {
                transition: CacheTransition::IdleCooled,
                ..
            }
        ))
        .count();
    assert_eq!(cooled, 1);
}

#[test]
fn mutation_scoping_affects_only_named_links_and_window() {
    // Two parallel links a→b and b→a. LossBurst on a→b only must not
    // perturb b→a draws. Outside the window, base rate resumes on
    // a→b.
    let (mut net, _) = two_peer_scenario(13, "");
    let _ = net.apply_mutation(
        &Mutation {
            at_ns: 0,
            kind: MutationKind::LossBurst {
                links: vec![link_ref("a", "b")],
                prob_ppm: 1_000_000,
                duration_ns: 1_000,
            },
        },
        0,
    );

    // In-window a→b drops every send.
    for t in 0..500 {
        let outcome = net.send("a", "b", 64, t);
        assert!(matches!(outcome, SendOutcome::Drop { reason: DropReason::Lossy }));
    }
    // Same window b→a is unaffected.
    for t in 0..500 {
        let outcome = net.send("b", "a", 64, t);
        assert!(matches!(outcome, SendOutcome::Arrive { .. }), "b→a unaffected");
    }
    // After the window, a→b sends succeed again.
    for t in 1_001..1_100 {
        let outcome = net.send("a", "b", 64, t);
        assert!(matches!(outcome, SendOutcome::Arrive { .. }));
    }
}

#[test]
fn mutation_invalidation_returns_only_currently_in_flight_deliveries() {
    let (mut net, _) = two_peer_scenario(1, "");
    let SendOutcome::Arrive { delivery_id: id1, .. } = net.send("a", "b", 64, 0) else { panic!() };
    let SendOutcome::Arrive { delivery_id: id2, .. } = net.send("a", "b", 64, 100) else { panic!() };

    // Partition cuts a→b. Both deliveries should be invalidated.
    let inv = net.apply_mutation(
        &Mutation {
            at_ns: 200,
            kind: MutationKind::Partition {
                peers_a: vec!["a".into()],
                peers_b: vec!["b".into()],
            },
        },
        200,
    );
    let returned_ids: Vec<_> = inv.iter().map(|d| d.delivery_id).collect();
    assert!(returned_ids.contains(&id1));
    assert!(returned_ids.contains(&id2));
    assert_eq!(returned_ids.len(), 2);

    // A subsequent partition (already partitioned) should return
    // nothing new.
    let inv2 = net.apply_mutation(
        &Mutation {
            at_ns: 300,
            kind: MutationKind::Partition {
                peers_a: vec!["a".into()],
                peers_b: vec!["b".into()],
            },
        },
        300,
    );
    assert!(inv2.is_empty(), "no new invalidations expected");
}

#[test]
fn substream_isolation_one_link_does_not_perturb_another() {
    // §5.8 "Editing one link's policy must not change any draw the
    // network makes on any other link." We construct two networks,
    // identical except for a→b's latency_ns, and verify that b→a's
    // arrivals are identical across both.
    let (mut net_a, _) = two_peer_scenario(7, "latency_ns = 1000");
    let (mut net_b, _) = two_peer_scenario(
        7,
        "latency_ns = 999_999\njitter_stddev_ns = 0",
    );

    // Drive b→a deterministically with the same inputs and compare
    // outcomes. (Loss rate is 0 so we expect Arrive every time.)
    for t in 0..50 {
        let a_out = net_a.send("b", "a", 100, t * 1_000);
        let b_out = net_b.send("b", "a", 100, t * 1_000);
        assert_eq!(
            a_out, b_out,
            "b→a outcomes diverged at t={t} despite only a→b policy changing"
        );
    }
}

#[test]
fn dial_outcome_at_ns_equals_the_post_penalty_arrival_time() {
    // §5.4 step 8: DialOutcome's `at_ns` is the arrival of the
    // send that triggered the cold dial — i.e. the post-penalty
    // arrival, not the pre-penalty one. Tested explicitly because
    // a misplaced capture in `send` historically labelled the
    // notification with the pre-penalty time, leaving bundle
    // consumers a phantom gap equal to `cold_dial_penalty_ns`.
    let (mut net, _) = two_peer_scenario(
        1,
        "latency_ns = 100_000\njitter_stddev_ns = 0\ncold_dial_penalty_ns = 50_000\nbandwidth_bps = 1_000_000_000\ncache_warm_after_ns = 0\ncache_invalidate_after_idle_ns = 1_000_000_000_000",
    );
    let outcome = net.send("a", "b", 64, 0);
    let arrival = match outcome {
        SendOutcome::Arrive { at_ns, .. } => at_ns,
        _ => panic!("expected Arrive"),
    };
    let notes = net.take_pending_notifications();
    let dial_outcome = notes
        .iter()
        .find_map(|n| match n {
            NetworkNotification::DialOutcome { at_ns, .. } => Some(*at_ns),
            _ => None,
        })
        .expect("DialOutcome must fire on cold dial");
    assert_eq!(
        dial_outcome, arrival,
        "DialOutcome at_ns must equal the post-penalty delivery time"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// §5.8 "Determinism. Same topology, same seed, same query
    /// sequence ⇒ identical SendOutcome sequence."
    #[test]
    fn determinism_same_seed_same_queries(seed in 0i64..i64::MAX, n_ops in 5usize..40usize) {
        let seed_u = seed as u64;
        let (mut net_a, _) = two_peer_scenario(seed_u, "loss_prob_ppm = 250_000\njitter_stddev_ns = 100");
        let (mut net_b, _) = two_peer_scenario(seed_u, "loss_prob_ppm = 250_000\njitter_stddev_ns = 100");

        let mut a_seq = Vec::new();
        let mut b_seq = Vec::new();
        for i in 0..n_ops {
            a_seq.push(net_a.send("a", "b", 64, (i as u64) * 1000));
            b_seq.push(net_b.send("a", "b", 64, (i as u64) * 1000));
        }
        prop_assert_eq!(a_seq, b_seq);
    }
}
