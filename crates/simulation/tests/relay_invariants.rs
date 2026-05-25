//! RELAY_SPEC §4.7 / §6.2 behavioural tests for the relay vertex
//! and its mutations. These exercise the network layer directly,
//! mirroring `tests/network_invariants.rs` for the parent §5.7
//! properties.
//!
//! Each test name maps to a §4.7 / §6.2 property; the assertions
//! avoid mirroring the implementation and focus on what a third
//! party reading the spec would expect to observe.

use simulation::network::{DropReason, Network, NetworkNotification, RelayDropReason, SendOutcome};
use simulation::scenario::{HostKindRegistry, load_from_str};
use std::path::Path;

const ALL_NOTIFICATIONS: &str = include_str!("../scenarios/parity/reference.toml");

fn registry() -> HostKindRegistry {
    let mut r = HostKindRegistry::with_swim();
    r.register(Box::new(simulation::parity_host::ParityStubKindValidator));
    r
}

// Build a minimal scenario from inline TOML so the tests are self-
// contained — they don't reach for shipped scenario files.
fn scenario_from(text: &str) -> simulation::scenario::Scenario {
    load_from_str(Path::new("(test)"), text, &registry())
        .expect("test scenario must validate")
}

/// Three host topology routed through one relay. Used by most relay
/// tests.
fn one_relay_three_hosts() -> simulation::scenario::Scenario {
    scenario_from(
        r#"
        name = "relay_three_hosts"
        seed = 1
        duration_ns = 1_000_000_000

        [default_tick]
        period_ns = 1_000_000

        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000

        [[relays]]
        id = "R"
        ingress_capacity_bps = 1_000_000_000
        egress_capacity_bps_per_link = 8_000_000  # 1 MB/s per outbound link
        queue_depth_bytes = 1_000_000
        cold_start_penalty_ns = 0

        [[peers]]
        id = "alpha"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["alpha", "bravo", "charlie"] }
        [[peers]]
        id = "bravo"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["alpha", "bravo", "charlie"] }
        [[peers]]
        id = "charlie"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["alpha", "bravo", "charlie"] }

        [[links]]
        from = "alpha"
        to = "bravo"
        via = "R"
        [[links]]
        from = "bravo"
        to = "alpha"
        via = "R"
        [[links]]
        from = "alpha"
        to = "charlie"
        via = "R"
        [[links]]
        from = "charlie"
        to = "alpha"
        via = "R"
        [[links]]
        from = "bravo"
        to = "charlie"
        via = "R"
        [[links]]
        from = "charlie"
        to = "bravo"
        via = "R"
        "#,
    )
}

fn collect_notifications(net: &mut Network) -> Vec<NetworkNotification> {
    net.take_pending_notifications()
}

// ────────────────────────────────────────────────────────────────────
// RELAY_SPEC §4.7
// ────────────────────────────────────────────────────────────────────

#[test]
fn composition_is_transparent_to_hosts() {
    // §4.7 "Composition is transparent to hosts." A `send` over a
    // relayed route returns one `SendOutcome` shaped identically to a
    // direct send's. The arrival is a single integer; no extra
    // structure leaks.
    let scen = one_relay_three_hosts();
    let mut net = Network::new(&scen);
    let outcome = net.send("alpha", "bravo", 1024, 0);
    match outcome {
        SendOutcome::Arrive { delivery_id: _, at_ns } => {
            assert!(at_ns > 0, "relayed send should produce a positive arrival");
        }
        SendOutcome::Drop { reason } => panic!("unexpected drop: {reason:?}"),
    }
}

#[test]
fn relayed_arrival_is_later_than_a_direct_send_of_the_same_size() {
    // RELAY_SPEC §11 phase R1: "A direct send through a relay arrives
    // later than the same send over a direct edge of equal policy by
    // the relay's ingress + egress serialization time." We confirm
    // the direction; the exact numeric is the calibration suite's job.
    let scen_direct = scenario_from(
        r#"
        name = "direct"
        seed = 1
        duration_ns = 1_000_000_000
        [default_tick]
        period_ns = 1_000_000
        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000
        [[peers]]
        id = "a"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[peers]]
        id = "b"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[links]]
        from = "a"
        to = "b"
        [[links]]
        from = "b"
        to = "a"
        "#,
    );
    let scen_relayed = one_relay_three_hosts();
    let mut direct = Network::new(&scen_direct);
    let mut relayed = Network::new(&scen_relayed);
    let SendOutcome::Arrive { at_ns: direct_arrival, .. } = direct.send("a", "b", 8192, 0) else {
        panic!("direct send must arrive");
    };
    let SendOutcome::Arrive { at_ns: relayed_arrival, .. } =
        relayed.send("alpha", "bravo", 8192, 0)
    else {
        panic!("relayed send must arrive");
    };
    assert!(
        relayed_arrival > direct_arrival,
        "relayed arrival ({relayed_arrival}) should exceed direct arrival ({direct_arrival}) by the relay's serialization time"
    );
}

#[test]
fn head_of_line_is_observable_on_a_shared_egress() {
    // §4.7 "HOL is observable and bounded." Two messages from the
    // same sender to the same destination through the relay are
    // serialized on the shared egress: the second's arrival is at or
    // after the first's arrival plus the first's egress serialization.
    let scen = one_relay_three_hosts();
    let mut net = Network::new(&scen);
    // 100 KB messages so the egress serialization time dominates.
    let SendOutcome::Arrive { at_ns: first, .. } = net.send("alpha", "bravo", 100_000, 0) else {
        panic!("first send must arrive");
    };
    let SendOutcome::Arrive { at_ns: second, .. } = net.send("alpha", "bravo", 100_000, 0) else {
        panic!("second send must arrive");
    };
    // egress_capacity_bps_per_link in the spec is *bytes* per second
    // (SIM_SPEC §5.2 / RELAY_SPEC §4.2). 8_000_000 ⇒ 8 MB/s. 100 KB
    // takes 100_000 / 8_000_000 = 12.5 ms = 12_500_000 ns. The gap
    // between two shared-egress sends must be ≥ that floor.
    let expected_min_gap = 12_500_000u64;
    assert!(
        second >= first + expected_min_gap,
        "second arrival should follow first by ≥ egress serialization (first={first}, second={second}, expected_min_gap={expected_min_gap})"
    );
}

#[test]
fn ingress_and_egress_are_independent_across_destinations() {
    // §4.7 "Ingress and egress are independent." A message destined
    // for peer X does not delay a message destined for peer Y on the
    // egress side. Two simultaneous sends to *different*
    // destinations share only the ingress; the egress queues are
    // independent.
    let scen = one_relay_three_hosts();
    let mut net = Network::new(&scen);
    let SendOutcome::Arrive { at_ns: to_bravo, .. } = net.send("alpha", "bravo", 100_000, 0) else {
        panic!("first send must arrive");
    };
    let SendOutcome::Arrive { at_ns: to_charlie, .. } =
        net.send("alpha", "charlie", 100_000, 0)
    else {
        panic!("second send must arrive");
    };
    // Same source so the inbound leg serializes, and the ingress
    // serializes, but the egress queues are separate. The second's
    // egress should NOT have to wait for the first's egress
    // serialization. So the gap should be much smaller than the
    // shared-egress case in `head_of_line_is_observable_on_a_shared_egress`.
    let shared_egress_min_gap = 100_000_000u64;
    let gap = to_charlie - to_bravo;
    assert!(
        gap < shared_egress_min_gap,
        "different-destination gap ({gap}) should be smaller than the shared-egress gap ({shared_egress_min_gap})"
    );
}

#[test]
fn queue_overflow_drops_with_relay_queue_full() {
    // §4.7 "Queue overflow is exact." A send that would push
    // `enqueued_bytes` strictly above `queue_depth_bytes` drops with
    // `RelayQueueFull` and emits a `RelayDrop` record.
    let scen = scenario_from(
        r#"
        name = "tight_queue"
        seed = 1
        duration_ns = 1_000_000_000
        [default_tick]
        period_ns = 1_000_000
        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000
        [[relays]]
        id = "R"
        ingress_capacity_bps = 1_000_000_000
        egress_capacity_bps_per_link = 8_000  # 1 KB/s; egress can't drain
        queue_depth_bytes = 1024  # 1 KB total queue
        cold_start_penalty_ns = 0
        [[peers]]
        id = "a"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[peers]]
        id = "b"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[links]]
        from = "a"
        to = "b"
        via = "R"
        [[links]]
        from = "b"
        to = "a"
        via = "R"
        "#,
    );
    let mut net = Network::new(&scen);
    // Three 512 B messages back to back: first fits, second fills the
    // queue (1024 total), third overflows.
    let SendOutcome::Arrive { .. } = net.send("a", "b", 512, 0) else {
        panic!("first 512B should fit");
    };
    let SendOutcome::Arrive { .. } = net.send("a", "b", 512, 0) else {
        panic!("second 512B should fit (queue = 1024 total)");
    };
    let outcome = net.send("a", "b", 1, 0);
    assert!(
        matches!(outcome, SendOutcome::Drop { reason: DropReason::RelayQueueFull }),
        "third send must overflow with RelayQueueFull, got {outcome:?}"
    );
    let notes = collect_notifications(&mut net);
    let drops: Vec<_> = notes
        .iter()
        .filter_map(|n| {
            if let NetworkNotification::RelayDrop {
                relay,
                reason: RelayDropReason::QueueFull,
                ..
            } = n
            {
                Some(relay.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        drops,
        vec!["R".to_string()],
        "exactly one RelayDrop{{QueueFull}} notification expected"
    );
}

#[test]
fn cold_start_penalty_is_paid_exactly_once_per_boot() {
    // §4.7 "Cold-start penalty is paid once per boot." After a
    // RelayBoot, the first forwarded message includes the penalty;
    // the second does not.
    let scen = scenario_from(
        r#"
        name = "boot_penalty"
        seed = 1
        duration_ns = 1_000_000_000
        [default_tick]
        period_ns = 1_000_000
        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000
        [[relays]]
        id = "R"
        ingress_capacity_bps = 1_000_000_000
        egress_capacity_bps_per_link = 1_000_000_000
        queue_depth_bytes = 100_000
        cold_start_penalty_ns = 50_000_000  # 50ms
        [[peers]]
        id = "a"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[peers]]
        id = "b"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[links]]
        from = "a"
        to = "b"
        via = "R"
        [[links]]
        from = "b"
        to = "a"
        via = "R"
        [[mutations]]
        at_ns = 100_000_000
        kind = "relay_kill"
        relay = "R"
        [[mutations]]
        at_ns = 200_000_000
        kind = "relay_boot"
        relay = "R"
        "#,
    );
    use simulation::scenario::{Mutation, MutationKind};
    let mut net = Network::new(&scen);
    // Boot the relay at t=200ms (simulating engine dispatch).
    net.apply_mutation(
        &Mutation {
            at_ns: 200_000_000,
            kind: MutationKind::RelayBoot {
                relay: "R".into(),
            },
        },
        200_000_000,
    );
    let SendOutcome::Arrive { at_ns: first, .. } = net.send("a", "b", 1024, 300_000_000) else {
        panic!("first post-boot send must arrive");
    };
    let SendOutcome::Arrive { at_ns: second, .. } = net.send("a", "b", 1024, 400_000_000) else {
        panic!("second post-boot send must arrive");
    };
    // The first should include the 50ms penalty; the second should
    // not, so first - sent ≥ 50ms and second - sent < 50ms.
    let first_relative = first - 300_000_000;
    let second_relative = second - 400_000_000;
    assert!(
        first_relative >= 50_000_000,
        "first post-boot send should include the 50ms cold-start penalty (relative={first_relative}ns)"
    );
    assert!(
        second_relative < 50_000_000,
        "second post-boot send should NOT include the 50ms cold-start penalty (relative={second_relative}ns)"
    );
}

#[test]
fn relay_kill_drops_subsequent_sends_with_relay_down() {
    // §4.7 "Mutation invalidation is exact." After a RelayKill, sends
    // through the relay drop with RelayDown.
    let scen = scenario_from(
        r#"
        name = "kill_then_send"
        seed = 1
        duration_ns = 1_000_000_000
        [default_tick]
        period_ns = 1_000_000
        [default_link]
        latency_ns = 1_000_000
        jitter_stddev_ns = 0
        loss_prob_ppm = 0
        reorder_prob_ppm = 0
        bandwidth_bps = 1_000_000_000
        cold_dial_penalty_ns = 0
        cache_warm_after_ns = 1_000_000_000
        cache_invalidate_after_idle_ns = 10_000_000_000
        [[relays]]
        id = "R"
        ingress_capacity_bps = 1_000_000_000
        egress_capacity_bps_per_link = 1_000_000_000
        queue_depth_bytes = 100_000
        cold_start_penalty_ns = 0
        [[peers]]
        id = "a"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[peers]]
        id = "b"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["a", "b"] }
        [[links]]
        from = "a"
        to = "b"
        via = "R"
        [[links]]
        from = "b"
        to = "a"
        via = "R"
        "#,
    );
    use simulation::scenario::{Mutation, MutationKind};
    let mut net = Network::new(&scen);
    net.apply_mutation(
        &Mutation {
            at_ns: 50_000_000,
            kind: MutationKind::RelayKill {
                relay: "R".into(),
            },
        },
        50_000_000,
    );
    let outcome = net.send("a", "b", 100, 60_000_000);
    assert!(
        matches!(outcome, SendOutcome::Drop { reason: DropReason::RelayDown }),
        "post-kill send must drop with RelayDown, got {outcome:?}"
    );
}

#[test]
fn relay_capacity_change_affects_only_future_sends() {
    // §4.7 "Mutation invalidation is exact." A RelayCapacityChange
    // invalidates no deliveries; it affects only sends that begin
    // after its time. We assert that pre-change in-flight count is
    // unchanged across the mutation.
    let scen = one_relay_three_hosts();
    use simulation::scenario::{Mutation, MutationKind};
    let mut net = Network::new(&scen);
    // Queue up a delivery, then apply the policy change.
    let _ = net.send("alpha", "bravo", 1024, 0);
    let before = net.relay_in_flight_count("R");
    let invalidated = net.apply_mutation(
        &Mutation {
            at_ns: 100_000,
            kind: MutationKind::RelayCapacityChange {
                relay: "R".into(),
                ingress_capacity_bps: Some(1),
                egress_capacity_bps_per_link: Some(1),
                queue_depth_bytes: Some(1),
            },
        },
        100_000,
    );
    let after = net.relay_in_flight_count("R");
    assert!(invalidated.is_empty(), "RelayCapacityChange invalidates no deliveries");
    assert_eq!(
        before, after,
        "in-flight relay state must not change across RelayCapacityChange"
    );
}

#[test]
fn relay_kill_invalidates_in_flight_through_that_relay() {
    // §4.7 "Mutation invalidation is exact." A RelayKill returns
    // exactly the deliveries in-flight through the relay at the
    // mutation's virtual time and no others.
    let scen = one_relay_three_hosts();
    use simulation::scenario::{Mutation, MutationKind};
    let mut net = Network::new(&scen);
    let SendOutcome::Arrive { delivery_id, .. } = net.send("alpha", "bravo", 1024, 0) else {
        panic!("send must arrive");
    };
    let invalidated = net.apply_mutation(
        &Mutation {
            at_ns: 1,
            kind: MutationKind::RelayKill {
                relay: "R".into(),
            },
        },
        1,
    );
    assert_eq!(invalidated.len(), 1, "exactly one in-flight delivery");
    assert_eq!(invalidated[0].delivery_id, delivery_id);
    // Relay's in-flight bookkeeping is drained.
    assert_eq!(net.relay_in_flight_count("R"), 0);
}

#[test]
fn determinism_relayed_send_sequences_are_identical_across_runs() {
    // §4.7 "Determinism." Same topology, same seed, same query
    // sequence ⇒ identical composed SendOutcome sequence.
    let scen = one_relay_three_hosts();
    let run = |s: &simulation::scenario::Scenario| -> Vec<u64> {
        let mut net = Network::new(s);
        let mut arrivals = Vec::new();
        for (i, (from, to)) in [
            ("alpha", "bravo"),
            ("bravo", "charlie"),
            ("charlie", "alpha"),
            ("alpha", "charlie"),
        ]
        .iter()
        .enumerate()
        {
            let outcome = net.send(from, to, 4096, (i as u64) * 1_000);
            if let SendOutcome::Arrive { at_ns, .. } = outcome {
                arrivals.push(at_ns);
            } else {
                panic!("unexpected drop in determinism test");
            }
        }
        arrivals
    };
    let a = run(&scen);
    let b = run(&scen);
    assert_eq!(a, b, "two runs of the same scenario must produce identical arrivals");
}

// Smoke check: this constant is referenced to suppress the
// otherwise-unused `include_str!` import. It's a sanity guard that
// the parity scenario lives where we expect.
#[test]
fn parity_scenario_text_includes_default_link_header() {
    assert!(ALL_NOTIFICATIONS.contains("default_link"));
}
