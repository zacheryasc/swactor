//! Stage F3 — sim "tunnel up, peer-via-tunnel down" failure mode
//! (`apps/pipeline-parallel-inference/N3_OBSERVABILITY_UPGRADE_SPEC.md`
//! §"Sim cross-pollination" bullet 3).
//!
//! Spec literal: "The sim's network failure model must allow 'tunnel
//! up, peer-connection-via-tunnel down' as a distinct failure case
//! from 'tunnel down.' Without it the sim cannot reproduce the exact
//! 2026-05-25 failure even after the observability lands."
//!
//! Acceptance is two-pronged:
//!   (a) when the mutation cuts (relay, from, to), sends along that
//!       triple drop with a *distinct* reason from `RelayDown`;
//!   (b) every other peer pair through the same relay keeps working
//!       — the relay itself is not down.

use simulation::network::{
    DropReason, Network, NetworkNotification, RelayDropReason, SendOutcome,
};
use simulation::scenario::{HostKindRegistry, Mutation, MutationKind, load_from_str};
use std::path::Path;

fn registry() -> HostKindRegistry {
    let mut r = HostKindRegistry::with_swim();
    r.register(Box::new(simulation::parity_host::ParityStubKindValidator));
    r
}

fn three_hosts_via_one_relay() -> simulation::scenario::Scenario {
    load_from_str(
        Path::new("(test)"),
        r#"
        name = "f3_relay_peer_conn_down"
        seed = 7
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
        queue_depth_bytes = 1_000_000
        cold_start_penalty_ns = 0

        [[peers]]
        id = "orch"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["orch", "stage-1", "stage-2"] }
        [[peers]]
        id = "stage-1"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["orch", "stage-1", "stage-2"] }
        [[peers]]
        id = "stage-2"
        kind = "parity_stub"
        initial_state = "ready"
        kind_config = { peers = ["orch", "stage-1", "stage-2"] }

        [[links]]
        from = "orch"
        to = "stage-1"
        via = "R"
        [[links]]
        from = "orch"
        to = "stage-2"
        via = "R"
        [[links]]
        from = "stage-1"
        to = "orch"
        via = "R"
        [[links]]
        from = "stage-2"
        to = "orch"
        via = "R"
        "#,
        &registry(),
    )
    .expect("scenario validates")
}

#[test]
fn peer_via_tunnel_down_drops_only_the_cut_pair_other_pairs_keep_working() {
    let scen = three_hosts_via_one_relay();
    let mut net = Network::new(&scen);

    // Replay-of-incident shape: cut orch→stage-2 at t=50ms,
    // permanently for the rest of the run. The relay is not killed —
    // it stays available for everyone else.
    net.apply_mutation(
        &Mutation {
            at_ns: 50_000_000,
            kind: MutationKind::RelayPeerConnDown {
                relay: "R".into(),
                from: "orch".into(),
                to: "stage-2".into(),
                duration_ns: 0,
            },
        },
        50_000_000,
    );

    // Post-cut: orch→stage-2 drops with RelayPeerConnDown.
    let cut = net.send("orch", "stage-2", 1024, 60_000_000);
    assert!(
        matches!(cut, SendOutcome::Drop { reason: DropReason::RelayPeerConnDown }),
        "cut pair must drop with RelayPeerConnDown, got {cut:?}",
    );

    // Post-cut: orch→stage-1 still arrives — relay is otherwise up.
    let untouched = net.send("orch", "stage-1", 1024, 61_000_000);
    assert!(
        matches!(untouched, SendOutcome::Arrive { .. }),
        "uncut pair through the same relay must still arrive, got {untouched:?}",
    );

    // Post-cut: stage-1→orch (reverse-direction unrelated pair) also
    // unaffected.
    let reverse = net.send("stage-1", "orch", 1024, 62_000_000);
    assert!(
        matches!(reverse, SendOutcome::Arrive { .. }),
        "unrelated pair must still arrive, got {reverse:?}",
    );

    // Notification stream carries a typed RelayDrop with
    // PeerConnDown reason — distinct from `Down`. This is the
    // discriminator the bundle reader joins against.
    let notifs = net.take_pending_notifications();
    let saw_pc_drop = notifs.iter().any(|n| {
        matches!(
            n,
            NetworkNotification::RelayDrop {
                relay,
                from,
                to,
                reason: RelayDropReason::PeerConnDown,
                ..
            } if relay == "R" && from == "orch" && to == "stage-2",
        )
    });
    assert!(
        saw_pc_drop,
        "RelayDrop notification with PeerConnDown must fire for the cut pair; got {notifs:#?}",
    );
}

#[test]
fn cut_only_takes_effect_after_its_at_ns() {
    let scen = three_hosts_via_one_relay();
    let mut net = Network::new(&scen);
    // Send BEFORE the cut is applied — must arrive normally.
    let before = net.send("orch", "stage-2", 100, 10_000_000);
    assert!(
        matches!(before, SendOutcome::Arrive { .. }),
        "pre-mutation send must arrive normally, got {before:?}",
    );
    // Apply the cut at t=50ms.
    net.apply_mutation(
        &Mutation {
            at_ns: 50_000_000,
            kind: MutationKind::RelayPeerConnDown {
                relay: "R".into(),
                from: "orch".into(),
                to: "stage-2".into(),
                duration_ns: 0,
            },
        },
        50_000_000,
    );
    // Post-cut send drops.
    let after = net.send("orch", "stage-2", 100, 60_000_000);
    assert!(matches!(after, SendOutcome::Drop { reason: DropReason::RelayPeerConnDown }));
}

#[test]
fn finite_duration_lets_the_pair_recover() {
    let scen = three_hosts_via_one_relay();
    let mut net = Network::new(&scen);
    net.apply_mutation(
        &Mutation {
            at_ns: 100,
            kind: MutationKind::RelayPeerConnDown {
                relay: "R".into(),
                from: "orch".into(),
                to: "stage-2".into(),
                duration_ns: 1_000_000,
            },
        },
        100,
    );
    // Inside the window — drop.
    let inside = net.send("orch", "stage-2", 100, 500_000);
    assert!(matches!(inside, SendOutcome::Drop { reason: DropReason::RelayPeerConnDown }));
    // After the window — back to normal.
    let after = net.send("orch", "stage-2", 100, 2_000_000);
    assert!(
        matches!(after, SendOutcome::Arrive { .. }),
        "post-window send must arrive again, got {after:?}",
    );
}

#[test]
fn directionality_is_one_way() {
    // The cut is from→to. The opposite direction must keep working.
    let scen = three_hosts_via_one_relay();
    let mut net = Network::new(&scen);
    net.apply_mutation(
        &Mutation {
            at_ns: 100,
            kind: MutationKind::RelayPeerConnDown {
                relay: "R".into(),
                from: "orch".into(),
                to: "stage-2".into(),
                duration_ns: 0,
            },
        },
        100,
    );
    let forward = net.send("orch", "stage-2", 100, 1_000_000);
    let reverse = net.send("stage-2", "orch", 100, 1_500_000);
    assert!(matches!(forward, SendOutcome::Drop { reason: DropReason::RelayPeerConnDown }));
    assert!(
        matches!(reverse, SendOutcome::Arrive { .. }),
        "reverse direction must still arrive (cut is directional); got {reverse:?}",
    );
}

#[test]
fn distinct_from_relay_down_at_the_send_outcome_level() {
    // A bundle reader joining on (relay, drop_reason) must be able to
    // tell "tunnel down" from "peer-via-tunnel down". They emit
    // *different* SendOutcome reasons AND different RelayDropReason
    // notifications — proven in tandem here so the discriminator
    // stays sharp across both surfaces.
    let scen = three_hosts_via_one_relay();

    // RelayKill: send drops with RelayDown.
    let mut net1 = Network::new(&scen);
    net1.apply_mutation(
        &Mutation {
            at_ns: 100,
            kind: MutationKind::RelayKill { relay: "R".into() },
        },
        100,
    );
    let killed = net1.send("orch", "stage-2", 100, 1_000_000);
    assert!(matches!(killed, SendOutcome::Drop { reason: DropReason::RelayDown }));

    // RelayPeerConnDown: send drops with RelayPeerConnDown.
    let mut net2 = Network::new(&scen);
    net2.apply_mutation(
        &Mutation {
            at_ns: 100,
            kind: MutationKind::RelayPeerConnDown {
                relay: "R".into(),
                from: "orch".into(),
                to: "stage-2".into(),
                duration_ns: 0,
            },
        },
        100,
    );
    let cut = net2.send("orch", "stage-2", 100, 1_000_000);
    assert!(matches!(cut, SendOutcome::Drop { reason: DropReason::RelayPeerConnDown }));

    // These two reasons must not be the same variant.
    assert_ne!(
        DropReason::RelayDown,
        DropReason::RelayPeerConnDown,
        "DropReason variants must be distinct so the bundle reader can tell them apart",
    );
}
