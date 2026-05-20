//! Verifies that the diagnostics emitter wired through `IrohDriver` and
//! `SwimNode` actually produces the structured events the post-processor
//! relies on (`DIAGNOSTICS_PLAN.md` T1.3).
//!
//! The test forms a two-node iroh cluster with `RelayMode::Disabled`,
//! installs an [`InMemorySink`] on each side via an [`Aggregator`], and
//! pumps until the nodes see each other alive. The recorded event
//! stream is then checked against the minimum vocabulary S4 promises:
//! `SwimTransition`, `DialStarted`, `DialOutcome`, `MessageSent`,
//! `MessageReceived`, plus the `boot` record emitted at aggregator
//! construction.
//!
//! Behavioural assertions only — no specific counts or ordering beyond
//! "monotonic_seq is strictly increasing per node", so the test
//! survives small changes in dial-retry counts or probe cadence.

#![cfg(feature = "iroh")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::iroh::*;
use distribution::diagnostics::{
    Aggregator, DialOutcome as DiagDialOutcome, Event, Identity, InMemorySink, PeerState, Role,
};
use distribution::iroh_driver::IrohDriver;
use iroh::PublicKey;

/// Wire an `InMemorySink`-backed aggregator into `driver`. The sink is
/// returned so the caller can inspect the event stream afterwards.
fn wire_inmemory_diagnostics(driver: &mut IrohDriver, run_id: &str) -> Arc<InMemorySink> {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(driver.node_id(), Role::stage(), run_id);
    let aggregator = Arc::new(Aggregator::new(identity, sink.clone()));
    driver.set_diagnostics(aggregator);
    sink
}

#[test]
fn two_node_join_produces_dial_message_and_swim_transition_events() {
    let mut a = make_driver();
    let mut b = make_driver();

    let sink_a = wire_inmemory_diagnostics(&mut a, "run-emission");
    let sink_b = wire_inmemory_diagnostics(&mut b, "run-emission");

    // A joins B. From A's perspective this triggers dial + message-send;
    // B sees an incoming connection + message + a membership transition
    // into Alive.
    let b_addr = b.endpoint_addr();
    a.join(&[b_addr]);

    let converged = pump_until_pair(
        &mut a,
        &mut b,
        Duration::from_secs(5),
        |a, b| {
            let a_key = PublicKey::from_bytes(&a.node_id().0).unwrap();
            let b_key = PublicKey::from_bytes(&b.node_id().0).unwrap();
            sees_alive(a, &b_key) && sees_alive(b, &a_key)
        },
    );
    assert!(converged, "nodes did not converge within 5s");

    a.shutdown();
    b.shutdown();

    // ── A's side: dialed B, sent the join, observed B as Alive. ─────
    let records_a = sink_a.records();
    assert!(
        records_a
            .iter()
            .any(|r| matches!(r.event, Event::DialStarted { .. })),
        "A should have emitted at least one DialStarted",
    );
    assert!(
        records_a.iter().any(|r| matches!(
            r.event,
            Event::DialOutcome {
                outcome: DiagDialOutcome::Success,
                ..
            }
        )),
        "A should have emitted a successful DialOutcome",
    );
    assert!(
        records_a
            .iter()
            .any(|r| matches!(r.event, Event::MessageSent { .. })),
        "A should have emitted at least one MessageSent",
    );
    assert!(
        records_a.iter().any(|r| matches!(
            &r.event,
            Event::SwimTransition {
                to: PeerState::Alive,
                ..
            }
        )),
        "A should have observed B transitioning to Alive",
    );

    // ── B's side: accepted A's connection, processed the join,
    //    observed A as Alive. ────────────────────────────────────────
    let records_b = sink_b.records();
    assert!(
        records_b
            .iter()
            .any(|r| matches!(r.event, Event::MessageReceived { .. })),
        "B should have emitted at least one MessageReceived",
    );
    assert!(
        records_b.iter().any(|r| matches!(
            &r.event,
            Event::SwimTransition {
                to: PeerState::Alive,
                ..
            }
        )),
        "B should have observed A transitioning to Alive",
    );

    // ── Boot record sent at aggregator construction. ────────────────
    assert_eq!(sink_a.boots().len(), 1, "exactly one boot record on A");
    assert_eq!(sink_b.boots().len(), 1, "exactly one boot record on B");

    // ── monotonic_seq strictly increases per node. ──────────────────
    let mut prev = 0u64;
    for rec in &records_a {
        assert!(
            rec.monotonic_seq > prev,
            "A's monotonic_seq must strictly increase (saw {} after {})",
            rec.monotonic_seq,
            prev,
        );
        prev = rec.monotonic_seq;
    }
}

#[test]
fn dial_to_unreachable_peer_emits_non_success_dial_outcome() {
    use iroh::{EndpointAddr, SecretKey};

    let mut a = make_driver();
    let sink = wire_inmemory_diagnostics(&mut a, "run-unreachable");

    // Construct a well-formed but unreachable peer addr: arbitrary
    // (but fixed) key, no direct addresses, no relay. With
    // `RelayMode::Disabled` iroh has no way to reach this peer and
    // the dial must fail.
    let unreachable_sk = SecretKey::from_bytes(&[0xab; 32]);
    let unreachable_addr = EndpointAddr::new(unreachable_sk.public());
    a.join(&[unreachable_addr]);

    // Pump until the spawned join task records at least one dial
    // attempt, or give up after a few seconds.
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        pump_one(&mut a);
        if sink
            .records()
            .iter()
            .any(|r| matches!(r.event, Event::DialOutcome { .. }))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    a.shutdown();

    let records = sink.records();
    assert!(
        records
            .iter()
            .any(|r| matches!(r.event, Event::DialStarted { .. })),
        "should have emitted at least one DialStarted",
    );
    let outcomes: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.event {
            Event::DialOutcome { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !outcomes.is_empty(),
        "should have emitted at least one DialOutcome",
    );
    assert!(
        outcomes
            .iter()
            .all(|o| !matches!(o, DiagDialOutcome::Success)),
        "no dial should succeed to an unreachable peer, got: {outcomes:?}",
    );
}
