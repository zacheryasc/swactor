//! Tier-2 iroh-internal scrape, end to end.
//!
//! Boots two iroh drivers with `RelayMode::Disabled`, installs a full
//! diagnostics aggregator wired to an [`InMemorySink`], lets them join
//! and pump until each side sees the other Alive, then asserts that
//! the latest snapshot contains the tier-2 fields the post-processor
//! depends on:
//!
//! - per-remote-peer entries in `body.iroh.peers` with classified
//!   direct/relay addresses,
//! - an `iroh_api_missing` Custom event listing fields the linked iroh
//!   version does not expose,
//! - at least one `iroh-metrics` sample,
//! - a populated `body.iroh` block on every node.
//!
//! Behavioural — counts and exact addresses are not asserted, since
//! iroh dynamics can vary across runs. The shape of the data is what
//! matters for the post-processor.

#![cfg(feature = "iroh")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::iroh::*;
use distribution::diagnostics::{
    Aggregator, Event, Identity, InMemorySink, Role, SnapshotTrigger,
};
use distribution::diagnostics::iroh_introspect::IntrospectConfig;
use distribution::iroh_driver::IrohDriver;
use iroh::PublicKey;

fn install_full_diagnostics(
    driver: &mut IrohDriver,
    run_id: &str,
) -> (Arc<InMemorySink>, Arc<Aggregator<Arc<InMemorySink>>>) {
    let sink = Arc::new(InMemorySink::new());
    let identity = Identity::new(driver.node_id(), Role::stage(), run_id);
    let aggregator = Arc::new(Aggregator::new(identity, sink.clone()));
    driver.install_diagnostics_with_config(
        aggregator.clone(),
        IntrospectConfig {
            // Fast cadence — test asserts on cached state, not on the
            // raw poll frequency, but a tight tick keeps the
            // background task warm.
            scrape_interval: Duration::from_millis(50),
        },
    );
    (sink, aggregator)
}

#[test]
fn two_node_cluster_produces_tier2_iroh_snapshot_block() {
    let mut a = make_driver();
    let mut b = make_driver();

    let (sink_a, agg_a) = install_full_diagnostics(&mut a, "run-tier2");
    let (sink_b, agg_b) = install_full_diagnostics(&mut b, "run-tier2");

    let b_addr = b.endpoint_addr();
    let a_id = a.node_id();
    let b_id = b.node_id();
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

    // Force a synchronous refresh on each introspector so the assertions
    // below don't race against the polling task. Without this the test
    // could pass or fail based on whether the background scrape happened
    // to fire between the join and the snapshot.
    a.force_iroh_introspect_refresh();
    b.force_iroh_introspect_refresh();

    let snap_a = agg_a.snapshot(SnapshotTrigger::OnDemand);
    let snap_b = agg_b.snapshot(SnapshotTrigger::OnDemand);

    a.shutdown();
    b.shutdown();

    // Each side has a populated tier-2 iroh block.
    let iroh_a = snap_a
        .body
        .iroh
        .as_ref()
        .expect("A's snapshot must include the tier-2 iroh block");
    let iroh_b = snap_b
        .body
        .iroh
        .as_ref()
        .expect("B's snapshot must include the tier-2 iroh block");

    // The introspector lists the iroh API gaps so the post-processor
    // can render "absent" vs "zero" honestly. The gap list is computed
    // from observed peer slots — for the linked iroh version, derived
    // conn_type and unpopulated latency_ms should still show up.
    let gaps_present = !iroh_a.api_gaps.is_empty() && !iroh_b.api_gaps.is_empty();
    assert!(
        gaps_present,
        "tier-2 iroh state should list api_gaps for fields the linked iroh version does not expose",
    );
    assert!(
        iroh_a
            .api_gaps
            .iter()
            .any(|g| g.contains("conn_type") || g.contains("latency_ms")),
        "expected api_gaps to call out conn_type / latency_ms; got {:?}",
        iroh_a.api_gaps,
    );

    // Each node lists the other in its peer set with at least one
    // classified address. Either direct or relay is fine — we just
    // need the classification machinery to be exercised.
    let peer_in = |iroh: &distribution::diagnostics::Tier2IrohState, peer_hex: &str| -> bool {
        iroh.peers.iter().any(|p| {
            p.peer_node_id_hex == peer_hex
                && (!p.direct_addresses.is_empty() || !p.relay_urls.is_empty())
        })
    };
    let b_hex = node_hex(&b_id);
    let a_hex = node_hex(&a_id);
    assert!(
        peer_in(iroh_a, &b_hex),
        "A's tier-2 peers must include B with at least one classified addr; got {:#?}",
        iroh_a.peers,
    );
    assert!(
        peer_in(iroh_b, &a_hex),
        "B's tier-2 peers must include A with at least one classified addr; got {:#?}",
        iroh_b.peers,
    );

    // The metrics scrape produces *some* samples — iroh always has at
    // least the socket counters wired up.
    assert!(
        !iroh_a.metrics.is_empty(),
        "expected at least one iroh-metrics sample on A; got {:?}",
        iroh_a.metrics,
    );
    assert!(
        !iroh_b.metrics.is_empty(),
        "expected at least one iroh-metrics sample on B; got {:?}",
        iroh_b.metrics,
    );

    // The one-time `iroh_api_missing` Custom event fires on each side
    // at introspector start.
    let api_missing = |sink: &InMemorySink| {
        sink.records().iter().any(|r| {
            matches!(
                &r.event,
                Event::Custom { kind, .. } if kind == "iroh_api_missing"
            )
        })
    };
    assert!(
        api_missing(&sink_a),
        "expected an iroh_api_missing Custom event on A",
    );
    assert!(
        api_missing(&sink_b),
        "expected an iroh_api_missing Custom event on B",
    );
}

#[test]
fn introspector_emits_iroh_api_missing_event_on_startup() {
    // Tighter scenario — even a single driver with no peers should
    // emit the api-missing event at introspector start, so the bundle
    // reader is never left guessing whether a missing field means
    // "iroh didn't have it" or "we didn't bother to look."
    let mut a = make_driver();
    let (sink, _agg) = install_full_diagnostics(&mut a, "run-tier2-bare");

    // Give the background tasks a moment to spawn.
    std::thread::sleep(Duration::from_millis(50));
    a.shutdown();

    let saw_event = sink.records().iter().any(|r| {
        matches!(
            &r.event,
            Event::Custom { kind, .. } if kind == "iroh_api_missing"
        )
    });
    assert!(saw_event, "introspector should emit iroh_api_missing on startup");
}

fn node_hex(id: &distribution::types::NodeId) -> String {
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
