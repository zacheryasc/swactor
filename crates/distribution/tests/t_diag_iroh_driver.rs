//! Tier-2 connection-cache + NodeMap delta tracking (S7).
//!
//! Verifies two things the iroh driver promises after S7:
//!
//! 1. **Every peer-addr handoff to iroh is surfaced.** Each time the
//!    driver feeds iroh a relay URL (or a direct address) for a peer,
//!    a [`Event::NodeMapUpdate`] event is emitted with the source the
//!    addr came from (`join_seed`, `swim_metadata`,
//!    `explicit_relay_cache`, `home_relay_fallback`).
//!
//! 2. **Tier-2 snapshots carry the per-peer cache lifecycle.** Once
//!    a connection has been opened and used, the snapshot's
//!    `body.iroh.connection_cache` block contains an entry for the
//!    peer with `generation >= 1`, `created_at_ms` populated, and
//!    `last_successful_send_at_ms` populated.
//!
//! These are behavioural checks: the test asserts on the shape of
//! the event stream and snapshot, not on exact timestamps or
//! generation counts (which can wobble with iroh's dynamics).

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
use distribution::types::NodeId;
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
            scrape_interval: Duration::from_millis(50),
        },
    );
    (sink, aggregator)
}

fn node_hex(id: &NodeId) -> String {
    let mut s = String::with_capacity(64);
    for b in id.0 {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[test]
fn join_emits_node_map_update_for_seed_addr() {
    let mut a = make_driver();
    let mut b = make_driver();
    let (sink_a, _agg_a) = install_full_diagnostics(&mut a, "run-s7-join");
    let _ = install_full_diagnostics(&mut b, "run-s7-join");

    let b_id = b.node_id();
    let b_addr = b.endpoint_addr();
    a.join(&[b_addr]);

    // No pumping required — the NodeMapUpdate emit happens inside
    // join() before any async work is kicked off.
    let saw = sink_a.records().iter().any(|r| {
        matches!(
            &r.event,
            Event::NodeMapUpdate { peer, from_source, accepted }
                if peer == &b_id
                    && (from_source == "join_seed"
                        || from_source == "join_seed_direct")
                    && *accepted
        )
    });
    a.shutdown();
    b.shutdown();
    assert!(
        saw,
        "join() should emit NodeMapUpdate with from_source in \
         {{join_seed, join_seed_direct}} for the seed peer",
    );
}

#[test]
fn join_with_bare_seed_emits_no_node_map_update() {
    let mut a = make_driver();
    let (sink_a, _agg_a) = install_full_diagnostics(&mut a, "run-s7-bare");

    // Build a peer addr with neither relay nor direct addresses —
    // just the public key. iroh has nothing new to learn here, so
    // we shouldn't emit a NodeMapUpdate either.
    let bare_key = iroh::SecretKey::from_bytes(&[0xcd; 32]).public();
    let bare_addr = iroh::EndpointAddr::new(bare_key);
    let bare_id = NodeId(*bare_key.as_bytes());

    a.join(&[bare_addr]);

    let saw = sink_a
        .records()
        .iter()
        .any(|r| matches!(&r.event, Event::NodeMapUpdate { peer, .. } if peer == &bare_id));
    a.shutdown();
    assert!(
        !saw,
        "bare-key join() should NOT emit NodeMapUpdate — iroh has \
         no new address to learn",
    );
}

#[test]
fn cache_aggregate_appears_in_tier2_snapshot_after_successful_send() {
    let mut a = make_driver();
    let mut b = make_driver();
    let (sink_a, agg_a) = install_full_diagnostics(&mut a, "run-s7-cache");
    let _ = install_full_diagnostics(&mut b, "run-s7-cache");

    let a_id = a.node_id();
    let b_id = b.node_id();
    let b_hex = node_hex(&b_id);
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

    // Pump a few more rounds so SWIM probes (which exercise
    // send_message → note_send_success on the cached connection)
    // run beyond the initial join handshake.
    for _ in 0..10 {
        pump_one(&mut a);
        pump_one(&mut b);
        std::thread::sleep(Duration::from_millis(20));
    }

    a.force_iroh_introspect_refresh();
    let snap = agg_a.snapshot(SnapshotTrigger::OnDemand);

    a.shutdown();
    b.shutdown();

    let iroh = snap
        .body
        .iroh
        .as_ref()
        .expect("snapshot must include tier-2 iroh block");
    let entry = iroh
        .connection_cache
        .iter()
        .find(|e| e.peer_node_id_hex == b_hex)
        .unwrap_or_else(|| {
            panic!(
                "tier-2 snapshot connection_cache should include B \
                 (= {}); got entries: {:#?}",
                b_hex, iroh.connection_cache,
            );
        });
    assert!(
        entry.generation >= 1,
        "expected cache generation >= 1 after a successful dial; \
         got {} (entry: {:#?})",
        entry.generation,
        entry,
    );
    assert!(
        entry.created_at_ms.is_some(),
        "expected created_at_ms to be populated after a successful \
         dial; got {:#?}",
        entry,
    );
    assert!(
        entry.last_successful_send_at_ms.is_some(),
        "expected last_successful_send_at_ms to be populated after a \
         message round-trip; got {:#?}",
        entry,
    );

    // Sanity: a's own id should not appear (the cache only tracks
    // outbound peers).
    let a_hex = node_hex(&a_id);
    assert!(
        !iroh.connection_cache.iter().any(|e| e.peer_node_id_hex == a_hex),
        "connection_cache should not list the local node itself",
    );

    // Confirm that the NodeMapUpdate emit fired on the get_or_connect
    // dial path too — once SWIM probes start, the driver feeds iroh
    // a relay URL resolved from one of its caches.
    let saw_dial_path_nmu = sink_a.records().iter().any(|r| {
        matches!(
            &r.event,
            Event::NodeMapUpdate { peer, from_source, accepted }
                if peer == &b_id
                    && (from_source == "explicit_relay_cache"
                        || from_source == "swim_metadata"
                        || from_source == "home_relay_fallback")
                    && *accepted
        )
    });
    // Without a relay configured (RelayMode::Disabled) the home-relay
    // path is not available; if all caches are empty we'd skip the
    // emit entirely. Accept either: a NodeMapUpdate from the dial
    // path *or* the join-seed one we already asserted on.
    let saw_join_seed_nmu = sink_a.records().iter().any(|r| {
        matches!(
            &r.event,
            Event::NodeMapUpdate { peer, from_source, .. }
                if peer == &b_id
                    && (from_source == "join_seed"
                        || from_source == "join_seed_direct")
        )
    });
    assert!(
        saw_dial_path_nmu || saw_join_seed_nmu,
        "expected at least one NodeMapUpdate event for B (either from \
         join() or from the dial path); got {:#?}",
        sink_a
            .records()
            .iter()
            .filter_map(|r| match &r.event {
                Event::NodeMapUpdate { peer, from_source, accepted } =>
                    Some((peer, from_source.clone(), *accepted)),
                _ => None,
            })
            .collect::<Vec<_>>(),
    );
}

#[test]
fn generation_in_events_matches_snapshot_aggregate() {
    let mut a = make_driver();
    let mut b = make_driver();
    let (sink_a, agg_a) = install_full_diagnostics(&mut a, "run-s7-gen");
    let _ = install_full_diagnostics(&mut b, "run-s7-gen");

    let b_id = b.node_id();
    let b_hex = node_hex(&b_id);
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

    // A few more rounds so we get a ConnectionCacheHit event recorded
    // against the established connection.
    for _ in 0..10 {
        pump_one(&mut a);
        pump_one(&mut b);
        std::thread::sleep(Duration::from_millis(20));
    }

    a.force_iroh_introspect_refresh();
    let snap = agg_a.snapshot(SnapshotTrigger::OnDemand);

    a.shutdown();
    b.shutdown();

    // Find the highest generation reported in any per-touch event
    // for B and compare against the snapshot aggregate. They should
    // agree — the post-processor depends on this invariant when
    // stitching events to snapshots.
    let max_event_gen = sink_a
        .records()
        .iter()
        .filter_map(|r| match &r.event {
            Event::ConnectionCacheHit { peer, generation }
            | Event::ConnectionCacheInvalidated { peer, generation, .. }
                if peer == &b_id =>
            {
                Some(*generation)
            }
            _ => None,
        })
        .max()
        .expect("expected at least one cache-touch event for B");

    let iroh = snap.body.iroh.as_ref().expect("tier-2 iroh block");
    let entry = iroh
        .connection_cache
        .iter()
        .find(|e| e.peer_node_id_hex == b_hex)
        .expect("snapshot connection_cache must include B");

    assert!(
        entry.generation >= max_event_gen,
        "snapshot generation ({}) should be >= the highest generation \
         seen on the event stream ({})",
        entry.generation,
        max_event_gen,
    );
}
