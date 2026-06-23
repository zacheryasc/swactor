//! Behavioral tests for the cluster registry CRDT.
//!
//! These pin the `ClusterRegistry` LWW-Register merge semantics and tombstone
//! GC directly on the pure type. The actor-path registry convergence (gossip
//! propagation, tombstone dissemination, death-driven tombstoning across a
//! cluster) is covered by `gossip_actors_transport.rs` and `registry_actor.rs`.

use distribution::registry::{ClusterRegistry, RegistryConfig, RegistryEntry};
use distribution::types::NodeId;
use swactor::actor::ActorAddress;

// ─── LWW conflict — higher timestamp wins ──────────────────────────────────

#[test]
fn lww_conflict_higher_timestamp_wins() {
    let mut reg = ClusterRegistry::new(RegistryConfig::default());
    let addr_old = ActorAddress::new_random();
    let addr_new = ActorAddress::new_random();
    let node_id = NodeId([1; 32]);

    let old_entry = RegistryEntry {
        name: "svc".into(),
        actor_addr: addr_old,
        node_id,
        timestamp: 1,
        generation: 1,
        tombstone: false,
    };
    let new_entry = RegistryEntry {
        name: "svc".into(),
        actor_addr: addr_new,
        node_id,
        timestamp: 5,
        generation: 2,
        tombstone: false,
    };

    // Merge in either order — newer timestamp wins.
    reg.merge(new_entry.clone());
    reg.merge(old_entry.clone());

    assert_eq!(reg.resolve("svc"), Some((addr_new, node_id)));
}

// ─── LWW tiebreak — generation then node_id ────────────────────────────────

#[test]
fn lww_tiebreak_generation_then_node_id() {
    let mut reg = ClusterRegistry::new(RegistryConfig::default());

    let addr_a = ActorAddress::new_random();
    let addr_b = ActorAddress::new_random();
    let node_low = NodeId([0; 32]);
    let node_high = NodeId([255; 32]);

    // Same timestamp, same generation — node_id breaks the tie.
    let entry_low = RegistryEntry {
        name: "x".into(),
        actor_addr: addr_a,
        node_id: node_low,
        timestamp: 10,
        generation: 1,
        tombstone: false,
    };
    let entry_high = RegistryEntry {
        name: "x".into(),
        actor_addr: addr_b,
        node_id: node_high,
        timestamp: 10,
        generation: 1,
        tombstone: false,
    };

    reg.merge(entry_low);
    reg.merge(entry_high);

    // Higher node_id wins.
    assert_eq!(reg.resolve("x"), Some((addr_b, node_high)));

    // And same-timestamp, different-generation: higher generation wins.
    let mut reg2 = ClusterRegistry::new(RegistryConfig::default());
    let entry_gen1 = RegistryEntry {
        name: "y".into(),
        actor_addr: addr_a,
        node_id: node_low,
        timestamp: 10,
        generation: 1,
        tombstone: false,
    };
    let entry_gen2 = RegistryEntry {
        name: "y".into(),
        actor_addr: addr_b,
        node_id: node_low,
        timestamp: 10,
        generation: 2,
        tombstone: false,
    };
    reg2.merge(entry_gen1);
    reg2.merge(entry_gen2);
    assert_eq!(reg2.resolve("y"), Some((addr_b, node_low)));
}

// ─── Tombstone GC removes old tombstones ───────────────────────────────────

#[test]
fn tombstone_gc_removes_old_tombstones() {
    let mut reg = ClusterRegistry::new(RegistryConfig {
        tombstone_ttl: 10,
        gc_interval: 1,
        ..RegistryConfig::default()
    });

    let actor = ActorAddress::new_random();
    let node_id = NodeId([1; 32]);

    reg.register("gc-me".into(), actor, node_id, 1);
    reg.unregister("gc-me", node_id, 1);

    // Tombstone exists.
    assert_eq!(reg.resolve("gc-me"), None);
    assert_eq!(reg.tombstone_count(), 1);

    // Advance the clock past TTL by registering enough other things.
    for i in 0..15 {
        let a = ActorAddress::new_random();
        reg.register(format!("filler-{i}"), a, node_id, 1);
    }

    // Need to drain dissemination for "gc-me" tombstone so GC can remove it.
    for _ in 0..20 {
        reg.take_pending(100);
    }

    // Now run GC.
    reg.gc_tick();

    // The tombstone should be gone.
    assert_eq!(
        reg.tombstone_count(),
        0,
        "tombstone should be GC'd after TTL"
    );
}
