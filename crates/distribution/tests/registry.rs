//! Behavioral tests for the cluster registry.
//!
//! Tests gossip-propagated naming via LWW-Register CRDT, using the shared
//! `TestCluster` harness from `common`.

mod common;

use swactor::actor::ActorAddress;
use common::{test_config, TestCluster};
use distribution::node::DistributedNode;
use distribution::registry::{ClusterRegistry, RegistryConfig, RegistryEntry, RegistryEvent};
use distribution::types::NodeId;

// ─── Test 1: register and resolve ───────────────────────────────────────────

#[test]
fn register_and_resolve() {
    let mut node = DistributedNode::new(test_config());
    let actor = ActorAddress::new_random();
    let node_id = node.node_id();

    node.register_name("my-actor".into(), actor);

    let result = node.resolve_name("my-actor");
    assert_eq!(result, Some((actor, node_id)));
}

// ─── Test 2: unregistered name returns None ─────────────────────────────────

#[test]
fn unregistered_name_returns_none() {
    let node = DistributedNode::new(test_config());
    assert_eq!(node.resolve_name("nonexistent"), None);
}

// ─── Test 3: unregister tombstones name ─────────────────────────────────────

#[test]
fn unregister_tombstones_name() {
    let mut node = DistributedNode::new(test_config());
    let actor = ActorAddress::new_random();

    node.register_name("service".into(), actor);
    assert!(node.resolve_name("service").is_some());

    node.unregister_name("service");
    assert_eq!(node.resolve_name("service"), None);
}

// ─── Test 4: re-registration updates binding ────────────────────────────────

#[test]
fn re_registration_updates_binding() {
    let mut node = DistributedNode::new(test_config());
    let actor_a = ActorAddress::new_random();
    let actor_b = ActorAddress::new_random();
    let node_id = node.node_id();

    node.register_name("foo".into(), actor_a);
    assert_eq!(node.resolve_name("foo"), Some((actor_a, node_id)));

    node.register_name("foo".into(), actor_b);
    assert_eq!(node.resolve_name("foo"), Some((actor_b, node_id)));
}

// ─── Test 5: LWW conflict — higher timestamp wins ──────────────────────────

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

// ─── Test 6: LWW tiebreak — generation then node_id ────────────────────────

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

// ─── Test 7: gossip propagates registration ─────────────────────────────────

#[test]
fn gossip_propagates_registration() {
    let mut cluster = TestCluster::new(2);

    let actor = ActorAddress::new_random();
    cluster[0].register_name("greeter".into(), actor);

    // B doesn't know about "greeter" yet.
    assert_eq!(cluster[1].resolve_name("greeter"), None);

    // Run gossip rounds — registry entries piggyback on SWIM messages.
    cluster.gossip_rounds(5);

    // Now B should resolve "greeter" to A's actor.
    let a_id = cluster.node_id(0);
    assert_eq!(cluster[1].resolve_name("greeter"), Some((actor, a_id)));
}

// ─── Test 8: tombstone propagation via gossip ───────────────────────────────

#[test]
fn tombstone_propagation_via_gossip() {
    let mut cluster = TestCluster::new(2);

    let actor = ActorAddress::new_random();
    cluster[0].register_name("ephemeral".into(), actor);

    // Propagate the registration.
    cluster.gossip_rounds(5);
    let a_id = cluster.node_id(0);
    assert_eq!(cluster[1].resolve_name("ephemeral"), Some((actor, a_id)));

    // Now unregister on A.
    cluster[0].unregister_name("ephemeral");

    // Propagate the tombstone.
    cluster.gossip_rounds(5);

    assert_eq!(cluster[1].resolve_name("ephemeral"), None);
}

// ─── Test 9: node death tombstones entries ──────────────────────────────────

#[test]
fn node_death_tombstones_entries() {
    // Set up a 3-node cluster: A(0), B(1), C(2)
    let mut cluster = TestCluster::new(3);

    let b_id = cluster.node_id(1);

    // B registers a name.
    let actor = ActorAddress::new_random();
    cluster[1].register_name("b-service".into(), actor);

    // Propagate B's registration to A and C via mesh gossip.
    cluster.gossip_rounds(5);

    assert_eq!(cluster[0].resolve_name("b-service"), Some((actor, b_id)));
    assert_eq!(cluster[2].resolve_name("b-service"), Some((actor, b_id)));

    // B dies — SWIM detects via timeout. We simulate by running rounds
    // without B participating, until suspicion_timeout expires.
    cluster.gossip_rounds_excluding(&[1], 20);

    // After enough ticks, A should declare B dead, which tombstones "b-service".
    assert_eq!(
        cluster[0].resolve_name("b-service"),
        None,
        "A must tombstone b-service after declaring B dead"
    );

    // Propagate tombstone from A to C.
    cluster.gossip_rounds_excluding(&[1], 5);
    assert_eq!(
        cluster[2].resolve_name("b-service"),
        None,
        "C should see tombstone after B's death propagates"
    );
}

// ─── Test 10: registry events emitted on change ─────────────────────────────

#[test]
fn registry_events_emitted_on_change() {
    let mut node = DistributedNode::new(test_config());
    let actor = ActorAddress::new_random();
    let node_id = node.node_id();

    node.register_name("evt-test".into(), actor);
    node.unregister_name("evt-test");

    let events = node.registry_events();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0],
        RegistryEvent::Registered {
            name: "evt-test".into(),
            actor_addr: actor,
            node_id,
        }
    );
    assert!(matches!(
        &events[1],
        RegistryEvent::Unregistered { name, previous_addr }
        if name == "evt-test" && *previous_addr == actor
    ));
}

// ─── Test 11: tombstone GC removes old tombstones ──────────────────────────

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
    assert_eq!(reg.tombstone_count(), 0, "tombstone should be GC'd after TTL");
}

// ─── Test 12: gossip convergence with five nodes ────────────────────────────

#[test]
fn gossip_convergence_five_nodes() {
    let mut cluster = TestCluster::new(5);

    // Each node registers a unique name.
    let actors: Vec<ActorAddress> = (0..5).map(|_| ActorAddress::new_random()).collect();
    for i in 0..5 {
        cluster[i].register_name(format!("service-{i}"), actors[i]);
    }

    // Run many gossip rounds.
    cluster.gossip_rounds(15);

    // All 5 names should be resolvable on all 5 nodes.
    for i in 0..5 {
        for j in 0..5 {
            let result = cluster[i].resolve_name(&format!("service-{j}"));
            assert_eq!(
                result,
                Some((actors[j], cluster.node_id(j))),
                "node {i} should resolve service-{j}"
            );
        }
    }
}
