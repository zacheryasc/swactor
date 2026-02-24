//! Behavioral tests for directory repair and republish.
//!
//! These tests verify the consumer-facing behavior:
//! - When a node dies, its directory entries are queued for re-replication
//! - Periodic republish yields all locally-registered actors at the right cadence
//! - Repair queue can be drained by the caller

use swactor::actor::ActorAddress;
use distribution::crypto::{Keypair, KeypairExt};
use distribution::kademlia::directory::DirectoryShard;
use distribution::kademlia::repair::{RepairQueue, RepublishTracker};

// ─── Repair Queue: node death triggers re-replication ────────────────────────

#[test]
fn node_death_queues_affected_entries_for_re_replication() {
    // Given: a directory shard holding entries from two different nodes
    let kp_a = Keypair::generate();
    let kp_b = Keypair::generate();
    let actor1 = ActorAddress::new_random();
    let actor2 = ActorAddress::new_random();
    let actor3 = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp_a.sign_directory_entry(actor1, 1));
    shard.store(kp_a.sign_directory_entry(actor2, 1));
    shard.store(kp_b.sign_directory_entry(actor3, 1));

    let mut repair = RepairQueue::new();

    // When: node A dies
    let queued = repair.on_node_death(&kp_a.node_id(), &mut shard);

    // Then: both of node A's entries are queued, node B's entry remains in shard
    assert_eq!(queued, 2);
    assert_eq!(repair.len(), 2);
    assert!(shard.get(&actor3).is_some(), "node B's entry should survive");
    assert!(shard.get(&actor1).is_none(), "node A's entry should be removed from shard");
    assert!(shard.get(&actor2).is_none(), "node A's entry should be removed from shard");
}

#[test]
fn drain_yields_all_pending_entries_and_empties_queue() {
    // Given: a repair queue with entries from a dead node
    let kp = Keypair::generate();
    let actor1 = ActorAddress::new_random();
    let actor2 = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp.sign_directory_entry(actor1, 1));
    shard.store(kp.sign_directory_entry(actor2, 1));

    let mut repair = RepairQueue::new();
    repair.on_node_death(&kp.node_id(), &mut shard);

    // When: caller drains the queue
    let entries = repair.drain();

    // Then: all entries are returned and queue is empty
    assert_eq!(entries.len(), 2);
    assert!(repair.is_empty());
}

#[test]
fn multiple_node_deaths_accumulate_in_repair_queue() {
    // Given: entries from three nodes
    let kp_a = Keypair::generate();
    let kp_b = Keypair::generate();
    let kp_c = Keypair::generate();
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    let a3 = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp_a.sign_directory_entry(a1, 1));
    shard.store(kp_b.sign_directory_entry(a2, 1));
    shard.store(kp_c.sign_directory_entry(a3, 1));

    let mut repair = RepairQueue::new();

    // When: two nodes die in sequence
    repair.on_node_death(&kp_a.node_id(), &mut shard);
    repair.on_node_death(&kp_b.node_id(), &mut shard);

    // Then: both nodes' entries are queued
    assert_eq!(repair.len(), 2);
    assert_eq!(shard.entry_count(), 1, "only node C's entry remains");
}

// ─── Republish Tracker: periodic re-STORE ────────────────────────────────────

#[test]
fn republish_fires_at_configured_interval() {
    // Given: a tracker with interval=10, two registered actors
    let mut tracker = RepublishTracker::new(10);
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    tracker.register(a1, 1);
    tracker.register(a2, 3);

    // When: ticking before the interval
    assert!(tracker.tick(5).is_empty(), "too early");
    assert!(tracker.tick(9).is_empty(), "still too early");

    // When: ticking at the interval
    let batch = tracker.tick(10);

    // Then: all registered actors are returned
    assert_eq!(batch.len(), 2);
    let addrs: Vec<ActorAddress> = batch.iter().map(|(a, _)| *a).collect();
    assert!(addrs.contains(&a1));
    assert!(addrs.contains(&a2));
}

#[test]
fn republish_reschedules_after_firing() {
    // Given: a tracker that just fired at tick 10 (interval=10)
    let mut tracker = RepublishTracker::new(10);
    tracker.register(ActorAddress::new_random(), 1);
    let _ = tracker.tick(10); // fires

    // When: ticking at 15 (before next interval at 20)
    assert!(tracker.tick(15).is_empty());

    // When: ticking at 20 (next interval)
    let batch = tracker.tick(20);

    // Then: fires again
    assert_eq!(batch.len(), 1);
}

#[test]
fn unregistered_actors_are_excluded_from_republish() {
    // Given: two actors registered, then one unregistered
    let mut tracker = RepublishTracker::new(5);
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    tracker.register(a1, 1);
    tracker.register(a2, 1);
    tracker.unregister(&a1);

    // When: republish fires
    let batch = tracker.tick(5);

    // Then: only the remaining actor is included
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].0, a2);
}
