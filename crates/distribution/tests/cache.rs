use swactor::actor::ActorAddress;
use distribution::cache::LocationCache;
use distribution::types::NodeId;

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

#[test]
fn insert_and_get() {
    let mut cache = LocationCache::new(10);
    let actor = ActorAddress::new_random();
    cache.insert(actor, node(1));
    assert_eq!(cache.get(&actor), Some(node(1)));
}

#[test]
fn get_missing_returns_none() {
    let mut cache = LocationCache::new(10);
    let actor = ActorAddress::new_random();
    assert_eq!(cache.get(&actor), None);
}

#[test]
fn invalidate_removes_entry() {
    let mut cache = LocationCache::new(10);
    let actor = ActorAddress::new_random();
    cache.insert(actor, node(1));
    assert!(cache.invalidate(&actor));
    assert_eq!(cache.get(&actor), None);
}

#[test]
fn capacity_evicts_lru() {
    let mut cache = LocationCache::new(2);
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    let a3 = ActorAddress::new_random();

    cache.insert(a1, node(1));
    cache.insert(a2, node(2));
    // a1 is LRU, inserting a3 should evict it
    cache.insert(a3, node(3));

    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get(&a1), None, "a1 should have been evicted");
    assert_eq!(cache.get(&a2), Some(node(2)));
    assert_eq!(cache.get(&a3), Some(node(3)));
}

#[test]
fn get_refreshes_lru_order() {
    let mut cache = LocationCache::new(2);
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    let a3 = ActorAddress::new_random();

    cache.insert(a1, node(1));
    cache.insert(a2, node(2));
    // Touch a1 — now a2 is LRU
    cache.get(&a1);
    // Insert a3 — should evict a2 (LRU), not a1
    cache.insert(a3, node(3));

    assert_eq!(cache.get(&a1), Some(node(1)), "a1 should survive (recently accessed)");
    assert_eq!(cache.get(&a2), None, "a2 should have been evicted");
}

#[test]
fn invalidate_node_removes_all_entries_for_that_node() {
    let mut cache = LocationCache::new(10);
    let a1 = ActorAddress::new_random();
    let a2 = ActorAddress::new_random();
    let a3 = ActorAddress::new_random();

    cache.insert(a1, node(1));
    cache.insert(a2, node(1)); // same node
    cache.insert(a3, node(2)); // different node

    let removed = cache.invalidate_node(&node(1));
    assert_eq!(removed, 2);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.peek(&a3), Some(node(2)));
}

#[test]
fn update_existing_entry() {
    let mut cache = LocationCache::new(10);
    let actor = ActorAddress::new_random();
    cache.insert(actor, node(1));
    cache.insert(actor, node(2));
    assert_eq!(cache.get(&actor), Some(node(2)));
    assert_eq!(cache.len(), 1);
}
