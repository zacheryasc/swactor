use distribution::kademlia::routing_table::RoutingTable;
use distribution::types::NodeId;

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

// ─── Basic operations ───────────────────────────────────────────────────────

#[test]
fn insert_and_contains() {
    let mut rt = RoutingTable::new(node(0));
    assert!(rt.insert(node(1)));
    assert!(rt.contains(&node(1)));
    assert!(!rt.contains(&node(2)));
}

#[test]
fn insert_self_is_rejected() {
    let mut rt = RoutingTable::new(node(0));
    assert!(!rt.insert(node(0)));
    assert_eq!(rt.len(), 0);
}

#[test]
fn remove_node() {
    let mut rt = RoutingTable::new(node(0));
    rt.insert(node(1));
    assert!(rt.remove(&node(1)));
    assert!(!rt.contains(&node(1)));
    assert_eq!(rt.len(), 0);
}

#[test]
fn remove_nonexistent_returns_false() {
    let mut rt = RoutingTable::new(node(0));
    assert!(!rt.remove(&node(1)));
}

#[test]
fn duplicate_insert_updates_position() {
    let mut rt = RoutingTable::new(node(0));
    rt.insert(node(1));
    rt.insert(node(2));
    // Re-insert node 1 — should move to most-recently-seen
    assert!(rt.insert(node(1)));
    assert_eq!(rt.len(), 2);
}

// ─── Closest query ──────────────────────────────────────────────────────────

#[test]
fn closest_returns_k_nearest_by_xor() {
    let self_id = NodeId([0x00; 32]);
    let mut rt = RoutingTable::new(self_id);

    // Insert nodes with varying distances
    for i in 1..=10u8 {
        let mut bytes = [0u8; 32];
        bytes[0] = i;
        rt.insert(NodeId(bytes));
    }

    let target = NodeId([0x00; 32]); // same as self, closest by XOR
    let closest = rt.closest(&target, 3);
    assert_eq!(closest.len(), 3);

    // XOR distance to [0x00...] is [i, 0, 0, ...] — smallest i first
    assert_eq!(closest[0].node_id.0[0], 1);
    assert_eq!(closest[1].node_id.0[0], 2);
    assert_eq!(closest[2].node_id.0[0], 3);
}

#[test]
fn closest_returns_all_when_fewer_than_count() {
    let mut rt = RoutingTable::new(node(0));
    rt.insert(node(1));
    rt.insert(node(2));

    let closest = rt.closest(&node(0), 10);
    assert_eq!(closest.len(), 2);
}

#[test]
fn closest_to_specific_target() {
    let self_id = NodeId([0x00; 32]);
    let mut rt = RoutingTable::new(self_id);

    // Node A: XOR distance to target [0xFF...] is [0xFF ^ 0x01, ...] = [0xFE, ...]
    let mut a = [0u8; 32];
    a[0] = 0x01;
    rt.insert(NodeId(a));

    // Node B: XOR distance to target [0xFF...] is [0xFF ^ 0xFE, ...] = [0x01, ...]
    let mut b = [0u8; 32];
    b[0] = 0xFE;
    rt.insert(NodeId(b));

    let target = NodeId([0xFF; 32]);
    let closest = rt.closest(&target, 1);

    // B is closer to target (XOR = 0x01) than A (XOR = 0xFE)
    assert_eq!(closest[0].node_id.0[0], 0xFE);
}

// ─── Bucket capacity and replacement ────────────────────────────────────────

#[test]
fn bucket_overflow_goes_to_replacement_cache() {
    // Use k=2 for easy testing
    let self_id = NodeId([0x00; 32]);
    let mut rt = RoutingTable::with_k(self_id, 2);

    // Insert 3 nodes that all land in the same bucket
    // All have first byte != 0, so XOR leading zeros = 0 → bucket 0
    let mut bytes_a = [0u8; 32]; bytes_a[0] = 0x80;
    let mut bytes_b = [0u8; 32]; bytes_b[0] = 0xC0;
    let mut bytes_c = [0u8; 32]; bytes_c[0] = 0xA0;

    assert!(rt.insert(NodeId(bytes_a)));   // fits
    assert!(rt.insert(NodeId(bytes_b)));   // fits
    assert!(!rt.insert(NodeId(bytes_c)));  // goes to replacement

    assert_eq!(rt.len(), 2);
    assert!(rt.contains(&NodeId(bytes_a)));
    assert!(rt.contains(&NodeId(bytes_b)));
    assert!(!rt.contains(&NodeId(bytes_c)));
}

#[test]
fn removing_node_promotes_from_replacement() {
    let self_id = NodeId([0x00; 32]);
    let mut rt = RoutingTable::with_k(self_id, 2);

    let mut bytes_a = [0u8; 32]; bytes_a[0] = 0x80;
    let mut bytes_b = [0u8; 32]; bytes_b[0] = 0xC0;
    let mut bytes_c = [0u8; 32]; bytes_c[0] = 0xA0;

    rt.insert(NodeId(bytes_a));
    rt.insert(NodeId(bytes_b));
    rt.insert(NodeId(bytes_c)); // replacement

    // Remove A — C should be promoted
    rt.remove(&NodeId(bytes_a));
    assert_eq!(rt.len(), 2);
    assert!(rt.contains(&NodeId(bytes_b)));
    assert!(rt.contains(&NodeId(bytes_c)));
}

// ─── XOR distance ordering ─────────────────────────────────────────────────

#[test]
fn xor_distance_is_correct() {
    let a = NodeId([0x00; 32]);
    let b = NodeId([0xFF; 32]);
    let dist = a.xor_distance(&b);
    assert_eq!(dist, [0xFF; 32]);
}

#[test]
fn closest_ordering_is_stable_with_many_nodes() {
    let self_id = NodeId([0x00; 32]);
    let mut rt = RoutingTable::new(self_id);

    // Insert 50 nodes with random-ish IDs
    for i in 1..=50u8 {
        let mut bytes = [0u8; 32];
        bytes[0] = i;
        bytes[1] = i.wrapping_mul(37);
        rt.insert(NodeId(bytes));
    }

    let target = NodeId([0x10; 32]);
    let closest = rt.closest(&target, 10);

    // Verify sorted by XOR distance
    for window in closest.windows(2) {
        let d0 = window[0].node_id.xor_distance(&target);
        let d1 = window[1].node_id.xor_distance(&target);
        assert!(d0 <= d1, "closest results should be sorted by XOR distance");
    }
}

// ─── Empty table ────────────────────────────────────────────────────────────

#[test]
fn empty_table_closest_returns_empty() {
    let rt = RoutingTable::new(node(0));
    let closest = rt.closest(&node(1), 10);
    assert!(closest.is_empty());
}

#[test]
fn empty_table_has_zero_len() {
    let rt = RoutingTable::new(node(0));
    assert_eq!(rt.len(), 0);
    assert!(rt.is_empty());
}
