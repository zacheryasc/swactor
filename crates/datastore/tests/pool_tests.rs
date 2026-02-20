//! Pool protocol integration tests.
//!
//! Tests the pool disseminator's convergence behavior, the gossip channel
//! integration, and the coordinator actor's lifecycle.

use std::sync::{Arc, Mutex};

use distribution::gossip_channel::{GossipChannel, serialize_each};
use distribution::types::NodeId;
use shared_types::ContentHash;
use shared_types::pool::*;
use swactor_datastore::pool::disseminator::{PoolDisseminator, SharedPoolChannel};

fn node_id(b: u8) -> NodeId {
    NodeId([b; 32])
}

fn make_pool_disseminator(node_byte: u8) -> PoolDisseminator {
    PoolDisseminator::new(
        PoolId::from_name("test-pool"),
        "test-pool".into(),
        node_id(node_byte),
        3,
    )
}

// ─── Disseminator convergence scenarios ────────────────────────────────────

/// Two nodes join a pool and exchange gossip until they converge
/// on identical state.
#[test]
fn two_nodes_converge_on_membership() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);

    d1.join(2);
    d2.join(2);

    // Simulate 3 gossip rounds
    for _ in 0..3 {
        let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
        let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));

        let bytes1 = ch1.take_pending_bytes(100);
        let bytes2 = ch2.take_pending_bytes(100);

        ch1.apply_incoming_bytes(&bytes2, 2);
        ch2.apply_incoming_bytes(&bytes1, 2);

        d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();
        d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();
    }

    assert_eq!(d1.member_count(), 2);
    assert_eq!(d2.member_count(), 2);
}

/// Content announced on one node is visible on another after gossip.
#[test]
fn content_location_propagates_via_gossip() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);
    let hash = ContentHash::of(b"test-content");

    d1.join(2);
    d2.join(2);
    d1.announce_content(hash, 2);

    // Gossip d1 → d2
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let bytes = ch1.take_pending_bytes(100);
    let _d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();

    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    let d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert_eq!(d2.locate_content(&hash), vec![node_id(1)]);
}

/// A node's leave is propagated and removes it from the active member list
/// on the receiving node.
#[test]
fn leave_propagates_via_gossip() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);

    d1.join(2);
    d2.join(2);

    // Exchange so both see each other
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    let bytes1 = ch1.take_pending_bytes(100);
    let bytes2 = ch2.take_pending_bytes(100);
    ch1.apply_incoming_bytes(&bytes2, 2);
    ch2.apply_incoming_bytes(&bytes1, 2);
    d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();
    d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert_eq!(d1.member_count(), 2);
    assert_eq!(d2.member_count(), 2);

    // d1 leaves
    d1.leave(2);

    // Gossip leave to d2
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let bytes = ch1.take_pending_bytes(100);
    let _d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();

    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    let d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert_eq!(d2.member_count(), 1); // only d2 remains active
}

/// Content tombstone propagates and removes the location.
#[test]
fn content_deletion_propagates() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);
    let hash = ContentHash::of(b"ephemeral");

    d1.join(2);
    d2.join(2);
    d1.announce_content(hash, 2);

    // Propagate content announcement
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let bytes = ch1.take_pending_bytes(100);
    d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();
    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert_eq!(d2.locate_content(&hash).len(), 1);

    // d1 removes content
    d1.remove_content(hash, 2);

    // Propagate tombstone
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let bytes = ch1.take_pending_bytes(100);
    let _ = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();
    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert!(d2.locate_content(&hash).is_empty());
}

/// ACL grant propagates to other nodes.
#[test]
fn acl_grant_propagates() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);

    d1.grant_access(node_id(3), 2);

    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let bytes = ch1.take_pending_bytes(100);
    let _ = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();

    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert!(d2.is_node_authorized(&node_id(3)));
    assert!(!d2.is_node_authorized(&node_id(4)));
}

/// Capacity announcement propagates and is reflected in summary.
#[test]
fn capacity_propagates_and_summarizes() {
    let mut d1 = make_pool_disseminator(1);
    let mut d2 = make_pool_disseminator(2);

    d1.join(2);
    d2.join(2);
    d1.announce_capacity(1_000_000, 100_000, 2);
    d2.announce_capacity(2_000_000, 200_000, 2);

    // Exchange
    let mut ch1 = SharedPoolChannel::new(Arc::new(Mutex::new(d1)));
    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    let bytes1 = ch1.take_pending_bytes(100);
    let bytes2 = ch2.take_pending_bytes(100);
    ch1.apply_incoming_bytes(&bytes2, 2);
    ch2.apply_incoming_bytes(&bytes1, 2);
    d1 = Arc::try_unwrap(ch1.into_inner()).unwrap().into_inner().unwrap();
    d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    let (total1, used1) = d1.pool_capacity_summary();
    let (total2, used2) = d2.pool_capacity_summary();

    assert_eq!(total1, 3_000_000);
    assert_eq!(used1, 300_000);
    assert_eq!(total2, 3_000_000);
    assert_eq!(used2, 300_000);
}

/// The node_with_most_free_space query returns the correct node.
#[test]
fn placement_query_picks_node_with_most_space() {
    let mut d = make_pool_disseminator(1);
    d.join(3);

    // Simulate node 2 joining and having lots of space
    let member2 = PoolEntry::Membership(PoolMemberEntry {
        pool_id: PoolId::from_name("test-pool"),
        node_id: [2u8; 32],
        state: PoolMemberState::Active,
        generation: 1,
    });
    let cap2 = PoolEntry::Capacity(PoolCapacityEntry {
        pool_id: PoolId::from_name("test-pool"),
        node_id: [2u8; 32],
        total_bytes: 10_000_000,
        used_bytes: 1_000_000,
        generation: 1,
    });

    // Simulate node 3 with less free space
    let member3 = PoolEntry::Membership(PoolMemberEntry {
        pool_id: PoolId::from_name("test-pool"),
        node_id: [3u8; 32],
        state: PoolMemberState::Active,
        generation: 1,
    });
    let cap3 = PoolEntry::Capacity(PoolCapacityEntry {
        pool_id: PoolId::from_name("test-pool"),
        node_id: [3u8; 32],
        total_bytes: 5_000_000,
        used_bytes: 4_000_000,
        generation: 1,
    });

    let entries = vec![member2, cap2, member3, cap3];
    let bytes = serialize_each(&entries);
    let mut ch = SharedPoolChannel::new(Arc::new(Mutex::new(d)));
    ch.apply_incoming_bytes(&bytes, 3);
    d = Arc::try_unwrap(ch.into_inner()).unwrap().into_inner().unwrap();

    // d1 has no capacity announced, node 2 has 9M free, node 3 has 1M free
    let best = d.node_with_most_free_space().unwrap();
    assert_eq!(best, node_id(2));
}

/// Five-node convergence: all nodes join, one announces content, everyone converges.
#[test]
fn five_node_pool_converges() {
    let pool = PoolId::from_name("five-pool");
    let mut nodes: Vec<PoolDisseminator> = (0..5)
        .map(|i| PoolDisseminator::new(pool, "five-pool".into(), node_id(i as u8), 3))
        .collect();

    // All join
    for n in &mut nodes {
        n.join(5);
    }

    // Node 0 and 2 announce content
    let hash_a = ContentHash::of(b"file-a");
    let hash_b = ContentHash::of(b"file-b");
    nodes[0].announce_content(hash_a, 5);
    nodes[2].announce_content(hash_b, 5);

    // Run 10 gossip rounds
    for _ in 0..10 {
        let mut channels: Vec<SharedPoolChannel> = nodes
            .into_iter()
            .map(|d| SharedPoolChannel::new(Arc::new(Mutex::new(d))))
            .collect();

        let pending: Vec<Vec<Vec<u8>>> = channels
            .iter_mut()
            .map(|ch| ch.take_pending_bytes(100))
            .collect();

        for (recv_idx, ch) in channels.iter_mut().enumerate() {
            for (send_idx, bytes) in pending.iter().enumerate() {
                if recv_idx != send_idx {
                    ch.apply_incoming_bytes(bytes, 5);
                }
            }
        }

        nodes = channels
            .into_iter()
            .map(|ch| Arc::try_unwrap(ch.into_inner()).unwrap().into_inner().unwrap())
            .collect();
    }

    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(node.member_count(), 5, "node {i} should see 5 members");
        assert_eq!(
            node.locate_content(&hash_a),
            vec![node_id(0)],
            "node {i} should locate file-a on node 0"
        );
        assert_eq!(
            node.locate_content(&hash_b),
            vec![node_id(2)],
            "node {i} should locate file-b on node 2"
        );
    }
}

// ─── GossipChannel wire format tests ───────────────────────────────────────

/// Verify the GossipChannel topic tag is correct.
#[test]
fn shared_pool_channel_topic_tag() {
    let d = make_pool_disseminator(1);
    let ch = SharedPoolChannel::new(Arc::new(Mutex::new(d)));
    assert_eq!(GossipChannel::topic_tag(&ch), "pool");
}

/// Verify serialization round-trip through GossipChannel bytes interface.
#[test]
fn gossip_channel_bytes_roundtrip() {
    let mut d = make_pool_disseminator(1);
    d.join(2);
    let hash = ContentHash::of(b"roundtrip");
    d.announce_content(hash, 2);

    let mut ch = SharedPoolChannel::new(Arc::new(Mutex::new(d)));
    let bytes = ch.take_pending_bytes(100);
    assert!(!bytes.is_empty());

    // Apply to a fresh disseminator
    let d2 = make_pool_disseminator(2);
    let mut ch2 = SharedPoolChannel::new(Arc::new(Mutex::new(d2)));
    ch2.apply_incoming_bytes(&bytes, 2);
    let d2 = Arc::try_unwrap(ch2.into_inner()).unwrap().into_inner().unwrap();

    assert_eq!(d2.member_count(), 1);
    assert_eq!(d2.locate_content(&hash), vec![node_id(1)]);
}
