use distribution::swim::dissemination::{membership_update, DisseminationQueue};
use distribution::types::{MemberState, NodeId};

fn node(byte: u8) -> NodeId {
    NodeId([byte; 32])
}

// ─── Basic queue operations ─────────────────────────────────────────────────

#[test]
fn enqueue_and_take_single_update() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        5,
    );
    assert_eq!(q.len(), 1);

    let updates = q.take(10);
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].node_id, node(1));
}

#[test]
fn take_respects_max_count() {
    let mut q = DisseminationQueue::new(3);
    for i in 1..=5 {
        q.enqueue(
            membership_update(node(i), MemberState::Alive, 0),
            10,
        );
    }
    let updates = q.take(2);
    assert_eq!(updates.len(), 2);
}

// ─── Priority ordering ─────────────────────────────────────────────────────

#[test]
fn dead_updates_are_prioritized_over_suspect_and_alive() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        10,
    );
    q.enqueue(
        membership_update(node(2), MemberState::Dead, 0),
        10,
    );
    q.enqueue(
        membership_update(node(3), MemberState::Suspect, 0),
        10,
    );

    let updates = q.take(3);
    assert_eq!(updates[0].state, MemberState::Dead, "Dead should be first");
    assert_eq!(updates[1].state, MemberState::Suspect, "Suspect should be second");
    assert_eq!(updates[2].state, MemberState::Alive, "Alive should be last");
}

// ─── Transmit budget and eviction ───────────────────────────────────────────

#[test]
fn entries_evicted_after_transmit_budget_exhausted() {
    // lambda=1, cluster_size=2 → budget = 1 * ceil(log2(2)) = 1
    let mut q = DisseminationQueue::new(1);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        2,
    );

    // First take: remaining goes from 1 to 0
    let updates = q.take(10);
    assert_eq!(updates.len(), 1);

    // Entry should be evicted now
    assert_eq!(q.len(), 0);
    let updates = q.take(10);
    assert_eq!(updates.len(), 0);
}

#[test]
fn larger_cluster_gives_higher_transmit_budget() {
    // lambda=2, cluster_size=16 → budget = 2 * ceil(log2(16)) = 2 * 4 = 8
    let mut q = DisseminationQueue::new(2);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        16,
    );

    // Take 8 times — entry should survive all of them
    for i in 0..8 {
        let updates = q.take(10);
        assert_eq!(updates.len(), 1, "take #{i} should still have the entry");
    }

    // 9th take: entry should be evicted
    assert_eq!(q.len(), 0);
}

// ─── Dedup: newer update for same node replaces older ───────────────────────

#[test]
fn newer_update_for_same_node_replaces_older() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        10,
    );
    q.enqueue(
        membership_update(node(1), MemberState::Suspect, 0),
        10,
    );

    assert_eq!(q.len(), 1, "should replace, not duplicate");
    let updates = q.take(10);
    assert_eq!(updates[0].state, MemberState::Suspect);
}

#[test]
fn higher_incarnation_replaces_lower() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Dead, 5),
        10,
    );
    // Same node, higher incarnation, Alive (incarnation wins over state)
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 6),
        10,
    );

    assert_eq!(q.len(), 1);
    let updates = q.take(10);
    assert_eq!(updates[0].incarnation, 6);
    assert_eq!(updates[0].state, MemberState::Alive);
}

#[test]
fn lower_incarnation_is_ignored() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 5),
        10,
    );
    q.enqueue(
        membership_update(node(1), MemberState::Dead, 3),
        10,
    );

    let updates = q.take(10);
    assert_eq!(updates[0].incarnation, 5, "older incarnation should be ignored");
    assert_eq!(updates[0].state, MemberState::Alive);
}

// ─── Piggyback serialization ────────────────────────────────────────────────

#[test]
fn pack_and_unpack_piggyback_roundtrip() {
    let mut q = DisseminationQueue::new(3);
    q.enqueue(
        membership_update(node(1), MemberState::Alive, 0),
        10,
    );
    q.enqueue(
        membership_update(node(2), MemberState::Dead, 3),
        10,
    );

    let bytes = q.pack_piggyback(10);
    assert!(!bytes.is_empty());

    let unpacked = DisseminationQueue::unpack_piggyback(&bytes);
    assert_eq!(unpacked.len(), 2);
    // Dead should be first (priority ordering from take())
    assert_eq!(unpacked[0].state, MemberState::Dead);
}

#[test]
fn unpack_empty_piggyback_returns_empty() {
    let unpacked = DisseminationQueue::unpack_piggyback(&[]);
    assert!(unpacked.is_empty());
}

#[test]
fn unpack_garbage_returns_empty() {
    let unpacked = DisseminationQueue::unpack_piggyback(b"not valid json");
    assert!(unpacked.is_empty());
}
