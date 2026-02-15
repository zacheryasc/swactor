use swactor::actor::ActorAddress;
use distribution::crypto::Keypair;
use distribution::kademlia::directory::{
    actor_addr_as_node_id, resolve_quorum, DirectoryShard, QuorumResult,
};
use distribution::types::{NodeId, Signature};

// ─── DirectoryShard ─────────────────────────────────────────────────────────

#[test]
fn store_valid_entry() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();
    let entry = kp.sign_directory_entry(actor, 1);

    let mut shard = DirectoryShard::new();
    assert!(shard.store(entry));
    assert_eq!(shard.entry_count(), 1);
    assert!(shard.get(&actor).is_some());
}

#[test]
fn store_rejects_invalid_signature() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();
    let mut entry = kp.sign_directory_entry(actor, 1);
    entry.signature = Signature([0xFF; 64]); // corrupt signature

    let mut shard = DirectoryShard::new();
    assert!(!shard.store(entry));
    assert_eq!(shard.entry_count(), 0);
}

#[test]
fn store_higher_generation_replaces_lower() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp.sign_directory_entry(actor, 1));
    shard.store(kp.sign_directory_entry(actor, 2));

    let entries = shard.get(&actor).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].generation, 2);
}

#[test]
fn store_lower_generation_is_ignored() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp.sign_directory_entry(actor, 5));
    shard.store(kp.sign_directory_entry(actor, 3));

    let entries = shard.get(&actor).unwrap();
    assert_eq!(entries[0].generation, 5);
}

#[test]
fn multiple_nodes_can_register_same_actor() {
    let kp1 = Keypair::generate();
    let kp2 = Keypair::generate();
    let actor = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp1.sign_directory_entry(actor, 1));
    shard.store(kp2.sign_directory_entry(actor, 1));

    let entries = shard.get(&actor).unwrap();
    assert_eq!(entries.len(), 2);
}

#[test]
fn remove_by_node_clears_entries() {
    let kp = Keypair::generate();
    let actor1 = ActorAddress::new_random();
    let actor2 = ActorAddress::new_random();

    let mut shard = DirectoryShard::new();
    shard.store(kp.sign_directory_entry(actor1, 1));
    shard.store(kp.sign_directory_entry(actor2, 1));
    assert_eq!(shard.entry_count(), 2);

    let removed = shard.remove_by_node(&kp.node_id());
    assert_eq!(removed.len(), 2);
    assert_eq!(shard.entry_count(), 0);
}

// ─── Quorum resolution ─────────────────────────────────────────────────────

#[test]
fn quorum_resolved_with_majority_agreement() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();

    // 3 copies of the same entry (from 3 different nodes storing it)
    let entry = kp.sign_directory_entry(actor, 1);
    let entries = vec![entry.clone(), entry.clone(), entry.clone()];

    match resolve_quorum(&entries, 2) {
        QuorumResult::Resolved(e) => {
            assert_eq!(e.generation, 1);
            assert_eq!(e.node_id, kp.node_id());
        }
        other => panic!("expected Resolved, got {:?}", other),
    }
}

#[test]
fn quorum_not_met_returns_no_quorum() {
    let kp1 = Keypair::generate();
    let kp2 = Keypair::generate();
    let actor = ActorAddress::new_random();

    // One entry from kp1, one from kp2 — neither has quorum of 2
    let entries = vec![
        kp1.sign_directory_entry(actor, 1),
        kp2.sign_directory_entry(actor, 1),
    ];

    match resolve_quorum(&entries, 2) {
        QuorumResult::NoQuorum(all) => {
            assert_eq!(all.len(), 2);
        }
        other => panic!("expected NoQuorum, got {:?}", other),
    }
}

#[test]
fn quorum_empty_input_returns_not_found() {
    match resolve_quorum(&[], 1) {
        QuorumResult::NotFound => {}
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[test]
fn quorum_higher_generation_wins() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();

    let old = kp.sign_directory_entry(actor, 1);
    let new = kp.sign_directory_entry(actor, 2);

    // 2 copies of gen 1, 2 copies of gen 2 — both have quorum, but gen 2 wins
    let entries = vec![old.clone(), old, new.clone(), new];

    match resolve_quorum(&entries, 2) {
        QuorumResult::Resolved(e) => {
            assert_eq!(e.generation, 2);
        }
        other => panic!("expected Resolved, got {:?}", other),
    }
}

#[test]
fn quorum_ignores_entries_with_bad_signatures() {
    let kp = Keypair::generate();
    let actor = ActorAddress::new_random();

    let good = kp.sign_directory_entry(actor, 1);
    let mut bad = kp.sign_directory_entry(actor, 1);
    bad.signature = Signature([0xAA; 64]);

    // 1 good, 1 bad — quorum of 2 not met
    let entries = vec![good, bad];

    match resolve_quorum(&entries, 2) {
        QuorumResult::NoQuorum(_) => {}
        other => panic!("expected NoQuorum, got {:?}", other),
    }
}

// ─── actor_addr_as_node_id ──────────────────────────────────────────────────

#[test]
fn actor_addr_maps_to_node_id_correctly() {
    let addr = ActorAddress([0xAB; 32]);
    let nid = actor_addr_as_node_id(&addr);
    assert_eq!(nid, NodeId([0xAB; 32]));
}
