//! Local support-state contracts.
//!
//! These tests cover state that is not itself gossip: bounded local caches and local peer-
//! authorization policy.
//!
//! Behavioral/correctness guarantees:
//! - Local support state is bounded, deterministic, and explicitly invalidated.
//! - Cached actor locations never outlive invalidation by actor or host node.
//! - Peer authorization policy is deterministic across memory and disk.

mod location_cache {
    //! Bounded ActorAddress-to-NodeId cache behavior: hit/miss, LRU refresh, updates, and explicit
    //! invalidation.

    use distribution::cache::LocationCache;
    use distribution::types::NodeId;
    use swactor::actor::ActorAddress;

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

        assert_eq!(
            cache.get(&a1),
            Some(node(1)),
            "a1 should survive (recently accessed)"
        );
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
}

mod peer_authorization {
    //! Peer allow-list behavior: open mode, restrictive file-backed mode, persistence, and
    //! revocation.

    use distribution::peer_auth::PeerAllowList;
    use distribution::types::NodeId;
    use std::fs;

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    fn temp_peer_file(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "swactor-dist-peer-auth-{name}-{}-{}.json",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = fs::remove_file(&path);
        path
    }

    #[test]
    fn open_mode_allows_every_peer_without_persisted_entries() {
        // Correctness: open mode is an explicit policy state, not an empty restrictive
        // allow-list. It accepts unknown peers and has no listed peer entries to save.
        let allow = PeerAllowList::open();

        assert!(allow.is_open());
        assert!(allow.is_allowed(&id(1)));
        assert!(allow.is_allowed(&id(2)));
        assert!(allow.list_peers().is_empty());
    }

    #[test]
    fn file_backed_allow_list_is_restrictive_and_round_trips() {
        // Correctness: a missing file creates restrictive mode; only added peers are
        // allowed, and saving/reloading preserves the exact authorization policy.
        let path = temp_peer_file("round-trip");
        let trusted = id(7);
        let other = id(8);

        let mut allow = PeerAllowList::from_file(&path).unwrap();
        assert!(!allow.is_open());
        assert!(!allow.is_allowed(&trusted));

        allow.add_peer(trusted, "trusted-peer".into());
        assert!(allow.is_allowed(&trusted));
        assert!(!allow.is_allowed(&other));
        allow.save().unwrap();

        let reloaded = PeerAllowList::from_file(&path).unwrap();
        assert!(reloaded.is_allowed(&trusted));
        assert!(!reloaded.is_allowed(&other));
        assert_eq!(reloaded.list_peers()[0].label, "trusted-peer");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn removing_a_peer_revokes_authorization() {
        // Correctness: authorization is explicit and reversible; removing a peer from a
        // restrictive allow-list immediately revokes it without changing open-mode policy.
        let mut allow = PeerAllowList::from_file(&temp_peer_file("remove")).unwrap();
        let trusted = id(9);

        allow.add_peer(trusted, "temporary".into());
        assert!(allow.is_allowed(&trusted));

        allow.remove_peer(&trusted);
        assert!(!allow.is_allowed(&trusted));
        assert!(allow.list_peers().is_empty());
    }
}
