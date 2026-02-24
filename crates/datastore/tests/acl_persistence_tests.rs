//! ACL file persistence tests — roundtrip save/load.

use std::collections::{HashMap, HashSet};

use swactor_datastore::crypto::Keypair;
use swactor_datastore::auth::AccessControlList;

// ═══════════════════════════════════════════════════════════════════════════
// 1. Save + load preserves owner and authorized_keys
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn save_and_load_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("acl.json");

    let owner = Keypair::generate().node_id();
    let client_a = Keypair::generate().node_id();
    let client_b = Keypair::generate().node_id();

    let mut keys = HashSet::new();
    keys.insert(client_a);
    keys.insert(client_b);

    let acl = AccessControlList {
        owner,
        authorized_keys: keys.clone(),
        key_labels: HashMap::new(),
    };
    acl.save(&path).unwrap();

    let loaded = AccessControlList::load_or_create(&path, owner).unwrap();
    assert_eq!(loaded.owner, owner);
    assert_eq!(loaded.authorized_keys, keys);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. load_or_create on missing file creates default
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn load_or_create_on_missing_file_creates_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent/acl.json");

    let owner = Keypair::generate().node_id();
    let acl = AccessControlList::load_or_create(&path, owner).unwrap();

    assert_eq!(acl.owner, owner);
    assert!(acl.authorized_keys.is_empty());

    // File should now exist
    assert!(path.exists());

    // Loading again should give same result
    let acl2 = AccessControlList::load_or_create(&path, owner).unwrap();
    assert_eq!(acl2.owner, owner);
    assert!(acl2.authorized_keys.is_empty());
}
