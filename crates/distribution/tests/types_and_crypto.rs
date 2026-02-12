use swactor::actor::ActorAddress;
use distribution::crypto::{self, Keypair};
use distribution::types::{DirectoryEntry, MemberState, NodeId, NodeRecord, Signature};

// ─── Keypair generation and identity ────────────────────────────────────────

#[test]
fn keypair_generates_distinct_identities() {
    let kp1 = Keypair::generate();
    let kp2 = Keypair::generate();
    assert_ne!(kp1.node_id(), kp2.node_id());
}

#[test]
fn keypair_roundtrips_through_secret_bytes() {
    let kp = Keypair::generate();
    let secret = kp.secret_bytes();
    let restored = Keypair::from_bytes(&secret);
    assert_eq!(kp.node_id(), restored.node_id());
}

// ─── Sign and verify raw bytes ──────────────────────────────────────────────

#[test]
fn sign_then_verify_succeeds() {
    let kp = Keypair::generate();
    let msg = b"hello distributed world";
    let sig = kp.sign(msg);
    assert!(crypto::verify(&kp.node_id(), msg, &sig));
}

#[test]
fn verify_rejects_wrong_message() {
    let kp = Keypair::generate();
    let sig = kp.sign(b"correct message");
    assert!(!crypto::verify(&kp.node_id(), b"wrong message", &sig));
}

#[test]
fn verify_rejects_wrong_key() {
    let kp1 = Keypair::generate();
    let kp2 = Keypair::generate();
    let sig = kp1.sign(b"some data");
    assert!(!crypto::verify(&kp2.node_id(), b"some data", &sig));
}

#[test]
fn verify_rejects_corrupted_signature() {
    let kp = Keypair::generate();
    let msg = b"important data";
    let mut sig = kp.sign(msg);
    sig.0[0] ^= 0xff; // flip bits
    assert!(!crypto::verify(&kp.node_id(), msg, &sig));
}

// ─── Directory entry signing ────────────────────────────────────────────────

#[test]
fn signed_directory_entry_verifies() {
    let kp = Keypair::generate();
    let actor_addr = ActorAddress::new_random();
    let entry = kp.sign_directory_entry(actor_addr, 1);

    assert_eq!(entry.actor_addr, actor_addr);
    assert_eq!(entry.node_id, kp.node_id());
    assert_eq!(entry.generation, 1);
    assert!(crypto::verify_directory_entry(&entry));
}

#[test]
fn tampered_directory_entry_fails_verification() {
    let kp = Keypair::generate();
    let actor_addr = ActorAddress::new_random();
    let mut entry = kp.sign_directory_entry(actor_addr, 1);

    // Tamper with generation
    entry.generation = 999;
    assert!(!crypto::verify_directory_entry(&entry));
}

#[test]
fn directory_entry_signed_by_wrong_key_fails() {
    let kp1 = Keypair::generate();
    let kp2 = Keypair::generate();
    let actor_addr = ActorAddress::new_random();
    let mut entry = kp1.sign_directory_entry(actor_addr, 1);

    // Replace node_id with a different key — signature won't match
    entry.node_id = kp2.node_id();
    assert!(!crypto::verify_directory_entry(&entry));
}

// ─── Serde round-trips ─────────────────────────────────────────────────────

#[test]
fn node_id_serde_roundtrip() {
    let kp = Keypair::generate();
    let id = kp.node_id();
    let json = serde_json::to_string(&id).unwrap();
    let back: NodeId = serde_json::from_str(&json).unwrap();
    assert_eq!(id, back);
}

#[test]
fn signature_serde_roundtrip() {
    let kp = Keypair::generate();
    let sig = kp.sign(b"test");
    let json = serde_json::to_string(&sig).unwrap();
    let back: Signature = serde_json::from_str(&json).unwrap();
    assert_eq!(sig, back);
}

#[test]
fn directory_entry_serde_roundtrip() {
    let kp = Keypair::generate();
    let actor_addr = ActorAddress::new_random();
    let entry = kp.sign_directory_entry(actor_addr, 42);

    let json = serde_json::to_string(&entry).unwrap();
    let back: DirectoryEntry = serde_json::from_str(&json).unwrap();

    assert_eq!(entry.actor_addr, back.actor_addr);
    assert_eq!(entry.node_id, back.node_id);
    assert_eq!(entry.generation, back.generation);
    assert_eq!(entry.signature, back.signature);
    assert!(crypto::verify_directory_entry(&back));
}

#[test]
fn node_record_serde_roundtrip() {
    let kp = Keypair::generate();
    let record = NodeRecord {
        node_id: kp.node_id(),
        addr: "127.0.0.1:8080".parse().unwrap(),
        state: MemberState::Alive,
        incarnation: 5,
    };
    let json = serde_json::to_string(&record).unwrap();
    let back: NodeRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(record.node_id, back.node_id);
    assert_eq!(record.incarnation, back.incarnation);
}

// ─── XOR distance ───────────────────────────────────────────────────────────

#[test]
fn xor_distance_to_self_is_zero() {
    let kp = Keypair::generate();
    let id = kp.node_id();
    let dist = id.xor_distance(&id);
    assert_eq!(dist, [0u8; 32]);
}

#[test]
fn xor_distance_is_symmetric() {
    let a = Keypair::generate().node_id();
    let b = Keypair::generate().node_id();
    assert_eq!(a.xor_distance(&b), b.xor_distance(&a));
}

#[test]
fn xor_leading_zeros_self_is_256() {
    let id = Keypair::generate().node_id();
    assert_eq!(id.xor_leading_zeros(&id), 256);
}

#[test]
fn xor_leading_zeros_opposite_is_zero() {
    let a = NodeId([0x00; 32]);
    let b = NodeId([0xff; 32]);
    assert_eq!(a.xor_leading_zeros(&b), 0);
}

#[test]
fn xor_leading_zeros_one_bit_difference() {
    let a = NodeId([0x00; 32]);
    let mut b_bytes = [0x00u8; 32];
    b_bytes[0] = 0x01; // differs only in bit 7 of first byte
    let b = NodeId(b_bytes);
    // XOR = 0x01 0x00 ... → leading zeros = 7
    assert_eq!(a.xor_leading_zeros(&b), 7);
}

// ─── MemberState ordering ───────────────────────────────────────────────────

#[test]
fn member_state_dead_overrides_suspect_overrides_alive() {
    assert!(MemberState::Dead > MemberState::Suspect);
    assert!(MemberState::Suspect > MemberState::Alive);
    assert!(MemberState::Dead > MemberState::Alive);
}
