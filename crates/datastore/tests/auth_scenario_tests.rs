//! Scenario tests for AuthzEngine — no actor system, pure auth logic.

use std::collections::{HashMap, HashSet};

use distribution::crypto::Keypair;
use shared_types::ContentHash;
use swactor_datastore::auth::{
    sign_request, AccessControlList, AuthzEngine, AuthzResult, DatastoreAction, DeniedReason,
    SignedRequestPayload,
};

fn owner_engine() -> (Keypair, AuthzEngine) {
    let owner_kp = Keypair::generate();
    let acl = AccessControlList {
        owner: owner_kp.node_id(),
        authorized_keys: HashSet::new(),
        key_labels: HashMap::new(),
    };
    (owner_kp, AuthzEngine::new(acl))
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Owner always allowed; random key denied
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn owner_is_always_allowed() {
    let (owner_kp, engine) = owner_engine();
    assert_eq!(engine.check_node(&owner_kp.node_id()), AuthzResult::Allowed);
}

#[test]
fn unknown_key_is_denied() {
    let (_owner_kp, engine) = owner_engine();
    let stranger = Keypair::generate().node_id();
    assert_eq!(
        engine.check_node(&stranger),
        AuthzResult::Denied(DeniedReason::NotAuthorized)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Grant → access → revoke → denied (lifecycle story)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn grant_then_revoke_lifecycle() {
    let (owner_kp, mut engine) = owner_engine();
    let client = Keypair::generate().node_id();

    // Initially denied
    assert_eq!(
        engine.check_node(&client),
        AuthzResult::Denied(DeniedReason::NotAuthorized)
    );

    // Grant
    engine.grant(&owner_kp.node_id(), client, None).unwrap();
    assert_eq!(engine.check_node(&client), AuthzResult::Allowed);

    // Revoke
    engine.revoke(&owner_kp.node_id(), client).unwrap();
    assert_eq!(
        engine.check_node(&client),
        AuthzResult::Denied(DeniedReason::NotAuthorized)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Only owner can grant/revoke; non-owner gets NotAuthorized
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn non_owner_cannot_grant() {
    let (_owner_kp, mut engine) = owner_engine();
    let impostor = Keypair::generate().node_id();
    let target = Keypair::generate().node_id();

    assert_eq!(
        engine.grant(&impostor, target, None),
        Err(DeniedReason::NotAuthorized)
    );
}

#[test]
fn non_owner_cannot_revoke() {
    let (owner_kp, mut engine) = owner_engine();
    let client = Keypair::generate().node_id();
    engine.grant(&owner_kp.node_id(), client, None).unwrap();

    let impostor = Keypair::generate().node_id();
    assert_eq!(
        engine.revoke(&impostor, client),
        Err(DeniedReason::NotAuthorized)
    );

    // Client still authorized
    assert_eq!(engine.check_node(&client), AuthzResult::Allowed);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Cannot revoke owner's implicit access
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn revoking_owner_is_noop() {
    let (owner_kp, mut engine) = owner_engine();
    let owner_id = owner_kp.node_id();

    // Attempt to revoke owner — should succeed (idempotent no-op) but owner remains allowed
    engine.revoke(&owner_id, owner_id).unwrap();
    assert_eq!(engine.check_node(&owner_id), AuthzResult::Allowed);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Signed request happy path (sign → verify → allowed)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn signed_request_happy_path() {
    let (owner_kp, mut engine) = owner_engine();
    let now = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"hello"),
        },
        timestamp: now,
        nonce: [1; 16],
    };
    let request = sign_request(&owner_kp, payload);

    assert_eq!(
        engine.check_signed_request(&request, now),
        AuthzResult::Allowed
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Tampered signature → InvalidSignature
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn tampered_signature_is_rejected() {
    let (owner_kp, mut engine) = owner_engine();
    let now = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"hello"),
        },
        timestamp: now,
        nonce: [2; 16],
    };
    let mut request = sign_request(&owner_kp, payload);
    // Tamper with signature
    request.signature.0[0] ^= 0xFF;

    assert_eq!(
        engine.check_signed_request(&request, now),
        AuthzResult::Denied(DeniedReason::InvalidSignature)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Stale timestamp → RequestExpired
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn stale_timestamp_is_rejected() {
    let (owner_kp, mut engine) = owner_engine();
    let now = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"stale"),
        },
        timestamp: now - 400, // 400s ago, outside 300s window
        nonce: [3; 16],
    };
    let request = sign_request(&owner_kp, payload);

    assert_eq!(
        engine.check_signed_request(&request, now),
        AuthzResult::Denied(DeniedReason::RequestExpired)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Replayed nonce → ReplayDetected
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn replayed_nonce_is_rejected() {
    let (owner_kp, mut engine) = owner_engine();
    let now = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"first"),
        },
        timestamp: now,
        nonce: [4; 16],
    };
    let request = sign_request(&owner_kp, payload);

    // First time — allowed
    assert_eq!(
        engine.check_signed_request(&request, now),
        AuthzResult::Allowed
    );

    // Replay — same nonce
    let payload2 = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"first"),
        },
        timestamp: now,
        nonce: [4; 16],
    };
    let request2 = sign_request(&owner_kp, payload2);
    assert_eq!(
        engine.check_signed_request(&request2, now),
        AuthzResult::Denied(DeniedReason::ReplayDetected)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Nonce GC frees old nonces for reuse
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nonce_gc_frees_old_nonces() {
    let (owner_kp, mut engine) = owner_engine();
    let t0 = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"gc-test"),
        },
        timestamp: t0,
        nonce: [5; 16],
    };
    let request = sign_request(&owner_kp, payload);
    assert_eq!(
        engine.check_signed_request(&request, t0),
        AuthzResult::Allowed
    );

    // Advance time past the window and GC
    let t1 = t0 + 400;
    engine.gc_nonces(t1);

    // Same nonce but with current timestamp — no longer flagged as replay
    let payload2 = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"gc-test"),
        },
        timestamp: t1,
        nonce: [5; 16],
    };
    let request2 = sign_request(&owner_kp, payload2);
    assert_eq!(
        engine.check_signed_request(&request2, t1),
        AuthzResult::Allowed
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Unauthorized key with valid signature → NotAuthorized
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn unauthorized_key_with_valid_signature_is_denied() {
    let (_owner_kp, mut engine) = owner_engine();
    let stranger_kp = Keypair::generate();
    let now = 1_000_000u64;

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"intrusion"),
        },
        timestamp: now,
        nonce: [6; 16],
    };
    let request = sign_request(&stranger_kp, payload);

    assert_eq!(
        engine.check_signed_request(&request, now),
        AuthzResult::Denied(DeniedReason::NotAuthorized)
    );
}
