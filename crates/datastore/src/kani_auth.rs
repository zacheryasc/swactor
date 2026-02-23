//! Kani proof harnesses for AuthzEngine properties.
//!
//! Provides a bounded mirror of [`crate::auth::AuthzEngine`] that replaces
//! hash-based collections with fixed-size arrays and stubs out ed25519
//! crypto. This makes the logic tractable for Kani's symbolic execution
//! while preserving identical control-flow branches.
//!
//! Properties proven:
//! - **S1**: `check_node(n) = Allowed` ⟹ `n == owner ∨ n ∈ authorized_keys`
//! - **S2/S3/S4**: 4-step signed-request check rejects in the correct order
//! - **S5**: non-owner cannot mutate the ACL
//! - **S6**: owner access survives any sequence of grant/revoke operations
//! - **GC**: owner survives GC; expired nonces are freed

use crate::auth::{AuthzResult, DeniedReason};

// ─── Bounded types ──────────────────────────────────────────────────────────

const MAX_KEYS: usize = 3;
const MAX_NONCES: usize = 2;

/// Narrowed NodeId — 2 bytes (65 536 values) is plenty for proving
/// control-flow properties. Equality semantics identical to `[u8; 32]`.
#[derive(Clone, Copy, PartialEq, Eq)]
struct KaniNodeId([u8; 2]);

/// Bounded mirror of `AuthzEngine`. Array-backed collections replace
/// `HashSet`/`HashMap` so Kani avoids the SipHash symbolic explosion.
struct KaniAuthzEngine {
    owner: KaniNodeId,
    authorized_keys: [Option<KaniNodeId>; MAX_KEYS],
    key_count: usize,
    seen_nonces: [Option<([u8; 2], u64)>; MAX_NONCES],
    nonce_count: usize,
    timestamp_window: u64,
}

impl KaniAuthzEngine {
    fn new(owner: KaniNodeId) -> Self {
        Self {
            owner,
            authorized_keys: [None; MAX_KEYS],
            key_count: 0,
            seen_nonces: [None; MAX_NONCES],
            nonce_count: 0,
            timestamp_window: 300,
        }
    }

    // ── Bounded-set helpers: authorized_keys ─────────────────────────────

    fn keys_contains(&self, id: &KaniNodeId) -> bool {
        let mut i = 0;
        while i < self.key_count {
            if let Some(k) = self.authorized_keys[i] {
                if k == *id {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    fn keys_insert(&mut self, id: KaniNodeId) {
        if self.keys_contains(&id) {
            return;
        }
        if self.key_count < MAX_KEYS {
            self.authorized_keys[self.key_count] = Some(id);
            self.key_count += 1;
        }
    }

    fn keys_remove(&mut self, id: &KaniNodeId) {
        let mut i = 0;
        while i < self.key_count {
            if let Some(k) = self.authorized_keys[i] {
                if k == *id {
                    self.authorized_keys[i] = self.authorized_keys[self.key_count - 1];
                    self.authorized_keys[self.key_count - 1] = None;
                    self.key_count -= 1;
                    return;
                }
            }
            i += 1;
        }
    }

    // ── Bounded-set helpers: seen_nonces ─────────────────────────────────

    fn nonces_contains(&self, nonce: &[u8; 2]) -> bool {
        let mut i = 0;
        while i < self.nonce_count {
            if let Some((n, _)) = self.seen_nonces[i] {
                if n == *nonce {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    fn nonces_insert(&mut self, nonce: [u8; 2], ts: u64) {
        if self.nonce_count < MAX_NONCES {
            self.seen_nonces[self.nonce_count] = Some((nonce, ts));
            self.nonce_count += 1;
        }
    }

    // ── Mirror methods (identical control flow to auth.rs) ───────────────

    /// Mirrors `auth.rs` lines 211-217.
    fn check_node(&self, node_id: &KaniNodeId) -> AuthzResult {
        if *node_id == self.owner || self.keys_contains(node_id) {
            AuthzResult::Allowed
        } else {
            AuthzResult::Denied(DeniedReason::NotAuthorized)
        }
    }

    /// Mirrors `auth.rs` lines 277-290 (label omitted — irrelevant to auth logic).
    fn grant(
        &mut self,
        requester: &KaniNodeId,
        key: KaniNodeId,
    ) -> Result<(), DeniedReason> {
        if *requester != self.owner {
            return Err(DeniedReason::NotAuthorized);
        }
        if key == self.owner {
            return Ok(());
        }
        self.keys_insert(key);
        Ok(())
    }

    /// Mirrors `auth.rs` lines 294-305.
    fn revoke(
        &mut self,
        requester: &KaniNodeId,
        key: KaniNodeId,
    ) -> Result<(), DeniedReason> {
        if *requester != self.owner {
            return Err(DeniedReason::NotAuthorized);
        }
        if key != self.owner {
            self.keys_remove(&key);
        }
        Ok(())
    }

    /// Mirrors `auth.rs` lines 252-273.
    /// `sig_valid` replaces the `verify_signed_request` call (crypto stub).
    fn check_signed_request(
        &mut self,
        sig_valid: bool,
        public_key: &KaniNodeId,
        timestamp: u64,
        nonce: [u8; 2],
        now: u64,
    ) -> AuthzResult {
        // 1. Signature
        if !sig_valid {
            return AuthzResult::Denied(DeniedReason::InvalidSignature);
        }

        // 2. Timestamp freshness
        let diff = if now >= timestamp {
            now - timestamp
        } else {
            timestamp - now
        };
        if diff > self.timestamp_window {
            return AuthzResult::Denied(DeniedReason::RequestExpired);
        }

        // 3. Nonce uniqueness
        if self.nonces_contains(&nonce) {
            return AuthzResult::Denied(DeniedReason::ReplayDetected);
        }
        self.nonces_insert(nonce, timestamp);

        // 4. ACL check
        self.check_node(public_key)
    }

    /// Mirrors `auth.rs` lines 321-326.
    fn gc_nonces(&mut self, now: u64) {
        let mut write = 0;
        let mut read = 0;
        while read < self.nonce_count {
            if let Some((nonce, ts)) = self.seen_nonces[read] {
                let diff = if now >= ts { now - ts } else { ts - now };
                if diff <= self.timestamp_window {
                    self.seen_nonces[write] = Some((nonce, ts));
                    write += 1;
                }
            }
            read += 1;
        }
        let mut clear = write;
        while clear < self.nonce_count {
            self.seen_nonces[clear] = None;
            clear += 1;
        }
        self.nonce_count = write;
    }
}

// ─── Proof harnesses ────────────────────────────────────────────────────────

/// **S5**: If `requester != owner`, both `grant()` and `revoke()` return
/// `Err(NotAuthorized)`. No iteration — simplest harness.
#[kani::proof]
fn proof_s5_non_owner_cannot_mutate_acl() {
    let owner = KaniNodeId(kani::any());
    let requester = KaniNodeId(kani::any());
    let target = KaniNodeId(kani::any());

    kani::assume(requester != owner);

    let mut engine = KaniAuthzEngine::new(owner);

    assert!(engine.grant(&requester, target) == Err(DeniedReason::NotAuthorized));
    assert!(engine.revoke(&requester, target) == Err(DeniedReason::NotAuthorized));
}

/// **S1**: `check_node(n) = Allowed` implies `n == owner` or `n` was granted.
#[kani::proof]
#[kani::unwind(5)]
fn proof_s1_check_node() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    // Grant 0..MAX_KEYS symbolic keys
    let num_keys: usize = kani::any();
    kani::assume(num_keys <= MAX_KEYS);

    let mut granted = [KaniNodeId([0; 2]); MAX_KEYS];
    let mut i = 0;
    while i < num_keys {
        granted[i] = KaniNodeId(kani::any());
        engine.keys_insert(granted[i]);
        i += 1;
    }

    // Query with a symbolic node
    let query = KaniNodeId(kani::any());
    let result = engine.check_node(&query);

    if result == AuthzResult::Allowed {
        let mut is_authorized = query == owner;
        let mut j = 0;
        while j < num_keys {
            if query == granted[j] {
                is_authorized = true;
            }
            j += 1;
        }
        assert!(is_authorized);
    }
}

/// **S6**: After any sequence of grant/revoke operations (by any requester),
/// `check_node(owner)` always returns `Allowed`.
#[kani::proof]
#[kani::unwind(7)]
fn proof_s6_owner_irremovable() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    const MAX_OPS: usize = 5;
    let mut i = 0;
    while i < MAX_OPS {
        let requester = KaniNodeId(kani::any());
        let target = KaniNodeId(kani::any());
        let is_grant: bool = kani::any();

        if is_grant {
            let _ = engine.grant(&requester, target);
        } else {
            let _ = engine.revoke(&requester, target);
        }
        i += 1;
    }

    assert!(engine.check_node(&owner) == AuthzResult::Allowed);
}

/// **S2/S3/S4**: The 4-step signed-request check rejects in strict order.
/// Each denial reason implies the correct preconditions.
#[kani::proof]
#[kani::unwind(4)]
fn proof_signed_request_4step_ordering() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    // Optionally grant one key
    let has_granted: bool = kani::any();
    let granted_key = KaniNodeId(kani::any());
    if has_granted {
        engine.keys_insert(granted_key);
    }

    // Optionally pre-insert a nonce (to test replay detection)
    let pre_nonce: bool = kani::any();
    let nonce: [u8; 2] = kani::any();
    if pre_nonce {
        let old_ts: u64 = kani::any();
        engine.nonces_insert(nonce, old_ts);
    }

    let sig_valid: bool = kani::any();
    let public_key = KaniNodeId(kani::any());
    let timestamp: u64 = kani::any();
    let now: u64 = kani::any();

    let diff = if now >= timestamp {
        now - timestamp
    } else {
        timestamp - now
    };

    let result = engine.check_signed_request(sig_valid, &public_key, timestamp, nonce, now);

    match result {
        AuthzResult::Denied(DeniedReason::InvalidSignature) => {
            // Step 1 rejected: signature was invalid
            assert!(!sig_valid);
        }
        AuthzResult::Denied(DeniedReason::RequestExpired) => {
            // Step 2 rejected: sig valid, but timestamp outside window
            assert!(sig_valid);
            assert!(diff > 300);
        }
        AuthzResult::Denied(DeniedReason::ReplayDetected) => {
            // Step 3 rejected: sig valid, timestamp fresh, but nonce replayed
            assert!(sig_valid);
            assert!(diff <= 300);
            assert!(pre_nonce);
        }
        AuthzResult::Denied(DeniedReason::NotAuthorized) => {
            // Step 4 rejected: sig valid, timestamp fresh, nonce fresh, not in ACL
            assert!(sig_valid);
            assert!(diff <= 300);
            assert!(public_key != owner);
        }
        AuthzResult::Allowed => {
            // All 4 steps passed
            assert!(sig_valid);
            assert!(diff <= 300);
            assert!(public_key == owner || engine.keys_contains(&public_key));
        }
    }
}

/// **GC correctness**: Owner access survives GC; expired nonces are freed.
#[kani::proof]
#[kani::unwind(4)]
fn proof_gc_preserves_owner_and_frees_expired() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    let nonce: [u8; 2] = kani::any();
    let t1: u64 = kani::any();
    let t2: u64 = kani::any();
    kani::assume(t2 >= t1);

    engine.nonces_insert(nonce, t1);
    engine.gc_nonces(t2);

    // Owner always survives GC
    assert!(engine.check_node(&owner) == AuthzResult::Allowed);

    // Expired nonces must be freed
    if t2 - t1 > 300 {
        assert!(!engine.nonces_contains(&nonce));
    }
}
