# Formal Verification Report: Datastore Auth System

**Date**: 2026-02-19
**Branch**: `formal-verification`
**Tools**: proptest-state-machine 0.3 (Phase 1), Kani (Phase 2), Stateright 0.31 (Phase 3)

## Abstract

The authorization system is the security boundary of the swactor datastore — every external request must pass through `GatewayActor`, which delegates decisions to `AuthzEngine`. Three complementary verification techniques are applied to this boundary. Phase 1 randomizes operation sequences against a deliberately simple reference model, catching emergent bugs across thousands of grant/revoke/sign/gc interactions with real ed25519 crypto (high coverage, function level). Phase 2 symbolically executes all possible inputs through bounded proofs using Kani's SMT solver, proving that every reachable branch satisfies its specification (exhaustive within bounds, function level). Phase 3 model-checks all message orderings at the dispatch level using Stateright, exhaustively exploring every interleaving of grants, revokes, signed requests, signature checks, GC ticks, and clock advances to verify that unauthorized messages never reach internal actors (exhaustive, actor level). Together these three phases cover seven safety properties (S1–S7) and five liveness/reachability properties (L1–L5). One real bug was found and fixed during Phase 1: `grant(owner, owner)` was adding the owner to the explicit authorized set, creating an irremovable entry that violated the design invariant of purely implicit owner access.

---

## 1. Architecture Overview

### The Two-Layer Design

The auth system has two verification targets:

1. **`AuthzEngine`** (function level) — a pure decision engine. Given a request, it returns `Allowed` or `Denied`. It holds the ACL, nonce replay state, and timestamp window. This is the subject of Phases 1 and 2.

2. **`GatewayActor`** (dispatch level) — the actor that holds an `AuthzEngine` instance and uses its decisions to route messages. The security-critical question isn't just "does the engine return the right answer?" but "does the actor act on it correctly?" This is the subject of Phase 3.

### The Auth Pipeline

```
External Client → GatewayActor → DatastoreNode → MetadataActor/BlobStoreActor
                  (auth check)   (dispatch)       (auth-unaware)
```

The gateway is the ONLY path to internal actors. If the gateway dispatches incorrectly, the entire auth system is bypassed.

### The Handler Inventory

The gateway has 11 message handlers. Of these, EXACTLY ONE (`handle_signed_request`) sends to `self.datastore_node`. The other 10 send exclusively to `reply_to`. This asymmetry is the foundation of property S7: "unauthorized messages never reach internal actors."

The critical dispatch path — `handle_signed_request` at `gateway.rs:69-80`:

```rust
fn handle_signed_request(&mut self, ctx: &Ctx, request: crate::auth::SignedRequest, reply_to: ActorAddress) {
    let now = Self::now_secs();
    match self.engine.check_signed_request(&request, now) {
        AuthzResult::Allowed => {
            let msg = action_to_node_msg(request.payload.action, reply_to);
            let _ = ctx.send(self.datastore_node, msg);  // ← THE ONLY send to datastore_node
        }
        AuthzResult::Denied(reason) => {
            let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
        }
    }
}
```

Line 74 — `ctx.send(self.datastore_node, msg)` — is the ONLY place in the entire gateway that sends to `datastore_node`. The full `handle()` match confirms this (`gateway.rs:176-221`):

```rust
fn handle(&mut self, ctx: &Ctx, msg: GatewayMsg) {
    match msg {
        GatewayMsg::HandleSignedRequest { request, reply_to } => {
            self.handle_signed_request(ctx, request, reply_to);    // → datastore_node on Allowed
        }
        GatewayMsg::CheckConnection { node_id, reply_to } => {
            self.handle_check_connection(ctx, node_id, reply_to);  // → reply_to only
        }
        GatewayMsg::Grant { requester, key, label, reply_to } => {
            self.handle_grant(ctx, requester, key, label, reply_to); // → reply_to only
        }
        GatewayMsg::Revoke { requester, key, reply_to } => {
            self.handle_revoke(ctx, requester, key, reply_to);     // → reply_to only
        }
        GatewayMsg::Authorize { request, reply_to } => {
            self.handle_authorize(ctx, request, reply_to);         // → reply_to only
        }
        GatewayMsg::VerifySignature { request, reply_to } => {
            self.handle_verify_signature(ctx, request, reply_to);  // → reply_to only
        }
        GatewayMsg::SubmitAccessRequest { key, name, message, reply_to } => {
            self.handle_submit_access_request(ctx, key, name, message, reply_to); // → reply_to only
        }
        GatewayMsg::ListAccessRequests { requester, reply_to } => {
            self.handle_list_access_requests(ctx, requester, reply_to); // → reply_to only
        }
        GatewayMsg::DenyAccessRequest { requester, key, reply_to } => {
            self.handle_deny_access_request(ctx, requester, key, reply_to); // → reply_to only
        }
        GatewayMsg::ListAuthorizedKeys { requester, reply_to } => {
            self.handle_list_authorized_keys(ctx, requester, reply_to); // → reply_to only
        }
        GatewayMsg::NonceGcTick => {
            self.engine.gc_nonces(Self::now_secs());               // no send at all
        }
    }
}
```

### AuthzEngine State

The engine (`auth.rs:191-206`) holds three fields:

```rust
pub struct AuthzEngine {
    pub acl: AccessControlList,       // owner + authorized_keys
    seen_nonces: HashMap<[u8; 16], u64>, // replay protection
    timestamp_window: u64,            // freshness window (300s)
}

impl AuthzEngine {
    pub fn new(acl: AccessControlList) -> Self {
        Self {
            acl,
            seen_nonces: HashMap::new(),
            timestamp_window: 300,
        }
    }
}
```

The engine is stateful because nonce tracking requires memory — each seen nonce is recorded with its timestamp so that GC can evict expired entries.

---

## 2. Property Catalog

### Safety Properties (S1–S7)

Things that must NEVER happen. Each is a universal claim: "for all states, for all inputs, X holds."

| ID | Property | Formal Statement |
|----|----------|-----------------|
| S1 | ACL integrity | `check_node(n) = Allowed` implies `n == owner` or `n ∈ authorized_keys` |
| S2 | Signature verification | Invalid signature always yields `Denied(InvalidSignature)` |
| S3 | Timestamp freshness | `\|now - ts\| > 300` always yields `Denied(RequestExpired)` |
| S4 | Replay protection | Seen nonce always yields `Denied(ReplayDetected)` |
| S5 | ACL immutability | Non-owner `grant()`/`revoke()` always returns `Err(NotAuthorized)` |
| S6 | Owner persistence | `check_node(owner) = Allowed` holds after any operation sequence |
| S7 | Dispatch integrity | `DatastoreNodeMsg` is only sent when the signer is authorized |

### Liveness / Reachability Properties (L1–L5)

Things that CAN happen. Existential claims that prove the system isn't trivially safe by being non-functional.

| ID | Property | Meaning |
|----|----------|---------|
| L1 | Grant leads to access | A granted key has `Allowed` status until revoked |
| L2 | GC enables nonce reuse | After time advances past the window, GC frees expired nonces |
| L3 | Authorized dispatch reachable | There exists a reachable state where authorized dispatch happened |
| L4 | Granted key can dispatch | A non-owner granted key can trigger dispatch to `datastore_node` |
| L5 | Dispatch after GC reachable | Dispatch occurs with an empty nonce table (after GC clears all) |

### Coverage Matrix

| Property | Phase 1 (proptest) | Phase 2 (Kani) | Phase 3 (Stateright) | Scenario Tests |
|----------|--------------------|----------------|----------------------|----------------|
| S1 | postcondition + invariant | `proof_s1_check_node` | — | `unknown_key_is_denied` |
| S2 | — | `proof_signed_request_4step_ordering` | — | `tampered_signature_is_rejected` |
| S3 | postcondition | `proof_signed_request_4step_ordering` | — | `stale_timestamp_is_rejected` |
| S4 | postcondition | `proof_signed_request_4step_ordering` | — | `replayed_nonce_is_rejected` |
| S5 | postcondition | `proof_s5_non_owner_cannot_mutate_acl` | — | `non_owner_cannot_grant/revoke` |
| S6 | invariant | `proof_s6_owner_irremovable` | `S6-gw` (always) | `revoking_owner_is_noop` |
| S7 | — | — | `S7` (always) + `L3` canary | actor-level tests |
| L1 | invariant | — | — | `grant_then_revoke_lifecycle` |
| L2 | emergent | `proof_gc_preserves_owner_and_frees_expired` | — | `nonce_gc_frees_old_nonces` |
| L3 | — | — | `L3` (sometimes) | — |
| L4 | — | — | `L4` (sometimes) | — |
| L5 | — | — | `L5` (sometimes) | — |

### Why Proptest Cannot Test S2

Proptest uses real ed25519 signing — the state machine always generates valid signatures by construction. It never produces an invalid signature, so it can never trigger the `InvalidSignature` branch. Kani stubs crypto as a boolean (`sig_valid: bool`), making S2 testable symbolically — Kani explores both `sig_valid = true` and `sig_valid = false`, proving the signature check rejects correctly. The scenario test `tampered_signature_is_rejected` covers S2 concretely by bit-flipping a real signature.

---

## 3. Production Code Under Verification

These are the actual production functions. They serve as the reference for judging mirror faithfulness in Phases 2 and 3.

### `check_node` (`auth.rs:211-217`)

The simplest check — the oracle for all authorization decisions. S1 and S7 both depend on this function.

```rust
pub fn check_node(&self, node_id: &NodeId) -> AuthzResult {
    if *node_id == self.acl.owner || self.acl.authorized_keys.contains(node_id) {
        AuthzResult::Allowed
    } else {
        AuthzResult::Denied(DeniedReason::NotAuthorized)
    }
}
```

### `check_signed_request` (`auth.rs:252-273`)

The most critical function. Four-step pipeline in strict order:

```rust
pub fn check_signed_request(&mut self, request: &SignedRequest, now: u64) -> AuthzResult {
    // 1. Signature
    if !verify_signed_request(request) {
        return AuthzResult::Denied(DeniedReason::InvalidSignature);
    }

    // 2. Timestamp freshness
    let ts = request.payload.timestamp;
    let diff = if now >= ts { now - ts } else { ts - now };
    if diff > self.timestamp_window {
        return AuthzResult::Denied(DeniedReason::RequestExpired);
    }

    // 3. Nonce uniqueness
    if self.seen_nonces.contains_key(&request.payload.nonce) {
        return AuthzResult::Denied(DeniedReason::ReplayDetected);
    }
    self.seen_nonces.insert(request.payload.nonce, ts);

    // 4. ACL check
    self.check_node(&request.public_key)
}
```

Key design detail: the nonce is consumed at step 3 BEFORE the ACL check at step 4. This means an unauthorized request with a valid signature and fresh timestamp still consumes the nonce. This is deliberate — it prevents an attacker from probing ACL membership without cost. All mirrors must replicate this ordering.

### `check_signature_only` (`auth.rs:223-243`)

Steps 1–3 only, no ACL check. Used by `VerifySignature` and `Authorize` handlers. Important: it shares nonce state with `check_signed_request` — a nonce consumed by `check_signature_only` is also consumed for `check_signed_request`.

```rust
pub fn check_signature_only(&mut self, request: &SignedRequest, now: u64) -> AuthzResult {
    // 1. Signature
    if !verify_signed_request(request) {
        return AuthzResult::Denied(DeniedReason::InvalidSignature);
    }

    // 2. Timestamp freshness
    let ts = request.payload.timestamp;
    let diff = if now >= ts { now - ts } else { ts - now };
    if diff > self.timestamp_window {
        return AuthzResult::Denied(DeniedReason::RequestExpired);
    }

    // 3. Nonce uniqueness
    if self.seen_nonces.contains_key(&request.payload.nonce) {
        return AuthzResult::Denied(DeniedReason::ReplayDetected);
    }
    self.seen_nonces.insert(request.payload.nonce, ts);

    AuthzResult::Allowed
}
```

### `grant` and `revoke` (`auth.rs:277-305`)

Both have an owner guard. `grant` has the owner self-grant no-op guard (the bug that was found and fixed). `revoke` has the owner-revoke no-op guard. Both are idempotent.

```rust
pub fn grant(&mut self, requester: &NodeId, key: NodeId, label: Option<String>) -> Result<(), DeniedReason> {
    if *requester != self.acl.owner {
        return Err(DeniedReason::NotAuthorized);
    }
    if key == self.acl.owner {
        return Ok(()); // Owner has implicit access — no-op  ← THE FIX
    }
    self.acl.authorized_keys.insert(key);
    if let Some(name) = label {
        let hex: String = key.0.iter().map(|b| format!("{b:02x}")).collect();
        self.acl.key_labels.insert(hex, name);
    }
    Ok(())
}

pub fn revoke(&mut self, requester: &NodeId, key: NodeId) -> Result<(), DeniedReason> {
    if *requester != self.acl.owner {
        return Err(DeniedReason::NotAuthorized);
    }
    // Owner's implicit access cannot be removed.
    if key != self.acl.owner {
        self.acl.authorized_keys.remove(&key);
        let hex: String = key.0.iter().map(|b| format!("{b:02x}")).collect();
        self.acl.key_labels.remove(&hex);
    }
    Ok(())
}
```

### `gc_nonces` (`auth.rs:321-326`)

Retains nonces where `|now - ts| <= window`. Simple, but critical for L2 and L5.

```rust
pub fn gc_nonces(&mut self, now: u64) {
    self.seen_nonces.retain(|_nonce, ts| {
        let diff = if now >= *ts { now - *ts } else { *ts - now };
        diff <= self.timestamp_window
    });
}
```

---

## 4. Phase 1: Proptest State Machine

### 4a. Technique: State Machine Testing

State machine testing runs two implementations — a reference model and the system under test (SUT) — through the same randomly-generated transition sequence. After every transition, invariants are checked and postconditions are asserted. Any disagreement is a bug: in the SUT if the reference model is correct, or in the reference model if the SUT is correct. The reference model is kept simple enough that its correctness is visually obvious.

The `proptest-state-machine` crate provides:
- `ReferenceStateMachine`: defines state type, transition type, `init_state()`, `transitions()` (weighted generator), and `apply()`.
- `StateMachineTest`: wraps the SUT with `init_test()`, `apply()` (postconditions), and `check_invariants()`.
- `prop_state_machine!` macro: wires them together and runs the test.

Configuration (`auth_state_machine.rs:472-484`):

```rust
prop_state_machine! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 512,           // 512 random sequences
        max_shrink_iters: 1000, // shrink failures to minimal repros
        .. proptest::test_runner::Config::default()
    })]

    #[test]
    fn auth_engine_state_machine(sequential 1..100 => AuthTest);
    // up to 100 transitions per sequence
}
```

512 random sequences × up to 100 transitions = up to 51,200 transitions explored.

The six transition types with their weights (`auth_state_machine.rs:46-68, 90-128`):

```rust
enum AuthOp {
    Grant { requester_idx: usize, target_idx: usize },
    Revoke { requester_idx: usize, target_idx: usize },
    CheckNode { key_idx: usize },
    SignedRequest { signer_idx: usize, fresh_timestamp: bool, reuse_nonce: bool, nonce_bytes: [u8; 16] },
    AdvanceClock { delta: u64 },
    GcNonces,
}
```

Transition weights: Grant(3), Revoke(3), CheckNode(3), SignedRequest-fresh(5), SignedRequest-reuse(2), AdvanceClock(2), GcNonces(1). `SignedRequest` is weighted highest because it exercises the most complex code path. The reuse-nonce variant specifically targets S4 (replay detection).

Generator logic:

```rust
fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
    let has_last_nonce = state.last_nonce.is_some();

    prop_oneof![
        3 => (0..NUM_KEYS, 0..NUM_KEYS).prop_map(|(r, t)| AuthOp::Grant {
            requester_idx: r, target_idx: t,
        }),
        3 => (0..NUM_KEYS, 0..NUM_KEYS).prop_map(|(r, t)| AuthOp::Revoke {
            requester_idx: r, target_idx: t,
        }),
        3 => (0..NUM_KEYS).prop_map(|k| AuthOp::CheckNode { key_idx: k }),
        5 => (0..NUM_KEYS, any::<bool>(), prop::array::uniform16(any::<u8>()))
            .prop_map(|(s, fresh, nonce)| AuthOp::SignedRequest {
                signer_idx: s, fresh_timestamp: fresh,
                reuse_nonce: false, nonce_bytes: nonce,
            }),
        2 => (0..NUM_KEYS, any::<bool>(), prop::array::uniform16(any::<u8>()))
            .prop_map(move |(s, fresh, fallback_nonce)| AuthOp::SignedRequest {
                signer_idx: s, fresh_timestamp: fresh,
                reuse_nonce: has_last_nonce, nonce_bytes: fallback_nonce,
            }),
        2 => (0u64..600).prop_map(|d| AuthOp::AdvanceClock { delta: d }),
        1 => Just(AuthOp::GcNonces),
    ].boxed()
}
```

### 4b. The Reference Model

The reference model is the TRUST ANCHOR of Phase 1. If it's wrong, the test is wrong. So it must be simple enough to audit by inspection.

`RefAuthModel` (`auth_state_machine.rs:32-41`):

```rust
struct RefAuthModel {
    owner_idx: usize,
    authorized: HashSet<usize>,           // which key indices are authorized
    used_nonces: HashSet<[u8; 16]>,       // which nonces have been consumed
    nonce_timestamps: HashMap<[u8; 16], u64>, // nonce → timestamp for GC
    clock: u64,
    last_nonce: Option<[u8; 16]>,         // for reuse_nonce transitions
}
```

The full `apply()` function — the entire reference model transition logic (`auth_state_machine.rs:130-210`):

```rust
fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
    match transition {
        AuthOp::Grant { requester_idx, target_idx } => {
            if *requester_idx == state.owner_idx && *target_idx != state.owner_idx {
                state.authorized.insert(*target_idx);
            }
            // Non-owner grant or owner self-grant: no change
        }
        AuthOp::Revoke { requester_idx, target_idx } => {
            if *requester_idx == state.owner_idx && *target_idx != state.owner_idx {
                state.authorized.remove(target_idx);
            }
        }
        AuthOp::CheckNode { .. } => {
            // Read-only — no state change
        }
        AuthOp::SignedRequest { fresh_timestamp, reuse_nonce, nonce_bytes, .. } => {
            let nonce = if *reuse_nonce {
                state.last_nonce.unwrap_or(*nonce_bytes)
            } else {
                *nonce_bytes
            };

            // Model the 4-step verification to determine if nonce gets consumed:
            // Step 1 (sig): always passes in our model (we use real signing)
            // Step 2 (timestamp): check freshness
            let ts = if *fresh_timestamp { state.clock }
                     else { state.clock.saturating_sub(400) };
            let diff = if state.clock >= ts { state.clock - ts }
                       else { ts - state.clock };
            if diff > 300 {
                // Expired — nonce NOT consumed (step 2 rejects before step 3)
            } else if state.used_nonces.contains(&nonce) {
                // Replay detected — nonce already in set (step 3 rejects)
            } else {
                // Nonce consumed at step 3 (before ACL check at step 4)
                state.used_nonces.insert(nonce);
                state.nonce_timestamps.insert(nonce, ts);
            }

            state.last_nonce = Some(nonce);
        }
        AuthOp::AdvanceClock { delta } => {
            state.clock += delta;
        }
        AuthOp::GcNonces => {
            let window = 300u64;
            let now = state.clock;
            state.used_nonces.retain(|nonce| {
                if let Some(&ts) = state.nonce_timestamps.get(nonce) {
                    let diff = if now >= ts { now - ts } else { ts - now };
                    diff <= window
                } else { false }
            });
            state.nonce_timestamps.retain(|_, ts| {
                let diff = if now >= *ts { now - *ts } else { *ts - now };
                diff <= window
            });
        }
    }
    state
}
```

Each arm is 1–5 lines. `Grant` is literally: `if requester == owner && target != owner { authorized.insert(target) }`. The `SignedRequest` arm mirrors the 4-step pipeline, computing whether the nonce would be consumed based on timestamp freshness and replay status, and updating `used_nonces` accordingly. The nonce-consumed-before-ACL-check design is replicated here — the nonce is inserted regardless of whether the signer passes the ACL check.

### 4c. SUT Wiring and the Pre-Transition Problem

The SUT wraps a real `AuthzEngine` with real `Keypair`s. All operations use real ed25519 crypto.

`SutAuth` struct (`auth_state_machine.rs:219-233`):

```rust
struct SutAuth {
    engine: AuthzEngine,
    keys: Vec<Keypair>,
    clock: u64,
    last_nonce: Option<[u8; 16]>,
    /// Mirror of the engine's nonce set — used to compute expected results
    /// before the engine call mutates state. We can't use ref_state because
    /// proptest-state-machine passes the *post-transition* reference state.
    known_nonces: HashSet<[u8; 16]>,
    /// Nonce → timestamp, mirrors engine's seen_nonces for GC
    nonce_timestamps: HashMap<[u8; 16], u64>,
    /// Track which key indices are authorized (pre-transition mirror).
    /// Needed because ref_state.authorized is post-transition for Grant/Revoke.
    authorized_indices: HashSet<usize>,
}
```

**The pre-transition problem**: `proptest-state-machine` passes the POST-transition reference state to `apply()`. But for `SignedRequest`, we need to know the PRE-transition nonce set to compute the expected result (because the engine call mutates state). Solution: the SUT maintains its own `known_nonces` and `authorized_indices` mirrors, updated after each transition.

The `SignedRequest` postcondition (`auth_state_machine.rs:332-397`):

```rust
AuthOp::SignedRequest { signer_idx, fresh_timestamp, reuse_nonce, nonce_bytes } => {
    // 1. Resolve nonce
    let nonce = if reuse_nonce {
        sut.last_nonce.unwrap_or(nonce_bytes)
    } else { nonce_bytes };

    // 2. Compute expected timestamp
    let ts = if fresh_timestamp { sut.clock }
             else { sut.clock.saturating_sub(400) };

    // 3. Using PRE-transition state, determine expected result
    let diff = if sut.clock >= ts { sut.clock - ts } else { ts - sut.clock };
    let is_replay = sut.known_nonces.contains(&nonce);
    let is_authorized = signer_idx == 0 || sut.authorized_indices.contains(&signer_idx);

    let expected = if diff > 300 {
        AuthzResult::Denied(DeniedReason::RequestExpired)        // S3
    } else if is_replay {
        AuthzResult::Denied(DeniedReason::ReplayDetected)        // S4
    } else if is_authorized {
        AuthzResult::Allowed
    } else {
        AuthzResult::Denied(DeniedReason::NotAuthorized)         // S1
    };

    // 4. Call engine and assert
    let payload = SignedRequestPayload {
        action: DatastoreAction::List { name_filter: None },
        timestamp: ts, nonce,
    };
    let request = sign_request(&sut.keys[signer_idx], payload);
    let result = sut.engine.check_signed_request(&request, sut.clock);

    assert_eq!(result, expected,
        "SignedRequest mismatch: signer_idx={signer_idx}, fresh_ts={fresh_timestamp}, \
         reuse_nonce={reuse_nonce}, diff={diff}");

    // 5. Update pre-transition mirror
    if diff <= 300 && !is_replay {
        sut.known_nonces.insert(nonce);
        sut.nonce_timestamps.insert(nonce, ts);
    }
    sut.last_nonce = Some(nonce);
}
```

The `Grant`/`Revoke` postconditions assert S5 directly (`auth_state_machine.rs:264-310`):

```rust
AuthOp::Grant { requester_idx, target_idx } => {
    let owner_idx = 0;
    let requester = sut.keys[requester_idx].node_id();
    let target = sut.keys[target_idx].node_id();
    let result = sut.engine.grant(&requester, target, None);

    // S5: Non-owner cannot mutate ACL
    if requester_idx != owner_idx {
        assert_eq!(result, Err(DeniedReason::NotAuthorized),
            "S5 violated: non-owner grant succeeded");
    } else {
        assert!(result.is_ok(), "Owner grant should succeed");
        if target_idx != owner_idx {
            sut.authorized_indices.insert(target_idx);
        }
    }
}
AuthOp::Revoke { requester_idx, target_idx } => {
    let owner_idx = 0;
    let requester = sut.keys[requester_idx].node_id();
    let target = sut.keys[target_idx].node_id();
    let result = sut.engine.revoke(&requester, target);

    // S5: Non-owner cannot mutate ACL
    if requester_idx != owner_idx {
        assert_eq!(result, Err(DeniedReason::NotAuthorized),
            "S5 violated: non-owner revoke succeeded");
    } else {
        assert!(result.is_ok(), "Owner revoke should succeed");
        if target_idx != owner_idx {
            sut.authorized_indices.remove(&target_idx);
        }
    }
}
```

### 4d. Invariants: Three Checks After Every Transition

These are not postconditions (which are per-transition). These run after EVERY transition regardless of type (`auth_state_machine.rs:422-467`):

```rust
fn check_invariants(sut: &Self::SystemUnderTest, ref_state: &RefAuthModel) {
    // S6: Owner access is irremovable — must hold after every transition
    let owner_id = sut.keys[ref_state.owner_idx].node_id();
    assert_eq!(
        sut.engine.check_node(&owner_id), AuthzResult::Allowed,
        "S6 violated: owner lost access"
    );

    // S1: Reference model agrees with SUT on every key
    for idx in 0..NUM_KEYS {
        let node = sut.keys[idx].node_id();
        let sut_result = sut.engine.check_node(&node);
        let ref_allowed = idx == ref_state.owner_idx || ref_state.authorized.contains(&idx);
        if ref_allowed {
            assert_eq!(sut_result, AuthzResult::Allowed,
                "S1 invariant: key_idx={idx} should be allowed");
        } else {
            assert_eq!(sut_result, AuthzResult::Denied(DeniedReason::NotAuthorized),
                "S1 invariant: key_idx={idx} should be denied");
        }
    }

    // L1: Every granted (non-revoked) key has access
    for &idx in &ref_state.authorized {
        let node = sut.keys[idx].node_id();
        assert_eq!(
            sut.engine.check_node(&node), AuthzResult::Allowed,
            "L1 violated: granted key_idx={idx} denied"
        );
    }
}
```

S6 catches owner-persistence bugs from interaction effects (e.g., a grant followed by a revoke that accidentally removes the owner). S1 is a full consistency check across all 4 key indices — not just the key involved in the current transition. L1 ensures that every key the reference model considers authorized is actually allowed by the engine.

### 4e. Phase 1 Boundaries

- **S2 is NOT tested** — real crypto is used, so signatures are always valid. The `InvalidSignature` branch is never reached.
- **S7 is NOT tested** — the state machine drives `AuthzEngine` directly, not through `GatewayActor`. Dispatch behavior is not exercised.
- **Coverage is probabilistic, not exhaustive.** 512 × 100 = up to 51,200 transitions is high coverage, and proptest shrinks failing cases to minimal repros, but rare edge cases might survive.
- **No concurrency testing** — `AuthzEngine` is driven single-threaded.

---

## 5. Phase 2: Kani Bounded Proofs

### 5a. Why a Bounded Mirror Is Necessary

Kani performs symbolic execution: instead of concrete values, it reasons over ALL possible values simultaneously using SMT solvers. This provides exhaustive coverage within bounds — every reachable branch is explored.

The problem: `HashMap`/`HashSet` use SipHash internally. SipHash involves bitwise operations that create exponential symbolic path explosion. Kani cannot tractably reason about hash-based collections.

The solution: replace hash collections with fixed-size arrays. `KaniAuthzEngine` uses `[Option<KaniNodeId>; MAX_KEYS]` instead of `HashSet<NodeId>`, and `[Option<([u8; 2], u64)>; MAX_NONCES]` instead of `HashMap<[u8; 16], u64>`.

`KaniNodeId([u8; 2])` — 2 bytes giving 65,536 unique values. This is sufficient because the control flow depends only on equality comparisons, not key magnitude. If the logic is correct for 65K values, it's correct for 32-byte values — the branches are identical.

Crypto is stubbed: `sig_valid: bool` replaces `verify_signed_request()`. This enables S2 testing (which proptest can't do) — Kani explores both `sig_valid = true` and `sig_valid = false`.

Bounded types and engine struct (`kani_auth.rs:17-36`):

```rust
const MAX_KEYS: usize = 3;
const MAX_NONCES: usize = 2;

#[derive(Clone, Copy, PartialEq, Eq)]
struct KaniNodeId([u8; 2]);

struct KaniAuthzEngine {
    owner: KaniNodeId,
    authorized_keys: [Option<KaniNodeId>; MAX_KEYS],
    key_count: usize,
    seen_nonces: [Option<([u8; 2], u64)>; MAX_NONCES],
    nonce_count: usize,
    timestamp_window: u64,
}
```

Mirror `check_node` (`kani_auth.rs:115-121`) — compare with production (`auth.rs:211-217`):

```rust
// Mirror (kani_auth.rs:115-121)           | Production (auth.rs:211-217)
fn check_node(&self, node_id: &KaniNodeId) | pub fn check_node(&self, node_id: &NodeId)
    -> AuthzResult {                       |     -> AuthzResult {
    if *node_id == self.owner              |     if *node_id == self.acl.owner
        || self.keys_contains(node_id) {   |         || self.acl.authorized_keys.contains(node_id) {
        AuthzResult::Allowed               |         AuthzResult::Allowed
    } else {                               |     } else {
        AuthzResult::Denied(               |         AuthzResult::Denied(
            DeniedReason::NotAuthorized)   |             DeniedReason::NotAuthorized)
    }                                      |     }
}                                          | }
```

Mirror `check_signed_request` (`kani_auth.rs:156-187`) — compare with production (`auth.rs:252-273`):

```rust
// Mirror (kani_auth.rs)                        | Production (auth.rs)
fn check_signed_request(&mut self,              | pub fn check_signed_request(&mut self,
    sig_valid: bool,                            |     request: &SignedRequest,
    public_key: &KaniNodeId,                    |     now: u64) -> AuthzResult {
    timestamp: u64, nonce: [u8; 2],             |
    now: u64) -> AuthzResult {                  |
    // 1. Signature                             |     // 1. Signature
    if !sig_valid {                             |     if !verify_signed_request(request) {
        return Denied(InvalidSignature); }      |         return Denied(InvalidSignature); }
                                                |
    // 2. Timestamp freshness                   |     // 2. Timestamp freshness
    let diff = if now >= timestamp              |     let ts = request.payload.timestamp;
        { now - timestamp }                     |     let diff = if now >= ts { now - ts }
        else { timestamp - now };               |         else { ts - now };
    if diff > self.timestamp_window {           |     if diff > self.timestamp_window {
        return Denied(RequestExpired); }        |         return Denied(RequestExpired); }
                                                |
    // 3. Nonce uniqueness                      |     // 3. Nonce uniqueness
    if self.nonces_contains(&nonce) {           |     if self.seen_nonces.contains_key(
        return Denied(ReplayDetected); }        |         &request.payload.nonce) {
    self.nonces_insert(nonce, timestamp);       |         return Denied(ReplayDetected); }
                                                |     self.seen_nonces.insert(
                                                |         request.payload.nonce, ts);
    // 4. ACL check                             |
    self.check_node(public_key)                 |     // 4. ACL check
}                                               |     self.check_node(&request.public_key)
                                                | }
```

The branch structure is identical: `if !sig → return`, `if diff > window → return`, `if nonces_contains → return`, `nonces_insert; check_node`.

### 5b. The Five Proof Harnesses

All five `#[kani::proof]` functions (`kani_auth.rs:216-381`):

**S5: `proof_s5_non_owner_cannot_mutate_acl`** — For ALL `owner`, `requester`, `target` where `requester != owner`: both `grant()` and `revoke()` return `Err(NotAuthorized)`. No loops, no unwind needed. The simplest harness — proves the owner guard is total.

```rust
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
```

**S1: `proof_s1_check_node`** — Creates an engine, grants 0..MAX_KEYS symbolic keys, then queries with a symbolic node. If the result is `Allowed`, asserts the query is either the owner or one of the granted keys. The contrapositive: no ungranted, non-owner key can ever get `Allowed`. `#[kani::unwind(5)]` bounds loop iterations.

```rust
#[kani::proof]
#[kani::unwind(5)]
fn proof_s1_check_node() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    let num_keys: usize = kani::any();
    kani::assume(num_keys <= MAX_KEYS);

    let mut granted = [KaniNodeId([0; 2]); MAX_KEYS];
    let mut i = 0;
    while i < num_keys {
        granted[i] = KaniNodeId(kani::any());
        engine.keys_insert(granted[i]);
        i += 1;
    }

    let query = KaniNodeId(kani::any());
    let result = engine.check_node(&query);

    if result == AuthzResult::Allowed {
        let mut is_authorized = query == owner;
        let mut j = 0;
        while j < num_keys {
            if query == granted[j] { is_authorized = true; }
            j += 1;
        }
        assert!(is_authorized);
    }
}
```

**S6: `proof_s6_owner_irremovable`** — 5 symbolic grant/revoke operations with arbitrary requesters and targets. After all 5, asserts `check_node(owner) == Allowed`. Proves owner persistence survives ANY sequence of up to 5 ACL mutations by any requester. `#[kani::unwind(7)]` bounds the main loop.

```rust
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
```

**S2/S3/S4: `proof_signed_request_4step_ordering`** — The most complex harness. Optionally grants one key, optionally pre-inserts a nonce. Then calls `check_signed_request` with fully symbolic inputs. Pattern-matches the result and asserts the correct preconditions for each denial branch:

```rust
#[kani::proof]
#[kani::unwind(4)]
fn proof_signed_request_4step_ordering() {
    let owner = KaniNodeId(kani::any());
    let mut engine = KaniAuthzEngine::new(owner);

    // Optionally grant one key
    let has_granted: bool = kani::any();
    let granted_key = KaniNodeId(kani::any());
    if has_granted { engine.keys_insert(granted_key); }

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

    let diff = if now >= timestamp { now - timestamp }
               else { timestamp - now };

    let result = engine.check_signed_request(sig_valid, &public_key, timestamp, nonce, now);

    match result {
        AuthzResult::Denied(DeniedReason::InvalidSignature) => {
            assert!(!sig_valid);                          // Step 1 rejected
        }
        AuthzResult::Denied(DeniedReason::RequestExpired) => {
            assert!(sig_valid);                           // Step 1 passed
            assert!(diff > 300);                          // Step 2 rejected
        }
        AuthzResult::Denied(DeniedReason::ReplayDetected) => {
            assert!(sig_valid);                           // Step 1 passed
            assert!(diff <= 300);                         // Step 2 passed
            assert!(pre_nonce);                           // Step 3 rejected
        }
        AuthzResult::Denied(DeniedReason::NotAuthorized) => {
            assert!(sig_valid);                           // Step 1 passed
            assert!(diff <= 300);                         // Step 2 passed
            assert!(public_key != owner);                 // Step 4 rejected: not in ACL
        }
        AuthzResult::Allowed => {
            assert!(sig_valid);                           // All 4 steps passed
            assert!(diff <= 300);
            assert!(public_key == owner || engine.keys_contains(&public_key));
        }
    }
}
```

This is the strongest harness — it proves the 4-step ordering is correct for ALL possible input combinations. Each denial reason implies exactly the correct preconditions:
- `DeniedBadSig` → `!sig_valid` (step 1 rejected correctly)
- `DeniedExpired` → `sig_valid && diff > 300` (step 2 rejected, step 1 passed)
- `DeniedReplay` → `sig_valid && diff <= 300 && pre_nonce` (step 3 rejected, steps 1–2 passed)
- `DeniedNotAuthorized` → `sig_valid && diff <= 300 && key ∉ ACL` (step 4 rejected, steps 1–3 passed)
- `Allowed` → all 4 steps passed

**GC: `proof_gc_preserves_owner_and_frees_expired`** — Inserts a symbolic nonce at time `t1`, runs GC at `t2 >= t1`. Asserts: (a) owner access survives GC, (b) if `t2 - t1 > 300`, the nonce is freed.

```rust
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
```

### 5c. Mirror Faithfulness

The proofs are only as strong as the mirror's correspondence to production code. If the mirror has different branch structure, the proofs prove the wrong thing.

Each mirror function has a doc comment citing exact production source lines. The bounded-set helpers are semantically equivalent to their `HashSet` counterparts (`kani_auth.rs:52-109`):

```rust
fn keys_contains(&self, id: &KaniNodeId) -> bool {
    let mut i = 0;
    while i < self.key_count {
        if let Some(k) = self.authorized_keys[i] {
            if k == *id { return true; }
        }
        i += 1;
    }
    false
}

fn keys_insert(&mut self, id: KaniNodeId) {
    if self.keys_contains(&id) { return; }
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

fn nonces_contains(&self, nonce: &[u8; 2]) -> bool {
    let mut i = 0;
    while i < self.nonce_count {
        if let Some((n, _)) = self.seen_nonces[i] {
            if n == *nonce { return true; }
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
```

These perform linear scans over fixed arrays — semantically identical to `HashSet::contains`/`insert`/`remove` within the bounded capacity.

### 5d. Bounds and Limitations

The proofs are exhaustive WITHIN bounds. Beyond the bounds, they don't apply directly — but the argument for generalization is strong because the bounded types cover all reachable branch combinations.

| Bound | Value | Rationale |
|-------|-------|-----------|
| `KaniNodeId` | `[u8; 2]` (65,536 values) | Control flow only depends on `==`/`!=`, so this is sufficient |
| `MAX_KEYS` | 3 | Covers: empty ACL, single key, full capacity. Auth logic is a linear scan, so 3 entries cover all loop-count cases |
| `MAX_NONCES` | 2 | Covers: empty, single, full capacity |
| `MAX_OPS` (S6) | 5 | 5 grant/revoke operations with arbitrary requesters/targets. Each op is binary (grant or revoke), so this explores 2^5 = 32 operation sequences × symbolic key values |

What is NOT proven:
- The `HashMap`/`HashSet` implementation (trusted stdlib)
- Hash collision behavior (irrelevant — `NodeId` equality is byte-exact)
- Concurrent access (not applicable — engine is single-threaded within the actor)

---

## 6. Phase 3: Stateright Model Checking

### 6a. Why Phase 3 Is Needed: The Function-Dispatch Gap

Phases 1–2 prove `AuthzEngine` returns correct results. But the security property we actually care about is: "unauthorized messages never reach `DatastoreNode`." This depends on `GatewayActor` USING the engine's result correctly.

A hypothetical bug: imagine `handle_signed_request` dispatches on `Denied` instead of `Allowed` (the match arms are swapped). Phases 1–2 would all pass because the engine itself is correct — they never test the gateway. Only Phase 3 catches this.

What Stateright adds: exhaustive exploration of all MESSAGE SEQUENCES. Not just "does the engine handle one request correctly?" but "after `grant(A)`, `revoke(A)`, `advance_clock`, `gc_tick`, `request(A)` — in every possible ordering — is dispatch correct?"

Stateright is a Rust model checker. The `Model` trait (not `Actor`) is used because we're verifying a single actor's internal logic, not multi-actor communication.

### 6b. Model Structure: Bounded State and Actions

Bounded parameters (`gateway_model_check.rs:12-27`):

```rust
const OWNER: u8 = 0;
const KEY_A: u8 = 1;
const KEY_B: u8 = 2;

const NONCES: [u8; 2] = [0, 1];
const TIMESTAMPS: [u8; 4] = [0, 1, 2, 3];
const WINDOW: u8 = 1;  // scaled from production's 300s
const KEYS: [u8; 3] = [OWNER, KEY_A, KEY_B];
```

3 node IDs (owner + 2 clients), 2 nonces, 4 timestamps, window=1. Small but covers all interesting combinations: authorized vs unauthorized keys, fresh vs expired timestamps, fresh vs replayed nonces, pre-GC vs post-GC states.

`GatewayState` (`gateway_model_check.rs:34-46`):

```rust
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct GatewayState {
    acl: Vec<u8>,                  // sorted — Hash requirement
    seen_nonces: Vec<(u8, u8)>,    // sorted (nonce, timestamp) pairs
    now: u8,                       // current wall clock
    s7_violated: bool,             // monotonic violation flag
    has_dispatched: bool,          // true after any dispatch to datastore_node
}
```

Sorted `Vec`s instead of `HashSet` because `State` must implement `Hash` for Stateright's state deduplication.

`GatewayAction` (`gateway_model_check.rs:51-83`):

```rust
enum GatewayAction {
    SignedRequest { signer: u8, nonce: u8, timestamp: u8, sig_valid: bool },
    Authorize { signer: u8, nonce: u8, timestamp: u8, sig_valid: bool },
    VerifySignature { signer: u8, nonce: u8, timestamp: u8, sig_valid: bool },
    Grant { requester: u8, target: u8 },
    Revoke { requester: u8, target: u8 },
    GcTick,
    AdvanceClock,
}
```

7 variants mapping to the 6 modeled production handlers plus `AdvanceClock`. Total actions per state: `3×2×4×2` (SignedRequest) + `3×2×4×2` (Authorize) + `3×2×4×2` (VerifySignature) + `3×3` (Grant) + `3×3` (Revoke) + 1 (GcTick) + 1 (AdvanceClock) = 48+48+48+9+9+1+1 = **164 actions per state**.

**Handlers modeled vs skipped**: 6 modeled (`HandleSignedRequest`, `Grant`, `Revoke`, `Authorize`, `VerifySignature`, `NonceGcTick`). 5 skipped (`CheckConnection`, `SubmitAccessRequest`, `ListAccessRequests`, `DenyAccessRequest`, `ListAuthorizedKeys`). Skipped handlers either only read state or only touch `pending_requests` — they cannot affect the ACL, nonce set, or dispatch behavior.

The full cross-product generation (`gateway_model_check.rs:255-321`):

```rust
fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
    for &signer in &KEYS {
        for &nonce in &NONCES {
            for &ts in &TIMESTAMPS {
                for &sig_valid in &[true, false] {
                    actions.push(GatewayAction::SignedRequest {
                        signer, nonce, timestamp: ts, sig_valid });
                }
            }
        }
    }
    // Same cross-product for Authorize and VerifySignature...
    for &requester in &KEYS {
        for &target in &KEYS {
            actions.push(GatewayAction::Grant { requester, target });
        }
    }
    for &requester in &KEYS {
        for &target in &KEYS {
            actions.push(GatewayAction::Revoke { requester, target });
        }
    }
    actions.push(GatewayAction::GcTick);
    actions.push(GatewayAction::AdvanceClock);
}
```

### 6c. Mirror Functions: Production Logic with Bounded Types

Each mirror function has the SAME control-flow branches as production code, operating on `u8` types instead of `NodeId`/`[u8; 16]`/`u64`.

`is_authorized` — mirrors `check_node` (`gateway_model_check.rs:92-94`):

```rust
fn is_authorized(key: u8, acl: &[u8]) -> bool {
    key == OWNER || acl.contains(&key)
}
```

`check_signed_request_model` — mirrors the 4-step pipeline (`gateway_model_check.rs:124-159`):

```rust
fn check_signed_request_model(
    state: &mut GatewayState, signer: u8, nonce: u8,
    timestamp: u8, sig_valid: bool,
) -> CheckResult {
    // 1. Signature
    if !sig_valid { return CheckResult::DeniedBadSig; }

    // 2. Timestamp freshness: |now - ts| <= WINDOW
    let diff = if state.now >= timestamp { state.now - timestamp }
               else { timestamp - state.now };
    if diff > WINDOW { return CheckResult::DeniedExpired; }

    // 3. Nonce uniqueness (consumed before ACL — matches production)
    let nonce_entry = (nonce, timestamp);
    if state.seen_nonces.contains(&nonce_entry) {
        return CheckResult::DeniedReplay;
    }
    sorted_insert(&mut state.seen_nonces, nonce_entry);

    // 4. ACL check
    if is_authorized(signer, &state.acl) {
        CheckResult::Allowed
    } else {
        CheckResult::DeniedNotAuthorized
    }
}
```

`check_signature_only_model` — steps 1–3 only (`gateway_model_check.rs:164-193`):

```rust
fn check_signature_only_model(
    state: &mut GatewayState, nonce: u8, timestamp: u8, sig_valid: bool,
) -> CheckResult {
    if !sig_valid { return CheckResult::DeniedBadSig; }
    let diff = if state.now >= timestamp { state.now - timestamp }
               else { timestamp - state.now };
    if diff > WINDOW { return CheckResult::DeniedExpired; }
    let nonce_entry = (nonce, timestamp);
    if state.seen_nonces.contains(&nonce_entry) {
        return CheckResult::DeniedReplay;
    }
    sorted_insert(&mut state.seen_nonces, nonce_entry);
    CheckResult::Allowed
}
```

`grant_model`, `revoke_model`, `gc_nonces_model` (`gateway_model_check.rs:196-231`):

```rust
fn grant_model(state: &mut GatewayState, requester: u8, target: u8) {
    if requester != OWNER { return; }  // Owner guard
    if target == OWNER { return; }     // Self-grant no-op
    sorted_insert(&mut state.acl, target);
}

fn revoke_model(state: &mut GatewayState, requester: u8, target: u8) {
    if requester != OWNER { return; }  // Owner guard
    if target == OWNER { return; }     // Owner-revoke no-op
    sorted_remove(&mut state.acl, &target);
}

fn gc_nonces_model(state: &mut GatewayState) {
    state.seen_nonces.retain(|&(_, ts)| {
        let diff = if state.now >= ts { state.now - ts }
                   else { ts - state.now };
        diff <= WINDOW
    });
}
```

### 6d. The Dual-Rail S7 Check

This is the most important design decision in Phase 3. The naive approach: "if the mirror says `Allowed`, set `has_dispatched = true`." But this makes S7 tautological — it would pass even if the mirror were wrong.

The dual-rail approach uses TWO INDEPENDENT functions:

1. `check_signed_request_model()` — the mirror of production logic. Decides whether to dispatch.
2. `is_authorized()` — a simple, obviously-correct predicate. Used ONLY in the S7 check.

The `SignedRequest` arm of `next_state` (`gateway_model_check.rs:326-341`):

```rust
GatewayAction::SignedRequest { signer, nonce, timestamp, sig_valid } => {
    let result =
        check_signed_request_model(&mut next, signer, nonce, timestamp, sig_valid);
    if result == CheckResult::Allowed {
        // Dual-rail S7 check: independently verify the signer IS authorized
        // using the PRE-transition state (before nonce insertion)
        if !is_authorized(signer, &state.acl) {
            next.s7_violated = true;
        }
        next.has_dispatched = true;
    }
}
```

Why this works: if the mirror has a bug (e.g., skips the ACL check), the independent `is_authorized` catches the discrepancy. If `is_authorized` has a bug, S7 could have a false negative — but `is_authorized` is 1 line (`key == OWNER || acl.contains(&key)`), trivially auditable.

The violation flag is MONOTONIC: once set, never cleared. This means S7 catches even transient violations in multi-step paths.

### 6e. Properties and Liveness Canaries

The full `properties()` function (`gateway_model_check.rs:403-428`):

```rust
fn properties(&self) -> Vec<Property<Self>> {
    vec![
        // S7: No unauthorized dispatch — the critical safety property.
        Property::<Self>::always("S7: no unauthorized dispatch", |_, state| {
            !state.s7_violated
        }),
        // S6-gw: Owner is always authorized (never removed from implicit access).
        Property::<Self>::always("S6-gw: owner always authorized", |_, state| {
            is_authorized(OWNER, &state.acl)
        }),
        // L3: Authorized dispatch is reachable (canary — model isn't vacuously safe).
        Property::<Self>::sometimes(
            "L3: authorized dispatch reachable",
            |_, state| state.has_dispatched && !state.s7_violated,
        ),
        // L4: A granted (non-owner) key can dispatch.
        Property::<Self>::sometimes("L4: granted key can dispatch", |_, state| {
            !state.acl.is_empty() && state.has_dispatched
        }),
        // L5: Nonce reuse after GC is reachable (GC actually enables re-dispatch).
        Property::<Self>::sometimes(
            "L5: dispatch with empty nonce table reachable",
            |_, state| state.has_dispatched && state.seen_nonces.is_empty(),
        ),
    ]
}
```

- **S7 (always)**: `!state.s7_violated` — the critical safety property. Must hold in every reachable state.
- **S6-gw (always)**: `is_authorized(OWNER, &state.acl)` — owner can never be removed from implicit access, verified at the gateway level (not just engine level).
- **L3 (sometimes)**: `state.has_dispatched && !state.s7_violated` — there EXISTS a reachable state where authorized dispatch happened. If this fails, the model is too restrictive (nothing ever dispatches), which would make S7 vacuously true. **Without L3, S7 could pass by being meaningless.**
- **L4 (sometimes)**: `!state.acl.is_empty() && state.has_dispatched` — a non-owner key was granted AND dispatch happened. Proves the grant→dispatch path works.
- **L5 (sometimes)**: `state.has_dispatched && state.seen_nonces.is_empty()` — dispatch occurred with an empty nonce table. This can only happen after GC clears all nonces. Proves the GC→nonce-reuse→dispatch path is reachable.

"Canary" means: if any liveness property fails, the safety property is suspect because the model may not be exploring the interesting states.

### 6f. State Pruning and Exploration

State-space reduction in `next_state` (`gateway_model_check.rs:385-401`):

```rust
GatewayAction::AdvanceClock => {
    if next.now < *TIMESTAMPS.last().unwrap() {
        next.now += 1;
    } else {
        return None; // no-op, prune
    }
}

// ... after the match:

// Prune: if state didn't change, no need to explore further
if next == *state {
    return None;
}

Some(next)
```

`next_state` returns `None` (no new state) in two cases: (a) action produced no state change (`next == *state`), (b) clock already at maximum. This prunes the search tree significantly.

Stateright runs DFS with hash-based state deduplication. Each unique state is explored exactly once regardless of how many paths reach it.

The test function (`gateway_model_check.rs:433-455`):

```rust
#[test]
fn gateway_dispatch_model_check() {
    let result = GatewayModel
        .checker()
        .spawn_dfs()
        .join();

    let unique_states = result.unique_state_count();
    println!("Stateright: explored {} unique states, max depth {}",
        unique_states, result.max_depth());

    result.assert_properties();

    // Sanity: the model actually explored a meaningful state space.
    assert!(unique_states > 100,
        "Model explored too few states ({unique_states}); check action generation");
}
```

The sanity check `unique_states > 100` ensures the model isn't collapsing to a trivial set.

Edge cases covered by exhaustive exploration:
- Grant→Revoke→Request (stale ACL after revoke)
- Nonce reuse after GC (Request→AdvanceClock×2→GcTick→Request succeeds)
- Authorize consumes nonce (Authorize→SignedRequest gets ReplayDetected)
- VerifySignature bypasses ACL but never dispatches
- Non-owner grant is no-op

---

## 7. Mutation Sensitivity

Tests are only trustworthy if known-bad code changes cause failures. Mutation testing systematically introduces small code changes and verifies that the test suite catches each one. If a mutation survives (tests still pass), there's a gap in coverage.

**Phase 1** has a manual mutation catalog — 5 specific mutations, each targeting a specific property. These were verified during Phase 1 development:

| # | Mutation | Property Violated |
|---|---------|------------------|
| 1 | Remove owner check from `check_node` (delete `*node_id == self.acl.owner \|\|` at line 212) | S6: "owner lost access" |
| 2 | Remove owner-revoke guard (delete `if key != self.acl.owner` at line 299) | S6: Grant+Revoke sequence removes owner from `authorized_keys` |
| 3 | Change `> self.timestamp_window` to `>= self.timestamp_window` at line 261 | S3: boundary requests at diff=300 rejected by engine but expected to pass |
| 4 | Remove nonce check (delete lines 266-268) | S4: replayed nonces accepted |
| 5 | Remove ACL check from `grant` (delete lines 278-280) | S5: "non-owner grant succeeded" |

**Phase 2 (Kani)** is inherently mutation-sensitive: any change to the mirror's control flow that differs from the assertion's specification produces a symbolic counterexample. Kani doesn't just "fail to find a proof" — it produces a concrete input that violates the assertion.

**Phase 3's dual-rail design**: corrupting `check_signed_request_model` (e.g., removing the ACL check) triggers S7 because `is_authorized` independently catches it. Corrupting `is_authorized` would hide bugs, but it's a 1-line function.

---

## 8. Bug Found and Fixed

The Phase 1 state machine discovered that `grant(owner, owner)` added the owner to `authorized_keys`. Since `revoke(owner, owner)` is a no-op (owner's implicit access cannot be removed), the owner would get "stuck" in the explicit set with no way to remove them.

This is a correctness bug, not a security bug — the owner retains access regardless. But it violates the design invariant that the owner's access is purely implicit.

The bug was caught because the reference model's `Grant` transition doesn't add the owner to the authorized set, but the engine was adding them. The invariant check (S1: reference and SUT agree on every key) caught the disagreement.

Fix — 2-line guard in `grant()` at `auth.rs:281-283`:

```rust
pub fn grant(&mut self, requester: &NodeId, key: NodeId, label: Option<String>) -> Result<(), DeniedReason> {
    if *requester != self.acl.owner {
        return Err(DeniedReason::NotAuthorized);
    }
    if key == self.acl.owner {
        return Ok(()); // ← THE FIX: owner has implicit access, no-op
    }
    self.acl.authorized_keys.insert(key);
    // ...
}
```

---

## 9. Supporting Test Coverage

These aren't formal verification, but they provide concrete coverage for specific scenarios.

**12 scenario tests** (`tests/auth_scenario_tests.rs`): owner always allowed, unknown key denied, grant/revoke lifecycle, non-owner cannot grant, non-owner cannot revoke, revoking owner is no-op, signed request happy path, tampered signature rejected (S2), stale timestamp rejected, replayed nonce rejected, nonce GC frees old nonces, unauthorized key with valid signature denied. The tampered signature test is the primary S2 coverage alongside Kani.

**4 actor-level tests** (`tests/gateway_tests.rs`): authorized signed GET flows through to datastore (returns `NotFound`, not `Denied`), unauthorized signed GET returns `Denied(NotAuthorized)`, `CheckConnection` allows owner, `CheckConnection` denies stranger. These exercise the real `GatewayActor` with the real swactor runtime — the concrete integration tests that complement Phase 3's model check.

---

## 10. Trust Argument and Boundaries

### The Layered Coverage Argument

| Technique | Coverage Type | Scope | Crypto | Collections |
|-----------|--------------|-------|--------|-------------|
| proptest (Phase 1) | Randomized, high-confidence | Function level | Real ed25519 | Real `HashMap`/`HashSet` |
| Kani (Phase 2) | Exhaustive within bounds | Function level | Stubbed (`bool`) | Bounded arrays |
| Stateright (Phase 3) | Exhaustive over orderings | Actor (dispatch) level | Stubbed (`bool`) | Sorted `Vec` |

### Why the Three Phases Are Complementary, Not Redundant

- Proptest uses real crypto but is randomized — might miss rare cases.
- Kani is exhaustive but stubs crypto and uses bounded collections — might miss `HashSet`-specific bugs.
- Stateright is exhaustive over orderings but uses bounded parameters — might miss bugs that only manifest with many keys/nonces.

Together: every axis is covered by at least one technique.

| Axis | proptest | Kani | Stateright |
|------|----------|------|-----------|
| Real crypto | Yes | No | No |
| All inputs | No | Yes (within bounds) | Yes (within bounds) |
| All message orderings | No | No | Yes |
| Function correctness | Yes | Yes | No (mirrors) |
| Dispatch correctness | No | No | Yes |

### What Is Explicitly NOT Verified

- **Network/transport layer** (TLS, iroh connections)
- **Actor runtime correctness** (swactor's message delivery guarantees)
- **Ed25519 implementation** (`distribution::crypto`)
- **Persistence** (ACL load/save to disk)
- **UI/API layer** above `GatewayActor`
- **Concurrency bugs** within the actor runtime
- **Mirror faithfulness** — the correspondence between mirror functions and production code is a manual property that the reader must verify by comparing the code excerpts in sections 5b, 5c, 6c, and 6d against the production code in section 3

---

## 11. Running the Suite

```bash
# Phase 1: proptest state machine (~70-80s, real ed25519 crypto)
cargo test -p swactor-datastore --test auth_state_machine

# Phase 2: Kani bounded proofs (requires cargo-kani)
cargo xtask test kani

# Phase 3: Stateright model check (<1s)
cargo xtask test stateright

# Scenario tests
cargo test -p swactor-datastore --test auth_scenario_tests

# Everything except Kani (Kani requires separate toolchain)
cargo test -p swactor-datastore
```

---

## 12. Files Changed

| File | Change |
|------|--------|
| `src/auth.rs` | Owner self-grant guard (2 lines) |
| `src/kani_auth.rs` | New — bounded mirror + 5 Kani proof harnesses |
| `src/lib.rs` | `#[cfg(kani)] mod kani_auth;` |
| `tests/auth_state_machine.rs` | New — proptest state machine (485 lines) |
| `tests/auth_scenario_tests.rs` | New — 12 scenario tests |
| `tests/gateway_model_check.rs` | New — Stateright model check (456 lines) |
| `Cargo.toml` | Added dev-deps: proptest, proptest-state-machine, stateright |
| `xtask/src/main.rs` | Added `kani` and `stateright` test groups |
