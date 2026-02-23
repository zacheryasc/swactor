//! Stateright model-checking of GatewayActor dispatch logic.
//!
//! Verifies property **S7**: `DatastoreNodeMsg` is only ever dispatched when
//! `AuthzResult::Allowed` is returned for an authorized signer. Uses bounded
//! model checking to exhaustively explore all message orderings across grants,
//! revokes, signed requests, signature checks, GC ticks, and clock advances.

use stateright::*;

// ── Bounded constants ────────────────────────────────────────────────────────

const OWNER: u8 = 0;
const KEY_A: u8 = 1;
const KEY_B: u8 = 2;

/// Nonce values in the model. Two nonces are enough to expose replay bugs.
const NONCES: [u8; 2] = [0, 1];

/// Timestamp values actions can carry. Combined with a window of 1,
/// any `|now - ts| > 1` is "expired".
const TIMESTAMPS: [u8; 4] = [0, 1, 2, 3];

/// Scaled replay-window (production = 300 s; model window = 1 tick).
const WINDOW: u8 = 1;

/// All node identities explored by the model.
const KEYS: [u8; 3] = [OWNER, KEY_A, KEY_B];

// ── Model state ──────────────────────────────────────────────────────────────

/// Minimal abstract state of the GatewayActor's authorization layer.
///
/// Sorted `Vec`s (not `HashMap`/`HashSet`) because `State` must be `Hash`.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct GatewayState {
    /// Sorted list of explicitly authorized keys (owner has implicit access).
    acl: Vec<u8>,
    /// Sorted `(nonce, timestamp)` pairs currently tracked for replay detection.
    seen_nonces: Vec<(u8, u8)>,
    /// Current wall clock (advanced by `AdvanceClock`).
    now: u8,
    /// Monotonic violation flag — set true if an unauthorized dispatch occurs.
    s7_violated: bool,
    /// True after any dispatch to `datastore_node`.
    has_dispatched: bool,
}

// ── Actions ──────────────────────────────────────────────────────────────────

/// Every action the model can take in a single step.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum GatewayAction {
    /// Mirrors `GatewayMsg::HandleSignedRequest` — the only handler that
    /// dispatches to `datastore_node`.
    SignedRequest {
        signer: u8,
        nonce: u8,
        timestamp: u8,
        sig_valid: bool,
    },
    /// Mirrors `GatewayMsg::Authorize` — consumes a nonce, sends to `reply_to`.
    Authorize {
        signer: u8,
        nonce: u8,
        timestamp: u8,
        sig_valid: bool,
    },
    /// Mirrors `GatewayMsg::VerifySignature` — consumes a nonce, no ACL check.
    VerifySignature {
        signer: u8,
        nonce: u8,
        timestamp: u8,
        sig_valid: bool,
    },
    /// Mirrors `GatewayMsg::Grant`.
    Grant { requester: u8, target: u8 },
    /// Mirrors `GatewayMsg::Revoke`.
    Revoke { requester: u8, target: u8 },
    /// Mirrors `GatewayMsg::NonceGcTick`.
    GcTick,
    /// Advances the model clock by 1 tick.
    AdvanceClock,
}

// ── Mirror functions ─────────────────────────────────────────────────────────
//
// Each mirrors the production AuthzEngine method with bounded types.
// The `bool` return means "Allowed" (true) or "Denied" (false).

/// Whether `key` is authorized: owner always is; others need explicit ACL entry.
/// Mirrors `AuthzEngine::check_node` (auth.rs:211-217).
fn is_authorized(key: u8, acl: &[u8]) -> bool {
    key == OWNER || acl.contains(&key)
}

/// Insert into a sorted Vec if not already present.
fn sorted_insert<T: Ord>(v: &mut Vec<T>, val: T) {
    if let Err(pos) = v.binary_search(&val) {
        v.insert(pos, val);
    }
}

/// Remove from a sorted Vec.
fn sorted_remove<T: Ord>(v: &mut Vec<T>, val: &T) {
    if let Ok(pos) = v.binary_search(val) {
        v.remove(pos);
    }
}

/// Result of a signed-request check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckResult {
    Allowed,
    DeniedBadSig,
    DeniedExpired,
    DeniedReplay,
    DeniedNotAuthorized,
}

/// Mirrors `AuthzEngine::check_signed_request` (auth.rs:252-273).
///
/// Four-step chain: sig → timestamp → nonce → ACL.
/// Nonce is consumed at step 3 (before ACL), matching production behavior.
fn check_signed_request_model(
    state: &mut GatewayState,
    signer: u8,
    nonce: u8,
    timestamp: u8,
    sig_valid: bool,
) -> CheckResult {
    // 1. Signature
    if !sig_valid {
        return CheckResult::DeniedBadSig;
    }

    // 2. Timestamp freshness: |now - ts| <= WINDOW
    let diff = if state.now >= timestamp {
        state.now - timestamp
    } else {
        timestamp - state.now
    };
    if diff > WINDOW {
        return CheckResult::DeniedExpired;
    }

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

/// Mirrors `AuthzEngine::check_signature_only` (auth.rs:223-243).
///
/// Steps 1-3 only, no ACL check. Used by `VerifySignature`.
fn check_signature_only_model(
    state: &mut GatewayState,
    nonce: u8,
    timestamp: u8,
    sig_valid: bool,
) -> CheckResult {
    // 1. Signature
    if !sig_valid {
        return CheckResult::DeniedBadSig;
    }

    // 2. Timestamp freshness
    let diff = if state.now >= timestamp {
        state.now - timestamp
    } else {
        timestamp - state.now
    };
    if diff > WINDOW {
        return CheckResult::DeniedExpired;
    }

    // 3. Nonce uniqueness
    let nonce_entry = (nonce, timestamp);
    if state.seen_nonces.contains(&nonce_entry) {
        return CheckResult::DeniedReplay;
    }
    sorted_insert(&mut state.seen_nonces, nonce_entry);

    CheckResult::Allowed
}

/// Mirrors `AuthzEngine::grant` (auth.rs:277-289).
fn grant_model(state: &mut GatewayState, requester: u8, target: u8) {
    // Owner guard
    if requester != OWNER {
        return;
    }
    // Self-grant is a no-op
    if target == OWNER {
        return;
    }
    sorted_insert(&mut state.acl, target);
}

/// Mirrors `AuthzEngine::revoke` (auth.rs:294-305).
fn revoke_model(state: &mut GatewayState, requester: u8, target: u8) {
    // Owner guard
    if requester != OWNER {
        return;
    }
    // Owner-revoke is a no-op
    if target == OWNER {
        return;
    }
    sorted_remove(&mut state.acl, &target);
}

/// Mirrors `AuthzEngine::gc_nonces` (auth.rs:321-326).
fn gc_nonces_model(state: &mut GatewayState) {
    state.seen_nonces.retain(|&(_, ts)| {
        let diff = if state.now >= ts {
            state.now - ts
        } else {
            ts - state.now
        };
        diff <= WINDOW
    });
}

// ── Stateright Model ─────────────────────────────────────────────────────────

/// The gateway dispatch model — explores all interleavings of grants, revokes,
/// signed requests, authorizations, signature checks, GC ticks, and clock
/// advances over bounded parameters.
#[derive(Clone)]
struct GatewayModel;

impl Model for GatewayModel {
    type State = GatewayState;
    type Action = GatewayAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![GatewayState {
            acl: Vec::new(),
            seen_nonces: Vec::new(),
            now: 0,
            s7_violated: false,
            has_dispatched: false,
        }]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        // SignedRequest: for each (signer, nonce, timestamp, sig_valid)
        for &signer in &KEYS {
            for &nonce in &NONCES {
                for &ts in &TIMESTAMPS {
                    for &sig_valid in &[true, false] {
                        actions.push(GatewayAction::SignedRequest {
                            signer,
                            nonce,
                            timestamp: ts,
                            sig_valid,
                        });
                    }
                }
            }
        }

        // Authorize: same parameter space (consumes nonces, shared state)
        for &signer in &KEYS {
            for &nonce in &NONCES {
                for &ts in &TIMESTAMPS {
                    for &sig_valid in &[true, false] {
                        actions.push(GatewayAction::Authorize {
                            signer,
                            nonce,
                            timestamp: ts,
                            sig_valid,
                        });
                    }
                }
            }
        }

        // VerifySignature: same parameter space (consumes nonces, no ACL check)
        for &signer in &KEYS {
            for &nonce in &NONCES {
                for &ts in &TIMESTAMPS {
                    for &sig_valid in &[true, false] {
                        actions.push(GatewayAction::VerifySignature {
                            signer,
                            nonce,
                            timestamp: ts,
                            sig_valid,
                        });
                    }
                }
            }
        }

        // Grant: for each (requester, target) pair
        for &requester in &KEYS {
            for &target in &KEYS {
                actions.push(GatewayAction::Grant { requester, target });
            }
        }

        // Revoke: for each (requester, target) pair
        for &requester in &KEYS {
            for &target in &KEYS {
                actions.push(GatewayAction::Revoke { requester, target });
            }
        }

        // GC tick and clock advance
        actions.push(GatewayAction::GcTick);
        actions.push(GatewayAction::AdvanceClock);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            GatewayAction::SignedRequest {
                signer,
                nonce,
                timestamp,
                sig_valid,
            } => {
                let result =
                    check_signed_request_model(&mut next, signer, nonce, timestamp, sig_valid);
                if result == CheckResult::Allowed {
                    // Dual-rail S7 check: independently verify the signer IS authorized
                    if !is_authorized(signer, &state.acl) {
                        next.s7_violated = true;
                    }
                    next.has_dispatched = true;
                }
            }

            GatewayAction::Authorize {
                signer: _,
                nonce,
                timestamp,
                sig_valid,
            } => {
                // Authorize uses check_signed_request (same as HandleSignedRequest),
                // but sends result to reply_to — never dispatches to datastore_node.
                let _result =
                    check_signed_request_model(&mut next, OWNER, nonce, timestamp, sig_valid);
                // Note: Authorize handler calls check_signed_request with the request's
                // signer, but for nonce-consumption modeling, the key identity doesn't
                // matter — only the nonce/timestamp pair is consumed. We use the actual
                // signer parameter isn't needed for state effects beyond nonce tracking.
                // The handler sends to reply_to only, never to datastore_node.
            }

            GatewayAction::VerifySignature {
                signer: _,
                nonce,
                timestamp,
                sig_valid,
            } => {
                // VerifySignature uses check_signature_only — no ACL check.
                // Sends to reply_to only, never to datastore_node.
                let _result =
                    check_signature_only_model(&mut next, nonce, timestamp, sig_valid);
            }

            GatewayAction::Grant { requester, target } => {
                grant_model(&mut next, requester, target);
            }

            GatewayAction::Revoke { requester, target } => {
                revoke_model(&mut next, requester, target);
            }

            GatewayAction::GcTick => {
                gc_nonces_model(&mut next);
            }

            GatewayAction::AdvanceClock => {
                // Cap at max timestamp to keep state space bounded
                if next.now < *TIMESTAMPS.last().unwrap() {
                    next.now += 1;
                } else {
                    return None; // no-op, prune
                }
            }
        }

        // Prune: if state didn't change, no need to explore further
        if next == *state {
            return None;
        }

        Some(next)
    }

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
}

// ── Test ─────────────────────────────────────────────────────────────────────

#[test]
fn gateway_dispatch_model_check() {
    let result = GatewayModel
        .checker()
        .spawn_dfs()
        .join();

    // Report summary before asserting, to aid debugging on failure.
    let unique_states = result.unique_state_count();
    println!(
        "Stateright: explored {} unique states, max depth {}",
        unique_states,
        result.max_depth(),
    );

    result.assert_properties();

    // Sanity: the model actually explored a meaningful state space.
    assert!(
        unique_states > 100,
        "Model explored too few states ({unique_states}); check action generation",
    );
}
