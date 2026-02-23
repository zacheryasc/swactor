//! Authorization types and engine for the distributed datastore.
//!
//! Enforces binary access control (authorized or not) using ed25519 identities.
//! Two auth paths:
//! - **Path 1 (Direct iroh):** connection-level `check_node` against the ACL.
//! - **Path 2 (Browser relay):** per-request `check_signed_request` with
//!   signature, timestamp, nonce, and ACL verification.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use distribution::crypto;
use distribution::types::{NodeId, Signature};
use shared_types::ContentHash;

// ─── Access Request / Authorized Key Info ──────────────────────────────────

/// A pending access request from a browser user.
#[derive(Debug, Clone)]
pub struct AccessRequestInfo {
    pub key: NodeId,
    pub name: String,
    pub message: String,
    pub requested_at: u64,
}

/// An authorized key with its human-readable label.
#[derive(Debug, Clone)]
pub struct AuthorizedKeyInfo {
    pub key: NodeId,
    pub label: String,
}

// ─── DatastoreAction ────────────────────────────────────────────────────────

/// An action a client wants to perform on the datastore.
///
/// Carried inside a `SignedRequestPayload` for browser-relay auth (Auth Path 2).
/// Aligned to match `DatastoreNodeMsg` variants — content-hash-first addressing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatastoreAction {
    Put {
        name: Option<String>,
        content_hash: ContentHash,
        size_bytes: u64,
        tags: BTreeMap<String, String>,
    },
    Get {
        content_hash: ContentHash,
    },
    Delete {
        content_hash: ContentHash,
    },
    List {
        name_filter: Option<String>,
    },
    /// Browser-originated request — proves identity without binding to specific content.
    Access,
}

// ─── SignedRequestPayload ───────────────────────────────────────────────────

/// The signable payload of a client request.
///
/// Serialized canonically (serde_json) and signed by the client's ed25519 key.
/// Includes timestamp and nonce for replay protection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedRequestPayload {
    pub action: DatastoreAction,
    /// Unix timestamp in seconds.
    pub timestamp: u64,
    /// 16 random bytes — prevents replay within the timestamp window.
    pub nonce: [u8; 16],
}

// ─── SignedRequest ──────────────────────────────────────────────────────────

/// A signed request envelope for browser-relay auth (Auth Path 2).
///
/// The relay forwards this opaquely — it cannot forge, modify, or replay it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedRequest {
    pub payload: SignedRequestPayload,
    /// The client's ed25519 public key.
    pub public_key: NodeId,
    /// ed25519 signature over the canonical serialization of `payload`.
    pub signature: Signature,
}

// ─── AccessControlList ──────────────────────────────────────────────────────

/// The datastore's access control list.
///
/// Persisted as `acl.json` alongside the datastore's `storage_path`.
/// The owner always has implicit full access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessControlList {
    /// The datastore owner's public key — always has full access.
    pub owner: NodeId,
    /// Explicitly authorized client keys.
    pub authorized_keys: HashSet<NodeId>,
    /// Human-readable labels for authorized keys (hex → name).
    #[serde(default)]
    pub key_labels: HashMap<String, String>,
}

impl AccessControlList {
    /// Load an ACL from disk, or create a default one with the given owner.
    pub fn load_or_create(path: &Path, owner: NodeId) -> io::Result<Self> {
        if path.exists() {
            let data = std::fs::read_to_string(path)?;
            let acl: AccessControlList = serde_json::from_str(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(acl)
        } else {
            let acl = AccessControlList {
                owner,
                authorized_keys: HashSet::new(),
                key_labels: HashMap::new(),
            };
            acl.save(path)?;
            Ok(acl)
        }
    }

    /// Persist the ACL to disk as JSON.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(path, json)
    }
}

// ─── AuthzResult ────────────────────────────────────────────────────────────

/// The outcome of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzResult {
    Allowed,
    Denied(DeniedReason),
}

/// Why a request was denied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeniedReason {
    /// The key is not in the ACL.
    NotAuthorized,
    /// The ed25519 signature is invalid.
    InvalidSignature,
    /// The request timestamp is outside the ±300s window.
    RequestExpired,
    /// The nonce has already been seen within the time window.
    ReplayDetected,
}

// ─── Signing / Verification ─────────────────────────────────────────────────

/// Sign a request payload, returning a complete `SignedRequest` envelope.
pub fn sign_request(keypair: &crypto::Keypair, payload: SignedRequestPayload) -> SignedRequest {
    let bytes = serde_json::to_vec(&payload).expect("SignedRequestPayload is always serializable");
    let signature = keypair.sign(&bytes);
    SignedRequest {
        payload,
        public_key: keypair.node_id(),
        signature,
    }
}

/// Verify a `SignedRequest`'s signature against its embedded `public_key`.
///
/// Checks only signature validity — does NOT check timestamp, nonce, or ACL.
pub fn verify_signed_request(request: &SignedRequest) -> bool {
    let Ok(bytes) = serde_json::to_vec(&request.payload) else {
        return false;
    };
    crypto::verify(&request.public_key, &bytes, &request.signature)
}

// ─── AuthzEngine ────────────────────────────────────────────────────────────

/// Authorization engine — checks requests against the ACL and replay state.
///
/// Sits at the edge of the actor system (Auth Gate / GatewayActor) and decides
/// whether to accept or reject external requests before they reach internal actors.
#[derive(Debug)]
pub struct AuthzEngine {
    pub acl: AccessControlList,
    seen_nonces: HashMap<[u8; 16], u64>,
    timestamp_window: u64,
}

impl AuthzEngine {
    /// Create a new engine with the given ACL and a default 300-second window.
    pub fn new(acl: AccessControlList) -> Self {
        Self {
            acl,
            seen_nonces: HashMap::new(),
            timestamp_window: 300,
        }
    }

    /// Check whether a `NodeId` is authorized (connection-level, Auth Path 1).
    ///
    /// The owner always has implicit access. Other keys must be in `authorized_keys`.
    pub fn check_node(&self, node_id: &NodeId) -> AuthzResult {
        if *node_id == self.acl.owner || self.acl.authorized_keys.contains(node_id) {
            AuthzResult::Allowed
        } else {
            AuthzResult::Denied(DeniedReason::NotAuthorized)
        }
    }

    /// Verify signature, timestamp, and nonce — but skip the ACL check.
    ///
    /// Used for endpoints where the caller proves key ownership without
    /// needing to be in the ACL (e.g. submitting an access request).
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

    /// Verify and authorize a signed request (Auth Path 2).
    ///
    /// Four-step verification in strict order:
    /// 1. Signature validity
    /// 2. Timestamp freshness (±window)
    /// 3. Nonce uniqueness
    /// 4. ACL check
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

    /// Grant access to a `NodeId`. Owner-only, idempotent.
    /// If `label` is provided, it's stored as a human-readable name for the key.
    pub fn grant(&mut self, requester: &NodeId, key: NodeId, label: Option<String>) -> Result<(), DeniedReason> {
        if *requester != self.acl.owner {
            return Err(DeniedReason::NotAuthorized);
        }
        if key == self.acl.owner {
            return Ok(()); // Owner has implicit access — no-op
        }
        self.acl.authorized_keys.insert(key);
        if let Some(name) = label {
            let hex: String = key.0.iter().map(|b| format!("{b:02x}")).collect();
            self.acl.key_labels.insert(hex, name);
        }
        Ok(())
    }

    /// Revoke access from a `NodeId`. Owner-only, idempotent.
    /// Revoking the owner is a no-op (owner's implicit access cannot be removed).
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

    /// List all authorized keys with their labels.
    pub fn authorized_key_list(&self) -> Vec<AuthorizedKeyInfo> {
        self.acl
            .authorized_keys
            .iter()
            .map(|key| {
                let hex: String = key.0.iter().map(|b| format!("{b:02x}")).collect();
                let label = self.acl.key_labels.get(&hex).cloned().unwrap_or_default();
                AuthorizedKeyInfo { key: *key, label }
            })
            .collect()
    }

    /// Evict nonces whose timestamps fall outside the current window.
    pub fn gc_nonces(&mut self, now: u64) {
        self.seen_nonces.retain(|_nonce, ts| {
            let diff = if now >= *ts { now - *ts } else { *ts - now };
            diff <= self.timestamp_window
        });
    }
}
