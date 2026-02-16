# Swactor Datastore Auth Specification

**Version:** 0.1.0 (MVP)
**Status:** Draft
**Companion to:** `DATASTORE_PROTOCOL.md`

## 1. Overview

This document specifies the authorization layer for the Swactor Datastore. It defines how access is controlled for external clients connecting to a datastore node.

### Principles

- **Cryptographic identity** — keys, not passwords. Every participant is identified by an ed25519 public key (`NodeId`).
- **Binary access** — a client is either authorized or not. No permission tiers for MVP.
- **Owner-only administration** — only the datastore owner can grant or revoke access.
- **Transport-layer authentication** — iroh's QUIC handshake cryptographically proves a peer's `NodeId`. This spec builds authorization on top of that.

### Non-Goals (MVP)

- Per-path permission scoping.
- Permission tiers (read-only, read-write, admin).
- Capability tokens or time-limited delegated access.
- Multi-level delegation chains.

## 2. Trust Boundaries

```
┌─────────────────────────────────────────────┐
│             Cluster (SWIM mesh)              │
│                                              │
│  Node A ◄──────────────► Node B             │
│           implicitly trusted                 │
│           (no auth checks)                   │
└──────────────────┬──────────────────────────┘
                   │
                   │  auth boundary
                   │
        ┌──────────▼──────────┐
        │   External Clients  │
        │                     │
        │  CLI tool           │
        │  Browser user       │
        └─────────────────────┘
```

- **Cluster-internal** (node-to-node via SWIM): implicitly trusted. Nodes that are members of the SWIM cluster communicate freely — no per-request auth checks.
- **External clients** (CLI, browser): must be authorized. Every external request is checked against the Access Control List before being dispatched to the actor system.

## 3. Identity Model

The auth layer reuses the existing ed25519 identity model from the distribution layer:

- Every client (CLI tool, browser user, node) has an ed25519 keypair.
- Identity is the 32-byte public key, represented as `NodeId`.
- The same `NodeId` type from `distribution::types` is used throughout.

There is no separate "user" concept — a keypair *is* an identity.

## 4. Access Control List

### 4.1 Structure

```
AccessControlList {
    owner:           NodeId,              // The datastore owner's public key
    authorized_keys: Set<NodeId>,         // Explicitly authorized client keys
}
```

- The **owner** always has full access (implicit; never needs to be in `authorized_keys`).
- An empty `authorized_keys` set means only the owner can access the datastore.

### 4.2 Persistence

The ACL is persisted as a JSON file alongside the datastore's `storage_path`:

```
{storage_path}/
├── chunks/
├── manifests/
└── acl.json              # AccessControlList
```

### 4.3 Mutations

| Operation | Signature | Who |
|-----------|-----------|-----|
| Grant access | `grant(key: NodeId)` | Owner only |
| Revoke access | `revoke(key: NodeId)` | Owner only |

- `grant` adds a `NodeId` to `authorized_keys`. Idempotent — granting an already-authorized key is a no-op.
- `revoke` removes a `NodeId` from `authorized_keys`. Idempotent — revoking a non-existent key is a no-op.
- Revoking the owner is a no-op (the owner's implicit access cannot be removed).
- Both operations persist the updated ACL to disk immediately.

## 5. Auth Path 1 — Direct iroh Connection

For clients that connect directly to the datastore node over iroh (QUIC):

```
Client (ed25519 keypair)              Datastore Node
        │                                    │
        │──── iroh QUIC handshake ──────────>│
        │     (proves client's NodeId)       │
        │                                    │
        │                              check NodeId
        │                              against ACL
        │                                    │
        │<─── accept / reject ──────────────│
        │                                    │
        │     (if accepted, all ops on       │
        │      this connection are allowed)  │
```

1. The iroh QUIC handshake cryptographically proves the peer's `NodeId` (ed25519 public key).
2. On connection establishment, the node checks the peer's `NodeId` against the ACL.
3. If authorized → connection accepted. All operations on that connection are allowed with no per-message overhead.
4. If not authorized → connection rejected immediately.

This is the preferred auth path — zero overhead after the initial handshake.

## 6. Auth Path 2 — Signed Requests (Browser Relay)

For browser users who cannot establish direct iroh connections (e.g., because the browser communicates via a website backend that relays requests):

### 6.1 Threat Model

The website backend acts as an **untrusted relay**. It forwards requests between the browser and the datastore node but never sees private keys. The relay cannot forge, modify, or replay requests.

### 6.2 Signed Envelope

Each request is wrapped in a signed envelope:

```
SignedRequest {
    payload:    SignedRequestPayload,    // The request details
    public_key: NodeId,                 // Client's public key
    signature:  Signature,              // ed25519 signature over serialized payload
}

SignedRequestPayload {
    action:    DatastoreAction,         // What the client wants to do
    timestamp: u64,                     // Unix timestamp (seconds)
    nonce:     [u8; 16],                // 16 random bytes
}

DatastoreAction = enum {
    Put { name, content_hash, size_bytes, tags },
    Get { content_hash },
    Delete { content_hash },
    List { name_filter },
}
```

### 6.3 Verification Steps

The datastore node verifies a signed request in strict order:

1. **Signature validity** — verify the ed25519 signature over the canonical serialization of `SignedRequestPayload` using the provided `public_key`.
2. **Timestamp freshness** — reject if `|now - payload.timestamp| > 300` seconds.
3. **Nonce uniqueness** — reject if `payload.nonce` has been seen before within the time window.
4. **ACL check** — reject if `public_key` is not in the ACL.

If any step fails, the request is denied with the corresponding `DeniedReason`.

### 6.4 Put Payload Note

`DatastoreAction::Put` references a `content_hash` rather than embedding raw file data. The bulk data is uploaded separately, and its integrity is guaranteed by blake3 content addressing. The signed envelope authorizes the *operation*, not the data transfer.

## 7. Replay Protection

### 7.1 Timestamp Window

- Requests must have a `timestamp` within ±300 seconds of the node's wall clock.
- This bounds the maximum clock drift between client and server.
- Requests outside this window are rejected with `DeniedReason::RequestExpired`.

### 7.2 Nonce

- Each request includes a 16-byte random nonce.
- The node maintains a set of recently seen nonces.
- Duplicate nonces within the time window are rejected with `DeniedReason::ReplayDetected`.

### 7.3 Nonce Garbage Collection

- Nonces are stored alongside their timestamps.
- When a nonce's timestamp falls outside the ±300 second window, it is eligible for GC.
- GC runs periodically (piggy-backed on request processing or a background sweep).

## 8. Enforcement Point

Auth is enforced at the **edge** of the actor system — between external clients and the internal actors:

```
External Client
      │
      ▼
┌─────────────┐
│  Auth Gate   │◄── ACL check happens here
└──────┬──────┘
       │
       ▼
┌──────────────┐    ┌─────────────────┐    ┌────────────────┐
│ MetadataActor│◄──►│ BlobStoreActor  │    │ TransferActor  │
│              │    │                 │    │                │
│  (auth-      │    │  (auth-         │    │  (auth-        │
│   unaware)   │    │   unaware)      │    │   unaware)     │
└──────────────┘    └─────────────────┘    └────────────────┘
```

### 8.1 Direct iroh Connections

- Auth check at connection acceptance time.
- Once accepted, the connection is fully trusted for all operations.
- No per-message overhead.

### 8.2 Signed Requests (Browser Relay)

- A `GatewayActor` receives signed request envelopes.
- The GatewayActor verifies the envelope (signature, timestamp, nonce, ACL).
- If valid, the GatewayActor dispatches the inner action to the `MetadataActor`.
- If invalid, the GatewayActor returns the denial reason to the relay.

### 8.3 Internal Actors

`MetadataActor`, `BlobStoreActor`, and `TransferActor` remain **auth-unaware**. They process messages from any source within the actor system. The auth boundary is strictly external.

## 9. Key Management

### 9.1 Key Generation

- Uses `ed25519_dalek` keypairs (same as node identity).
- CLI: `swactor-store auth keygen` generates a new keypair and prints both the secret key (for the client to store) and the public key (to share with the owner).
- Browser: keypair generated client-side using WebCrypto Ed25519 or wasm-compiled ed25519. The private key never leaves the browser.

### 9.2 Grant Flow

```
1. Client generates an ed25519 keypair.
2. Client shares their public key with the datastore owner (out-of-band).
3. Owner runs: swactor-store auth grant <pubkey>
4. Client can now access the datastore.
```

The out-of-band exchange is intentional — it keeps the trust model simple. The owner explicitly decides who gets access.

### 9.3 Revocation

```
1. Owner runs: swactor-store auth revoke <pubkey>
2. Client's access is immediately revoked.
3. Existing direct iroh connections from that client remain open until disconnected.
4. Signed requests from the revoked key are rejected immediately.
```

Note: revoking a key does not forcibly disconnect an active iroh session. The revocation takes effect on the next connection attempt. For immediate disconnection, the owner should also restart the node or implement connection tracking (future extension).

## 10. CLI Extensions

The following subcommands are added under `swactor-store auth`:

```
swactor-store auth keygen
    Generate a new ed25519 keypair.
    Prints the public key (hex) and secret key (hex) to stdout.

swactor-store auth grant <pubkey>
    Add a public key to the ACL's authorized_keys set.
    Requires running on the owner's node.

swactor-store auth revoke <pubkey>
    Remove a public key from the ACL's authorized_keys set.
    Requires running on the owner's node.

swactor-store auth list
    Show all authorized keys (including the owner).

swactor-store auth whoami
    Show this node's public key (NodeId).
```

## 11. Integration with Datastore Protocol

Each protocol flow from `DATASTORE_PROTOCOL.md` §6 has a clear auth integration point:

| Protocol Flow | Auth Path 1 (Direct) | Auth Path 2 (Signed Request) |
|---------------|----------------------|------------------------------|
| §6.1 PUT | Connection-level ACL check | `SignedRequest { action: Put { name, content_hash, size_bytes, tags }, .. }` |
| §6.2 GET (Local) | Connection-level ACL check | `SignedRequest { action: Get { content_hash }, .. }` |
| §6.3 GET (Remote) | Connection-level ACL check | `SignedRequest { action: Get { content_hash }, .. }` → node handles remote fetch internally |
| §6.4 DELETE | Connection-level ACL check | `SignedRequest { action: Delete { content_hash }, .. }` |
| §6.5 LIST (Local) | Connection-level ACL check | `SignedRequest { action: List { name_filter }, .. }` |
| §6.6 LIST (Swarm-Wide) | Connection-level ACL check | `SignedRequest { action: List { name_filter }, .. }` → node handles fan-out internally |

In all cases, auth is enforced *before* the request reaches the actor system. Internal inter-node communication (DHT replication, chunk transfers between cluster members) is not subject to auth checks.

## 12. Future Extensions

These are explicitly **out of scope** for MVP but inform the design:

- **Per-path permission scoping** — restrict a key to specific path prefixes (e.g., read-only access to `photos/`).
- **Permission tiers** — read-only, read-write, admin roles.
- **Capability tokens** — time-limited, scope-limited bearer tokens for delegated access without sharing long-lived keys.
- **Multi-level delegation** — allow authorized users to grant limited access to others.
- **Connection tracking** — forcibly disconnect revoked keys from active iroh sessions.
