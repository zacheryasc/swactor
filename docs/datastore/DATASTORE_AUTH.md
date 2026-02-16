# Swactor Datastore Auth Specification

**Version:** 0.2.0
**Status:** Implemented (MVP)

## 1. Overview

This document specifies the authorization layer for the Swactor Datastore as implemented. It defines how access is controlled for external clients connecting to a datastore node.

### Principles

- **Cryptographic identity** — keys, not passwords. Every participant is identified by an ed25519 public key (`NodeId`).
- **Binary access** — a client is either authorized or not. No permission tiers for MVP.
- **Owner-only administration** — only the datastore owner can grant or revoke access.
- **Two auth paths** — direct iroh connections (connection-level) and signed HTTP requests (browser/CLI). This spec covers the signed request path (Auth Path 2), which is fully implemented.

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

```rust
AccessControlList {
    owner:           NodeId,                      // The datastore owner's public key
    authorized_keys: HashSet<NodeId>,             // Explicitly authorized client keys
    key_labels:      HashMap<String, String>,     // hex(public_key) → human-readable name
}
```

- The **owner** always has full access (implicit; never needs to be in `authorized_keys`).
- An empty `authorized_keys` set means only the owner can access the datastore.
- `key_labels` maps the hex-encoded public key to a human-readable name. Labels are set on grant (from the access request's `name` field or an explicit `--name`/`?name=` parameter) and removed on revoke. The `#[serde(default)]` annotation ensures backward compatibility with ACL files written before labels existed.

### 4.2 Persistence

The ACL is persisted as JSON in the **auth directory**, separate from the storage path:

```
<auth-dir>/
├── owner.key.json    # Owner keypair
└── acl.json          # AccessControlList
```

Default `auth-dir` is `./auth` (configurable via `--auth-dir`).

### 4.3 Mutations

| Operation | Signature | Who |
|-----------|-----------|-----|
| Grant access | `grant(requester, key, label)` | Owner only |
| Revoke access | `revoke(requester, key)` | Owner only |

- `grant` adds a `NodeId` to `authorized_keys` and optionally sets a label in `key_labels`. If the key has a pending access request, the request's `name` is used as the label (unless an explicit label is provided). Idempotent.
- `revoke` removes a `NodeId` from `authorized_keys` and removes its label from `key_labels`. Idempotent.
- Revoking the owner is a no-op (the owner's implicit access cannot be removed).
- Both operations persist the updated ACL to disk immediately via `persist_acl()`.

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
2. On connection establishment, the node checks the peer's `NodeId` against the ACL via `check_node()`.
3. If authorized, connection accepted. All operations on that connection are allowed with no per-message overhead.
4. If not authorized, connection rejected immediately.

## 6. Auth Path 2 — Signed Requests (HTTP API)

For browser users and CLI clients communicating over HTTP.

### 6.1 Threat Model

The HTTP transport is treated as an **untrusted relay**. Each request is self-authenticating via a signed envelope. The relay cannot forge, modify, or replay requests.

### 6.2 Signed Envelope

Each request carries a signed envelope in the `X-Signed-Request` HTTP header:

```rust
SignedRequest {
    payload:    SignedRequestPayload,    // The request details
    public_key: NodeId,                 // Client's public key (as [u8; 32])
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
    Access,                             // Identity proof (no content binding)
}
```

The header value is the JSON serialization of `SignedRequest`. The `public_key` and `signature` fields are serialized as arrays of integers (e.g., `[163, 45, ...]`), matching serde's default serialization for `[u8; 32]` and `[u8; 64]`.

### 6.3 DatastoreAction::Access

The `Access` variant is a lightweight identity proof that does not bind to a specific content operation. It is used by:

- **Browser** — all API calls use `Access` (the browser proves identity, and the HTTP layer gates the actual operation).
- **CLI auth management** — `grant`, `revoke`, `requests`, `keys`, `deny` subcommands use `Access` since these admin operations don't correspond to content actions.

The CLI's data operations (`put`, `get`, `delete`, `list`) sign the corresponding specific action variants.

### 6.4 Verification Steps

The `AuthzEngine` verifies a signed request in strict order:

1. **Signature validity** — verify the ed25519 signature over the canonical JSON serialization of `SignedRequestPayload` using the provided `public_key`.
2. **Timestamp freshness** — reject if `|now - payload.timestamp| > 300` seconds.
3. **Nonce uniqueness** — reject if `payload.nonce` has been seen before within the time window.
4. **ACL check** — reject if `public_key` is not in the ACL (not owner and not in `authorized_keys`).

If any step fails, the request is denied with the corresponding `DeniedReason`:
- `InvalidSignature`
- `RequestExpired`
- `ReplayDetected`
- `NotAuthorized`

### 6.5 Signature-Only Verification

A separate `check_signature_only()` path performs steps 1-3 (signature, timestamp, nonce) but **skips** step 4 (ACL check). This is used for the access request endpoint (`POST /api/auth/request`), where an unauthorized user needs to prove they own the key they're requesting access for.

### 6.6 Put Payload Note

`DatastoreAction::Put` references a `content_hash` rather than embedding raw file data. The bulk data is uploaded separately, and its integrity is guaranteed by blake3 content addressing. The signed envelope authorizes the *operation*, not the data transfer.

## 7. Replay Protection

### 7.1 Timestamp Window

- Requests must have a `timestamp` within ±300 seconds of the node's wall clock.
- Requests outside this window are rejected with `DeniedReason::RequestExpired`.

### 7.2 Nonce

- Each request includes a 16-byte random nonce.
- The node maintains a set of recently seen nonces in `seen_nonces: HashMap<[u8; 16], u64>`.
- Duplicate nonces within the time window are rejected with `DeniedReason::ReplayDetected`.

### 7.3 Nonce Garbage Collection

- Nonces are stored alongside their timestamps.
- When a nonce's timestamp falls outside the ±300 second window, it is eligible for GC.
- `gc_nonces(now)` is called periodically via `GatewayMsg::NonceGcTick`, which piggybacks on the main loop's GC tick cadence.

## 8. Enforcement Point

Auth is enforced at the **edge** of the actor system via the `GatewayActor`:

```
External Client
      │
      ▼
┌─────────────┐
│ GatewayActor│◄── ACL check happens here
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

### 8.1 HTTP API Route Table

| Method | Path | Auth Level | Description |
|--------|------|------------|-------------|
| `GET` | `/` | None | Browser UI page |
| `GET` | `/admin` | None | Admin page |
| `GET` | `/crypto.wasm` | None | WASM Ed25519 module |
| `GET` | `/api/status` | None | Node identity |
| `POST` | `/api/put` | Full (`check_auth`) | Store an object |
| `GET` | `/api/get` | Full (`check_auth`) | Get object metadata |
| `GET` | `/api/data` | Full (`check_auth`) | Download object data |
| `POST` | `/api/delete` | Full (`check_auth`) | Delete an object |
| `GET` | `/api/list` | Full (`check_auth`) | List objects |
| `POST` | `/api/auth/grant` | Full (`check_auth_identity`) | Grant access to a key (owner-only) |
| `POST` | `/api/auth/revoke` | Full (`check_auth_identity`) | Revoke access from a key (owner-only) |
| `GET` | `/api/auth/requests` | Full (`check_auth_identity`) | List pending access requests (owner-only) |
| `GET` | `/api/auth/keys` | Full (`check_auth_identity`) | List authorized keys (owner-only) |
| `POST` | `/api/auth/deny` | Full (`check_auth_identity`) | Deny a pending request (owner-only) |
| `POST` | `/api/auth/request` | Signature-only (`check_auth_signature_only`) | Submit an access request |

**Auth levels:**
- **None** — no `X-Signed-Request` header required.
- **Full** — `X-Signed-Request` header required; full 4-step verification (signature + timestamp + nonce + ACL).
- **Signature-only** — `X-Signed-Request` header required; 3-step verification (signature + timestamp + nonce, no ACL check).

`check_auth_identity` is like `check_auth` but also returns the caller's `NodeId`, needed for grant/revoke/deny operations to identify the requester.

### 8.2 Internal Actors

`MetadataActor`, `BlobStoreActor`, and `TransferActor` remain **auth-unaware**. They process messages from any source within the actor system. The auth boundary is strictly external.

## 9. Browser Auth Flow

### 9.1 WASM Ed25519 Crypto

Browser clients use a WASM module (`/crypto.wasm`) compiled from `crates/crypto-wasm/` — a `no_std` Rust crate using `ed25519-dalek`. This replaces the earlier Web Crypto API approach, which has inconsistent Ed25519 support across browsers.

The WASM module exports three functions through a shared 8192-byte buffer:

| Function | Input | Output |
|----------|-------|--------|
| `buffer_ptr()` | — | Pointer to shared buffer |
| `get_public_key()` | `BUF[0..32]` = seed | `BUF[32..64]` = public key |
| `ed25519_sign(msg_len)` | `BUF[0..32]` = seed, `BUF[128..128+msg_len]` = message | `BUF[64..128]` = signature |

JavaScript wrapper functions:

```javascript
async function initCrypto() {
    const { instance } = await WebAssembly.instantiate(
        await (await fetch('/crypto.wasm')).arrayBuffer()
    );
    wasmExports = instance.exports;
    bufPtr = wasmExports.buffer_ptr();
}

function derivePublicKey(seed) { /* write seed → read pubkey */ }
function signBytes(message, seed) { /* write seed+message → read signature */ }
```

### 9.2 Device Key Management

On first visit (when auth is detected), the browser:

1. Generates a 32-byte random seed: `crypto.getRandomValues(new Uint8Array(32))`
2. Stores it as hex in `localStorage.deviceKeySeed`
3. Derives the public key via `derivePublicKey(seed)`

On subsequent visits, the seed is loaded from localStorage. A migration path handles legacy JWK keys (from an earlier Web Crypto implementation) by extracting the `d` parameter as the seed.

### 9.3 Auth Detection

On page load, the browser fetches `GET /api/list` without auth:
- If the response is 401, auth is enabled → initialize WASM crypto, generate/load keys, show device key in header
- If the response is 200, auth is disabled → proceed normally

### 9.4 Request Signing

All authenticated browser requests go through `authFetch()`:

```javascript
async function authFetch(url, opts) {
    const nonce = Array.from(crypto.getRandomValues(new Uint8Array(16)));
    const payload = {
        action: "Access",
        timestamp: Math.floor(Date.now() / 1000),
        nonce: nonce
    };
    const payloadBytes = new TextEncoder().encode(JSON.stringify(payload));
    const sigBytes = signBytes(payloadBytes, deviceSeed);
    const header = JSON.stringify({
        payload: payload,
        public_key: Array.from(pubKeyBytes),
        signature: Array.from(sigBytes)
    });
    opts.headers['X-Signed-Request'] = header;
    return fetch(url, opts);
}
```

The browser always uses `DatastoreAction::Access` — it proves identity without binding to a specific content operation. The HTTP API layer handles the actual data operation gating.

### 9.5 Access Request Flow

When a browser user is not yet authorized:

1. **Auth banner appears** — shows a form with name (required, max 64 chars) and message (optional, max 256 chars) fields.
2. **User submits** — `POST /api/auth/request` with JSON body `{ name, message }` and `X-Signed-Request` header (signature-only check).
3. **Pending state** — banner switches to "waiting for operator approval" with localStorage persistence (`accessRequestPending`, `accessRequestName`).
4. **Polling** — every 5 seconds, `authFetch('/api/list')` checks if the user has been granted access.
5. **Granted** — when `/api/list` returns 200, polling stops, banner disappears, object list loads.
6. **Re-submission on reload** — if the page is reloaded while pending, the request is re-submitted to handle node restarts.

## 10. Admin Page

The admin page (`/admin`) provides a browser interface for the datastore owner to manage access.

### 10.1 Authentication

The owner authenticates by uploading their `key.json` file:
1. File is parsed for `secret_key` (hex) and `public_key` (hex).
2. Public key is derived from the secret key via WASM and compared to the stored `public_key` for integrity.
3. A test call to `GET /api/auth/requests` verifies this is actually the owner key (non-owners get 403).

### 10.2 Capabilities

- **Pending access requests** — table showing name, message, key (truncated), with grant/deny buttons per request.
- **Authorized keys** — table showing label, key (truncated), with revoke button per key.
- **Manual grant** — input fields for a 64-char hex public key + optional name, bypassing the access request flow.
- **Name disambiguation** — when multiple entries share the same name, a key prefix `(abcd1234)` is appended for disambiguation.

### 10.3 Admin Request Signing

All admin API calls use `ownerAuthFetch()`, which signs with `DatastoreAction::Access` using the owner's seed.

## 11. CLI

### 11.1 Auth Signing

The CLI uses `--key <path>` to load a key.json file. Each command signs an `X-Signed-Request` header:

- **Data operations** (`put`, `get`, `delete`, `list`) sign with the corresponding `DatastoreAction` variant (e.g., `DatastoreAction::Put { name, content_hash, size_bytes, tags }`).
- **Auth management** (`grant`, `revoke`, `requests`, `keys`, `deny`) sign with `DatastoreAction::Access`.
- **`status`** — never signed (endpoint is always open).
- Without `--key`, no header is sent (backward compatible with non-auth nodes).

### 11.2 Subcommands

```
swactor-store --key <path> put <file> [--name <label>]
    Upload a file. Signs DatastoreAction::Put.

swactor-store --key <path> get <hash> [--output <path>]
    Retrieve metadata (or download with --output). Signs DatastoreAction::Get.

swactor-store --key <path> delete <hash>
    Delete an object. Signs DatastoreAction::Delete.

swactor-store --key <path> list [--name <filter>] [--all]
    List objects. Signs DatastoreAction::List.

swactor-store status
    Show node identity. No signing.

swactor-store --key <path> grant <key_or_name> [--name <label>]
    Authorize a public key. Owner-only. Accepts 64 hex chars or a name.

swactor-store --key <path> revoke <key_or_name>
    Revoke a public key. Owner-only. Accepts 64 hex chars or a name.

swactor-store --key <path> requests
    List pending access requests. Owner-only.

swactor-store --key <path> keys
    List authorized keys with labels. Owner-only.

swactor-store --key <path> deny <key_or_name>
    Deny a pending access request. Owner-only. Accepts 64 hex chars or a name.
```

### 11.3 Name Resolution

`grant`, `revoke`, and `deny` accept either:
- A **64-character hex public key** — used directly.
- A **human-readable name** — resolved by fetching the pending requests (`/api/auth/requests`) or authorized keys (`/api/auth/keys`) list and matching by name.

If multiple entries match the same name, the CLI prints disambiguated names (e.g., `alice (c9d0e1f2)`) and asks the user to re-run with the disambiguated form. The `(prefix)` suffix uses the first 8 hex characters of the key.

## 12. Key Management

### 12.1 Key File Format

All keys use the same JSON format:

```json
{
  "version": 1,
  "secret_key": "...64 hex chars (32 bytes ed25519 seed)...",
  "public_key": "...64 hex chars (32 bytes ed25519 public key)...",
  "created_at": "2026-02-15T12:00:00Z"
}
```

- Generated by the node on first `--auth` run at `<auth-dir>/owner.key.json`.
- The CLI reads it via `--key`.
- The admin page accepts it via file upload for authentication.

### 12.2 Node Key Generation

When `--auth` is enabled:
1. If `<auth-dir>/owner.key.json` exists, load the keypair from it.
2. Otherwise, generate a new `Keypair`, write the key file with ISO-8601 `created_at`.
3. The keypair's `node_id()` becomes the node's `NodeId` (deterministic identity across restarts).
4. Create/load `<auth-dir>/acl.json` with this `NodeId` as owner.

### 12.3 Browser Key Generation

Browser keys are simpler — 32 random bytes stored as hex in `localStorage.deviceKeySeed`. No key file is produced. The public key is derived on each page load via the WASM `get_public_key()` function.

### 12.4 Grant Flow

Two paths to granting access:

**Via access request (browser-initiated):**
1. Browser user visits the page, generates device key, submits access request with name.
2. Owner views pending requests on `/admin` or via `swactor-store requests`.
3. Owner grants via admin page button or `swactor-store grant <name_or_key>`.
4. Pending request is removed, name becomes key label, ACL is persisted.
5. Browser's polling detects the grant and loads the object list.

**Via manual grant (out-of-band):**
1. Client generates a keypair (or uses an existing one).
2. Client shares their public key with the owner out-of-band.
3. Owner runs: `swactor-store --key owner.key.json grant <pubkey> --name <label>`
4. Or: uses the admin page's "Grant Key Manually" form.

### 12.5 Revocation

1. Owner runs: `swactor-store --key owner.key.json revoke <pubkey_or_name>`
2. Or: clicks "revoke" on the admin page's authorized keys table.
3. Client's access is immediately revoked for HTTP requests.
4. Existing direct iroh connections from that client remain open until disconnected.

## 13. Protocol Integration

Each datastore operation has a clear auth integration point:

| Operation | CLI Signing | Browser Signing |
|-----------|-------------|-----------------|
| PUT | `DatastoreAction::Put { name, content_hash, size_bytes, tags }` | `DatastoreAction::Access` |
| GET | `DatastoreAction::Get { content_hash }` | `DatastoreAction::Access` |
| DELETE | `DatastoreAction::Delete { content_hash }` | `DatastoreAction::Access` |
| LIST | `DatastoreAction::List { name_filter }` | `DatastoreAction::Access` |
| Grant/Revoke/etc. | `DatastoreAction::Access` | `DatastoreAction::Access` |

The browser uses `Access` for all operations because:
- Computing content hashes client-side would add complexity to the browser JS.
- The HTTP API already gates the actual data operation — the signed request only needs to prove identity.
- The `Access` action maps to `DatastoreNodeMsg::Status` in the gateway (a lightweight no-op that returns a valid response).

The CLI uses per-action signing for data operations because it has access to the `ContentHash` and can construct precise action payloads.

In all cases, auth is enforced *before* the request reaches the actor system. Internal inter-node communication (DHT replication, chunk transfers between cluster members) is not subject to auth checks.

## 14. Future Extensions

These are explicitly **out of scope** for MVP but inform the design:

- **Per-path permission scoping** — restrict a key to specific path prefixes.
- **Permission tiers** — read-only, read-write, admin roles.
- **Capability tokens** — time-limited, scope-limited bearer tokens for delegated access.
- **Multi-level delegation** — allow authorized users to grant limited access to others.
- **Connection tracking** — forcibly disconnect revoked keys from active iroh sessions.
- **Persistent access requests** — currently in-memory only; lost on node restart (browser re-submits on reload as mitigation).
