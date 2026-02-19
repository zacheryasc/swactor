# Datastore Auth: Development History

**Branch:** `swactor-auth`
**Base commit:** `af15416` (pre-auth baseline)
**5 commits + uncommitted working tree changes**

---

## What Was Built

A complete ed25519 authorization layer for the distributed datastore, spanning:

- **Auth engine** — `AuthzEngine` with ACL, signed request verification, replay protection, nonce GC
- **GatewayActor** — actor-level enforcement point with grant/revoke, access requests, key listing
- **Browser auth flow** — WASM Ed25519 crypto, device key generation, access request/grant/deny lifecycle
- **Admin page** — owner key upload, pending request management, manual key grant, authorized key list
- **Expanded CLI** — full CRUD + auth subcommands (`grant`, `revoke`, `requests`, `keys`, `deny`) with name resolution
- **Storage persistence** — entry/manifest persistence to filesystem, startup bulk-load
- **xtask** — `node`, `cli`, `wasm` subcommands with `config.toml` support
- **WASM crypto crate** — `crates/crypto-wasm/`, a `no_std` cdylib exporting `ed25519_sign()`, `get_public_key()`, `buffer_ptr()`

The auth system enforces binary access control (authorized or not) at the HTTP API boundary. Internal actors remain auth-unaware. Two auth paths: connection-level (iroh QUIC handshake proves NodeId) and per-request signed envelopes (for browser/HTTP API). This branch implements Path 2 end-to-end, including the browser UX.

---

## Architecture

```
                  ┌──────────────────────────────────────────────────────┐
                  │                 HTTP API (api.rs)                    │
                  │                                                     │
                  │  Ungated:                                           │
                  │    GET  /              → browser UI (access page)   │
                  │    GET  /admin         → admin page                 │
                  │    GET  /crypto.wasm   → WASM Ed25519 module        │
                  │    GET  /api/status    → node identity              │
                  │                                                     │
                  │  Auth-gated (X-Signed-Request header):              │
                  │    POST /api/put       → check_auth → handle_put   │
                  │    GET  /api/get       → check_auth → handle_get   │
                  │    GET  /api/data      → check_auth → handle_data  │
                  │    POST /api/delete    → check_auth → handle_delete│
                  │    GET  /api/list      → check_auth → handle_list  │
                  │                                                     │
                  │  Auth management (owner-only):                      │
                  │    POST /api/auth/grant   → check_auth_identity    │
                  │    POST /api/auth/revoke  → check_auth_identity    │
                  │    GET  /api/auth/requests→ check_auth_identity    │
                  │    GET  /api/auth/keys    → check_auth_identity    │
                  │    POST /api/auth/deny    → check_auth_identity    │
                  │                                                     │
                  │  Signature-only (proves key, no ACL check):         │
                  │    POST /api/auth/request → check_auth_sig_only    │
                  └───────────────┬─────────────────────────────────────┘
                                  │
                        GatewayMsg (various)
                                  │
                  ┌───────────────▼───────────────┐
                  │        GatewayActor           │
                  │                               │
                  │  AuthzEngine:                  │
                  │    1. verify ed25519 signature │
                  │    2. check timestamp ±300s    │
                  │    3. check nonce uniqueness   │
                  │    4. check ACL                │
                  │                               │
                  │  Access request management:    │
                  │    pending_requests HashMap    │
                  │    grant resolves label from   │
                  │    pending request name        │
                  │                               │
                  │  ACL persistence:              │
                  │    persist_acl() on grant/     │
                  │    revoke                      │
                  └───────────────┬───────────────┘
                                  │
                  ┌───────────────▼───────────────┐
                  │       DatastoreNode           │
                  │                               │
                  │  MetadataActor ◄──► BlobStore │
                  │  (auth-unaware)               │
                  └───────────────────────────────┘

                  ┌───────────────────────────────┐
                  │    Browser (WASM Ed25519)      │
                  │                               │
                  │  /crypto.wasm → initCrypto()  │
                  │  deviceKeySeed in localStorage │
                  │  signBytes() per request      │
                  │  Access action for all ops     │
                  │  → X-Signed-Request header     │
                  └───────────────────────────────┘

                  ┌───────────────────────────────┐
                  │       CLI (store_cli)          │
                  │                               │
                  │  --key owner.key.json          │
                  │  Per-action signing:           │
                  │    Put/Get/Delete/List/Access  │
                  │  Name resolution for           │
                  │    grant/revoke/deny           │
                  └───────────────────────────────┘
```

---

## Commit-by-Commit

### `863185e` — feat: distributed datastore primitives protocol

Foundation commit establishing the distributed datastore protocol. Defined the protocol messages (`GetChunkRequest`, `FindObjectRequest`, `StoreObjectRequest`, `ListObjectsRequest` and their responses), all implementing `NetworkMessage` with stable `type_tag()` strings. This is the wire protocol for inter-node communication over iroh/QUIC.

**Key files:** `src/messages.rs` (inter-node message types)

### `84c4408` — fix: cli for datastore works

Brought up the `store_node` and `store_cli` binaries as `[[bin]]` targets with feature-gated dependencies (`node` and `cli` features). The node binary spawns the actor runtime, wires up BlobStoreActor/MetadataActor/DatastoreNode, and serves the HTTP API via `tiny_http`. The CLI binary talks to the node over HTTP with `ureq`. Added `clap` for arg parsing, `ctrlc` for graceful shutdown, and `runtime-dashboard` integration.

**Key files:** `Cargo.toml` (features `node`/`cli`), `src/bin/store_node.rs`, `src/bin/store_cli.rs`, `src/api.rs`

### `ebee109` — feat: mvp auth protocol

Core auth implementation:

- **`src/auth.rs`** — `DatastoreAction` enum, `SignedRequestPayload`, `SignedRequest` envelope, `AccessControlList` (with JSON persistence via `save()`/`load_or_create()`), `AuthzEngine` (4-step verification: signature, timestamp, nonce, ACL), `sign_request()`/`verify_signed_request()` helpers, `AuthzResult`/`DeniedReason` enums.
- **`src/actors/gateway.rs`** — `GatewayActor` wrapping `AuthzEngine`. Handles `Authorize` (pure auth check), `HandleSignedRequest` (auth + dispatch via `action_to_node_msg()`), `CheckConnection` (Path 1), `Grant`/`Revoke` (owner-only ACL mutations), `NonceGcTick`.
- **`src/messages.rs`** — `GatewayMsg` enum, `DatastoreResponse::Denied` variant.
- **`crates/shared-types/`** — Extracted `ContentHash` into its own crate to break dependency cycles between `distribution` and `datastore`.

Tests added (18 total):
- `auth_scenario_tests.rs` (10 tests) — owner access, stranger denial, grant/revoke lifecycle, signed request happy path, tampered signature, stale timestamp, replayed nonce, nonce GC, non-owner grant/revoke rejection.
- `acl_persistence_tests.rs` (2 tests) — save/load round-trip, create-on-missing.
- `gateway_tests.rs` (4 tests) — connection allow/deny, signed request flow-through, unauthorized signed request denial.

### `8ac45e5` — fix: adjust auth protocol to datastore protocol

Aligned the auth types with the content-hash-first datastore protocol:
- `DatastoreAction::Put` carries `content_hash`, `size_bytes`, and `tags` (not raw data).
- `DatastoreAction::Get`/`Delete` use `content_hash`.
- `DatastoreAction::List` uses `name_filter`.
- `GatewayActor::action_to_node_msg()` maps actions to `DatastoreNodeMsg` variants.
- Wired `check_auth()` into the HTTP API handlers (put, get, data, delete, list) — reads `X-Signed-Request` header, sends `GatewayMsg::Authorize` to the gateway actor, denies with 401/403/504 on failure.
- `handle_status` intentionally left ungated.

### `e549eef` — feat: auth MVP with integrated tests

Wired auth into both binaries:

**`store_node.rs`** — `--auth` and `--auth-dir <PATH>` flags:
- Loads or generates owner keypair from `<auth-dir>/owner.key.json`.
- Owner keypair's public key becomes the `NodeId` (deterministic identity across restarts).
- Loads/creates `<auth-dir>/acl.json` with owner as sole authorized key.
- Spawns `GatewayActor` and passes `Some(gateway_addr)` to `start_api_server`.
- Sends `GatewayMsg::NonceGcTick` on the same cadence as the metadata GC tick.

**`store_cli.rs`** — `--key <PATH>` flag:
- Each command builds the appropriate `DatastoreAction`, signs it, sends as `X-Signed-Request` header.
- `status` never signs (always open by design).

**`http_auth_integration.rs`** — Full-stack integration test: spins up the actor runtime with GatewayActor, starts the HTTP server, proves owner is allowed (PUT/GET/LIST/DELETE), stranger gets 403, missing header gets 401.

---

## Uncommitted Working Tree Changes

The uncommitted changes represent the bulk of the user-facing work: browser UI, admin page, WASM crypto, expanded CLI, storage persistence, and xtask.

### Browser UI (`ui_html.rs` — `DATASTORE_UI_HTML`)

Complete browser access page served at `/`:

- **Upload panel** — file input + optional name, PUT via `authFetch()`
- **Object table** — list all objects with hash, name, size; download and delete buttons
- **Detail modal** — click a row to see full metadata, chunks, tags
- **Auth detection** — on load, `detectAuth()` fetches `/api/list`; if 401, enables auth mode
- **WASM crypto integration** — `initCrypto()` fetches `/crypto.wasm`, `initKeys()` generates or loads device seed from `localStorage`, derives public key via WASM
- **Auth banner** — shown when user is not authorized, with access request form (name + optional message)
- **Pending state** — after submitting request, shows "waiting for operator approval" with 5-second polling; auto-refreshes when granted
- **Device key display** — shows truncated public key hex in header when auth is active
- **JWK migration** — handles legacy `localStorage.deviceKey` (JWK format) by extracting the `d` parameter as seed

### Admin Page (`ui_html.rs` — `DATASTORE_ADMIN_HTML`)

Owner administration page served at `/admin`:

- **Owner key upload** — file input for `key.json`, loads secret/public key hex, derives via WASM to verify, test call to `/api/auth/requests` to confirm ownership
- **Pending access requests table** — name, message, key (truncated), grant/deny buttons
- **Authorized keys table** — name (label), key (truncated), revoke button
- **Manual grant form** — input for 64-char hex public key + optional name
- **Name disambiguation** — when multiple entries share the same name, appends `(key_prefix)` suffix
- **`ownerAuthFetch()`** — signs all admin API calls with `DatastoreAction::Access`

### WASM Ed25519 Crypto (`crates/crypto-wasm/`)

New `no_std` Rust crate compiled to `wasm32-unknown-unknown`:

- **`Cargo.toml`** — `swactor-crypto-wasm`, `cdylib` crate type, depends on `ed25519-dalek` (no default features)
- **`src/lib.rs`** — Three exported functions:
  - `buffer_ptr()` → pointer to 8192-byte shared buffer
  - `get_public_key()` — reads 32-byte seed from `BUF[0..32]`, writes public key to `BUF[32..64]`
  - `ed25519_sign(msg_len)` — reads seed from `BUF[0..32]`, message from `BUF[128..128+msg_len]`, writes 64-byte signature to `BUF[64..128]`
- **`crypto_wasm.wasm`** — pre-built binary embedded in the datastore via `include_bytes!("crypto_wasm.wasm")`
- Served at `/crypto.wasm` endpoint (ungated)
- Replaces the earlier Web Crypto API approach — Web Crypto's Ed25519 support is inconsistent across browsers; WASM provides deterministic behavior using the same `ed25519-dalek` crate as the Rust backend

### Expanded GatewayActor (`actors/gateway.rs`)

New message handlers beyond the original `Authorize`/`HandleSignedRequest`/`CheckConnection`/`Grant`/`Revoke`:

- **`VerifySignature`** — calls `check_signature_only()` (no ACL check). Used for access request submissions where the caller needs to prove key ownership without being in the ACL.
- **`SubmitAccessRequest`** — stores `AccessRequestInfo { key, name, message, requested_at }` in `pending_requests: HashMap<NodeId, AccessRequestInfo>`.
- **`ListAccessRequests`** — owner-only; returns all pending requests.
- **`DenyAccessRequest`** — owner-only; removes a pending request.
- **`ListAuthorizedKeys`** — owner-only; returns `Vec<AuthorizedKeyInfo>` with labels.

Grant now resolves labels: when granting a key that has a pending request, the request's `name` field becomes the key's label (unless an explicit label is provided).

### Expanded Auth Types (`auth.rs`)

- **`AccessRequestInfo`** — `{ key: NodeId, name: String, message: String, requested_at: u64 }`
- **`AuthorizedKeyInfo`** — `{ key: NodeId, label: String }`
- **`DatastoreAction::Access`** — new variant for browser-originated requests that prove identity without binding to specific content. The browser uses `Access` for all operations (auth is at the HTTP layer).
- **`key_labels: HashMap<String, String>`** added to `AccessControlList` — maps hex public key to human-readable name. Populated by `grant()`, removed by `revoke()`.
- **`check_signature_only()`** on `AuthzEngine` — verifies signature, timestamp, and nonce but skips ACL check.
- **`authorized_key_list()`** on `AuthzEngine` — returns all authorized keys with their labels.

### Storage Persistence (`storage/mod.rs`, `storage/in_memory.rs`)

Extended `StorageBackend` trait with entry persistence:

- **`write_entry()`** / **`read_entry()`** / **`delete_entry()`** / **`list_entries()`** — persist `ObjectEntry` JSON to disk
- **`FilesystemBackend`** layout extended:
  ```
  {root}/
  ├── chunks/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
  ├── manifests/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
  └── entries/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
  ```
- **`BlobStoreMsg::WriteEntry`** / **`DeleteEntry`** — fire-and-forget messages for entry persistence
- **`BlobStoreMsg::LoadAll`** — startup bulk-load of all entries + their manifests
- **`MetadataMsg::BulkLoad`** — injects loaded entries into MetadataActor's index
- **`store_node.rs` startup sequence** — sends `LoadAll` to BlobStoreActor, polls for `LoadedAll` response, sends `BulkLoad` to MetadataActor

### Expanded CLI (`store_cli.rs`)

Full CRUD + auth management subcommands:

| Subcommand | Auth | Description |
|------------|------|-------------|
| `put <path> [--name]` | `--key` signs `DatastoreAction::Put` | Upload a file |
| `get <hash> [--output]` | `--key` signs `DatastoreAction::Get` | Metadata or download |
| `delete <hash>` | `--key` signs `DatastoreAction::Delete` | Delete an object |
| `list [--name] [--all]` | `--key` signs `DatastoreAction::List` | List objects |
| `status` | Never signed | Node identity |
| `grant <key_or_name> [--name]` | `--key` signs `Access` | Authorize a key (owner-only) |
| `revoke <key_or_name>` | `--key` signs `Access` | Revoke a key (owner-only) |
| `requests` | `--key` signs `Access` | List pending access requests |
| `keys` | `--key` signs `Access` | List authorized keys |
| `deny <key_or_name>` | `--key` signs `Access` | Deny a pending request |

**Name resolution:** `grant`, `revoke`, and `deny` accept either a 64-char hex key or a human-readable name. When given a name, the CLI fetches the relevant list from the API and resolves the name to a key. Disambiguated names (`"alice (c9d0e1f2)"`) are supported.

### xtask (`xtask/src/main.rs`)

Development task runner with three new subcommands beyond the existing `test`:

- **`cargo xtask node`** — builds and runs `swactor-store-node`. Flags: `--port`, `--storage-path`, `--auth` (default: true), `--auth-dir`. Builds with `--features node` first, then runs the binary directly (not via `cargo run`) to avoid SIGINT issues. Ignores SIGINT in the xtask process so the child handles Ctrl-C.
- **`cargo xtask cli`** — builds and runs `swactor-store`. Flags: `--url`, `--key`. Auto-detects `./auth/owner.key.json` if present. Passes extra args through.
- **`cargo xtask wasm`** — builds `swactor-crypto-wasm` for `wasm32-unknown-unknown --release`, copies the output to `crates/datastore/src/crypto_wasm.wasm`, optionally runs `wasm-strip`.
- **`config.toml` support** — reads `xtask/config.toml` for default values (node port, storage path, auth settings, CLI url/key).

**`xtask/Cargo.toml`** — added `toml`, `serde`, `libc` dependencies.

### HTTP API Expansion (`api.rs`)

New endpoints:

| Method | Path | Auth | Handler |
|--------|------|------|---------|
| `POST` | `/api/auth/grant?key=<hex>[&name=<label>]` | Owner (full check) | `handle_auth_grant` |
| `POST` | `/api/auth/revoke?key=<hex>` | Owner (full check) | `handle_auth_revoke` |
| `POST` | `/api/auth/request` | Signature-only | `handle_auth_request` |
| `GET`  | `/api/auth/requests` | Owner (full check) | `handle_auth_requests_list` |
| `GET`  | `/api/auth/keys` | Owner (full check) | `handle_auth_keys_list` |
| `POST` | `/api/auth/deny?key=<hex>` | Owner (full check) | `handle_auth_deny` |
| `GET`  | `/` | None | Browser UI |
| `GET`  | `/admin` | None | Admin page |
| `GET`  | `/crypto.wasm` | None | WASM module |

New internal functions:
- `check_auth_identity()` — like `check_auth()` but returns the caller's `NodeId` (needed for grant/revoke to identify the requester).
- `check_auth_signature_only()` — verifies signature without ACL check (for access request submission).
- `respond_wasm()`, `respond_admin_html()` — serve the new static assets.
- `CRYPTO_WASM` constant — `include_bytes!("crypto_wasm.wasm")`.

### DatastoreResponse Expansion (`messages.rs`)

New response variants:
- `AccessRequests { requests: Vec<AccessRequestInfo> }` — response to `ListAccessRequests`
- `AuthorizedKeys { keys: Vec<AuthorizedKeyInfo> }` — response to `ListAuthorizedKeys`
- `LoadedAll { entries: Vec<(ObjectEntry, ObjectManifest)> }` — response to `BlobStoreMsg::LoadAll`

---

## Key File Format

`owner.key.json` / any client `key.json`:

```json
{
  "version": 1,
  "secret_key": "...64 hex chars (32 bytes)...",
  "public_key": "...64 hex chars (32 bytes)...",
  "created_at": "2026-02-15T12:00:00Z"
}
```

Generated by the node on first `--auth` run. The CLI reads it via `--key`. The admin page uploads it for authentication. The browser generates a simpler device seed (32 random bytes stored as hex in `localStorage.deviceKeySeed`).

---

## Test Summary

| Test File | Count | What |
|-----------|-------|------|
| `auth_scenario_tests.rs` | 10 | AuthzEngine: signing, verification, timestamp, nonce, ACL, grant/revoke |
| `acl_persistence_tests.rs` | 2 | ACL JSON round-trip, create-on-missing |
| `gateway_tests.rs` | 4 | GatewayActor: connection check, signed request flow, denial |
| `http_auth_integration.rs` | 1 | Full HTTP stack: owner PUT/GET/LIST/DELETE, stranger 403, no-header 401 |
| **Auth total** | **17** | |

Pre-existing datastore tests (blob_store, metadata, datastore_node, chunking, gc, storage, transfer, multi_node, api_integration, dashboard_integration) continue to pass.

---

## Design Decisions

1. **WASM Ed25519 over Web Crypto** — Web Crypto's Ed25519 support varies by browser (Safari lacking, Firefox gated behind flags as of early 2026). A WASM module using `ed25519-dalek` with `no_std` gives deterministic, cross-browser behavior and byte-level compatibility with the Rust backend. The compiled module is ~27KB stripped.

2. **`DatastoreAction::Access` for browser ops** — The browser signs a lightweight `Access` action for every API call rather than constructing per-operation payloads. This simplifies the browser JS (no need to compute content hashes client-side) while still proving identity. The actual data operations are auth-gated at the HTTP layer.

3. **Signature-only check for access requests** — `POST /api/auth/request` uses `check_auth_signature_only()` which verifies the signature/timestamp/nonce but skips the ACL check. This allows an unauthorized user to prove key ownership when requesting access, without being in the ACL yet.

4. **Key labels in ACL** — `key_labels: HashMap<String, String>` maps hex public key to human-readable name. Labels are set on grant (from the access request's `name` field or an explicit `--name` flag) and removed on revoke. This enables the admin page and CLI to show meaningful names instead of raw hex keys.

5. **Access request flow** — Instead of requiring out-of-band key exchange, browser users can submit an access request with their name and a message. The request is stored in-memory in the GatewayActor's `pending_requests`. The owner can grant or deny from the admin page or CLI. On grant, the pending request is removed and its name becomes the key label.

6. **Entry persistence** — `StorageBackend` trait extended with `write_entry()`/`read_entry()`/`delete_entry()`/`list_entries()`. The `FilesystemBackend` stores entries as JSON files in a `entries/` directory with the same 2-level hex sharding as chunks. On startup, `BlobStoreMsg::LoadAll` reads all entries and their manifests, then `MetadataMsg::BulkLoad` injects them into the MetadataActor's index. This means stored objects survive node restarts.

7. **xtask builds then execs** — `cargo xtask node` and `cargo xtask cli` build the binary first, then exec it directly (not via `cargo run`). This avoids cargo sitting in the process chain and dying from SIGINT before the node finishes its shutdown sequence.

8. **Status endpoint stays open** — `/api/status`, `/`, `/admin`, and `/crypto.wasm` are never auth-gated. Status enables health checks; the UI/admin pages need to be loadable before authentication; the WASM module is needed to perform authentication.

9. **ACL persisted to auth-dir** — The ACL is stored at `<auth-dir>/acl.json` (default: `./auth/acl.json`), not inside the storage path. This separates auth config from data storage.

10. **CLI name resolution** — `grant`, `revoke`, and `deny` accept human-readable names in addition to hex keys. When given a name, the CLI fetches the pending requests or authorized keys list from the API and resolves the name. If multiple entries match, it prints disambiguated names (e.g., `"alice (c9d0e1f2)"`) and asks the user to re-run.

---

## File Inventory

| File | What |
|------|------|
| `crates/shared-types/` | `ContentHash` crate (breaks dependency cycles) |
| `crates/crypto-wasm/Cargo.toml` | WASM crypto crate config |
| `crates/crypto-wasm/src/lib.rs` | `no_std` Ed25519 sign/derive/buffer exports |
| `crates/datastore/src/crypto_wasm.wasm` | Pre-built WASM binary (embedded via `include_bytes!`) |
| `crates/datastore/Cargo.toml` | Feature flags (`node`/`cli`), dependencies |
| `crates/datastore/src/auth.rs` | Auth engine, ACL, signing, verification, access request types |
| `crates/datastore/src/actors/gateway.rs` | GatewayActor — auth enforcement + access request management |
| `crates/datastore/src/actors/blob_store.rs` | BlobStoreActor — entry persistence, LoadAll |
| `crates/datastore/src/actors/metadata.rs` | MetadataActor — entry persistence writes, BulkLoad |
| `crates/datastore/src/messages.rs` | GatewayMsg, BlobStoreMsg (WriteEntry/DeleteEntry/LoadAll), DatastoreResponse extensions |
| `crates/datastore/src/api.rs` | HTTP API — auth endpoints, WASM/admin serving, auth checking functions |
| `crates/datastore/src/ui_html.rs` | Browser UI (access page) + Admin page HTML/CSS/JS |
| `crates/datastore/src/storage/mod.rs` | StorageBackend trait (entry methods), FilesystemBackend |
| `crates/datastore/src/storage/in_memory.rs` | InMemoryBackend (entry methods) |
| `crates/datastore/src/bin/store_node.rs` | Node binary — `--auth`, `--auth-dir`, keypair mgmt, gateway spawn, bulk-load |
| `crates/datastore/src/bin/store_cli.rs` | CLI binary — `--key`, all subcommands, name resolution |
| `xtask/Cargo.toml` | xtask dependencies (toml, serde, libc) |
| `xtask/src/main.rs` | `node`, `cli`, `wasm` subcommands, `config.toml` support |
| `docs/datastore/DATASTORE_AUTH.md` | Auth specification document |
| `tests/auth_scenario_tests.rs` | 10 AuthzEngine scenario tests |
| `tests/acl_persistence_tests.rs` | 2 ACL persistence tests |
| `tests/gateway_tests.rs` | 4 GatewayActor tests |
| `tests/http_auth_integration.rs` | 1 full-stack HTTP auth integration test |
