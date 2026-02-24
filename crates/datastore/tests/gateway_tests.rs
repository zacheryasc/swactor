//! Actor-level tests for the GatewayActor.
//!
//! Uses the swactor runtime to spawn GatewayActor + DatastoreNode and verify
//! that authorized requests flow through while unauthorized ones are denied.

mod common;

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use common::{spawn_blob_store, spawn_metadata, test_runtime, tick_until_recv};

use swactor_datastore::crypto::Keypair;
use swactor_datastore::content_hash::ContentHash;
use swactor_datastore::actors::{DatastoreNode, GatewayActor};
use swactor_datastore::auth::{
    sign_request, AccessControlList, AuthzEngine, DatastoreAction, DeniedReason,
    SignedRequestPayload,
};
use swactor_datastore::messages::{DatastoreResponse, GatewayMsg};
use swactor_datastore::types::DatastoreConfig;

struct GatewayHarness {
    rt: swactor::runtime::Runtime,
    gateway: swactor::actor::ActorAddress,
    inbox: swactor::runtime::Inbox<DatastoreResponse>,
    owner_kp: Keypair,
}

impl GatewayHarness {
    fn new() -> Self {
        let owner_kp = Keypair::generate();
        let rt = test_runtime();

        let blob_store = spawn_blob_store(&rt);
        let node_id = owner_kp.node_id();
        let metadata = spawn_metadata(&rt, node_id);

        let mut config = DatastoreConfig::default();
        config.chunk_size = 64;
        let datastore_node = rt
            .spawn(DatastoreNode::new(node_id, blob_store, metadata, config))
            .unwrap();

        let acl = AccessControlList {
            owner: owner_kp.node_id(),
            authorized_keys: HashSet::new(),
            key_labels: HashMap::new(),
        };
        let engine = AuthzEngine::new(acl);
        let gateway = rt
            .spawn(GatewayActor::new(engine, datastore_node, None))
            .unwrap();

        let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

        // Let actors initialize
        for _ in 0..3 {
            rt.tick();
        }

        Self {
            rt,
            gateway,
            inbox,
            owner_kp,
        }
    }

    fn reply_addr(&self) -> swactor::actor::ActorAddress {
        *self.inbox.addr()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Authorized signed GET dispatches and returns result
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn authorized_signed_get_flows_through_to_datastore() {
    let h = GatewayHarness::new();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    // PUT some data first via signed request
    let data = b"gateway test data";
    let content_hash = ContentHash::of(data);

    // Store data by sending directly to datastore through a Put via gateway
    // (For simplicity, store via DatastoreNode first, then GET through gateway)
    // Actually, let's just do a GET for a nonexistent hash — we should get NotFound (not Denied)
    let payload = SignedRequestPayload {
        action: DatastoreAction::Get { content_hash },
        timestamp: now,
        nonce: [10; 16],
    };
    let request = sign_request(&h.owner_kp, payload);

    h.rt.send_to(
        h.gateway,
        GatewayMsg::HandleSignedRequest {
            request,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 30).unwrap();
    // Should get NotFound (authorized, but object doesn't exist) — NOT Denied
    assert!(
        matches!(resp, DatastoreResponse::NotFound),
        "expected NotFound (authorized but missing), got {resp:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Unauthorized signed GET returns Denied
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn unauthorized_signed_get_returns_denied() {
    let h = GatewayHarness::new();
    let stranger_kp = Keypair::generate();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let payload = SignedRequestPayload {
        action: DatastoreAction::Get {
            content_hash: ContentHash::of(b"unauthorized"),
        },
        timestamp: now,
        nonce: [11; 16],
    };
    let request = sign_request(&stranger_kp, payload);

    h.rt.send_to(
        h.gateway,
        GatewayMsg::HandleSignedRequest {
            request,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 30).unwrap();
    assert!(
        matches!(
            resp,
            DatastoreResponse::Denied {
                reason: DeniedReason::NotAuthorized
            }
        ),
        "expected Denied(NotAuthorized), got {resp:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Connection check allows/denies correctly
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn check_connection_allows_owner() {
    let h = GatewayHarness::new();

    h.rt.send_to(
        h.gateway,
        GatewayMsg::CheckConnection {
            node_id: h.owner_kp.node_id(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::Bool(true)),
        "expected Bool(true), got {resp:?}"
    );
}

#[test]
fn check_connection_denies_stranger() {
    let h = GatewayHarness::new();
    let stranger = Keypair::generate().node_id();

    h.rt.send_to(
        h.gateway,
        GatewayMsg::CheckConnection {
            node_id: stranger,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(
            resp,
            DatastoreResponse::Denied {
                reason: DeniedReason::NotAuthorized
            }
        ),
        "expected Denied(NotAuthorized), got {resp:?}"
    );
}
