//! GatewayActor — auth enforcement point for the datastore.
//!
//! Sits in front of the `DatastoreNode` coordinator. All external requests
//! pass through the gateway, which checks authorization before forwarding
//! to the internal actors.
//!
//! ```text
//! External Client → GatewayActor → DatastoreNode → MetadataActor/BlobStoreActor
//!                   (auth check)   (dispatch)       (auth-unaware)
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::auth::{AccessRequestInfo, AuthzEngine, AuthzResult, DatastoreAction, DeniedReason};
use crate::messages::{DatastoreNodeMsg, DatastoreResponse, GatewayMsg};
use swactor_transport::NodeId;

/// The auth gateway actor wrapping an `AuthzEngine`.
pub struct GatewayActor {
    engine: AuthzEngine,
    datastore_node: ActorAddress,
    acl_path: Option<PathBuf>,
    pending_requests: HashMap<NodeId, AccessRequestInfo>,
}

impl GatewayActor {
    pub fn new(
        engine: AuthzEngine,
        datastore_node: ActorAddress,
        acl_path: Option<PathBuf>,
    ) -> Self {
        Self {
            engine,
            datastore_node,
            acl_path,
            pending_requests: HashMap::new(),
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn persist_acl(&self) {
        if let Some(ref path) = self.acl_path {
            let _ = self.engine.acl.save(path);
        }
    }

    fn handle_authorize(&mut self, ctx: &Ctx, request: crate::auth::SignedRequest, reply_to: ActorAddress) {
        let now = Self::now_secs();
        match self.engine.check_signed_request(&request, now) {
            AuthzResult::Allowed => {
                let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
            }
            AuthzResult::Denied(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_signed_request(&mut self, ctx: &Ctx, request: crate::auth::SignedRequest, reply_to: ActorAddress) {
        let now = Self::now_secs();
        match self.engine.check_signed_request(&request, now) {
            AuthzResult::Allowed => {
                let msg = action_to_node_msg(request.payload.action, reply_to);
                let _ = ctx.send(self.datastore_node, msg);
            }
            AuthzResult::Denied(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_check_connection(&self, ctx: &Ctx, node_id: NodeId, reply_to: ActorAddress) {
        match self.engine.check_node(&node_id) {
            AuthzResult::Allowed => {
                let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
            }
            AuthzResult::Denied(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_grant(&mut self, ctx: &Ctx, requester: NodeId, key: NodeId, label: Option<String>, reply_to: ActorAddress) {
        // If the key has a pending request, use its name as the label (unless an explicit label was provided)
        let resolved_label = label.or_else(|| {
            self.pending_requests.remove(&key).map(|req| req.name)
        });
        match self.engine.grant(&requester, key, resolved_label) {
            Ok(()) => {
                self.persist_acl();
                let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
            }
            Err(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_revoke(&mut self, ctx: &Ctx, requester: NodeId, key: NodeId, reply_to: ActorAddress) {
        match self.engine.revoke(&requester, key) {
            Ok(()) => {
                self.persist_acl();
                let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
            }
            Err(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_verify_signature(&mut self, ctx: &Ctx, request: crate::auth::SignedRequest, reply_to: ActorAddress) {
        let now = Self::now_secs();
        match self.engine.check_signature_only(&request, now) {
            AuthzResult::Allowed => {
                let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
            }
            AuthzResult::Denied(reason) => {
                let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason });
            }
        }
    }

    fn handle_submit_access_request(&mut self, ctx: &Ctx, key: NodeId, name: String, message: String, reply_to: ActorAddress) {
        let info = AccessRequestInfo {
            key,
            name,
            message,
            requested_at: Self::now_secs(),
        };
        self.pending_requests.insert(key, info);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_list_access_requests(&self, ctx: &Ctx, requester: NodeId, reply_to: ActorAddress) {
        if requester != self.engine.acl.owner {
            let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason: DeniedReason::NotAuthorized });
            return;
        }
        let requests: Vec<AccessRequestInfo> = self.pending_requests.values().cloned().collect();
        let _ = ctx.send(reply_to, DatastoreResponse::AccessRequests { requests });
    }

    fn handle_deny_access_request(&mut self, ctx: &Ctx, requester: NodeId, key: NodeId, reply_to: ActorAddress) {
        if requester != self.engine.acl.owner {
            let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason: DeniedReason::NotAuthorized });
            return;
        }
        self.pending_requests.remove(&key);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_list_authorized_keys(&self, ctx: &Ctx, requester: NodeId, reply_to: ActorAddress) {
        if requester != self.engine.acl.owner {
            let _ = ctx.send(reply_to, DatastoreResponse::Denied { reason: DeniedReason::NotAuthorized });
            return;
        }
        let keys = self.engine.authorized_key_list();
        let _ = ctx.send(reply_to, DatastoreResponse::AuthorizedKeys { keys });
    }
}

impl ActorInterface for GatewayActor {
    type Incoming = GatewayMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: GatewayMsg) {
        match msg {
            GatewayMsg::HandleSignedRequest { request, reply_to } => {
                self.handle_signed_request(ctx, request, reply_to);
            }
            GatewayMsg::CheckConnection { node_id, reply_to } => {
                self.handle_check_connection(ctx, node_id, reply_to);
            }
            GatewayMsg::Grant {
                requester,
                key,
                label,
                reply_to,
            } => {
                self.handle_grant(ctx, requester, key, label, reply_to);
            }
            GatewayMsg::Revoke {
                requester,
                key,
                reply_to,
            } => {
                self.handle_revoke(ctx, requester, key, reply_to);
            }
            GatewayMsg::Authorize { request, reply_to } => {
                self.handle_authorize(ctx, request, reply_to);
            }
            GatewayMsg::VerifySignature { request, reply_to } => {
                self.handle_verify_signature(ctx, request, reply_to);
            }
            GatewayMsg::SubmitAccessRequest { key, name, message, reply_to } => {
                self.handle_submit_access_request(ctx, key, name, message, reply_to);
            }
            GatewayMsg::ListAccessRequests { requester, reply_to } => {
                self.handle_list_access_requests(ctx, requester, reply_to);
            }
            GatewayMsg::DenyAccessRequest { requester, key, reply_to } => {
                self.handle_deny_access_request(ctx, requester, key, reply_to);
            }
            GatewayMsg::ListAuthorizedKeys { requester, reply_to } => {
                self.handle_list_authorized_keys(ctx, requester, reply_to);
            }
            GatewayMsg::NonceGcTick => {
                self.engine.gc_nonces(Self::now_secs());
            }
        }
    }
}

/// Translate a `DatastoreAction` into the corresponding `DatastoreNodeMsg`.
fn action_to_node_msg(action: DatastoreAction, reply_to: ActorAddress) -> DatastoreNodeMsg {
    match action {
        DatastoreAction::Get { content_hash } => DatastoreNodeMsg::Get {
            content_hash,
            reply_to,
        },
        DatastoreAction::Delete { content_hash } => DatastoreNodeMsg::Delete {
            content_hash,
            reply_to,
        },
        DatastoreAction::List { name_filter } => DatastoreNodeMsg::List {
            name_filter,
            all: false,
            reply_to,
        },
        DatastoreAction::Put {
            name,
            content_hash: _,
            size_bytes: _,
            tags,
        } => {
            // Put via signed request is an authorization of the operation.
            // The actual data upload happens separately. We forward as a
            // zero-data Put — the DatastoreNode will handle the metadata.
            // In the full flow, the data is uploaded separately and the
            // signed request only authorizes it.
            DatastoreNodeMsg::Put {
                data: Vec::new(),
                name,
                tags,
                reply_to,
            }
        }
        DatastoreAction::Access => {
            // Access is a lightweight identity proof — no content operation.
            // Forward as Status to return a valid response to the caller.
            DatastoreNodeMsg::Status { reply_to }
        }
    }
}
