//! Pool coordinator actor — placement-aware CRUD facade.
//!
//! Owns an `Arc<Mutex<PoolDisseminator>>` for query access and delegates
//! storage operations to the co-located `DatastoreNode` actor.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use distribution::types::NodeId;
use shared_types::ContentHash;
use shared_types::pool::PoolConfig;

use crate::messages::{DatastoreNodeMsg, DatastoreResponse};
use super::disseminator::PoolDisseminator;
use super::messages::PoolCoordinatorMsg;

/// The pool coordinator actor.
pub struct PoolCoordinator {
    node_id: NodeId,
    pool_config: PoolConfig,
    disseminator: Arc<Mutex<PoolDisseminator>>,

    // Co-located actor addresses
    datastore_addr: ActorAddress,

    tick_count: u64,
}

impl PoolCoordinator {
    pub fn new(
        node_id: NodeId,
        pool_config: PoolConfig,
        disseminator: Arc<Mutex<PoolDisseminator>>,
        datastore_addr: ActorAddress,
    ) -> Self {
        Self {
            node_id,
            pool_config,
            disseminator,
            datastore_addr,
            tick_count: 0,
        }
    }

    /// Access the shared disseminator.
    pub fn disseminator(&self) -> &Arc<Mutex<PoolDisseminator>> {
        &self.disseminator
    }

    fn cluster_size(&self) -> usize {
        let d = self.disseminator.lock().unwrap();
        d.member_count().max(1)
    }

    // ─── Message handlers ──────────────────────────────────────────────

    fn handle_pool_put(
        &self,
        ctx: &Ctx,
        data: Vec<u8>,
        name: Option<String>,
        tags: BTreeMap<String, String>,
        reply_to: ActorAddress,
    ) {
        // Delegate to local DatastoreNode for now.
        // Future: check capacity and redirect to best node.
        let _ = ctx.send(
            self.datastore_addr,
            DatastoreNodeMsg::Put {
                data,
                name,
                tags,
                reply_to,
            },
        );

        // Note: content announcement happens after PutOk is received.
        // For now, caller is responsible for announcing content via PoolTick
        // or a future PutOk callback.
    }

    fn handle_pool_get(
        &self,
        ctx: &Ctx,
        content_hash: ContentHash,
        reply_to: ActorAddress,
    ) {
        // Try local first via DatastoreNode
        let _ = ctx.send(
            self.datastore_addr,
            DatastoreNodeMsg::Get {
                content_hash,
                reply_to,
            },
        );

        // Future: if local not found, use disseminator.locate_content()
        // to fetch from a specific peer instead of fan-out.
    }

    fn handle_pool_delete(
        &self,
        ctx: &Ctx,
        content_hash: ContentHash,
        reply_to: ActorAddress,
    ) {
        // Delete locally
        let _ = ctx.send(
            self.datastore_addr,
            DatastoreNodeMsg::Delete {
                content_hash,
                reply_to,
            },
        );

        // Announce tombstone via gossip
        let cluster_size = self.cluster_size();
        self.disseminator
            .lock()
            .unwrap()
            .remove_content(content_hash, cluster_size);
    }

    fn handle_pool_list(
        &self,
        ctx: &Ctx,
        name_filter: Option<String>,
        reply_to: ActorAddress,
    ) {
        let _ = ctx.send(
            self.datastore_addr,
            DatastoreNodeMsg::List {
                name_filter,
                all: false,
                reply_to,
            },
        );
    }

    fn handle_pool_status(&self, ctx: &Ctx, reply_to: ActorAddress) {
        let d = self.disseminator.lock().unwrap();
        let (total_bytes, used_bytes) = d.pool_capacity_summary();
        let members: Vec<String> = d
            .active_members()
            .iter()
            .map(|id| id.0.iter().map(|b| format!("{b:02x}")).collect())
            .collect();

        let member_count = d.member_count();
        let content_count = d.content_count();
        drop(d);

        let json = serde_json::json!({
            "pool_name": self.pool_config.pool_name,
            "pool_id": self.pool_config.pool_id.to_hex(),
            "member_count": member_count,
            "content_count": content_count,
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "members": members,
        });

        let _ = ctx.send(
            reply_to,
            DatastoreResponse::PoolStatus {
                json: json.to_string(),
            },
        );
    }

    fn handle_join_pool(&mut self, ctx: &Ctx, reply_to: ActorAddress) {
        let cluster_size = self.cluster_size();
        let mut d = self.disseminator.lock().unwrap();

        if !d.is_node_authorized(&self.node_id) {
            let _ = ctx.send(
                reply_to,
                DatastoreResponse::Error {
                    reason: "not authorized to join pool".into(),
                },
            );
            return;
        }

        d.join(cluster_size);
        d.announce_capacity(self.pool_config.capacity_bytes, 0, cluster_size);
        drop(d);

        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_leave_pool(&mut self, ctx: &Ctx, reply_to: ActorAddress) {
        let cluster_size = self.cluster_size();
        self.disseminator.lock().unwrap().leave(cluster_size);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_grant_access(&mut self, ctx: &Ctx, target: NodeId, reply_to: ActorAddress) {
        let cluster_size = self.cluster_size();
        self.disseminator
            .lock()
            .unwrap()
            .grant_access(target, cluster_size);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_revoke_access(&mut self, ctx: &Ctx, target: NodeId, reply_to: ActorAddress) {
        let cluster_size = self.cluster_size();
        self.disseminator
            .lock()
            .unwrap()
            .revoke_access(target, cluster_size);
        let _ = ctx.send(reply_to, DatastoreResponse::Bool(true));
    }

    fn handle_pool_tick(&mut self) {
        self.tick_count += 1;
        // Periodic capacity re-announcement (every 100 ticks)
        if self.tick_count % 100 == 0 {
            let cluster_size = self.cluster_size();
            self.disseminator.lock().unwrap().announce_capacity(
                self.pool_config.capacity_bytes,
                0, // TODO: query actual usage from BlobStore
                cluster_size,
            );
        }
    }
}

impl ActorInterface for PoolCoordinator {
    type Incoming = PoolCoordinatorMsg;
    type Response = DatastoreResponse;

    fn handle(&mut self, ctx: &Ctx, msg: PoolCoordinatorMsg) {
        match msg {
            PoolCoordinatorMsg::PoolPut {
                data,
                name,
                tags,
                reply_to,
            } => self.handle_pool_put(ctx, data, name, tags, reply_to),
            PoolCoordinatorMsg::PoolGet {
                content_hash,
                reply_to,
            } => self.handle_pool_get(ctx, content_hash, reply_to),
            PoolCoordinatorMsg::PoolDelete {
                content_hash,
                reply_to,
            } => self.handle_pool_delete(ctx, content_hash, reply_to),
            PoolCoordinatorMsg::PoolList {
                name_filter,
                reply_to,
            } => self.handle_pool_list(ctx, name_filter, reply_to),
            PoolCoordinatorMsg::PoolStatus { reply_to } => {
                self.handle_pool_status(ctx, reply_to)
            }
            PoolCoordinatorMsg::JoinPool { reply_to } => {
                self.handle_join_pool(ctx, reply_to)
            }
            PoolCoordinatorMsg::LeavePool { reply_to } => {
                self.handle_leave_pool(ctx, reply_to)
            }
            PoolCoordinatorMsg::GrantPoolAccess { target, reply_to } => {
                self.handle_grant_access(ctx, target, reply_to)
            }
            PoolCoordinatorMsg::RevokePoolAccess { target, reply_to } => {
                self.handle_revoke_access(ctx, target, reply_to)
            }
            PoolCoordinatorMsg::PoolTick => self.handle_pool_tick(),
        }
    }
}
