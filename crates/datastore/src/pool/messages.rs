//! Messages for the `PoolCoordinator` actor.

use std::collections::BTreeMap;

use distribution::types::NodeId;
use shared_types::ContentHash;

/// Messages handled by the `PoolCoordinator` actor.
#[derive(Debug, Clone)]
pub enum PoolCoordinatorMsg {
    // ── User-facing operations ──────────────────────────────────────────
    /// Store data in the pool (placement-aware).
    PoolPut {
        data: Vec<u8>,
        name: Option<String>,
        tags: BTreeMap<String, String>,
        reply_to: swactor::actor::ActorAddress,
    },
    /// Retrieve data from the pool (location-aware).
    PoolGet {
        content_hash: ContentHash,
        reply_to: swactor::actor::ActorAddress,
    },
    /// Delete data from the pool.
    PoolDelete {
        content_hash: ContentHash,
        reply_to: swactor::actor::ActorAddress,
    },
    /// List objects in the pool.
    PoolList {
        name_filter: Option<String>,
        reply_to: swactor::actor::ActorAddress,
    },
    /// Pool status (members, capacity, content count).
    PoolStatus {
        reply_to: swactor::actor::ActorAddress,
    },

    // ── Pool lifecycle ──────────────────────────────────────────────────
    /// Join the pool.
    JoinPool {
        reply_to: swactor::actor::ActorAddress,
    },
    /// Leave the pool.
    LeavePool {
        reply_to: swactor::actor::ActorAddress,
    },

    // ── Auth management ─────────────────────────────────────────────────
    /// Grant a node access to the pool.
    GrantPoolAccess {
        target: NodeId,
        reply_to: swactor::actor::ActorAddress,
    },
    /// Revoke a node's access to the pool.
    RevokePoolAccess {
        target: NodeId,
        reply_to: swactor::actor::ActorAddress,
    },

    // ── Periodic ────────────────────────────────────────────────────────
    /// Periodic tick: announce capacity, drive dissemination.
    PoolTick,
}

/// Pool-specific response variants.
#[derive(Debug, Clone)]
pub enum PoolResponse {
    /// Pool status snapshot.
    PoolStatus {
        pool_name: String,
        pool_id_hex: String,
        member_count: usize,
        content_count: usize,
        total_bytes: u64,
        used_bytes: u64,
        members: Vec<String>,
    },
}
