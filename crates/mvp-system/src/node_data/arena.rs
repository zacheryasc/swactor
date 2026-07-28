//! MVP arena-manager adapter over reusable data-plane arena contracts.

use std::time::Duration;

use datastream::Record;
use serde::{Deserialize, Serialize};

pub use data_plane::arena::{
    ArenaCommand, ArenaConfig, ArenaEvent, ArenaFault, ArenaRequest, LayoutPointer, LeaseRequestId,
    LeaseRing, NodeId, QuiescenceProof, RingId, RingLayout, RingLease, RingLeaseRejection,
    RingReleaseRejection, RingSpec,
};

pub const ARENA_SAMPLE_CHANNEL: &str = "mvp.arena";
pub const ARENA_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArenaSample {
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub capacity_bytes: u64,
    pub live_bytes: u64,
    pub free_bytes: u64,
    pub active_leases: u64,
    pub pending_leases: u64,
    pub largest_free_range_bytes: u64,
    pub allocation_failures_total: u64,
    pub release_failures_total: u64,
}

impl Record for ArenaSample {
    const CHANNEL: &'static str = ARENA_SAMPLE_CHANNEL;
}

impl From<data_plane::arena::ArenaSnapshot> for ArenaSample {
    fn from(snapshot: data_plane::arena::ArenaSnapshot) -> Self {
        Self {
            seq: snapshot.seq,
            sample_unix_ms: snapshot.sample_unix_ms,
            capacity_bytes: snapshot.capacity_bytes,
            live_bytes: snapshot.live_bytes,
            free_bytes: snapshot.free_bytes,
            active_leases: snapshot.active_leases,
            pending_leases: snapshot.pending_leases,
            largest_free_range_bytes: snapshot.largest_free_range_bytes,
            allocation_failures_total: snapshot.allocation_failures_total,
            release_failures_total: snapshot.release_failures_total,
        }
    }
}

pub struct ArenaManager {
    inner: data_plane::arena::ArenaManager,
}

impl ArenaManager {
    pub fn boot(config: ArenaConfig) -> Result<Self, ArenaFault> {
        data_plane::arena::ArenaManager::boot(config).map(|inner| Self { inner })
    }

    pub fn request(&mut self, request: ArenaRequest) -> Vec<ArenaEvent> {
        self.inner.request(request)
    }

    pub fn live_leases(&self) -> &[RingLease] {
        self.inner.live_leases()
    }

    pub fn lookup_lease(&self, ring_id: RingId) -> Option<&RingLease> {
        self.inner.lookup_lease(ring_id)
    }

    pub fn sample(&self, seq: u64) -> ArenaSample {
        self.inner.sample(seq).into()
    }

    #[cfg(target_os = "linux")]
    pub fn arena_fd(&self) -> std::os::fd::RawFd {
        self.inner.arena_fd()
    }

    #[cfg(target_os = "linux")]
    pub fn write_arena(&self, offset: u64, bytes: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write_arena(offset, bytes)
    }

    #[cfg(target_os = "linux")]
    pub fn read_arena(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        self.inner.read_arena(offset, len)
    }
}

pub struct ArenaManagerHarness {
    manager: ArenaManager,
    events: Vec<ArenaEvent>,
    commands: Vec<ArenaCommand>,
}

impl ArenaManagerHarness {
    pub fn boot(config: ArenaConfig) -> Result<Self, ArenaFault> {
        Ok(Self {
            manager: ArenaManager::boot(config)?,
            events: Vec::new(),
            commands: Vec::new(),
        })
    }

    pub fn request(&mut self, request: ArenaRequest) {
        self.events.extend(self.manager.request(request));
    }

    pub fn events(&self) -> &[ArenaEvent] {
        &self.events
    }

    pub fn commands(&self) -> &[ArenaCommand] {
        &self.commands
    }

    pub fn live_leases(&self) -> &[RingLease] {
        self.manager.live_leases()
    }

    pub fn lookup_lease(&self, ring_id: RingId) -> Option<&RingLease> {
        self.manager.lookup_lease(ring_id)
    }

    pub fn sample(&self, seq: u64) -> ArenaSample {
        self.manager.sample(seq)
    }
}
