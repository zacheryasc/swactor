//! Reusable arena-backed ring allocation contracts.

use std::collections::{BTreeMap, VecDeque};
use std::ptr::NonNull;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use telemetry::Record;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArenaSnapshot {
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

impl From<ArenaSnapshot> for ArenaSample {
    fn from(snapshot: ArenaSnapshot) -> Self {
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

pub use crate::ids::{LeaseRequestId, NodeId, RingId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArenaConfig {
    pub node_id: NodeId,
    pub reservation_ceiling: u64,
    pub base_alignment: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingSpec {
    pub header_bytes: u64,
    pub data_bytes: u64,
    pub alignment: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRing {
    pub request_id: LeaseRequestId,
    pub ring_spec: RingSpec,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutPointer {
    NoProcessPointer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingLayout {
    pub start_offset: u64,
    pub header_offset: u64,
    pub data_offset: u64,
    pub end_offset: u64,
    pub data_bytes: u64,
    pub alignment: u64,
    pub pointer: LayoutPointer,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingLease {
    pub request_id: LeaseRequestId,
    pub ring_id: RingId,
    pub node_id: NodeId,
    pub layout: RingLayout,
    pub requested_alignment: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuiescenceProof {
    verified: bool,
}

impl QuiescenceProof {
    pub fn verified() -> Self {
        Self { verified: true }
    }

    pub fn missing() -> Self {
        Self { verified: false }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArenaRequest {
    LeaseRing(LeaseRing),
    CancelLease {
        request_id: LeaseRequestId,
    },
    ReleaseRing {
        ring_id: RingId,
        proof: QuiescenceProof,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArenaFault {
    InvalidReservationCeiling,
    InvalidBaseAlignment,
    BackingUnavailable,
    UnsupportedPlatform,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingLeaseRejection {
    CannotFitWithinCeiling,
    ArenaShuttingDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingReleaseRejection {
    MissingQuiescenceProof,
    UnknownRingId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArenaEvent {
    RingLeased {
        lease: RingLease,
    },
    RingLeaseQueued {
        request_id: LeaseRequestId,
    },
    RingLeaseRejected {
        request_id: LeaseRequestId,
        reason: RingLeaseRejection,
    },
    RingReleased {
        ring_id: RingId,
        start_offset: u64,
        end_offset: u64,
    },
    RingReleaseRejected {
        ring_id: RingId,
        reason: RingReleaseRejection,
    },
    CancelledFreshLeaseReleased {
        request_id: LeaseRequestId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArenaCommand {
    InstallWorkerOrPumpState {
        request_id: LeaseRequestId,
        ring_id: RingId,
        layout: RingLayout,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArenaState {
    Ready,
    ShuttingDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FreeRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueuedLease {
    request: LeaseRing,
    cancelled: bool,
}

pub struct ArenaManager {
    config: ArenaConfig,
    _backing: ArenaBacking,
    state: ArenaState,
    next_ring_id: u64,
    free_ranges: Vec<FreeRange>,
    pending: VecDeque<QueuedLease>,
    live_order: Vec<RingLease>,
    live_index: BTreeMap<RingId, usize>,
    allocation_failures_total: u64,
    release_failures_total: u64,
}

impl ArenaManager {
    pub fn boot(config: ArenaConfig) -> Result<Self, ArenaFault> {
        let backing = ArenaBacking::create(&config)?;
        Ok(Self {
            free_ranges: vec![FreeRange {
                start: 0,
                end: config.reservation_ceiling,
            }],
            config,
            _backing: backing,
            state: ArenaState::Ready,
            next_ring_id: 1,
            pending: VecDeque::new(),
            live_order: Vec::new(),
            live_index: BTreeMap::new(),
            allocation_failures_total: 0,
            release_failures_total: 0,
        })
    }

    pub fn request(&mut self, request: ArenaRequest) -> Vec<ArenaEvent> {
        match request {
            ArenaRequest::LeaseRing(request) => self.lease_ring(request),
            ArenaRequest::CancelLease { request_id } => {
                self.cancel_lease(request_id);
                Vec::new()
            }
            ArenaRequest::ReleaseRing { ring_id, proof } => self.release_ring(ring_id, proof),
            ArenaRequest::Shutdown => self.shutdown(),
        }
    }

    pub fn live_leases(&self) -> &[RingLease] {
        &self.live_order
    }

    pub fn lookup_lease(&self, ring_id: RingId) -> Option<&RingLease> {
        self.live_index
            .get(&ring_id)
            .and_then(|index| self.live_order.get(*index))
    }

    pub fn sample(&self, seq: u64) -> ArenaSnapshot {
        let free_bytes = self
            .free_ranges
            .iter()
            .map(|range| range.end.saturating_sub(range.start))
            .sum::<u64>();
        let largest_free_range_bytes = self
            .free_ranges
            .iter()
            .map(|range| range.end.saturating_sub(range.start))
            .max()
            .unwrap_or(0);
        let pending_leases = self.pending.iter().filter(|lease| !lease.cancelled).count();

        ArenaSnapshot {
            seq,
            sample_unix_ms: unix_ms_now(),
            capacity_bytes: self.config.reservation_ceiling,
            live_bytes: self.config.reservation_ceiling.saturating_sub(free_bytes),
            free_bytes,
            active_leases: saturating_usize_to_u64(self.live_order.len()),
            pending_leases: saturating_usize_to_u64(pending_leases),
            largest_free_range_bytes,
            allocation_failures_total: self.allocation_failures_total,
            release_failures_total: self.release_failures_total,
        }
    }

    /// True backing length of the arena in bytes.
    ///
    /// This is the ground truth the bootstrap header's `arena_size` must
    /// match; it equals `config.reservation_ceiling` because the backing is
    /// created with exactly that length.
    pub fn arena_len(&self) -> u64 {
        self.config.reservation_ceiling
    }

    #[cfg(target_os = "linux")]
    pub fn arena_fd(&self) -> std::os::fd::RawFd {
        self._backing.fd()
    }

    #[cfg(target_os = "linux")]
    pub fn write_arena(&self, offset: u64, bytes: &[u8]) -> Result<(), std::io::Error> {
        self._backing.write_at(offset, bytes)
    }

    #[cfg(target_os = "linux")]
    pub fn read_arena(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        self._backing.read_at(offset, len)
    }

    /// Bounds-checked pointer to `[offset, offset+len)` inside the live
    /// mapping. Returns `None` when the range leaves the arena; the check is
    /// against the backing's true length, never a caller claim.
    #[cfg(target_os = "linux")]
    pub fn region_ptr(&self, offset: u64, len: u64) -> Option<std::ptr::NonNull<u8>> {
        let end = offset.checked_add(len)?;
        let backing_len = u64::try_from(self._backing.len).ok()?;
        if end > backing_len {
            return None;
        }
        // SAFETY: `offset` is bounds-checked against the mapping above.
        NonNull::new(unsafe { self._backing.ptr.cast::<u8>().add(offset as usize) })
    }

    fn lease_ring(&mut self, request: LeaseRing) -> Vec<ArenaEvent> {
        if self.state == ArenaState::ShuttingDown {
            self.allocation_failures_total = self.allocation_failures_total.saturating_add(1);
            return vec![ArenaEvent::RingLeaseRejected {
                request_id: request.request_id,
                reason: RingLeaseRejection::ArenaShuttingDown,
            }];
        }

        if self.request_layout_at(&request.ring_spec, 0).is_none() {
            self.allocation_failures_total = self.allocation_failures_total.saturating_add(1);
            return vec![ArenaEvent::RingLeaseRejected {
                request_id: request.request_id,
                reason: RingLeaseRejection::CannotFitWithinCeiling,
            }];
        }

        if self.pending.is_empty() {
            if let Some(lease) = self.try_allocate(&request) {
                return vec![ArenaEvent::RingLeased { lease }];
            }
        }

        let request_id = request.request_id;
        self.pending.push_back(QueuedLease {
            request,
            cancelled: false,
        });
        vec![ArenaEvent::RingLeaseQueued { request_id }]
    }

    fn cancel_lease(&mut self, request_id: LeaseRequestId) {
        for pending in &mut self.pending {
            if pending.request.request_id == request_id {
                pending.cancelled = true;
            }
        }
    }

    fn release_ring(&mut self, ring_id: RingId, proof: QuiescenceProof) -> Vec<ArenaEvent> {
        let Some(index) = self.live_index.get(&ring_id).copied() else {
            self.release_failures_total = self.release_failures_total.saturating_add(1);
            return vec![ArenaEvent::RingReleaseRejected {
                ring_id,
                reason: RingReleaseRejection::UnknownRingId,
            }];
        };

        if !proof.verified {
            self.release_failures_total = self.release_failures_total.saturating_add(1);
            return vec![ArenaEvent::RingReleaseRejected {
                ring_id,
                reason: RingReleaseRejection::MissingQuiescenceProof,
            }];
        }

        let lease = self.live_order.remove(index);
        self.rebuild_live_index();
        self.insert_free_range(FreeRange {
            start: lease.layout.start_offset,
            end: lease.layout.end_offset,
        });

        let mut events = vec![ArenaEvent::RingReleased {
            ring_id,
            start_offset: lease.layout.start_offset,
            end_offset: lease.layout.end_offset,
        }];
        if self.state == ArenaState::Ready {
            self.retry_pending_leases(&mut events);
        }
        events
    }

    fn shutdown(&mut self) -> Vec<ArenaEvent> {
        self.state = ArenaState::ShuttingDown;
        let mut events = Vec::new();
        while let Some(pending) = self.pending.pop_front() {
            if pending.cancelled {
                events.push(ArenaEvent::CancelledFreshLeaseReleased {
                    request_id: pending.request.request_id,
                });
            } else {
                events.push(ArenaEvent::RingLeaseRejected {
                    request_id: pending.request.request_id,
                    reason: RingLeaseRejection::ArenaShuttingDown,
                });
            }
        }
        events
    }

    fn retry_pending_leases(&mut self, events: &mut Vec<ArenaEvent>) {
        while let Some(pending) = self.pending.pop_front() {
            if pending.cancelled {
                events.push(ArenaEvent::CancelledFreshLeaseReleased {
                    request_id: pending.request.request_id,
                });
                continue;
            }

            if let Some(lease) = self.try_allocate(&pending.request) {
                events.push(ArenaEvent::RingLeased { lease });
                continue;
            }

            self.pending.push_front(pending);
            break;
        }
    }

    fn try_allocate(&mut self, request: &LeaseRing) -> Option<RingLease> {
        for index in 0..self.free_ranges.len() {
            let range = self.free_ranges[index];
            let Some(layout) = self.request_layout_at(&request.ring_spec, range.start) else {
                continue;
            };
            if layout.end_offset > range.end {
                continue;
            }

            self.free_ranges.remove(index);
            let mut insert_index = index;
            if range.start < layout.start_offset {
                self.free_ranges.insert(
                    insert_index,
                    FreeRange {
                        start: range.start,
                        end: layout.start_offset,
                    },
                );
                insert_index += 1;
            }
            if layout.end_offset < range.end {
                self.free_ranges.insert(
                    insert_index,
                    FreeRange {
                        start: layout.end_offset,
                        end: range.end,
                    },
                );
            }

            let ring_id = RingId(self.next_ring_id);
            self.next_ring_id = self.next_ring_id.checked_add(1)?;
            let lease = RingLease {
                request_id: request.request_id,
                ring_id,
                node_id: self.config.node_id,
                layout,
                requested_alignment: request.ring_spec.alignment,
            };
            self.live_index.insert(ring_id, self.live_order.len());
            self.live_order.push(lease.clone());
            return Some(lease);
        }
        None
    }

    fn request_layout_at(&self, spec: &RingSpec, range_start: u64) -> Option<RingLayout> {
        let alignment = lcm(self.config.base_alignment, spec.alignment)?;
        let start_offset = align_up(range_start, alignment)?;
        let header_offset = start_offset;
        let after_header = header_offset.checked_add(spec.header_bytes)?;
        let data_offset = align_up(after_header, alignment)?;
        let end_offset = data_offset.checked_add(spec.data_bytes)?;
        if end_offset > self.config.reservation_ceiling {
            return None;
        }
        Some(RingLayout {
            start_offset,
            header_offset,
            data_offset,
            end_offset,
            data_bytes: spec.data_bytes,
            alignment,
            pointer: LayoutPointer::NoProcessPointer,
        })
    }

    fn insert_free_range(&mut self, range: FreeRange) {
        self.free_ranges.push(range);
        self.free_ranges.sort_by_key(|range| range.start);

        let mut coalesced: Vec<FreeRange> = Vec::with_capacity(self.free_ranges.len());
        for range in self.free_ranges.drain(..) {
            if range.start == range.end {
                continue;
            }
            if let Some(last) = coalesced.last_mut() {
                if range.start <= last.end {
                    last.end = last.end.max(range.end);
                    continue;
                }
            }
            coalesced.push(range);
        }
        self.free_ranges = coalesced;
    }

    fn rebuild_live_index(&mut self) {
        self.live_index.clear();
        for (index, lease) in self.live_order.iter().enumerate() {
            self.live_index.insert(lease.ring_id, index);
        }
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

    pub fn sample(&self, seq: u64) -> ArenaSnapshot {
        self.manager.sample(seq)
    }
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    if alignment == 0 {
        return None;
    }
    let remainder = value % alignment;
    if remainder == 0 {
        Some(value)
    } else {
        value.checked_add(alignment - remainder)
    }
}

fn lcm(left: u64, right: u64) -> Option<u64> {
    if left == 0 || right == 0 {
        return None;
    }
    let gcd = gcd(left, right);
    (left / gcd).checked_mul(right)
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn unix_ms_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    millis.min(u128::from(u64::MAX)) as u64
}

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(target_os = "linux")]
struct ArenaBacking {
    len: usize,
    ptr: *mut libc::c_void,
    fd: std::os::fd::OwnedFd,
}

// The mmap base pointer is only used behind ArenaManager ownership. Moving the
// backing between threads is safe because the mapping lifetime is tied to this
// struct and Drop unmaps it exactly once.
#[cfg(target_os = "linux")]
unsafe impl Send for ArenaBacking {}

#[cfg(target_os = "linux")]
impl ArenaBacking {
    fn create(config: &ArenaConfig) -> Result<Self, ArenaFault> {
        if config.reservation_ceiling == 0 {
            return Err(ArenaFault::InvalidReservationCeiling);
        }
        if config.base_alignment == 0 {
            return Err(ArenaFault::InvalidBaseAlignment);
        }

        let len = usize::try_from(config.reservation_ceiling)
            .map_err(|_| ArenaFault::InvalidReservationCeiling)?;

        let fd = unsafe {
            let name = b"data-plane-arena\0";
            libc::memfd_create(name.as_ptr().cast(), 0)
        };
        if fd < 0 {
            return Err(ArenaFault::BackingUnavailable);
        }

        let fd = unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) };
        let truncate_result =
            unsafe { libc::ftruncate(std::os::fd::AsRawFd::as_raw_fd(&fd), len as libc::off_t) };
        if truncate_result != 0 {
            return Err(ArenaFault::BackingUnavailable);
        }

        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                std::os::fd::AsRawFd::as_raw_fd(&fd),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(ArenaFault::BackingUnavailable);
        }

        Ok(Self { len, ptr, fd })
    }

    fn fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.fd)
    }

    fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<(), std::io::Error> {
        let written = unsafe {
            libc::pwrite(
                self.fd(),
                bytes.as_ptr().cast(),
                bytes.len(),
                offset as libc::off_t,
            )
        };
        if written == bytes.len() as isize {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, std::io::Error> {
        let mut bytes = vec![0u8; len];
        let read = unsafe {
            libc::pread(
                self.fd(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                offset as libc::off_t,
            )
        };
        if read == len as isize {
            Ok(bytes)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

#[cfg(target_os = "linux")]
impl Drop for ArenaBacking {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr, self.len);
        }
    }
}

#[cfg(not(target_os = "linux"))]
struct ArenaBacking;

#[cfg(not(target_os = "linux"))]
impl ArenaBacking {
    fn create(_config: &ArenaConfig) -> Result<Self, ArenaFault> {
        Err(ArenaFault::UnsupportedPlatform)
    }
}
