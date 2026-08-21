//! Arena-resident byte-movement rings.
//!
//! Shared-memory protocol beneath the Python data-plane surface: one
//! producer and one consumer move a byte stream through a ring leased from
//! the arena. This module owns the wire layout, the cursor protocol, and
//! the record framing; nothing here knows about paths or identities.
//!
//! Wire layout (all integers little-endian):
//!
//! | offset | field                          | owner                  |
//! |--------|--------------------------------|------------------------|
//! | 0      | magic u32 (`SWRG`)             | fixed at install       |
//! | 4      | version u16                    | fixed at install       |
//! | 6      | reserved u16 (zero)            | fixed at install       |
//! | 8      | capacity u64                   | fixed at install       |
//! | 16     | generation u64                 | fixed at install       |
//! | 24     | commit u64 (atomic)            | producer writes only   |
//! | 32     | consume u64 (atomic)           | consumer writes only   |
//! | 40..   | reserved (zero)                | fixed at install       |
//! | 128    | data[capacity]                 | protocol               |
//!
//! Memory model (property P4): the producer copies bytes and then
//! release-stores `commit`; the consumer acquire-loads `commit`, copies
//! bytes out, and release-stores `consume`; the producer acquire-loads
//! `consume` before reusing freed space. That is the entire ordering story.
//!
//! Properties a correct implementation must uphold (each defended in
//! `tests/byte_ring_guarantees.rs`):
//!
//! - P1  exactly-once, in-order byte delivery across interleavings,
//!       wake timings, and wraparound;
//! - P2  unread bytes are never overwritten by the producer;
//! - P3  single-writer cursor fields (SPSC), enforced by endpoint roles;
//! - P4  data publication is release-ordered before `commit` advance,
//!       and `consume` advance after reads (acquire);
//! - P5  untrusted-header containment: arbitrary header bytes yield typed
//!       errors or bounded access, never out-of-region access;
//! - P6  (binding slice) Python performs ring operations via helpers only;
//! - P7  backpressure: reserving beyond free space fails cleanly with the
//!       exact free amount, and succeeds again once the peer consumes;
//! - P8  (binding slice) no lost wakeups: signals follow publication;
//! - P9  completion is explicit: `Data`/`Eof`/`Fault` records, `Eof` only
//!       after all bytes, torn (uncommitted) records stay invisible;
//! - P10 generation fence: stale reservations and stale generations are
//!       rejected, never applied;
//! - P11 (binding slice) fast path performs no syscalls;
//! - P12 one copy per side per byte.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::arena::{ArenaEvent, ArenaManager, ArenaRequest, LeaseRing, LeaseRequestId, RingLease, RingLeaseRejection, RingSpec};

/// `"SWRG"` read little-endian.
pub const RING_MAGIC: u32 = u32::from_le_bytes(*b"SWRG");
pub const RING_VERSION: u16 = 1;

/// Byte offset of the data region inside the ring lease.
pub const DATA_OFFSET: u64 = 128;

pub const OFF_MAGIC: u64 = 0;
pub const OFF_VERSION: u64 = 4;
pub const OFF_RESERVED: u64 = 6;
pub const OFF_CAPACITY: u64 = 8;
pub const OFF_GENERATION: u64 = 16;
pub const OFF_COMMIT: u64 = 24;
pub const OFF_CONSUME: u64 = 32;

const ZERO_CHUNK: usize = 4 * 1024;

/// Record prefix: `kind u8 | len u32 LE`.
const RECORD_HEADER_LEN: u64 = 5;
pub struct ByteRingSpec {
    /// Data-region capacity in bytes (the header adds `DATA_OFFSET`).
    pub capacity: u64,
    /// First generation of the installed ring (nonzero).
    pub generation: u64,
    /// Placement alignment requested from the arena.
    pub alignment: u64,
    /// Lease request identity; distinct rings must use distinct ids.
    pub request_id: u64,
}

/// A located, installed ring: what `attach` needs to find it again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingHandle {
    /// Lease start (the header lives here).
    pub offset: u64,
    /// Data-region capacity.
    pub capacity: u64,
    /// Ring generation.
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Producer,
    Consumer,
}


/// A reserved, not-yet-committed span of the data region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    /// Logical stream position (monotonic, wraps modulo capacity on
    /// physical access).
    pub start: u64,
    pub len: u64,
    pub generation: u64,
}

/// Record framing layered on the byte stream (property P9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Data,
    Eof,
    Fault,
}

impl RecordKind {
    fn to_byte(self) -> u8 {
        match self {
            Self::Data => 1,
            Self::Eof => 2,
            Self::Fault => 3,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Data),
            2 => Some(Self::Eof),
            3 => Some(Self::Fault),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum InstallError {
    ZeroCapacity,
    ZeroGeneration,
    ZeroAlignment,
    LeaseRejected(RingLeaseRejection),
    LeaseQueued,
    UnexpectedLeaseOutcome,
    Io(std::io::Error),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeaderError {
    BadMagic { found: u32 },
    UnsupportedVersion { found: u16, supported: u16 },
    ReservedBytesNotZero { at: u64 },
    CapacityMismatch { header: u64, handle: u64 },
    GenerationMismatch { header: u64, handle: u64 },
    CommitBelowConsume { commit: u64, consume: u64 },
    ReadableExceedsCapacity { commit: u64, consume: u64, capacity: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachError {
    /// The handle itself points outside the arena.
    OutOfBounds { end: u64, arena_len: u64 },
    Header(HeaderError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordError {
    LengthExceedsCapacity { len: u64, capacity: u64 },
    InvalidKind(u8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FlowError {
    InsufficientSpace { requested: u64, free: u64 },
    BeyondCommitted { requested: u64, readable: u64 },
    /// The reservation's generation no longer matches the ring header.
    StaleReservation { reservation: u64, ring: u64 },
    /// Operation reserved for the other role (property P3).
    RoleViolation { operation: &'static str, role: Role },
    /// The header stopped validating mid-protocol (property P5).
    Corrupt(HeaderError),
    BadRecord(RecordError),
    Io,
}

/// One side of an installed ring. Owns its access path into the mapping so
/// endpoints can cross threads without sharing the arena manager.
pub struct Endpoint {
    header: NonNull<u8>,
    info: RingHandle,
    role: Role,
}

// SAFETY: an endpoint touches only its role-owned cursor field and the data
// region under the single-writer protocol (properties P3/P4); the raw
// pointer is never dereferenced outside `[header, header + DATA_OFFSET +
// capacity)`.
unsafe impl Send for Endpoint {}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("info", &self.info)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

/// Lease a region, write the ring header, and zero the data region.
///
/// All writes complete before the handle is returned, so the peer may
/// `attach` at any time afterwards.
pub fn install(arena: &mut ArenaManager, spec: ByteRingSpec) -> Result<RingHandle, InstallError> {
    if spec.capacity == 0 {
        return Err(InstallError::ZeroCapacity);
    }
    if spec.generation == 0 {
        return Err(InstallError::ZeroGeneration);
    }
    if spec.alignment == 0 {
        return Err(InstallError::ZeroAlignment);
    }

    let lease = lease_ring(
        arena,
        spec.request_id,
        RingSpec {
            header_bytes: DATA_OFFSET,
            data_bytes: spec.capacity,
            alignment: spec.alignment,
        },
    )?;
    let zeros = [0_u8; ZERO_CHUNK];
    let data_start = lease.layout.start_offset + DATA_OFFSET;
    let data_end = lease.layout.end_offset;
    let mut offset = data_start;
    while offset < data_end {
        let take = ((data_end - offset) as usize).min(ZERO_CHUNK);
        arena
            .write_arena(offset, &zeros[..take])
            .map_err(InstallError::Io)?;
        offset += take as u64;
    }

    let mut header = [0_u8; DATA_OFFSET as usize];
    header[0..4].copy_from_slice(&RING_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&RING_VERSION.to_le_bytes());
    header[8..16].copy_from_slice(&spec.capacity.to_le_bytes());
    header[16..24].copy_from_slice(&spec.generation.to_le_bytes());
    // commit and consume stay zero.
    arena
        .write_arena(lease.layout.start_offset, &header)
        .map_err(InstallError::Io)?;

    Ok(RingHandle {
        offset: lease.layout.start_offset,
        capacity: spec.capacity,
        generation: spec.generation,
    })
}

/// Validate the header against `handle` and the arena bounds, then open one
/// side of the ring.
pub fn attach(
    arena: &ArenaManager,
    handle: RingHandle,
    role: Role,
) -> Result<Endpoint, AttachError> {
    let end = handle
        .offset
        .checked_add(DATA_OFFSET)
        .and_then(|v| v.checked_add(handle.capacity))
        .ok_or(AttachError::OutOfBounds {
            end: u64::MAX,
            arena_len: arena.arena_len(),
        })?;
    if end > arena.arena_len() {
        return Err(AttachError::OutOfBounds {
            end,
            arena_len: arena.arena_len(),
        });
    }
    let header = arena
        .region_ptr(handle.offset, DATA_OFFSET + handle.capacity)
        .ok_or(AttachError::OutOfBounds {
            end,
            arena_len: arena.arena_len(),
        })?;

    let endpoint = Endpoint { header, info: handle, role };
    endpoint
        .validate_fixed()
        .map_err(AttachError::Header)?;
    endpoint
        .validate_generation(handle.generation)
        .map_err(AttachError::Header)?;
    endpoint
        .validate_cursors()
        .map_err(AttachError::Header)?;
    Ok(endpoint)
}

fn lease_ring(
    arena: &mut ArenaManager,
    request_id: u64,
    ring_spec: RingSpec,
) -> Result<RingLease, InstallError> {
    let request = LeaseRing {
        request_id: LeaseRequestId(request_id),
        ring_spec,
    };
    let mut events = arena.request(ArenaRequest::LeaseRing(request));
    match (events.len(), events.pop()) {
        (1, Some(ArenaEvent::RingLeased { lease })) => Ok(lease),
        (1, Some(ArenaEvent::RingLeaseRejected { reason, .. })) => {
            Err(InstallError::LeaseRejected(reason))
        }
        (1, Some(ArenaEvent::RingLeaseQueued { .. })) => Err(InstallError::LeaseQueued),
        _ => Err(InstallError::UnexpectedLeaseOutcome),
    }
}

impl Endpoint {
    // ─── field access ────────────────────────────────────────────────────────

    /// SAFETY: callers keep `self` alive; the pointer is bounds-checked at
    /// attach against the lease and the arena.
    fn field_u32(&self, off: u64) -> u32 {
        // SAFETY: fixed fields sit inside the bounds-checked lease.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.header.as_ptr(), DATA_OFFSET as usize) };
        u32::from_le_bytes(bytes[off as usize..off as usize + 4].try_into().unwrap())
    }

    fn field_u16(&self, off: u64) -> u16 {
        // SAFETY: as `field_u32`.
        let bytes =
            unsafe { std::slice::from_raw_parts(self.header.as_ptr(), DATA_OFFSET as usize) };
        u16::from_le_bytes(bytes[off as usize..off as usize + 2].try_into().unwrap())
    }

    /// Relaxed load of a fixed-at-install field.
    fn field_u64(&self, off: u64) -> u64 {
        self.atomic(off).load(Ordering::Relaxed)
    }

    /// The cursor words live at 8-aligned offsets inside a 64-aligned lease.
    fn atomic(&self, off: u64) -> &AtomicU64 {
        // SAFETY: attach bounds-checked the lease and arena placement
        // guarantees 64-byte alignment, so `header + off` is 8-aligned and
        // inside the mapping for the cursor offsets.
        unsafe { &*(self.header.as_ptr().add(off as usize) as *const AtomicU64) }
    }

    fn commit_cursor(&self) -> u64 {
        self.atomic(OFF_COMMIT).load(Ordering::Acquire)
    }

    fn consume_cursor(&self) -> u64 {
        self.atomic(OFF_CONSUME).load(Ordering::Acquire)
    }

    // ─── validation (P5: never trust the header) ─────────────────────────────

    fn validate_fixed(&self) -> Result<(), HeaderError> {
        let magic = self.field_u32(OFF_MAGIC);
        if magic != RING_MAGIC {
            return Err(HeaderError::BadMagic { found: magic });
        }
        let version = self.field_u16(OFF_VERSION);
        if version != RING_VERSION {
            return Err(HeaderError::UnsupportedVersion {
                found: version,
                supported: RING_VERSION,
            });
        }
        let reserved = self.field_u16(OFF_RESERVED);
        if reserved != 0 {
            return Err(HeaderError::ReservedBytesNotZero { at: OFF_RESERVED });
        }
        let capacity = self.field_u64(OFF_CAPACITY);
        if capacity != self.info.capacity {
            return Err(HeaderError::CapacityMismatch {
                header: capacity,
                handle: self.info.capacity,
            });
        }
        Ok(())
    }

    fn validate_generation(&self, expected: u64) -> Result<(), HeaderError> {
        let generation = self.field_u64(OFF_GENERATION);
        if generation != expected {
            return Err(HeaderError::GenerationMismatch {
                header: generation,
                handle: expected,
            });
        }
        Ok(())
    }

    fn validate_cursors(&self) -> Result<(), HeaderError> {
        let (commit, consume) = (self.commit_cursor(), self.consume_cursor());
        if commit < consume {
            return Err(HeaderError::CommitBelowConsume { commit, consume });
        }
        if commit - consume > self.info.capacity {
            return Err(HeaderError::ReadableExceedsCapacity {
                commit,
                consume,
                capacity: self.info.capacity,
            });
        }
        Ok(())
    }

    /// Every protocol operation revalidates before touching memory.
    fn check(&self, operation: &'static str, role: Role) -> Result<(), FlowError> {
        if self.role != role {
            return Err(FlowError::RoleViolation {
                operation,
                role: self.role,
            });
        }
        self.validate_fixed().map_err(FlowError::Corrupt)?;
        self.validate_cursors().map_err(FlowError::Corrupt)?;
        Ok(())
    }

    // ─── wrapped copies (the only wrap arithmetic in the protocol) ───────────

    fn data_ptr(&self) -> *mut u8 {
        // SAFETY: attach bounds-checked `[offset, offset + DATA_OFFSET +
        // capacity)` against the arena.
        unsafe { self.header.as_ptr().add(DATA_OFFSET as usize) }
    }

    fn copy_into(&self, stream_pos: u64, bytes: &[u8]) {
        let capacity = self.info.capacity as usize;
        let start = (stream_pos % self.info.capacity) as usize;
        let first = bytes.len().min(capacity - start);
        // SAFETY: both slices are in-bounds by construction (lease checked,
        // `first` splits the wrap).
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.data_ptr().add(start), first);
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr().add(first),
                self.data_ptr(),
                bytes.len() - first,
            );
        }
    }

    fn copy_out(&self, stream_pos: u64, len: u64) -> Vec<u8> {
        let capacity = self.info.capacity as usize;
        let start = (stream_pos % self.info.capacity) as usize;
        let len = len as usize;
        let first = len.min(capacity - start);
        let mut bytes = vec![0_u8; len];
        // SAFETY: as `copy_into`.
        unsafe {
            std::ptr::copy_nonoverlapping(self.data_ptr().add(start), bytes.as_mut_ptr(), first);
            std::ptr::copy_nonoverlapping(self.data_ptr(), bytes.as_mut_ptr().add(first), len - first);
        }
        bytes
    }

    // ─── producer side ───────────────────────────────────────────────────────

    /// Free space computed from the consumer's published cursor.
    pub fn reserve(&self, len: u64) -> Result<Reservation, FlowError> {
        self.check("reserve", Role::Producer)?;
        self.validate_generation(self.info.generation)
            .map_err(FlowError::Corrupt)?;
        let (commit, consume) = (self.commit_cursor(), self.consume_cursor());
        let used = commit - consume;
        let free = self.info.capacity - used;
        if len > free {
            return Err(FlowError::InsufficientSpace { requested: len, free });
        }
        Ok(Reservation {
            start: commit,
            len,
            generation: self.info.generation,
        })
    }

    /// Copy bytes into the reserved (producer-side) span.
    pub fn write(&self, reservation: &Reservation, bytes: &[u8]) -> Result<(), FlowError> {
        self.check("write", Role::Producer)?;
        self.stale_check(reservation)?;
        assert_eq!(
            reservation.len as usize, bytes.len(),
            "reservation length must match the payload"
        );
        self.copy_into(reservation.start, bytes);
        Ok(())
    }

    /// Publish reserved bytes to the consumer (release-ordered).
    pub fn commit(&mut self, reservation: Reservation) -> Result<(), FlowError> {
        self.check("commit", Role::Producer)?;
        // Generation fence first (P10): a replaced ring rejects stale work.
        self.stale_check(&reservation)?;
        let commit = self.commit_cursor();
        assert_eq!(
            reservation.start, commit,
            "reservations must commit in stream order"
        );
        self.atomic(OFF_COMMIT)
            .store(commit + reservation.len, Ordering::Release);
        Ok(())
    }

    fn stale_check(&self, reservation: &Reservation) -> Result<(), FlowError> {
        let generation = self.field_u64(OFF_GENERATION);
        if generation != reservation.generation {
            return Err(FlowError::StaleReservation {
                reservation: reservation.generation,
                ring: generation,
            });
        }
        Ok(())
    }

    // ─── consumer side ───────────────────────────────────────────────────────

    /// Committed-but-unconsumed byte count.
    pub fn readable(&self) -> Result<u64, FlowError> {
        self.check("readable", Role::Consumer)?;
        Ok(self.commit_cursor() - self.consume_cursor())
    }

    /// Copy committed bytes out without advancing the cursor.
    pub fn read(&self, len: u64) -> Result<Vec<u8>, FlowError> {
        self.check("read", Role::Consumer)?;
        let (commit, consume) = (self.commit_cursor(), self.consume_cursor());
        let readable = commit - consume;
        if len > readable {
            return Err(FlowError::BeyondCommitted { requested: len, readable });
        }
        Ok(self.copy_out(consume, len))
    }

    /// Release consumed bytes back to the producer.
    pub fn consume(&mut self, len: u64) -> Result<(), FlowError> {
        self.check("consume", Role::Consumer)?;
        let (commit, consume) = (self.commit_cursor(), self.consume_cursor());
        let readable = commit - consume;
        if len > readable {
            return Err(FlowError::BeyondCommitted { requested: len, readable });
        }
        self.atomic(OFF_CONSUME)
            .store(consume + len, Ordering::Release);
        Ok(())
    }

    // ─── records (P9) ────────────────────────────────────────────────────────

    /// Frame and commit one record in a single step.
    pub fn send_record(&mut self, kind: RecordKind, bytes: &[u8]) -> Result<(), FlowError> {
        let len = bytes.len() as u64;
        if RECORD_HEADER_LEN + len > self.info.capacity {
            return Err(FlowError::BadRecord(RecordError::LengthExceedsCapacity {
                len,
                capacity: self.info.capacity,
            }));
        }
        let reservation = self.reserve(RECORD_HEADER_LEN + len)?;
        let mut prefix = [0_u8; RECORD_HEADER_LEN as usize];
        prefix[0] = kind.to_byte();
        prefix[1..5].copy_from_slice(&(len as u32).to_le_bytes());
        self.copy_into(reservation.start, &prefix);
        self.copy_into(reservation.start + RECORD_HEADER_LEN, bytes);
        self.commit(reservation)
    }

    /// Receive one complete record; `Ok(None)` when nothing (or only a
    /// torn, uncommitted prefix) is readable.
    pub fn recv_record(&mut self) -> Result<Option<(RecordKind, Vec<u8>)>, FlowError> {
        self.check("recv_record", Role::Consumer)?;
        let (commit, consume) = (self.commit_cursor(), self.consume_cursor());
        let readable = commit - consume;
        if readable < RECORD_HEADER_LEN {
            return Ok(None);
        }
        let prefix = self.copy_out(consume, RECORD_HEADER_LEN);
        let kind = RecordKind::from_byte(prefix[0])
            .ok_or(FlowError::BadRecord(RecordError::InvalidKind(prefix[0])))?;
        let len = u32::from_le_bytes(prefix[1..5].try_into().unwrap()) as u64;
        if RECORD_HEADER_LEN + len > self.info.capacity {
            return Err(FlowError::BadRecord(RecordError::LengthExceedsCapacity {
                len,
                capacity: self.info.capacity,
            }));
        }
        if readable < RECORD_HEADER_LEN + len {
            // Torn tail: the producer has not committed the whole record.
            return Ok(None);
        }
        let payload = self.copy_out(consume + RECORD_HEADER_LEN, len);
        self.atomic(OFF_CONSUME)
            .store(consume + RECORD_HEADER_LEN + len, Ordering::Release);
        Ok(Some((kind, payload)))
    }
}
