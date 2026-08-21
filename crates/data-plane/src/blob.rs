//! Stable arena leases for finite zero-copy blobs.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::arena::{ArenaManager, RingLease};
use crate::ids::BlobLeaseId;
use crate::mapped_arena::MappedArena;

pub const BLOB_HEADER_LEN: u64 = 128;
const BLOB_MAGIC: u32 = u32::from_le_bytes(*b"SWBL");
const BLOB_VERSION: u16 = 1;
const ACCESS_OFFSET: usize = 6;
const DIGEST_KIND_OFFSET: usize = 7;
const GENERATION_OFFSET: usize = 8;
const LENGTH_OFFSET: usize = 16;
const STATE_OFFSET: usize = 24;
const DIGEST_OFFSET: usize = 32;
const DIGEST_LEN: usize = 32;
const RESERVED_OFFSET: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BlobAccess {
    ReadOnly = 1,
    Writable = 2,
}

impl BlobAccess {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::ReadOnly),
            2 => Some(Self::Writable),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentDigest {
    Sha256([u8; DIGEST_LEN]),
}

impl ContentDigest {
    pub fn sha256(bytes: &[u8]) -> Self {
        Self::Sha256(Sha256::digest(bytes).into())
    }

    pub fn matches(&self, bytes: &[u8]) -> bool {
        *self == Self::sha256(bytes)
    }

    pub fn bytes(&self) -> &[u8; DIGEST_LEN] {
        match self {
            Self::Sha256(bytes) => bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub length: u64,
    pub digest: Option<ContentDigest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLease {
    pub lease_id: BlobLeaseId,
    pub offset: u64,
    pub length: u64,
    pub generation: u64,
    pub access: BlobAccess,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum BlobSharedState {
    Vacant = 0,
    Filling = 1,
    Writable = 2,
    Sealed = 3,
    Aborted = 4,
    Released = 5,
}

impl BlobSharedState {
    fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::Vacant),
            1 => Some(Self::Filling),
            2 => Some(Self::Writable),
            3 => Some(Self::Sealed),
            4 => Some(Self::Aborted),
            5 => Some(Self::Released),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobError {
    RangeOutOfBounds {
        offset: u64,
        length: u64,
        arena_size: u64,
    },
    UnalignedHeader {
        offset: u64,
    },
    BadMagic {
        found: u32,
    },
    UnsupportedVersion {
        found: u16,
    },
    ReservedBytesNotZero {
        at: usize,
    },
    InvalidAccess {
        found: u8,
    },
    AccessDenied,
    StaleGeneration {
        expected: u64,
        found: u64,
    },
    LengthMismatch {
        expected: u64,
        found: u64,
    },
    InvalidState {
        found: u64,
    },
    DigestMetadataMismatch,
    DigestMismatch,
    InvalidHostLease,
    ActiveWritableView,
    AlreadyFinished,
}

impl fmt::Display for BlobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RangeOutOfBounds {
                offset,
                length,
                arena_size,
            } => write!(
                f,
                "blob range at {offset} with length {length} exceeds arena size {arena_size}"
            ),
            Self::UnalignedHeader { offset } => {
                write!(f, "blob header state at lease offset {offset} is unaligned")
            }
            Self::BadMagic { found } => write!(f, "blob magic mismatch: {found:#010x}"),
            Self::UnsupportedVersion { found } => {
                write!(f, "unsupported blob header version {found}")
            }
            Self::ReservedBytesNotZero { at } => {
                write!(f, "blob header reserved byte at offset {at} is nonzero")
            }
            Self::InvalidAccess { found } => write!(f, "invalid blob access value {found}"),
            Self::AccessDenied => f.write_str("blob lease does not grant the requested access"),
            Self::StaleGeneration { expected, found } => write!(
                f,
                "stale blob generation: lease expects {expected}, header contains {found}"
            ),
            Self::LengthMismatch { expected, found } => write!(
                f,
                "blob length mismatch: expected {expected}, header contains {found}"
            ),
            Self::InvalidState { found } => write!(f, "blob shared state {found} is invalid here"),
            Self::DigestMetadataMismatch => {
                f.write_str("blob digest metadata disagrees with the sealed header")
            }
            Self::DigestMismatch => f.write_str("blob content digest mismatch"),
            Self::InvalidHostLease => f.write_str("arena allocation is not a valid blob lease"),
            Self::ActiveWritableView => f.write_str("a writable arena view is still active"),
            Self::AlreadyFinished => f.write_str("blob writer is already sealed or aborted"),
        }
    }
}

impl std::error::Error for BlobError {}

pub trait LeaseReleaser: Send + Sync + 'static {
    fn release(&self, lease: BlobLease);
}

pub struct BlobLeaseGuard {
    arena: Arc<MappedArena>,
    lease: BlobLease,
    releaser: Arc<dyn LeaseReleaser>,
}

impl fmt::Debug for BlobLeaseGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobLeaseGuard")
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

impl Drop for BlobLeaseGuard {
    fn drop(&mut self) {
        self.releaser.release(self.lease);
    }
}

#[derive(Clone, Debug)]
pub struct Blob {
    guard: Arc<BlobLeaseGuard>,
    metadata: BlobMetadata,
}

impl Blob {
    pub fn from_sealed_lease(
        arena: Arc<MappedArena>,
        lease: BlobLease,
        metadata: BlobMetadata,
        releaser: Arc<dyn LeaseReleaser>,
    ) -> Result<Self, BlobError> {
        validate_mapped_header(&arena, lease, &metadata, BlobSharedState::Sealed)?;
        Ok(Self {
            guard: Arc::new(BlobLeaseGuard {
                arena,
                lease,
                releaser,
            }),
            metadata,
        })
    }

    pub fn length(&self) -> u64 {
        self.metadata.length
    }

    pub fn digest(&self) -> Option<&ContentDigest> {
        self.metadata.digest.as_ref()
    }

    pub fn lease(&self) -> BlobLease {
        self.guard.lease
    }

    pub fn map(&self) -> Result<ArenaView, BlobError> {
        validate_mapped_header(
            &self.guard.arena,
            self.guard.lease,
            &self.metadata,
            BlobSharedState::Sealed,
        )?;
        let payload_offset = self.guard.lease.offset.checked_add(BLOB_HEADER_LEN).ok_or(
            BlobError::RangeOutOfBounds {
                offset: self.guard.lease.offset,
                length: self.guard.lease.length,
                arena_size: self.guard.arena.len() as u64,
            },
        )?;
        let range = mapped_range(&self.guard.arena, payload_offset, self.guard.lease.length)?;
        Ok(ArenaView {
            guard: self.guard.clone(),
            payload_offset: range.start,
            length: range.len(),
        })
    }
}

pub struct ArenaView {
    guard: Arc<BlobLeaseGuard>,
    payload_offset: usize,
    length: usize,
}

impl ArenaView {
    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.guard
            .arena
            .ptr_at(self.payload_offset)
            .as_ptr()
            .cast_const()
    }
}

impl AsRef<[u8]> for ArenaView {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: construction bounds-checks the range, the arena mapping is
        // stable, and the shared guard prevents lease reuse while this view is
        // live. Sealed payloads are immutable.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.length) }
    }
}

impl Deref for ArenaView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub(crate) struct WritableBlobLease {
    arena: Arc<MappedArena>,
    lease: BlobLease,
    metadata: BlobMetadata,
    active_view: AtomicBool,
    finished: AtomicBool,
}

impl WritableBlobLease {
    pub(crate) fn from_grant(
        arena: Arc<MappedArena>,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) -> Result<Arc<Self>, BlobError> {
        if lease.access != BlobAccess::Writable {
            return Err(BlobError::AccessDenied);
        }
        validate_mapped_header(&arena, lease, &metadata, BlobSharedState::Writable)?;
        Ok(Arc::new(Self {
            arena,
            lease,
            metadata,
            active_view: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        }))
    }

    pub(crate) fn lease(&self) -> BlobLease {
        self.lease
    }

    pub(crate) fn metadata(&self) -> &BlobMetadata {
        &self.metadata
    }

    pub(crate) fn map(self: &Arc<Self>) -> Result<WritableArenaView, BlobError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(BlobError::AlreadyFinished);
        }
        self.active_view
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| BlobError::ActiveWritableView)?;
        let payload_offset =
            self.lease
                .offset
                .checked_add(BLOB_HEADER_LEN)
                .ok_or(BlobError::RangeOutOfBounds {
                    offset: self.lease.offset,
                    length: self.lease.length,
                    arena_size: self.arena.len() as u64,
                })?;
        let range = match mapped_range(&self.arena, payload_offset, self.lease.length) {
            Ok(range) => range,
            Err(error) => {
                self.active_view.store(false, Ordering::Release);
                return Err(error);
            }
        };
        Ok(WritableArenaView {
            owner: self.clone(),
            payload_offset: range.start,
            length: range.len(),
        })
    }

    pub(crate) fn seal(&self) -> Result<BlobMetadata, BlobError> {
        if self.active_view.load(Ordering::Acquire) {
            return Err(BlobError::ActiveWritableView);
        }
        self.finished
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| BlobError::AlreadyFinished)?;

        let state = mapped_state(&self.arena, self.lease)?;
        let found = state.load(Ordering::Acquire);
        if found != BlobSharedState::Writable as u64 {
            return Err(BlobError::InvalidState { found });
        }

        if let Some(digest) = &self.metadata.digest {
            let payload_offset = self.lease.offset + BLOB_HEADER_LEN;
            let range = mapped_range(&self.arena, payload_offset, self.lease.length)?;
            // SAFETY: the range is valid and no writable view is active.
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    self.arena.ptr_at(range.start).as_ptr().cast_const(),
                    range.len(),
                )
            };
            if !digest.matches(bytes) {
                state.store(BlobSharedState::Aborted as u64, Ordering::Release);
                return Err(BlobError::DigestMismatch);
            }
        }

        state.store(BlobSharedState::Sealed as u64, Ordering::Release);
        Ok(self.metadata.clone())
    }

    pub(crate) fn abort(&self) -> Result<(), BlobError> {
        if self.active_view.load(Ordering::Acquire) {
            return Err(BlobError::ActiveWritableView);
        }
        self.finished
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| BlobError::AlreadyFinished)?;
        let state = mapped_state(&self.arena, self.lease)?;
        state.store(BlobSharedState::Aborted as u64, Ordering::Release);
        Ok(())
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

pub struct WritableArenaView {
    owner: Arc<WritableBlobLease>,
    payload_offset: usize,
    length: usize,
}

impl WritableArenaView {
    pub fn len(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.owner.arena.ptr_at(self.payload_offset).as_ptr()
    }
}

impl AsRef<[u8]> for WritableArenaView {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the owner grants exactly one active writable view and retains
        // the bounds-checked stable mapping for this view's lifetime.
        unsafe { std::slice::from_raw_parts(self.as_ptr().cast_const(), self.length) }
    }
}

impl AsMut<[u8]> for WritableArenaView {
    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: `WritableBlobLease::map` admits only one live view.
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.length) }
    }
}

impl Deref for WritableArenaView {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl DerefMut for WritableArenaView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl Drop for WritableArenaView {
    fn drop(&mut self) {
        self.owner.active_view.store(false, Ordering::Release);
    }
}

pub(crate) fn install_read_blob(
    arena: &ArenaManager,
    allocation: &RingLease,
    generation: u64,
    bytes: &[u8],
    digest: Option<ContentDigest>,
) -> Result<(BlobLease, BlobMetadata), BlobError> {
    let length = u64::try_from(bytes.len()).map_err(|_| BlobError::InvalidHostLease)?;
    let lease = descriptor_from_allocation(allocation, generation, length, BlobAccess::ReadOnly)?;
    let metadata = BlobMetadata { length, digest };
    let header = host_header_ptr(arena, lease)?;
    write_header(header, lease, &metadata, BlobSharedState::Filling);

    if let Some(expected) = &metadata.digest {
        if !expected.matches(bytes) {
            host_state(arena, lease)?.store(BlobSharedState::Aborted as u64, Ordering::Release);
            return Err(BlobError::DigestMismatch);
        }
    }

    let payload = arena
        .region_ptr(lease.offset + BLOB_HEADER_LEN, length)
        .ok_or(BlobError::InvalidHostLease)?;
    // SAFETY: the allocation owns exactly `length` payload bytes and `bytes`
    // has that same length. This is the only host-to-child process-boundary copy.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), payload.as_ptr(), bytes.len());
    }
    host_state(arena, lease)?.store(BlobSharedState::Sealed as u64, Ordering::Release);
    Ok((lease, metadata))
}

pub(crate) fn install_filling_read_blob(
    arena: &ArenaManager,
    allocation: &RingLease,
    generation: u64,
    length: u64,
    digest: Option<ContentDigest>,
) -> Result<(BlobLease, BlobMetadata), BlobError> {
    let lease = descriptor_from_allocation(allocation, generation, length, BlobAccess::ReadOnly)?;
    let metadata = BlobMetadata { length, digest };
    let header = host_header_ptr(arena, lease)?;
    write_header(header, lease, &metadata, BlobSharedState::Filling);
    Ok((lease, metadata))
}

pub(crate) fn write_host_blob_chunk(
    arena: &ArenaManager,
    lease: BlobLease,
    offset: u64,
    bytes: &[u8],
) -> Result<(), BlobError> {
    let state = host_state(arena, lease)?;
    let found = state.load(Ordering::Acquire);
    if found != BlobSharedState::Filling as u64 {
        return Err(BlobError::InvalidState { found });
    }
    let count = u64::try_from(bytes.len()).map_err(|_| BlobError::InvalidHostLease)?;
    offset
        .checked_add(count)
        .filter(|end| *end <= lease.length)
        .ok_or(BlobError::RangeOutOfBounds {
            offset,
            length: count,
            arena_size: lease.length,
        })?;
    let payload = arena
        .region_ptr(lease.offset + BLOB_HEADER_LEN + offset, count)
        .ok_or(BlobError::InvalidHostLease)?;
    // SAFETY: the target range is wholly inside the uniquely filling lease.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), payload.as_ptr(), bytes.len());
    }
    Ok(())
}

pub(crate) fn seal_host_read_blob(
    arena: &ArenaManager,
    lease: BlobLease,
    metadata: &BlobMetadata,
    written: u64,
) -> Result<(), BlobError> {
    if written != lease.length || metadata.length != lease.length {
        return Err(BlobError::LengthMismatch {
            expected: lease.length,
            found: written,
        });
    }
    let state = host_state(arena, lease)?;
    let found = state.load(Ordering::Acquire);
    if found != BlobSharedState::Filling as u64 {
        return Err(BlobError::InvalidState { found });
    }
    if let Some(expected) = &metadata.digest {
        let payload = arena
            .region_ptr(lease.offset + BLOB_HEADER_LEN, lease.length)
            .ok_or(BlobError::InvalidHostLease)?;
        // SAFETY: the full payload lies inside the filling lease and all writes
        // precede this allocator-actor transition.
        let bytes = unsafe {
            std::slice::from_raw_parts(payload.as_ptr().cast_const(), lease.length as usize)
        };
        if !expected.matches(bytes) {
            state.store(BlobSharedState::Aborted as u64, Ordering::Release);
            return Err(BlobError::DigestMismatch);
        }
    }
    state.store(BlobSharedState::Sealed as u64, Ordering::Release);
    Ok(())
}

pub(crate) fn abort_host_blob(arena: &ArenaManager, lease: BlobLease) -> Result<(), BlobError> {
    host_state(arena, lease)?.store(BlobSharedState::Aborted as u64, Ordering::Release);
    Ok(())
}

pub(crate) fn install_writable_blob(
    arena: &ArenaManager,
    allocation: &RingLease,
    generation: u64,
    length: u64,
    digest: Option<ContentDigest>,
) -> Result<(BlobLease, BlobMetadata), BlobError> {
    let lease = descriptor_from_allocation(allocation, generation, length, BlobAccess::Writable)?;
    let metadata = BlobMetadata { length, digest };
    let header = host_header_ptr(arena, lease)?;
    write_header(header, lease, &metadata, BlobSharedState::Writable);
    let payload = arena
        .region_ptr(lease.offset + BLOB_HEADER_LEN, length)
        .ok_or(BlobError::InvalidHostLease)?;
    // SAFETY: the freshly allocated payload is wholly inside the live lease.
    unsafe {
        std::ptr::write_bytes(payload.as_ptr(), 0, length as usize);
    }
    Ok((lease, metadata))
}

pub(crate) fn validate_host_sealed(
    arena: &ArenaManager,
    lease: BlobLease,
    metadata: &BlobMetadata,
) -> Result<(), BlobError> {
    let base = arena
        .region_ptr(0, arena.arena_len())
        .ok_or(BlobError::InvalidHostLease)?;
    validate_header(
        base,
        arena.arena_len(),
        lease,
        metadata,
        BlobSharedState::Sealed,
    )
}

pub(crate) fn mark_host_released(arena: &ArenaManager, lease: BlobLease) -> Result<(), BlobError> {
    host_state(arena, lease)?.store(BlobSharedState::Released as u64, Ordering::Release);
    Ok(())
}

fn descriptor_from_allocation(
    allocation: &RingLease,
    generation: u64,
    length: u64,
    access: BlobAccess,
) -> Result<BlobLease, BlobError> {
    if generation == 0
        || allocation.layout.header_offset != allocation.layout.start_offset
        || allocation.layout.data_offset
            != allocation
                .layout
                .start_offset
                .checked_add(BLOB_HEADER_LEN)
                .ok_or(BlobError::InvalidHostLease)?
        || allocation.layout.data_bytes != length
    {
        return Err(BlobError::InvalidHostLease);
    }
    Ok(BlobLease {
        lease_id: BlobLeaseId(allocation.ring_id.0),
        offset: allocation.layout.start_offset,
        length,
        generation,
        access,
    })
}

fn write_header(
    header: NonNull<u8>,
    lease: BlobLease,
    metadata: &BlobMetadata,
    initial_state: BlobSharedState,
) {
    let mut bytes = [0_u8; BLOB_HEADER_LEN as usize];
    bytes[0..4].copy_from_slice(&BLOB_MAGIC.to_le_bytes());
    bytes[4..6].copy_from_slice(&BLOB_VERSION.to_le_bytes());
    bytes[ACCESS_OFFSET] = lease.access as u8;
    bytes[GENERATION_OFFSET..GENERATION_OFFSET + 8]
        .copy_from_slice(&lease.generation.to_le_bytes());
    bytes[LENGTH_OFFSET..LENGTH_OFFSET + 8].copy_from_slice(&lease.length.to_le_bytes());
    if let Some(ContentDigest::Sha256(digest)) = metadata.digest {
        bytes[DIGEST_KIND_OFFSET] = 1;
        bytes[DIGEST_OFFSET..DIGEST_OFFSET + DIGEST_LEN].copy_from_slice(&digest);
    }
    // SAFETY: `header` points to the allocation's full fixed header.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), header.as_ptr(), bytes.len());
        let state = &*header.as_ptr().add(STATE_OFFSET).cast::<AtomicU64>();
        state.store(initial_state as u64, Ordering::Release);
    }
}

fn validate_mapped_header(
    arena: &MappedArena,
    lease: BlobLease,
    metadata: &BlobMetadata,
    expected_state: BlobSharedState,
) -> Result<(), BlobError> {
    validate_header(
        NonNull::new(arena.base_ptr().cast_mut()).expect("mapped base"),
        arena.len() as u64,
        lease,
        metadata,
        expected_state,
    )
}

fn validate_header(
    base: NonNull<u8>,
    arena_size: u64,
    lease: BlobLease,
    metadata: &BlobMetadata,
    expected_state: BlobSharedState,
) -> Result<(), BlobError> {
    checked_raw_range(arena_size, lease.offset, BLOB_HEADER_LEN + lease.length)?;
    let state_address =
        lease
            .offset
            .checked_add(STATE_OFFSET as u64)
            .ok_or(BlobError::RangeOutOfBounds {
                offset: lease.offset,
                length: lease.length,
                arena_size,
            })?;
    if state_address as usize % std::mem::align_of::<AtomicU64>() != 0 {
        return Err(BlobError::UnalignedHeader {
            offset: lease.offset,
        });
    }
    // SAFETY: the full header and atomic alignment were checked above.
    let header = unsafe { base.as_ptr().add(lease.offset as usize) };
    let state = unsafe { &*header.add(STATE_OFFSET).cast::<AtomicU64>() };
    let found_state = state.load(Ordering::Acquire);
    if found_state != expected_state as u64 {
        return Err(BlobError::InvalidState { found: found_state });
    }

    // SAFETY: immutable fields were published before the acquire load above.
    let bytes =
        unsafe { std::slice::from_raw_parts(header.cast_const(), BLOB_HEADER_LEN as usize) };
    let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("magic"));
    if magic != BLOB_MAGIC {
        return Err(BlobError::BadMagic { found: magic });
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("version"));
    if version != BLOB_VERSION {
        return Err(BlobError::UnsupportedVersion { found: version });
    }
    if let Some(relative) = bytes[RESERVED_OFFSET..].iter().position(|byte| *byte != 0) {
        return Err(BlobError::ReservedBytesNotZero {
            at: RESERVED_OFFSET + relative,
        });
    }
    let header_access =
        BlobAccess::from_byte(bytes[ACCESS_OFFSET]).ok_or(BlobError::InvalidAccess {
            found: bytes[ACCESS_OFFSET],
        })?;
    if lease.access == BlobAccess::Writable && header_access != BlobAccess::Writable {
        return Err(BlobError::AccessDenied);
    }
    let found_generation = u64::from_le_bytes(
        bytes[GENERATION_OFFSET..GENERATION_OFFSET + 8]
            .try_into()
            .expect("generation"),
    );
    if found_generation != lease.generation {
        return Err(BlobError::StaleGeneration {
            expected: lease.generation,
            found: found_generation,
        });
    }
    let found_length = u64::from_le_bytes(
        bytes[LENGTH_OFFSET..LENGTH_OFFSET + 8]
            .try_into()
            .expect("length"),
    );
    if found_length != lease.length || metadata.length != lease.length {
        return Err(BlobError::LengthMismatch {
            expected: lease.length,
            found: found_length,
        });
    }
    let header_digest = match bytes[DIGEST_KIND_OFFSET] {
        0 if bytes[DIGEST_OFFSET..DIGEST_OFFSET + DIGEST_LEN]
            .iter()
            .all(|byte| *byte == 0) =>
        {
            None
        }
        1 => Some(ContentDigest::Sha256(
            bytes[DIGEST_OFFSET..DIGEST_OFFSET + DIGEST_LEN]
                .try_into()
                .expect("digest"),
        )),
        _ => return Err(BlobError::DigestMetadataMismatch),
    };
    if header_digest != metadata.digest {
        return Err(BlobError::DigestMetadataMismatch);
    }
    Ok(())
}

fn mapped_state(arena: &MappedArena, lease: BlobLease) -> Result<&AtomicU64, BlobError> {
    let range = mapped_range(arena, lease.offset, BLOB_HEADER_LEN + lease.length)?;
    let state_offset = range.start + STATE_OFFSET;
    if state_offset % std::mem::align_of::<AtomicU64>() != 0 {
        return Err(BlobError::UnalignedHeader {
            offset: lease.offset,
        });
    }
    // SAFETY: bounds and alignment are checked; mapping lifetime is borrowed.
    Ok(unsafe { &*arena.ptr_at(state_offset).as_ptr().cast::<AtomicU64>() })
}

fn host_header_ptr(arena: &ArenaManager, lease: BlobLease) -> Result<NonNull<u8>, BlobError> {
    arena
        .region_ptr(lease.offset, BLOB_HEADER_LEN + lease.length)
        .ok_or(BlobError::InvalidHostLease)
}

fn host_state(arena: &ArenaManager, lease: BlobLease) -> Result<&AtomicU64, BlobError> {
    let header = host_header_ptr(arena, lease)?;
    let address = lease.offset + STATE_OFFSET as u64;
    if address as usize % std::mem::align_of::<AtomicU64>() != 0 {
        return Err(BlobError::UnalignedHeader {
            offset: lease.offset,
        });
    }
    // SAFETY: the arena range and atomic alignment were checked above.
    Ok(unsafe { &*header.as_ptr().add(STATE_OFFSET).cast::<AtomicU64>() })
}

fn mapped_range(
    arena: &MappedArena,
    offset: u64,
    length: u64,
) -> Result<std::ops::Range<usize>, BlobError> {
    arena
        .checked_range(offset, length)
        .map_err(|_| BlobError::RangeOutOfBounds {
            offset,
            length,
            arena_size: arena.len() as u64,
        })
}

fn checked_raw_range(arena_size: u64, offset: u64, length: u64) -> Result<(), BlobError> {
    offset
        .checked_add(length)
        .filter(|end| *end <= arena_size)
        .map(|_| ())
        .ok_or(BlobError::RangeOutOfBounds {
            offset,
            length,
            arena_size,
        })
}

#[allow(dead_code)]
fn state_name(value: u64) -> Option<BlobSharedState> {
    BlobSharedState::from_u64(value)
}
