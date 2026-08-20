//! Exec-time bootstrap ABI shared by the job host and the Python binding.
//!
//! The host launches a job process with two inherited file descriptors named
//! by environment variables ([`ENV_ARENA_FD`], [`ENV_WAKE_FD`]) and, before
//! exec, writes a fixed-layout header at arena offset 0. The job-side entry
//! point maps the arena, validates the header against the descriptor's true
//! length (`fstat`, never the header's own claim), and only then constructs
//! the application context's data plane.
//!
//! All identities (actors, nodes, edges, rings) stay on the host side of this
//! boundary: the header names one control-ring region and nothing else.
//!
//! Layout (all integers little-endian, no implicit padding):
//!
//! | offset | size | field                         |
//! |--------|------|-------------------------------|
//! | 0      | 4    | magic (`SWBS`)                |
//! | 4      | 2    | version                       |
//! | 6      | 2    | reserved (must be zero)       |
//! | 8      | 8    | arena_size                    |
//! | 16     | 8    | control_ring_offset           |
//! | 24     | 8    | control_ring_capacity         |
//! | 32     | 8    | control_ring_generation       |
//! | 40     | 8    | reserved (must be zero)       |

use std::fmt;

/// `"SWBS"` read little-endian.
pub const BOOTSTRAP_MAGIC: u32 = u32::from_le_bytes(*b"SWBS");
/// The only version this crate understands.
pub const BOOTSTRAP_VERSION: u16 = 1;

/// Environment variable naming the inherited arena memfd.
pub const ENV_ARENA_FD: &str = "SWACTOR_ARENA_FD";
/// Environment variable naming the inherited wake descriptor.
pub const ENV_WAKE_FD: &str = "SWACTOR_WAKE_FD";

/// Fixed byte length of the bootstrap header at arena offset 0.
pub const HEADER_LEN: usize = 48;
/// Arena offsets `[0, HEADER_END_OFFSET)` belong to the bootstrap header.
pub const HEADER_END_OFFSET: u64 = HEADER_LEN as u64;

const RESERVED0_OFFSET: usize = 6;
const RESERVED1_OFFSET: usize = 40;

fn read_u32(page: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(page[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u16(page: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(page[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u64(page: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(page[offset..offset + 8].try_into().expect("u64 slice"))
}

/// The control-ring region named by the bootstrap header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlRingLayout {
    pub offset: u64,
    pub capacity: u64,
    pub generation: u64,
}

/// The validated bootstrap header, as encoded at arena offset 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapHeader {
    pub arena_size: u64,
    pub control_ring: ControlRingLayout,
}

impl BootstrapHeader {
    /// Encode into the fixed 48-byte little-endian layout.
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut page = [0_u8; HEADER_LEN];
        page[0..4].copy_from_slice(&BOOTSTRAP_MAGIC.to_le_bytes());
        page[4..6].copy_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
        page[8..16].copy_from_slice(&self.arena_size.to_le_bytes());
        page[16..24].copy_from_slice(&self.control_ring.offset.to_le_bytes());
        page[24..32].copy_from_slice(&self.control_ring.capacity.to_le_bytes());
        page[32..40].copy_from_slice(&self.control_ring.generation.to_le_bytes());
        // Reserved bytes stay zero.
        page
    }
}

/// The result of a successful bootstrap parse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedBootstrap {
    pub arena_size: u64,
    pub control_ring: ControlRingLayout,
}

/// Every way a bootstrap page can fail validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    /// Fewer than [`HEADER_LEN`] bytes available at offset 0.
    Truncated { available: usize },
    BadMagic { found: u32 },
    /// A reserved field is nonzero: the page is not a v1-shape header.
    ReservedBytesNotZero { at: usize },
    UnsupportedVersion { found: u16, supported: u16 },
    /// The header's claimed size disagrees with the descriptor's true length.
    ArenaSizeMismatch { header: u64, backing: u64 },
    /// The control ring overlaps the bootstrap header region.
    RingOverlapsHeader { offset: u64 },
    /// The control ring (or its end offset) falls outside the arena, including
    /// offset+capacity overflow.
    RingOutOfBounds {
        offset: u64,
        capacity: u64,
        arena_size: u64,
    },
    ZeroRingCapacity,
    ZeroRingGeneration,
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { available } => {
                write!(f, "bootstrap header truncated: {available} of {HEADER_LEN} bytes")
            }
            Self::BadMagic { found } => {
                write!(f, "bootstrap magic mismatch: found {found:#010x}")
            }
            Self::ReservedBytesNotZero { at } => {
                write!(f, "bootstrap reserved bytes at offset {at} are nonzero")
            }
            Self::UnsupportedVersion { found, supported } => {
                write!(f, "unsupported bootstrap version {found} (supported: {supported})")
            }
            Self::ArenaSizeMismatch { header, backing } => {
                write!(
                    f,
                    "bootstrap arena size {header} disagrees with backing length {backing}"
                )
            }
            Self::RingOverlapsHeader { offset } => {
                write!(f, "control ring offset {offset} overlaps the bootstrap header")
            }
            Self::RingOutOfBounds {
                offset,
                capacity,
                arena_size,
            } => {
                write!(
                    f,
                    "control ring [{offset}, +{capacity}] exceeds arena size {arena_size}"
                )
            }
            Self::ZeroRingCapacity => write!(f, "control ring capacity is zero"),
            Self::ZeroRingGeneration => write!(f, "control ring generation is zero"),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Validate a bootstrap page read from arena offset 0.
///
/// `backing_len` is the true length of the arena descriptor (from `fstat` on
/// the job side, from the arena config on the host side) — never a value
/// taken from the page itself.
pub fn parse_bootstrap(page: &[u8], backing_len: u64) -> Result<ResolvedBootstrap, BootstrapError> {
    if page.len() < HEADER_LEN {
        return Err(BootstrapError::Truncated {
            available: page.len(),
        });
    }
    let magic = read_u32(page, 0);
    if magic != BOOTSTRAP_MAGIC {
        return Err(BootstrapError::BadMagic { found: magic });
    }
    if read_u16(page, RESERVED0_OFFSET) != 0 {
        return Err(BootstrapError::ReservedBytesNotZero {
            at: RESERVED0_OFFSET,
        });
    }
    let version = read_u16(page, 4);
    if version != BOOTSTRAP_VERSION {
        return Err(BootstrapError::UnsupportedVersion {
            found: version,
            supported: BOOTSTRAP_VERSION,
        });
    }
    if page[RESERVED1_OFFSET..HEADER_LEN].iter().any(|&b| b != 0) {
        return Err(BootstrapError::ReservedBytesNotZero {
            at: RESERVED1_OFFSET,
        });
    }
    let arena_size = read_u64(page, 8);
    if arena_size != backing_len {
        return Err(BootstrapError::ArenaSizeMismatch {
            header: arena_size,
            backing: backing_len,
        });
    }
    let control_ring = ControlRingLayout {
        offset: read_u64(page, 16),
        capacity: read_u64(page, 24),
        generation: read_u64(page, 32),
    };
    if control_ring.capacity == 0 {
        return Err(BootstrapError::ZeroRingCapacity);
    }
    if control_ring.generation == 0 {
        return Err(BootstrapError::ZeroRingGeneration);
    }
    if control_ring.offset < HEADER_END_OFFSET {
        return Err(BootstrapError::RingOverlapsHeader {
            offset: control_ring.offset,
        });
    }
    match control_ring.offset.checked_add(control_ring.capacity) {
        Some(end) if end <= arena_size => Ok(ResolvedBootstrap {
            arena_size,
            control_ring,
        }),
        _ => Err(BootstrapError::RingOutOfBounds {
            offset: control_ring.offset,
            capacity: control_ring.capacity,
            arena_size,
        }),
    }
}

// ─── Host-side writer ────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod write {
    use std::collections::BTreeMap;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use crate::arena::{
        ArenaEvent, ArenaManager, ArenaRequest, LeaseRing, LeaseRequestId, RingLease,
        RingLeaseRejection, RingSpec,
    };
    use crate::bootstrap::{
        BootstrapHeader, ControlRingLayout, HEADER_LEN as HEADER_REGION_BYTES,
        HEADER_END_OFFSET,
    };

    /// First generation stamped into a freshly written control ring.
    pub const CONTROL_RING_GENERATION: u64 = 1;

    /// Request id for the one-time bootstrap-header region lease.
    const HEADER_REGION_REQUEST_ID: u64 = 1;
    /// Request id for the control-ring lease.
    const CONTROL_RING_REQUEST_ID: u64 = 2;

    const ZERO_CHUNK: usize = 4 * 1024;

    /// Caller-requested shape of the control ring.
    #[derive(Clone, Copy, Debug)]
    pub struct ControlRingSpec {
        pub data_bytes: u64,
        pub alignment: u64,
    }

    /// Everything the spawner needs to exec a job process.
    #[derive(Debug)]
    pub struct JobHandoff {
        /// Environment map naming the two inherited descriptors.
        pub env: BTreeMap<String, String>,
        /// Inheritable (no `FD_CLOEXEC`) duplicate of the arena memfd.
        pub arena_fd: OwnedFd,
        /// Inheritable (no `FD_CLOEXEC`) duplicate of the wake eventfd.
        pub wake_fd: OwnedFd,
        /// Host-side `EFD_CLOEXEC` eventfd copy; write to it to wake the job.
        pub host_wake_fd: OwnedFd,
        /// The control-ring region recorded in the bootstrap header.
        pub control_ring: ControlRingLayout,
    }

    #[derive(Debug)]
    pub enum BootstrapWriteError {
        Io(std::io::Error),
        InvalidControlRingSpec,
        ControlRingLeaseRejected(RingLeaseRejection),
        /// A fresh arena must place leases immediately; queueing is a bug.
        ControlRingLeaseQueued,
        UnexpectedLeaseOutcome,
        FdSetup(std::io::Error),
    }

    impl std::fmt::Display for BootstrapWriteError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Io(error) => write!(f, "arena write failed: {error}"),
                Self::InvalidControlRingSpec => {
                    write!(f, "control ring spec needs nonzero data_bytes and alignment")
                }
                Self::ControlRingLeaseRejected(reason) => {
                    write!(f, "control ring lease rejected: {reason:?}")
                }
                Self::ControlRingLeaseQueued => {
                    write!(f, "control ring lease queued on a fresh arena")
                }
                Self::UnexpectedLeaseOutcome => {
                    write!(f, "arena produced an unexpected lease outcome")
                }
                Self::FdSetup(error) => write!(f, "bootstrap fd setup failed: {error}"),
            }
        }
    }

    impl std::error::Error for BootstrapWriteError {}

    /// Reserve the bootstrap header region, lease and initialize the control
    /// ring, write the header, and produce the job-process handoff.
    ///
    /// The header region (`0..HEADER_END_OFFSET`, rounded up by arena
    /// alignment) and the control ring are disjoint leases from the arena's
    /// own placement law, so no other lease can ever overlap them. All writes
    /// complete before this returns: after [`JobHandoff`] exists, the child
    /// may exec at any time.
    pub fn write_bootstrap(
        arena: &mut ArenaManager,
        spec: ControlRingSpec,
    ) -> Result<JobHandoff, BootstrapWriteError> {
        use super::ControlRingLayout as Layout;

        if spec.data_bytes == 0 || spec.alignment == 0 {
            return Err(BootstrapWriteError::InvalidControlRingSpec);
        }

        // Reserve offset 0 for the header so no ring can ever be placed there.
        let header_lease = lease(
            arena,
            HEADER_REGION_REQUEST_ID,
            RingSpec {
                header_bytes: HEADER_REGION_BYTES as u64,
                data_bytes: 0,
                alignment: spec.alignment,
            },
        )?;
        debug_assert!(header_lease.layout.start_offset < HEADER_END_OFFSET);

        let ring_lease = lease(
            arena,
            CONTROL_RING_REQUEST_ID,
            RingSpec {
                // The control ring's own header is a u64 generation slot for
                // now; framing arrives with path resolution.
                header_bytes: 8,
                data_bytes: spec.data_bytes,
                alignment: spec.alignment,
            },
        )?;

        let ring = Layout {
            offset: ring_lease.layout.start_offset,
            capacity: ring_lease.layout.end_offset - ring_lease.layout.start_offset,
            generation: CONTROL_RING_GENERATION,
        };

        zero_region(arena, ring_lease.layout.start_offset, ring_lease.layout.end_offset)?;
        arena
            .write_arena(ring_lease.layout.start_offset, &ring.generation.to_le_bytes())
            .map_err(BootstrapWriteError::Io)?;

        let header = BootstrapHeader {
            arena_size: arena.arena_len(),
            control_ring: ring,
        };
        arena
            .write_arena(header_lease.layout.start_offset, &header.encode())
            .map_err(BootstrapWriteError::Io)?;

        let host_wake_fd = create_wake_eventfd()?;
        let wake_fd = dup_without_cloexec(host_wake_fd.as_raw_fd())?;
        let arena_fd = dup_without_cloexec(arena.arena_fd())?;

        let env = BTreeMap::from([
            (super::ENV_ARENA_FD.to_owned(), arena_fd.as_raw_fd().to_string()),
            (super::ENV_WAKE_FD.to_owned(), wake_fd.as_raw_fd().to_string()),
        ]);

        Ok(JobHandoff {
            env,
            arena_fd,
            wake_fd,
            host_wake_fd,
            control_ring: ring,
        })
    }

    fn lease(
        arena: &mut ArenaManager,
        request_id: u64,
        ring_spec: RingSpec,
    ) -> Result<RingLease, BootstrapWriteError> {
        let request = LeaseRing {
            request_id: LeaseRequestId(request_id),
            ring_spec,
        };
        let mut events = arena.request(ArenaRequest::LeaseRing(request));
        match (events.len(), events.pop()) {
            (1, Some(ArenaEvent::RingLeased { lease })) => Ok(lease),
            (1, Some(ArenaEvent::RingLeaseRejected { reason, .. })) => {
                Err(BootstrapWriteError::ControlRingLeaseRejected(reason))
            }
            (1, Some(ArenaEvent::RingLeaseQueued { .. })) => {
                Err(BootstrapWriteError::ControlRingLeaseQueued)
            }
            _ => Err(BootstrapWriteError::UnexpectedLeaseOutcome),
        }
    }

    fn zero_region(
        arena: &ArenaManager,
        start: u64,
        end: u64,
    ) -> Result<(), BootstrapWriteError> {
        let zeros = [0_u8; ZERO_CHUNK];
        let mut offset = start;
        while offset < end {
            let take = ((end - offset) as usize).min(ZERO_CHUNK);
            arena
                .write_arena(offset, &zeros[..take])
                .map_err(BootstrapWriteError::Io)?;
            offset += take as u64;
        }
        Ok(())
    }

    fn create_wake_eventfd() -> Result<OwnedFd, BootstrapWriteError> {
        // SAFETY: eventfd returns a new owned fd or -1.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(BootstrapWriteError::FdSetup(std::io::Error::last_os_error()));
        }
        // SAFETY: fd is a fresh, uniquely owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// `dup` clears `FD_CLOEXEC` on the copy, which is exactly the child's
    /// view: inheritable at exec, and nothing else changes.
    fn dup_without_cloexec(fd: std::os::fd::RawFd) -> Result<OwnedFd, BootstrapWriteError> {
        // SAFETY: dup returns a new owned fd or -1.
        let dup = unsafe { libc::dup(fd) };
        if dup < 0 {
            return Err(BootstrapWriteError::FdSetup(std::io::Error::last_os_error()));
        }
        // SAFETY: dup is a fresh, uniquely owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(dup) })
    }
}

#[cfg(target_os = "linux")]
pub use write::{
    BootstrapWriteError, ControlRingSpec, JobHandoff, CONTROL_RING_GENERATION, write_bootstrap,
};
