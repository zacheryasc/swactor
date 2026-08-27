//! Exec-time bootstrap ABI shared by job hosts and language bindings.
//!
//! The child inherits exactly one descriptor: the shared arena. Actor-routing
//! capability and session identity are private, non-descriptor metadata.
//!
//! Layout (all integers little-endian, no implicit padding):
//!
//! | offset | size | field                         |
//! |--------|------|-------------------------------|
//! | 0      | 4    | magic (`SWBS`)                |
//! | 4      | 2    | version (2)                   |
//! | 6      | 2    | reserved (zero)               |
//! | 8      | 8    | arena size                    |
//! | 16     | 8    | arena generation              |
//! | 24     | 8    | private control offset or zero|
//! | 32     | 8    | private control length or zero|
//! | 40     | 24   | reserved (zero)               |

pub mod channel;
use std::fmt;

/// `"SWBS"` read little-endian.
pub const BOOTSTRAP_MAGIC: u32 = u32::from_le_bytes(*b"SWBS");
/// The only version this crate understands.
pub const BOOTSTRAP_VERSION: u16 = 2;

/// Fixed byte length of the v2 bootstrap header.
pub const HEADER_LEN: usize = 64;
pub const HEADER_END_OFFSET: u64 = HEADER_LEN as u64;

fn read_u16(page: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(page[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u32(page: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(page[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u64(page: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(page[offset..offset + 8].try_into().expect("u64 slice"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlRegion {
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapHeader {
    pub arena_size: u64,
    pub arena_generation: u64,
    pub control_region: Option<ControlRegion>,
}

impl BootstrapHeader {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut page = [0_u8; HEADER_LEN];
        page[0..4].copy_from_slice(&BOOTSTRAP_MAGIC.to_le_bytes());
        page[4..6].copy_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
        page[8..16].copy_from_slice(&self.arena_size.to_le_bytes());
        page[16..24].copy_from_slice(&self.arena_generation.to_le_bytes());
        if let Some(control) = self.control_region {
            page[24..32].copy_from_slice(&control.offset.to_le_bytes());
            page[32..40].copy_from_slice(&control.length.to_le_bytes());
        }
        page
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedBootstrap {
    pub arena_size: u64,
    pub arena_generation: u64,
    pub control_region: Option<ControlRegion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    Truncated {
        available: usize,
    },
    BadMagic {
        found: u32,
    },
    ReservedBytesNotZero {
        at: usize,
    },
    UnsupportedVersion {
        found: u16,
        supported: u16,
    },
    ArenaSizeMismatch {
        header: u64,
        backing: u64,
    },
    ZeroArenaGeneration,
    PartialControlRegion {
        offset: u64,
        length: u64,
    },
    ControlOverlapsHeader {
        offset: u64,
    },
    ControlOutOfBounds {
        offset: u64,
        length: u64,
        arena_size: u64,
    },
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { available } => {
                write!(
                    f,
                    "bootstrap header truncated: {available} of {HEADER_LEN} bytes"
                )
            }
            Self::BadMagic { found } => {
                write!(f, "bootstrap magic mismatch: found {found:#010x}")
            }
            Self::ReservedBytesNotZero { at } => {
                write!(f, "bootstrap reserved bytes at offset {at} are nonzero")
            }
            Self::UnsupportedVersion { found, supported } => {
                write!(
                    f,
                    "unsupported bootstrap version {found} (supported: {supported})"
                )
            }
            Self::ArenaSizeMismatch { header, backing } => {
                write!(
                    f,
                    "bootstrap arena size {header} disagrees with backing length {backing}"
                )
            }
            Self::ZeroArenaGeneration => f.write_str("bootstrap arena generation is zero"),
            Self::PartialControlRegion { offset, length } => write!(
                f,
                "bootstrap control region must set both offset and length (offset={offset}, length={length})"
            ),
            Self::ControlOverlapsHeader { offset } => {
                write!(
                    f,
                    "bootstrap control region offset {offset} overlaps the header"
                )
            }
            Self::ControlOutOfBounds {
                offset,
                length,
                arena_size,
            } => write!(
                f,
                "bootstrap control region at {offset} with length {length} exceeds arena size {arena_size}"
            ),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Validate a v2 bootstrap page against the arena descriptor's true length.
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
    if page[6..8].iter().any(|byte| *byte != 0) {
        return Err(BootstrapError::ReservedBytesNotZero { at: 6 });
    }
    if let Some(relative) = page[40..HEADER_LEN].iter().position(|byte| *byte != 0) {
        return Err(BootstrapError::ReservedBytesNotZero { at: 40 + relative });
    }
    let version = read_u16(page, 4);
    if version != BOOTSTRAP_VERSION {
        return Err(BootstrapError::UnsupportedVersion {
            found: version,
            supported: BOOTSTRAP_VERSION,
        });
    }
    let arena_size = read_u64(page, 8);
    if arena_size != backing_len {
        return Err(BootstrapError::ArenaSizeMismatch {
            header: arena_size,
            backing: backing_len,
        });
    }
    let arena_generation = read_u64(page, 16);
    if arena_generation == 0 {
        return Err(BootstrapError::ZeroArenaGeneration);
    }

    let control_offset = read_u64(page, 24);
    let control_length = read_u64(page, 32);
    let control_region = match (control_offset, control_length) {
        (0, 0) => None,
        (0, _) | (_, 0) => {
            return Err(BootstrapError::PartialControlRegion {
                offset: control_offset,
                length: control_length,
            });
        }
        (offset, length) => {
            if offset < HEADER_END_OFFSET {
                return Err(BootstrapError::ControlOverlapsHeader { offset });
            }
            offset
                .checked_add(length)
                .filter(|end| *end <= arena_size)
                .ok_or(BootstrapError::ControlOutOfBounds {
                    offset,
                    length,
                    arena_size,
                })?;
            Some(ControlRegion { offset, length })
        }
    };

    Ok(ResolvedBootstrap {
        arena_size,
        arena_generation,
        control_region,
    })
}

#[cfg(target_os = "linux")]
mod write {
    use std::os::fd::{FromRawFd, OwnedFd};

    use crate::arena::{
        ArenaEvent, ArenaManager, ArenaRequest, LeaseRequestId, LeaseRing, RingLease,
        RingLeaseRejection, RingSpec,
    };

    const HEADER_REQUEST_ID: u64 = 1;

    #[derive(Clone, Copy, Debug)]
    pub struct BootstrapSpec {
        pub arena_generation: u64,
        pub alignment: u64,
    }
    #[derive(Debug)]
    pub struct PreparedArena {
        pub arena_fd: OwnedFd,
        pub arena_generation: u64,
    }

    #[derive(Debug)]
    pub enum BootstrapWriteError {
        InvalidSpec,
        HeaderLeaseRejected(RingLeaseRejection),
        HeaderLeaseQueued,
        UnexpectedLeaseOutcome,
        Io(std::io::Error),
        FdSetup(std::io::Error),
    }

    impl std::fmt::Display for BootstrapWriteError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::InvalidSpec => {
                    f.write_str("arena generation and bootstrap alignment must be nonzero")
                }
                Self::HeaderLeaseRejected(reason) => {
                    write!(f, "bootstrap header lease rejected: {reason:?}")
                }
                Self::HeaderLeaseQueued => {
                    f.write_str("bootstrap header lease queued on a fresh arena")
                }
                Self::UnexpectedLeaseOutcome => {
                    f.write_str("unexpected bootstrap arena lease outcome")
                }
                Self::Io(error) => write!(f, "write bootstrap header: {error}"),
                Self::FdSetup(error) => write!(f, "bootstrap descriptor setup: {error}"),
            }
        }
    }

    impl std::error::Error for BootstrapWriteError {}

    pub fn prepare_arena(
        arena: &mut ArenaManager,
        spec: BootstrapSpec,
    ) -> Result<PreparedArena, BootstrapWriteError> {
        if spec.arena_generation == 0 || spec.alignment == 0 {
            return Err(BootstrapWriteError::InvalidSpec);
        }

        let header_lease = lease_header(arena, spec.alignment)?;
        if header_lease.layout.start_offset != 0 {
            return Err(BootstrapWriteError::UnexpectedLeaseOutcome);
        }
        let header = super::BootstrapHeader {
            arena_size: arena.arena_len(),
            arena_generation: spec.arena_generation,
            control_region: None,
        };
        arena
            .write_arena(header_lease.layout.start_offset, &header.encode())
            .map_err(BootstrapWriteError::Io)?;

        let arena_fd = dup_cloexec(arena.arena_fd())?;
        Ok(PreparedArena {
            arena_fd,
            arena_generation: spec.arena_generation,
        })
    }

    fn lease_header(
        arena: &mut ArenaManager,
        alignment: u64,
    ) -> Result<RingLease, BootstrapWriteError> {
        let request = LeaseRing {
            request_id: LeaseRequestId(HEADER_REQUEST_ID),
            ring_spec: RingSpec {
                header_bytes: super::HEADER_LEN as u64,
                data_bytes: 0,
                alignment,
            },
        };
        let mut events = arena.request(ArenaRequest::LeaseRing(request));
        match (events.len(), events.pop()) {
            (1, Some(ArenaEvent::RingLeased { lease })) => Ok(lease),
            (1, Some(ArenaEvent::RingLeaseRejected { reason, .. })) => {
                Err(BootstrapWriteError::HeaderLeaseRejected(reason))
            }
            (1, Some(ArenaEvent::RingLeaseQueued { .. })) => {
                Err(BootstrapWriteError::HeaderLeaseQueued)
            }
            _ => Err(BootstrapWriteError::UnexpectedLeaseOutcome),
        }
    }

    fn dup_cloexec(fd: std::os::fd::RawFd) -> Result<OwnedFd, BootstrapWriteError> {
        // SAFETY: F_DUPFD_CLOEXEC returns a fresh close-on-exec descriptor or -1.
        let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
        if duplicated < 0 {
            return Err(BootstrapWriteError::FdSetup(std::io::Error::last_os_error()));
        }
        // SAFETY: `duplicated` is fresh and uniquely owned.
        Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
    }
}

#[cfg(target_os = "linux")]
pub use write::{BootstrapSpec, BootstrapWriteError, PreparedArena, prepare_arena};
