//! Process-local ownership of the shared data-plane arena mapping.

use std::fmt;
use std::ops::Range;
use std::os::fd::OwnedFd;
use std::ptr::NonNull;

use crate::bootstrap::{BootstrapError, HEADER_LEN, ResolvedBootstrap, parse_bootstrap};

#[derive(Debug)]
pub enum ArenaMapError {
    Io(std::io::Error),
    BackingTooLarge(u64),
    Bootstrap(BootstrapError),
    RangeOutOfBounds {
        offset: u64,
        length: u64,
        arena_size: u64,
    },
    UnsupportedPlatform,
}

impl fmt::Display for ArenaMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "arena mapping: {error}"),
            Self::BackingTooLarge(length) => {
                write!(
                    f,
                    "arena backing length {length} exceeds process address space"
                )
            }
            Self::Bootstrap(error) => write!(f, "arena bootstrap: {error}"),
            Self::RangeOutOfBounds {
                offset,
                length,
                arena_size,
            } => write!(
                f,
                "arena range at {offset} with length {length} exceeds arena size {arena_size}"
            ),
            Self::UnsupportedPlatform => f.write_str("shared arena mappings require Linux"),
        }
    }
}

impl std::error::Error for ArenaMapError {}

impl From<BootstrapError> for ArenaMapError {
    fn from(error: BootstrapError) -> Self {
        Self::Bootstrap(error)
    }
}

/// Stable process-local mapping of one validated arena backing.
pub struct MappedArena {
    ptr: NonNull<u8>,
    len: usize,
}

// The mapping is stable for `Self`'s lifetime. Cross-thread and cross-process
// access is exposed only through lease types that enforce publication and
// aliasing; moving the owner or sharing an immutable handle does not itself
// access mapped bytes.
unsafe impl Send for MappedArena {}
unsafe impl Sync for MappedArena {}

impl MappedArena {
    /// Take, map, validate, and close the inherited arena descriptor.
    #[cfg(target_os = "linux")]
    pub fn map(fd: OwnedFd) -> Result<(Self, ResolvedBootstrap), ArenaMapError> {
        let file = std::fs::File::from(fd);
        let backing_len = file.metadata().map_err(ArenaMapError::Io)?.len();
        let len = usize::try_from(backing_len)
            .map_err(|_| ArenaMapError::BackingTooLarge(backing_len))?;
        if len < HEADER_LEN {
            return Err(ArenaMapError::Bootstrap(BootstrapError::Truncated {
                available: len,
            }));
        }

        // SAFETY: `file` is live, `len` is its true nonzero backing length, and
        // MAP_FAILED is checked before constructing the owner.
        let mapped = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                std::os::fd::AsRawFd::as_raw_fd(&file),
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            return Err(ArenaMapError::Io(std::io::Error::last_os_error()));
        }

        let arena = Self {
            ptr: NonNull::new(mapped.cast()).expect("mmap returned non-null"),
            len,
        };
        let resolved = parse_bootstrap(arena.header(), backing_len)?;
        Ok((arena, resolved))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn map(_fd: OwnedFd) -> Result<(Self, ResolvedBootstrap), ArenaMapError> {
        Err(ArenaMapError::UnsupportedPlatform)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn base_ptr(&self) -> *const u8 {
        self.ptr.as_ptr().cast_const()
    }

    pub(crate) fn checked_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Range<usize>, ArenaMapError> {
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.len as u64)
            .ok_or(ArenaMapError::RangeOutOfBounds {
                offset,
                length,
                arena_size: self.len as u64,
            })?;
        let start = usize::try_from(offset).map_err(|_| ArenaMapError::RangeOutOfBounds {
            offset,
            length,
            arena_size: self.len as u64,
        })?;
        let end = usize::try_from(end).map_err(|_| ArenaMapError::RangeOutOfBounds {
            offset,
            length,
            arena_size: self.len as u64,
        })?;
        Ok(start..end)
    }

    pub(crate) fn ptr_at(&self, offset: usize) -> NonNull<u8> {
        debug_assert!(offset <= self.len);
        // SAFETY: callers obtain `offset` from `checked_range`; one-past-end is
        // permitted for zero-length slices.
        unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(offset)) }
    }

    fn header(&self) -> &[u8] {
        // SAFETY: construction requires a mapping at least HEADER_LEN bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), HEADER_LEN) }
    }
}

impl Drop for MappedArena {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        // SAFETY: unmaps exactly the mapping created by `MappedArena::map`.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}
