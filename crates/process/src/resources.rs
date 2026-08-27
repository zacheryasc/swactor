use std::fmt;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::Command;

/// Descriptors owned by one process spawn attempt.
///
/// Every source remains close-on-exec in the parent. During child setup each
/// source is duplicated to its declared child descriptor without opening an
/// ambient parent-side inheritance window.
#[derive(Debug, Default)]
pub struct ProcessSpawnResources {
    descriptors: Vec<DescriptorMapping>,
}

#[derive(Debug)]
struct DescriptorMapping {
    source: OwnedFd,
    child_fd: RawFd,
}

#[derive(Debug)]
pub enum ProcessResourceError {
    InvalidSource(io::Error),
    InvalidChildDescriptor(RawFd),
    DuplicateChildDescriptor(RawFd),
}

impl fmt::Display for ProcessResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSource(error) => write!(f, "invalid process resource descriptor: {error}"),
            Self::InvalidChildDescriptor(fd) => {
                write!(
                    f,
                    "child process resource descriptor must be at least 3: {fd}"
                )
            }
            Self::DuplicateChildDescriptor(fd) => {
                write!(f, "duplicate child process resource descriptor: {fd}")
            }
        }
    }
}

impl std::error::Error for ProcessResourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidSource(error) => Some(error),
            Self::InvalidChildDescriptor(_) | Self::DuplicateChildDescriptor(_) => None,
        }
    }
}

impl ProcessSpawnResources {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one owned source descriptor and its descriptor number in the child.
    pub fn add_descriptor(
        &mut self,
        source: OwnedFd,
        child_fd: RawFd,
    ) -> Result<(), ProcessResourceError> {
        if child_fd < 3 {
            return Err(ProcessResourceError::InvalidChildDescriptor(child_fd));
        }
        if self
            .descriptors
            .iter()
            .any(|mapping| mapping.child_fd == child_fd)
        {
            return Err(ProcessResourceError::DuplicateChildDescriptor(child_fd));
        }

        set_cloexec(source.as_raw_fd()).map_err(ProcessResourceError::InvalidSource)?;
        self.descriptors
            .push(DescriptorMapping { source, child_fd });
        Ok(())
    }

    pub fn with_descriptor(
        mut self,
        source: OwnedFd,
        child_fd: RawFd,
    ) -> Result<Self, ProcessResourceError> {
        self.add_descriptor(source, child_fd)?;
        Ok(self)
    }

    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }

    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    pub(crate) fn configure_command(&self, command: &mut Command) {
        if self.descriptors.is_empty() {
            return;
        }

        let mappings = self
            .descriptors
            .iter()
            .map(|mapping| (mapping.source.as_raw_fd(), mapping.child_fd))
            .collect::<Vec<_>>();
        let temporary_floor = mappings
            .iter()
            .map(|(_, child_fd)| *child_fd)
            .max()
            .and_then(|fd| fd.checked_add(1))
            .unwrap_or(3);
        let mut temporary = vec![-1; mappings.len()];

        // SAFETY: the closure performs only async-signal-safe descriptor syscalls
        // and mutates storage allocated before `fork`.
        unsafe {
            command.pre_exec(move || {
                map_child_descriptors(&mappings, &mut temporary, temporary_floor)
            });
        }
    }
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is borrowed from a live OwnedFd and F_GETFD/F_SETFD do not
    // transfer ownership.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::FD_CLOEXEC == 0 {
        // SAFETY: same live descriptor; flags preserve all existing bits.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn map_child_descriptors(
    mappings: &[(RawFd, RawFd)],
    temporary: &mut [RawFd],
    temporary_floor: RawFd,
) -> io::Result<()> {
    for (index, (source, _)) in mappings.iter().enumerate() {
        // F_DUPFD_CLOEXEC prevents one mapping from destroying a source needed
        // by a later mapping and keeps temporary copies closed across exec.
        // SAFETY: `source` is kept alive by ProcessSpawnResources until spawn
        // resolves; fcntl returns a new descriptor or -1.
        let duplicated = unsafe { libc::fcntl(*source, libc::F_DUPFD_CLOEXEC, temporary_floor) };
        if duplicated < 0 {
            close_descriptors(&temporary[..index]);
            return Err(io::Error::last_os_error());
        }
        temporary[index] = duplicated;
    }

    for (index, (_, child_fd)) in mappings.iter().enumerate() {
        // SAFETY: both descriptors are valid in this child. dup2 atomically
        // replaces child_fd and clears FD_CLOEXEC on the resulting descriptor.
        if unsafe { libc::dup2(temporary[index], *child_fd) } < 0 {
            close_descriptors(temporary);
            return Err(io::Error::last_os_error());
        }
    }

    close_descriptors(temporary);
    Ok(())
}

fn close_descriptors(descriptors: &[RawFd]) {
    for fd in descriptors.iter().copied().filter(|fd| *fd >= 0) {
        // SAFETY: each temporary descriptor was created uniquely above and is
        // closed exactly once in this child.
        let _ = unsafe { libc::close(fd) };
    }
}
