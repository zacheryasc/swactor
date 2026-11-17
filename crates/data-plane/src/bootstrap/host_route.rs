//! Terminal observation of the process owning an inherited bootstrap channel.
//!
//! Contextual hosts create the Unix bootstrap socket and own the advertised
//! host-session actor in that process. Its pidfd fences the process identity:
//! reconnects and transient transport loss are not interpreted as host death.

use std::io;
use std::net::Shutdown;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use swactor::actor::ActorAddress;
use swactor_engine::{BlockingWorkHandle, BlockingWorkSender};

use super::channel::CHILD_BOOTSTRAP_FD;
use crate::data_plane::{HostSessionRouteObserver, HostSessionRouteWatch};

/// Host-process liveness established from the kernel's bootstrap peer identity.
pub struct BootstrapHostRoute {
    host: Arc<OwnedFd>,
    blocking: BlockingWorkSender,
}

impl BootstrapHostRoute {
    /// Capture the concrete host before `GuestBootstrap::claim` consumes the fd.
    pub fn inherited(blocking: BlockingWorkSender) -> io::Result<Self> {
        // Linux SO_PEERPIDFD (since 6.5) captures the peer atomically. Older
        // kernels can safely pin our still-current direct parent instead.
        const SO_PEERPIDFD: libc::c_int = 77;
        let mut descriptor: RawFd = -1;
        let mut length = std::mem::size_of::<RawFd>() as libc::socklen_t;
        // SAFETY: both output pointers refer to initialized, correctly sized values.
        let result = unsafe {
            libc::getsockopt(
                CHILD_BOOTSTRAP_FD,
                libc::SOL_SOCKET,
                SO_PEERPIDFD,
                (&raw mut descriptor).cast(),
                &raw mut length,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOPROTOOPT) | Some(libc::EOPNOTSUPP)
            ) {
                return Ok(Self {
                    host: Arc::new(inherited_parent_pidfd()?),
                    blocking,
                });
            }
            return Err(error);
        }
        if descriptor < 0 {
            return Err(io::Error::other(
                "bootstrap peer did not yield a process descriptor",
            ));
        }
        // SAFETY: successful SO_PEERPIDFD transfers this fresh descriptor to us.
        let host = unsafe { OwnedFd::from_raw_fd(descriptor) };
        Ok(Self {
            host: Arc::new(host),
            blocking,
        })
    }
}

fn inherited_parent_pidfd() -> io::Result<OwnedFd> {
    let mut peer = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: peer and length are initialized outputs of the expected sizes.
    let result = unsafe {
        libc::getsockopt(
            CHILD_BOOTSTRAP_FD,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut peer).cast(),
            &raw mut length,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize != std::mem::size_of::<libc::ucred>() || peer.pid <= 0 {
        return Err(io::Error::other(
            "bootstrap channel has no concrete peer process",
        ));
    }
    // SAFETY: pidfd_open accepts a numeric pid and zero flags, returning a new fd.
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, peer.pid, 0_u32) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful pidfd_open transfers ownership of the returned descriptor.
    let host = unsafe { OwnedFd::from_raw_fd(descriptor as RawFd) };
    // SAFETY: getppid has no arguments or memory-safety preconditions.
    let parent = unsafe { libc::getppid() };
    if parent != peer.pid {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "bootstrap peer is no longer the direct contextual host parent",
        ));
    }
    // Checking parenthood AFTER pinning closes the credential/PID-reuse race:
    // the original parent is still our parent, so it could not have died and
    // had its pid reassigned before pidfd_open. Reparented children fail closed.
    Ok(host)
}

fn host_terminal(host: RawFd) -> bool {
    let mut descriptor = libc::pollfd {
        fd: host,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: descriptor is valid for one pollfd and the nonblocking call.
        let result = unsafe { libc::poll(&raw mut descriptor, 1, 0) };
        if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return result != 0 && (result < 0 || descriptor.revents != 0);
    }
}

struct Subscription {
    cancellation: UnixStream,
    work: Option<BlockingWorkHandle<()>>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Wake a poll even if this watch is dropped during a healthy session.
        let _ = self.cancellation.shutdown(Shutdown::Both);
        if let Some(work) = self.work.take() {
            let _ = work.join();
        }
    }
}

impl HostSessionRouteObserver for BootstrapHostRoute {
    fn watch_terminal(
        &self,
        _host: ActorAddress,
        terminal: Arc<dyn Fn() + Send + Sync>,
    ) -> HostSessionRouteWatch {
        if host_terminal(self.host.as_raw_fd()) {
            terminal();
            return HostSessionRouteWatch::new(());
        }
        let (cancellation, cancelled) = match UnixStream::pair() {
            Ok(pair) => pair,
            Err(_) => {
                terminal();
                return HostSessionRouteWatch::new(());
            }
        };
        let host = Arc::clone(&self.host);
        let callback = Arc::clone(&terminal);
        let work = self.blocking.submit_joinable(move || {
            let mut descriptors = [
                libc::pollfd {
                    fd: host.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: cancelled.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            loop {
                // SAFETY: both descriptors remain owned for the entire blocking poll.
                let result = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
                if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if descriptors[1].revents != 0 {
                    return;
                }
                if result < 0 || descriptors[0].revents != 0 {
                    callback();
                    return;
                }
            }
        });
        match work {
            Ok(work) => HostSessionRouteWatch::new(Subscription {
                cancellation,
                work: Some(work),
            }),
            Err(_) => {
                terminal();
                HostSessionRouteWatch::new(())
            }
        }
    }
}
