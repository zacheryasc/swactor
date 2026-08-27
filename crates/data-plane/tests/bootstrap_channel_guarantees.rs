#![cfg(target_os = "linux")]

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use data_plane::bootstrap::channel::{
    AttachmentResult, BootstrapChannelError, GuestBootstrap, SessionBootstrap, bootstrap_channel,
};
use data_plane::protocol::SessionCapability;
use swactor::actor::ActorAddress;

fn arena_fd() -> OwnedFd {
    let name = CString::new("swactor-bootstrap-test").expect("memfd name");
    // SAFETY: name is NUL-terminated and memfd_create returns a fresh descriptor.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    assert!(fd >= 0, "memfd_create: {}", io::Error::last_os_error());
    // SAFETY: fd is fresh and uniquely owned.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let bytes = b"arena";
    // SAFETY: fd is writable and bytes is a valid input buffer.
    assert_eq!(
        unsafe { libc::pwrite(fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len(), 0) },
        bytes.len() as isize
    );
    fd
}

fn material() -> SessionBootstrap {
    SessionBootstrap {
        host_session: ActorAddress([0x11; 32]),
        session_capability: SessionCapability::new([0x22; 32]),
        routing: br#"{"endpoint":"host"}"#.to_vec(),
    }
}

#[test]
fn one_use_claim_transfers_arena_and_reports_attachment() {
    let (mut host, child) = bootstrap_channel().expect("bootstrap channel");
    let arena = arena_fd();
    let expected = material();
    let pending = GuestBootstrap::begin_claim(child).expect("begin guest claim");
    let mut attachment = host
        .accept_claim(&expected, arena.as_raw_fd())
        .expect("accept claim");
    let guest = pending.receive().expect("guest claim");
    assert_eq!(guest.material, expected);
    // SAFETY: F_GETFD only reads flags for the received live descriptor.
    let flags = unsafe { libc::fcntl(guest.arena_fd.as_raw_fd(), libc::F_GETFD) };
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
    let mut bytes = [0_u8; 5];
    // SAFETY: descriptor is readable and bytes is writable.
    assert_eq!(
        unsafe {
            libc::pread(
                guest.arena_fd.as_raw_fd(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                0,
            )
        },
        bytes.len() as isize
    );
    assert_eq!(&bytes, b"arena");
    guest.attachment_succeeded().expect("notify attachment");
    assert_eq!(
        attachment.receive_result().expect("attachment result"),
        AttachmentResult::Succeeded
    );
    let duplicate = host.accept_claim(&expected, arena.as_raw_fd());
    assert!(matches!(
        duplicate,
        Err(BootstrapChannelError::AlreadyClaimed)
    ));
}

#[test]
fn rejected_claim_is_reported_without_descriptor() {
    let (mut host, child) = bootstrap_channel().expect("bootstrap channel");
    let pending = GuestBootstrap::begin_claim(child).expect("begin guest claim");
    host.reject_claim("session revoked")
        .expect("host rejection");
    let result = pending.receive();
    assert!(matches!(
        result,
        Err(BootstrapChannelError::Rejected(reason)) if reason == "session revoked"
    ));
}

#[test]
fn incompatible_and_truncated_claims_are_rejected() {
    for packet in [
        vec![b'S', b'W', b'C', b'L', 99, 0, 0, 0, 0, 0, 0, 0],
        vec![b'S', b'W', b'C'],
    ] {
        let (mut host, child) = bootstrap_channel().expect("bootstrap channel");
        // SAFETY: child is connected and packet is a valid readable buffer.
        assert_eq!(
            unsafe {
                libc::send(
                    child.as_raw_fd(),
                    packet.as_ptr().cast(),
                    packet.len(),
                    libc::MSG_NOSIGNAL,
                )
            },
            packet.len() as isize
        );
        let result = host.accept_claim(&material(), arena_fd().as_raw_fd());
        if packet.len() == 12 {
            assert!(matches!(
                result,
                Err(BootstrapChannelError::UnsupportedVersion {
                    found: 99,
                    supported: 1
                })
            ));
        } else {
            assert!(matches!(
                result,
                Err(BootstrapChannelError::Malformed("short frame prefix"))
            ));
        }
    }
}

#[test]
fn peer_closure_resolves_blocked_claim_as_closed() {
    let (mut host, child) = bootstrap_channel().expect("bootstrap channel");
    drop(child);
    assert!(matches!(
        host.accept_claim(&material(), arena_fd().as_raw_fd()),
        Err(BootstrapChannelError::ChannelClosed)
    ));
}

#[test]
fn guest_failure_reason_reaches_host() {
    let (mut host, child) = bootstrap_channel().expect("bootstrap channel");
    let arena = arena_fd();
    let pending = GuestBootstrap::begin_claim(child).expect("begin guest claim");
    let mut attachment = host
        .accept_claim(&material(), arena.as_raw_fd())
        .expect("accept claim");
    pending
        .receive()
        .expect("guest claim")
        .attachment_failed("route rejected")
        .expect("notify failure");
    assert_eq!(
        attachment.receive_result().expect("attachment result"),
        AttachmentResult::Failed("route rejected".to_owned())
    );
}
