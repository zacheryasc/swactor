use std::fmt;
use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

use crate::protocol::SessionCapability;

pub const CHILD_BOOTSTRAP_FD: RawFd = 198;
const CLAIM_MAGIC: [u8; 4] = *b"SWCL";
const RESPONSE_MAGIC: [u8; 4] = *b"SWBR";
const ATTACHMENT_MAGIC: [u8; 4] = *b"SWAT";
const ATTACHMENT_ACK_MAGIC: [u8; 4] = *b"SWAK";
const PROTOCOL_VERSION: u16 = 1;
const PREFIX_LEN: usize = 12;
const MAX_PACKET_LEN: usize = 64 * 1024;
const MAX_TRANSFERRED_FDS: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionBootstrap {
    pub host_session: ActorAddress,
    pub session_capability: SessionCapability,
    /// Binding-private routing material. The contextual composition defines its
    /// encoding; the bootstrap transport treats it as opaque bytes.
    pub routing: Vec<u8>,
}

#[derive(Debug)]
pub struct BootstrapHost {
    socket: Option<OwnedFd>,
    claimed: bool,
}
#[derive(Debug)]
pub struct BootstrapCancellation {
    socket: Option<OwnedFd>,
}

#[derive(Debug)]
pub struct BootstrapAttachment {
    socket: Option<OwnedFd>,
}

#[derive(Debug)]
pub struct GuestBootstrap {
    pub material: SessionBootstrap,
    pub arena_fd: OwnedFd,
    attachment: Option<OwnedFd>,
}
#[derive(Debug)]
pub struct GuestAttachment {
    socket: Option<OwnedFd>,
}

#[derive(Debug)]
pub struct PendingGuestBootstrap {
    socket: OwnedFd,
}
#[derive(Debug)]
pub struct PendingAttachmentAcknowledgement {
    socket: OwnedFd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentResult {
    Succeeded,
    Failed(String),
}

#[derive(Debug)]
pub enum BootstrapChannelError {
    Io(io::Error),
    MissingDescriptor,
    AlreadyClaimed,
    ChannelClosed,
    TruncatedPacket,
    UnexpectedDescriptorCount { expected: usize, found: usize },
    Malformed(&'static str),
    UnsupportedVersion { found: u16, supported: u16 },
    Rejected(String),
    Payload(String),
    TimedOut,
}

impl fmt::Display for BootstrapChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "bootstrap channel I/O: {error}"),
            Self::MissingDescriptor => {
                write!(
                    f,
                    "bootstrap descriptor {CHILD_BOOTSTRAP_FD} is unavailable"
                )
            }
            Self::AlreadyClaimed => f.write_str("bootstrap handle was already claimed"),
            Self::ChannelClosed => f.write_str("bootstrap channel closed"),
            Self::TruncatedPacket => f.write_str("bootstrap packet was truncated"),
            Self::UnexpectedDescriptorCount { expected, found } => write!(
                f,
                "bootstrap packet transferred {found} descriptors, expected {expected}"
            ),
            Self::Malformed(reason) => write!(f, "malformed bootstrap packet: {reason}"),
            Self::UnsupportedVersion { found, supported } => write!(
                f,
                "unsupported bootstrap protocol version {found} (supported: {supported})"
            ),
            Self::Rejected(reason) => write!(f, "bootstrap claim rejected: {reason}"),
            Self::Payload(reason) => write!(f, "invalid bootstrap payload: {reason}"),
            Self::TimedOut => f.write_str("bootstrap channel wait timed out"),
        }
    }
}

impl std::error::Error for BootstrapChannelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for BootstrapChannelError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub fn bootstrap_channel() -> Result<(BootstrapHost, OwnedFd), BootstrapChannelError> {
    let mut descriptors = [-1; 2];
    // SAFETY: storage has two slots and successful socketpair initializes both.
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            descriptors.as_mut_ptr(),
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: successful socketpair returned two fresh descriptors.
    let host = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    // SAFETY: same as above; this is the distinct peer descriptor.
    let child = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    Ok((
        BootstrapHost {
            socket: Some(host),
            claimed: false,
        },
        child,
    ))
}

impl BootstrapHost {
    pub fn cancellation_handle(&self) -> Result<BootstrapCancellation, BootstrapChannelError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or(BootstrapChannelError::AlreadyClaimed)?;
        // SAFETY: the source is a live socket and F_DUPFD_CLOEXEC returns a
        // fresh descriptor referring to the same socket endpoint.
        let duplicate = unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if duplicate < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: fcntl returned a fresh descriptor.
        Ok(BootstrapCancellation {
            socket: Some(unsafe { OwnedFd::from_raw_fd(duplicate) }),
        })
    }

    pub fn accept_claim(
        &mut self,
        material: &SessionBootstrap,
        arena_fd: RawFd,
    ) -> Result<BootstrapAttachment, BootstrapChannelError> {
        if self.claimed {
            return Err(BootstrapChannelError::AlreadyClaimed);
        }
        let socket = self
            .socket
            .take()
            .ok_or(BootstrapChannelError::AlreadyClaimed)?;
        self.claimed = true;

        let packet = recv_packet(socket.as_raw_fd())?;
        require_descriptor_count(&packet, 0)?;
        parse_claim(&packet.bytes)?;

        let payload = serde_json::to_vec(material)
            .map_err(|error| BootstrapChannelError::Payload(error.to_string()))?;
        let response = frame(RESPONSE_MAGIC, 0, &payload)?;
        send_packet(socket.as_raw_fd(), &response, Some(arena_fd))?;
        Ok(BootstrapAttachment {
            socket: Some(socket),
        })
    }

    pub fn reject_claim(&mut self, reason: &str) -> Result<(), BootstrapChannelError> {
        if self.claimed {
            return Err(BootstrapChannelError::AlreadyClaimed);
        }
        let socket = self
            .socket
            .take()
            .ok_or(BootstrapChannelError::AlreadyClaimed)?;
        self.claimed = true;

        let packet = recv_packet(socket.as_raw_fd())?;
        require_descriptor_count(&packet, 0)?;
        parse_claim(&packet.bytes)?;
        let response = frame(RESPONSE_MAGIC, 1, reason.as_bytes())?;
        send_packet(socket.as_raw_fd(), &response, None)
    }
}

impl BootstrapCancellation {
    pub fn cancel(&mut self) {
        if let Some(socket) = self.socket.take() {
            // SAFETY: socket is live; shutdown wakes blocking send/receive calls
            // on every descriptor referring to this endpoint.
            let _ = unsafe { libc::shutdown(socket.as_raw_fd(), libc::SHUT_RDWR) };
        }
    }
}

impl BootstrapAttachment {
    pub fn receive_result(&mut self) -> Result<AttachmentResult, BootstrapChannelError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or(BootstrapChannelError::ChannelClosed)?;
        let packet = recv_packet(socket.as_raw_fd())?;
        require_descriptor_count(&packet, 0)?;
        let (status, body) = parse_frame(&packet.bytes, ATTACHMENT_MAGIC)?;
        match status {
            0 if body.is_empty() => Ok(AttachmentResult::Succeeded),
            1 => String::from_utf8(body.to_vec())
                .map(AttachmentResult::Failed)
                .map_err(|error| BootstrapChannelError::Payload(error.to_string())),
            0 => Err(BootstrapChannelError::Malformed(
                "successful attachment result contains a body",
            )),
            _ => Err(BootstrapChannelError::Malformed(
                "unknown attachment result status",
            )),
        }
    }

    pub fn acknowledge(mut self) -> Result<(), BootstrapChannelError> {
        let socket = self
            .socket
            .take()
            .ok_or(BootstrapChannelError::ChannelClosed)?;
        let packet = frame(ATTACHMENT_ACK_MAGIC, 0, &[])?;
        send_packet(socket.as_raw_fd(), &packet, None)
    }
}

impl GuestBootstrap {
    /// Claim the fixed inherited descriptor. Ownership transfers to this call.
    pub fn claim() -> Result<Self, BootstrapChannelError> {
        // SAFETY: F_GETFD only inspects the fixed descriptor.
        if unsafe { libc::fcntl(CHILD_BOOTSTRAP_FD, libc::F_GETFD) } < 0 {
            return Err(BootstrapChannelError::MissingDescriptor);
        }
        // SAFETY: the private binding contract gives this call sole ownership of
        // the fixed inherited descriptor.
        let socket = unsafe { OwnedFd::from_raw_fd(CHILD_BOOTSTRAP_FD) };
        Self::claim_socket(socket)
    }

    pub fn claim_socket(socket: OwnedFd) -> Result<Self, BootstrapChannelError> {
        Self::begin_claim(socket)?.receive()
    }

    pub fn begin_claim(socket: OwnedFd) -> Result<PendingGuestBootstrap, BootstrapChannelError> {
        let claim = frame(CLAIM_MAGIC, 0, &[])?;
        send_packet(socket.as_raw_fd(), &claim, None)?;
        Ok(PendingGuestBootstrap { socket })
    }
    pub fn into_parts(
        mut self,
    ) -> Result<(SessionBootstrap, OwnedFd, GuestAttachment), BootstrapChannelError> {
        let socket = self
            .attachment
            .take()
            .ok_or(BootstrapChannelError::ChannelClosed)?;
        Ok((
            self.material,
            self.arena_fd,
            GuestAttachment {
                socket: Some(socket),
            },
        ))
    }

    pub fn attachment_succeeded(self) -> Result<(), BootstrapChannelError> {
        self.into_parts()?.2.attachment_succeeded()
    }

    pub fn attachment_failed(self, reason: &str) -> Result<(), BootstrapChannelError> {
        self.into_parts()?.2.attachment_failed(reason)
    }
}

impl GuestAttachment {
    pub fn attachment_succeeded(self) -> Result<(), BootstrapChannelError> {
        self.begin_attachment_succeeded().map(|_| ())
    }

    pub fn attachment_failed(self, reason: &str) -> Result<(), BootstrapChannelError> {
        self.begin_attachment_failed(reason).map(|_| ())
    }

    pub fn begin_attachment_succeeded(
        self,
    ) -> Result<PendingAttachmentAcknowledgement, BootstrapChannelError> {
        self.begin_attachment_result(0, &[])
    }

    pub fn begin_attachment_failed(
        self,
        reason: &str,
    ) -> Result<PendingAttachmentAcknowledgement, BootstrapChannelError> {
        self.begin_attachment_result(1, reason.as_bytes())
    }

    fn begin_attachment_result(
        mut self,
        status: u8,
        body: &[u8],
    ) -> Result<PendingAttachmentAcknowledgement, BootstrapChannelError> {
        let socket = self
            .socket
            .take()
            .ok_or(BootstrapChannelError::ChannelClosed)?;
        let packet = frame(ATTACHMENT_MAGIC, status, body)?;
        send_packet(socket.as_raw_fd(), &packet, None)?;
        Ok(PendingAttachmentAcknowledgement { socket })
    }
}
impl PendingGuestBootstrap {
    pub fn receive(self) -> Result<GuestBootstrap, BootstrapChannelError> {
        let socket = self.socket;
        let mut packet = recv_packet(socket.as_raw_fd())?;
        let (status, body) = parse_frame(&packet.bytes, RESPONSE_MAGIC)?;
        if status == 1 {
            require_descriptor_count(&packet, 0)?;
            let reason = String::from_utf8(body.to_vec())
                .map_err(|error| BootstrapChannelError::Payload(error.to_string()))?;
            return Err(BootstrapChannelError::Rejected(reason));
        }
        if status != 0 {
            return Err(BootstrapChannelError::Malformed(
                "unknown bootstrap response status",
            ));
        }
        require_descriptor_count(&packet, 1)?;
        let arena_fd = packet.descriptors.pop().expect("one descriptor checked");
        let material = serde_json::from_slice(body)
            .map_err(|error| BootstrapChannelError::Payload(error.to_string()))?;
        Ok(GuestBootstrap {
            material,
            arena_fd,
            attachment: Some(socket),
        })
    }
}

impl PendingAttachmentAcknowledgement {
    pub fn wait(self) -> Result<(), BootstrapChannelError> {
        let packet = recv_packet(self.socket.as_raw_fd())?;
        require_descriptor_count(&packet, 0)?;
        let (status, body) = parse_frame(&packet.bytes, ATTACHMENT_ACK_MAGIC)?;
        if status != 0 || !body.is_empty() {
            return Err(BootstrapChannelError::Malformed(
                "invalid attachment acknowledgement",
            ));
        }
        Ok(())
    }

    /// Wait for the host acknowledgement with a deadline.
    ///
    /// A host that never acknowledges (crashed between attach and ACK) must
    /// not hang the guest entrypoint: readiness is polled with `poll(2)` and
    /// the wait fails with [`BootstrapChannelError::TimedOut`] instead.
    pub fn wait_deadline(self, timeout: std::time::Duration) -> Result<(), BootstrapChannelError> {
        let millis = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut pollfd = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: pollfd is a valid single-fd poll request.
        let ready = unsafe { libc::poll(&mut pollfd, 1, millis) };
        if ready == 0 {
            return Err(BootstrapChannelError::TimedOut);
        }
        if ready < 0 {
            return Err(io::Error::last_os_error().into());
        }
        self.wait()
    }
}

fn parse_claim(packet: &[u8]) -> Result<(), BootstrapChannelError> {
    let (status, body) = parse_frame(packet, CLAIM_MAGIC)?;
    if status != 0 || !body.is_empty() {
        return Err(BootstrapChannelError::Malformed("invalid claim body"));
    }
    Ok(())
}

fn frame(magic: [u8; 4], status: u8, body: &[u8]) -> Result<Vec<u8>, BootstrapChannelError> {
    let body_len = u32::try_from(body.len())
        .map_err(|_| BootstrapChannelError::Malformed("packet body is too large"))?;
    if PREFIX_LEN + body.len() > MAX_PACKET_LEN {
        return Err(BootstrapChannelError::Malformed("packet body is too large"));
    }
    let mut packet = Vec::with_capacity(PREFIX_LEN + body.len());
    packet.extend_from_slice(&magic);
    packet.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    packet.push(status);
    packet.push(0);
    packet.extend_from_slice(&body_len.to_le_bytes());
    packet.extend_from_slice(body);
    Ok(packet)
}

fn parse_frame(
    packet: &[u8],
    expected_magic: [u8; 4],
) -> Result<(u8, &[u8]), BootstrapChannelError> {
    if packet.len() < PREFIX_LEN {
        return Err(BootstrapChannelError::Malformed("short frame prefix"));
    }
    if packet[..4] != expected_magic {
        return Err(BootstrapChannelError::Malformed("unexpected frame magic"));
    }
    let version = u16::from_le_bytes([packet[4], packet[5]]);
    if version != PROTOCOL_VERSION {
        return Err(BootstrapChannelError::UnsupportedVersion {
            found: version,
            supported: PROTOCOL_VERSION,
        });
    }
    if packet[7] != 0 {
        return Err(BootstrapChannelError::Malformed(
            "reserved frame byte is nonzero",
        ));
    }
    let body_len = u32::from_le_bytes(packet[8..12].try_into().expect("length prefix")) as usize;
    if packet.len() != PREFIX_LEN + body_len {
        return Err(BootstrapChannelError::Malformed(
            "declared frame length does not match packet",
        ));
    }
    Ok((packet[6], &packet[PREFIX_LEN..]))
}

struct ReceivedPacket {
    bytes: Vec<u8>,
    descriptors: Vec<OwnedFd>,
}

fn require_descriptor_count(
    packet: &ReceivedPacket,
    expected: usize,
) -> Result<(), BootstrapChannelError> {
    if packet.descriptors.len() != expected {
        return Err(BootstrapChannelError::UnexpectedDescriptorCount {
            expected,
            found: packet.descriptors.len(),
        });
    }
    Ok(())
}

fn send_packet(
    socket: RawFd,
    packet: &[u8],
    descriptor: Option<RawFd>,
) -> Result<(), BootstrapChannelError> {
    let mut iov = libc::iovec {
        iov_base: packet.as_ptr().cast_mut().cast(),
        iov_len: packet.len(),
    };
    // SAFETY: zero is a valid initial state for msghdr before fields are set.
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    let mut control = descriptor.map(|_| {
        // SAFETY: CMSG_SPACE computes storage for one RawFd ancillary payload.
        vec![0_u8; unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize }]
    });
    if let (Some(descriptor), Some(control)) = (descriptor, control.as_mut()) {
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len();
        // SAFETY: control has CMSG_SPACE bytes and message points at it.
        let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        if header.is_null() {
            return Err(BootstrapChannelError::Malformed(
                "failed to construct descriptor control message",
            ));
        }
        // SAFETY: header points inside control with room for cmsghdr and RawFd.
        unsafe {
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize;
            std::ptr::write(libc::CMSG_DATA(header).cast::<RawFd>(), descriptor);
        }
    }

    // SAFETY: message references packet/control storage for the duration of the call.
    let sent = unsafe { libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL) };
    if sent < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if sent as usize != packet.len() {
        return Err(BootstrapChannelError::TruncatedPacket);
    }
    Ok(())
}

fn recv_packet(socket: RawFd) -> Result<ReceivedPacket, BootstrapChannelError> {
    let mut bytes = vec![0_u8; MAX_PACKET_LEN];
    // Space for several descriptors lets us reject duplicates explicitly rather
    // than accepting one while silently truncating the ancillary data.
    // SAFETY: CMSG_SPACE computes the required ancillary buffer size.
    let control_len =
        unsafe { libc::CMSG_SPACE((MAX_TRANSFERRED_FDS * size_of::<RawFd>()) as u32) as usize };
    let mut control = vec![0_u8; control_len];
    let mut iov = libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    };
    // SAFETY: zero is a valid initial state for msghdr before fields are set.
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len();

    // SAFETY: message references writable byte and control buffers.
    let received = unsafe { libc::recvmsg(socket, &mut message, libc::MSG_CMSG_CLOEXEC) };
    if received < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if received == 0 {
        return Err(BootstrapChannelError::ChannelClosed);
    }
    if message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(BootstrapChannelError::TruncatedPacket);
    }
    bytes.truncate(received as usize);

    let mut descriptors = Vec::new();
    // SAFETY: recvmsg initialized the ancillary region described by message.
    let mut header = unsafe { libc::CMSG_FIRSTHDR(&message) };
    while !header.is_null() {
        // SAFETY: header is yielded by CMSG_FIRSTHDR/CMSG_NXTHDR for message.
        let current = unsafe { &*header };
        if current.cmsg_level != libc::SOL_SOCKET || current.cmsg_type != libc::SCM_RIGHTS {
            return Err(BootstrapChannelError::Malformed(
                "unexpected ancillary message",
            ));
        }
        // SAFETY: zero payload length is valid for computing cmsghdr size.
        let header_len = unsafe { libc::CMSG_LEN(0) as usize };
        if current.cmsg_len < header_len {
            return Err(BootstrapChannelError::Malformed(
                "short descriptor ancillary message",
            ));
        }
        let payload_len = current.cmsg_len - header_len;
        if payload_len % size_of::<RawFd>() != 0 {
            return Err(BootstrapChannelError::Malformed(
                "misaligned descriptor ancillary payload",
            ));
        }
        let count = payload_len / size_of::<RawFd>();
        // SAFETY: SCM_RIGHTS payload contains `count` RawFd values.
        let data = unsafe { libc::CMSG_DATA(header).cast::<RawFd>() };
        for index in 0..count {
            // SAFETY: index is within the validated ancillary payload.
            let descriptor = unsafe { std::ptr::read(data.add(index)) };
            // SAFETY: each SCM_RIGHTS descriptor is fresh and uniquely owned by
            // this receiving process.
            descriptors.push(unsafe { OwnedFd::from_raw_fd(descriptor) });
        }
        // SAFETY: message remains live and header came from this message.
        header = unsafe { libc::CMSG_NXTHDR(&message, header) };
    }

    Ok(ReceivedPacket { bytes, descriptors })
}
