//! TCP transport with connection pooling and length-prefix framing.
//!
//! Wire format per envelope:
//!   [4 bytes: total frame len (BE u32)]
//!   [32 bytes: dest address]
//!   [4 bytes: type_tag len (BE u32)]
//!   [N bytes: type_tag UTF-8]
//!   [4 bytes: hints len (BE u32)]
//!   [M bytes: hints (JSON, may be empty)]
//!   [remaining: payload bytes]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Mutex;

use swactor::actor::ActorAddress;
use swactor::transport::{Transport, WireEnvelope};
use swactor::Error;

// ─── TcpTransport ──────────────────────────────────────────────────────────

/// TCP transport with connection pooling.
///
/// Maintains a pool of connections keyed by `SocketAddr`. Connections are
/// created on first use and reused for subsequent sends.
pub struct TcpTransport {
    pool: Mutex<HashMap<SocketAddr, TcpStream>>,
    /// Default destination for sends that don't specify an address.
    /// Used when the transport is registered per-address in a TransportRouter.
    default_dest: Option<SocketAddr>,
}

impl TcpTransport {
    /// Create a transport that sends to a specific destination.
    pub fn new(dest: SocketAddr) -> Self {
        Self {
            pool: Mutex::new(HashMap::new()),
            default_dest: Some(dest),
        }
    }

    /// Create a transport with no default destination.
    /// The destination must be determined by the caller (e.g. via TransportRouter).
    pub fn pool() -> Self {
        Self {
            pool: Mutex::new(HashMap::new()),
            default_dest: None,
        }
    }

    pub fn get_or_connect(&self, addr: SocketAddr) -> Result<TcpStream, Error> {
        let mut pool = self.pool.lock().unwrap();
        if let Some(stream) = pool.get(&addr) {
            match stream.try_clone() {
                Ok(s) => return Ok(s),
                Err(_) => {
                    pool.remove(&addr);
                }
            }
        }
        let stream =
            TcpStream::connect(addr).map_err(|e| Error::from(format!("TCP connect to {addr}: {e}")))?;
        stream
            .set_nodelay(true)
            .map_err(|e| Error::from(format!("set_nodelay: {e}")))?;
        pool.insert(addr, stream.try_clone().unwrap());
        Ok(stream)
    }

    /// Evict a pooled connection for an address.
    pub fn evict(&self, addr: SocketAddr) {
        self.pool.lock().unwrap().remove(&addr);
    }

    /// Send an envelope to a specific address.
    ///
    /// If the write fails (e.g. stale connection from a dead peer), evicts
    /// the pooled connection and retries once with a fresh one.
    pub fn send_to(&self, addr: SocketAddr, envelope: WireEnvelope) -> Result<(), Error> {
        let buf = encode_wire_envelope(&envelope);
        let mut stream = self.get_or_connect(addr)?;
        match stream.write_all(&buf) {
            Ok(()) => Ok(()),
            Err(_) => {
                // Evict stale connection and retry once
                self.pool.lock().unwrap().remove(&addr);
                let mut stream = self.get_or_connect(addr)?;
                stream
                    .write_all(&buf)
                    .map_err(|e| Error::from(format!("TCP send to {addr}: {e}")))
            }
        }
    }
}

impl Transport for TcpTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        let dest = self
            .default_dest
            .ok_or_else(|| Error::from("TcpTransport: no default destination"))?;
        self.send_to(dest, envelope)
    }
}

// ─── TcpListener wrapper ───────────────────────────────────────────────────

/// Accept loop that reads wire envelopes from incoming TCP connections.
pub struct TcpAcceptor {
    listener: TcpListener,
}

impl TcpAcceptor {
    /// Bind to a local address.
    pub fn bind(addr: SocketAddr) -> Result<Self, Error> {
        let listener =
            TcpListener::bind(addr).map_err(|e| Error::from(format!("TCP bind {addr}: {e}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| Error::from(format!("set_nonblocking: {e}")))?;
        Ok(Self { listener })
    }

    /// The local address this acceptor is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Non-blocking: accept new connections, read complete envelopes from them.
    /// Returns all envelopes that could be read without blocking.
    /// Each entry contains (envelope, peer address, raw address hint bytes).
    pub fn try_recv(&self, streams: &mut Vec<TcpStream>) -> Vec<(WireEnvelope, SocketAddr, Vec<u8>)> {
        // Accept new connections
        loop {
            match self.listener.accept() {
                Ok((stream, _peer)) => {
                    let _ = stream.set_nonblocking(true);
                    streams.push(stream);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // Read from all streams
        let mut envelopes = Vec::new();
        let mut dead = Vec::new();

        for (i, stream) in streams.iter_mut().enumerate() {
            let peer = stream.peer_addr().unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());
            loop {
                match read_wire_envelope(stream) {
                    Ok((env, hints)) => envelopes.push((env, peer, hints)),
                    Err(ReadError::WouldBlock) => break,
                    Err(ReadError::Disconnected) => {
                        dead.push(i);
                        break;
                    }
                    Err(ReadError::Other(_)) => {
                        dead.push(i);
                        break;
                    }
                }
            }
        }

        // Remove dead connections in reverse order
        dead.sort_unstable();
        dead.dedup();
        for i in dead.into_iter().rev() {
            streams.swap_remove(i);
        }

        envelopes
    }
}

// ─── Wire format encoding/decoding ─────────────────────────────────────────

/// Encode a WireEnvelope to bytes in the length-prefixed wire format.
pub fn encode_wire_envelope(envelope: &WireEnvelope) -> Vec<u8> {
    let tag_bytes = envelope.type_tag.as_bytes();
    let frame_len: u32 = (32 + 4 + tag_bytes.len() + 4 + envelope.payload.len()) as u32;

    let mut buf = Vec::with_capacity(4 + frame_len as usize);
    buf.extend_from_slice(&frame_len.to_be_bytes());
    buf.extend_from_slice(&envelope.dest.0);
    buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(tag_bytes);
    buf.extend_from_slice(&0u32.to_be_bytes()); // hints_len = 0
    buf.extend_from_slice(&envelope.payload);
    buf
}

enum ReadError {
    WouldBlock,
    Disconnected,
    Other(std::io::Error),
}

impl From<std::io::Error> for ReadError {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::WouldBlock => ReadError::WouldBlock,
            std::io::ErrorKind::UnexpectedEof => ReadError::Disconnected,
            std::io::ErrorKind::ConnectionReset => ReadError::Disconnected,
            _ => ReadError::Other(e),
        }
    }
}

/// Read one WireEnvelope and address hints from a TCP stream.
fn read_wire_envelope(stream: &mut TcpStream) -> Result<(WireEnvelope, Vec<u8>), ReadError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;

    let mut frame = vec![0u8; frame_len];
    stream.read_exact(&mut frame)?;

    let mut dest = [0u8; 32];
    dest.copy_from_slice(&frame[0..32]);

    let tag_len = u32::from_be_bytes(frame[32..36].try_into().unwrap()) as usize;
    let type_tag = String::from_utf8_lossy(&frame[36..36 + tag_len]).to_string();

    let after_tag = 36 + tag_len;
    let hints_len = u32::from_be_bytes(frame[after_tag..after_tag + 4].try_into().unwrap()) as usize;
    let hints_bytes = frame[after_tag + 4..after_tag + 4 + hints_len].to_vec();
    let payload = frame[after_tag + 4 + hints_len..].to_vec();

    Ok((
        WireEnvelope {
            dest: ActorAddress(dest),
            type_tag,
            payload,
        },
        hints_bytes,
    ))
}

/// Read a single envelope and hints from a blocking stream. Public for use in tests/examples.
pub fn read_envelope_blocking(stream: &mut TcpStream) -> Result<(WireEnvelope, Vec<u8>), Error> {
    // Temporarily set blocking mode
    stream
        .set_nonblocking(false)
        .map_err(|e| Error::from(format!("set_blocking: {e}")))?;
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| Error::from(format!("read frame len: {e}")))?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;

    let mut frame = vec![0u8; frame_len];
    stream
        .read_exact(&mut frame)
        .map_err(|e| Error::from(format!("read frame: {e}")))?;

    let mut dest = [0u8; 32];
    dest.copy_from_slice(&frame[0..32]);

    let tag_len = u32::from_be_bytes(frame[32..36].try_into().unwrap()) as usize;
    let type_tag = String::from_utf8_lossy(&frame[36..36 + tag_len]).to_string();

    let after_tag = 36 + tag_len;
    let hints_len = u32::from_be_bytes(frame[after_tag..after_tag + 4].try_into().unwrap()) as usize;
    let hints_bytes = frame[after_tag + 4..after_tag + 4 + hints_len].to_vec();
    let payload = frame[after_tag + 4 + hints_len..].to_vec();

    let _ = stream.set_nonblocking(true);

    Ok((
        WireEnvelope {
            dest: ActorAddress(dest),
            type_tag,
            payload,
        },
        hints_bytes,
    ))
}
