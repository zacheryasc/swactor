//! The transport envelope: how a delivered frame is encoded as bytes for
//! a real carrier (spec §7, testing spec §9).
//!
//! This is the **only** place the transport touches a frame as bytes. The
//! envelope is self-describing and length-prefixed so it can be framed on
//! any byte transport (a datagram, a length-delimited stream). It MUST
//! round-trip exactly — the conformance check (testing spec §9) leans on
//! "payloads byte-identical, positions intact" — and it MUST fail
//! gracefully on a truncated or malformed buffer rather than panic, since
//! a best-effort carrier can hand us anything.
//!
//! Layout (all integers little-endian):
//!
//! ```text
//!   node_len: u32 | node: utf8[node_len]
//!   life:     u64
//!   position: u64
//!   chan_len: u32 | channel: utf8[chan_len]
//!   pay_len:  u32 | payload: bytes[pay_len]
//! ```

use super::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};

/// Why a buffer could not be decoded as an envelope. A carrier that
/// receives one of these drops the datagram (best-effort) rather than
/// crashing the consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// The buffer ended before a declared field was complete.
    Truncated,
    /// A length prefix claimed more bytes than the buffer holds.
    BadLength,
    /// A string field was not valid UTF-8.
    NotUtf8,
    /// Bytes remained after a complete envelope. One datagram carries
    /// exactly one frame, so a trailing tail is a malformed buffer.
    TrailingBytes,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Truncated => f.write_str("envelope truncated"),
            WireError::BadLength => f.write_str("envelope length prefix exceeds buffer"),
            WireError::NotUtf8 => f.write_str("envelope string field is not valid UTF-8"),
            WireError::TrailingBytes => f.write_str("bytes remain after a complete envelope"),
        }
    }
}

impl std::error::Error for WireError {}

/// Encode a delivery — a `(StreamId, Frame)` — into a self-describing byte
/// buffer.
pub fn encode_delivery(stream: &StreamId, frame: &Frame) -> Vec<u8> {
    let node = stream.node.as_str().as_bytes();
    let channel = frame.channel.as_str().as_bytes();
    let mut out = Vec::with_capacity(4 + node.len() + 8 + 8 + 4 + channel.len() + 4 + frame.payload.len());
    put_bytes(&mut out, node);
    out.extend_from_slice(&stream.life.0.to_le_bytes());
    out.extend_from_slice(&frame.position.0.to_le_bytes());
    put_bytes(&mut out, channel);
    put_bytes(&mut out, &frame.payload);
    out
}

/// Decode a delivery previously produced by [`encode_delivery`]. Returns a
/// [`WireError`] on any malformed buffer instead of panicking.
pub fn decode_delivery(buf: &[u8]) -> Result<(StreamId, Frame), WireError> {
    let mut cur = Cursor { buf, pos: 0 };
    let node = cur.take_str()?;
    let life = Lifetime(cur.take_u64()?);
    let position = Position(cur.take_u64()?);
    let channel = cur.take_str()?;
    let payload = cur.take_bytes()?.to_vec();
    if cur.pos != buf.len() {
        return Err(WireError::TrailingBytes);
    }
    let stream = StreamId::new(NodeId::new(node), life);
    let frame = Frame::new(ChannelId::new(channel), position, payload);
    Ok((stream, frame))
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self.pos.checked_add(n).ok_or(WireError::BadLength)?;
        let slice = self.buf.get(self.pos..end).ok_or(WireError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn take_u32(&mut self) -> Result<u32, WireError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn take_u64(&mut self) -> Result<u64, WireError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    fn take_bytes(&mut self) -> Result<&'a [u8], WireError> {
        let len = self.take_u32()? as usize;
        self.take(len)
    }

    fn take_str(&mut self) -> Result<&'a str, WireError> {
        let bytes = self.take_bytes()?;
        std::str::from_utf8(bytes).map_err(|_| WireError::NotUtf8)
    }
}
