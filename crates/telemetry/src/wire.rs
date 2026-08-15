//! Wire helpers for telemetry actor messages and compact event records.

use serde::{Deserialize, Serialize};
use swactor::Error;
use swactor_transport::{Codec, CodecRegistry, NetworkMessage};

use crate::frame::{ChannelId, TelemetryEvent, Frame, Lifetime, NodeId, Position, StreamId};

/// Why a buffer could not be decoded as an envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    Truncated,
    BadLength,
    NotUtf8,
    TrailingBytes,
    UnknownTag,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Truncated => f.write_str("envelope truncated"),
            WireError::BadLength => f.write_str("envelope length prefix exceeds buffer"),
            WireError::NotUtf8 => f.write_str("envelope string field is not valid UTF-8"),
            WireError::TrailingBytes => f.write_str("bytes remain after a complete envelope"),
            WireError::UnknownTag => f.write_str("unknown telemetry event record tag"),
        }
    }
}

impl std::error::Error for WireError {}

/// Legacy delivery envelope retained for transitional tests. Layout:
/// node_len/node, life, position, channel_id, payload_len/payload.
pub fn encode_delivery(stream: &StreamId, frame: &Frame) -> Vec<u8> {
    let node = stream.node.as_str().as_bytes();
    let mut out = Vec::with_capacity(4 + node.len() + 8 + 8 + 4 + 4 + frame.payload.len());
    put_bytes(&mut out, node);
    out.extend_from_slice(&stream.life.0.to_le_bytes());
    out.extend_from_slice(&frame.position.0.to_le_bytes());
    out.extend_from_slice(&frame.channel.0.to_le_bytes());
    put_bytes(&mut out, &frame.payload);
    out
}

/// Decode a delivery previously produced by [`encode_delivery`].
pub fn decode_delivery(buf: &[u8]) -> Result<(StreamId, Frame), WireError> {
    let mut cur = Cursor { buf, pos: 0 };
    let node = cur.take_str()?;
    let life = Lifetime(cur.take_u64()?);
    let position = Position(cur.take_u64()?);
    let channel = ChannelId(cur.take_u32()?);
    let payload = cur.take_bytes()?.to_vec();
    if cur.pos != buf.len() {
        return Err(WireError::TrailingBytes);
    }
    let stream = StreamId::new(NodeId::new(node), life);
    let frame = Frame::new(channel, position, payload);
    Ok((stream, frame))
}

/// Legacy actor-message payload wrapper for telemetry bytes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TelemetryFrame {
    pub payload: Vec<u8>,
}

impl NetworkMessage for TelemetryFrame {
    fn type_tag() -> &'static str {
        "swactor::TelemetryFrame"
    }
}

/// Identity codec for [`TelemetryFrame`].
pub struct TelemetryFrameCodec;

impl Codec<TelemetryFrame> for TelemetryFrameCodec {
    fn encode(&self, msg: &TelemetryFrame) -> Result<Vec<u8>, Error> {
        Ok(msg.payload.clone())
    }
    fn decode(&self, bytes: &[u8]) -> Result<TelemetryFrame, Error> {
        Ok(TelemetryFrame {
            payload: bytes.to_vec(),
        })
    }
}

/// Register the [`TelemetryFrame`] codec.
pub fn register_telemetry_codec(cr: &mut CodecRegistry) {
    cr.register::<TelemetryFrame, _>(TelemetryFrameCodec);
}

/// JSON event envelope for actor/control paths that can tolerate metadata size.
pub fn encode_telemetry_event(event: &TelemetryEvent) -> Vec<u8> {
    serde_json::to_vec(event).expect("telemetry event serializes")
}

pub fn decode_telemetry_event(bytes: &[u8]) -> Result<TelemetryEvent, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

pub(crate) struct Cursor<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self.pos.checked_add(n).ok_or(WireError::BadLength)?;
        let slice = self.buf.get(self.pos..end).ok_or(WireError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    pub(crate) fn take_u32(&mut self) -> Result<u32, WireError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn take_u64(&mut self) -> Result<u64, WireError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub(crate) fn take_bytes(&mut self) -> Result<&'a [u8], WireError> {
        let len = self.take_u32()? as usize;
        self.take(len)
    }

    pub(crate) fn take_str(&mut self) -> Result<&'a str, WireError> {
        let bytes = self.take_bytes()?;
        std::str::from_utf8(bytes).map_err(|_| WireError::NotUtf8)
    }
}
