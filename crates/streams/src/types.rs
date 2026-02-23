use std::fmt;

use serde::{Deserialize, Serialize};

/// Unique identifier for a stream, generated randomly.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId(pub [u8; 16]);

impl StreamId {
    pub fn new_random() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).expect("getrandom failed");
        StreamId(bytes)
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamId(")?;
        for b in &self.0[..4] {
            write!(f, "{b:02x}")?;
        }
        write!(f, "\u{2026})")
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{b:02x}")?;
        }
        write!(f, "\u{2026}")
    }
}

/// Mode of a stream -- what kind of data flows through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamMode {
    BlobTransfer,
}

/// Configuration for a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Number of parallel QUIC stripes for data transfer.
    pub stripe_count: u8,
    /// Maximum frame payload size in bytes.
    pub frame_size: u32,
    /// Opaque metadata attached to the stream negotiation.
    pub metadata: Vec<u8>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        StreamConfig {
            stripe_count: 4,
            frame_size: 256 * 1024, // 256 KB
            metadata: Vec::new(),
        }
    }
}

/// Errors that can occur during stream operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    Closed,
    BrokenPipe(String),
    Disconnected,
    BufferExhausted,
    InvalidHeader(String),
    ChunkVerificationFailed {
        chunk_index: usize,
        expected: [u8; 32],
        actual: [u8; 32],
    },
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::Closed => write!(f, "stream closed"),
            StreamError::BrokenPipe(msg) => write!(f, "broken pipe: {msg}"),
            StreamError::Disconnected => write!(f, "disconnected"),
            StreamError::BufferExhausted => write!(f, "buffer pool exhausted"),
            StreamError::InvalidHeader(msg) => write!(f, "invalid header: {msg}"),
            StreamError::ChunkVerificationFailed {
                chunk_index,
                expected,
                actual,
            } => {
                write!(f, "chunk {chunk_index} verification failed: expected ")?;
                for b in &expected[..4] {
                    write!(f, "{b:02x}")?;
                }
                write!(f, "\u{2026}, got ")?;
                for b in &actual[..4] {
                    write!(f, "{b:02x}")?;
                }
                write!(f, "\u{2026}")
            }
        }
    }
}

impl std::error::Error for StreamError {}

/// Token that allows resuming an interrupted stream transfer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeToken {
    pub stream_id: StreamId,
    pub mode: StreamMode,
    pub chunks_completed: u64,
    pub bytes_transferred: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_random_is_unique() {
        let a = StreamId::new_random();
        let b = StreamId::new_random();
        assert_ne!(a, b);
    }

    #[test]
    fn stream_id_debug_shows_prefix() {
        let id = StreamId([0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0, 0, 0, 0, 0, 0, 0, 0]);
        let dbg = format!("{id:?}");
        assert_eq!(dbg, "StreamId(abcdef01\u{2026})");
    }

    #[test]
    fn stream_id_display_shows_8_bytes() {
        let id = StreamId([0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xaa, 0xbb, 0, 0, 0, 0, 0, 0]);
        let disp = format!("{id}");
        assert_eq!(disp, "abcdef0123456789\u{2026}");
    }

    #[test]
    fn stream_config_defaults() {
        let cfg = StreamConfig::default();
        assert_eq!(cfg.stripe_count, 4);
        assert_eq!(cfg.frame_size, 256 * 1024);
        assert!(cfg.metadata.is_empty());
    }
}
