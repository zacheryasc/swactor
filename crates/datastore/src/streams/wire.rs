use crate::streams::types::{StreamConfig, StreamError, StreamId, StreamMode};

/// Magic bytes identifying the swactor stream protocol.
pub const MAGIC: [u8; 2] = [0x53, 0x57];

/// Wire protocol version.
pub const VERSION: u8 = 0x01;

/// ALPN protocol identifier for QUIC negotiation.
pub const ALPN: &[u8] = b"swactor/stream/1";

/// Header sent at the beginning of a stream connection.
///
/// Wire layout:
/// ```text
/// [2B magic] [1B version] [16B stream_id] [1B mode] [1B stripe_count]
/// [4B frame_size] [4B metadata_len] [metadata_len B metadata]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamHeader {
    pub stream_id: StreamId,
    pub mode: StreamMode,
    pub config: StreamConfig,
}

/// Fixed portion of the header (before variable-length metadata).
const HEADER_FIXED_SIZE: usize = 2 + 1 + 16 + 1 + 1 + 4 + 4; // 29 bytes

/// Encode a stream header into bytes.
pub fn encode_header(header: &StreamHeader) -> Vec<u8> {
    let meta_len = header.config.metadata.len() as u32;
    let total = HEADER_FIXED_SIZE + header.config.metadata.len();
    let mut buf = Vec::with_capacity(total);

    // Magic + version
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);

    // Stream ID
    buf.extend_from_slice(&header.stream_id.0);

    // Mode
    let mode_byte = match header.mode {
        StreamMode::BlobTransfer => 0x01,
    };
    buf.push(mode_byte);

    // Config: stripe_count, frame_size, metadata
    buf.push(header.config.stripe_count);
    buf.extend_from_slice(&header.config.frame_size.to_be_bytes());
    buf.extend_from_slice(&meta_len.to_be_bytes());
    buf.extend_from_slice(&header.config.metadata);

    buf
}

/// Decode a stream header from bytes.
pub fn decode_header(data: &[u8]) -> Result<StreamHeader, StreamError> {
    if data.len() < HEADER_FIXED_SIZE {
        return Err(StreamError::InvalidHeader(format!(
            "too short: {} bytes, need at least {HEADER_FIXED_SIZE}",
            data.len()
        )));
    }

    // Magic
    if data[0..2] != MAGIC {
        return Err(StreamError::InvalidHeader(format!(
            "bad magic: [{:#04x}, {:#04x}]",
            data[0], data[1]
        )));
    }

    // Version
    if data[2] != VERSION {
        return Err(StreamError::InvalidHeader(format!(
            "unsupported version: {}",
            data[2]
        )));
    }

    // Stream ID
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&data[3..19]);
    let stream_id = StreamId(id_bytes);

    // Mode
    let mode = match data[19] {
        0x01 => StreamMode::BlobTransfer,
        other => {
            return Err(StreamError::InvalidHeader(format!(
                "unknown mode: {other:#04x}"
            )));
        }
    };

    // Config
    let stripe_count = data[20];
    let frame_size = u32::from_be_bytes([data[21], data[22], data[23], data[24]]);
    let meta_len = u32::from_be_bytes([data[25], data[26], data[27], data[28]]) as usize;

    if data.len() < HEADER_FIXED_SIZE + meta_len {
        return Err(StreamError::InvalidHeader(format!(
            "metadata truncated: have {} bytes after fixed header, need {meta_len}",
            data.len() - HEADER_FIXED_SIZE
        )));
    }

    let metadata = data[HEADER_FIXED_SIZE..HEADER_FIXED_SIZE + meta_len].to_vec();

    Ok(StreamHeader {
        stream_id,
        mode,
        config: StreamConfig {
            stripe_count,
            frame_size,
            metadata,
        },
    })
}

/// Encode a data frame: `[4B payload_len (big-endian)] [payload]`.
/// A payload length of 0 signals end-of-stripe.
pub fn encode_data_frame(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// End-of-stripe sentinel: a frame with zero-length payload.
pub fn encode_end_of_stripe() -> [u8; 4] {
    [0, 0, 0, 0]
}

/// Result of decoding a data frame from a byte slice.
#[derive(Debug, PartialEq, Eq)]
pub enum DataFrameDecoded<'a> {
    /// A data frame with payload.
    Data(&'a [u8]),
    /// End-of-stripe sentinel.
    EndOfStripe,
}

/// Decode a data frame from a byte slice.
/// Returns the decoded frame and the number of bytes consumed.
pub fn decode_data_frame(data: &[u8]) -> Result<(DataFrameDecoded<'_>, usize), StreamError> {
    if data.len() < 4 {
        return Err(StreamError::InvalidHeader(
            "data frame too short for length prefix".into(),
        ));
    }

    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;

    if len == 0 {
        return Ok((DataFrameDecoded::EndOfStripe, 4));
    }

    if data.len() < 4 + len {
        return Err(StreamError::InvalidHeader(format!(
            "data frame truncated: need {len} bytes, have {}",
            data.len() - 4
        )));
    }

    Ok((DataFrameDecoded::Data(&data[4..4 + len]), 4 + len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn header_round_trip_basic() {
        let header = StreamHeader {
            stream_id: StreamId([1; 16]),
            mode: StreamMode::BlobTransfer,
            config: StreamConfig {
                stripe_count: 4,
                frame_size: 262144,
                metadata: vec![10, 20, 30],
            },
        };
        let encoded = encode_header(&header);
        let decoded = decode_header(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut encoded = encode_header(&StreamHeader {
            stream_id: StreamId([0; 16]),
            mode: StreamMode::BlobTransfer,
            config: StreamConfig::default(),
        });
        encoded[0] = 0xFF;
        assert!(matches!(
            decode_header(&encoded),
            Err(StreamError::InvalidHeader(_))
        ));
    }

    #[test]
    fn header_rejects_bad_version() {
        let mut encoded = encode_header(&StreamHeader {
            stream_id: StreamId([0; 16]),
            mode: StreamMode::BlobTransfer,
            config: StreamConfig::default(),
        });
        encoded[2] = 0xFF;
        assert!(matches!(
            decode_header(&encoded),
            Err(StreamError::InvalidHeader(_))
        ));
    }

    #[test]
    fn header_rejects_truncated() {
        let encoded = encode_header(&StreamHeader {
            stream_id: StreamId([0; 16]),
            mode: StreamMode::BlobTransfer,
            config: StreamConfig {
                metadata: vec![1, 2, 3],
                ..StreamConfig::default()
            },
        });
        // Chop off the metadata
        let truncated = &encoded[..HEADER_FIXED_SIZE];
        assert!(matches!(
            decode_header(truncated),
            Err(StreamError::InvalidHeader(_))
        ));
    }

    #[test]
    fn data_frame_round_trip() {
        let payload = b"hello world";
        let encoded = encode_data_frame(payload);
        let (decoded, consumed) = decode_data_frame(&encoded).unwrap();
        assert_eq!(decoded, DataFrameDecoded::Data(b"hello world"));
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn end_of_stripe_sentinel() {
        let sentinel = encode_end_of_stripe();
        assert_eq!(sentinel, [0, 0, 0, 0]);
        let (decoded, consumed) = decode_data_frame(&sentinel).unwrap();
        assert_eq!(decoded, DataFrameDecoded::EndOfStripe);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn data_frame_rejects_truncated() {
        let encoded = encode_data_frame(b"hello");
        // Only give the length prefix + partial payload
        let truncated = &encoded[..6];
        assert!(matches!(
            decode_data_frame(truncated),
            Err(StreamError::InvalidHeader(_))
        ));
    }

    proptest! {
        #[test]
        fn header_round_trip_arbitrary(
            id_bytes in prop::array::uniform16(any::<u8>()),
            stripe_count in 1u8..=16,
            frame_size in 1024u32..=1_048_576,
            metadata in prop::collection::vec(any::<u8>(), 0..256),
        ) {
            let header = StreamHeader {
                stream_id: StreamId(id_bytes),
                mode: StreamMode::BlobTransfer,
                config: StreamConfig {
                    stripe_count,
                    frame_size,
                    metadata,
                },
            };
            let encoded = encode_header(&header);
            let decoded = decode_header(&encoded).unwrap();
            prop_assert_eq!(header, decoded);
        }

        #[test]
        fn data_frame_round_trip_arbitrary(
            payload in prop::collection::vec(any::<u8>(), 0..262144),
        ) {
            if payload.is_empty() {
                // Empty payload encodes as end-of-stripe
                let encoded = encode_data_frame(&payload);
                let (decoded, _) = decode_data_frame(&encoded).unwrap();
                prop_assert_eq!(decoded, DataFrameDecoded::EndOfStripe);
            } else {
                let encoded = encode_data_frame(&payload);
                let (decoded, consumed) = decode_data_frame(&encoded).unwrap();
                prop_assert_eq!(decoded, DataFrameDecoded::Data(&payload));
                prop_assert_eq!(consumed, 4 + payload.len());
            }
        }
    }
}
