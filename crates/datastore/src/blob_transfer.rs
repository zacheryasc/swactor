//! BlobTransfer wire protocol — async functions for sending and receiving
//! content-addressed blobs over a QUIC stream (SendHalf / RecvHalf).
//!
//! Wire format:
//! ```text
//! [4B manifest_json_length (u32 BE)]
//! [N bytes manifest JSON]
//! [chunk_0 raw bytes]  ← size from manifest.chunks[0].size
//! [chunk_1 raw bytes]
//! ...
//! ```
//!
//! These run inside tokio tasks (NOT actor handlers).

use std::future::Future;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use swactor::runtime::Inbox;
use swactor::actor::Message;
use swactor_streams::handle::{RecvHalf, SendHalf};

use crate::types::{ContentHash, ObjectManifest};

// ─── Metadata encoding ──────────────────────────────────────────────────

/// Version tag for new-format metadata (byte 0).
const METADATA_VERSION_1: u8 = 0x01;

/// Structured metadata sent in `StreamConfig.metadata` for blob transfers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobTransferMetadata {
    pub content_hash: ContentHash,
    #[serde(default)]
    pub resume_from_chunk: Option<u64>,
}

/// Encode metadata into the wire format: `[0x01][JSON bytes]`.
pub fn encode_metadata(meta: &BlobTransferMetadata) -> Vec<u8> {
    let json = serde_json::to_vec(meta).expect("BlobTransferMetadata serialization cannot fail");
    let mut buf = Vec::with_capacity(1 + json.len());
    buf.push(METADATA_VERSION_1);
    buf.extend_from_slice(&json);
    buf
}

/// Parse metadata from either the legacy 32-byte format or the new versioned format.
pub fn parse_metadata(data: &[u8]) -> Option<BlobTransferMetadata> {
    if data.len() == 32 {
        // Legacy format: raw 32-byte ContentHash
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(data);
        return Some(BlobTransferMetadata {
            content_hash: ContentHash(hash_bytes),
            resume_from_chunk: None,
        });
    }
    if data.len() > 1 && data[0] == METADATA_VERSION_1 {
        return serde_json::from_slice(&data[1..]).ok();
    }
    None
}

// ─── Error type ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum BlobTransferError {
    /// The stream was closed or disconnected before the transfer completed.
    IncompleteTransfer(String),
    /// A chunk failed blake3 verification.
    ChunkVerificationFailed {
        index: usize,
        expected: ContentHash,
        actual: ContentHash,
    },
    /// Manifest JSON could not be parsed.
    InvalidManifest(String),
    /// An error from the underlying storage layer.
    Storage(String),
}

impl std::fmt::Display for BlobTransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobTransferError::IncompleteTransfer(msg) => {
                write!(f, "incomplete transfer: {msg}")
            }
            BlobTransferError::ChunkVerificationFailed {
                index,
                expected,
                actual,
            } => write!(
                f,
                "chunk {index} verification failed: expected {expected}, got {actual}"
            ),
            BlobTransferError::InvalidManifest(msg) => {
                write!(f, "invalid manifest: {msg}")
            }
            BlobTransferError::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for BlobTransferError {}

// ─── ReceivedBlob ────────────────────────────────────────────────────────

/// Result of a successful `recv_blob` call.
#[derive(Debug)]
pub struct ReceivedBlob {
    pub manifest: ObjectManifest,
    pub chunks: Vec<(ContentHash, Vec<u8>)>,
}

// ─── Core protocol ───────────────────────────────────────────────────────

/// Send a blob over a stream. Chunks are read on-demand via `read_chunk`.
///
/// `read_chunk` is called once per chunk — at most one chunk is in memory
/// at a time on the sender side.
///
/// `skip_chunks` allows resuming a previous transfer: the first `skip_chunks`
/// chunks are not read or written. The manifest preamble is always sent so the
/// receiver can verify integrity.
pub async fn send_blob<F, Fut>(
    send: &mut SendHalf,
    manifest: &ObjectManifest,
    read_chunk: F,
    skip_chunks: u64,
) -> Result<(), BlobTransferError>
where
    F: Fn(ContentHash) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, BlobTransferError>>,
{
    // Serialize manifest
    let manifest_json = serde_json::to_vec(manifest)
        .map_err(|e| BlobTransferError::InvalidManifest(e.to_string()))?;

    // Write manifest preamble: [4B length BE] [manifest JSON]
    let len_bytes = (manifest_json.len() as u32).to_be_bytes();
    write_all(send, &len_bytes).await?;
    write_all(send, &manifest_json).await?;

    // Write chunks, skipping already-transferred ones
    for (i, chunk_ref) in manifest.chunks.iter().enumerate() {
        if (i as u64) < skip_chunks {
            continue;
        }
        let data = read_chunk(chunk_ref.hash).await?;
        write_all(send, &data).await?;
    }

    // Flush and close
    send.flush()
        .map_err(|e| BlobTransferError::IncompleteTransfer(e.to_string()))?;
    send.close()
        .map_err(|e| BlobTransferError::IncompleteTransfer(e.to_string()))?;

    Ok(())
}

/// Receive a blob from a stream. Reads manifest, then reads and verifies
/// each chunk via blake3.
///
/// `skip_chunks` allows resuming: the sender skipped the first `skip_chunks`
/// chunks, so the receiver only reads chunks from `skip_chunks` onward.
/// The manifest preamble is always read.
pub async fn recv_blob(
    recv: &mut RecvHalf,
    skip_chunks: u64,
) -> Result<ReceivedBlob, BlobTransferError> {
    // Read manifest preamble
    let mut len_buf = [0u8; 4];
    read_exact(recv, &mut len_buf).await?;
    let manifest_len = u32::from_be_bytes(len_buf) as usize;

    // Read manifest JSON
    let mut manifest_buf = vec![0u8; manifest_len];
    read_exact(recv, &mut manifest_buf).await?;
    let manifest: ObjectManifest = serde_json::from_slice(&manifest_buf)
        .map_err(|e| BlobTransferError::InvalidManifest(e.to_string()))?;

    // Read and verify each chunk (only those the sender actually sent)
    let total = manifest.chunks.len();
    let start = (skip_chunks as usize).min(total);
    let mut chunks = Vec::with_capacity(total - start);
    for (i, chunk_ref) in manifest.chunks.iter().enumerate().skip(start) {
        let mut chunk_data = vec![0u8; chunk_ref.size as usize];
        read_exact(recv, &mut chunk_data).await?;

        // Verify blake3
        let actual_hash = ContentHash::of(&chunk_data);
        if actual_hash != chunk_ref.hash {
            return Err(BlobTransferError::ChunkVerificationFailed {
                index: i,
                expected: chunk_ref.hash,
                actual: actual_hash,
            });
        }

        chunks.push((chunk_ref.hash, chunk_data));
    }

    Ok(ReceivedBlob { manifest, chunks })
}

// ─── Helpers ─────────────────────────────────────────────────────────────

/// Write all bytes to a SendHalf, yielding when backpressured.
async fn write_all(send: &mut SendHalf, data: &[u8]) -> Result<(), BlobTransferError> {
    let mut offset = 0;
    while offset < data.len() {
        match send.try_write(&data[offset..]) {
            Ok(0) => {
                // Backpressure — yield and retry
                tokio::task::yield_now().await;
            }
            Ok(n) => {
                offset += n;
            }
            Err(e) => {
                return Err(BlobTransferError::IncompleteTransfer(e.to_string()));
            }
        }
    }
    Ok(())
}

/// Read exactly `buf.len()` bytes from a RecvHalf, yielding when no data.
async fn read_exact(recv: &mut RecvHalf, buf: &mut [u8]) -> Result<(), BlobTransferError> {
    let mut offset = 0;
    while offset < buf.len() {
        match recv.try_read(&mut buf[offset..]) {
            Ok(0) => {
                // No data available — yield and retry
                tokio::task::yield_now().await;
            }
            Ok(n) => {
                offset += n;
            }
            Err(e) => {
                return Err(BlobTransferError::IncompleteTransfer(e.to_string()));
            }
        }
    }
    Ok(())
}

/// Async version of `bridge.rs:poll_response` — yields instead of thread::sleep.
pub async fn poll_inbox<M: Message>(inbox: &Inbox<M>, timeout: Duration) -> Option<M> {
    let start = tokio::time::Instant::now();
    loop {
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunking::chunk_blob;
    use swactor_streams::handle::create_stream_handle;
    use swactor_streams::types::{StreamConfig, StreamId};

    /// Helper: create a pair of (SendHalf, RecvHalf) connected via tokio tasks
    /// that relay data through a DuplexStream.
    fn create_test_pair() -> (SendHalf, RecvHalf) {
        let stream_id = StreamId::new_random();
        let config = StreamConfig {
            stripe_count: 1,
            frame_size: 256 * 1024,
            metadata: Vec::new(),
        };
        let (handle_a, endpoints_a) = create_stream_handle(stream_id, &config, 128, 64);
        let (handle_b, endpoints_b) = create_stream_handle(stream_id, &config, 128, 64);

        // Wire a's send → b's recv via a DuplexStream
        let (client, server) = tokio::io::duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);

        // a's send data-plane task: read from cmd_rx, write to client_write
        spawn_send_task(endpoints_a.send_cmd_rx, endpoints_a.send_evt_tx, endpoints_a.pool.clone(), client_write);
        // b's recv data-plane task: read from server_read, push to evt_tx
        spawn_recv_task(server_read, endpoints_b.recv_evt_tx, endpoints_b.pool.clone());

        // b's send data-plane task: for the other direction (not used in basic tests)
        spawn_send_task(endpoints_b.send_cmd_rx, endpoints_b.send_evt_tx, endpoints_b.pool.clone(), server_write);
        // a's recv data-plane task
        spawn_recv_task(client_read, endpoints_a.recv_evt_tx, endpoints_a.pool.clone());

        // Return a's send half and b's recv half for unidirectional testing
        (handle_a.send, handle_b.recv)
    }

    fn spawn_send_task(
        mut cmd_rx: tokio::sync::mpsc::Receiver<swactor_streams::channel::SendCommand>,
        evt_tx: tokio::sync::mpsc::Sender<swactor_streams::channel::SendEvent>,
        pool: swactor_streams::BufferPool,
        mut writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    ) {
        use tokio::io::AsyncWriteExt;
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    swactor_streams::channel::SendCommand::Data(buf) => {
                        let data = buf.written();
                        // Write length-prefixed frame
                        let len = (data.len() as u32).to_be_bytes();
                        if writer.write_all(&len).await.is_err() {
                            pool.checkin(buf);
                            let _ = evt_tx.send(swactor_streams::channel::SendEvent::Error(
                                swactor_streams::StreamError::Disconnected,
                            )).await;
                            return;
                        }
                        if writer.write_all(data).await.is_err() {
                            pool.checkin(buf);
                            let _ = evt_tx.send(swactor_streams::channel::SendEvent::Error(
                                swactor_streams::StreamError::Disconnected,
                            )).await;
                            return;
                        }
                        pool.checkin(buf);
                    }
                    swactor_streams::channel::SendCommand::Flush => {
                        let _ = writer.flush().await;
                    }
                    swactor_streams::channel::SendCommand::Close => {
                        let _ = writer.shutdown().await;
                        break;
                    }
                }
            }
        });
    }

    fn spawn_recv_task(
        mut reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
        evt_tx: tokio::sync::mpsc::Sender<swactor_streams::channel::RecvEvent>,
        pool: swactor_streams::BufferPool,
    ) {
        use tokio::io::AsyncReadExt;
        tokio::spawn(async move {
            loop {
                // Read length-prefixed frame
                let mut len_buf = [0u8; 4];
                match reader.read_exact(&mut len_buf).await {
                    Ok(_) => {}
                    Err(_) => {
                        let _ = evt_tx.send(swactor_streams::channel::RecvEvent::Closed).await;
                        return;
                    }
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut data = vec![0u8; len];
                match reader.read_exact(&mut data).await {
                    Ok(_) => {}
                    Err(_) => {
                        let _ = evt_tx.send(swactor_streams::channel::RecvEvent::Closed).await;
                        return;
                    }
                }

                // Write data into FrameBufs and send
                let mut offset = 0;
                while offset < data.len() {
                    let mut buf = match pool.checkout() {
                        Some(b) => b,
                        None => {
                            let _ = evt_tx.send(swactor_streams::channel::RecvEvent::Error(
                                swactor_streams::StreamError::BufferExhausted,
                            )).await;
                            return;
                        }
                    };
                    let written = buf.write(&data[offset..]);
                    offset += written;
                    let _ = evt_tx.send(swactor_streams::channel::RecvEvent::Data(buf)).await;
                }
            }
        });
    }

    #[tokio::test]
    async fn small_blob_round_trips() {
        let data = b"hello, world!";
        let (_, manifest, chunks) = chunk_blob(data, 1024);

        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, 0)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, 0).await.unwrap()
        });

        send_task.await.unwrap();
        let received = recv_task.await.unwrap();

        assert_eq!(received.manifest, manifest);
        assert_eq!(received.chunks.len(), chunks.len());
        for (i, (hash, data)) in received.chunks.iter().enumerate() {
            assert_eq!(*hash, chunks[i].0);
            assert_eq!(*data, chunks[i].1);
        }
    }

    #[tokio::test]
    async fn multi_chunk_round_trips() {
        // 4MB with 256KB chunks = 16 chunks
        let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let (_, manifest, chunks) = chunk_blob(&data, 256 * 1024);
        assert_eq!(chunks.len(), 16);

        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, 0)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, 0).await.unwrap()
        });

        send_task.await.unwrap();
        let received = recv_task.await.unwrap();

        assert_eq!(received.manifest, manifest);
        assert_eq!(received.chunks.len(), 16);
        // Verify all chunk data matches
        for (i, (hash, cdata)) in received.chunks.iter().enumerate() {
            assert_eq!(*hash, chunks[i].0);
            assert_eq!(cdata.len(), chunks[i].1.len());
        }
    }

    #[tokio::test]
    async fn corrupted_chunk_detected() {
        let data = b"integrity test data here";
        let (_, manifest, mut chunks) = chunk_blob(data, 1024);

        // Flip a byte in the chunk data
        chunks[0].1[0] ^= 0xFF;

        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    // Note: we send the corrupted data (hash won't match)
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, 0)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, 0).await
        });

        send_task.await.unwrap();
        let result = recv_task.await.unwrap();

        match result {
            Err(BlobTransferError::ChunkVerificationFailed { index: 0, .. }) => {}
            other => panic!("expected ChunkVerificationFailed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn truncated_stream_detected() {
        // Create a blob with multiple chunks
        let data: Vec<u8> = vec![42u8; 4096];
        let (_, manifest, chunks) = chunk_blob(&data, 1024);

        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        // Only send the manifest + first chunk, then close
        let send_task = tokio::spawn(async move {
            let manifest_json = serde_json::to_vec(&manifest_clone).unwrap();
            let len_bytes = (manifest_json.len() as u32).to_be_bytes();
            write_all(&mut send, &len_bytes).await.unwrap();
            write_all(&mut send, &manifest_json).await.unwrap();
            // Write first chunk
            write_all(&mut send, &chunks[0].1).await.unwrap();
            // Close without writing remaining chunks
            send.flush().unwrap();
            send.close().unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, 0).await
        });

        send_task.await.unwrap();
        let result = recv_task.await.unwrap();

        match result {
            Err(BlobTransferError::IncompleteTransfer(_)) => {}
            other => panic!("expected IncompleteTransfer, got: {other:?}"),
        }
    }

    // ── Resume token tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn resume_skips_first_n_chunks() {
        // 16 chunks, resume from chunk 8 → only chunks 8-15 transferred
        let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let (_, manifest, chunks) = chunk_blob(&data, 256 * 1024);
        assert_eq!(chunks.len(), 16);

        let skip = 8u64;
        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, skip)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, skip).await.unwrap()
        });

        send_task.await.unwrap();
        let received = recv_task.await.unwrap();

        assert_eq!(received.manifest, manifest);
        assert_eq!(received.chunks.len(), 8); // only chunks 8-15
        for (j, (hash, cdata)) in received.chunks.iter().enumerate() {
            let orig_idx = skip as usize + j;
            assert_eq!(*hash, chunks[orig_idx].0);
            assert_eq!(*cdata, chunks[orig_idx].1);
        }
    }

    #[tokio::test]
    async fn resume_from_zero_is_full_transfer() {
        let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let (_, manifest, chunks) = chunk_blob(&data, 256 * 1024);

        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, 0)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, 0).await.unwrap()
        });

        send_task.await.unwrap();
        let received = recv_task.await.unwrap();

        assert_eq!(received.chunks.len(), chunks.len());
    }

    #[tokio::test]
    async fn resume_from_last_chunk() {
        // 16 chunks, skip=15 → only the final chunk transfers
        let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let (_, manifest, chunks) = chunk_blob(&data, 256 * 1024);
        assert_eq!(chunks.len(), 16);

        let skip = 15u64;
        let (mut send, mut recv) = create_test_pair();

        let manifest_clone = manifest.clone();
        let chunks_clone = chunks.clone();
        let send_task = tokio::spawn(async move {
            send_blob(&mut send, &manifest_clone, |hash| {
                let chunks = chunks_clone.clone();
                async move {
                    chunks
                        .iter()
                        .find(|(h, _)| *h == hash)
                        .map(|(_, d)| d.clone())
                        .ok_or_else(|| BlobTransferError::Storage("chunk not found".into()))
                }
            }, skip)
            .await
            .unwrap();
        });

        let recv_task = tokio::spawn(async move {
            recv_blob(&mut recv, skip).await.unwrap()
        });

        send_task.await.unwrap();
        let received = recv_task.await.unwrap();

        assert_eq!(received.chunks.len(), 1);
        assert_eq!(received.chunks[0].0, chunks[15].0);
        assert_eq!(received.chunks[0].1, chunks[15].1);
    }

    #[test]
    fn metadata_encoding_round_trip() {
        let meta = BlobTransferMetadata {
            content_hash: ContentHash([42u8; 32]),
            resume_from_chunk: Some(50),
        };
        let encoded = encode_metadata(&meta);
        assert_eq!(encoded[0], 0x01);
        let decoded = parse_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn metadata_backward_compat() {
        // Legacy 32-byte format → parsed as no resume offset
        let raw_hash = [0xABu8; 32];
        let decoded = parse_metadata(&raw_hash).unwrap();
        assert_eq!(decoded.content_hash, ContentHash(raw_hash));
        assert_eq!(decoded.resume_from_chunk, None);
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn arbitrary_blob_round_trips(
                data in proptest::collection::vec(any::<u8>(), 1..=128 * 1024),
                chunk_size in 256u32..=64 * 1024,
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let (_, manifest, chunks) = chunk_blob(&data, chunk_size);
                    let (mut send, mut recv) = create_test_pair();

                    let manifest_clone = manifest.clone();
                    let chunks_clone = chunks.clone();
                    let send_task = tokio::spawn(async move {
                        send_blob(&mut send, &manifest_clone, |hash| {
                            let chunks = chunks_clone.clone();
                            async move {
                                chunks
                                    .iter()
                                    .find(|(h, _)| *h == hash)
                                    .map(|(_, d)| d.clone())
                                    .ok_or_else(|| BlobTransferError::Storage("not found".into()))
                            }
                        }, 0)
                        .await
                        .unwrap();
                    });

                    let recv_task = tokio::spawn(async move {
                        recv_blob(&mut recv, 0).await.unwrap()
                    });

                    send_task.await.unwrap();
                    let received = recv_task.await.unwrap();

                    prop_assert_eq!(received.manifest, manifest);
                    prop_assert_eq!(received.chunks.len(), chunks.len());
                    for (i, (hash, cdata)) in received.chunks.iter().enumerate() {
                        prop_assert_eq!(*hash, chunks[i].0);
                        prop_assert_eq!(cdata, &chunks[i].1);
                    }

                    Ok(())
                })?;
            }

            #[test]
            fn arbitrary_resume_offset_round_trips(
                data in proptest::collection::vec(any::<u8>(), 1..=128 * 1024),
                chunk_size in 256u32..=64 * 1024,
                skip_frac in 0.0f64..1.0,
            ) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let (_, manifest, chunks) = chunk_blob(&data, chunk_size);
                    let total = chunks.len() as u64;
                    let skip = (skip_frac * total as f64).floor() as u64;

                    let (mut send, mut recv) = create_test_pair();

                    let manifest_clone = manifest.clone();
                    let chunks_clone = chunks.clone();
                    let send_task = tokio::spawn(async move {
                        send_blob(&mut send, &manifest_clone, |hash| {
                            let chunks = chunks_clone.clone();
                            async move {
                                chunks
                                    .iter()
                                    .find(|(h, _)| *h == hash)
                                    .map(|(_, d)| d.clone())
                                    .ok_or_else(|| BlobTransferError::Storage("not found".into()))
                            }
                        }, skip)
                        .await
                        .unwrap();
                    });

                    let recv_task = tokio::spawn(async move {
                        recv_blob(&mut recv, skip).await.unwrap()
                    });

                    send_task.await.unwrap();
                    let received = recv_task.await.unwrap();

                    let expected_count = total - skip;
                    prop_assert_eq!(received.chunks.len() as u64, expected_count);
                    for (j, (hash, cdata)) in received.chunks.iter().enumerate() {
                        let orig_idx = skip as usize + j;
                        prop_assert_eq!(*hash, chunks[orig_idx].0);
                        prop_assert_eq!(cdata, &chunks[orig_idx].1);
                    }

                    Ok(())
                })?;
            }
        }
    }
}
