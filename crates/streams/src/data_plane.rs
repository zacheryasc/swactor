use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::buffer::BufferPool;
use crate::channel::{RecvEvent, SendCommand};
use crate::notify::NotifySink;
use crate::types::StreamError;
use crate::wire;

/// A send-side data-plane task for a single stripe.
///
/// Reads `SendCommand`s from the actor's channel, encodes them as wire
/// frames, and writes them to the underlying transport. Returns consumed
/// buffers to the pool.
///
/// Generic over `AsyncWrite` so it can be tested with `DuplexStream`.
pub async fn send_stripe_task<W>(
    mut writer: W,
    mut cmd_rx: mpsc::Receiver<SendCommand>,
    pool: BufferPool,
    notify: Option<NotifySink>,
) -> Result<(), StreamError>
where
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            SendCommand::Data(buf) => {
                let frame = wire::encode_data_frame(buf.written());
                writer
                    .write_all(&frame)
                    .await
                    .map_err(|e| StreamError::BrokenPipe(e.to_string()))?;

                // Return buffer to pool
                pool.checkin(buf);

                // Signal write ready
                if let Some(ref sink) = notify {
                    sink.write_ready();
                }
            }
            SendCommand::Flush => {
                writer
                    .flush()
                    .await
                    .map_err(|e| StreamError::BrokenPipe(e.to_string()))?;
            }
            SendCommand::Close => {
                // Write end-of-stripe sentinel
                let sentinel = wire::encode_end_of_stripe();
                writer
                    .write_all(&sentinel)
                    .await
                    .map_err(|e| StreamError::BrokenPipe(e.to_string()))?;
                writer
                    .flush()
                    .await
                    .map_err(|e| StreamError::BrokenPipe(e.to_string()))?;
                break;
            }
        }
    }

    Ok(())
}

/// A recv-side data-plane task for a single stripe.
///
/// Reads wire-encoded frames from the transport, fills `FrameBuf`s from
/// the pool, and sends them to the actor via the event channel.
///
/// Generic over `AsyncRead` so it can be tested with `DuplexStream`.
pub async fn recv_stripe_task<R>(
    mut reader: R,
    evt_tx: mpsc::Sender<RecvEvent>,
    pool: BufferPool,
    notify: Option<NotifySink>,
) -> Result<(), StreamError>
where
    R: AsyncReadExt + Unpin + Send + 'static,
{
    loop {
        // Read the 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Connection closed
                let _ = evt_tx.send(RecvEvent::Closed).await;
                if let Some(ref sink) = notify {
                    sink.closed();
                }
                return Ok(());
            }
            Err(e) => {
                let err = StreamError::BrokenPipe(e.to_string());
                let _ = evt_tx.send(RecvEvent::Error(err.clone())).await;
                if let Some(ref sink) = notify {
                    sink.error();
                }
                return Err(err);
            }
        }

        let payload_len = u32::from_be_bytes(len_buf) as usize;

        // End-of-stripe sentinel
        if payload_len == 0 {
            let _ = evt_tx.send(RecvEvent::Closed).await;
            if let Some(ref sink) = notify {
                sink.closed();
            }
            return Ok(());
        }

        // Read the payload into a buffer from the pool
        let mut buf = match pool.checkout() {
            Some(b) => b,
            None => {
                let err = StreamError::BufferExhausted;
                let _ = evt_tx.send(RecvEvent::Error(err.clone())).await;
                if let Some(ref sink) = notify {
                    sink.error();
                }
                return Err(err);
            }
        };

        let mut temp = vec![0u8; payload_len];
        match reader.read_exact(&mut temp).await {
            Ok(_) => {}
            Err(e) => {
                pool.checkin(buf);
                let err = StreamError::BrokenPipe(e.to_string());
                let _ = evt_tx.send(RecvEvent::Error(err.clone())).await;
                if let Some(ref sink) = notify {
                    sink.error();
                }
                return Err(err);
            }
        }
        buf.load(&temp);

        // Send to actor
        if evt_tx.send(RecvEvent::Data(buf)).await.is_err() {
            return Err(StreamError::Disconnected);
        }

        if let Some(ref sink) = notify {
            sink.data_ready();
        }
    }
}

/// Spawn a complete set of send-side stripe tasks.
///
/// Returns a Vec of `mpsc::Sender<SendCommand>` -- one per stripe.
/// The caller assigns chunks round-robin: chunk `i` goes to stripe `i % stripe_count`.
pub fn spawn_send_stripes<W, F>(
    stripe_count: usize,
    pool: BufferPool,
    _notify: Option<NotifySink>,
    mut writer_factory: F,
    channel_capacity: usize,
) -> Vec<mpsc::Sender<SendCommand>>
where
    W: AsyncWriteExt + Unpin + Send + 'static,
    F: FnMut(usize) -> W,
{
    let mut senders = Vec::with_capacity(stripe_count);

    for i in 0..stripe_count {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let writer = writer_factory(i);
        let pool = pool.clone();
        tokio::spawn(async move {
            let _ = send_stripe_task(writer, rx, pool, None).await;
        });
        senders.push(tx);
    }

    senders
}

/// Spawn a complete set of recv-side stripe tasks.
///
/// Returns a single `mpsc::Receiver<RecvEvent>` that merges events from all stripes.
pub fn spawn_recv_stripes<R, F>(
    stripe_count: usize,
    pool: BufferPool,
    _notify: Option<NotifySink>,
    mut reader_factory: F,
    channel_capacity: usize,
) -> mpsc::Receiver<RecvEvent>
where
    R: AsyncReadExt + Unpin + Send + 'static,
    F: FnMut(usize) -> R,
{
    // All stripes feed into a single merged channel
    let (merged_tx, merged_rx) = mpsc::channel(channel_capacity * stripe_count);

    for i in 0..stripe_count {
        let reader = reader_factory(i);
        let pool = pool.clone();
        let tx = merged_tx.clone();
        tokio::spawn(async move {
            let _ = recv_stripe_task(reader, tx, pool, None).await;
        });
    }

    merged_rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPool;
    use crate::types::StreamId;

    fn make_test_pool(count: usize, capacity: usize) -> BufferPool {
        BufferPool::new(count, capacity)
    }

    /// End-to-end: send data through a single stripe, receive it back.
    #[tokio::test]
    async fn single_stripe_transfer() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let pool = make_test_pool(16, 1024);

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (evt_tx, mut evt_rx) = mpsc::channel(16);

        let send_pool = pool.clone();
        let recv_pool = pool.clone();

        let send_handle = tokio::spawn(async move {
            send_stripe_task(client, cmd_rx, send_pool, None).await
        });
        let recv_handle = tokio::spawn(async move {
            recv_stripe_task(server, evt_tx, recv_pool, None).await
        });

        // Send some data
        let test_data = b"hello, streams!";
        let mut buf = pool.checkout().unwrap();
        buf.write(test_data);
        cmd_tx.send(SendCommand::Data(buf)).await.unwrap();

        // Send close
        cmd_tx.send(SendCommand::Close).await.unwrap();

        // Receive data
        let evt = evt_rx.recv().await.unwrap();
        match evt {
            RecvEvent::Data(mut buf) => {
                let mut out = vec![0u8; test_data.len()];
                let n = buf.read(&mut out);
                assert_eq!(n, test_data.len());
                assert_eq!(&out, test_data);
            }
            other => panic!("expected Data, got {:?}", std::mem::discriminant(&other)),
        }

        // Receive close
        let evt = evt_rx.recv().await.unwrap();
        assert!(matches!(evt, RecvEvent::Closed));

        send_handle.await.unwrap().unwrap();
        recv_handle.await.unwrap().unwrap();
    }

    /// Multiple chunks through a single stripe.
    #[tokio::test]
    async fn multiple_chunks_single_stripe() {
        let (client, server) = tokio::io::duplex(256 * 1024);
        let pool = make_test_pool(32, 1024);

        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (evt_tx, mut evt_rx) = mpsc::channel(32);

        let sp = pool.clone();
        let rp = pool.clone();

        tokio::spawn(async move { send_stripe_task(client, cmd_rx, sp, None).await });
        tokio::spawn(async move { recv_stripe_task(server, evt_tx, rp, None).await });

        let chunk_count = 20;
        for i in 0..chunk_count {
            let mut buf = pool.checkout().unwrap();
            let data = format!("chunk-{i:04}");
            buf.write(data.as_bytes());
            cmd_tx.send(SendCommand::Data(buf)).await.unwrap();
        }
        cmd_tx.send(SendCommand::Close).await.unwrap();

        let mut received = Vec::new();
        loop {
            match evt_rx.recv().await.unwrap() {
                RecvEvent::Data(mut buf) => {
                    let mut out = vec![0u8; buf.remaining()];
                    buf.read(&mut out);
                    received.push(String::from_utf8(out).unwrap());
                }
                RecvEvent::Closed => break,
                RecvEvent::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        assert_eq!(received.len(), chunk_count);
        for (i, chunk) in received.iter().enumerate() {
            assert_eq!(chunk, &format!("chunk-{i:04}"));
        }
    }

    /// Multi-stripe transfer with round-robin assignment.
    #[tokio::test]
    async fn multi_stripe_round_robin() {
        let stripe_count = 4;
        let chunk_count = 100;
        // Pool needs enough buffers for in-flight data on both sides
        let pool = make_test_pool(256, 256);

        // Create duplex pairs for each stripe
        let mut send_writers = Vec::new();
        let mut recv_readers = Vec::new();
        for _ in 0..stripe_count {
            let (client, server) = tokio::io::duplex(64 * 1024);
            send_writers.push(Some(client));
            recv_readers.push(Some(server));
        }

        // Spawn recv stripe tasks
        let (merged_tx, mut merged_rx) = mpsc::channel(chunk_count * 2);
        for i in 0..stripe_count {
            let reader = recv_readers[i].take().unwrap();
            let p = pool.clone();
            let tx = merged_tx.clone();
            tokio::spawn(async move {
                recv_stripe_task(reader, tx, p, None).await
            });
        }
        drop(merged_tx); // so merged_rx closes when all tasks finish

        // Spawn send stripe tasks
        let mut stripe_txs = Vec::new();
        for i in 0..stripe_count {
            let (tx, rx) = mpsc::channel(32);
            let writer = send_writers[i].take().unwrap();
            let p = pool.clone();
            tokio::spawn(async move {
                send_stripe_task(writer, rx, p, None).await
            });
            stripe_txs.push(tx);
        }

        // Send chunks round-robin
        for i in 0..chunk_count {
            let stripe_idx = i % stripe_count;
            let mut buf = pool.checkout().unwrap();
            let data = format!("chunk-{i:04}");
            buf.write(data.as_bytes());
            stripe_txs[stripe_idx]
                .send(SendCommand::Data(buf))
                .await
                .unwrap();
        }

        // Close all stripes
        for tx in &stripe_txs {
            tx.send(SendCommand::Close).await.unwrap();
        }

        // Collect all received data (order may differ per stripe)
        let mut received = Vec::new();
        let mut closed_count = 0;
        while let Some(evt) = merged_rx.recv().await {
            match evt {
                RecvEvent::Data(mut buf) => {
                    let mut out = vec![0u8; buf.remaining()];
                    buf.read(&mut out);
                    received.push(String::from_utf8(out).unwrap());
                }
                RecvEvent::Closed => {
                    closed_count += 1;
                    if closed_count == stripe_count {
                        break;
                    }
                }
                RecvEvent::Error(e) => panic!("unexpected error: {e}"),
            }
        }

        // All chunks should have arrived (order may vary across stripes)
        assert_eq!(received.len(), chunk_count);
        received.sort();
        for (i, chunk) in received.iter().enumerate() {
            assert_eq!(chunk, &format!("chunk-{i:04}"));
        }
    }

    /// Graceful close: writer closes, receiver sees end-of-stripe then Closed.
    #[tokio::test]
    async fn graceful_close() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let pool = make_test_pool(8, 256);

        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (evt_tx, mut evt_rx) = mpsc::channel(8);

        let sp = pool.clone();
        let rp = pool.clone();

        tokio::spawn(async move { send_stripe_task(client, cmd_rx, sp, None).await });
        tokio::spawn(async move { recv_stripe_task(server, evt_tx, rp, None).await });

        // Close immediately without sending data
        cmd_tx.send(SendCommand::Close).await.unwrap();

        // Should receive Closed
        let evt = evt_rx.recv().await.unwrap();
        assert!(matches!(evt, RecvEvent::Closed));
    }

    /// Notification coalescing through NotifySink.
    #[tokio::test]
    async fn notification_coalescing() {
        use crate::notify::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let flag = Arc::new(NotifyFlag::new());
        let inject_count = Arc::new(AtomicUsize::new(0));
        let count_clone = inject_count.clone();

        let stream_id = StreamId::new_random();
        let sink = NotifySink::new(flag.clone(), stream_id, move |_evt| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        // First notification should inject
        sink.data_ready();
        assert_eq!(inject_count.load(Ordering::SeqCst), 1);

        // Duplicate should coalesce (no inject)
        sink.data_ready();
        assert_eq!(inject_count.load(Ordering::SeqCst), 1);

        // Clear and re-notify
        flag.clear(DATA_READY);
        sink.data_ready();
        assert_eq!(inject_count.load(Ordering::SeqCst), 2);

        // Different flag should still inject independently
        sink.write_ready();
        assert_eq!(inject_count.load(Ordering::SeqCst), 3);
    }

    /// Backpressure: when channel and active buffer are saturated, try_write returns 0.
    #[tokio::test]
    async fn send_backpressure() {
        use crate::handle::create_stream_handle;
        use crate::types::StreamConfig;

        let config = StreamConfig {
            stripe_count: 1,
            frame_size: 64,
            metadata: vec![],
        };

        // Pool of 4, channel of 2 -- we can fill both quickly
        let (mut handle, _endpoints) = create_stream_handle(
            StreamId::new_random(),
            &config,
            4,
            2,
        );

        let data = vec![0xAA; 64]; // exactly fills one buffer

        // Write 1: checks out buf, fills it (64 bytes), buf is full -> try_send succeeds
        let n1 = handle.send.try_write(&data).unwrap();
        assert_eq!(n1, 64);

        // Write 2: checks out new buf, fills it, buf is full -> try_send succeeds
        let n2 = handle.send.try_write(&data).unwrap();
        assert_eq!(n2, 64);

        // Write 3: checks out new buf, fills it, buf is full -> try_send fails (channel full)
        // Data IS in the buffer (written=64), buffer kept locally
        let n3 = handle.send.try_write(&data).unwrap();
        assert_eq!(n3, 64);

        // Write 4: active buf still full, buf.write() returns 0 (no space),
        // try_send fails again -> returns 0 signaling backpressure
        let n4 = handle.send.try_write(&data).unwrap();
        assert_eq!(n4, 0); // backpressure!
    }
}
