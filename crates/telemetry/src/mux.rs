//! The per-stream mux: the single ordering authority (spec §4.4).
//!
//! Producers submit bytes tagged with a stream-local channel id. The mux accepts
//! payloads into a bounded queue first, then assigns a single monotonic position
//! sequence while draining accepted payloads.

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frame::{ChannelId, Frame, Position, StreamId};

struct PendingFrame {
    channel: ChannelId,
    payload: Vec<u8>,
}

/// A node's single position authority and outgoing telemetry queue.
pub struct Mux {
    stream: StreamId,
    next: AtomicU64,
    dropped: AtomicU64,
    tx: Sender<PendingFrame>,
    rx: Receiver<PendingFrame>,
}

impl Mux {
    /// Create a mux for `stream` with a bounded outgoing queue.
    pub fn new(stream: StreamId, capacity: usize) -> Self {
        let capacity = capacity.clamp(1, 1_048_576);
        let (tx, rx) = bounded(capacity);
        Mux {
            stream,
            next: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            tx,
            rx,
        }
    }

    /// Create a mux whose queue is large enough for tests that drain promptly.
    pub fn unbounded(stream: StreamId) -> Self {
        Mux::new(stream, usize::MAX)
    }

    /// The stream this mux produces (spec §2.2, §7.1 ingest key).
    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    /// Submit opaque bytes on a registered channel id.
    pub fn submit(&self, channel: ChannelId, payload: Vec<u8>) -> bool {
        match self.tx.try_send(PendingFrame { channel, payload }) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Pull all currently queued frames in mux queue order.
    pub fn drain(&self) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Ok(pending) = self.rx.try_recv() {
            // Position is consumed only after a pending frame has left
            // the queue; failed submit never reaches this point.
            let position = Position(self.next.fetch_add(1, Ordering::Relaxed));
            frames.push(Frame {
                channel: pending.channel,
                position,
                payload: pending.payload,
            });
        }
        frames
    }

    /// How many positions have been assigned while draining accepted frames.
    pub fn assigned(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// How many submissions have been dropped before entering the mux.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
