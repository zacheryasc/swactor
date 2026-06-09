//! The per-node mux: the single ordering authority (spec §5).
//!
//! Every producer on a node submits its bytes — tagged with a channel — to
//! one mux, and the mux interleaves them into the node's single ordered
//! stream. Because all channels pass through one assigner, a structured
//! event and a log line emitted close together have a well-defined relative
//! order (spec §5.1): that is what makes the one-timeline guarantee real.
//!
//! Two invariants do the heavy lifting:
//!
//! * **Gap-free, monotonic numbering** (spec §5.2). The mux hands out
//!   positions `0, 1, 2, …` with an atomic counter — never reused, never
//!   skipped. Numbering is independent of delivery: assigning a position
//!   does not mean the frame is, or ever will be, delivered.
//! * **A drop is a missing position, never a renumber** (spec §5.3). The
//!   mux buffers within a bound to smooth bursts; on overflow it drops the
//!   frame. But the position was already consumed, so the drop surfaces
//!   downstream as a detectable gap (spec §7.5) rather than a silent
//!   renumbering.
//!
//! Submission is non-blocking in spirit: the only shared section is an
//! O(1) counter bump and a push onto a bounded queue, so telemetry never
//! stalls the node's real work (spec §5.3).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::frame::{ChannelId, Frame, Position, StreamId};

/// A node's single position authority and outgoing telemetry buffer.
///
/// One mux belongs to one stream — one life of one node (spec §8.4). It is
/// `Send + Sync`: producers on different threads may submit concurrently
/// and the mux serializes them into one position order (spec §5.3).
pub struct Mux {
    stream: StreamId,
    next: AtomicU64,
    dropped: AtomicU64,
    capacity: usize,
    buffer: Mutex<VecDeque<Frame>>,
}

impl Mux {
    /// Create a mux for `stream` with a bounded outgoing buffer. When more
    /// than `capacity` frames are waiting to be drained, further
    /// submissions are dropped (spec §5.3) — but still consume a position.
    pub fn new(stream: StreamId, capacity: usize) -> Self {
        Mux {
            stream,
            next: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            capacity,
            buffer: Mutex::new(VecDeque::new()),
        }
    }

    /// Create a mux whose buffer never overflows. Useful when a caller
    /// drains promptly and wants every assigned frame retained.
    pub fn unbounded(stream: StreamId) -> Self {
        Mux::new(stream, usize::MAX)
    }

    /// The stream this mux produces (spec §8.4 ingest key).
    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    /// Submit opaque bytes on a channel. Assigns and returns the next
    /// position. The frame is buffered for the transport to drain, or
    /// dropped on overflow — either way the returned position is consumed,
    /// so a drop becomes a missing position downstream (spec §5.3).
    ///
    /// The mux never inspects `payload`; it is opaque (spec §4.1).
    pub fn submit(&self, channel: impl Into<ChannelId>, payload: Vec<u8>) -> Position {
        // Assign first, unconditionally: numbering is independent of
        // whether the frame survives the buffer (spec §5.2).
        let position = Position(self.next.fetch_add(1, Ordering::Relaxed));
        let frame = Frame { channel: channel.into(), position, payload };

        let mut buffer = self.buffer.lock().expect("mux buffer poisoned");
        if buffer.len() < self.capacity {
            buffer.push_back(frame);
        } else {
            // Overflow: drop the frame that does not fit. Its position is
            // already spent, so it will read as a gap, not a renumber.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        position
    }

    /// Pull all currently buffered frames, in the order they were buffered,
    /// emptying the buffer. The transport drains the mux's outgoing stream
    /// this way.
    pub fn drain(&self) -> Vec<Frame> {
        let mut buffer = self.buffer.lock().expect("mux buffer poisoned");
        buffer.drain(..).collect()
    }

    /// How many positions have been assigned — the gap-free high-water mark
    /// (spec §5.2). Equal to the number of `submit` calls.
    pub fn assigned(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// How many frames have been dropped on overflow (spec §5.3). Each
    /// dropped frame is one missing position downstream.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
