//! The per-node mux: the single ordering authority (spec §5).
//!
//! Every producer on a node submits bytes tagged with a stream-local channel id
//! to one mux, and the mux assigns a single monotonic position sequence across
//! all channels. A drop consumes a position and is therefore visible downstream
//! as a gap.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::time::{SystemTime, UNIX_EPOCH};

use super::frame::{ChannelId, Frame, Position, StreamId};
use super::record::Record;
use super::timing::FRAME_TIME_CHANNEL_ID;
use super::timing::FrameTimeSample;

/// A node's single position authority and outgoing telemetry queue.
pub struct Mux {
    stream: StreamId,
    next: AtomicU64,
    dropped: AtomicU64,
    frame_timing_enabled: AtomicBool,
    tx: SyncSender<Frame>,
    rx: Mutex<Receiver<Frame>>,
}

impl Mux {
    /// Create a mux for `stream` with a bounded outgoing queue.
    pub fn new(stream: StreamId, capacity: usize) -> Self {
        let capacity = capacity.max(1).min(1_048_576);
        let (tx, rx) = sync_channel(capacity);
        Mux {
            stream,
            next: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            frame_timing_enabled: AtomicBool::new(true),
            tx,
            rx: Mutex::new(rx),
        }
    }

    /// Create a mux whose queue is large enough for tests that drain promptly.
    pub fn unbounded(stream: StreamId) -> Self {
        Mux::new(stream, usize::MAX)
    }

    /// The stream this mux produces (spec §8.4 ingest key).
    pub fn stream_id(&self) -> &StreamId {
        &self.stream
    }

    /// Submit opaque bytes on a registered channel id.
    pub fn submit(&self, channel: ChannelId, payload: Vec<u8>) -> Position {
        let position = Position(self.next.fetch_add(1, Ordering::Relaxed));
        let timing_sample = self
            .frame_timing_enabled
            .load(Ordering::Relaxed)
            .then(|| now_unix_ns())
            .filter(|_| channel != FRAME_TIME_CHANNEL_ID)
            .map(|created_at_unix_ns| FrameTimeSample::new(position, created_at_unix_ns));
        let frame = Frame {
            channel,
            position,
            payload,
        };

        match self.tx.try_send(frame) {
            Ok(()) => {
                if let Some(sample) = timing_sample {
                    self.push_timing_sample_if_room(sample);
                }
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        position
    }

    /// Pull all currently queued frames, sorted by mux position.
    pub fn drain(&self) -> Vec<Frame> {
        let rx = self.rx.lock().expect("mux receiver poisoned");
        let mut frames = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(frame) => frames.push(frame),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        frames.sort_by_key(|frame| frame.position);
        frames
    }

    /// Enable or disable optional sidecar timing samples for newly submitted frames.
    pub fn set_frame_timing_enabled(&self, enabled: bool) {
        self.frame_timing_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Whether this mux currently emits sidecar frame timing samples.
    pub fn frame_timing_enabled(&self) -> bool {
        self.frame_timing_enabled.load(Ordering::Relaxed)
    }

    fn push_timing_sample_if_room(&self, sample: FrameTimeSample) {
        let position = Position(self.next.fetch_add(1, Ordering::Relaxed));
        let frame = Frame {
            channel: FRAME_TIME_CHANNEL_ID,
            position,
            payload: sample.encode(),
        };
        let _ = self.tx.try_send(frame);
    }

    /// How many positions have been assigned — the gap-free high-water mark.
    pub fn assigned(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }

    /// How many data frames have been dropped on overflow.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn now_unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
