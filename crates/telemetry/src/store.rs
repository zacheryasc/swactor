//! The stored stream — the consumer's frame source of truth (spec §7).
//!
//! Storage holds each node's complete stream, whole and append-only. Nothing is
//! thinned, aggregated, decoded-and-discarded, or truncated at ingest (spec
//! §7.1, §7.2); everything a view shows is derived from here (spec §8.1).
//! Frames on channels the consumer cannot decode are kept as opaque bytes,
//! alongside the rest (spec §7.2, §8.3) — the store never looks at a channel or
//! payload.
//!
//! A [`StoredStream`] is keyed in the [`Store`] by [`StreamId`] — node plus
//! lifetime — so a re-incarnated node does not append to its prior life (spec
//! §2.2, §7.1).

use std::collections::BTreeMap;

use super::frame::{Frame, Position, StreamId};

/// One node's reconstructed stream: its frames in position order, whole.
///
/// Backed by a position-keyed map so out-of-order arrivals land in order
/// and a position seen twice collapses to one (the carrier may not
/// fabricate content, spec §9.1). Gaps are not stored — they are *derived*
/// at read time from the positions that are present (spec §7.3, §8.1).
#[derive(Debug, Clone, Default)]
pub struct StoredStream {
    frames: BTreeMap<u64, Frame>,
}

impl StoredStream {
    /// An empty stream.
    pub fn new() -> Self {
        StoredStream::default()
    }

    /// Record a delivered frame. Idempotent by position: the first frame
    /// seen for a position wins and is never mutated (append-only,
    /// spec §7.2). Returns `true` if this was the first time the position
    /// was seen.
    pub fn record(&mut self, frame: Frame) -> bool {
        match self.frames.entry(frame.position.0) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(frame);
                true
            }
            std::collections::btree_map::Entry::Occupied(_) => false,
        }
    }

    /// The stored frames, in position order.
    pub fn frames(&self) -> impl Iterator<Item = &Frame> {
        self.frames.values()
    }

    /// The stored frames cloned into a vector, in position order. Handy for
    /// asserting against the reference model.
    pub fn to_vec(&self) -> Vec<Frame> {
        self.frames.values().cloned().collect()
    }

    /// How many frames are stored.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Whether the stream has no frames yet.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// The frame at an exact position, if stored.
    pub fn at(&self, position: Position) -> Option<&Frame> {
        self.frames.get(&position.0)
    }

    /// The **interior** gaps — runs of positions assigned between the first
    /// and last delivered frame but never delivered (spec §7.3), each as one
    /// [`GapSpan`].
    ///
    /// Cost is O(stored frames), never O(gap size): it walks adjacent stored
    /// positions and reads each span's endpoints from them, rather than
    /// enumerating the (possibly enormous) range in between. A stream that
    /// brackets a huge interior gap — what a long consumer outage produces
    /// (spec §6.3, §7.3), or a single wild position from a corrupt datagram —
    /// still surfaces in work proportional to the frames held, not to `u64::MAX`.
    ///
    /// Leading and trailing losses are not derivable from stored frames because
    /// no bracketing position exists. A position lost after the last delivered
    /// frame shows up as the stream simply ending (spec §5.7, §7.3), not a gap.
    pub fn gap_spans(&self) -> Vec<GapSpan> {
        let mut spans = Vec::new();
        let mut prev: Option<u64> = None;
        for &p in self.frames.keys() {
            if let Some(q) = prev
                && p > q + 1
            {
                spans.push(GapSpan {
                    start: q + 1,
                    end: p - 1,
                });
            }
            prev = Some(p);
        }
        spans
    }
}

/// A contiguous run of missing positions surfaced in a stored stream
/// (spec §7.3). Inclusive on both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapSpan {
    /// First missing position.
    pub start: u64,
    /// Last missing position.
    pub end: u64,
}

impl GapSpan {
    /// How many positions the gap spans (always at least one).
    pub fn count(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// All stored streams at the consumer, keyed by [`StreamId`] (spec §2.2, §7.1).
///
/// Two streams with the same node but different lifetime are distinct keys
/// and never merge.
#[derive(Debug, Clone, Default)]
pub struct Store {
    streams: BTreeMap<StreamId, StoredStream>,
}

impl Store {
    /// An empty store.
    pub fn new() -> Self {
        Store::default()
    }

    /// The stored stream for a node's life, if any frames have landed.
    pub fn stream(&self, id: &StreamId) -> Option<&StoredStream> {
        self.streams.get(id)
    }

    /// The stored stream for a node's life, creating an empty one if needed.
    pub fn stream_mut(&mut self, id: &StreamId) -> &mut StoredStream {
        self.streams.entry(id.clone()).or_default()
    }

    /// Every stream id the store holds, in a stable order.
    pub fn stream_ids(&self) -> impl Iterator<Item = &StreamId> {
        self.streams.keys()
    }

    /// How many distinct streams the store holds.
    pub fn len(&self) -> usize {
        self.streams.len()
    }

    /// Whether the store holds no streams.
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }
}
