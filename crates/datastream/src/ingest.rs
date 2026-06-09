//! Consumer ingest: reconstruct each node's stream from deliveries (spec
//! §8.1).
//!
//! The consumer receives frames from many nodes, in any order, some never
//! arriving, and reconstructs each node's stream — keyed by stream id,
//! ordered by position — into the [`Store`]. Ingest is deliberately thin:
//! it routes a delivery to its stream and records the frame whole. It MUST
//! NOT thin, aggregate, decode-and-discard, or truncate (spec §8.2), and it
//! never inspects a channel or a payload, so a channel it cannot decode is
//! retained exactly like any other (spec §8.3).
//!
//! Reconstruction is by position, not arrival: out-of-order deliveries land
//! in order in the store, and a position delivered twice collapses to one
//! (spec §7.5). Two lives of one node are different stream ids and never
//! merge (spec §8.4).

use super::store::Store;
use super::transport::Delivery;

/// The single consumer toward which all telemetry flows (spec §2). It owns
/// the stored streams and grows them as deliveries arrive.
#[derive(Debug, Default)]
pub struct Consumer {
    store: Store,
}

impl Consumer {
    /// A consumer with an empty store.
    pub fn new() -> Self {
        Consumer::default()
    }

    /// Accept one delivery: route it to its stream and record the frame.
    /// Returns `true` if the frame was new (a duplicate position is
    /// ignored, keeping the first — the carrier cannot fabricate content,
    /// spec §9).
    pub fn accept(&mut self, delivery: Delivery) -> bool {
        let Delivery { stream, frame } = delivery;
        self.store.stream_mut(&stream).record(frame)
    }

    /// Accept a batch of deliveries, in whatever order they arrive.
    pub fn ingest(&mut self, deliveries: impl IntoIterator<Item = Delivery>) {
        for delivery in deliveries {
            self.accept(delivery);
        }
    }

    /// The stored streams — the source of truth for every view (spec §8.2).
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Consume the consumer, yielding its store.
    pub fn into_store(self) -> Store {
        self.store
    }
}
