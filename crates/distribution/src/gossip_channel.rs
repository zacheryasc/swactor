//! Generic gossip channel abstraction.
//!
//! Provides `GossipChannel` — a trait for any topic that wants to piggyback
//! on SWIM protocol messages — and `DisseminationBuffer<T>` — a reusable
//! budget-limited dissemination queue that replaces the 4 independent copies
//! of the `Λ * ceil(log₂(n))` pattern.

use serde::{Serialize, de::DeserializeOwned};

// ─── GossipChannel trait ────────────────────────────────────────────────────

/// A gossip channel that can piggyback serialized entries on SWIM messages.
///
/// Each channel has a unique topic tag and handles its own serialization.
/// The wire transport uses type-erased `Vec<u8>` entries.
pub trait GossipChannel: Send {
    /// Unique string tag identifying this channel in the piggyback payload.
    fn topic_tag(&self) -> &'static str;

    /// Take up to `max_entries` pending entries, serialized as bytes.
    fn take_pending_bytes(&mut self, max_entries: usize) -> Vec<Vec<u8>>;

    /// Apply incoming entries (deserialized from bytes) received from gossip.
    fn apply_incoming_bytes(&mut self, entries: &[Vec<u8>], cluster_size: usize);

    /// Re-enqueue all state for dissemination (anti-entropy on membership recovery).
    fn re_disseminate_all(&mut self, cluster_size: usize);

    /// Handle a node being declared dead.
    fn on_node_death(&mut self, node_id: &crate::types::NodeId);

    /// Periodic garbage collection tick.
    fn gc_tick(&mut self);
}

// ─── DisseminationBuffer<T> ────────────────────────────────────────────────

/// A queued entry with a remaining transmit budget.
#[derive(Debug, Clone)]
struct BufferEntry<T> {
    item: T,
    remaining: usize,
}

/// Reusable generic dissemination buffer.
///
/// Manages budget-limited gossip dissemination for any entry type.
/// Each entry is transmitted `Λ * ceil(log₂(n))` times before eviction.
#[derive(Debug)]
pub struct DisseminationBuffer<T> {
    entries: Vec<BufferEntry<T>>,
    lambda: usize,
}

impl<T: Clone> DisseminationBuffer<T> {
    /// Create a new buffer with the given dissemination multiplier (Λ).
    pub fn new(lambda: usize) -> Self {
        Self {
            entries: Vec::new(),
            lambda,
        }
    }

    /// Compute the transmit budget: `Λ * ceil(log₂(max(n, 2)))`.
    pub fn transmit_budget(&self, cluster_size: usize) -> usize {
        let n = cluster_size.max(2) as f64;
        let log_n = n.log2().ceil() as usize;
        self.lambda * log_n.max(1)
    }

    /// Enqueue an entry for dissemination. Does not check for duplicates.
    pub fn enqueue(&mut self, item: T, cluster_size: usize) {
        let budget = self.transmit_budget(cluster_size);
        self.entries.push(BufferEntry {
            item,
            remaining: budget,
        });
    }

    /// Enqueue an entry, replacing an existing one if `matcher` returns true.
    /// If no match is found, pushes a new entry.
    pub fn enqueue_or_replace<F>(&mut self, item: T, cluster_size: usize, matcher: F)
    where
        F: Fn(&T) -> bool,
    {
        let budget = self.transmit_budget(cluster_size);

        if let Some(existing) = self.entries.iter_mut().find(|e| matcher(&e.item)) {
            existing.item = item;
            existing.remaining = budget;
            return;
        }

        self.entries.push(BufferEntry {
            item,
            remaining: budget,
        });
    }

    /// Take up to `max_count` entries for piggyback.
    /// Decrements remaining budget and evicts exhausted entries.
    pub fn take(&mut self, max_count: usize) -> Vec<T> {
        let count = max_count.min(self.entries.len());
        let mut result = Vec::with_capacity(count);

        for entry in self.entries.iter_mut().take(count) {
            result.push(entry.item.clone());
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        self.entries.retain(|e| e.remaining > 0);
        result
    }

    /// Re-enqueue all given items with fresh budgets.
    pub fn re_enqueue_all(&mut self, items: impl IntoIterator<Item = T>, cluster_size: usize) {
        for item in items {
            self.enqueue(item, cluster_size);
        }
    }

    /// Retain only entries matching the predicate.
    pub fn retain<F>(&mut self, mut predicate: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.entries.retain(|e| predicate(&e.item));
    }

    /// Number of queued entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ─── Serialization helpers ─────────────────────────────────────────────────

/// Serialize a slice of items to a Vec of byte vectors.
pub fn serialize_each<T: Serialize>(items: &[T]) -> Vec<Vec<u8>> {
    items
        .iter()
        .filter_map(|item| serde_json::to_vec(item).ok())
        .collect()
}

/// Deserialize a slice of byte vectors into items, skipping failures.
pub fn deserialize_each<T: DeserializeOwned>(entries: &[Vec<u8>]) -> Vec<T> {
    entries
        .iter()
        .filter_map(|bytes| serde_json::from_slice(bytes).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_math_two_nodes() {
        let buf: DisseminationBuffer<u32> = DisseminationBuffer::new(3);
        // log2(2) = 1, ceil = 1, 3 * 1 = 3
        assert_eq!(buf.transmit_budget(2), 3);
    }

    #[test]
    fn budget_math_single_node_floors_to_two() {
        let buf: DisseminationBuffer<u32> = DisseminationBuffer::new(3);
        // cluster_size=1 → max(1,2)=2, log2(2)=1, 3*1=3
        assert_eq!(buf.transmit_budget(1), 3);
    }

    #[test]
    fn budget_math_32_nodes() {
        let buf: DisseminationBuffer<u32> = DisseminationBuffer::new(3);
        // log2(32) = 5, 3 * 5 = 15
        assert_eq!(buf.transmit_budget(32), 15);
    }

    #[test]
    fn budget_math_33_nodes() {
        let buf: DisseminationBuffer<u32> = DisseminationBuffer::new(3);
        // log2(33) ≈ 5.04, ceil = 6, 3 * 6 = 18
        assert_eq!(buf.transmit_budget(33), 18);
    }

    #[test]
    fn enqueue_take_evicts_after_budget() {
        let mut buf = DisseminationBuffer::new(3);
        buf.enqueue(42u32, 2); // budget = 3

        // Take 3 times — each take decrements once
        let r1 = buf.take(1);
        assert_eq!(r1, vec![42]);
        assert_eq!(buf.len(), 1);

        let r2 = buf.take(1);
        assert_eq!(r2, vec![42]);
        assert_eq!(buf.len(), 1);

        let r3 = buf.take(1);
        assert_eq!(r3, vec![42]);
        // After 3 takes, budget exhausted → evicted
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn enqueue_or_replace_replaces_matching_entry() {
        let mut buf = DisseminationBuffer::new(3);
        buf.enqueue_or_replace(("key", 1), 2, |e| e.0 == "key");
        buf.enqueue_or_replace(("key", 2), 2, |e| e.0 == "key");

        assert_eq!(buf.len(), 1);
        let taken = buf.take(1);
        assert_eq!(taken, vec![("key", 2)]);
    }

    #[test]
    fn enqueue_or_replace_adds_when_no_match() {
        let mut buf = DisseminationBuffer::new(3);
        buf.enqueue_or_replace(("a", 1), 2, |e| e.0 == "a");
        buf.enqueue_or_replace(("b", 2), 2, |e| e.0 == "b");

        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn re_enqueue_all_refreshes_budgets() {
        let mut buf = DisseminationBuffer::new(3);
        buf.enqueue(1u32, 2);
        buf.enqueue(2u32, 2);

        // Drain them
        for _ in 0..3 {
            buf.take(2);
        }
        assert!(buf.is_empty());

        // Re-enqueue
        buf.re_enqueue_all(vec![1, 2, 3], 2);
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn retain_removes_non_matching() {
        let mut buf = DisseminationBuffer::new(3);
        buf.enqueue(1u32, 2);
        buf.enqueue(2u32, 2);
        buf.enqueue(3u32, 2);

        buf.retain(|item| *item != 2);
        assert_eq!(buf.len(), 2);
        let taken = buf.take(3);
        assert_eq!(taken, vec![1, 3]);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let items = vec![1u32, 2, 3];
        let bytes = serialize_each(&items);
        let recovered: Vec<u32> = deserialize_each(&bytes);
        assert_eq!(recovered, items);
    }
}
