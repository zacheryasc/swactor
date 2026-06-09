//! Transport: best-effort carriage of a node's stream to the one consumer
//! (spec §7), and a scripted in-process carrier for offline tests (testing
//! spec §2, §9).
//!
//! A [`Delivery`] is the value on the transport→ingest seam: which stream a
//! frame belongs to, and the frame. A real carrier rides the connections
//! the system already maintains (spec §7.1); a test replaces it with the
//! [`ScriptedTransport`] here, whose faults are chosen by the scenario and
//! stay inside the **envelope** (testing spec §9): a carrier may *deliver*,
//! *drop*, *reorder*, or *delay*, and it MUST NOT corrupt a payload,
//! fabricate a frame, or alter a position.
//!
//! Under position-ordering a *delay* is indistinguishable from a *reorder*
//! (a delayed frame simply arrives later), so the envelope's delay is
//! covered by [`Reorder`]. Everything the scripted carrier produces is a
//! reordered subsequence of what was sent — never a superset, never a
//! mutation — which is exactly the property the real-transport conformance
//! check pins (testing spec §9).

use std::collections::BTreeSet;

use super::frame::{Frame, StreamId};

/// A frame as the consumer receives it from the transport (testing spec §2
/// seam): tagged with the stream it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Which node's life produced the frame (spec §8.1 ingest key).
    pub stream: StreamId,
    /// The carried frame, its payload and position untouched.
    pub frame: Frame,
}

impl Delivery {
    /// Pair a stream id with a frame.
    pub fn new(stream: StreamId, frame: Frame) -> Self {
        Delivery { stream, frame }
    }
}

/// How the surviving frames of a stream are reordered on arrival. This is
/// the envelope's *reorder* (and *delay*) axis (testing spec §9); each
/// variant is a permutation of the survivors, never adding or dropping.
#[derive(Debug, Clone, Default)]
pub enum Reorder {
    /// Delivered in the order sent.
    #[default]
    InOrder,
    /// Delivered fully reversed — the deepest reorder a stream admits.
    Reversed,
    /// Each consecutive run of `width` frames is reversed, bounding how far
    /// out of order any frame can arrive (reorder depth ≤ `width`).
    Windows(usize),
    /// An explicit permutation: `delivered[i] = survivors[indices[i]]`.
    /// `indices` should be a permutation of `0..survivors.len()`; entries
    /// out of range are skipped so a mis-authored vector cannot panic.
    Permutation(Vec<usize>),
}

/// The faults a scripted carrier applies to one stream (testing spec §9
/// envelope). Drops and reorders only — payloads and positions are never
/// touched.
#[derive(Debug, Clone, Default)]
pub struct StreamScript {
    /// Positions the carrier never delivers. This one set expresses a
    /// single drop, a total-loss span, and a consumer outage alike — every
    /// position produced during the loss is simply listed here.
    pub dropped: BTreeSet<u64>,
    /// How the surviving frames are reordered on arrival.
    pub reorder: Reorder,
}

impl StreamScript {
    /// A clean carrier: deliver everything, in order.
    pub fn perfect() -> Self {
        StreamScript::default()
    }

    /// Drop exactly these positions, otherwise deliver in order.
    pub fn dropping(positions: impl IntoIterator<Item = u64>) -> Self {
        StreamScript { dropped: positions.into_iter().collect(), reorder: Reorder::InOrder }
    }

    /// Set the reorder behavior (builder style).
    pub fn with_reorder(mut self, reorder: Reorder) -> Self {
        self.reorder = reorder;
        self
    }
}

/// A scripted, in-process transport (testing spec §2). It is a pure,
/// deterministic transform from what a node *sent* to what the consumer is
/// *delivered* — the entanglement of real wires replaced by a script so a
/// run completes in microseconds and returns the same result every time.
pub struct ScriptedTransport;

impl ScriptedTransport {
    /// Carry one node's sent frames to the consumer under `script`,
    /// returning the deliveries in arrival order. Dropped positions are
    /// removed; the survivors are reordered; nothing else changes.
    pub fn carry(stream: &StreamId, sent: &[Frame], script: &StreamScript) -> Vec<Delivery> {
        let survivors: Vec<Frame> =
            sent.iter().filter(|f| !script.dropped.contains(&f.position.0)).cloned().collect();
        let ordered = reorder(survivors, &script.reorder);
        ordered.into_iter().map(|frame| Delivery::new(stream.clone(), frame)).collect()
    }

    /// Carry several nodes' streams and interleave their deliveries in a
    /// fixed round-robin, the way one wire would multiplex many senders.
    /// Cross-node order is meaningless (spec §4.3); this just proves ingest
    /// routes by stream id, not by arrival.
    pub fn carry_all(streams: &[(StreamId, Vec<Frame>, StreamScript)]) -> Vec<Delivery> {
        let per_stream: Vec<Vec<Delivery>> =
            streams.iter().map(|(id, sent, script)| Self::carry(id, sent, script)).collect();
        round_robin(per_stream)
    }
}

fn reorder(mut survivors: Vec<Frame>, reorder: &Reorder) -> Vec<Frame> {
    match reorder {
        Reorder::InOrder => survivors,
        Reorder::Reversed => {
            survivors.reverse();
            survivors
        }
        Reorder::Windows(width) => {
            let width = (*width).max(1);
            let mut out = Vec::with_capacity(survivors.len());
            for chunk in survivors.chunks(width) {
                out.extend(chunk.iter().rev().cloned());
            }
            out
        }
        Reorder::Permutation(indices) => indices
            .iter()
            .filter_map(|&i| survivors.get(i).cloned())
            .collect(),
    }
}

fn round_robin(mut lists: Vec<Vec<Delivery>>) -> Vec<Delivery> {
    // Reverse each so we can pop from the back cheaply while preserving the
    // per-stream arrival order.
    for list in &mut lists {
        list.reverse();
    }
    let mut out = Vec::new();
    let mut any = true;
    while any {
        any = false;
        for list in &mut lists {
            if let Some(d) = list.pop() {
                out.push(d);
                any = true;
            }
        }
    }
    out
}
