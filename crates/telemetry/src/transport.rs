//! Legacy transport/test seam for carrying positioned frames into ingest.
//!
//! The live endpoint path now fans out catalog-aware [`TelemetryEvent`] values;
//! this module keeps the older [`Delivery`] shape used by ingest, storage tests,
//! and scripted conformance checks.
//!
//! A [`Delivery`] pairs the producing stream id with one frame. A real carrier
//! rides connections the system already maintains; tests can replace it with
//! [`ScriptedTransport`], whose faults stay inside the transport envelope: it
//! may *deliver*, *drop*, *reorder*, or *delay*, and it MUST NOT corrupt a
//! payload, fabricate a frame, or alter a position.
//!
//! Under position-ordering a *delay* is indistinguishable from a *reorder*:
//! a delayed frame simply arrives later. Everything the scripted carrier
//! produces is a reordered subsequence of what was sent — never a superset and
//! never a mutation.

use std::collections::BTreeSet;

use super::frame::{Frame, StreamId};

/// A frame as the legacy transport/ingest seam receives it: tagged with the
/// stream it belongs to.
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

/// How the surviving frames of a stream are reordered on arrival. This is the
/// envelope's *reorder* and *delay* axis (spec §6.3, §9.1); each variant is a
/// permutation of the survivors, never adding or dropping.
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

/// The faults a scripted carrier applies to one stream (spec §6.3, §9.1
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
        StreamScript {
            dropped: positions.into_iter().collect(),
            reorder: Reorder::InOrder,
        }
    }

    /// Set the reorder behavior (builder style).
    pub fn with_reorder(mut self, reorder: Reorder) -> Self {
        self.reorder = reorder;
        self
    }
}

/// A scripted, in-process transport for conformance scenarios (spec §9.1). It
/// is a pure, deterministic transform from what a node sent to what the
/// consumer receives, replacing real wire behavior with a fast test script.
pub struct ScriptedTransport;

impl ScriptedTransport {
    /// Carry one node's sent frames to the consumer under `script`,
    /// returning the deliveries in arrival order. Dropped positions are
    /// removed; the survivors are reordered; nothing else changes.
    pub fn carry(stream: &StreamId, sent: &[Frame], script: &StreamScript) -> Vec<Delivery> {
        let survivors: Vec<Frame> = sent
            .iter()
            .filter(|f| !script.dropped.contains(&f.position.0))
            .cloned()
            .collect();
        let ordered = reorder(survivors, &script.reorder);
        ordered
            .into_iter()
            .map(|frame| Delivery::new(stream.clone(), frame))
            .collect()
    }

    /// Carry several nodes' streams and interleave their deliveries in a
    /// fixed round-robin, the way one wire would multiplex many senders.
    /// Cross-node order is meaningless (spec §4.3); this just proves ingest
    /// routes by stream id, not by arrival.
    pub fn carry_all(streams: &[(StreamId, Vec<Frame>, StreamScript)]) -> Vec<Delivery> {
        let per_stream: Vec<Vec<Delivery>> = streams
            .iter()
            .map(|(id, sent, script)| Self::carry(id, sent, script))
            .collect();
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
