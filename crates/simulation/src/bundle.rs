//! Bundle-writer interface and record types (SIM_SPEC §3.2 / §9.2).
//!
//! This module defines only the *writer trait* and the record shapes
//! that cross the engine→writer boundary. The on-disk layout of §9
//! lives behind that trait in a later implementation.

use crate::network::{CacheTransition, DropReason};
use crate::scenario::Mutation;

/// One record the engine produces. The writer is append-only and may
/// reorder for storage but never invent or omit records.
#[derive(Debug, Clone, PartialEq)]
pub enum BundleRecord {
    /// Host-emitted or engine-synthesized event.
    Event(EventRecord),
    /// One snapshot result.
    Snapshot(SnapshotRecord),
    /// A mutation pop. Includes a clone of the mutation the engine ran.
    Mutation(MutationRecord),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    pub virtual_time_ns: u64,
    pub host_id: Option<String>,
    pub kind_tag: String,
    pub event: EventPayload,
}

/// Either a host-emitted opaque payload or one of the engine-
/// synthesized records §4.7 names. The structured form lets tests and
/// the §9 schema check observe engine-synthesized events without
/// peering into opaque bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum EventPayload {
    Bytes(Vec<u8>),
    DropOnSend {
        from: String,
        to: String,
        reason: DropReason,
    },
    DropOnDelivery {
        to: String,
        reason: DeliveryDropReason,
    },
    CacheStateChange {
        from: String,
        to: String,
        transition: CacheTransition,
    },
    DialStart {
        from: String,
        to: String,
    },
    DialOutcome {
        from: String,
        to: String,
        warm: bool,
    },
}

/// Why the engine dropped a delivery at arrival time. `Partition` is
/// emitted when a network partition mutation invalidates an
/// in-flight message; `HostKilled` when a `PeerKill` mutation does;
/// `HostHalted` when the destination has been removed from the host
/// table (the §4.4 "host gone" fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryDropReason {
    HostHalted,
    HostKilled,
    Partition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRecord {
    pub virtual_time_ns: u64,
    pub host_id: String,
    pub kind_tag: String,
    pub snapshot: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationRecord {
    pub virtual_time_ns: u64,
    pub mutation: Mutation,
}

/// The engine writes records here. The MVP ships an in-memory
/// `VecWriter` so the engine can be exercised end-to-end without
/// touching disk; the §9 file-format writer arrives later.
pub trait BundleWriter {
    fn write(&mut self, record: BundleRecord);
}

/// In-memory writer that captures every record. Useful for engine
/// tests and as a stand-in until the §9 file writer lands.
#[derive(Debug, Default)]
pub struct VecWriter {
    pub records: Vec<BundleRecord>,
}

impl BundleWriter for VecWriter {
    fn write(&mut self, record: BundleRecord) {
        self.records.push(record);
    }
}
