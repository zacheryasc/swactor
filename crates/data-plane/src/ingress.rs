#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortId(pub String);

pub use crate::object_record::{
    FLAG_BEGIN_SEQUENCE, FLAG_END_OF_SEQUENCE, HEADER_LEN, KNOWN_FLAGS_MASK, OBJECT_MAGIC,
    OBJECT_MAGIC_BYTES, OBJECT_VERSION, ObjectFailureReason, ObjectFlags, ObjectHeader, ObjectId,
    ObjectLayout, ObjectRecord, ObjectRecordBuilder, ObjectRecordRead, ObjectSpec,
    read_object_record,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub edge_id: EdgeId,
    pub object_id: ObjectId,
}

impl ObjectKey {
    pub fn new(edge_id: EdgeId, object_id: ObjectId) -> Self {
        Self { edge_id, object_id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceHandle {
    pub generation: WorkerGeneration,
    pub id: u64,
}

impl DeviceHandle {
    pub fn new(generation: WorkerGeneration, id: u64) -> Self {
        Self { generation, id }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingDirection {
    Ingress,
    Egress,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallRing {
    pub ring_id: RingId,
    pub edge_id: EdgeId,
    pub port_id: PortId,
    pub direction: RingDirection,
    pub object_spec: ObjectSpec,
    pub generation: WorkerGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerIngressEvent {
    InstallRing(InstallRing),
    RingReadable {
        ring_id: RingId,
    },
    Eof {
        ring_id: RingId,
    },
    DeviceCopyCompleted {
        object_id: ObjectId,
        byte_count: u64,
    },
    DeviceHandleCreated {
        object_id: ObjectId,
        handle: DeviceHandle,
    },
    RingFault {
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerIngressOut {
    ObjectLoaded {
        ring_id: RingId,
        edge_id: EdgeId,
        port_id: PortId,
        object_id: ObjectId,
        sequence: u64,
        extent: u64,
        handle: DeviceHandle,
    },
    ObjectFailed {
        ring_id: RingId,
        edge_id: EdgeId,
        port_id: PortId,
        object_id: Option<ObjectId>,
        sequence: Option<u64>,
        reason: ObjectFailureReason,
    },
    RingFault {
        ring_id: RingId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceCopyLog {
    pub object_id: ObjectId,
    pub byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingObject {
    record: ObjectRecord,
    copy_done: bool,
    handle: Option<DeviceHandle>,
}

pub struct IngressParserHarness {
    generation: WorkerGeneration,
    install: Option<InstallRing>,
    buffers: std::collections::BTreeMap<RingId, Vec<u8>>,
    consume: std::collections::BTreeMap<RingId, u64>,
    cursor_reload: std::collections::BTreeMap<RingId, u64>,
    faulted_rings: std::collections::BTreeSet<RingId>,
    expected_sequence: u64,
    pending: std::collections::BTreeMap<ObjectKey, PendingObject>,
    copy_log: Vec<DeviceCopyLog>,
    events: Vec<WorkerIngressOut>,
}

impl IngressParserHarness {
    pub fn new(generation: WorkerGeneration) -> Self {
        Self {
            generation,
            install: None,
            buffers: std::collections::BTreeMap::new(),
            consume: std::collections::BTreeMap::new(),
            cursor_reload: std::collections::BTreeMap::new(),
            faulted_rings: std::collections::BTreeSet::new(),
            expected_sequence: 0,
            pending: std::collections::BTreeMap::new(),
            copy_log: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn observe(&mut self, event: WorkerIngressEvent) {
        match event {
            WorkerIngressEvent::InstallRing(install) => {
                if install.direction == RingDirection::Ingress
                    && install.generation == self.generation
                {
                    self.consume.entry(install.ring_id).or_insert(0);
                    self.install = Some(install);
                }
            }
            WorkerIngressEvent::RingReadable { ring_id } => self.parse_ring(ring_id, false),
            WorkerIngressEvent::Eof { ring_id } => self.parse_ring(ring_id, true),
            WorkerIngressEvent::DeviceCopyCompleted {
                object_id,
                byte_count,
            } => {
                self.copy_log.push(DeviceCopyLog {
                    object_id,
                    byte_count,
                });
                if let Some(key) = self.object_key(object_id) {
                    if let Some(pending) = self.pending.get_mut(&key) {
                        pending.copy_done = true;
                        if byte_count == pending.record.extent {
                            if let Some(install) = &self.install {
                                *self.consume.entry(install.ring_id).or_insert(0) +=
                                    pending.record.total_len as u64;
                            }
                        }
                    }
                    self.maybe_loaded(key);
                }
            }
            WorkerIngressEvent::DeviceHandleCreated { object_id, handle } => {
                if let Some(key) = self.object_key(object_id) {
                    if let Some(pending) = self.pending.get_mut(&key) {
                        pending.handle = Some(handle);
                    }
                    self.maybe_loaded(key);
                }
            }
            WorkerIngressEvent::RingFault { ring_id } => {
                self.faulted_rings.insert(ring_id);
                self.events.push(WorkerIngressOut::RingFault { ring_id });
            }
        }
    }

    pub fn write_committed_bytes(&mut self, ring_id: RingId, bytes: Vec<u8>) {
        self.buffers.entry(ring_id).or_default().extend(bytes);
    }

    pub fn write_uncommitted_bytes(&mut self, _ring_id: RingId, _bytes: Vec<u8>) {}

    pub fn consume_cursor(&self, ring_id: RingId) -> u64 {
        self.consume.get(&ring_id).copied().unwrap_or(0)
    }

    pub fn cursor_reload_count(&self, ring_id: RingId) -> u64 {
        self.cursor_reload.get(&ring_id).copied().unwrap_or(0)
    }

    pub fn device_copy_log(&self) -> &[DeviceCopyLog] {
        &self.copy_log
    }

    pub fn events(&self) -> &[WorkerIngressOut] {
        &self.events
    }

    fn parse_ring(&mut self, ring_id: RingId, eof: bool) {
        let Some(install) = &self.install else {
            return;
        };
        if install.ring_id != ring_id || self.faulted_rings.contains(&ring_id) {
            return;
        }
        *self.cursor_reload.entry(ring_id).or_insert(0) += 1;
        let buffer = self.buffers.get(&ring_id).cloned().unwrap_or_default();
        if buffer.is_empty() {
            return;
        }
        match read_object_record(&buffer, install.object_spec, eof) {
            Ok(ObjectRecordRead::Complete(record)) => {
                if record.sequence != self.expected_sequence {
                    self.events.push(WorkerIngressOut::ObjectFailed {
                        ring_id,
                        edge_id: install.edge_id,
                        port_id: install.port_id.clone(),
                        object_id: Some(record.object_id),
                        sequence: Some(record.sequence),
                        reason: ObjectFailureReason::SequenceViolation,
                    });
                    return;
                }
                self.expected_sequence += 1;
                self.pending.insert(
                    ObjectKey::new(install.edge_id, record.object_id),
                    PendingObject {
                        record: record.clone(),
                        copy_done: false,
                        handle: None,
                    },
                );
                self.copy_log.push(DeviceCopyLog {
                    object_id: record.object_id,
                    byte_count: record.extent,
                });
            }
            Ok(ObjectRecordRead::Incomplete) => {}
            Err(reason) => {
                let (object_id, sequence) = object_failure_metadata(&buffer, reason);
                self.events.push(WorkerIngressOut::ObjectFailed {
                    ring_id,
                    edge_id: install.edge_id,
                    port_id: install.port_id.clone(),
                    object_id,
                    sequence,
                    reason,
                });
            }
        }
    }

    fn object_key(&self, object_id: ObjectId) -> Option<ObjectKey> {
        self.install
            .as_ref()
            .map(|install| ObjectKey::new(install.edge_id, object_id))
    }

    fn maybe_loaded(&mut self, key: ObjectKey) {
        let Some(pending) = self.pending.get(&key).cloned() else {
            return;
        };
        let Some(handle) = pending.handle else {
            return;
        };
        if !pending.copy_done || handle.generation != self.generation {
            return;
        }
        let install = self.install.as_ref().unwrap();
        if !self.events.iter().any(|event| matches!(event, WorkerIngressOut::ObjectLoaded { edge_id, object_id, .. } if *edge_id == key.edge_id && *object_id == key.object_id)) {
            self.events.push(WorkerIngressOut::ObjectLoaded {
                ring_id: install.ring_id,
                edge_id: install.edge_id,
                port_id: install.port_id.clone(),
                object_id: key.object_id,
                sequence: pending.record.sequence,
                extent: pending.record.extent,
                handle,
            });
        }
    }
}

fn object_failure_metadata(
    bytes: &[u8],
    reason: ObjectFailureReason,
) -> (Option<ObjectId>, Option<u64>) {
    if bytes.len() < HEADER_LEN
        || matches!(
            reason,
            ObjectFailureReason::UnsupportedMagic
                | ObjectFailureReason::UnsupportedVersion
                | ObjectFailureReason::MalformedHeaderLength
        )
    {
        return (None, None);
    }
    (
        Some(ObjectId(u64::from_le_bytes(
            bytes[8..16].try_into().unwrap(),
        ))),
        Some(u64::from_le_bytes(bytes[16..24].try_into().unwrap())),
    )
}
