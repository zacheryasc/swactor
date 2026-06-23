#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(pub u64);
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortId(pub String);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectLayout {
    Token,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSpec {
    pub max_extent: u64,
    pub alignment: u64,
    pub layout: ObjectLayout,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectFailureReason {
    UnsupportedMagic,
    UnsupportedVersion,
    MalformedHeaderLength,
    ExtentExceedsMax,
    ExtentAlignmentViolation,
    SequenceViolation,
    EofBeforeFullPayload,
    DeviceCopyFailed,
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
        object_id: Option<ObjectId>,
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

#[derive(Clone, Debug)]
pub struct ObjectRecordBuilder {
    spec: ObjectSpec,
    object_id: ObjectId,
    sequence: u64,
    extent: u64,
    payload: Vec<u8>,
    magic: [u8; 4],
    version: u8,
    header_len: u8,
}

impl ObjectRecordBuilder {
    pub fn new(spec: ObjectSpec) -> Self {
        Self {
            spec,
            object_id: ObjectId(9000),
            sequence: 0,
            extent: 0,
            payload: Vec::new(),
            magic: *b"MO01",
            version: 1,
            header_len: HEADER_LEN as u8,
        }
    }

    pub fn object_id(mut self, object_id: ObjectId) -> Self {
        self.object_id = object_id;
        self
    }

    pub fn sequence(mut self, sequence: u64) -> Self {
        self.sequence = sequence;
        self
    }

    pub fn extent(mut self, extent: u64) -> Self {
        self.extent = extent;
        self
    }

    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    pub fn partial_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    pub fn unsupported_magic(mut self) -> Self {
        self.magic = *b"BAD!";
        self
    }

    pub fn unsupported_version(mut self) -> Self {
        self.version = 99;
        self
    }

    pub fn malformed_header_length(mut self) -> Self {
        self.header_len = 1;
        self
    }

    pub fn encode(mut self) -> Vec<u8> {
        if self.extent == 0 && !self.payload.is_empty() {
            self.extent = self.payload.len() as u64;
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.extend_from_slice(&self.magic);
        out.push(self.version);
        out.push(self.header_len);
        out.extend_from_slice(&[0u8; 2]);
        out.extend_from_slice(&self.object_id.0.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&self.extent.to_le_bytes());
        out.extend_from_slice(&self.spec.max_extent.to_le_bytes());
        out.extend_from_slice(&self.spec.alignment.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

const HEADER_LEN: usize = 48;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedRecord {
    object_id: ObjectId,
    sequence: u64,
    extent: u64,
    total_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingObject {
    record: ParsedRecord,
    copy_done: bool,
    handle: Option<DeviceHandle>,
}

#[cfg(test)]
pub struct IngressParserHarness {
    generation: WorkerGeneration,
    install: Option<InstallRing>,
    buffers: std::collections::BTreeMap<RingId, Vec<u8>>,
    consume: std::collections::BTreeMap<RingId, u64>,
    cursor_reload: std::collections::BTreeMap<RingId, u64>,
    faulted_rings: std::collections::BTreeSet<RingId>,
    expected_sequence: u64,
    pending: std::collections::BTreeMap<ObjectId, PendingObject>,
    copy_log: Vec<DeviceCopyLog>,
    events: Vec<WorkerIngressOut>,
}

#[cfg(test)]
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
                if let Some(pending) = self.pending.get_mut(&object_id) {
                    pending.copy_done = true;
                    if byte_count == pending.record.extent {
                        if let Some(install) = &self.install {
                            *self.consume.entry(install.ring_id).or_insert(0) +=
                                pending.record.total_len as u64;
                        }
                    }
                }
                self.maybe_loaded(object_id);
            }
            WorkerIngressEvent::DeviceHandleCreated { object_id, handle } => {
                if let Some(pending) = self.pending.get_mut(&object_id) {
                    pending.handle = Some(handle);
                }
                self.maybe_loaded(object_id);
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
        match decode_record(&buffer, install.object_spec, eof) {
            Ok(record) => {
                if record.sequence != self.expected_sequence {
                    self.events.push(WorkerIngressOut::ObjectFailed {
                        ring_id,
                        object_id: Some(record.object_id),
                        reason: ObjectFailureReason::SequenceViolation,
                    });
                    return;
                }
                self.expected_sequence += 1;
                self.pending.insert(
                    record.object_id,
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
            Err(reason) => self.events.push(WorkerIngressOut::ObjectFailed {
                ring_id,
                object_id: None,
                reason,
            }),
        }
    }

    fn maybe_loaded(&mut self, object_id: ObjectId) {
        let Some(pending) = self.pending.get(&object_id).cloned() else {
            return;
        };
        let Some(handle) = pending.handle else {
            return;
        };
        if !pending.copy_done || handle.generation != self.generation {
            return;
        }
        let install = self.install.as_ref().unwrap();
        if !self.events.iter().any(|event| matches!(event, WorkerIngressOut::ObjectLoaded { object_id: seen, .. } if *seen == object_id)) {
            self.events.push(WorkerIngressOut::ObjectLoaded {
                ring_id: install.ring_id,
                edge_id: install.edge_id,
                port_id: install.port_id.clone(),
                object_id,
                sequence: pending.record.sequence,
                extent: pending.record.extent,
                handle,
            });
        }
    }
}

fn decode_record(
    bytes: &[u8],
    spec: ObjectSpec,
    eof: bool,
) -> Result<ParsedRecord, ObjectFailureReason> {
    if bytes.len() < HEADER_LEN {
        return if eof {
            Err(ObjectFailureReason::EofBeforeFullPayload)
        } else {
            Err(ObjectFailureReason::MalformedHeaderLength)
        };
    }
    if &bytes[0..4] != b"MO01" {
        return Err(ObjectFailureReason::UnsupportedMagic);
    }
    if bytes[4] != 1 {
        return Err(ObjectFailureReason::UnsupportedVersion);
    }
    if bytes[5] as usize != HEADER_LEN {
        return Err(ObjectFailureReason::MalformedHeaderLength);
    }
    let object_id = ObjectId(u64::from_le_bytes(bytes[8..16].try_into().unwrap()));
    let sequence = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let extent = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    if extent > spec.max_extent {
        return Err(ObjectFailureReason::ExtentExceedsMax);
    }
    if spec.alignment != 0 && extent % spec.alignment != 0 {
        return Err(ObjectFailureReason::ExtentAlignmentViolation);
    }
    let total_len = HEADER_LEN + extent as usize;
    if bytes.len() < total_len {
        return if eof {
            Err(ObjectFailureReason::EofBeforeFullPayload)
        } else {
            Err(ObjectFailureReason::MalformedHeaderLength)
        };
    }
    Ok(ParsedRecord {
        object_id,
        sequence,
        extent,
        total_len,
    })
}
