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

pub const HEADER_LEN: usize = 40;
pub const OBJECT_MAGIC_BYTES: [u8; 4] = *b"MO01";
pub const OBJECT_MAGIC: u32 = u32::from_le_bytes(OBJECT_MAGIC_BYTES);
pub const OBJECT_VERSION: u16 = 1;
pub const FLAG_END_OF_SEQUENCE: u32 = 1;
pub const KNOWN_FLAGS_MASK: u32 = FLAG_END_OF_SEQUENCE;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ObjectFlags {
    pub end_of_sequence: bool,
}

impl ObjectFlags {
    pub fn bits(self) -> u32 {
        if self.end_of_sequence {
            FLAG_END_OF_SEQUENCE
        } else {
            0
        }
    }

    pub fn from_bits(bits: u32) -> Option<Self> {
        if bits & !KNOWN_FLAGS_MASK != 0 {
            return None;
        }
        Some(Self {
            end_of_sequence: bits & FLAG_END_OF_SEQUENCE != 0,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHeader {
    pub object_id: ObjectId,
    pub sequence: u64,
    pub extent: u64,
    pub flags: ObjectFlags,
}

impl ObjectHeader {
    pub fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN);
        out.extend_from_slice(&OBJECT_MAGIC.to_le_bytes());
        out.extend_from_slice(&OBJECT_VERSION.to_le_bytes());
        out.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        out.extend_from_slice(&self.object_id.0.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&self.extent.to_le_bytes());
        out.extend_from_slice(&self.flags.bits().to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ObjectFailureReason> {
        if bytes.len() < HEADER_LEN {
            return Err(ObjectFailureReason::EofBeforeFullPayload);
        }
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != OBJECT_MAGIC {
            return Err(ObjectFailureReason::UnsupportedMagic);
        }
        if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != OBJECT_VERSION {
            return Err(ObjectFailureReason::UnsupportedVersion);
        }
        if u16::from_le_bytes(bytes[6..8].try_into().unwrap()) as usize != HEADER_LEN {
            return Err(ObjectFailureReason::MalformedHeaderLength);
        }
        let flags = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
        if u32::from_le_bytes(bytes[36..40].try_into().unwrap()) != 0 {
            return Err(ObjectFailureReason::MalformedHeader);
        }
        let Some(flags) = ObjectFlags::from_bits(flags) else {
            return Err(ObjectFailureReason::MalformedHeader);
        };
        Ok(Self {
            object_id: ObjectId(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
            sequence: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            extent: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            flags,
        })
    }
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
    MalformedHeader,
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

#[derive(Clone, Debug)]
pub struct ObjectRecordBuilder {
    spec: ObjectSpec,
    object_id: ObjectId,
    sequence: u64,
    extent: u64,
    payload: Vec<u8>,
    magic: u32,
    version: u16,
    header_len: u16,
    flags: ObjectFlags,
}

impl ObjectRecordBuilder {
    pub fn new(spec: ObjectSpec) -> Self {
        Self {
            spec,
            object_id: ObjectId(9000),
            sequence: 0,
            extent: 0,
            payload: Vec::new(),
            magic: OBJECT_MAGIC,
            version: OBJECT_VERSION,
            header_len: HEADER_LEN as u16,
            flags: ObjectFlags::default(),
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
        self.magic = u32::from_le_bytes(*b"BAD!");
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

    pub fn flags(mut self, flags: ObjectFlags) -> Self {
        self.flags = flags;
        self
    }

    pub fn encode(mut self) -> Vec<u8> {
        if self.extent == 0 && !self.payload.is_empty() {
            self.extent = self.payload.len() as u64;
        }
        let mut out = ObjectHeader {
            object_id: self.object_id,
            sequence: self.sequence,
            extent: self.extent,
            flags: self.flags,
        }
        .encode();
        if self.magic != OBJECT_MAGIC {
            out[0..4].copy_from_slice(&self.magic.to_le_bytes());
        }
        if self.version != OBJECT_VERSION {
            out[4..6].copy_from_slice(&self.version.to_le_bytes());
        }
        if self.header_len as usize != HEADER_LEN {
            out[6..8].copy_from_slice(&self.header_len.to_le_bytes());
        }
        out.extend_from_slice(&self.payload);
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRecord {
    pub object_id: ObjectId,
    pub sequence: u64,
    pub extent: u64,
    pub flags: ObjectFlags,
    pub total_len: usize,
}

impl ObjectRecord {
    pub fn payload<'a>(&self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        bytes.get(HEADER_LEN..self.total_len)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectRecordRead {
    Incomplete,
    Complete(ObjectRecord),
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingObject {
    record: ObjectRecord,
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
    pending: std::collections::BTreeMap<ObjectKey, PendingObject>,
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

pub fn read_object_record(
    bytes: &[u8],
    spec: ObjectSpec,
    eof: bool,
) -> Result<ObjectRecordRead, ObjectFailureReason> {
    if bytes.len() < HEADER_LEN {
        return if eof && !bytes.is_empty() {
            Err(ObjectFailureReason::EofBeforeFullPayload)
        } else {
            Ok(ObjectRecordRead::Incomplete)
        };
    }
    let header = ObjectHeader::decode(bytes)?;
    if header.extent > spec.max_extent {
        return Err(ObjectFailureReason::ExtentExceedsMax);
    }
    if spec.alignment != 0 && header.extent % spec.alignment != 0 {
        return Err(ObjectFailureReason::ExtentAlignmentViolation);
    }
    let total_len = HEADER_LEN + header.extent as usize;
    if bytes.len() < total_len {
        return if eof {
            Err(ObjectFailureReason::EofBeforeFullPayload)
        } else {
            Ok(ObjectRecordRead::Incomplete)
        };
    }
    Ok(ObjectRecordRead::Complete(ObjectRecord {
        object_id: header.object_id,
        sequence: header.sequence,
        extent: header.extent,
        flags: header.flags,
        total_len,
    }))
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
