//! Reusable MO01 object-record framing and validation contracts.

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

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
pub const FLAG_BEGIN_SEQUENCE: u32 = 1 << 1;
pub const KNOWN_FLAGS_MASK: u32 = FLAG_END_OF_SEQUENCE | FLAG_BEGIN_SEQUENCE;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ObjectFlags {
    pub end_of_sequence: bool,
    pub begin_sequence: bool,
}

impl ObjectFlags {
    pub fn bits(self) -> u32 {
        let mut bits = 0;
        if self.end_of_sequence {
            bits |= FLAG_END_OF_SEQUENCE;
        }
        if self.begin_sequence {
            bits |= FLAG_BEGIN_SEQUENCE;
        }
        bits
    }

    pub fn from_bits(bits: u32) -> Option<Self> {
        if bits & !KNOWN_FLAGS_MASK != 0 {
            return None;
        }
        Some(Self {
            end_of_sequence: bits & FLAG_END_OF_SEQUENCE != 0,
            begin_sequence: bits & FLAG_BEGIN_SEQUENCE != 0,
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

#[derive(Clone, Debug)]
pub struct ObjectRecordBuilder {
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
    pub fn new(_spec: ObjectSpec) -> Self {
        Self {
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
