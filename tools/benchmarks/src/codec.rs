use std::any::{Any, TypeId};

use crate::{WorkUnits, Workload};
use serde::{Deserialize, Serialize};
use swactor::Error;
use swactor::actor::ActorAddress;
use swactor_transport::{
    Codec, CodecRegistrationError, CodecRegistry, JsonCodec, NetworkMessage, WireEnvelope,
};

const PAYLOAD_SIZES: [usize; 3] = [32, 1024, 65_536];

#[derive(Clone, Debug, PartialEq, Eq)]
struct FixedMessage(u64);

impl NetworkMessage for FixedMessage {
    fn type_tag() -> &'static str {
        "bench::FixedMessage"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BytesMessage(Vec<u8>);

impl NetworkMessage for BytesMessage {
    fn type_tag() -> &'static str {
        "bench::BytesMessage"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StructuredMessage {
    id: u64,
    name: String,
    fields: Vec<String>,
    nested: Vec<Vec<u64>>,
}

impl NetworkMessage for StructuredMessage {
    fn type_tag() -> &'static str {
        "bench::StructuredMessage"
    }
}

#[derive(Clone, Copy)]
struct FixedCodec;

impl Codec<FixedMessage> for FixedCodec {
    fn encode(&self, message: &FixedMessage) -> Result<Vec<u8>, Error> {
        Ok(message.0.to_le_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<FixedMessage, Error> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::from("fixed benchmark payload must contain eight bytes"))?;
        Ok(FixedMessage(u64::from_le_bytes(bytes)))
    }
}

#[derive(Clone, Copy)]
struct BytesCodec;

impl Codec<BytesMessage> for BytesCodec {
    fn encode(&self, message: &BytesMessage) -> Result<Vec<u8>, Error> {
        Ok(message.0.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<BytesMessage, Error> {
        Ok(BytesMessage(bytes.to_vec()))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Format {
    Fixed,
    Bytes,
    JsonFlat,
    JsonNested,
}

impl Format {
    fn label(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::Bytes => "bytes",
            Self::JsonFlat => "json-flat",
            Self::JsonNested => "json-nested",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DirectOperation {
    Encode,
    Decode,
}

impl DirectOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RegistryOperation {
    Encode,
    Decode,
    Receive,
}

impl RegistryOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
            Self::Receive => "receive",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RegistrationKind {
    Fresh,
    DuplicateType,
    DuplicateTag,
}

impl RegistrationKind {
    fn label(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::DuplicateType => "duplicate-type",
            Self::DuplicateTag => "duplicate-tag",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CodecWorkload {
    Direct {
        format: Format,
        operation: DirectOperation,
        size: usize,
    },
    Registry {
        format: Format,
        operation: RegistryOperation,
        size: usize,
    },
    Registration(RegistrationKind),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CodecValue {
    Fixed(FixedMessage),
    Bytes(BytesMessage),
    Structured(StructuredMessage),
}

pub struct MessageState {
    value: CodecValue,
    encoded: Vec<u8>,
    registry: Option<CodecRegistry>,
    destination: ActorAddress,
}

pub struct RegistrationState {
    registry: CodecRegistry,
    kind: RegistrationKind,
}

pub enum CodecState {
    Message(MessageState),
    Registration(RegistrationState),
}

pub enum CodecOutput {
    Bytes(Vec<u8>),
    Tagged {
        tag: String,
        bytes: Vec<u8>,
    },
    Decoded(Box<dyn Any + Send>),
    Received {
        destination: ActorAddress,
        decoded: Box<dyn Any + Send>,
    },
    Registration(Result<(), CodecRegistrationError>),
}

fn deterministic_bytes(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| ((index.wrapping_mul(31) + 17) % 251) as u8)
        .collect()
}

fn structured_value(size: usize, nested: bool) -> StructuredMessage {
    if !nested {
        return StructuredMessage {
            id: 0x5a5a_a5a5,
            name: "f".repeat(size),
            fields: Vec::new(),
            nested: Vec::new(),
        };
    }

    let width = 16;
    let field_len = (size / (width * 2)).max(1);
    let row_len = (size / (width * 16)).max(1);
    StructuredMessage {
        id: 0x5a5a_a5a5,
        name: "nested".to_owned(),
        fields: (0..width)
            .map(|index| format!("{index:02}-{}", "w".repeat(field_len)))
            .collect(),
        nested: (0..width)
            .map(|row| {
                (0..row_len)
                    .map(|column| (row * row_len + column) as u64)
                    .collect()
            })
            .collect(),
    }
}

fn value(format: Format, size: usize) -> CodecValue {
    match format {
        Format::Fixed => CodecValue::Fixed(FixedMessage(0x0123_4567_89ab_cdef)),
        Format::Bytes => CodecValue::Bytes(BytesMessage(deterministic_bytes(size))),
        Format::JsonFlat => CodecValue::Structured(structured_value(size, false)),
        Format::JsonNested => CodecValue::Structured(structured_value(size, true)),
    }
}

fn direct_encode(value: &CodecValue) -> Vec<u8> {
    match value {
        CodecValue::Fixed(message) => FixedCodec.encode(message).unwrap(),
        CodecValue::Bytes(message) => BytesCodec.encode(message).unwrap(),
        CodecValue::Structured(message) => JsonCodec::<StructuredMessage>::default()
            .encode(message)
            .unwrap(),
    }
}

fn direct_decode(template: &CodecValue, bytes: &[u8]) -> CodecValue {
    match template {
        CodecValue::Fixed(_) => CodecValue::Fixed(FixedCodec.decode(bytes).unwrap()),
        CodecValue::Bytes(_) => CodecValue::Bytes(BytesCodec.decode(bytes).unwrap()),
        CodecValue::Structured(_) => CodecValue::Structured(
            JsonCodec::<StructuredMessage>::default()
                .decode(bytes)
                .unwrap(),
        ),
    }
}

fn register_value(registry: &mut CodecRegistry, value: &CodecValue) {
    match value {
        CodecValue::Fixed(_) => registry
            .register::<FixedMessage, _>(FixedCodec)
            .expect("fresh fixed benchmark registration"),
        CodecValue::Bytes(_) => registry
            .register::<BytesMessage, _>(BytesCodec)
            .expect("fresh bytes benchmark registration"),
        CodecValue::Structured(_) => registry
            .register::<StructuredMessage, _>(JsonCodec::<StructuredMessage>::default())
            .expect("fresh JSON benchmark registration"),
    }
}

fn value_type_id(value: &CodecValue) -> TypeId {
    match value {
        CodecValue::Fixed(_) => TypeId::of::<FixedMessage>(),
        CodecValue::Bytes(_) => TypeId::of::<BytesMessage>(),
        CodecValue::Structured(_) => TypeId::of::<StructuredMessage>(),
    }
}

fn value_tag(value: &CodecValue) -> &'static str {
    match value {
        CodecValue::Fixed(_) => FixedMessage::type_tag(),
        CodecValue::Bytes(_) => BytesMessage::type_tag(),
        CodecValue::Structured(_) => StructuredMessage::type_tag(),
    }
}

fn boxed_value(value: &CodecValue) -> Box<dyn Any + Send> {
    match value {
        CodecValue::Fixed(message) => Box::new(message.clone()),
        CodecValue::Bytes(message) => Box::new(message.clone()),
        CodecValue::Structured(message) => Box::new(message.clone()),
    }
}

fn decoded_matches(value: &CodecValue, decoded: &dyn Any) -> bool {
    match value {
        CodecValue::Fixed(expected) => decoded.downcast_ref::<FixedMessage>() == Some(expected),
        CodecValue::Bytes(expected) => decoded.downcast_ref::<BytesMessage>() == Some(expected),
        CodecValue::Structured(expected) => {
            decoded.downcast_ref::<StructuredMessage>() == Some(expected)
        }
    }
}

fn verify_registration_state(state: &RegistrationState, kind: RegistrationKind) {
    let probe = BytesMessage(deterministic_bytes(32));
    match kind {
        RegistrationKind::Fresh => {
            let (tag, bytes) = state
                .registry
                .encode(TypeId::of::<BytesMessage>(), Box::new(probe.clone()))
                .unwrap();
            assert_eq!(tag, BytesMessage::type_tag());
            assert_eq!(bytes, probe.0);
        }
        RegistrationKind::DuplicateType => {
            let (tag, bytes) = state
                .registry
                .encode(TypeId::of::<BytesMessage>(), Box::new(probe.clone()))
                .unwrap();
            assert_eq!(tag, BytesMessage::type_tag());
            assert_eq!(bytes, probe.0);
            assert!(
                state
                    .registry
                    .decode(BytesMessage::type_tag(), &probe.0)
                    .is_err()
            );
        }
        RegistrationKind::DuplicateTag => {
            assert!(
                state
                    .registry
                    .encode(TypeId::of::<BytesMessage>(), Box::new(probe))
                    .is_err()
            );
            let decoded = state
                .registry
                .decode(BytesMessage::type_tag(), &0u64.to_le_bytes())
                .unwrap();
            assert_eq!(
                *decoded.downcast::<FixedMessage>().unwrap(),
                FixedMessage(0)
            );
        }
    }
}

impl Workload for CodecWorkload {
    type State = CodecState;
    type Output = CodecOutput;

    fn name(&self) -> String {
        match self {
            Self::Direct {
                format,
                operation,
                size,
            } => format!(
                "codec/direct/{}/{}/{}b",
                format.label(),
                operation.label(),
                size
            ),
            Self::Registry {
                format,
                operation,
                size,
            } => format!(
                "codec/registry/{}/{}/{}b",
                format.label(),
                operation.label(),
                size
            ),
            Self::Registration(kind) => {
                format!("codec/registration/{}", kind.label())
            }
        }
    }

    fn setup(&self) -> Self::State {
        match self {
            Self::Direct { format, size, .. } => {
                let value = value(*format, *size);
                let encoded = direct_encode(&value);
                CodecState::Message(MessageState {
                    value,
                    encoded,
                    registry: None,
                    destination: ActorAddress([0x5a; 32]),
                })
            }
            Self::Registry { format, size, .. } => {
                let value = value(*format, *size);
                let encoded = direct_encode(&value);
                let mut registry = CodecRegistry::new();
                register_value(&mut registry, &value);
                CodecState::Message(MessageState {
                    value,
                    encoded,
                    registry: Some(registry),
                    destination: ActorAddress([0x5a; 32]),
                })
            }
            Self::Registration(kind) => {
                let mut registry = CodecRegistry::new();
                match kind {
                    RegistrationKind::Fresh => {}
                    RegistrationKind::DuplicateType => registry
                        .register_encoder::<BytesMessage>(|message| {
                            Ok((BytesMessage::type_tag().to_owned(), message.0.clone()))
                        })
                        .expect("initial benchmark encoder"),
                    RegistrationKind::DuplicateTag => registry
                        .register_decoder::<FixedMessage>(BytesMessage::type_tag(), |bytes| {
                            FixedCodec.decode(bytes)
                        })
                        .expect("initial benchmark decoder"),
                }
                CodecState::Registration(RegistrationState {
                    registry,
                    kind: *kind,
                })
            }
        }
    }

    fn execute(&self, state: &mut Self::State) -> Self::Output {
        match (self, state) {
            (
                Self::Direct {
                    operation: DirectOperation::Encode,
                    ..
                },
                CodecState::Message(state),
            ) => CodecOutput::Bytes(direct_encode(&state.value)),
            (
                Self::Direct {
                    operation: DirectOperation::Decode,
                    ..
                },
                CodecState::Message(state),
            ) => CodecOutput::Decoded(boxed_value(&direct_decode(&state.value, &state.encoded))),
            (
                Self::Registry {
                    operation: RegistryOperation::Encode,
                    ..
                },
                CodecState::Message(state),
            ) => {
                let (tag, bytes) = state
                    .registry
                    .as_ref()
                    .unwrap()
                    .encode(value_type_id(&state.value), boxed_value(&state.value))
                    .unwrap();
                CodecOutput::Tagged { tag, bytes }
            }
            (
                Self::Registry {
                    operation: RegistryOperation::Decode,
                    ..
                },
                CodecState::Message(state),
            ) => CodecOutput::Decoded(
                state
                    .registry
                    .as_ref()
                    .unwrap()
                    .decode(value_tag(&state.value), &state.encoded)
                    .unwrap(),
            ),
            (
                Self::Registry {
                    operation: RegistryOperation::Receive,
                    ..
                },
                CodecState::Message(state),
            ) => {
                let (destination, decoded) = state
                    .registry
                    .as_ref()
                    .unwrap()
                    .receive(WireEnvelope {
                        dest: state.destination,
                        type_tag: value_tag(&state.value).to_owned(),
                        payload: state.encoded.clone(),
                    })
                    .unwrap();
                CodecOutput::Received {
                    destination,
                    decoded,
                }
            }
            (Self::Registration(_), CodecState::Registration(state)) => {
                CodecOutput::Registration(state.registry.register::<BytesMessage, _>(BytesCodec))
            }
            _ => panic!("codec workload and state mismatch"),
        }
    }

    fn verify(&self, state: &Self::State, output: &Self::Output) {
        match (self, state, output) {
            (
                Self::Direct {
                    operation: DirectOperation::Encode,
                    ..
                },
                CodecState::Message(state),
                CodecOutput::Bytes(bytes),
            ) => assert_eq!(bytes, &state.encoded),
            (
                Self::Direct {
                    operation: DirectOperation::Decode,
                    ..
                }
                | Self::Registry {
                    operation: RegistryOperation::Decode,
                    ..
                },
                CodecState::Message(state),
                CodecOutput::Decoded(decoded),
            ) => assert!(decoded_matches(&state.value, decoded.as_ref())),
            (
                Self::Registry {
                    operation: RegistryOperation::Encode,
                    ..
                },
                CodecState::Message(state),
                CodecOutput::Tagged { tag, bytes },
            ) => {
                assert_eq!(tag, value_tag(&state.value));
                assert_eq!(bytes, &state.encoded);
            }
            (
                Self::Registry {
                    operation: RegistryOperation::Receive,
                    ..
                },
                CodecState::Message(state),
                CodecOutput::Received {
                    destination,
                    decoded,
                },
            ) => {
                assert_eq!(*destination, state.destination);
                assert!(decoded_matches(&state.value, decoded.as_ref()));
            }
            (
                Self::Registration(kind),
                CodecState::Registration(state),
                CodecOutput::Registration(result),
            ) => {
                assert_eq!(state.kind.label(), kind.label());
                match kind {
                    RegistrationKind::Fresh => assert!(result.is_ok()),
                    RegistrationKind::DuplicateType => assert!(matches!(
                        result,
                        Err(CodecRegistrationError::EncoderAlreadyRegistered { .. })
                    )),
                    RegistrationKind::DuplicateTag => assert!(matches!(
                        result,
                        Err(CodecRegistrationError::DecoderAlreadyRegistered { .. })
                    )),
                }
                verify_registration_state(state, *kind);
            }
            _ => panic!("codec workload, state, and output mismatch"),
        }
    }

    fn units(&self) -> WorkUnits {
        match self {
            Self::Registration(_) => WorkUnits::Operations(1),
            Self::Direct { format, size, .. } | Self::Registry { format, size, .. } => {
                WorkUnits::Bytes(direct_encode(&value(*format, *size)).len() as u64)
            }
        }
    }
}

pub fn workloads() -> Vec<CodecWorkload> {
    let mut workloads = Vec::new();

    for operation in [DirectOperation::Encode, DirectOperation::Decode] {
        workloads.push(CodecWorkload::Direct {
            format: Format::Fixed,
            operation,
            size: 8,
        });
    }
    for format in [Format::Bytes, Format::JsonFlat, Format::JsonNested] {
        for size in PAYLOAD_SIZES {
            for operation in [DirectOperation::Encode, DirectOperation::Decode] {
                workloads.push(CodecWorkload::Direct {
                    format,
                    operation,
                    size,
                });
            }
        }
    }

    for operation in [
        RegistryOperation::Encode,
        RegistryOperation::Decode,
        RegistryOperation::Receive,
    ] {
        workloads.push(CodecWorkload::Registry {
            format: Format::Fixed,
            operation,
            size: 8,
        });
    }
    for format in [Format::Bytes, Format::JsonFlat, Format::JsonNested] {
        for size in PAYLOAD_SIZES {
            for operation in [
                RegistryOperation::Encode,
                RegistryOperation::Decode,
                RegistryOperation::Receive,
            ] {
                workloads.push(CodecWorkload::Registry {
                    format,
                    operation,
                    size,
                });
            }
        }
    }

    workloads.extend([
        CodecWorkload::Registration(RegistrationKind::Fresh),
        CodecWorkload::Registration(RegistrationKind::DuplicateType),
        CodecWorkload::Registration(RegistrationKind::DuplicateTag),
    ]);
    workloads
}
