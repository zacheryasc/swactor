use std::any::TypeId;
use std::collections::HashSet;

use proptest::prelude::*;
use swactor::actor::ActorAddress;
use swactor::Error;
use swactor_transport::{
    Codec, CodecRegistrationError, CodecRegistry, JsonCodec, NetworkMessage, WireEnvelope,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Number(u64);

impl NetworkMessage for Number {
    fn type_tag() -> &'static str {
        "contract::Number"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Text(String);

impl NetworkMessage for Text {
    fn type_tag() -> &'static str {
        "contract::Text"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Blob(Vec<u8>);

impl NetworkMessage for Blob {
    fn type_tag() -> &'static str {
        "contract::Blob"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Structured {
    id: u64,
    labels: Vec<String>,
}

impl NetworkMessage for Structured {
    fn type_tag() -> &'static str {
        "contract::Structured"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fallible(u8);

impl NetworkMessage for Fallible {
    fn type_tag() -> &'static str {
        "contract::Fallible"
    }
}

#[derive(Clone, Copy)]
struct NumberCodec;

impl Codec<Number> for NumberCodec {
    fn encode(&self, msg: &Number) -> Result<Vec<u8>, Error> {
        Ok(msg.0.to_be_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Number, Error> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::from("number payload must contain eight bytes"))?;
        Ok(Number(u64::from_be_bytes(bytes)))
    }
}

#[derive(Clone, Copy)]
struct TextCodec;

impl Codec<Text> for TextCodec {
    fn encode(&self, msg: &Text) -> Result<Vec<u8>, Error> {
        Ok(msg.0.as_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Text, Error> {
        let text = std::str::from_utf8(bytes)
            .map_err(|error| Error::from(format!("invalid text payload: {error}")))?;
        Ok(Text(text.to_owned()))
    }
}

#[derive(Clone, Copy)]
struct BlobCodec;

impl Codec<Blob> for BlobCodec {
    fn encode(&self, msg: &Blob) -> Result<Vec<u8>, Error> {
        Ok(msg.0.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Blob, Error> {
        Ok(Blob(bytes.to_vec()))
    }
}

#[derive(Clone, Copy)]
struct FallibleCodec;

impl Codec<Fallible> for FallibleCodec {
    fn encode(&self, msg: &Fallible) -> Result<Vec<u8>, Error> {
        if msg.0 == u8::MAX {
            return Err(Error::from("refused value"));
        }
        Ok(vec![msg.0])
    }

    fn decode(&self, bytes: &[u8]) -> Result<Fallible, Error> {
        match bytes {
            [value] if *value != u8::MAX => Ok(Fallible(*value)),
            _ => Err(Error::from("malformed fallible payload")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Sample {
    Number(Number),
    Text(Text),
    Blob(Blob),
    Structured(Structured),
    Fallible(Fallible),
}

impl Sample {
    fn register(&self, registry: &mut CodecRegistry) -> Result<(), CodecRegistrationError> {
        match self {
            Self::Number(_) => registry.register::<Number, _>(NumberCodec),
            Self::Text(_) => registry.register::<Text, _>(TextCodec),
            Self::Blob(_) => registry.register::<Blob, _>(BlobCodec),
            Self::Structured(_) => registry.register::<Structured, _>(JsonCodec::default()),
            Self::Fallible(_) => registry.register::<Fallible, _>(FallibleCodec),
        }
    }

    fn type_id(&self) -> TypeId {
        match self {
            Self::Number(_) => TypeId::of::<Number>(),
            Self::Text(_) => TypeId::of::<Text>(),
            Self::Blob(_) => TypeId::of::<Blob>(),
            Self::Structured(_) => TypeId::of::<Structured>(),
            Self::Fallible(_) => TypeId::of::<Fallible>(),
        }
    }

    fn tag(&self) -> &'static str {
        match self {
            Self::Number(_) => Number::type_tag(),
            Self::Text(_) => Text::type_tag(),
            Self::Blob(_) => Blob::type_tag(),
            Self::Structured(_) => Structured::type_tag(),
            Self::Fallible(_) => Fallible::type_tag(),
        }
    }

    fn boxed(&self) -> Box<dyn std::any::Any + Send> {
        match self {
            Self::Number(value) => Box::new(value.clone()),
            Self::Text(value) => Box::new(value.clone()),
            Self::Blob(value) => Box::new(value.clone()),
            Self::Structured(value) => Box::new(value.clone()),
            Self::Fallible(value) => Box::new(value.clone()),
        }
    }

    fn assert_decoded(&self, decoded: Box<dyn std::any::Any + Send>) {
        match self {
            Self::Number(expected) => assert_eq!(*decoded.downcast::<Number>().unwrap(), *expected),
            Self::Text(expected) => assert_eq!(*decoded.downcast::<Text>().unwrap(), *expected),
            Self::Blob(expected) => assert_eq!(*decoded.downcast::<Blob>().unwrap(), *expected),
            Self::Structured(expected) => {
                assert_eq!(*decoded.downcast::<Structured>().unwrap(), *expected)
            }
            Self::Fallible(expected) => {
                assert_eq!(*decoded.downcast::<Fallible>().unwrap(), *expected)
            }
        }
    }
}

fn address(seed: u8) -> ActorAddress {
    ActorAddress([seed; 32])
}

fn assert_sample(registry: &CodecRegistry, sample: &Sample, destination: ActorAddress) -> Vec<u8> {
    let (tag, payload) = registry
        .encode(sample.type_id(), sample.boxed())
        .expect("registered sample encodes");
    assert_eq!(tag, sample.tag());
    sample.assert_decoded(
        registry
            .decode(&tag, &payload)
            .expect("registered sample decodes"),
    );

    let (received_destination, decoded) = registry
        .receive(WireEnvelope {
            dest: destination,
            type_tag: tag,
            payload: payload.clone(),
        })
        .expect("valid envelope receives");
    assert_eq!(received_destination, destination);
    sample.assert_decoded(decoded);
    payload
}

fn fingerprint(registry: &CodecRegistry, samples: &[Sample]) -> Vec<Vec<u8>> {
    let type_ids: HashSet<TypeId> = samples.iter().map(Sample::type_id).collect();
    let tags: HashSet<&str> = samples.iter().map(Sample::tag).collect();
    assert_eq!(
        type_ids.len(),
        samples.len(),
        "registered TypeIds are unique"
    );
    assert_eq!(tags.len(), samples.len(), "registered wire tags are unique");
    samples
        .iter()
        .enumerate()
        .map(|(index, sample)| assert_sample(registry, sample, address(index as u8)))
        .collect()
}

#[derive(Clone, Debug)]
enum LegalOperation {
    Encode(usize),
    Decode(usize),
    Receive(usize, u8),
}

fn legal_operation() -> impl Strategy<Value = LegalOperation> {
    prop_oneof![
        any::<usize>().prop_map(LegalOperation::Encode),
        any::<usize>().prop_map(LegalOperation::Decode),
        (any::<usize>(), any::<u8>()).prop_map(|(slot, dest)| LegalOperation::Receive(slot, dest)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 10_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn long_legal_action_sequences_preserve_every_registration(
        number in any::<u64>(),
        text in any::<String>(),
        blob in prop::collection::vec(any::<u8>(), 0..512),
        structured_id in any::<u64>(),
        labels in prop::collection::vec(any::<String>(), 0..16),
        fallible in 0u8..u8::MAX,
        operations in prop::collection::vec(legal_operation(), 128..1025),
    ) {
        let samples = vec![
            Sample::Number(Number(number)),
            Sample::Text(Text(text)),
            Sample::Blob(Blob(blob)),
            Sample::Structured(Structured { id: structured_id, labels }),
            Sample::Fallible(Fallible(fallible)),
        ];
        let mut registry = CodecRegistry::new();
        let mut registered = Vec::new();

        for sample in &samples {
            sample.register(&mut registry).expect("fresh type and tag register");
            registered.push(sample.clone());
            fingerprint(&registry, &registered);
        }

        for operation in operations {
            let slot = match operation {
                LegalOperation::Encode(slot)
                | LegalOperation::Decode(slot)
                | LegalOperation::Receive(slot, _) => slot % samples.len(),
            };
            let sample = &samples[slot];
            match operation {
                LegalOperation::Encode(_) => {
                    let (tag, _) = registry.encode(sample.type_id(), sample.boxed()).unwrap();
                    prop_assert_eq!(tag, sample.tag());
                }
                LegalOperation::Decode(_) => {
                    let (_, payload) = registry.encode(sample.type_id(), sample.boxed()).unwrap();
                    sample.assert_decoded(registry.decode(sample.tag(), &payload).unwrap());
                }
                LegalOperation::Receive(_, dest) => {
                    assert_sample(&registry, sample, address(dest));
                }
            }
            fingerprint(&registry, &registered);
        }
    }
}

#[test]
fn every_registration_duplicate_is_rejected_without_replacement() {
    let sample = Sample::Number(Number(7));
    let mut registry = CodecRegistry::new();
    sample.register(&mut registry).unwrap();
    let before = fingerprint(&registry, std::slice::from_ref(&sample));

    assert!(matches!(
        registry.register::<Number, _>(NumberCodec),
        Err(CodecRegistrationError::EncoderAlreadyRegistered { .. })
    ));
    assert_eq!(
        fingerprint(&registry, std::slice::from_ref(&sample)),
        before
    );

    assert!(matches!(
        registry.register_encoder::<Number>(|number| {
            Ok(("replacement".to_owned(), number.0.to_le_bytes().to_vec()))
        }),
        Err(CodecRegistrationError::EncoderAlreadyRegistered { .. })
    ));
    assert_eq!(
        fingerprint(&registry, std::slice::from_ref(&sample)),
        before
    );

    assert!(matches!(
        registry.register_decoder::<Text>(Number::type_tag(), |_| Ok(Text("replacement".into()))),
        Err(CodecRegistrationError::DecoderAlreadyRegistered { .. })
    ));
    assert_eq!(
        fingerprint(&registry, std::slice::from_ref(&sample)),
        before
    );
}

#[test]
fn symmetric_registration_is_atomic_when_only_encoder_conflicts() {
    let mut registry = CodecRegistry::new();
    registry
        .register_encoder::<Number>(|number| {
            Ok((
                Number::type_tag().to_owned(),
                number.0.to_be_bytes().to_vec(),
            ))
        })
        .unwrap();

    assert!(matches!(
        registry.register::<Number, _>(NumberCodec),
        Err(CodecRegistrationError::EncoderAlreadyRegistered { .. })
    ));
    assert!(registry
        .decode(Number::type_tag(), &0u64.to_be_bytes())
        .is_err());
    let (tag, bytes) = registry
        .encode(TypeId::of::<Number>(), Box::new(Number(9)))
        .unwrap();
    assert_eq!(tag, Number::type_tag());
    assert_eq!(bytes, 9u64.to_be_bytes());
}

#[test]
fn symmetric_registration_is_atomic_when_only_decoder_conflicts() {
    let mut registry = CodecRegistry::new();
    registry
        .register_decoder::<Text>(Number::type_tag(), |bytes| {
            Ok(Text(String::from_utf8_lossy(bytes).into_owned()))
        })
        .unwrap();

    assert!(matches!(
        registry.register::<Number, _>(NumberCodec),
        Err(CodecRegistrationError::DecoderAlreadyRegistered { .. })
    ));
    assert!(registry
        .encode(TypeId::of::<Number>(), Box::new(Number(9)))
        .is_err());
    let decoded = registry.decode(Number::type_tag(), b"first").unwrap();
    assert_eq!(*decoded.downcast::<Text>().unwrap(), Text("first".into()));
}

#[test]
fn operation_failures_do_not_change_registered_behavior() {
    let samples = vec![Sample::Number(Number(11)), Sample::Fallible(Fallible(3))];
    let mut registry = CodecRegistry::new();
    for sample in &samples {
        sample.register(&mut registry).unwrap();
    }
    let before = fingerprint(&registry, &samples);

    assert!(registry
        .encode(TypeId::of::<Text>(), Box::new(Text("unknown".into())))
        .is_err());
    assert_eq!(fingerprint(&registry, &samples), before);

    assert!(registry
        .encode(TypeId::of::<Number>(), Box::new(Text("wrong".into())))
        .is_err());
    assert_eq!(fingerprint(&registry, &samples), before);

    assert!(registry.decode("contract::Unknown", b"anything").is_err());
    assert_eq!(fingerprint(&registry, &samples), before);

    assert!(registry.decode(Number::type_tag(), &[1, 2, 3]).is_err());
    assert_eq!(fingerprint(&registry, &samples), before);

    assert!(registry
        .encode(TypeId::of::<Fallible>(), Box::new(Fallible(u8::MAX)))
        .is_err());
    assert_eq!(fingerprint(&registry, &samples), before);

    assert!(registry.decode(Fallible::type_tag(), &[u8::MAX]).is_err());
    assert_eq!(fingerprint(&registry, &samples), before);
}
