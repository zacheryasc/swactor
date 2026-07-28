use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::orchestration::run_plan::{self, DTypeFamily, GgufSource, TokenizerSource};

const GGUF_MAGIC: &[u8; 4] = b"GGUF";
const SUPPORTED_GGUF_VERSION: u32 = 3;
const DEFAULT_EFFECTIVE_CONTEXT: u64 = 512;
const MAX_METADATA_STRING_BYTES: u64 = 16 * 1024 * 1024;
const MAX_METADATA_KEY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GgufPlanningMetadata {
    pub version: u32,
    pub architecture: String,
    pub name: Option<String>,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub context_length: u64,
    pub eos_token_id: u32,
}

impl GgufPlanningMetadata {
    pub fn to_model_facts(
        &self,
        model_id: impl Into<String>,
        gguf_source: GgufSource,
        tokenizer: TokenizerSource,
        max_context: Option<u32>,
    ) -> Result<run_plan::ModelFacts, String> {
        let requested_context = max_context
            .map(u64::from)
            .unwrap_or(DEFAULT_EFFECTIVE_CONTEXT);
        if requested_context == 0 {
            return Err("MVP_MAX_CONTEXT/--max-context must be greater than 0".to_owned());
        }
        let effective_context = requested_context.min(self.context_length);
        if effective_context == 0 {
            return Err("GGUF context length must be greater than 0".to_owned());
        }
        Ok(run_plan::ModelFacts {
            model_id: model_id.into(),
            gguf_source,
            num_layers: self.num_layers,
            hidden_dim: self.hidden_dim,
            dtype_family: DTypeFamily::BFloat,
            dtype_width_bytes: 2,
            max_seq_len: effective_context,
            eos_token_id: self.eos_token_id,
            tokenizer,
        })
    }
}

pub fn read_gguf_planning_metadata(path: &Path) -> Result<GgufPlanningMetadata, String> {
    let file =
        File::open(path).map_err(|e| format!("open GGUF metadata {}: {e}", path.display()))?;
    read_gguf_planning_metadata_from_reader(file)
        .map_err(|e| format!("read GGUF metadata {}: {e}", path.display()))
}

fn read_gguf_planning_metadata_from_reader<R>(mut reader: R) -> Result<GgufPlanningMetadata, String>
where
    R: Read + Seek,
{
    let mut magic = [0; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|e| format!("read magic: {e}"))?;
    if &magic != GGUF_MAGIC {
        return Err("invalid GGUF magic".to_owned());
    }
    let version = read_u32(&mut reader)?;
    if version != SUPPORTED_GGUF_VERSION {
        return Err(format!(
            "unsupported GGUF version {version}; expected {SUPPORTED_GGUF_VERSION}"
        ));
    }
    let _tensor_count = read_u64(&mut reader)?;
    let metadata_count = read_u64(&mut reader)?;

    let mut strings = BTreeMap::<String, String>::new();
    let mut integers = BTreeMap::<String, u64>::new();

    for _ in 0..metadata_count {
        let key = read_gguf_string(&mut reader, MAX_METADATA_KEY_BYTES)?;
        let value_type = GgufValueType::read(&mut reader)?;
        match value_type {
            GgufValueType::String if key == "general.architecture" || key == "general.name" => {
                strings.insert(
                    key,
                    read_gguf_string(&mut reader, MAX_METADATA_STRING_BYTES)?,
                );
            }
            GgufValueType::String => {
                skip_gguf_string(&mut reader)?;
            }
            value_type if value_type.is_integer() => {
                let value = read_integer_value(&mut reader, value_type)?;
                if key.ends_with(".block_count")
                    || key.ends_with(".embedding_length")
                    || key.ends_with(".context_length")
                    || key == "tokenizer.ggml.eos_token_id"
                {
                    integers.insert(key, value);
                }
            }
            GgufValueType::Array => skip_array(&mut reader)?,
            other => skip_scalar(&mut reader, other)?,
        }
    }

    let architecture = strings
        .remove("general.architecture")
        .ok_or_else(|| "GGUF metadata missing general.architecture".to_owned())?;
    let name = strings.remove("general.name");
    let num_layers = required_u32(
        &integers,
        &format!("{architecture}.block_count"),
        "layer count",
    )?;
    let hidden_dim = required_u64(
        &integers,
        &format!("{architecture}.embedding_length"),
        "hidden dimension",
    )?;
    let context_length = required_u64(
        &integers,
        &format!("{architecture}.context_length"),
        "context length",
    )?;
    let eos_token_id = required_u32(&integers, "tokenizer.ggml.eos_token_id", "EOS token id")?;

    Ok(GgufPlanningMetadata {
        version,
        architecture,
        name,
        num_layers,
        hidden_dim,
        context_length,
        eos_token_id,
    })
}

fn required_u64(map: &BTreeMap<String, u64>, key: &str, label: &str) -> Result<u64, String> {
    map.get(key)
        .copied()
        .ok_or_else(|| format!("GGUF metadata missing {label} key {key}"))
}

fn required_u32(map: &BTreeMap<String, u64>, key: &str, label: &str) -> Result<u32, String> {
    let value = required_u64(map, key, label)?;
    u32::try_from(value).map_err(|_| format!("GGUF metadata {label} key {key} exceeds u32"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GgufValueType {
    Uint8,
    Int8,
    Uint16,
    Int16,
    Uint32,
    Int32,
    Float32,
    Bool,
    String,
    Array,
    Uint64,
    Int64,
    Float64,
}

impl GgufValueType {
    fn read<R: Read>(reader: &mut R) -> Result<Self, String> {
        let raw = read_u32(reader)?;
        match raw {
            0 => Ok(Self::Uint8),
            1 => Ok(Self::Int8),
            2 => Ok(Self::Uint16),
            3 => Ok(Self::Int16),
            4 => Ok(Self::Uint32),
            5 => Ok(Self::Int32),
            6 => Ok(Self::Float32),
            7 => Ok(Self::Bool),
            8 => Ok(Self::String),
            9 => Ok(Self::Array),
            10 => Ok(Self::Uint64),
            11 => Ok(Self::Int64),
            12 => Ok(Self::Float64),
            other => Err(format!("unsupported GGUF metadata value type {other}")),
        }
    }

    fn is_integer(self) -> bool {
        matches!(
            self,
            Self::Uint8
                | Self::Int8
                | Self::Uint16
                | Self::Int16
                | Self::Uint32
                | Self::Int32
                | Self::Uint64
                | Self::Int64
        )
    }

    fn fixed_width(self) -> Option<u64> {
        match self {
            Self::Uint8 | Self::Int8 | Self::Bool => Some(1),
            Self::Uint16 | Self::Int16 => Some(2),
            Self::Uint32 | Self::Int32 | Self::Float32 => Some(4),
            Self::Uint64 | Self::Int64 | Self::Float64 => Some(8),
            Self::String | Self::Array => None,
        }
    }
}

fn read_integer_value<R: Read>(reader: &mut R, value_type: GgufValueType) -> Result<u64, String> {
    match value_type {
        GgufValueType::Uint8 => read_u8(reader).map(u64::from),
        GgufValueType::Int8 => read_i8(reader).and_then(non_negative_i64_to_u64),
        GgufValueType::Uint16 => read_u16(reader).map(u64::from),
        GgufValueType::Int16 => {
            read_i16(reader).and_then(|v| non_negative_i64_to_u64(i64::from(v)))
        }
        GgufValueType::Uint32 => read_u32(reader).map(u64::from),
        GgufValueType::Int32 => {
            read_i32(reader).and_then(|v| non_negative_i64_to_u64(i64::from(v)))
        }
        GgufValueType::Uint64 => read_u64(reader),
        GgufValueType::Int64 => read_i64(reader).and_then(non_negative_i64_to_u64),
        other => Err(format!("GGUF value type {other:?} is not an integer")),
    }
}

fn non_negative_i64_to_u64(value: i64) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("negative integer metadata value {value}"))
}

fn skip_scalar<R: Read + Seek>(reader: &mut R, value_type: GgufValueType) -> Result<(), String> {
    match value_type {
        GgufValueType::String => skip_gguf_string(reader),
        GgufValueType::Array => skip_array(reader),
        other => skip_bytes(reader, other.fixed_width().expect("fixed scalar width")),
    }
}

fn skip_array<R: Read + Seek>(reader: &mut R) -> Result<(), String> {
    let element_type = GgufValueType::read(reader)?;
    let len = read_u64(reader)?;
    match element_type {
        GgufValueType::String => {
            for _ in 0..len {
                skip_gguf_string(reader)?;
            }
            Ok(())
        }
        GgufValueType::Array => {
            for _ in 0..len {
                skip_array(reader)?;
            }
            Ok(())
        }
        scalar => {
            let width = scalar.fixed_width().expect("fixed scalar array width");
            let bytes = width
                .checked_mul(len)
                .ok_or_else(|| "GGUF metadata array byte count overflow".to_owned())?;
            skip_bytes(reader, bytes)
        }
    }
}

fn read_gguf_string<R: Read + Seek>(reader: &mut R, max_len: u64) -> Result<String, String> {
    let len = read_u64(reader)?;
    if len > max_len {
        return Err(format!(
            "GGUF metadata string length {len} exceeds limit {max_len}"
        ));
    }
    let len_usize =
        usize::try_from(len).map_err(|_| "GGUF string length does not fit usize".to_owned())?;
    let mut bytes = vec![0; len_usize];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read GGUF metadata string: {e}"))?;
    String::from_utf8(bytes).map_err(|e| format!("GGUF metadata string is not UTF-8: {e}"))
}

fn skip_gguf_string<R: Read + Seek>(reader: &mut R) -> Result<(), String> {
    let len = read_u64(reader)?;
    skip_bytes(reader, len)
}

fn skip_bytes<R: Seek>(reader: &mut R, mut bytes: u64) -> Result<(), String> {
    while bytes > 0 {
        let chunk = bytes.min(i64::MAX as u64);
        reader
            .seek(SeekFrom::Current(chunk as i64))
            .map_err(|e| format!("skip GGUF metadata bytes: {e}"))?;
        bytes -= chunk;
    }
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, String> {
    let mut bytes = [0; 1];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read u8: {e}"))?;
    Ok(bytes[0])
}

fn read_i8<R: Read>(reader: &mut R) -> Result<i64, String> {
    read_u8(reader).map(|value| i8::from_le_bytes([value]) as i64)
}

fn read_u16<R: Read>(reader: &mut R) -> Result<u16, String> {
    let mut bytes = [0; 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read u16: {e}"))?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_i16<R: Read>(reader: &mut R) -> Result<i16, String> {
    let mut bytes = [0; 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read i16: {e}"))?;
    Ok(i16::from_le_bytes(bytes))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, String> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read u32: {e}"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_i32<R: Read>(reader: &mut R) -> Result<i32, String> {
    let mut bytes = [0; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read i32: {e}"))?;
    Ok(i32::from_le_bytes(bytes))
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, String> {
    let mut bytes = [0; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read u64: {e}"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64<R: Read>(reader: &mut R) -> Result<i64, String> {
    let mut bytes = [0; 8];
    reader
        .read_exact(&mut bytes)
        .map_err(|e| format!("read i64: {e}"))?;
    Ok(i64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reads_planning_metadata_from_minimal_gguf_header() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"GGUF");
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(&6_u64.to_le_bytes());
        push_string_kv(&mut bytes, "general.architecture", "llama");
        push_string_kv(&mut bytes, "general.name", "fixture");
        push_u32_kv(&mut bytes, "llama.block_count", 30);
        push_u32_kv(&mut bytes, "llama.embedding_length", 576);
        push_u32_kv(&mut bytes, "llama.context_length", 8192);
        push_u32_kv(&mut bytes, "tokenizer.ggml.eos_token_id", 2);

        let metadata = read_gguf_planning_metadata_from_reader(Cursor::new(bytes)).unwrap();

        assert_eq!(metadata.version, 3);
        assert_eq!(metadata.architecture, "llama");
        assert_eq!(metadata.name.as_deref(), Some("fixture"));
        assert_eq!(metadata.num_layers, 30);
        assert_eq!(metadata.hidden_dim, 576);
        assert_eq!(metadata.context_length, 8192);
        assert_eq!(metadata.eos_token_id, 2);
    }

    #[test]
    fn model_facts_clamp_effective_context_to_gguf_context() {
        let metadata = GgufPlanningMetadata {
            version: 3,
            architecture: "llama".to_owned(),
            name: None,
            num_layers: 30,
            hidden_dim: 576,
            context_length: 256,
            eos_token_id: 2,
        };

        let facts = metadata
            .to_model_facts(
                "fixture",
                GgufSource::LocalPath("/models/fixture.gguf".to_owned()),
                TokenizerSource::EmbeddedGguf,
                Some(512),
            )
            .unwrap();

        assert_eq!(facts.max_seq_len, 256);
        assert_eq!(facts.dtype_family, DTypeFamily::BFloat);
        assert_eq!(facts.dtype_width_bytes, 2);
    }

    #[test]
    fn model_facts_use_explicit_context_when_it_fits_inside_gguf_context() {
        let metadata = GgufPlanningMetadata {
            version: 3,
            architecture: "llama".to_owned(),
            name: Some("fixture".to_owned()),
            num_layers: 30,
            hidden_dim: 576,
            context_length: 8192,
            eos_token_id: 2,
        };

        let facts = metadata
            .to_model_facts(
                "fixture-model",
                GgufSource::LocalPath("/models/fixture.gguf".to_owned()),
                TokenizerSource::EmbeddedGguf,
                Some(384),
            )
            .unwrap();

        assert_eq!(facts.model_id, "fixture-model");
        assert_eq!(
            facts.gguf_source,
            GgufSource::LocalPath("/models/fixture.gguf".to_owned())
        );
        assert_eq!(facts.num_layers, 30);
        assert_eq!(facts.hidden_dim, 576);
        assert_eq!(facts.dtype_family, DTypeFamily::BFloat);
        assert_eq!(facts.dtype_width_bytes, 2);
        assert_eq!(facts.max_seq_len, 384);
        assert_eq!(facts.eos_token_id, 2);
        assert_eq!(facts.tokenizer, TokenizerSource::EmbeddedGguf);
    }

    #[test]
    fn reader_rejects_missing_required_planning_metadata() {
        for (missing_key, expected_error) in [
            ("general.architecture", "missing general.architecture"),
            (
                "llama.block_count",
                "missing layer count key llama.block_count",
            ),
            (
                "llama.embedding_length",
                "missing hidden dimension key llama.embedding_length",
            ),
            (
                "llama.context_length",
                "missing context length key llama.context_length",
            ),
            (
                "tokenizer.ggml.eos_token_id",
                "missing EOS token id key tokenizer.ggml.eos_token_id",
            ),
        ] {
            let error = read_gguf_planning_metadata_from_reader(Cursor::new(minimal_gguf_without(
                missing_key,
            )))
            .expect_err("metadata with a missing planning field must reject");
            assert!(
                error.contains(expected_error),
                "missing {missing_key} error {error:?} should contain {expected_error:?}"
            );
        }
    }

    fn minimal_gguf_without(missing_key: &str) -> Vec<u8> {
        let fields = [
            "general.architecture",
            "general.name",
            "llama.block_count",
            "llama.embedding_length",
            "llama.context_length",
            "tokenizer.ggml.eos_token_id",
        ];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"GGUF");
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.extend_from_slice(
            &(fields.iter().filter(|field| **field != missing_key).count() as u64).to_le_bytes(),
        );
        if missing_key != "general.architecture" {
            push_string_kv(&mut bytes, "general.architecture", "llama");
        }
        if missing_key != "general.name" {
            push_string_kv(&mut bytes, "general.name", "fixture");
        }
        if missing_key != "llama.block_count" {
            push_u32_kv(&mut bytes, "llama.block_count", 30);
        }
        if missing_key != "llama.embedding_length" {
            push_u32_kv(&mut bytes, "llama.embedding_length", 576);
        }
        if missing_key != "llama.context_length" {
            push_u32_kv(&mut bytes, "llama.context_length", 8192);
        }
        if missing_key != "tokenizer.ggml.eos_token_id" {
            push_u32_kv(&mut bytes, "tokenizer.ggml.eos_token_id", 2);
        }
        bytes
    }

    fn push_string_kv(bytes: &mut Vec<u8>, key: &str, value: &str) {
        push_string(bytes, key);
        bytes.extend_from_slice(&8_u32.to_le_bytes());
        push_string(bytes, value);
    }

    fn push_u32_kv(bytes: &mut Vec<u8>, key: &str, value: u32) {
        push_string(bytes, key);
        bytes.extend_from_slice(&4_u32.to_le_bytes());
        bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
}
