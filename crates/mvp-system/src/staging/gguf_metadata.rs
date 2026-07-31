use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::gguf_common::{GgufValueType, read_integer_value, read_u32, read_u64};
use crate::run_plan::{self, DTypeFamily, GgufSource, TokenizerSource};

pub(crate) const GGUF_MAGIC: &[u8; 4] = b"GGUF";
pub(crate) const SUPPORTED_GGUF_VERSION: u32 = 3;
const DEFAULT_EFFECTIVE_CONTEXT: u64 = 512;
const MAX_METADATA_STRING_BYTES: u64 = 16 * 1024 * 1024;
const MAX_METADATA_KEY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GgufPlanningMetadata {
    pub version: u32,
    pub architecture: String,
    pub name: Option<String>,
    pub num_layers: u32,
    pub hidden_dim: u64,
    pub context_length: u64,
    pub eos_token_id: u32,
}

impl GgufPlanningMetadata {
    pub(crate) fn to_model_facts(
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

pub(crate) fn read_gguf_planning_metadata(path: &Path) -> Result<GgufPlanningMetadata, String> {
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
        let value_type = GgufValueType::read(&mut reader, "GGUF metadata value type")?;
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
                let value = read_integer_value(
                    &mut reader,
                    value_type,
                    |other| format!("GGUF value type {other:?} is not an integer"),
                    |value| format!("negative integer metadata value {value}"),
                )?;
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

pub(crate) fn skip_scalar<R: Read + Seek>(reader: &mut R, value_type: GgufValueType) -> Result<(), String> {
    match value_type {
        GgufValueType::String => skip_gguf_string(reader),
        GgufValueType::Array => skip_array(reader),
        other => skip_bytes(reader, other.fixed_width().expect("fixed scalar width")),
    }
}

pub(crate) fn skip_array<R: Read + Seek>(reader: &mut R) -> Result<(), String> {
    let element_type = GgufValueType::read(reader, "GGUF metadata value type")?;
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

pub(crate) fn read_gguf_string<R: Read + Seek>(reader: &mut R, max_len: u64) -> Result<String, String> {
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

pub(crate) fn skip_gguf_string<R: Read + Seek>(reader: &mut R) -> Result<(), String> {
    let len = read_u64(reader)?;
    skip_bytes(reader, len)
}

pub(crate) fn skip_bytes<R: Seek>(reader: &mut R, mut bytes: u64) -> Result<(), String> {
    while bytes > 0 {
        let chunk = bytes.min(i64::MAX as u64);
        reader
            .seek(SeekFrom::Current(chunk as i64))
            .map_err(|e| format!("skip GGUF metadata bytes: {e}"))?;
        bytes -= chunk;
    }
    Ok(())
}
