use std::io::{Read, Seek, SeekFrom};

use crate::gguf_common::{GgufValueType, read_u64};

pub(crate) const GGUF_MAGIC: &[u8; 4] = b"GGUF";
pub(crate) const SUPPORTED_GGUF_VERSION: u32 = 3;

pub(crate) fn skip_scalar<R: Read + Seek>(
    reader: &mut R,
    value_type: GgufValueType,
) -> Result<(), String> {
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

pub(crate) fn read_gguf_string<R: Read + Seek>(
    reader: &mut R,
    max_len: u64,
) -> Result<String, String> {
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
