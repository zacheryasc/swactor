use std::io::Read;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GgufValueType {
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
    pub(crate) fn read<R: Read>(reader: &mut R, value_label: &str) -> Result<Self, String> {
        const VALUE_TYPES: [GgufValueType; 13] = [
            GgufValueType::Uint8,
            GgufValueType::Int8,
            GgufValueType::Uint16,
            GgufValueType::Int16,
            GgufValueType::Uint32,
            GgufValueType::Int32,
            GgufValueType::Float32,
            GgufValueType::Bool,
            GgufValueType::String,
            GgufValueType::Array,
            GgufValueType::Uint64,
            GgufValueType::Int64,
            GgufValueType::Float64,
        ];
        let raw = read_u32(reader)?;
        VALUE_TYPES
            .get(raw as usize)
            .copied()
            .ok_or_else(|| format!("unsupported {value_label} {raw}"))
    }

    pub(crate) fn fixed_width(self) -> Option<u64> {
        match self {
            Self::Uint8 | Self::Int8 | Self::Bool => Some(1),
            Self::Uint16 | Self::Int16 => Some(2),
            Self::Uint32 | Self::Int32 | Self::Float32 => Some(4),
            Self::Uint64 | Self::Int64 | Self::Float64 => Some(8),
            Self::String | Self::Array => None,
        }
    }

    pub(crate) fn is_integer(self) -> bool {
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
}

pub(crate) fn read_integer_value<R: Read>(
    reader: &mut R,
    value_type: GgufValueType,
    type_error: impl FnOnce(GgufValueType) -> String,
    negative_error: impl Fn(i64) -> String,
) -> Result<u64, String> {
    match value_type {
        GgufValueType::Uint8 => read_u8(reader).map(u64::from),
        GgufValueType::Int8 => {
            read_i8(reader).and_then(|value| non_negative_i64_to_u64(value, negative_error))
        }
        GgufValueType::Uint16 => read_u16(reader).map(u64::from),
        GgufValueType::Int16 => read_i16(reader)
            .and_then(|value| non_negative_i64_to_u64(i64::from(value), negative_error)),
        GgufValueType::Uint32 => read_u32(reader).map(u64::from),
        GgufValueType::Int32 => read_i32(reader)
            .and_then(|value| non_negative_i64_to_u64(i64::from(value), negative_error)),
        GgufValueType::Uint64 => read_u64(reader),
        GgufValueType::Int64 => {
            read_i64(reader).and_then(|value| non_negative_i64_to_u64(value, negative_error))
        }
        other => Err(type_error(other)),
    }
}

fn non_negative_i64_to_u64(
    value: i64,
    negative_error: impl Fn(i64) -> String,
) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| negative_error(value))
}

macro_rules! read_le {
    ($name:ident, $ret:ty, $len:expr, |$bytes:ident| $body:expr) => {
        pub(crate) fn $name<R: Read>(reader: &mut R) -> Result<$ret, String> {
            let mut bytes = [0; $len];
            reader.read_exact(&mut bytes).map_err(|e| {
                format!(
                    "read {}: {e}",
                    stringify!($name).trim_start_matches("read_")
                )
            })?;
            Ok({
                let $bytes = bytes;
                $body
            })
        }
    };
}

read_le!(read_u8, u8, 1, |bytes| bytes[0]);
read_le!(read_i8, i64, 1, |bytes| i8::from_le_bytes(bytes) as i64);
read_le!(read_u16, u16, 2, |bytes| u16::from_le_bytes(bytes));
read_le!(read_i16, i16, 2, |bytes| i16::from_le_bytes(bytes));
read_le!(read_u32, u32, 4, |bytes| u32::from_le_bytes(bytes));
read_le!(read_i32, i32, 4, |bytes| i32::from_le_bytes(bytes));
read_le!(read_u64, u64, 8, |bytes| u64::from_le_bytes(bytes));
read_le!(read_i64, i64, 8, |bytes| i64::from_le_bytes(bytes));
