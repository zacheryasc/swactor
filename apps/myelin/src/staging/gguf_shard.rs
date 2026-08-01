use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::gguf_common::{GgufValueType, read_integer_value, read_u32, read_u64};
use serde::{Deserialize, Serialize};

use crate::run_plan::GgufSource;
use crate::staging::gguf_metadata::{skip_scalar as skip_value, read_gguf_string, GGUF_MAGIC, SUPPORTED_GGUF_VERSION};

const DEFAULT_ALIGNMENT: u64 = 32;
const MAX_STRING_BYTES: u64 = 64 * 1024 * 1024;
const STAGE_SHARD_CACHE_FORMAT_VERSION: &str = "stage-shard-cache-v2";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ByteRange {
    pub start: u64,
    pub len: u64,
}

impl ByteRange {
    pub(crate) fn end_exclusive(self) -> Option<u64> {
        self.start.checked_add(self.len)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StageShardTensor {
    pub name: String,
    pub dims: Vec<u64>,
    pub ggml_type: u32,
    /// Tensor byte offset relative to the source GGUF data section.
    pub source_offset: u64,
    /// Tensor storage bytes in the source GGUF. Includes any source-side tensor padding
    /// before the next tensor, which is safe to copy and keeps range math independent of
    /// GGML quantization block-size tables.
    pub byte_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StageShardPlan {
    pub source: GgufSource,
    pub stage_index: u32,
    pub stage_count: u32,
    pub layer_start: u32,
    pub layer_end_exclusive: u32,
    pub metadata_count: u64,
    /// Offset immediately after the metadata KV section in the source file.
    pub metadata_end: u64,
    /// Offset of the source data section.
    pub data_start: u64,
    pub alignment: u32,
    pub source_total_bytes: u64,
    pub tensors: Vec<StageShardTensor>,
    /// Coalesced source-file byte ranges this stage must fetch from the origin.
    /// Metadata/header bytes are intentionally separate: every stage fetches
    /// `0..metadata_end` so it can write a valid stage-local GGUF header.
    pub merged_tensor_ranges: Vec<ByteRange>,
    pub cache_key: String,
}

impl StageShardPlan {
    pub(crate) fn cache_file_name(&self) -> String {
        format!("{}.stage-{:05}.gguf", self.cache_key, self.stage_index)
    }

    pub(crate) fn planned_fetch_bytes(&self) -> u64 {
        self.metadata_end
            .saturating_add(self.merged_tensor_ranges.iter().map(|r| r.len).sum())
    }

    pub(crate) fn planned_range_count(&self) -> usize {
        self.merged_tensor_ranges.len() + if self.metadata_end > 0 { 1 } else { 0 }
    }
}

#[derive(Clone, Debug)]
struct GgufTensorEntry {
    name: String,
    dims: Vec<u64>,
    ggml_type: u32,
    source_offset: u64,
    byte_len: u64,
}

#[derive(Clone, Debug)]
struct GgufDirectory {
    metadata_count: u64,
    metadata_end: u64,
    data_start: u64,
    alignment: u32,
    total_bytes: u64,
    tensors: Vec<GgufTensorEntry>,
}

pub(crate) fn plan_stage_shard(
    planning_gguf: &Path,
    source: GgufSource,
    stage_index: u32,
    stage_count: u32,
    layer_start: u32,
    layer_end_exclusive: u32,
) -> Result<StageShardPlan, String> {
    if layer_start >= layer_end_exclusive {
        return Err(format!(
            "stage {stage_index} has empty layer range {layer_start}..{layer_end_exclusive}"
        ));
    }
    if stage_count == 0 || stage_index >= stage_count {
        return Err(format!(
            "invalid stage index/count: stage {stage_index}, count {stage_count}"
        ));
    }

    let directory = read_gguf_directory(planning_gguf)?;
    let tensors = select_stage_tensors(
        &directory.tensors,
        stage_index,
        stage_count,
        layer_start,
        layer_end_exclusive,
    )?;
    let merged_tensor_ranges = merge_tensor_ranges(directory.data_start, &tensors)?;
    let cache_key = shard_cache_key(
        &source,
        stage_index,
        stage_count,
        layer_start,
        layer_end_exclusive,
        &tensors,
    );

    Ok(StageShardPlan {
        source,
        stage_index,
        stage_count,
        layer_start,
        layer_end_exclusive,
        metadata_count: directory.metadata_count,
        metadata_end: directory.metadata_end,
        data_start: directory.data_start,
        alignment: directory.alignment,
        source_total_bytes: directory.total_bytes,
        tensors: tensors
            .into_iter()
            .map(|tensor| StageShardTensor {
                name: tensor.name,
                dims: tensor.dims,
                ggml_type: tensor.ggml_type,
                source_offset: tensor.source_offset,
                byte_len: tensor.byte_len,
            })
            .collect(),
        merged_tensor_ranges,
        cache_key,
    })
}

pub(crate) fn validate_stage_shard_cache(path: &Path, plan: &StageShardPlan) -> Result<(), String> {
    let directory = read_gguf_directory(path)
        .map_err(|error| format!("invalid cached stage shard {}: {error}", path.display()))?;
    if directory.tensors.len() != plan.tensors.len() {
        return Err(format!(
            "cached stage shard {} has {} tensors; expected {}",
            path.display(),
            directory.tensors.len(),
            plan.tensors.len()
        ));
    }
    if directory.alignment != plan.alignment {
        return Err(format!(
            "cached stage shard {} has alignment {}; expected {}",
            path.display(),
            directory.alignment,
            plan.alignment
        ));
    }
    for (index, (actual, expected)) in directory.tensors.iter().zip(&plan.tensors).enumerate() {
        if actual.name != expected.name
            || actual.dims != expected.dims
            || actual.ggml_type != expected.ggml_type
        {
            return Err(format!(
                "cached stage shard {} tensor {index} is {} {:?} type {}; expected {} {:?} type {}",
                path.display(),
                actual.name,
                actual.dims,
                actual.ggml_type,
                expected.name,
                expected.dims,
                expected.ggml_type
            ));
        }
    }
    Ok(())
}

pub(crate) fn source_url(source: &GgufSource) -> Result<String, String> {
    match source {
        GgufSource::HuggingFaceGguf {
            repo,
            file,
            revision,
        } => Ok(format!(
            "https://huggingface.co/{repo}/resolve/{}/{}",
            revision.as_deref().unwrap_or("main"),
            file.split('/').map(percent_encode_path_segment).collect::<Vec<_>>().join("/")
        )),
        GgufSource::LocalPath(path) => Err(format!(
            "stage shard range fetching requires a remote Hugging Face source; got local path {path:?}"
        )),
    }
}

fn percent_encode_path_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn read_gguf_directory(path: &Path) -> Result<GgufDirectory, String> {
    let mut file = File::open(path).map_err(|e| format!("open GGUF {}: {e}", path.display()))?;
    let total_bytes = file
        .metadata()
        .map_err(|e| format!("stat GGUF {}: {e}", path.display()))?
        .len();
    let mut magic = [0; 4];
    file.read_exact(&mut magic)
        .map_err(|e| format!("read GGUF magic: {e}"))?;
    if &magic != GGUF_MAGIC {
        return Err("invalid GGUF magic".to_owned());
    }
    let version = read_u32(&mut file)?;
    if version != SUPPORTED_GGUF_VERSION {
        return Err(format!(
            "unsupported GGUF version {version}; expected {SUPPORTED_GGUF_VERSION}"
        ));
    }
    let tensor_count = read_u64(&mut file)?;
    let metadata_count = read_u64(&mut file)?;
    let mut alignment = DEFAULT_ALIGNMENT;

    for _ in 0..metadata_count {
        let key = read_gguf_string(&mut file, MAX_STRING_BYTES)?;
        let value_type = GgufValueType::read(&mut file, "GGUF value type")?;
        if key == "general.alignment" && value_type.is_integer() {
            alignment = read_integer_value(
                &mut file,
                value_type,
                |other| format!("GGUF value type {other:?} is not integer"),
                |value| format!("negative GGUF integer {value}"),
            )?;
        } else {
            skip_value(&mut file, value_type)?;
        }
    }
    let metadata_end = file
        .stream_position()
        .map_err(|e| format!("locate GGUF metadata end: {e}"))?;

    let mut tensor_infos = Vec::new();
    for _ in 0..tensor_count {
        let name = read_gguf_string(&mut file, MAX_STRING_BYTES)?;
        let dims_len = read_u32(&mut file)?;
        let mut dims = Vec::with_capacity(dims_len as usize);
        for _ in 0..dims_len {
            dims.push(read_u64(&mut file)?);
        }
        let ggml_type = read_u32(&mut file)?;
        let source_offset = read_u64(&mut file)?;
        tensor_infos.push((name, dims, ggml_type, source_offset));
    }
    let tensor_table_end = file
        .stream_position()
        .map_err(|e| format!("locate GGUF tensor table end: {e}"))?;
    let data_start = align_to(tensor_table_end, alignment)?;
    if data_start > total_bytes {
        return Err(format!(
            "GGUF data section starts at {data_start}, beyond file size {total_bytes}"
        ));
    }

    let mut order = tensor_infos
        .iter()
        .enumerate()
        .map(|(index, (_, _, _, offset))| (*offset, index))
        .collect::<Vec<_>>();
    order.sort_by_key(|(offset, _)| *offset);
    let mut byte_lens = vec![0_u64; tensor_infos.len()];
    for (position, (offset, tensor_index)) in order.iter().copied().enumerate() {
        let absolute = data_start
            .checked_add(offset)
            .ok_or_else(|| format!("tensor offset overflow at {offset}"))?;
        if absolute > total_bytes {
            return Err(format!(
                "tensor offset {offset} points beyond GGUF data size in {}",
                path.display()
            ));
        }
        let next_absolute = if let Some((next_offset, _)) = order.get(position + 1) {
            data_start
                .checked_add(*next_offset)
                .ok_or_else(|| format!("next tensor offset overflow at {next_offset}"))?
        } else {
            total_bytes
        };
        if next_absolute < absolute {
            return Err("GGUF tensor offsets are not monotonic".to_owned());
        }
        byte_lens[tensor_index] = next_absolute - absolute;
    }

    let tensors = tensor_infos
        .into_iter()
        .enumerate()
        .map(
            |(index, (name, dims, ggml_type, source_offset))| GgufTensorEntry {
                name,
                dims,
                ggml_type,
                source_offset,
                byte_len: byte_lens[index],
            },
        )
        .collect();

    Ok(GgufDirectory {
        metadata_count,
        metadata_end,
        data_start,
        alignment: u32::try_from(alignment)
            .map_err(|_| format!("GGUF alignment {alignment} exceeds u32"))?,
        total_bytes,
        tensors,
    })
}

fn select_stage_tensors(
    tensors: &[GgufTensorEntry],
    stage_index: u32,
    stage_count: u32,
    layer_start: u32,
    layer_end_exclusive: u32,
) -> Result<Vec<GgufTensorEntry>, String> {
    let first_stage = layer_start == 0;
    let final_stage = stage_index + 1 == stage_count;
    let has_output_weight = tensors.iter().any(|tensor| tensor.name == "output.weight");
    let mut selected = Vec::new();
    for tensor in tensors {
        if tensor.name == "token_embd.weight" && (first_stage || final_stage && !has_output_weight)
        {
            selected.push(tensor.clone());
            continue;
        }
        if final_stage && matches!(tensor.name.as_str(), "output.weight" | "output_norm.weight") {
            selected.push(tensor.clone());
            continue;
        }
        if let Some(layer) = tensor_layer_index(&tensor.name)
            && layer_start <= layer
            && layer < layer_end_exclusive
        {
            selected.push(tensor.clone());
        }
    }
    if selected.is_empty() {
        return Err(format!(
            "stage {stage_index} selected no tensors for layer range {layer_start}..{layer_end_exclusive}"
        ));
    }
    selected.sort_by_key(|tensor| tensor.source_offset);
    Ok(selected)
}

fn tensor_layer_index(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("blk.")?;
    let (raw, _) = rest.split_once('.')?;
    raw.parse().ok()
}

fn merge_tensor_ranges(
    data_start: u64,
    tensors: &[GgufTensorEntry],
) -> Result<Vec<ByteRange>, String> {
    let mut ranges = tensors
        .iter()
        .map(|tensor| {
            let start = data_start
                .checked_add(tensor.source_offset)
                .ok_or_else(|| format!("range start overflow for tensor {}", tensor.name))?;
            Ok(ByteRange {
                start,
                len: tensor.byte_len,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<ByteRange> = Vec::new();
    for range in ranges {
        if range.len == 0 {
            continue;
        }
        let range_end = range
            .end_exclusive()
            .ok_or_else(|| format!("range end overflow at {}", range.start))?;
        if let Some(last) = merged.last_mut() {
            let last_end = last
                .end_exclusive()
                .ok_or_else(|| format!("range end overflow at {}", last.start))?;
            if range.start <= last_end {
                last.len = range_end.saturating_sub(last.start).max(last.len);
                continue;
            }
        }
        merged.push(range);
    }
    Ok(merged)
}

fn shard_cache_key(
    source: &GgufSource,
    stage_index: u32,
    stage_count: u32,
    layer_start: u32,
    layer_end_exclusive: u32,
    tensors: &[GgufTensorEntry],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(format!("format:{STAGE_SHARD_CACHE_FORMAT_VERSION}\n").as_bytes());
    hasher.update(format!("source:{source:?}\n").as_bytes());
    hasher.update(
        format!("stage:{stage_index}/{stage_count}:{layer_start}-{layer_end_exclusive}\n")
            .as_bytes(),
    );
    for tensor in tensors {
        hasher.update(
            format!(
                "{}:{}:{}:{:?}\n",
                tensor.name, tensor.source_offset, tensor.byte_len, tensor.dims
            )
            .as_bytes(),
        );
    }
    hasher.finalize().to_hex()[..24].to_owned()
}

pub(crate) fn materialize_stage_shard_http<F>(
    plan: &StageShardPlan,
    output_path: &Path,
    emit: F,
) -> Result<(), String>
where
    F: FnMut(serde_json::Value),
{
    let url = source_url(&plan.source)?;
    materialize_stage_shard_from_url(plan, &url, output_path, emit)
}

pub(crate) fn materialize_stage_shard_from_url<F>(
    plan: &StageShardPlan,
    url: &str,
    output_path: &Path,
    mut emit: F,
) -> Result<(), String>
where
    F: FnMut(serde_json::Value),
{
    let bytes_total = plan.planned_fetch_bytes();
    let range_count = plan.planned_range_count();
    emit(serde_json::json!({
        "type":"StageShardFetchStarted",
        "stage_index":plan.stage_index,
        "url":url,
        "tensor_count":plan.tensors.len(),
        "range_count":range_count,
        "tensor_range_count":plan.merged_tensor_ranges.len(),
        "source_total_bytes":plan.source_total_bytes,
        "bytes_done":0_u64,
        "bytes_total":bytes_total,
        "output_path":output_path,
    }));
    let mut bytes_done = 0_u64;
    let mut next_range_index = 0_usize;
    if plan.metadata_end > 0 {
        emit(serde_json::json!({
            "type":"StageShardRangeFetchStarted",
            "stage_index":plan.stage_index,
            "range_index":next_range_index,
            "range_count":range_count,
            "range_kind":"metadata",
            "source_start":0_u64,
            "bytes":plan.metadata_end,
            "bytes_done":bytes_done,
            "bytes_total":bytes_total,
        }));
    }
    let metadata_prefix = fetch_http_range(&url, 0, plan.metadata_end)?;
    bytes_done = bytes_done.saturating_add(plan.metadata_end);
    if plan.metadata_end > 0 {
        emit(serde_json::json!({
            "type":"StageShardRangeFetchReady",
            "stage_index":plan.stage_index,
            "range_index":next_range_index,
            "range_count":range_count,
            "range_kind":"metadata",
            "source_start":0_u64,
            "bytes":plan.metadata_end,
            "bytes_done":bytes_done,
            "bytes_total":bytes_total,
        }));
        next_range_index += 1;
    }
    if metadata_prefix.len() < 24 {
        return Err(format!(
            "GGUF metadata prefix too short: {} bytes",
            metadata_prefix.len()
        ));
    }
    let metadata_body = &metadata_prefix[24..];
    let alignment = u64::from(plan.alignment.max(1));
    let data_offsets = stage_shard_data_offsets(plan, alignment)?;

    let partial_path = partial_stage_shard_path(output_path);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create stage shard cache dir {}: {e}", parent.display()))?;
    }
    let mut out = File::create(&partial_path)
        .map_err(|e| format!("create stage shard {}: {e}", partial_path.display()))?;
    write_stage_shard_header(&mut out, plan, metadata_body, &data_offsets, alignment)?;
    let mut written_data = 0_u64;
    let mut tensor_index = 0_usize;
    for range in &plan.merged_tensor_ranges {
        let range_index = next_range_index;
        emit(serde_json::json!({
            "type":"StageShardRangeFetchStarted",
            "stage_index":plan.stage_index,
            "range_index":range_index,
            "range_count":range_count,
            "range_kind":"tensor_data",
            "source_start":range.start,
            "bytes":range.len,
            "bytes_done":bytes_done,
            "bytes_total":bytes_total,
        }));
        let range_bytes = fetch_http_range(&url, range.start, range.len)?;
        bytes_done = bytes_done.saturating_add(range.len);
        emit(serde_json::json!({
            "type":"StageShardRangeFetchReady",
            "stage_index":plan.stage_index,
            "range_index":range_index,
            "range_count":range_count,
            "range_kind":"tensor_data",
            "source_start":range.start,
            "bytes":range.len,
            "bytes_done":bytes_done,
            "bytes_total":bytes_total,
        }));
        let range_end = range
            .end_exclusive()
            .ok_or_else(|| format!("stage shard range end overflow at {}", range.start))?;
        while let Some(tensor) = plan.tensors.get(tensor_index) {
            let source_start = plan
                .data_start
                .checked_add(tensor.source_offset)
                .ok_or_else(|| format!("source range overflow for {}", tensor.name))?;
            if source_start >= range_end {
                break;
            }
            let source_end = source_start
                .checked_add(tensor.byte_len)
                .ok_or_else(|| format!("source range overflow for {}", tensor.name))?;
            if source_start < range.start || source_end > range_end {
                return Err(format!(
                    "tensor {} source range {source_start}..{source_end} is not covered by planned range {}..{range_end}",
                    tensor.name, range.start
                ));
            }
            let target_offset = data_offsets
                .get(tensor_index)
                .copied()
                .ok_or_else(|| format!("missing target offset for {}", tensor.name))?;
            while written_data < target_offset {
                out.write_all(&[0])
                    .map_err(|e| format!("pad tensor data before {}: {e}", tensor.name))?;
                written_data += 1;
            }
            emit(serde_json::json!({
                "type":"StageShardTensorFetchStarted",
                "stage_index":plan.stage_index,
                "range_index":range_index,
                "range_count":range_count,
                "tensor_index":tensor_index,
                "tensor_count":plan.tensors.len(),
                "tensor":tensor.name,
                "source_start":source_start,
                "bytes":tensor.byte_len,
                "bytes_done":bytes_done,
                "bytes_total":bytes_total,
            }));
            let offset = usize::try_from(source_start - range.start)
                .map_err(|_| format!("tensor {} range offset exceeds usize", tensor.name))?;
            let len = usize::try_from(tensor.byte_len)
                .map_err(|_| format!("tensor {} byte length exceeds usize", tensor.name))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| format!("tensor {} range slice overflows", tensor.name))?;
            out.write_all(&range_bytes[offset..end])
                .map_err(|e| format!("write tensor {} bytes: {e}", tensor.name))?;
            written_data = written_data
                .checked_add(tensor.byte_len)
                .ok_or_else(|| format!("written data overflow after {}", tensor.name))?;
            emit(serde_json::json!({
                "type":"StageShardTensorFetchReady",
                "stage_index":plan.stage_index,
                "range_index":range_index,
                "range_count":range_count,
                "tensor_index":tensor_index,
                "tensor_count":plan.tensors.len(),
                "tensor":tensor.name,
                "bytes":tensor.byte_len,
                "bytes_done":bytes_done,
                "bytes_total":bytes_total,
            }));
            tensor_index += 1;
        }
        next_range_index += 1;
    }
    if tensor_index != plan.tensors.len() {
        return Err(format!(
            "planned ranges covered {tensor_index} of {} stage tensors",
            plan.tensors.len()
        ));
    }
    out.flush()
        .map_err(|e| format!("flush stage shard {}: {e}", partial_path.display()))?;
    drop(out);
    std::fs::rename(&partial_path, output_path).map_err(|e| {
        format!(
            "commit stage shard {} -> {}: {e}",
            partial_path.display(),
            output_path.display()
        )
    })?;
    emit(serde_json::json!({
        "type":"StageShardReady",
        "stage_index":plan.stage_index,
        "path":output_path,
        "range_count":range_count,
        "tensor_count":plan.tensors.len(),
        "bytes":std::fs::metadata(output_path).map(|metadata| metadata.len()).unwrap_or(0),
        "bytes_done":bytes_done,
        "bytes_total":bytes_total,
    }));
    Ok(())
}

fn stage_shard_data_offsets(plan: &StageShardPlan, alignment: u64) -> Result<Vec<u64>, String> {
    let mut offsets = Vec::with_capacity(plan.tensors.len());
    let mut cursor = 0_u64;
    for tensor in &plan.tensors {
        cursor = align_to(cursor, alignment)?;
        offsets.push(cursor);
        cursor = cursor
            .checked_add(tensor.byte_len)
            .ok_or_else(|| format!("stage shard data size overflow at tensor {}", tensor.name))?;
    }
    Ok(offsets)
}

fn partial_stage_shard_path(output_path: &Path) -> PathBuf {
    output_path.with_extension(format!(
        "{}partial",
        output_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|ext| format!("{ext}."))
            .unwrap_or_default()
    ))
}

fn write_stage_shard_header(
    out: &mut File,
    plan: &StageShardPlan,
    metadata_body: &[u8],
    data_offsets: &[u64],
    alignment: u64,
) -> Result<(), String> {
    out.write_all(GGUF_MAGIC)
        .map_err(|e| format!("write stage shard magic: {e}"))?;
    out.write_all(&SUPPORTED_GGUF_VERSION.to_le_bytes())
        .map_err(|e| format!("write stage shard version: {e}"))?;
    out.write_all(&(plan.tensors.len() as u64).to_le_bytes())
        .map_err(|e| format!("write stage shard tensor count: {e}"))?;
    out.write_all(&plan.metadata_count.to_le_bytes())
        .map_err(|e| format!("write stage shard metadata count: {e}"))?;
    out.write_all(metadata_body)
        .map_err(|e| format!("write stage shard metadata: {e}"))?;
    for (tensor, data_offset) in plan.tensors.iter().zip(data_offsets.iter().copied()) {
        write_gguf_string(out, &tensor.name)?;
        out.write_all(&(tensor.dims.len() as u32).to_le_bytes())
            .map_err(|e| format!("write tensor dim count for {}: {e}", tensor.name))?;
        for dim in &tensor.dims {
            out.write_all(&dim.to_le_bytes())
                .map_err(|e| format!("write tensor dim for {}: {e}", tensor.name))?;
        }
        out.write_all(&tensor.ggml_type.to_le_bytes())
            .map_err(|e| format!("write tensor type for {}: {e}", tensor.name))?;
        out.write_all(&data_offset.to_le_bytes())
            .map_err(|e| format!("write tensor offset for {}: {e}", tensor.name))?;
    }
    pad_writer_to_alignment(out, alignment)
}

fn fetch_http_range(url: &str, start: u64, len: u64) -> Result<Vec<u8>, String> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let end = start
        .checked_add(len - 1)
        .ok_or_else(|| format!("HTTP range overflow at {start}+{len}"))?;
    let range = format!("bytes={start}-{end}");
    let mut request = ureq::get(url)
        .set("Range", &range)
        .set("User-Agent", "myelin-node/0.1");
    if let Ok(token) = std::env::var("HF_TOKEN")
        && !token.trim().is_empty()
    {
        request = request.set("Authorization", &format!("Bearer {}", token.trim()));
    }
    let response = request
        .call()
        .map_err(|error| format!("GET {url} {range}: {error}"))?;
    if response.status() != 206 {
        return Err(format!(
            "GET {url} {range} returned HTTP {}; refusing full-body fallback",
            response.status()
        ));
    }
    let mut reader = response.into_reader();
    let capacity = usize::try_from(len).unwrap_or(usize::MAX.min(64 * 1024 * 1024));
    let mut bytes = Vec::with_capacity(capacity.min(64 * 1024 * 1024));
    reader
        .read_to_end(&mut bytes)
        .map_err(|e| format!("read {url} {range}: {e}"))?;
    if bytes.len() as u64 != len {
        return Err(format!(
            "GET {url} {range} returned {} bytes, expected {len}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

fn write_gguf_string<W: Write>(writer: &mut W, value: &str) -> Result<(), String> {
    writer
        .write_all(&(value.len() as u64).to_le_bytes())
        .map_err(|e| format!("write GGUF string len: {e}"))?;
    writer
        .write_all(value.as_bytes())
        .map_err(|e| format!("write GGUF string bytes: {e}"))
}

fn pad_writer_to_alignment<W: Write + Seek>(writer: &mut W, alignment: u64) -> Result<(), String> {
    let pos = writer
        .stream_position()
        .map_err(|e| format!("locate writer for alignment: {e}"))?;
    let aligned = align_to(pos, alignment)?;
    for _ in pos..aligned {
        writer
            .write_all(&[0])
            .map_err(|e| format!("write alignment padding: {e}"))?;
    }
    Ok(())
}

fn align_to(value: u64, alignment: u64) -> Result<u64, String> {
    if alignment == 0 {
        return Err("GGUF alignment must be non-zero".to_owned());
    }
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| format!("align {value} to {alignment} overflows"))
    }
}
