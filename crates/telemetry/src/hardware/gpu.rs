use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::record::Record;

pub const HOST_GPU_CHANNEL: &str = "host.gpu";
pub const GPU_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

const SCHEMA: &str = "host.gpu.v1";
const GPU_QUERY_ARGS: &[&str] = &[
    "--query-gpu=index,uuid,name,memory.used,memory.total,utilization.gpu,utilization.memory,temperature.gpu,power.draw",
    "--format=csv,noheader,nounits",
];
const PROCESS_QUERY_ARGS: &[&str] = &[
    "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
    "--format=csv,noheader,nounits",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostGpuSample {
    pub schema: String,
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub query_elapsed_ms: Option<u64>,
    pub gpus: Vec<GpuDeviceSample>,
    pub processes: Vec<GpuProcessSample>,
    pub error: Option<String>,
}

impl HostGpuSample {
    pub fn error(seq: u64, error: impl Into<String>) -> Self {
        Self {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms: unix_ms_now(),
            query_elapsed_ms: None,
            gpus: Vec::new(),
            processes: Vec::new(),
            error: Some(error.into()),
        }
    }
}

impl Record for HostGpuSample {
    const CHANNEL: &'static str = HOST_GPU_CHANNEL;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuDeviceSample {
    pub index: Option<u32>,
    pub uuid: String,
    pub name: String,
    pub memory_used_mib: Option<u64>,
    pub memory_total_mib: Option<u64>,
    pub utilization_gpu_percent: Option<u64>,
    pub utilization_memory_percent: Option<u64>,
    pub temperature_c: Option<i64>,
    pub power_draw_w: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuProcessSample {
    pub gpu_uuid: String,
    pub pid: Option<u32>,
    pub process_name: String,
    pub used_memory_mib: Option<u64>,
}

pub fn sample(seq: u64) -> HostGpuSample {
    let started = Instant::now();
    let sample_unix_ms = unix_ms_now();

    let gpu_output = match run_nvidia_smi(GPU_QUERY_ARGS) {
        Ok(output) => output,
        Err(error) => return HostGpuSample::error(seq, error),
    };

    if !gpu_output.success {
        return HostGpuSample {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms,
            query_elapsed_ms: Some(elapsed_ms(started)),
            gpus: Vec::new(),
            processes: Vec::new(),
            error: Some(format_query_error("gpu", &gpu_output)),
        };
    }

    let gpus = parse_gpu_rows(&gpu_output.stdout);
    let process_output = run_nvidia_smi(PROCESS_QUERY_ARGS);
    let mut error = None;
    let processes = match process_output {
        Ok(output) if output.success => parse_process_rows(&output.stdout),
        Ok(output) => {
            error = Some(format_query_error("process", &output));
            Vec::new()
        }
        Err(err) => {
            error = Some(err);
            Vec::new()
        }
    };

    HostGpuSample {
        schema: SCHEMA.to_string(),
        seq,
        sample_unix_ms,
        query_elapsed_ms: Some(elapsed_ms(started)),
        gpus,
        processes,
        error,
    }
}

#[derive(Debug)]
struct QueryOutput {
    success: bool,
    stdout: String,
    stderr: String,
    status: Option<i32>,
}

fn run_nvidia_smi(args: &[&str]) -> Result<QueryOutput, String> {
    let output = Command::new("nvidia-smi")
        .args(args)
        .output()
        .map_err(|err| format!("run nvidia-smi: {err}"))?;
    Ok(QueryOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        status: output.status.code(),
    })
}

fn format_query_error(name: &str, output: &QueryOutput) -> String {
    match (&output.stderr, output.status) {
        (stderr, Some(status)) if !stderr.is_empty() => {
            format!("{name} query failed with status {status}: {stderr}")
        }
        (_, Some(status)) => format!("{name} query failed with status {status}"),
        (stderr, None) if !stderr.is_empty() => format!("{name} query terminated: {stderr}"),
        (_, None) => format!("{name} query terminated"),
    }
}

fn parse_gpu_rows(raw: &str) -> Vec<GpuDeviceSample> {
    raw.lines()
        .filter_map(|line| {
            let fields = split_csv_line(line);
            if fields.len() < 9 {
                return None;
            }
            Some(GpuDeviceSample {
                index: parse_u32(&fields[0]),
                uuid: fields[1].to_string(),
                name: fields[2].to_string(),
                memory_used_mib: parse_u64(&fields[3]),
                memory_total_mib: parse_u64(&fields[4]),
                utilization_gpu_percent: parse_u64(&fields[5]),
                utilization_memory_percent: parse_u64(&fields[6]),
                temperature_c: parse_i64(&fields[7]),
                power_draw_w: parse_f64(&fields[8]),
            })
        })
        .collect()
}

fn parse_process_rows(raw: &str) -> Vec<GpuProcessSample> {
    raw.lines()
        .filter_map(|line| {
            let fields = split_csv_line(line);
            if fields.len() < 4 {
                return None;
            }
            Some(GpuProcessSample {
                gpu_uuid: fields[0].to_string(),
                pid: parse_u32(&fields[1]),
                process_name: fields[2].to_string(),
                used_memory_mib: parse_u64(&fields[3]),
            })
        })
        .collect()
}

fn split_csv_line(line: &str) -> Vec<String> {
    line.split(',')
        .map(|field| field.trim().to_string())
        .collect()
}

fn parse_u32(value: &str) -> Option<u32> {
    parse_f64(value).and_then(|parsed| {
        if parsed.is_finite() && parsed >= 0.0 && parsed <= u32::MAX as f64 {
            Some(parsed as u32)
        } else {
            None
        }
    })
}

fn parse_u64(value: &str) -> Option<u64> {
    parse_f64(value).and_then(|parsed| {
        if parsed.is_finite() && parsed >= 0.0 && parsed <= u64::MAX as f64 {
            Some(parsed as u64)
        } else {
            None
        }
    })
}

fn parse_i64(value: &str) -> Option<i64> {
    parse_f64(value).and_then(|parsed| {
        if parsed.is_finite() && parsed >= i64::MIN as f64 && parsed <= i64::MAX as f64 {
            Some(parsed as i64)
        } else {
            None
        }
    })
}

fn parse_f64(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("N/A")
        || trimmed.eq_ignore_ascii_case("[Not Supported]")
        || trimmed.eq_ignore_ascii_case("Not Supported")
    {
        return None;
    }
    trimmed.parse::<f64>().ok()
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gpu_rows() {
        let rows = parse_gpu_rows("0, GPU-abc, NVIDIA RTX 4090, 8120, 24564, 73, 41, 61, 212.40\n");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].index, Some(0));
        assert_eq!(rows[0].uuid, "GPU-abc");
        assert_eq!(rows[0].name, "NVIDIA RTX 4090");
        assert_eq!(rows[0].memory_used_mib, Some(8120));
        assert_eq!(rows[0].memory_total_mib, Some(24564));
        assert_eq!(rows[0].utilization_gpu_percent, Some(73));
        assert_eq!(rows[0].utilization_memory_percent, Some(41));
        assert_eq!(rows[0].temperature_c, Some(61));
        assert_eq!(rows[0].power_draw_w, Some(212.40));
    }

    #[test]
    fn parses_process_rows() {
        let rows = parse_process_rows("GPU-abc, 1234, python3, 7988\n");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].gpu_uuid, "GPU-abc");
        assert_eq!(rows[0].pid, Some(1234));
        assert_eq!(rows[0].process_name, "python3");
        assert_eq!(rows[0].used_memory_mib, Some(7988));
    }

    #[test]
    fn unsupported_values_parse_to_none() {
        let rows = parse_gpu_rows(
            "0, GPU-abc, NVIDIA Test, N/A, [Not Supported], 0, 0, N/A, [Not Supported]\n",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].memory_used_mib, None);
        assert_eq!(rows[0].memory_total_mib, None);
        assert_eq!(rows[0].temperature_c, None);
        assert_eq!(rows[0].power_draw_w, None);
    }

    #[test]
    fn sample_error_uses_host_gpu_schema() {
        let sample = HostGpuSample::error(9, "no gpu");

        assert_eq!(HostGpuSample::CHANNEL, "host.gpu");
        assert_eq!(sample.schema, "host.gpu.v1");
        assert_eq!(sample.seq, 9);
        assert_eq!(sample.error, Some("no gpu".to_string()));
    }
}
