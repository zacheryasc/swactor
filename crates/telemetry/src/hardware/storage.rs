use std::ffi::CString;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::pressure::{self, PressureSample};
use crate::record::Record;

pub const HOST_STORAGE_CHANNEL: &str = "host.storage";
pub const STORAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

const SCHEMA: &str = "host.storage.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostStorageSample {
    pub schema: String,
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub query_elapsed_ms: Option<u64>,
    pub filesystems: Vec<FilesystemSample>,
    pub pressure: Option<PressureSample>,
    pub error: Option<String>,
}

impl Record for HostStorageSample {
    const CHANNEL: &'static str = HOST_STORAGE_CHANNEL;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FilesystemSample {
    pub mount: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: Option<f64>,
}

pub fn sample(seq: u64) -> HostStorageSample {
    let started = Instant::now();
    match read_filesystem("/") {
        Ok(filesystem) => HostStorageSample {
            schema: SCHEMA.to_owned(),
            seq,
            sample_unix_ms: unix_ms_now(),
            query_elapsed_ms: Some(elapsed_ms(started)),
            filesystems: vec![filesystem],
            pressure: pressure::read("io").ok(),
            error: None,
        },
        Err(error) => HostStorageSample {
            schema: SCHEMA.to_owned(),
            seq,
            sample_unix_ms: unix_ms_now(),
            query_elapsed_ms: Some(elapsed_ms(started)),
            filesystems: Vec::new(),
            pressure: pressure::read("io").ok(),
            error: Some(error),
        },
    }
}

#[cfg(target_os = "linux")]
fn read_filesystem(mount: &str) -> Result<FilesystemSample, String> {
    let path = CString::new(mount).map_err(|error| format!("filesystem path: {error}"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a live NUL-terminated string and `stats` points to writable storage.
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "statvfs {mount}: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: successful `statvfs` initialized the output structure.
    let stats = unsafe { stats.assume_init() };
    let fragment_size = stats.f_frsize;
    let total_bytes = stats.f_blocks.saturating_mul(fragment_size);
    let free_bytes = stats.f_bfree.saturating_mul(fragment_size);
    let available_bytes = stats.f_bavail.saturating_mul(fragment_size);
    let used_bytes = total_bytes.saturating_sub(free_bytes);
    let used_percent = (total_bytes > 0).then_some(used_bytes as f64 * 100.0 / total_bytes as f64);
    Ok(FilesystemSample {
        mount: mount.to_owned(),
        total_bytes,
        used_bytes,
        available_bytes,
        used_percent,
    })
}

#[cfg(not(target_os = "linux"))]
fn read_filesystem(mount: &str) -> Result<FilesystemSample, String> {
    Err(format!("filesystem sampling unsupported for {mount}"))
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::sample;

    #[test]
    fn samples_root_filesystem_capacity() {
        let sample = sample(9);
        assert_eq!(sample.seq, 9);
        let root = sample.filesystems.first().expect("root filesystem");
        assert_eq!(root.mount, "/");
        assert!(root.total_bytes > 0);
        assert!(root.used_bytes <= root.total_bytes);
        assert!(sample.error.is_none());
    }
}
