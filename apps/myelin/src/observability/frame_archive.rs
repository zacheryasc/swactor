use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use telemetry::frame::{Frame, StreamId};
use serde_json::json;

use crate::observability::benchmark;

/// JSONL archive for Myelin-owned telemetry frames.
///
/// The telemetry crate owns frame transport; this helper owns the Myelin archive
/// record shape used as benchmark and contract evidence.
pub(crate) struct FrameArchive {
    file: File,
    next_seq: u64,
    path: PathBuf,
    label: &'static str,
}

impl FrameArchive {
    pub(crate) fn open_with_label(path: &Path, label: &'static str) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create {label} dir {}: {e}", parent.display()))?;
        }
        let next_seq = match File::open(path) {
            Ok(file) => BufReader::new(file).lines().count() as u64,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(format!("read {label} {}: {error}", path.display())),
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("open {label} {}: {e}", path.display()))?;
        Ok(Self {
            file,
            next_seq,
            path: path.to_path_buf(),
            label,
        })
    }

    pub(crate) fn record(
        &mut self,
        source: &str,
        stream: &StreamId,
        channel: &str,
        frame: &Frame,
    ) -> Result<(), String> {
        let payload = match std::str::from_utf8(&frame.payload) {
            Ok(text) => json!({"encoding": "utf8", "value": text}),
            Err(_) => json!({"encoding": "bytes", "value": frame.payload}),
        };
        let record = json!({
            "arrival_seq": self.next_seq,
            "arrival_unix_ms": benchmark::unix_ms_now(),
            "source": source,
            "stream": stream.to_string(),
            "channel": channel,
            "channel_id": frame.channel.0,
            "position": frame.position.0,
            "payload": payload,
        });
        self.next_seq += 1;
        let mut line =
            serde_json::to_vec(&record).map_err(|e| format!("serialize {}: {e}", self.label))?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .map_err(|e| format!("write {} {}: {e}", self.label, self.path.display()))?;
        self.file
            .flush()
            .map_err(|e| format!("flush {} {}: {e}", self.label, self.path.display()))
    }
}
