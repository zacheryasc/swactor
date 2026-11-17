//! Immutable, digest-verified objects and crash-explicit append-only journals.
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use crate::budget::{ExecutionStage, WorkKind, execution_span};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub digest: String,
    pub length: u64,
}

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    pub fn new(root: &Path) -> Result<Self, String> {
        let objects = root.join("objects");
        fs::create_dir_all(&objects).map_err(|e| format!("create evidence objects: {e}"))?;
        sync_directory(root)?;
        Ok(Self {
            root: root.to_owned(),
        })
    }

    fn path(&self, object: &ObjectRef) -> Result<PathBuf, String> {
        if object.digest.len() != 64 || !object.digest.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid evidence digest".to_owned());
        }
        Ok(self
            .root
            .join("objects")
            .join(format!("{}.gz", object.digest)))
    }

    pub fn put_bytes(&self, bytes: &[u8]) -> Result<ObjectRef, String> {
        let _span = execution_span(
            ExecutionStage::EvidenceSerialization,
            WorkKind::RunnerOverhead,
        );
        let object = ObjectRef {
            digest: format!("{:x}", Sha256::digest(bytes)),
            length: bytes.len() as u64,
        };
        let path = self.path(&object)?;
        if path.exists() {
            self.bytes(&object)?;
            return Ok(object);
        }
        let mut temporary = tempfile::NamedTempFile::new_in(self.root.join("objects"))
            .map_err(|e| format!("stage immutable evidence: {e}"))?;
        {
            let mut encoder = GzEncoder::new(temporary.as_file_mut(), Compression::fast());
            encoder
                .write_all(bytes)
                .map_err(|e| format!("compress evidence: {e}"))?;
            encoder
                .finish()
                .map_err(|e| format!("finish evidence compression: {e}"))?;
        }
        let _sync = execution_span(
            ExecutionStage::EvidenceSynchronization,
            WorkKind::RunnerOverhead,
        );
        temporary
            .as_file()
            .sync_all()
            .map_err(|e| format!("sync immutable evidence: {e}"))?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.bytes(&object)?;
            }
            Err(error) => return Err(format!("commit immutable evidence: {}", error.error)),
        }
        sync_directory(&self.root.join("objects"))?;
        Ok(object)
    }

    pub fn bytes(&self, object: &ObjectRef) -> Result<Vec<u8>, String> {
        let source = File::open(self.path(object)?)
            .map_err(|e| format!("open evidence {}: {e}", object.digest))?;
        let mut bytes = Vec::new();
        GzDecoder::new(source)
            .take(
                object
                    .length
                    .checked_add(1)
                    .ok_or("evidence length overflow")?,
            )
            .read_to_end(&mut bytes)
            .map_err(|e| format!("decode evidence {}: {e}", object.digest))?;
        if bytes.len() as u64 != object.length
            || format!("{:x}", Sha256::digest(&bytes)) != object.digest
        {
            return Err(format!(
                "immutable evidence {} failed digest/length verification",
                object.digest
            ));
        }
        Ok(bytes)
    }

    pub fn put<T: Serialize>(&self, value: &T) -> Result<ObjectRef, String> {
        let _span = execution_span(
            ExecutionStage::EvidenceSerialization,
            WorkKind::RunnerOverhead,
        );
        let mut value = serde_json::to_value(value).map_err(|e| format!("encode evidence: {e}"))?;
        self.pack(&mut value)?;
        self.put_bytes(&serde_json::to_vec(&value).map_err(|e| e.to_string())?)
    }

    pub fn get<T: DeserializeOwned>(&self, object: &ObjectRef) -> Result<T, String> {
        let mut value: Value =
            serde_json::from_slice(&self.bytes(object)?).map_err(|e| e.to_string())?;
        self.unpack(&mut value)?;
        serde_json::from_value(value).map_err(|e| format!("decode typed evidence: {e}"))
    }

    fn pack(&self, value: &mut Value) -> Result<(), String> {
        match value {
            Value::Array(items)
                if items.len() >= 64
                    && items.iter().all(|v| v.as_u64().is_some_and(|n| n <= 255)) =>
            {
                let bytes: Vec<u8> = items.iter().map(|v| v.as_u64().unwrap() as u8).collect();
                let digest = format!("{:x}", Sha256::digest(&bytes));
                let period = (1..=32.min(bytes.len()))
                    .find(|&p| bytes.iter().enumerate().all(|(i, b)| *b == bytes[i % p]));
                *value = if let Some(period) = period {
                    json!({"$e2e_payload": {"digest": digest, "length": bytes.len(), "period": &bytes[..period]}})
                } else {
                    json!({"$e2e_payload": {"object": self.put_bytes(&bytes)?}})
                };
            }
            Value::Array(items) => {
                for item in items {
                    self.pack(item)?;
                }
            }
            Value::Object(fields) => {
                for item in fields.values_mut() {
                    self.pack(item)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn unpack(&self, value: &mut Value) -> Result<(), String> {
        if let Some(payload) = value.get("$e2e_payload") {
            let bytes = if let Some(reference) = payload.get("object") {
                self.bytes(&serde_json::from_value(reference.clone()).map_err(|e| e.to_string())?)?
            } else {
                let length = payload
                    .get("length")
                    .and_then(Value::as_u64)
                    .ok_or("payload length absent")?;
                // A campaign's admission bound also limits expansion of untrusted artifacts.
                if length > 512 * 1024 * 1024 {
                    return Err("payload exceeds campaign resource bound".to_owned());
                }
                let period: Vec<u8> = serde_json::from_value(
                    payload
                        .get("period")
                        .cloned()
                        .ok_or("payload period absent")?,
                )
                .map_err(|e| e.to_string())?;
                if period.is_empty() || period.len() > 32 {
                    return Err("invalid generated payload period".to_owned());
                }
                let bytes: Vec<u8> = period
                    .iter()
                    .copied()
                    .cycle()
                    .take(length as usize)
                    .collect();
                if payload.get("digest").and_then(Value::as_str)
                    != Some(format!("{:x}", Sha256::digest(&bytes)).as_str())
                {
                    return Err("generated payload digest differs".to_owned());
                }
                bytes
            };
            *value = serde_json::to_value(bytes).map_err(|e| e.to_string())?;
        } else {
            match value {
                Value::Array(items) => {
                    for item in items {
                        self.unpack(item)?;
                    }
                }
                Value::Object(fields) => {
                    for item in fields.values_mut() {
                        self.unpack(item)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Copy the complete recursively referenced closure, not just a manifest.
    pub fn export(&self, object: &ObjectRef, destination: &Store) -> Result<(), String> {
        let bytes = self.bytes(object)?;
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
            self.export_children(&value, destination)?;
        }
        if destination.put_bytes(&bytes)? != *object {
            return Err("export changed evidence identity".to_owned());
        }
        Ok(())
    }

    fn export_children(&self, value: &Value, destination: &Store) -> Result<(), String> {
        if let Ok(reference) = serde_json::from_value::<ObjectRef>(value.clone()) {
            return self.export(&reference, destination);
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    self.export_children(value, destination)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    self.export_children(value, destination)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    sequence: u64,
    previous: String,
    body: Value,
    digest: String,
}

fn frame_digest(sequence: u64, previous: &str, body: &Value) -> Result<String, String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(sequence, previous, body)).map_err(|e| e.to_string())?)
    ))
}
pub struct Journal {
    file: File,
    pub records: Vec<Value>,
    previous: String,
}

impl Journal {
    pub fn open(path: &Path, store: &Store) -> Result<Self, String> {
        let parent = path.parent().ok_or("journal has no parent")?;
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("open progress journal: {e}"))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err("progress journal is already owned by another runner".to_owned());
        }
        sync_directory(parent)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
        let complete = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        let interrupted = if complete != bytes.len() {
            Some(store.put_bytes(&bytes[complete..])?)
        } else {
            None
        };
        let mut records = Vec::new();
        let mut previous = String::new();
        for line in bytes[..complete]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
        {
            let frame: Frame = serde_json::from_slice(line)
                .map_err(|e| format!("corrupt committed journal frame: {e}"))?;
            if frame.sequence != records.len() as u64
                || frame.previous != previous
                || frame.digest != frame_digest(frame.sequence, &frame.previous, &frame.body)?
            {
                return Err("journal sequence/hash chain is corrupt".to_owned());
            }
            previous = frame.digest;
            records.push(frame.body);
        }
        if interrupted.is_some() {
            file.set_len(complete as u64).map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
        }
        file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
        let mut journal = Self {
            file,
            records,
            previous,
        };
        if let Some(tail) = interrupted {
            journal.append(json!({"kind":"crash_tail", "discarded_uncommitted_bytes":tail}))?;
        }
        Ok(journal)
    }

    pub fn append(&mut self, body: Value) -> Result<(), String> {
        let _span = execution_span(
            ExecutionStage::EvidenceSynchronization,
            WorkKind::RunnerOverhead,
        );
        let sequence = self.records.len() as u64;
        let digest = frame_digest(sequence, &self.previous, &body)?;
        let frame = Frame {
            sequence,
            previous: self.previous.clone(),
            body: body.clone(),
            digest: digest.clone(),
        };
        let mut bytes = serde_json::to_vec(&frame).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
            .map_err(|e| format!("commit progress journal: {e}"))?;
        self.previous = digest;
        self.records.push(body);
        Ok(())
    }

    /// Explicit boundary compaction is an immutable snapshot plus a durable
    /// journal marker. The original journal remains crash-recoverable evidence.
    pub fn compact<T: Serialize>(&mut self, store: &Store, value: &T) -> Result<ObjectRef, String> {
        let _span = execution_span(ExecutionStage::EvidenceCompaction, WorkKind::RunnerOverhead);
        let snapshot = store.put(value)?;
        self.append(json!({"kind":"compaction", "through_sequence":self.records.len(), "through_digest":self.previous, "snapshot":snapshot}))?;
        Ok(snapshot)
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| format!("sync evidence directory: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupted_tail_is_preserved_and_never_accepted_as_committed() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::new(&temporary.path().join("evidence")).unwrap();
        let journal_path = temporary.path().join("progress.jsonl");
        let mut journal = Journal::open(&journal_path, &store).unwrap();
        journal
            .append(json!({"kind":"case", "id":"case-1"}))
            .unwrap();
        drop(journal);

        let uncommitted = br#"{"sequence":1,"previous":"partial"#;
        let mut file = OpenOptions::new().append(true).open(&journal_path).unwrap();
        file.write_all(uncommitted).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let recovered = Journal::open(&journal_path, &store).unwrap();
        assert_eq!(recovered.records[0], json!({"kind":"case", "id":"case-1"}));
        let tail: ObjectRef =
            serde_json::from_value(recovered.records[1]["discarded_uncommitted_bytes"].clone())
                .unwrap();
        assert_eq!(store.bytes(&tail).unwrap(), uncommitted);
        drop(recovered);

        let reopened = Journal::open(&journal_path, &store).unwrap();
        assert_eq!(reopened.records.len(), 2);
    }

    #[test]
    fn corrupt_newline_terminated_frame_fails_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::new(&temporary.path().join("evidence")).unwrap();
        let journal_path = temporary.path().join("progress.jsonl");
        fs::write(&journal_path, b"not-a-committed-frame\n").unwrap();
        assert!(Journal::open(&journal_path, &store).is_err());
    }
}
