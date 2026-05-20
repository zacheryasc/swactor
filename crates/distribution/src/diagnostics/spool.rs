//! On-disk FIFO spool for the [`HttpSink`].
//!
//! When the collector is unreachable, batches are written here so the
//! events survive process restarts and reappear at the collector once
//! it recovers. Each entry is a single POST body, named with a
//! monotonic sequence so lexical sort matches FIFO delivery.
//!
//! Layout under `{spool_dir}/{run_id}/`:
//!
//! ```text
//! 000000001-events.bin
//! 000000002-events.bin
//! 000000003-snapshot.bin
//! 000000004-finalize.bin
//! ```
//!
//! Drainage is the [`HttpSink`]'s responsibility — this module only
//! reads and writes.
//!
//! [`HttpSink`]: super::sink::HttpSink

#[cfg(feature = "collector")]
use std::path::{Path, PathBuf};
#[cfg(feature = "collector")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "collector")]
use crate::diagnostics::collector::protocol::RecordKind;

/// A FIFO spool rooted at `{spool_dir}/{run_id}/`. Cloneable so the
/// drainer can move it into its task.
#[cfg(feature = "collector")]
#[derive(Debug)]
pub struct Spool {
    dir: PathBuf,
    next_seq: AtomicU64,
}

#[cfg(feature = "collector")]
#[derive(Debug, Clone)]
pub struct SpoolEntry {
    pub seq: u64,
    pub kind: RecordKind,
    pub path: PathBuf,
}

#[cfg(feature = "collector")]
impl Spool {
    /// Synchronous open used at sink construction time (we want errors
    /// up front rather than spread across the async drainer's path).
    /// Creates the spool directory if missing and scans for the
    /// highest existing seq so a restart preserves ordering.
    pub fn open_sync(spool_dir: &Path, run_id: &str) -> std::io::Result<Self> {
        let dir = spool_dir.join(sanitize(run_id));
        std::fs::create_dir_all(&dir)?;
        let mut max_seq = 0u64;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((seq, _)) = parse_name(&name) {
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
        Ok(Self {
            dir,
            next_seq: AtomicU64::new(max_seq),
        })
    }

    /// Directory this spool writes to. Useful for tests.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Persist a batch body. Returns the path written.
    ///
    /// Best-effort durability: writes the bytes, then renames into
    /// place. A crash mid-write loses the in-flight entry but never
    /// corrupts the visible spool.
    pub async fn append(&self, kind: RecordKind, body: &[u8]) -> std::io::Result<PathBuf> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let final_name = format!("{:09}-{}.bin", seq, kind.as_str());
        let tmp_name = format!("{:09}-{}.bin.tmp", seq, kind.as_str());
        let tmp_path = self.dir.join(&tmp_name);
        let final_path = self.dir.join(&final_name);
        tokio::fs::write(&tmp_path, body).await?;
        tokio::fs::rename(&tmp_path, &final_path).await?;
        Ok(final_path)
    }

    /// List spooled entries in FIFO (seq-ascending) order.
    pub async fn list(&self) -> std::io::Result<Vec<SpoolEntry>> {
        let mut entries = Vec::new();
        let mut iter = tokio::fs::read_dir(&self.dir).await?;
        while let Some(entry) = iter.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((seq, kind)) = parse_name(&name) {
                entries.push(SpoolEntry {
                    seq,
                    kind,
                    path: entry.path(),
                });
            }
        }
        entries.sort_by_key(|e| e.seq);
        Ok(entries)
    }
}

#[cfg(feature = "collector")]
fn parse_name(name: &str) -> Option<(u64, RecordKind)> {
    let stem = name.strip_suffix(".bin")?;
    let (seq_str, kind_str) = stem.split_once('-')?;
    let seq: u64 = seq_str.parse().ok()?;
    let kind = RecordKind::parse(kind_str)?;
    Some((seq, kind))
}

#[cfg(feature = "collector")]
fn sanitize(s: &str) -> String {
    if s.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out == "." || out == ".." {
        return "_".to_string();
    }
    out
}

#[cfg(all(test, feature = "collector"))]
mod tests {
    use super::*;

    fn tmp(label: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "swactor-spool-test-{}-{}-{}",
            std::process::id(),
            label,
            n
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn append_then_list_is_fifo_ordered() {
        let root = tmp("fifo");
        let spool = Spool::open_sync(&root, "run-x").unwrap();
        spool.append(RecordKind::Events, b"a").await.unwrap();
        spool.append(RecordKind::Snapshot, b"b").await.unwrap();
        spool.append(RecordKind::Events, b"c").await.unwrap();
        let entries = spool.list().await.unwrap();
        let seqs: Vec<u64> = entries.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(entries[1].kind, RecordKind::Snapshot);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn reopen_continues_seq_from_highest_existing() {
        let root = tmp("reopen");
        let spool = Spool::open_sync(&root, "run-x").unwrap();
        spool.append(RecordKind::Events, b"a").await.unwrap();
        spool.append(RecordKind::Events, b"b").await.unwrap();
        drop(spool);
        let reopened = Spool::open_sync(&root, "run-x").unwrap();
        let path = reopened.append(RecordKind::Events, b"c").await.unwrap();
        assert!(
            path.file_name().unwrap().to_string_lossy().starts_with("000000003"),
            "expected next seq to be 3, got {path:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn sanitize_keeps_safe_chars_and_blocks_dots() {
        assert_eq!(sanitize("good_run-1"), "good_run-1");
        assert_eq!(sanitize(""), "_");
        assert_eq!(sanitize("."), "_");
        assert_eq!(sanitize(".."), "_");
        assert_eq!(sanitize("a/b"), "a_b");
    }
}
