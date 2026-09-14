//! Crash-consistent durable state for the virtual blob namespace.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

use crate::namespace::StreamIncarnation;
use crate::path::DataPath;

const SCHEMA_VERSION: u32 = 1;

static TRACE_COMMITS: AtomicU64 = AtomicU64::new(0);
static TRACE_COMMIT_MICROS: AtomicU64 = AtomicU64::new(0);
static TRACE_WORST_COMMIT_MICROS: AtomicU64 = AtomicU64::new(0);
static COMMIT_BYTES: AtomicU64 = AtomicU64::new(0);
static COMMIT_FAILURES: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_BINDINGS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_OPERATIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize)]
pub struct NamespaceCommitMetrics {
    pub schema_version: u32,
    pub attempts: u64,
    pub failures: u64,
    pub total_micros: u64,
    pub worst_micros: u64,
    pub serialized_bytes: u64,
    pub bindings: u64,
    pub operations: u64,
}

/// Process-local attribution only; these counters never decide durability or
/// namespace correctness. Readers need no namespace actor scheduling round.
pub fn commit_metrics() -> NamespaceCommitMetrics {
    NamespaceCommitMetrics {
        schema_version: 1,
        attempts: TRACE_COMMITS.load(Ordering::Relaxed),
        failures: COMMIT_FAILURES.load(Ordering::Relaxed),
        total_micros: TRACE_COMMIT_MICROS.load(Ordering::Relaxed),
        worst_micros: TRACE_WORST_COMMIT_MICROS.load(Ordering::Relaxed),
        serialized_bytes: COMMIT_BYTES.load(Ordering::Relaxed),
        bindings: SNAPSHOT_BINDINGS.load(Ordering::Relaxed),
        operations: SNAPSHOT_OPERATIONS.load(Ordering::Relaxed),
    }
}

fn trace_commit_observed(elapsed: std::time::Duration) {
    let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    TRACE_COMMITS.fetch_add(1, Ordering::Relaxed);
    TRACE_COMMIT_MICROS.fetch_add(micros, Ordering::Relaxed);
    TRACE_WORST_COMMIT_MICROS.fetch_max(micros, Ordering::Relaxed);
}

#[cfg(feature = "directory-trace")]
pub fn trace_commit_count() -> u64 {
    TRACE_COMMITS.load(Ordering::Relaxed)
}

#[cfg(feature = "directory-trace")]
pub fn trace_commit_avg_ms() -> f64 {
    let commits = TRACE_COMMITS.load(Ordering::Relaxed).max(1);
    TRACE_COMMIT_MICROS.load(Ordering::Relaxed) as f64 / commits as f64 / 1000.0
}

#[cfg(feature = "directory-trace")]
pub fn trace_commit_worst_ms() -> f64 {
    TRACE_WORST_COMMIT_MICROS.load(Ordering::Relaxed) as f64 / 1000.0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId([u8; 16]);

impl OperationId {
    pub const fn from_u128(value: u128) -> Self {
        Self(value.to_be_bytes())
    }

    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

impl Serialize for OperationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{:032x}", u128::from_be_bytes(self.0)))
    }
}

impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        if encoded.len() != 32 {
            return Err(serde::de::Error::custom(
                "namespace operation ID must contain 32 hexadecimal digits",
            ));
        }
        u128::from_str_radix(&encoded, 16)
            .map(Self::from_u128)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceRecovery {
    File {
        path: PathBuf,
    },
    Actor {
        actor: ActorAddress,
        #[serde(default)]
        node: [u8; 32],
        #[serde(default)]
        owner: Option<ActorAddress>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedBinding {
    pub length: u64,
    pub revision: u64,
    pub recovery: SourceRecovery,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationRequest {
    Register {
        path: DataPath,
        length: u64,
        recovery: SourceRecovery,
    },
    BindStream {
        path: DataPath,
    },
    Unregister {
        path: DataPath,
    },
    Rename {
        source: DataPath,
        destination: DataPath,
        replace: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationRejection {
    PathNotFound(DataPath),
    PathExists(DataPath),
    StreamActive(DataPath),
    PathReplaced(DataPath),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedMutationResult {
    Committed(MutationReceipt),
    Rejected(MutationRejection),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedOperation {
    pub request: MutationRequest,
    pub result: PersistedMutationResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceSnapshot {
    schema_version: u32,
    pub authority_epoch: u64,
    pub next_revision: u64,
    pub bindings: BTreeMap<DataPath, PersistedBinding>,
    #[serde(default)]
    pub stream_nodes: BTreeMap<DataPath, u64>,
    pub operations: BTreeMap<OperationId, PersistedOperation>,
    #[serde(default)]
    pub retirements: Vec<ActorAddress>,
    /// Endpoints of stream generations displaced by a replacement whose
    /// fence has not been acknowledged yet; the directory re-fans the
    /// displacement until every endpoint acknowledges.
    #[serde(default)]
    pub stream_retirements: Vec<(ActorAddress, StreamIncarnation)>,
}

impl Default for NamespaceSnapshot {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            authority_epoch: 0,
            next_revision: 1,
            bindings: BTreeMap::new(),
            operations: BTreeMap::new(),
            stream_nodes: BTreeMap::new(),
            retirements: Vec::new(),
            stream_retirements: Vec::new(),
        }
    }
}

impl NamespaceSnapshot {
    fn validate(&self) -> Result<(), NamespaceStoreError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(NamespaceStoreError::UnsupportedSchema {
                found: self.schema_version,
                supported: SCHEMA_VERSION,
            });
        }
        if self.next_revision == 0 {
            return Err(NamespaceStoreError::Corrupt(
                "next namespace revision is zero".to_owned(),
            ));
        }
        if self
            .bindings
            .values()
            .any(|binding| binding.revision == 0 || binding.revision >= self.next_revision)
            || self
                .stream_nodes
                .values()
                .any(|revision| *revision == 0 || *revision >= self.next_revision)
        {
            return Err(NamespaceStoreError::Corrupt(
                "binding revision is outside the committed revision range".to_owned(),
            ));
        }
        if self.operations.values().any(|operation| {
            matches!(
                operation.result,
                PersistedMutationResult::Committed(receipt)
                    if receipt.revision == 0 || receipt.revision >= self.next_revision
            )
        }) {
            return Err(NamespaceStoreError::Corrupt(
                "operation revision is outside the committed revision range".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceStoreError {
    Io(String),
    Corrupt(String),
    UnsupportedSchema { found: u32, supported: u32 },
    RevisionExhausted,
    AuthorityEpochExhausted,
}

impl fmt::Display for NamespaceStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(reason) => write!(f, "namespace storage I/O failed: {reason}"),
            Self::Corrupt(reason) => write!(f, "namespace storage is corrupt: {reason}"),
            Self::UnsupportedSchema { found, supported } => write!(
                f,
                "unsupported namespace schema {found}; supported schema is {supported}"
            ),
            Self::RevisionExhausted => f.write_str("namespace revision exhausted"),
            Self::AuthorityEpochExhausted => f.write_str("namespace authority epoch exhausted"),
        }
    }
}

impl std::error::Error for NamespaceStoreError {}

impl From<std::io::Error> for NamespaceStoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitFailpoint {
    None,
    BeforeRename,
    AfterRename,
}

pub struct NamespaceStore {
    path: PathBuf,
    snapshot: NamespaceSnapshot,
}

impl NamespaceStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, NamespaceStoreError> {
        let path = path.as_ref().to_path_buf();
        let snapshot = match File::open(&path) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                let snapshot: NamespaceSnapshot = serde_json::from_slice(&bytes)
                    .map_err(|error| NamespaceStoreError::Corrupt(error.to_string()))?;
                snapshot.validate()?;
                snapshot
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                NamespaceSnapshot::default()
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self { path, snapshot })
    }

    pub fn snapshot(&self) -> &NamespaceSnapshot {
        &self.snapshot
    }

    pub fn advance_authority_epoch(&mut self) -> Result<u64, NamespaceStoreError> {
        let mut next = self.snapshot.clone();
        next.authority_epoch = next
            .authority_epoch
            .checked_add(1)
            .filter(|epoch| *epoch != 0)
            .ok_or(NamespaceStoreError::AuthorityEpochExhausted)?;
        self.commit(next)?;
        Ok(self.snapshot.authority_epoch)
    }

    pub fn commit(&mut self, next: NamespaceSnapshot) -> Result<(), NamespaceStoreError> {
        let started = std::time::Instant::now();
        let result = next.validate().and_then(|()| self.persist(&next));
        trace_commit_observed(started.elapsed());
        if result.is_err() {
            COMMIT_FAILURES.fetch_add(1, Ordering::Relaxed);
        }
        result?;
        SNAPSHOT_BINDINGS.store(next.bindings.len() as u64, Ordering::Relaxed);
        SNAPSHOT_OPERATIONS.store(next.operations.len() as u64, Ordering::Relaxed);
        self.snapshot = next;
        Ok(())
    }

    fn persist(&self, snapshot: &NamespaceSnapshot) -> Result<(), NamespaceStoreError> {
        self.persist_with_failpoint(snapshot, CommitFailpoint::None)
    }

    fn persist_with_failpoint(
        &self,
        snapshot: &NamespaceSnapshot,
        failpoint: CommitFailpoint,
    ) -> Result<(), NamespaceStoreError> {
        let parent = self.path.parent().ok_or_else(|| {
            NamespaceStoreError::Io("namespace state path has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        let file_name = self.path.file_name().ok_or_else(|| {
            NamespaceStoreError::Io("namespace state path has no file name".to_owned())
        })?;
        let temporary = parent.join(format!(".{}.tmp", file_name.to_string_lossy()));
        let bytes = serde_json::to_vec(snapshot)
            .map_err(|error| NamespaceStoreError::Corrupt(error.to_string()))?;
        COMMIT_BYTES.fetch_add(bytes.len() as u64, Ordering::Relaxed);

        let write_result = (|| -> Result<(), NamespaceStoreError> {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            if failpoint == CommitFailpoint::BeforeRename {
                return Err(NamespaceStoreError::Io(
                    "injected failure before durable replacement".to_owned(),
                ));
            }
            fs::rename(&temporary, &self.path)?;
            File::open(parent)?.sync_all()?;
            if failpoint == CommitFailpoint::AfterRename {
                return Err(NamespaceStoreError::Io(
                    "injected crash after durable replacement".to_owned(),
                ));
            }
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        write_result
    }

    #[cfg(test)]
    fn commit_with_failpoint(
        &mut self,
        next: NamespaceSnapshot,
        failpoint: CommitFailpoint,
    ) -> Result<(), NamespaceStoreError> {
        next.validate()?;
        self.persist_with_failpoint(&next, failpoint)?;
        self.snapshot = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

    struct TestState {
        root: PathBuf,
        file: PathBuf,
    }

    impl TestState {
        fn new() -> Self {
            let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "swactor-namespace-store-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("create test state directory");
            let file = root.join("namespace.json");
            Self { root, file }
        }
    }

    impl Drop for TestState {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn state_with_binding(store: &NamespaceStore) -> NamespaceSnapshot {
        let mut next = store.snapshot().clone();
        next.bindings.insert(
            DataPath::parse("/models/a").unwrap(),
            PersistedBinding {
                length: 8,
                revision: 1,
                recovery: SourceRecovery::Actor {
                    actor: ActorAddress([7; 32]),
                    node: [8; 32],
                    owner: None,
                },
            },
        );
        next.next_revision = 2;
        next
    }

    #[test]
    fn failure_before_rename_preserves_previous_snapshot() {
        let state = TestState::new();
        let mut store = NamespaceStore::open(&state.file).unwrap();
        store.advance_authority_epoch().unwrap();
        let next = state_with_binding(&store);

        assert!(
            store
                .commit_with_failpoint(next, CommitFailpoint::BeforeRename)
                .is_err()
        );
        let reopened = NamespaceStore::open(&state.file).unwrap();
        assert!(reopened.snapshot().bindings.is_empty());
    }

    #[test]
    fn crash_after_rename_recovers_new_snapshot() {
        let state = TestState::new();
        let mut store = NamespaceStore::open(&state.file).unwrap();
        store.advance_authority_epoch().unwrap();
        let next = state_with_binding(&store);

        assert!(
            store
                .commit_with_failpoint(next, CommitFailpoint::AfterRename)
                .is_err()
        );
        let reopened = NamespaceStore::open(&state.file).unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .bindings
                .get(&DataPath::parse("/models/a").unwrap())
                .unwrap()
                .length,
            8
        );
    }

    #[test]
    fn malformed_snapshot_is_rejected() {
        let state = TestState::new();
        fs::write(&state.file, b"{not-json").unwrap();
        assert!(matches!(
            NamespaceStore::open(&state.file),
            Err(NamespaceStoreError::Corrupt(_))
        ));
    }
}
