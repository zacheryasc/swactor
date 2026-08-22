//! Crash-consistent durable state for the virtual blob namespace.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

use crate::path::DataPath;

const SCHEMA_VERSION: u32 = 1;

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
    File { path: PathBuf },
    Actor { actor: ActorAddress },
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
    Unregister {
        path: DataPath,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationRejection {
    PathNotFound(DataPath),
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
    pub operations: BTreeMap<OperationId, PersistedOperation>,
    #[serde(default)]
    pub retirements: Vec<ActorAddress>,
}

impl Default for NamespaceSnapshot {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            authority_epoch: 0,
            next_revision: 1,
            bindings: BTreeMap::new(),
            operations: BTreeMap::new(),
            retirements: Vec::new(),
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
        next.validate()?;
        self.persist(&next)?;
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
