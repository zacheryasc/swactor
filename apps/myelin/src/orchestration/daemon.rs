//! Fleet-control daemon: persistent identity, cluster snapshot, and manual
//! command dispatch.
//!
//! The daemon is deliberately manual: nothing is provisioned, replaced, or
//! destroyed except in response to an operator command. Provider resources
//! (docker containers, vastai leases) are ground truth; the snapshot records
//! intent and facts so a restarted daemon can adopt what still exists and
//! never silently re-provisions.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::orchestration::manual_control::{CommandKind, CommandRecord, CommandState, NodePhase};
use crate::provisioning::NodeProvisionSpec;
use distribution::types::NodeId as DistNodeId;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 2;
pub(crate) const IDENTITY_FILE: &str = "identity.key";
pub(crate) const SNAPSHOT_FILE: &str = "cluster.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyNodeStatus {
    Running,
    Dead,
    Orphan,
}

#[derive(Deserialize)]
struct LegacySnapshotNode {
    logical_node_id: u64,
    spec: Option<NodeProvisionSpec>,
    provider_ref: Option<String>,
    status: LegacyNodeStatus,
    runtime: Option<RuntimeFacts>,
    last_seen_unix_ms: u64,
}

#[derive(Deserialize)]
struct LegacyClusterSnapshot {
    schema_version: u32,
    run_id: u64,
    label: String,
    next_node_id: u64,
    #[serde(default)]
    accepted_command_ids: BTreeSet<String>,
    nodes: Vec<LegacySnapshotNode>,
}

/// Join/readiness facts captured when a node announced itself. Persisted so a
/// restarted daemon can re-subscribe telemetry once routes recover.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeFacts {
    #[serde(default)]
    pub run_id: u64,
    #[serde(default)]
    pub attempt_id: u64,
    pub endpoint: String,
    pub node_actor: ActorAddress,
    pub swim_node_id: DistNodeId,
    pub stage_index: u32,
    pub readiness_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotNode {
    pub logical_node_id: u64,
    /// Provision intent for nodes added through this daemon. Absent for
    /// orphans (discovered, not managed).
    pub spec: Option<NodeProvisionSpec>,
    /// Exact operator selection. Only Vast.ai nodes set this.
    #[serde(default)]
    pub selected_offer_id: Option<u64>,
    /// Provider-side address (container name, contract label, or process id).
    pub provider_ref: Option<String>,
    pub phase: NodePhase,
    pub runtime: Option<RuntimeFacts>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub last_seen_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ClusterSnapshot {
    pub schema_version: u32,
    pub run_id: u64,
    pub label: String,
    pub next_node_id: u64,
    /// Durable at-most-once command ledger, including terminal outcomes.
    #[serde(default)]
    pub commands: BTreeMap<String, CommandRecord>,
    pub nodes: Vec<SnapshotNode>,
}

impl ClusterSnapshot {
    pub(crate) fn fresh(run_id: u64, label: impl Into<String>) -> Self {
        Self {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            run_id,
            label: label.into(),
            next_node_id: 1,
            commands: BTreeMap::new(),
            nodes: Vec::new(),
        }
    }

    pub(crate) fn node(&self, logical_node_id: u64) -> Option<&SnapshotNode> {
        self.nodes
            .iter()
            .find(|node| node.logical_node_id == logical_node_id)
    }

    pub(crate) fn node_mut(&mut self, logical_node_id: u64) -> Option<&mut SnapshotNode> {
        self.nodes
            .iter_mut()
            .find(|node| node.logical_node_id == logical_node_id)
    }

    /// Allocates the next logical node id. Ids are monotonic and never reused.
    pub(crate) fn allocate_node_id(&mut self) -> u64 {
        let id = self.next_node_id;
        self.next_node_id = self
            .next_node_id
            .checked_add(1)
            .expect("logical node id space exhausted");
        id
    }

    pub(crate) fn upsert_node(&mut self, node: SnapshotNode) {
        if node.logical_node_id != 0 {
            self.next_node_id = self
                .next_node_id
                .max(node.logical_node_id.saturating_add(1));
        }
        match self
            .nodes
            .iter()
            .position(|existing| existing.logical_node_id == node.logical_node_id)
        {
            Some(index) => self.nodes[index] = node,
            None => self.nodes.push(node),
        }
    }
}

/// Where the daemon keeps `identity.key` and `cluster.json`.
#[derive(Clone, Debug)]
pub(crate) struct StateDir {
    root: PathBuf,
}

impl StateDir {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn identity_path(&self) -> PathBuf {
        self.root.join(IDENTITY_FILE)
    }

    fn snapshot_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_FILE)
    }
    pub(crate) fn process_registry_path(&self) -> PathBuf {
        self.root.join("process-nodes.json")
    }

    /// Loads the persisted iroh secret key, creating it on first boot. The
    /// endpoint address baked into every launched node's env stays valid
    /// across daemon restarts because of this.
    pub(crate) fn load_or_create_identity(&self) -> Result<iroh::SecretKey, String> {
        fs::create_dir_all(&self.root)
            .map_err(|error| format!("create state dir {}: {error}", self.root.display()))?;
        let path = self.identity_path();
        match fs::read(&path) {
            Ok(bytes) => {
                let array: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| format!("identity key {} is not 32 bytes", path.display()))?;
                Ok(iroh::SecretKey::from_bytes(&array))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let key = iroh::SecretKey::generate();
                write_atomic(&path, &key.to_bytes())?;
                Ok(key)
            }
            Err(error) => Err(format!("read identity key {}: {error}", path.display())),
        }
    }

    /// Loads the cluster snapshot. Missing file is a fresh (empty) cluster; a
    /// corrupt file is a hard error so a stale state can never cause a silent
    /// re-provision.
    pub(crate) fn load_snapshot(&self) -> Result<ClusterSnapshot, String> {
        let path = self.snapshot_path();
        match fs::read_to_string(&path) {
            Ok(content) => {
                let schema_version = serde_json::from_str::<serde_json::Value>(&content)
                    .ok()
                    .and_then(|value| value.get("schema_version").and_then(|value| value.as_u64()))
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| {
                        format!(
                            "cluster snapshot {} is corrupt (missing schema_version); inspect it or remove it with \
                             --reset-state — refusing to silently re-provision",
                            path.display()
                        )
                    })?;
                match schema_version {
                    SNAPSHOT_SCHEMA_VERSION => serde_json::from_str(&content).map_err(|error| {
                        format!(
                            "cluster snapshot {} is corrupt ({error}); inspect it or remove it with \
                             --reset-state — refusing to silently re-provision",
                            path.display()
                        )
                    }),
                    1 => {
                        let legacy: LegacyClusterSnapshot =
                            serde_json::from_str(&content).map_err(|error| {
                                format!(
                                    "cluster snapshot {} schema v1 is corrupt ({error}); inspect it or remove it with \
                                     --reset-state — refusing to silently re-provision",
                                    path.display()
                                )
                            })?;
                        Ok(migrate_v1(legacy))
                    }
                    unsupported => Err(format!(
                        "cluster snapshot {} has unsupported schema_version {} (expected {} or migratable v1); \
                         migrate or remove it with --reset-state",
                        path.display(),
                        unsupported,
                        SNAPSHOT_SCHEMA_VERSION
                    )),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Caller decides the run id/label for a fresh snapshot.
                Ok(ClusterSnapshot {
                    schema_version: SNAPSHOT_SCHEMA_VERSION,
                    run_id: 0,
                    label: String::new(),
                    next_node_id: 1,
                    commands: BTreeMap::new(),
                    nodes: Vec::new(),
                })
            }
            Err(error) => Err(format!("read cluster snapshot {}: {error}", path.display())),
        }
    }

    pub(crate) fn save_snapshot(&self, snapshot: &ClusterSnapshot) -> Result<(), String> {
        fs::create_dir_all(&self.root)
            .map_err(|error| format!("create state dir {}: {error}", self.root.display()))?;
        let bytes = serde_json::to_vec_pretty(snapshot)
            .map_err(|error| format!("serialize cluster snapshot: {error}"))?;
        write_atomic(&self.snapshot_path(), &bytes)
    }

    /// Removes the crash-recovery checkpoint after a graceful shutdown only
    /// when neither the snapshot nor the local process registry owns work.
    pub(crate) fn cleanup_recovery_checkpoint(
        &self,
        snapshot: &ClusterSnapshot,
    ) -> Result<bool, String> {
        if snapshot
            .nodes
            .iter()
            .any(|node| !matches!(node.phase, NodePhase::Stopped | NodePhase::Orphan))
        {
            return Ok(false);
        }

        let registry_path = self.process_registry_path();
        match fs::read_to_string(&registry_path) {
            Ok(content) => {
                let registry = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(
                    &content,
                )
                .map_err(|error| {
                    format!(
                        "parse process registry {} before checkpoint cleanup: {error}",
                        registry_path.display()
                    )
                })?;
                if !registry.is_empty() {
                    return Ok(false);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "read process registry {} before checkpoint cleanup: {error}",
                    registry_path.display()
                ));
            }
        }

        for path in [self.snapshot_path(), registry_path] {
            if let Err(error) = fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(format!("remove {}: {error}", path.display()));
            }
        }
        Ok(true)
    }

    /// Removes all state files. Explicit operator action only.
    pub(crate) fn reset(&self) -> Result<(), String> {
        for path in [self.identity_path(), self.snapshot_path()] {
            if let Err(error) = fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                return Err(format!("remove {}: {error}", path.display()));
            }
        }
        Ok(())
    }
}

fn migrate_v1(legacy: LegacyClusterSnapshot) -> ClusterSnapshot {
    debug_assert_eq!(legacy.schema_version, 1);
    let commands = legacy
        .accepted_command_ids
        .into_iter()
        .map(|command_id| {
            (
                command_id.clone(),
                CommandRecord {
                    command_id,
                    kind: CommandKind::Migrated,
                    state: CommandState::Failed,
                    node_ids: Vec::new(),
                    error: Some(
                        "migrated schema-v1 command; outcome was not recorded and will not be replayed"
                            .to_owned(),
                    ),
                },
            )
        })
        .collect();
    let nodes = legacy
        .nodes
        .into_iter()
        .map(|legacy_node| {
            let mut runtime = legacy_node.runtime;
            if let (Some(spec), Some(facts)) = (legacy_node.spec.as_ref(), runtime.as_mut()) {
                facts.run_id = spec.run_id;
                facts.attempt_id = spec.attempt_id;
            }
            SnapshotNode {
                logical_node_id: legacy_node.logical_node_id,
                spec: legacy_node.spec,
                selected_offer_id: None,
                provider_ref: legacy_node.provider_ref,
                phase: match legacy_node.status {
                    LegacyNodeStatus::Running => NodePhase::Running,
                    LegacyNodeStatus::Dead => NodePhase::Stopped,
                    LegacyNodeStatus::Orphan => NodePhase::Orphan,
                },
                runtime,
                last_error: None,
                last_seen_unix_ms: legacy_node.last_seen_unix_ms,
            }
        })
        .collect();
    ClusterSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        run_id: legacy.run_id,
        label: legacy.label,
        next_node_id: legacy.next_node_id,
        commands,
        nodes,
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).map_err(|error| format!("write {}: {error}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|error| {
        let _ = fs::remove_file(&tmp);
        format!("persist {}: {error}", path.display())
    })
}

pub(crate) fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_v1_migrates_without_replaying_accepted_commands() {
        let temp = tempfile::tempdir().unwrap();
        let state = StateDir::new(temp.path());
        fs::write(
            temp.path().join(SNAPSHOT_FILE),
            serde_json::json!({
                "schema_version": 1,
                "run_id": 7,
                "label": "legacy",
                "next_node_id": 2,
                "accepted_command_ids": ["already-accepted"],
                "nodes": [{
                    "logical_node_id": 1,
                    "spec": null,
                    "provider_ref": "legacy-resource",
                    "status": "orphan",
                    "runtime": null,
                    "last_seen_unix_ms": 4
                }]
            })
            .to_string(),
        )
        .unwrap();

        let migrated = state.load_snapshot().unwrap();
        assert_eq!(migrated.schema_version, SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(migrated.nodes[0].phase, NodePhase::Orphan);
        assert_eq!(
            migrated.commands["already-accepted"].state,
            CommandState::Failed
        );
        assert_eq!(
            migrated.commands["already-accepted"].kind,
            CommandKind::Migrated
        );
    }

    #[test]
    fn unsupported_and_corrupt_snapshots_are_hard_errors() {
        let temp = tempfile::tempdir().unwrap();
        let state = StateDir::new(temp.path());
        fs::write(temp.path().join(SNAPSHOT_FILE), r#"{"schema_version":99}"#).unwrap();
        assert!(state.load_snapshot().unwrap_err().contains("unsupported"));
        fs::write(temp.path().join(SNAPSHOT_FILE), "{broken").unwrap();
        assert!(state.load_snapshot().unwrap_err().contains("corrupt"));
    }

    #[test]
    fn atomic_round_trip_preserves_monotonic_ids_and_commands() {
        let temp = tempfile::tempdir().unwrap();
        let state = StateDir::new(temp.path());
        let mut snapshot = ClusterSnapshot::fresh(9, "roundtrip");
        assert_eq!(snapshot.allocate_node_id(), 1);
        assert_eq!(snapshot.allocate_node_id(), 2);
        snapshot.commands.insert(
            "done".to_owned(),
            CommandRecord {
                command_id: "done".to_owned(),
                kind: CommandKind::Kill,
                state: CommandState::Succeeded,
                node_ids: vec![1],
                error: None,
            },
        );
        state.save_snapshot(&snapshot).unwrap();
        let loaded = state.load_snapshot().unwrap();
        assert_eq!(loaded.next_node_id, 3);
        assert_eq!(loaded.commands["done"].state, CommandState::Succeeded);
        assert!(!temp.path().join("cluster.tmp").exists());
    }

    #[test]
    fn graceful_cleanup_removes_only_completed_recovery_state() {
        let temp = tempfile::tempdir().unwrap();
        let state = StateDir::new(temp.path());
        let complete = ClusterSnapshot::fresh(9, "complete");
        state.save_snapshot(&complete).unwrap();
        fs::write(state.process_registry_path(), "{}").unwrap();

        assert!(state.cleanup_recovery_checkpoint(&complete).unwrap());
        assert!(!temp.path().join(SNAPSHOT_FILE).exists());
        assert!(!state.process_registry_path().exists());

        let mut active = ClusterSnapshot::fresh(10, "active");
        active.nodes.push(SnapshotNode {
            logical_node_id: 1,
            spec: None,
            selected_offer_id: None,
            provider_ref: Some("resource-1".to_owned()),
            phase: NodePhase::Running,
            runtime: None,
            last_error: None,
            last_seen_unix_ms: 0,
        });
        state.save_snapshot(&active).unwrap();

        assert!(!state.cleanup_recovery_checkpoint(&active).unwrap());
        assert!(temp.path().join(SNAPSHOT_FILE).exists());
    }
}
