//! Fleet-control daemon: persistent identity, cluster snapshot, and manual
//! command dispatch.
//!
//! The daemon is deliberately manual: nothing is provisioned, replaced, or
//! destroyed except in response to an operator command. Provider resources
//! (docker containers, vastai leases) are ground truth; the snapshot records
//! intent and facts so a restarted daemon can adopt what still exists and
//! never silently re-provisions.

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::provisioning::NodeProvisionSpec;
use distribution::types::NodeId as DistNodeId;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub(crate) const IDENTITY_FILE: &str = "identity.key";
pub(crate) const SNAPSHOT_FILE: &str = "cluster.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeStatus {
    /// Provider resource exists and the runtime joined (or is expected to).
    Running,
    /// Tracked by the snapshot but gone from the provider.
    Dead,
    /// Exists at the provider under this daemon's label but was never added
    /// through this daemon's command surface.
    Orphan,
}

/// Join/readiness facts captured when a node announced itself. Persisted so a
/// restarted daemon can re-subscribe telemetry once routes recover.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RuntimeFacts {
    pub endpoint: String,
    pub node_actor: ActorAddress,
    pub swim_node_id: DistNodeId,
    pub stage_index: u32,
    pub readiness_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotNode {
    pub logical_node_id: u64,
    /// Provision intent for nodes added through this daemon. Absent for
    /// orphans (discovered, not managed).
    pub spec: Option<NodeProvisionSpec>,
    /// Provider-side address (e.g. docker container name) for records that
    /// exist without a full spec.
    pub provider_ref: Option<String>,
    pub status: NodeStatus,
    pub runtime: Option<RuntimeFacts>,
    pub last_seen_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ClusterSnapshot {
    pub schema_version: u32,
    pub run_id: u64,
    pub label: String,
    pub next_node_id: u64,
    /// Durable at-most-once ledger for dashboard requests. A command id is
    /// recorded before provider mutation, so retrying after a timeout or crash
    /// cannot create or destroy a second resource.
    #[serde(default)]
    pub accepted_command_ids: BTreeSet<String>,
    pub nodes: Vec<SnapshotNode>,
}

impl ClusterSnapshot {
    pub(crate) fn fresh(run_id: u64, label: impl Into<String>) -> Self {
        Self {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            run_id,
            label: label.into(),
            next_node_id: 1,
            accepted_command_ids: BTreeSet::new(),
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

    pub(crate) fn running_nodes(&self) -> impl Iterator<Item = &SnapshotNode> {
        self.nodes
            .iter()
            .filter(|node| node.status == NodeStatus::Running)
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

    pub(crate) fn remove_node(&mut self, logical_node_id: u64) -> Option<SnapshotNode> {
        let index = self
            .nodes
            .iter()
            .position(|node| node.logical_node_id == logical_node_id)?;
        Some(self.nodes.remove(index))
    }

    /// Makes orphan records exactly match provider-only resources. Orphans are
    /// keyed by provider reference because they intentionally have no logical
    /// node id or provision spec.
    pub(crate) fn sync_orphans(
        &mut self,
        provider_refs: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let provider_refs = provider_refs
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        self.nodes.retain(|node| {
            node.status != NodeStatus::Orphan
                || node
                    .provider_ref
                    .as_ref()
                    .is_some_and(|provider_ref| provider_refs.contains(provider_ref))
        });
        let known = self
            .nodes
            .iter()
            .filter(|node| node.status == NodeStatus::Orphan)
            .filter_map(|node| node.provider_ref.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let added = provider_refs
            .difference(&known)
            .cloned()
            .collect::<Vec<_>>();
        let now = unix_ms_now();
        for provider_ref in &added {
            self.nodes.push(SnapshotNode {
                logical_node_id: 0,
                spec: None,
                provider_ref: Some(provider_ref.clone()),
                status: NodeStatus::Orphan,
                runtime: None,
                last_seen_unix_ms: now,
            });
        }
        added
    }

    pub(crate) fn accept_command(&mut self, command_id: &str) -> Result<bool, String> {
        let command_id = command_id.trim();
        if command_id.is_empty() {
            return Err("dashboard command_id must not be empty".to_owned());
        }
        Ok(self.accepted_command_ids.insert(command_id.to_owned()))
    }
}

/// Operator commands accepted by the dispatcher. The dashboard (and later the
/// CLI) is a transport into this surface.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DaemonCommand {
    /// Provision exactly one node using the configured provider + selection
    /// policy. Fails (does not retry) if bring-up fails.
    AddNode,
    /// Terminate a node's provider resource; the record stays (status Dead).
    Kill { logical_node_id: u64 },
    /// Terminate and forget a node.
    Destroy { logical_node_id: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DaemonOutcome {
    Added { logical_node_id: u64 },
    Killed { logical_node_id: u64 },
    Destroyed { logical_node_id: u64 },
    NoSuchNode { logical_node_id: u64 },
    Failed { command: String, reason: String },
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

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn identity_path(&self) -> PathBuf {
        self.root.join(IDENTITY_FILE)
    }

    fn snapshot_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_FILE)
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
                let snapshot: ClusterSnapshot = serde_json::from_str(&content).map_err(|e| {
                    format!(
                        "cluster snapshot {} is corrupt ({e}); inspect it or remove it with \
                         --reset-state — refusing to silently re-provision",
                        path.display()
                    )
                })?;
                if snapshot.schema_version != SNAPSHOT_SCHEMA_VERSION {
                    return Err(format!(
                        "cluster snapshot {} has unsupported schema_version {} (expected {}); \
                         migrate or remove it with --reset-state",
                        path.display(),
                        snapshot.schema_version,
                        SNAPSHOT_SCHEMA_VERSION
                    ));
                }
                Ok(snapshot)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Caller decides the run id/label for a fresh snapshot.
                Ok(ClusterSnapshot {
                    schema_version: SNAPSHOT_SCHEMA_VERSION,
                    run_id: 0,
                    label: String::new(),
                    next_node_id: 1,
                    nodes: Vec::new(),
                    accepted_command_ids: BTreeSet::new(),
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

/// Joins snapshot intent with provider ground truth at boot. Never takes a
/// lifecycle action: live nodes are adopted for observation, missing ones are
/// marked dead, provider-only resources are recorded as orphans.
pub(crate) struct BootJoin {
    pub snapshot: ClusterSnapshot,
}

pub(crate) struct JoinOutcome {
    pub adopted: Vec<u64>,
    pub dead: Vec<u64>,
    pub orphans: Vec<String>,
}

impl BootJoin {
    /// `labeled` lists provider resources carrying this daemon's label that
    /// the snapshot does not account for.
    pub(crate) fn apply_provider_truthtable(
        &mut self,
        live_specs: &BTreeMap<u64, bool>,
        labeled_orphans: Vec<String>,
    ) -> JoinOutcome {
        let mut adopted = Vec::new();
        let mut dead = Vec::new();
        let now = unix_ms_now();
        for node in &mut self.snapshot.nodes {
            if node.status == NodeStatus::Orphan {
                continue;
            }
            match live_specs.get(&node.logical_node_id) {
                Some(true) => {
                    node.status = NodeStatus::Running;
                    node.last_seen_unix_ms = now;
                    adopted.push(node.logical_node_id);
                }
                _ => {
                    node.status = NodeStatus::Dead;
                    dead.push(node.logical_node_id);
                }
            }
        }
        self.snapshot.sync_orphans(labeled_orphans.clone());
        JoinOutcome {
            adopted,
            dead,
            orphans: labeled_orphans,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(1);

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "myelin-{name}-{}-{}",
            std::process::id(),
            NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn node(id: u64, status: NodeStatus) -> SnapshotNode {
        SnapshotNode {
            logical_node_id: id,
            spec: None,
            provider_ref: Some(format!("container-{id}")),
            status,
            runtime: None,
            last_seen_unix_ms: 0,
        }
    }

    #[test]
    fn node_ids_allocate_monotonically_and_never_reuse() {
        let mut snapshot = ClusterSnapshot::fresh(1, "test");
        assert_eq!(snapshot.allocate_node_id(), 1);
        assert_eq!(snapshot.allocate_node_id(), 2);
        snapshot.next_node_id = u64::MAX;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                snapshot.allocate_node_id()
            }))
            .is_err()
        );
    }

    #[test]
    fn remove_then_allocate_does_not_reuse_ids() {
        let mut snapshot = ClusterSnapshot::fresh(1, "test");
        snapshot.upsert_node(node(1, NodeStatus::Running));
        assert!(snapshot.remove_node(1).is_some());
        assert_eq!(snapshot.allocate_node_id(), 2);
    }

    #[test]
    fn join_marks_live_missing_and_orphans_without_actions() {
        let mut join = BootJoin {
            snapshot: ClusterSnapshot::fresh(1, "test"),
        };
        join.snapshot.upsert_node(node(1, NodeStatus::Running));
        join.snapshot.upsert_node(node(2, NodeStatus::Running));
        let live = BTreeMap::from([(1_u64, true), (2_u64, false)]);
        let outcome = join.apply_provider_truthtable(&live, vec!["orphan-a".to_owned()]);
        assert_eq!(outcome.adopted, vec![1]);
        assert_eq!(outcome.dead, vec![2]);
        assert_eq!(outcome.orphans, vec!["orphan-a".to_owned()]);
        assert_eq!(join.snapshot.node(1).unwrap().status, NodeStatus::Running);
        assert_eq!(join.snapshot.node(2).unwrap().status, NodeStatus::Dead);
        assert_eq!(
            join.snapshot
                .nodes
                .iter()
                .find(|node| node.provider_ref.as_deref() == Some("orphan-a"))
                .unwrap()
                .status,
            NodeStatus::Orphan
        );
    }

    #[test]
    fn snapshot_round_trips_through_disk() {
        let dir = test_dir("snapshot");
        let state = StateDir::new(&dir);
        let mut snapshot = ClusterSnapshot::fresh(7, "label");
        snapshot.upsert_node(SnapshotNode {
            logical_node_id: 1,
            spec: Some(NodeProvisionSpec {
                run_id: 7,
                node_id: 1,
                attempt_id: 0,
                stage_index: Some(0),
                image: "myelin-node:latest".to_owned(),
                env: vec![("A".to_owned(), "B".to_owned())],
                args: vec![],
                mounts: vec![],
            }),
            provider_ref: Some("container-1".to_owned()),
            status: NodeStatus::Running,
            runtime: None,
            last_seen_unix_ms: 42,
        });
        state.save_snapshot(&snapshot).unwrap();
        let loaded = state.load_snapshot().unwrap();
        assert_eq!(loaded.run_id, 7);
        assert_eq!(loaded.nodes.len(), 1);
        assert_eq!(
            loaded.nodes[0].spec.as_ref().unwrap().image,
            "myelin-node:latest"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_snapshot_is_a_hard_error() {
        let dir = test_dir("corrupt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(SNAPSHOT_FILE), b"{ not json").unwrap();
        let state = StateDir::new(&dir);
        let error = state.load_snapshot().unwrap_err();
        assert!(error.contains("corrupt"), "unexpected error: {error}");
        assert!(error.contains("refusing"), "unexpected error: {error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_key_is_stable_across_loads() {
        let dir = test_dir("identity");
        let state = StateDir::new(&dir);
        let first = state.load_or_create_identity().unwrap();
        let second = state.load_or_create_identity().unwrap();
        assert_eq!(first.to_bytes(), second.to_bytes());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn command_ids_are_durable_at_most_once_tokens() {
        let dir = test_dir("commands");
        let state = StateDir::new(&dir);
        let mut snapshot = ClusterSnapshot::fresh(1, "test");
        assert!(snapshot.accept_command("request-a").unwrap());
        assert!(!snapshot.accept_command("request-a").unwrap());
        assert!(snapshot.accept_command("request-b").unwrap());
        assert!(snapshot.accept_command(" ").is_err());
        state.save_snapshot(&snapshot).unwrap();
        let mut loaded = state.load_snapshot().unwrap();
        assert!(!loaded.accept_command("request-a").unwrap());
        assert!(!loaded.accept_command("request-b").unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn orphan_records_exactly_follow_provider_ground_truth() {
        let mut snapshot = ClusterSnapshot::fresh(1, "test");
        snapshot.sync_orphans(["b".to_owned(), "a".to_owned()]);
        snapshot.sync_orphans(["b".to_owned(), "c".to_owned()]);
        let refs = snapshot
            .nodes
            .iter()
            .filter(|node| node.status == NodeStatus::Orphan)
            .filter_map(|node| node.provider_ref.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(refs, BTreeSet::from(["b".to_owned(), "c".to_owned()]));
    }

    #[test]
    fn partial_temp_write_never_replaces_last_snapshot() {
        let dir = test_dir("atomic");
        let state = StateDir::new(&dir);
        let mut snapshot = ClusterSnapshot::fresh(7, "stable");
        snapshot.allocate_node_id();
        state.save_snapshot(&snapshot).unwrap();
        std::fs::write(dir.join("cluster.tmp"), b"{partial").unwrap();
        let loaded = state.load_snapshot().unwrap();
        assert_eq!(loaded.run_id, 7);
        assert_eq!(loaded.next_node_id, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_changes_only_after_explicit_reset() {
        let dir = test_dir("identity-reset");
        let state = StateDir::new(&dir);
        let first = state.load_or_create_identity().unwrap();
        assert_eq!(
            first.to_bytes(),
            state.load_or_create_identity().unwrap().to_bytes()
        );
        state.reset().unwrap();
        let replacement = state.load_or_create_identity().unwrap();
        assert_ne!(first.to_bytes(), replacement.to_bytes());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fuzzed_manual_interleavings_preserve_snapshot_invariants() {
        let dir = test_dir("state-fuzz");
        let state = StateDir::new(&dir);
        let mut snapshot = ClusterSnapshot::fresh(9, "fuzz");
        let mut model_nodes = BTreeMap::<u64, NodeStatus>::new();
        let mut provider_orphans = BTreeSet::<String>::new();
        let mut command_ids = BTreeSet::<String>::new();
        let mut rng = 0x6a09_e667_f3bc_c909_u64;

        for step in 0..2_000_u64 {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            match rng % 7 {
                0 => {
                    let id = snapshot.allocate_node_id();
                    snapshot.upsert_node(node(id, NodeStatus::Running));
                    model_nodes.insert(id, NodeStatus::Running);
                }
                1 => {
                    let id = 1 + rng.rotate_left(17) % snapshot.next_node_id.max(2);
                    if let Some(status) = model_nodes.get_mut(&id) {
                        *status = NodeStatus::Dead;
                        snapshot.node_mut(id).unwrap().status = NodeStatus::Dead;
                    }
                }
                2 => {
                    let id = 1 + rng.rotate_right(11) % snapshot.next_node_id.max(2);
                    model_nodes.remove(&id);
                    snapshot.remove_node(id);
                }
                3 => {
                    provider_orphans.insert(format!("orphan-{}", rng % 19));
                }
                4 => {
                    provider_orphans.remove(&format!("orphan-{}", rng % 19));
                }
                5 => {
                    let command_id = format!("command-{}", rng % 31);
                    let expected = command_ids.insert(command_id.clone());
                    assert_eq!(snapshot.accept_command(&command_id).unwrap(), expected);
                }
                _ => {
                    state.save_snapshot(&snapshot).unwrap();
                    snapshot = state.load_snapshot().unwrap();
                }
            }
            snapshot.sync_orphans(provider_orphans.iter().cloned());

            let managed_ids = snapshot
                .nodes
                .iter()
                .filter(|node| node.status != NodeStatus::Orphan)
                .map(|node| node.logical_node_id)
                .collect::<Vec<_>>();
            assert_eq!(
                managed_ids.iter().copied().collect::<BTreeSet<_>>().len(),
                managed_ids.len(),
                "duplicate managed id after step {step}"
            );
            for (id, expected) in &model_nodes {
                assert_eq!(
                    snapshot.node(*id).map(|node| &node.status),
                    Some(expected),
                    "node model diverged after step {step}"
                );
            }
            assert!(managed_ids.iter().all(|id| *id < snapshot.next_node_id));
            let observed_orphans = snapshot
                .nodes
                .iter()
                .filter(|node| node.status == NodeStatus::Orphan)
                .filter_map(|node| node.provider_ref.clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(observed_orphans, provider_orphans);
            assert_eq!(snapshot.accepted_command_ids, command_ids);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
