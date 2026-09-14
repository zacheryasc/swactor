//! Static raw-SSH fleet provider.
//!
//! A deployment E2E fixture: pre-created blank Docker containers, each on
//! its own isolated network, reachable only through a published SSH port.
//! The plugin maps deterministic node identities to fixture slots, ships a
//! deployment bundle through the shared SSH artifact bootstrap, and stops
//! containers through local Docker control — never through SSH.
//!
//! Slot mapping is derived from the logical node id (`node_id - 1`), not
//! `stage_index`: fleet provisioning assigns every node `stage_index = 0`,
//! while logical node ids are unique and persisted across recovery.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::json;

use crate::orchestration::provider_adapters::ssh_bootstrap::{
    SshArtifactBootstrapLauncher, SshBootstrapLauncher, SshEndpoint,
};
use crate::provisioning::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink,
    ProvisionPlugin,
};

/// Fixed install path of the worker inside a raw node.
pub(crate) const WORKER_BIN_PATH: &str = "/opt/myelin/current/bin/myelin-worker";

/// Fixture manifest written by the harness; describes pre-created raw nodes.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct StaticFleetManifest {
    pub nodes: Vec<StaticFleetNode>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct StaticFleetNode {
    pub slot: u32,
    pub container: String,
    pub ssh_host: String,
    pub ssh_port: u16,
    #[serde(default)]
    pub ssh_user: Option<String>,
}

impl StaticFleetManifest {
    pub(crate) fn load(path: &std::path::Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("read static fleet manifest {}: {error}", path.display()))?;
        let manifest: StaticFleetManifest = serde_json::from_str(&text)
            .map_err(|error| format!("decode static fleet manifest {}: {error}", path.display()))?;
        if manifest.nodes.is_empty() {
            return Err(format!(
                "static fleet manifest {} has no nodes",
                path.display()
            ));
        }
        let mut slots = std::collections::BTreeSet::new();
        for node in &manifest.nodes {
            if !slots.insert(node.slot) {
                return Err(format!(
                    "static fleet manifest {} has duplicate slot {}",
                    path.display(),
                    node.slot
                ));
            }
        }
        Ok(manifest)
    }

    fn node_for_slot(&self, slot: u32) -> Option<&StaticFleetNode> {
        self.nodes.iter().find(|node| node.slot == slot)
    }
}

/// Deterministic node → slot mapping. Logical node ids are assigned from 1.
fn slot_for_spec(spec: &NodeProvisionSpec) -> Result<u32, String> {
    let node_id = spec.node_id;
    if node_id == 0 {
        return Err("static-ssh provisioning requires a nonzero logical node id".to_owned());
    }
    u32::try_from(node_id - 1)
        .map_err(|_| format!("logical node id {node_id} overflows the fixture slot space"))
}

pub(crate) struct StaticSshFleetPlugin<B>
where
    B: SshBootstrapLauncher,
{
    manifest: StaticFleetManifest,
    bootstrap: B,
    nodes: BTreeMap<u64, StaticNode<B>>,
    next_handle_id: u64,
}

struct StaticNode<B>
where
    B: SshBootstrapLauncher,
{
    container: String,
    endpoint: SshEndpoint,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    bootstrap: Option<B::Handle>,
}

impl StaticSshFleetPlugin<SshArtifactBootstrapLauncher> {
    pub(crate) fn new(
        manifest: StaticFleetManifest,
        bootstrap: SshArtifactBootstrapLauncher,
    ) -> Self {
        Self::with_launcher(manifest, bootstrap)
    }
}

impl<B> StaticSshFleetPlugin<B>
where
    B: SshBootstrapLauncher,
{
    pub(crate) fn with_launcher(manifest: StaticFleetManifest, bootstrap: B) -> Self {
        Self {
            manifest,
            bootstrap,
            nodes: BTreeMap::new(),
            next_handle_id: 1,
        }
    }

    fn node_line(&self, node: &StaticNode<B>, message: impl Into<String>) {
        node.sink.observe(PluginObservation::ProviderLine {
            run_id: node.spec.run_id,
            node_id: node.spec.node_id,
            line: message.into(),
        });
    }

    /// Binds one provision attempt to its fixture slot. Idempotent: the same
    /// logical node always maps to the same slot and container.
    fn bind_slot(
        &mut self,
        spec: NodeProvisionSpec,
        sink: &PluginSink,
    ) -> Result<(u64, String), String> {
        let slot = slot_for_spec(&spec)?;
        let fixture = self.manifest.node_for_slot(slot).ok_or_else(|| {
            format!(
                "static fleet has no node for slot {slot} (logical node {})",
                spec.node_id
            )
        })?;
        if crate::provisioning::docker_container_is_absent(&fixture.container)? {
            return Err(format!(
                "substrate_lost: static fleet container {} for slot {slot} is absent; deployment cannot replace it",
                fixture.container
            ));
        }
        if !crate::provisioning::docker_container_is_running(&fixture.container)? {
            return Err(format!(
                "substrate_lost: static fleet container {} for slot {slot} is stopped; deployment cannot restart it",
                fixture.container
            ));
        }
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: json!({
                "type": "StaticSshSlotBound",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "slot": slot,
                "container": fixture.container,
                "ssh_endpoint": format!("{}@{}:{}", fixture.ssh_user.as_deref().unwrap_or("root"), fixture.ssh_host, fixture.ssh_port),
            })
            .to_string(),
        });
        let handle_id = self.next_handle_id;
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle_id,
            StaticNode {
                container: fixture.container.clone(),
                endpoint: SshEndpoint {
                    host: fixture.ssh_host.clone(),
                    port: fixture.ssh_port,
                    user: fixture
                        .ssh_user
                        .clone()
                        .unwrap_or_else(|| "root".to_owned()),
                },
                spec: spec.clone(),
                sink: sink.clone(),
                bootstrap: None,
            },
        );
        Ok((handle_id, fixture.container.clone()))
    }

    fn stop_bootstrap_for(&mut self, handle: &PluginNodeHandle) {
        if let Some(node) = self.nodes.get_mut(&handle.id)
            && let Some(mut bootstrap) = node.bootstrap.take()
        {
            self.bootstrap.stop_bootstrap(&mut bootstrap);
        }
    }
}

fn docker(args: &[&str]) -> Result<std::process::Output, String> {
    let mut command = std::process::Command::new("docker");
    command.args(args);
    swactor_process::command_output(&mut command)
        .map_err(|error| format!("spawn docker {:?}: {error}", args))
}

fn kill_container(container: &str) -> Result<bool, String> {
    if crate::provisioning::docker_container_is_absent(container)? {
        return Ok(false);
    }
    let output = docker(&["kill", container])?;
    if !output.status.success() {
        return Err(format!(
            "docker kill {container} exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(true)
}

impl<B> ProvisionPlugin for StaticSshFleetPlugin<B>
where
    B: SshBootstrapLauncher + 'static,
{
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        if spec.deployment.is_none() {
            return Err(format!(
                "static-ssh node {} requires a deployment identity in its spec",
                spec.node_id
            ));
        }
        let (handle_id, _container) = self.bind_slot(spec, &sink)?;
        Ok(PluginNodeHandle {
            id: handle_id,
            provider_process_id: None,
        })
    }

    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let node = self
            .nodes
            .get_mut(&handle.id)
            .ok_or_else(|| format!("static-ssh node handle {} is absent", handle.id))?;
        if node.bootstrap.is_some() {
            return Ok(());
        }
        let endpoint = node.endpoint.clone();
        let bootstrap = self.bootstrap.start_bootstrap(
            node.spec.clone(),
            endpoint,
            node.sink.clone(),
            None,
            swactor_vastai::LifecyclePolicy::default(),
        )?;
        let node = self.nodes.get_mut(&handle.id).expect("node re-registered");
        node.bootstrap = Some(bootstrap);
        Ok(())
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.stop_bootstrap_for(handle);
        Ok(())
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get(&handle.id) else {
            return Ok(());
        };
        self.node_line(
            node,
            json!({
                "type": "StaticSshBootstrapComplete",
                "run_id": node.spec.run_id,
                "node_id": node.spec.node_id,
                "classification": "runtime_ready_over_data_plane",
            })
            .to_string(),
        );
        self.stop_bootstrap_for(handle);
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.stop_bootstrap_for(handle);
        let Some(node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        let result = kill_container(&node.container);
        self.node_line(
            &node,
            json!({
                "type": "StaticSshNodeStopped",
                "run_id": node.spec.run_id,
                "node_id": node.spec.node_id,
                "container": node.container,
                "result": if result.is_ok() { "ok" } else { "failed" },
                "error": result.as_ref().err(),
            })
            .to_string(),
        );
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                self.nodes.insert(handle.id, node);
                Err(error)
            }
        }
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let slot = slot_for_spec(spec)?;
        let Some(fixture) = self.manifest.node_for_slot(slot) else {
            return Ok(None);
        };
        if crate::provisioning::docker_container_is_absent(&fixture.container)? {
            return Ok(None);
        }
        let (handle_id, container) = self.bind_slot(spec.clone(), &sink)?;
        Ok(Some(AdoptedNode {
            handle: PluginNodeHandle {
                id: handle_id,
                provider_process_id: None,
            },
            provider_ref: format!("static-ssh:{container}"),
        }))
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        let slot = slot_for_spec(spec).unwrap_or(u32::MAX);
        self.manifest
            .node_for_slot(slot)
            .map(|fixture| format!("static-ssh:{}", fixture.container))
            .unwrap_or_else(|| format!("static-ssh:slot-{slot}"))
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        let mut refs = Vec::new();
        for fixture in &self.manifest.nodes {
            if !crate::provisioning::docker_container_is_absent(&fixture.container)? {
                refs.push(format!("static-ssh:{}", fixture.container));
            }
        }
        Ok(refs)
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let slot = slot_for_spec(spec)?;
        let Some(fixture) = self.manifest.node_for_slot(slot) else {
            return Ok(false);
        };
        let stopped = kill_container(&fixture.container)?;
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: json!({
                "type": "StaticSshNodeStoppedBySpec",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "container": fixture.container,
                "existed": stopped,
            })
            .to_string(),
        });
        Ok(stopped)
    }
}

/// Resolved static-ssh provider configuration held by the live orchestrator.
/// The bundle path is live configuration only; durable state persists the
/// deployment identity, never host-local paths.
#[derive(Clone, Debug)]
pub(crate) struct StaticSshRuntimeConfig {
    pub manifest: StaticFleetManifest,
    pub identity_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::provider_adapters::ssh_bootstrap::{
        SshBootstrapLauncher, SshEndpoint,
    };
    use crate::provisioning::DeploymentIdentity;
    use std::sync::Mutex;
    use telemetry::TelemetryProducer;

    fn manifest_text(containers: &[&str]) -> String {
        let nodes = containers
            .iter()
            .enumerate()
            .map(|(slot, container)| {
                format!(
                    "{{\"slot\":{slot},\"container\":\"{container}\",\
                     \"ssh_host\":\"127.0.0.1\",\"ssh_port\":{}}}",
                    32_100 + slot
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("{{\"nodes\":[{nodes}]}}")
    }

    fn manifest(containers: &[&str]) -> StaticFleetManifest {
        let directory = tempfile::tempdir().expect("test directory");
        let path = directory.path().join("manifest.json");
        std::fs::write(&path, manifest_text(containers)).expect("write manifest");
        StaticFleetManifest::load(&path).expect("valid manifest")
    }

    fn deployment_spec(node_id: u64) -> NodeProvisionSpec {
        NodeProvisionSpec {
            deployment: Some(DeploymentIdentity {
                artifact_digest: "sha256:abcd".to_owned(),
                deployment_generation: "gen".to_owned(),
            }),
            run_id: 7,
            node_id,
            attempt_id: 0,
            stage_index: Some(0),
            image: "raw".to_owned(),
            env: Vec::new(),
            args: vec!["exec /opt/myelin/current/bin/myelin-worker".to_owned()],
            offer_criteria_json: None,
            mounts: Vec::new(),
        }
    }

    /// Records bootstrap launches; owns no processes.
    struct RecordingLauncher {
        started: Mutex<Vec<(u64, SshEndpoint)>>,
    }

    impl SshBootstrapLauncher for RecordingLauncher {
        type Handle = u64;

        fn start_bootstrap(
            &mut self,
            spec: NodeProvisionSpec,
            endpoint: SshEndpoint,
            _sink: PluginSink,
            _producer: Option<TelemetryProducer>,
            _lifecycle: swactor_vastai::LifecyclePolicy,
        ) -> Result<Self::Handle, String> {
            self.started
                .lock()
                .expect("recorder")
                .push((spec.node_id, endpoint));
            Ok(spec.node_id)
        }

        fn stop_bootstrap(&mut self, _handle: &mut Self::Handle) {}
    }

    #[test]
    fn slot_mapping_is_node_id_minus_one() {
        assert_eq!(slot_for_spec(&deployment_spec(1)).unwrap(), 0);
        assert_eq!(slot_for_spec(&deployment_spec(5)).unwrap(), 4);
        assert!(slot_for_spec(&deployment_spec(0)).is_err());
        let mut overflow = deployment_spec(1);
        overflow.node_id = u64::MAX;
        assert!(slot_for_spec(&overflow).is_err());
    }

    #[test]
    fn manifest_rejects_duplicates_and_empty_fleets() {
        let directory = tempfile::tempdir().expect("test directory");
        let duplicate = directory.path().join("duplicate.json");
        let mut text = manifest_text(&["a"]);
        text.truncate(text.len() - 2);
        text.push_str(
            ",{\"slot\":0,\"container\":\"b\",\"ssh_host\":\"127.0.0.1\",\"ssh_port\":1}]}",
        );
        std::fs::write(&duplicate, text).unwrap();
        assert!(StaticFleetManifest::load(&duplicate).is_err());
        let empty = directory.path().join("empty.json");
        std::fs::write(&empty, "{\"nodes\":[]}").unwrap();
        assert!(StaticFleetManifest::load(&empty).is_err());
    }

    #[test]
    fn provider_ref_is_stable_and_slot_scoped() {
        let fleet = manifest(&["raw-a", "raw-b"]);
        let launcher = RecordingLauncher {
            started: Mutex::new(Vec::new()),
        };
        let plugin = StaticSshFleetPlugin::with_launcher(fleet, launcher);
        assert_eq!(
            plugin.provider_ref_for(&deployment_spec(1)),
            "static-ssh:raw-a"
        );
        assert_eq!(
            plugin.provider_ref_for(&deployment_spec(2)),
            "static-ssh:raw-b"
        );
    }

    #[test]
    fn create_node_requires_an_identity_and_a_live_slot() {
        let fleet = manifest(&["myelin-test-absent-container"]);
        let launcher = RecordingLauncher {
            started: Mutex::new(Vec::new()),
        };
        let mut plugin = StaticSshFleetPlugin::with_launcher(fleet, launcher);
        let sink = PluginSink::new(std::sync::Arc::new(NullSink));
        let mut unidentified = deployment_spec(1);
        unidentified.deployment = None;
        assert!(plugin.create_node(unidentified, sink.clone()).is_err());
        // Slot 2 has no fixture node at all.
        let out_of_range = plugin
            .create_node(deployment_spec(3), sink.clone())
            .unwrap_err();
        assert!(out_of_range.contains("no node for slot 2"));
        // Slot 0 exists in the manifest but its container is absent, so the
        // attempt fails semantically instead of retrying.
        let absent = plugin.create_node(deployment_spec(1), sink).unwrap_err();
        assert!(absent.contains("absent"));
    }

    struct NullSink;

    impl crate::provisioning::PluginObservationSink for NullSink {
        fn observe(&self, _observation: PluginObservation) {}
    }
}
