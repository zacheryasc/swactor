//! Process-level conformance: a `ProvisionPlugin` whose resources are
//! real OS processes (`sleep infinity` children). The kit battery and
//! seam contracts run against it, so "no double-create", "destroy
//! releases", "ambiguous create adopts", and "converged leaks nothing"
//! are verified by counting actual live PIDs.

mod common;

use std::collections::{BTreeMap, VecDeque};
use std::process::{Child, Command};
use std::sync::Arc;

use parking_lot::Mutex;
use provisioning::plugin::{NodeProvisionSpec, PluginNodeHandle, PluginSink, ProvisionPlugin};

use common::{
    AMBIGUOUS_FAULT_MARKER, Fault, PluginBackendAdapter, TestablePlugin, assert_plugin_contracts,
    run_trace_battery,
};

struct ProcessPluginState {
    faults: VecDeque<Fault>,
    /// attempt -> live child, present until stopped.
    children: BTreeMap<u64, Child>,
    created: usize,
}

/// A process provisioner: create spawns a real child keyed by attempt,
/// stop kills and reaps it, ambiguous faults spawn-then-fail.
struct ProcessPlugin {
    state: Arc<Mutex<ProcessPluginState>>,
}

impl Default for ProcessPlugin {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProcessPluginState {
                faults: VecDeque::new(),
                children: BTreeMap::new(),
                created: 0,
            })),
        }
    }
}

impl Drop for ProcessPlugin {
    fn drop(&mut self) {
        // CI hygiene: never leave children behind, even on failure.
        let mut state = self.state.lock();
        let children: Vec<Child> = std::mem::take(&mut state.children).into_values().collect();
        for mut child in children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl TestablePlugin for ProcessPlugin {
    fn apply_fault(&self, fault: Fault) {
        let mut state = self.state.lock();
        match fault {
            Fault::Heal => state.faults.clear(),
            other => state.faults.push_back(other),
        }
    }

    fn leaked_resources(&self, live_handles: &[u64]) -> Vec<String> {
        let state = self.state.lock();
        state
            .children
            .keys()
            .filter(|attempt| !live_handles.contains(attempt))
            .map(|attempt| {
                let pid = state
                    .children
                    .get(attempt)
                    .map(|child| child.id())
                    .unwrap_or_default();
                format!("attempt={attempt} pid={pid}")
            })
            .collect()
    }

    fn resources_created(&self) -> usize {
        self.state.lock().created
    }
}

impl ProvisionPlugin for ProcessPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let mut state = self.state.lock();
        let attempt = spec.attempt_id;
        if let Some(child) = state.children.get(&attempt) {
            // Adoption: the child for this attempt already exists.
            return Ok(PluginNodeHandle {
                id: attempt,
                provider_process_id: Some(child.id()),
            });
        }
        let fault = state.faults.pop_front();
        if matches!(fault, Some(Fault::Panic)) {
            panic!("scripted process plugin panic");
        }
        if matches!(fault, Some(Fault::Definite)) {
            return Err("scripted definite failure".to_owned());
        }
        let child = Command::new("sleep")
            .arg("infinity")
            .spawn()
            .map_err(|error| format!("spawn failed: {error}"))?;
        let pid = child.id();
        state.children.insert(attempt, child);
        state.created += 1;
        if matches!(fault, Some(Fault::Ambiguous)) {
            // The child exists but the caller cannot know; a retry with
            // the same attempt must adopt it.
            return Err(AMBIGUOUS_FAULT_MARKER.to_owned());
        }
        Ok(PluginNodeHandle {
            id: attempt,
            provider_process_id: Some(pid),
        })
    }

    fn start_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let mut state = self.state.lock();
        if let Some(mut child) = state.children.remove(&handle.id) {
            child
                .kill()
                .map_err(|error| format!("kill failed: {error}"))?;
            child
                .wait()
                .map_err(|error| format!("reap failed: {error}"))?;
        }
        Ok(())
    }
}

#[test]
fn process_plugin_passes_seam_contracts() {
    let mut plugin = ProcessPlugin::default();
    assert_plugin_contracts(&mut plugin);
    // Belt and braces: contracts released everything.
    assert!(plugin.leaked_resources(&[]).is_empty());
}

#[test]
fn process_plugin_battery_holds_invariants_and_converges() {
    run_trace_battery(
        || PluginBackendAdapter::new(ProcessPlugin::default()),
        16,
        28,
    );
}
