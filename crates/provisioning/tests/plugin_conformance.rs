//! Plugin-level conformance: the kit's reference in-memory plugin runs
//! the seam contracts and the full trace battery through the real
//! `PluginBackendAdapter` — one conformance level below the fake
//! backend, still without leaving the crate.

pub mod common;

use common::{FakePlugin, PluginBackendAdapter, assert_plugin_contracts, run_trace_battery};

#[test]
fn in_memory_plugin_passes_seam_contracts() {
    let mut plugin = FakePlugin::default();
    assert_plugin_contracts(&mut plugin);
}

#[test]
fn in_memory_plugin_battery_holds_invariants_and_converges() {
    run_trace_battery(|| PluginBackendAdapter::new(FakePlugin::default()), 256, 64);
}
