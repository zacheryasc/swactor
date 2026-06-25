//! Shared test fixture for the pipeline-parallel-inference test suite.
//!
//! Single source of truth for "make a node on the actorized distribution
//! stack" used by `t_topology`, `t_cluster`, and `t_integration`. Each helper
//! returns a [`ClusterNode`] (driver + per-node swactor runtime + the four
//! protocol actors), pumped synchronously by tests.
//!
//! Mirrors `crates/distribution/tests/common/iroh.rs` — same idea, but the
//! codec registry is the pp `inference_codec_registry()` (which already layers
//! the protocol codec) so app actors share the runtime with protocol actors.
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use iroh::{PublicKey, RelayMode};
use tokio::runtime::{Handle, Runtime as TokioRuntime};

use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh_driver::IrohDriverConfig;

use pipeline_parallel_inference::cluster::ClusterNode;
use pipeline_parallel_inference::messages::inference_codec_registry;

/// Wall-clock granularity of one gossip round for tests. Smaller than
/// production so the test suite finishes quickly; the actors are wall-clock
/// driven so the values flow through unchanged.
const TICK: Duration = Duration::from_millis(10);

pub fn test_node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 3,
            indirect_probes: 1,
            suspicion_timeout: TICK * 5,
            dead_reprobe_interval: Duration::ZERO,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

/// Shared tokio runtime for sync `#[test]`s — iroh needs a tokio context, and
/// each test creating its own multi-thread runtime would balloon thread count.
fn test_tokio_handle() -> Handle {
    static RT: OnceLock<TokioRuntime> = OnceLock::new();
    RT.get_or_init(|| TokioRuntime::new().expect("build test tokio runtime"))
        .handle()
        .clone()
}

pub fn make_node() -> ClusterNode {
    ClusterNode::with_handle(
        test_tokio_handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: test_node_config(),
            peer_auth: None,
            additional_alpns: vec![],
        },
        test_node_config(),
        inference_codec_registry(),
        |_| {},
    )
    .expect("failed to create cluster node")
}

/// One pump iteration of a single node (driver + runtime).
pub fn pump_one(node: &mut ClusterNode) {
    node.pump_once();
}

/// One pump iteration across every node, in order.
pub fn pump_all(nodes: &mut [ClusterNode]) {
    for n in nodes.iter_mut() {
        n.pump_once();
    }
}

/// Pump every node until `cond` holds or `timeout` expires. Returns the
/// outcome so callers can assert on it. Sleeps ~10ms per iteration so SWIM's
/// wall-clock timers advance.
pub fn pump_until<F>(nodes: &mut [ClusterNode], timeout: Duration, cond: F) -> bool
where
    F: Fn(&[ClusterNode]) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        pump_all(nodes);
        if cond(nodes) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Whether `node` sees `peer_key` as `Alive` in its membership mirror.
pub fn sees_alive(node: &ClusterNode, peer_key: &PublicKey) -> bool {
    let peer = distribution::types::NodeId(*peer_key.as_bytes());
    node.sees_alive(&peer)
}

pub fn pubkey_of(node: &ClusterNode) -> PublicKey {
    PublicKey::from_bytes(&node.node_id().0).expect("valid node id")
}

/// Build a three-node cluster (orchestrator + stage 0 + stage 1) joined via
/// the orchestrator as the seed. Returns the nodes in `[orchestrator,
/// stage0, stage1]` order once every node sees every other node alive.
pub fn make_three_node_cluster() -> [ClusterNode; 3] {
    let mut orch = make_node();
    let mut s0 = make_node();
    let mut s1 = make_node();

    let seed = orch.endpoint_addr();
    s0.join(&[seed.clone()]);
    s1.join(&[seed]);

    let orch_key = pubkey_of(&orch);
    let s0_key = pubkey_of(&s0);
    let s1_key = pubkey_of(&s1);

    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(10) {
        orch.pump_once();
        s0.pump_once();
        s1.pump_once();

        let orch_sees_all = sees_alive(&orch, &s0_key) && sees_alive(&orch, &s1_key);
        let s0_sees_all = sees_alive(&s0, &orch_key) && sees_alive(&s0, &s1_key);
        let s1_sees_all = sees_alive(&s1, &orch_key) && sees_alive(&s1, &s0_key);
        if orch_sees_all && s0_sees_all && s1_sees_all {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(converged, "3-node cluster did not converge within 10s");
    [orch, s0, s1]
}
