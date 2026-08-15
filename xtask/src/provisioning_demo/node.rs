//! Node role: a real swactor runtime that joins the supervisor over iroh.
//!
//! Re-exec'd from the xtask binary by the demo provider. Builds an engine +
//! `IrohDriver` (relay disabled), joins the supervisor's endpoint, and
//! reports its swactor node key + heartbeats to the key file given in
//! `DEMO_NODE_KEY_FILE` (the process actor supervises children with stdio
//! null, so stdout is not observable).

use std::io::Write;

use iroh::RelayMode;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use distribution::node::DistributedNodeConfig;
use iroh_driver::{IrohDriver, IrohDriverConfig};

use crate::provisioning_demo::HEARTBEAT_PERIOD;

/// Key-file line marking the node key (first line): `<ms> KEY <hex>`.
pub const KEY_LINE_KIND: &str = "KEY";

/// Key-file heartbeat line: `<ms> ALIVE`.
pub const ALIVE_LINE_KIND: &str = "ALIVE";

/// Run the node role. `supervisor_addr_json` is a serde-serialized
/// `iroh::EndpointAddr` of the supervisor's iroh endpoint.
pub fn run_node_role(supervisor_addr_json: &str) -> Result<(), String> {
    let supervisor_addr: iroh::EndpointAddr = serde_json::from_str(supervisor_addr_json)
        .map_err(|error| format!("invalid supervisor endpoint address: {error}"))?;
    let key_file = std::env::var("DEMO_NODE_KEY_FILE")
        .map_err(|_| "DEMO_NODE_KEY_FILE not set".to_owned())?;

    let parts = RuntimeParts::new(RuntimeConfig::default());
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).map_err(|error| format!("backend: {error}"))?,
    )
    .map_err(|error| format!("engine: {error}"))?;

    // Bind the driver, then keep it alive for the process lifetime: dropping
    // it closes the endpoint.
    let driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![],
        },
    )
    .map_err(|error| format!("iroh driver: {error}"))?;

    let node_hex = swactor_transport::hex_encode(&driver.node_id().0);
    driver.join(&[supervisor_addr]);
    append_key_line(&key_file, KEY_LINE_KIND, &node_hex);

    let heartbeat_file = key_file.clone();
    let interval_handle = engine.handle();
    interval_handle.clone().spawn(async move {
        let mut interval = interval_handle.interval(HEARTBEAT_PERIOD);
        loop {
            (&mut interval).await;
            append_key_line(&heartbeat_file, ALIVE_LINE_KIND, "");
        }
    });

    // The engine owns progression; park this thread until killed. `driver`
    // stays alive until process exit.
    let _keep_driver = driver;
    loop {
        std::thread::park();
    }
}

fn append_key_line(path: &str, kind: &str, value: &str) {
    let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let _ = writeln!(file, "{now} {kind} {value}");
}

/// Parsed key-file contents: the child's node key and its last heartbeat.
pub struct NodeKeyReport {
    pub node_hex: String,
    pub last_seen_ms: u64,
}

/// Read a node's key file: its node key and last heartbeat.
pub fn read_key_report(path: &std::path::Path) -> Option<NodeKeyReport> {
    let contents = std::fs::read_to_string(path).ok()?;
    let mut node_hex: Option<String> = None;
    let mut last_seen_ms = 0_u64;
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let Some(stamp) = parts.next().and_then(|value| value.parse::<u64>().ok()) else {
            continue;
        };
        let Some(kind) = parts.next() else {
            continue;
        };
        if kind == KEY_LINE_KIND {
            node_hex = parts.next().map(str::to_owned);
            last_seen_ms = last_seen_ms.max(stamp);
        } else if kind == ALIVE_LINE_KIND {
            last_seen_ms = last_seen_ms.max(stamp);
        }
    }
    Some(NodeKeyReport {
        node_hex: node_hex?,
        last_seen_ms,
    })
}
