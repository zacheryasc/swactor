//! Boot identity for a single diagnostics-emitting node.
//!
//! Every process that emits diagnostics declares its [`Identity`] at
//! startup. The identity is what binds an opaque `node_id_hex` to a
//! human-meaningful role and host. Without it nothing else parses,
//! which is why it is the *keystone* (per `DIAGNOSTICS_PLAN.md`,
//! principle 4).
//!
//! The identity is emitted to the collector on boot and is embedded in
//! the header of every snapshot.

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Role a node plays in a pipeline-parallel (or similar) run.
///
/// Open-ended on purpose — diagnostics is not pipeline-specific, but
/// `"orchestrator"` and `"stage"` are the two well-known values that
/// the post-processor understands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role(pub String);

impl Role {
    pub fn orchestrator() -> Self {
        Self("orchestrator".to_string())
    }
    pub fn stage() -> Self {
        Self("stage".to_string())
    }
    pub fn custom(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

/// Self-describing identity record for a single node in a run.
///
/// Stable for the lifetime of the process. Re-emitted in every
/// snapshot header so that any single record in the bundle is
/// interpretable on its own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Full hex of the node's public key. Never truncated in records.
    pub node_id_hex: String,
    /// First 8 hex chars — what local stdout logs typically print.
    pub node_id_short: String,
    pub role: Role,
    /// Stage index for pipeline roles. `None` for orchestrator.
    pub stage_index: Option<u32>,
    /// Total stages in the run. `None` if not applicable.
    pub stage_count: Option<u32>,
    /// Opaque string assigned by the run driver. Identical across all
    /// nodes participating in the same run.
    pub run_id: String,
    /// vast.ai contract id, or `None` if not on vast.ai.
    pub vastai_contract_id: Option<String>,
    /// Best-effort public ip of the host, from vast.ai metadata or an
    /// external reflection probe at boot.
    pub host_ip_public: Option<String>,
    pub host_country: Option<String>,
    pub datacenter_id: Option<String>,
    pub hostname: Option<String>,
    pub container_id: Option<String>,
    pub process_start_unix_ms: u64,
    /// Incremented on restart inside the same contract. Distinguishes
    /// reruns under one vast.ai contract id.
    pub boot_sequence: u32,
    pub binary_version: Option<String>,
    pub git_sha: Option<String>,
    pub iroh_version: Option<String>,
    /// Home relay URL at the time the identity was constructed.
    /// `None` if iroh has not yet picked one.
    pub home_relay_url_at_boot: Option<String>,
}

impl Identity {
    /// Construct an identity from the minimum required fields.
    ///
    /// All other fields default to `None`/`0` and can be set with the
    /// `with_*` builders.
    pub fn new(node_id: NodeId, role: Role, run_id: impl Into<String>) -> Self {
        let node_id_hex = hex_encode(&node_id.0);
        let node_id_short = node_id_hex.chars().take(8).collect();
        Self {
            node_id_hex,
            node_id_short,
            role,
            stage_index: None,
            stage_count: None,
            run_id: run_id.into(),
            vastai_contract_id: None,
            host_ip_public: None,
            host_country: None,
            datacenter_id: None,
            hostname: None,
            container_id: None,
            process_start_unix_ms: 0,
            boot_sequence: 0,
            binary_version: None,
            git_sha: None,
            iroh_version: None,
            home_relay_url_at_boot: None,
        }
    }

    pub fn with_stage(mut self, stage_index: u32, stage_count: u32) -> Self {
        self.stage_index = Some(stage_index);
        self.stage_count = Some(stage_count);
        self
    }

    pub fn with_process_start(mut self, unix_ms: u64) -> Self {
        self.process_start_unix_ms = unix_ms;
        self
    }

    pub fn with_boot_sequence(mut self, seq: u32) -> Self {
        self.boot_sequence = seq;
        self
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_short_id_matches_first_8_chars_of_full_hex() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        bytes[1] = 0xcd;
        bytes[2] = 0xef;
        bytes[3] = 0x12;
        let id = Identity::new(NodeId(bytes), Role::stage(), "run-x");
        assert_eq!(id.node_id_short.len(), 8);
        assert!(id.node_id_hex.starts_with(&id.node_id_short));
        assert_eq!(id.node_id_short, "abcdef12");
    }

    #[test]
    fn identity_roundtrips_through_json() {
        let id = Identity::new(NodeId([0u8; 32]), Role::orchestrator(), "run-y")
            .with_stage(2, 3)
            .with_process_start(1_700_000_000_000)
            .with_boot_sequence(4);
        let json = serde_json::to_string(&id).expect("serialize");
        let back: Identity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.node_id_hex, id.node_id_hex);
        assert_eq!(back.stage_index, Some(2));
        assert_eq!(back.stage_count, Some(3));
        assert_eq!(back.boot_sequence, 4);
        assert_eq!(back.role, Role::orchestrator());
    }
}
