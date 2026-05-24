//! A deterministic stub host kind used by the §7.6 cross-architecture
//! parity test (and only by it). Kept in `src/` rather than `tests/`
//! so the host-kind registry and validator can be shared with the
//! scenario loader, which a test-private definition cannot do.
//!
//! Behaviour: on every tick, the stub sends a fixed payload to every
//! other peer named in the host's own "peers" list and emits a
//! `RecordEvent` with the tick index. On `recv` it emits a
//! `RecordEvent` with the message envelope.
//!
//! This is a test fixture; production code does not depend on it.

use serde_json::json;

use crate::host::{Action, Host, HostFactory, HostMessage, KindTag, SnapshotBytes};
use crate::scenario::HostKindValidator;

// ──────────────────────────────────────────────────────────────────────
// Factory
// ──────────────────────────────────────────────────────────────────────

pub struct ParityStubFactory;

impl HostFactory for ParityStubFactory {
    fn kind_tag(&self) -> KindTag {
        "parity_stub"
    }
    fn build(
        &self,
        host_id: &str,
        _kind_config: &toml::value::Table,
        peers: &[String],
        _tick_period_ns: u64,
    ) -> Box<dyn Host> {
        Box::new(ParityStubHost::new(host_id, peers.to_vec()))
    }
}

/// Validator for the `parity_stub` host kind. Accepts any
/// `kind_config` that names a `peers` list of strings; the host's
/// behaviour uses that list to drive `Send`s.
pub struct ParityStubKindValidator;

impl HostKindValidator for ParityStubKindValidator {
    fn kind_tag(&self) -> &'static str {
        "parity_stub"
    }

    fn validate_config(&self, config: &toml::value::Table) -> Result<(), String> {
        let peers = config
            .get("peers")
            .ok_or_else(|| "required key missing: peers".to_string())?;
        let arr = peers
            .as_array()
            .ok_or_else(|| "peers must be an array of strings".to_string())?;
        for v in arr {
            v.as_str()
                .ok_or_else(|| "peers entries must be strings".to_string())?;
        }
        Ok(())
    }

    fn validate_initial_state(&self, state: &str) -> Result<(), String> {
        if state == "ready" {
            Ok(())
        } else {
            Err(format!(
                "unknown parity_stub initial_state {state:?}; expected \"ready\""
            ))
        }
    }
}

pub struct ParityStubHost {
    id: String,
    peers: Vec<String>,
    tick_count: u64,
    recv_count: u64,
}

impl ParityStubHost {
    pub fn new(id: impl Into<String>, peers: Vec<String>) -> Self {
        Self {
            id: id.into(),
            peers,
            tick_count: 0,
            recv_count: 0,
        }
    }
}

impl Host for ParityStubHost {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind_tag(&self) -> KindTag {
        "parity_stub"
    }

    fn tick(&mut self, now_ns: u64) -> Vec<Action> {
        self.tick_count += 1;
        let mut actions = Vec::new();
        let payload = json!({
            "kind": "tick_record",
            "tick": self.tick_count,
            "now_ns": now_ns,
            "from": self.id,
        });
        actions.push(Action::RecordEvent {
            kind_tag: "parity_stub".into(),
            event: serde_json::to_vec(&payload).unwrap(),
        });
        for peer in &self.peers {
            if peer == &self.id {
                continue;
            }
            // Fixed-length payload so byte length is identical
            // regardless of tick number formatting.
            let msg = json!({"kind": "ping", "tick": self.tick_count});
            actions.push(Action::Send {
                to: peer.clone(),
                encoded: serde_json::to_vec(&msg).unwrap(),
            });
        }
        actions
    }

    fn recv(&mut self, message: HostMessage, now_ns: u64) -> Vec<Action> {
        self.recv_count += 1;
        let record = match message {
            HostMessage::App(bytes) => json!({
                "kind": "app_recv",
                "now_ns": now_ns,
                "host": self.id,
                "n_bytes": bytes.len() as u64,
            }),
            HostMessage::TimerFired { token } => json!({
                "kind": "timer_recv",
                "now_ns": now_ns,
                "host": self.id,
                "token": token,
            }),
            HostMessage::SendFailed { to, .. } => json!({
                "kind": "send_failed_recv",
                "now_ns": now_ns,
                "host": self.id,
                "to": to,
            }),
        };
        vec![Action::RecordEvent {
            kind_tag: "parity_stub".into(),
            event: serde_json::to_vec(&record).unwrap(),
        }]
    }

    fn snapshot(&self) -> SnapshotBytes {
        serde_json::to_vec(&json!({
            "members": {},
            "self_incarnation": 0,
            "tick_count": self.tick_count,
            "recv_count": self.recv_count,
        }))
        .unwrap()
    }
}
