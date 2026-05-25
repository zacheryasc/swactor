//! Pipeline-stage host kind (RELAY_SPEC §5).
//!
//! Wraps the production stage supervisor lifecycle as a simulator
//! host kind. The MVP host kind models only the lifecycle (Cold →
//! Registering → Running → Halted) and the three diagnostic
//! emissions named in RELAY_SPEC §5.4 (`register_name`,
//! `stage_lifecycle`, `worker_exited`). It does not engage in
//! inter-stage application traffic (§5.1): its codec is intentionally
//! empty for the MVP.
//!
//! The §5.3 internal-cause exit semantics are: a `WorkerExit`
//! envelope arriving via `recv` produces exactly two actions in
//! order — `RecordEvent` carrying a `worker_exited` payload, then
//! `Halt`. The order is normative; reordering would lose the
//! diagnostic on runs that terminate adjacent to the exit time.

use std::collections::BTreeMap;

use serde_json::json;

use crate::host::{
    Action, EventBytes, Host, HostFactory, HostId, HostMessage, KindTag, SnapshotBytes,
};

const KIND_TAG: KindTag = "stage";

/// RELAY_SPEC §5.2 lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageState {
    Cold,
    Registering,
    Running,
    Halted,
}

impl StageState {
    pub fn as_str(&self) -> &'static str {
        match self {
            StageState::Cold => "Cold",
            StageState::Registering => "Registering",
            StageState::Running => "Running",
            StageState::Halted => "Halted",
        }
    }
}

pub struct StageHost {
    id: HostId,
    name: String,
    address: String,
    state: StageState,
    name_registry: BTreeMap<String, String>,
    last_exit_reason: Option<String>,
}

impl StageHost {
    pub fn new(id: impl Into<String>, name: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            address: address.into(),
            state: StageState::Cold,
            name_registry: BTreeMap::new(),
            last_exit_reason: None,
        }
    }

    fn lifecycle_event(&self, from: StageState, to: StageState) -> Action {
        Action::RecordEvent {
            kind_tag: KIND_TAG.to_string(),
            event: encode(&json!({
                "kind": "stage_lifecycle",
                "from": from.as_str(),
                "to": to.as_str(),
            })),
        }
    }
}

fn encode(v: &serde_json::Value) -> EventBytes {
    serde_json::to_vec(v).expect("stage host event serialises")
}

impl Host for StageHost {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind_tag(&self) -> KindTag {
        KIND_TAG
    }

    fn tick(&mut self, _now_ns: u64) -> Vec<Action> {
        // RELAY_SPEC §5.2 — the first tick drives Cold → Registering
        // and immediately Registering → Running. Each transition
        // emits exactly one `stage_lifecycle` event; the
        // Registering → Running transition additionally emits one
        // `register_name`. Subsequent ticks are no-ops.
        match self.state {
            StageState::Cold => {
                let mut actions = Vec::new();
                actions.push(self.lifecycle_event(StageState::Cold, StageState::Registering));
                self.state = StageState::Registering;
                // The transition to Running is synchronous in the
                // MVP — there is no out-of-band ack to wait for.
                let payload = json!({
                    "kind": "register_name",
                    "name": self.name,
                    "address": self.address,
                    "peer_node_id": self.id,
                });
                actions.push(Action::RecordEvent {
                    kind_tag: KIND_TAG.to_string(),
                    event: encode(&payload),
                });
                self.name_registry
                    .insert(self.name.clone(), self.address.clone());
                actions.push(self.lifecycle_event(StageState::Registering, StageState::Running));
                self.state = StageState::Running;
                actions
            }
            _ => Vec::new(),
        }
    }

    fn recv(&mut self, message: HostMessage, _now_ns: u64) -> Vec<Action> {
        match message {
            HostMessage::WorkerExit {
                reason,
                status_code,
                signal,
            } => {
                if self.state == StageState::Halted {
                    return Vec::new();
                }
                // RELAY_SPEC §5.3 — the recv for `WorkerExit` returns
                // exactly: (1) `RecordEvent` carrying the
                // `worker_exited` payload, then (2) `Halt`. The
                // lifecycle transition into `Halted` is emitted as
                // part of the same envelope so a streaming reader
                // sees the canonical Running → Halted edge.
                let mut actions = Vec::new();
                let mut payload = serde_json::Map::new();
                payload.insert("kind".to_string(), json!("worker_exited"));
                payload.insert("reason".to_string(), json!(reason));
                if let Some(s) = status_code {
                    payload.insert("status_code".to_string(), json!(s));
                }
                if let Some(s) = signal {
                    payload.insert("signal".to_string(), json!(s));
                }
                actions.push(Action::RecordEvent {
                    kind_tag: KIND_TAG.to_string(),
                    event: encode(&serde_json::Value::Object(payload)),
                });
                actions.push(self.lifecycle_event(self.state, StageState::Halted));
                self.last_exit_reason = Some(reason);
                self.state = StageState::Halted;
                actions.push(Action::Halt);
                actions
            }
            // App, TimerFired, SendFailed: the MVP stage host kind
            // does not engage in inter-stage application traffic
            // (§5.1) and does not schedule its own timers. Anything
            // arriving here is unsolicited; we drop it without
            // emitting actions, but the engine still records the
            // envelope's arrival through the bundle writer.
            _ => Vec::new(),
        }
    }

    fn snapshot(&self) -> SnapshotBytes {
        // RELAY_SPEC §5.4. Stage snapshot is opaque to the §9 bundle
        // schema for SWIM; the evaluator picks `name_registry` and
        // optionally `last_exit_reason` from it.
        let mut payload = json!({
            "state": self.state.as_str(),
            "name_registry": self.name_registry,
            "members": {},
            "self_incarnation": 0,
        });
        if self.state == StageState::Halted {
            if let Some(reason) = &self.last_exit_reason {
                payload["last_exit_reason"] = json!(reason);
            }
        }
        serde_json::to_vec(&payload).expect("stage snapshot serialises")
    }
}

// ──────────────────────────────────────────────────────────────────────
// Factory
// ──────────────────────────────────────────────────────────────────────

pub struct StageHostFactory;

impl HostFactory for StageHostFactory {
    fn kind_tag(&self) -> KindTag {
        KIND_TAG
    }
    fn build(
        &self,
        host_id: &str,
        kind_config: &toml::value::Table,
        _peers: &[String],
        _tick_period_ns: u64,
    ) -> Box<dyn Host> {
        let name = kind_config
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let address = kind_config
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Box::new(StageHost::new(host_id, name, address))
    }
}
