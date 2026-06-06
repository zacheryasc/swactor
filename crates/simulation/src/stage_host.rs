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

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::host::{
    Action, EventBytes, Host, HostFactory, HostId, HostMessage, KindTag, SnapshotBytes,
};

const KIND_TAG: KindTag = "stage";

// ──────────────────────────────────────────────────────────────────────
// Datastream-style channel records
// ──────────────────────────────────────────────────────────────────────
//
// The datastream's `ChannelId` is open/string-based: a producer may
// emit typed records on channels the catalog has never heard of, and
// consumers retain unknown channels whole (spec §6.3). The sim host
// leans on exactly that extensibility — its subprocess / inference /
// relay facts are sim-defined records on sim-defined channels, framed
// in the bundle as `{kind: "channel_record", channel, record}` so a
// bundle reader dispatches on the channel name the same way the
// dashboard's `FleetView` dispatches on frame channels.

/// Channel carrying [`SubprocessLifecycle`] records.
pub const SUBPROCESS_LIFECYCLE_CHANNEL: &str = "subprocess.lifecycle";
/// Channel carrying [`InferenceResponse`] records.
pub const INFERENCE_RESPONSE_CHANNEL: &str = "inference.response";

/// One subprocess lifecycle fact: spawned / ready / exited.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubprocessLifecycle {
    /// `"spawned"`, `"ready"`, or `"exited"`.
    pub phase: String,
    pub label: String,
    pub pid: u32,
    #[serde(default)]
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_ms: Option<u64>,
}

/// The response-leg send outcome for one inference request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub target_peer: distribution::types::NodeId,
    pub request_id: String,
    pub byte_size: u64,
    /// One of `"success"`, `"timeout"`, `"connection_closed"`,
    /// `"refused"`, `"unresolved"`, `"queued_unacked"`.
    pub send_outcome: String,
}

/// Per-snapshot tunnel-state block. Sim-defined (the old production
/// `Tier2RelaySession` shape died with the diagnostics subsystem);
/// defaults to `unknown / derived` per spec §2 honesty-under-absence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelaySession {
    pub relay_url: Option<String>,
    pub status: String,
    pub status_source: String,
    #[serde(default)]
    pub status_changed_at_ms: Option<u64>,
    #[serde(default)]
    pub status_entered_at_ms: Option<u64>,
    #[serde(default)]
    pub last_send_at_ms: Option<u64>,
    #[serde(default)]
    pub last_recv_at_ms: Option<u64>,
    #[serde(default)]
    pub tx_bytes_total: Option<u64>,
    #[serde(default)]
    pub rx_bytes_total: Option<u64>,
}

/// Per-snapshot subprocess block built from the scenario-configured
/// subprocess fake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubprocessState {
    pub subprocesses: Vec<Subprocess>,
    /// Virtual-time stamp (§7.1: never the host wall clock).
    pub scraped_at_ms: u64,
}

/// One subprocess row in [`SubprocessState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subprocess {
    pub label: String,
    pub pid: u32,
    #[serde(default)]
    pub parent_pid: Option<u32>,
    pub status: String,
    #[serde(default)]
    pub spawn_at_ms: Option<u64>,
    #[serde(default)]
    pub exit_at_ms: Option<u64>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub exit_signal: Option<i32>,
    #[serde(default)]
    pub cmdline: Option<String>,
}

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
    /// Sim cross-pollination F2: per-snapshot tunnel-state field a
    /// scenario can configure. When set, the stage host renders a
    /// [`RelaySession`] in its snapshot under the `relay_session` key.
    /// Defaults to `unknown / derived` per spec §2 honesty-under-absence.
    relay_session: RelaySession,
    /// Sim cross-pollination F1: per-snapshot subprocess block driven
    /// by the scenario's `subprocess_fake` config. Populated lazily
    /// from `subprocess_fake_spec` on the first tick — emitting a
    /// `spawned` [`SubprocessLifecycle`] record then either staying in
    /// `running` (when `never_ready=true`, the
    /// "spawned-stayed-alive-no-output" bucket from spec §4) or
    /// transitioning to `exited` and emitting an `exited` record.
    subprocess_fake_spec: Option<SubprocessFakeSpec>,
    subprocess_fake_state: Option<SubprocessFakeState>,
    /// Coverage 2.4: scenario-driven inference response-leg fake. When
    /// set, the stage host emits one [`InferenceResponse`] record at
    /// `fire_at_ns`, carrying the declared target / request / size /
    /// outcome discriminator. The `send_outcome` mirrors the
    /// iroh-level result set: `success` / `timeout` /
    /// `connection_closed` / `refused` / `unresolved` /
    /// `queued_unacked`.
    inference_fake_spec: Option<InferenceFakeSpec>,
    inference_fake_fired: bool,
}

/// Scenario-driven configuration for the coverage 2.4 inference
/// response-leg fake. Drives the last stage's emission of the typed
/// `InferenceResponseSent` event under a chosen outcome, so the
/// bundle reader can match "the response did not arrive" against
/// "stage-N tried to send and the transport returned X."
///
/// The scenario or test sets the spec; the stage host fires exactly
/// one event at `fire_at_ns`. `target_peer_node_id_hex` is the
/// orchestrator's `NodeId`-hex; absent the host renders the hex
/// it received literally — honesty-under-absence.
#[derive(Debug, Clone)]
pub struct InferenceFakeSpec {
    pub fire_at_ns: u64,
    pub target_peer_node_id: distribution::types::NodeId,
    pub request_id: String,
    pub byte_size: u64,
    /// One of `"success"`, `"timeout"`, `"connection_closed"`,
    /// `"refused"`, `"unresolved"`, `"queued_unacked"`. The host
    /// emits the value verbatim; the production stage actor's
    /// emitter validates the discriminator before emit. Keeping the
    /// sim permissive surfaces test-author typos as bundle-reader
    /// confusion rather than silent acceptance.
    pub send_outcome: String,
}

/// Scenario-driven configuration for the F1 subprocess fake. The
/// engine knows nothing about subprocesses; this drives the stage
/// host's emission of the §4 lifecycle records and the per-snapshot
/// `subprocess` block.
#[derive(Debug, Clone)]
pub struct SubprocessFakeSpec {
    pub label: String,
    pub pid: u32,
    pub command: String,
    /// When `true`, the stage host emits `SubprocessSpawned` but
    /// *no* following `worker_ready` Custom event and *no*
    /// `SubprocessExited` — exactly the "spawned-stayed-alive-but-
    /// never-produced-protocol-output" scenario spec §4 calls out as
    /// one of the three buckets the bundle reader must be able to
    /// distinguish.
    pub never_ready: bool,
    /// When `Some(ns)`, the subprocess "exits" `ns` virtual-time
    /// after spawn, with the given exit code/signal. When `None`,
    /// the subprocess stays running for the whole run.
    pub exit_after_ns: Option<u64>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
}

#[derive(Debug, Clone)]
struct SubprocessFakeState {
    spec: SubprocessFakeSpec,
    spawn_at_ns: u64,
    exited_at_ns: Option<u64>,
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
            relay_session: default_unknown_relay_session(),
            subprocess_fake_spec: None,
            subprocess_fake_state: None,
            inference_fake_spec: None,
            inference_fake_fired: false,
        }
    }

    /// F2: scenario-driven override of the per-snapshot tunnel
    /// state. Use to model "tunnel up" / "tunnel down" /
    /// "tunnel unknown" for a simulated node.
    pub fn set_relay_session(&mut self, session: RelaySession) {
        self.relay_session = session;
    }

    /// F1: scenario-driven subprocess fake. After this is set, the
    /// host's next `tick` emits a `spawned` [`SubprocessLifecycle`]
    /// record on the `subprocess.lifecycle` channel and populates the
    /// per-snapshot subprocess block. Behaviour after that is driven
    /// by the spec's `never_ready` / `exit_after_ns` flags.
    pub fn set_subprocess_fake(&mut self, spec: SubprocessFakeSpec) {
        self.subprocess_fake_spec = Some(spec);
    }

    /// Coverage 2.4: scenario-driven inference response-leg fake. The
    /// next `tick` whose `now_ns >= spec.fire_at_ns` emits exactly
    /// one [`InferenceResponse`] record with the declared
    /// discriminator. Subsequent ticks are no-ops for this surface.
    pub fn set_inference_fake(&mut self, spec: InferenceFakeSpec) {
        self.inference_fake_spec = Some(spec);
        self.inference_fake_fired = false;
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

/// Spec §2 honesty-under-absence default: a simulated stage with no
/// scenario-configured tunnel state emits `unknown / derived` rather
/// than fabricating a `connected` or `disconnected` claim.
fn default_unknown_relay_session() -> RelaySession {
    RelaySession {
        relay_url: None,
        status: "unknown".to_string(),
        status_source: "derived".to_string(),
        status_changed_at_ms: None,
        status_entered_at_ms: None,
        last_send_at_ms: None,
        last_recv_at_ms: None,
        tx_bytes_total: None,
        rx_bytes_total: None,
    }
}

/// Emit a sim Event carrying a typed record on a named channel —
/// the datastream frame idiom (`channel` tags the lane, the record is
/// the payload). A bundle reader dispatches on the channel name the
/// same way the dashboard's `FleetView` dispatches on frame channels.
fn emit_channel_record<R: Serialize>(kind_tag: &str, channel: &str, record: &R) -> Action {
    let inner = serde_json::to_value(record).unwrap_or(serde_json::Value::Null);
    let payload = json!({ "kind": "channel_record", "channel": channel, "record": inner });
    Action::RecordEvent {
        kind_tag: kind_tag.to_string(),
        event: encode(&payload),
    }
}

impl Host for StageHost {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind_tag(&self) -> KindTag {
        KIND_TAG
    }

    fn tick(&mut self, now_ns: u64) -> Vec<Action> {
        // RELAY_SPEC §5.2 — the first tick drives Cold → Registering
        // and immediately Registering → Running. Each transition
        // emits exactly one `stage_lifecycle` event; the
        // Registering → Running transition additionally emits one
        // `register_name`. Subsequent ticks are no-ops, except for
        // the F1 subprocess fake which can fire an exit event after
        // `exit_after_ns` virtual time has elapsed.
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
                // Sim cross-pollination F1: spec §4 lifecycle record
                // for the configured subprocess fake. On spawn, emit a
                // `spawned` record on the subprocess channel. If the
                // spec opts into `never_ready=false`, the companion
                // `ready` record is emitted too — distinguishing
                // "spawned and running, worker reported ready" from
                // "spawned and running, never produced protocol
                // output."
                if let Some(spec) = self.subprocess_fake_spec.take() {
                    actions.push(emit_channel_record(
                        KIND_TAG,
                        SUBPROCESS_LIFECYCLE_CHANNEL,
                        &SubprocessLifecycle {
                            phase: "spawned".to_string(),
                            label: spec.label.clone(),
                            pid: spec.pid,
                            command: spec.command.clone(),
                            exit_code: None,
                            exit_signal: None,
                            uptime_ms: None,
                        },
                    ));
                    if !spec.never_ready {
                        actions.push(emit_channel_record(
                            KIND_TAG,
                            SUBPROCESS_LIFECYCLE_CHANNEL,
                            &SubprocessLifecycle {
                                phase: "ready".to_string(),
                                label: spec.label.clone(),
                                pid: spec.pid,
                                command: spec.command.clone(),
                                exit_code: None,
                                exit_signal: None,
                                uptime_ms: None,
                            },
                        ));
                    }
                    self.subprocess_fake_state = Some(SubprocessFakeState {
                        spec,
                        spawn_at_ns: now_ns,
                        exited_at_ns: None,
                    });
                }
                actions
            }
            StageState::Running => {
                let mut actions = Vec::new();
                // Coverage 2.4: fire the inference response-leg event
                // when its scheduled time has arrived. Exactly one
                // emission per spec — `inference_fake_fired` guards
                // against re-emit on later ticks.
                if !self.inference_fake_fired {
                    if let Some(spec) = self.inference_fake_spec.as_ref() {
                        if now_ns >= spec.fire_at_ns {
                            actions.push(emit_channel_record(
                                KIND_TAG,
                                INFERENCE_RESPONSE_CHANNEL,
                                &InferenceResponse {
                                    target_peer: spec.target_peer_node_id,
                                    request_id: spec.request_id.clone(),
                                    byte_size: spec.byte_size,
                                    send_outcome: spec.send_outcome.clone(),
                                },
                            ));
                            self.inference_fake_fired = true;
                        }
                    }
                }
                if let Some(state) = self.subprocess_fake_state.as_mut() {
                    if state.exited_at_ns.is_none() {
                        if let Some(after_ns) = state.spec.exit_after_ns {
                            if now_ns >= state.spawn_at_ns.saturating_add(after_ns) {
                                let uptime_ns = now_ns.saturating_sub(state.spawn_at_ns);
                                actions.push(emit_channel_record(
                                    KIND_TAG,
                                    SUBPROCESS_LIFECYCLE_CHANNEL,
                                    &SubprocessLifecycle {
                                        phase: "exited".to_string(),
                                        label: state.spec.label.clone(),
                                        pid: state.spec.pid,
                                        command: state.spec.command.clone(),
                                        exit_code: state.spec.exit_code,
                                        exit_signal: state.spec.exit_signal,
                                        uptime_ms: Some(uptime_ns / 1_000_000),
                                    },
                                ));
                                state.exited_at_ns = Some(now_ns);
                            }
                        }
                    }
                }
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
        // optionally `last_exit_reason` from it. Sim cross-pollination
        // adds two typed nested blocks so a bundle reader can decode
        // the same facts a real node's datastream carries (per spec
        // §"Sim cross-pollination"):
        //   - `relay_session`: a [`RelaySession`]
        //   - `subprocess`:    a [`SubprocessState`]
        let mut payload = json!({
            "state": self.state.as_str(),
            "name_registry": self.name_registry,
            "members": {},
            "self_incarnation": 0,
            "relay_session": serde_json::to_value(&self.relay_session)
                .expect("RelaySession serialises by construction"),
        });
        if let Some(subprocess) = self.subprocess_snapshot() {
            payload["subprocess"] = serde_json::to_value(&subprocess)
                .expect("SubprocessState serialises by construction");
        }
        if self.state == StageState::Halted {
            if let Some(reason) = &self.last_exit_reason {
                payload["last_exit_reason"] = json!(reason);
            }
        }
        serde_json::to_vec(&payload).expect("stage snapshot serialises")
    }
}

impl StageHost {
    /// Build the [`SubprocessState`] block from the scenario-configured
    /// subprocess fake. `None` when no fake is configured, in which
    /// case the snapshot omits the block (honesty-under-absence).
    fn subprocess_snapshot(&self) -> Option<SubprocessState> {
        let state = self.subprocess_fake_state.as_ref()?;
        let (status, exit_code, exit_signal, exit_at_ms) = match state.exited_at_ns {
            Some(ns) => (
                "exited".to_string(),
                state.spec.exit_code,
                state.spec.exit_signal,
                Some(ns / 1_000_000),
            ),
            None => ("running".to_string(), None, None, None),
        };
        Some(SubprocessState {
            subprocesses: vec![Subprocess {
                label: state.spec.label.clone(),
                pid: state.spec.pid,
                parent_pid: None,
                status,
                spawn_at_ms: Some(state.spawn_at_ns / 1_000_000),
                exit_at_ms,
                exit_code,
                exit_signal,
                cmdline: Some(state.spec.command.clone()),
            }],
            // §7.1: never read the host wall clock — use virtual time
            // (the spawn ns we already have).
            scraped_at_ms: state.spawn_at_ns / 1_000_000,
        })
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
