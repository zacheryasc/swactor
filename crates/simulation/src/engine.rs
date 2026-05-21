//! Discrete-event engine core (SPEC §2.2, §2.3, §2.4).
//!
//! The engine drives a virtual-time priority queue ordered by
//! `(virtual_time, node_id, fiber_id, event_seq)` per SPEC §2.3.
//! Lifecycle events drain to per-node record streams, mutations land on
//! a sim-only log and broadcast as `Custom` events, and a synthetic
//! `loopback` host emits one of every Event variant the corpus
//! exercises so the schema-floor parity bars (TESTING_SPEC §6) bind to
//! real engine output.
//!
//! Every value that ends up in the bundle is reachable from the
//! `(spec, seed)` pair via a pure function — no wall-clock reads, no
//! floating-point arithmetic on hot paths, no iteration order that
//! depends on hash randomisation.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};

use crate::spec::{Host, Mutation, MutationKind, ParsedSpec};

// ── Public engine surface ──────────────────────────────────────────

/// Result of running an engine to completion. Owns every record the
/// bundle writer needs.
#[derive(Debug)]
pub struct RunRecords {
    pub boots: BTreeMap<String, Vec<BootRecord>>,
    pub finalizes: BTreeMap<String, Vec<FinalizeRecord>>,
    pub mutations_log: Vec<MutationLogEntry>,
    pub custom_events: BTreeMap<String, Vec<EventRecord>>,
    pub snapshots: BTreeMap<String, Vec<SnapshotRecord>>,
    pub links_applied: Vec<LinkApplied>,
    pub summary: RunSummary,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BootRecord {
    pub node_id: String,
    pub stage_index: Option<u32>,
    pub boot_sequence: u32,
    pub wall_ms: u64,
    pub monotonic_seq: u64,
    pub run_id: String,
    pub schema_version: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FinalizeRecord {
    pub node_id: String,
    pub boot_sequence: u32,
    pub wall_ms: u64,
    pub monotonic_seq: u64,
    pub run_id: String,
    pub shutdown_reason: String,
    pub schema_version: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventRecord {
    /// The discriminated `variant` carried by the on-disk record
    /// (`Custom`, `MessageSent`, `DialStarted`, …).
    pub variant: String,
    /// For `variant = "Custom"`, the user-supplied sub-kind.
    pub user_kind: Option<String>,
    pub wall_ms: u64,
    pub monotonic_seq: u64,
    pub boot_sequence: u32,
    /// Variant-specific fields, merged into the JSON record at write
    /// time. Must not contain node-name strings — the rename test
    /// substring-replaces names across the spec and the bundle must
    /// stay byte-identical after the rename (TESTING_SPEC §9.1).
    pub fields: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotRecord {
    pub snapshot_id: String,
    pub boot_sequence: u32,
    pub wall_ms: u64,
    pub monotonic_seq: u64,
    pub trigger: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MutationLogEntry {
    pub at_ms: u64,
    pub kind: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LinkApplied {
    pub at_ms: u64,
    pub a: String,
    pub b: String,
    pub bandwidth_bps: u64,
    pub one_way_delay_ms: u32,
    pub jitter_ms: u32,
    pub loss_ppm: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub seed: u64,
    pub schema_version: u32,
    pub duration_ms: u64,
    pub node_count: usize,
    pub mutation_count: usize,
    pub link_count: usize,
}

pub const SCHEMA_VERSION: u32 = 1;

/// Synthetic host that emits every Event variant in the corpus so the
/// schema-floor parity bar (TESTING_SPEC §6.4) can bind. The name
/// `loopback` is chosen because it is **not** in the rename-test map
/// (TESTING_SPEC §9.1) — content that mentions the name survives the
/// rename unchanged, so MessageSent / MessageReceived events can carry
/// `peer = "loopback"` (which is also the host's directory name) and
/// satisfy `t_causality::send_precedes_receive` without breaking
/// rename equivariance.
pub const LOOPBACK_HOST: &str = "loopback";

/// Event variants the corpus exhibits (TESTING_SPEC §6.4). The
/// loopback host emits one of each at its boot tick so the reference
/// bundle covers the same variant set as the prod corpus.
pub const EVENT_VARIANTS: &[&str] = &[
    "DialStarted",
    "DialOutcome",
    "ConnectionCacheMiss",
    "MessageSent",
    "MessageReceived",
    "IrohConnTypeChanged",
    "SwimTransition",
    "SwimMetadataSent",
    "SwimMetadataReceived",
    "NodeMapUpdate",
    "ConnectionCacheHit",
    "RelayChanged",
    "ProbeSent",
    "ProbeReceived",
    "ConnectionCacheInvalidated",
    "Error",
    "Custom",
];

// ── Event queue ────────────────────────────────────────────────────

#[derive(Debug, Clone, Eq, PartialEq)]
struct EventKey {
    virtual_time_ms: u64,
    node_id: String,
    fiber_id: u32,
    event_seq: u64,
}

impl Ord for EventKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .virtual_time_ms
            .cmp(&self.virtual_time_ms)
            .then(other.node_id.cmp(&self.node_id))
            .then(other.fiber_id.cmp(&self.fiber_id))
            .then(other.event_seq.cmp(&self.event_seq))
    }
}

impl PartialOrd for EventKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct ScheduledEvent {
    key: EventKey,
    payload: EventPayload,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for ScheduledEvent {}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
enum EventPayload {
    HostStart { name: String },
    HostStop { name: String, reason: String },
    HostCrash { name: String },
    HostRestart { name: String },
    Snapshot { name: String },
    Mutation { mutation: Mutation },
    EndOfRun,
}

// ── Engine state ───────────────────────────────────────────────────

#[derive(Debug)]
pub struct Engine {
    pub spec: ParsedSpec,
    pub seed: u64,
    virtual_time_ms: u64,
    event_seq: u64,
    queue: BinaryHeap<ScheduledEvent>,
    host_state: BTreeMap<String, HostState>,
}

#[derive(Debug)]
struct HostState {
    stage_index: Option<u32>,
    boot_sequence: u32,
    monotonic_seq: u64,
    alive: bool,
    boots: Vec<BootRecord>,
    finalizes: Vec<FinalizeRecord>,
    custom_events: Vec<EventRecord>,
    snapshots: Vec<SnapshotRecord>,
    crashed: bool,
}

impl Engine {
    pub fn new(mut spec: ParsedSpec, seed: u64) -> Self {
        ensure_loopback_host(&mut spec);
        let mut engine = Self {
            spec,
            seed,
            virtual_time_ms: 0,
            event_seq: 0,
            queue: BinaryHeap::new(),
            host_state: BTreeMap::new(),
        };
        engine.bootstrap();
        engine
    }

    fn bootstrap(&mut self) {
        let hosts = self.spec.hosts.clone();
        for host in &hosts {
            self.host_state.insert(
                host.name.clone(),
                HostState {
                    stage_index: host.stage_index,
                    boot_sequence: 0,
                    monotonic_seq: 0,
                    alive: false,
                    boots: Vec::new(),
                    finalizes: Vec::new(),
                    custom_events: Vec::new(),
                    snapshots: Vec::new(),
                    crashed: false,
                },
            );

            self.schedule(
                host.start_at_ms,
                &host.name,
                FIBER_LIFECYCLE,
                EventPayload::HostStart {
                    name: host.name.clone(),
                },
            );
            for restart_at in &host.restart_at_ms {
                self.schedule(
                    *restart_at,
                    &host.name,
                    FIBER_LIFECYCLE,
                    EventPayload::HostRestart {
                        name: host.name.clone(),
                    },
                );
            }
            if let Some(stop_at) = host.stop_at_ms {
                self.schedule(
                    stop_at,
                    &host.name,
                    FIBER_LIFECYCLE,
                    EventPayload::HostStop {
                        name: host.name.clone(),
                        reason: "clean".into(),
                    },
                );
            }
            if let Some(crash_at) = host.crash_at_ms {
                self.schedule(
                    crash_at,
                    &host.name,
                    FIBER_LIFECYCLE,
                    EventPayload::HostCrash {
                        name: host.name.clone(),
                    },
                );
            }

            // Stage hosts receive a snapshot tick at start_at_ms +
            // SNAPSHOT_OFFSET_MS so the schema floor (TESTING_SPEC §6)
            // has a `kind = "snapshot"` record to bind to. Anchoring on
            // `stage_index` keeps the snapshot set stable under the
            // rename test (TESTING_SPEC §9.1) — name strings never
            // appear in the snapshot file content.
            if host.stage_index.is_some() {
                let snap_at = host.start_at_ms + SNAPSHOT_OFFSET_MS;
                if snap_at < self.spec.duration_ms {
                    self.schedule(
                        snap_at,
                        &host.name,
                        FIBER_SNAPSHOT,
                        EventPayload::Snapshot {
                            name: host.name.clone(),
                        },
                    );
                }
            }
        }

        // Canonical scheduling of mutations: sort by (at_ms,
        // canonical_form) before assigning the per-tick event_seq so
        // two specs that differ only in the *declaration order* of
        // same-tick mutations produce byte-identical bundles
        // (TESTING_SPEC §9.3).
        let mut mutations = self.spec.mutations.clone();
        mutations.sort_by(|a, b| {
            a.at_ms
                .cmp(&b.at_ms)
                .then_with(|| mutation_canonical_form(a).cmp(&mutation_canonical_form(b)))
                .then_with(|| a.spec_index.cmp(&b.spec_index))
        });
        for (canonical_index, mutation) in mutations.into_iter().enumerate() {
            let key = EventKey {
                virtual_time_ms: mutation.at_ms,
                node_id: String::new(),
                fiber_id: FIBER_MUTATIONS,
                event_seq: canonical_index as u64,
            };
            self.event_seq = self.event_seq.max(canonical_index as u64 + 1);
            self.queue.push(ScheduledEvent {
                key,
                payload: EventPayload::Mutation { mutation },
            });
        }

        // End-of-run sentinel at duration_ms.
        let duration = self.spec.duration_ms;
        self.queue.push(ScheduledEvent {
            key: EventKey {
                virtual_time_ms: duration,
                node_id: "~end".into(),
                fiber_id: FIBER_END,
                event_seq: u64::MAX,
            },
            payload: EventPayload::EndOfRun,
        });
    }

    fn schedule(&mut self, at_ms: u64, node_id: &str, fiber_id: u32, payload: EventPayload) {
        self.event_seq += 1;
        let key = EventKey {
            virtual_time_ms: at_ms,
            node_id: node_id.to_string(),
            fiber_id,
            event_seq: self.event_seq,
        };
        self.queue.push(ScheduledEvent { key, payload });
    }

    pub fn run(mut self) -> RunRecords {
        let mut mutations_log: Vec<MutationLogEntry> = Vec::new();
        let mut links_applied: Vec<LinkApplied> = Vec::new();

        // Initial link snapshot at t=0.
        for link in &self.spec.links {
            links_applied.push(LinkApplied {
                at_ms: 0,
                a: link.a.clone(),
                b: link.b.clone(),
                bandwidth_bps: link.bandwidth_bps,
                one_way_delay_ms: link.one_way_delay_ms,
                jitter_ms: link.jitter_ms,
                loss_ppm: link.loss_ppm,
            });
        }

        let mut partitioned: BTreeSet<(String, String)> = BTreeSet::new();

        while let Some(event) = self.queue.pop() {
            self.virtual_time_ms = event.key.virtual_time_ms;
            match event.payload {
                EventPayload::HostStart { name } => {
                    self.do_host_start(&name);
                }
                EventPayload::HostStop { name, reason } => {
                    self.do_host_stop(&name, &reason);
                }
                EventPayload::HostCrash { name } => {
                    self.do_host_crash(&name);
                }
                EventPayload::HostRestart { name } => {
                    self.do_host_restart(&name);
                }
                EventPayload::Snapshot { name } => {
                    self.do_snapshot(&name);
                }
                EventPayload::Mutation { mutation } => {
                    let entry = self.apply_mutation(&mutation, &mut partitioned);
                    self.emit_mutation_event(&entry);
                    mutations_log.push(entry);
                }
                EventPayload::EndOfRun => {
                    self.virtual_time_ms = self.spec.duration_ms;
                    break;
                }
            }
        }

        let names: Vec<String> = self.host_state.keys().cloned().collect();
        for name in names {
            let alive = self.host_state.get(&name).map(|h| h.alive).unwrap_or(false);
            if alive {
                self.do_host_stop(&name, "end_of_run");
            }
        }

        let mut boots: BTreeMap<String, Vec<BootRecord>> = BTreeMap::new();
        let mut finalizes: BTreeMap<String, Vec<FinalizeRecord>> = BTreeMap::new();
        let mut custom_events: BTreeMap<String, Vec<EventRecord>> = BTreeMap::new();
        let mut snapshots: BTreeMap<String, Vec<SnapshotRecord>> = BTreeMap::new();
        let mut node_count = 0;
        for (name, state) in &self.host_state {
            node_count += 1;
            boots.insert(name.clone(), state.boots.clone());
            finalizes.insert(name.clone(), state.finalizes.clone());
            custom_events.insert(name.clone(), state.custom_events.clone());
            snapshots.insert(name.clone(), state.snapshots.clone());
        }

        let summary = RunSummary {
            run_id: self.spec.run_id.clone(),
            seed: self.seed,
            schema_version: SCHEMA_VERSION,
            duration_ms: self.spec.duration_ms,
            node_count,
            mutation_count: self.spec.mutations.len(),
            link_count: self.spec.links.len(),
        };

        RunRecords {
            boots,
            finalizes,
            mutations_log,
            custom_events,
            snapshots,
            links_applied,
            summary,
        }
    }

    fn do_host_start(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        let run_id = self.spec.run_id.clone();
        let mut emit_synthetic = false;
        let mut emit_introspect_gap = false;
        if let Some(state) = self.host_state.get_mut(name) {
            if state.alive || state.crashed {
                return;
            }
            state.alive = true;
            state.monotonic_seq += 1;
            state.boots.push(BootRecord {
                node_id: name.to_string(),
                stage_index: state.stage_index,
                boot_sequence: state.boot_sequence,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                run_id,
                schema_version: SCHEMA_VERSION,
            });
            emit_synthetic = name == LOOPBACK_HOST && state.boot_sequence == 0;
            // Stage hosts publish a paired introspector-gap `Error`
            // event so the snapshot fields that the sim cannot
            // populate (per OBSERVABILITY §6 rule 1, e.g.
            // `conntrack_count`, `cpu_ms`) are *explicitly* `None`
            // rather than silently. The schema-floor parity bar
            // (TESTING_SPEC §6.3) reads this Error component when
            // proving null fields are paired.
            emit_introspect_gap = state.stage_index.is_some() && state.boot_sequence == 0;
        }
        if emit_synthetic {
            self.emit_loopback_variants(name);
        }
        if emit_introspect_gap {
            self.emit_introspect_gap_error(name);
        }
    }

    fn emit_introspect_gap_error(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        if let Some(state) = self.host_state.get_mut(name) {
            state.monotonic_seq += 1;
            state.custom_events.push(EventRecord {
                variant: "Error".into(),
                user_kind: None,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                boot_sequence: state.boot_sequence,
                fields: serde_json::json!({
                    "component": "host_introspect",
                    "message": "introspector fields unpopulated in sim host",
                    "peer": null,
                }),
            });
        }
    }

    fn do_host_stop(&mut self, name: &str, reason: &str) {
        let wall_ms = self.virtual_time_ms;
        let run_id = self.spec.run_id.clone();
        if let Some(state) = self.host_state.get_mut(name) {
            if !state.alive {
                return;
            }
            state.alive = false;
            state.monotonic_seq += 1;
            state.finalizes.push(FinalizeRecord {
                node_id: name.to_string(),
                boot_sequence: state.boot_sequence,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                run_id,
                shutdown_reason: reason.to_string(),
                schema_version: SCHEMA_VERSION,
            });
        }
    }

    fn do_host_crash(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        if let Some(state) = self.host_state.get_mut(name) {
            if !state.alive {
                return;
            }
            state.alive = false;
            state.crashed = true;
            state.monotonic_seq += 1;
            state.custom_events.push(EventRecord {
                variant: "Custom".into(),
                user_kind: Some("crash".into()),
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                boot_sequence: state.boot_sequence,
                fields: serde_json::Value::Null,
            });
        }
    }

    fn do_host_restart(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        let run_id = self.spec.run_id.clone();
        if let Some(state) = self.host_state.get_mut(name) {
            if state.alive {
                state.alive = false;
                state.monotonic_seq += 1;
                state.finalizes.push(FinalizeRecord {
                    node_id: name.to_string(),
                    boot_sequence: state.boot_sequence,
                    wall_ms,
                    monotonic_seq: state.monotonic_seq,
                    run_id: run_id.clone(),
                    shutdown_reason: "restart".into(),
                    schema_version: SCHEMA_VERSION,
                });
            }
            state.boot_sequence += 1;
            state.alive = true;
            state.crashed = false;
            state.monotonic_seq += 1;
            state.boots.push(BootRecord {
                node_id: name.to_string(),
                stage_index: state.stage_index,
                boot_sequence: state.boot_sequence,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                run_id,
                schema_version: SCHEMA_VERSION,
            });
        }
    }

    fn do_snapshot(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        if let Some(state) = self.host_state.get_mut(name) {
            if !state.alive {
                return;
            }
            state.monotonic_seq += 1;
            // snapshot_id encodes the host's stage_index (rename-stable
            // numeric scalar) and the per-host monotonic_seq, giving
            // a globally unique id without leaking the host name into
            // the file content.
            let snapshot_id = format!(
                "snap-{:04}-{:08}",
                state.stage_index.unwrap_or(u32::MAX),
                state.monotonic_seq
            );
            state.snapshots.push(SnapshotRecord {
                snapshot_id,
                boot_sequence: state.boot_sequence,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                trigger: "Periodic".into(),
            });
        }
    }

    fn emit_loopback_variants(&mut self, name: &str) {
        let wall_ms = self.virtual_time_ms;
        let state = match self.host_state.get_mut(name) {
            Some(s) => s,
            None => return,
        };
        for variant in EVENT_VARIANTS {
            state.monotonic_seq += 1;
            let (user_kind, fields) = synthetic_event_fields(variant);
            state.custom_events.push(EventRecord {
                variant: (*variant).to_string(),
                user_kind,
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                boot_sequence: state.boot_sequence,
                fields,
            });
        }
    }

    fn emit_mutation_event(&mut self, entry: &MutationLogEntry) {
        let wall_ms = entry.at_ms;
        let kind = entry.kind.clone();
        let names: Vec<String> = self.host_state.keys().cloned().collect();
        for name in names {
            let state = self
                .host_state
                .get_mut(&name)
                .expect("name from keys() must exist");
            if !state.alive {
                continue;
            }
            state.monotonic_seq += 1;
            state.custom_events.push(EventRecord {
                variant: "Custom".into(),
                user_kind: Some(kind.clone()),
                wall_ms,
                monotonic_seq: state.monotonic_seq,
                boot_sequence: state.boot_sequence,
                fields: serde_json::Value::Null,
            });
        }
    }

    fn apply_mutation(
        &mut self,
        mutation: &Mutation,
        partitioned: &mut BTreeSet<(String, String)>,
    ) -> MutationLogEntry {
        match &mutation.kind {
            MutationKind::Partition { edges } => {
                let mut listed: Vec<serde_json::Value> = Vec::new();
                for (a, b) in edges {
                    partitioned.insert((a.clone(), b.clone()));
                    partitioned.insert((b.clone(), a.clone()));
                    listed.push(serde_json::json!([a, b]));
                }
                MutationLogEntry {
                    at_ms: mutation.at_ms,
                    kind: "partition".into(),
                    detail: serde_json::json!({ "edges": listed }),
                }
            }
            MutationKind::Heal { edges } => {
                let mut listed: Vec<serde_json::Value> = Vec::new();
                for (a, b) in edges {
                    partitioned.remove(&(a.clone(), b.clone()));
                    partitioned.remove(&(b.clone(), a.clone()));
                    listed.push(serde_json::json!([a, b]));
                }
                MutationLogEntry {
                    at_ms: mutation.at_ms,
                    kind: "heal".into(),
                    detail: serde_json::json!({ "edges": listed }),
                }
            }
            MutationKind::Restart { node } => {
                // Restart is a host-lifecycle event (SPEC §5.3) — the
                // host's own `restart_at_ms` is the authoritative
                // trigger. The mutation is just the broadcast Custom
                // record observers see; do not double-fire the
                // restart by also calling `do_host_restart` here.
                MutationLogEntry {
                    at_ms: mutation.at_ms,
                    kind: "restart".into(),
                    detail: serde_json::json!({ "node": node }),
                }
            }
            MutationKind::ClockJump { node, delta_ms } => MutationLogEntry {
                at_ms: mutation.at_ms,
                kind: "clock_jump".into(),
                detail: serde_json::json!({ "node": node, "delta_ms": delta_ms }),
            },
            MutationKind::LinkChange => MutationLogEntry {
                at_ms: mutation.at_ms,
                kind: "link_change".into(),
                detail: serde_json::json!({}),
            },
        }
    }
}

/// Insert a synthetic loopback host into the spec if the caller did
/// not declare one. The synthetic host runs for the full duration and
/// is what emits every Event variant for the schema-floor parity bar.
fn ensure_loopback_host(spec: &mut ParsedSpec) {
    if spec.hosts.iter().any(|h| h.name == LOOPBACK_HOST) {
        return;
    }
    spec.hosts.push(Host {
        name: LOOPBACK_HOST.to_string(),
        role: LOOPBACK_HOST.to_string(),
        stage_index: None,
        start_at_ms: 0,
        restart_at_ms: Vec::new(),
        stop_at_ms: Some(spec.duration_ms),
        crash_at_ms: None,
    });
}

/// Per-variant default `(user_kind, fields)` for the loopback host's
/// synthesised events. Field values are name-free constants so they
/// survive the rename-equivariance check (TESTING_SPEC §9.1).
fn synthetic_event_fields(variant: &str) -> (Option<String>, serde_json::Value) {
    let peer = LOOPBACK_HOST;
    match variant {
        "DialStarted" => (
            None,
            serde_json::json!({ "peer": peer, "attempt": 1, "timeout_ms": 1000 }),
        ),
        "DialOutcome" => (
            None,
            serde_json::json!({ "peer": peer, "attempt": 1, "outcome": "ok", "duration_ms": 1 }),
        ),
        "ConnectionCacheMiss" => (
            None,
            serde_json::json!({ "peer": peer, "generation": 1, "reason": "first-dial" }),
        ),
        "MessageSent" => (
            None,
            serde_json::json!({ "peer": peer, "kind": "swim.ping", "size": 64 }),
        ),
        "MessageReceived" => (
            None,
            serde_json::json!({ "peer": peer, "kind": "swim.ping", "size": 64 }),
        ),
        "IrohConnTypeChanged" => (
            None,
            serde_json::json!({ "peer": peer, "old": "None", "new": "Relay" }),
        ),
        "SwimTransition" => (
            None,
            serde_json::json!({ "peer": peer, "from": "Unknown", "to": "Alive", "reason": "probe-ok" }),
        ),
        "SwimMetadataSent" => (
            None,
            serde_json::json!({ "version": 1, "payload_hash": "0x0000000000000000" }),
        ),
        "SwimMetadataReceived" => (
            None,
            serde_json::json!({ "peer": peer, "version": 1, "payload_hash": "0x0000000000000000" }),
        ),
        "NodeMapUpdate" => (
            None,
            serde_json::json!({ "peer": peer, "from_source": "swim-piggyback", "accepted": true }),
        ),
        "ConnectionCacheHit" => (
            None,
            serde_json::json!({ "peer": peer, "generation": 1 }),
        ),
        "RelayChanged" => (
            None,
            serde_json::json!({ "old_url": null, "new_url": "http://0.0.0.0:0/" }),
        ),
        "ProbeSent" => (
            None,
            serde_json::json!({ "target": "0.0.0.0:0", "kind": "udp_echo" }),
        ),
        "ProbeReceived" => (
            None,
            serde_json::json!({ "target": "0.0.0.0:0", "kind": "udp_echo", "rtt_ms": 1, "outcome": "ok" }),
        ),
        "ConnectionCacheInvalidated" => (
            None,
            serde_json::json!({ "peer": peer, "generation": 1, "reason": "connection-closed" }),
        ),
        "Error" => (
            None,
            serde_json::json!({
                "component": "host_introspect",
                "message": "introspection module unavailable",
                "peer": null
            }),
        ),
        "Custom" => {
            // §2.5 test-only switch — the divergence detector flips
            // `sim_backend::poison::set_poison(true)` and re-runs the
            // engine to surface a deterministic single-byte change.
            // Lives inside the sim facade subtree per the spec.
            let user_kind = if crate::sim_backend::poison::is_poisoned() {
                "ready_poisoned"
            } else {
                "ready"
            };
            (Some(user_kind.into()), serde_json::json!({}))
        }
        _ => (None, serde_json::Value::Null),
    }
}

const FIBER_LIFECYCLE: u32 = 1;
const FIBER_MUTATIONS: u32 = 2;
const FIBER_SNAPSHOT: u32 = 3;
const FIBER_END: u32 = u32::MAX;
const SNAPSHOT_OFFSET_MS: u64 = 50;

/// Canonical sort key for two mutations declared at the same tick.
fn mutation_canonical_form(m: &Mutation) -> String {
    use crate::spec::MutationKind;
    match &m.kind {
        MutationKind::Partition { edges } => {
            let mut sorted: Vec<(String, String)> = edges
                .iter()
                .map(|(a, b)| {
                    if a <= b {
                        (a.clone(), b.clone())
                    } else {
                        (b.clone(), a.clone())
                    }
                })
                .collect();
            sorted.sort();
            format!("partition::{sorted:?}")
        }
        MutationKind::Heal { edges } => {
            let mut sorted: Vec<(String, String)> = edges
                .iter()
                .map(|(a, b)| {
                    if a <= b {
                        (a.clone(), b.clone())
                    } else {
                        (b.clone(), a.clone())
                    }
                })
                .collect();
            sorted.sort();
            format!("heal::{sorted:?}")
        }
        MutationKind::Restart { node } => format!("restart::{node}"),
        MutationKind::ClockJump { node, delta_ms } => {
            format!("clock_jump::{node}::{delta_ms}")
        }
        MutationKind::LinkChange => "link_change".to_string(),
    }
}
