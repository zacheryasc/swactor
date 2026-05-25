//! Scenario loader (SIM_SPEC §8).
//!
//! Parses and validates a TOML scenario file into a [`Scenario`] value
//! the engine consumes. The loader owns no state: it is a pure function
//! from input bytes (and an optional base resolver) to a validated
//! scenario.
//!
//! The §8 schema is the simulator's only input. Anything the engine,
//! network, hosts, bundle writer, or assertion evaluator read at runtime
//! lives inside the parsed value this module returns; no other code in
//! the simulator reads files outside the bundle output path.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────

/// A fully validated scenario, ready to feed to the engine.
///
/// Field order matches §8.1; field shapes are normalised (defaults
/// flattened, host-kind configs resolved against their kind's validator,
/// link policies resolved per edge).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub seed: u64,
    pub duration_ns: u64,
    #[serde(default)]
    pub early_terminate_on_all_assertions_resolved: bool,
    pub default_tick: DefaultTick,
    pub default_link: LinkPolicy,
    pub peers: Vec<Peer>,
    #[serde(default)]
    pub relays: Vec<Relay>,
    pub links: Vec<Link>,
    #[serde(default)]
    pub mutations: Vec<Mutation>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub assertions: Vec<Assertion>,
    /// Per-ordered-host-pair route resolution. Built by the loader
    /// from `links` (and any `via` shorthands the loader expanded into
    /// explicit relay legs). For a direct edge, the route names the
    /// edge directly; for a relayed route, it names the inbound leg,
    /// the relay, and the outbound leg. Networks consume this to
    /// avoid re-deriving routes per send. Always skipped on serialize
    /// — the loader re-derives it from `links` + `relays`, so
    /// emitting it would duplicate the info and risk drift.
    #[serde(default, skip_serializing)]
    pub routes: Vec<HostRoute>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultTick {
    pub period_ns: u64,
}

/// Per-edge link policy. Every field is integer-valued per SIM_SPEC §7.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkPolicy {
    pub latency_ns: u64,
    pub jitter_stddev_ns: u64,
    pub loss_prob_ppm: u32,
    pub reorder_prob_ppm: u32,
    pub bandwidth_bps: u64,
    pub cold_dial_penalty_ns: u64,
    pub cache_warm_after_ns: u64,
    pub cache_invalidate_after_idle_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub kind: String,
    pub kind_config: toml::value::Table,
    pub initial_state: String,
    /// `None` ⇒ inherit `default_tick.period_ns`.
    #[serde(default)]
    pub tick_period_ns_override: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub from: String,
    pub to: String,
    #[serde(flatten)]
    pub policy: LinkPolicy,
}

/// A relay vertex (RELAY_SPEC §4). Not a host: it has no host trait,
/// no tick, no recv. It is part of the network topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relay {
    pub id: String,
    pub ingress_capacity_bps: u64,
    pub egress_capacity_bps_per_link: u64,
    pub queue_depth_bytes: u64,
    #[serde(default)]
    pub cold_start_penalty_ns: u64,
}

/// A resolved route between two hosts. Direct = one edge from host
/// `from` to host `to`. Relayed = inbound edge `from→relay`, then the
/// relay's queues, then outbound edge `relay→to`. Multi-hop relays
/// are rejected by the loader (RELAY_SPEC §4.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HostRoute {
    Direct {
        from: String,
        to: String,
    },
    Relayed {
        from: String,
        relay: String,
        to: String,
    },
}

impl HostRoute {
    pub fn from(&self) -> &str {
        match self {
            HostRoute::Direct { from, .. } | HostRoute::Relayed { from, .. } => from,
        }
    }
    pub fn to(&self) -> &str {
        match self {
            HostRoute::Direct { to, .. } | HostRoute::Relayed { to, .. } => to,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    pub at_ns: u64,
    #[serde(flatten)]
    pub kind: MutationKind,
}

/// One closed enum across the §5.5 variants. Field shape per variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MutationKind {
    Partition {
        peers_a: Vec<String>,
        peers_b: Vec<String>,
    },
    Heal,
    LatencySpike {
        links: Vec<LinkRef>,
        factor_x100: u32,
        duration_ns: u64,
    },
    LossBurst {
        links: Vec<LinkRef>,
        prob_ppm: u32,
        duration_ns: u64,
    },
    RelayBuffer {
        links: Vec<LinkRef>,
        floor_ns: u64,
        duration_ns: u64,
    },
    PeerKill {
        peer: String,
    },
    PeerResurrect {
        peer: String,
        preserve_state: bool,
    },
    /// RELAY_SPEC §6.1. Delivers a `WorkerExit` envelope to the named
    /// peer at the mutation's time. Targeting a host kind that does
    /// not accept it aborts the run.
    WorkerExit {
        peer: String,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signal: Option<i32>,
    },
    /// RELAY_SPEC §4.5. Relay stops forwarding; queued messages
    /// invalidate; later sends drop with `RelayDown`.
    RelayKill {
        relay: String,
    },
    /// RELAY_SPEC §4.5. Relay returns to service in `Booting` state.
    /// The next forwarded message pays the cold-start penalty.
    RelayBoot {
        relay: String,
    },
    /// RELAY_SPEC §4.5. Replace any subset of the three policy fields
    /// at the mutation's virtual time.
    RelayCapacityChange {
        relay: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ingress_capacity_bps: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        egress_capacity_bps_per_link: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        queue_depth_bytes: Option<u64>,
    },
    /// Spec §"Sim cross-pollination" F3 — cut the (`relay`,
    /// `from`, `to`) relay-mediated peer-connection while leaving
    /// the relay itself otherwise functional for every other pair.
    /// Distinct from `RelayKill` (which takes the entire relay
    /// down) and from `Partition` (which cuts traffic regardless of
    /// the route). Models the 2026-05-25 "tunnel up, peer-via-
    /// tunnel down" asymmetry: stage-2's tunnel to the relay stays
    /// alive but the relay→stage-2 leg silently drops, so the
    /// orchestrator's relay-mediated sends to stage-2 fail while
    /// stage-2 itself sees no tunnel-state change. `duration_ns =
    /// 0` means permanent (until run end).
    RelayPeerConnDown {
        relay: String,
        from: String,
        to: String,
        #[serde(default)]
        duration_ns: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkRef {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub at_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assertion {
    #[serde(flatten)]
    pub kind: AssertionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssertionKind {
    AllAliveAt {
        at_ns: u64,
        peers: Vec<String>,
    },
    AllAliveThroughout {
        window_start_ns: u64,
        window_end_ns: u64,
        peers: Vec<String>,
    },
    ConvergenceAfter {
        after_ns: u64,
        within_ns: u64,
        peers: Vec<String>,
    },
    NoFlapWhileProbesOk {
        peer: String,
        window_start_ns: u64,
        window_end_ns: u64,
    },
    NoDeadWhenProbesOk {
        peer: String,
        window_start_ns: u64,
        window_end_ns: u64,
    },
    SelfIncarnationBounded {
        peer: String,
        max_value: u64,
    },
    MessageSizeBounded {
        message_kind: String,
        max_bytes: u64,
    },
    DeadPeerResurrectsWithin {
        peer: String,
        after_ns: u64,
        within_ns: u64,
    },
    EventCount {
        event_kind: String,
        max: u64,
    },
    EventRate {
        event_kind: String,
        window_ns: u64,
        max_per_window: u64,
    },
    /// RELAY_SPEC §7.1. The named relay's `enqueued_bytes` never
    /// exceeds `max_bytes` across the (defaults: whole-run) window.
    RelayQueueDepthBounded {
        relay: String,
        max_bytes: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window_start_ns: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window_end_ns: Option<u64>,
    },
    /// RELAY_SPEC §7.1. The named peer's lifecycle state remains
    /// `Running` throughout the window. Fails on any
    /// `stage_lifecycle` event into `Halted` whose time falls in the
    /// window.
    WorkerAliveThroughout {
        peer: String,
        window_start_ns: u64,
        window_end_ns: u64,
    },
    /// RELAY_SPEC §7.1. Starting at `from_ns`, every observer in
    /// `observers` produces a snapshot whose `name_registry`
    /// contains `name` within `within_ns`.
    NameResolvesWithin {
        name: String,
        observers: Vec<String>,
        within_ns: u64,
        from_ns: u64,
    },
}

// ──────────────────────────────────────────────────────────────────────
// Host-kind validation (delegated)
// ──────────────────────────────────────────────────────────────────────

/// A host kind's own validation routine over its opaque config table.
/// Per §8.2 the loader delegates kind-config validation to the kind
/// owner; the error returned here is surfaced verbatim as the rule the
/// scenario violated.
pub trait HostKindValidator {
    fn kind_tag(&self) -> &'static str;
    fn validate_config(&self, config: &toml::value::Table) -> Result<(), String>;
    /// Validate that `initial_state` is one this kind recognises. The
    /// loader cannot know the kind's state alphabet; the kind owner can.
    fn validate_initial_state(&self, state: &str) -> Result<(), String>;
}

/// Built-in host-kind registry. The MVP ships the SWIM kind here; new
/// kinds register through [`HostKindRegistry::register`]. The registry
/// is owned by the caller (the engine, when it wires components).
#[derive(Default)]
pub struct HostKindRegistry {
    by_tag: BTreeMap<&'static str, Box<dyn HostKindValidator>>,
}

impl HostKindRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_swim() -> Self {
        let mut reg = Self::default();
        reg.register(Box::new(SwimHostKindValidator));
        reg.register(Box::new(StageHostKindValidator));
        reg
    }

    pub fn register(&mut self, v: Box<dyn HostKindValidator>) {
        self.by_tag.insert(v.kind_tag(), v);
    }

    pub fn get(&self, tag: &str) -> Option<&dyn HostKindValidator> {
        self.by_tag.get(tag).map(|b| b.as_ref())
    }
}

/// SWIM host-kind validator. Mirrors the §8.2 example —
/// `probe_interval < suspicion_timeout` — plus a few sibling rules
/// implied by the production SWIM state machine the host wraps.
pub struct SwimHostKindValidator;

impl HostKindValidator for SwimHostKindValidator {
    fn kind_tag(&self) -> &'static str {
        "swim"
    }

    fn validate_config(&self, config: &toml::value::Table) -> Result<(), String> {
        let probe_interval = require_u64(config, "probe_interval_ns")?;
        let suspicion_timeout = require_u64(config, "suspicion_timeout_ns")?;
        if probe_interval >= suspicion_timeout {
            return Err(format!(
                "probe_interval_ns ({probe_interval}) must be < suspicion_timeout_ns ({suspicion_timeout})"
            ));
        }
        // Optional: indirect-ping fanout must be positive if present.
        if let Some(value) = config.get("indirect_ping_fanout") {
            let fanout = value
                .as_integer()
                .ok_or_else(|| "indirect_ping_fanout must be an integer".to_string())?;
            if fanout < 1 {
                return Err("indirect_ping_fanout must be >= 1".to_string());
            }
        }
        Ok(())
    }

    fn validate_initial_state(&self, state: &str) -> Result<(), String> {
        match state {
            "alive" | "joining" => Ok(()),
            other => Err(format!(
                "unknown SWIM initial_state {other:?}; expected one of \"alive\", \"joining\""
            )),
        }
    }
}

/// `stage` host kind validator (RELAY_SPEC §5, §8.1). Requires a
/// `name` (the name the stage registers at `Registering → Running`)
/// and an `address` (the address the stage registers under), both
/// non-empty strings.
pub struct StageHostKindValidator;

impl HostKindValidator for StageHostKindValidator {
    fn kind_tag(&self) -> &'static str {
        "stage"
    }
    fn validate_config(&self, config: &toml::value::Table) -> Result<(), String> {
        let name = config
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "required key missing: name".to_string())?;
        if name.is_empty() {
            return Err("name must not be empty".to_string());
        }
        let address = config
            .get("address")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "required key missing: address".to_string())?;
        if address.is_empty() {
            return Err("address must not be empty".to_string());
        }
        Ok(())
    }
    fn validate_initial_state(&self, state: &str) -> Result<(), String> {
        if state == "cold" {
            Ok(())
        } else {
            Err(format!(
                "unknown stage initial_state {state:?}; expected \"cold\""
            ))
        }
    }
}

fn require_u64(t: &toml::value::Table, key: &str) -> Result<u64, String> {
    let v = t
        .get(key)
        .ok_or_else(|| format!("required key missing: {key}"))?;
    let n = v
        .as_integer()
        .ok_or_else(|| format!("{key} must be an integer"))?;
    if n < 0 {
        return Err(format!("{key} must be non-negative"));
    }
    Ok(n as u64)
}

// ──────────────────────────────────────────────────────────────────────
// Error type
// ──────────────────────────────────────────────────────────────────────

/// A structured load error. Per §8.2 every rejection names the file,
/// the offending field, and the violated rule on one line each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError {
    pub file: PathBuf,
    pub field: String,
    pub rule: String,
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "file: {}", self.file.display())?;
        writeln!(f, "field: {}", self.field)?;
        write!(f, "rule: {}", self.rule)
    }
}

impl std::error::Error for LoadError {}

fn err(file: &Path, field: impl Into<String>, rule: impl Into<String>) -> LoadError {
    LoadError {
        file: file.to_path_buf(),
        field: field.into(),
        rule: rule.into(),
    }
}

// ──────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────

/// Parse and validate `text` as a scenario file at `path`. `path` is
/// used only for error reporting and `[base].extends` resolution; no
/// filesystem read happens unless the scenario extends a base. The
/// loader performs no writes — §8.4 "Loading is pure".
pub fn load_from_str(
    path: &Path,
    text: &str,
    registry: &HostKindRegistry,
) -> Result<Scenario, LoadError> {
    let raw: RawScenario = toml::from_str(text).map_err(|e| LoadError {
        file: path.to_path_buf(),
        field: "(root)".to_string(),
        rule: format!("not valid TOML: {e}"),
    })?;
    let resolved = resolve_extends(path, raw, &mut BTreeSet::new())?;
    validate(path, resolved, registry)
}

/// Convenience: read `path`, then call [`load_from_str`]. The single
/// non-purity in the loader, isolated here so tests that supply text
/// directly never touch the filesystem.
pub fn load_from_path(path: &Path, registry: &HostKindRegistry) -> Result<Scenario, LoadError> {
    let text = std::fs::read_to_string(path).map_err(|e| LoadError {
        file: path.to_path_buf(),
        field: "(io)".to_string(),
        rule: format!("could not read scenario file: {e}"),
    })?;
    load_from_str(path, &text, registry)
}

/// Render a [`Scenario`] back to TOML. The §8.4 "parse is invertible"
/// property tests use this: `parse(emit(parse(t))) == parse(t)`.
pub fn to_toml(s: &Scenario) -> String {
    toml::to_string_pretty(s).expect("Scenario serialises to TOML by construction")
}

// ──────────────────────────────────────────────────────────────────────
// Raw (on-disk) shape and merging
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Deserialize)]
struct RawScenario {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default)]
    duration_ns: Option<u64>,
    #[serde(default)]
    early_terminate_on_all_assertions_resolved: Option<bool>,
    #[serde(default)]
    default_tick: Option<DefaultTick>,
    #[serde(default)]
    default_link: Option<RawLinkPolicy>,
    #[serde(default)]
    peers: Vec<RawPeer>,
    #[serde(default)]
    relays: Vec<Relay>,
    #[serde(default)]
    links: Vec<RawLink>,
    #[serde(default)]
    mutations: Vec<toml::value::Table>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
    #[serde(default)]
    assertions: Vec<toml::value::Table>,
    #[serde(default)]
    base: Option<Base>,
}

#[derive(Debug, Clone, Deserialize)]
struct Base {
    extends: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct RawLinkPolicy {
    #[serde(default)]
    latency_ns: Option<i64>,
    #[serde(default)]
    jitter_stddev_ns: Option<i64>,
    #[serde(default)]
    loss_prob_ppm: Option<i64>,
    #[serde(default)]
    reorder_prob_ppm: Option<i64>,
    #[serde(default)]
    bandwidth_bps: Option<i64>,
    #[serde(default)]
    cold_dial_penalty_ns: Option<i64>,
    #[serde(default)]
    cache_warm_after_ns: Option<i64>,
    #[serde(default)]
    cache_invalidate_after_idle_ns: Option<i64>,
    /// Catch-all: any field not in §5.2 fails validation with its name.
    #[serde(flatten)]
    extra: toml::value::Table,
}

#[derive(Debug, Clone, Deserialize)]
struct RawPeer {
    id: String,
    kind: String,
    #[serde(default)]
    kind_config: toml::value::Table,
    initial_state: String,
    #[serde(default)]
    tick_period_ns_override: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawLink {
    from: String,
    to: String,
    #[serde(default)]
    via: Option<String>,
    #[serde(flatten)]
    overrides: RawLinkPolicy,
}

// ──────────────────────────────────────────────────────────────────────
// Extends resolution
// ──────────────────────────────────────────────────────────────────────

fn resolve_extends(
    path: &Path,
    raw: RawScenario,
    seen: &mut BTreeSet<PathBuf>,
) -> Result<RawScenario, LoadError> {
    let Some(base) = raw.base.clone() else {
        return Ok(raw);
    };
    let canonical = path
        .canonicalize()
        .unwrap_or_else(|_| path.to_path_buf());
    if !seen.insert(canonical.clone()) {
        return Err(err(
            path,
            "base.extends",
            format!("extends cycle through {}", path.display()),
        ));
    }
    let base_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(&base.extends);
    let base_text = std::fs::read_to_string(&base_path).map_err(|e| {
        err(
            path,
            "base.extends",
            format!("could not read base {}: {e}", base_path.display()),
        )
    })?;
    let parent_raw: RawScenario = toml::from_str(&base_text).map_err(|e| LoadError {
        file: base_path.clone(),
        field: "(root)".to_string(),
        rule: format!("not valid TOML: {e}"),
    })?;
    let parent_resolved = resolve_extends(&base_path, parent_raw, seen)?;
    Ok(merge(parent_resolved, raw))
}

/// "Leaves-override, lists-append" per §8.4. Scalars from the child
/// replace the parent's; lists in the child append onto the parent's.
fn merge(parent: RawScenario, child: RawScenario) -> RawScenario {
    RawScenario {
        name: child.name.or(parent.name),
        seed: child.seed.or(parent.seed),
        duration_ns: child.duration_ns.or(parent.duration_ns),
        early_terminate_on_all_assertions_resolved: child
            .early_terminate_on_all_assertions_resolved
            .or(parent.early_terminate_on_all_assertions_resolved),
        default_tick: child.default_tick.or(parent.default_tick),
        default_link: match (parent.default_link, child.default_link) {
            (None, c) => c,
            (Some(p), None) => Some(p),
            (Some(p), Some(c)) => Some(merge_policy(p, c)),
        },
        peers: append(parent.peers, child.peers),
        relays: append(parent.relays, child.relays),
        links: append(parent.links, child.links),
        mutations: append(parent.mutations, child.mutations),
        snapshots: append(parent.snapshots, child.snapshots),
        assertions: append(parent.assertions, child.assertions),
        base: None,
    }
}

fn append<T>(mut p: Vec<T>, mut c: Vec<T>) -> Vec<T> {
    p.append(&mut c);
    p
}

fn merge_policy(parent: RawLinkPolicy, child: RawLinkPolicy) -> RawLinkPolicy {
    let mut extra = parent.extra;
    for (k, v) in child.extra {
        extra.insert(k, v);
    }
    RawLinkPolicy {
        latency_ns: child.latency_ns.or(parent.latency_ns),
        jitter_stddev_ns: child.jitter_stddev_ns.or(parent.jitter_stddev_ns),
        loss_prob_ppm: child.loss_prob_ppm.or(parent.loss_prob_ppm),
        reorder_prob_ppm: child.reorder_prob_ppm.or(parent.reorder_prob_ppm),
        bandwidth_bps: child.bandwidth_bps.or(parent.bandwidth_bps),
        cold_dial_penalty_ns: child.cold_dial_penalty_ns.or(parent.cold_dial_penalty_ns),
        cache_warm_after_ns: child.cache_warm_after_ns.or(parent.cache_warm_after_ns),
        cache_invalidate_after_idle_ns: child
            .cache_invalidate_after_idle_ns
            .or(parent.cache_invalidate_after_idle_ns),
        extra,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Validation
// ──────────────────────────────────────────────────────────────────────

fn validate(
    path: &Path,
    raw: RawScenario,
    registry: &HostKindRegistry,
) -> Result<Scenario, LoadError> {
    let name = raw.name.ok_or_else(|| err(path, "name", "required"))?;
    if name.is_empty() {
        return Err(err(path, "name", "must not be empty"));
    }
    let seed = raw.seed.ok_or_else(|| err(path, "seed", "required"))?;
    let duration_ns = raw
        .duration_ns
        .ok_or_else(|| err(path, "duration_ns", "required"))?;

    let default_tick = raw
        .default_tick
        .ok_or_else(|| err(path, "default_tick.period_ns", "required"))?;
    if default_tick.period_ns == 0 {
        return Err(err(
            path,
            "default_tick.period_ns",
            "must be > 0",
        ));
    }

    let raw_default_link = raw
        .default_link
        .ok_or_else(|| err(path, "default_link", "required"))?;
    reject_unknown_fields(path, "default_link", &raw_default_link.extra)?;
    let default_link = resolve_policy(path, "default_link", &raw_default_link, None)?;

    // Peers — IDs unique, host-kind valid, kind-config validated.
    let mut peers = Vec::with_capacity(raw.peers.len());
    let mut peer_ids: BTreeSet<String> = BTreeSet::new();
    for (i, p) in raw.peers.iter().enumerate() {
        let field = |s: &str| format!("peers[{i}].{s}");
        if p.id.is_empty() {
            return Err(err(path, field("id"), "must not be empty"));
        }
        if !peer_ids.insert(p.id.clone()) {
            return Err(err(
                path,
                field("id"),
                format!("duplicate peer id {:?}", p.id),
            ));
        }
        let validator = registry.get(p.kind.as_str()).ok_or_else(|| {
            err(
                path,
                field("kind"),
                format!("unknown host kind {:?}", p.kind),
            )
        })?;
        validator
            .validate_config(&p.kind_config)
            .map_err(|e| err(path, field("kind_config"), e))?;
        validator
            .validate_initial_state(&p.initial_state)
            .map_err(|e| err(path, field("initial_state"), e))?;
        if let Some(0) = p.tick_period_ns_override {
            return Err(err(
                path,
                field("tick_period_ns_override"),
                "must be > 0 when present",
            ));
        }
        peers.push(Peer {
            id: p.id.clone(),
            kind: p.kind.clone(),
            kind_config: p.kind_config.clone(),
            initial_state: p.initial_state.clone(),
            tick_period_ns_override: p.tick_period_ns_override,
        });
    }
    if peers.is_empty() {
        return Err(err(path, "peers", "must declare at least one peer"));
    }

    // Relays — IDs unique across (peers ∪ relays). RELAY_SPEC §4.1 /
    // §8.2.
    let mut relay_ids: BTreeSet<String> = BTreeSet::new();
    let mut relays = Vec::with_capacity(raw.relays.len());
    for (i, r) in raw.relays.iter().enumerate() {
        let field = |s: &str| format!("relays[{i}].{s}");
        if r.id.is_empty() {
            return Err(err(path, field("id"), "must not be empty"));
        }
        if peer_ids.contains(&r.id) {
            return Err(err(
                path,
                field("id"),
                format!(
                    "relay id {:?} collides with a declared peer id (peer and relay namespaces are shared)",
                    r.id
                ),
            ));
        }
        if !relay_ids.insert(r.id.clone()) {
            return Err(err(
                path,
                field("id"),
                format!("duplicate relay id {:?}", r.id),
            ));
        }
        if r.ingress_capacity_bps == 0 {
            return Err(err(path, field("ingress_capacity_bps"), "must be > 0"));
        }
        if r.egress_capacity_bps_per_link == 0 {
            return Err(err(
                path,
                field("egress_capacity_bps_per_link"),
                "must be > 0",
            ));
        }
        relays.push(r.clone());
    }

    // Nodes (the union of peers and relays). Edges may touch either.
    let node_ids: BTreeSet<String> = peer_ids.union(&relay_ids).cloned().collect();

    // Links — endpoints must be declared nodes; no duplicate ordered
    // pairs. Optional `via` shorthand synthesises two relay legs.
    // RELAY_SPEC §4.1 / §8.1 / §8.2.
    let mut links = Vec::with_capacity(raw.links.len());
    let mut seen_edges: BTreeSet<(String, String)> = BTreeSet::new();
    // (from_host, to_host) → relay through which the host pair is routed.
    // Only relayed routes from `via` shorthand land here; explicit
    // host→relay or relay→host edges go through the standard path.
    let mut via_routes: BTreeMap<(String, String), String> = BTreeMap::new();
    // Validate first; deferred resolution into HostRoute happens once
    // every link's policy is in hand.
    let mut via_links: Vec<(String, String, String, LinkPolicy)> = Vec::new();
    for (i, l) in raw.links.iter().enumerate() {
        let field = |s: &str| format!("links[{i}].{s}");
        if !node_ids.contains(&l.from) {
            return Err(err(
                path,
                field("from"),
                format!("references undeclared peer or relay {:?}", l.from),
            ));
        }
        if !node_ids.contains(&l.to) {
            return Err(err(
                path,
                field("to"),
                format!("references undeclared peer or relay {:?}", l.to),
            ));
        }
        if l.from == l.to {
            return Err(err(
                path,
                field("to"),
                "self-loop links are not permitted",
            ));
        }
        reject_unknown_fields(path, &format!("links[{i}]"), &l.overrides.extra)?;
        let policy = resolve_policy(
            path,
            &format!("links[{i}]"),
            &l.overrides,
            Some(default_link),
        )?;
        if let Some(via) = &l.via {
            // `via` shorthand. Both endpoints must be hosts; `via`
            // must name a declared relay. The loader synthesizes the
            // inbound and outbound legs with the same policy.
            if relay_ids.contains(&l.from) || relay_ids.contains(&l.to) {
                return Err(err(
                    path,
                    field("via"),
                    format!(
                        "a link with `via` must have host endpoints; {:?}->{:?} touches a relay",
                        l.from, l.to
                    ),
                ));
            }
            if !relay_ids.contains(via) {
                return Err(err(
                    path,
                    field("via"),
                    format!("via must reference a declared relay, got {via:?}"),
                ));
            }
            if via_routes
                .insert((l.from.clone(), l.to.clone()), via.clone())
                .is_some()
            {
                return Err(err(
                    path,
                    field("via"),
                    format!(
                        "duplicate relayed route {:?} -> via {:?} -> {:?}",
                        l.from, via, l.to
                    ),
                ));
            }
            via_links.push((l.from.clone(), via.clone(), l.to.clone(), policy));
            continue;
        }
        let key = (l.from.clone(), l.to.clone());
        if !seen_edges.insert(key) {
            return Err(err(
                path,
                field("from"),
                format!("duplicate edge {:?} -> {:?}", l.from, l.to),
            ));
        }
        links.push(Link {
            from: l.from.clone(),
            to: l.to.clone(),
            policy,
        });
    }
    // Expand via shorthands. Reject conflicts with explicit edges;
    // merge with existing synth edges when policies agree.
    // `explicit_edges` is the set declared verbatim in `[[links]]`
    // (without `via`); `synth_edges` is what the loader has so far
    // synthesized from earlier via shorthands. Overlapping synth
    // edges across multiple via routes through the same relay are
    // allowed iff their policies match — otherwise we cannot pick a
    // single legpolicy for the shared edge.
    let explicit_edges = seen_edges.clone();
    let mut synth_edges: BTreeSet<(String, String)> = BTreeSet::new();
    for (from, via, to, policy) in &via_links {
        let inbound = (from.clone(), via.clone());
        let outbound = (via.clone(), to.clone());
        if explicit_edges.contains(&inbound) {
            return Err(err(
                path,
                format!("links via {via:?}"),
                format!(
                    "via shorthand conflicts with an explicit edge {from:?} -> {via:?}; declare one or the other"
                ),
            ));
        }
        if explicit_edges.contains(&outbound) {
            return Err(err(
                path,
                format!("links via {via:?}"),
                format!(
                    "via shorthand conflicts with an explicit edge {via:?} -> {to:?}; declare one or the other"
                ),
            ));
        }
        if synth_edges.contains(&inbound) {
            // Already added from another via through this relay; the
            // policy must agree.
            let existing = links
                .iter()
                .find(|l| l.from == inbound.0 && l.to == inbound.1)
                .expect("synth_edges set ⇒ links entry");
            if existing.policy != *policy {
                return Err(err(
                    path,
                    format!("links via {via:?}"),
                    format!(
                        "multiple via shorthands through {via:?} from {from:?} disagree on the inbound leg policy"
                    ),
                ));
            }
        } else {
            synth_edges.insert(inbound.clone());
            seen_edges.insert(inbound.clone());
            links.push(Link {
                from: inbound.0.clone(),
                to: inbound.1.clone(),
                policy: *policy,
            });
        }
        if synth_edges.contains(&outbound) {
            let existing = links
                .iter()
                .find(|l| l.from == outbound.0 && l.to == outbound.1)
                .expect("synth_edges set ⇒ links entry");
            if existing.policy != *policy {
                return Err(err(
                    path,
                    format!("links via {via:?}"),
                    format!(
                        "multiple via shorthands through {via:?} to {to:?} disagree on the outbound leg policy"
                    ),
                ));
            }
        } else {
            synth_edges.insert(outbound.clone());
            seen_edges.insert(outbound.clone());
            links.push(Link {
                from: outbound.0.clone(),
                to: outbound.1.clone(),
                policy: *policy,
            });
        }
    }
    // Build routes: every ordered host pair with an explicit direct
    // edge or a `via` shorthand. Reject ambiguity (both kinds for the
    // same pair). Multi-hop relays (which would require an edge from
    // one relay to another) are rejected on edge construction below.
    let mut routes: BTreeMap<(String, String), HostRoute> = BTreeMap::new();
    for (key, via) in &via_routes {
        routes.insert(
            key.clone(),
            HostRoute::Relayed {
                from: key.0.clone(),
                relay: via.clone(),
                to: key.1.clone(),
            },
        );
    }
    for link in &links {
        if !peer_ids.contains(&link.from) || !peer_ids.contains(&link.to) {
            // Edges touching a relay are not by themselves a host
            // route — they are legs of a route. Host-pair routes
            // come from `via` shorthand or from direct host edges.
            continue;
        }
        let key = (link.from.clone(), link.to.clone());
        if routes.contains_key(&key) {
            return Err(err(
                path,
                "links",
                format!(
                    "ambiguous route for {:?} -> {:?}: declared both directly and through a relay via shorthand",
                    link.from, link.to
                ),
            ));
        }
        routes.insert(
            key,
            HostRoute::Direct {
                from: link.from.clone(),
                to: link.to.clone(),
            },
        );
    }
    // Infer relayed routes from explicit host↔relay edges. For any
    // ordered host pair (a, b) without a route yet, look for a relay
    // R such that explicit edges a→R and R→b both exist. Exactly one
    // such R ⇒ relayed route; more than one ⇒ ambiguous (reject).
    // This makes the loader robust against TOML round-trips that
    // emit the synth legs explicitly: the loader recovers the same
    // routing decision the original via shorthand made.
    for a in &peer_ids {
        for b in &peer_ids {
            if a == b {
                continue;
            }
            let key = (a.clone(), b.clone());
            if routes.contains_key(&key) {
                continue;
            }
            let candidates: Vec<&String> = relay_ids
                .iter()
                .filter(|r| {
                    seen_edges.contains(&(a.clone(), (*r).clone()))
                        && seen_edges.contains(&((*r).clone(), b.clone()))
                })
                .collect();
            match candidates.len() {
                0 => {}
                1 => {
                    routes.insert(
                        key,
                        HostRoute::Relayed {
                            from: a.clone(),
                            relay: candidates[0].clone(),
                            to: b.clone(),
                        },
                    );
                }
                _ => {
                    let names: Vec<String> =
                        candidates.iter().map(|s| format!("{s:?}")).collect();
                    return Err(err(
                        path,
                        "links",
                        format!(
                            "ambiguous route for {:?} -> {:?}: multiple relays ({}) carry both legs",
                            a,
                            b,
                            names.join(", ")
                        ),
                    ));
                }
            }
        }
    }
    // Reject multi-hop relay paths (relay-to-relay edges that would
    // allow chaining). MVP rejects any direct relay→relay edge.
    for link in &links {
        if relay_ids.contains(&link.from) && relay_ids.contains(&link.to) {
            return Err(err(
                path,
                "links",
                format!(
                    "multi-hop relay routes are not supported in the MVP: relay {:?} -> relay {:?}",
                    link.from, link.to
                ),
            ));
        }
    }
    let routes_vec: Vec<HostRoute> = routes.into_values().collect();

    // Build a quick map peer_id → kind so worker_exit mutations can
    // validate that the target is `stage`-kind. RELAY_SPEC §6.2 /
    // §8.2.
    let peer_kinds: BTreeMap<String, String> = peers
        .iter()
        .map(|p| (p.id.clone(), p.kind.clone()))
        .collect();
    // Mutations: parse from raw table, validate references, validate at_ns.
    let mut mutations = Vec::with_capacity(raw.mutations.len());
    for (i, m) in raw.mutations.iter().enumerate() {
        let parsed = parse_mutation(
            path,
            i,
            m,
            &peer_ids,
            &seen_edges,
            &relay_ids,
            &peer_kinds,
        )?;
        if parsed.at_ns > duration_ns {
            return Err(err(
                path,
                format!("mutations[{i}].at_ns"),
                format!(
                    "{} exceeds duration_ns {}",
                    parsed.at_ns, duration_ns
                ),
            ));
        }
        mutations.push(parsed);
    }

    // Snapshots.
    let mut snapshots = Vec::with_capacity(raw.snapshots.len());
    for (i, s) in raw.snapshots.iter().enumerate() {
        if s.at_ns > duration_ns {
            return Err(err(
                path,
                format!("snapshots[{i}].at_ns"),
                format!("{} exceeds duration_ns {}", s.at_ns, duration_ns),
            ));
        }
        snapshots.push(*s);
    }

    // Assertions.
    let mut assertions = Vec::with_capacity(raw.assertions.len());
    for (i, a) in raw.assertions.iter().enumerate() {
        let parsed = parse_assertion(path, i, a, &peer_ids, &relay_ids, duration_ns)?;
        assertions.push(parsed);
    }

    Ok(Scenario {
        name,
        seed,
        duration_ns,
        early_terminate_on_all_assertions_resolved: raw
            .early_terminate_on_all_assertions_resolved
            .unwrap_or(false),
        default_tick,
        default_link,
        peers,
        relays,
        links,
        mutations,
        snapshots,
        assertions,
        routes: routes_vec,
    })
}

fn reject_unknown_fields(
    path: &Path,
    parent_field: &str,
    extra: &toml::value::Table,
) -> Result<(), LoadError> {
    if let Some((k, _)) = extra.iter().next() {
        return Err(err(
            path,
            format!("{parent_field}.{k}"),
            format!(
                "unknown field {k:?}; only the §5.2 names are permitted (e.g. latency_ms is not — only latency_ns)"
            ),
        ));
    }
    Ok(())
}

fn resolve_policy(
    path: &Path,
    parent_field: &str,
    overrides: &RawLinkPolicy,
    base: Option<LinkPolicy>,
) -> Result<LinkPolicy, LoadError> {
    macro_rules! pick {
        ($n:ident, $name:literal) => {{
            let override_val = overrides.$n;
            match (override_val, base.map(|b| b.$n)) {
                (Some(v), _) => {
                    let v: i64 = v;
                    if v < 0 {
                        return Err(err(
                            path,
                            format!("{}.{}", parent_field, $name),
                            "must be non-negative",
                        ));
                    }
                    v as u64
                }
                (None, Some(b)) => b,
                (None, None) => {
                    return Err(err(
                        path,
                        format!("{}.{}", parent_field, $name),
                        "required (not supplied here and no default_link to inherit from)",
                    ));
                }
            }
        }};
    }
    macro_rules! pick_u32 {
        ($n:ident, $name:literal) => {{
            let override_val = overrides.$n;
            match (override_val, base.map(|b| b.$n)) {
                (Some(v), _) => {
                    let v: i64 = v;
                    if v < 0 {
                        return Err(err(
                            path,
                            format!("{}.{}", parent_field, $name),
                            "must be non-negative",
                        ));
                    }
                    if v > 1_000_000 {
                        return Err(err(
                            path,
                            format!("{}.{}", parent_field, $name),
                            "must be <= 1_000_000 (parts-per-million)",
                        ));
                    }
                    v as u32
                }
                (None, Some(b)) => b,
                (None, None) => {
                    return Err(err(
                        path,
                        format!("{}.{}", parent_field, $name),
                        "required (not supplied here and no default_link to inherit from)",
                    ));
                }
            }
        }};
    }

    let latency_ns = pick!(latency_ns, "latency_ns");
    let jitter_stddev_ns = pick!(jitter_stddev_ns, "jitter_stddev_ns");
    let loss_prob_ppm = pick_u32!(loss_prob_ppm, "loss_prob_ppm");
    let reorder_prob_ppm = pick_u32!(reorder_prob_ppm, "reorder_prob_ppm");
    let bandwidth_bps = pick!(bandwidth_bps, "bandwidth_bps");
    if bandwidth_bps == 0 {
        return Err(err(
            path,
            format!("{parent_field}.bandwidth_bps"),
            "must be > 0",
        ));
    }
    let cold_dial_penalty_ns = pick!(cold_dial_penalty_ns, "cold_dial_penalty_ns");
    let cache_warm_after_ns = pick!(cache_warm_after_ns, "cache_warm_after_ns");
    let cache_invalidate_after_idle_ns =
        pick!(cache_invalidate_after_idle_ns, "cache_invalidate_after_idle_ns");

    Ok(LinkPolicy {
        latency_ns,
        jitter_stddev_ns,
        loss_prob_ppm,
        reorder_prob_ppm,
        bandwidth_bps,
        cold_dial_penalty_ns,
        cache_warm_after_ns,
        cache_invalidate_after_idle_ns,
    })
}

fn parse_mutation(
    path: &Path,
    i: usize,
    table: &toml::value::Table,
    peers: &BTreeSet<String>,
    edges: &BTreeSet<(String, String)>,
    relays: &BTreeSet<String>,
    peer_kinds: &BTreeMap<String, String>,
) -> Result<Mutation, LoadError> {
    let field = |s: &str| format!("mutations[{i}].{s}");
    let at_ns = require_u64_field(path, &field("at_ns"), table.get("at_ns"))?;
    let kind_str = table
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| err(path, field("kind"), "required string"))?;
    let kind = match kind_str {
        "partition" => {
            let peers_a = string_list(path, &field("peers_a"), table.get("peers_a"))?;
            let peers_b = string_list(path, &field("peers_b"), table.get("peers_b"))?;
            for (j, p) in peers_a.iter().enumerate() {
                if !peers.contains(p) {
                    return Err(err(
                        path,
                        format!("mutations[{i}].peers_a[{j}]"),
                        format!("references undeclared peer {p:?}"),
                    ));
                }
            }
            for (j, p) in peers_b.iter().enumerate() {
                if !peers.contains(p) {
                    return Err(err(
                        path,
                        format!("mutations[{i}].peers_b[{j}]"),
                        format!("references undeclared peer {p:?}"),
                    ));
                }
            }
            MutationKind::Partition { peers_a, peers_b }
        }
        "heal" => MutationKind::Heal,
        "latency_spike" => MutationKind::LatencySpike {
            links: link_refs(path, i, table, peers, edges)?,
            factor_x100: require_u32_field(path, &field("factor_x100"), table.get("factor_x100"))?,
            duration_ns: require_u64_field(path, &field("duration_ns"), table.get("duration_ns"))?,
        },
        "loss_burst" => MutationKind::LossBurst {
            links: link_refs(path, i, table, peers, edges)?,
            prob_ppm: ppm_field(path, &field("prob_ppm"), table.get("prob_ppm"))?,
            duration_ns: require_u64_field(path, &field("duration_ns"), table.get("duration_ns"))?,
        },
        "relay_buffer" => MutationKind::RelayBuffer {
            links: link_refs(path, i, table, peers, edges)?,
            floor_ns: require_u64_field(path, &field("floor_ns"), table.get("floor_ns"))?,
            duration_ns: require_u64_field(path, &field("duration_ns"), table.get("duration_ns"))?,
        },
        "peer_kill" => MutationKind::PeerKill {
            peer: peer_field(path, &field("peer"), table.get("peer"), peers)?,
        },
        "peer_resurrect" => MutationKind::PeerResurrect {
            peer: peer_field(path, &field("peer"), table.get("peer"), peers)?,
            preserve_state: table
                .get("preserve_state")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        },
        "worker_exit" => {
            let peer = peer_field(path, &field("peer"), table.get("peer"), peers)?;
            // RELAY_SPEC §8.2 — worker_exit must target a stage peer.
            match peer_kinds.get(&peer).map(|s| s.as_str()) {
                Some("stage") => {}
                Some(other) => {
                    return Err(err(
                        path,
                        field("peer"),
                        format!(
                            "worker_exit targets peer {peer:?} of kind {other:?}; only \"stage\" peers accept it"
                        ),
                    ));
                }
                None => {
                    return Err(err(
                        path,
                        field("peer"),
                        format!("references undeclared peer {peer:?}"),
                    ));
                }
            }
            let reason = table
                .get("reason")
                .and_then(|v| v.as_str())
                .ok_or_else(|| err(path, field("reason"), "required string"))?
                .to_string();
            let status_code = match table.get("status_code") {
                None => None,
                Some(v) => Some(
                    v.as_integer()
                        .ok_or_else(|| {
                            err(path, field("status_code"), "must be an integer")
                        })?
                        .try_into()
                        .map_err(|_| {
                            err(path, field("status_code"), "must fit in i32")
                        })?,
                ),
            };
            let signal = match table.get("signal") {
                None => None,
                Some(v) => Some(
                    v.as_integer()
                        .ok_or_else(|| err(path, field("signal"), "must be an integer"))?
                        .try_into()
                        .map_err(|_| err(path, field("signal"), "must fit in i32"))?,
                ),
            };
            MutationKind::WorkerExit {
                peer,
                reason,
                status_code,
                signal,
            }
        }
        "relay_kill" => MutationKind::RelayKill {
            relay: relay_field(path, &field("relay"), table.get("relay"), relays)?,
        },
        "relay_boot" => MutationKind::RelayBoot {
            relay: relay_field(path, &field("relay"), table.get("relay"), relays)?,
        },
        "relay_capacity_change" => {
            let relay = relay_field(path, &field("relay"), table.get("relay"), relays)?;
            let opt_u64 = |key: &str| -> Result<Option<u64>, LoadError> {
                match table.get(key) {
                    None => Ok(None),
                    Some(v) => {
                        let n = v.as_integer().ok_or_else(|| {
                            err(path, format!("mutations[{i}].{key}"), "must be an integer")
                        })?;
                        if n < 0 {
                            return Err(err(
                                path,
                                format!("mutations[{i}].{key}"),
                                "must be non-negative",
                            ));
                        }
                        if key != "queue_depth_bytes" && n == 0 {
                            return Err(err(
                                path,
                                format!("mutations[{i}].{key}"),
                                "must be > 0",
                            ));
                        }
                        Ok(Some(n as u64))
                    }
                }
            };
            let ingress = opt_u64("ingress_capacity_bps")?;
            let egress = opt_u64("egress_capacity_bps_per_link")?;
            let depth = opt_u64("queue_depth_bytes")?;
            if ingress.is_none() && egress.is_none() && depth.is_none() {
                return Err(err(
                    path,
                    field("kind"),
                    "relay_capacity_change must change at least one of ingress_capacity_bps / egress_capacity_bps_per_link / queue_depth_bytes",
                ));
            }
            MutationKind::RelayCapacityChange {
                relay,
                ingress_capacity_bps: ingress,
                egress_capacity_bps_per_link: egress,
                queue_depth_bytes: depth,
            }
        }
        "relay_peer_conn_down" => {
            let relay = relay_field(path, &field("relay"), table.get("relay"), relays)?;
            let from = peer_field(path, &field("from"), table.get("from"), peers)?;
            let to = peer_field(path, &field("to"), table.get("to"), peers)?;
            let duration_ns = match table.get("duration_ns") {
                None => 0u64,
                Some(v) => v
                    .as_integer()
                    .ok_or_else(|| err(path, field("duration_ns"), "must be an integer"))?
                    .try_into()
                    .map_err(|_| err(path, field("duration_ns"), "must be non-negative"))?,
            };
            MutationKind::RelayPeerConnDown {
                relay,
                from,
                to,
                duration_ns,
            }
        }
        other => {
            return Err(err(
                path,
                field("kind"),
                format!("unknown mutation kind {other:?}"),
            ));
        }
    };
    Ok(Mutation { at_ns, kind })
}

fn link_refs(
    path: &Path,
    i: usize,
    table: &toml::value::Table,
    peers: &BTreeSet<String>,
    edges: &BTreeSet<(String, String)>,
) -> Result<Vec<LinkRef>, LoadError> {
    let arr = table
        .get("links")
        .and_then(|v| v.as_array())
        .ok_or_else(|| err(path, format!("mutations[{i}].links"), "required array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for (j, v) in arr.iter().enumerate() {
        let t = v.as_table().ok_or_else(|| {
            err(
                path,
                format!("mutations[{i}].links[{j}]"),
                "must be a table with from/to",
            )
        })?;
        let from = t
            .get("from")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                err(
                    path,
                    format!("mutations[{i}].links[{j}].from"),
                    "required string",
                )
            })?
            .to_string();
        let to = t
            .get("to")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                err(
                    path,
                    format!("mutations[{i}].links[{j}].to"),
                    "required string",
                )
            })?
            .to_string();
        if !peers.contains(&from) {
            return Err(err(
                path,
                format!("mutations[{i}].links[{j}].from"),
                format!("references undeclared peer {from:?}"),
            ));
        }
        if !peers.contains(&to) {
            return Err(err(
                path,
                format!("mutations[{i}].links[{j}].to"),
                format!("references undeclared peer {to:?}"),
            ));
        }
        if !edges.contains(&(from.clone(), to.clone())) {
            return Err(err(
                path,
                format!("mutations[{i}].links[{j}]"),
                format!("references undeclared edge {from:?} -> {to:?}"),
            ));
        }
        out.push(LinkRef { from, to });
    }
    Ok(out)
}

fn peer_field(
    path: &Path,
    field: &str,
    v: Option<&toml::Value>,
    peers: &BTreeSet<String>,
) -> Result<String, LoadError> {
    let s = v
        .and_then(|x| x.as_str())
        .ok_or_else(|| err(path, field, "required string"))?
        .to_string();
    if !peers.contains(&s) {
        return Err(err(path, field, format!("references undeclared peer {s:?}")));
    }
    Ok(s)
}

fn relay_field(
    path: &Path,
    field: &str,
    v: Option<&toml::Value>,
    relays: &BTreeSet<String>,
) -> Result<String, LoadError> {
    let s = v
        .and_then(|x| x.as_str())
        .ok_or_else(|| err(path, field, "required string"))?
        .to_string();
    if !relays.contains(&s) {
        return Err(err(path, field, format!("references undeclared relay {s:?}")));
    }
    Ok(s)
}

fn require_u64_field(path: &Path, field: &str, v: Option<&toml::Value>) -> Result<u64, LoadError> {
    let n = v
        .and_then(|x| x.as_integer())
        .ok_or_else(|| err(path, field, "required non-negative integer"))?;
    if n < 0 {
        return Err(err(path, field, "must be non-negative"));
    }
    Ok(n as u64)
}

fn require_u32_field(path: &Path, field: &str, v: Option<&toml::Value>) -> Result<u32, LoadError> {
    let n = require_u64_field(path, field, v)?;
    if n > u32::MAX as u64 {
        return Err(err(path, field, "must fit in u32"));
    }
    Ok(n as u32)
}

fn ppm_field(path: &Path, field: &str, v: Option<&toml::Value>) -> Result<u32, LoadError> {
    let n = require_u32_field(path, field, v)?;
    if n > 1_000_000 {
        return Err(err(path, field, "must be <= 1_000_000 (parts-per-million)"));
    }
    Ok(n)
}

fn string_list(path: &Path, field: &str, v: Option<&toml::Value>) -> Result<Vec<String>, LoadError> {
    let arr = v
        .and_then(|x| x.as_array())
        .ok_or_else(|| err(path, field, "required array of strings"))?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let s = item
            .as_str()
            .ok_or_else(|| err(path, format!("{field}[{i}]"), "must be a string"))?
            .to_string();
        out.push(s);
    }
    Ok(out)
}

fn parse_assertion(
    path: &Path,
    i: usize,
    table: &toml::value::Table,
    peers: &BTreeSet<String>,
    relays: &BTreeSet<String>,
    duration_ns: u64,
) -> Result<Assertion, LoadError> {
    let field = |s: &str| format!("assertions[{i}].{s}");
    let kind_str = table
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| err(path, field("kind"), "required string"))?;

    let check_t = |t: u64, name: &str| -> Result<(), LoadError> {
        if t > duration_ns {
            Err(err(
                path,
                format!("assertions[{i}].{name}"),
                format!("{t} exceeds duration_ns {duration_ns}"),
            ))
        } else {
            Ok(())
        }
    };

    let check_peer_list = |list: &Vec<String>, fname: &str| -> Result<(), LoadError> {
        for (j, p) in list.iter().enumerate() {
            if !peers.contains(p) {
                return Err(err(
                    path,
                    format!("assertions[{i}].{fname}[{j}]"),
                    format!("references undeclared peer {p:?}"),
                ));
            }
        }
        Ok(())
    };

    let kind = match kind_str {
        "all_alive_at" => {
            let at_ns = require_u64_field(path, &field("at_ns"), table.get("at_ns"))?;
            check_t(at_ns, "at_ns")?;
            let peers_list = string_list(path, &field("peers"), table.get("peers"))?;
            check_peer_list(&peers_list, "peers")?;
            AssertionKind::AllAliveAt { at_ns, peers: peers_list }
        }
        "all_alive_throughout" => {
            let window_start_ns = require_u64_field(
                path,
                &field("window_start_ns"),
                table.get("window_start_ns"),
            )?;
            let window_end_ns =
                require_u64_field(path, &field("window_end_ns"), table.get("window_end_ns"))?;
            check_t(window_end_ns, "window_end_ns")?;
            if window_start_ns > window_end_ns {
                return Err(err(
                    path,
                    field("window_start_ns"),
                    "window_start_ns must be <= window_end_ns",
                ));
            }
            let peers_list = string_list(path, &field("peers"), table.get("peers"))?;
            check_peer_list(&peers_list, "peers")?;
            AssertionKind::AllAliveThroughout {
                window_start_ns,
                window_end_ns,
                peers: peers_list,
            }
        }
        "convergence_after" => {
            let after_ns =
                require_u64_field(path, &field("after_ns"), table.get("after_ns"))?;
            let within_ns =
                require_u64_field(path, &field("within_ns"), table.get("within_ns"))?;
            check_t(after_ns.saturating_add(within_ns), "after_ns + within_ns")?;
            let peers_list = string_list(path, &field("peers"), table.get("peers"))?;
            check_peer_list(&peers_list, "peers")?;
            AssertionKind::ConvergenceAfter {
                after_ns,
                within_ns,
                peers: peers_list,
            }
        }
        "no_flap_while_probes_ok" => {
            let peer = peer_field(path, &field("peer"), table.get("peer"), peers)?;
            let window_start_ns = require_u64_field(
                path,
                &field("window_start_ns"),
                table.get("window_start_ns"),
            )?;
            let window_end_ns =
                require_u64_field(path, &field("window_end_ns"), table.get("window_end_ns"))?;
            check_t(window_end_ns, "window_end_ns")?;
            if window_start_ns > window_end_ns {
                return Err(err(
                    path,
                    field("window_start_ns"),
                    "window_start_ns must be <= window_end_ns",
                ));
            }
            AssertionKind::NoFlapWhileProbesOk {
                peer,
                window_start_ns,
                window_end_ns,
            }
        }
        "no_dead_when_probes_ok" => {
            let peer = peer_field(path, &field("peer"), table.get("peer"), peers)?;
            let window_start_ns = require_u64_field(
                path,
                &field("window_start_ns"),
                table.get("window_start_ns"),
            )?;
            let window_end_ns =
                require_u64_field(path, &field("window_end_ns"), table.get("window_end_ns"))?;
            check_t(window_end_ns, "window_end_ns")?;
            if window_start_ns > window_end_ns {
                return Err(err(
                    path,
                    field("window_start_ns"),
                    "window_start_ns must be <= window_end_ns",
                ));
            }
            AssertionKind::NoDeadWhenProbesOk {
                peer,
                window_start_ns,
                window_end_ns,
            }
        }
        "self_incarnation_bounded" => AssertionKind::SelfIncarnationBounded {
            peer: peer_field(path, &field("peer"), table.get("peer"), peers)?,
            max_value: require_u64_field(path, &field("max_value"), table.get("max_value"))?,
        },
        "message_size_bounded" => AssertionKind::MessageSizeBounded {
            message_kind: table
                .get("message_kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| err(path, field("message_kind"), "required string"))?
                .to_string(),
            max_bytes: require_u64_field(path, &field("max_bytes"), table.get("max_bytes"))?,
        },
        "dead_peer_resurrects_within" => {
            let peer = peer_field(path, &field("peer"), table.get("peer"), peers)?;
            let after_ns =
                require_u64_field(path, &field("after_ns"), table.get("after_ns"))?;
            let within_ns =
                require_u64_field(path, &field("within_ns"), table.get("within_ns"))?;
            check_t(after_ns.saturating_add(within_ns), "after_ns + within_ns")?;
            AssertionKind::DeadPeerResurrectsWithin {
                peer,
                after_ns,
                within_ns,
            }
        }
        "event_count" => AssertionKind::EventCount {
            event_kind: table
                .get("event_kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| err(path, field("event_kind"), "required string"))?
                .to_string(),
            max: require_u64_field(path, &field("max"), table.get("max"))?,
        },
        "event_rate" => AssertionKind::EventRate {
            event_kind: table
                .get("event_kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| err(path, field("event_kind"), "required string"))?
                .to_string(),
            window_ns: require_u64_field(path, &field("window_ns"), table.get("window_ns"))?,
            max_per_window: require_u64_field(
                path,
                &field("max_per_window"),
                table.get("max_per_window"),
            )?,
        },
        "relay_queue_depth_bounded" => {
            let relay = relay_field(path, &field("relay"), table.get("relay"), relays)?;
            let max_bytes = require_u64_field(path, &field("max_bytes"), table.get("max_bytes"))?;
            let window_start_ns = match table.get("window_start_ns") {
                None => None,
                Some(v) => Some(require_u64_field(path, &field("window_start_ns"), Some(v))?),
            };
            let window_end_ns = match table.get("window_end_ns") {
                None => None,
                Some(v) => Some(require_u64_field(path, &field("window_end_ns"), Some(v))?),
            };
            if let (Some(s), Some(e)) = (window_start_ns, window_end_ns) {
                if s > e {
                    return Err(err(
                        path,
                        field("window_start_ns"),
                        "window_start_ns must be <= window_end_ns",
                    ));
                }
            }
            if let Some(e) = window_end_ns {
                check_t(e, "window_end_ns")?;
            }
            AssertionKind::RelayQueueDepthBounded {
                relay,
                max_bytes,
                window_start_ns,
                window_end_ns,
            }
        }
        "worker_alive_throughout" => {
            let peer = peer_field(path, &field("peer"), table.get("peer"), peers)?;
            let window_start_ns = require_u64_field(
                path,
                &field("window_start_ns"),
                table.get("window_start_ns"),
            )?;
            let window_end_ns = require_u64_field(
                path,
                &field("window_end_ns"),
                table.get("window_end_ns"),
            )?;
            check_t(window_end_ns, "window_end_ns")?;
            if window_start_ns > window_end_ns {
                return Err(err(
                    path,
                    field("window_start_ns"),
                    "window_start_ns must be <= window_end_ns",
                ));
            }
            AssertionKind::WorkerAliveThroughout {
                peer,
                window_start_ns,
                window_end_ns,
            }
        }
        "name_resolves_within" => {
            let name = table
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| err(path, field("name"), "required string"))?
                .to_string();
            if name.is_empty() {
                return Err(err(path, field("name"), "must not be empty"));
            }
            let observers = string_list(path, &field("observers"), table.get("observers"))?;
            check_peer_list(&observers, "observers")?;
            let within_ns = require_u64_field(path, &field("within_ns"), table.get("within_ns"))?;
            let from_ns = require_u64_field(path, &field("from_ns"), table.get("from_ns"))?;
            check_t(from_ns.saturating_add(within_ns), "from_ns + within_ns")?;
            AssertionKind::NameResolvesWithin {
                name,
                observers,
                within_ns,
                from_ns,
            }
        }
        other => {
            return Err(err(
                path,
                field("kind"),
                format!("unknown assertion kind {other:?}"),
            ));
        }
    };

    Ok(Assertion { kind })
}
