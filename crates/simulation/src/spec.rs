//! Topology-spec parsing (SPEC §4.1, §5.3).
//!
//! Parses the TOML scenario into a normalised in-memory representation
//! the engine drives. The parsed form is intentionally small: enough
//! to walk hosts and mutations through the event loop and emit a
//! reproducible bundle. Host names and the run id are validated as
//! safe path components — see `validate_path_component`.

use serde::Deserialize;

use crate::SimError;

#[derive(Debug, Clone)]
pub struct ParsedSpec {
    pub source_text: String,
    pub run_id: String,
    pub duration_ms: u64,
    pub host_defaults: HostDefaults,
    pub hosts: Vec<Host>,
    pub links: Vec<Link>,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, Default)]
pub struct HostDefaults {
    pub mtu: Option<u32>,
    pub nat: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Host {
    pub name: String,
    pub role: String,
    pub stage_index: Option<u32>,
    pub start_at_ms: u64,
    pub restart_at_ms: Vec<u64>,
    pub stop_at_ms: Option<u64>,
    pub crash_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Link {
    pub a: String,
    pub b: String,
    pub bandwidth_bps: u64,
    pub one_way_delay_ms: u32,
    pub jitter_ms: u32,
    pub loss_ppm: u32,
}

#[derive(Debug, Clone)]
pub enum MutationKind {
    Partition { edges: Vec<(String, String)> },
    Heal { edges: Vec<(String, String)> },
    Restart { node: String },
    ClockJump { node: String, delta_ms: i64 },
    LinkChange,
}

#[derive(Debug, Clone)]
pub struct Mutation {
    pub at_ms: u64,
    pub spec_index: usize,
    pub kind: MutationKind,
}

#[derive(Debug, Deserialize)]
struct Raw {
    run_id: String,
    #[serde(default)]
    seed: Option<u64>,
    duration_ms: u64,
    #[serde(default)]
    host_defaults: RawHostDefaults,
    #[serde(default)]
    hosts: Vec<RawHost>,
    #[serde(default)]
    links: Vec<RawLink>,
    #[serde(default)]
    mutations: Vec<RawMutation>,
}

#[derive(Debug, Default, Deserialize)]
struct RawHostDefaults {
    #[serde(default)]
    mtu: Option<u32>,
    #[serde(default)]
    nat: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawHost {
    name: String,
    role: String,
    #[serde(default)]
    stage_index: Option<u32>,
    start_at_ms: u64,
    #[serde(default)]
    restart_at_ms: Vec<u64>,
    #[serde(default)]
    stop_at_ms: Option<u64>,
    #[serde(default)]
    crash_at_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawLink {
    a: String,
    b: String,
    bandwidth_bps: u64,
    one_way_delay_ms: u32,
    #[serde(default)]
    jitter_ms: u32,
    #[serde(default)]
    loss: f64,
}

#[derive(Debug, Deserialize)]
struct RawMutation {
    kind: String,
    at_ms: u64,
    #[serde(default)]
    edges: Vec<(String, String)>,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    delta_ms: Option<i64>,
}

/// Reject identifiers used as directory components: empty, `.`/`..`,
/// path separators, NUL, or Windows drive prefixes. Used for `run_id`
/// (forms the bundle root) and host names (form per-host subdirs);
/// without this guard a hostile spec could write outside the bundle.
pub fn validate_path_component(name: &str, what: &str) -> Result<(), SimError> {
    if name.is_empty() {
        return Err(SimError::SpecParse(format!("{what} must not be empty")));
    }
    if name == "." || name == ".." {
        return Err(SimError::SpecParse(format!(
            "{what} {name:?} is a reserved path component"
        )));
    }
    if name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name.contains('\n')
        || name.contains('\r')
    {
        return Err(SimError::SpecParse(format!(
            "{what} {name:?} must not contain path separators, control characters, or NUL"
        )));
    }
    if name.len() >= 2 && name.as_bytes()[1] == b':' {
        return Err(SimError::SpecParse(format!(
            "{what} {name:?} looks like an absolute drive path"
        )));
    }
    Ok(())
}

pub fn parse(text: &str) -> Result<ParsedSpec, SimError> {
    let raw: Raw = toml::from_str(text).map_err(|e| SimError::SpecParse(e.to_string()))?;
    let _ = raw.seed;

    validate_path_component(&raw.run_id, "run_id")?;

    let mut hosts: Vec<Host> = Vec::with_capacity(raw.hosts.len());
    let mut seen_names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for h in raw.hosts {
        validate_path_component(&h.name, "host name")?;
        if !seen_names.insert(h.name.clone()) {
            return Err(SimError::SpecParse(format!(
                "duplicate host name {:?}",
                h.name
            )));
        }
        hosts.push(Host {
            name: h.name,
            role: h.role,
            stage_index: h.stage_index,
            start_at_ms: h.start_at_ms,
            restart_at_ms: h.restart_at_ms,
            stop_at_ms: h.stop_at_ms,
            crash_at_ms: h.crash_at_ms,
        });
    }

    let links: Vec<Link> = raw
        .links
        .into_iter()
        .map(|l| {
            // loss in [0, 1] → ppm in [0, 1_000_000]. Convert through a
            // fixed-point step so debug/release runs see the same u32
            // (serde-json's float printer is deterministic, but we keep
            // loss in integer ppm inside the engine to keep every
            // engine-level number out of the float domain).
            let clamped = if l.loss.is_nan() {
                0.0
            } else if l.loss < 0.0 {
                0.0
            } else if l.loss > 1.0 {
                1.0
            } else {
                l.loss
            };
            let loss_ppm = (clamped * 1_000_000.0 + 0.5) as u32;
            Link {
                a: l.a,
                b: l.b,
                bandwidth_bps: l.bandwidth_bps,
                one_way_delay_ms: l.one_way_delay_ms,
                jitter_ms: l.jitter_ms,
                loss_ppm,
            }
        })
        .collect();

    let mut mutations = Vec::with_capacity(raw.mutations.len());
    for (idx, m) in raw.mutations.into_iter().enumerate() {
        let kind = match m.kind.as_str() {
            "partition" => MutationKind::Partition { edges: m.edges },
            "heal" => MutationKind::Heal { edges: m.edges },
            "restart" => MutationKind::Restart {
                node: m
                    .node
                    .ok_or_else(|| SimError::SpecParse("restart mutation missing node".into()))?,
            },
            "clock_jump" => MutationKind::ClockJump {
                node: m
                    .node
                    .ok_or_else(|| SimError::SpecParse("clock_jump missing node".into()))?,
                delta_ms: m.delta_ms.unwrap_or(0),
            },
            "link_change" => MutationKind::LinkChange,
            other => {
                return Err(SimError::SpecParse(format!(
                    "unknown mutation kind: {other}"
                )));
            }
        };
        mutations.push(Mutation {
            at_ms: m.at_ms,
            spec_index: idx,
            kind,
        });
    }

    Ok(ParsedSpec {
        source_text: text.to_string(),
        run_id: raw.run_id,
        duration_ms: raw.duration_ms,
        host_defaults: HostDefaults {
            mtu: raw.host_defaults.mtu,
            nat: raw.host_defaults.nat,
        },
        hosts,
        links,
        mutations,
    })
}
