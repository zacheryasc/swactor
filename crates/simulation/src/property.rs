//! Library-property runner (SIM_SPEC §10.3).
//!
//! A property is a parameterised assertion kind evaluated across a
//! generated distribution of scenarios. The runner draws scenario
//! parameters from a `PropertySpace`, builds and runs each scenario
//! through the same engine + bundle + evaluator pipeline a single-
//! scenario run uses, and records `(seed, scenario, verdicts)` per
//! sample. The MVP target is the gossip-flap detector — the
//! framework is host-kind-agnostic so future properties (e.g.,
//! convergence latency) plug in the same way.
//!
//! Determinism (§10.5 "Property failures replay exactly"): the
//! root seed plus the sample index deterministically derives a
//! per-sample scenario seed via the §7.2 substream tree, and the
//! scenario it generates is a pure function of that seed and the
//! `PropertySpace`. Calling `replay_property` with the same root
//! seed and sample index produces the same scenario and the same
//! verdicts that `run_property` recorded.

use serde::Serialize;

use crate::bundle::VecWriter;
use crate::engine::Engine;
use crate::evaluator::{Verdict, evaluate};
use crate::host::HostFactory;
use crate::network::Network;
use crate::rng::{SubstreamKey, SubstreamRng};
use crate::scenario::{
    Assertion, AssertionKind, DefaultTick, HostKindRegistry, Link, LinkPolicy, Peer,
    Scenario,
};

// ──────────────────────────────────────────────────────────────────────
// Property space
// ──────────────────────────────────────────────────────────────────────

/// The parameter space a property samples from. Inclusive ranges; the
/// runner picks integer values uniformly. The scenario the runner
/// builds is always an all-to-all mesh of the chosen peer count with
/// a shared link policy drawn from the latency / jitter / loss
/// ranges.
#[derive(Debug, Clone, Serialize)]
pub struct PropertySpace {
    pub peer_count_min: usize,
    pub peer_count_max: usize,
    pub latency_ns_min: u64,
    pub latency_ns_max: u64,
    pub jitter_ns_min: u64,
    pub jitter_ns_max: u64,
    pub loss_ppm_min: u32,
    pub loss_ppm_max: u32,
    pub duration_ns: u64,
    pub default_tick_period_ns: u64,
    /// Host kind to install on every generated peer. Must have a
    /// registered factory.
    pub host_kind: String,
    /// TOML table the runner attaches to every generated peer's
    /// `kind_config`. Static across samples.
    pub kind_config: toml::value::Table,
    /// `peer.initial_state` the runner declares.
    pub initial_state: String,
    /// The §10 assertion kinds the runner attaches to each generated
    /// scenario. The MVP gossip-flap property is one
    /// `no_flap_while_probes_ok` per peer over the run window.
    pub assertion_templates: Vec<AssertionTemplate>,
}

/// A per-sample assertion template. Concrete `peers` / `peer` fields
/// are bound at scenario-generation time to the peers the sample
/// drew.
#[derive(Debug, Clone, Serialize)]
pub enum AssertionTemplate {
    NoFlapWhileProbesOkForEachPeer {
        window_start_ns: u64,
        window_end_ns: u64,
    },
    NoDeadWhenProbesOkForEachPeer {
        window_start_ns: u64,
        window_end_ns: u64,
    },
    AllAliveAt {
        at_ns: u64,
    },
    EventCount {
        event_kind: String,
        max: u64,
    },
}

// ──────────────────────────────────────────────────────────────────────
// Result
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct PropertyResult {
    /// Sample index inside the run.
    pub sample_index: u32,
    /// Per-sample scenario seed derived from the root seed.
    pub seed: u64,
    /// The fully realised scenario for this sample. Useful for
    /// debugging — a recorded `Fail` carries everything you need to
    /// replay it.
    pub scenario: Scenario,
    /// One verdict per assertion the scenario declared, in
    /// declaration order.
    pub verdicts: Vec<Verdict>,
}

impl PropertyResult {
    /// Whether any verdict in this sample is `Fail`. A property "fails"
    /// when at least one of its samples produces a Fail; callers
    /// typically grep `results.iter().any(PropertyResult::failed)`.
    pub fn failed(&self) -> bool {
        self.verdicts
            .iter()
            .any(|v| matches!(v.outcome, crate::evaluator::Outcome::Fail))
    }
}

// ──────────────────────────────────────────────────────────────────────
// Runner
// ──────────────────────────────────────────────────────────────────────

/// Run `sample_count` scenarios drawn from the property space. The
/// `make_factory` closure is invoked once per sample so each engine
/// run gets a fresh `Box<dyn HostFactory>` (factories carry no per-
/// run state today, but the seam keeps the contract clean if they
/// ever do).
pub fn run_property<F>(
    space: &PropertySpace,
    root_seed: u64,
    sample_count: u32,
    registry: &HostKindRegistry,
    mut make_factory: F,
) -> Vec<PropertyResult>
where
    F: FnMut() -> Box<dyn HostFactory>,
{
    (0..sample_count)
        .map(|i| run_single_sample(space, root_seed, i, registry, make_factory()))
        .collect()
}

/// Replay a single sample. Equivalent to picking the `sample_index`-th
/// element from `run_property(..., sample_count >= sample_index + 1,
/// ...)` but skips the runs you don't need.
pub fn replay_property(
    space: &PropertySpace,
    root_seed: u64,
    sample_index: u32,
    registry: &HostKindRegistry,
    factory: Box<dyn HostFactory>,
) -> PropertyResult {
    run_single_sample(space, root_seed, sample_index, registry, factory)
}

fn run_single_sample(
    space: &PropertySpace,
    root_seed: u64,
    sample_index: u32,
    registry: &HostKindRegistry,
    factory: Box<dyn HostFactory>,
) -> PropertyResult {
    let seed = derive_sample_seed(root_seed, sample_index);
    let scenario = generate_scenario(space, seed, registry)
        .expect("generated scenario must validate against the supplied registry");
    // Run engine and collect records.
    let writer = VecWriter::default();
    let network = Network::new(&scenario);
    let mut engine = Engine::new(&scenario, network, writer);
    engine.register_factory(factory);
    engine.auto_install_hosts();
    engine.set_pop_budget(200_000);
    let _ = engine.run();
    let records = engine.into_writer().records;
    let (events, snapshots) = parse_records_to_evaluator_inputs(&records);
    let verdicts = evaluate(&scenario, &events, &snapshots);
    PropertyResult {
        sample_index,
        seed,
        scenario,
        verdicts,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Scenario generation
// ──────────────────────────────────────────────────────────────────────

fn derive_sample_seed(root_seed: u64, sample_index: u32) -> u64 {
    // Reuse the §7.2 substream tree so per-sample seeds are stable
    // across architectures and across simulator versions.
    let mut rng = SubstreamRng::derive(
        root_seed,
        &SubstreamKey::Mutation {
            index: sample_index as u64,
        },
    );
    // Mask to i64::MAX so the seed survives the TOML round-trip rule.
    rng.next_u64() & (i64::MAX as u64)
}

fn generate_scenario(
    space: &PropertySpace,
    seed: u64,
    registry: &HostKindRegistry,
) -> Result<Scenario, String> {
    let mut rng = SubstreamRng::derive(seed, &SubstreamKey::Mutation { index: 0 });
    let peer_count = uniform_usize(&mut rng, space.peer_count_min, space.peer_count_max);
    let latency_ns = uniform_u64(&mut rng, space.latency_ns_min, space.latency_ns_max);
    let jitter_ns = uniform_u64(&mut rng, space.jitter_ns_min, space.jitter_ns_max);
    let loss_ppm = uniform_u32(&mut rng, space.loss_ppm_min, space.loss_ppm_max);

    let peers: Vec<String> = (0..peer_count).map(|i| format!("peer_{i}")).collect();
    let peer_records: Vec<Peer> = peers
        .iter()
        .map(|id| Peer {
            id: id.clone(),
            kind: space.host_kind.clone(),
            kind_config: space.kind_config.clone(),
            initial_state: space.initial_state.clone(),
            tick_period_ns_override: None,
        })
        .collect();
    let link_policy = LinkPolicy {
        latency_ns,
        jitter_stddev_ns: jitter_ns,
        loss_prob_ppm: loss_ppm,
        reorder_prob_ppm: 0,
        bandwidth_bps: 1_000_000_000,
        cold_dial_penalty_ns: 0,
        cache_warm_after_ns: 0,
        cache_invalidate_after_idle_ns: 10_000_000_000,
    };
    let mut links = Vec::new();
    for from in &peers {
        for to in &peers {
            if from == to {
                continue;
            }
            links.push(Link {
                from: from.clone(),
                to: to.clone(),
                policy: link_policy,
            });
        }
    }
    let assertions = space
        .assertion_templates
        .iter()
        .flat_map(|t| expand_template(t, &peers))
        .collect::<Vec<_>>();
    let scenario = Scenario {
        name: format!("property_sample_{seed}"),
        seed,
        duration_ns: space.duration_ns,
        early_terminate_on_all_assertions_resolved: false,
        default_tick: DefaultTick {
            period_ns: space.default_tick_period_ns,
        },
        default_link: link_policy,
        peers: peer_records,
        relays: Vec::new(),
        links,
        mutations: Vec::new(),
        snapshots: Vec::new(),
        assertions,
        routes: Vec::new(),
    };
    // Round-trip through the loader so we get the same validation
    // the on-disk path does.
    let text = crate::scenario::to_toml(&scenario);
    crate::scenario::load_from_str(
        std::path::Path::new("property://generated.toml"),
        &text,
        registry,
    )
    .map_err(|e| e.to_string())
}

fn expand_template(t: &AssertionTemplate, peers: &[String]) -> Vec<Assertion> {
    match t {
        AssertionTemplate::NoFlapWhileProbesOkForEachPeer {
            window_start_ns,
            window_end_ns,
        } => peers
            .iter()
            .map(|p| Assertion {
                kind: AssertionKind::NoFlapWhileProbesOk {
                    peer: p.clone(),
                    window_start_ns: *window_start_ns,
                    window_end_ns: *window_end_ns,
                },
            })
            .collect(),
        AssertionTemplate::NoDeadWhenProbesOkForEachPeer {
            window_start_ns,
            window_end_ns,
        } => peers
            .iter()
            .map(|p| Assertion {
                kind: AssertionKind::NoDeadWhenProbesOk {
                    peer: p.clone(),
                    window_start_ns: *window_start_ns,
                    window_end_ns: *window_end_ns,
                },
            })
            .collect(),
        AssertionTemplate::AllAliveAt { at_ns } => vec![Assertion {
            kind: AssertionKind::AllAliveAt {
                at_ns: *at_ns,
                peers: peers.to_vec(),
            },
        }],
        AssertionTemplate::EventCount { event_kind, max } => vec![Assertion {
            kind: AssertionKind::EventCount {
                event_kind: event_kind.clone(),
                max: *max,
            },
        }],
    }
}

fn uniform_u64(rng: &mut SubstreamRng, lo: u64, hi: u64) -> u64 {
    if hi <= lo {
        return lo;
    }
    let span = (hi - lo) + 1;
    // gen_range_u32 is u32; for u64 spans we draw two and combine.
    let lo_word = rng.next_u32() as u64;
    let hi_word = rng.next_u32() as u64;
    let raw = (hi_word << 32) | lo_word;
    lo + (raw % span)
}

fn uniform_u32(rng: &mut SubstreamRng, lo: u32, hi: u32) -> u32 {
    if hi <= lo {
        return lo;
    }
    let span = (hi - lo) + 1;
    lo + rng.gen_range_u32(span)
}

fn uniform_usize(rng: &mut SubstreamRng, lo: usize, hi: usize) -> usize {
    if hi <= lo {
        return lo;
    }
    let span = (hi - lo + 1) as u32;
    lo + rng.gen_range_u32(span) as usize
}

// ──────────────────────────────────────────────────────────────────────
// Record → evaluator-input projection
// ──────────────────────────────────────────────────────────────────────

fn parse_records_to_evaluator_inputs(
    records: &[crate::bundle::BundleRecord],
) -> (Vec<crate::evaluator::EventLine>, crate::evaluator::SnapshotIndex) {
    use crate::bundle::BundleRecord;
    use crate::evaluator::SnapshotIndex;
    let mut events = Vec::new();
    let mut idx = SnapshotIndex::default();
    let mut line_idx = 0usize;
    let mut seq_by_host: std::collections::BTreeMap<String, u32> =
        std::collections::BTreeMap::new();
    for rec in records {
        match rec {
            BundleRecord::Event(e) => {
                events.push(crate::evaluator::EventLine::from_event_record(e, line_idx));
                line_idx += 1;
            }
            BundleRecord::Mutation(m) => {
                events
                    .push(crate::evaluator::EventLine::from_mutation_record(m, line_idx));
                line_idx += 1;
            }
            BundleRecord::Snapshot(s) => {
                let seq = seq_by_host.entry(s.host_id.clone()).or_insert(0);
                let entry = crate::evaluator::SnapshotEntry::from_snapshot_record(s, *seq);
                *seq += 1;
                idx.by_host.entry(s.host_id.clone()).or_default().push(entry);
            }
        }
    }
    (events, idx)
}
