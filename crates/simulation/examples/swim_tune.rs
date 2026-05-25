//! SWIM tuning harness.
//!
//! Runs the gossip-flap reproduction, the three N3 calibration scenarios,
//! and a synthesised §10.3 library property under a configurable SWIM
//! `kind_config`. Prints one line of NDJSON per scenario per config:
//!
//! ```text
//! {"scenario": "gossip_flap", "config": {...}, "verdicts": [...],
//!  "metrics": {"self_incarnation_peak": 12, "relay_queue_peak_bytes": 0, ...}}
//! ```
//!
//! Invocation
//!
//! ```text
//! cargo run --release --example swim_tune -- \
//!     --probe_interval_ns 1000000000 \
//!     --probe_timeout_ns 350000000 \
//!     --suspicion_timeout_ns 8000000000 \
//!     --indirect_ping_fanout 3 \
//!     --dead_reprobe_interval_ns 5000000000
//! ```
//!
//! Each flag is optional; omitted flags use the production default
//! (which the binary derives from `SwimConfig::default()` translated
//! through the scenario's tick period). The CLI is positional/loose
//! on purpose — this is an internal sweep tool, not a stable interface.
//!
//! `--mode baseline` strips SWIM kind_config overrides from the scenario
//! so the live `SwimConfig::default()` values take effect. `--mode tuned`
//! (the default) injects the supplied knobs into every SWIM peer's
//! kind_config.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use distribution::swim::probe::SwimConfig;
use simulation::bundle::VecWriter;
use simulation::engine::Engine;
use simulation::evaluator::{EventLine, Outcome, SnapshotEntry, SnapshotIndex, evaluate};
use simulation::network::Network;
use simulation::scenario::{
    Assertion, AssertionKind, DefaultTick, HostKindRegistry, Link, LinkPolicy, Peer, Scenario,
    load_from_path,
};
use simulation::swim_host::SwimHostFactory;

#[derive(Debug, Clone, Copy)]
struct Knobs {
    probe_interval_ns: Option<u64>,
    probe_timeout_ns: Option<u64>,
    suspicion_timeout_ns: Option<u64>,
    indirect_ping_fanout: Option<u64>,
    dead_reprobe_interval_ns: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Strip kind_config overrides so SwimConfig::default() takes
    /// effect (used to capture the *current* production defaults).
    Baseline,
    /// Inject the supplied knobs into every SWIM peer's kind_config.
    Tuned,
}

fn parse_args() -> (Mode, Knobs, Option<String>) {
    let mut knobs = Knobs {
        probe_interval_ns: None,
        probe_timeout_ns: None,
        suspicion_timeout_ns: None,
        indirect_ping_fanout: None,
        dead_reprobe_interval_ns: None,
    };
    let mut mode = Mode::Tuned;
    let mut scenario: Option<String> = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    while i < args.len() {
        let a = &args[i];
        i += 1;
        let mut take = || {
            let v = args.get(i).cloned().expect("value");
            i += 1;
            v
        };
        match a.as_str() {
            "--mode" => {
                mode = match take().as_str() {
                    "baseline" => Mode::Baseline,
                    "tuned" => Mode::Tuned,
                    other => panic!("--mode must be baseline|tuned, got {other}"),
                };
            }
            "--scenario" => scenario = Some(take()),
            "--probe_interval_ns" => knobs.probe_interval_ns = Some(take().parse().unwrap()),
            "--probe_timeout_ns" => knobs.probe_timeout_ns = Some(take().parse().unwrap()),
            "--suspicion_timeout_ns" => knobs.suspicion_timeout_ns = Some(take().parse().unwrap()),
            "--indirect_ping_fanout" => knobs.indirect_ping_fanout = Some(take().parse().unwrap()),
            "--dead_reprobe_interval_ns" => {
                knobs.dead_reprobe_interval_ns = Some(take().parse().unwrap())
            }
            other => panic!("unknown arg {other}"),
        }
    }
    (mode, knobs, scenario)
}

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn cargo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn load(rel: &str) -> Scenario {
    let path = cargo_root().join(rel);
    load_from_path(&path, &registry()).expect("scenario validates")
}

/// Mutate every `swim` peer's `kind_config` according to mode + knobs.
///
/// - Baseline strips probe_interval_ns / probe_timeout_ns /
///   suspicion_timeout_ns / indirect_ping_fanout /
///   dead_reprobe_interval_ns so `SwimHost::config_from_kind` falls
///   through to SwimConfig::default()-derived values.
/// - Tuned writes the supplied knobs and removes the rest (so the
///   adapter's tick-period fallback gives default-equivalent values).
fn apply_knobs(scenario: &mut Scenario, mode: Mode, knobs: Knobs) {
    if mode == Mode::Baseline {
        // Baseline runs the scenario exactly as it sits on disk. The
        // §8 validator requires probe_interval_ns and
        // suspicion_timeout_ns, so we cannot blanket-strip; the
        // scenarios' own kind_config values are the "before" picture.
        return;
    }
    for peer in &mut scenario.peers {
        if peer.kind != "swim" {
            continue;
        }
        if let Some(v) = knobs.probe_interval_ns {
            peer.kind_config
                .insert("probe_interval_ns".into(), toml::Value::Integer(v as i64));
        }
        if let Some(v) = knobs.probe_timeout_ns {
            peer.kind_config
                .insert("probe_timeout_ns".into(), toml::Value::Integer(v as i64));
        }
        if let Some(v) = knobs.suspicion_timeout_ns {
            peer.kind_config.insert(
                "suspicion_timeout_ns".into(),
                toml::Value::Integer(v as i64),
            );
        }
        if let Some(v) = knobs.indirect_ping_fanout {
            peer.kind_config.insert(
                "indirect_ping_fanout".into(),
                toml::Value::Integer(v as i64),
            );
        }
        if let Some(v) = knobs.dead_reprobe_interval_ns {
            peer.kind_config.insert(
                "dead_reprobe_interval_ns".into(),
                toml::Value::Integer(v as i64),
            );
        }
    }
}

#[derive(serde::Serialize)]
struct ScenarioReport {
    scenario: String,
    verdicts: Vec<VerdictBrief>,
    metrics: Metrics,
}

#[derive(serde::Serialize)]
struct VerdictBrief {
    name: String,
    kind: &'static str,
    outcome: String,
}

#[derive(serde::Serialize, Default)]
struct Metrics {
    /// Peak self_incarnation across any snapshot.
    self_incarnation_peak: u64,
    /// Peak relay enqueued_bytes seen via replay of relay events.
    relay_queue_peak_bytes: u64,
    /// Largest piggybacked message_send `bytes` value over the run.
    message_size_peak: u64,
    /// Earliest convergence time across observers (ns from t=0). None
    /// if no snapshot witnessed agreement.
    convergence_observed_ns: Option<u64>,
    /// Number of state_transition events into Suspect across the run.
    suspect_events: u64,
    /// Number of state_transition events into Dead across the run.
    dead_events: u64,
    /// Number of state_transition events into Alive across the run.
    alive_events: u64,
}

fn run_scenario_report(name: &str, mut scen: Scenario, mode: Mode, knobs: Knobs) -> ScenarioReport {
    apply_knobs(&mut scen, mode, knobs);
    let writer = VecWriter::default();
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(SwimHostFactory));
    engine.register_factory(Box::new(simulation::stage_host::StageHostFactory));
    engine.auto_install_hosts();
    engine.set_pop_budget(2_000_000);
    let _ = engine.run();
    let records = engine.into_writer().records;
    let (events, snapshots) = records_to_eval_inputs(&records);
    let verdicts = evaluate(&scen, &events, &snapshots);
    let metrics = collect_metrics(&events, &snapshots, &scen);
    ScenarioReport {
        scenario: name.to_string(),
        verdicts: verdicts
            .iter()
            .map(|v| VerdictBrief {
                name: v.name.clone(),
                kind: v.kind,
                outcome: outcome_word(&v.outcome).to_string(),
            })
            .collect(),
        metrics,
    }
}

fn outcome_word(o: &Outcome) -> &'static str {
    match o {
        Outcome::Pass => "PASS",
        Outcome::Fail => "FAIL",
        Outcome::Inconclusive => "INCONCLUSIVE",
    }
}

fn records_to_eval_inputs(
    records: &[simulation::bundle::BundleRecord],
) -> (Vec<EventLine>, SnapshotIndex) {
    use simulation::bundle::BundleRecord;
    let mut events = Vec::new();
    let mut idx = SnapshotIndex::default();
    let mut line_idx = 0usize;
    let mut seq_by_host: BTreeMap<String, u32> = BTreeMap::new();
    for rec in records {
        match rec {
            BundleRecord::Event(e) => {
                events.push(EventLine::from_event_record(e, line_idx));
                line_idx += 1;
            }
            BundleRecord::Mutation(m) => {
                events.push(EventLine::from_mutation_record(m, line_idx));
                line_idx += 1;
            }
            BundleRecord::Snapshot(s) => {
                let seq = seq_by_host.entry(s.host_id.clone()).or_insert(0);
                let entry = SnapshotEntry::from_snapshot_record(s, *seq);
                *seq += 1;
                idx.by_host.entry(s.host_id.clone()).or_default().push(entry);
            }
        }
    }
    (events, idx)
}

fn collect_metrics(events: &[EventLine], snaps: &SnapshotIndex, scen: &Scenario) -> Metrics {
    let mut m = Metrics::default();
    // Snapshot-derived: self_incarnation peak.
    for list in snaps.by_host.values() {
        for s in list {
            if s.self_incarnation > m.self_incarnation_peak {
                m.self_incarnation_peak = s.self_incarnation;
            }
        }
    }
    // Relay queue peak: replay enqueue/dequeue in time order.
    let mut relay_events: Vec<&EventLine> = events
        .iter()
        .filter(|e| {
            e.kind_tag == "relay"
                && (e.event["kind"] == "relay_enqueue" || e.event["kind"] == "relay_dequeue")
        })
        .collect();
    relay_events.sort_by(|a, b| {
        a.virtual_time_ns
            .cmp(&b.virtual_time_ns)
            .then(a.line_idx.cmp(&b.line_idx))
    });
    let mut relay_depths: BTreeMap<String, u64> = BTreeMap::new();
    for e in &relay_events {
        let relay = e.event["relay"].as_str().unwrap_or("").to_string();
        let bl = e.event["byte_len"].as_u64().unwrap_or(0);
        let entry = relay_depths.entry(relay).or_insert(0);
        match e.event["kind"].as_str() {
            Some("relay_enqueue") => {
                *entry = entry.saturating_add(bl);
                if *entry > m.relay_queue_peak_bytes {
                    m.relay_queue_peak_bytes = *entry;
                }
            }
            Some("relay_dequeue") => {
                *entry = entry.saturating_sub(bl);
            }
            _ => {}
        }
    }
    for e in events {
        if e.event["kind"] == "message_send" {
            let bytes = e.event["bytes"].as_u64().unwrap_or(0);
            if bytes > m.message_size_peak {
                m.message_size_peak = bytes;
            }
        }
        if e.event["kind"] == "state_transition" {
            match e.event["to"].as_str() {
                Some("Suspect") => m.suspect_events += 1,
                Some("Dead") => m.dead_events += 1,
                Some("Alive") => m.alive_events += 1,
                _ => {}
            }
        }
    }
    // Convergence: earliest snapshot time at which every observer's
    // membership view of every other peer agrees. We approximate by
    // checking each observer's full snapshot list and looking for the
    // smallest virtual_time_ns where all observers agree on every
    // subject's `state`.
    let peers: Vec<String> = scen
        .peers
        .iter()
        .filter(|p| p.kind == "swim")
        .map(|p| p.id.clone())
        .collect();
    let mut all_times: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for p in &peers {
        if let Some(list) = snaps.by_host.get(p) {
            for s in list {
                all_times.insert(s.virtual_time_ns);
            }
        }
    }
    for t in all_times {
        let mut converged = true;
        'outer: for subject in &peers {
            let mut last: Option<String> = None;
            for observer in &peers {
                if observer == subject {
                    continue;
                }
                let Some(list) = snaps.by_host.get(observer) else {
                    converged = false;
                    break 'outer;
                };
                let snap = list.iter().filter(|s| s.virtual_time_ns <= t).next_back();
                let Some(snap) = snap else {
                    converged = false;
                    break 'outer;
                };
                let state = snap
                    .members
                    .get(subject)
                    .map(|mv| mv.state.clone())
                    .unwrap_or_else(|| "Unknown".to_string());
                if let Some(prev) = &last {
                    if prev != &state {
                        converged = false;
                        break 'outer;
                    }
                } else {
                    last = Some(state);
                }
            }
        }
        if converged && !peers.is_empty() {
            m.convergence_observed_ns = Some(t);
            break;
        }
    }
    m
}

// ──────────────────────────────────────────────────────────────────────
// Synthesised gossip-flap library property (§10.3 primary scorer).
//
// 3-peer mesh, 60 ms link latency, 15 ms jitter, 0.5 % loss, 20 s
// duration, snapshots every 2 s — matches `gossip_flap.toml`'s shape
// but built in code so we can vary the SWIM kind_config per run without
// disturbing the on-disk scenario. A passing tuning brings
// self_incarnation_bounded into Pass on this scenario.
// ──────────────────────────────────────────────────────────────────────

fn gossip_flap_property_scenario(mode: Mode, knobs: Knobs) -> Scenario {
    let mut kind_config = toml::value::Table::new();
    // Baseline values mirror the on-disk gossip_flap.toml's kind_config.
    // The §8 validator requires probe_interval_ns and suspicion_timeout_ns
    // to be present, so we always seed them; tuned mode overrides.
    kind_config.insert(
        "probe_interval_ns".into(),
        toml::Value::Integer(500_000_000),
    );
    kind_config.insert("probe_timeout_ns".into(), toml::Value::Integer(100_000_000));
    kind_config.insert(
        "suspicion_timeout_ns".into(),
        toml::Value::Integer(2_000_000_000),
    );
    kind_config.insert("indirect_ping_fanout".into(), toml::Value::Integer(3));
    if mode == Mode::Tuned {
        if let Some(v) = knobs.probe_interval_ns {
            kind_config.insert("probe_interval_ns".into(), toml::Value::Integer(v as i64));
        }
        if let Some(v) = knobs.probe_timeout_ns {
            kind_config.insert("probe_timeout_ns".into(), toml::Value::Integer(v as i64));
        }
        if let Some(v) = knobs.suspicion_timeout_ns {
            kind_config.insert(
                "suspicion_timeout_ns".into(),
                toml::Value::Integer(v as i64),
            );
        }
        if let Some(v) = knobs.indirect_ping_fanout {
            kind_config.insert(
                "indirect_ping_fanout".into(),
                toml::Value::Integer(v as i64),
            );
        }
        if let Some(v) = knobs.dead_reprobe_interval_ns {
            kind_config.insert(
                "dead_reprobe_interval_ns".into(),
                toml::Value::Integer(v as i64),
            );
        }
    }
    let peers_ids = ["orchestrator", "worker_a", "worker_b"];
    let peers: Vec<Peer> = peers_ids
        .iter()
        .map(|id| Peer {
            id: (*id).into(),
            kind: "swim".into(),
            kind_config: kind_config.clone(),
            initial_state: "alive".into(),
            tick_period_ns_override: None,
        })
        .collect();
    let policy = LinkPolicy {
        latency_ns: 60_000_000,
        jitter_stddev_ns: 15_000_000,
        loss_prob_ppm: 5_000,
        reorder_prob_ppm: 0,
        bandwidth_bps: 25_000_000,
        cold_dial_penalty_ns: 200_000_000,
        cache_warm_after_ns: 200_000_000,
        cache_invalidate_after_idle_ns: 10_000_000_000,
    };
    let mut links = Vec::new();
    for a in &peers_ids {
        for b in &peers_ids {
            if a == b {
                continue;
            }
            links.push(Link {
                from: (*a).into(),
                to: (*b).into(),
                policy,
            });
        }
    }
    let mut snapshots = Vec::new();
    for at_ns in [2_000_000_000u64, 4_000_000_000, 6_000_000_000, 8_000_000_000,
        10_000_000_000, 12_000_000_000, 14_000_000_000, 16_000_000_000,
        18_000_000_000, 19_500_000_000]
    {
        snapshots.push(simulation::scenario::Snapshot { at_ns });
    }
    let assertions = vec![
        Assertion {
            kind: AssertionKind::SelfIncarnationBounded {
                peer: "orchestrator".into(),
                max_value: 2,
            },
        },
        Assertion {
            kind: AssertionKind::SelfIncarnationBounded {
                peer: "worker_a".into(),
                max_value: 2,
            },
        },
        Assertion {
            kind: AssertionKind::SelfIncarnationBounded {
                peer: "worker_b".into(),
                max_value: 2,
            },
        },
        Assertion {
            kind: AssertionKind::ConvergenceAfter {
                after_ns: 0,
                within_ns: 10_000_000_000,
                peers: peers_ids.iter().map(|s| (*s).into()).collect(),
            },
        },
        Assertion {
            kind: AssertionKind::MessageSizeBounded {
                message_kind: "swactor_dist::Ping".into(),
                max_bytes: 4_096,
            },
        },
    ];
    let scen = Scenario {
        name: "gossip_flap_property".into(),
        seed: 42,
        duration_ns: 20_000_000_000,
        early_terminate_on_all_assertions_resolved: false,
        default_tick: DefaultTick { period_ns: 50_000_000 },
        default_link: policy,
        peers,
        relays: Vec::new(),
        links,
        mutations: Vec::new(),
        snapshots,
        assertions,
        routes: Vec::new(),
    };
    // Round-trip through the loader to populate routes etc.
    let text = simulation::scenario::to_toml(&scen);
    simulation::scenario::load_from_str(
        Path::new("property://gossip_flap.toml"),
        &text,
        &registry(),
    )
    .expect("synthesised scenario validates")
}

fn main() {
    let (mode, knobs, only) = parse_args();
    // Emit the effective SwimConfig::default() once so the operator
    // sees what "baseline" actually means in tick-units.
    let defaults = SwimConfig::default();
    eprintln!(
        "[meta] SwimConfig::default = {{ probe_interval: {}, probe_timeout: {}, suspicion_timeout: {}, indirect_probes: {}, dead_reprobe_interval: {} }}",
        defaults.probe_interval,
        defaults.probe_timeout,
        defaults.suspicion_timeout,
        defaults.indirect_probes,
        defaults.dead_reprobe_interval,
    );
    eprintln!("[meta] mode={mode:?} knobs={knobs:?}");

    // The four scenarios we score.
    let scenarios: Vec<(&str, Scenario)> = vec![
        (
            "gossip_flap_repro",
            load("scenarios/reproduction/gossip_flap.toml"),
        ),
        (
            "n3_own_relay_stub",
            load("scenarios/calibration/n3_own_relay_stub.toml"),
        ),
        (
            "n3_own_relay_real_worker",
            load("scenarios/calibration/n3_own_relay_real_worker.toml"),
        ),
        (
            "n3_canary_relay_real_worker",
            load("scenarios/calibration/n3_canary_relay_real_worker.toml"),
        ),
        (
            "gossip_flap_property",
            gossip_flap_property_scenario(mode, knobs),
        ),
    ];

    for (name, scen) in scenarios {
        if let Some(only_name) = &only {
            if name != only_name {
                continue;
            }
        }
        let report = run_scenario_report(name, scen, mode, knobs);
        let line =
            serde_json::to_string(&report).expect("ScenarioReport serialises by construction");
        println!("{line}");
    }
}
