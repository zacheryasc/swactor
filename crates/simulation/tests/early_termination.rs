//! SIM_SPEC §4.8 ↔ §10.4 integration: the engine consults the
//! streaming evaluator after every dispatch and stops as soon as
//! every assertion is resolved, but only when the scenario opted
//! in via `early_terminate_on_all_assertions_resolved = true`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use simulation::bundle::VecWriter;
use simulation::engine::{Engine, TerminationReason};
use simulation::evaluator::StreamingEvaluator;
use simulation::host::{Action, Host, HostMessage, KindTag, SnapshotBytes};
use simulation::network::Network;
use simulation::scenario::{HostKindRegistry, Scenario, load_from_str};

// ──────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn fake_path() -> &'static Path {
    Path::new("test://early.toml")
}

/// A trivial host that emits one probe_sent event on every tick. Used
/// to drive `event_count`-style assertions over the streaming side.
struct PingFloodHost {
    id: String,
    log: Arc<Mutex<Vec<u64>>>,
}

impl Host for PingFloodHost {
    fn id(&self) -> &str {
        &self.id
    }
    fn kind_tag(&self) -> KindTag {
        "swim"
    }
    fn tick(&mut self, now_ns: u64) -> Vec<Action> {
        self.log.lock().unwrap().push(now_ns);
        vec![Action::RecordEvent {
            kind_tag: "swim".into(),
            event: serde_json::to_vec(&serde_json::json!({"kind": "probe_sent"})).unwrap(),
        }]
    }
    fn recv(&mut self, _message: HostMessage, _now_ns: u64) -> Vec<Action> {
        Vec::new()
    }
    fn snapshot(&self) -> SnapshotBytes {
        b"{}".to_vec()
    }
}

fn make_scenario(opt_in: bool) -> Scenario {
    let flag = if opt_in {
        "early_terminate_on_all_assertions_resolved = true\n"
    } else {
        ""
    };
    let body = format!(
        r#"
name = "early"
seed = 1
duration_ns = 10_000_000
{flag}
[default_tick]
period_ns = 1_000_000

[default_link]
latency_ns = 0
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1_000_000_000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 10_000_000_000

[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = {{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }}

[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = {{ probe_interval_ns = 1, suspicion_timeout_ns = 10 }}

[[links]]
from = "a"
to = "b"

[[links]]
from = "b"
to = "a"

# Two assertions. The first fails as soon as probe_sent fires twice
# in a row (max=0 ⇒ Fail at the first event). The second has no
# matching events so it is Pass at construction.
[[assertions]]
kind = "event_count"
event_kind = "probe_sent"
max = 0

[[assertions]]
kind = "event_count"
event_kind = "never_fires"
max = 5
"#,
    );
    load_from_str(fake_path(), &body, &registry()).unwrap()
}

fn run_with_streaming(scenario: &Scenario) -> (TerminationReason, Vec<u64>) {
    let writer = VecWriter::default();
    let network = Network::new(scenario);
    let mut engine = Engine::new(scenario, network, writer);
    engine.enable_streaming(StreamingEvaluator::new(scenario.clone()));
    let log = Arc::new(Mutex::new(Vec::new()));
    engine.install_host(Box::new(PingFloodHost {
        id: "a".into(),
        log: log.clone(),
    }));
    engine.install_host(Box::new(PingFloodHost {
        id: "b".into(),
        log: log.clone(),
    }));
    engine.set_pop_budget(2000);
    let term = engine.run();
    let ticks = log.lock().unwrap().clone();
    (term, ticks)
}

// ──────────────────────────────────────────────────────────────────────
// §4.8 / §10.4
// ──────────────────────────────────────────────────────────────────────

#[test]
fn early_terminate_fires_when_scenario_opted_in_and_assertions_are_resolved() {
    let scen = make_scenario(true);
    let (term, ticks) = run_with_streaming(&scen);
    assert_eq!(term, TerminationReason::EarlyAllAssertionsResolved);
    // The Fail-on-first-probe assertion resolves on the first tick,
    // so we should see no more than a handful of ticks before
    // termination (vs the 10ms / 1ms = 10 ticks the run would have
    // gone otherwise).
    assert!(
        ticks.len() <= 6,
        "should have terminated early; saw {} ticks",
        ticks.len()
    );
}

#[test]
fn early_terminate_does_not_fire_when_scenario_did_not_opt_in() {
    let scen = make_scenario(false);
    let (term, ticks) = run_with_streaming(&scen);
    assert_eq!(term, TerminationReason::DurationReached);
    // Without the opt-in, the engine runs until duration_ns even
    // though every assertion is resolved.
    assert!(
        ticks.len() >= 10,
        "should have run to duration; saw {} ticks",
        ticks.len()
    );
}

#[test]
fn streaming_evaluator_observes_engine_events_in_order() {
    // The streaming side and the bundle writer see the same record
    // stream. We assert that by feeding a scenario where the
    // `event_count` resolution depends on the order events appear:
    // assertion `max = 1` is Pass after 1 event but Fail after 2.
    let body = r#"
name = "stream_order"
seed = 1
duration_ns = 10_000_000
early_terminate_on_all_assertions_resolved = true

[default_tick]
period_ns = 1_000_000

[default_link]
latency_ns = 0
jitter_stddev_ns = 0
loss_prob_ppm = 0
reorder_prob_ppm = 0
bandwidth_bps = 1_000_000_000
cold_dial_penalty_ns = 0
cache_warm_after_ns = 0
cache_invalidate_after_idle_ns = 10_000_000_000

[[peers]]
id = "a"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }

[[peers]]
id = "b"
kind = "swim"
initial_state = "alive"
kind_config = { probe_interval_ns = 1, suspicion_timeout_ns = 10 }

[[links]]
from = "a"
to = "b"

[[links]]
from = "b"
to = "a"

[[assertions]]
kind = "event_count"
event_kind = "probe_sent"
max = 1
"#;
    let scen = load_from_str(fake_path(), body, &registry()).unwrap();
    let (term, ticks) = run_with_streaming(&scen);
    assert_eq!(term, TerminationReason::EarlyAllAssertionsResolved);
    // Two ticks (one per peer) push the probe_sent count above 1,
    // so termination should fire shortly after the second event.
    assert!(ticks.len() <= 6, "early-resolved after 2nd probe_sent; saw {}", ticks.len());
}
