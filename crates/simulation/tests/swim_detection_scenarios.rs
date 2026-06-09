//! Layer 2 (simulation) — Goal 2 / 3 / 6 detection scenarios, run end-to-end
//! through the engine + evaluator with their expected verdicts asserted.
//!
//! These exercise the net-new `peer_detected_dead_within` assertion kind and the
//! detection / resurrection / infection topologies that BEHAVIORAL_TEST_SPEC.md
//! "Move 3" calls the central gap:
//!   * Goal 2 — a genuinely killed node is *detected* Dead by every survivor
//!     (`peer_kill_detection.toml`, `peer_detected_dead_within`).
//!   * Goal 3 — a partitioned node's death is *resolved* (Dead→Alive) after heal
//!     (`partition_heal.toml`, `dead_peer_resurrects_within`).
//!   * Goal 6 — a change reaches every node across an infection topology where
//!     the changed node is a direct probe partner of only some
//!     (`infection_star.toml`, `convergence_after` over all N).

use std::path::PathBuf;

use tempfile::TempDir;

use simulation::bundle_file::FileBundleWriter;
use simulation::engine::Engine;
use simulation::evaluator::{Outcome, Verdict, evaluate_bundle};
use simulation::network::Network;
use simulation::scenario::{HostKindRegistry, Scenario, load_from_path};
use simulation::swim_host::SwimHostFactory;

fn load(rel: &str) -> Scenario {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    load_from_path(&path, &HostKindRegistry::with_swim()).expect("scenario validates")
}

fn run_and_evaluate(rel: &str) -> Vec<Verdict> {
    let scen = load(rel);
    let tmp = TempDir::new().unwrap();
    let out = tmp.path().join("bundle");
    let writer = FileBundleWriter::new(&out, scen.clone());
    let network = Network::new(&scen);
    let mut engine = Engine::new(&scen, network, writer);
    engine.register_factory(Box::new(SwimHostFactory));
    engine.auto_install_hosts();
    engine.set_pop_budget(2_000_000);
    let _ = engine.run();
    let writer = engine.into_writer();
    writer.finalize().expect("finalize bundle");
    evaluate_bundle(&out).expect("evaluator runs")
}

/// The verdict for the single assertion of the given kind.
fn verdict_for<'a>(verdicts: &'a [Verdict], kind: &str) -> &'a Verdict {
    verdicts
        .iter()
        .find(|v| v.kind == kind)
        .unwrap_or_else(|| panic!("no `{kind}` verdict among {:?}", verdicts.iter().map(|v| v.kind).collect::<Vec<_>>()))
}

// ─── Goal 2 — real detection ─────────────────────────────────────────────────

#[test]
fn goal2_peer_kill_is_detected_dead_by_every_survivor() {
    let verdicts = run_and_evaluate("scenarios/topology/peer_kill_detection.toml");
    let v = verdict_for(&verdicts, "peer_detected_dead_within");
    assert_eq!(
        v.outcome,
        Outcome::Pass,
        "the genuinely-killed node must be detected Dead by every survivor (Goal 2)"
    );
}

// ─── Goal 3 — death is provisional ───────────────────────────────────────────

#[test]
fn goal3_partitioned_node_resurrects_after_heal() {
    let verdicts = run_and_evaluate("scenarios/topology/partition_heal.toml");
    let v = verdict_for(&verdicts, "dead_peer_resurrects_within");
    assert_eq!(
        v.outcome,
        Outcome::Pass,
        "the isolated node's death must resolve to Alive after the heal (Goal 3)"
    );
}

// ─── Goal 6 — dissemination reaches everyone ─────────────────────────────────

#[test]
fn goal6_infection_topology_converges_over_all_nodes() {
    let verdicts = run_and_evaluate("scenarios/topology/infection_star.toml");
    let v = verdict_for(&verdicts, "convergence_after");
    assert_eq!(
        v.outcome,
        Outcome::Pass,
        "every node must converge on the shared view across the infection topology (Goal 6)"
    );
}
