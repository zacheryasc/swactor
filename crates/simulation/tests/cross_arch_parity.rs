//! SIM_SPEC §7.6 cross-architecture parity test.
//!
//! Runs the reference scenario through the full pipeline (scenario
//! loader → engine → network → bundle writer) using the deterministic
//! `parity_stub` host kind, then compares the SHA-256 of the bundle's
//! `events.ndjson` against the constant `EXPECTED_EVENTS_NDJSON_SHA256`
//! below.
//!
//! A mismatch is either:
//!   - a deliberate spec amendment (in which case the commit that
//!     changes the simulator's emission updates this constant and
//!     explains why), or
//!   - a determinism bug. The §7 contract says the bundle is
//!     byte-identical across runs *and* architectures for the same
//!     `seed`; this test gates against both.
//!
//! The test also runs the scenario twice in-process and asserts the
//! two digests match — that catches per-run drift even before the
//! constant gets updated.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use simulation::bundle_file::FileBundleWriter;
use simulation::engine::Engine;
use simulation::network::Network;
use simulation::parity_host::{ParityStubHost, ParityStubKindValidator};
use simulation::scenario::{HostKindRegistry, Scenario, load_from_path};

/// SHA-256 of `events.ndjson` for the reference scenario at
/// `scenarios/parity/reference.toml`. Checked in; updated only as
/// part of a deliberate spec amendment.
// Updated when the parity reference scenario gained a relayed route
// (RELAY_SPEC §13 — "the cross-architecture parity test extended to
// cover relay-mediated routes"). The alpha↔charlie pair now routes
// through relay `R`, so the bundle stream contains `relay_enqueue` /
// `relay_dequeue` records and the alpha↔charlie arrival times shift
// to reflect the relay's ingress + egress serialization.
//
// Previous value, from iteration 10's DialOutcome correction:
//   a76b557d3da7a5d0f393446231669b8f7b1945a251805bed33fa09fb27ab3db0
const EXPECTED_EVENTS_NDJSON_SHA256: &str =
    "c7ee2a328c796f482b6c62694fc27936aa58a959b17f59fedd14a3f8c20ad2f2";

fn registry() -> HostKindRegistry {
    let mut r = HostKindRegistry::with_swim();
    r.register(Box::new(ParityStubKindValidator));
    r
}

fn load_reference() -> Scenario {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios/parity/reference.toml");
    load_from_path(&path, &registry()).expect("reference scenario must validate")
}

fn run_and_hash(scenario: &Scenario, out_dir: PathBuf) -> String {
    let writer = FileBundleWriter::new(&out_dir, scenario.clone());
    let network = Network::new(scenario);
    let mut engine = Engine::new(scenario, network, writer);
    let peer_ids: Vec<String> = scenario.peers.iter().map(|p| p.id.clone()).collect();
    for peer in &scenario.peers {
        engine.install_host(Box::new(ParityStubHost::new(
            peer.id.clone(),
            peer_ids.clone(),
        )));
    }
    engine.set_pop_budget(10_000);
    let _ = engine.run();
    let writer = engine.into_writer();
    writer.finalize().expect("finalize bundle");
    hex_sha256_of(&fs::read(out_dir.join("events.ndjson")).unwrap())
}

fn hex_sha256_of(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

// ──────────────────────────────────────────────────────────────────────
// §7.6 cross-architecture parity
// ──────────────────────────────────────────────────────────────────────

#[test]
fn reference_scenario_events_ndjson_matches_checked_in_digest() {
    let scen = load_reference();
    let tmp = TempDir::new().unwrap();
    let digest = run_and_hash(&scen, tmp.path().join("bundle"));
    assert_eq!(
        digest, EXPECTED_EVENTS_NDJSON_SHA256,
        "\
events.ndjson digest changed.

If this was a deliberate spec amendment, update the constant
EXPECTED_EVENTS_NDJSON_SHA256 in this file to:
  {digest}
…and explain why in the commit message.

Otherwise this is a determinism regression: the same scenario+seed
produced different bytes than the checked-in reference."
    );
}

#[test]
fn reference_scenario_is_deterministic_across_runs() {
    let scen = load_reference();
    let tmp = TempDir::new().unwrap();
    let d1 = run_and_hash(&scen, tmp.path().join("bundle_a"));
    let d2 = run_and_hash(&scen, tmp.path().join("bundle_b"));
    assert_eq!(d1, d2, "two runs of the same scenario must produce the same digest");
}

#[test]
fn reference_scenario_changing_seed_changes_the_digest() {
    // Negative: tweak the seed and verify the digest is *not* equal to
    // the reference. Confirms the digest is sensitive to scenario input
    // (i.e. not a constant byte sequence by accident).
    let mut scen = load_reference();
    scen.seed ^= 0x1234_5678;
    let tmp = TempDir::new().unwrap();
    let d = run_and_hash(&scen, tmp.path().join("bundle"));
    assert_ne!(d, EXPECTED_EVENTS_NDJSON_SHA256);
}
