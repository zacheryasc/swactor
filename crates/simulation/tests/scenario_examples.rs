//! §8.4 "Examples are well-formed" — every shipped scenario parses and
//! validates. Also exercises the round-trip property and the "Loading
//! is pure" rule (no filesystem writes during load).
//!
//! These are scenario-level tests: they treat the loader as a black
//! box and assert the behaviour §8 names.

use std::path::{Path, PathBuf};

use simulation::scenario::{HostKindRegistry, load_from_path, load_from_str, to_toml};

fn scenarios_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scenarios")
}

fn shipped_scenarios() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for sub in ["smoke", "reproduction", "topology", "calibration"] {
        let dir = scenarios_dir().join(sub);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|s| s.to_str()) == Some("toml") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

#[test]
fn every_shipped_scenario_parses_and_validates() {
    let registry = HostKindRegistry::with_swim();
    let scenarios = shipped_scenarios();
    assert!(
        !scenarios.is_empty(),
        "no shipped scenarios found under {}",
        scenarios_dir().display()
    );
    for path in scenarios {
        let scenario = load_from_path(&path, &registry)
            .unwrap_or_else(|e| panic!("scenario {} failed to load:\n{e}", path.display()));
        assert!(!scenario.peers.is_empty());
        assert!(scenario.duration_ns > 0);
    }
}

#[test]
fn parse_emit_parse_is_identity() {
    // §8.4 "Parse is invertible." Parse, emit back to TOML, re-parse,
    // and assert equality. We do this for every shipped scenario so
    // any future field that breaks the round trip is caught here.
    let registry = HostKindRegistry::with_swim();
    for path in shipped_scenarios() {
        let first = load_from_path(&path, &registry)
            .unwrap_or_else(|e| panic!("first parse {}: {e}", path.display()));
        let emitted = to_toml(&first);
        let second = load_from_str(Path::new("(roundtrip)"), &emitted, &registry)
            .unwrap_or_else(|e| {
                panic!(
                    "round-trip parse failed for {}: {e}\n--- emitted text follows ---\n{emitted}",
                    path.display()
                )
            });
        assert_eq!(
            first,
            second,
            "parse(emit(parse(t))) != parse(t) for {}",
            path.display()
        );
    }
}

#[test]
fn loading_is_pure_no_filesystem_writes() {
    // §8.4 "Loading is pure. Loading the same file twice produces
    // equal values and performs no filesystem writes." We don't have a
    // perfect process-level write detector, but we can: (a) ensure the
    // scenarios directory contents are unchanged after a load, and
    // (b) assert idempotence directly.
    let registry = HostKindRegistry::with_swim();
    let dir = scenarios_dir();
    let snapshot_before = snapshot_dir(&dir);
    for path in shipped_scenarios() {
        let a = load_from_path(&path, &registry).expect("load 1");
        let b = load_from_path(&path, &registry).expect("load 2");
        assert_eq!(a, b, "two loads of {} disagree", path.display());
    }
    let snapshot_after = snapshot_dir(&dir);
    assert_eq!(
        snapshot_before, snapshot_after,
        "scenarios directory changed during load — loader is not pure"
    );
}

fn snapshot_dir(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    visit(root, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn visit(dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            visit(&p, out);
        } else if let Ok(bytes) = std::fs::read(&p) {
            out.push((p, bytes));
        }
    }
}
