//! SIM_SPEC §9.6 behavioural property tests for the bundle writer.
//! Scenario-level posture: build a `FileBundleWriter` against a temp
//! directory, feed it records (in various orders), call `finalize`,
//! then verify the directory layout, NDJSON envelope, hash integrity,
//! and ordering rules.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use simulation::bundle::{
    BundleRecord, BundleWriter, DeliveryDropReason, EventPayload, EventRecord, MutationRecord,
    SnapshotRecord,
};
use simulation::bundle_file::FileBundleWriter;
use simulation::network::{CacheTransition, DropReason};
use simulation::scenario::{HostKindRegistry, Mutation, MutationKind, Scenario, load_from_str};

// ──────────────────────────────────────────────────────────────────────
// Fixtures
// ──────────────────────────────────────────────────────────────────────

fn registry() -> HostKindRegistry {
    HostKindRegistry::with_swim()
}

fn fake_path() -> &'static Path {
    Path::new("test://bundle.toml")
}

fn small_scenario() -> Scenario {
    let body = r#"
name = "bundle_test"
seed = 99
duration_ns = 1_000_000

[default_tick]
period_ns = 1000

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
"#;
    load_from_str(fake_path(), body, &registry()).unwrap()
}

fn sample_records() -> Vec<BundleRecord> {
    vec![
        BundleRecord::Event(EventRecord {
            virtual_time_ns: 100,
            host_id: Some("a".into()),
            kind_tag: "swim".into(),
            event: EventPayload::Bytes(br#"{"kind":"state_transition","from":"alive","to":"suspect"}"#.to_vec()),
        }),
        BundleRecord::Event(EventRecord {
            virtual_time_ns: 50,
            host_id: Some("b".into()),
            kind_tag: "engine".into(),
            event: EventPayload::DropOnSend {
                from: "a".into(),
                to: "b".into(),
                reason: DropReason::Lossy,
            },
        }),
        BundleRecord::Event(EventRecord {
            virtual_time_ns: 75,
            host_id: Some("a".into()),
            kind_tag: "engine".into(),
            event: EventPayload::CacheStateChange {
                from: "a".into(),
                to: "b".into(),
                transition: CacheTransition::Warmed,
            },
        }),
        BundleRecord::Snapshot(SnapshotRecord {
            virtual_time_ns: 200,
            host_id: "a".into(),
            kind_tag: "swim".into(),
            snapshot: br#"{"members":[{"id":"a","state":"alive"}]}"#.to_vec(),
        }),
        BundleRecord::Snapshot(SnapshotRecord {
            virtual_time_ns: 300,
            host_id: "a".into(),
            kind_tag: "swim".into(),
            snapshot: br#"{"members":[{"id":"a","state":"alive"},{"id":"b","state":"alive"}]}"#
                .to_vec(),
        }),
        BundleRecord::Snapshot(SnapshotRecord {
            virtual_time_ns: 200,
            host_id: "b".into(),
            kind_tag: "swim".into(),
            snapshot: br#"{"members":[{"id":"b","state":"alive"}]}"#.to_vec(),
        }),
        BundleRecord::Mutation(MutationRecord {
            virtual_time_ns: 150,
            mutation: Mutation {
                at_ns: 150,
                kind: MutationKind::Partition {
                    peers_a: vec!["a".into()],
                    peers_b: vec!["b".into()],
                },
            },
        }),
    ]
}

fn write_bundle(tmp: &Path, records: &[BundleRecord]) -> PathBuf {
    let out = tmp.join("bundle");
    let mut w = FileBundleWriter::new(&out, small_scenario());
    for r in records {
        w.write(r.clone());
    }
    w.finalize().expect("finalize");
    out
}

// ──────────────────────────────────────────────────────────────────────
// §9.6 Layout
// ──────────────────────────────────────────────────────────────────────

#[test]
fn every_produced_bundle_has_the_9_1_entries() {
    let tmp = TempDir::new().unwrap();
    let out = write_bundle(tmp.path(), &sample_records());

    assert!(out.join("manifest.json").is_file());
    assert!(out.join("scenario.toml").is_file());
    assert!(out.join("events.ndjson").is_file());
    assert!(out.join("snapshots").is_dir());
    // verdicts.json is the assertion evaluator's responsibility per
    // §10.5; the writer must not pre-create it.
    assert!(!out.join("verdicts.json").exists());
    // Per §9.1 snapshots/<host>/<seq>.json:
    assert!(out.join("snapshots/a/0.json").is_file());
    assert!(out.join("snapshots/a/1.json").is_file());
    assert!(out.join("snapshots/b/0.json").is_file());
}

// ──────────────────────────────────────────────────────────────────────
// §9.6 Envelope conformance
// ──────────────────────────────────────────────────────────────────────

#[test]
fn every_events_ndjson_line_is_valid_json_with_envelope_shape() {
    let tmp = TempDir::new().unwrap();
    let out = write_bundle(tmp.path(), &sample_records());
    let txt = fs::read_to_string(out.join("events.ndjson")).unwrap();
    let mut lines = 0;
    for line in txt.lines() {
        let v: serde_json::Value =
            serde_json::from_str(line).expect("each line must be valid JSON");
        let obj = v.as_object().expect("envelope must be an object");
        for key in ["virtual_time_ns", "host_id", "kind_tag", "event"] {
            assert!(obj.contains_key(key), "envelope missing {key}: {line}");
        }
        assert!(obj["virtual_time_ns"].is_u64());
        assert!(obj["kind_tag"].is_string());
        // host_id is string-or-null
        assert!(obj["host_id"].is_string() || obj["host_id"].is_null());
        lines += 1;
    }
    assert!(lines >= 4, "expected ≥4 event lines, got {lines}");
}

// ──────────────────────────────────────────────────────────────────────
// §9.6 Hash integrity
// ──────────────────────────────────────────────────────────────────────

#[test]
fn every_manifest_hash_matches_the_file_it_names() {
    use sha2::{Digest, Sha256};

    let tmp = TempDir::new().unwrap();
    let out = write_bundle(tmp.path(), &sample_records());
    let manifest: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();

    let scenario_hash = manifest["scenario_sha256"].as_str().unwrap();
    let actual_scenario = hex_sha256_of(&fs::read(out.join("scenario.toml")).unwrap());
    assert_eq!(scenario_hash, actual_scenario);

    let events_hash = manifest["events_ndjson_sha256"].as_str().unwrap();
    let actual_events = hex_sha256_of(&fs::read(out.join("events.ndjson")).unwrap());
    assert_eq!(events_hash, actual_events);

    let snap_hashes = manifest["snapshot_sha256"].as_object().unwrap();
    for (rel, claimed) in snap_hashes {
        let claimed = claimed.as_str().unwrap();
        let actual = hex_sha256_of(&fs::read(out.join(rel)).unwrap());
        assert_eq!(claimed, actual, "snapshot {rel} hash mismatch");
    }

    fn hex_sha256_of(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }
}

// ──────────────────────────────────────────────────────────────────────
// §9.6 Snapshot organization
// ──────────────────────────────────────────────────────────────────────

#[test]
fn snapshots_are_one_file_per_record_with_monotonic_seq_per_host() {
    let tmp = TempDir::new().unwrap();
    let records = sample_records();
    let snap_count_a = records
        .iter()
        .filter(|r| matches!(r, BundleRecord::Snapshot(s) if s.host_id == "a"))
        .count();
    let snap_count_b = records
        .iter()
        .filter(|r| matches!(r, BundleRecord::Snapshot(s) if s.host_id == "b"))
        .count();

    let out = write_bundle(tmp.path(), &records);

    for host in ["a", "b"] {
        let dir = out.join("snapshots").join(host);
        let mut names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        let expected = if host == "a" { snap_count_a } else { snap_count_b };
        assert_eq!(names.len(), expected, "host {host} file count");
        let expected_names: Vec<String> = (0..expected).map(|i| format!("{i}.json")).collect();
        assert_eq!(names, expected_names);
    }
}

// ──────────────────────────────────────────────────────────────────────
// §9.6 Ordering is deterministic + arrival-order independence
// ──────────────────────────────────────────────────────────────────────

#[test]
fn identical_record_streams_produce_byte_identical_bundles() {
    let tmp = TempDir::new().unwrap();
    let recs = sample_records();
    let a = write_bundle(&tmp.path().join("a"), &recs);
    let b = write_bundle(&tmp.path().join("b"), &recs);
    assert_eq!(bundle_signature(&a), bundle_signature(&b));
}

#[test]
fn arrival_order_does_not_affect_produced_bundle() {
    let tmp = TempDir::new().unwrap();
    let r1 = sample_records();
    let mut r2 = r1.clone();
    // Shuffle deterministically by reversing.
    r2.reverse();

    let a = write_bundle(&tmp.path().join("a"), &r1);
    let b = write_bundle(&tmp.path().join("b"), &r2);
    assert_eq!(bundle_signature(&a), bundle_signature(&b));
}

#[test]
fn writing_same_stream_twice_to_a_fresh_path_is_byte_identical() {
    let tmp = TempDir::new().unwrap();
    let recs = sample_records();
    let a = write_bundle(&tmp.path().join("a"), &recs);
    let b = write_bundle(&tmp.path().join("b"), &recs);

    // Compare every regular file under each tree.
    let a_files = collect_files(&a);
    let b_files = collect_files(&b);
    assert_eq!(a_files.keys().collect::<Vec<_>>(), b_files.keys().collect::<Vec<_>>());
    for (rel, a_bytes) in &a_files {
        if rel == "manifest.json" {
            // The manifest may include the architecture string; that's
            // identical across two runs on the same machine.
        }
        assert_eq!(a_bytes, &b_files[rel], "file diff at {rel}");
    }
}

// ──────────────────────────────────────────────────────────────────────
// Engine-synth events render with structured payloads
// ──────────────────────────────────────────────────────────────────────

#[test]
fn engine_synth_events_serialise_with_kind_discriminator() {
    let tmp = TempDir::new().unwrap();
    let recs = vec![
        BundleRecord::Event(EventRecord {
            virtual_time_ns: 1,
            host_id: Some("a".into()),
            kind_tag: "engine".into(),
            event: EventPayload::DropOnDelivery {
                to: "b".into(),
                reason: DeliveryDropReason::HostKilled,
            },
        }),
        BundleRecord::Event(EventRecord {
            virtual_time_ns: 2,
            host_id: Some("a".into()),
            kind_tag: "engine".into(),
            event: EventPayload::DialOutcome {
                from: "a".into(),
                to: "b".into(),
                warm: true,
            },
        }),
    ];
    let out = write_bundle(tmp.path(), &recs);
    let txt = fs::read_to_string(out.join("events.ndjson")).unwrap();
    assert!(txt.contains("\"drop_on_delivery\""));
    assert!(txt.contains("\"host_killed\""));
    assert!(txt.contains("\"dial_outcome\""));
    assert!(txt.contains("\"warm\":true"));
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

/// Returns a relative-path → sha256 map for every regular file under
/// `root`. Used to compare two bundle trees content-by-content without
/// caring about path order in a vec.
fn bundle_signature(root: &Path) -> BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let files = collect_files(root);
    files
        .into_iter()
        .map(|(rel, bytes)| {
            let mut h = Sha256::new();
            h.update(&bytes);
            (rel, h.finalize().iter().map(|b| format!("{b:02x}")).collect())
        })
        .collect()
}

fn collect_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    fn walk(root: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            let rel = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let ft = entry.file_type().unwrap();
            if ft.is_dir() {
                walk(&entry.path(), &rel, out);
            } else if ft.is_file() {
                out.insert(rel, fs::read(entry.path()).unwrap());
            }
        }
    }
    walk(root, "", &mut out);
    out
}
