//! Bundle-layout test: the independent vastai layer and the swactor layer share
//! one run bundle without colliding.
//!
//! A run that has both a swactor stage node and a vastai in-VM node must tar into
//! a tarball where the swactor records keep their `stage-0/events|snapshots`
//! layout and the vastai records land under a sibling `vastai-stage-0/vastai/`
//! directory — proving the two layers coexist and that vastai records are bundled
//! (not silently dropped) while staying out of swactor's accounting.

#![cfg(feature = "collector")]

use std::io::Read;
use std::path::PathBuf;

use distribution::diagnostics::collector::bundle;
use distribution::diagnostics::collector::{CollectorState, Manifest, RecordKind};
use flate2::read::GzDecoder;
use serde_json::json;

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "vastai-bundle-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entry_paths(tar_gz: &[u8]) -> Vec<String> {
    let dec = GzDecoder::new(tar_gz);
    let mut ar = tar::Archive::new(dec);
    ar.entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.path().ok().map(|p| p.to_string_lossy().into_owned()))
        .collect()
}

#[test]
fn swactor_and_vastai_records_coexist_in_one_bundle() {
    let store = TempDir::new();
    let state = CollectorState::new(store.path());
    let run = "mixed-run";

    // A swactor stage node: boot (identity) + one events batch.
    let swactor_node = "a".repeat(64);
    state
        .persist(
            run,
            &swactor_node,
            RecordKind::Boot,
            1,
            &json!({"role": "stage", "stage_index": 0}),
        )
        .unwrap();
    state
        .persist(run, &swactor_node, RecordKind::Events, 2, &json!([]))
        .unwrap();

    // The vastai in-VM node for the same stage: a sample and a log batch.
    let vastai_node = "vastai-stage-0";
    state
        .persist(
            run,
            vastai_node,
            RecordKind::VastaiSample,
            3,
            &json!({"v": 1, "body": {"t": "host_sample"}}),
        )
        .unwrap();
    state
        .persist(
            run,
            vastai_node,
            RecordKind::VastaiLogs,
            4,
            &json!({"v": 1, "body": {"t": "logs"}}),
        )
        .unwrap();

    let bytes = bundle::assemble_bytes(&state, run).expect("assemble");
    let paths = entry_paths(&bytes);

    // The swactor node keeps its kind-named layout.
    assert!(
        paths.iter().any(|p| p.contains("/stage-0/events/")),
        "swactor stage events present, got {paths:?}"
    );
    // The vastai node lands under its own clean dir, bucketed under vastai/.
    assert!(
        paths.iter().any(|p| p.contains("/vastai-stage-0/vastai/")),
        "vastai records bundled under vastai-stage-0/vastai/, got {paths:?}"
    );

    // The manifest reports both nodes; the vastai node's records are counted as
    // vastai, not as swactor event batches.
    let manifest_raw = {
        let dec = GzDecoder::new(&bytes[..]);
        let mut ar = tar::Archive::new(dec);
        let mut found = None;
        for e in ar.entries().unwrap() {
            let mut e = e.unwrap();
            if e.path().unwrap().to_string_lossy().ends_with("MANIFEST.json") {
                let mut s = String::new();
                e.read_to_string(&mut s).unwrap();
                found = Some(s);
                break;
            }
        }
        found.expect("manifest present")
    };
    let manifest: Manifest = serde_json::from_str(&manifest_raw).unwrap();
    let vastai = manifest
        .nodes
        .iter()
        .find(|n| n.label == "vastai-stage-0")
        .expect("vastai node in manifest");
    assert_eq!(vastai.vastai_records, 2);
    assert_eq!(vastai.event_batches, 0);

    let swactor = manifest
        .nodes
        .iter()
        .find(|n| n.label == "stage-0")
        .expect("swactor node in manifest");
    assert_eq!(swactor.event_batches, 1);
    assert_eq!(swactor.vastai_records, 0);
}

#[test]
fn vastai_only_run_bundles_cleanly() {
    // Independence: a run with no swactor records at all still bundles, with the
    // vastai node present. (The reverse — swactor-only — is the pre-existing
    // collector behaviour, already covered by t_diag_collector.)
    let store = TempDir::new();
    let state = CollectorState::new(store.path());
    let run = "vastai-only";
    state
        .persist(
            run,
            "vastai-external",
            RecordKind::VastaiInstance,
            1,
            &json!({"v": 1, "body": {"t": "instance"}}),
        )
        .unwrap();

    let bytes = bundle::assemble_bytes(&state, run).expect("assemble");
    let paths = entry_paths(&bytes);
    assert!(
        paths.iter().any(|p| p.contains("/vastai-external/vastai/")),
        "vastai-only run bundles the external node, got {paths:?}"
    );
}
