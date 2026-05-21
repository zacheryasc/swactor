//! Bundle writer — translates [`RunRecords`] into the on-disk layout
//! described in SPEC §6.4.
//!
//! Lives under `src/facade/sim/` because it is the only part of the
//! simulation crate that calls `std::fs` directly: the lint-determinstic
//! allowlist (TESTING_SPEC §4.1) names this subtree as the facade-impl
//! boundary. The engine itself never touches the file system; it hands
//! a `RunRecords` to this module which serialises everything in a
//! sorted, deterministic order.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use serde_json::json;

use crate::engine::{
    BootRecord, EventRecord, FinalizeRecord, RunRecords, SnapshotRecord, SCHEMA_VERSION,
};
use crate::spec::{validate_path_component, ParsedSpec};
use crate::SimError;

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn write_run(spec: &ParsedSpec, seed: u64, records: &RunRecords) -> Result<PathBuf, SimError> {
    let root = allocate_run_dir(&spec.run_id)?;
    write_bundle(&root, spec, seed, records).map_err(SimError::Io)?;
    Ok(root)
}

fn allocate_run_dir(run_id: &str) -> Result<PathBuf, SimError> {
    validate_path_component(run_id, "run_id")?;
    let pid = std::process::id();
    let n = RUN_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    let base = env::temp_dir().join("simulation-runs");
    fs::create_dir_all(&base).map_err(SimError::Io)?;
    let dir = base.join(format!("{run_id}-{pid:x}-{n:08x}"));
    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(SimError::Io)?;
    }
    fs::create_dir_all(&dir).map_err(SimError::Io)?;
    Ok(dir)
}

fn write_bundle(
    root: &Path,
    spec: &ParsedSpec,
    seed: u64,
    records: &RunRecords,
) -> io::Result<()> {
    write_manifest(root, &records.summary)?;
    for (node, boots) in &records.boots {
        validate_path_component(node, "host name")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let node_dir = root.join(node);
        fs::create_dir_all(&node_dir)?;
        fs::create_dir_all(node_dir.join("events"))?;
        fs::create_dir_all(node_dir.join("snapshots"))?;
        write_boot_files(&node_dir, boots)?;
        if let Some(finalizes) = records.finalizes.get(node) {
            write_finalize_files(&node_dir, finalizes)?;
        }
        let empty_events: Vec<EventRecord> = Vec::new();
        let events = records.custom_events.get(node).unwrap_or(&empty_events);
        write_events(&node_dir, events)?;
        let empty_snaps: Vec<SnapshotRecord> = Vec::new();
        let snapshots = records.snapshots.get(node).unwrap_or(&empty_snaps);
        write_snapshots(&node_dir, snapshots)?;
    }

    // detector/verdicts.json — present iff the spec declared a host
    // named `detector` (TESTING_SPEC §10.2). The detector peer runs the
    // closed D01–D12 catalogue from `sim-detector` and writes the
    // outcomes here for the parity-bar to assert no DetectedSim.
    if spec.hosts.iter().any(|h| h.name == "detector") {
        let detector_dir = root.join("detector");
        fs::create_dir_all(&detector_dir)?;
        let verdicts = crate::detector::run_all();
        let mut text = crate::detector::verdicts_to_json(&verdicts);
        text.push('\n');
        fs::write(detector_dir.join("verdicts.json"), text.as_bytes())?;
    }

    // sim/ subtree — sim-only fields, excluded from parity comparison.
    let sim_dir = root.join("sim");
    fs::create_dir_all(&sim_dir)?;
    fs::write(sim_dir.join("seed"), format!("{seed}\n").as_bytes())?;
    // sim/spec.toml carries the original spec text byte-for-byte so a
    // self-replay (which is sim/spec.toml round-trip) produces a
    // byte-identical bundle (TESTING_SPEC §3.1).
    fs::write(sim_dir.join("spec.toml"), spec.source_text.as_bytes())?;
    write_json(&sim_dir.join("links_applied.json"), &links_applied_json(records))?;
    write_mutations_log(&sim_dir.join("mutations.log"), records)?;

    Ok(())
}

fn write_manifest(root: &Path, summary: &crate::engine::RunSummary) -> io::Result<()> {
    // MANIFEST.json carries only prod-recoverable fields. `seed` lives
    // under `sim/seed`; `link_count` is sim-only and lives nowhere in
    // the prod shape. Excluding them keeps a prod-shape replay
    // (TESTING_SPEC §3.3) byte-equal to the original on this file.
    let payload = json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "manifest",
        "run_id": summary.run_id,
        "duration_ms": summary.duration_ms,
        "node_count": summary.node_count,
        "mutation_count": summary.mutation_count,
        "origin": "sim",
    });
    write_json(&root.join("MANIFEST.json"), &payload)
}

fn write_boot_files(node_dir: &Path, boots: &[BootRecord]) -> io::Result<()> {
    if boots.is_empty() {
        return Ok(());
    }
    for (i, boot) in boots.iter().enumerate() {
        let name = if i == 0 {
            "boot.json".to_string()
        } else {
            format!("boot-{:03}.json", i)
        };
        write_json(&node_dir.join(&name), &boot_json(boot))?;
    }
    Ok(())
}

fn write_finalize_files(node_dir: &Path, finalizes: &[FinalizeRecord]) -> io::Result<()> {
    for (i, fin) in finalizes.iter().enumerate() {
        let name = if i == 0 {
            "finalize.json".to_string()
        } else {
            format!("finalize-{:03}.json", i)
        };
        write_json(&node_dir.join(&name), &finalize_json(fin))?;
    }
    Ok(())
}

fn write_events(node_dir: &Path, events: &[EventRecord]) -> io::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let mut buckets: BTreeMap<u32, Vec<&EventRecord>> = BTreeMap::new();
    for ev in events {
        buckets.entry(ev.boot_sequence).or_default().push(ev);
    }
    for (boot_seq, evs) in buckets {
        let envelope = json!({
            "schema_version": SCHEMA_VERSION,
            "kind": "events",
            "boot_sequence": boot_seq,
            "records": evs.iter().map(|e| event_record_json(e)).collect::<Vec<_>>(),
        });
        let filename = format!("{:06}.json", boot_seq);
        write_json(&node_dir.join("events").join(filename), &envelope)?;
    }
    Ok(())
}

fn write_snapshots(node_dir: &Path, snapshots: &[SnapshotRecord]) -> io::Result<()> {
    for (idx, snap) in snapshots.iter().enumerate() {
        let filename = format!("{:06}.json", idx);
        write_json(&node_dir.join("snapshots").join(filename), &snapshot_json(snap))?;
    }
    Ok(())
}

fn boot_json(boot: &BootRecord) -> serde_json::Value {
    json!({
        "schema_version": boot.schema_version,
        "kind": "boot",
        "stage_index": boot.stage_index,
        "boot_sequence": boot.boot_sequence,
        "wall_ms": boot.wall_ms,
        "monotonic_seq": boot.monotonic_seq,
        "run_id": boot.run_id,
    })
}

fn finalize_json(fin: &FinalizeRecord) -> serde_json::Value {
    json!({
        "schema_version": fin.schema_version,
        "kind": "finalize",
        "boot_sequence": fin.boot_sequence,
        "wall_ms": fin.wall_ms,
        "monotonic_seq": fin.monotonic_seq,
        "run_id": fin.run_id,
        "shutdown_reason": fin.shutdown_reason,
    })
}

fn snapshot_json(snap: &SnapshotRecord) -> serde_json::Value {
    // Snapshot body fields are deliberately sparse: every field that
    // the sim cannot populate is explicit `None` (paired with the
    // boot-time `host_introspect` Error event, see engine.rs). The
    // body shape mirrors OBSERVABILITY §3 envelope minus name-bearing
    // strings so rename-equivariance (TESTING_SPEC §9.1) holds.
    json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "snapshot_id": snap.snapshot_id,
        "boot_sequence": snap.boot_sequence,
        "wall_ms": snap.wall_ms,
        "monotonic_seq": snap.monotonic_seq,
        "trigger": snap.trigger,
        "body": {
            "process": {
                "rss_bytes": null,
                "vm_size_bytes": null,
                "open_fd_count": null,
                "cpu_ms": null,
                "captured_at_ms": snap.wall_ms,
            },
            "host": {
                "conntrack_count": null,
                "ipv6_enabled": false,
                "refreshed_at_ms": snap.wall_ms,
            }
        }
    })
}

fn event_record_json(ev: &EventRecord) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("variant".into(), json!(ev.variant));
    if let Some(kind) = &ev.user_kind {
        obj.insert("user_kind".into(), json!(kind));
    }
    obj.insert("wall_ms".into(), json!(ev.wall_ms));
    obj.insert("monotonic_seq".into(), json!(ev.monotonic_seq));
    if let serde_json::Value::Object(map) = &ev.fields {
        for (k, v) in map {
            obj.insert(k.clone(), v.clone());
        }
    } else if !ev.fields.is_null() {
        obj.insert("fields".into(), ev.fields.clone());
    }
    serde_json::Value::Object(obj)
}

fn links_applied_json(records: &RunRecords) -> serde_json::Value {
    let arr: Vec<serde_json::Value> = records
        .links_applied
        .iter()
        .map(|l| {
            json!({
                "at_ms": l.at_ms,
                "a": l.a,
                "b": l.b,
                "bandwidth_bps": l.bandwidth_bps,
                "one_way_delay_ms": l.one_way_delay_ms,
                "jitter_ms": l.jitter_ms,
                "loss_ppm": l.loss_ppm,
            })
        })
        .collect();
    serde_json::Value::Array(arr)
}

fn write_mutations_log(path: &Path, records: &RunRecords) -> io::Result<()> {
    let mut buf = String::new();
    for entry in &records.mutations_log {
        let line = json!({
            "at_ms": entry.at_ms,
            "kind": entry.kind,
            "detail": entry.detail,
        });
        buf.push_str(&serde_json::to_string(&line).map_err(io::Error::other)?);
        buf.push('\n');
    }
    fs::write(path, buf.as_bytes())
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let mut text = serde_json::to_string_pretty(value).map_err(io::Error::other)?;
    text.push('\n');
    fs::write(path, text.as_bytes())
}

/// Read a UTF-8 text file. Used by the replay loader; centralised
/// here because it is one of the only spots that touches `std::fs`
/// directly and the lint-deterministic allowlist binds to this file.
pub fn read_text(path: &Path) -> io::Result<String> {
    fs::read_to_string(path)
}

/// List subdirectories of `path` (immediate children only) in
/// sorted order. Used by the prod-shape replay loader to enumerate
/// node directories without pulling raw `std::fs::read_dir` into
/// the engine module.
pub fn list_subdirs(path: &Path) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                out.push(name.to_string());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// List files in a directory whose name starts with `prefix` and ends
/// with `.json`, sorted by file name. Used by the replay loader to
/// enumerate per-host boot / finalize epoch files.
pub fn list_files_with_prefix(
    dir: &Path,
    prefix: &str,
) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(it) => it,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(prefix) && name.ends_with(".json") {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}
