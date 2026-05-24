//! On-disk bundle layout (SIM_SPEC §9).
//!
//! A `FileBundleWriter` collects records as the engine emits them, then
//! `finalize` flushes the §9.1 directory layout: `manifest.json`,
//! `scenario.toml`, `events.ndjson`, `snapshots/<host>/<seq>.json`.
//! `verdicts.json` is the assertion evaluator's job and is not written
//! here.
//!
//! The writer is *order-independent*: the same multiset of records
//! produces the same bundle no matter the arrival order, because every
//! step that writes to disk sorts records into a content-defined order
//! first.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::bundle::{
    BundleRecord, BundleWriter, DeliveryDropReason, EventPayload, EventRecord, MutationRecord,
    SnapshotRecord,
};
use crate::network::{CacheTransition, DropReason};
use crate::scenario::{Scenario, to_toml};

/// The version string the manifest records under `simulator_version`.
/// The MVP keeps this constant so the cross-architecture parity test
/// (§7.6) does not have to embed a moving target. Bumping it is a
/// deliberate spec amendment.
pub const SIMULATOR_VERSION: &str = "0.1.0-mvp";

/// The on-disk file writer. Construct with the output directory and the
/// scenario it will record; feed it records; call `finalize()` once.
pub struct FileBundleWriter {
    out_dir: PathBuf,
    scenario: Scenario,
    buf: Vec<BundleRecord>,
}

impl FileBundleWriter {
    pub fn new(out_dir: impl Into<PathBuf>, scenario: Scenario) -> Self {
        Self {
            out_dir: out_dir.into(),
            scenario,
            buf: Vec::new(),
        }
    }

    /// Flush the buffer to `out_dir` as a §9.1 bundle.
    pub fn finalize(self) -> std::io::Result<()> {
        let out = &self.out_dir;
        fs::create_dir_all(out)?;

        // 1. scenario.toml — exact echo (round-trippable per §8.4).
        let scenario_text = to_toml(&self.scenario);
        write_file(&out.join("scenario.toml"), scenario_text.as_bytes())?;

        // 2. events.ndjson — sort events + mutations by
        //    (virtual_time_ns, canonical-line-bytes) and emit one
        //    line per record. Snapshots go to disk in their own
        //    directory and do not appear here.
        let mut event_lines: Vec<(u64, String)> = Vec::new();
        let mut snapshots: BTreeMap<String, Vec<SnapshotRecord>> = BTreeMap::new();

        for rec in &self.buf {
            match rec {
                BundleRecord::Event(e) => {
                    event_lines.push((e.virtual_time_ns, render_event_line(e)));
                }
                BundleRecord::Mutation(m) => {
                    event_lines.push((m.virtual_time_ns, render_mutation_line(m)));
                }
                BundleRecord::Snapshot(s) => {
                    snapshots.entry(s.host_id.clone()).or_default().push(s.clone());
                }
            }
        }
        // Stable secondary sort key = the rendered line itself, so
        // the order is purely content-defined.
        event_lines.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let events_path = out.join("events.ndjson");
        {
            let mut f = fs::File::create(&events_path)?;
            for (_t, line) in &event_lines {
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")?;
            }
        }
        let events_hash = hex_sha256_of_file(&events_path)?;

        // 3. snapshots/<host>/<seq>.json — group per host, sort by
        //    virtual_time_ns, assign seq 0,1,2,…
        let mut snapshot_hashes: BTreeMap<String, String> = BTreeMap::new();
        for (host_id, mut bucket) in snapshots {
            bucket.sort_by(|a, b| {
                a.virtual_time_ns
                    .cmp(&b.virtual_time_ns)
                    .then_with(|| a.snapshot.cmp(&b.snapshot))
            });
            let host_dir = out.join("snapshots").join(&host_id);
            fs::create_dir_all(&host_dir)?;
            for (seq, snap) in bucket.iter().enumerate() {
                let path = host_dir.join(format!("{seq}.json"));
                let line = render_snapshot(snap);
                write_file(&path, line.as_bytes())?;
                let rel = format!("snapshots/{}/{}.json", host_id, seq);
                snapshot_hashes.insert(rel, hex_sha256_of_file(&path)?);
            }
        }

        // 4. manifest.json — §9.4 fields plus declared time unit.
        let scenario_hash = hex_sha256_of_file(&out.join("scenario.toml"))?;
        let manifest = Manifest {
            simulator_version: SIMULATOR_VERSION.into(),
            scenario_path: "scenario.toml".into(),
            scenario_sha256: scenario_hash,
            seed: self.scenario.seed,
            duration_ns: self.scenario.duration_ns,
            time_unit: "ns".into(),
            host_architecture: std::env::consts::ARCH.into(),
            events_ndjson_sha256: events_hash,
            snapshot_sha256: snapshot_hashes,
        };
        let manifest_text = serde_json::to_string_pretty(&manifest)
            .expect("Manifest serialises to JSON by construction");
        write_file(&out.join("manifest.json"), manifest_text.as_bytes())?;
        Ok(())
    }
}

impl BundleWriter for FileBundleWriter {
    fn write(&mut self, record: BundleRecord) {
        self.buf.push(record);
    }
}

// ──────────────────────────────────────────────────────────────────────
// Manifest
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct Manifest {
    simulator_version: String,
    scenario_path: String,
    scenario_sha256: String,
    seed: u64,
    duration_ns: u64,
    time_unit: String,
    host_architecture: String,
    events_ndjson_sha256: String,
    /// Relative path → hex SHA-256. BTreeMap so iteration order is
    /// determined by the path, not insertion order.
    snapshot_sha256: BTreeMap<String, String>,
}

// ──────────────────────────────────────────────────────────────────────
// Rendering
// ──────────────────────────────────────────────────────────────────────

fn render_event_line(e: &EventRecord) -> String {
    let envelope = serde_json::json!({
        "virtual_time_ns": e.virtual_time_ns,
        "host_id": e.host_id,
        "kind_tag": e.kind_tag,
        "event": render_event_payload(&e.event),
    });
    serde_json::to_string(&envelope).expect("envelope serialises")
}

fn render_event_payload(p: &EventPayload) -> serde_json::Value {
    match p {
        EventPayload::Bytes(b) => match serde_json::from_slice::<serde_json::Value>(b) {
            Ok(v) => v,
            // Host emitted non-JSON bytes. Wrap them so the envelope
            // is still valid JSON without losing information.
            Err(_) => serde_json::json!({
                "kind": "raw",
                "len": b.len(),
                "hex": hex_of(b),
            }),
        },
        EventPayload::DropOnSend { from, to, reason } => serde_json::json!({
            "kind": "drop_on_send",
            "from": from,
            "to": to,
            "reason": drop_reason_str(reason),
        }),
        EventPayload::DropOnDelivery { to, reason } => serde_json::json!({
            "kind": "drop_on_delivery",
            "to": to,
            "reason": delivery_drop_reason_str(reason),
        }),
        EventPayload::CacheStateChange { from, to, transition } => serde_json::json!({
            "kind": "cache_state_change",
            "from": from,
            "to": to,
            "transition": cache_transition_str(transition),
        }),
        EventPayload::DialStart { from, to } => serde_json::json!({
            "kind": "dial_start",
            "from": from,
            "to": to,
        }),
        EventPayload::DialOutcome { from, to, warm } => serde_json::json!({
            "kind": "dial_outcome",
            "from": from,
            "to": to,
            "warm": warm,
        }),
    }
}

fn render_mutation_line(m: &MutationRecord) -> String {
    let mutation_json =
        serde_json::to_value(&m.mutation).expect("Mutation serialises to JSON by construction");
    let envelope = serde_json::json!({
        "virtual_time_ns": m.virtual_time_ns,
        "host_id": serde_json::Value::Null,
        "kind_tag": "mutation",
        "event": mutation_json,
    });
    serde_json::to_string(&envelope).expect("envelope serialises")
}

fn render_snapshot(s: &SnapshotRecord) -> String {
    let payload = serde_json::from_slice::<serde_json::Value>(&s.snapshot).unwrap_or_else(|_| {
        serde_json::json!({
            "kind": "raw",
            "len": s.snapshot.len(),
            "hex": hex_of(&s.snapshot),
        })
    });
    let envelope = serde_json::json!({
        "virtual_time_ns": s.virtual_time_ns,
        "host_id": s.host_id,
        "kind_tag": s.kind_tag,
        "snapshot": payload,
    });
    serde_json::to_string_pretty(&envelope).expect("snapshot envelope serialises")
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

fn write_file(path: &Path, content: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = fs::File::create(path)?;
    f.write_all(content)?;
    Ok(())
}

fn hex_sha256_of_file(path: &Path) -> std::io::Result<String> {
    let bytes = fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hex_of(&hasher.finalize()))
}

fn hex_of(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn drop_reason_str(r: &DropReason) -> &'static str {
    match r {
        DropReason::NoRoute => "no_route",
        DropReason::Partitioned => "partitioned",
        DropReason::Lossy => "lossy",
    }
}

fn delivery_drop_reason_str(r: &DeliveryDropReason) -> &'static str {
    match r {
        DeliveryDropReason::HostHalted => "host_halted",
        DeliveryDropReason::HostKilled => "host_killed",
        DeliveryDropReason::Partition => "partition",
    }
}

fn cache_transition_str(t: &CacheTransition) -> &'static str {
    match t {
        CacheTransition::Warmed => "warmed",
        CacheTransition::IdleCooled => "idle_cooled",
        CacheTransition::Invalidated => "invalidated",
    }
}
