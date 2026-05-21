//! Bundle post-processor — the read path shared between the prod
//! collector pipeline and the sim's replay loader (SPEC §8.1).
//!
//! The post-processor takes a bundle on disk, walks every envelope,
//! and reports any record kind / field it does not recognise. The
//! parity-bar's §7.1 / §7.2 checks bind to this: a sim bundle and
//! a prod bundle both yield a [`ParseReport`] with `status == "ok"`
//! and no `unknown_*` entries.
//!
//! v1 deliberately keeps the recogniser permissive: any field whose
//! name matches a documented prefix (OBSERVABILITY §3 envelopes, the
//! corpus shape) is accepted. Unknown record kinds still flag — that
//! is the parity floor. Cross-record correlation (events ↔
//! snapshots, identity ↔ peer refs) is parked on the v2 calibration
//! loop and is not part of the parity-bar.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::sim_backend::bundle as fs_io;

/// Outcome of a single bundle parse.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ParseReport {
    pub status: String,
    pub errors: Vec<String>,
    pub unknown_fields: Vec<String>,
    pub unknown_record_kinds: Vec<String>,
}

/// Walk a bundle on disk and report any record kinds or fields the
/// recogniser does not know. Used by both the prod collector
/// pipeline (`prod_parser_reads_sim_bundle`) and the sim's replay
/// loader (`sim_replay_parser_reads_prod_bundle`) — same code path.
pub fn parse_bundle(bundle_root: &Path) -> ParseReport {
    let mut report = ParseReport {
        status: "ok".to_string(),
        ..Default::default()
    };

    if !bundle_root.is_dir() {
        report.status = "error".into();
        report.errors.push(format!(
            "bundle root is not a directory: {}",
            bundle_root.display()
        ));
        return report;
    }

    // MANIFEST is the bundle's anchor; if it's missing or malformed
    // we still walk the rest of the tree but record an explicit
    // error.
    match read_json(&bundle_root.join("MANIFEST.json")) {
        Ok(manifest) => audit_envelope(&manifest, "MANIFEST", &mut report),
        Err(err) => {
            report
                .errors
                .push(format!("MANIFEST.json unreadable: {err}"));
            report.status = "error".into();
        }
    }

    let node_dirs = match enumerate_node_dirs(bundle_root) {
        Ok(d) => d,
        Err(err) => {
            report
                .errors
                .push(format!("cannot enumerate node dirs: {err}"));
            report.status = "error".into();
            return report;
        }
    };

    for node_dir in node_dirs {
        for special in ["boot.json", "finalize.json"] {
            for path in match_files_with_basename_stem(&node_dir, special) {
                if let Ok(value) = read_json(&path) {
                    audit_envelope(&value, special, &mut report);
                }
            }
        }
        audit_sub_dir(&node_dir.join("snapshots"), "snapshot", &mut report);
        audit_sub_dir(&node_dir.join("events"), "events", &mut report);
    }

    report.unknown_fields.sort();
    report.unknown_fields.dedup();
    report.unknown_record_kinds.sort();
    report.unknown_record_kinds.dedup();

    if !report.unknown_record_kinds.is_empty() || !report.unknown_fields.is_empty() {
        // unknown_* are warnings, not errors — `status` stays "ok"
        // unless an actual structural problem (missing MANIFEST,
        // unparsable JSON) was hit above. The parity bar reads the
        // unknown_* lists directly.
    }

    report
}

fn enumerate_node_dirs(bundle_root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for sub in fs_io::list_subdirs(bundle_root)? {
        if sub == "sim" {
            continue;
        }
        out.push(bundle_root.join(sub));
    }
    Ok(out)
}

fn match_files_with_basename_stem(dir: &Path, basename: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let main = dir.join(basename);
    if main.is_file() {
        out.push(main);
    }
    let stem = basename.trim_end_matches(".json");
    if let Ok(paths) = fs_io::list_files_with_prefix(dir, stem) {
        for p in paths {
            if p.file_name().and_then(|s| s.to_str()) == Some(basename) {
                continue;
            }
            out.push(p);
        }
    }
    out
}

fn audit_sub_dir(dir: &Path, kind_hint: &str, report: &mut ParseReport) {
    if !dir.is_dir() {
        return;
    }
    let paths = match fs_io::list_files_with_prefix(dir, "") {
        Ok(p) => p,
        Err(err) => {
            report
                .errors
                .push(format!("cannot list {}: {err}", dir.display()));
            return;
        }
    };
    for path in paths {
        match read_json(&path) {
            Ok(value) => audit_envelope(&value, kind_hint, report),
            Err(err) => {
                report
                    .errors
                    .push(format!("unparseable {}: {err}", path.display()));
                report.status = "error".into();
            }
        }
    }
}

fn audit_envelope(value: &serde_json::Value, kind_hint: &str, report: &mut ParseReport) {
    let kind = value
        .get("kind")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| kind_hint.to_string());
    if !is_known_record_kind(&kind) {
        report.unknown_record_kinds.push(kind.clone());
    }
    if let serde_json::Value::Object(map) = value {
        for key in map.keys() {
            if !is_known_envelope_field(key) {
                report.unknown_fields.push(key.clone());
            }
        }
        // Drill into the events `records` array to audit per-record
        // variants and per-record fields.
        if let Some(arr) = value.get("records").and_then(|v| v.as_array()) {
            for rec in arr {
                if let Some(variant) = rec.get("variant").and_then(|v| v.as_str()) {
                    if !is_known_event_variant(variant) {
                        report
                            .unknown_record_kinds
                            .push(format!("event::{variant}"));
                    }
                }
                if let serde_json::Value::Object(rec_map) = rec {
                    for key in rec_map.keys() {
                        if !is_known_event_field(key) {
                            report.unknown_fields.push(key.clone());
                        }
                    }
                }
            }
        }
    }
}

fn is_known_record_kind(kind: &str) -> bool {
    KNOWN_RECORD_KINDS.contains(&kind)
}

fn is_known_event_variant(variant: &str) -> bool {
    KNOWN_EVENT_VARIANTS.contains(&variant)
}

fn is_known_envelope_field(field: &str) -> bool {
    KNOWN_ENVELOPE_FIELDS.contains(&field)
}

fn is_known_event_field(field: &str) -> bool {
    KNOWN_EVENT_FIELDS.contains(&field)
}

fn read_json(path: &Path) -> std::io::Result<serde_json::Value> {
    let text = fs_io::read_text(path)?;
    serde_json::from_str(&text).map_err(std::io::Error::other)
}

const KNOWN_RECORD_KINDS: &[&str] = &[
    // Sim envelope kinds.
    "manifest",
    "boot",
    "events",
    "snapshot",
    "finalize",
    // Corpus envelope kinds. The corpus MANIFEST.json does not carry
    // a `kind` field; `MANIFEST` is the kind_hint passed when its
    // top-level is audited.
    "MANIFEST",
    "MANIFEST.json",
    "boot.json",
    "finalize.json",
];

const KNOWN_EVENT_VARIANTS: &[&str] = &[
    "Custom",
    "Error",
    "DialStarted",
    "DialOutcome",
    "ConnectionCacheMiss",
    "ConnectionCacheHit",
    "ConnectionCacheInvalidated",
    "MessageSent",
    "MessageReceived",
    "IrohConnTypeChanged",
    "SwimTransition",
    "SwimMetadataSent",
    "SwimMetadataReceived",
    "NodeMapUpdate",
    "RelayChanged",
    "ProbeSent",
    "ProbeReceived",
];

const KNOWN_ENVELOPE_FIELDS: &[&str] = &[
    // Envelope-level fields (sim + corpus union).
    "schema_version",
    "kind",
    "run_id",
    "boot_sequence",
    "wall_ms",
    "monotonic_seq",
    "snapshot_id",
    "trigger",
    "body",
    "records",
    "node_id_hex",
    "identity",
    "shutdown_reason",
    "stage_index",
    "events_emitted",
    "snapshots_emitted",
    // MANIFEST-only fields.
    "captured_branch",
    "captured_at_unix_ms",
    "duration_ms",
    "nodes",
    "collector",
    "files",
    "node_count",
    "mutation_count",
    "origin",
];

const KNOWN_EVENT_FIELDS: &[&str] = &[
    "variant",
    "user_kind",
    "wall_ms",
    "monotonic_seq",
    "schema_version",
    "fields",
    "peer",
    "attempt",
    "timeout_ms",
    "outcome",
    "duration_ms",
    "generation",
    "reason",
    "kind",
    "size",
    "old",
    "new",
    "from",
    "to",
    "version",
    "payload_hash",
    "from_source",
    "accepted",
    "old_url",
    "new_url",
    "target",
    "rtt_ms",
    "component",
    "message",
];
