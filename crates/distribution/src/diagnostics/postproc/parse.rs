//! Tarball reader for finalized diagnostics bundles.
//!
//! Walks every entry in `{run_id}/...` and classifies it by its path
//! suffix: `MANIFEST.json`, `boot.json`, `finalize.json`,
//! `snapshots/snapshot-NNNNNN.json`, `events/events-NNNNNN.json`.
//! Unknown entries are skipped — the schema can grow with additive
//! fields without breaking already-shipped post-processors.
//!
//! Parser intentionally does *not* enforce strict serde matches.
//! Snapshot bodies and event records evolve as tiers ship; the
//! renderers fall back gracefully when an expected field is missing.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;

use crate::diagnostics::event::EventRecord;
use crate::diagnostics::identity::Identity;
use crate::diagnostics::snapshot::Snapshot;

/// One parsed bundle, ready for the renderers.
#[derive(Debug)]
pub struct Bundle {
    /// Run id parsed from the bundle's tree (the single top-level
    /// directory). Mirrors `manifest.run_id`.
    pub run_id: String,
    /// `MANIFEST.json` payload. Absent only on malformed bundles; the
    /// parser returns an error rather than carry `None` here.
    pub manifest: PostprocManifest,
    /// Per-node data, keyed by the friendly label (`orchestrator`,
    /// `stage-0`, …) the collector wrote into the manifest.
    pub nodes: BTreeMap<String, NodeData>,
}

impl Bundle {
    /// Read and parse the tarball at `path`.
    pub fn parse_path(path: &Path) -> Result<Self, ParseError> {
        let mut f = File::open(path)
            .map_err(|e| ParseError::Io(format!("open {}: {e}", path.display())))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)
            .map_err(|e| ParseError::Io(format!("read {}: {e}", path.display())))?;
        Self::parse_bytes(&buf)
    }

    /// Read and parse a gzipped tar buffer.
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        let mut raw_entries: Vec<RawEntry> = Vec::new();
        let gz = GzDecoder::new(bytes);
        let mut ar = tar::Archive::new(gz);
        let entries = ar.entries().map_err(|e| ParseError::Tar(e.to_string()))?;
        for entry in entries {
            let mut entry = entry.map_err(|e| ParseError::Tar(e.to_string()))?;
            let path_str: String = entry
                .path()
                .map_err(|e| ParseError::Tar(e.to_string()))?
                .to_string_lossy()
                .into_owned();
            // Skip directory entries (no body anyway).
            if path_str.ends_with('/') {
                continue;
            }
            let mut body = Vec::new();
            entry
                .read_to_end(&mut body)
                .map_err(|e| ParseError::Tar(e.to_string()))?;
            raw_entries.push(RawEntry { path: path_str, body });
        }

        let run_id = detect_run_id(&raw_entries)?;
        let mut manifest: Option<PostprocManifest> = None;
        let mut node_bins: BTreeMap<String, NodeBin> = BTreeMap::new();

        for entry in &raw_entries {
            let Some(rel) = entry.path.strip_prefix(&format!("{run_id}/")) else {
                continue;
            };
            if rel == "MANIFEST.json" {
                manifest = Some(parse_json(rel, &entry.body)?);
                continue;
            }
            // Path is `{label}/...`.
            let Some((label, sub)) = rel.split_once('/') else {
                continue;
            };
            let bin = node_bins.entry(label.to_string()).or_default();
            classify(sub, &entry.body, bin)?;
        }

        let manifest = manifest.ok_or_else(|| {
            ParseError::Schema(format!("MANIFEST.json missing under {run_id}/"))
        })?;

        // Build the final `nodes` map. Anything in the manifest that
        // didn't ship records still appears as an empty NodeData so
        // the renderers can render "node went silent."
        let mut nodes: BTreeMap<String, NodeData> = BTreeMap::new();
        for node in &manifest.nodes {
            let bin = node_bins.remove(&node.label).unwrap_or_default();
            nodes.insert(
                node.label.clone(),
                NodeData {
                    label: node.label.clone(),
                    node_id_hex: node.node_id_hex.clone(),
                    role: node.role.clone(),
                    stage_index: node.stage_index,
                    identity: bin.identity,
                    snapshots: bin.snapshots,
                    events: bin.events,
                    finalize: bin.finalize,
                },
            );
        }
        // Pick up any orphan node directories that lack manifest
        // entries (defensive against truncated manifests).
        for (label, bin) in node_bins {
            nodes.insert(
                label.clone(),
                NodeData {
                    label: label.clone(),
                    node_id_hex: bin
                        .identity
                        .as_ref()
                        .map(|i| i.node_id_hex.clone())
                        .unwrap_or_default(),
                    role: None,
                    stage_index: None,
                    identity: bin.identity,
                    snapshots: bin.snapshots,
                    events: bin.events,
                    finalize: bin.finalize,
                },
            );
        }

        // Sort events and snapshots inside each node by (wall_ms,
        // monotonic_seq) so the renderers can rely on chronological
        // order without re-sorting.
        for node in nodes.values_mut() {
            node.events
                .sort_by_key(|e| (e.wall_ms, e.monotonic_seq));
            node.snapshots
                .sort_by_key(|s| (s.wall_ms, s.monotonic_seq));
        }

        Ok(Self {
            run_id,
            manifest,
            nodes,
        })
    }

    /// Labels in the manifest's order. Falls back to BTreeMap insertion
    /// order when the manifest is missing for a node.
    pub fn labels_in_order(&self) -> impl Iterator<Item = &str> {
        self.manifest.nodes.iter().map(|n| n.label.as_str())
    }

    /// Resolve a `NodeId` hex string to its bundle-side label. Returns
    /// the hex back if the node is unknown.
    pub fn label_for_hex(&self, hex: &str) -> String {
        for node in &self.manifest.nodes {
            if node.node_id_hex.eq_ignore_ascii_case(hex) {
                return node.label.clone();
            }
        }
        // Defensive fall-through: a node that produced events but
        // didn't make it into the manifest gets a `node-{short}` style
        // label for stable rendering.
        if hex.len() >= 8 {
            format!("node-{}", &hex[..8])
        } else {
            hex.to_string()
        }
    }
}

/// What a single node's directory contains.
#[derive(Debug, Default)]
pub struct NodeData {
    pub label: String,
    pub node_id_hex: String,
    pub role: Option<String>,
    pub stage_index: Option<u32>,
    pub identity: Option<Identity>,
    pub snapshots: Vec<Snapshot>,
    pub events: Vec<EventRecord>,
    pub finalize: Option<Value>,
}

/// Forgiving deserialization mirror of [`super::super::collector::Manifest`].
///
/// Uses `#[serde(default)]` so a bundle produced by a future collector
/// that adds fields still parses.
#[derive(Debug, Clone, Deserialize)]
pub struct PostprocManifest {
    pub run_id: String,
    #[serde(default)]
    pub run_start_collector_ms: Option<u64>,
    #[serde(default)]
    pub run_end_collector_ms: Option<u64>,
    #[serde(default)]
    pub finalize_received: bool,
    #[serde(default)]
    pub nodes: Vec<PostprocManifestNode>,
}

/// Per-node manifest entry. Mirrors
/// [`super::super::collector::ManifestNode`].
#[derive(Debug, Clone, Deserialize)]
pub struct PostprocManifestNode {
    pub node_id_hex: String,
    pub label: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub stage_index: Option<u32>,
    #[serde(default)]
    pub boot_recorded: bool,
    #[serde(default)]
    pub event_batches: u64,
    #[serde(default)]
    pub snapshots: u64,
    #[serde(default)]
    pub finalize_recorded: bool,
}

#[derive(Debug)]
pub enum ParseError {
    Io(String),
    Tar(String),
    Schema(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Io(m) => write!(f, "io: {m}"),
            ParseError::Tar(m) => write!(f, "tar: {m}"),
            ParseError::Schema(m) => write!(f, "schema: {m}"),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
struct RawEntry {
    path: String,
    body: Vec<u8>,
}

#[derive(Debug, Default)]
struct NodeBin {
    identity: Option<Identity>,
    snapshots: Vec<Snapshot>,
    events: Vec<EventRecord>,
    finalize: Option<Value>,
}

fn detect_run_id(entries: &[RawEntry]) -> Result<String, ParseError> {
    // The bundle assembler writes a single top-level directory equal
    // to `{run_id}`. Pick the first non-empty path segment we see; if
    // the bundle has multiple top-level dirs, prefer the one that
    // contains a `MANIFEST.json`.
    let mut candidates: BTreeMap<String, bool> = BTreeMap::new();
    for entry in entries {
        let Some((top, rest)) = entry.path.split_once('/') else {
            continue;
        };
        let has_manifest = rest == "MANIFEST.json";
        let slot = candidates.entry(top.to_string()).or_insert(false);
        if has_manifest {
            *slot = true;
        }
    }
    if let Some(name) = candidates.iter().find(|(_, has)| **has).map(|(k, _)| k.clone()) {
        return Ok(name);
    }
    if let Some((name, _)) = candidates.into_iter().next() {
        return Ok(name);
    }
    Err(ParseError::Schema("bundle has no top-level directory".into()))
}

fn classify(sub: &str, body: &[u8], bin: &mut NodeBin) -> Result<(), ParseError> {
    if sub == "boot.json" {
        bin.identity = Some(parse_json("boot.json", body)?);
        return Ok(());
    }
    if sub == "finalize.json" {
        bin.finalize = Some(parse_json("finalize.json", body)?);
        return Ok(());
    }
    if let Some(name) = sub.strip_prefix("snapshots/") {
        if name.is_empty() || name.ends_with('/') {
            return Ok(());
        }
        let snap: Snapshot = parse_json(sub, body)?;
        bin.snapshots.push(snap);
        return Ok(());
    }
    if let Some(name) = sub.strip_prefix("events/") {
        if name.is_empty() || name.ends_with('/') {
            return Ok(());
        }
        let mut batch: Vec<EventRecord> = parse_json(sub, body)?;
        bin.events.append(&mut batch);
        return Ok(());
    }
    // Older boot/finalize retries land under `boot/` and `finalize/`
    // (per `collector/bundle.rs`). The "current" boot.json /
    // finalize.json is what we want; ignore the rest.
    Ok(())
}

fn parse_json<T: for<'de> Deserialize<'de>>(name: &str, body: &[u8]) -> Result<T, ParseError> {
    serde_json::from_slice(body)
        .map_err(|e| ParseError::Schema(format!("parse {name}: {e}")))
}

