//! Tarball assembly for a finalized run.
//!
//! Walks `{root}/{run_id}/` and packs it into
//! `{root}/bundles/{run_id}.tar.gz`. Per-node directories are
//! relabeled from `{node_id}` to a friendlier `{role}` (or
//! `{role}-{stage_index}`) when the boot record gave us a role. The
//! mapping is recorded in `MANIFEST.json` so the full hex is always
//! recoverable.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;

use super::protocol::{Manifest, ManifestNode};
use super::state::CollectorState;

/// Assemble the canonical bundle on disk. Used by the `/diag/finalize`
/// handler when a run finalizes cleanly. The synthesized tarball lands
/// at `state.bundle_path(run_id)` so subsequent `GET /diag/bundle/<run>`
/// calls serve it from the cache without re-walking staging.
pub fn assemble(state: &CollectorState, run_id: &str) -> io::Result<PathBuf> {
    let run_dir = state.run_dir(run_id);
    if !run_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no records on disk for run_id {run_id}"),
        ));
    }
    let bundles_dir = state.bundles_dir();
    std::fs::create_dir_all(&bundles_dir)?;
    let bundle_path = state.bundle_path(run_id);
    let file = File::create(&bundle_path)?;
    assemble_into(state, run_id, file)?;
    Ok(bundle_path)
}

/// Assemble the bundle for `run_id` in memory and return the bytes
/// (spec §7, gap 7). Used by `GET /diag/bundle/<run>` when no
/// canonical tarball exists yet — typically because the orchestrator
/// died before sending the finalize record. The resulting bundle's
/// `MANIFEST.json` carries `finalize_received: false`, matching
/// whatever the collector observed for the run.
///
/// Returns `Err(NotFound)` when the run has no staging directory at
/// all (truly unknown run id); a partial run with even one boot
/// record returns Ok.
pub fn assemble_bytes(state: &CollectorState, run_id: &str) -> io::Result<Vec<u8>> {
    let run_dir = state.run_dir(run_id);
    if !run_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no records on disk for run_id {run_id}"),
        ));
    }
    let mut buf = Vec::new();
    assemble_into(state, run_id, &mut buf)?;
    Ok(buf)
}

/// Shared core: write the gzipped tar of `run_id` into `writer`. The
/// public callers wrap this with either a `File` (canonical
/// on-finalize path) or a `Vec<u8>` (on-demand HTTP path).
fn assemble_into<W: Write>(
    state: &CollectorState,
    run_id: &str,
    writer: W,
) -> io::Result<()> {
    let stats = state.run_stats(run_id);
    let labels = build_labels(&stats);
    let manifest = build_manifest(run_id, &stats, &labels);

    let gz = GzEncoder::new(writer, Compression::default());
    let mut tar = tar::Builder::new(gz);
    tar.mode(tar::HeaderMode::Deterministic);

    // Root entry for the run.
    append_dir(&mut tar, run_id)?;

    // MANIFEST.json at tar root for this run.
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    append_bytes(
        &mut tar,
        &format!("{run_id}/MANIFEST.json"),
        &manifest_bytes,
    )?;

    // Walk per-node directories in label order so the tarball is
    // deterministic for tests / cross-run diffs.
    let mut ordered: Vec<(&String, &String)> = labels.iter().collect();
    ordered.sort_by(|a, b| a.1.cmp(b.1).then(a.0.cmp(b.0)));
    for (node_id, label) in ordered {
        let node_src = state.node_dir(run_id, node_id);
        if !node_src.is_dir() {
            continue;
        }
        let tar_node = format!("{run_id}/{label}");
        append_dir(&mut tar, &tar_node)?;
        append_node_dir(&mut tar, &node_src, &tar_node)?;
    }

    tar.finish()?;
    Ok(())
}

fn append_node_dir<W: Write>(
    tar: &mut tar::Builder<GzEncoder<W>>,
    src: &Path,
    dst_prefix: &str,
) -> io::Result<()> {
    // We organize files by kind into kind-named subdirs to match
    // the bundle layout in DIAGNOSTICS_PLAN.md (snapshots/, events/).
    // Boot and finalize records become single files at the node root.
    let mut boots: Vec<PathBuf> = Vec::new();
    let mut events: Vec<PathBuf> = Vec::new();
    let mut snapshots: Vec<PathBuf> = Vec::new();
    let mut finalizes: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if name.starts_with("boot-") {
            boots.push(path);
        } else if name.starts_with("events-") {
            events.push(path);
        } else if name.starts_with("snapshot-") {
            snapshots.push(path);
        } else if name.starts_with("finalize-") {
            finalizes.push(path);
        }
    }
    boots.sort();
    events.sort();
    snapshots.sort();
    finalizes.sort();

    // Promote the latest boot/finalize to a single file at the node
    // root — the format in DIAGNOSTICS_PLAN.md expects exactly one of
    // each. Earlier boot retries (if any) stay under `boot/` for
    // forensic value.
    if let Some(latest_boot) = boots.last() {
        append_file(tar, latest_boot, &format!("{dst_prefix}/boot.json"))?;
    }
    if let Some(latest_finalize) = finalizes.last() {
        append_file(tar, latest_finalize, &format!("{dst_prefix}/finalize.json"))?;
    }
    if boots.len() > 1 {
        append_dir(tar, &format!("{dst_prefix}/boot"))?;
        for p in &boots[..boots.len() - 1] {
            append_under(tar, p, &format!("{dst_prefix}/boot"))?;
        }
    }
    if !events.is_empty() {
        append_dir(tar, &format!("{dst_prefix}/events"))?;
        for p in &events {
            append_under(tar, p, &format!("{dst_prefix}/events"))?;
        }
    }
    if !snapshots.is_empty() {
        append_dir(tar, &format!("{dst_prefix}/snapshots"))?;
        for p in &snapshots {
            append_under(tar, p, &format!("{dst_prefix}/snapshots"))?;
        }
    }
    if finalizes.len() > 1 {
        append_dir(tar, &format!("{dst_prefix}/finalize"))?;
        for p in &finalizes[..finalizes.len() - 1] {
            append_under(tar, p, &format!("{dst_prefix}/finalize"))?;
        }
    }
    Ok(())
}

fn append_under<W: Write>(
    tar: &mut tar::Builder<GzEncoder<W>>,
    src: &Path,
    dst_dir: &str,
) -> io::Result<()> {
    let name = src
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 filename"))?;
    append_file(tar, src, &format!("{dst_dir}/{name}"))
}

fn append_file<W: Write>(
    tar: &mut tar::Builder<GzEncoder<W>>,
    src: &Path,
    dst: &str,
) -> io::Result<()> {
    let mut f = File::open(src)?;
    let meta = f.metadata()?;
    let mut header = tar::Header::new_gnu();
    header.set_size(meta.len());
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    tar.append_data(&mut header, dst, &mut f)
}

fn append_dir<W: Write>(tar: &mut tar::Builder<GzEncoder<W>>, dst: &str) -> io::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(0);
    header.set_mode(0o755);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Directory);
    header.set_cksum();
    let path = format!("{}/", dst.trim_end_matches('/'));
    tar.append_data(&mut header, path, &mut io::empty())
}

fn append_bytes<W: Write>(
    tar: &mut tar::Builder<GzEncoder<W>>,
    dst: &str,
    bytes: &[u8],
) -> io::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    tar.append_data(&mut header, dst, bytes)
}

fn build_labels(stats: &Option<super::state::RunStats>) -> BTreeMap<String, String> {
    // Maps full hex node_id -> bundle-side directory label. Falls
    // back to `node-{short}` when boot didn't supply enough.
    let mut out = BTreeMap::new();
    let Some(stats) = stats else { return out };
    for (node_id, node) in &stats.nodes {
        let label = label_for(node_id, node.identity.as_ref());
        out.insert(node_id.clone(), label);
    }
    // Disambiguate label collisions (two nodes both calling themselves
    // "orchestrator", for example) by suffixing with the short id.
    let mut seen: BTreeMap<String, u32> = BTreeMap::new();
    for label in out.values() {
        *seen.entry(label.clone()).or_insert(0) += 1;
    }
    let collisions: Vec<String> = seen
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(k, _)| k.clone())
        .collect();
    for (node_id, label) in out.iter_mut() {
        if collisions.contains(label) {
            let short = node_id.chars().take(8).collect::<String>();
            *label = format!("{label}-{short}");
        }
    }
    out
}

fn label_for(node_id: &str, identity: Option<&Value>) -> String {
    let short: String = node_id.chars().take(8).collect();
    let Some(id) = identity else {
        return format!("node-{short}");
    };
    let role = id.get("role").and_then(|r| match r {
        Value::String(s) => Some(s.clone()),
        Value::Object(o) => o.get("0").and_then(|v| v.as_str()).map(str::to_string),
        Value::Array(arr) => arr.first().and_then(|v| v.as_str()).map(str::to_string),
        _ => None,
    });
    let stage_index = id.get("stage_index").and_then(|v| v.as_u64());
    match (role.as_deref(), stage_index) {
        (Some("stage"), Some(idx)) => format!("stage-{idx}"),
        (Some(r), _) if !r.is_empty() => r.to_string(),
        _ => format!("node-{short}"),
    }
}

fn build_manifest(
    run_id: &str,
    stats: &Option<super::state::RunStats>,
    labels: &BTreeMap<String, String>,
) -> Manifest {
    let stats = match stats {
        Some(s) => s,
        None => {
            return Manifest {
                run_id: run_id.to_string(),
                run_start_collector_ms: None,
                run_end_collector_ms: None,
                finalize_received: false,
                nodes: Vec::new(),
            };
        }
    };
    let mut nodes: Vec<ManifestNode> = stats
        .nodes
        .iter()
        .map(|(node_id, node)| {
            let identity = node.identity.as_ref();
            let role = identity.and_then(|id| id.get("role")).and_then(|r| match r {
                Value::String(s) => Some(s.clone()),
                Value::Object(o) => o.get("0").and_then(|v| v.as_str()).map(str::to_string),
                Value::Array(arr) => arr.first().and_then(|v| v.as_str()).map(str::to_string),
                _ => None,
            });
            let stage_index = identity
                .and_then(|id| id.get("stage_index"))
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            ManifestNode {
                node_id_hex: node_id.clone(),
                label: labels
                    .get(node_id)
                    .cloned()
                    .unwrap_or_else(|| format!("node-{}", node_id.chars().take(8).collect::<String>())),
                role,
                stage_index,
                boot_recorded: node.boot_recorded,
                event_batches: node.event_batches,
                snapshots: node.snapshots,
                finalize_recorded: node.finalize_recorded,
            }
        })
        .collect();
    nodes.sort_by(|a, b| a.label.cmp(&b.label).then(a.node_id_hex.cmp(&b.node_id_hex)));
    Manifest {
        run_id: run_id.to_string(),
        run_start_collector_ms: stats.run_start_collector_ms,
        run_end_collector_ms: stats.run_end_collector_ms,
        finalize_received: stats.finalize_received,
        nodes,
    }
}
