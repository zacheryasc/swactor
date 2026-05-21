//! Replay loader — reconstruct a runnable spec from an on-disk
//! bundle. Used by `simulation::run_to_tempdir` when the caller
//! passes a spec text that names `[replay] bundle = <path>`.
//!
//! Two reconstruction paths (SPEC §7.1):
//!
//! 1. **`sim/spec.toml` present.** The bundle is sim-origin and the
//!    spec text was preserved byte-for-byte; the replay just re-runs
//!    the engine against that text. Self-replay byte-equality
//!    follows from the engine's determinism (TESTING_SPEC §3.1).
//!
//! 2. **Prod-shape (no `sim/spec.toml`).** Walk the bundle, derive
//!    `(run_id, duration_ms)` from MANIFEST.json, walk per-host
//!    directories for boot / finalize / restart wall_ms's, and
//!    pull mutation triples (kind, at_ms) from the orchestrator's
//!    `Custom` event stream. Links are not recoverable from
//!    prod-shape and stay empty; the parity diff treats absent
//!    links as parameter-unknown (SPEC §7.1).

use std::path::{Path, PathBuf};

use crate::sim_backend::bundle as fs_io;
use crate::spec::{self, validate_path_component, Host, Mutation, MutationKind, ParsedSpec};
use crate::SimError;

/// Load a [`ParsedSpec`] for a replay run. `bundle_path` is the
/// bundle root; `seed` is the run seed the engine should use.
pub fn load_replay_spec(bundle_path: &Path) -> Result<ParsedSpec, SimError> {
    if !bundle_path.is_dir() {
        return Err(SimError::SpecParse(format!(
            "replay bundle not found at {}",
            bundle_path.display()
        )));
    }
    let spec_toml = bundle_path.join("sim/spec.toml");
    if spec_toml.is_file() {
        let text = fs_io::read_text(&spec_toml).map_err(SimError::Io)?;
        return spec::parse(&text);
    }
    reconstruct_from_prod_shape(bundle_path)
}

fn reconstruct_from_prod_shape(bundle_path: &Path) -> Result<ParsedSpec, SimError> {
    let manifest_path = bundle_path.join("MANIFEST.json");
    let manifest_text = fs_io::read_text(&manifest_path).map_err(SimError::Io)?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest_text)
        .map_err(|e| SimError::SpecParse(format!("MANIFEST.json: {e}")))?;
    let run_id = manifest
        .get("run_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| SimError::SpecParse("MANIFEST.json missing run_id".into()))?
        .to_string();
    validate_path_component(&run_id, "MANIFEST.run_id")?;
    let duration_ms = manifest
        .get("duration_ms")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| SimError::SpecParse("MANIFEST.json missing duration_ms".into()))?;

    let mut hosts: Vec<Host> = Vec::new();
    let subdirs = fs_io::list_subdirs(bundle_path).map_err(SimError::Io)?;
    for name in subdirs {
        if name == "sim" || name == "detector" {
            continue;
        }
        validate_path_component(&name, "bundle host directory")?;
        let host_dir = bundle_path.join(&name);
        let host = build_host(&name, &host_dir)?;
        hosts.push(host);
    }
    hosts.sort_by(|a, b| a.name.cmp(&b.name));

    let mutations = collect_mutations(bundle_path)?;

    // Synthesize a spec text so the bundle that the replay writes
    // back to `sim/spec.toml` reflects what the engine actually ran.
    // The text is deterministic in the reconstructed fields.
    let source_text = synthesize_spec_text(&run_id, duration_ms, &hosts, &mutations);
    Ok(ParsedSpec {
        source_text,
        run_id,
        duration_ms,
        host_defaults: Default::default(),
        hosts,
        links: Vec::new(),
        mutations,
    })
}

fn build_host(name: &str, host_dir: &Path) -> Result<Host, SimError> {
    // Initial boot.
    let boot_path = host_dir.join("boot.json");
    let boot_text = fs_io::read_text(&boot_path).map_err(SimError::Io)?;
    let boot: serde_json::Value = serde_json::from_str(&boot_text)
        .map_err(|e| SimError::SpecParse(format!("{}: {e}", boot_path.display())))?;
    let start_at_ms = boot
        .get("wall_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let stage_index = boot
        .get("stage_index")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);

    // Subsequent epochs from boot-NNN.json files.
    let mut restart_at_ms: Vec<u64> = Vec::new();
    for path in fs_io::list_files_with_prefix(host_dir, "boot-")
        .map_err(SimError::Io)?
    {
        let text = fs_io::read_text(&path).map_err(SimError::Io)?;
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SimError::SpecParse(format!("{}: {e}", path.display())))?;
        if let Some(w) = v.get("wall_ms").and_then(|x| x.as_u64()) {
            restart_at_ms.push(w);
        }
    }
    restart_at_ms.sort();

    // Last finalize.
    let mut finalize_paths: Vec<PathBuf> = Vec::new();
    let direct = host_dir.join("finalize.json");
    if direct.is_file() {
        finalize_paths.push(direct);
    }
    for path in fs_io::list_files_with_prefix(host_dir, "finalize-")
        .map_err(SimError::Io)?
    {
        finalize_paths.push(path);
    }
    let mut stop_at_ms: Option<u64> = None;
    if let Some(last) = finalize_paths.last() {
        let text = fs_io::read_text(last).map_err(SimError::Io)?;
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SimError::SpecParse(format!("{}: {e}", last.display())))?;
        let reason = v
            .get("shutdown_reason")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        // Only "clean" gets propagated as a spec-declared stop_at_ms.
        // "end_of_run" finalizes synthesize when the host was alive
        // at duration_ms; they get re-emitted automatically when the
        // replay's engine reaches duration_ms. "restart" finalizes
        // are intermediate.
        if reason == "clean" {
            stop_at_ms = v.get("wall_ms").and_then(|x| x.as_u64());
        }
    }

    Ok(Host {
        name: name.to_string(),
        role: name.to_string(),
        stage_index,
        start_at_ms,
        restart_at_ms,
        stop_at_ms,
        crash_at_ms: None,
    })
}

fn collect_mutations(bundle_path: &Path) -> Result<Vec<Mutation>, SimError> {
    // The mutation schedule is reconstructed from the orchestrator's
    // `Custom` event stream (SPEC §7.1). Fall back to scanning every
    // node if no `orchestrator` directory exists — the corpus's host
    // ordering is fixed, but a generic replay-target may not have
    // one. We pick the alphabetically-first host that has events.
    let subdirs = fs_io::list_subdirs(bundle_path).map_err(SimError::Io)?;
    let mut chosen: Option<PathBuf> = None;
    for name in &subdirs {
        if name == "sim" {
            continue;
        }
        let events_dir = bundle_path.join(name).join("events");
        if events_dir.is_dir() {
            chosen = Some(events_dir);
            break;
        }
    }
    let Some(events_dir) = chosen else {
        return Ok(Vec::new());
    };
    let paths = fs_io::list_files_with_prefix(&events_dir, "")
        .map_err(SimError::Io)?;
    let mut mutations: Vec<Mutation> = Vec::new();
    for path in paths {
        let text = fs_io::read_text(&path).map_err(SimError::Io)?;
        let env: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| SimError::SpecParse(format!("{}: {e}", path.display())))?;
        let Some(arr) = env.get("records").and_then(|v| v.as_array()) else {
            continue;
        };
        for rec in arr {
            let variant = rec.get("variant").and_then(|v| v.as_str()).unwrap_or("");
            if variant != "Custom" {
                continue;
            }
            let user_kind = rec
                .get("user_kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let at_ms = rec.get("wall_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            let kind = match user_kind.as_str() {
                "partition" => MutationKind::Partition { edges: Vec::new() },
                "heal" => MutationKind::Heal { edges: Vec::new() },
                "restart" => MutationKind::Restart {
                    node: String::new(),
                },
                "clock_jump" => MutationKind::ClockJump {
                    node: String::new(),
                    delta_ms: 0,
                },
                "link_change" => MutationKind::LinkChange,
                _ => continue,
            };
            mutations.push(Mutation {
                at_ms,
                spec_index: mutations.len(),
                kind,
            });
        }
    }
    Ok(mutations)
}

fn synthesize_spec_text(
    run_id: &str,
    duration_ms: u64,
    hosts: &[Host],
    mutations: &[Mutation],
) -> String {
    // Emit a TOML representation of what we recovered. Used purely
    // as the new bundle's `sim/spec.toml`; the engine does not
    // re-parse from this text (it has the already-built ParsedSpec).
    let mut out = String::new();
    out.push_str(&format!("run_id = {}\n", toml_string(run_id)));
    out.push_str(&format!("duration_ms = {duration_ms}\n"));
    out.push('\n');
    for h in hosts {
        out.push_str("[[hosts]]\n");
        out.push_str(&format!("name = {}\n", toml_string(&h.name)));
        out.push_str(&format!("role = {}\n", toml_string(&h.role)));
        if let Some(idx) = h.stage_index {
            out.push_str(&format!("stage_index = {idx}\n"));
        }
        out.push_str(&format!("start_at_ms = {}\n", h.start_at_ms));
        if !h.restart_at_ms.is_empty() {
            out.push_str(&format!("restart_at_ms = {:?}\n", h.restart_at_ms));
        }
        if let Some(stop) = h.stop_at_ms {
            out.push_str(&format!("stop_at_ms = {stop}\n"));
        }
        out.push('\n');
    }
    for m in mutations {
        out.push_str("[[mutations]]\n");
        out.push_str(&format!("at_ms = {}\n", m.at_ms));
        match &m.kind {
            MutationKind::Partition { .. } => out.push_str("kind = \"partition\"\n"),
            MutationKind::Heal { .. } => out.push_str("kind = \"heal\"\n"),
            MutationKind::Restart { .. } => out.push_str("kind = \"restart\"\n"),
            MutationKind::ClockJump { .. } => out.push_str("kind = \"clock_jump\"\n"),
            MutationKind::LinkChange => out.push_str("kind = \"link_change\"\n"),
        }
        out.push('\n');
    }
    out
}

/// Render `s` as a TOML basic string. Escapes `\\`, `"`, and control
/// characters so the result round-trips through any TOML parser.
fn toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
