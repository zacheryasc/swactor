//! Simulator crate — see `SPEC.md`, `NORTH_STAR.md`, `OBSERVABILITY.md`,
//! and `TESTING_SPEC.md` in this directory for the design contract.

use std::io;
use std::path::{Path, PathBuf};

pub mod detector;
pub mod divergence;
pub mod engine;
pub mod lint;
pub mod postproc;
pub mod replay;
pub mod runtime;
pub mod spec;

#[path = "facade/sim/mod.rs"]
pub mod sim_backend;

/// Outcome of attempting to run the engine to completion.
#[derive(Debug)]
pub enum SimError {
    /// The engine has not been wired in yet for the requested
    /// surface. Phase-1 sentinel for the parity-bar driver.
    NotImplemented,
    SpecParse(String),
    Io(io::Error),
}

impl std::fmt::Display for SimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimError::NotImplemented => f.write_str("simulation engine not implemented yet"),
            SimError::SpecParse(msg) => write!(f, "simulation spec parse error: {msg}"),
            SimError::Io(err) => write!(f, "simulation io error: {err}"),
        }
    }
}

impl std::error::Error for SimError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SimError::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Bundle {
    pub root: PathBuf,
}

/// Engine entry point used by parity-bar tests. Parses `spec_text`,
/// drives the discrete-event engine to completion, and writes a
/// SPEC §6.4-shaped bundle into a fresh tempdir. The bundle's bytes
/// are a pure function of `(spec_text, seed)`.
pub fn run_to_tempdir(spec_text: &str, seed: u64) -> Result<Bundle, SimError> {
    let parsed = if let Some(bundle_path) = detect_replay_bundle(spec_text) {
        replay::load_replay_spec(Path::new(&bundle_path))?
    } else {
        spec::parse(spec_text)?
    };
    let engine = engine::Engine::new(parsed.clone(), seed);
    let records = engine.run();
    let root = sim_backend::bundle::write_run(&parsed, seed, &records)?;
    Ok(Bundle { root })
}

/// Locate the `bundle = "..."` value inside a `[replay]` table in
/// the spec text. The check is a focused linear scan — rolling out a
/// dedicated TOML pre-parser for this one optional table would tie
/// us to the toml crate's table-walking surface for very little
/// benefit. Returns `None` if no `[replay]` table is present.
fn detect_replay_bundle(spec_text: &str) -> Option<String> {
    let mut in_replay = false;
    for raw in spec_text.lines() {
        let line = raw.trim();
        if line.starts_with('[') && line.ends_with(']') {
            in_replay = line == "[replay]";
            continue;
        }
        if !in_replay {
            continue;
        }
        let Some(eq) = line.find('=') else {
            continue;
        };
        let (key, value) = line.split_at(eq);
        if key.trim() != "bundle" {
            continue;
        }
        let trimmed = value.trim_start_matches('=').trim();
        // toml-quoted path: strip a single layer of double quotes.
        let unquoted = trimmed
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_string();
        return Some(unquoted);
    }
    None
}
