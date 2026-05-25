//! Versions of dependencies the diagnostics layer reports about.
//!
//! Sourced from `Cargo.lock` via `build.rs`. The whole point of this
//! module is gap 6 from
//! `examples/pipeline-parallel-inference/N3_OBSERVABILITY_UPGRADE_SPEC.md`:
//! the bundle never contains a version string that disagrees with what
//! was actually linked. If the build script could not find the entry it
//! fails the build, never falls back to a literal.

/// Version of the `iroh` crate linked into this build.
///
/// `build.rs` emits this from `Cargo.lock`. Used wherever the bundle
/// reports a version: the `iroh_api_missing` event payload, the
/// `iroh_version` field on every tier-2 transport snapshot.
pub const IROH_VERSION: &str = env!("DISTRIBUTION_IROH_VERSION");

/// Best-effort `git rev-parse HEAD` of the source tree at build time.
///
/// `None` when the build script could not call `git` (e.g. CI checkout
/// stripped, or an out-of-tree build). The runtime never fabricates a
/// placeholder — gap 5 acceptance is that missing means missing.
pub const GIT_SHA: Option<&str> = option_env!("DISTRIBUTION_GIT_SHA");
