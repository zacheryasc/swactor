//! Vast.ai-side context captured at boot (`DIAGNOSTICS_PLAN.md` T3.4).
//!
//! Best-effort, env-var-only — we never call the vast.ai API. The
//! captured set carries enough to answer "is this a bad host pool?"
//! and to correlate failures across runs ("do failures cluster on
//! specific `DATACENTER_ID`s?"). Values are fixed at process start;
//! subsequent snapshots reuse the cached struct unchanged.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::diagnostics::snapshot::{Tier3VastaiContext, VastaiIntrospector};
use crate::diagnostics::wall_ms_now;

/// Env var prefixes worth capturing. Anything matching one of these
/// (case-insensitive) lands in the captured set; everything else is
/// ignored. Order doesn't matter — capture sorts alphabetically.
const CAPTURE_PREFIXES: &[&str] = &["VAST_", "VASTAI_", "CONTAINER_", "CUDA_", "NVIDIA_"];

/// Substrings that suggest secret material. Any env var whose key
/// contains one of these (case-insensitive) is dropped before reaching
/// the wire, so a misconfigured host that exposes `VASTAI_API_KEY`
/// doesn't leak it into a bundle.
const SECRET_MARKERS: &[&str] = &["TOKEN", "KEY", "SECRET", "PASSWORD", "PASS"];

/// Frozen snapshot of vast.ai-side env. Construct once with
/// [`VastaiContext::capture_now`], install on the aggregator via
/// [`crate::diagnostics::Aggregator::set_vastai_introspector`].
#[derive(Debug, Clone)]
pub struct VastaiContext {
    cached: Tier3VastaiContext,
}

impl VastaiContext {
    /// Capture from the current process environment. Cheap — walks
    /// `std::env::vars()` once and applies the filter rules.
    pub fn capture_now() -> Self {
        let now = wall_ms_now();
        Self {
            cached: capture_from_env(&|| std::env::vars().collect(), now),
        }
    }

    /// Capture from an explicit env map. Useful for tests that want to
    /// avoid mutating the global process environment.
    pub fn capture_from(env: BTreeMap<String, String>) -> Self {
        let now = wall_ms_now();
        let provider = move || env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        Self {
            cached: capture_from_env(&provider, now),
        }
    }

    /// Cheap shareable handle for installing on an aggregator.
    pub fn into_arc(self) -> Arc<dyn VastaiIntrospector> {
        Arc::new(self)
    }

    /// Direct access to the cached context — mostly for tests.
    pub fn cached(&self) -> &Tier3VastaiContext {
        &self.cached
    }
}

impl VastaiIntrospector for VastaiContext {
    fn capture(&self) -> Tier3VastaiContext {
        self.cached.clone()
    }
}

fn capture_from_env(
    provider: &dyn Fn() -> Vec<(String, String)>,
    now: u64,
) -> Tier3VastaiContext {
    let mut container_id: Option<String> = None;
    let mut hostname: Option<String> = None;
    let mut kept: Vec<(String, String)> = Vec::new();
    for (k, v) in provider() {
        let upper_k = k.to_ascii_uppercase();
        if upper_k == "CONTAINER_ID" {
            container_id = Some(v.clone());
        }
        if upper_k == "HOSTNAME" {
            hostname = Some(v.clone());
        }
        if !matches_prefix(&upper_k) {
            continue;
        }
        if looks_secret(&upper_k) {
            continue;
        }
        kept.push((k, v));
    }
    kept.sort_by(|a, b| a.0.cmp(&b.0));
    Tier3VastaiContext {
        container_id,
        hostname,
        env_vars: kept,
        process_start_ms: now,
        captured_at_ms: now,
    }
}

fn matches_prefix(upper: &str) -> bool {
    CAPTURE_PREFIXES.iter().any(|p| upper.starts_with(p))
}

fn looks_secret(upper: &str) -> bool {
    SECRET_MARKERS.iter().any(|m| upper.contains(m))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn captures_matching_prefixes_and_skips_others() {
        let ctx = VastaiContext::capture_from(env(&[
            ("VAST_CONTAINER_LABEL", "blue-7"),
            ("VASTAI_DATACENTER_ID", "dc-42"),
            ("PATH", "/usr/bin:/bin"),
            ("HOME", "/root"),
        ]));
        let keys: Vec<&str> = ctx.cached().env_vars.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"VAST_CONTAINER_LABEL"));
        assert!(keys.contains(&"VASTAI_DATACENTER_ID"));
        assert!(!keys.contains(&"PATH"));
        assert!(!keys.contains(&"HOME"));
    }

    #[test]
    fn drops_keys_that_look_like_secrets() {
        let ctx = VastaiContext::capture_from(env(&[
            ("VASTAI_API_KEY", "hunter2"),
            ("VAST_AUTH_TOKEN", "secret"),
            ("VAST_FOO_PASSWORD", "x"),
            ("VASTAI_INSTANCE", "i-7"),
        ]));
        let keys: Vec<&str> = ctx.cached().env_vars.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["VASTAI_INSTANCE"]);
    }

    #[test]
    fn captures_container_id_and_hostname() {
        let ctx = VastaiContext::capture_from(env(&[
            ("CONTAINER_ID", "abc123"),
            ("HOSTNAME", "stage-0"),
        ]));
        assert_eq!(ctx.cached().container_id.as_deref(), Some("abc123"));
        assert_eq!(ctx.cached().hostname.as_deref(), Some("stage-0"));
    }

    #[test]
    fn returns_empty_set_when_no_matching_vars() {
        let ctx = VastaiContext::capture_from(env(&[
            ("HOME", "/root"),
            ("LANG", "C"),
        ]));
        assert!(ctx.cached().env_vars.is_empty());
        assert!(ctx.cached().container_id.is_none());
        assert!(ctx.cached().captured_at_ms > 0);
    }
}
