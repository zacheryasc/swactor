//! Picks the iroh `RelayMode` from process environment.
//!
//! `SWACTOR_IROH_RELAY_URL` — when set, both pp-orchestrator and pp-worker
//! use `RelayMode::Custom(<url>)` instead of the canary default. The
//! orchestrator's [`StageEnv`](crate::vastai::StageEnv) propagates the same
//! var into every rented container so the whole cluster homes onto one
//! operator-controlled relay (typically `swactor-iroh-relay` on a VPS).
//!
//! Falls back to `RelayMode::Default` when unset.

use iroh::{RelayMode, RelayUrl};

/// Env var read by [`relay_mode_from_env`]. Public so callers (vastai
/// container env injection, run scripts, tests) reference one constant.
pub const ENV_IROH_RELAY_URL: &str = "SWACTOR_IROH_RELAY_URL";

/// Returns a `RelayMode` honoring `SWACTOR_IROH_RELAY_URL` from process
/// env. Logs to stderr on invalid URLs and falls back to `Default` so a
/// misconfigured deployment still tries to come up rather than panicking.
pub fn relay_mode_from_env() -> RelayMode {
    match std::env::var(ENV_IROH_RELAY_URL) {
        Ok(s) => {
            let s = s.trim();
            if s.is_empty() {
                return RelayMode::Default;
            }
            match s.parse::<RelayUrl>() {
                Ok(url) => {
                    eprintln!("pp: using custom iroh relay {url} (from {ENV_IROH_RELAY_URL})");
                    RelayMode::custom([url])
                }
                Err(e) => {
                    eprintln!(
                        "pp: invalid {ENV_IROH_RELAY_URL}={s:?}: {e}; falling back to default"
                    );
                    RelayMode::Default
                }
            }
        }
        Err(_) => RelayMode::Default,
    }
}
