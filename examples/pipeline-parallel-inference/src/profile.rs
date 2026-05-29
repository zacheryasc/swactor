//! One `.env`-style run profile as the single source of truth for the
//! example's many `PP_*` / `SWACTOR_*` knobs.
//!
//! The binaries already read everything from process env (relay URL, stage
//! count, image, model, worker, timeouts, …). The problem this solves is that
//! those knobs were scattered and set out-of-band in one operator's shell — a
//! teammate or CI had no way to discover them, so the deployment read as
//! "hardcoded to my VPS".
//!
//! [`load_profile`] fills in env vars from a `KEY=VALUE` file *without*
//! overriding anything already present, giving a clean precedence chain:
//!
//! ```text
//! compiled default  <  profile file  <  real process env  <  CLI flag
//! ```
//!
//! Committed templates live in `profiles/` (`example.env`, `local-seed.env`,
//! `vastai.env`); an operator's real relay/secrets go in the untracked
//! `profiles/local.env`.

use std::path::{Path, PathBuf};

/// Env var naming the profile file to load. When unset, `load_profile` falls
/// back to `profiles/local.env` (untracked) if it exists, else does nothing.
pub const ENV_PROFILE: &str = "PP_PROFILE";

/// Default profile path tried when [`ENV_PROFILE`] is unset.
const DEFAULT_PROFILE: &str = "profiles/local.env";

/// Load a `.env`-style profile into the process environment, setting each key
/// only if it is not already set. Logs to stderr which profile (if any) was
/// applied. Safe to call once at the top of `main()`.
pub fn load_profile() {
    let (path, explicit) = match std::env::var(ENV_PROFILE) {
        Ok(p) if !p.trim().is_empty() => (PathBuf::from(p.trim()), true),
        _ => (PathBuf::from(DEFAULT_PROFILE), false),
    };

    if !path.exists() {
        if explicit {
            eprintln!("pp: {ENV_PROFILE}={} not found; using env + defaults", path.display());
        } else {
            eprintln!("pp: no profile ({}); using env + defaults", path.display());
        }
        return;
    }

    match apply_profile(&path) {
        Ok(applied) => eprintln!(
            "pp: loaded profile {} ({applied} var(s) applied; existing env wins)",
            path.display()
        ),
        Err(e) => eprintln!("pp: failed to read profile {}: {e}", path.display()),
    }
}

/// Parse `path` and set each `KEY=VALUE` whose key is currently unset in the
/// environment. Returns how many vars were newly applied.
fn apply_profile(path: &Path) -> std::io::Result<usize> {
    let contents = std::fs::read_to_string(path)?;
    let mut applied = 0;
    for (key, value) in contents.lines().filter_map(parse_line) {
        if std::env::var_os(&key).is_none() {
            // SAFETY: load_profile runs as the first statement of main(),
            // before any threads are spawned, so there is no concurrent env
            // access. Keys come from the profile file, values are owned.
            unsafe { std::env::set_var(&key, &value) };
            applied += 1;
        }
    }
    Ok(applied)
}

/// Parse a single profile line into a `(key, value)` pair, or `None` for blank
/// lines, comments, and malformed entries. Tolerates a leading `export ` and
/// strips one layer of surrounding single/double quotes from the value.
fn parse_line(raw: &str) -> Option<(String, String)> {
    let line = raw.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
    let (key, value) = line.split_once('=')?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    Some((key.to_string(), unquote(value.trim()).to_string()))
}

/// Strip one matching pair of surrounding single or double quotes, if present.
fn unquote(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let (first, last) = (bytes[0], bytes[bytes.len() - 1]);
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &s[1..s.len() - 1];
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kv_with_export_and_quotes() {
        assert_eq!(
            parse_line("export FOO=\"bar baz\""),
            Some(("FOO".into(), "bar baz".into()))
        );
        assert_eq!(parse_line("N=3"), Some(("N".into(), "3".into())));
        assert_eq!(parse_line("  # comment"), None);
        assert_eq!(parse_line(""), None);
        assert_eq!(parse_line("=novalue"), None);
    }

    #[test]
    fn profile_fills_unset_but_real_env_wins() {
        // Contract: a profile value lands only when the var is unset; a value
        // already present in the environment is never clobbered.
        let dir = std::env::temp_dir().join("pp-profile-contract-test");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("p.env");
        std::fs::write(
            &file,
            "PP_TEST_ONLY_UNSET=from_profile\nPP_TEST_ONLY_PRESET=from_profile\n",
        )
        .unwrap();

        // SAFETY: test is single-threaded; vars are test-only sentinels.
        unsafe {
            std::env::remove_var("PP_TEST_ONLY_UNSET");
            std::env::set_var("PP_TEST_ONLY_PRESET", "from_env");
        }

        let applied = apply_profile(&file).unwrap();

        assert_eq!(std::env::var("PP_TEST_ONLY_UNSET").unwrap(), "from_profile");
        assert_eq!(std::env::var("PP_TEST_ONLY_PRESET").unwrap(), "from_env");
        assert_eq!(applied, 1, "only the unset var should be applied");

        unsafe {
            std::env::remove_var("PP_TEST_ONLY_UNSET");
            std::env::remove_var("PP_TEST_ONLY_PRESET");
        }
        let _ = std::fs::remove_file(&file);
    }
}
