//! Banned-API scanner (TESTING_SPEC §4.1) and parity-test
//! hygiene linter (§12.2/§12.3/§12.4).
//!
//! Behaviour is a single `Scanner` parameterised by [`Config`]. The
//! crate exposes the scanner as a library so its self-tests can
//! drive it against fixture inputs without invoking the binary.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const ALLOW_MARKER: &str = "lint-deterministic: allow";

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub banned: Vec<BannedApi>,
    pub scope: Scope,
    pub parity_bar: ParityBar,
}

#[derive(Debug, Deserialize)]
pub struct BannedApi {
    pub pattern: String,
    pub replacement: String,
}

#[derive(Debug, Deserialize)]
pub struct Scope {
    pub include: Vec<String>,
    pub allowlist: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ParityBar {
    pub root: String,
    pub banned_substrings: Vec<String>,
    pub banned_fn_prefixes: Vec<String>,
    pub banned_attrs: Vec<String>,
    pub banned_skip_patterns: Vec<String>,
}

impl Config {
    pub fn load(path: &Path) -> io::Result<Self> {
        let text = fs::read_to_string(path)?;
        toml::from_str(&text).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Debug, Clone)]
pub struct Violation {
    pub file: PathBuf,
    pub line: usize,
    pub matched: String,
    pub reason: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}: {} — {}",
            self.file.display(),
            self.line,
            self.matched,
            self.reason
        )
    }
}

/// Scan `root` according to `config` for banned-API references.
pub fn scan_banned_apis(root: &Path, config: &Config) -> io::Result<Vec<Violation>> {
    let mut violations = Vec::new();
    for include in &config.scope.include {
        let dir = root.join(include);
        if !dir.exists() {
            continue;
        }
        walk_rust_files(&dir, &mut |path| {
            if is_allowlisted(path, root, &config.scope.allowlist) {
                return Ok(());
            }
            scan_file_for_banned(path, &config.banned, &mut violations)
        })?;
    }
    Ok(violations)
}

/// Scan the parity-bar tree per §12.2/§12.3/§12.4.
pub fn scan_parity_bar(root: &Path, config: &Config) -> io::Result<Vec<Violation>> {
    let dir = root.join(&config.parity_bar.root);
    let mut violations = Vec::new();
    if !dir.exists() {
        return Ok(violations);
    }
    walk_rust_files(&dir, &mut |path| {
        scan_file_for_parity_bar(path, &config.parity_bar, &mut violations)
    })?;
    Ok(violations)
}

/// Self-test entry point: scan a single file as if it lived in
/// the parity-bar tree. Used by fixture-driven tests.
pub fn scan_text_for_parity_bar(
    file: &Path,
    text: &str,
    rules: &ParityBar,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    scan_text_parity(file, text, rules, &mut violations);
    violations
}

/// Self-test entry point: scan a single file's text for banned APIs.
pub fn scan_text_for_banned(
    file: &Path,
    text: &str,
    banned: &[BannedApi],
) -> Vec<Violation> {
    let mut violations = Vec::new();
    scan_text_banned(file, text, banned, &mut violations);
    violations
}

fn walk_rust_files<F>(dir: &Path, visit: &mut F) -> io::Result<()>
where
    F: FnMut(&Path) -> io::Result<()>,
{
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_dir() {
            walk_rust_files(&path, visit)?;
        } else if kind.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rs") {
            visit(&path)?;
        }
    }
    Ok(())
}

fn is_allowlisted(path: &Path, root: &Path, allowlist: &[String]) -> bool {
    let rel = match path.strip_prefix(root) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let rel_str = rel.to_string_lossy().replace('\\', "/");
    allowlist
        .iter()
        .any(|prefix| rel_str == *prefix || rel_str.starts_with(&format!("{prefix}/")))
}

fn scan_file_for_banned(
    path: &Path,
    banned: &[BannedApi],
    violations: &mut Vec<Violation>,
) -> io::Result<()> {
    let text = fs::read_to_string(path)?;
    scan_text_banned(path, &text, banned, violations);
    Ok(())
}

fn scan_text_banned(
    path: &Path,
    text: &str,
    banned: &[BannedApi],
    violations: &mut Vec<Violation>,
) {
    for (idx, line) in text.lines().enumerate() {
        for rule in banned {
            if !line.contains(&rule.pattern) {
                continue;
            }
            if line_allows(line, &rule.pattern) {
                continue;
            }
            violations.push(Violation {
                file: path.to_path_buf(),
                line: idx + 1,
                matched: rule.pattern.clone(),
                reason: format!("use {} instead", rule.replacement),
            });
        }
    }
}

fn scan_file_for_parity_bar(
    path: &Path,
    rules: &ParityBar,
    violations: &mut Vec<Violation>,
) -> io::Result<()> {
    let text = fs::read_to_string(path)?;
    scan_text_parity(path, &text, rules, violations);
    Ok(())
}

fn scan_text_parity(
    path: &Path,
    text: &str,
    rules: &ParityBar,
    violations: &mut Vec<Violation>,
) {
    for (idx, line) in text.lines().enumerate() {
        for pat in &rules.banned_substrings {
            if line.contains(pat) && !line_allows(line, pat) {
                violations.push(Violation {
                    file: path.to_path_buf(),
                    line: idx + 1,
                    matched: pat.clone(),
                    reason:
                        "probabilistic primitives banned in parity-bar tests (TESTING_SPEC §12.2)"
                            .to_string(),
                });
            }
        }
        for pat in &rules.banned_attrs {
            if line.contains(pat) && !line_allows(line, pat) {
                violations.push(Violation {
                    file: path.to_path_buf(),
                    line: idx + 1,
                    matched: pat.clone(),
                    reason: "conditional skip banned in parity-bar tests (TESTING_SPEC §12.3)"
                        .to_string(),
                });
            }
        }
        for pat in &rules.banned_skip_patterns {
            if line.contains(pat) && !line_allows(line, pat) {
                violations.push(Violation {
                    file: path.to_path_buf(),
                    line: idx + 1,
                    matched: pat.clone(),
                    reason:
                        "env-driven skips banned in parity-bar tests (TESTING_SPEC §12.3)"
                            .to_string(),
                });
            }
        }
        for prefix in &rules.banned_fn_prefixes {
            let needle = format!("fn {prefix}");
            if line.contains(&needle) && !line_allows(line, &needle) {
                violations.push(Violation {
                    file: path.to_path_buf(),
                    line: idx + 1,
                    matched: needle,
                    reason: "fuzz-prefixed test fns banned in parity-bar (TESTING_SPEC §12.2)"
                        .to_string(),
                });
            }
        }
        // §12.4: bare `.unwrap()` on Result-returning engine calls.
        if line.contains(".unwrap()") && !line_allows(line, ".unwrap()") {
            violations.push(Violation {
                file: path.to_path_buf(),
                line: idx + 1,
                matched: ".unwrap()".to_string(),
                reason: "use .expect(\"<reason>\") in parity-bar tests (TESTING_SPEC §12.4)"
                    .to_string(),
            });
        }
    }
}

fn line_allows(line: &str, pattern: &str) -> bool {
    if let Some(start) = line.find(ALLOW_MARKER) {
        let tail = &line[start + ALLOW_MARKER.len()..];
        let trimmed = tail.trim();
        if trimmed.is_empty() {
            return true;
        }
        return trimmed
            .split_whitespace()
            .any(|allowed| allowed == pattern);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parity_bar_rules() -> ParityBar {
        ParityBar {
            root: "tests/parity-bar".into(),
            banned_substrings: vec!["proptest::".into(), "rand::random".into()],
            banned_fn_prefixes: vec!["fuzz_".into()],
            banned_attrs: vec!["#[ignore]".into(), "#[cfg(not(".into()],
            banned_skip_patterns: vec!["std::env::var".into()],
        }
    }

    #[test]
    fn allow_marker_silences_match() {
        let banned = vec![BannedApi {
            pattern: "std::time::Instant".into(),
            replacement: "Facade::clock()".into(),
        }];
        let with_marker =
            "let now = std::time::Instant::now(); // lint-deterministic: allow std::time::Instant";
        let v = scan_text_for_banned(Path::new("x.rs"), with_marker, &banned);
        assert!(
            v.is_empty(),
            "allow marker should silence the match: {:?}",
            v
        );
    }

    #[test]
    fn allow_marker_is_pattern_specific() {
        let banned = vec![BannedApi {
            pattern: "std::time::Instant".into(),
            replacement: "Facade::clock()".into(),
        }];
        // Marker names a different pattern — still a violation.
        let line = "let now = std::time::Instant::now(); // lint-deterministic: allow tokio::spawn";
        let v = scan_text_for_banned(Path::new("x.rs"), line, &banned);
        assert_eq!(v.len(), 1, "pattern-mismatched marker should not silence");
    }

    #[test]
    fn parity_rules_flag_ignore() {
        let rules = parity_bar_rules();
        let text = "#[test]\n#[ignore]\nfn x() {}\n";
        let v = scan_text_for_parity_bar(Path::new("t_x.rs"), text, &rules);
        assert!(v.iter().any(|v| v.matched == "#[ignore]"));
    }

    #[test]
    fn parity_rules_flag_unwrap_without_message() {
        let rules = parity_bar_rules();
        let text = "let b = run().unwrap();\n";
        let v = scan_text_for_parity_bar(Path::new("t_x.rs"), text, &rules);
        assert!(v.iter().any(|v| v.matched == ".unwrap()"));
    }

    #[test]
    fn parity_rules_flag_fuzz_fn() {
        let rules = parity_bar_rules();
        let text = "fn fuzz_test_engine() {}\n";
        let v = scan_text_for_parity_bar(Path::new("t_x.rs"), text, &rules);
        assert!(v.iter().any(|v| v.matched == "fn fuzz_"));
    }
}
