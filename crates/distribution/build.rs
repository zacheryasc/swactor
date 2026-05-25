//! Build-time discovery of dependency versions that the runtime needs to
//! report honestly in the diagnostics bundle.
//!
//! Today we only emit the `iroh` version (gap 6 in
//! `examples/pipeline-parallel-inference/N3_OBSERVABILITY_UPGRADE_SPEC.md`),
//! but the same parser handles any other crate the diagnostics layer
//! reports about — add another `emit_version` call and a const in
//! `diagnostics::dep_versions` when one comes up.
//!
//! Versions come from the workspace `Cargo.lock`, located by walking
//! upward from `OUT_DIR`'s ancestors until a sibling file named
//! `Cargo.lock` is found. We never fall back to a hardcoded literal —
//! the whole point of this is to keep the bundle honest about what was
//! linked, so a missing lockfile is a build failure, not a silent zero.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let lock_path = find_cargo_lock().expect(
        "build.rs could not locate Cargo.lock — diagnostics requires it for honest version \
         reporting. Run from inside the workspace.",
    );
    println!("cargo:rerun-if-changed={}", lock_path.display());
    let body = fs::read_to_string(&lock_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", lock_path.display()));
    emit_version(&body, "iroh", "DISTRIBUTION_IROH_VERSION");
    emit_build_git_sha();
}

/// Best-effort `git rev-parse HEAD` capture. If the repo is unavailable
/// or the call fails, the env var is left unset and the runtime
/// constant resolves to `None`. The point is to keep the bundle honest
/// — never fabricate a placeholder — while letting builds outside a
/// git checkout still succeed.
fn emit_build_git_sha() {
    println!("cargo:rerun-if-env-changed=DISTRIBUTION_GIT_SHA_OVERRIDE");
    if let Ok(override_sha) = env::var("DISTRIBUTION_GIT_SHA_OVERRIDE") {
        let trimmed = override_sha.trim();
        if !trimmed.is_empty() {
            println!("cargo:rustc-env=DISTRIBUTION_GIT_SHA={trimmed}");
            return;
        }
    }
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
    if let Some(dir) = manifest_dir {
        if let Ok(out) = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&dir)
            .output()
        {
            if out.status.success() {
                let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !sha.is_empty() {
                    println!("cargo:rustc-env=DISTRIBUTION_GIT_SHA={sha}");
                    // Re-run when the head commit changes so a dirty
                    // rebuild reports the right SHA.
                    let head = locate_git_head(&dir);
                    if let Some(head) = head {
                        println!("cargo:rerun-if-changed={}", head.display());
                    }
                }
            }
        }
    }
}

fn locate_git_head(start: &Path) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        let candidate = dir.join(".git").join("HEAD");
        if candidate.is_file() {
            return Some(candidate);
        }
        dir = dir.parent()?;
    }
}

fn emit_version(lockfile: &str, package: &str, env_var: &str) {
    let version = lockfile_version(lockfile, package).unwrap_or_else(|| {
        panic!(
            "Cargo.lock has no entry for `{package}`. The diagnostics layer reports its version \
             and refuses to make one up."
        )
    });
    println!("cargo:rustc-env={env_var}={version}");
}

/// Lookup the `version = "..."` line of the `[[package]]` block named
/// `package`. Naive but adequate — Cargo.lock is well-formed TOML with
/// predictable layout. We deliberately avoid a TOML dependency in
/// build.rs so this stays a zero-cost build script.
fn lockfile_version(body: &str, package: &str) -> Option<String> {
    let needle = format!("name = \"{package}\"");
    let mut lines = body.lines();
    while let Some(line) = lines.next() {
        if line.trim() != needle {
            continue;
        }
        for next in lines.by_ref() {
            let t = next.trim();
            if t.starts_with("[[package]]") {
                // Reached the next package without a version line.
                return None;
            }
            if let Some(rest) = t.strip_prefix("version = \"") {
                if let Some(end) = rest.find('"') {
                    return Some(rest[..end].to_string());
                }
            }
        }
    }
    None
}

fn find_cargo_lock() -> Option<PathBuf> {
    // CARGO_MANIFEST_DIR points at the crate root. Walk up looking for
    // a sibling Cargo.lock — both the workspace root and standalone
    // crates have one.
    let start = env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from)?;
    let mut dir: &Path = &start;
    loop {
        let candidate = dir.join("Cargo.lock");
        if candidate.is_file() {
            return Some(candidate);
        }
        dir = dir.parent()?;
    }
}
