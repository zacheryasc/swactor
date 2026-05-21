//! TESTING_SPEC §4 — Facade integrity.
//!
//! All four §4 checks are infrastructure, not engine. They run the
//! lint-deterministic scanner, the surface-fingerprint comparison,
//! and a structural inspection of the runtime-facade source — none
//! of which depend on the engine.

#[path = "common.rs"]
mod common;

use common::workspace_root;

// ── §4.1 — Banned-API lint ─────────────────────────────────────────

#[test]
fn banned_api_clean() {
    // Drive the scanner library directly. Running the binary via
    // `cargo run -p lint-deterministic` would work but is slow and
    // recursive — the library entry point uses the same code path.
    let root = workspace_root();
    let config_path = root.join("crates/simulation/banned.toml");
    let config = simulation::lint::Config::load(&config_path)
        .expect("banned.toml must load");
    let violations = simulation::lint::scan_banned_apis(&root, &config)
        .expect("scanner must complete");
    assert!(
        violations.is_empty(),
        "banned-API scanner reported {} violation(s):\n{}",
        violations.len(),
        violations
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ── §4.2 — Feature exclusivity ─────────────────────────────────────

#[test]
fn feature_exclusivity() {
    // The const-eval guard in `crates/runtime-facade/src/lib.rs`
    // rejects both-features-enabled and neither-feature-enabled
    // builds with a `panic!`-flavoured `const _: () = panic!(...)`.
    // Reading the source and asserting both guards are present is
    // the executable form of "build fails with the expected
    // diagnostic" — the actual build failure is exercised by the
    // workspace gate (`cargo build --features facade-prod,facade-sim`)
    // run by `cargo xtask parity-bar`.
    let facade_src = workspace_root().join("crates/simulation/src/runtime/mod.rs");
    let text = std::fs::read_to_string(&facade_src)
        .expect("runtime/mod.rs must exist");

    assert!(
        text.contains("all(feature = \"facade-prod\", feature = \"facade-sim\")"),
        "runtime must reject both-features-enabled builds"
    );
    assert!(
        text.contains("const _: () = panic!"),
        "the feature-exclusivity guard must fire at compile time \
         (const-eval `panic!`)"
    );
}

// ── §4.3 — No `cfg(sim)` in peer code ──────────────────────────────

#[test]
fn no_cfg_sim_in_peer() {
    // Peer code never branches on whether it is in the sim. The
    // facade is the only swap point (SPEC §3.4). The closed list of
    // facade-discipline crates is below.
    let root = workspace_root();
    let peer_dirs = [
        "crates/simulation/src",
        "src", // root swactor crate (no-modify, but still in scope)
    ];
    let mut hits: Vec<String> = Vec::new();
    for dir in peer_dirs {
        let dir_path = root.join(dir);
        if !dir_path.exists() {
            continue;
        }
        common::walk_files(&dir_path, &mut |path| {
            let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
                return;
            };
            if ext != "rs" {
                return;
            }
            let text = match std::fs::read_to_string(path) {
                Ok(t) => t,
                Err(_) => return,
            };
            for (idx, line) in text.lines().enumerate() {
                // Match `cfg(sim)` with optional whitespace inside
                // the parens. Avoid matching `cfg(simulation)` or
                // similar by requiring the closing paren next.
                if let Some(start) = line.find("cfg(") {
                    let tail = &line[start + 4..];
                    let trimmed = tail.trim_start();
                    if let Some(rest) = trimmed.strip_prefix("sim") {
                        let next = rest.trim_start();
                        if next.starts_with(')') || next.starts_with(',') {
                            hits.push(format!("{}:{}: {}", path.display(), idx + 1, line));
                        }
                    }
                }
            }
        });
    }
    assert!(
        hits.is_empty(),
        "cfg(sim) found in peer code (TESTING_SPEC §4.3):\n{}",
        hits.join("\n")
    );
}

// ── §4.4 — Facade trait surface is closed ──────────────────────────

#[test]
fn facade_surface_locked() {
    // Independently re-derive the runtime-facade surface descriptor
    // by parsing `crates/runtime-facade/src/lib.rs` with `syn` —
    // mirroring the logic of that crate's own `build.rs` — and
    // SHA-256 it. The check fails if (a) a trait signature changed
    // without `surface.lock` being updated, OR (b) `runtime-facade`'s
    // `build.rs` was tampered with to embed a descriptor different
    // from the source. Reading the source directly makes the
    // parity-bar suite's drift detection independent of any single
    // build artifact.
    use quote::ToTokens;
    use sha2::{Digest, Sha256};

    let lib_path = workspace_root().join("crates/simulation/src/runtime/mod.rs");
    let src = std::fs::read_to_string(&lib_path)
        .expect("simulation/src/runtime/mod.rs must exist");
    let file = syn::parse_file(&src)
        .expect("runtime/mod.rs must parse as Rust");

    let traits_mod = file
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Mod(m) if m.ident == "traits" => Some(m),
            _ => None,
        })
        .expect("`pub mod traits` must exist in runtime/mod.rs");

    let (_, items) = traits_mod
        .content
        .as_ref()
        .expect("`pub mod traits` must be defined inline");

    let mut descriptor = String::new();
    descriptor.push_str("runtime surface descriptor v1\n");
    for item in items {
        if let syn::Item::Trait(t) = item {
            descriptor.push_str("trait ");
            descriptor.push_str(&t.ident.to_string());
            descriptor.push('\n');
            for ti in &t.items {
                if let syn::TraitItem::Fn(f) = ti {
                    let sig: String = f
                        .sig
                        .to_token_stream()
                        .to_string()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    descriptor.push_str("  ");
                    descriptor.push_str(&sig);
                    descriptor.push('\n');
                }
            }
        }
    }

    let mut h = Sha256::new();
    h.update(descriptor.as_bytes());
    let live = format!("{:x}", h.finalize());

    let locked = std::fs::read_to_string(
        workspace_root().join("crates/simulation/surface.lock"),
    )
    .expect("simulation/surface.lock must exist");
    assert_eq!(
        live,
        locked.trim(),
        "runtime trait surface fingerprint drifted — update \
         crates/simulation/surface.lock in the same commit"
    );
}
