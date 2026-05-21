//! TESTING_SPEC §2 — Determinism oracle.
//!
//! Five binary checks on the engine's determinism: two-run equality
//! (in-process), two-process equality (subprocess), debug/release
//! equality (cargo build --release vs debug binary), no wall-clock
//! taint (host clock perturbation via the child's `FAKETIME` env),
//! and divergence detection (a poisoned-RNG re-run produces a
//! structured `Error` envelope and a non-zero exit code).

#[path = "common.rs"]
mod common;

use common::{reference_scenario_text, run_reference_scenario, sha256_tree, workspace_root};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

// ── §2.1 — Two-run byte equality ───────────────────────────────────

#[test]
fn two_run_byte_equality() {
    // Two invocations, same (spec, seed). Every regular file in
    // either bundle must hash to the same SHA-256.
    let b1 = run_reference_scenario();
    let b2 = run_reference_scenario();
    let h1 = sha256_tree(&b1.root);
    let h2 = sha256_tree(&b2.root);
    assert_eq!(
        h1, h2,
        "two runs of the reference scenario must produce byte-identical bundles"
    );
}

// ── §2.2 — Two-process concurrent equality ─────────────────────────

#[test]
fn two_process_equality() {
    // Same `(spec, seed)` invoked in two separate OS processes via
    // the `sim-driver` binary. The bundles must be byte-identical
    // — catching non-determinism from process-global state
    // (ASLR / vtable layout, environment ordering, thread-local
    // randomness).
    let spec = stage_spec_file();
    let bin = sim_driver_binary("debug");

    let b1 = invoke_sim_driver(&bin, &spec, 42, &[]);
    let b2 = invoke_sim_driver(&bin, &spec, 42, &[]);
    let h1 = sha256_tree(&b1);
    let h2 = sha256_tree(&b2);
    assert_eq!(
        h1, h2,
        "two-process runs of the reference scenario produced divergent bundles"
    );
}

// ── §2.3 — Debug/release equality ─────────────────────────────────

#[test]
fn debug_release_equality() {
    // The engine compiled with optimisations must produce the same
    // bundle as the debug build. Floating-point arithmetic on any
    // recording path is the usual culprit — the engine bans it
    // per SPEC §2.4.
    let spec = stage_spec_file();
    let debug_bin = sim_driver_binary("debug");
    let release_bin = sim_driver_binary("release");

    let b_debug = invoke_sim_driver(&debug_bin, &spec, 42, &[]);
    let b_release = invoke_sim_driver(&release_bin, &spec, 42, &[]);
    let h_debug = sha256_tree(&b_debug);
    let h_release = sha256_tree(&b_release);
    assert_eq!(
        h_debug, h_release,
        "debug and release sim-driver invocations produced divergent bundles"
    );
}

// ── §2.4 — No wall-clock taint ────────────────────────────────────

#[test]
fn no_wall_clock_taint() {
    // Spawn the sim-driver twice. The second invocation has
    // `FAKETIME` set in its child environment (the libfaketime
    // injection point per the spec). If `libfaketime.so.1` is
    // preloaded the host clock advances by an hour; if it isn't,
    // setting the env var still proves the engine does not read
    // `$FAKETIME` directly. Either way, the bundle bytes must not
    // change.
    let spec = stage_spec_file();
    let bin = sim_driver_binary("debug");

    let baseline = invoke_sim_driver(&bin, &spec, 42, &[]);
    let perturbed = invoke_sim_driver(
        &bin,
        &spec,
        42,
        &[("FAKETIME", "@2030-01-01 12:00:00"), ("TZ", "Asia/Tokyo")],
    );
    assert_eq!(
        sha256_tree(&baseline),
        sha256_tree(&perturbed),
        "host clock perturbation changed bundle bytes (FAKETIME / TZ env leak)"
    );
}

// ── §2.5 — Divergence detection ───────────────────────────────────

#[test]
fn divergence_detected() {
    // Run the divergence detector: clean baseline vs poisoned re-run.
    // The poison flag (the only test-only switch permitted, per the
    // spec, and living in the sim-facade subtree) flips one byte of
    // the loopback host's `Custom` event payload. The detector must:
    //   * name the first divergent file in its report;
    //   * emit a structured `Error` event (component, message);
    //   * propose a non-zero exit code.
    let spec = reference_scenario_text();
    // Poisoned re-run must diverge and surface a structured Error.
    let report = simulation::divergence::check(&spec, 42, true)
        .expect("divergence detector itself must run cleanly");

    assert!(
        report.is_diverged(),
        "poisoned run produced an identical bundle — poison switch broken: {report:?}"
    );
    assert_ne!(report.exit_code, 0, "divergence must propose non-zero exit");

    match report.outcome {
        simulation::divergence::Outcome::Diverged {
            first_divergent_path,
            error_event,
            ..
        } => {
            assert!(
                !first_divergent_path.is_empty(),
                "report must name the first divergent path"
            );
            assert_eq!(error_event.variant, "Error");
            assert_eq!(error_event.component, "divergence_detector");
            assert!(
                error_event.message.contains(&first_divergent_path),
                "Error message must name the divergent path: {error_event:?}"
            );
        }
        other => panic!("expected Diverged, got {other:?}"),
    }

    // Sanity: a clean baseline against a clean re-run must not
    // report divergence. The detector must not fire false positives.
    let clean = simulation::divergence::check(&spec, 42, false)
        .expect("rerun divergence detector");
    assert!(
        matches!(clean.outcome, simulation::divergence::Outcome::Identical),
        "detector reported Diverged on two clean runs (false positive): {clean:?}"
    );
    assert_eq!(
        clean.exit_code, 0,
        "clean detector run must propose exit code 0"
    );
}

// ── Helpers ────────────────────────────────────────────────────────

fn stage_spec_file() -> PathBuf {
    // Persist the reference-scenario text into a per-test tempdir so
    // child processes can read it. Reuses the same temp dir across
    // calls (cheap; the test binary's pid is the directory's salt).
    let dir = std::env::temp_dir().join(format!(
        "sim-parity-bar-spec-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp spec dir");
    let path = dir.join("reference.toml");
    std::fs::write(&path, common::reference_scenario_text())
        .expect("write reference scenario");
    path
}

fn sim_driver_binary(profile: &str) -> PathBuf {
    // Build the sim-driver binary in the requested profile and
    // return its absolute path. `cargo build` is a no-op if the
    // binary is already current.
    let mut args = vec![
        "build",
        "-p",
        "simulation",
        "--bin",
        "sim-driver",
        "--features",
        "facade-prod",
        "--quiet",
    ];
    if profile == "release" {
        args.push("--release");
    }
    let status = Command::new(cargo_bin())
        .args(&args)
        .current_dir(workspace_root())
        .status()
        .unwrap_or_else(|e| panic!("invoke cargo build for sim-driver ({profile}): {e}"));
    assert!(
        status.success(),
        "cargo build -p sim-driver ({profile}) failed: {status}"
    );
    let bin = workspace_root()
        .join("target")
        .join(profile)
        .join("sim-driver");
    assert!(
        bin.is_file(),
        "sim-driver ({profile}) missing at {}",
        bin.display()
    );
    bin
}

fn invoke_sim_driver(bin: &Path, spec: &Path, seed: u64, env: &[(&str, &str)]) -> PathBuf {
    let mut cmd = Command::new(bin);
    cmd.arg(spec).arg("--seed").arg(seed.to_string());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("invoke sim-driver: {e}"));
    assert!(
        output.status.success(),
        "sim-driver exited {} (stderr: {})",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let path = stdout.trim();
    assert!(
        !path.is_empty(),
        "sim-driver did not print a bundle path on stdout (stderr: {})",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = PathBuf::from(path);
    assert!(
        path.is_dir(),
        "sim-driver returned non-existent bundle dir {}",
        path.display()
    );
    path
}

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}

#[allow(dead_code)]
fn lock_in_sha256_tree(_: BTreeMap<PathBuf, String>) {
    // sha256_tree returns BTreeMap<PathBuf, String>; this fn exists
    // only to silence unused-import warnings when individual checks
    // are commented out for local debugging.
}
