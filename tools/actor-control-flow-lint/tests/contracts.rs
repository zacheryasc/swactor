use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("lint package lives under tools/")
        .to_path_buf()
}

fn driver_revision(root: &Path) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for path in ["driver.rs", "timing_policy.rs"] {
        let source = std::fs::read(root.join("tools/actor-control-flow-lint").join(path))
            .expect("read compiler policy source");
        for byte in source {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    format!("--cfg=actor_lint_driver_{hash:016x}")
}

fn cargo_check(fixture: &str, extra_args: &[&str]) -> Output {
    let root = repository_root();
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixture);
    let target_dir = root
        .join("target/actor-control-flow-contracts")
        .join(fixture);
    let wrapper = root.join("tools/actor-control-flow-lint/rustc-wrapper.py");

    let mut command = Command::new(env!("CARGO"));
    command
        .arg("check")
        .arg("--quiet")
        .args(extra_args)
        .current_dir(fixture_dir)
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_TERM_COLOR", "never")
        .env("RUSTC_WORKSPACE_WRAPPER", wrapper)
        .env("RUSTFLAGS", driver_revision(&root))
        .env_remove("CARGO_MAKEFLAGS")
        .env_remove("MAKEFLAGS");
    command.output().expect("run fixture cargo check")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn compiler_policy_contracts() {
    let direct = cargo_check("fail-domain-capabilities", &[]);
    assert!(
        !direct.status.success(),
        "forbidden domain fixture compiled"
    );
    let direct_stderr = stderr(&direct);
    for expected in [
        "asynchronous task spawning",
        "blocking task spawning",
        "engine task scheduling",
        "OS thread creation",
        "unapproved timing primitive",
        "direct timer driving",
        "runtime construction or driving",
        "blocking receive used as a controller",
        "process creation",
    ] {
        assert!(
            direct_stderr.contains(expected),
            "missing `{expected}` diagnostic:\n{direct_stderr}"
        );
    }

    let dependency = cargo_check("fail-owner-dependency", &[]);
    assert!(
        !dependency.status.success(),
        "execution owner depending on domain control compiled"
    );
    let dependency_stderr = stderr(&dependency);
    assert!(
        dependency_stderr
            .contains("execution owner `swactor-engine` depends on domain-control crate `myelin`"),
        "missing owner dependency diagnostic:\n{dependency_stderr}"
    );

    let owner_timing = cargo_check("fail-owner-timing", &[]);
    assert!(
        !owner_timing.status.success(),
        "unapproved execution-owner timer compiled"
    );
    let owner_timing_stderr = stderr(&owner_timing);
    assert!(
        owner_timing_stderr.contains(
            "unapproved timing primitive: `tokio::time::sleep` is forbidden in workspace package `iroh-driver`"
        ),
        "missing resolved owner-timer diagnostic:\n{owner_timing_stderr}"
    );

    let timing_overage = cargo_check("fail-timing-overage", &[]);
    assert!(
        !timing_overage.status.success(),
        "timing allowance overage compiled"
    );
    let timing_overage_stderr = stderr(&timing_overage);
    assert!(
        timing_overage_stderr.contains("occurrence 2 exceeds the 1 audited call(s)"),
        "missing timing-overage diagnostic:\n{timing_overage_stderr}"
    );

    for fixture in ["pass-actor-domain", "pass-execution-owner"] {
        let output = cargo_check(fixture, &[]);
        assert!(
            output.status.success(),
            "compile-pass fixture `{fixture}` failed:\n{}",
            stderr(&output)
        );
    }

    let test_wait = cargo_check("pass-test-wait", &["--tests"]);
    assert!(
        test_wait.status.success(),
        "narrow test wait fixture failed:\n{}",
        stderr(&test_wait)
    );
}
