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
    for path in [
        "driver.rs",
        "timing_policy.rs",
        "execution_policy.rs",
        "myelin_time_policy.rs",
    ] {
        let source = std::fs::read(root.join("tools/actor-control-flow-lint").join(path))
            .expect("read compiler policy source");
        for byte in source {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    format!("--cfg=actor_lint_driver_{hash:016x}")
}

fn cargo_check_command(fixture: &str, extra_args: &[&str]) -> Command {
    let root = repository_root();
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(fixture);
    // The single contract test visits fixtures serially. Reuse dependency
    // artifacts, but salt all compilations with every compiler-policy source.
    let target_dir = root.join("target/actor-control-flow-contracts");
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
    command
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn myelin_no_time_contracts() {
    let check = |args: &[&str]| {
        cargo_check_command("myelin-no-time", args)
            .output()
            .expect("run Myelin no-time fixture")
    };
    let pass = check(&[]);
    assert!(
        pass.status.success(),
        "opaque external clock must be allowed:\n{}",
        stderr(&pass)
    );
    for (args, expected) in [
        (&["--features=types"][..], "temporal type"),
        (&["--features=calls"][..], "timing API"),
        (&["--features=inferred"][..], "temporal type"),
        (&["--features=scheduling"][..], "timing API"),
        (&["--tests"][..], "timing API"),
    ] {
        let output = check(args);
        assert!(
            !output.status.success(),
            "temporal fixture compiled: {args:?}"
        );
        assert!(
            stderr(&output).contains(&format!("Myelin no-time policy: {expected}")),
            "fixture failed without expected policy diagnostic: {args:?}\n{}",
            stderr(&output)
        );
    }
}

#[test]
fn compiler_policy_contracts() {
    // Process coordination belongs to this actual test root, not a privileged
    // package or an ungoverned helper. Closures keep their lexical root.
    let cargo_check = |fixture: &str, extra_args: &[&str]| {
        cargo_check_command(fixture, extra_args)
            .output()
            .expect("run fixture cargo check")
    };
    let fail_cases: &[(&str, &[&str], &[&str])] = &[
        (
            "fail-domain-capabilities",
            &[],
            &[
                "asynchronous task spawning",
                "blocking task spawning",
                "engine task scheduling",
                "OS thread creation",
                "unapproved timing primitive",
                "direct timer driving",
                "runtime construction or driving",
                "blocking receive used as a controller",
                "process creation",
            ],
        ),
        (
            "fail-renamed-spawn",
            &[],
            &["asynchronous task spawning", "blocking task spawning"],
        ),
        (
            "fail-reexport-spawn",
            &[],
            &["asynchronous task spawning", "blocking task spawning"],
        ),
        ("fail-helper-spawn", &[], &["asynchronous task spawning"]),
        (
            "fail-python-runtime",
            &["--features=direct"],
            &[
                "implicit Python runtime task scheduling",
                "receive TaskSpawner explicitly",
            ],
        ),
        (
            "fail-python-runtime",
            &["--features=aliased-helper"],
            &[
                "implicit Python runtime task scheduling",
                "receive TaskSpawner explicitly",
            ],
        ),
        (
            "fail-python-runtime",
            &["--features=with-locals"],
            &[
                "implicit Python runtime task scheduling",
                "receive TaskSpawner explicitly",
            ],
        ),
        (
            "fail-python-runtime",
            &["--features=runtime-access"],
            &[
                "implicit Python runtime task scheduling",
                "receive TaskSpawner explicitly",
            ],
        ),
        (
            "fail-python-runtime",
            &["--features=runtime-spawn"],
            &[
                "implicit Python runtime task scheduling",
                "receive TaskSpawner explicitly",
            ],
        ),
        (
            "fail-closure-spawn",
            &[],
            &[
                "asynchronous task spawning",
                "blocking task spawning",
                "OS thread creation",
            ],
        ),
        (
            "fail-owner-spawn",
            &[],
            &[
                "asynchronous task spawning",
                "blocking task spawning",
                "OS thread creation",
            ],
        ),
        (
            "fail-substrate-neighbor",
            &[],
            &[
                "asynchronous task spawning",
                "blocking task spawning",
                "OS thread creation",
            ],
        ),
        (
            "fail-alternate-raw-spawn",
            &[],
            &["asynchronous task spawning", "OS thread creation"],
        ),
        ("fail-backend-scheduling", &[], &["engine task scheduling"]),
        (
            "fail-removed-handle-spawn",
            &[],
            &["no method named `spawn`"],
        ),
        ("fail-minting", &[], &["task-capability minting violation"]),
        (
            "fail-binary-helper-mint",
            &[],
            &["task-capability minting violation"],
        ),
        (
            "fail-minting-function-pointer",
            &[],
            &["task-capability minting violation"],
        ),
        (
            "fail-root-mint-function-pointer",
            &[],
            &["task-capability minting violation"],
        ),
        (
            "fail-test-helper-mint",
            &["--tests"],
            &["task-capability minting violation"],
        ),
        (
            "fail-test-package-spawn",
            &["--tests"],
            &[
                "asynchronous task spawning",
                "blocking task spawning",
                "OS thread creation",
            ],
        ),
        (
            "fail-test-helper-coordination",
            &["--tests"],
            &["blocking receive used as a controller", "process creation"],
        ),
        (
            "fail-actor-storage",
            &[],
            &["ActorInterface must not store TaskSpawner"],
        ),
        (
            "fail-actor-nested-storage",
            &[],
            &["ActorInterface must not store TaskSpawner"],
        ),
        (
            "fail-actor-alias",
            &[],
            &["ActorInterface must not store TaskSpawner"],
        ),
        (
            "fail-generic-actor-storage",
            &[],
            &["instantiated ActorInterface type must not store TaskSpawner"],
        ),
        (
            "fail-actor-message",
            &[],
            &["incoming-message type must not carry TaskSpawner"],
        ),
        (
            "fail-actor-nested-message",
            &[],
            &["incoming-message type must not carry TaskSpawner"],
        ),
        (
            "fail-actor-handler",
            &[],
            &["TaskSpawner must not be introduced inside an actor handler"],
        ),
        (
            "fail-actor-handler-closure",
            &[],
            &["TaskSpawner must not be introduced inside an actor handler"],
        ),
        (
            "fail-owner-dependency",
            &[],
            &["execution owner `swactor-engine` depends on domain-control crate `myelin`"],
        ),
        (
            "fail-owner-timing",
            &[],
            &[
                "unapproved timing primitive: `tokio::time::sleep` is forbidden in workspace package `iroh-driver`",
            ],
        ),
        (
            "fail-timing-overage",
            &[],
            &["occurrence 2 exceeds the 1 audited call(s)"],
        ),
    ];
    for &(fixture, args, diagnostics) in fail_cases {
        let output = cargo_check(fixture, args);
        assert!(
            !output.status.success(),
            "forbidden fixture `{fixture}` compiled"
        );
        let errors = stderr(&output);
        for expected in diagnostics {
            assert!(
                errors.contains(expected),
                "fixture `{fixture}` missing `{expected}` diagnostic:\n{errors}"
            );
        }
    }

    for (fixture, args) in [
        ("pass-actor-domain", &[][..]),
        ("pass-execution-owner", &[][..]),
        ("pass-binary-root", &[][..]),
        ("pass-test-composition", &["--tests"][..]),
        ("pass-test-wait", &["--tests"][..]),
    ] {
        let output = cargo_check(fixture, args);
        assert!(
            output.status.success(),
            "compile-pass fixture `{fixture}` failed:\n{}",
            stderr(&output)
        );
    }
}
