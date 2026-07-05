use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{Instant, SystemTime};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

struct TestStep {
    label: &'static str,
    args: &'static [&'static str],
}

const MVP_CHAT_REBUILD_INPUTS: &[&str] = &[
    "Cargo.lock",
    "Cargo.toml",
    "src",
    "crates/datastream/Cargo.toml",
    "crates/datastream/src",
    "crates/dashboard/Cargo.toml",
    "crates/dashboard/src",
    "crates/distribution/Cargo.toml",
    "crates/distribution/src",
    "crates/iroh-driver/Cargo.toml",
    "crates/iroh-driver/src",
    "crates/mvp-system/Cargo.toml",
    "crates/mvp-system/src",
    "crates/transport/Cargo.toml",
    "crates/transport/src",
    "tools/vastai/Cargo.toml",
    "tools/vastai/src",
];

const BASIC_TESTS: &[TestStep] = &[
    TestStep {
        label: "root crate",
        args: &["test"],
    },
    TestStep {
        label: "datastream",
        args: &["test", "-p", "datastream"],
    },
    TestStep {
        label: "distribution",
        args: &["test", "-p", "distribution"],
    },
    TestStep {
        label: "iroh-driver",
        args: &["test", "-p", "iroh-driver"],
    },
    TestStep {
        label: "mvp-system",
        args: &["test", "-p", "mvp-system"],
    },
    TestStep {
        label: "swactor-process",
        args: &["test", "-p", "swactor-process"],
    },
    TestStep {
        label: "swactor-transport",
        args: &["test", "-p", "swactor-transport"],
    },
    TestStep {
        label: "dashboard",
        args: &["test", "-p", "dashboard"],
    },
    TestStep {
        label: "swactor-vastai",
        args: &["test", "-p", "swactor-vastai"],
    },
    TestStep {
        label: "xtask",
        args: &["test", "-p", "xtask"],
    },
];

fn cargo_bin() -> String {
    option_env!("CARGO")
        .map(str::to_string)
        .unwrap_or_else(|| "cargo".to_string())
}

fn print_usage() {
    println!(
        "\
USAGE: cargo xtask <command>

COMMANDS:
  mvp-chat [args...]  Run mvp-one-node-chat, building it only when tracked inputs are newer.
  test                Run all basic non-binding tests. This includes the root crate with
                      `cargo test` plus each non-binding repository package with `cargo test -p`.
                      Feature-gated E2E/bin tests are intentionally excluded."
    );
}

fn run_step(step: &TestStep) -> bool {
    println!("\n=== {} ===", step.label);
    println!("    cargo {}", step.args.join(" "));
    println!();

    match Command::new(cargo_bin()).args(step.args).status() {
        Ok(status) => status.success(),
        Err(error) => {
            eprintln!("Failed to execute cargo: {error}");
            false
        }
    }
}

fn run_tests() -> ExitCode {
    let start = Instant::now();

    for (index, step) in BASIC_TESTS.iter().enumerate() {
        if !run_step(step) {
            eprintln!(
                "\n--- FAILED after {:.1}s ({index} passed, 1 failed) ---",
                start.elapsed().as_secs_f64()
            );
            return ExitCode::from(1);
        }
    }

    println!(
        "\n--- All {} step(s) passed in {:.1}s ---",
        BASIC_TESTS.len(),
        start.elapsed().as_secs_f64()
    );
    ExitCode::SUCCESS
}

fn run_mvp_chat(args: Vec<String>) -> ExitCode {
    match ensure_mvp_chat_binary() {
        Ok(bin) => exec_mvp_chat(&bin, args),
        Err(error) => {
            eprintln!("cargo mvp-chat: {error}");
            ExitCode::from(1)
        }
    }
}

fn ensure_mvp_chat_binary() -> Result<PathBuf, String> {
    let root = workspace_root();
    let bin = root
        .join("target")
        .join("debug")
        .join(format!("mvp-one-node-chat{}", std::env::consts::EXE_SUFFIX));
    if rebuild_needed(&bin, &root, MVP_CHAT_REBUILD_INPUTS)? {
        eprintln!("cargo mvp-chat: building mvp-one-node-chat");
        let status = Command::new(cargo_bin())
            .current_dir(&root)
            .args([
                "build",
                "--quiet",
                "-p",
                "mvp-system",
                "--features",
                "local-e2e",
                "--bin",
                "mvp-one-node-chat",
            ])
            .status()
            .map_err(|e| format!("run cargo build for mvp-one-node-chat: {e}"))?;
        if !status.success() {
            return Err(format!("build mvp-one-node-chat failed with {status}"));
        }
    } else {
        eprintln!("cargo mvp-chat: mvp-one-node-chat is up to date; skipping cargo build");
    }
    Ok(bin)
}

fn exec_mvp_chat(bin: &Path, args: Vec<String>) -> ExitCode {
    let mut command = Command::new(bin);
    command.args(args);
    #[cfg(target_os = "linux")]
    {
        let error = command.exec();
        eprintln!("exec {}: {error}", bin.display());
        ExitCode::from(1)
    }
    #[cfg(not(target_os = "linux"))]
    {
        match command.status() {
            Ok(status) if status.success() => ExitCode::SUCCESS,
            Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
            Err(error) => {
                eprintln!("run {}: {error}", bin.display());
                ExitCode::from(1)
            }
        }
    }
}

fn rebuild_needed(bin: &Path, root: &Path, inputs: &[&str]) -> Result<bool, String> {
    if !bin.is_file() {
        return Ok(true);
    }
    let bin_mtime = modified_time(root, bin)?;
    for input in inputs {
        let path = root.join(input);
        if latest_mtime(root, &path)? > bin_mtime {
            return Ok(true);
        }
    }
    Ok(false)
}

fn latest_mtime(root: &Path, path: &Path) -> Result<SystemTime, String> {
    let display = display_workspace_path(root, path);
    let metadata = fs::metadata(path).map_err(|e| format!("stat {display}: {e}"))?;
    let mut latest = metadata
        .modified()
        .map_err(|e| format!("modified time {display}: {e}"))?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|e| format!("read dir {display}: {e}"))? {
            let entry = entry.map_err(|e| format!("read dir entry {display}: {e}"))?;
            let entry_mtime = latest_mtime(root, &entry.path())?;
            if entry_mtime > latest {
                latest = entry_mtime;
            }
        }
    }
    Ok(latest)
}

fn modified_time(root: &Path, path: &Path) -> Result<SystemTime, String> {
    let display = display_workspace_path(root, path);
    fs::metadata(path)
        .map_err(|e| format!("stat {display}: {e}"))?
        .modified()
        .map_err(|e| format!("modified time {display}: {e}"))
}

fn display_workspace_path(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => format!("./{}", relative.display()),
        Err(_) => path.display().to_string(),
    }
}

fn workspace_root() -> PathBuf {
    let git_root = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output();
    if let Ok(output) = git_root {
        if output.status.success() {
            return PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        }
    }
    std::env::current_dir().expect("current directory is available")
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("test") if args.next().is_none() => run_tests(),
        Some("mvp-chat") => run_mvp_chat(args.collect()),
        Some("help" | "--help" | "-h") | None => {
            print_usage();
            ExitCode::SUCCESS
        }
        _ => {
            print_usage();
            ExitCode::from(1)
        }
    }
}
