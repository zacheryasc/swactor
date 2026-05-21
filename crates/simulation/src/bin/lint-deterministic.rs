use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use simulation::lint::{Config, Violation};

const HELP: &str = "\
lint-deterministic — banned-API + parity-bar hygiene scanner

USAGE:
    lint-deterministic [OPTIONS]

OPTIONS:
    --check-parity-bar     Run §12.2/§12.3/§12.4 checks against
                           crates/simulation/tests/parity-bar/.
    --workspace-root PATH  Override the workspace root.
                           Defaults to CARGO_MANIFEST_DIR/../.. .
    --config PATH          Override the banned.toml path.
    --quiet                Suppress per-violation output; print only summary.
    --help                 Show this message.
";

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut mode = Mode::Workspace;
    let mut quiet = false;
    let mut workspace_root: Option<PathBuf> = None;
    let mut config_path: Option<PathBuf> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--check-parity-bar" => mode = Mode::ParityBar,
            "--quiet" => quiet = true,
            "--help" => {
                println!("{HELP}");
                return ExitCode::SUCCESS;
            }
            "--workspace-root" => {
                workspace_root = iter.next().map(PathBuf::from);
            }
            "--config" => {
                config_path = iter.next().map(PathBuf::from);
            }
            other => {
                eprintln!("lint-deterministic: unknown argument {other:?}\n{HELP}");
                return ExitCode::from(2);
            }
        }
    }

    let workspace_root = workspace_root.unwrap_or_else(default_workspace_root);
    let config_path = config_path
        .unwrap_or_else(|| workspace_root.join("crates/simulation/banned.toml"));

    let config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "lint-deterministic: failed to load config at {}: {e}",
                config_path.display()
            );
            return ExitCode::from(2);
        }
    };

    let violations = match mode {
        Mode::Workspace => simulation::lint::scan_banned_apis(&workspace_root, &config),
        Mode::ParityBar => simulation::lint::scan_parity_bar(&workspace_root, &config),
    };
    let violations = match violations {
        Ok(v) => v,
        Err(e) => {
            eprintln!("lint-deterministic: scan failed: {e}");
            return ExitCode::from(2);
        }
    };

    report(&violations, mode, quiet);
    if violations.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

#[derive(Copy, Clone)]
enum Mode {
    Workspace,
    ParityBar,
}

fn report(violations: &[Violation], mode: Mode, quiet: bool) {
    let label = match mode {
        Mode::Workspace => "banned-API scan",
        Mode::ParityBar => "parity-bar hygiene scan",
    };
    if violations.is_empty() {
        if !quiet {
            println!("lint-deterministic: {label} — no violations.");
        }
        return;
    }
    if !quiet {
        for v in violations {
            eprintln!("{v}");
        }
    }
    eprintln!(
        "lint-deterministic: {label} — {} violation(s).",
        violations.len()
    );
}

fn default_workspace_root() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    Path::new(manifest_dir)
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(manifest_dir))
}
