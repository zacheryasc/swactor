//! sim-driver — entry point that runs the simulation engine against
//! a topology spec and writes the SPEC §6.4 bundle to disk.

use std::path::PathBuf;
use std::process::ExitCode;

use simulation::{run_to_tempdir, SimError};

const HELP: &str = "\
sim-driver — run a topology spec through the simulation engine.

USAGE:
    sim-driver <spec.toml> [--seed N]

OPTIONS:
    --seed N       Run seed (u64). Defaults to 0.
    --help         Show this message.

OUTPUT:
    On success, prints the bundle path on stdout and exits 0.
    Bundle layout follows SPEC §6.4.
";

fn main() -> ExitCode {
    let mut spec_path: Option<PathBuf> = None;
    let mut seed: u64 = 0;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("{HELP}");
                return ExitCode::SUCCESS;
            }
            "--seed" => {
                let value = match args.next() {
                    Some(v) => v,
                    None => {
                        eprintln!("sim-driver: --seed requires a u64 value");
                        return ExitCode::from(2);
                    }
                };
                seed = match value.parse() {
                    Ok(n) => n,
                    Err(err) => {
                        eprintln!("sim-driver: --seed {value:?} is not a valid u64: {err}");
                        return ExitCode::from(2);
                    }
                };
            }
            other if other.starts_with("--") => {
                eprintln!("sim-driver: unknown option {other:?}\n{HELP}");
                return ExitCode::from(2);
            }
            _ => {
                if spec_path.is_some() {
                    eprintln!("sim-driver: extra positional argument {arg:?}\n{HELP}");
                    return ExitCode::from(2);
                }
                spec_path = Some(PathBuf::from(arg));
            }
        }
    }

    let path = match spec_path {
        Some(p) => p,
        None => {
            eprintln!("sim-driver: missing topology spec path\n{HELP}");
            return ExitCode::from(2);
        }
    };

    let spec_text = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(err) => {
            eprintln!(
                "sim-driver: cannot read spec {}: {err}",
                path.display()
            );
            return ExitCode::from(1);
        }
    };

    match run_to_tempdir(&spec_text, seed) {
        Ok(bundle) => {
            println!("{}", bundle.root.display());
            ExitCode::SUCCESS
        }
        Err(SimError::NotImplemented) => {
            eprintln!("sim-driver: simulation engine is not implemented yet");
            ExitCode::from(75)
        }
        Err(err) => {
            eprintln!("sim-driver: run failed: {err}");
            ExitCode::from(1)
        }
    }
}
