//! `swactor-diag-postproc` — turn a finalized diagnostics bundle into
//! a human-readable report.
//!
//! ## Usage
//!
//! ```text
//! swactor-diag-postproc <bundle.tar.gz> [-o OUT_DIR]
//! swactor-diag-postproc diff <bundle-a.tar.gz> <bundle-b.tar.gz>
//! ```
//!
//! Default `OUT_DIR` is `<bundle>.out/` (sibling to the input). The
//! tool writes `summary.md`, `reachability.tsv`, and one
//! `timeline-{a}-to-{b}.tsv` per ordered pair of nodes named in the
//! bundle's manifest. The `diff` subcommand prints to stdout — pipe to
//! a file if you want it persisted.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use distribution::diagnostics::postproc::{render_diff, Bundle, Outputs};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match parse_args(&args) {
        Ok(Cmd::Render { input, out_dir }) => run_render(&input, out_dir.as_deref()),
        Ok(Cmd::Diff { a, b }) => run_diff(&a, &b),
        Ok(Cmd::Help) => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("swactor-diag-postproc: {e}");
            eprintln!();
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

const USAGE: &str = "Usage:
  swactor-diag-postproc <bundle.tar.gz> [-o OUT_DIR]
  swactor-diag-postproc diff <a.tar.gz> <b.tar.gz>
  swactor-diag-postproc -h | --help

Default OUT_DIR is <bundle>.out/, sibling to the input bundle.";

#[derive(Debug)]
enum Cmd {
    Render {
        input: PathBuf,
        out_dir: Option<PathBuf>,
    },
    Diff {
        a: PathBuf,
        b: PathBuf,
    },
    Help,
}

fn parse_args(args: &[String]) -> Result<Cmd, String> {
    let mut iter = args.iter().skip(1);
    let Some(first) = iter.next() else {
        return Err("missing arguments".into());
    };
    match first.as_str() {
        "-h" | "--help" => Ok(Cmd::Help),
        "diff" => {
            let a = iter
                .next()
                .ok_or_else(|| "diff: missing first bundle path".to_string())?;
            let b = iter
                .next()
                .ok_or_else(|| "diff: missing second bundle path".to_string())?;
            if iter.next().is_some() {
                return Err("diff: too many arguments".into());
            }
            Ok(Cmd::Diff {
                a: PathBuf::from(a),
                b: PathBuf::from(b),
            })
        }
        other if other.starts_with('-') => Err(format!("unrecognized argument: {other}")),
        path => {
            let input = PathBuf::from(path);
            let mut out_dir: Option<PathBuf> = None;
            while let Some(flag) = iter.next() {
                match flag.as_str() {
                    "-o" | "--out" => {
                        let dir = iter
                            .next()
                            .ok_or_else(|| "{flag} expects a directory".to_string())?;
                        out_dir = Some(PathBuf::from(dir));
                    }
                    other => return Err(format!("unrecognized argument: {other}")),
                }
            }
            Ok(Cmd::Render { input, out_dir })
        }
    }
}

fn run_render(input: &Path, out_dir: Option<&Path>) -> ExitCode {
    let bundle = match Bundle::parse_path(input) {
        Ok(b) => b,
        Err(e) => {
            eprintln!(
                "swactor-diag-postproc: parse {}: {e}",
                input.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let outputs = Outputs::from_bundle(&bundle);
    let out_dir = match out_dir {
        Some(p) => p.to_path_buf(),
        None => default_out_dir(input),
    };
    match outputs.write_all_to(&out_dir) {
        Ok(paths) => {
            eprintln!("Wrote {} files to {}", paths.len(), out_dir.display());
            for p in paths {
                println!("{}", p.display());
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("swactor-diag-postproc: write {}: {e}", out_dir.display());
            ExitCode::FAILURE
        }
    }
}

fn run_diff(a_path: &Path, b_path: &Path) -> ExitCode {
    let a = match Bundle::parse_path(a_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("swactor-diag-postproc: parse {}: {e}", a_path.display());
            return ExitCode::FAILURE;
        }
    };
    let b = match Bundle::parse_path(b_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("swactor-diag-postproc: parse {}: {e}", b_path.display());
            return ExitCode::FAILURE;
        }
    };
    print!("{}", render_diff(&a, &b));
    ExitCode::SUCCESS
}

fn default_out_dir(input: &Path) -> PathBuf {
    let name = input
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "bundle".to_string());
    let stem = name.trim_end_matches(".tar.gz").trim_end_matches(".tgz");
    let out_name = format!("{stem}.out");
    match input.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(out_name),
        _ => PathBuf::from(out_name),
    }
}
