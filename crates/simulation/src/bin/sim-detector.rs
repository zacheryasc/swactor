use std::process::ExitCode;

use simulation::detector::{run_all, verdicts_to_json, Verdict};

const HELP: &str = "\
sim-detector — peer binary that probes the runtime for sim/prod tells.

USAGE:
    sim-detector [--emit-json]

OPTIONS:
    --emit-json    Print verdicts as a single JSON array to stdout
                   (machine-readable). Without this flag the binary
                   prints one line per technique in a human-readable
                   form.
    --help         Show this message.

EXIT CODE:
    0 — no technique returned `DetectedSim`. A `DetectedProd` verdict
        is informational (it means the detector thinks it's in prod,
        which is fine if the run actually is prod).
    1 — at least one technique returned `DetectedSim`, which is a
        parity violation in any sim run (TESTING_SPEC §10.1).
";

fn main() -> ExitCode {
    let mut emit_json = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--emit-json" => emit_json = true,
            "--help" => {
                println!("{HELP}");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("sim-detector: unknown argument {other:?}\n{HELP}");
                return ExitCode::from(2);
            }
        }
    }

    let verdicts = run_all();
    if emit_json {
        println!("{}", verdicts_to_json(&verdicts));
    } else {
        for (id, verdict) in &verdicts {
            println!("{id}: {}", verdict.outcome_str());
        }
    }

    let any_detected_sim = verdicts
        .iter()
        .any(|(_, v)| matches!(v, Verdict::DetectedSim(_)));
    if any_detected_sim {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
