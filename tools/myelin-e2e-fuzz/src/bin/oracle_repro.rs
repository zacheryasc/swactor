use myelin_e2e_fuzz::{BehaviorCase, BehaviorOracle, CaseObservation};
use std::env;
use std::fs;
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: oracle_repro <case.json> <observed.json>");
        std::process::exit(2);
    }
    let mut case: BehaviorCase =
        serde_json::from_slice(&fs::read(&args[1]).expect("read case")).expect("parse case");
    let observation: CaseObservation =
        serde_json::from_slice(&fs::read(&args[2]).expect("read observation"))
            .expect("parse observation");
    if let Ok(budget) = std::env::var("REPRO_RACE_BUDGET") {
        case.resource_bounds.max_race_states = budget.parse().expect("budget integer");
    }
    let started = Instant::now();
    match BehaviorOracle::verify(&case, &observation) {
        Ok(()) => {
            println!("ORACLE OK in {:?}", started.elapsed());
        }
        Err(violation) => {
            println!(
                "ORACLE VIOLATION {} {} in {:?}",
                violation.invariant,
                violation.detail,
                started.elapsed()
            );
            std::process::exit(1);
        }
    }
}
