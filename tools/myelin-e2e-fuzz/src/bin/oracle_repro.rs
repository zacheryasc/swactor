use myelin_control_contract::RetainedBlobsReply;
use myelin_e2e_fuzz::{BehaviorCase, BehaviorOracle, Budget, CaseObservation};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = env::args().collect();
    if !matches!(args.len(), 3 | 4) {
        eprintln!("usage: oracle_repro <case.json> <observed.json> [retained-blobs.json]");
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
    let retained = (args.len() == 4).then(|| {
        let reply: RetainedBlobsReply =
            serde_json::from_slice(&fs::read(&args[3]).expect("read retained blobs"))
                .expect("parse retained blobs");
        reply
            .blobs
            .into_iter()
            .map(|(path, blob)| (path, (blob.revision, blob.length)))
            .collect::<BTreeMap<_, _>>()
    });
    let started = Instant::now();
    let result = if let Some(retained) = &retained {
        BehaviorOracle::verify_retained_blobs_with_budget(
            &case,
            &observation,
            retained,
            &Budget::new(Duration::from_secs(120)),
        )
    } else {
        BehaviorOracle::verify(&case, &observation)
    };
    match result {
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
