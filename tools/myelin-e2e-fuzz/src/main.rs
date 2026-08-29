use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use myelin_e2e_fuzz::{
    BehaviorCase, ClusterHarness, ClusterHarnessConfig, failure_corpus, random_short_dags,
    render_case_python, shrink_failure, stable_corpus,
};

struct Options {
    seed: u64,
    nodes: u8,
    random_cases: usize,
    max_cases: Option<usize>,
    failure_cases: bool,
    smoke_only: bool,
    recovery_only: bool,
    artifacts: PathBuf,
    image: Option<String>,
    build_image: bool,
    replay: Option<PathBuf>,
    shrink: bool,
    deadline: Duration,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_secs();
        let mut options = Self {
            seed,
            nodes: 2,
            failure_cases: false,
            random_cases: 16,
            max_cases: None,
            smoke_only: false,
            recovery_only: false,
            artifacts: PathBuf::from("target/e2e-behavioral-fuzz"),
            image: None,
            build_image: true,
            replay: None,
            shrink: true,
            deadline: Duration::ZERO,
        };
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            let mut next = |name: &str| {
                args.next()
                    .ok_or_else(|| format!("{name} requires a value"))
            };
            match argument.as_str() {
                "--seed" => {
                    options.seed = next("--seed")?
                        .parse()
                        .map_err(|error| format!("invalid --seed: {error}"))?;
                }
                "--nodes" => {
                    options.nodes = next("--nodes")?
                        .parse()
                        .map_err(|error| format!("invalid --nodes: {error}"))?;
                }
                "--random-cases" => {
                    options.random_cases = next("--random-cases")?
                        .parse()
                        .map_err(|error| format!("invalid --random-cases: {error}"))?;
                }
                "--max-cases" => {
                    options.max_cases = Some(
                        next("--max-cases")?
                            .parse()
                            .map_err(|error| format!("invalid --max-cases: {error}"))?,
                    );
                }
                "--smoke-only" => options.smoke_only = true,
                "--artifacts" => options.artifacts = PathBuf::from(next("--artifacts")?),
                "--image" => options.image = Some(next("--image")?),
                "--no-build-image" => options.build_image = false,
                "--replay" => options.replay = Some(PathBuf::from(next("--replay")?)),
                "--failure-cases" => options.failure_cases = true,
                "--recovery-only" => options.recovery_only = true,
                "--no-shrink" => options.shrink = false,
                "--deadline-secs" => {
                    options.deadline = Duration::from_secs(
                        next("--deadline-secs")?
                            .parse()
                            .map_err(|error| format!("invalid --deadline-secs: {error}"))?,
                    );
                }
                "--help" | "-h" => {
                    println!(
                        "myelin-e2e-fuzz [--seed N] [--nodes 2|3] [--random-cases N] \
                         [--max-cases N] [--smoke-only] [--failure-cases] [--recovery-only] \
                         [--artifacts PATH] [--image TAG] [--no-build-image] [--no-shrink] \
                         [--replay CASE.json] [--deadline-secs N]"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option {other:?}")),
            }
        }
        if !(2..=3).contains(&options.nodes) {
            return Err("--nodes must be 2 or 3".to_owned());
        }
        if options.deadline.is_zero() {
            return Err("--deadline-secs is required and must be nonzero".to_owned());
        }
        Ok(options)
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("myelin-e2e-behavioral-fuzz: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let workspace = std::env::current_dir()
        .map_err(|error| format!("read current workspace directory: {error}"))?;
    let mut cases = if let Some(replay) = &options.replay {
        vec![
            serde_json::from_slice::<BehaviorCase>(
                &fs::read(replay).map_err(|error| format!("read replay case: {error}"))?,
            )
            .map_err(|error| format!("parse replay case: {error}"))?,
        ]
    } else if options.failure_cases {
        failure_corpus(options.nodes, options.seed)
    } else {
        let mut stable = stable_corpus(options.nodes, options.seed);
        if options.smoke_only {
            stable.truncate(usize::from(options.nodes) * (usize::from(options.nodes) - 1) * 2);
        } else {
            stable.extend(random_short_dags(
                options.seed.rotate_left(17),
                options.random_cases,
                options.nodes,
            ));
        }
        stable
    };
    if let Some(max_cases) = options.max_cases {
        cases.truncate(max_cases);
    }
    if options.recovery_only {
        cases.clear();
    }
    if cases.is_empty() && !options.recovery_only {
        return Err("no behavioral cases selected".to_owned());
    }

    fs::create_dir_all(&options.artifacts)
        .map_err(|error| format!("create artifact root: {error}"))?;
    let config = ClusterHarnessConfig {
        workspace,
        artifacts: options.artifacts.clone(),
        node_count: options.nodes,
        seed: options.seed,
        image: options.image,
        build_image: options.build_image,
        deadline: options.deadline,
    };
    let mut harness = ClusterHarness::start(config.clone())?;
    harness.verify_running_recovery()?;
    if !options.recovery_only {
        harness.verify_workload_convergence()?;
    }
    println!(
        "cluster ready: nodes={:?} image={} cases={} seed={}",
        harness.node_ids(),
        harness.image(),
        cases.len(),
        options.seed
    );

    for (index, case) in cases.iter().enumerate() {
        println!("case {}/{}: {}", index + 1, cases.len(), case.id);
        let result = harness
            .run_case(case)
            .and_then(|_| harness.assert_healthy());
        if let Err(error) = result {
            let image = harness.image().to_owned();
            let _ = harness.teardown();
            persist_regression(&options.artifacts, case, &error)?;
            if !options.shrink {
                return Err(format!(
                    "case {} failed: {error}; replayable regression persisted",
                    case.id
                ));
            }
            let shrink_root = options.artifacts.join("shrink");
            let expected_failure = failure_signature(&error);
            let minimized = shrink_failure(case.clone(), |candidate| {
                let attempt = ClusterHarnessConfig {
                    workspace: config.workspace.clone(),
                    artifacts: shrink_root.join(format!("attempt-{}", candidate.processes.len())),
                    node_count: candidate.node_count,
                    seed: candidate.seed,
                    image: Some(image.clone()),
                    build_image: false,
                    deadline: config.deadline,
                };
                let Ok(mut harness) = ClusterHarness::start(attempt) else {
                    return false;
                };
                let result = harness
                    .run_case(candidate)
                    .and_then(|_| harness.assert_healthy());
                let _ = harness.teardown();
                matches!(
                    result,
                    Err(candidate_error)
                        if failure_signature(&candidate_error) == expected_failure
                )
            });
            let final_attempt = ClusterHarnessConfig {
                workspace: config.workspace.clone(),
                artifacts: shrink_root.join("final"),
                node_count: minimized.node_count,
                seed: minimized.seed,
                image: Some(image),
                build_image: false,
                deadline: config.deadline,
            };
            let mut final_harness = ClusterHarness::start(final_attempt)?;
            let final_result = final_harness
                .run_case(&minimized)
                .and_then(|_| final_harness.assert_healthy());
            let _ = final_harness.teardown();
            let minimized_error = final_result.map_or_else(
                |candidate_error| {
                    if failure_signature(&candidate_error) == expected_failure {
                        Ok(candidate_error)
                    } else {
                        Err(format!(
                            "minimized case changed failure from {expected_failure:?} to {:?}",
                            failure_signature(&candidate_error)
                        ))
                    }
                },
                |()| Err("minimized case no longer reproduces the failure".to_owned()),
            )?;
            persist_regression(&options.artifacts, &minimized, &minimized_error)?;
            return Err(format!(
                "case {} failed: {error}; minimized regression persisted",
                case.id
            ));
        }
    }
    harness.verify_missing_resource_recovery()?;
    harness.teardown()?;
    println!("behavioral fuzz harness completed {} cases", cases.len());
    Ok(())
}

fn failure_signature(error: &str) -> String {
    let first_line = error.lines().next().unwrap_or(error);
    for separator in ["; lifecycle=", "; stdout=", "; health="] {
        if let Some((signature, _)) = first_line.split_once(separator) {
            return signature.to_owned();
        }
    }
    if let Some((invariant, _)) = first_line.split_once(": ")
        && invariant
            .chars()
            .all(|character| character.is_ascii_lowercase() || character == '_')
    {
        return invariant.to_owned();
    }
    first_line.to_owned()
}

fn persist_regression(
    root: &std::path::Path,
    case: &BehaviorCase,
    error: &str,
) -> Result<(), String> {
    let directory = root.join("regressions").join(&case.id);
    fs::create_dir_all(&directory)
        .map_err(|write_error| format!("create regression directory: {write_error}"))?;
    fs::write(
        directory.join("case.json"),
        serde_json::to_vec_pretty(case)
            .map_err(|write_error| format!("serialize minimized case: {write_error}"))?,
    )
    .map_err(|write_error| format!("write minimized case: {write_error}"))?;
    fs::write(directory.join("failure.txt"), error)
        .map_err(|write_error| format!("write minimized failure: {write_error}"))?;
    for process in &case.processes {
        fs::write(
            directory.join(format!("{}.py", process.id)),
            render_case_python(case, process),
        )
        .map_err(|write_error| format!("write minimized Python source: {write_error}"))?;
    }
    Ok(())
}
