#![cfg(not(test))]

use std::path::{Path, PathBuf};
use std::time::Duration;

use myelin_e2e_fuzz::{
    Budget, HarnessExecution, read_paid_state, scan_artifacts_for_secret, write_paid_state,
};
use swactor_engine::{BlockingWorkSender, Engine, TokioBackend, TokioConfig};

const USAGE: &str = "myelin-e2e-fuzz --plan-campaign CONFIG.json [--regressions CASES.json] --artifacts PATH --deadline-secs N\n\
    myelin-e2e-fuzz --prepare-artifacts --artifacts PATH --deadline-secs N\n\
    myelin-e2e-fuzz --prepare-python-artifacts --artifacts PATH --deadline-secs N\n\
    myelin-e2e-fuzz --cleanup-only PAID_STATE.json --deadline-secs N [--api-key-env NAME]\n\
    myelin-e2e-fuzz --paid-cleanup-supervisor ADMISSION.json [--api-key-env NAME]\n\
    myelin-e2e-fuzz --paid-cleanup-owner ADMISSION.json [--api-key-env NAME]\n\
    myelin-e2e-fuzz --scripted-retained-lifecycle ADMISSION.json [--api-key-env NAME]";

enum Command {
    PlanCampaign(PathBuf),
    PrepareArtifacts,
    PreparePythonArtifacts,
    CleanupOnly(PathBuf),
    CleanupSupervisor(PathBuf),
    CleanupOwner(PathBuf),
    ScriptedRetainedLifecycle(PathBuf),
}

struct Options {
    command: Command,
    artifacts: PathBuf,
    deadline: Duration,
    api_key_env: String,
    regressions: Option<PathBuf>,
}

impl Options {
    fn parse() -> Result<Option<Self>, String> {
        let mut command = None;
        let mut artifacts = PathBuf::from("target/e2e-behavioral-fuzz");
        let mut deadline = Duration::ZERO;
        let mut api_key_env = "VASTAI_API_KEY".to_owned();
        let mut regressions = None;
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            let mut next = || {
                args.next()
                    .ok_or_else(|| format!("{argument} requires a value"))
            };
            let selected = match argument.as_str() {
                "--plan-campaign" => Some(Command::PlanCampaign(PathBuf::from(next()?))),
                "--prepare-artifacts" => Some(Command::PrepareArtifacts),
                "--prepare-python-artifacts" => Some(Command::PreparePythonArtifacts),
                "--cleanup-only" => Some(Command::CleanupOnly(PathBuf::from(next()?))),
                "--paid-cleanup-supervisor" => {
                    Some(Command::CleanupSupervisor(PathBuf::from(next()?)))
                }
                "--paid-cleanup-owner" => Some(Command::CleanupOwner(PathBuf::from(next()?))),
                "--scripted-retained-lifecycle" => {
                    Some(Command::ScriptedRetainedLifecycle(PathBuf::from(next()?)))
                }
                "--artifacts" => {
                    artifacts = PathBuf::from(next()?);
                    None
                }
                "--regressions" => {
                    regressions = Some(PathBuf::from(next()?));
                    None
                }
                "--deadline-secs" => {
                    deadline = Duration::from_secs(
                        next()?
                            .parse()
                            .map_err(|error| format!("invalid --deadline-secs: {error}"))?,
                    );
                    None
                }
                "--api-key-env" => {
                    api_key_env = next()?;
                    None
                }
                "--help" | "-h" => {
                    println!("{USAGE}");
                    return Ok(None);
                }
                other => return Err(format!("unknown option {other:?}\n{USAGE}")),
            };
            if let Some(selected) = selected {
                if command.replace(selected).is_some() {
                    return Err("select exactly one resource or cleanup command".to_owned());
                }
            }
        }
        let command = command.ok_or_else(|| format!("a command is required\n{USAGE}"))?;
        if matches!(
            command,
            Command::PlanCampaign(_)
                | Command::PrepareArtifacts
                | Command::PreparePythonArtifacts
                | Command::CleanupOnly(_)
        ) && deadline.is_zero()
        {
            return Err("--deadline-secs is required and must be nonzero".to_owned());
        }
        Ok(Some(Self {
            command,
            artifacts,
            deadline,
            api_key_env,
            regressions,
        }))
    }
}

fn main() {
    let options = match Options::parse() {
        Ok(Some(options)) => options,
        Ok(None) => return,
        Err(error) => {
            eprintln!("myelin-e2e-fuzz: {error}");
            std::process::exit(1);
        }
    };
    let parts = swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine =
        TokioBackend::new(TokioConfig::default()).and_then(|backend| Engine::new(parts, backend));
    let result = match engine {
        Ok(engine) => {
            let execution = HarnessExecution {
                runtime,
                engine: engine.handle(),
                tasks: engine.task_spawner(),
                blocking: engine.handle().blocking_work_sender(),
            };
            run(&options, &execution)
        }
        Err(error) => Err(format!("start tool execution engine: {error}")),
    };
    if let Err(error) = result {
        eprintln!("myelin-e2e-fuzz: {error}");
        std::process::exit(1);
    }
}

fn run(options: &Options, execution: &HarnessExecution) -> Result<(), String> {
    match &options.command {
        Command::PlanCampaign(path) => plan_campaign(path, options),
        Command::PrepareArtifacts | Command::PreparePythonArtifacts => {
            let workspace = std::env::current_dir()
                .map_err(|error| format!("read workspace directory: {error}"))?;
            let budget = Budget::new(options.deadline);
            if matches!(options.command, Command::PreparePythonArtifacts) {
                myelin_e2e_fuzz::stage_python_conformance_payload(
                    &workspace,
                    &options.artifacts,
                    &budget,
                )?;
            } else {
                myelin_e2e_fuzz::stage_deployment_payload(&workspace, &options.artifacts, &budget)?;
            }
            budget.check("complete immutable workload preparation")
        }
        Command::CleanupOnly(path) => cleanup_only(&execution.blocking, path, options),
        Command::CleanupSupervisor(path) => {
            myelin_e2e_fuzz::run_cleanup_supervisor(path, &options.api_key_env)
        }
        Command::CleanupOwner(path) => {
            myelin_e2e_fuzz::run_cleanup_owner(path, &options.api_key_env)
        }
        Command::ScriptedRetainedLifecycle(path) => {
            myelin_e2e_fuzz::run_scripted_retained_lifecycle(
                &execution.blocking,
                path,
                &options.api_key_env,
            )
        }
    }
}

fn plan_campaign(config_path: &Path, options: &Options) -> Result<(), String> {
    let config = serde_json::from_slice::<myelin_e2e_fuzz::CampaignConfig>(
        &std::fs::read(config_path)
            .map_err(|error| format!("read campaign config {}: {error}", config_path.display()))?,
    )
    .map_err(|error| format!("decode campaign config {}: {error}", config_path.display()))?;
    let regressions = options
        .regressions
        .as_ref()
        .map(|path| {
            serde_json::from_slice::<Vec<myelin_e2e_fuzz::BehaviorCase>>(
                &std::fs::read(path).map_err(|error| {
                    format!("read regression corpus {}: {error}", path.display())
                })?,
            )
            .map_err(|error| format!("decode regression corpus {}: {error}", path.display()))
        })
        .transpose()?
        .unwrap_or_default();
    let budget = Budget::new(options.deadline);
    let plan = myelin_e2e_fuzz::CampaignPlan::build_with_budget(&config, regressions, &budget)?;
    std::fs::create_dir_all(&options.artifacts)
        .map_err(|error| format!("create campaign artifact directory: {error}"))?;
    myelin_e2e_fuzz::write_campaign_plan(
        &options.artifacts.join("campaign-plan.json"),
        &plan,
        &budget,
    )?;
    for (phase, ledger) in [
        ("normal", &plan.normal_coverage),
        ("four-node", &plan.four_node_coverage),
        ("three-node", &plan.three_node_coverage),
    ] {
        myelin_e2e_fuzz::write_coverage_ledger(
            &options.artifacts.join(format!("{phase}-coverage.json")),
            &plan.campaign_id,
            phase,
            ledger,
        )?;
    }
    println!("{}", options.artifacts.join("campaign-plan.json").display());
    Ok(())
}


fn cleanup_only(
    blocking_work: &BlockingWorkSender,
    state_path: &Path,
    options: &Options,
) -> Result<(), String> {
    let mut state = read_paid_state(state_path)?;
    state.enter_cleanup_only();
    let initial_persistence = write_paid_state(state_path, &state);
    let cleanup = myelin_e2e_fuzz::cleanup_paid_ownership(
        blocking_work,
        &mut state,
        state_path,
        &std::env::current_exe().map_err(|error| error.to_string())?,
        &options.api_key_env,
        options.deadline,
    );
    let persistence = write_paid_state(state_path, &state);
    let secret_scan = (|| -> Result<(), String> {
        let secret = std::env::var(&options.api_key_env)
            .map_err(|_| "paid credential environment is unset".to_owned())?;
        let campaign = state_path.parent().ok_or("paid state parent missing")?;
        for root in [
            options.artifacts.as_path(),
            state.state_dir.as_path(),
            campaign,
        ] {
            if root.exists() {
                scan_artifacts_for_secret(root, secret.as_bytes())?;
            }
        }
        Ok(())
    })();
    let errors = [initial_persistence, cleanup, persistence, secret_scan]
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
