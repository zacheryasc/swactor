// Engine boundary enforcement: disallowed scheduling/time/core-driving methods
// are hard errors in this crate (ENGINE_SPEC.md §2). The VastAI
// provider adapter carries a module-level `#![allow]` pending its separate
// redesign; unit tests that drive a raw Runtime in isolation are exempted
// locally.
#![deny(clippy::disallowed_methods)]
#![recursion_limit = "256"]

#[cfg(test)]
extern crate self as myelin;

const DEFAULT_PIPELINE_CACHED_MODEL_FILE: &str = "SmolLM2-135M-Instruct.Q4_0.gguf";
#[doc(hidden)]
pub const ORCHESTRATOR_WORKER_MODE_ARG: &str = "--myelin-worker-node";
pub fn run_orchestrator_from_args<I>(args: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    orchestration::app::run_with_options(args, true, None)
}

pub fn run_worker_node_from_env() -> std::process::ExitCode {
    node::worker_node_runtime::run_from_env()
}

mod job_deploy;
mod job_data_plane;

/// `myelin-job-worker` — GPU-node side of the iroh job runner.
pub fn run_job_worker_from_args<I>(args: I) -> std::process::ExitCode
where
    I: IntoIterator<Item = String>,
{
    let mut orch_identity = None;

    let mut workdir = std::path::PathBuf::from("/root/workspace");
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--orch-identity" => orch_identity = it.next(),
            "--workdir" => workdir = std::path::PathBuf::from(it.next().unwrap_or_default()),
            _ => {}
        }
    }
    let identity = match orch_identity {
        Some(s) => s,
        None => {
            eprintln!("--orch-identity required");
            return std::process::ExitCode::from(2);
        }
    };
    match job_deploy::run_worker(identity, workdir) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("myelin-job-worker: {e}");
            std::process::ExitCode::from(1)
        }
    }
}

/// `myelin-job` — operator side: load a job, drive it over iroh to a worker.
pub fn run_job_serve_from_args<I>(args: I) -> std::process::ExitCode
where
    I: IntoIterator<Item = String>,
{
    let mut job_path = None;

    let mut landing = std::path::PathBuf::from("job-outputs");
    let mut provider = None;
    let mut vastai = match vastai_job_options_from_env() {
        Ok(options) => options,
        Err(error) => {
            eprintln!("myelin-job: {error}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "serve" => {}
            "reconcile-vastai" | "vastai" => provider = Some("vastai".to_owned()),
            "--job" => job_path = it.next(),
            "--landing" => landing = std::path::PathBuf::from(it.next().unwrap_or_default()),
            "--provider" => provider = it.next(),
            "--image" | "--node-image" => vastai.image = it.next(),
            "--vastai-api-key" => vastai.api_key = it.next(),
            "--vastai-ssh-identity" => match it.next() {
                Some(path) => match orchestration::app::expand_home_path(&path) {
                    Ok(path) => vastai.ssh_identity = Some(path),
                    Err(error) => {
                        eprintln!("myelin-job: {error}");
                        return std::process::ExitCode::from(2);
                    }
                },
                None => {}
            },
            "--remote-worker-bin" => {
                vastai.remote_worker_bin = it.next().unwrap_or_default();
            }
            "--worker-workdir" => {
                vastai.worker_workdir = it.next().unwrap_or_default();
            }
            "--run-id" => match parse_next(&mut it, "--run-id") {
                Ok(value) => vastai.run_id = value,
                Err(error) => return job_arg_error(error),
            },
            "--node-id" => match parse_next(&mut it, "--node-id") {
                Ok(value) => vastai.node_id = value,
                Err(error) => return job_arg_error(error),
            },
            "--relay-mode" => vastai.relay_mode = it.next(),
            "--relay-url" => vastai.relay_url = it.next(),
            "--endpoint-addr-mask" => {
                vastai.endpoint_addr_mask = it.next().unwrap_or_default();
            }
            "--vastai-disk-gb" => match parse_next(&mut it, "--vastai-disk-gb") {
                Ok(value) => vastai.disk_gb = value,
                Err(error) => return job_arg_error(error),
            },
            "--vastai-ssh-user" => vastai.ssh_user = it.next().unwrap_or_default(),
            "--vastai-gpu-name" => vastai.gpu_name = it.next(),
            "--vastai-min-gpu-ram-mb" => match parse_next(&mut it, "--vastai-min-gpu-ram-mb") {
                Ok(value) => vastai.min_gpu_ram_mb = Some(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-min-down-mbps" => match parse_next(&mut it, "--vastai-min-down-mbps") {
                Ok(value) => vastai.min_down_mbps = Some(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-min-up-mbps" => match parse_next(&mut it, "--vastai-min-up-mbps") {
                Ok(value) => vastai.min_up_mbps = Some(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-max-dph-total" => match parse_next(&mut it, "--vastai-max-dph-total") {
                Ok(value) => vastai.max_dph_total = Some(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-min-reliability" => match parse_next(&mut it, "--vastai-min-reliability") {
                Ok(value) => vastai.min_reliability = Some(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-require-verified" => {
                match parse_next_bool(&mut it, "--vastai-require-verified") {
                    Ok(value) => vastai.require_verified = Some(value),
                    Err(error) => return job_arg_error(error),
                }
            }
            "--vastai-blacklist-host" => match parse_next(&mut it, "--vastai-blacklist-host") {
                Ok(value) => vastai.blacklist_hosts.push(value),
                Err(error) => return job_arg_error(error),
            },
            "--vastai-blacklist-hosts" => match it.next() {
                Some(value) => match parse_csv::<u64>("MYELIN_VASTAI_BLACKLIST_HOSTS", &value) {
                    Ok(values) => vastai.blacklist_hosts.extend(values),
                    Err(error) => return job_arg_error(error),
                },
                None => {
                    return job_arg_error("--vastai-blacklist-hosts requires a value".to_owned());
                }
            },
            "--vastai-poll-interval-secs" => {
                match parse_next::<u64>(&mut it, "--vastai-poll-interval-secs") {
                    Ok(value) => vastai.poll_interval = Some(std::time::Duration::from_secs(value)),
                    Err(error) => return job_arg_error(error),
                }
            }
            "--provision-timeout-secs" | "--wait-secs" => {
                match parse_next::<u64>(&mut it, "--provision-timeout-secs") {
                    Ok(value) => vastai.provision_timeout = std::time::Duration::from_secs(value),
                    Err(error) => return job_arg_error(error),
                }
            }
            "--vastai-onstart" => vastai.onstart = it.next(),
            "--vastai-confirm-lease" => vastai.confirm_lease = true,
            "--no-vastai-confirm-lease" => vastai.confirm_lease = false,
            _ => {}
        }
    }
    let job_path = match job_path {
        Some(p) => p,
        None => {
            eprintln!("--job required");
            return std::process::ExitCode::from(2);
        }
    };
    let text = match std::fs::read_to_string(&job_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("read job {}: {e}", job_path);
            return std::process::ExitCode::from(2);
        }
    };
    #[derive(serde::Deserialize)]
    struct JobFile {
        job: swactor_job_runner::Job,
    }
    let file: JobFile = match toml::from_str(&text) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("parse job toml: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let result = match provider.as_deref() {
        None | Some("manual") | Some("stdio") => job_deploy::run_serve(file.job, landing),
        Some("vastai") => orchestration::job_reconciler::run_vastai_job(file.job, landing, vastai),
        Some(other) => {
            eprintln!("unsupported myelin-job provider {other:?}; use vastai or omit --provider");
            return std::process::ExitCode::from(2);
        }
    };
    match result {
        Ok(done) => {
            eprintln!("job result: {:?} exit={:?}", done.state, done.exit_code);
            if done.state == swactor_job_runner::JobState::Completed {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::from(1)
            }
        }
        Err(e) => {
            eprintln!("myelin-job: {e}");
            std::process::ExitCode::from(1)
        }
    }
}

fn vastai_job_options_from_env() -> Result<orchestration::job_reconciler::VastAiJobOptions, String>
{
    let mut options = orchestration::job_reconciler::VastAiJobOptions::default();
    options.api_key = first_env(["VAST_API_KEY", "MYELIN_VASTAI_API_KEY", "VASTAI_API_KEY"]);
    options.image = env_optional("MYELIN_NODE_IMAGE");
    options.ssh_identity = env_optional("MYELIN_VASTAI_SSH_IDENTITY")
        .map(|path| orchestration::app::expand_home_path(&path))
        .transpose()?;
    apply_env("MYELIN_JOB_REMOTE_WORKER_BIN", |value| {
        options.remote_worker_bin = value;
        Ok(())
    })?;
    apply_env("MYELIN_JOB_WORKER_WORKDIR", |value| {
        options.worker_workdir = value;
        Ok(())
    })?;
    apply_env_parse("MYELIN_RUN_ID", |value| options.run_id = value)?;
    apply_env_parse("MYELIN_LOGICAL_NODE_ID", |value| options.node_id = value)?;
    apply_env_parse("MYELIN_VASTAI_DISK_GB", |value| options.disk_gb = value)?;
    apply_env("MYELIN_VASTAI_SSH_USER", |value| {
        options.ssh_user = value;
        Ok(())
    })?;
    apply_env_parse_bool("MYELIN_VASTAI_CONFIRM_LEASE", |value| {
        options.confirm_lease = value;
    })?;
    options.onstart = env_optional("MYELIN_VASTAI_ONSTART");
    options.relay_mode = env_optional("MYELIN_IROH_RELAY_MODE").or(options.relay_mode);
    options.relay_url =
        first_env(["MYELIN_IROH_RELAY_URL", "SWACTOR_IROH_RELAY_URL"]).or(options.relay_url);
    if let Some(mask) = env_optional("MVP_IROH_ENDPOINT_ADDR_MASK") {
        options.endpoint_addr_mask = mask;
    }
    options.gpu_name = env_optional("MYELIN_VASTAI_GPU_NAME");
    apply_env_parse("MYELIN_VASTAI_MIN_GPU_RAM_MB", |value| {
        options.min_gpu_ram_mb = Some(value)
    })?;
    apply_env_parse("MYELIN_VASTAI_MIN_DOWN_MBPS", |value| {
        options.min_down_mbps = Some(value)
    })?;
    apply_env_parse("MYELIN_VASTAI_MIN_UP_MBPS", |value| {
        options.min_up_mbps = Some(value)
    })?;
    apply_env_parse("MYELIN_VASTAI_MAX_DPH_TOTAL", |value| {
        options.max_dph_total = Some(value)
    })?;
    apply_env_parse("MYELIN_VASTAI_MIN_RELIABILITY", |value| {
        options.min_reliability = Some(value)
    })?;
    apply_env_parse_bool("MYELIN_VASTAI_REQUIRE_VERIFIED", |value| {
        options.require_verified = Some(value)
    })?;
    if let Some(value) = env_optional("MYELIN_VASTAI_BLACKLIST_HOSTS") {
        options
            .blacklist_hosts
            .extend(parse_csv::<u64>("MYELIN_VASTAI_BLACKLIST_HOSTS", &value)?);
    }
    apply_env_parse("MYELIN_VASTAI_POLL_INTERVAL_SECS", |value: u64| {
        options.poll_interval = Some(std::time::Duration::from_secs(value))
    })?;
    apply_env_parse("MYELIN_JOB_PROVISION_TIMEOUT_SECS", |value: u64| {
        options.provision_timeout = std::time::Duration::from_secs(value)
    })?;
    Ok(options)
}

fn job_arg_error(error: String) -> std::process::ExitCode {
    eprintln!("myelin-job: {error}");
    std::process::ExitCode::from(2)
}

fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn first_env<const N: usize>(names: [&str; N]) -> Option<String> {
    names.into_iter().find_map(env_optional)
}

fn apply_env<F>(name: &str, mut apply: F) -> Result<(), String>
where
    F: FnMut(String) -> Result<(), String>,
{
    if let Some(value) = env_optional(name) {
        apply(value)?;
    }
    Ok(())
}

fn apply_env_parse<T, F>(name: &str, mut apply: F) -> Result<(), String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
    F: FnMut(T),
{
    if let Some(value) = env_optional(name) {
        apply(parse_value(name, &value)?);
    }
    Ok(())
}

fn apply_env_parse_bool<F>(name: &str, mut apply: F) -> Result<(), String>
where
    F: FnMut(bool),
{
    if let Some(value) = env_optional(name) {
        apply(parse_bool(name, &value)?);
    }
    Ok(())
}

fn parse_next<T>(it: &mut impl Iterator<Item = String>, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = it
        .next()
        .ok_or_else(|| format!("{name} requires a value"))?;
    parse_value(name, &value)
}

fn parse_next_bool(it: &mut impl Iterator<Item = String>, name: &str) -> Result<bool, String> {
    let value = it
        .next()
        .ok_or_else(|| format!("{name} requires a value"))?;
    parse_bool(name, &value)
}

fn parse_value<T>(name: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .map_err(|e| format!("invalid {name}={value:?}: {e}"))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
        )),
    }
}

fn parse_csv<T>(name: &str, value: &str) -> Result<Vec<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| parse_value(name, part))
        .collect()
}

#[path = "staging/gguf_common.rs"]
mod gguf_common;
#[path = "staging/gguf_shard.rs"]
mod gguf_shard;
#[path = "node/actor.rs"]
mod node_actor;
#[path = "orchestration/node_provisioning.rs"]
mod node_provisioning;
#[path = "orchestration/provisioning.rs"]
mod provisioning;
#[path = "orchestration/run_fsm.rs"]
mod run_fsm;

#[path = "orchestration/run_plan.rs"]
mod run_plan;

mod codecs;
mod node;
mod observability;
mod orchestration;
mod staging;

#[cfg(test)]
mod tests;
