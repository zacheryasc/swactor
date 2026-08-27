//! Contextual process control: case execution, spawn/stop, failure injection.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Write as IoWrite;
use std::thread;
use std::time::Instant;

use base64::Engine as _;
use serde_json::{Value, json};

use crate::codegen::{case_cleanup_paths, render_case_python, render_cleanup_python};
use crate::harness::ClusterHarness;
use crate::ir::{
    AccessSpec, ActionObservation, BehaviorCase, CaseObservation, ExecutionObservation,
    FailureInjection, LaunchFailureKind, ProcessProgram, ProcessStopPhase,
};
use crate::oracle::BehaviorOracle;
use crate::resources::{http_json, list_containers, list_workload_processes};

use super::{GENERATED_LAUNCHER, POLL_INTERVAL};

const GENERATED_SOURCE_ENV: &str = "MYELIN_E2E_PROGRAM_B64";
const BOOTSTRAP_HOLD_ENV: &str = "MYELIN_E2E_HOLD_BEFORE_BOOTSTRAP";
const MAX_CONTEXTUAL_SOURCE_ENV_BYTES: usize = 48 * 1024;

impl ClusterHarness {
    pub fn run_case(&mut self, case: &BehaviorCase) -> Result<CaseObservation, String> {
        case.validate()?;
        if case.node_count != self.config.node_count {
            return Err(format!(
                "case topology {} differs from harness topology {}",
                case.node_count, self.config.node_count
            ));
        }
        let case_dir = self.config.artifacts.join(&case.id);
        fs::create_dir_all(&case_dir)
            .map_err(|error| format!("create case artifact directory: {error}"))?;
        fs::write(
            case_dir.join("case.json"),
            serde_json::to_vec_pretty(case)
                .map_err(|error| format!("serialize case IR: {error}"))?,
        )
        .map_err(|error| format!("write case IR: {error}"))?;
        let sources = case
            .processes
            .iter()
            .map(|program| (program.id.clone(), render_case_python(case, program)))
            .collect::<BTreeMap<_, _>>();
        for (process, source) in &sources {
            fs::write(case_dir.join(format!("{process}.py")), source)
                .map_err(|error| format!("write generated program {process}: {error}"))?;
        }

        let mut completed = BTreeSet::new();
        let mut pending = case
            .processes
            .iter()
            .map(|process| process.id.clone())
            .collect::<BTreeSet<_>>();
        let mut observations = Vec::new();
        let mut failure_injected = false;
        let mut running = VecDeque::new();
        while !pending.is_empty() || !running.is_empty() {
            let ready = case
                .processes
                .iter()
                .filter(|process| pending.contains(&process.id))
                .filter(|process| {
                    process
                        .depends_on
                        .iter()
                        .all(|dependency| completed.contains(dependency))
                })
                .collect::<Vec<_>>();
            if ready.is_empty() && running.is_empty() {
                return Err("case dependency graph made no progress".to_owned());
            }
            let prioritize_new = !running.is_empty();
            let mut launched = Vec::new();
            for process in ready {
                let request_id = format!("{}-{}-{}", case.id, process.id, case.seed);
                let stop_injection = match &case.failure {
                    FailureInjection::StopProcess {
                        process: target,
                        phase,
                        kill_after_ms,
                    } if !failure_injected && target == &process.id => {
                        Some((*phase, *kill_after_ms))
                    }
                    _ => None,
                };
                let launch_failure = match &case.failure {
                    FailureInjection::LaunchFailure {
                        process: target,
                        kind,
                    } if target == &process.id => Some(*kind),
                    _ => None,
                };
                let hold_before_bootstrap = stop_injection
                    .is_some_and(|(phase, _)| phase == ProcessStopPhase::DuringBootstrap);
                self.spawn_program(
                    process,
                    &request_id,
                    &sources[&process.id],
                    hold_before_bootstrap,
                    launch_failure,
                )?;
                let deferred_stop = if let Some((phase, kill_after_ms)) = stop_injection {
                    failure_injected = true;
                    match phase {
                        ProcessStopPhase::AfterSpawn => {
                            self.stop_process(&request_id, kill_after_ms)?;
                            None
                        }
                        ProcessStopPhase::DuringBootstrap | ProcessStopPhase::AfterContextReady => {
                            Some((phase, kill_after_ms))
                        }
                    }
                } else {
                    None
                };
                pending.remove(&process.id);
                launched.push((process, request_id, deferred_stop));
            }
            if prioritize_new {
                for execution in launched.into_iter().rev() {
                    running.push_front(execution);
                }
            } else {
                running.extend(launched);
            }
            if !failure_injected
                && let FailureInjection::KillNode { logical_node_id } = case.failure
            {
                self.kill_node(logical_node_id)?;
                failure_injected = true;
            }
            if !failure_injected && matches!(case.failure, FailureInjection::StopOrchestrator) {
                self.stop_orchestrator_injection()?;
                pending.clear();
                running.clear();
                break;
            }
            let Some((process, request_id, deferred_stop)) = running.pop_front() else {
                continue;
            };
            let observation = self.wait_execution(process, &request_id, deferred_stop)?;
            if !observation.exit_success && matches!(case.failure, FailureInjection::None) {
                return Err(format!(
                    "process {} failed with status {:?}\nstdout={}\nstderr={}",
                    process.id, observation.exit_status, observation.stdout, observation.stderr
                ));
            }
            completed.insert(process.id.clone());
            observations.push(observation);
        }
        let observation = CaseObservation {
            case_id: case.id.clone(),
            executions: observations,
        };
        fs::write(
            case_dir.join("observed.json"),
            serde_json::to_vec_pretty(&observation)
                .map_err(|error| format!("serialize case observation: {error}"))?,
        )
        .map_err(|error| format!("write case observation: {error}"))?;
        if !matches!(case.failure, FailureInjection::StopOrchestrator) {
            BehaviorOracle::verify(case, &observation).map_err(|error| error.to_string())?;
        }
        if matches!(case.failure, FailureInjection::None) {
            self.cleanup_case(case)?;
        }
        Ok(observation)
    }

    fn cleanup_case(&mut self, case: &BehaviorCase) -> Result<(), String> {
        let paths = case_cleanup_paths(case);
        if paths.is_empty() {
            return Ok(());
        }
        let process = ProcessProgram {
            id: format!("cleanup-{}", case.id),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(format!("cleanup-{}", case.id)),
            depends_on: Vec::new(),
            actions: Vec::new(),
        };
        let request_id = format!("{}-cleanup-{}", case.id, case.seed);
        self.spawn_program(
            &process,
            &request_id,
            &render_cleanup_python(&paths),
            false,
            None,
        )?;
        let observation = self.wait_execution(&process, &request_id, None)?;
        if !observation.exit_success {
            return Err(format!(
                "case {} cleanup failed with status {:?}\nstdout={}\nstderr={}",
                case.id, observation.exit_status, observation.stdout, observation.stderr
            ));
        }
        Ok(())
    }

    pub(super) fn spawn_program(
        &self,
        process: &ProcessProgram,
        request_id: &str,
        source: &str,
        hold_before_bootstrap: bool,
        launch_failure: Option<LaunchFailureKind>,
    ) -> Result<(), String> {
        let mut compressor =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        compressor
            .write_all(source.as_bytes())
            .map_err(|error| format!("compress generated Python source: {error}"))?;
        let compressed = compressor
            .finish()
            .map_err(|error| format!("finish generated Python compression: {error}"))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(compressed);
        if encoded.len() > MAX_CONTEXTUAL_SOURCE_ENV_BYTES {
            return Err(format!(
                "compressed generated Python source is {} bytes; control limit is {}",
                encoded.len(),
                MAX_CONTEXTUAL_SOURCE_ENV_BYTES
            ));
        }
        let mut env = BTreeMap::from([(GENERATED_SOURCE_ENV.to_owned(), encoded)]);
        if hold_before_bootstrap {
            env.insert(BOOTSTRAP_HOLD_ENV.to_owned(), "1".to_owned());
        }
        let (command, args) = match launch_failure {
            Some(LaunchFailureKind::EmptyCommand) => (String::new(), Vec::<String>::new()),
            Some(LaunchFailureKind::MissingExecutable) => (
                "/definitely/missing/myelin-e2e-executable".to_owned(),
                Vec::new(),
            ),
            _ => (
                GENERATED_LAUNCHER.to_owned(),
                vec![
                    "--source-env-zlib".to_owned(),
                    GENERATED_SOURCE_ENV.to_owned(),
                ],
            ),
        };
        let execution_id = if launch_failure == Some(LaunchFailureKind::MalformedExecutionIdentity)
        {
            String::new()
        } else {
            process.access.execution_id.clone()
        };
        let attach_timeout_ms = u64::try_from(self.config.deadline.as_millis()).unwrap_or(u64::MAX);
        let reply = http_json(
            "POST",
            &format!("{}/api/control/contextual/spawn", self.base_url),
            Some(json!({
                "logical_node_id": process.logical_node_id,
                "request_id": request_id,
                "command": command,
                "args": args,
                "env": env,
                "working_dir": null,
                "label": process.id,
                "execution_id": execution_id,
                "read_prefixes": process.access.read_prefixes,
                "write_prefixes": process.access.write_prefixes,
                "attach_timeout_ms": attach_timeout_ms,
            })),
        )?;
        let event_type = reply
            .pointer("/observation/event/type")
            .and_then(Value::as_str);
        if reply.get("type").and_then(Value::as_str) != Some("event")
            || (event_type != Some("spawned")
                && !(launch_failure.is_some() && event_type == Some("spawn_rejected")))
        {
            return Err(format!("contextual spawn was not accepted: {reply}"));
        }
        Ok(())
    }

    fn stop_process(&self, request_id: &str, kill_after_ms: Option<u64>) -> Result<(), String> {
        let control_request_id = format!("stop-{request_id}");
        let reply = http_json(
            "POST",
            &format!("{}/api/control/contextual/{request_id}/stop", self.base_url),
            Some(json!({
                "control_request_id": control_request_id,
                "kill_after_ms": kill_after_ms,
            })),
        )?;
        if reply
            .pointer("/observation/event/type")
            .and_then(Value::as_str)
            != Some("stop_accepted")
        {
            return Err(format!("contextual stop was not accepted: {reply}"));
        }
        Ok(())
    }

    pub(super) fn wait_execution(
        &mut self,
        process: &ProcessProgram,
        request_id: &str,
        stop_at: Option<(ProcessStopPhase, Option<u64>)>,
    ) -> Result<ExecutionObservation, String> {
        let deadline = Instant::now() + self.config.deadline;
        let mut after_sequence = 0_u64;
        let mut lifecycle = Vec::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut terminal = false;
        let mut exit_success = false;
        let mut exit_status = None;
        let mut stop_requested = false;
        while !terminal {
            self.ensure_orchestrator_live()?;
            let reply = http_json(
                "GET",
                &format!(
                    "{}/api/control/contextual/{request_id}/events?after_sequence={after_sequence}",
                    self.base_url
                ),
                None,
            )?;
            let execution = reply
                .get("execution")
                .ok_or_else(|| format!("contextual events response is malformed: {reply}"))?;
            let truncated_before = execution
                .get("truncated_before")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    format!("contextual events response has no truncation cursor: {reply}")
                })?;
            if truncated_before > after_sequence {
                return Err(format!(
                    "contextual event history for {request_id} was truncated before sequence {truncated_before}; requested {after_sequence}"
                ));
            }
            after_sequence = execution
                .get("next_sequence")
                .and_then(Value::as_u64)
                .unwrap_or(after_sequence);
            for record in execution
                .get("events")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let event = record
                    .pointer("/observation/event")
                    .ok_or_else(|| format!("contextual event record is malformed: {record}"))?;
                let event_type = event
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("contextual event type is missing: {record}"))?;
                match event_type {
                    "spawned" => lifecycle.push("spawned".to_owned()),
                    "process_started" => lifecycle.push("process_started".to_owned()),
                    "context_ready" => lifecycle.push("context_ready".to_owned()),
                    "stdout" => {
                        append_json_bytes(event, &mut stdout)?;
                        lifecycle.push("user_result".to_owned());
                    }
                    "stderr" => append_json_bytes(event, &mut stderr)?,
                    "exited" => {
                        lifecycle.push("exited".to_owned());
                        terminal = true;
                        exit_status = event.get("status").map(Value::to_string);
                        exit_success = event.pointer("/status/kind").and_then(Value::as_str)
                            == Some("code")
                            && event.pointer("/status/value").and_then(Value::as_i64) == Some(0);
                    }
                    "spawn_failed" | "process_error" | "spawn_rejected" => {
                        lifecycle.push(event_type.to_owned());
                        terminal = true;
                    }
                    "bootstrap_failed" | "stop_accepted" | "stop_rejected" => {
                        lifecycle.push(event_type.to_owned());
                    }
                    "live_executions" => {}
                    other => return Err(format!("unknown contextual event {other:?}")),
                }
                let stop_event = match stop_at.map(|(phase, _)| phase) {
                    Some(ProcessStopPhase::DuringBootstrap) => Some("process_started"),
                    Some(ProcessStopPhase::AfterContextReady) => Some("context_ready"),
                    Some(ProcessStopPhase::AfterSpawn) | None => None,
                };
                if !stop_requested && stop_event == Some(event_type) {
                    let kill_after_ms = stop_at
                        .expect("deferred stop retains its escalation policy")
                        .1;
                    self.stop_process(request_id, kill_after_ms)?;
                    stop_requested = true;
                }
            }
            terminal |= execution
                .get("terminal")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !terminal {
                if Instant::now() >= deadline {
                    return Err(self.timeout_evidence(&format!(
                        "contextual process {} terminal; lifecycle={lifecycle:?}",
                        process.id
                    )));
                }
                thread::sleep(POLL_INTERVAL);
            }
        }
        if stop_at.is_some() && !stop_requested {
            return Err(format!(
                "contextual process {request_id} terminated before the requested stop phase"
            ));
        }
        let stdout_text = String::from_utf8(stdout)
            .map_err(|error| format!("{} stdout is not UTF-8: {error}", process.id))?;
        let stderr_text = String::from_utf8_lossy(&stderr).into_owned();
        let results = stdout_text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<ActionObservation>(line)
                    .map_err(|error| format!("parse structured Python output {line:?}: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExecutionObservation {
            process: process.id.clone(),
            request_id: request_id.to_owned(),
            logical_node_id: process.logical_node_id,
            lifecycle,
            results,
            terminal,
            exit_success,
            exit_status,
            stdout: stdout_text,
            stderr: stderr_text,
        })
    }

    pub(super) fn kill_node(&self, logical_node_id: u64) -> Result<(), String> {
        http_json(
            "POST",
            &format!("{}/api/control/nodes/{logical_node_id}/kill", self.base_url),
            Some(json!({
                "command_id": format!("e2e-kill-{}-{logical_node_id}", self.config.seed),
            })),
        )?;
        let deadline = Instant::now() + self.config.deadline;
        loop {
            let fleet = http_json("GET", &format!("{}/api/control/fleet", self.base_url), None)?;
            let phase = fleet
                .pointer("/FleetStatus/nodes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|node| {
                    node.get("logical_node_id").and_then(Value::as_u64) == Some(logical_node_id)
                })
                .and_then(|node| node.get("phase"))
                .and_then(Value::as_str);
            match phase {
                Some("stopped") => return Ok(()),
                Some("stop_failed") => {
                    return Err(format!("node {logical_node_id} failed to stop: {fleet}"));
                }
                _ if Instant::now() >= deadline => {
                    return Err(self.timeout_evidence(&format!(
                        "node {logical_node_id} stopped; fleet={fleet}"
                    )));
                }
                _ => thread::sleep(POLL_INTERVAL),
            }
        }
    }

    fn stop_orchestrator_injection(&mut self) -> Result<(), String> {
        // Record the worker containers that exist before the injection:
        // earlier failure cases may legally have removed containers (e.g. a
        // killed node whose replacement never arrived), so the post-mortem
        // census must compare against this snapshot, not the configured node
        // count. Orchestrator loss itself must not add or remove any.
        self.pre_stop_containers = list_containers(&self.container_prefix)?;
        self.orchestrator
            .kill()
            .map_err(|error| format!("stop orchestrator injection: {error}"))?;
        let deadline = Instant::now() + self.config.deadline;
        loop {
            if self
                .orchestrator
                .try_wait()
                .map_err(|error| format!("observe stopped orchestrator: {error}"))?
                .is_some()
            {
                self.orchestrator_stopped = true;
                break;
            }
            if Instant::now() >= deadline {
                return Err(self.timeout_evidence("orchestrator failure injection"));
            }
            thread::sleep(POLL_INTERVAL);
        }
        self.wait_for_no_workload_processes()
    }

    pub(super) fn wait_for_no_workload_processes(&self) -> Result<(), String> {
        let deadline = Instant::now() + self.config.deadline;
        loop {
            match list_workload_processes(&self.container_prefix)? {
                Some(processes) if processes.is_empty() => return Ok(()),
                Some(processes) => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "deadline waiting for contextual native processes to exit: {processes:?}"
                        ));
                    }
                }
                // `docker top` cannot introspect a container that is not
                // running right now (stopped, paused, or mid-transition),
                // which hosts no workload anyway. Keep polling: Docker
                // either starts introspecting it again or it disappears.
                None => {
                    if Instant::now() >= deadline {
                        return Err(
                            "deadline waiting to inspect harness workload containers".to_owned()
                        );
                    }
                }
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

fn append_json_bytes(event: &Value, output: &mut Vec<u8>) -> Result<(), String> {
    let bytes = event
        .get("bytes")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("contextual byte event is malformed: {event}"))?;
    for byte in bytes {
        let byte = byte
            .as_u64()
            .and_then(|byte| u8::try_from(byte).ok())
            .ok_or_else(|| format!("contextual byte is outside u8: {byte}"))?;
        output.push(byte);
    }
    Ok(())
}
