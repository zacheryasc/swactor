//! Cluster convergence: dashboard, provisioning, and pairwise functional proof.

use std::collections::BTreeMap;
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::codegen::{digest, render_python};
use crate::harness::ClusterHarness;
use crate::ir::{AccessSpec, Action, ActionOp, ExecutionObservation, ProcessProgram};
use crate::oracle::BehaviorOracle;
use crate::resources::{http_json, transient_actor_identities};

use super::POLL_INTERVAL;

impl ClusterHarness {
    pub(super) fn wait_for_dashboard(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + self.config.deadline;
        loop {
            self.ensure_orchestrator_live()?;
            if let Ok(status) = http_json(
                "GET",
                &format!("{}/api/control/status", self.base_url),
                None,
            ) {
                let configured_image = status
                    .pointer("/Status/provider/runtime_image")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        format!("control status omitted configured runtime image: {status}")
                    })?;
                if configured_image != self.image {
                    return Err(format!(
                        "control status runtime image {configured_image:?} does not match harness image {:?}",
                        self.image
                    ));
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.timeout_evidence("dashboard readiness"));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    pub(super) fn provision_nodes(&mut self) -> Result<(), String> {
        let command_id = format!("e2e-provision-{}", self.config.seed);
        let selected_offer_ids = (1..=u64::from(self.config.node_count)).collect::<Vec<_>>();
        http_json(
            "POST",
            &format!("{}/api/control/provision", self.base_url),
            Some(json!({
                "command_id": command_id,
                "count": self.config.node_count,
                "selected_offer_ids": selected_offer_ids,
            })),
        )?;
        let deadline = Instant::now() + self.config.deadline;
        loop {
            self.ensure_orchestrator_live()?;
            let fleet = http_json("GET", &format!("{}/api/control/fleet", self.base_url), None)?;
            let nodes = fleet
                .get("FleetStatus")
                .and_then(|fleet| fleet.get("nodes"))
                .and_then(Value::as_array);
            let running = nodes
                .into_iter()
                .flatten()
                .filter(|node| node.get("phase").and_then(Value::as_str) == Some("running"))
                .filter_map(|node| node.get("logical_node_id").and_then(Value::as_u64))
                .collect::<Vec<_>>();
            if running.len() == usize::from(self.config.node_count) {
                self.node_ids = running;
                self.node_ids.sort_unstable();
                return Ok(());
            }
            if nodes.is_some_and(|nodes| {
                nodes.len() == usize::from(self.config.node_count)
                    && nodes.iter().all(|node| {
                        node.get("phase").and_then(Value::as_str) == Some("stopped")
                            && !node.get("last_error").is_none_or(Value::is_null)
                    })
            }) {
                return Err(format!("worker provisioning failed: {fleet}"));
            }
            if Instant::now() >= deadline {
                return Err(self.timeout_evidence(&format!(
                    "{} workers running; fleet={fleet}",
                    self.config.node_count
                )));
            }

            thread::sleep(POLL_INTERVAL);
        }
    }
    pub fn verify_workload_convergence(&mut self) -> Result<(), String> {
        self.wait_contextual_control()?;
        if self.resource_baseline.is_empty() {
            let baseline = self.health_snapshot()?;
            self.resource_baseline = transient_actor_identities(&baseline);
        }
        self.verify_ordered_pair_communication()
    }

    fn wait_contextual_control(&mut self) -> Result<(), String> {
        for node in self.node_ids.clone() {
            let deadline = Instant::now() + self.config.deadline;
            let mut attempt = 0_u64;
            loop {
                self.ensure_orchestrator_live()?;
                attempt = attempt.saturating_add(1);
                let control_request_id =
                    format!("convergence-{}-{node}-{attempt}", self.config.seed);
                let response = http_json(
                    "GET",
                    &format!(
                        "{}/api/control/contextual/nodes/{node}?control_request_id={control_request_id}",
                        self.base_url
                    ),
                    None,
                );
                if response.as_ref().is_ok_and(|reply| {
                    reply
                        .pointer("/observation/event/type")
                        .and_then(Value::as_str)
                        == Some("live_executions")
                }) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(self.timeout_evidence(&format!(
                        "contextual process control reachable on node {node}; last={response:?}"
                    )));
                }
                thread::sleep(POLL_INTERVAL);
            }
        }
        Ok(())
    }

    /// Prove functional communication across every ordered node pair.
    ///
    /// Fleet status and per-node control reachability do not establish
    /// convergence: for every ordered pair (source, destination) a process on
    /// the source node publishes a small blob at a run-unique path and a
    /// process on the destination node resolves, consumes, and unlinks it.
    /// Every wait reuses the contextual spawn machinery, whose terminal-event
    /// predicate polls by deadline and never sleeps arbitrarily.
    fn verify_ordered_pair_communication(&mut self) -> Result<(), String> {
        let run_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_millis();
        let nodes = self.node_ids.clone();
        let pair_path = |source: u64, destination: u64| {
            format!("/cases/convergence/{run_nonce}/{source}-to-{destination}")
        };
        let mut payloads = BTreeMap::new();
        let mut programs = Vec::new();
        for &source in &nodes {
            let mut actions = Vec::new();
            for &destination in &nodes {
                if source == destination {
                    continue;
                }
                let path = pair_path(source, destination);
                let payload = format!("myelin-e2e-convergence:{run_nonce}:{source}:{destination}")
                    .into_bytes();
                actions.push(Action::ok(ActionOp::PublishBlob {
                    path: path.clone(),
                    bytes: payload.clone(),
                }));
                payloads.insert(path, payload);
            }
            programs.push(ProcessProgram {
                id: format!("convergence-writer-{source}"),
                logical_node_id: source,
                access: AccessSpec::unrestricted(format!(
                    "convergence-writer-{run_nonce}-{source}"
                )),
                depends_on: Vec::new(),
                actions,
            });
        }
        for &destination in &nodes {
            let mut actions = Vec::new();
            for &source in &nodes {
                if source == destination {
                    continue;
                }
                let path = pair_path(source, destination);
                actions.push(Action::ok(ActionOp::ReadBlob {
                    path: path.clone(),
                    expected: payloads[&path].clone(),
                }));
                actions.push(Action::ok(ActionOp::Unlink { path }));
            }
            programs.push(ProcessProgram {
                id: format!("convergence-reader-{destination}"),
                logical_node_id: destination,
                access: AccessSpec::unrestricted(format!(
                    "convergence-reader-{run_nonce}-{destination}"
                )),
                depends_on: Vec::new(),
                actions,
            });
        }
        for program in &programs {
            let request_id = format!("convergence-{run_nonce}-{}", program.id);
            self.spawn_program(program, &request_id, &render_python(program), false, None)?;
            let observation = self.wait_execution(program, &request_id, None)?;
            verify_convergence_execution(program, &observation)
                .map_err(|error| format!("ordered-pair convergence proof: {error}"))?;
        }
        // Convergence processes must be fully reclaimed before the resource
        // baseline is captured, otherwise their transient actors would be
        // grandfathered into every later health assertion.
        self.assert_healthy()?;
        Ok(())
    }
}

fn verify_convergence_execution(
    program: &ProcessProgram,
    observation: &ExecutionObservation,
) -> Result<(), String> {
    BehaviorOracle::verify_execution(observation).map_err(|error| error.to_string())?;
    if !observation.exit_success {
        return Err(format!(
            "process {} on node {} failed with status {:?}\nstdout={}\nstderr={}",
            program.id,
            program.logical_node_id,
            observation.exit_status,
            observation.stdout,
            observation.stderr
        ));
    }
    if observation.results.len() != program.actions.len() {
        return Err(format!(
            "process {} expected {} action results, observed {}",
            program.id,
            program.actions.len(),
            observation.results.len()
        ));
    }
    for (step, action) in program.actions.iter().enumerate() {
        let result = observation
            .results
            .iter()
            .find(|result| result.step == step)
            .ok_or_else(|| format!("process {} omitted step {step}", program.id))?;
        if result.outcome != "ok" {
            return Err(format!(
                "process {} step {step} on node {} reported outcome {:?} (errno {:?}, error {:?})",
                program.id, program.logical_node_id, result.outcome, result.errno, result.error
            ));
        }
        if result.path != action.operation.path() {
            return Err(format!(
                "process {} step {step} reported path {:?} instead of {:?}",
                program.id,
                result.path,
                action.operation.path()
            ));
        }
        let expected_bytes = match &action.operation {
            ActionOp::PublishBlob { bytes, .. }
            | ActionOp::ReadBlob {
                expected: bytes, ..
            } => Some(bytes.as_slice()),
            _ => None,
        };
        if let Some(expected) = expected_bytes
            && (result.length != Some(expected.len())
                || result.digest.as_deref() != Some(&digest(expected)))
        {
            return Err(format!(
                "process {} step {step} returned the wrong payload length or digest",
                program.id
            ));
        }
    }
    Ok(())
}
