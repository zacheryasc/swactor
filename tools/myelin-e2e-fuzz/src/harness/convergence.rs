//! Cluster convergence: dashboard, provisioning, and pairwise functional proof.

use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::budget::Budget;
use crate::harness::ClusterHarness;
use crate::ir::{
    AccessSpec, Action, ActionOp, BehaviorCase, CASE_SCHEMA_VERSION, CaseResourceBounds,
    FailureInjection, GENERATOR_VERSION, ProcessProgram, TopologyFamily,
};
use crate::resources::http_json_budget;

use super::POLL_INTERVAL;

impl ClusterHarness {
    pub(super) fn wait_for_dashboard(&mut self) -> Result<(), String> {
        let started = Instant::now();
        let budget = self.operation_budget().child(self.config.deadline);
        let result = (|| {
            let mut pending = "dashboard control status and actors".to_owned();
            loop {
                budget.check(&pending)?;
                self.ensure_orchestrator_live()?;
                let request_budget = budget.child(Duration::from_secs(2));
                match http_json_budget(
                    "GET",
                    &format!("{}/api/control/status", self.base_url),
                    None,
                    &request_budget,
                ) {
                    Ok(status) => {
                        let provider = status
                            .pointer("/Status/provider")
                            .ok_or_else(|| format!("control status omitted provider: {status}"))?;
                        let configured_image = provider
                            .get("runtime_image")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                format!("control status omitted configured runtime image: {status}")
                            })?;
                        if configured_image != self.image {
                            return Err(format!(
                                "control runtime image {configured_image:?} does not match {:?}",
                                self.image
                            ));
                        }
                        let readiness =
                            provider
                                .get("kind")
                                .and_then(Value::as_str)
                                .ok_or_else(|| {
                                    format!("control status omitted provider readiness: {status}")
                                })?;
                        if readiness != "ready" {
                            pending = format!(
                                "provider readiness {readiness}: {}",
                                provider
                                    .get("error")
                                    .and_then(Value::as_str)
                                    .unwrap_or("validation pending")
                            );
                        } else {
                            match http_json_budget(
                                "GET",
                                &format!("{}/api/control/actors", self.base_url),
                                None,
                                &request_budget,
                            ) {
                                Ok(_) => return Ok(()),
                                Err(error) => pending = format!("dashboard actor control: {error}"),
                            }
                        }
                    }
                    Err(error) => pending = format!("dashboard status: {error}"),
                }
                // Startup has no fleet-event subscription until the dashboard binds.
                budget.wait(POLL_INTERVAL, &pending)?;
            }
        })();
        self.record_lifecycle("dashboard_readiness", started, &result)?;
        result
    }

    pub(super) fn dispatch_provision(&self) -> Result<(), String> {
        let response = http_json_budget(
            "POST",
            &format!("{}/api/control/provision", self.base_url),
            Some(json!({
                "command_id": format!("e2e-provision-{}", self.config.seed),
                "count": self.config.node_count,
                "selected_offer_ids": self.config.selected_offer_ids,
                "search_id": self.config.offer_search_id,
            })),
            &self.operation_budget().child(self.config.deadline),
        )?;
        let admitted = response
            .pointer("/Accepted/node_ids")
            .and_then(Value::as_array)
            .is_some_and(|nodes| nodes.len() == usize::from(self.config.node_count));
        if !admitted {
            return Err(format!(
                "provision command did not durably admit the exact node count: {response}"
            ));
        }
        Ok(())
    }

    pub(super) fn provision_nodes(&mut self, provision: bool) -> Result<(), String> {
        let started = Instant::now();
        let budget = self.operation_budget().child(self.config.deadline);
        let result = (|| {
            if provision {
                self.dispatch_provision()?;
            }
            let expected = if self.node_ids.is_empty() {
                (1..=u64::from(self.config.node_count)).collect()
            } else {
                self.node_ids.clone()
            };
            loop {
                budget.check(&format!("exact running nodes {expected:?}"))?;
                self.ensure_orchestrator_live()?;
                let cursor = self.control_revision(&budget)?;
                let fleet = http_json_budget(
                    "GET",
                    &format!("{}/api/control/fleet", self.base_url),
                    None,
                    &budget,
                )?;
                let nodes = fleet
                    .pointer("/FleetStatus/nodes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("fleet omitted node census: {fleet}"))?;
                let expected_running = expected.iter().copied().collect::<BTreeSet<_>>();
                if expected_running.len() != expected.len() || expected_running.contains(&0) {
                    return Err(format!("invalid expected logical-node set {expected:?}"));
                }
                let mut seen = BTreeSet::new();
                let mut running = Vec::new();
                let mut stopped = BTreeSet::new();
                for node in nodes {
                    let node_id = node["logical_node_id"]
                        .as_u64()
                        .filter(|node_id| *node_id != 0)
                        .ok_or_else(|| format!("fleet node omitted valid identity: {node}"))?;
                    if !seen.insert(node_id) {
                        return Err(format!("fleet duplicated logical node {node_id}"));
                    }
                    if !expected_running.contains(&node_id)
                        && !self.stopped_nodes.contains(&node_id)
                    {
                        return Err(format!(
                            "unexpected replacement or bootstrap logical node {node_id}: {fleet}"
                        ));
                    }
                    if self.stopped_nodes.contains(&node_id) {
                        if node["phase"] == "stopped" {
                            stopped.insert(node_id);
                        } else {
                            return Err(format!(
                                "removed logical node {node_id} left stopped phase: {node}"
                            ));
                        }
                    } else if node["phase"] == "running" {
                        running.push(node_id);
                    } else if provisioning_failed(std::slice::from_ref(node)) {
                        return Err(format!("worker provisioning failed: {fleet}"));
                    }
                }
                running.sort_unstable();
                if running == expected && stopped == self.stopped_nodes {
                    self.node_ids = running;
                    return Ok(());
                }
                self.wait_for_control_change(
                    &cursor,
                    &budget,
                    &format!("running nodes {expected:?}; observed={fleet}"),
                )?;
            }
        })();
        self.record_lifecycle("fleet_convergence", started, &result)?;
        result
    }
    pub fn verify_workload_convergence(&mut self) -> Result<(), String> {
        self.wait_contextual_control()?;
        if self.provider_baseline.is_empty() {
            self.record_health_baseline()?;
        }
        self.verify_ordered_pair_communication()?;
        // The reusable fixture begins only after the readiness workload and
        // every owned readiness resource have been cleaned and re-observed.
        self.record_health_baseline()
    }

    pub(super) fn wait_contextual_control(&mut self) -> Result<(), String> {
        let started = Instant::now();
        let budget = self.operation_budget().child(self.config.deadline);
        let result = (|| {
            loop {
                budget.check("exact concurrent contextual readiness")?;
                self.ensure_orchestrator_live()?;
                let cursor = self.control_revision(&budget)?;
                match self.query_contextual_nodes(&self.node_ids, &budget) {
                    Ok(_) => return Ok(()),
                    Err(error) => self.wait_for_control_change(&cursor, &budget, &error)?,
                }
            }
        })();
        self.record_lifecycle("contextual_readiness", started, &result)?;
        result
    }

    pub(super) fn query_contextual_nodes(
        &self,
        nodes: &[u64],
        budget: &Budget,
    ) -> Result<Vec<myelin_control_contract::ContextualHealthReply>, String> {
        let expected = nodes.iter().copied().collect::<BTreeSet<_>>();
        if nodes.is_empty() || expected.len() != nodes.len() || expected.contains(&0) {
            return Err(format!(
                "contextual readiness requires exact, unique live logical nodes; observed {nodes:?}"
            ));
        }
        let generation = self.next_observation_generation();
        thread::scope(|scope| {
            let requests = nodes
                .iter()
                .map(|&node| {
                    let base_url = &self.base_url;
                    scope.spawn(move || {
                        let mut attempt = 0_u64;
                        loop {
                            budget.check(&format!("contextual health for node {node}"))?;
                            let request_id =
                                format!("health-{}-{generation}-{node}-{attempt}", self.config.seed);
                            let request_budget = budget.child(Duration::from_secs(5));
                            match http_json_budget(
                                "GET",
                                &format!(
                                    "{base_url}/api/control/contextual/nodes/{node}?control_request_id={request_id}"
                                ),
                                None,
                                &request_budget,
                            ) {
                                Ok(response) => {
                                    return validate_contextual_reply(&response, node, &request_id);
                                }
                                Err(error) => {
                                    attempt = attempt.saturating_add(1);
                                    budget.wait(
                                        POLL_INTERVAL,
                                        &format!(
                                            "contextual health for node {node}; retry after {error}"
                                        ),
                                    )?;
                                }
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            let mut responses = Vec::with_capacity(requests.len());
            let mut errors = Vec::new();
            for request in requests {
                match request.join() {
                    Ok(Ok(response)) => responses.push(response),
                    Ok(Err(error)) => errors.push(error),
                    Err(_) => errors.push("contextual readiness collector panicked".to_owned()),
                }
            }
            if errors.is_empty() {
                Ok(responses)
            } else {
                Err(errors.join("; "))
            }
        })
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
        let case = Self::convergence_case(&self.node_ids, run_nonce)?;
        self.run_case(&case).map(|_| ())
    }

    pub(crate) fn convergence_case(nodes: &[u64], run_nonce: u128) -> Result<BehaviorCase, String> {
        if nodes.is_empty() {
            return Err("convergence requires live nodes".to_owned());
        }
        let blob_path = |source: u64, destination: u64| {
            format!("/cases/convergence/{run_nonce}/blob-{source}-to-{destination}")
        };
        let stream_path = |source: u64, destination: u64| {
            format!("/cases/convergence/{run_nonce}/stream-{source}-to-{destination}")
        };
        let ready_path = |process: &str| format!("/cases/convergence/{run_nonce}/ready/{process}");
        let release_path = format!("/cases/convergence/{run_nonce}/release");
        let boundary_lengths = [0_usize, 1, 4_095, 4_096, 4_097, 65_537];
        let mut blob_payloads = BTreeMap::new();
        let mut stream_payloads = BTreeMap::new();
        let mut programs = Vec::new();
        for (source_index, &source) in nodes.iter().enumerate() {
            let process_id = format!("convergence-writer-{source}");
            let mut actions = vec![
                Action::ok(ActionOp::PublishBlob {
                    path: ready_path(&process_id),
                    bytes: Vec::new(),
                }),
                Action::ok(ActionOp::AwaitEntry {
                    path: release_path.clone(),
                    expected_kind: "blob".to_owned(),
                }),
            ];
            for round in 1..nodes.len() {
                let destination = nodes[(source_index + round) % nodes.len()];
                let pair_index = source_index * (nodes.len() - 1) + round - 1;
                let length = boundary_lengths[pair_index % boundary_lengths.len()];
                let body = (0..length)
                    .map(|offset| {
                        (u128::from(source) * 17 + u128::from(destination) * 31 + offset as u128)
                            as u8
                    })
                    .collect::<Vec<_>>();
                let blob = blob_path(source, destination);
                actions.push(Action::ok(ActionOp::PublishBlob {
                    path: blob.clone(),
                    bytes: body.clone(),
                }));
                blob_payloads.insert(blob, body.clone());

                let mut framed = Vec::with_capacity(body.len() + 8);
                let split = body.len() / 2;
                for frame in [&body[..split], &body[split..]] {
                    framed.extend_from_slice(&(frame.len() as u32).to_be_bytes());
                    framed.extend_from_slice(frame);
                }
                let stream = stream_path(source, destination);
                let mut cuts = vec![0, 1, 3, 257, framed.len().saturating_sub(1), framed.len()];
                cuts.retain(|cut| *cut <= framed.len());
                cuts.sort_unstable();
                cuts.dedup();
                let chunks = cuts
                    .windows(2)
                    .filter(|range| range[0] != range[1])
                    .map(|range| framed[range[0]..range[1]].to_vec())
                    .collect();
                actions.push(Action::ok(ActionOp::StreamWrite {
                    path: stream.clone(),
                    chunks,
                    replace: false,
                }));
                stream_payloads.insert(stream, framed);
            }
            programs.push(ProcessProgram {
                id: process_id,
                logical_node_id: source,
                access: AccessSpec::unrestricted(format!(
                    "convergence-writer-{run_nonce}-{source}"
                )),
                depends_on: Vec::new(),
                actions,
            });
        }
        for (destination_index, &destination) in nodes.iter().enumerate() {
            let process_id = format!("convergence-reader-{destination}");
            let mut actions = vec![
                Action::ok(ActionOp::PublishBlob {
                    path: ready_path(&process_id),
                    bytes: Vec::new(),
                }),
                Action::ok(ActionOp::AwaitEntry {
                    path: release_path.clone(),
                    expected_kind: "blob".to_owned(),
                }),
            ];
            for round in 1..nodes.len() {
                let source = nodes[(destination_index + nodes.len() - round) % nodes.len()];
                let blob = blob_path(source, destination);
                actions.push(Action::ok(ActionOp::AwaitEntry {
                    path: blob.clone(),
                    expected_kind: "blob".to_owned(),
                }));
                actions.push(Action::ok(ActionOp::ReadBlob {
                    path: blob.clone(),
                    expected: blob_payloads[&blob].clone(),
                }));
                actions.push(Action::ok(ActionOp::Unlink { path: blob }));

                let stream = stream_path(source, destination);
                actions.push(Action::ok(ActionOp::StreamRead {
                    path: stream.clone(),
                    expected: stream_payloads[&stream].clone(),
                }));
                actions.push(Action::ok(ActionOp::WaitForQuiescent {
                    path: stream.clone(),
                }));
                actions.push(Action::ok(ActionOp::Unlink { path: stream }));
            }
            programs.push(ProcessProgram {
                id: process_id,
                logical_node_id: destination,
                access: AccessSpec::unrestricted(format!(
                    "convergence-reader-{run_nonce}-{destination}"
                )),
                depends_on: Vec::new(),
                actions,
            });
        }
        let participant_ready_paths = programs
            .iter()
            .map(|program| ready_path(&program.id))
            .collect::<Vec<_>>();
        programs.push(ProcessProgram {
            id: "zz-convergence-release".to_owned(),
            logical_node_id: nodes[0],
            access: AccessSpec::unrestricted(format!("convergence-release-{run_nonce}")),
            depends_on: Vec::new(),
            actions: participant_ready_paths
                .into_iter()
                .map(|path| {
                    Action::ok(ActionOp::AwaitEntry {
                        path,
                        expected_kind: "blob".to_owned(),
                    })
                })
                .chain(std::iter::once(Action::ok(ActionOp::PublishBlob {
                    path: release_path,
                    bytes: Vec::new(),
                })))
                .collect(),
        });
        let case = BehaviorCase {
            schema_version: CASE_SCHEMA_VERSION,
            generator_version: GENERATOR_VERSION,
            id: format!("convergence-{run_nonce}"),
            seed: u64::try_from(run_nonce).unwrap_or(u64::MAX),
            live_nodes: nodes.iter().copied().collect(),
            topology: TopologyFamily::Fixed,
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: CaseResourceBounds {
                max_actions: 256,
                max_processes: 20,
                max_payload_bytes: 4 * 1024 * 1024,
                max_allocation_bytes: 64 * 1024 * 1024,
                max_race_states: 65_536,
            },
            processes: programs,
            failure: FailureInjection::None,
        };
        case.validate()?;
        Ok(case)
    }
}

fn validate_contextual_reply(
    reply: &Value,
    node: u64,
    request_id: &str,
) -> Result<myelin_control_contract::ContextualHealthReply, String> {
    let reply: myelin_control_contract::ContextualHealthReply =
        serde_json::from_value(reply.clone())
            .map_err(|error| format!("invalid contextual health reply for node {node}: {error}"))?;
    reply.validate(node, request_id)?;
    Ok(reply)
}

fn provisioning_failed(nodes: &[Value]) -> bool {
    nodes.iter().any(|node| {
        matches!(
            node.get("phase").and_then(Value::as_str),
            Some("kill_requested" | "stopping" | "stop_failed" | "stopped" | "orphan")
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_rejects_stale_or_wrong_node_replies() {
        let reply = crate::resources::resource_deadline_tests::health_reply("fresh-3", 3, 1, 0);
        assert!(validate_contextual_reply(&reply, 3, "fresh-3").is_ok());
        assert!(validate_contextual_reply(&reply, 4, "fresh-3").is_err());
        assert!(validate_contextual_reply(&reply, 3, "new-3").is_err());
        assert!(validate_contextual_reply(&json!({}), 3, "fresh-3").is_err());
        let mut unversioned = reply.clone();
        unversioned
            .as_object_mut()
            .unwrap()
            .remove("schema_version");
        assert!(validate_contextual_reply(&unversioned, 3, "fresh-3").is_err());
        let mut rejected = reply.clone();
        rejected["type"] = json!("rejected");
        assert!(validate_contextual_reply(&rejected, 3, "fresh-3").is_err());
    }

    #[test]
    fn terminal_node_fails_a_partially_converged_fleet() {
        let nodes = vec![
            json!({"logical_node_id": 1, "phase": "joining"}),
            json!({"logical_node_id": 2, "phase": "stopped", "last_error": "substrate lost"}),
        ];
        assert!(provisioning_failed(&nodes));
    }
}
