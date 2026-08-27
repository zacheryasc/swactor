//! Real-binary, multi-node behavioral fuzz harness for Myelin and Swactor.
//!
//! The crate is organized as a module tree; this root is a thin facade that
//! preserves the historical flat API used by `main.rs`.

mod codegen;
mod corpus;
mod harness;
mod ir;
mod oracle;
mod resources;
mod shrink;

pub use codegen::{render_case_python, render_python};
pub use corpus::{failure_corpus, ordered_pair_corpus, random_short_dags, stable_corpus};
pub use harness::{ClusterHarness, ClusterHarnessConfig};
pub use ir::{
    AccessSpec, Action, ActionClass, ActionObservation, ActionOp, BehaviorCase, CaseObservation,
    DescriptorFinish, DescriptorReadMethod, DescriptorWriteMethod, ExecutionObservation,
    ExpectedOutcome, FailureInjection, LaunchFailureKind, ProcessProgram, ProcessStopPhase,
    PythonException,
};
pub use oracle::{BehaviorOracle, BlobPublicationTrace, OracleViolation, StreamIncarnationTrace};
pub use shrink::shrink_failure;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use serde_json::{Value, json};

    use crate::codegen::digest;
    use crate::corpus::SEEDED_MODEL_PATH;
    use crate::resources::{TelemetryResourceCensus, pending_resource_cleanup};

    #[test]
    fn generated_barriers_poll_by_deadline_instead_of_attempt_counts() {
        let program = ProcessProgram {
            id: "barriers".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("barriers"),
            depends_on: Vec::new(),
            actions: vec![
                Action::ok(ActionOp::AwaitEntry {
                    path: "/cases/1/entry".to_owned(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(ActionOp::WaitForQuiescent {
                    path: "/cases/1/stream".to_owned(),
                }),
            ],
        };
        let source = render_python(&program);
        assert!(source.contains("deadline = loop.time() + "));
        assert!(source.contains("if loop.time() >= deadline:"));
        assert!(!source.contains("range("));

        let cleanup = crate::codegen::render_cleanup_python(&["/cases/1/entry".to_owned()]);
        assert!(cleanup.contains("deadline = loop.time() + "));
        assert!(cleanup.contains("if loop.time() >= deadline:"));
        assert!(!cleanup.contains("range("));
    }
    fn valid_execution() -> ExecutionObservation {
        ExecutionObservation {
            process: "p".to_owned(),
            request_id: "r".to_owned(),
            logical_node_id: 1,
            lifecycle: vec![
                "spawned".to_owned(),
                "process_started".to_owned(),
                "context_ready".to_owned(),
                "user_result".to_owned(),
                "exited".to_owned(),
            ],
            results: vec![ActionObservation {
                process: "p".to_owned(),
                step: 0,
                action: "lookup".to_owned(),
                path: SEEDED_MODEL_PATH.to_owned(),
                outcome: "ok".to_owned(),
                length: None,
                digest: None,
                kind: Some("blob".to_owned()),
                revision: Some(1),
                errno: None,
                active: None,
                error_type: None,
                error: None,
            }],
            terminal: true,
            exit_success: true,
            exit_status: Some(r#"{"kind":"code","value":0}"#.to_owned()),
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn generated_program_uses_only_public_binding_operations() {
        let program = ProcessProgram {
            id: "generated".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("generated"),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::Lookup {
                path: SEEDED_MODEL_PATH.to_owned(),
                expected_kind: "blob".to_owned(),
            })],
        };
        let source = render_python(&program);
        assert!(source.contains("swactor.run(main)"));
        assert!(source.contains("await data.lookup"));
        assert!(!source.contains("data_plane::"));
    }

    #[test]
    fn stable_corpus_covers_every_ordered_action_and_node_pair() {
        let corpus = stable_corpus(3, 7);
        let action_pairs = corpus
            .iter()
            .flat_map(|case| case.processes.iter())
            .flat_map(|process| {
                process
                    .actions
                    .windows(2)
                    .map(|pair| (pair[0].operation.class(), pair[1].operation.class()))
            })
            .collect::<BTreeSet<_>>();
        for left in ActionClass::ALL {
            for right in ActionClass::ALL {
                assert!(
                    action_pairs.contains(&(left, right)),
                    "missing {left:?}->{right:?}"
                );
            }
        }
        for source in 1_u64..=3 {
            for destination in 1_u64..=3 {
                if source == destination {
                    continue;
                }
                assert!(corpus.iter().any(|case| {
                    case.processes.iter().any(|process| {
                        process.logical_node_id == source
                            && process.actions.iter().any(|action| {
                                matches!(
                                    action.operation,
                                    ActionOp::PublishBlob { .. } | ActionOp::StreamWrite { .. }
                                )
                            })
                    }) && case.processes.iter().any(|process| {
                        process.logical_node_id == destination
                            && process.actions.iter().any(|action| {
                                matches!(
                                    action.operation,
                                    ActionOp::ReadBlob { .. }
                                        | ActionOp::StreamRead { .. }
                                        | ActionOp::StreamReadInto { .. }
                                )
                            })
                    })
                }));
            }
        }
    }

    #[test]
    fn controlled_faults_are_rejected_by_invariant_checker() {
        let valid = valid_execution();
        BehaviorOracle::verify_execution(&valid).unwrap();

        let mut ready_before_start = valid.clone();
        ready_before_start.lifecycle.swap(1, 2);
        assert_eq!(
            BehaviorOracle::verify_execution(&ready_before_start)
                .unwrap_err()
                .invariant,
            "context_order"
        );

        let mut missing_terminal = valid.clone();
        missing_terminal.lifecycle.pop();
        missing_terminal.terminal = false;
        assert_eq!(
            BehaviorOracle::verify_execution(&missing_terminal)
                .unwrap_err()
                .invariant,
            "terminal_resolution"
        );

        let mut torn = valid.clone();
        torn.results[0].kind = Some("stream".to_owned());
        let case = BehaviorCase {
            id: "case".to_owned(),
            seed: 1,
            node_count: 2,
            processes: vec![ProcessProgram {
                id: "p".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("p"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::Lookup {
                    path: SEEDED_MODEL_PATH.to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
            }],
            failure: FailureInjection::None,
        };
        assert_eq!(
            BehaviorOracle::verify(
                &case,
                &CaseObservation {
                    case_id: "case".to_owned(),
                    executions: vec![torn],
                },
            )
            .unwrap_err()
            .invariant,
            "namespace_kind"
        );

        let mut duplicated = valid.clone();
        duplicated.results.push(duplicated.results[0].clone());
        assert_eq!(
            BehaviorOracle::verify(
                &case,
                &CaseObservation {
                    case_id: "case".to_owned(),
                    executions: vec![duplicated],
                },
            )
            .unwrap_err()
            .invariant,
            "duplicate_action"
        );

        assert_eq!(
            BehaviorOracle::verify_blob_publication(&BlobPublicationTrace {
                declared_length: 5,
                declared_digest: digest(b"whole"),
                observed_bytes: b"whol".to_vec(),
            })
            .unwrap_err()
            .invariant,
            "torn_publication"
        );
        assert_eq!(
            BehaviorOracle::verify_stream_incarnations(&[
                StreamIncarnationTrace::Opened { incarnation: 2 },
                StreamIncarnationTrace::Closed { incarnation: 2 },
                StreamIncarnationTrace::Opened { incarnation: 3 },
                StreamIncarnationTrace::Bytes {
                    incarnation: 2,
                    bytes: vec![1],
                },
            ])
            .unwrap_err()
            .invariant,
            "stale_incarnation"
        );
    }
    #[test]
    fn linearization_groups_reject_impossible_success_counts() {
        let operation = || ActionOp::Lookup {
            path: SEEDED_MODEL_PATH.to_owned(),
            expected_kind: "blob".to_owned(),
        };
        let program = |id: &str| ProcessProgram {
            id: id.to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(id),
            depends_on: Vec::new(),
            actions: vec![Action::linearized(operation(), 7, 1, libc::ENOENT)],
        };
        let mut first = valid_execution();
        first.process = "first".to_owned();
        first.results[0].process = "first".to_owned();
        let mut second = valid_execution();
        second.process = "second".to_owned();
        second.results[0].process = "second".to_owned();
        let case = BehaviorCase {
            id: "linearized".to_owned(),
            seed: 1,
            node_count: 2,
            processes: vec![program("first"), program("second")],
            failure: FailureInjection::None,
        };
        let error = BehaviorOracle::verify(
            &case,
            &CaseObservation {
                case_id: case.id.clone(),
                executions: vec![first, second],
            },
        )
        .unwrap_err();
        assert_eq!(error.invariant, "linearizability");
    }

    #[test]
    fn failure_injections_validate_real_targets() {
        let corpus = failure_corpus(2, 11);
        assert_eq!(corpus.len(), 10);
        assert!(corpus.iter().all(|case| case.validate().is_ok()));

        let mut invalid = corpus[0].clone();
        invalid.failure = FailureInjection::StopProcess {
            process: "missing".to_owned(),
            phase: ProcessStopPhase::AfterSpawn,
            kill_after_ms: None,
        };
        assert!(invalid.validate().is_err());

        invalid.failure = FailureInjection::KillNode { logical_node_id: 3 };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn telemetry_census_tracks_transient_resources_to_zero() {
        let frame = |channel: &str, payload: Value| {
            json!({
                "channel": channel,
                "stream": "1#1",
                "payload": {
                    "encoding": "utf8",
                    "value": payload.to_string(),
                },
            })
            .to_string()
        };
        let started = frame(
            "runtime.actors",
            json!({
                "event": "started",
                "actor": {
                    "address": "process",
                    "actor_type": "swactor_process::actor::ProcessActor",
                    "poisoned": false,
                },
            }),
        );
        let live_arena = frame(
            "mvp.arena",
            json!({"live_bytes": 64, "active_leases": 1, "pending_leases": 0}),
        );
        let mut census = TelemetryResourceCensus::default();
        census
            .ingest(&format!("{started}\n{live_arena}\n"))
            .unwrap();
        let live = census.snapshot();
        let health = json!({
            "nodes": [{"observation": {"event": {"executions": []}}}],
            "running_nodes": [1],
            "resources": live,
        });
        assert!(pending_resource_cleanup(&health, &BTreeSet::new()).is_some());
        let lost_node_health = json!({
            "nodes": [],
            "running_nodes": [],
            "resources": health["resources"].clone(),
        });
        assert_eq!(
            pending_resource_cleanup(&lost_node_health, &BTreeSet::new()),
            None
        );

        let stopped = frame(
            "runtime.actors",
            json!({
                "event": "stopped",
                "actor": {
                    "address": "process",
                    "actor_type": "swactor_process::actor::ProcessActor",
                    "poisoned": false,
                },
            }),
        );
        let empty_arena = frame(
            "mvp.arena",
            json!({"live_bytes": 0, "active_leases": 0, "pending_leases": 0}),
        );
        census
            .ingest(&format!("{stopped}\n{empty_arena}\n"))
            .unwrap();
        let clean = census.snapshot();
        let health = json!({
            "nodes": [{"observation": {"event": {"executions": []}}}],
            "running_nodes": [1],
            "resources": clean,
        });
        assert_eq!(pending_resource_cleanup(&health, &BTreeSet::new()), None);
    }
    #[test]
    fn shrinker_removes_unrelated_work_and_reduces_payloads() {
        let case = random_short_dags(19, 1, 3).remove(0);
        let minimized = shrink_failure(case, |_| true);
        assert_eq!(minimized.processes.len(), 1);
        assert_eq!(minimized.processes[0].actions.len(), 1);
        assert!(matches!(
            &minimized.processes[0].actions[0].operation,
            ActionOp::PublishBlob { bytes, .. } if bytes.is_empty()
        ));
        assert_eq!(minimized.node_count, 2);
    }
}
