//! Real-binary, multi-node behavioral fuzz harness for Myelin and Swactor.
//!
//! The crate is organized as a module tree; this root is a thin facade that
//! preserves the historical flat API used by `main.rs`.

mod budget;
mod campaign;
mod codegen;
mod corpus;
mod coverage;
mod harness;
mod ir;
mod oracle;
mod remote;
mod resources;
mod shrink;

pub use budget::{Budget, execution_evidence, record_execution_stage};
pub use campaign::{
    ArtifactEnvelope, CAMPAIGN_DEADLINE_SECS, CLEANUP_DEADLINE_SECS, CampaignConfig,
    CampaignLimits, CampaignPlan, DIAGNOSTIC_DEADLINE_SECS, LOCAL_CLEANUP_RESERVE_SECS,
    OfferPolicy, PREPARATION_DEADLINE_SECS, PersistedFixtureEntry, ProviderMode, RecoveryCase,
    WORKLOAD_DEADLINE_SECS,
};
pub use codegen::{render_case_python, render_python};
pub use corpus::{
    failure_corpus, ordered_pair_corpus, random_short_dags, random_short_dags_budgeted,
    stable_corpus,
};
pub use coverage::{
    CoverageKey, CoverageLedger, EvidenceIdentity, NodeRole, PayloadClass, case_coverage,
};
pub use harness::raw_fleet::{
    DeploymentBoundary, RawDockerFleet, RawFleetSnapshot, RawNodeCensus, RawPriorState,
};
pub use harness::{
    ClusterHarness, ClusterHarnessConfig, FixturePathObservation, FixturePathSnapshot,
    HarnessProvider,
};
pub use ir::{
    AccessSpec, Action, ActionClass, ActionObservation, ActionOp, BehaviorCase, CaseObservation,
    CaseResourceBounds, CaseResources, CoverageScenario, DataEdge, DataKind, DataRoute,
    DescriptorFinish, DescriptorObservation, DescriptorReadMethod, DescriptorTerminalResult,
    DescriptorWriteMethod, ExecutionObservation, ExpectedOutcome, FailureInjection,
    LaunchFailureKind, ProcessProgram, ProcessStopPhase, PythonException, TopologyFamily,
    TransferObservation,
};
pub use oracle::{
    BehaviorOracle, BlobPublicationTrace, CausalRole, FailureClass, FailureSignature,
    ObservedOutcome, OracleViolation, OutcomeExpectation, SemanticAction, StreamIncarnationTrace,
};
pub use remote::{
    PaidCampaignPhase, PaidCampaignState, authorized_cleanup_limits, await_cleanup_owner,
    cleanup_paid_ownership, conservative_selected_cost, decode_offer_results, offer_search_request,
    read_campaign_plan, read_coverage_ledger, read_paid_state, recover_cleanup_owner,
    run_cleanup_owner, run_cleanup_supervisor, run_scripted_retained_lifecycle,
    scan_artifacts_for_secret, select_exact_offers, selected_cleanup_limits, start_cleanup_owner,
    write_campaign_plan, write_coverage_ledger, write_paid_state,
};
pub use resources::{
    BuiltBinaries, DeploymentBundle, GateImageIdentity, PreparedArtifactIdentity,
    assemble_deployment_bundle, distinguish_deployment_payload, immutable_registry_reference,
    private_fixture_dir, require_prepared_artifacts, resolve_myelin_binaries,
    runtime_image_identity, stage_deployment_payload,
};
pub use shrink::{shrink_failure, try_shrink_failure, try_shrink_failure_budgeted};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::process::{Command, Stdio};

    use crate::codegen::digest;
    use crate::corpus::SEEDED_MODEL_PATH;

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
                transfer: None,
                kind: Some("blob".to_owned()),
                revision: Some(1),
                errno: None,
                active: None,
                error_type: None,
                error: None,
                descriptor: None,
                barrier: None,
                incarnation: None,
                token: None,
                lap: None,
            }],
            terminal: true,
            exit_success: true,
            exit_status: Some(r#"{"kind":"code","value":0}"#.to_owned()),
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    #[test]
    fn complete_campaign_python_is_syntactically_valid() {
        let cases = crate::corpus::generated_campaign_cases(17, 128, 5).unwrap();
        let sources = cases
            .iter()
            .flat_map(|case| {
                case.processes.iter().map(|process| {
                    render_case_python(case, process, std::time::Duration::from_secs(120))
                })
            })
            .collect::<Vec<_>>();
        let mut child = Command::new("python3")
            .args([
                "-c",
                "import json,sys\nfor i,source in enumerate(json.load(sys.stdin)):\n compile(source, f'<generated-{i}>', 'exec')",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn Python syntax checker");
        serde_json::to_writer(
            child.stdin.as_mut().expect("Python syntax checker stdin"),
            &sources,
        )
        .unwrap();
        drop(child.stdin.take());
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "generated Python syntax failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn attempts_receive_fresh_process_and_namespace_ownership() {
        let case = stable_corpus(2, 19)
            .into_iter()
            .find(|case| {
                case.processes
                    .iter()
                    .flat_map(|process| &process.actions)
                    .any(|action| action.operation.path().starts_with("/cases/"))
            })
            .unwrap();
        let first = case.for_attempt(1, true);
        let second = case.for_attempt(2, true);
        assert_eq!(first.id, case.id);
        assert_ne!(
            first.processes[0].access.execution_id,
            second.processes[0].access.execution_id
        );
        let first_paths = first
            .processes
            .iter()
            .flat_map(|process| &process.actions)
            .map(|action| action.operation.path())
            .filter(|path| path.starts_with("/cases/"))
            .collect::<Vec<_>>();
        let second_paths = second
            .processes
            .iter()
            .flat_map(|process| &process.actions)
            .map(|action| action.operation.path())
            .filter(|path| path.starts_with("/cases/"))
            .collect::<Vec<_>>();
        assert!(first_paths.iter().all(|path| path.contains("/attempt-1/")));
        assert!(second_paths.iter().all(|path| path.contains("/attempt-2/")));
        assert_ne!(first_paths, second_paths);
    }
    #[test]
    fn read_only_fixture_paths_are_neither_scoped_nor_cleaned() {
        let path = "/cases/recovery/persisted".to_owned();
        let case = BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "read-only-fixture".to_owned(),
            seed: 1,
            live_nodes: BTreeSet::from([1, 2]),
            topology: TopologyFamily::Fixed,
            scenarios: BTreeSet::new(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::from([path.clone()]),
            resource_bounds: CaseResourceBounds::default(),
            processes: vec![ProcessProgram {
                id: "reader".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::Lookup {
                    path: path.clone(),
                    expected_kind: "blob".to_owned(),
                })],
            }],
            failure: FailureInjection::None,
        };
        case.validate().unwrap();
        let scoped = case.for_attempt(7, true);
        assert_eq!(scoped.processes[0].actions[0].operation.path(), path);
        assert!(scoped.owned_paths().is_empty());

        let mut invalid = case;
        invalid.processes[0].actions = vec![Action::ok(ActionOp::Unlink { path })];
        assert!(invalid.validate().unwrap_err().contains("read-only"));
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
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "case".to_owned(),
            seed: 1,
            live_nodes: crate::ir::contiguous_nodes(2),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: Default::default(),
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
        torn.request_id = case.execution_request_id(&case.processes[0]);
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

        let mut observation = CaseObservation {
            case_id: case.id.clone(),
            executions: vec![valid.clone()],
        };
        observation.executions[0].request_id = case.execution_request_id(&case.processes[0]);
        BehaviorOracle::verify(&case, &observation).unwrap();
        let duplicate = observation.executions[0].results[0].clone();
        observation.executions[0].results.push(duplicate);
        assert!(BehaviorOracle::verify(&case, &observation).is_err());

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
    fn typed_action_abort_replays_and_shrinks_without_inventing_a_suffix() {
        let action = Action::ok(ActionOp::Lookup {
            path: SEEDED_MODEL_PATH.to_owned(),
            expected_kind: "blob".to_owned(),
        });
        let case = BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "typed-abort".to_owned(),
            seed: 1,
            live_nodes: crate::ir::contiguous_nodes(2),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::from([SEEDED_MODEL_PATH.to_owned()]),
            resource_bounds: Default::default(),
            processes: vec![ProcessProgram {
                id: "p".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("p"),
                depends_on: Vec::new(),
                actions: vec![action.clone(), action],
            }],
            failure: FailureInjection::None,
        };
        let mut failed = valid_execution();
        failed.request_id = case.execution_request_id(&case.processes[0]);
        failed.exit_success = false;
        failed.exit_status = Some(r#"{"kind":"code","value":1}"#.to_owned());
        failed.results[0].outcome = "error".to_owned();
        failed.results[0].errno = Some(libc::ENOENT);
        failed.results[0].error_type = Some("FileNotFoundError".to_owned());
        failed.results[0].error = Some("binding lookup failed".to_owned());
        let observe = |candidate: &BehaviorCase| CaseObservation {
            case_id: candidate.id.clone(),
            executions: vec![failed.clone()],
        };
        let signature = BehaviorOracle::verify(&case, &observe(&case))
            .unwrap_err()
            .signature;
        assert_eq!(signature.invariant, "unexpected_error");
        assert_eq!(signature.failure_class, FailureClass::OutcomeMismatch);
        assert_eq!(
            signature.causal_role,
            CausalRole::Action(SemanticAction::Lookup)
        );
        let minimized = try_shrink_failure_budgeted(
            case.clone(),
            &Budget::new(std::time::Duration::from_secs(1)),
            |candidate| {
                Ok(BehaviorOracle::verify(candidate, &observe(candidate))
                    .is_err_and(|error| error.signature == signature))
            },
        )
        .unwrap();
        assert_eq!(minimized.processes[0].actions.len(), 1);
        assert_eq!(
            BehaviorOracle::verify(&minimized, &observe(&minimized))
                .unwrap_err()
                .signature,
            signature,
        );

        let mut canceled = failed.clone();
        canceled.results[0].error_type = Some("TimeoutError".to_owned());
        assert!(crate::oracle::recorded_action_failure(&case.processes[0], &canceled).is_none());
        let mut missing_prefix = failed.clone();
        missing_prefix.results[0].step = 1;
        assert!(
            crate::oracle::recorded_action_failure(&case.processes[0], &missing_prefix).is_none()
        );
        let mut signaled = failed.clone();
        signaled.exit_status = Some(r#"{"kind":"signal","value":9}"#.to_owned());
        assert!(crate::oracle::recorded_action_failure(&case.processes[0], &signaled).is_none());

        let mut with_sibling = case.clone();
        let mut sibling = case.processes[0].clone();
        sibling.id = "missing-output".to_owned();
        sibling.access.execution_id = "missing-output".to_owned();
        with_sibling.processes.push(sibling);
        let mut observation = observe(&with_sibling);
        let mut missing = valid_execution();
        missing.process = "missing-output".to_owned();
        missing.request_id = with_sibling.execution_request_id(&with_sibling.processes[1]);
        missing.results.clear();
        observation.executions.push(missing);
        assert_eq!(
            BehaviorOracle::verify(&with_sibling, &observation)
                .unwrap_err()
                .signature
                .failure_class,
            FailureClass::MissingEvidence,
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
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: "linearized".to_owned(),
            seed: 1,
            live_nodes: crate::ir::contiguous_nodes(2),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: std::collections::BTreeSet::from([
                SEEDED_MODEL_PATH.to_owned()
            ]),
            resource_bounds: Default::default(),
            processes: vec![program("first"), program("second")],
            failure: FailureInjection::None,
        };
        first.request_id = case.execution_request_id(&case.processes[0]);
        second.request_id = case.execution_request_id(&case.processes[1]);
        let error = BehaviorOracle::verify(
            &case,
            &CaseObservation {
                case_id: case.id.clone(),
                executions: vec![first, second],
            },
        )
        .unwrap_err();
        assert_eq!(error.invariant, "linearizability");
        let mut over_bound = case;
        over_bound.resource_bounds.max_race_states = 1;
        assert!(
            over_bound
                .validate()
                .unwrap_err()
                .contains("legal race states")
        );
    }

    #[test]
    fn failure_injections_validate_real_targets() {
        let corpus = failure_corpus(2, 11);
        assert_eq!(corpus.len(), 8);
        assert!(corpus.iter().all(|case| case.validate().is_ok()));

        let mut invalid = corpus[0].clone();
        invalid.failure = FailureInjection::StopProcess {
            process: "missing".to_owned(),
            phase: ProcessStopPhase::AfterSpawn,
            kill_after_ms: None,
        };
        assert!(invalid.validate().is_err());

        invalid.failure = FailureInjection::SlowProcess {
            process: "missing".to_owned(),
            parked_path: "/cases/slow/parked".to_owned(),
            release_path: "/cases/slow/release".to_owned(),
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn typed_read_abort_shrinking_preserves_publication_and_causal_order() {
        let path = "/cases/shrink/required";
        let publication = Action::ok(ActionOp::PublishBlob {
            path: path.to_owned(),
            bytes: b"payload".to_vec(),
        });
        let read = Action::ok(ActionOp::ReadBlob {
            path: path.to_owned(),
            expected: b"payload".to_vec(),
        });
        let unrelated = Action::ok(ActionOp::PublishBlob {
            path: "/cases/shrink/unrelated".to_owned(),
            bytes: b"unrelated".to_vec(),
        });
        let program = |id: &str, actions, depends_on| ProcessProgram {
            id: id.to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(id),
            depends_on,
            actions,
        };
        // The same ENOENT is a runtime defect after publication, but correct
        // behavior without publication. A typed signature alone cannot tell.
        let observe = |case: &BehaviorCase| CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .map(|process| {
                    let mut execution = valid_execution();
                    execution.process = process.id.clone();
                    execution.request_id = case.execution_request_id(process);
                    execution.logical_node_id = process.logical_node_id;
                    execution.results.clear();
                    for (step, action) in process.actions.iter().enumerate() {
                        let mut result = valid_execution().results.remove(0);
                        result.process = process.id.clone();
                        result.step = step;
                        result.path = action.operation.path().to_owned();
                        match &action.operation {
                            ActionOp::PublishBlob { bytes, .. } => {
                                result.action = "publish_blob".to_owned();
                                result.length = Some(bytes.len());
                                result.digest = Some(digest(bytes));
                                result.transfer = Some(TransferObservation {
                                    length: bytes.len(),
                                    digest: digest(bytes),
                                    complete: true,
                                });
                            }
                            ActionOp::ReadBlob { .. } => {
                                result.action = "read_blob".to_owned();
                                result.outcome = "error".to_owned();
                                result.errno = Some(libc::ENOENT);
                                result.error_type = Some("FileNotFoundError".to_owned());
                                result.error = Some("blob is absent".to_owned());
                                result.kind = None;
                                result.revision = None;
                                execution.exit_success = false;
                                execution.exit_status =
                                    Some(r#"{"kind":"code","value":1}"#.to_owned());
                            }
                            ActionOp::AwaitEntry { .. } => {
                                result.action = "lookup".to_owned();
                            }
                            _ => unreachable!(),
                        }
                        execution.results.push(result);
                        if !execution.exit_success {
                            break;
                        }
                    }
                    execution
                })
                .collect(),
        };
        // Cover local publication, dependency-ordered publication, and a
        // concurrent publisher ordered by the reader's explicit entry wait.
        for (separate_processes, wait_for_entry) in [(false, false), (true, false), (true, true)] {
            let processes = if separate_processes {
                vec![
                    program(
                        "setup",
                        vec![publication.clone(), unrelated.clone()],
                        Vec::new(),
                    ),
                    program(
                        "reader",
                        if wait_for_entry {
                            vec![
                                Action::ok(ActionOp::AwaitEntry {
                                    path: path.to_owned(),
                                    expected_kind: "blob".to_owned(),
                                }),
                                read.clone(),
                            ]
                        } else {
                            vec![read.clone()]
                        },
                        if wait_for_entry {
                            Vec::new()
                        } else {
                            vec!["setup".to_owned()]
                        },
                    ),
                    program("independent", vec![unrelated.clone()], Vec::new()),
                ]
            } else {
                vec![program(
                    "reader",
                    vec![publication.clone(), unrelated.clone(), read.clone()],
                    Vec::new(),
                )]
            };
            let case = BehaviorCase {
                schema_version: crate::ir::CASE_SCHEMA_VERSION,
                generator_version: crate::ir::GENERATOR_VERSION,
                id: "shrink-read-abort".to_owned(),
                seed: 1,
                live_nodes: crate::ir::contiguous_nodes(2),
                topology: Default::default(),
                scenarios: Default::default(),
                routes: Vec::new(),
                read_only_fixture_paths: BTreeSet::new(),
                resource_bounds: Default::default(),
                processes,
                failure: FailureInjection::None,
            };
            case.validate().unwrap();
            let signature = BehaviorOracle::verify(&case, &observe(&case))
                .unwrap_err()
                .signature;
            assert_eq!(signature.invariant, "unexpected_error");
            assert_eq!(signature.failure_class, FailureClass::OutcomeMismatch);
            assert_eq!(
                signature.causal_role,
                CausalRole::Action(SemanticAction::ReadBlob)
            );
            let mut unjustified = case.clone();
            unjustified.processes = vec![program("reader", vec![read.clone()], Vec::new())];
            unjustified.validate().unwrap();
            assert_eq!(
                BehaviorOracle::verify(&unjustified, &observe(&unjustified))
                    .unwrap_err()
                    .signature,
                signature,
            );
            let minimized = try_shrink_failure_budgeted(
                case,
                &Budget::new(std::time::Duration::from_secs(1)),
                |candidate| {
                    if !candidate
                        .processes
                        .iter()
                        .flat_map(|process| &process.actions)
                        .any(|action| matches!(action.operation, ActionOp::ReadBlob { .. }))
                    {
                        return Ok(false);
                    }
                    Ok(BehaviorOracle::verify(candidate, &observe(candidate))
                        .is_err_and(|error| error.signature == signature))
                },
            )
            .unwrap();
            let actions = minimized
                .processes
                .iter()
                .flat_map(|process| &process.actions)
                .collect::<Vec<_>>();
            assert_eq!(actions.len(), 2 + usize::from(wait_for_entry));
            assert!(matches!(&actions[0].operation,
                ActionOp::PublishBlob { path: actual, bytes } if actual == path && bytes.is_empty()));
            assert!(matches!(&actions.last().unwrap().operation,
                ActionOp::ReadBlob { path: actual, expected } if actual == path && expected.is_empty()));
            assert_eq!(
                minimized.processes.len(),
                if separate_processes { 2 } else { 1 }
            );
            if separate_processes && !wait_for_entry {
                assert_eq!(minimized.processes[1].depends_on, ["setup"]);
            }
            if wait_for_entry {
                assert!(matches!(actions[1].operation, ActionOp::AwaitEntry { .. }));
            }
            assert_eq!(
                BehaviorOracle::verify(&minimized, &observe(&minimized))
                    .unwrap_err()
                    .signature,
                signature,
            );
        }
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
        assert_eq!(minimized.node_count(), 2);
    }
    #[test]
    fn fallible_shrinker_stops_on_restoration_error() {
        let case = random_short_dags(23, 1, 3).remove(0);
        let mut attempts = 0;
        let error = try_shrink_failure(case, |_| {
            attempts += 1;
            Err::<bool, _>("fixture restoration failed")
        })
        .unwrap_err();
        assert_eq!(error, "fixture restoration failed");
        assert_eq!(attempts, 1);
    }

    #[test]
    fn cancelled_diagnosis_cannot_accept_a_partial_minimization() {
        let case = random_short_dags(23, 1, 3).remove(0);
        let budget = Budget::new(std::time::Duration::from_secs(30));
        let mut attempts = 0;
        let error = try_shrink_failure_budgeted(case, &budget, |_| {
            attempts += 1;
            budget.cancel();
            Ok(true)
        })
        .unwrap_err();
        assert!(error.contains("budget cancelled"), "{error}");
        assert_eq!(
            attempts, 1,
            "cancelled diagnosis must not launch another replay"
        );
    }
}
