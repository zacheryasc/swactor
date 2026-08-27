use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use proptest::prelude::*;
use swactor_process::{ExitStatus, ProcessOutput};
use swactor_process_context::{
    BootstrapFailure, ContextualProcessOutput, Coordinator, Effect, Event, EventKind,
    ExecutionIdentity,
};

mod common;
use common::oracle::{ContractOracle, ExecutionResources, ResourceLedger, TraceStep};
use common::scenario::ScenarioDag;

#[derive(Default)]
struct DeterministicDriver {
    coordinators: BTreeMap<u64, Coordinator>,
    ledger: ResourceLedger,
    trace: Vec<TraceStep>,
}

impl DeterministicDriver {
    fn deliver(&mut self, event: Event) {
        let identity = event.identity;
        if matches!(event.kind, EventKind::SpawnRequested) {
            let replace = self
                .coordinators
                .get(&identity.execution_id)
                .is_none_or(|coordinator| {
                    coordinator.snapshot().identity.generation < identity.generation
                });
            if replace {
                self.coordinators.insert(
                    identity.execution_id,
                    Coordinator::new(identity, Duration::from_millis(50)),
                );
            }
        }
        let current = self
            .coordinators
            .get(&identity.execution_id)
            .map(|coordinator| coordinator.snapshot().identity);
        if current == Some(identity) {
            self.observe_completion(&event);
        }
        let effects = self
            .coordinators
            .get_mut(&identity.execution_id)
            .map_or_else(Vec::new, |coordinator| coordinator.apply(event.clone()));
        self.apply_effects(identity, &effects);
        let outputs = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Emit(output) => Some(output.clone()),
                _ => None,
            })
            .collect();
        self.trace.push(TraceStep {
            event,
            effects,
            outputs,
            ledger: self.ledger.clone(),
        });
    }

    fn observe_completion(&mut self, event: &Event) {
        match event.kind {
            EventKind::ProvisionSucceeded => {
                self.ledger.executions.insert(
                    event.identity.execution_id,
                    ExecutionResources {
                        bootstrap_owner: Some(event.identity.execution_id),
                        session_owner: Some(event.identity.execution_id),
                        capability_owner: Some(event.identity.execution_id),
                        arena_owner: Some(event.identity.execution_id),
                        session_generation: Some(event.identity.generation),
                        accepted_generation: Some(event.identity.generation),
                        ..ExecutionResources::default()
                    },
                );
            }
            EventKind::AttachmentSucceeded => {
                let accepts_completion = self
                    .coordinators
                    .get(&event.identity.execution_id)
                    .is_some_and(|coordinator| {
                        let snapshot = coordinator.snapshot();
                        !snapshot.process_terminal && snapshot.context_resolution.is_none()
                    });
                if accepts_completion
                    && let Some(resources) =
                        self.ledger.executions.get_mut(&event.identity.execution_id)
                {
                    resources.route_owner = Some(event.identity.execution_id);
                }
            }
            _ => {}
        }
    }

    fn apply_effects(&mut self, identity: ExecutionIdentity, effects: &[Effect]) {
        for effect in effects {
            let resources = self
                .ledger
                .executions
                .entry(identity.execution_id)
                .or_default();
            match effect {
                Effect::SpawnNativeProcess => {
                    resources
                        .inherited_descriptor_owners
                        .insert(identity.execution_id);
                }
                Effect::ArmAttachmentDeadline(_) => {
                    resources.deadline_owner = Some(identity.execution_id);
                }
                Effect::CancelAttachmentDeadline => resources.deadline_owner = None,
                Effect::CloseBootstrap => {
                    resources.bootstrap_owner = None;
                    resources.inherited_descriptor_owners.clear();
                    resources.close_count += 1;
                }
                Effect::RevokeSession => {
                    resources.session_owner = None;
                    resources.capability_owner = None;
                    resources.route_owner = None;
                    resources.revoke_count += 1;
                }
                Effect::ReleaseArena => {
                    resources.arena_owner = None;
                    resources.release_count += 1;
                }
                Effect::ProvisionSession
                | Effect::AcceptBootstrap
                | Effect::RejectBootstrap { .. }
                | Effect::Emit(_)
                | Effect::StopNativeProcess
                | Effect::Finish => {}
            }
        }
    }
}

fn identity(execution_id: u64) -> ExecutionIdentity {
    ExecutionIdentity {
        execution_id,
        generation: 1,
    }
}

fn event(identity: ExecutionIdentity, kind: EventKind) -> Event {
    Event::new(identity, kind)
}

fn happy_events(identity: ExecutionIdentity) -> Vec<Event> {
    vec![
        event(identity, EventKind::SpawnRequested),
        event(identity, EventKind::ProvisionSucceeded),
        event(
            identity,
            EventKind::Process(ProcessOutput::Started { pid: 41 }),
        ),
        event(
            identity,
            EventKind::BootstrapClaimed {
                handle_execution_id: identity.execution_id,
            },
        ),
        event(identity, EventKind::AttachmentSucceeded),
        event(
            identity,
            EventKind::Process(ProcessOutput::Exited {
                status: ExitStatus::Code(0),
            }),
        ),
        event(identity, EventKind::CleanupCompleted),
    ]
}

fn drive(events: impl IntoIterator<Item = Event>) -> DeterministicDriver {
    let mut driver = DeterministicDriver::default();
    for event in events {
        driver.deliver(event);
    }
    driver
}

fn invariant(trace: &[TraceStep], quiescent: bool) -> &'static str {
    ContractOracle::check(trace, quiescent)
        .expect_err("trace should be rejected")
        .id
}

fn scenario(executions: u8, outcomes: &[u8], seed: u8) -> ScenarioDag {
    let mut dag = ScenarioDag::new();
    for index in 0..u32::from(executions) {
        let identity = identity(u64::from(index) + 1);
        let base = index * 100;
        dag.add_node(base, event(identity, EventKind::SpawnRequested))
            .unwrap();
        dag.add_node(base + 1, event(identity, EventKind::ProvisionSucceeded))
            .unwrap();
        dag.add_edge(base, base + 1).unwrap();
        let outcome = outcomes[index as usize % outcomes.len()] % 6;
        let stale_identity = ExecutionIdentity {
            execution_id: identity.execution_id,
            generation: 0,
        };
        dag.add_node(
            base + 50,
            event(stale_identity, EventKind::AttachmentDeadline),
        )
        .unwrap();
        dag.add_edge(base, base + 50).unwrap();
        if outcome == 4 {
            dag.add_node(
                base + 2,
                event(
                    identity,
                    EventKind::Process(ProcessOutput::SpawnFailed {
                        error: "missing executable".to_owned(),
                    }),
                ),
            )
            .unwrap();
            dag.add_node(base + 3, event(identity, EventKind::CleanupCompleted))
                .unwrap();
            dag.add_edge(base + 1, base + 2).unwrap();
            dag.add_edge(base + 2, base + 3).unwrap();
            continue;
        }

        dag.add_node(
            base + 2,
            event(
                identity,
                EventKind::Process(ProcessOutput::Started { pid: 100 + index }),
            ),
        )
        .unwrap();
        dag.add_edge(base + 1, base + 2).unwrap();
        let exit_node = base + 8;
        dag.add_node(
            exit_node,
            event(
                identity,
                EventKind::Process(ProcessOutput::Exited {
                    status: ExitStatus::Code(0),
                }),
            ),
        )
        .unwrap();
        match outcome {
            0 | 5 => {
                dag.add_node(
                    base + 3,
                    event(
                        identity,
                        EventKind::BootstrapClaimed {
                            handle_execution_id: identity.execution_id,
                        },
                    ),
                )
                .unwrap();
                dag.add_node(base + 4, event(identity, EventKind::AttachmentSucceeded))
                    .unwrap();
                dag.add_edge(base + 2, base + 3).unwrap();
                dag.add_edge(base + 3, base + 4).unwrap();
                if outcome == 0 {
                    dag.add_edge(base + 4, exit_node).unwrap();
                }
            }
            1 => {
                dag.add_node(
                    base + 3,
                    event(
                        identity,
                        EventKind::BootstrapClaimed {
                            handle_execution_id: identity.execution_id,
                        },
                    ),
                )
                .unwrap();
                dag.add_node(
                    base + 4,
                    event(
                        identity,
                        EventKind::AttachmentFailed("route rejected".to_owned()),
                    ),
                )
                .unwrap();
                dag.add_edge(base + 2, base + 3).unwrap();
                dag.add_edge(base + 3, base + 4).unwrap();
                dag.add_edge(base + 4, exit_node).unwrap();
            }
            2 => {
                dag.add_node(
                    base + 3,
                    event(
                        identity,
                        EventKind::BootstrapClaimed {
                            handle_execution_id: identity.execution_id,
                        },
                    ),
                )
                .unwrap();
                dag.add_node(base + 4, event(identity, EventKind::AttachmentDeadline))
                    .unwrap();
                dag.add_edge(base + 2, base + 3).unwrap();
                dag.add_edge(base + 2, base + 4).unwrap();
                dag.add_edge(base + 4, exit_node).unwrap();
                dag.add_node(base + 5, event(identity, EventKind::AttachmentDeadline))
                    .unwrap();
                dag.add_edge(base + 2, base + 5).unwrap();
                dag.add_edge(base + 5, exit_node).unwrap();
            }
            _ => {
                dag.add_node(base + 3, event(identity, EventKind::StopRequested))
                    .unwrap();
                dag.add_edge(base + 1, base + 3).unwrap();
                dag.add_edge(base + 3, exit_node).unwrap();
            }
        }
        dag.add_node(base + 9, event(identity, EventKind::CleanupCompleted))
            .unwrap();
        dag.add_edge(base + 2, exit_node).unwrap();
        dag.add_edge(exit_node, base + 9).unwrap();
    }

    if executions > 1 && seed & 1 == 1 {
        for index in 0..u32::from(executions - 1) {
            dag.add_edge(index * 100, (index + 1) * 100 + 1).unwrap();
        }
    }
    dag
}

#[test]
fn happy_path_resolves_ready_before_native_terminal_and_cleans_up() {
    let driver = drive(happy_events(identity(1)));
    ContractOracle::check(&driver.trace, true).expect("legal happy trace");
    let outputs = driver
        .trace
        .iter()
        .flat_map(|step| step.outputs.iter())
        .collect::<Vec<_>>();
    let ready = outputs
        .iter()
        .position(|output| matches!(output, ContextualProcessOutput::ContextReady))
        .unwrap();
    let exited = outputs
        .iter()
        .position(|output| {
            matches!(
                output,
                ContextualProcessOutput::Process(ProcessOutput::Exited { .. })
            )
        })
        .unwrap();
    assert!(ready < exited);
}

#[test]
fn exit_before_attachment_emits_failure_before_process_terminal() {
    let identity = identity(1);
    let driver = drive([
        event(identity, EventKind::SpawnRequested),
        event(identity, EventKind::ProvisionSucceeded),
        event(
            identity,
            EventKind::Process(ProcessOutput::Started { pid: 7 }),
        ),
        event(
            identity,
            EventKind::Process(ProcessOutput::Exited {
                status: ExitStatus::Code(3),
            }),
        ),
        event(identity, EventKind::CleanupCompleted),
    ]);
    ContractOracle::check(&driver.trace, true).expect("legal early-exit trace");
    let terminal_effects = &driver.trace[3].effects;
    let failed = terminal_effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                Effect::Emit(ContextualProcessOutput::BootstrapFailed { .. })
            )
        })
        .unwrap();
    let exited = terminal_effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                Effect::Emit(ContextualProcessOutput::Process(
                    ProcessOutput::Exited { .. }
                ))
            )
        })
        .unwrap();
    assert!(failed < exited);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_legal_scenario_dags_hold_for_deterministic_linearizations(
        executions in 1_u8..=3,
        outcomes in prop::collection::vec(0_u8..6, 3),
        seed in any::<u8>(),
    ) {
        let dag = scenario(executions, &outcomes, seed);
        let linearizations = dag.linearizations(32).expect("valid generated DAG");
        prop_assert!(!linearizations.is_empty());
        for events in linearizations {
            let driver = drive(events);
            prop_assert_eq!(ContractOracle::check(&driver.trace, true), Ok(()));
        }
    }
}

#[test]
fn oracle_rejects_minimally_invalid_contract_neighbors() {
    let identity = identity(1);

    let duplicate = drive([
        happy_events(identity)[0].clone(),
        happy_events(identity)[1].clone(),
        happy_events(identity)[2].clone(),
        happy_events(identity)[3].clone(),
        happy_events(identity)[3].clone(),
    ]);
    assert_eq!(invariant(&duplicate.trace, false), "I2");

    let cross = drive([
        happy_events(identity)[0].clone(),
        happy_events(identity)[1].clone(),
        happy_events(identity)[2].clone(),
        event(
            identity,
            EventKind::BootstrapClaimed {
                handle_execution_id: 2,
            },
        ),
    ]);
    assert_eq!(invariant(&cross.trace, false), "I3");

    let mut sibling_descriptor = drive(happy_events(identity));
    sibling_descriptor.trace[1]
        .ledger
        .executions
        .get_mut(&1)
        .unwrap()
        .inherited_descriptor_owners = BTreeSet::from([2]);
    assert_eq!(invariant(&sibling_descriptor.trace, false), "I4");

    let mut ready_before_start = drive([event(identity, EventKind::SpawnRequested)]).trace;
    ready_before_start.push(TraceStep {
        event: event(identity, EventKind::AttachmentSucceeded),
        effects: vec![Effect::Emit(ContextualProcessOutput::ContextReady)],
        outputs: vec![ContextualProcessOutput::ContextReady],
        ledger: ready_before_start.last().unwrap().ledger.clone(),
    });
    assert_eq!(invariant(&ready_before_start, false), "I6");

    let started = drive([
        happy_events(identity)[0].clone(),
        happy_events(identity)[1].clone(),
        happy_events(identity)[2].clone(),
    ]);
    let mut ready_without_attach = started.trace.clone();
    ready_without_attach.push(TraceStep {
        event: event(identity, EventKind::AttachmentSucceeded),
        effects: vec![Effect::Emit(ContextualProcessOutput::ContextReady)],
        outputs: vec![ContextualProcessOutput::ContextReady],
        ledger: ready_without_attach.last().unwrap().ledger.clone(),
    });
    assert_eq!(invariant(&ready_without_attach, false), "I7");

    let happy = drive(happy_events(identity));
    let mut double_outcome = happy.trace.clone();
    double_outcome.push(TraceStep {
        event: event(identity, EventKind::AttachmentFailed("late".to_owned())),
        effects: vec![Effect::Emit(ContextualProcessOutput::BootstrapFailed {
            reason: BootstrapFailure::Attachment("late".to_owned()),
        })],
        outputs: vec![ContextualProcessOutput::BootstrapFailed {
            reason: BootstrapFailure::Attachment("late".to_owned()),
        }],
        ledger: double_outcome.last().unwrap().ledger.clone(),
    });
    assert_eq!(invariant(&double_outcome, false), "I8");

    let spawn_failed = drive([
        event(identity, EventKind::SpawnRequested),
        event(identity, EventKind::ProvisionSucceeded),
        event(
            identity,
            EventKind::Process(ProcessOutput::SpawnFailed {
                error: "missing".to_owned(),
            }),
        ),
        event(identity, EventKind::CleanupCompleted),
    ]);
    let mut masquerade = spawn_failed.trace.clone();
    masquerade.push(TraceStep {
        event: event(identity, EventKind::BootstrapRejected("missing".to_owned())),
        effects: vec![Effect::Emit(ContextualProcessOutput::BootstrapFailed {
            reason: BootstrapFailure::ClaimRejected("missing".to_owned()),
        })],
        outputs: vec![ContextualProcessOutput::BootstrapFailed {
            reason: BootstrapFailure::ClaimRejected("missing".to_owned()),
        }],
        ledger: masquerade.last().unwrap().ledger.clone(),
    });
    assert_eq!(invariant(&masquerade, false), "I11");

    let mut unauthorized = happy.trace.clone();
    unauthorized
        .trace_resource_mut(1)
        .unauthorized_open_observed = true;
    assert_eq!(invariant(&unauthorized, false), "I13");

    let mut native_authority = happy.trace.clone();
    native_authority
        .trace_resource_mut(1)
        .native_context_authority = true;
    assert_eq!(invariant(&native_authority, false), "I15");

    let mut repeated_cleanup = happy.trace.clone();
    repeated_cleanup.trace_resource_mut(1).release_count = 2;
    assert_eq!(invariant(&repeated_cleanup, false), "I18");

    let mut leaked = happy.trace.clone();
    leaked
        .last_mut()
        .unwrap()
        .ledger
        .executions
        .get_mut(&1)
        .unwrap()
        .arena_owner = Some(1);
    assert_eq!(invariant(&leaked, true), "I19");

    let mut stale_driver = DeterministicDriver::default();
    stale_driver.deliver(event(identity, EventKind::SpawnRequested));
    stale_driver.deliver(event(
        ExecutionIdentity {
            execution_id: 1,
            generation: 2,
        },
        EventKind::SpawnRequested,
    ));
    let ledger = stale_driver.ledger.clone();
    stale_driver.trace.push(TraceStep {
        event: event(identity, EventKind::StopRequested),
        effects: vec![Effect::StopNativeProcess],
        outputs: Vec::new(),
        ledger,
    });
    assert_eq!(invariant(&stale_driver.trace, false), "I5");

    let mut sibling_failure = happy.trace.clone();
    sibling_failure
        .trace_resource_mut(1)
        .sibling_corruption_observed = true;
    assert_eq!(invariant(&sibling_failure, false), "I20");
}

#[test]
fn checker_calibration_rejects_broken_ledgers_and_effect_records() {
    let identity = identity(1);
    let happy = drive(happy_events(identity));

    let mut wrong_owner = happy.trace.clone();
    wrong_owner.trace_resource_mut(1).session_owner = Some(9);
    assert_eq!(invariant(&wrong_owner, false), "I1");

    let mut wrong_generation = happy.trace.clone();
    wrong_generation.trace_resource_mut(1).accepted_generation = Some(99);
    assert_eq!(invariant(&wrong_generation, false), "I12");

    let mut leaked_environment = happy.trace.clone();
    leaked_environment[0].ledger.bootstrap_internals_exposed = true;
    assert_eq!(invariant(&leaked_environment, false), "I14");

    let mut short_bootstrap_lifetime = happy.trace.clone();
    short_bootstrap_lifetime[1]
        .ledger
        .executions
        .get_mut(&1)
        .unwrap()
        .bootstrap_owner = None;
    assert_eq!(invariant(&short_bootstrap_lifetime, false), "I16");

    let mut early_arena_release = happy.trace.clone();
    early_arena_release[2]
        .ledger
        .executions
        .get_mut(&1)
        .unwrap()
        .arena_owner = None;
    assert_eq!(invariant(&early_arena_release, false), "I17");

    let mut mismatched_effect_record = happy.trace.clone();
    mismatched_effect_record[0]
        .outputs
        .push(ContextualProcessOutput::ContextReady);
    assert_eq!(invariant(&mismatched_effect_record, false), "I9");
}

trait TraceResourceMut {
    fn trace_resource_mut(&mut self, execution_id: u64) -> &mut ExecutionResources;
}

impl TraceResourceMut for Vec<TraceStep> {
    fn trace_resource_mut(&mut self, execution_id: u64) -> &mut ExecutionResources {
        self.iter_mut()
            .find_map(|step| step.ledger.executions.get_mut(&execution_id))
            .expect("execution resource snapshot")
    }
}
