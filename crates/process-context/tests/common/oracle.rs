use std::collections::{BTreeMap, BTreeSet};

use swactor_process::ProcessOutput;

use swactor_process_context::{
    ContextualProcessOutput, Effect, Event, EventKind, ExecutionIdentity,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecutionResources {
    pub bootstrap_owner: Option<u64>,
    pub inherited_descriptor_owners: BTreeSet<u64>,
    pub session_owner: Option<u64>,
    pub capability_owner: Option<u64>,
    pub route_owner: Option<u64>,
    pub arena_owner: Option<u64>,
    pub deadline_owner: Option<u64>,
    pub session_generation: Option<u64>,
    pub accepted_generation: Option<u64>,
    pub unauthorized_open_observed: bool,
    pub native_context_authority: bool,
    pub sibling_corruption_observed: bool,
    pub close_count: usize,
    pub revoke_count: usize,
    pub release_count: usize,
}

impl ExecutionResources {
    fn is_empty(&self) -> bool {
        self.bootstrap_owner.is_none()
            && self.inherited_descriptor_owners.is_empty()
            && self.session_owner.is_none()
            && self.capability_owner.is_none()
            && self.route_owner.is_none()
            && self.arena_owner.is_none()
            && self.deadline_owner.is_none()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceLedger {
    pub executions: BTreeMap<u64, ExecutionResources>,
    pub bootstrap_internals_exposed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceStep {
    pub event: Event,
    pub effects: Vec<Effect>,
    pub outputs: Vec<ContextualProcessOutput>,
    pub ledger: ResourceLedger,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvariantViolation {
    pub id: &'static str,
    pub detail: String,
}

#[derive(Default)]
struct Facts {
    started: bool,
    terminal: bool,
    claim_count: usize,
    attachment_succeeded: bool,
    context_outcome: Option<&'static str>,
    spawn_failed: bool,
    spawn_pending: bool,
}

pub struct ContractOracle;

impl ContractOracle {
    pub fn check(trace: &[TraceStep], quiescent: bool) -> Result<(), InvariantViolation> {
        let mut facts = BTreeMap::<ExecutionIdentity, Facts>::new();
        let mut current_generation = BTreeMap::<u64, u64>::new();
        for step in trace {
            let identity = step.event.identity;
            if matches!(step.event.kind, EventKind::SpawnRequested) {
                current_generation
                    .entry(identity.execution_id)
                    .and_modify(|generation| *generation = (*generation).max(identity.generation))
                    .or_insert(identity.generation);
            }
            let stale = current_generation
                .get(&identity.execution_id)
                .is_some_and(|generation| *generation != identity.generation);
            if stale && (!step.effects.is_empty() || !step.outputs.is_empty()) {
                return violation("I5", "stale event produced observable effects");
            }

            let emitted = step
                .effects
                .iter()
                .filter_map(|effect| match effect {
                    Effect::Emit(output) => Some(output.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if emitted != step.outputs {
                return violation("I9", "recorded outputs disagree with emitted effects");
            }

            if !stale {
                let execution = facts.entry(identity).or_default();
                Self::observe_event(identity, &step.event.kind, execution)?;
                Self::observe_effects(&step.effects, execution)?;
                Self::observe_outputs(&step.outputs, execution)?;
            }
            Self::check_ledger(&step.ledger, &facts)?;
        }

        if quiescent {
            let Some(last) = trace.last() else {
                return Ok(());
            };
            if last
                .ledger
                .executions
                .values()
                .any(|resources| !resources.is_empty())
            {
                return violation("I19", "context resources remain at quiescence");
            }
            for execution in facts.values() {
                if execution.started && (!execution.terminal || execution.context_outcome.is_none())
                {
                    return violation(
                        "I10",
                        "started execution did not reach terminal process and context outcomes",
                    );
                }
            }
        }
        Ok(())
    }

    fn observe_event(
        identity: ExecutionIdentity,
        kind: &EventKind,
        facts: &mut Facts,
    ) -> Result<(), InvariantViolation> {
        match kind {
            EventKind::ProvisionSucceeded => {}
            EventKind::Process(ProcessOutput::Started { .. }) => {
                facts.spawn_pending = false;
            }
            EventKind::Process(ProcessOutput::SpawnFailed { .. }) => {
                facts.spawn_pending = false;
            }
            EventKind::BootstrapClaimed {
                handle_execution_id,
            } => {
                if *handle_execution_id != identity.execution_id {
                    return violation("I3", "execution claimed another execution's handle");
                }
                facts.claim_count += 1;
                if facts.claim_count > 1 {
                    return violation("I2", "bootstrap handle was claimed more than once");
                }
            }
            EventKind::AttachmentSucceeded => facts.attachment_succeeded = true,
            _ => {}
        }
        Ok(())
    }

    fn observe_effects(effects: &[Effect], facts: &mut Facts) -> Result<(), InvariantViolation> {
        for effect in effects {
            if matches!(effect, Effect::SpawnNativeProcess) {
                facts.spawn_pending = true;
            }
            if let Effect::RejectBootstrap { .. } = effect {
                // Rejection is valid lifecycle behavior and does not mutate facts.
            }
        }
        Ok(())
    }

    fn observe_outputs(
        outputs: &[ContextualProcessOutput],
        facts: &mut Facts,
    ) -> Result<(), InvariantViolation> {
        for output in outputs {
            match output {
                ContextualProcessOutput::Process(ProcessOutput::Started { .. }) => {
                    if facts.started || facts.terminal {
                        return violation("I9", "process start fact changed after resolution");
                    }
                    facts.started = true;
                }
                ContextualProcessOutput::Process(ProcessOutput::SpawnFailed { .. }) => {
                    if facts.started || facts.terminal {
                        return violation("I9", "spawn failure conflicts with process facts");
                    }
                    facts.spawn_failed = true;
                    facts.terminal = true;
                }
                ContextualProcessOutput::Process(
                    ProcessOutput::Exited { .. } | ProcessOutput::Error { .. },
                ) => {
                    if facts.terminal {
                        return violation("I9", "process emitted multiple terminal facts");
                    }
                    if facts.started && facts.context_outcome.is_none() {
                        return violation(
                            "I10",
                            "started process terminated before context resolution output",
                        );
                    }
                    facts.terminal = true;
                }
                ContextualProcessOutput::Process(
                    ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_),
                ) => {}
                ContextualProcessOutput::ContextReady => {
                    if !facts.started {
                        return violation("I6", "context became ready before OS start");
                    }
                    if facts.claim_count != 1 || !facts.attachment_succeeded {
                        return violation(
                            "I7",
                            "context ready lacks claim or successful attachment",
                        );
                    }
                    if facts.context_outcome.replace("ready").is_some() {
                        return violation("I8", "context emitted more than one outcome");
                    }
                }
                ContextualProcessOutput::BootstrapFailed { .. } => {
                    if facts.spawn_failed {
                        return violation("I11", "OS spawn failure became bootstrap failure");
                    }
                    if !facts.started {
                        return violation("I6", "bootstrap failed before OS spawn resolved");
                    }
                    if facts.context_outcome.replace("failed").is_some() {
                        return violation("I8", "context emitted more than one outcome");
                    }
                }
            }
        }
        Ok(())
    }

    fn check_ledger(
        ledger: &ResourceLedger,
        facts: &BTreeMap<ExecutionIdentity, Facts>,
    ) -> Result<(), InvariantViolation> {
        if ledger.bootstrap_internals_exposed {
            return violation("I14", "child environment exposes bootstrap internals");
        }

        let mut bootstrap_owners = BTreeSet::new();
        let mut session_owners = BTreeSet::new();
        let mut arena_owners = BTreeSet::new();
        for (execution_id, resources) in &ledger.executions {
            for owner in [
                resources.bootstrap_owner,
                resources.session_owner,
                resources.capability_owner,
                resources.route_owner,
                resources.arena_owner,
                resources.deadline_owner,
            ]
            .into_iter()
            .flatten()
            {
                if owner != *execution_id {
                    return violation("I1", "resource owner differs from execution identity");
                }
            }
            if let Some(owner) = resources.bootstrap_owner
                && !bootstrap_owners.insert(owner)
            {
                return violation("I1", "bootstrap handle is shared by executions");
            }
            if let Some(owner) = resources.session_owner
                && !session_owners.insert(owner)
            {
                return violation("I1", "host session is shared by executions");
            }
            if let Some(owner) = resources.arena_owner
                && !arena_owners.insert(owner)
            {
                return violation("I1", "arena is shared by executions");
            }
            if resources
                .inherited_descriptor_owners
                .iter()
                .any(|owner| owner != execution_id)
            {
                return violation("I4", "child inherited a sibling descriptor");
            }
            if resources.session_owner != resources.capability_owner
                || resources.session_generation != resources.accepted_generation
            {
                return violation("I12", "session capability or generation is unauthorized");
            }
            if resources.unauthorized_open_observed {
                return violation("I13", "session opened a path outside authorized prefixes");
            }
            if resources.native_context_authority {
                return violation("I15", "native spawn received contextual authority");
            }
            if resources.close_count > 1
                || resources.revoke_count > 1
                || resources.release_count > 1
            {
                return violation("I18", "cleanup effect executed more than once");
            }
            if resources.sibling_corruption_observed {
                return violation("I20", "one execution corrupted sibling resources");
            }

            let running = facts.iter().any(|(identity, execution)| {
                identity.execution_id == *execution_id && execution.started && !execution.terminal
            });
            if running && resources.arena_owner != Some(*execution_id) {
                return violation("I17", "arena was released while child was running");
            }
            let spawn_pending = facts.iter().any(|(identity, execution)| {
                identity.execution_id == *execution_id && execution.spawn_pending
            });
            if spawn_pending && resources.bootstrap_owner != Some(*execution_id) {
                return violation("I16", "bootstrap resource did not outlive spawn attempt");
            }
        }
        Ok(())
    }
}

fn violation<T>(id: &'static str, detail: impl Into<String>) -> Result<T, InvariantViolation> {
    Err(InvariantViolation {
        id,
        detail: detail.into(),
    })
}
