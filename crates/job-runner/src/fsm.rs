//! Job lifecycle FSM — spec §6.
//!
//! States: `PENDING → RUNNING → COMPLETED | FAILED`.
//! The transition function is pure: given the current state, an observed event,
//! the job shape (workspace/setup presence), and the last observed exit code,
//! it returns the next state and the command (if any) the orchestrator emits.
//! This keeps the lifecycle logic fully testable and independent of transport.

/// Lifecycle states. Spec §6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Running,
    Completed,
    Failed,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Pending => f.write_str("PENDING"),
            JobState::Running => f.write_str("RUNNING"),
            JobState::Completed => f.write_str("COMPLETED"),
            JobState::Failed => f.write_str("FAILED"),
        }
    }
}

/// Commands the orchestrator emits, routed to the node. Spec §6 command table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobCommand {
    MaterializeWorkspace,
    RunSetup,
    RunJob,
    CollectOutputs,
}

/// Events the orchestrator observes. Spec §6 event table.
#[derive(Debug, Clone)]
pub enum JobEvent {
    JobSubmitted,
    /// Reconciler reports the bootstrapped node reached ready.
    NodeReady,
    WorkspaceMaterialized,
    SetupCompleted,
    /// Supervised run process exited with `code`.
    JobExited(i32),
    OutputsCollected,
    NodeFault(String),
    NodeLost,
    OperatorStop,
}

/// Context the transition needs that is not in the event itself: whether the
/// job declares a workspace / setup, and the last observed run exit code
/// (decides COMPLETED vs FAILED after output collection).
#[derive(Debug, Clone, Copy)]
pub struct TransitionCtx {
    pub has_workspace: bool,
    pub has_setup: bool,
    pub prior_exit: Option<i32>,
}

impl TransitionCtx {
    pub fn new(job_has_workspace: bool, job_has_setup: bool) -> Self {
        Self {
            has_workspace: job_has_workspace,
            has_setup: job_has_setup,
            prior_exit: None,
        }
    }
}

/// Apply one event. Returns `(new_state, optional_command_to_emit)`.
///
/// Faithful to the spec §6 transition table:
/// - `JobSubmitted` → `PENDING`
/// - `NodeReady` → emit `MaterializeWorkspace` (or `RunSetup`/`RunJob` when
///   there is no workspace) → `RUNNING`
/// - `WorkspaceMaterialized` → emit `RunSetup`, or `RunJob` when no setup
/// - `SetupCompleted` → emit `RunJob`
/// - `JobExited{0}` → emit `CollectOutputs`
/// - `JobExited{non-zero}` → emit `CollectOutputs` (best-effort)
/// - `OutputsCollected` → `COMPLETED` if the run exited 0 (or never ran),
///   else `FAILED`
/// - `NodeFault` / `NodeLost` / `OperatorStop` → `FAILED`
pub fn transition(
    state: JobState,
    event: &JobEvent,
    ctx: TransitionCtx,
) -> (JobState, Option<JobCommand>) {
    use JobCommand::*;
    use JobEvent::*;
    use JobState::*;

    let failing = matches!(ctx.prior_exit, Some(c) if c != 0);

    match (state, event) {
        // Submission.
        (_, JobSubmitted) => (Pending, None),

        // Node reached ready: begin work, skipping stages the job does not need.
        (Pending, NodeReady) => {
            let cmd = if ctx.has_workspace {
                MaterializeWorkspace
            } else if ctx.has_setup {
                RunSetup
            } else {
                RunJob
            };
            (Running, Some(cmd))
        }

        // Workspace materialized: run setup, or jump straight to the job.
        (Running, WorkspaceMaterialized) => {
            let cmd = if ctx.has_setup { RunSetup } else { RunJob };
            (Running, Some(cmd))
        }

        // Setup done: run the job.
        (Running, SetupCompleted) => (Running, Some(RunJob)),

        // Job exited zero: collect outputs, then complete.
        (Running, JobExited(0)) => (Running, Some(CollectOutputs)),
        // Job exited non-zero: best-effort collection, then fail.
        (Running, JobExited(_)) => (Running, Some(CollectOutputs)),

        // Outputs collected: terminal state decided by the run's exit code.
        (Running, OutputsCollected) => {
            if failing {
                (Failed, None)
            } else {
                (Completed, None)
            }
        }

        // Faults abort immediately.
        (_, NodeFault(_) | NodeLost | OperatorStop) => (Failed, None),

        // Any other (state, event) pairing is not reachable in the v1 flow.
        (s, e) => (s, event_noop(e)),
    }
}

#[inline]
fn event_noop(_: &JobEvent) -> Option<JobCommand> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(ws: bool, setup: bool, exit: Option<i32>) -> TransitionCtx {
        TransitionCtx {
            has_workspace: ws,
            has_setup: setup,
            prior_exit: exit,
        }
    }

    #[test]
    fn happy_path_with_workspace_and_setup() {
        let c = ctx(true, true, None);
        let (s, cmd) = transition(JobState::Pending, &JobEvent::JobSubmitted, c);
        assert_eq!(s, JobState::Pending);
        assert_eq!(cmd, None);

        let (s, cmd) = transition(s, &JobEvent::NodeReady, c);
        assert_eq!(s, JobState::Running);
        assert_eq!(cmd, Some(JobCommand::MaterializeWorkspace));

        let (s, cmd) = transition(s, &JobEvent::WorkspaceMaterialized, c);
        assert_eq!(s, JobState::Running);
        assert_eq!(cmd, Some(JobCommand::RunSetup));

        let (s, cmd) = transition(s, &JobEvent::SetupCompleted, c);
        assert_eq!(s, JobState::Running);
        assert_eq!(cmd, Some(JobCommand::RunJob));

        let c2 = ctx(true, true, Some(0));
        let (s, cmd) = transition(s, &JobEvent::JobExited(0), c2);
        assert_eq!(s, JobState::Running);
        assert_eq!(cmd, Some(JobCommand::CollectOutputs));

        let (s, cmd) = transition(s, &JobEvent::OutputsCollected, c2);
        assert_eq!(s, JobState::Completed);
        assert_eq!(cmd, None);
    }

    #[test]
    fn no_workspace_jumps_setup_or_run() {
        let c = ctx(false, true, None);
        let (s, cmd) = transition(JobState::Pending, &JobEvent::NodeReady, c);
        assert_eq!(cmd, Some(JobCommand::RunSetup));

        let c = ctx(false, false, None);
        let (s, cmd) = transition(JobState::Pending, &JobEvent::NodeReady, c);
        assert_eq!(cmd, Some(JobCommand::RunJob));
        assert_eq!(s, JobState::Running);
    }

    #[test]
    fn nonzero_exit_collects_then_fails() {
        let c = ctx(true, false, None);
        let (s, _) = transition(JobState::Pending, &JobEvent::NodeReady, c);
        let (s, _) = transition(s, &JobEvent::WorkspaceMaterialized, c);
        let c2 = ctx(true, false, Some(3));
        let (s, cmd) = transition(s, &JobEvent::JobExited(3), c2);
        assert_eq!(cmd, Some(JobCommand::CollectOutputs));
        let (s, cmd) = transition(s, &JobEvent::OutputsCollected, c2);
        assert_eq!(s, JobState::Failed);
        assert_eq!(cmd, None);
    }

    #[test]
    fn node_fault_aborts() {
        let c = ctx(true, true, None);
        let (s, cmd) = transition(JobState::Running, &JobEvent::NodeFault("oom".into()), c);
        assert_eq!(s, JobState::Failed);
        assert_eq!(cmd, None);
    }
}
