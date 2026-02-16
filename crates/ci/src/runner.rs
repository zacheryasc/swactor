//! RunnerSupervisor actor: manages SSH session and job execution on a spot instance.
//!
//! Spawned per-job by the Coordinator. Owns the connection to the spot instance.

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::coordinator::CoordinatorMsg;
use crate::{JobComplete, JobFailure, JobProgress, JobSuccess, StartJob};

/// Messages the RunnerSupervisor can receive.
#[derive(Debug, Clone)]
pub enum RunnerMsg {
    /// Begin executing the job (sent immediately after spawn via on_start).
    Execute,
    /// Simulated: job command output line.
    OutputLine(String),
    /// Simulated: job completed successfully.
    SimComplete(Result<(), String>),
}

/// RunnerSupervisor actor state.
///
/// In real deployment, this would manage an SSH connection.
/// In simulation, job execution is driven by external messages.
pub struct RunnerSupervisor {
    coordinator_addr: ActorAddress,
    start_job: StartJob,
}

impl RunnerSupervisor {
    pub fn new(coordinator_addr: ActorAddress, start_job: StartJob) -> Self {
        Self {
            coordinator_addr,
            start_job,
        }
    }
}

impl ActorInterface for RunnerSupervisor {
    type Incoming = RunnerMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        // In simulation, the sim harness will send SimComplete messages.
        // In real deployment, this would initiate SSH connection + command execution.
        let _ = ctx.send(ctx.self_addr(), RunnerMsg::Execute);
    }

    fn handle(&mut self, ctx: &Ctx, msg: RunnerMsg) {
        match msg {
            RunnerMsg::Execute => {
                // In real mode, we'd SSH into the instance and run commands.
                // In simulation, this is a no-op; SimComplete drives completion.
            }
            RunnerMsg::OutputLine(line) => {
                let _ = ctx.send(
                    self.coordinator_addr,
                    CoordinatorMsg::JobProgress(JobProgress {
                        job_id: self.start_job.job_id.clone(),
                        output_line: line,
                    }),
                );
            }
            RunnerMsg::SimComplete(result) => {
                let complete = JobComplete {
                    job_id: self.start_job.job_id.clone(),
                    result: match result {
                        Ok(()) => Ok(JobSuccess),
                        Err(msg) => Err(JobFailure::CommandFailed {
                            exit_code: 1,
                            last_lines: vec![msg],
                        }),
                    },
                    artifacts: Vec::new(),
                };
                let _ = ctx.send(
                    self.coordinator_addr,
                    CoordinatorMsg::JobComplete(complete),
                );
                ctx.stop_self();
            }
        }
    }
}
