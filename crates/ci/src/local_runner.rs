//! LocalRunner actor: executes job commands directly on the host via shell.
//!
//! Short-lived actor, one per job. Spawned by LocalCoordinator when a job
//! is ready to execute.

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::local_coordinator::LocalCoordinatorMsg;
use crate::{JobComplete, JobFailure, JobProgress, JobSuccess, LocalStartJob};

/// Messages the LocalRunner can receive.
#[derive(Debug, Clone)]
pub enum LocalRunnerMsg {
    /// Begin executing the job (sent to self in on_start).
    Execute,
    /// Simulated: job completed (for testing without real shell).
    SimComplete(Result<(), String>),
}

/// LocalRunner actor state.
pub struct LocalRunner {
    coordinator_addr: ActorAddress,
    start_job: LocalStartJob,
}

impl LocalRunner {
    pub fn new(coordinator_addr: ActorAddress, start_job: LocalStartJob) -> Self {
        Self {
            coordinator_addr,
            start_job,
        }
    }

    /// Execute all commands in the job definition, streaming output back.
    fn execute(&self, ctx: &Ctx) {
        let job_id = &self.start_job.job_id;
        let work_dir = &self.start_job.work_dir;
        let timeout_secs = self.start_job.job_def.timeout_secs;

        eprintln!(
            "[runner] job {}/{} starting ({} commands, timeout {}s, workdir {})",
            job_id.pipeline_id.0,
            job_id.job_name,
            self.start_job.job_def.run.len(),
            timeout_secs,
            work_dir,
        );

        for cmd_str in &self.start_job.job_def.run {
            eprintln!("[runner] exec: {cmd_str}");

            // Send progress: command being run.
            let _ = ctx.send(
                self.coordinator_addr,
                LocalCoordinatorMsg::JobProgress(JobProgress {
                    job_id: job_id.clone(),
                    output_line: format!("$ {cmd_str}"),
                }),
            );

            let child_result = Command::new("sh")
                .arg("-c")
                .arg(cmd_str)
                .current_dir(work_dir)
                .envs(&self.start_job.env_overrides)
                .envs(&self.start_job.job_def.env)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();

            let mut child = match child_result {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[runner] spawn failed: {e}");
                    let _ = ctx.send(
                        self.coordinator_addr,
                        LocalCoordinatorMsg::JobComplete(JobComplete {
                            job_id: job_id.clone(),
                            result: Err(JobFailure::ExecError(e.to_string())),
                            artifacts: Vec::new(),
                        }),
                    );
                    ctx.stop_self();
                    return;
                }
            };

            // Timeout: spawn a thread that kills the child after timeout_secs.
            let child_id = child.id();
            let kill_flag = Arc::new(Mutex::new(false));
            let kill_flag_clone = Arc::clone(&kill_flag);
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            let timeout_handle = std::thread::spawn(move || {
                if done_rx
                    .recv_timeout(std::time::Duration::from_secs(timeout_secs))
                    .is_err()
                {
                    *kill_flag_clone.lock().unwrap() = true;
                    // Actually kill the child process so the pipe readers unblock.
                    let _ = std::process::Command::new("kill")
                        .args(["-9", &child_id.to_string()])
                        .status();
                }
            });

            // Read stdout and stderr concurrently to avoid pipe-buffer deadlock.
            let (last_lines, timed_out) =
                drain_child_output(&mut child, job_id, ctx, self.coordinator_addr, &kill_flag);

            let status = child.wait();

            // Signal timeout thread that we're done.
            let _ = done_tx.send(());
            let _ = timeout_handle.join();

            if timed_out || *kill_flag.lock().unwrap() {
                eprintln!("[runner] command timed out after {timeout_secs}s");
                let _ = ctx.send(
                    self.coordinator_addr,
                    LocalCoordinatorMsg::JobComplete(JobComplete {
                        job_id: job_id.clone(),
                        result: Err(JobFailure::Timeout),
                        artifacts: Vec::new(),
                    }),
                );
                ctx.stop_self();
                return;
            }

            match status {
                Ok(exit) if exit.success() => {
                    eprintln!("[runner] command succeeded");
                }
                Ok(exit) => {
                    let exit_code = exit.code().unwrap_or(-1);
                    eprintln!("[runner] command failed (exit {exit_code})");
                    for line in &last_lines {
                        eprintln!("[runner]   {line}");
                    }
                    let _ = ctx.send(
                        self.coordinator_addr,
                        LocalCoordinatorMsg::JobComplete(JobComplete {
                            job_id: job_id.clone(),
                            result: Err(JobFailure::CommandFailed {
                                exit_code,
                                last_lines,
                            }),
                            artifacts: Vec::new(),
                        }),
                    );
                    ctx.stop_self();
                    return;
                }
                Err(e) => {
                    eprintln!("[runner] wait failed: {e}");
                    let _ = ctx.send(
                        self.coordinator_addr,
                        LocalCoordinatorMsg::JobComplete(JobComplete {
                            job_id: job_id.clone(),
                            result: Err(JobFailure::ExecError(e.to_string())),
                            artifacts: Vec::new(),
                        }),
                    );
                    ctx.stop_self();
                    return;
                }
            }
        }

        eprintln!(
            "[runner] job {}/{} passed",
            job_id.pipeline_id.0, job_id.job_name
        );
        let _ = ctx.send(
            self.coordinator_addr,
            LocalCoordinatorMsg::JobComplete(JobComplete {
                job_id: job_id.clone(),
                result: Ok(JobSuccess),
                artifacts: Vec::new(),
            }),
        );
        ctx.stop_self();
    }
}

/// Drain stdout and stderr from a child process concurrently.
///
/// Spawns a background thread for stderr so that both pipes are consumed
/// in parallel, preventing the classic pipe-buffer deadlock where the child
/// blocks writing to a full stderr while the parent blocks reading stdout.
///
/// Returns (last_lines, timed_out).
fn drain_child_output(
    child: &mut Child,
    job_id: &crate::JobId,
    ctx: &Ctx,
    coordinator_addr: ActorAddress,
    kill_flag: &Arc<Mutex<bool>>,
) -> (Vec<String>, bool) {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Collect stderr on a background thread.
    let stderr_job_id = job_id.clone();
    let stderr_kill = Arc::clone(kill_flag);
    let stderr_handle = std::thread::spawn(move || {
        let mut lines = Vec::new();
        if let Some(stderr) = stderr {
            let reader = std::io::BufReader::new(stderr);
            for line in reader.lines() {
                if *stderr_kill.lock().unwrap() {
                    break;
                }
                if let Ok(line) = line {
                    lines.push(line);
                }
            }
        }
        lines
    });

    // Read stdout on the current thread, streaming progress.
    let mut last_lines: Vec<String> = Vec::new();
    if let Some(stdout) = stdout {
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines() {
            if let Ok(line) = line {
                let _ = ctx.send(
                    coordinator_addr,
                    LocalCoordinatorMsg::JobProgress(JobProgress {
                        job_id: job_id.clone(),
                        output_line: line.clone(),
                    }),
                );
                last_lines.push(line);
                if last_lines.len() > 50 {
                    last_lines.remove(0);
                }
            }
        }
    }

    // Join stderr thread and stream its lines as progress.
    let timed_out = *kill_flag.lock().unwrap();
    let stderr_lines = stderr_handle.join().unwrap_or_default();
    for line in &stderr_lines {
        let _ = ctx.send(
            coordinator_addr,
            LocalCoordinatorMsg::JobProgress(JobProgress {
                job_id: stderr_job_id.clone(),
                output_line: format!("[stderr] {line}"),
            }),
        );
    }

    // Merge stderr into last_lines tail.
    for line in stderr_lines {
        last_lines.push(line);
        if last_lines.len() > 50 {
            last_lines.remove(0);
        }
    }

    (last_lines, timed_out)
}

impl ActorInterface for LocalRunner {
    type Incoming = LocalRunnerMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), LocalRunnerMsg::Execute);
    }

    fn handle(&mut self, ctx: &Ctx, msg: LocalRunnerMsg) {
        match msg {
            LocalRunnerMsg::Execute => {
                self.execute(ctx);
            }
            LocalRunnerMsg::SimComplete(result) => {
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
                    LocalCoordinatorMsg::JobComplete(complete),
                );
                ctx.stop_self();
            }
        }
    }
}
