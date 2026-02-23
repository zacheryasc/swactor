mod pipes;
mod signal;
mod wait;

use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};

use crate::action::ProcessAction;
use crate::driver::ProcessDriver;
use crate::event::ProcessEvent;
use crate::queue::EventQueue;
use crate::types::ProcessSpec;
use crate::waker::ProcessWaker;

/// A `ProcessDriver` that spawns real OS subprocesses via `std::process::Command`.
///
/// Background threads read stdout/stderr and wait for process exit,
/// pushing events into a shared `EventQueue`. The actor polls via `poll()`.
pub struct LocalDriver {
    queue: EventQueue,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    _reader_threads: Vec<JoinHandle<()>>,
    _wait_thread: Option<JoinHandle<()>>,
}

impl LocalDriver {
    pub fn new(queue: EventQueue, waker_slot: Arc<OnceLock<ProcessWaker>>) -> Self {
        Self {
            queue,
            waker_slot,
            child: None,
            stdin: None,
            _reader_threads: Vec::new(),
            _wait_thread: None,
        }
    }

    fn spawn_process(&mut self, spec: &ProcessSpec) {
        let mut cmd = Command::new(&spec.command);
        cmd.args(&spec.args);
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        if let Some(ref dir) = spec.working_dir {
            cmd.current_dir(dir);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        match cmd.spawn() {
            Ok(mut child) => {
                let pid = child.id();

                // Take the stdin handle
                self.stdin = child.stdin.take();

                // Spawn stdout reader thread
                if let Some(stdout) = child.stdout.take() {
                    let queue = self.queue.clone();
                    let waker = self.waker_slot.clone();
                    self._reader_threads.push(
                        thread::Builder::new()
                            .name(format!("proc-{}-stdout", pid))
                            .spawn(move || pipes::read_pipe(stdout, false, queue, waker))
                            .expect("failed to spawn stdout reader"),
                    );
                }

                // Spawn stderr reader thread
                if let Some(stderr) = child.stderr.take() {
                    let queue = self.queue.clone();
                    let waker = self.waker_slot.clone();
                    self._reader_threads.push(
                        thread::Builder::new()
                            .name(format!("proc-{}-stderr", pid))
                            .spawn(move || pipes::read_pipe(stderr, true, queue, waker))
                            .expect("failed to spawn stderr reader"),
                    );
                }

                // Spawn wait thread
                let queue = self.queue.clone();
                let waker = self.waker_slot.clone();
                self._wait_thread = Some(
                    thread::Builder::new()
                        .name(format!("proc-{}-wait", pid))
                        .spawn(move || wait::wait_for_exit(pid, queue, waker))
                        .expect("failed to spawn wait thread"),
                );

                self.child = Some(child);
                self.queue.push(ProcessEvent::Started);
            }
            Err(e) => {
                self.queue.push(ProcessEvent::SpawnFailed {
                    reason: e.to_string(),
                });
            }
        }
    }
}

impl ProcessDriver for LocalDriver {
    fn execute(&mut self, action: ProcessAction) {
        match action {
            ProcessAction::SpawnProcess { spec } => {
                self.spawn_process(&spec);
            }
            ProcessAction::WriteStdin { data } => {
                if let Some(ref mut stdin) = self.stdin {
                    match stdin.write_all(&data) {
                        Ok(()) => {
                            self.queue.push(ProcessEvent::StdinWritten {
                                byte_count: data.len(),
                            });
                        }
                        Err(e) => {
                            self.queue.push(ProcessEvent::ConnectionLost {
                                reason: format!("stdin write failed: {}", e),
                            });
                        }
                    }
                }
            }
            ProcessAction::SendSignal { signal } => {
                if let Some(ref child) = self.child {
                    let pid = child.id();
                    match signal::send_signal(pid, signal) {
                        Ok(()) => {
                            self.queue.push(ProcessEvent::SignalSent);
                        }
                        Err(reason) => {
                            self.queue.push(ProcessEvent::ConnectionLost { reason });
                        }
                    }
                }
            }
            ProcessAction::ResizePty { .. } => {
                // No-op for Phase 1 (pipes only, no PTY support)
                self.queue.push(ProcessEvent::PtyResized);
            }
            ProcessAction::CloseStdin => {
                // Drop the stdin handle to close the pipe
                self.stdin.take();
            }
            ProcessAction::ScheduleKillTimeout { duration } => {
                let queue = self.queue.clone();
                let waker = self.waker_slot.clone();
                thread::spawn(move || {
                    thread::sleep(duration);
                    queue.push(ProcessEvent::KillTimeout);
                    if let Some(w) = waker.get() {
                        w.wake();
                    }
                });
            }
            // Notification actions are not driver commands
            _ => {}
        }
    }

    fn poll(&mut self) -> Vec<ProcessEvent> {
        self.queue.drain()
    }
}

impl Drop for LocalDriver {
    fn drop(&mut self) {
        // Close stdin to let the process know we're done
        self.stdin.take();
        // Kill the process if still alive
        if let Some(ref mut child) = self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
