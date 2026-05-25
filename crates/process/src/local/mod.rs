use std::io::{Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};

use crate::action::ProcessAction;
use crate::types::ProcessDriver;
use crate::event::ProcessEvent;
use crate::types::EventQueue;
use crate::types::{ExitStatus, ProcessSpec, Signal};
use crate::types::ProcessWaker;

// ─── Signal ────────────────────────────────────────────────────────────────

/// Map a `Signal` enum variant to the corresponding libc signal constant.
fn signal_to_libc(signal: Signal) -> libc::c_int {
    match signal {
        Signal::Terminate => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
        Signal::Hangup => libc::SIGHUP,
        Signal::Interrupt => libc::SIGINT,
        Signal::Other(n) => n,
    }
}

/// Send a signal to a process by PID. Returns `Ok(())` on success.
fn send_signal(pid: u32, signal: Signal) -> Result<(), String> {
    let sig = signal_to_libc(signal);
    // Safety: kill() is safe to call with any pid/signal combo;
    // it returns -1 on error which we check.
    let ret = unsafe { libc::kill(pid as libc::pid_t, sig) };
    if ret == 0 {
        Ok(())
    } else {
        Err(format!(
            "kill({}, {}) failed: {}",
            pid,
            sig,
            std::io::Error::last_os_error()
        ))
    }
}

// ─── Pipes ─────────────────────────────────────────────────────────────────

/// Read from a pipe in a loop, pushing events to the queue and waking the actor.
///
/// Runs in a background thread. Exits when the pipe reaches EOF or errors.
fn read_pipe(
    mut pipe: impl Read + Send + 'static,
    is_stderr: bool,
    queue: EventQueue,
    waker: Arc<OnceLock<ProcessWaker>>,
) {
    let mut buf = [0u8; 8192];
    loop {
        match pipe.read(&mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => {
                queue.push(ProcessEvent::OutputReceived {
                    data: buf[..n].to_vec(),
                    is_stderr,
                });
                if let Some(w) = waker.get() {
                    w.wake();
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}

// ─── Wait ──────────────────────────────────────────────────────────────────

/// Wait for a child process to exit, then push the appropriate event.
///
/// Runs in a background thread. Uses `libc::waitpid` for accurate exit status.
/// After waitpid returns, joins the pipe reader threads so all buffered
/// stdout/stderr is drained before the `Exited` event is enqueued.
fn wait_for_exit(
    pid: u32,
    reader_threads: Vec<JoinHandle<()>>,
    queue: EventQueue,
    waker: Arc<OnceLock<ProcessWaker>>,
) {
    let mut status: libc::c_int = 0;
    let ret = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, 0) };

    let exit_status = if ret < 0 {
        ExitStatus::Unknown
    } else {
        decode_wait_status(status)
    };

    // Wait for pipe readers to finish draining all output before signaling exit.
    // Once the process exits, its pipe ends close, so readers will hit EOF shortly.
    for handle in reader_threads {
        let _ = handle.join();
    }

    queue.push(ProcessEvent::Exited {
        status: exit_status,
    });
    if let Some(w) = waker.get() {
        w.wake();
    }
}

fn decode_wait_status(status: libc::c_int) -> ExitStatus {
    if libc::WIFEXITED(status) {
        ExitStatus::Code(libc::WEXITSTATUS(status))
    } else if libc::WIFSIGNALED(status) {
        ExitStatus::Signal(libc::WTERMSIG(status))
    } else {
        ExitStatus::Unknown
    }
}

// ─── LocalDriver ───────────────────────────────────────────────────────────

/// A `ProcessDriver` that spawns real OS subprocesses via `std::process::Command`.
///
/// Background threads read stdout/stderr and wait for process exit,
/// pushing events into a shared `EventQueue`. The actor polls via `poll()`.
pub struct LocalDriver {
    queue: EventQueue,
    waker_slot: Arc<OnceLock<ProcessWaker>>,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    _wait_thread: Option<JoinHandle<()>>,
}

impl LocalDriver {
    pub fn new(queue: EventQueue, waker_slot: Arc<OnceLock<ProcessWaker>>) -> Self {
        Self {
            queue,
            waker_slot,
            child: None,
            stdin: None,
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
                let mut reader_threads = Vec::new();
                if let Some(stdout) = child.stdout.take() {
                    let queue = self.queue.clone();
                    let waker = self.waker_slot.clone();
                    reader_threads.push(
                        thread::Builder::new()
                            .name(format!("proc-{}-stdout", pid))
                            .spawn(move || read_pipe(stdout, false, queue, waker))
                            .expect("failed to spawn stdout reader"),
                    );
                }

                // Spawn stderr reader thread
                if let Some(stderr) = child.stderr.take() {
                    let queue = self.queue.clone();
                    let waker = self.waker_slot.clone();
                    reader_threads.push(
                        thread::Builder::new()
                            .name(format!("proc-{}-stderr", pid))
                            .spawn(move || read_pipe(stderr, true, queue, waker))
                            .expect("failed to spawn stderr reader"),
                    );
                }

                // Spawn wait thread — it joins the reader threads before pushing Exited,
                // ensuring all output is drained before the exit event.
                let queue = self.queue.clone();
                let waker = self.waker_slot.clone();
                self._wait_thread = Some(
                    thread::Builder::new()
                        .name(format!("proc-{}-wait", pid))
                        .spawn(move || wait_for_exit(pid, reader_threads, queue, waker))
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
                    match send_signal(pid, signal) {
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

    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
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
