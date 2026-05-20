//! Orchestrator helpers for `pp-smoke-run`.
//!
//! Extracted from the binary so the spawn-chain and convergence-wait logic
//! can be unit-tested without provisioning child processes or driving a
//! real iroh cluster:
//!
//! * [`spawn_chain`] spawns `N` stage children sequentially, reading each
//!   one's `PP_GPU_NODE_ADDR` stdout announcement and passing the
//!   predecessor's announcement into the next child's environment.
//! * [`await_convergence`] polls a closure that reports the current alive
//!   peer count and returns when the target is met (or times out).
//!
//! Tests inject a fake command builder (e.g. `sh -c "echo PP_GPU_NODE_ADDR
//! <hex> <direct>; sleep 60"`) so the chain can be exercised end-to-end
//! without `pp-gpu-node` on disk.

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Address announcement parsed from a stage child's stdout.
#[derive(Debug, Clone)]
pub struct StageAddr {
    /// 64-character hex node id.
    pub hex: String,
    /// Comma-separated direct addresses (`ip:port,ip:port,...`).
    pub direct: String,
}

/// A successfully-spawned stage process plus its announced address.
#[derive(Debug)]
pub struct SpawnedStage {
    pub stage: u32,
    pub child: Child,
    pub addr: StageAddr,
}

/// What the caller's `build_cmd` closure sees for each stage.
#[derive(Debug, Clone)]
pub struct StageSpawnCtx {
    pub stage: u32,
    pub num_stages: u32,
    /// The predecessor stage's announced address, or `None` for stage 0.
    /// Production builders set `PEER_NODE_ID` / `PEER_DIRECT` from this.
    pub peer: Option<StageAddr>,
    /// Stage 0's announced address, or `None` for stage 0 itself.
    /// Every later stage learns this so the autoregressive feedback edge
    /// (last → first) has both endpoints in each other's iroh NodeMap.
    /// Production builders surface this as `FIRST_PEER_NODE_ID` /
    /// `FIRST_PEER_DIRECT`; for `stage == 1` it coincides with `peer` and
    /// is the same node, but propagating it as a separate field keeps the
    /// chain-spawn helper free of a "did I already include this peer?"
    /// special case.
    pub first_peer: Option<StageAddr>,
}

/// RAII guard owning every successfully-spawned stage child. Killing on
/// drop is what lets [`spawn_chain`] roll back partial chains on failure
/// without leaking processes — and what lets the orchestrator itself
/// guarantee no orphans on any exit path.
#[derive(Debug)]
pub struct ChainGuard {
    stages: Vec<SpawnedStage>,
}

impl ChainGuard {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    pub fn push(&mut self, stage: SpawnedStage) {
        self.stages.push(stage);
    }

    pub fn stages(&self) -> &[SpawnedStage] {
        &self.stages
    }

    pub fn stages_mut(&mut self) -> &mut [SpawnedStage] {
        &mut self.stages
    }

    pub fn len(&self) -> usize {
        self.stages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Transfer ownership of the spawned stages out of the guard, leaving
    /// it empty (drop will be a no-op). Useful when the caller wants to
    /// own the lifetime of each child itself.
    pub fn into_inner(mut self) -> Vec<SpawnedStage> {
        std::mem::take(&mut self.stages)
    }
}

impl Default for ChainGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ChainGuard {
    fn drop(&mut self) {
        // SIGTERM every child first, then escalate to SIGKILL for any that
        // do not exit within `GRACEFUL_EXIT`. The graceful step is needed
        // when a wrapper script (e.g. `docker-gpu-node.sh`) is between us
        // and the real worker process: `docker run --rm` proxies SIGTERM
        // to the container's PID 1 and only then does the container exit
        // and `--rm` clean up. A bare SIGKILL bypasses that proxy and
        // orphans the container. For the no-wrapper host case the cost is
        // ~tens of ms — pp-gpu-node has no SIGTERM handler so it exits
        // immediately on receipt.
        //
        // The budget is *per stage*, not shared across the whole chain.
        // A shared 3s deadline starved later stages under back-to-back
        // load (e.g. cargo test running this test in a tight loop), so
        // the last shim sometimes got SIGKILL'd before its docker CLI
        // could proxy SIGTERM into the container — orphaning the
        // container even with `--init` in place.
        const GRACEFUL_EXIT: Duration = Duration::from_secs(5);
        for stage in self.stages.iter_mut() {
            #[cfg(unix)]
            unsafe {
                libc::kill(stage.child.id() as i32, libc::SIGTERM);
            }
        }
        for stage in self.stages.iter_mut() {
            let pid = stage.child.id();
            let stage_deadline = Instant::now() + GRACEFUL_EXIT;
            while Instant::now() < stage_deadline {
                match stage.child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                    Err(_) => break,
                }
            }
            // Final escalation for any laggard. `kill` is a no-op if the
            // child already exited (we ignore the error either way).
            let _ = stage.child.kill();
            let _ = stage.child.wait();
            eprintln!("pp-smoke-run: stopped stage {} child pid {pid}", stage.stage);
        }
    }
}

/// Why a [`spawn_chain`] attempt failed. The variant captures the stage
/// index that broke the chain — callers report it; the rollback is
/// handled by the dropped [`ChainGuard`] before this is constructed.
#[derive(Debug)]
pub enum SpawnChainError {
    /// `Command::spawn` failed for the given stage.
    Spawn { stage: u32, source: std::io::Error },
    /// The child started but never wrote a `PP_GPU_NODE_ADDR` line within
    /// the configured timeout.
    AddressTimeout { stage: u32, timeout: Duration },
    /// `num_stages` was zero or one — neither is a valid pipeline length.
    InvalidNumStages(u32),
}

impl std::fmt::Display for SpawnChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnChainError::Spawn { stage, source } => {
                write!(f, "spawn stage {stage} failed: {source}")
            }
            SpawnChainError::AddressTimeout { stage, timeout } => {
                write!(
                    f,
                    "stage {stage}: PP_GPU_NODE_ADDR not seen within {:.0}s",
                    timeout.as_secs_f32()
                )
            }
            SpawnChainError::InvalidNumStages(n) => {
                write!(f, "spawn_chain requires num_stages >= 2, got {n}")
            }
        }
    }
}

impl std::error::Error for SpawnChainError {}

/// Spawn `num_stages` children sequentially in pipeline order.
///
/// Each iteration:
///
/// 1. Calls `build_cmd(ctx)` to get a `Command` for stage `i`. `ctx.peer`
///    is `None` for stage 0, otherwise the predecessor's announcement.
/// 2. Forces `Stdio::piped()` on stdout (overriding whatever the closure
///    set — the announcement parser needs the pipe).
/// 3. Spawns, then reads stdout until a line starting with
///    `PP_GPU_NODE_ADDR <hex> <direct>` arrives. All preceding output is
///    forwarded to the parent's stdout verbatim. A background thread keeps
///    draining the pipe after the announcement so the child does not block
///    on its own stdout buffer.
/// 4. Records the announcement and moves on to stage `i + 1`.
///
/// On any failure — spawn error, address timeout, or invalid `num_stages`
/// — every already-spawned child is killed (via `ChainGuard::drop`) before
/// the error returns.
pub fn spawn_chain<F>(
    num_stages: u32,
    addr_timeout: Duration,
    mut build_cmd: F,
) -> Result<ChainGuard, SpawnChainError>
where
    F: FnMut(StageSpawnCtx) -> Command,
{
    if num_stages < 2 {
        return Err(SpawnChainError::InvalidNumStages(num_stages));
    }

    let mut guard = ChainGuard::new();
    let mut last_addr: Option<StageAddr> = None;
    let mut first_addr: Option<StageAddr> = None;

    for stage in 0..num_stages {
        let ctx = StageSpawnCtx {
            stage,
            num_stages,
            peer: last_addr.clone(),
            first_peer: first_addr.clone(),
        };
        let mut cmd = build_cmd(ctx);
        cmd.stdout(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| SpawnChainError::Spawn { stage, source: e })?;
        let stdout = child
            .stdout
            .take()
            .expect("stdout forced to piped before spawn");

        let addr = match read_stage_address(stdout, stage, addr_timeout) {
            Ok(a) => a,
            Err(()) => {
                guard.push(SpawnedStage {
                    stage,
                    child,
                    addr: StageAddr {
                        hex: String::new(),
                        direct: String::new(),
                    },
                });
                return Err(SpawnChainError::AddressTimeout {
                    stage,
                    timeout: addr_timeout,
                });
            }
        };

        if stage == 0 {
            first_addr = Some(addr.clone());
        }
        last_addr = Some(addr.clone());
        guard.push(SpawnedStage { stage, child, addr });
    }

    Ok(guard)
}

/// Drain `stdout` looking for a `PP_GPU_NODE_ADDR <hex> <direct>` line.
/// All output is forwarded verbatim to the parent process's stdout so the
/// user sees the child's logs. After the announcement is found the
/// background thread keeps draining so the child never blocks on its pipe.
fn read_stage_address(
    stdout: ChildStdout,
    stage: u32,
    timeout: Duration,
) -> Result<StageAddr, ()> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut announced = false;
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    print!("{line}");
                    if !announced {
                        if let Some(rest) = line.trim().strip_prefix("PP_GPU_NODE_ADDR ") {
                            if let Some((hex, direct)) = rest.split_once(' ') {
                                let _ = tx.send(StageAddr {
                                    hex: hex.to_string(),
                                    direct: direct.to_string(),
                                });
                                announced = true;
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx.recv_timeout(timeout).map_err(|_| {
        eprintln!(
            "pp-smoke-run: stage {stage} did not announce PP_GPU_NODE_ADDR within {:.0}s",
            timeout.as_secs_f32()
        );
    })
}

/// Outcome of [`await_convergence`].
#[derive(Debug, PartialEq, Eq)]
pub enum ConvergeError {
    /// The required alive count was never reached before `timeout`.
    Timeout {
        expected: usize,
        last_seen: usize,
        timeout: Duration,
    },
}

impl std::fmt::Display for ConvergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvergeError::Timeout {
                expected,
                last_seen,
                timeout,
            } => write!(
                f,
                "cluster did not converge in {:.0}s (expected {} alive peers, last saw {})",
                timeout.as_secs_f32(),
                expected,
                last_seen,
            ),
        }
    }
}

impl std::error::Error for ConvergeError {}

/// Poll `alive_count()` until it reports at least `expected` alive peers,
/// or `timeout` elapses. The orchestrator passes a closure that ticks the
/// iroh driver and reads its membership snapshot; tests pass a stub.
///
/// `poll_interval` controls how long the loop sleeps between polls. The
/// first poll happens immediately, before any sleep.
pub fn await_convergence<F>(
    expected: usize,
    timeout: Duration,
    poll_interval: Duration,
    mut alive_count: F,
) -> Result<(), ConvergeError>
where
    F: FnMut() -> usize,
{
    let deadline = Instant::now() + timeout;
    let mut last_seen = 0usize;
    loop {
        let n = alive_count();
        if n > last_seen {
            last_seen = n;
        }
        if n >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ConvergeError::Timeout {
                expected,
                last_seen,
                timeout,
            });
        }
        std::thread::sleep(poll_interval);
    }
}
