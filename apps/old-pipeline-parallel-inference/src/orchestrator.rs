//! Orchestrator helpers for `pp-orchestrator`.
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
//! * [`resolve_roster`] polls SWIM until every `pp-stage-K` resolves, then
//!   returns the per-stage (node_id_hex, node_id_short) roster used by the
//!   `pp_stage_roster` diagnostic event (spec §4.5).
//! * [`stage_roster_event_fields`] builds the JSON fields for a
//!   `pp_stage_roster` event from a resolved roster.
//!
//! Tests inject a fake command builder (e.g. `sh -c "echo PP_GPU_NODE_ADDR
//! <hex> <direct>; sleep 60"`) so the chain can be exercised end-to-end
//! without `pp-worker` on disk.

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
        // ~tens of ms — pp-worker has no SIGTERM handler so it exits
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
            eprintln!(
                "pp-orchestrator: stopped stage {} child pid {pid}",
                stage.stage
            );
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
fn read_stage_address(stdout: ChildStdout, stage: u32, timeout: Duration) -> Result<StageAddr, ()> {
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
            "pp-orchestrator: stage {stage} did not announce PP_GPU_NODE_ADDR within {:.0}s",
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

// ─── Roster + pipeline-wired helpers (spec §4.5 / §4.6) ───────────────

/// One entry in the resolved stage roster: stage_index → node id.
///
/// Built by [`resolve_roster`] once every per-stage SWIM name resolves.
/// The orchestrator emits these as the `stages` field of the
/// `pp_stage_roster` event (spec §4.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageRosterEntry {
    pub stage_index: u32,
    pub node_id_hex: String,
    pub node_id_short: String,
}

/// Why [`resolve_roster`] gave up.
#[derive(Debug, PartialEq, Eq)]
pub enum RosterError {
    /// At least one `pp-stage-K` did not resolve within the timeout. The
    /// missing stage indices are reported in ascending order.
    Timeout {
        missing_stages: Vec<u32>,
        timeout: Duration,
    },
}

impl std::fmt::Display for RosterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RosterError::Timeout {
                missing_stages,
                timeout,
            } => write!(
                f,
                "pipeline did not wire within {:.0}s: missing pp-stage-K for stages {:?}",
                timeout.as_secs_f32(),
                missing_stages,
            ),
        }
    }
}

impl std::error::Error for RosterError {}

/// Poll until every `pp-stage-K` (K in `0..num_stages`) resolves, or
/// `timeout` elapses. Returns the resolved roster ordered by
/// `stage_index`. Each iteration calls `resolve_stage(K)` for any stage
/// still missing — the callback returns `Some((actor_addr_unused,
/// node_id_hex))` once SWIM has propagated the registration.
///
/// The callback's first tuple element is discarded by this helper; it
/// exists because the orchestrator's per-name resolve returns
/// `(ActorAddress, NodeId)` and most callers want the address too, so
/// expressing the callback as "the resolve function" keeps adapter code
/// short.
pub fn resolve_roster<F>(
    num_stages: u32,
    timeout: Duration,
    poll_interval: Duration,
    mut resolve_stage: F,
) -> Result<Vec<StageRosterEntry>, RosterError>
where
    F: FnMut(u32) -> Option<String>,
{
    let deadline = Instant::now() + timeout;
    let mut resolved: Vec<Option<String>> = vec![None; num_stages as usize];
    loop {
        for k in 0..num_stages {
            if resolved[k as usize].is_some() {
                continue;
            }
            if let Some(hex) = resolve_stage(k) {
                resolved[k as usize] = Some(hex);
            }
        }
        if resolved.iter().all(|o| o.is_some()) {
            let out: Vec<StageRosterEntry> = resolved
                .into_iter()
                .enumerate()
                .map(|(k, hex)| {
                    let hex = hex.unwrap();
                    let short = hex.chars().take(8).collect::<String>();
                    StageRosterEntry {
                        stage_index: k as u32,
                        node_id_hex: hex,
                        node_id_short: short,
                    }
                })
                .collect();
            return Ok(out);
        }
        if Instant::now() >= deadline {
            let missing: Vec<u32> = resolved
                .iter()
                .enumerate()
                .filter_map(|(k, o)| o.is_none().then_some(k as u32))
                .collect();
            return Err(RosterError::Timeout {
                missing_stages: missing,
                timeout,
            });
        }
        std::thread::sleep(poll_interval);
    }
}

/// Build the `fields` JSON for a `pp_stage_roster` diagnostic event from
/// a resolved roster, attaching the given `drive_seq`. Spec §4.5: the
/// event MUST list every stage, ordered by `stage_index`, with each
/// entry carrying `stage_index`, `node_id_hex`, and `node_id_short`.
pub fn stage_roster_event_fields(drive_seq: u32, roster: &[StageRosterEntry]) -> serde_json::Value {
    let stages: Vec<serde_json::Value> = roster
        .iter()
        .map(|e| {
            serde_json::json!({
                "stage_index": e.stage_index,
                "node_id_hex": e.node_id_hex,
                "node_id_short": e.node_id_short,
            })
        })
        .collect();
    serde_json::json!({
        "drive_seq": drive_seq,
        "stages": stages,
    })
}
