#![no_main]

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Inbox, Runtime, RuntimeHandle};

// ─── Run Logging ────────────────────────────────────────────────────────────

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

fn log_interval() -> u64 {
    static INTERVAL: OnceLock<u64> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        std::env::var("FUZZ_LOG")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    })
}

// ─── Message Types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct FuzzMsg {
    value: u64,
}

impl<'a> Arbitrary<'a> for FuzzMsg {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FuzzMsg {
            value: u.arbitrary()?,
        })
    }
}

#[derive(Clone, Debug)]
struct WrongTypeMsg;

// ─── Actor Kinds ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ActorKind {
    Echo,
    Counter,
    Noop,
    Bomber,
    WrongType,
}

impl fmt::Display for ActorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorKind::Echo => write!(f, "Echo"),
            ActorKind::Counter => write!(f, "Counter"),
            ActorKind::Noop => write!(f, "Noop"),
            ActorKind::Bomber => write!(f, "Bomber"),
            ActorKind::WrongType => write!(f, "WrongType"),
        }
    }
}

struct EchoActor;
impl ActorInterface for EchoActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: FuzzMsg) {}
}

struct CounterActor {
    count: u64,
}
impl ActorInterface for CounterActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: FuzzMsg) {
        self.count += 1;
    }
}

struct NoopActor;
impl ActorInterface for NoopActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: FuzzMsg) {}
}

const BOMBER_BUDGET: u32 = 256;

struct BomberActor {
    n: u8,
    remaining: u32,
}

impl BomberActor {
    fn new(n: u8) -> Self {
        Self {
            n,
            remaining: BOMBER_BUDGET,
        }
    }
}

impl ActorInterface for BomberActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        let to_send = (self.n as u32).min(self.remaining);
        self.remaining = self.remaining.saturating_sub(to_send);
        for _ in 0..to_send {
            let _ = ctx.send(ctx.self_addr(), FuzzMsg { value: msg.value });
        }
    }
}

struct WrongTypeActor;
impl ActorInterface for WrongTypeActor {
    type Incoming = WrongTypeMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: WrongTypeMsg) {}
}

// ─── Action Enum ────────────────────────────────────────────────────────────

#[derive(Debug, Arbitrary)]
enum Action {
    // Spawning
    SpawnEcho,
    SpawnCounter,
    SpawnNoop,
    SpawnBomber { n: u8 },
    SpawnWrongType,

    // Messaging
    Send { actor_idx: u8, msg: FuzzMsg },
    SendWrongType { actor_idx: u8 },
    BurstSend { actor_idx: u8, count: u8, value: u64 },

    // Inboxes
    NewInbox,
    DrainInbox { inbox_idx: u8 },

    // Timing — sleep between actions to create interleaving variety
    SleepMicros { us: u8 },

    // Observation
    CheckStats,
}

// ─── FuzzInput ──────────────────────────────────────────────────────────────

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    num_threads: u8,
    max_actors: u8,
    actions: Vec<Action>,
}

// ─── FuzzState ──────────────────────────────────────────────────────────────

struct FuzzState {
    handle: RuntimeHandle,
    actors: Vec<(ActorAddress, ActorKind)>,
    inboxes: Vec<Inbox<FuzzMsg>>,
    total_spawned: usize,
    total_sent: usize,
    trace: Option<String>,
}

impl FuzzState {
    // ── Logging helpers ─────────────────────────────────────────

    fn log(&mut self, line: fmt::Arguments<'_>) {
        if let Some(ref mut t) = self.trace {
            let _ = writeln!(t, "  {line}");
        }
    }

    fn actor_label(&self, addr: ActorAddress) -> String {
        for (i, (a, kind)) in self.actors.iter().enumerate() {
            if *a == addr {
                return format!("actor#{i}({kind})");
            }
        }
        "actor#?".into()
    }

    // ── Resolvers ───────────────────────────────────────────────

    fn resolve_actor_addr(&self, idx: u8) -> Option<ActorAddress> {
        if self.actors.is_empty() {
            None
        } else {
            Some(self.actors[idx as usize % self.actors.len()].0)
        }
    }

    fn resolve_inbox(&self, idx: u8) -> Option<usize> {
        if self.inboxes.is_empty() {
            None
        } else {
            Some(idx as usize % self.inboxes.len())
        }
    }

    // ── Execution ───────────────────────────────────────────────

    fn execute(&mut self, action: &Action) {
        let rt = &self.handle.runtime;
        match action {
            Action::SpawnEcho => {
                if let Ok(addr) = rt.spawn(EchoActor) {
                    let id = self.actors.len();
                    self.actors.push((addr, ActorKind::Echo));
                    self.total_spawned += 1;
                    self.log(format_args!("[SPAWN]  Echo                 -> actor#{id}"));
                }
            }
            Action::SpawnCounter => {
                if let Ok(addr) = rt.spawn(CounterActor { count: 0 }) {
                    let id = self.actors.len();
                    self.actors.push((addr, ActorKind::Counter));
                    self.total_spawned += 1;
                    self.log(format_args!("[SPAWN]  Counter              -> actor#{id}"));
                }
            }
            Action::SpawnNoop => {
                if let Ok(addr) = rt.spawn(NoopActor) {
                    let id = self.actors.len();
                    self.actors.push((addr, ActorKind::Noop));
                    self.total_spawned += 1;
                    self.log(format_args!("[SPAWN]  Noop                 -> actor#{id}"));
                }
            }
            Action::SpawnBomber { n } => {
                let clamped = (*n).max(1).min(16);
                if let Ok(addr) = rt.spawn(BomberActor::new(clamped)) {
                    let id = self.actors.len();
                    self.actors.push((addr, ActorKind::Bomber));
                    self.total_spawned += 1;
                    self.log(format_args!("[SPAWN]  Bomber(n={clamped})          -> actor#{id}"));
                }
            }
            Action::SpawnWrongType => {
                if let Ok(addr) = rt.spawn(WrongTypeActor) {
                    let id = self.actors.len();
                    self.actors.push((addr, ActorKind::WrongType));
                    self.total_spawned += 1;
                    self.log(format_args!("[SPAWN]  WrongType            -> actor#{id}"));
                }
            }
            Action::Send { actor_idx, msg } => {
                if let Some(addr) = self.resolve_actor_addr(*actor_idx) {
                    let label = self.actor_label(addr);
                    let _ = rt.send_to(addr, msg.clone());
                    self.total_sent += 1;
                    self.log(format_args!("[SEND]   {label} <- FuzzMsg(val={})", msg.value));
                }
            }
            Action::SendWrongType { actor_idx } => {
                if let Some(addr) = self.resolve_actor_addr(*actor_idx) {
                    let label = self.actor_label(addr);
                    let _ = rt.send_to(addr, WrongTypeMsg);
                    self.total_sent += 1;
                    self.log(format_args!("[SEND!]  {label} <- WrongTypeMsg (mismatch)"));
                }
            }
            Action::BurstSend {
                actor_idx,
                count,
                value,
            } => {
                if let Some(addr) = self.resolve_actor_addr(*actor_idx) {
                    let label = self.actor_label(addr);
                    let n = (*count).max(1).min(32) as usize;
                    for _ in 0..n {
                        let _ = rt.send_to(addr, FuzzMsg { value: *value });
                    }
                    self.total_sent += n;
                    self.log(format_args!("[BURST]  {label} <- FuzzMsg x{n}"));
                }
            }
            Action::NewInbox => {
                if let Ok(inbox) = rt.new_inbox::<FuzzMsg>() {
                    let id = self.inboxes.len();
                    self.inboxes.push(inbox);
                    self.log(format_args!("[INBOX]  new inbox#{id}"));
                }
            }
            Action::DrainInbox { inbox_idx } => {
                if let Some(i) = self.resolve_inbox(*inbox_idx) {
                    let mut count = 0usize;
                    while self.inboxes[i].try_recv().is_some() {
                        count += 1;
                    }
                    if count > 0 {
                        self.log(format_args!("[DRAIN]  inbox#{i} -> {count} msg(s)"));
                    } else {
                        self.log(format_args!("[DRAIN]  inbox#{i} -> empty"));
                    }
                }
            }
            Action::SleepMicros { us } => {
                let micros = (*us as u64).min(500);
                if micros > 0 {
                    thread::sleep(Duration::from_micros(micros));
                    self.log(format_args!("[SLEEP]  {micros}us"));
                }
            }
            Action::CheckStats => {
                self.check_invariants();
                let stats = self.handle.runtime.stats();
                let depth: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
                self.log(format_args!(
                    "[CHECK]  ok ({} actors, {depth} queued)",
                    stats.actors.len()
                ));
            }
        }
    }

    fn check_invariants(&self) {
        let stats = self.handle.runtime.stats();

        assert!(
            stats.num_workers >= 1,
            "num_workers must be >= 1, got {}",
            stats.num_workers
        );

        for (addr, wid) in &stats.actors {
            assert!(
                *wid < stats.num_workers,
                "actor {:?} on worker {} but only {} workers",
                addr,
                wid,
                stats.num_workers
            );
        }

        for info in &stats.workers {
            assert!(info.id < stats.num_workers);
        }

        assert_eq!(stats.workers.len(), stats.num_workers);
    }
}

impl fmt::Debug for FuzzState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuzzState")
            .field("actors", &self.actors.len())
            .field("inboxes", &self.inboxes.len())
            .field("total_spawned", &self.total_spawned)
            .finish()
    }
}

// ─── Fuzz Target ────────────────────────────────────────────────────────────

fuzz_target!(|input: FuzzInput| {
    let num_threads = (input.num_threads as usize).max(2).min(4);
    let max_actors = (input.max_actors as usize).max(1).min(200);

    let interval = log_interval();
    let run = if interval > 0 {
        RUN_COUNTER.fetch_add(1, Ordering::Relaxed) + 1
    } else {
        0
    };
    let tracing = interval > 0 && run % interval == 0;

    let config = RuntimeConfig {
        max_actors,
        num_threads,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    // run() consumes the Runtime and spawns worker threads
    let handle = match rt.run() {
        Ok(h) => h,
        Err(_) => return,
    };

    let mut state = FuzzState {
        handle,
        actors: Vec::new(),
        inboxes: Vec::new(),
        total_spawned: 0,
        total_sent: 0,
        trace: if tracing {
            Some(String::with_capacity(1024))
        } else {
            None
        },
    };

    let action_limit = input.actions.len().min(256);
    let actions = &input.actions[..action_limit];
    for action in actions {
        state.execute(action);
    }

    // Let workers process remaining messages
    thread::sleep(Duration::from_millis(10));

    state.check_invariants();

    // Print trace if this run was logged
    if let Some(trace) = &state.trace {
        let stats = state.handle.runtime.stats();
        let processed: u64 = stats.workers.iter().map(|w| w.messages_processed).sum();
        let depth: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        eprintln!(
            "\
\n=== Run #{run} (MT) | threads={num_threads} cap={max_actors} | {total} actions ===
{trace}\
--- {spawned} spawned, {sent} sent, {alive} alive, {depth} queued, {processed} processed ---\n",
            total = actions.len(),
            spawned = state.total_spawned,
            sent = state.total_sent,
            alive = stats.actors.len(),
        );
    }

    state.handle.shutdown();

    // Destructure to move handle out for join() which consumes self
    let FuzzState { handle, .. } = state;
    handle.join();
});
