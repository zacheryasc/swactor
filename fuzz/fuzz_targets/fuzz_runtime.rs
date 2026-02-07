#![no_main]

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Inbox, Runtime};

// ─── Run Logging ────────────────────────────────────────────────────────────
//   FUZZ_LOG=1   → trace every run
//   FUZZ_LOG=100 → trace every 100th run
//   unset        → silent

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

#[derive(Clone, Debug, Arbitrary)]
struct FuzzMsg {
    value: u64,
    reply_to_idx: Option<u8>,
}

#[derive(Clone, Debug)]
struct WrongTypeMsg;

// ─── Actor Kinds ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ActorKind {
    Echo,
    Counter,
    Forwarder,
    Spawner,
    Bomber,
    Noop,
    WrongType,
}

impl fmt::Display for ActorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorKind::Echo => write!(f, "Echo"),
            ActorKind::Counter => write!(f, "Counter"),
            ActorKind::Forwarder => write!(f, "Forwarder"),
            ActorKind::Spawner => write!(f, "Spawner"),
            ActorKind::Bomber => write!(f, "Bomber"),
            ActorKind::Noop => write!(f, "Noop"),
            ActorKind::WrongType => write!(f, "WrongType"),
        }
    }
}

struct EchoActor;
impl ActorInterface for EchoActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        if let Some(idx) = msg.reply_to_idx {
            let reply = FuzzMsg { value: msg.value, reply_to_idx: None };
            let _ = ctx.send(INBOX_ADDRS.lock_or_default().get(idx as usize), reply);
        }
    }
}

struct CounterActor { count: u64 }
impl ActorInterface for CounterActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        self.count += 1;
        if let Some(idx) = msg.reply_to_idx {
            let reply = FuzzMsg { value: self.count, reply_to_idx: None };
            let _ = ctx.send(INBOX_ADDRS.lock_or_default().get(idx as usize), reply);
        }
    }
}

struct ForwarderActor { target: ActorAddress }
impl ActorInterface for ForwarderActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        let _ = ctx.send(self.target, msg);
    }
}

struct SpawnerActor;
impl ActorInterface for SpawnerActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        let _ = ctx.spawn(NoopActor);
        if let Some(idx) = msg.reply_to_idx {
            let reply = FuzzMsg { value: msg.value, reply_to_idx: None };
            let _ = ctx.send(INBOX_ADDRS.lock_or_default().get(idx as usize), reply);
        }
    }
}

const BOMBER_BUDGET: u32 = 256;
struct BomberActor { n: u8, remaining: u32 }
impl BomberActor {
    fn new(n: u8) -> Self { Self { n, remaining: BOMBER_BUDGET } }
}
impl ActorInterface for BomberActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FuzzMsg) {
        let to_send = (self.n as u32).min(self.remaining);
        self.remaining = self.remaining.saturating_sub(to_send);
        for _ in 0..to_send {
            let _ = ctx.send(ctx.self_addr(), FuzzMsg { value: msg.value, reply_to_idx: None });
        }
    }
}

struct NoopActor;
impl ActorInterface for NoopActor {
    type Incoming = FuzzMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: FuzzMsg) {}
}

struct WrongTypeActor;
impl ActorInterface for WrongTypeActor {
    type Incoming = WrongTypeMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: WrongTypeMsg) {}
}

// ─── Shared Inbox Address Table ─────────────────────────────────────────────

struct InboxAddrs(Vec<ActorAddress>);
impl InboxAddrs {
    fn get(&self, idx: usize) -> ActorAddress {
        if self.0.is_empty() { ActorAddress::default() }
        else { self.0[idx % self.0.len()] }
    }
}
struct InboxAddrsCell(std::sync::Mutex<InboxAddrs>);
impl InboxAddrsCell {
    const fn new() -> Self { Self(std::sync::Mutex::new(InboxAddrs(Vec::new()))) }
    fn lock_or_default(&self) -> std::sync::MutexGuard<'_, InboxAddrs> { self.0.lock().unwrap() }
    fn update(&self, addrs: Vec<ActorAddress>) { self.0.lock().unwrap().0 = addrs; }
}
static INBOX_ADDRS: InboxAddrsCell = InboxAddrsCell::new();

// ─── Scenarios ──────────────────────────────────────────────────────────────

#[derive(Debug, Arbitrary)]
enum Scenario {
    EchoRoundTrip { value: u64 },
    CountToN { n: u8 },
    ForwardChain { len: u8, value: u64 },
    ForwardRing { len: u8, ticks: u8 },
    Fanout { n: u8, value: u64 },
    SpawnAndImmediateSend { value: u64 },
    SpawnSendTickSendTick { v1: u64, v2: u64 },
    InterleavedSpawnSend { count: u8 },
    BomberStress { bomber_n: u8, burst: u8 },
    BackpressureStepper { burst: u8 },
    TypeConfusionBarrage { num_fuzz: u8, num_wrong: u8, sends: u8 },
    WrongTypeToAll,
    DeadLetterFlood { count: u8 },
    InboxReuse { sends: u8, value: u64 },
    SpawnStorm { sends: u8 },
    PopulateAndObserve { kinds: Vec<ActorKindChoice> },
    RawActions { actions: Vec<RawAction> },
    TickFlush { n: u8 },
    CheckInvariants,
}

#[derive(Debug, Arbitrary, Clone, Copy)]
enum ActorKindChoice { Echo, Counter, Noop, Spawner, Bomber { n: u8 }, WrongType }

#[derive(Debug, Arbitrary)]
enum RawAction {
    SpawnEcho,
    SpawnCounter,
    SpawnForwarder { target_idx: u8 },
    SpawnNoop,
    SpawnWrongType,
    Send { actor_idx: u8, msg: FuzzMsg },
    SendWrongType { actor_idx: u8 },
    SendToRandomAddr { seed: u16 },
    BurstSend { actor_idx: u8, count: u8, value: u64 },
    NewInbox,
    DrainInbox { inbox_idx: u8 },
    DrainAll,
    Tick,
    TickN { n: u8 },
}

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    max_actors: u8,
    mailbox_waterlevel: u8,
    scenarios: Vec<Scenario>,
}

// ─── FuzzState ──────────────────────────────────────────────────────────────

struct FuzzState {
    runtime: Runtime,
    actors: Vec<(ActorAddress, ActorKind)>,
    inboxes: Vec<Inbox<FuzzMsg>>,
    total_spawned: usize,
    total_sent: usize,
    total_inbox_received: usize,
    trace: Option<String>,
}

impl FuzzState {
    fn new(runtime: Runtime, tracing: bool) -> Self {
        Self {
            runtime,
            actors: Vec::new(),
            inboxes: Vec::new(),
            total_spawned: 0,
            total_sent: 0,
            total_inbox_received: 0,
            trace: if tracing { Some(String::with_capacity(2048)) } else { None },
        }
    }

    // ── Logging helpers ─────────────────────────────────────────

    fn log(&mut self, line: fmt::Arguments<'_>) {
        if let Some(ref mut t) = self.trace {
            let _ = writeln!(t, "  {line}");
        }
    }

    fn log_header(&mut self, name: &str) {
        if let Some(ref mut t) = self.trace {
            let _ = writeln!(t, "--- {name} ---");
        }
    }

    fn actor_label(&self, addr: ActorAddress) -> String {
        for (i, (a, kind)) in self.actors.iter().enumerate() {
            if *a == addr { return format!("actor#{i}({kind})"); }
        }
        "actor#?".into()
    }

    fn msg_label(&self, msg: &FuzzMsg) -> String {
        match msg.reply_to_idx {
            Some(idx) => {
                let resolved = if self.inboxes.is_empty() { "none".into() }
                    else { format!("inbox#{}", idx as usize % self.inboxes.len()) };
                format!("FuzzMsg(reply_to={resolved})")
            }
            None => "FuzzMsg".into(),
        }
    }

    // ── Shared helpers ──────────────────────────────────────────

    fn sync_inbox_addrs(&self) {
        let addrs: Vec<ActorAddress> = self.inboxes.iter().map(|i| *i.addr()).collect();
        INBOX_ADDRS.update(addrs);
    }

    fn resolve_actor(&self, idx: u8) -> Option<ActorAddress> {
        if self.actors.is_empty() { None }
        else { Some(self.actors[idx as usize % self.actors.len()].0) }
    }

    fn resolve_inbox_idx(&self, idx: u8) -> Option<usize> {
        if self.inboxes.is_empty() { None }
        else { Some(idx as usize % self.inboxes.len()) }
    }

    fn msg(&self, value: u64, inbox_idx: Option<u8>) -> FuzzMsg {
        FuzzMsg { value, reply_to_idx: inbox_idx }
    }

    // ── Primitive operations (with tracing) ─────────────────────

    fn spawn_actor(&mut self, kind: ActorKind) -> Option<ActorAddress> {
        let result = match kind {
            ActorKind::Echo => self.runtime.spawn(EchoActor),
            ActorKind::Counter => self.runtime.spawn(CounterActor { count: 0 }),
            ActorKind::Noop => self.runtime.spawn(NoopActor),
            ActorKind::Spawner => self.runtime.spawn(SpawnerActor),
            ActorKind::WrongType => self.runtime.spawn(WrongTypeActor),
            _ => return None,
        };
        if let Ok(addr) = result {
            let id = self.actors.len();
            self.actors.push((addr, kind));
            self.total_spawned += 1;
            self.log(format_args!("[SPAWN]  {kind:<20} -> actor#{id}"));
            Some(addr)
        } else {
            self.log(format_args!("[SPAWN]  {kind:<20} -> FAILED (full)"));
            None
        }
    }

    fn spawn_forwarder(&mut self, target: ActorAddress) -> Option<ActorAddress> {
        let target_label = self.actor_label(target);
        if let Ok(addr) = self.runtime.spawn(ForwarderActor { target }) {
            let id = self.actors.len();
            self.actors.push((addr, ActorKind::Forwarder));
            self.total_spawned += 1;
            self.log(format_args!("[SPAWN]  Forwarder -> {target_label:<10} -> actor#{id}"));
            Some(addr)
        } else {
            self.log(format_args!("[SPAWN]  Forwarder              -> FAILED (full)"));
            None
        }
    }

    fn spawn_bomber(&mut self, n: u8) -> Option<ActorAddress> {
        let clamped = n.max(1).min(16);
        if let Ok(addr) = self.runtime.spawn(BomberActor::new(clamped)) {
            let id = self.actors.len();
            self.actors.push((addr, ActorKind::Bomber));
            self.total_spawned += 1;
            self.log(format_args!("[SPAWN]  Bomber(n={clamped}){:<13} -> actor#{id}", ""));
            Some(addr)
        } else {
            self.log(format_args!("[SPAWN]  Bomber(n={clamped})            -> FAILED (full)"));
            None
        }
    }

    fn send_msg(&mut self, addr: ActorAddress, msg: FuzzMsg) {
        let actor = self.actor_label(addr);
        let m = self.msg_label(&msg);
        let _ = self.runtime.send_to(addr, msg);
        self.total_sent += 1;
        self.log(format_args!("[SEND]   {actor} <- {m}"));
    }

    fn send_wrong_type(&mut self, addr: ActorAddress) {
        let actor = self.actor_label(addr);
        let _ = self.runtime.send_to(addr, WrongTypeMsg);
        self.total_sent += 1;
        self.log(format_args!("[SEND!]  {actor} <- WrongTypeMsg (mismatch)"));
    }

    fn send_dead_letter(&mut self, seed: u16) {
        let mut bytes = [0u8; 32];
        bytes[0..2].copy_from_slice(&seed.to_le_bytes());
        bytes[2] = 0xFF;
        bytes[3] = 0xFF;
        let _ = self.runtime.send_to(
            ActorAddress(bytes),
            FuzzMsg { value: seed as u64, reply_to_idx: None },
        );
        self.log(format_args!("[SEND?]  <dead letter seed={seed}>"));
    }

    fn burst_send(&mut self, addr: ActorAddress, count: usize, value: u64) {
        let actor = self.actor_label(addr);
        for _ in 0..count {
            let _ = self.runtime.send_to(addr, self.msg(value, None));
            self.total_sent += 1;
        }
        self.log(format_args!("[BURST]  {actor} <- FuzzMsg x{count}"));
    }

    fn new_inbox(&mut self) -> Option<usize> {
        if let Ok(inbox) = self.runtime.new_inbox::<FuzzMsg>() {
            let id = self.inboxes.len();
            self.inboxes.push(inbox);
            self.sync_inbox_addrs();
            self.log(format_args!("[INBOX]  new inbox#{id}"));
            Some(id)
        } else { None }
    }

    fn drain_inbox(&mut self, idx: usize) -> usize {
        let mut count = 0;
        while self.inboxes[idx].try_recv().is_some() {
            count += 1;
            self.total_inbox_received += 1;
        }
        if count > 0 {
            self.log(format_args!("[DRAIN]  inbox#{idx} -> {count} msg(s)"));
        } else {
            self.log(format_args!("[DRAIN]  inbox#{idx} -> empty"));
        }
        count
    }

    fn drain_all_inboxes(&mut self) -> usize {
        let mut count = 0;
        for inbox in &self.inboxes {
            while inbox.try_recv().is_some() { count += 1; }
        }
        self.total_inbox_received += count;
        self.log(format_args!("[DRAIN]  all {} inbox(es) -> {count} msg(s)", self.inboxes.len()));
        count
    }

    fn tick(&mut self) {
        self.runtime.tick();
        self.log(format_args!("[TICK]"));
    }

    fn tick_n(&mut self, n: usize) {
        for _ in 0..n { self.runtime.tick(); }
        if n > 1 {
            self.log(format_args!("[TICK]   x{n}"));
        } else {
            self.log(format_args!("[TICK]"));
        }
    }

    fn check_invariants(&self) {
        let stats = self.runtime.stats();
        assert!(stats.num_workers >= 1);
        for (addr, wid) in &stats.actors {
            assert!(*wid < stats.num_workers,
                "actor {:?} on worker {} but only {} workers", addr, wid, stats.num_workers);
        }
        for info in &stats.workers {
            assert!(info.id < stats.num_workers);
        }
        assert_eq!(stats.workers.len(), stats.num_workers);
        for inbox in &self.inboxes {
            if let Some(msg) = inbox.try_recv() { let _ = msg.value; }
        }
    }

    fn log_check(&mut self) {
        self.check_invariants();
        let stats = self.runtime.stats();
        let depth: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        self.log(format_args!("[CHECK]  ok ({} actors, {depth} queued)", stats.actors.len()));
    }

    // ── Scenario execution ──────────────────────────────────────

    fn run_scenario(&mut self, scenario: &Scenario) {
        match scenario {
            Scenario::EchoRoundTrip { value } => {
                self.log_header("Echo Round-Trip");
                let inbox_i = self.new_inbox();
                let inbox_reply = inbox_i.map(|i| i as u8);
                if let Some(addr) = self.spawn_actor(ActorKind::Echo) {
                    self.tick();
                    self.send_msg(addr, self.msg(*value, inbox_reply));
                    self.tick_n(3);
                    if let Some(i) = inbox_i { self.drain_inbox(i); }
                }
            }

            Scenario::CountToN { n } => {
                let count = (*n).max(1).min(32) as usize;
                self.log_header(&format!("Count to {count}"));
                let inbox_i = self.new_inbox();
                let inbox_reply = inbox_i.map(|i| i as u8);
                if let Some(addr) = self.spawn_actor(ActorKind::Counter) {
                    self.tick();
                    let label = self.actor_label(addr);
                    for _ in 0..count {
                        let _ = self.runtime.send_to(addr, self.msg(0, inbox_reply));
                        self.total_sent += 1;
                    }
                    self.log(format_args!("[BURST]  {label} <- FuzzMsg x{count}"));
                    self.tick_n(count + 2);
                    if let Some(i) = inbox_i { self.drain_inbox(i); }
                }
            }

            Scenario::ForwardChain { len, value } => {
                let chain_len = (*len).max(1).min(16) as usize;
                self.log_header(&format!("Forward Chain ({chain_len} hops)"));
                let inbox_i = self.new_inbox();
                let inbox_reply = inbox_i.map(|i| i as u8);
                let tail = self.spawn_actor(ActorKind::Echo);
                self.tick();
                let mut next = match tail { Some(a) => a, None => return };
                for _ in 0..chain_len {
                    match self.spawn_forwarder(next) {
                        Some(a) => { self.tick(); next = a; }
                        None => return,
                    }
                }
                self.send_msg(next, self.msg(*value, inbox_reply));
                self.tick_n(chain_len + 4);
                if let Some(i) = inbox_i { self.drain_inbox(i); }
            }

            Scenario::ForwardRing { len, ticks } => {
                let ring_len = (*len).max(2).min(12) as usize;
                let tick_count = (*ticks).max(1).min(64) as usize;
                self.log_header(&format!("Forward Ring ({ring_len} nodes, {tick_count} ticks)"));
                let mut prev_addr = ActorAddress::default();
                let mut addrs = Vec::with_capacity(ring_len);
                for _ in 0..ring_len {
                    match self.spawn_forwarder(prev_addr) {
                        Some(a) => { self.tick(); addrs.push(a); prev_addr = a; }
                        None => return,
                    }
                }
                if let Some(&last) = addrs.last() {
                    self.send_msg(last, self.msg(0, None));
                    self.tick_n(tick_count);
                }
            }

            Scenario::Fanout { n, value } => {
                let fan = (*n).max(1).min(20) as usize;
                self.log_header(&format!("Fan-Out ({fan} echoes)"));
                let inbox_i = self.new_inbox();
                let inbox_reply = inbox_i.map(|i| i as u8);
                let mut targets = Vec::with_capacity(fan);
                for _ in 0..fan {
                    if let Some(addr) = self.spawn_actor(ActorKind::Echo) {
                        targets.push(addr);
                    }
                }
                self.tick();
                for addr in &targets {
                    self.send_msg(*addr, self.msg(*value, inbox_reply));
                }
                self.tick_n(fan + 2);
                if let Some(i) = inbox_i { self.drain_inbox(i); }
            }

            Scenario::SpawnAndImmediateSend { value } => {
                self.log_header("Spawn + Immediate Send (NO TICK)");
                if let Some(addr) = self.spawn_actor(ActorKind::Echo) {
                    let inbox_i = self.new_inbox();
                    let inbox_reply = inbox_i.map(|i| i as u8);
                    self.log(format_args!("         (actor not ticked into pool yet)"));
                    self.send_msg(addr, self.msg(*value, inbox_reply));
                    self.tick_n(4);
                    if let Some(i) = inbox_i { self.drain_inbox(i); }
                }
            }

            Scenario::SpawnSendTickSendTick { v1, v2 } => {
                self.log_header("Two-Phase Delivery");
                let inbox_i = self.new_inbox();
                let inbox_reply = inbox_i.map(|i| i as u8);
                if let Some(addr) = self.spawn_actor(ActorKind::Counter) {
                    self.send_msg(addr, self.msg(*v1, inbox_reply));
                    self.tick_n(2);
                    self.send_msg(addr, self.msg(*v2, inbox_reply));
                    self.tick_n(2);
                    if let Some(i) = inbox_i { self.drain_inbox(i); }
                }
            }

            Scenario::InterleavedSpawnSend { count } => {
                let n = (*count).max(1).min(20) as usize;
                self.log_header(&format!("Interleaved Spawn+Send (x{n}, no ticks)"));
                for v in 0..n {
                    self.spawn_actor(ActorKind::Noop);
                    if let Some(addr) = self.resolve_actor(v as u8) {
                        self.send_msg(addr, self.msg(v as u64, None));
                    }
                }
                self.log(format_args!("         (now flushing)"));
                self.tick_n(n + 2);
            }

            Scenario::BomberStress { bomber_n, burst } => {
                let n = (*bomber_n).max(1).min(16);
                let burst_count = (*burst).max(1).min(32) as usize;
                self.log_header(&format!("Bomber Stress (n={n}, burst={burst_count})"));
                if let Some(addr) = self.spawn_bomber(n) {
                    self.tick();
                    self.burst_send(addr, burst_count, 0);
                    self.tick_n(16);
                    self.log_check();
                }
            }

            Scenario::BackpressureStepper { burst } => {
                let burst_count = (*burst).max(4).min(64) as usize;
                self.log_header(&format!("Backpressure Stepper (burst={burst_count})"));
                if let Some(addr) = self.spawn_actor(ActorKind::Noop) {
                    self.tick();
                    self.burst_send(addr, burst_count, 0);
                    self.tick();
                    self.log_check();
                    self.tick();
                    self.log_check();
                    self.tick_n(burst_count);
                }
            }

            Scenario::TypeConfusionBarrage { num_fuzz, num_wrong, sends } => {
                let nf = (*num_fuzz).max(1).min(10) as usize;
                let nw = (*num_wrong).max(1).min(10) as usize;
                let nsends = (*sends).max(1).min(32) as usize;
                self.log_header(&format!("Type Confusion ({nf} normal + {nw} wrong-type, {nsends} rounds)"));
                let start_idx = self.actors.len();
                for _ in 0..nf { self.spawn_actor(ActorKind::Echo); }
                for _ in 0..nw { self.spawn_actor(ActorKind::WrongType); }
                self.tick();
                let end_idx = self.actors.len();
                for _ in 0..nsends {
                    for i in start_idx..end_idx {
                        let addr = self.actors[i].0;
                        let kind = self.actors[i].1;
                        match kind {
                            ActorKind::WrongType => {
                                self.send_msg(addr, self.msg(0, None));
                            }
                            _ => {
                                self.send_wrong_type(addr);
                            }
                        }
                    }
                }
                self.tick_n(nsends + 2);
            }

            Scenario::WrongTypeToAll => {
                let n = self.actors.len();
                self.log_header(&format!("Wrong Type Blast ({n} actors)"));
                for i in 0..n {
                    let addr = self.actors[i].0;
                    self.send_wrong_type(addr);
                }
                self.tick_n(4);
            }

            Scenario::DeadLetterFlood { count } => {
                let n = (*count).max(1).min(64) as usize;
                self.log_header(&format!("Dead Letter Flood (x{n})"));
                for seed in 0..n {
                    self.send_dead_letter(seed as u16);
                }
            }

            Scenario::InboxReuse { sends, value } => {
                let n = (*sends).max(1).min(16) as usize;
                self.log_header(&format!("Inbox Reuse ({n} sends x2 rounds)"));
                if let Some(idx) = self.new_inbox() {
                    let addr = *self.inboxes[idx].addr();
                    for round in 0..2u64 {
                        self.log(format_args!("         round {}", round + 1));
                        for _ in 0..n {
                            let _ = self.runtime.send_to(
                                addr, self.msg(value.wrapping_add(round), None),
                            );
                            self.total_sent += 1;
                        }
                        self.log(format_args!("[SEND]   inbox#{idx} <- FuzzMsg x{n}"));
                        self.drain_inbox(idx);
                    }
                }
            }

            Scenario::SpawnStorm { sends } => {
                let n = (*sends).max(1).min(24) as usize;
                self.log_header(&format!("Spawn Storm ({n} child spawns)"));
                if let Some(addr) = self.spawn_actor(ActorKind::Spawner) {
                    self.tick();
                    self.burst_send(addr, n, 0);
                    self.log(format_args!("         (each msg spawns a child Noop)"));
                    self.tick_n(n + 4);
                    self.log_check();
                }
            }

            Scenario::PopulateAndObserve { kinds } => {
                let limit = kinds.len().min(30);
                self.log_header(&format!("Populate & Observe ({limit} actors)"));
                for k in &kinds[..limit] {
                    match k {
                        ActorKindChoice::Echo => { self.spawn_actor(ActorKind::Echo); }
                        ActorKindChoice::Counter => { self.spawn_actor(ActorKind::Counter); }
                        ActorKindChoice::Noop => { self.spawn_actor(ActorKind::Noop); }
                        ActorKindChoice::Spawner => { self.spawn_actor(ActorKind::Spawner); }
                        ActorKindChoice::Bomber { n } => { self.spawn_bomber(*n); }
                        ActorKindChoice::WrongType => { self.spawn_actor(ActorKind::WrongType); }
                    }
                }
                self.tick();
                self.log_check();
            }

            Scenario::RawActions { actions } => {
                let limit = actions.len().min(64);
                self.log_header(&format!("Raw Actions ({limit} ops)"));
                for action in &actions[..limit] {
                    self.run_raw(action);
                }
            }

            Scenario::TickFlush { n } => {
                let t = (*n).max(1).min(64) as usize;
                self.log_header(&format!("Tick Flush (x{t})"));
                self.tick_n(t);
            }

            Scenario::CheckInvariants => {
                self.log_header("Invariant Check");
                self.log_check();
            }
        }
    }

    fn run_raw(&mut self, action: &RawAction) {
        match action {
            RawAction::SpawnEcho => { self.spawn_actor(ActorKind::Echo); }
            RawAction::SpawnCounter => { self.spawn_actor(ActorKind::Counter); }
            RawAction::SpawnForwarder { target_idx } => {
                let target = self.resolve_actor(*target_idx)
                    .unwrap_or(ActorAddress::default());
                self.spawn_forwarder(target);
            }
            RawAction::SpawnNoop => { self.spawn_actor(ActorKind::Noop); }
            RawAction::SpawnWrongType => { self.spawn_actor(ActorKind::WrongType); }
            RawAction::Send { actor_idx, msg } => {
                if let Some(addr) = self.resolve_actor(*actor_idx) {
                    self.send_msg(addr, msg.clone());
                }
            }
            RawAction::SendWrongType { actor_idx } => {
                if let Some(addr) = self.resolve_actor(*actor_idx) {
                    self.send_wrong_type(addr);
                }
            }
            RawAction::SendToRandomAddr { seed } => { self.send_dead_letter(*seed); }
            RawAction::BurstSend { actor_idx, count, value } => {
                if let Some(addr) = self.resolve_actor(*actor_idx) {
                    self.burst_send(addr, (*count).max(1).min(64) as usize, *value);
                }
            }
            RawAction::NewInbox => { self.new_inbox(); }
            RawAction::DrainInbox { inbox_idx } => {
                if let Some(i) = self.resolve_inbox_idx(*inbox_idx) {
                    self.drain_inbox(i);
                }
            }
            RawAction::DrainAll => { self.drain_all_inboxes(); }
            RawAction::Tick => { self.tick(); }
            RawAction::TickN { n } => { self.tick_n((*n).max(1).min(64) as usize); }
        }
    }
}

impl fmt::Debug for FuzzState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuzzState")
            .field("actors", &self.actors.len())
            .field("inboxes", &self.inboxes.len())
            .field("total_spawned", &self.total_spawned)
            .field("total_sent", &self.total_sent)
            .finish()
    }
}

// ─── Fuzz Target ────────────────────────────────────────────────────────────

fuzz_target!(|input: FuzzInput| {
    let max_actors = (input.max_actors as usize).max(1).min(200);
    let mailbox_waterlevel = (input.mailbox_waterlevel as usize).max(1).min(50);

    let interval = log_interval();
    let run = if interval > 0 {
        RUN_COUNTER.fetch_add(1, Ordering::Relaxed) + 1
    } else { 0 };
    let tracing = interval > 0 && run % interval == 0;

    let config = RuntimeConfig {
        max_actors,
        mailbox_waterlevel,
        num_threads: 1,
        ..Default::default()
    };
    let rt = Runtime::new(config);
    let mut state = FuzzState::new(rt, tracing);

    let scenario_limit = input.scenarios.len().min(64);
    let scenarios = &input.scenarios[..scenario_limit];
    for scenario in scenarios {
        state.run_scenario(scenario);
    }

    // Final flush
    for _ in 0..64 { state.runtime.tick(); }
    state.check_invariants();

    // Print trace if this run was logged
    if let Some(trace) = &state.trace {
        let stats = state.runtime.stats();
        let depth: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        let processed: u64 = stats.workers.iter().map(|w| w.messages_processed).sum();
        eprintln!("\
\n=== Run #{run} | cap={max_actors} waterlevel={mailbox_waterlevel} ===
{trace}\
--- {spawned} spawned, {sent} sent, {recv} received, \
{alive} alive, {depth} queued, {processed} processed ---\n",
            spawned = state.total_spawned,
            sent = state.total_sent,
            recv = state.total_inbox_received,
            alive = stats.actors.len(),
        );
    }
});
