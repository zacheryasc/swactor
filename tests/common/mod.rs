// Shared types and helpers for runtime test files.

#![allow(dead_code, unused_imports)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub use swactor::actor::{
    ActorAddress, ActorExited, ActorInterface, CapabilitySet, Down, Environment,
    EnvironmentBuilder, ExitReason, ExitValue, LogicalName, MonitorRef, ServiceBinding,
    SpawnBuilder, SpawnTimestamp, StopReason,
};
pub use swactor::runtime::{Ctx, Inbox, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime};
pub use swactor::std::{CtxGroups, CtxWatching, RuntimeGroups, RuntimeNaming, StdExtension};

// ── Messages ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Ping {
    pub reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Pong;

#[derive(Clone)]
pub struct Increment {
    pub reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Count(pub usize);

#[derive(Clone)]
pub struct Forward {
    pub value: usize,
    pub reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Done(pub usize);

/// Ask an actor for its own address.
#[derive(Clone)]
pub struct WhoAreYou {
    pub reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MyAddr(pub ActorAddress);

#[derive(Clone)]
pub struct PanicMsg;

/// Tells FanOutActor to distribute work.
#[derive(Clone)]
pub struct FanOut {
    pub count: usize,
    pub reply_to: ActorAddress,
}

/// Message used in the chain test -- carries remaining hops and final reply address.
#[derive(Clone)]
pub struct ChainMsg {
    pub remaining: usize,
    pub depth: usize,
    pub reply_to: ActorAddress,
}

// ── Actors ──────────────────────────────────────────────────────────────────

/// Replies Pong to every Ping. Stateless.
pub struct PingPongActor;

impl ActorInterface for PingPongActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Counts Increment messages, replies Count(n) after each.
pub struct CounterActor {
    pub count: usize,
}

impl ActorInterface for CounterActor {
    type Incoming = Increment;
    type Response = Count;
    fn handle(&mut self, ctx: &Ctx, msg: Increment) {
        self.count += 1;
        let _ = ctx.send(msg.reply_to, Count(self.count));
    }
}

/// Replies Done(value * 2).
pub struct DoubleActor;

impl ActorInterface for DoubleActor {
    type Incoming = Forward;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let _ = ctx.send(msg.reply_to, Done(msg.value * 2));
    }
}

/// Spawns a DoubleActor child and forwards the work to it.
pub struct DelegatorActor;

impl ActorInterface for DelegatorActor {
    type Incoming = Forward;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let child = ctx.spawn(DoubleActor).unwrap();
        let _ = ctx.send(
            child,
            Forward {
                value: msg.value,
                reply_to: msg.reply_to,
            },
        );
    }
}

/// Spawns a child chain: each level spawns the next until remaining == 0,
/// then the leaf replies Done(depth).
pub struct ChainActor;

impl ActorInterface for ChainActor {
    type Incoming = ChainMsg;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: ChainMsg) {
        if msg.remaining == 0 {
            let _ = ctx.send(msg.reply_to, Done(msg.depth));
        } else {
            let child = ctx.spawn(ChainActor).unwrap();
            let _ = ctx.send(
                child,
                ChainMsg {
                    remaining: msg.remaining - 1,
                    depth: msg.depth + 1,
                    reply_to: msg.reply_to,
                },
            );
        }
    }
}

/// Spawns N DoubleActor children, sends Forward { value: i, reply_to } to each.
pub struct FanOutActor;

impl ActorInterface for FanOutActor {
    type Incoming = FanOut;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FanOut) {
        for i in 1..=msg.count {
            let child = ctx.spawn(DoubleActor).unwrap();
            let _ = ctx.send(
                child,
                Forward {
                    value: i,
                    reply_to: msg.reply_to,
                },
            );
        }
    }
}

/// Replies with its own address.
pub struct SelfAddrActor;

impl ActorInterface for SelfAddrActor {
    type Incoming = WhoAreYou;
    type Response = MyAddr;
    fn handle(&mut self, ctx: &Ctx, msg: WhoAreYou) {
        let _ = ctx.send(msg.reply_to, MyAddr(ctx.self_addr()));
    }
}

/// Panics on every message. Used to test panic isolation.
pub struct PanicActor;

impl ActorInterface for PanicActor {
    type Incoming = PanicMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: PanicMsg) {
        panic!("intentional test panic");
    }
}

/// Increments a shared counter on each Ping. Used to observe processing from outside.
pub struct CountingPingActor {
    pub counter: Arc<AtomicUsize>,
}

impl ActorInterface for CountingPingActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.counter.fetch_add(1, Ordering::SeqCst);
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Null actor that accepts Ping but does nothing visible.
pub struct NullActor;

impl ActorInterface for NullActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

/// Actor that replies with its inbox address.
pub struct InboxReplyActor;

impl ActorInterface for InboxReplyActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Helper: build a Runtime handle + single-thread host with StdExtension installed.
/// Returns `(rt, host)`: use `rt` for spawn/send/inbox and `host` for ticking.
pub fn std_host(config: RuntimeConfig) -> (Runtime, SingleThreadRuntime) {
    let parts = RuntimeParts::new(config).with_extension(Arc::new(StdExtension::new()));
    let rt = parts.runtime().clone();
    let host = SingleThreadRuntime::new(parts);
    (rt, host)
}

/// Helper: build a Runtime handle + single-thread host with no extension.
pub fn plain_host(config: RuntimeConfig) -> (Runtime, SingleThreadRuntime) {
    let parts = RuntimeParts::new(config);
    let rt = parts.runtime().clone();
    let host = SingleThreadRuntime::new(parts);
    (rt, host)
}

/// Tick up to `max` times, returning as soon as `inbox` has a message.
pub fn tick_until_recv<M: swactor::actor::Message>(
    host: &mut SingleThreadRuntime,
    inbox: &Inbox<M>,
    max: usize,
) -> Option<M> {
    for _ in 0..max {
        host.try_tick();
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
    }
    None
}

/// Tick exactly `n` times (no inbox polling).
pub fn tick_n(host: &mut SingleThreadRuntime, n: usize) {
    for _ in 0..n {
        host.try_tick();
    }
}

/// Tick `n` times, then drain all messages from the inbox.
pub fn tick_and_drain<M: swactor::actor::Message>(
    host: &mut SingleThreadRuntime,
    inbox: &Inbox<M>,
    ticks: usize,
) -> Vec<M> {
    for _ in 0..ticks {
        host.try_tick();
    }
    std::iter::from_fn(|| inbox.try_recv()).collect()
}
