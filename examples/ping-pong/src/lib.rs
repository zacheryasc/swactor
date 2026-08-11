//! swactor · ping-pong -- a minimal WebAssembly actor demo.
//!
//! Two actors, `Ping` and `Pong`, volley a ball back and forth on a
//! single-threaded actor runtime compiled to wasm. The host (Node.js) advances
//! the runtime one step at a time with `tick()` and drains a shared log inbox
//! to print each volley.
//!
//! When the volley cap is reached the hitter stops; the other actor, which is
//! *watching* it, observes the death, posts a final summary, and stops too.
//! This shows the three load-bearing swactor ideas in one place: spawning
//! actors, message passing, and death monitoring.
//!
//! Build & run from this directory:  `./run.sh`

use std::sync::Arc;

use wasm_bindgen::prelude::*;

use swactor::actor::{ActorAddress, ActorExited, ActorInterface};
use swactor::runtime::{
    Ctx, Inbox, Runtime as SwactorRuntime, RuntimeConfig, RuntimeParts, SingleThreadRuntime,
};
use swactor::std::{CtxWatching, StdExtension};

// ─── Host-facing bindings ───────────────────────────────────────────────────

/// Opaque actor-address handle, passed between spawn calls and the host.
#[wasm_bindgen]
#[derive(Clone)]
pub struct Addr(ActorAddress);

/// Inbox the host polls each tick for log lines and the final summary.
#[wasm_bindgen]
pub struct LogInbox {
    inner: Inbox<String>,
}

#[wasm_bindgen]
impl LogInbox {
    /// The address actors send their log lines to.
    pub fn addr(&self) -> Addr {
        Addr(*self.inner.addr())
    }

    /// Pop the next log line, or `undefined` when empty.
    pub fn try_recv(&self) -> Option<String> {
        self.inner.try_recv()
    }
}

/// The ping-pong app: a single-threaded swactor runtime with the std extension
/// (watching) installed. The host drives it by calling [`App::tick`].
#[wasm_bindgen]
pub struct App {
    rt: SwactorRuntime,
    host: SingleThreadRuntime,
}

#[wasm_bindgen]
impl App {
    #[wasm_bindgen(constructor)]
    pub fn new() -> App {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        })
        .with_extension(Arc::new(StdExtension::new()));
        let rt = parts.runtime().clone();
        let host = SingleThreadRuntime::new(parts);
        App { rt, host }
    }

    /// Advance the runtime one tick.
    pub fn tick(&mut self) {
        self.host.tick();
    }

    /// Actors currently alive.
    pub fn actor_count(&self) -> usize {
        self.rt.stats().actors.len()
    }

    /// Total messages processed across all workers.
    pub fn total_messages(&self) -> f64 {
        self.rt
            .stats()
            .workers
            .iter()
            .map(|w| w.messages_processed)
            .sum::<u64>() as f64
    }

    /// Create a log inbox the host drains each tick.
    pub fn new_log(&self) -> LogInbox {
        LogInbox {
            inner: self.rt.new_inbox().expect("new_log"),
        }
    }

    /// Spawn the `Pong` actor. Returns its address.
    pub fn spawn_pong(&self, log: &Addr, max_volleys: u32) -> Addr {
        let addr = self
            .rt
            .spawn(Pong {
                log: log.0,
                max: max_volleys,
            })
            .expect("spawn pong");
        Addr(addr)
    }

    /// Spawn the `Ping` actor, pointed at an existing `Pong`. Returns its address.
    pub fn spawn_ping(&self, pong: &Addr, log: &Addr, max_volleys: u32) -> Addr {
        let addr = self
            .rt
            .spawn(Ping {
                pong: pong.0,
                log: log.0,
                max: max_volleys,
            })
            .expect("spawn ping");
        Addr(addr)
    }
}

// ─── The ball ───────────────────────────────────────────────────────────────

/// A ball in flight between the two actors.
///
/// `volleys` is the running hit count -- each hitter increments it. `from` is
/// the address the ball came from (and should be returned to). `Ping` knows
/// `Pong` from spawn time so only `Pong` reads `from`, but both set it so the
/// protocol reads symmetrically.
#[derive(Clone)]
struct Ball {
    volleys: u32,
    from: ActorAddress,
}

// ─── Ping ───────────────────────────────────────────────────────────────────

struct Ping {
    pong: ActorAddress,
    log: ActorAddress,
    max: u32,
}

impl Ping {
    /// Hit the ball as volley number `v`: log it, then either return it to Pong
    /// or, if the cap is reached, stop.
    fn volley(&self, ctx: &Ctx, v: u32) {
        let _ = ctx.send(self.log, format!("ping  | volley {:>2}/{}", v, self.max));
        if v < self.max {
            let _ = ctx.send(
                self.pong,
                Ball {
                    volleys: v,
                    from: ctx.self_addr(),
                },
            );
        } else {
            let _ = ctx.send(self.log, "ping  | cap reached, stopping".to_string());
            ctx.stop_self();
        }
    }
}

impl ActorInterface for Ping {
    type Incoming = Ball;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        // Ping knows Pong from spawn time, so it can watch it immediately.
        ctx.watch(self.pong);
        self.volley(ctx, 1); // serve
    }

    fn handle(&mut self, ctx: &Ctx, ball: Ball) {
        // Pong returned the ball; this is our next hit.
        self.volley(ctx, ball.volleys + 1);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        let _ = ctx.send(
            self.log,
            format!(
                "done  | rally complete -- {} volleys played (pong exited: {:?})",
                self.max, exited.reason
            ),
        );
        ctx.stop_self();
    }
}

// ─── Pong ───────────────────────────────────────────────────────────────────

struct Pong {
    log: ActorAddress,
    max: u32,
}

impl ActorInterface for Pong {
    type Incoming = Ball;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, ball: Ball) {
        // Watch whoever served this ball. Idempotent across volleys, so this is
        // also how Pong (spawned before Ping) first learns Ping's address.
        ctx.watch(ball.from);

        let v = ball.volleys + 1;
        let _ = ctx.send(self.log, format!("pong  | volley {:>2}/{}", v, self.max));
        if v < self.max {
            let _ = ctx.send(
                ball.from,
                Ball {
                    volleys: v,
                    from: ctx.self_addr(),
                },
            );
        } else {
            let _ = ctx.send(self.log, "pong  | cap reached, stopping".to_string());
            ctx.stop_self();
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        let _ = ctx.send(
            self.log,
            format!(
                "done  | rally complete -- {} volleys played (ping exited: {:?})",
                self.max, exited.reason
            ),
        );
        ctx.stop_self();
    }
}
