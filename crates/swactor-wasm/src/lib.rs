use wasm_bindgen::prelude::*;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Inbox, Runtime, RuntimeConfig};

// ---------------------------------------------------------------------------
// Actors (private — only exposed through the wasm API)
// ---------------------------------------------------------------------------

struct Counter {
    total: u32,
    report_to: ActorAddress,
}

impl ActorInterface for Counter {
    type Incoming = u32;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: u32) {
        self.total += msg;
        let _ = ctx.send(self.report_to, self.total);
    }
}

struct Relay {
    target: ActorAddress,
}

impl ActorInterface for Relay {
    type Incoming = u32;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: u32) {
        let _ = ctx.send(self.target, msg);
    }
}

// ---------------------------------------------------------------------------
// JS-facing runtime wrapper
// ---------------------------------------------------------------------------

#[wasm_bindgen]
pub struct SwactorRuntime {
    rt: Runtime,
    inbox: Inbox<u32>,
    actors: Vec<ActorAddress>,
}

#[wasm_bindgen]
impl SwactorRuntime {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let rt = Runtime::new(RuntimeConfig {
            num_threads: 1,
            ..RuntimeConfig::default()
        });
        let inbox = rt.new_inbox().unwrap();
        Self {
            rt,
            inbox,
            actors: Vec::new(),
        }
    }

    /// Spawn a counter actor. Returns its index (used with `send`).
    pub fn spawn_counter(&mut self) -> usize {
        let addr = self
            .rt
            .spawn(Counter {
                total: 0,
                report_to: *self.inbox.addr(),
            })
            .expect("spawn counter");
        let idx = self.actors.len();
        self.actors.push(addr);
        idx
    }

    /// Spawn a relay that forwards every message to `target_idx`.
    pub fn spawn_relay(&mut self, target_idx: usize) -> usize {
        let target = self.actors[target_idx];
        let addr = self
            .rt
            .spawn(Relay { target })
            .expect("spawn relay");
        let idx = self.actors.len();
        self.actors.push(addr);
        idx
    }

    /// Send a u32 to the actor at `actor_idx`.
    pub fn send(&self, actor_idx: usize, value: u32) -> bool {
        if actor_idx >= self.actors.len() {
            return false;
        }
        self.rt.send_to(self.actors[actor_idx], value).is_ok()
    }

    /// Drive one tick of the single-threaded runtime.
    pub fn tick(&self) {
        self.rt.tick();
    }

    /// Try to read the next result from the inbox. Returns `undefined` when empty.
    pub fn try_recv(&self) -> Option<u32> {
        self.inbox.try_recv()
    }

    /// Number of actors the runtime knows about.
    pub fn actor_count(&self) -> usize {
        self.rt.stats().actors.len()
    }
}
