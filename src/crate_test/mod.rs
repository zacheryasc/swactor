//! Tests meant to be run against the crate-level API

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crate::actor::{ActorAddress, AnyActor, Ctx};
use crate::channel::Receiver;
use crate::config::RuntimeConfig;
use crate::delivery::{AddressMap, Envelope, InboxRegistry, Placement, TickContext, WorkerId};
use crate::stats::{MailboxSnapshot, WorkerStats};

use crate::worker::Worker;

// ── Actors ─────────────────────────────────────────────────────────

/// Counts how many u64 messages it successfully handled.
struct CounterActor(Arc<AtomicUsize>);

impl AnyActor for CounterActor {
    fn handle_any(&mut self, _ctx: &Ctx, msg: Box<dyn Any + Send>) {
        if msg.downcast::<u64>().is_ok() {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ── Harness ────────────────────────────────────────────────────────

fn addr(id: u8) -> ActorAddress {
    let mut bytes = [0u8; 32];
    bytes[0] = id;
    ActorAddress(bytes)
}

/// Self-contained single-worker test environment.
///
/// Holds the worker, its channels, shared state for TickContext, and
/// per-actor handle counters — everything needed to drive scenarios.
struct Env {
    worker: Worker,
    stats: Arc<WorkerStats>,
    feed_transfer: crate::channel::Sender<Envelope>,
    feed_spawn: crate::channel::Sender<(ActorAddress, Box<dyn AnyActor>)>,
    address_map: AddressMap,
    placement: Placement,
    inbox_registry: InboxRegistry,
    config: RuntimeConfig,
    tc_transfer_txs: Vec<crate::channel::Sender<Envelope>>,
    tc_spawn_txs: Vec<crate::channel::Sender<(ActorAddress, Box<dyn AnyActor>)>>,
    counters: HashMap<u8, Arc<AtomicUsize>>,
}

impl Env {
    fn new() -> Self {
        Self::with_config(RuntimeConfig::default())
    }

    fn with_config(config: RuntimeConfig) -> Self {
        let transfer_rx = Receiver::<Envelope>::new(256);
        let spawn_rx = Receiver::<(ActorAddress, Box<dyn AnyActor>)>::new(256);

        let feed_transfer = transfer_rx.new_sender();
        let feed_spawn = spawn_rx.new_sender();
        let tc_transfer = transfer_rx.new_sender();
        let tc_spawn = spawn_rx.new_sender();

        let stats = Arc::new(WorkerStats::new());
        let mbox_snap = Arc::new(std::sync::Mutex::new(MailboxSnapshot::new()));
        let worker = Worker::new(WorkerId(0), transfer_rx, spawn_rx, stats.clone(), mbox_snap);

        Self {
            worker,
            stats,
            feed_transfer,
            feed_spawn,
            address_map: AddressMap::new(),
            placement: Placement::new(1),
            inbox_registry: InboxRegistry::new(),
            config,
            tc_transfer_txs: vec![tc_transfer],
            tc_spawn_txs: vec![tc_spawn],
            counters: HashMap::new(),
        }
    }

    /// Enqueue an actor spawn (drained on next tick, phase 1).
    fn spawn(&mut self, id: u8) {
        let counter = Arc::new(AtomicUsize::new(0));
        let actor: Box<dyn AnyActor> = Box::new(CounterActor(counter.clone()));
        self.feed_spawn.try_send((addr(id), actor)).ok().unwrap();
        self.counters.insert(id, counter);
    }

    /// Enqueue a u64 message (drained on next tick, phase 2).
    fn send(&self, id: u8, val: u64) {
        self.feed_transfer
            .try_send(Envelope::new(addr(id), Box::new(val)))
            .ok()
            .unwrap();
    }

    /// Enqueue a wrong-typed message (String instead of u64).
    fn send_bad(&self, id: u8) {
        self.feed_transfer
            .try_send(Envelope::new(addr(id), Box::new("bad".to_string())))
            .ok()
            .unwrap();
    }

    /// Remove an actor from the pool (immediate, no tick needed).
    fn remove(&mut self, id: u8) {
        self.worker.pool.remove(&addr(id));
    }

    /// Run one tick of the worker loop.
    fn tick(&mut self) {
        let tc = TickContext {
            address_map: &self.address_map,
            transfer_txs: &self.tc_transfer_txs,
            spawn_txs: &self.tc_spawn_txs,
            placement: &self.placement,
            inbox_registry: &self.inbox_registry,
            config: &self.config,
        };
        self.worker.tick_once(&tc);
    }

    // ── Readouts ───────────────────────────────────────────────────

    fn handled(&self, id: u8) -> usize {
        self.counters[&id].load(Ordering::Relaxed)
    }

    fn pool_len(&self) -> usize {
        self.worker.pool.len()
    }

    fn depth(&self) -> usize {
        self.stats.total_mailbox_depth.load(Ordering::Relaxed)
    }

    fn processed(&self) -> u64 {
        self.stats.messages_processed.load(Ordering::Relaxed)
    }

    fn num_actors_stat(&self) -> usize {
        self.stats.num_actors.load(Ordering::Relaxed)
    }
}

// ── Step-driven runner ─────────────────────────────────────────────

enum Step {
    Spawn(u8),
    Send(u8, u64),
    SendBad(u8),
    Remove(u8),
    Tick,
    Expect { pool_len: usize, depth: usize, processed: u64 },
    ExpectHandled(u8, usize),
}

fn run(steps: &[Step]) {
    run_with(RuntimeConfig::default(), steps);
}

fn run_with(config: RuntimeConfig, steps: &[Step]) {
    let mut env = Env::with_config(config);
    for (i, step) in steps.iter().enumerate() {
        match step {
            Step::Spawn(id) => env.spawn(*id),
            Step::Send(id, val) => env.send(*id, *val),
            Step::SendBad(id) => env.send_bad(*id),
            Step::Remove(id) => env.remove(*id),
            Step::Tick => env.tick(),
            Step::Expect { pool_len, depth, processed } => {
                assert_eq!(env.pool_len(), *pool_len, "step {i}: pool_len");
                assert_eq!(env.depth(), *depth, "step {i}: depth");
                assert_eq!(env.processed(), *processed, "step {i}: processed");
            }
            Step::ExpectHandled(id, n) => {
                assert_eq!(env.handled(*id), *n, "step {i}: handled({})", id);
            }
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[test]
fn spawn_send_process() {
    run(&[
        Step::Spawn(1),
        Step::Spawn(2),
        Step::Tick,
        Step::Expect { pool_len: 2, depth: 0, processed: 0 },

        Step::Send(1, 10),
        Step::Send(1, 20),
        Step::Send(2, 30),
        Step::Tick,
        Step::Expect { pool_len: 2, depth: 0, processed: 3 },
        Step::ExpectHandled(1, 2),
        Step::ExpectHandled(2, 1),
    ]);
}

#[test]
fn remove_drops_future_messages() {
    run(&[
        Step::Spawn(1),
        Step::Spawn(2),
        Step::Tick,

        // Remove actor 1 directly from pool
        Step::Remove(1),
        Step::Expect { pool_len: 1, depth: 0, processed: 0 },

        // Messages to actor 1 are drained from the transfer queue
        // but pool.deliver finds no slot — silently dropped
        Step::Send(1, 42),
        Step::Send(2, 99),
        Step::Tick,

        Step::Expect { pool_len: 1, depth: 0, processed: 1 },
        Step::ExpectHandled(1, 0),
        Step::ExpectHandled(2, 1),
    ]);
}

#[test]
fn wrong_type_silently_dropped() {
    run(&[
        Step::Spawn(1),
        Step::Tick,

        // Mix correct (u64) and incorrect (String) types
        Step::Send(1, 1),
        Step::SendBad(1),
        Step::Send(1, 2),
        Step::SendBad(1),
        Step::SendBad(1),
        Step::Send(1, 3),
        Step::Tick,

        // All 6 popped from mailbox ("processed" by the pool),
        // but only the 3 u64 messages were handled by the actor
        Step::Expect { pool_len: 1, depth: 0, processed: 6 },
        Step::ExpectHandled(1, 3),
    ]);
}

#[test]
fn all_messages_drain_in_one_tick() {
    run(&[
        Step::Spawn(1),
        Step::Tick,

        // Send 10 messages
        Step::Send(1, 0), Step::Send(1, 1), Step::Send(1, 2), Step::Send(1, 3),
        Step::Send(1, 4), Step::Send(1, 5), Step::Send(1, 6), Step::Send(1, 7),
        Step::Send(1, 8), Step::Send(1, 9),

        // All 10 processed in a single tick
        Step::Tick,
        Step::Expect { pool_len: 1, depth: 0, processed: 10 },
        Step::ExpectHandled(1, 10),
    ]);
}

#[test]
fn spawn_and_send_same_tick() {
    // Spawn is phase 1, transfer is phase 2, processing is phase 3.
    // All three happen within a single tick_once call.
    run(&[
        Step::Spawn(1),
        Step::Send(1, 42),
        Step::Tick,
        Step::Expect { pool_len: 1, depth: 0, processed: 1 },
        Step::ExpectHandled(1, 1),
    ]);
}

#[test]
fn stats_track_pool_mutations() {
    let mut env = Env::new();

    // Before any tick, stats are zeroed
    assert_eq!(env.num_actors_stat(), 0);
    assert_eq!(env.depth(), 0);
    assert_eq!(env.processed(), 0);

    // Spawn 3 + tick → stats reflect 3 actors
    env.spawn(1);
    env.spawn(2);
    env.spawn(3);
    env.tick();
    assert_eq!(env.num_actors_stat(), 3);

    // Send 5 to actor 1 + tick → processed increases
    for i in 0..5 {
        env.send(1, i);
    }
    env.tick();
    assert_eq!(env.processed(), 5);
    assert_eq!(env.depth(), 0);
    assert_eq!(env.num_actors_stat(), 3);

    // Remove actor 2 + tick → stats update
    env.remove(2);
    env.tick();
    assert_eq!(env.num_actors_stat(), 2);
    assert_eq!(env.pool_len(), 2);
}

/// A long mixed-action sequence: spawns, sends, removes, wrong types,
/// and backpressure — all in one run.
#[test]
fn interleaved_lifecycle() {
    run(&[
        // ── Phase 1: build the pool ────────────────────────────────
        Step::Spawn(1),
        Step::Spawn(2),
        Step::Spawn(3),
        Step::Tick,
        Step::Expect { pool_len: 3, depth: 0, processed: 0 },

        // ── Phase 2: normal message flow ───────────────────────────
        Step::Send(1, 100),
        Step::Send(2, 200),
        Step::Send(3, 300),
        Step::Tick,
        Step::ExpectHandled(1, 1),
        Step::ExpectHandled(2, 1),
        Step::ExpectHandled(3, 1),
        Step::Expect { pool_len: 3, depth: 0, processed: 3 },

        // ── Phase 3: remove actor 2, send to all 3 ────────────────
        Step::Remove(2),
        Step::Send(1, 101),
        Step::Send(2, 201), // actor 2 gone — dropped at deliver
        Step::Send(3, 301),
        Step::Tick,
        Step::Expect { pool_len: 2, depth: 0, processed: 5 },
        Step::ExpectHandled(1, 2),
        Step::ExpectHandled(2, 1), // unchanged since removal
        Step::ExpectHandled(3, 2),

        // ── Phase 4: late spawn + immediate send ───────────────────
        Step::Spawn(4),
        Step::Send(4, 400),
        Step::Tick,
        Step::Expect { pool_len: 3, depth: 0, processed: 6 },
        Step::ExpectHandled(4, 1),

        // ── Phase 5: bad types mixed with good ─────────────────────
        Step::SendBad(1),
        Step::SendBad(1),
        Step::SendBad(1),
        Step::Send(1, 999),
        Step::Tick,
        // 4 popped (3 bad + 1 good), only 1 handled by actor
        Step::Expect { pool_len: 3, depth: 0, processed: 10 },
        Step::ExpectHandled(1, 3), // 2 from prior phases + 1 good

        // ── Phase 6: remove all, send to ghosts ────────────────────
        Step::Remove(1),
        Step::Remove(3),
        Step::Remove(4),
        Step::Expect { pool_len: 0, depth: 0, processed: 10 },
        Step::Send(1, 0),
        Step::Send(3, 0),
        Step::Tick,
        Step::Expect { pool_len: 0, depth: 0, processed: 10 },
    ]);
}

#[test]
fn run_loop_stops_on_shutdown() {
    let transfer_rx = Receiver::<Envelope>::new(64);
    let spawn_rx = Receiver::<(ActorAddress, Box<dyn AnyActor>)>::new(64);
    let transfer_tx = transfer_rx.new_sender();
    let spawn_tx = spawn_rx.new_sender();

    let stats = Arc::new(WorkerStats::new());
    let mbox_snap = Arc::new(std::sync::Mutex::new(MailboxSnapshot::new()));
    let mut worker = Worker::new(WorkerId(0), transfer_rx, spawn_rx, stats, mbox_snap);

    let is_running = AtomicBool::new(false);
    let address_map = AddressMap::new();
    let placement = Placement::new(1);
    let inbox_registry = InboxRegistry::new();
    let config = RuntimeConfig::default();

    let tc = TickContext {
        address_map: &address_map,
        transfer_txs: &[transfer_tx],
        spawn_txs: &[spawn_tx],
        placement: &placement,
        inbox_registry: &inbox_registry,
        config: &config,
    };

    thread::scope(|s| {
        s.spawn(|| worker.run(&tc, &is_running));
    });
}
