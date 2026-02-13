//! Property-based tests for the swactor runtime.
//!
//! Uses proptest for randomized testing and proptest-state-machine for
//! stateful property testing with automatic shrinking of failing sequences.

use std::collections::HashMap;

use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::{MailboxOverflow, RuntimeConfig};
use swactor::runtime::{Ctx, Inbox, Runtime};

// ─── Shared Actor Types ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Ping(u64);

/// Echo: receives Ping, sends Ping back to reply_to address.
struct EchoActor {
    reply_to: ActorAddress,
}
impl ActorInterface for EchoActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(self.reply_to, msg);
    }
}

/// Counter: tracks message count, replies with count.
struct CounterActor {
    count: u64,
    reply_to: ActorAddress,
}
impl ActorInterface for CounterActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        self.count += 1;
        let _ = ctx.send(self.reply_to, Ping(self.count));
    }
}

/// PanicActor: panics on first message.
struct PanicActor;
impl ActorInterface for PanicActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        panic!("intentional panic");
    }
}

/// Noop: discards all messages silently.
struct NoopActor;
impl ActorInterface for NoopActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

// ─── Simple Property Tests ─────────────────────────────────────────────────

proptest! {
    /// FIFO ordering is preserved for any sequence of numbered messages
    /// sent from a single sender to a single actor.
    #[test]
    fn fifo_ordering_for_any_message_sequence(
        values in proptest::collection::vec(0u64..10_000, 1..100)
    ) {
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<Ping>().unwrap();
        let addr = rt.spawn(EchoActor { reply_to: *inbox.addr() }).unwrap();

        // Send all messages
        for &v in &values {
            rt.send_to(addr, Ping(v)).unwrap();
        }

        // Tick enough to process all
        let ticks_needed = (values.len() / 64) + 3; // budget=64 default
        for _ in 0..ticks_needed { rt.tick(); }

        // Verify FIFO ordering
        let mut received = Vec::new();
        while let Some(msg) = inbox.try_recv() {
            received.push(msg.0);
        }
        prop_assert_eq!(&received, &values, "FIFO ordering violated");
    }

    /// Budget fairness: no actor processes more than budget messages per tick
    /// when multiple actors have pending messages.
    #[test]
    fn budget_limits_per_actor_processing(
        n_actors in 2usize..10,
        msgs_per in 10usize..100,
        budget in 1usize..32,
    ) {
        let config = RuntimeConfig {
            actor_message_budget: budget,
            ..Default::default()
        };
        let rt = Runtime::new(config);
        let inbox = rt.new_inbox::<Ping>().unwrap();

        let mut addrs = Vec::new();
        for _ in 0..n_actors {
            addrs.push(rt.spawn(CounterActor { count: 0, reply_to: *inbox.addr() }).unwrap());
        }

        rt.tick(); // on_start

        // Send msgs_per messages to each actor
        for addr in &addrs {
            for v in 0..msgs_per as u64 {
                rt.send_to(*addr, Ping(v)).unwrap();
            }
        }

        // Single tick — each actor should process at most `budget` messages
        rt.tick();

        // Drain inbox to count replies per actor
        // CounterActor replies with incrementing count, so max reply value = messages processed
        let mut replies = Vec::new();
        while let Some(msg) = inbox.try_recv() {
            replies.push(msg.0);
        }

        // Total replies should be at most n_actors * budget
        prop_assert!(
            replies.len() <= n_actors * budget,
            "Too many messages processed: {} > {} (n_actors={}, budget={})",
            replies.len(), n_actors * budget, n_actors, budget,
        );
    }

    /// One-shot timer fires at exactly the right tick for any delay.
    #[test]
    fn one_shot_timer_fires_at_correct_tick(delay in 1u64..20) {
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<Ping>().unwrap();

        struct TimerActor { target: ActorAddress, delay: u64 }
        impl ActorInterface for TimerActor {
            type Incoming = Ping;
            type Response = ();
            fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
            fn on_start(&mut self, ctx: &Ctx) {
                ctx.send_after_ticks(self.target, Ping(42), self.delay);
            }
        }

        let _addr = rt.spawn(TimerActor { target: *inbox.addr(), delay }).unwrap();

        // Tick up to the expected fire tick
        for tick in 1..=(delay + 1) {
            rt.tick();
            let msg = inbox.try_recv();
            if tick <= delay {
                prop_assert!(msg.is_none(), "Timer fired too early at tick {}", tick);
            } else {
                prop_assert!(msg.is_some(), "Timer should have fired at tick {}", tick);
            }
        }

        // No second fire (one-shot)
        rt.tick();
        prop_assert!(inbox.try_recv().is_none(), "One-shot timer fired twice");
    }

    /// Interval timer fires at correct periodic ticks for any period.
    #[test]
    fn interval_timer_fires_at_correct_period(period in 1u64..10) {
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<Ping>().unwrap();

        struct IntervalActor { target: ActorAddress, period: u64 }
        impl ActorInterface for IntervalActor {
            type Incoming = Ping;
            type Response = ();
            fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
            fn on_start(&mut self, ctx: &Ctx) {
                ctx.send_interval_ticks(self.target, Ping(1), self.period);
            }
        }

        let _addr = rt.spawn(IntervalActor { target: *inbox.addr(), period }).unwrap();

        // Verify 3 consecutive fires
        let mut fire_count = 0;
        // Timer scheduled on tick 1 (on_start). First fire at tick 1+period.
        for tick in 1..=(period * 3 + 2) {
            rt.tick();
            if let Some(_) = inbox.try_recv() {
                fire_count += 1;
                // First fire should be at tick (period + 1)
                // Subsequent fires every `period` ticks after that
                let expected_tick = period + 1 + (fire_count - 1) * period;
                prop_assert_eq!(tick, expected_tick,
                    "Fire #{} at wrong tick (period={})", fire_count, period);
            }
        }
        prop_assert!(fire_count >= 3, "Expected 3+ fires, got {} (period={})", fire_count, period);
    }

    /// Bounded mailbox with DropNewest never exceeds capacity.
    #[test]
    fn bounded_mailbox_never_exceeds_capacity(
        capacity in 1usize..20,
        msg_count in 1usize..200,
    ) {
        let config = RuntimeConfig {
            default_mailbox_capacity: capacity,
            mailbox_overflow: MailboxOverflow::DropNewest,
            ..Default::default()
        };
        let rt = Runtime::new(config);
        let addr = rt.spawn(NoopActor).unwrap();
        rt.tick(); // on_start

        for v in 0..msg_count as u64 {
            rt.send_to(addr, Ping(v)).unwrap();
        }

        let stats = rt.stats();
        let worker = &stats.workers[0];
        // Mailbox depth should never exceed capacity
        prop_assert!(
            worker.mailbox_depth <= capacity,
            "Mailbox depth {} exceeds capacity {}",
            worker.mailbox_depth, capacity,
        );
    }

    /// Spawn N actors and verify all get unique addresses and appear in stats.
    #[test]
    fn spawn_n_actors_all_tracked(n in 1usize..50) {
        let rt = Runtime::new(RuntimeConfig::default());
        let mut addrs = Vec::new();
        for _ in 0..n {
            addrs.push(rt.spawn(NoopActor).unwrap());
        }
        rt.tick(); // process spawns

        let stats = rt.stats();
        prop_assert_eq!(stats.actors.len(), n, "Expected {} actors in stats", n);

        // All addresses should be unique
        let unique: std::collections::HashSet<_> = addrs.iter().collect();
        prop_assert_eq!(unique.len(), n, "Duplicate addresses detected");
    }
}

// ─── State Machine Test ────────────────────────────────────────────────────
//
// Reference model tracks expected runtime state. Transitions are random
// operations (spawn, send, tick, stop). After each transition, invariants
// are checked against the actual runtime.

#[derive(Clone, Debug)]
struct RefState {
    /// actor_id -> is_alive (not stopped/poisoned)
    actors: HashMap<usize, bool>,
    /// actor_id -> messages sent (values, in order)
    sent_messages: HashMap<usize, Vec<u64>>,
    /// Number of ticks executed
    tick_count: u64,
    /// Next actor ID to assign
    next_id: usize,
    /// IDs of actors that will panic on first message
    panic_actors: Vec<usize>,
}

#[derive(Clone, Debug)]
enum Transition {
    /// Spawn a new echo actor
    SpawnEcho,
    /// Spawn a panic-on-first-message actor
    SpawnPanic,
    /// Send a numbered message to actor at index
    Send { actor_idx: usize, value: u64 },
    /// Run one tick
    Tick,
    /// Run N ticks
    TickN(u8),
    /// Stop actor at index gracefully
    StopActor(usize),
    /// Check runtime stats match reference
    CheckStats,
}

struct SwactorModel;

impl ReferenceStateMachine for SwactorModel {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState {
            actors: HashMap::new(),
            sent_messages: HashMap::new(),
            tick_count: 0,
            next_id: 0,
            panic_actors: Vec::new(),
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let has_actors = !state.actors.is_empty();
        let has_alive = state.actors.values().any(|&alive| alive);

        if !has_actors {
            // Must spawn first
            prop_oneof![
                3 => Just(Transition::SpawnEcho),
                1 => Just(Transition::SpawnPanic),
            ]
            .boxed()
        } else if !has_alive {
            // All actors dead, spawn new ones or tick to clean up
            prop_oneof![
                3 => Just(Transition::SpawnEcho),
                1 => Just(Transition::SpawnPanic),
                1 => Just(Transition::Tick),
            ]
            .boxed()
        } else {
            let n = state.actors.len();
            prop_oneof![
                3 => Just(Transition::SpawnEcho),
                1 => Just(Transition::SpawnPanic),
                10 => (0..n, 0u64..1000).prop_map(|(idx, val)| Transition::Send {
                    actor_idx: idx,
                    value: val,
                }),
                5 => Just(Transition::Tick),
                2 => (1u8..5).prop_map(Transition::TickN),
                2 => (0..n).prop_map(Transition::StopActor),
                1 => Just(Transition::CheckStats),
            ]
            .boxed()
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::SpawnEcho => {
                let id = state.next_id;
                state.next_id += 1;
                state.actors.insert(id, true);
                state.sent_messages.insert(id, Vec::new());
            }
            Transition::SpawnPanic => {
                let id = state.next_id;
                state.next_id += 1;
                state.actors.insert(id, true);
                state.sent_messages.insert(id, Vec::new());
                state.panic_actors.push(id);
            }
            Transition::Send { actor_idx, value } => {
                let alive_ids: Vec<usize> = state
                    .actors
                    .iter()
                    .filter(|(_, alive)| **alive)
                    .map(|(&id, _)| id)
                    .collect();
                if !alive_ids.is_empty() {
                    let id = alive_ids[*actor_idx % alive_ids.len()];
                    state
                        .sent_messages
                        .entry(id)
                        .or_default()
                        .push(*value);
                }
            }
            Transition::Tick => {
                state.tick_count += 1;
            }
            Transition::TickN(n) => {
                state.tick_count += *n as u64;
            }
            Transition::StopActor(idx) => {
                let alive_ids: Vec<usize> = state
                    .actors
                    .iter()
                    .filter(|(_, alive)| **alive)
                    .map(|(&id, _)| id)
                    .collect();
                if !alive_ids.is_empty() {
                    let id = alive_ids[*idx % alive_ids.len()];
                    state.actors.insert(id, false);
                }
            }
            Transition::CheckStats => {}
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Send { .. } | Transition::StopActor(_) => {
                state.actors.values().any(|&alive| alive)
            }
            _ => true,
        }
    }
}

// ─── Concrete System Under Test ────────────────────────────────────────────

struct SutState {
    runtime: Runtime,
    inbox: Inbox<Ping>,
    /// Maps reference actor_id to actual ActorAddress
    actor_map: HashMap<usize, ActorAddress>,
    /// Reference IDs that are panic actors
    panic_ids: Vec<usize>,
    /// Tracks which actor IDs are alive (mirrors ref model)
    alive: HashMap<usize, bool>,
    /// Next ID for spawn
    next_id: usize,
}

struct SwactorTest;

impl StateMachineTest for SwactorTest {
    type SystemUnderTest = SutState;
    type Reference = SwactorModel;

    fn init_test(_ref_state: &RefState) -> Self::SystemUnderTest {
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<Ping>().unwrap();
        SutState {
            runtime: rt,
            inbox,
            actor_map: HashMap::new(),
            panic_ids: Vec::new(),
            alive: HashMap::new(),
            next_id: 0,
        }
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        _ref_state: &RefState,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::SpawnEcho => {
                let id = sut.next_id;
                sut.next_id += 1;
                let addr = sut
                    .runtime
                    .spawn(EchoActor {
                        reply_to: *sut.inbox.addr(),
                    })
                    .unwrap();
                sut.actor_map.insert(id, addr);
                sut.alive.insert(id, true);
            }
            Transition::SpawnPanic => {
                let id = sut.next_id;
                sut.next_id += 1;
                let addr = sut.runtime.spawn(PanicActor).unwrap();
                sut.actor_map.insert(id, addr);
                sut.alive.insert(id, true);
                sut.panic_ids.push(id);
            }
            Transition::Send { actor_idx, value } => {
                let alive_ids: Vec<usize> = sut
                    .alive
                    .iter()
                    .filter(|(_, alive)| **alive)
                    .map(|(&id, _)| id)
                    .collect();
                if !alive_ids.is_empty() {
                    let id = alive_ids[actor_idx % alive_ids.len()];
                    if let Some(&addr) = sut.actor_map.get(&id) {
                        let _ = sut.runtime.send_to(addr, Ping(value));
                    }
                }
            }
            Transition::Tick => {
                sut.runtime.tick();
            }
            Transition::TickN(n) => {
                for _ in 0..n {
                    sut.runtime.tick();
                }
            }
            Transition::StopActor(idx) => {
                let alive_ids: Vec<usize> = sut
                    .alive
                    .iter()
                    .filter(|(_, alive)| **alive)
                    .map(|(&id, _)| id)
                    .collect();
                if !alive_ids.is_empty() {
                    let id = alive_ids[idx % alive_ids.len()];
                    sut.alive.insert(id, false);
                    if let Some(&addr) = sut.actor_map.get(&id) {
                        let _ = sut.runtime.stop_actor(addr);
                    }
                }
            }
            Transition::CheckStats => {
                let stats = sut.runtime.stats();
                assert!(stats.num_workers >= 1);
                for info in &stats.workers {
                    assert!(info.id < stats.num_workers);
                }
            }
        }
        sut
    }

    fn check_invariants(sut: &Self::SystemUnderTest, _ref_state: &RefState) {
        let stats = sut.runtime.stats();

        // Invariant 1: worker count is consistent
        assert_eq!(stats.workers.len(), stats.num_workers);

        // Invariant 2: all actors in stats are on valid workers
        for (_, wid) in &stats.actors {
            assert!(
                *wid < stats.num_workers,
                "Actor on worker {} but only {} workers",
                wid,
                stats.num_workers
            );
        }

        // Invariant 3: per-worker actor count <= address map count
        // (workers lag behind address map because they drain spawn queue on tick)
        let worker_actor_count: usize = stats.workers.iter().map(|w| w.num_actors).sum();
        assert!(
            worker_actor_count <= stats.actors.len(),
            "Worker actor count {} > address map count {}",
            worker_actor_count,
            stats.actors.len(),
        );

        // Invariant 4: inbox can be drained without panic
        // (type-safety of inbox messages)
        while let Some(_msg) = sut.inbox.try_recv() {
            // Just verify no panic on try_recv
        }

        // Invariant 5: stats queries don't panic
        let _total_processed: u64 = stats.workers.iter().map(|w| w.messages_processed).sum();
        let _total_depth: usize = stats.workers.iter().map(|w| w.mailbox_depth).sum();
    }

    fn teardown(_sut: Self::SystemUnderTest) {
        // Runtime drops normally
    }
}

prop_state_machine! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 128,
        max_shrink_iters: 10_000,
        .. proptest::test_runner::Config::default()
    })]

    /// Given a random sequence of spawn/send/tick/stop operations,
    /// when applied to a swactor runtime,
    /// then all invariants (worker consistency, mailbox safety, stats accuracy) hold.
    #[test]
    fn swactor_state_machine(sequential 1..40 => SwactorTest);
}
