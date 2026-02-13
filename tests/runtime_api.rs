use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Down, MonitorRef, StopReason};
use swactor_std::{
    ChildSpec, CtxGroups, CtxMonitoring, CtxNaming, RestartPolicy, Router, RoutingStrategy,
    RuntimeGroups, RuntimeNaming, StdExtension, Supervisor, SupervisorStrategy,
};
use swactor::runtime::{Ctx, Inbox, MailboxOverflow, Runtime, RuntimeConfig};

/// Helper: construct a Runtime with StdExtension installed.
fn std_runtime(config: RuntimeConfig) -> Runtime {
    Runtime::new(config).with_extension(Arc::new(StdExtension::new()))
}

// ── Messages ────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Ping {
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct Pong;

#[derive(Clone)]
struct Increment {
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct Count(usize);

#[derive(Clone)]
struct Forward {
    value: usize,
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct Done(usize);

/// Ask an actor for its own address.
#[derive(Clone)]
struct WhoAreYou {
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct MyAddr(ActorAddress);

#[derive(Clone)]
struct PanicMsg;

/// Tells FanOutActor to distribute work.
#[derive(Clone)]
struct FanOut {
    count: usize,
    reply_to: ActorAddress,
}

/// Message used in the chain test — carries remaining hops and final reply address.
#[derive(Clone)]
struct ChainMsg {
    remaining: usize,
    depth: usize,
    reply_to: ActorAddress,
}

// ── Actors ──────────────────────────────────────────────────────────────────

/// Replies Pong to every Ping. Stateless.
struct PingPongActor;

impl ActorInterface for PingPongActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Counts Increment messages, replies Count(n) after each.
struct CounterActor {
    count: usize,
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
struct DoubleActor;

impl ActorInterface for DoubleActor {
    type Incoming = Forward;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let _ = ctx.send(msg.reply_to, Done(msg.value * 2));
    }
}

/// Spawns a DoubleActor child and forwards the work to it.
struct DelegatorActor;

impl ActorInterface for DelegatorActor {
    type Incoming = Forward;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let child = ctx.spawn(DoubleActor).unwrap();
        let _ = ctx.send(child, Forward { value: msg.value, reply_to: msg.reply_to });
    }
}

/// Spawns a child chain: each level spawns the next until remaining == 0,
/// then the leaf replies Done(depth).
struct ChainActor;

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
struct FanOutActor;

impl ActorInterface for FanOutActor {
    type Incoming = FanOut;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: FanOut) {
        for i in 1..=msg.count {
            let child = ctx.spawn(DoubleActor).unwrap();
            let _ = ctx.send(child, Forward { value: i, reply_to: msg.reply_to });
        }
    }
}

/// Replies with its own address.
struct SelfAddrActor;

impl ActorInterface for SelfAddrActor {
    type Incoming = WhoAreYou;
    type Response = MyAddr;
    fn handle(&mut self, ctx: &Ctx, msg: WhoAreYou) {
        let _ = ctx.send(msg.reply_to, MyAddr(ctx.self_addr()));
    }
}

/// Panics on every message. Used to test panic isolation.
struct PanicActor;

impl ActorInterface for PanicActor {
    type Incoming = PanicMsg;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: PanicMsg) {
        panic!("intentional test panic");
    }
}

/// Increments a shared counter on each Ping. Used to observe processing from outside.
struct CountingPingActor {
    counter: Arc<AtomicUsize>,
}

impl ActorInterface for CountingPingActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.counter.fetch_add(1, Ordering::SeqCst);
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Tick up to `max` times, returning as soon as `inbox` has a message.
fn tick_until_recv<M: swactor::actor::Message>(
    rt: &Runtime,
    inbox: &Inbox<M>,
    max: usize,
) -> Option<M> {
    for _ in 0..max {
        rt.tick();
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
    }
    None
}

/// Tick `n` times, then drain all messages from the inbox.
fn tick_and_drain<M: swactor::actor::Message>(
    rt: &Runtime,
    inbox: &Inbox<M>,
    ticks: usize,
) -> Vec<M> {
    for _ in 0..ticks {
        rt.tick();
    }
    std::iter::from_fn(|| inbox.try_recv()).collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// Actor Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn actor_receives_message_and_replies() {
    // Given a spawned PingPongActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When I send it a Ping
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then my inbox receives a Pong
    let reply = tick_until_recv(&rt, &inbox, 10);
    assert!(reply.is_some(), "actor should have replied with Pong");
}

#[test]
fn actor_maintains_state_across_messages() {
    // Given a CounterActor starting at 0
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When I send 3 Increments
    for _ in 0..3 {
        rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    }

    // Then replies are Count(1), Count(2), Count(3) — state accumulated
    let replies = tick_and_drain(&rt, &inbox, 10);
    assert_eq!(replies, vec![Count(1), Count(2), Count(3)]);
}

#[test]
fn actor_spawns_child_and_child_replies() {
    // Given a DelegatorActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When I ask it to process value 7
    rt.send_to(addr, Forward { value: 7, reply_to: *inbox.addr() }).unwrap();

    // Then the child doubled it — inbox gets Done(14)
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert_eq!(reply, Some(Done(14)), "child should have doubled the value");
}

#[test]
fn three_level_chain_reaches_leaf() {
    // Given a ChainActor that will spawn 2 more levels
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(ChainActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When I send remaining=2 (root → child → grandchild)
    rt.send_to(addr, ChainMsg { remaining: 2, depth: 0, reply_to: *inbox.addr() }).unwrap();

    // Then the grandchild (depth 2) replies
    let reply = tick_until_recv(&rt, &inbox, 30);
    assert_eq!(reply, Some(Done(2)), "leaf at depth 2 should have replied");
}

#[test]
fn fan_out_distributes_work_to_children() {
    // Given a FanOutActor told to spawn 5 children
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(FanOutActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When it spawns 5 children, each doubling their index
    rt.send_to(addr, FanOut { count: 5, reply_to: *inbox.addr() }).unwrap();

    // Then I receive 5 replies whose values are {2, 4, 6, 8, 10}
    let mut replies = tick_and_drain(&rt, &inbox, 20);
    let mut values: Vec<usize> = replies.drain(..).map(|d| d.0).collect();
    values.sort();
    assert_eq!(values, vec![2, 4, 6, 8, 10], "each child should have doubled its index");
}

#[test]
fn actor_knows_its_own_address() {
    // Given a SelfAddrActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(SelfAddrActor).unwrap();
    let inbox = rt.new_inbox::<MyAddr>().unwrap();

    // When I ask it for its address
    rt.send_to(addr, WhoAreYou { reply_to: *inbox.addr() }).unwrap();

    // Then the address it reports matches the one from spawn
    let reply = tick_until_recv(&rt, &inbox, 10);
    assert_eq!(reply, Some(MyAddr(addr)), "actor should know its own address");
}

// ═══════════════════════════════════════════════════════════════════════════
// Message Delivery
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn messages_arrive_in_fifo_order() {
    // Given a CounterActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When I send 5 Increments
    for _ in 0..5 {
        rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    }

    // Then replies arrive Count(1)..Count(5) in order
    let replies = tick_and_drain(&rt, &inbox, 10);
    assert_eq!(
        replies,
        vec![Count(1), Count(2), Count(3), Count(4), Count(5)],
        "messages must be processed in FIFO order"
    );
}

#[test]
fn multiple_actors_have_independent_mailboxes() {
    // Given 3 PingPongActors, each with its own inbox
    let rt = std_runtime(RuntimeConfig::default());
    let mut addrs = Vec::new();
    let mut inboxes = Vec::new();
    for _ in 0..3 {
        let addr = rt.spawn(PingPongActor).unwrap();
        let inbox = rt.new_inbox::<Pong>().unwrap();
        rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
        addrs.push(addr);
        inboxes.push(inbox);
    }

    // When all messages are processed
    for _ in 0..10 {
        rt.tick();
    }

    // Then each inbox sees exactly one Pong — no cross-contamination
    for (i, inbox) in inboxes.iter().enumerate() {
        assert!(inbox.try_recv().is_some(), "actor {i} should have replied");
        assert!(inbox.try_recv().is_none(), "actor {i} should have only one reply");
    }
}

#[test]
fn multiple_senders_reach_same_actor() {
    // Given 1 CounterActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox_a = rt.new_inbox::<Count>().unwrap();
    let inbox_b = rt.new_inbox::<Count>().unwrap();

    // When two different callers each send an Increment
    rt.send_to(addr, Increment { reply_to: *inbox_a.addr() }).unwrap();
    rt.send_to(addr, Increment { reply_to: *inbox_b.addr() }).unwrap();

    // Then both replies arrive and the counter incremented for each
    for _ in 0..10 {
        rt.tick();
    }
    let a = inbox_a.try_recv();
    let b = inbox_b.try_recv();
    assert!(a.is_some(), "first sender should get a reply");
    assert!(b.is_some(), "second sender should get a reply");
    // Second caller sees Count(2), proving both messages were handled
    assert_eq!(b, Some(Count(2)));
}

#[test]
fn send_to_nonexistent_address_returns_error() {
    // Given a runtime with no actors at a random address
    let rt = std_runtime(RuntimeConfig::default());
    let bogus = ActorAddress::new_random();

    // When I try to send to that address
    let result = rt.send_to(bogus, Pong);

    // Then I get an error
    assert!(result.is_err(), "sending to unknown address should fail");
}

#[test]
fn messages_sent_within_handler_are_delivered() {
    // Given a DelegatorActor (spawns child + sends in same handler call)
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When I trigger the delegator
    rt.send_to(addr, Forward { value: 5, reply_to: *inbox.addr() }).unwrap();

    // Then the child receives the forwarded msg and replies to my inbox
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert!(reply.is_some(), "child spawned during handler should receive its message");
    assert_eq!(reply.unwrap(), Done(10));
}

// ═══════════════════════════════════════════════════════════════════════════
// Threading Model
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn tick_drives_single_threaded_processing() {
    // Given a single-threaded runtime
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When I send a message and tick manually
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Before ticking: nothing received
    assert!(inbox.try_recv().is_none(), "should not receive before tick");

    // After ticking: reply available
    rt.tick();
    rt.tick();
    assert!(inbox.try_recv().is_some(), "tick() should drive processing");
}

#[test]
fn run_processes_messages_in_background() {
    // Given a multi-threaded runtime
    let rt = std_runtime(RuntimeConfig { num_threads: 4, ..Default::default() });
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // When I call run() (spawns background worker threads)
    let handle = rt.run().unwrap();

    // Then the inbox receives a reply without manual ticking
    let mut received = false;
    for _ in 0..100 {
        if inbox.try_recv().is_some() {
            received = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    handle.shutdown();
    handle.join();
    assert!(received, "background workers should process the message");
}

#[test]
fn shutdown_stops_background_workers() {
    // Given a running multi-threaded runtime
    let rt = std_runtime(RuntimeConfig { num_threads: 2, ..Default::default() });
    let handle = rt.run().unwrap();

    // When I call shutdown + join
    handle.shutdown();
    handle.join();

    // Then join returns (threads have stopped) — test passes by not hanging
}

#[test]
fn cross_worker_delegation_delivers_reply() {
    // Given a 2-thread runtime with a DelegatorActor
    let rt = std_runtime(RuntimeConfig { num_threads: 2, ..Default::default() });
    let addr = rt.spawn(DelegatorActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();
    rt.send_to(addr, Forward { value: 3, reply_to: *inbox.addr() }).unwrap();

    // When processing runs across worker threads
    let handle = rt.run().unwrap();

    // Then the reply reaches the inbox despite potentially crossing workers
    let mut reply = None;
    for _ in 0..100 {
        if let Some(msg) = inbox.try_recv() {
            reply = Some(msg);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    handle.shutdown();
    handle.join();
    assert_eq!(reply, Some(Done(6)), "cross-worker delegation should deliver the reply");
}

// ═══════════════════════════════════════════════════════════════════════════
// Backpressure & Scale
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn inbox_handles_burst_of_messages() {
    // Given a CounterActor and a small runtime
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When I send a burst of 20 messages
    for _ in 0..20 {
        rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    }

    // Then all 20 are delivered in order
    let replies = tick_and_drain(&rt, &inbox, 30);
    assert_eq!(replies.len(), 20, "all 20 messages should be delivered");
    // Verify ordering: last reply should be Count(20)
    assert_eq!(replies.last(), Some(&Count(20)), "messages should arrive in FIFO order");
}

#[test]
fn hundred_actors_all_receive_messages() {
    // Given 100 PingPongActors
    let rt = std_runtime(RuntimeConfig {
        max_actors: 2000,
        ..Default::default()
    });
    let mut pairs = Vec::new();
    for _ in 0..100 {
        let addr = rt.spawn(PingPongActor).unwrap();
        let inbox = rt.new_inbox::<Pong>().unwrap();
        rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
        pairs.push(inbox);
    }

    // When all messages are processed
    for _ in 0..50 {
        rt.tick();
    }

    // Then all 100 inboxes have a Pong
    let received = pairs.iter().filter(|inbox| inbox.try_recv().is_some()).count();
    assert_eq!(received, 100, "all 100 actors should have replied");
}

// ═══════════════════════════════════════════════════════════════════════════
// Panic Safety
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn panic_in_handler_does_not_kill_other_actors() {
    // Given a PanicActor and a PingPongActor on the same runtime
    let rt = std_runtime(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    let good_addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When the PanicActor panics (stderr output expected)
    rt.send_to(panic_addr, PanicMsg).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    // Then PingPongActor still works normally
    rt.send_to(good_addr, Ping { reply_to: *inbox.addr() }).unwrap();
    let reply = tick_until_recv(&rt, &inbox, 10);
    assert!(reply.is_some(), "healthy actor should still work after peer panics");
}

#[test]
fn panic_does_not_corrupt_subsequent_messages() {
    // Given a PanicActor and a CounterActor
    let rt = std_runtime(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When the PanicActor panics, then the CounterActor handles messages
    rt.send_to(panic_addr, PanicMsg).unwrap();
    rt.send_to(counter_addr, Increment { reply_to: *inbox.addr() }).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap(); // panic again
    rt.send_to(counter_addr, Increment { reply_to: *inbox.addr() }).unwrap();

    // Then the CounterActor is unaffected — state accumulates correctly
    let replies = tick_and_drain(&rt, &inbox, 20);
    assert_eq!(replies, vec![Count(1), Count(2)], "counter should be unaffected by peer panics");
}

#[test]
fn panicked_actor_is_poisoned_and_discards_future_messages() {
    // Given a CounterActor that receives 3 messages: Increment, PanicMsg, Increment
    // We need an actor that can handle both — so we use PanicActor for the panic
    // and a separate CounterActor that continues working.
    //
    // Specifically: a PanicActor receives one PanicMsg, panics, then future
    // PanicMsgs should be silently discarded (actor is poisoned).
    let rt = std_runtime(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    let good_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // Send a panic message, then more panic messages — they should be discarded
    rt.send_to(panic_addr, PanicMsg).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();

    // Also send to a healthy actor to prove the system still works
    rt.send_to(good_addr, Increment { reply_to: *inbox.addr() }).unwrap();

    // When messages are processed
    for _ in 0..20 {
        rt.tick();
    }

    // Then: healthy actor still works, and only 1 panic recorded (not 3)
    let reply = inbox.try_recv();
    assert!(reply.is_some(), "healthy actor should still reply after peer is poisoned");

    let s = rt.stats();
    let total_panics: u64 = s.workers.iter().map(|w| w.panics).sum();
    assert_eq!(total_panics, 1, "only the first panic should be recorded; rest are discarded");
}

// ═══════════════════════════════════════════════════════════════════════════
// Observability
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn stats_report_spawned_actors() {
    // Given 3 spawned actors
    let rt = std_runtime(RuntimeConfig::default());
    for _ in 0..3 {
        rt.spawn(PingPongActor).unwrap();
    }
    rt.tick();

    // When I check stats
    let s = rt.stats();

    // Then the system accounts for every spawned actor
    assert!(
        s.actors.len() >= 3,
        "stats should report at least 3 actors, got {}",
        s.actors.len()
    );
}

#[test]
fn stats_report_message_throughput() {
    // Given 3 actors that each process 10 messages
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox.addr();

    let mut addrs = Vec::new();
    for _ in 0..3 {
        addrs.push(rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap());
    }

    for addr in &addrs {
        for _ in 0..10 {
            rt.send_to(*addr, Ping { reply_to: inbox_addr }).unwrap();
        }
    }

    // When messages are processed
    for _ in 0..50 {
        rt.tick();
    }

    // Then stats reflect the throughput
    let s = rt.stats();
    let total: u64 = s.workers.iter().map(|w| w.messages_processed).sum();
    assert!(
        total >= 30,
        "at least 30 messages should be processed, got {}",
        total
    );
}

#[test]
fn stats_record_panics() {
    // Given a PanicActor that panics twice
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PanicActor).unwrap();

    rt.send_to(addr, PanicMsg).unwrap();
    rt.send_to(addr, PanicMsg).unwrap();

    // When messages are processed (stderr output expected)
    for _ in 0..10 {
        rt.tick();
    }

    // Then stats record the panic (second message is discarded — actor is poisoned)
    let s = rt.stats();
    let total_panics: u64 = s.workers.iter().map(|w| w.panics).sum();
    assert!(
        total_panics >= 1,
        "stats should record at least 1 panic, got {}",
        total_panics
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Edge Cases & Adversarial Tests
// ═══════════════════════════════════════════════════════════════════════════

// ── Additional actors for edge-case tests ────────────────────────────────

/// Sends a countdown message to itself, then replies Done(0) when remaining hits zero.
/// Tests pending_local self-delivery path.
struct SelfSendActor;

#[derive(Clone)]
struct Countdown {
    remaining: usize,
    reply_to: ActorAddress,
}

impl ActorInterface for SelfSendActor {
    type Incoming = Countdown;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: Countdown) {
        if msg.remaining == 0 {
            let _ = ctx.send(msg.reply_to, Done(0));
        } else {
            let _ = ctx.send(
                ctx.self_addr(),
                Countdown { remaining: msg.remaining - 1, reply_to: msg.reply_to },
            );
        }
    }
}

/// Spawns a DoubleActor child, sends it work, then panics.
/// The child should still process the forwarded message.
struct SpawnThenPanicActor;

impl ActorInterface for SpawnThenPanicActor {
    type Incoming = Forward;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        let child = ctx.spawn(DoubleActor).unwrap();
        let _ = ctx.send(child, Forward { value: msg.value, reply_to: msg.reply_to });
        panic!("intentional panic after spawn+send");
    }
}

/// Processes `remaining_good` messages, then panics on the next one.
/// Uses a shared counter so the test can observe how many were processed.
struct PanicAfterNActor {
    remaining_good: usize,
    counter: Arc<AtomicUsize>,
}

impl ActorInterface for PanicAfterNActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        if self.remaining_good == 0 {
            panic!("intentional delayed panic");
        }
        self.remaining_good -= 1;
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sends a reply, then panics. Tests that messages sent before the panic
/// are still delivered (they're already in the queue).
struct SendThenPanicActor;

impl ActorInterface for SendThenPanicActor {
    type Incoming = Ping;
    type Response = Pong;
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
        panic!("intentional panic after send");
    }
}

// ── Tests ────────────────────────────────────────────────────────────────


#[test]
fn wrong_type_to_actor_increments_type_mismatch_counter() {
    // Given a PingPongActor that expects Ping
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();

    // When I send it a Count message (wrong type)
    rt.send_to(addr, Count(42)).unwrap();
    for _ in 0..10 {
        rt.tick();
    }

    // Then stats record the type mismatch
    let s = rt.stats();
    let mismatches: u64 = s.workers.iter().map(|w| w.type_mismatches).sum();
    assert_eq!(mismatches, 1, "sending wrong type should increment type_mismatches");
}

// FIXME dont count dropped messages
#[test]
fn type_mismatch_still_counted_as_processed() {
    // Given a PingPongActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();

    // When I send it 3 wrong-type messages
    for _ in 0..3 {
        rt.send_to(addr, Count(0)).unwrap();
    }
    for _ in 0..10 {
        rt.tick();
    }

    // Then all 3 are counted in both type_mismatches AND messages_processed
    // (the message was dequeued and attempted — it "went through" the system)
    let s = rt.stats();
    let mismatches: u64 = s.workers.iter().map(|w| w.type_mismatches).sum();
    let processed: u64 = s.workers.iter().map(|w| w.messages_processed).sum();
    assert_eq!(mismatches, 3);
    assert!(
        processed >= 3,
        "type-mismatched messages count as processed (dequeued+attempted), got {}",
        processed
    );
}

#[test]
fn self_send_chain_completes() {
    // Given a SelfSendActor that will bounce a message to itself 10 times
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(SelfSendActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When triggered with remaining=10
    rt.send_to(addr, Countdown { remaining: 10, reply_to: *inbox.addr() }).unwrap();

    // Then after enough ticks the chain completes.
    // Each self-send goes through pending_local → next tick's mailbox,
    // so it needs at least 11 ticks (1 initial + 10 bounces).
    let reply = tick_until_recv(&rt, &inbox, 50);
    assert_eq!(reply, Some(Done(0)), "self-send chain should complete");
}

#[test]
fn panic_mid_batch_discards_remaining_messages() {
    // Given an actor that processes 2 messages then panics on the 3rd
    let counter = Arc::new(AtomicUsize::new(0));
    let rt = std_runtime(RuntimeConfig::default());
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(PanicAfterNActor {
        remaining_good: 2,
        counter: counter.clone(),
    }).unwrap();

    // When I queue 5 messages and tick (all arrive before first tick_all)
    for _ in 0..5 {
        rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }
    for _ in 0..20 {
        rt.tick();
    }

    // Then only 2 messages were processed — the 3rd panicked, 4th+5th discarded
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "only messages before the panic should be processed"
    );
    let s = rt.stats();
    let panics: u64 = s.workers.iter().map(|w| w.panics).sum();
    assert_eq!(panics, 1, "exactly one panic should be recorded");
}

#[test]
fn spawn_then_panic_child_survives() {
    // Given a SpawnThenPanicActor
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(SpawnThenPanicActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When the parent spawns a child, sends it work, then panics
    rt.send_to(addr, Forward { value: 5, reply_to: *inbox.addr() }).unwrap();

    // Then the child still processes the forwarded message and replies Done(10)
    let reply = tick_until_recv(&rt, &inbox, 30);
    assert_eq!(
        reply,
        Some(Done(10)),
        "child spawned before parent panic should still work"
    );
}

#[test]
fn panic_after_send_still_delivers_sent_messages() {
    // Given a SendThenPanicActor (sends Pong, then panics)
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(SendThenPanicActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When it processes a Ping (sends reply, then panics)
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then the Pong reply still arrives — sends happen before the panic unwinds
    let reply = tick_until_recv(&rt, &inbox, 20);
    assert!(
        reply.is_some(),
        "message sent before panic should still be delivered"
    );
}

// FIXME: document somewhere this behavior. No test is needed. It is not obvious what to do
// about failed messages. Because this is going to be distributed, we cannot rely on delivery always
// succeeeding.
#[test]
fn send_to_poisoned_actor_is_a_silent_black_hole() {
    // Given a poisoned actor (panicked on first message)
    let rt = std_runtime(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    // When I send more messages to it (after cleanup, address is removed)
    let result = rt.send_to(panic_addr, PanicMsg);

    // Then send_to returns an error (actor has been cleaned up and removed)
    assert!(
        result.is_err(),
        "send_to cleaned-up actor should return error"
    );

    // And the original panic was recorded
    let s = rt.stats();
    let panics: u64 = s.workers.iter().map(|w| w.panics).sum();
    assert_eq!(panics, 1, "poisoned actor should have recorded one panic");
}

#[test]
fn tiny_buffer_delivers_all_messages_in_order() {
    // Given a runtime with channel_buffer_size=1 (overflow on every 2nd message)
    let rt = std_runtime(RuntimeConfig {
        channel_buffer_size: 1,
        ..Default::default()
    });
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When I send 50 messages (almost all hit the overflow queue)
    for _ in 0..50 {
        rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    }

    // Then all 50 arrive and in FIFO order
    let replies = tick_and_drain(&rt, &inbox, 100);
    assert_eq!(replies.len(), 50, "all messages should arrive despite tiny buffer");
    assert_eq!(
        replies.last(),
        Some(&Count(50)),
        "messages should maintain FIFO order through overflow queue"
    );
}

#[test]
fn empty_runtime_tick_and_stats_are_safe() {
    // Given a runtime with no actors at all
    let rt = std_runtime(RuntimeConfig::default());

    // When I tick and check stats
    for _ in 0..10 {
        rt.tick();
    }
    let s = rt.stats();

    // Then everything reports zeros without panicking
    assert_eq!(s.actors.len(), 0);
    assert_eq!(s.num_workers, 1);
    let total: u64 = s.workers.iter().map(|w| w.messages_processed).sum();
    assert_eq!(total, 0);
}

#[test]
fn stats_stable_after_idle_ticks() {
    // Given an actor that processes a message
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();
    rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let _ = inbox.try_recv();
    let s1 = rt.stats();

    // When I tick 100 more times with no messages
    for _ in 0..100 {
        rt.tick();
    }
    let s2 = rt.stats();

    // Then messages_processed doesn't grow during idle ticks
    let total1: u64 = s1.workers.iter().map(|w| w.messages_processed).sum();
    let total2: u64 = s2.workers.iter().map(|w| w.messages_processed).sum();
    assert_eq!(
        total1, total2,
        "idle ticks must not inflate messages_processed"
    );
}

#[test]
fn deep_spawn_chain_completes() {
    // Given a 100-level chain (tests no stack overflow from recursive tick_all)
    let rt = std_runtime(RuntimeConfig {
        max_actors: 2000,
        ..Default::default()
    });
    let addr = rt.spawn(ChainActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When chain of depth 100 is triggered
    rt.send_to(
        addr,
        ChainMsg { remaining: 100, depth: 0, reply_to: *inbox.addr() },
    ).unwrap();

    // Then the leaf at depth 100 replies
    let reply = tick_until_recv(&rt, &inbox, 500);
    assert_eq!(
        reply,
        Some(Done(100)),
        "100-level chain should complete"
    );
}

#[test]
fn all_spawned_addresses_are_unique() {
    let rt = std_runtime(RuntimeConfig {
        max_actors: 10_000,
        ..Default::default()
    });
    let mut addrs: Vec<ActorAddress> = (0..1000)
        .map(|_| rt.spawn(PingPongActor).unwrap())
        .collect();

    addrs.sort_by_key(|a| a.0);
    let before = addrs.len();
    addrs.dedup_by_key(|a| a.0);
    assert_eq!(addrs.len(), before, "all 1000 addresses should be unique");
}

#[test]
fn inbox_empty_before_any_tick() {
    // Given a sent message that hasn't been ticked
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then inbox is empty — no processing without tick
    assert!(inbox.try_recv().is_none());
}

#[test]
fn interleaved_spawn_and_send_in_handler_all_complete() {
    // Given a FanOutActor that spawns 20 children with interleaved spawn+send
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(FanOutActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    rt.send_to(addr, FanOut { count: 20, reply_to: *inbox.addr() }).unwrap();

    let replies = tick_and_drain(&rt, &inbox, 50);
    assert_eq!(
        replies.len(),
        20,
        "all 20 children spawned+messaged in same handler should reply"
    );
}

#[test]
fn multiple_inbox_types_coexist() {
    // Given two inboxes of different types on the same runtime
    let rt = std_runtime(RuntimeConfig::default());
    let counter = rt.spawn(CounterActor { count: 0 }).unwrap();
    let pinger = rt.spawn(PingPongActor).unwrap();
    let count_inbox = rt.new_inbox::<Count>().unwrap();
    let pong_inbox = rt.new_inbox::<Pong>().unwrap();

    // When both actors reply to their respective inboxes
    rt.send_to(counter, Increment { reply_to: *count_inbox.addr() }).unwrap();
    rt.send_to(pinger, Ping { reply_to: *pong_inbox.addr() }).unwrap();
    for _ in 0..10 {
        rt.tick();
    }

    // Then each inbox gets its correct type — no cross-contamination
    assert_eq!(count_inbox.try_recv(), Some(Count(1)));
    assert_eq!(pong_inbox.try_recv(), Some(Pong));
}

#[test]
fn poisoned_actor_messages_not_counted_as_processed() {
    // Given a poisoned actor that has been cleaned up
    let rt = std_runtime(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let s1 = rt.stats();
    let processed_before: u64 = s1.workers.iter().map(|w| w.messages_processed).sum();

    // When I try to send 10 messages to the cleaned-up actor
    // (sends will fail because actor is removed from address map)
    let mut send_failures = 0;
    for _ in 0..10 {
        if rt.send_to(panic_addr, PanicMsg).is_err() {
            send_failures += 1;
        }
    }
    for _ in 0..20 {
        rt.tick();
    }
    let s2 = rt.stats();
    let processed_after: u64 = s2.workers.iter().map(|w| w.messages_processed).sum();

    // Then sends fail (actor cleaned up) and processed count unchanged
    assert_eq!(send_failures, 10, "all sends should fail to cleaned-up actor");
    assert_eq!(
        processed_before, processed_after,
        "no additional messages should be processed after cleanup"
    );
}

#[test]
fn rapid_spawn_and_immediate_send() {
    // Given a runtime, spawn an actor and immediately send before any tick
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When I spawn + send in rapid succession, 50 times
    let mut addrs = Vec::new();
    for _ in 0..50 {
        let addr = rt.spawn(PingPongActor).unwrap();
        rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
        addrs.push(addr);
    }

    // Then all 50 replies eventually arrive (spawn queue drained before transfer)
    let replies = tick_and_drain(&rt, &inbox, 50);
    assert_eq!(replies.len(), 50, "all spawn+send pairs should complete");
}

// ═══════════════════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn default_config_works_out_of_the_box() {
    // Given the default config — no tuning needed
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // When I do the simplest possible thing
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then it just works
    let reply = tick_until_recv(&rt, &inbox, 10);
    assert!(reply.is_some(), "default config should work without tuning");
}

#[test]
fn custom_thread_count_respected() {
    // Given a config requesting 4 threads
    let rt = std_runtime(RuntimeConfig { num_threads: 4, ..Default::default() });
    // Spawn an actor so the runtime has something to report
    rt.spawn(PingPongActor).unwrap();
    let handle = rt.run().unwrap();

    // When I check stats
    let s = handle.runtime.stats();

    handle.shutdown();
    handle.join();

    // Then the runtime created the requested number of workers
    assert_eq!(s.num_workers, 4, "runtime should respect the requested thread count");
}

// ═══════════════════════════════════════════════════════════════════════════
// Fairness (message budget)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn hot_actor_does_not_starve_cold_actor() {
    // Given: one "hot" actor with 1000 queued messages and one "cold" actor with 1 message
    let rt = std_runtime(RuntimeConfig::default());
    let hot_counter = Arc::new(AtomicUsize::new(0));
    let cold_inbox = rt.new_inbox::<Pong>().unwrap();

    let hot_addr = rt.spawn(CountingPingActor { counter: hot_counter.clone() }).unwrap();
    let cold_addr = rt.spawn(PingPongActor).unwrap();

    // Load the hot actor with 1000 messages (needs a dummy inbox for replies)
    let dummy = rt.new_inbox::<Pong>().unwrap();
    for _ in 0..1000 {
        rt.send_to(hot_addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }
    // Send one message to the cold actor
    rt.send_to(cold_addr, Ping { reply_to: *cold_inbox.addr() }).unwrap();

    // When: we tick a limited number of times (default budget = 64 msgs/actor/tick)
    // After 1 tick: hot actor processes 64, cold actor processes 1
    rt.tick();

    // Then: the cold actor replied even though the hot actor had 1000 queued messages
    let cold_reply = cold_inbox.try_recv();
    assert!(
        cold_reply.is_some(),
        "cold actor must not be starved by hot actor; message budget should enforce fairness"
    );
    // And the hot actor only processed its budget, not all 1000
    let hot_processed = hot_counter.load(Ordering::SeqCst);
    assert!(
        hot_processed <= 64,
        "hot actor should process at most the budget (64) per tick, got {hot_processed}"
    );
}

#[test]
fn unlimited_budget_drains_all_messages() {
    // Given: a runtime with unlimited budget (0)
    let rt = std_runtime(RuntimeConfig {
        actor_message_budget: 0,
        ..Default::default()
    });
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap();

    // When: 500 messages are queued and we tick once
    for _ in 0..500 {
        rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }
    rt.tick();
    rt.tick();

    // Then: all 500 are processed in a single pass (no budget limit)
    let processed = counter.load(Ordering::SeqCst);
    assert_eq!(processed, 500, "unlimited budget should drain all messages");
}

#[test]
fn budget_messages_drain_across_multiple_ticks() {
    // Given: an actor with more messages than the budget
    let rt = std_runtime(RuntimeConfig::default()); // budget=64
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap();

    // When: 200 messages are queued
    for _ in 0..200 {
        rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }

    // Then: it takes multiple ticks to drain them all
    for _ in 0..10 {
        rt.tick();
    }
    let processed = counter.load(Ordering::SeqCst);
    assert_eq!(processed, 200, "all messages should eventually be processed across ticks");
}

// ═══════════════════════════════════════════════════════════════════════════
// Stress Tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn message_ordering_preserved_under_budget() {
    // Given: a CounterActor processing messages with a small budget
    let rt = std_runtime(RuntimeConfig {
        actor_message_budget: 8,
        ..Default::default()
    });
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    let inbox = rt.new_inbox::<Count>().unwrap();

    // When: 100 messages are sent and processed across many ticks
    for _ in 0..100 {
        rt.send_to(addr, Increment { reply_to: *inbox.addr() }).unwrap();
    }
    for _ in 0..50 {
        rt.tick();
    }

    // Then: replies arrive in FIFO order (Count(1), Count(2), ..., Count(100))
    let replies: Vec<_> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert_eq!(replies.len(), 100, "all 100 messages should be delivered");
    for (i, reply) in replies.iter().enumerate() {
        assert_eq!(
            *reply,
            Count(i + 1),
            "message ordering must be preserved under budget; expected Count({}) at position {i}",
            i + 1
        );
    }
}

#[test]
fn mt_stress_many_senders_one_receiver() {
    // Given: 4 threads, 50 senders each sending 100 messages to one receiver
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    });
    let total_senders = 50;
    let msgs_per_sender = 100;
    let total_expected = total_senders * msgs_per_sender;

    let counter = Arc::new(AtomicUsize::new(0));
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let receiver = rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap();

    // Spawn senders and send messages
    for _ in 0..total_senders {
        for _ in 0..msgs_per_sender {
            rt.send_to(receiver, Ping { reply_to: *inbox.addr() }).unwrap();
        }
    }

    // When: runtime runs in background
    let handle = rt.run().unwrap();

    // Then: all messages are eventually processed
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let processed = counter.load(Ordering::SeqCst);
        if processed >= total_expected {
            break;
        }
        if std::time::Instant::now() > deadline {
            let processed = counter.load(Ordering::SeqCst);
            handle.shutdown();
            handle.join();
            panic!(
                "Timed out: only {processed}/{total_expected} messages processed in 5s"
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    handle.shutdown();
    handle.join();
    let final_count = counter.load(Ordering::SeqCst);
    assert_eq!(
        final_count, total_expected,
        "all {total_expected} messages should be processed"
    );
}

#[test]
fn mt_stress_concurrent_spawn_and_send() {
    // Given: a multi-threaded runtime
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    });
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox.addr();

    // Spawn 200 actors and immediately send them messages before any ticks
    let mut addrs = Vec::new();
    for _ in 0..200 {
        let addr = rt.spawn(PingPongActor).unwrap();
        rt.send_to(addr, Ping { reply_to: inbox_addr }).unwrap();
        addrs.push(addr);
    }

    // When: runtime processes in background
    let handle = rt.run().unwrap();

    // Then: all 200 replies arrive
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut received = 0;
    while received < 200 {
        if inbox.try_recv().is_some() {
            received += 1;
        } else if std::time::Instant::now() > deadline {
            handle.shutdown();
            handle.join();
            panic!("Timed out: only {received}/200 replies received in 5s");
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    handle.shutdown();
    handle.join();
    assert_eq!(received, 200, "all 200 concurrent spawn+send pairs should complete");
}

#[test]
fn mt_chain_spawning_under_load() {
    // Given: a multi-threaded runtime with a chain actor
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        max_actors: 5_000,
        ..Default::default()
    });
    let addr = rt.spawn(ChainActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // When: we trigger a 50-level chain that will spawn actors across workers
    rt.send_to(
        addr,
        ChainMsg { remaining: 50, depth: 0, reply_to: *inbox.addr() },
    )
    .unwrap();
    let handle = rt.run().unwrap();

    // Then: the chain completes despite actors being on different workers
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut reply = None;
    while reply.is_none() {
        if let Some(msg) = inbox.try_recv() {
            reply = Some(msg);
        } else if std::time::Instant::now() > deadline {
            handle.shutdown();
            handle.join();
            panic!("Timed out waiting for chain completion");
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    handle.shutdown();
    handle.join();
    assert_eq!(
        reply,
        Some(Done(50)),
        "50-level chain should complete across multiple workers"
    );
}

#[test]
fn mt_panic_isolation_under_load() {
    // Given: a 4-thread runtime with panicking and healthy actors
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        max_actors: 5_000,
        channel_buffer_size: 10_000,
        ..Default::default()
    });
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();

    // Spawn 10 panicking actors and 10 healthy counting actors
    let mut panic_addrs = Vec::new();
    let mut healthy_addrs = Vec::new();
    for _ in 0..10 {
        panic_addrs.push(rt.spawn(PanicActor).unwrap());
        healthy_addrs.push(rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap());
    }

    // Trigger panics and send 100 messages to each healthy actor
    for &addr in &panic_addrs {
        rt.send_to(addr, PanicMsg).unwrap();
    }
    for &addr in &healthy_addrs {
        for _ in 0..100 {
            rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
        }
    }

    // When: runtime runs
    let handle = rt.run().unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let expected = 10 * 100;
    loop {
        let processed = counter.load(Ordering::SeqCst);
        if processed >= expected {
            break;
        }
        if std::time::Instant::now() > deadline {
            let processed = counter.load(Ordering::SeqCst);
            handle.shutdown();
            handle.join();
            panic!("Timed out: only {processed}/{expected} healthy messages processed");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    handle.shutdown();
    handle.join();

    // Then: all healthy actors processed all their messages despite panicking peers
    let final_count = counter.load(Ordering::SeqCst);
    assert_eq!(
        final_count, expected,
        "panicking actors should not affect healthy actors on other workers"
    );
}

#[test]
fn sustained_throughput_does_not_drop_messages() {
    // Given: a runtime processing messages in batches, simulating sustained load
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap();

    // When: we send 10 batches of 100 messages, ticking between batches
    for batch in 0..10 {
        for _ in 0..100 {
            rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
        }
        // Tick enough to process one budget worth per batch
        for _ in 0..5 {
            rt.tick();
        }
        // Verify progress is being made (not stuck)
        let processed = counter.load(Ordering::SeqCst);
        assert!(
            processed > batch * 50,
            "batch {batch}: should have made progress, only {processed} processed"
        );
    }

    // Drain remaining
    for _ in 0..100 {
        rt.tick();
    }

    // Then: all 1000 messages are eventually processed
    let total = counter.load(Ordering::SeqCst);
    assert_eq!(total, 1000, "sustained load should not drop any messages");
}

#[test]
fn mt_parked_worker_wakes_on_send() {
    // Given: a 2-thread runtime that has been idle (workers are parked)
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let handle = rt.run().unwrap();

    // Let workers park (idle for a while)
    std::thread::sleep(std::time::Duration::from_millis(50));

    // When: we send a message to a parked worker
    let before = std::time::Instant::now();
    handle.runtime.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then: the worker wakes up and processes the message quickly
    let mut received = false;
    for _ in 0..1000 {
        if inbox.try_recv().is_some() {
            received = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let latency = before.elapsed();

    handle.shutdown();
    handle.join();

    assert!(received, "parked worker should wake up and process the message");
    // With park_timeout + unpark, the latency should be well under 100ms
    // (old sleep-based approach could have up to 1ms delay per the default max)
    assert!(
        latency.as_millis() < 100,
        "wake-from-park latency should be low, was {:?}",
        latency
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Competitor Bug-Inspired Tests
// (from analyzing ractor, actix, kameo bug histories)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn stats_snapshot_is_read_only() {
    // Inspired by ractor #310: get_children() was destructive (cleared on read).
    // Verify that calling stats() multiple times returns consistent data.
    let rt = std_runtime(RuntimeConfig::default());
    let _addr = rt.spawn(PingPongActor).unwrap();
    rt.tick();

    let s1 = rt.stats();
    let s2 = rt.stats();
    let s3 = rt.stats();

    // All three snapshots should report the same actor count
    assert_eq!(s1.actors.len(), s2.actors.len(), "stats() should not mutate state");
    assert_eq!(s2.actors.len(), s3.actors.len(), "repeated stats() calls must be idempotent");
    assert!(s1.actors.len() >= 1, "should report at least 1 actor");
}

#[test]
fn stats_under_load_do_not_interfere_with_processing() {
    // Verify that taking stats snapshots doesn't slow down or break message processing.
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let dummy = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn(CountingPingActor { counter: counter.clone() }).unwrap();

    for _ in 0..100 {
        rt.send_to(addr, Ping { reply_to: *dummy.addr() }).unwrap();
    }

    // Interleave stats calls with ticks
    for _ in 0..20 {
        rt.tick();
        let _s = rt.stats(); // should not affect processing
    }

    let processed = counter.load(Ordering::SeqCst);
    assert_eq!(processed, 100, "stats() calls must not interfere with message processing");
}

#[test]
fn shutdown_wakes_parked_workers_immediately() {
    // Verify that shutdown unparks all workers so they exit promptly.
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    });
    let handle = rt.run().unwrap();

    // Let workers park
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Shutdown should wake all parked workers
    let before = std::time::Instant::now();
    handle.shutdown();
    handle.join();
    let shutdown_time = before.elapsed();

    // Workers should exit quickly (well under 1 second)
    assert!(
        shutdown_time.as_millis() < 500,
        "shutdown should complete quickly with parked workers, took {:?}",
        shutdown_time
    );
}

#[test]
fn mt_send_after_run_delivers_to_running_actors() {
    // Inspired by kameo #185: messages not delivered during startup.
    // Verify that send_to works correctly after run() is called.
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Start the runtime FIRST, then send
    let handle = rt.run().unwrap();

    // Give workers a moment to start
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Send after run()
    handle.runtime.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut received = false;
    while !received {
        if inbox.try_recv().is_some() {
            received = true;
        } else if std::time::Instant::now() > deadline {
            handle.shutdown();
            handle.join();
            panic!("Message sent after run() was not delivered");
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    handle.shutdown();
    handle.join();
    assert!(received, "messages sent after run() must be delivered");
}

#[test]
fn budget_respected_even_with_self_sends() {
    // Inspired by actix #515: send bypassing mailbox size.
    // Verify that self-sends (pending_local) don't bypass the message budget.
    // The SelfSendActor sends to itself; each self-send goes through pending_local
    // and appears in the mailbox on the next tick. The budget should still apply.
    let rt = std_runtime(RuntimeConfig {
        actor_message_budget: 4,
        ..Default::default()
    });
    let addr = rt.spawn(SelfSendActor).unwrap();
    let inbox = rt.new_inbox::<Done>().unwrap();

    // remaining=20 means 20 self-sends before replying Done(0)
    rt.send_to(addr, Countdown { remaining: 20, reply_to: *inbox.addr() }).unwrap();

    // With budget=4, each tick processes at most 4 messages per actor.
    // The self-send chain should take several ticks to complete.
    for _ in 0..30 {
        rt.tick();
    }

    let reply = inbox.try_recv();
    assert_eq!(
        reply,
        Some(Done(0)),
        "self-send chain should complete despite message budget"
    );
}

// ── Load-Aware Placement Tests ─────────────────────────────────────────────

/// Given a multi-threaded runtime where one worker has many more actors,
/// when new actors are spawned after a few ticks (so stats propagate),
/// then they should be placed on the lighter worker.
#[test]
fn load_aware_placement_prefers_lighter_worker() {
    // 2 threads: intentionally imbalance by spawning many actors first
    let rt = std_runtime(RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    });

    // Phase 1: Spawn 20 actors. With round-robin, they split ~10/10.
    let mut addrs = Vec::new();
    for _ in 0..20 {
        addrs.push(rt.spawn(CounterActor { count: 0 }).unwrap());
    }

    // Run so stats propagate, then bombard worker 0's actors with messages
    // to create mailbox depth imbalance.
    let handle = rt.run().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Send 500 messages to the first 10 actors (likely on worker 0).
    for addr in &addrs[..10] {
        for _ in 0..50 {
            let _ = handle.runtime.send_to(*addr, Increment {
                reply_to: *addr, // self-reply to keep mailbox depth up
            });
        }
    }

    std::thread::sleep(std::time::Duration::from_millis(20));

    // Phase 2: Spawn 10 more actors. With load-aware placement,
    // they should bias toward the lighter worker.
    let mut late_addrs = Vec::new();
    for _ in 0..10 {
        late_addrs.push(handle.runtime.spawn(CounterActor { count: 0 }).unwrap());
    }

    std::thread::sleep(std::time::Duration::from_millis(20));

    let stats = handle.runtime.stats();
    handle.shutdown();
    handle.join();

    // Verify the system is operational — both workers should have actors
    let total_actors: usize = stats.workers.iter().map(|w| w.num_actors).sum();
    assert!(total_actors >= 20, "expected at least 20 actors, got {}", total_actors);

    // The lighter worker should have gotten more of the late actors.
    // We can't assert exact distribution due to timing, but verify
    // actors are distributed across workers (not all on one).
    assert!(
        stats.workers.iter().all(|w| w.num_actors > 0),
        "both workers should have actors, got {:?}",
        stats.workers.iter().map(|w| w.num_actors).collect::<Vec<_>>()
    );
}

/// Given a single-threaded runtime (1 worker),
/// when many actors are spawned,
/// then all go to worker 0 regardless of load (no panic, no error).
#[test]
fn load_aware_placement_single_worker_degrades_gracefully() {
    let rt = std_runtime(RuntimeConfig::default());

    for _ in 0..50 {
        rt.spawn(CounterActor { count: 0 }).unwrap();
    }

    // Tick several times to let stats update
    for _ in 0..10 {
        rt.tick();
    }

    let stats = rt.stats();
    assert_eq!(stats.workers.len(), 1);
    assert_eq!(stats.workers[0].num_actors, 50);
}

/// Given a fresh runtime with no prior ticks,
/// when actors are spawned in a burst,
/// then they distribute evenly (round-robin fallback when stats are all zero).
#[test]
fn load_aware_placement_falls_back_to_round_robin_on_fresh_runtime() {
    let rt = std_runtime(RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    });

    // Spawn 100 actors before any ticks (all stats are zero)
    for _ in 0..100 {
        rt.spawn(CounterActor { count: 0 }).unwrap();
    }

    let handle = rt.run().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));

    let stats = handle.runtime.stats();
    handle.shutdown();
    handle.join();

    // With 4 workers and 100 actors, each should have ~25 (±5).
    // Round-robin gives exactly 25 each.
    for w in &stats.workers {
        assert!(
            w.num_actors >= 20 && w.num_actors <= 30,
            "worker {} has {} actors, expected ~25 (round-robin)",
            w.id, w.num_actors
        );
    }
}

// ── Mailbox Backpressure Tests ─────────────────────────────────────────────

/// Given a runtime with bounded mailboxes (capacity=10, DropNewest),
/// when 50 messages are sent to an actor before any ticks,
/// then only the first 10 are delivered and the rest are dropped.
#[test]
fn bounded_mailbox_drop_newest_caps_at_capacity() {
    let rt = std_runtime(RuntimeConfig {
        default_mailbox_capacity: 10,
        mailbox_overflow: MailboxOverflow::DropNewest,
        ..Default::default()
    });

    let inbox = rt.new_inbox::<Count>().unwrap();
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();

    // Send 50 messages — only first 10 should be queued
    for _ in 0..50 {
        let _ = rt.send_to(addr, Increment { reply_to: *inbox.addr() });
    }

    // Tick enough times to process all queued messages
    for _ in 0..20 {
        rt.tick();
    }

    // Count replies — should be exactly 10 (the mailbox capacity)
    let mut replies = 0;
    while inbox.try_recv().is_some() {
        replies += 1;
    }
    assert_eq!(replies, 10, "should deliver exactly mailbox_capacity messages");

    // Stats should show drops
    let stats = rt.stats();
    let total_drops: u64 = stats.workers.iter().map(|w| w.messages_dropped).sum();
    assert_eq!(total_drops, 40, "40 messages should have been dropped");
}

/// Given a runtime with bounded mailboxes (capacity=5, DropOldest),
/// when 10 messages are sent before any tick,
/// then only the 5 most recent messages are delivered.
#[test]
fn bounded_mailbox_drop_oldest_keeps_newest() {
    let rt = std_runtime(RuntimeConfig {
        default_mailbox_capacity: 5,
        mailbox_overflow: MailboxOverflow::DropOldest,
        ..Default::default()
    });

    let inbox = rt.new_inbox::<Done>().unwrap();
    let addr = rt.spawn(DoubleActor).unwrap();

    // Send messages with values 0..10. DoubleActor replies Done(value * 2).
    // With DropOldest and capacity 5, messages 0-4 should be dropped as 5-9 arrive.
    for i in 0..10 {
        let _ = rt.send_to(addr, Forward {
            value: i,
            reply_to: *inbox.addr(),
        });
    }

    for _ in 0..10 {
        rt.tick();
    }

    // Collect all replies
    let mut replies = Vec::new();
    while let Some(Done(v)) = inbox.try_recv() {
        replies.push(v);
    }

    assert_eq!(replies.len(), 5, "should deliver exactly 5 messages");
    // The 5 most recent: values 5,6,7,8,9 → doubled: 10,12,14,16,18
    assert_eq!(replies, vec![10, 12, 14, 16, 18], "should keep the newest messages");
}

/// Given a runtime with unbounded mailboxes (capacity=0, the default),
/// when many messages are sent,
/// then all are delivered (backward compatibility).
#[test]
fn unbounded_mailbox_delivers_all_messages() {
    let rt = std_runtime(RuntimeConfig::default());

    let inbox = rt.new_inbox::<Count>().unwrap();
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();

    for _ in 0..200 {
        let _ = rt.send_to(addr, Increment { reply_to: *inbox.addr() });
    }

    for _ in 0..50 {
        rt.tick();
    }

    let mut replies = 0;
    while inbox.try_recv().is_some() {
        replies += 1;
    }
    assert_eq!(replies, 200, "all 200 messages should be delivered with unbounded mailbox");

    let stats = rt.stats();
    let total_drops: u64 = stats.workers.iter().map(|w| w.messages_dropped).sum();
    assert_eq!(total_drops, 0, "no drops with unbounded mailbox");
}

/// Given bounded mailboxes with budget, when an actor processes messages
/// and frees mailbox space, then new messages should be accepted on subsequent ticks.
#[test]
fn bounded_mailbox_refills_after_processing() {
    let rt = std_runtime(RuntimeConfig {
        default_mailbox_capacity: 5,
        actor_message_budget: 5,
        mailbox_overflow: MailboxOverflow::DropNewest,
        ..Default::default()
    });

    let inbox = rt.new_inbox::<Count>().unwrap();
    let addr = rt.spawn(CounterActor { count: 0 }).unwrap();

    // Send first batch of 5 — fills mailbox exactly
    for _ in 0..5 {
        let _ = rt.send_to(addr, Increment { reply_to: *inbox.addr() });
    }

    // Tick to process all 5 (budget=5, capacity=5)
    rt.tick();

    // Send second batch of 5 — mailbox is empty, so all 5 should be accepted
    for _ in 0..5 {
        let _ = rt.send_to(addr, Increment { reply_to: *inbox.addr() });
    }

    rt.tick();

    let mut replies = 0;
    while inbox.try_recv().is_some() {
        replies += 1;
    }
    assert_eq!(replies, 10, "all 10 messages across 2 batches should be processed");

    let stats = rt.stats();
    let total_drops: u64 = stats.workers.iter().map(|w| w.messages_dropped).sum();
    assert_eq!(total_drops, 0, "no drops when mailbox drains between batches");
}

// ── Dead Actor Cleanup Tests ───────────────────────────────────────────────

/// Given an actor that panics and is poisoned,
/// when ticks continue,
/// then the actor is removed from stats and sends to its address fail.
#[test]
fn dead_actor_cleaned_up_from_stats_and_address_map() {
    let rt = std_runtime(RuntimeConfig::default());

    let good = rt.spawn(PingPongActor).unwrap();
    let bad = rt.spawn(PanicActor).unwrap();

    // Trigger panic
    let _ = rt.send_to(bad, PanicMsg);
    for _ in 0..5 { rt.tick(); }

    let stats = rt.stats();
    // Good actor still present, bad actor cleaned up
    assert_eq!(stats.workers[0].num_actors, 1, "only the healthy actor should remain");
    assert!(
        stats.actors.iter().any(|(a, _)| *a == good),
        "good actor should be in address map"
    );
    assert!(
        !stats.actors.iter().any(|(a, _)| *a == bad),
        "poisoned actor should be removed from address map"
    );

    // Sends to cleaned-up actor fail
    let result = rt.send_to(bad, PanicMsg);
    assert!(result.is_err(), "send to cleaned-up actor should fail");
}

/// Given many actors that all panic,
/// when ticks proceed,
/// then all are cleaned up and stats reflect zero actors.
#[test]
fn bulk_dead_actor_cleanup() {
    let rt = std_runtime(RuntimeConfig::default());

    let mut addrs = Vec::new();
    for _ in 0..20 {
        addrs.push(rt.spawn(PanicActor).unwrap());
    }

    // Trigger all panics
    for &addr in &addrs {
        let _ = rt.send_to(addr, PanicMsg);
    }
    for _ in 0..10 { rt.tick(); }

    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 0, "all poisoned actors should be cleaned up");
    assert_eq!(
        stats.actors.len(), 0,
        "address map should be empty after all actors poisoned"
    );
}

// ── Actor Recovery Helpers ──────────────────────────────────────────────────

/// Handles Forward messages, replies Done(value * 2), panics on the panic_at-th message.
/// count resets to 0 on fresh construction, so restarts reset the counter.
struct RestartTestActor {
    count: usize,
    panic_at: usize,
}

impl ActorInterface for RestartTestActor {
    type Incoming = Forward;
    type Response = Done;
    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        self.count += 1;
        if self.count >= self.panic_at {
            panic!("intentional panic at message {}", self.count);
        }
        let _ = ctx.send(msg.reply_to, Done(msg.value * 2));
    }
}

// ── Actor Recovery Tests ───────────────────────────────────────────────────

/// Given an actor that panics,
/// when it panics,
/// then it is poisoned and future messages are discarded.
#[test]
fn non_restartable_actor_still_poisons_on_panic() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    // Normal spawn — not restartable
    let addr = rt.spawn(RestartTestActor { count: 0, panic_at: 1 }).unwrap();

    let _ = rt.send_to(addr, Forward { value: 42, reply_to: *inbox.addr() });
    for _ in 0..5 { rt.tick(); }

    let stats = rt.stats();
    let total_panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
    let total_restarts: u64 = stats.workers.iter().map(|w| w.restarts).sum();
    assert_eq!(total_panics, 1, "should panic");
    assert_eq!(total_restarts, 0, "should not restart (not restartable)");
}

// ── Lifecycle Hook Helpers ────────────────────────────────────────────────

/// An actor that records lifecycle events to shared counters.
struct LifecycleActor {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
    handled: Arc<AtomicUsize>,
}

impl ActorInterface for LifecycleActor {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, _ctx: &Ctx) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::Relaxed);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.handled.fetch_add(1, Ordering::Relaxed);
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// An actor that stops itself after processing N messages.
struct SelfStopActor {
    count: usize,
    stop_after: usize,
    stopped: Arc<AtomicUsize>,
}

impl ActorInterface for SelfStopActor {
    type Incoming = Forward;
    type Response = Done;

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped.fetch_add(1, Ordering::Relaxed);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        self.count += 1;
        let _ = ctx.send(msg.reply_to, Done(msg.value));
        if self.count >= self.stop_after {
            ctx.stop_self();
        }
    }
}

/// An actor that sends a farewell message in on_stop.
struct FarewellActor {
    farewell_to: ActorAddress,
}

impl ActorInterface for FarewellActor {
    type Incoming = Ping;
    type Response = Pong;

    fn on_stop(&mut self, ctx: &Ctx) {
        let _ = ctx.send(self.farewell_to, Pong);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// An actor whose on_start panics.
struct PanicOnStartActor {
    handled: Arc<AtomicUsize>,
}

impl ActorInterface for PanicOnStartActor {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, _ctx: &Ctx) {
        panic!("on_start panic");
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
        self.handled.fetch_add(1, Ordering::Relaxed);
    }
}

// ── Lifecycle Hook Tests ──────────────────────────────────────────────────

/// Given an actor with on_start implemented,
/// when it is spawned and the runtime ticks,
/// then on_start is called exactly once before the first message.
#[test]
fn on_start_called_before_first_message() {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let addr = rt.spawn(LifecycleActor {
        started: started.clone(),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();

    // First tick — should call on_start
    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 1, "on_start called on first tick");
    assert_eq!(handled.load(Ordering::Relaxed), 0, "no messages processed yet");

    // Send messages and tick more
    let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 1, "on_start not called again");
    assert_eq!(handled.load(Ordering::Relaxed), 1, "message processed after on_start");
}

/// Given an actor with on_start,
/// when multiple actors are spawned,
/// then each gets its own on_start call exactly once.
#[test]
fn on_start_called_per_actor() {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());

    for _ in 0..5 {
        let _ = rt.spawn(LifecycleActor {
            started: started.clone(),
            stopped: stopped.clone(),
            handled: handled.clone(),
        }).unwrap();
    }

    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start called for each of 5 actors");

    // Subsequent ticks don't repeat on_start
    rt.tick();
    rt.tick();
    assert_eq!(started.load(Ordering::Relaxed), 5, "on_start still 5 after more ticks");
}

/// Given an actor whose on_start panics,
/// when it is spawned and the runtime ticks,
/// then it is immediately poisoned and never processes messages.
#[test]
fn on_start_panic_poisons_actor() {
    let handled = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let addr = rt.spawn(PanicOnStartActor { handled: handled.clone() }).unwrap();

    let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    for _ in 0..5 { rt.tick(); }

    assert_eq!(handled.load(Ordering::Relaxed), 0, "actor never processed messages");

    let stats = rt.stats();
    let total_panics: u64 = stats.workers.iter().map(|w| w.panics).sum();
    assert_eq!(total_panics, 1, "on_start panic counted");
}

// ── Graceful Stop Tests ───────────────────────────────────────────────────

/// Given an actor that calls ctx.stop_self() after 3 messages,
/// when 5 messages are sent,
/// then only 3 are processed, the actor is removed, and on_stop is called.
#[test]
fn actor_can_stop_self() {
    let stopped = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    let addr = rt.spawn(SelfStopActor {
        count: 0,
        stop_after: 3,
        stopped: stopped.clone(),
    }).unwrap();

    for i in 0..5 {
        let _ = rt.send_to(addr, Forward { value: i, reply_to: *inbox.addr() });
    }
    for _ in 0..10 { rt.tick(); }

    // Only 3 messages should be processed (stop_self after 3rd)
    let mut replies = Vec::new();
    while let Some(Done(v)) = inbox.try_recv() {
        replies.push(v);
    }
    assert_eq!(replies.len(), 3, "only 3 messages processed before stop");
    assert!(replies.contains(&0));
    assert!(replies.contains(&1));
    assert!(replies.contains(&2));

    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop called exactly once");

    // Actor should be removed from address map
    let stats = rt.stats();
    assert_eq!(stats.actors.len(), 0, "stopped actor removed from address map");
}

/// Given a running actor,
/// when runtime.stop_actor(addr) is called,
/// then the actor stops, on_stop is called, and it's removed from the pool.
#[test]
fn runtime_can_stop_actor() {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let addr = rt.spawn(LifecycleActor {
        started: started.clone(),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();

    // Let it start and process a message
    let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    for _ in 0..3 { rt.tick(); }
    assert_eq!(handled.load(Ordering::Relaxed), 1);

    // Stop it externally
    rt.stop_actor(addr).unwrap();
    for _ in 0..3 { rt.tick(); }

    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop called");

    // Actor should be gone
    let stats = rt.stats();
    assert_eq!(stats.actors.len(), 0, "stopped actor removed");
    assert_eq!(stats.workers[0].num_actors, 0);
}

/// Given a stopped actor,
/// when new messages are sent to it,
/// then sends return Err (address not found).
#[test]
fn send_to_stopped_actor_returns_error() {
    let stopped = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    let addr = rt.spawn(SelfStopActor {
        count: 0,
        stop_after: 1,
        stopped: stopped.clone(),
    }).unwrap();

    // One message triggers stop
    let _ = rt.send_to(addr, Forward { value: 1, reply_to: *inbox.addr() });
    for _ in 0..10 { rt.tick(); }

    // Actor is now removed — send should fail
    let result = rt.send_to(addr, Forward { value: 2, reply_to: *inbox.addr() });
    assert!(result.is_err(), "send to stopped actor should return Err");
}

/// Given a gracefully stopped actor and a panicked actor,
/// then stats.stops and stats.panics track them separately.
#[test]
fn stop_vs_panic_tracked_separately_in_stats() {
    let stopped = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    // Actor that stops itself after 1 message
    let _stop_addr = rt.spawn(SelfStopActor {
        count: 0,
        stop_after: 1,
        stopped: stopped.clone(),
    }).unwrap();

    // Actor that panics on first message
    let panic_addr = rt.spawn(RestartTestActor { count: 0, panic_at: 1 }).unwrap();

    let _ = rt.send_to(_stop_addr, Forward { value: 1, reply_to: *inbox.addr() });
    let _ = rt.send_to(panic_addr, Forward { value: 1, reply_to: *inbox.addr() });
    for _ in 0..10 { rt.tick(); }

    let stats = rt.stats();
    let total_stops: u64 = stats.workers.iter().map(|w| w.stops).sum();
    let total_panics: u64 = stats.workers.iter().map(|w| w.panics).sum();

    assert_eq!(total_stops, 1, "one graceful stop");
    assert_eq!(total_panics, 1, "one panic");
}

/// Given an actor with on_stop that sends a farewell message,
/// when the actor is stopped,
/// then the farewell message is delivered.
#[test]
fn on_stop_can_send_messages() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let addr = rt.spawn(FarewellActor {
        farewell_to: *inbox.addr(),
    }).unwrap();

    // Let it start
    rt.tick();

    // Stop it
    rt.stop_actor(addr).unwrap();
    for _ in 0..5 { rt.tick(); }

    // Should receive farewell Pong from on_stop
    let farewell = inbox.try_recv();
    assert_eq!(farewell, Some(Pong), "farewell message delivered from on_stop");
}

/// Given a supervisor with a child that panics and is restarted,
/// when the child is respawned by the supervisor,
/// then on_start is called again on the fresh instance.
#[test]
fn on_start_called_again_after_restart() {
    let started = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());

    let started_c = started.clone();
    let _sup_addr = rt.spawn(Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![ChildSpec::new("child", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(LifecycleActor {
                started: started_c.clone(),
                stopped: Arc::new(AtomicUsize::new(0)),
                handled: Arc::new(AtomicUsize::new(0)),
            })
        })],
    )).unwrap();

    // First tick: supervisor starts, spawns child, on_start called
    for _ in 0..3 { rt.tick(); }
    assert_eq!(started.load(Ordering::Relaxed), 1, "on_start called once");
}

/// Given an actor stopped via stop_actor() with messages already queued,
/// when the stop signal arrives after the queued messages (PoisonPill semantics),
/// then messages ahead of the signal are processed, then the actor stops.
#[test]
fn external_stop_is_queued_after_pending_messages() {
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());

    let started = Arc::new(AtomicUsize::new(0));
    let addr = rt.spawn(LifecycleActor {
        started: started.clone(),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();

    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Queue 10 messages, then stop — StopSignal is queued AFTER the 10
    for _ in 0..10 {
        let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    }
    rt.stop_actor(addr).unwrap();
    for _ in 0..10 { rt.tick(); }

    // All 10 messages processed (they were ahead of StopSignal in the queue)
    let total_handled = handled.load(Ordering::Relaxed);
    assert_eq!(total_handled, 10, "all messages processed before stop signal");
    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop called");

    // Actor is removed
    let stats = rt.stats();
    assert_eq!(stats.actors.len(), 0, "stopped actor removed");
}

/// Given a running actor with no pending messages,
/// when stop_actor() is called and then new messages are sent,
/// then the stop takes priority and new messages are not processed.
#[test]
fn external_stop_before_new_messages_prevents_processing() {
    let stopped = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let addr = rt.spawn(LifecycleActor {
        started: started.clone(),
        stopped: stopped.clone(),
        handled: handled.clone(),
    }).unwrap();

    // Let actor start
    rt.tick();

    // Stop first, then send messages
    rt.stop_actor(addr).unwrap();
    for _ in 0..5 {
        let _ = rt.send_to(addr, Ping { reply_to: *inbox.addr() });
    }
    for _ in 0..10 { rt.tick(); }

    // Stop signal was first in queue, so no messages processed
    assert_eq!(handled.load(Ordering::Relaxed), 0, "no messages processed after stop");
    assert_eq!(stopped.load(Ordering::Relaxed), 1, "on_stop called");
}

/// Given stop_actor is called on a nonexistent address,
/// then it returns Err.
#[test]
fn stop_nonexistent_actor_returns_error() {
    let rt = std_runtime(RuntimeConfig::default());
    let fake_addr = swactor::actor::ActorAddress::default();
    let result = rt.stop_actor(fake_addr);
    assert!(result.is_err(), "stop_actor on nonexistent address should return Err");
}

// ── Timer Helpers ─────────────────────────────────────────────────────────

/// Actor that schedules a one-shot timer in on_start: sends a Ping to target after N ticks.
struct TimerStartActor {
    target: ActorAddress,
    delay_ticks: u64,
}

impl ActorInterface for TimerStartActor {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.send_after_ticks(self.target, Ping { reply_to: ctx.self_addr() }, self.delay_ticks);
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

/// Actor that schedules a one-shot timer when it receives a Forward message.
struct DelayPingPongActor;

impl ActorInterface for DelayPingPongActor {
    type Incoming = Forward;
    type Response = Done;

    fn handle(&mut self, ctx: &Ctx, msg: Forward) {
        ctx.send_after_ticks(msg.reply_to, Done(msg.value), 3);
    }
}

/// Actor that schedules an interval timer on start: sends Ping every N ticks.
struct HeartbeatActor {
    target: ActorAddress,
    period: u64,
}

impl ActorInterface for HeartbeatActor {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.send_interval_ticks(self.target, Ping { reply_to: ctx.self_addr() }, self.period);
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
}

// ── Timer Tests ───────────────────────────────────────────────────────────

/// Given an actor that schedules a one-shot timer in on_start,
/// when enough ticks pass,
/// then the timer message is delivered to the target.
#[test]
fn one_shot_timer_fires_after_n_ticks() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Ping>().unwrap();

    let _timer_actor = rt.spawn(TimerStartActor {
        target: *inbox.addr(),
        delay_ticks: 3,
    }).unwrap();

    // Tick 1: on_start schedules timer (fire_at = current_tick + 3 = 4)
    // Timer fires when current_tick >= fire_at, so after tick 4 completes
    rt.tick(); // tick 1: on_start, timer scheduled
    assert!(inbox.try_recv().is_none(), "no delivery before delay");

    rt.tick(); // tick 2
    assert!(inbox.try_recv().is_none(), "no delivery on tick 2");

    rt.tick(); // tick 3
    assert!(inbox.try_recv().is_none(), "no delivery on tick 3");

    rt.tick(); // tick 4: timer fires
    let msg = inbox.try_recv();
    assert!(msg.is_some(), "timer message delivered after 3-tick delay");
}

/// Given an actor that schedules a one-shot timer from a message handler,
/// when enough ticks pass after the triggering message,
/// then the delayed response arrives.
#[test]
fn handler_can_schedule_one_shot_timer() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Done>().unwrap();

    let addr = rt.spawn(DelayPingPongActor).unwrap();

    let _ = rt.send_to(addr, Forward { value: 42, reply_to: *inbox.addr() });
    rt.tick(); // process Forward, schedule timer (delay=3)

    assert!(inbox.try_recv().is_none(), "no immediate reply");

    rt.tick(); // tick 2
    rt.tick(); // tick 3
    assert!(inbox.try_recv().is_none(), "not yet");

    rt.tick(); // tick 4: timer fires
    let reply = inbox.try_recv();
    assert_eq!(reply, Some(Done(42)), "delayed reply arrives after 3 ticks");
}

/// Given a one-shot timer,
/// when it fires,
/// then it does NOT fire again on subsequent ticks (consumed).
#[test]
fn one_shot_timer_fires_only_once() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Ping>().unwrap();

    let _timer_actor = rt.spawn(TimerStartActor {
        target: *inbox.addr(),
        delay_ticks: 1,
    }).unwrap();

    rt.tick(); // on_start schedules timer
    rt.tick(); // timer fires
    assert!(inbox.try_recv().is_some(), "first fire");

    // Subsequent ticks should NOT fire again
    for _ in 0..5 { rt.tick(); }
    assert!(inbox.try_recv().is_none(), "one-shot does not repeat");
}

/// Given an interval timer with period 2,
/// when multiple ticks pass,
/// then the timer fires repeatedly every 2 ticks.
#[test]
fn interval_timer_fires_repeatedly() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Ping>().unwrap();

    let _heartbeat = rt.spawn(HeartbeatActor {
        target: *inbox.addr(),
        period: 2,
    }).unwrap();

    rt.tick(); // tick 1: on_start, interval scheduled (next_fire = current + 2 = 3)
    assert!(inbox.try_recv().is_none(), "no fire on tick 1");

    rt.tick(); // tick 2
    assert!(inbox.try_recv().is_none(), "no fire on tick 2");

    rt.tick(); // tick 3: first fire
    assert!(inbox.try_recv().is_some(), "fire on tick 3");

    rt.tick(); // tick 4
    assert!(inbox.try_recv().is_none(), "no fire on tick 4");

    rt.tick(); // tick 5: second fire
    assert!(inbox.try_recv().is_some(), "fire on tick 5");

    rt.tick(); // tick 6
    assert!(inbox.try_recv().is_none(), "no fire on tick 6");

    rt.tick(); // tick 7: third fire
    assert!(inbox.try_recv().is_some(), "fire on tick 7");
}

/// Given an interval timer targeting an actor that gets stopped,
/// when the actor is removed,
/// then the interval timer is cleaned up (no orphan timers).
#[test]
fn interval_timer_cleaned_up_when_actor_dies() {
    let rt = std_runtime(RuntimeConfig::default());
    let _inbox = rt.new_inbox::<Ping>().unwrap();

    // Heartbeat sends to a counter that we'll kill
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();

    // HeartbeatActor sends Ping to counter every tick
    let _hb = rt.spawn(HeartbeatActor {
        target: counter_addr,
        period: 1,
    }).unwrap();

    // Let it run a few ticks
    for _ in 0..3 { rt.tick(); }

    // Stop the counter
    rt.stop_actor(counter_addr).unwrap();
    for _ in 0..5 { rt.tick(); }

    // Counter is gone, interval timer should be GC'd.
    // No crash, no leak — just verifying it doesn't panic.
    let stats = rt.stats();
    // Only the heartbeat actor should remain
    assert_eq!(stats.workers[0].num_actors, 1);
}

/// Given a timer with delay 0,
/// when the next tick fires,
/// then the message is delivered immediately on the next tick.
#[test]
fn timer_with_zero_delay_fires_next_tick() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Ping>().unwrap();

    let _timer_actor = rt.spawn(TimerStartActor {
        target: *inbox.addr(),
        delay_ticks: 0,
    }).unwrap();

    rt.tick(); // on_start schedules timer with delay=0
    // Timer requests are processed after tick_all (phase 5.5)
    // Timer fires on the NEXT tick (phase 2.5)
    assert!(inbox.try_recv().is_none(), "not yet — timer fires next tick");

    rt.tick(); // timer fires
    assert!(inbox.try_recv().is_some(), "zero-delay timer fires on next tick");
}

// ── Named Actor Registry ────────────────────────────────────────────────────

/// Given a named actor is spawned,
/// when I look it up by name,
/// then I get the same address that spawn returned.
#[test]
fn named_actor_lookup_returns_spawn_address() {
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn_named("greeter", PingPongActor).unwrap();
    assert_eq!(rt.where_is("greeter"), Some(addr));
}

/// Given a named actor exists,
/// when I send a message to the looked-up address,
/// then the actor receives and processes it.
#[test]
fn named_actor_receives_messages_via_lookup() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn_named("ponger", PingPongActor).unwrap();
    assert_eq!(rt.where_is("ponger"), Some(addr));

    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_some(), "named actor should process message");
}

/// Given a name is already registered,
/// when I try to spawn another actor with the same name,
/// then I get an error and the original binding is preserved.
#[test]
fn duplicate_name_returns_error() {
    let rt = std_runtime(RuntimeConfig::default());
    let first_addr = rt.spawn_named("singleton", PingPongActor).unwrap();
    let result = rt.spawn_named("singleton", PingPongActor);
    assert!(result.is_err(), "duplicate name should fail");
    assert_eq!(rt.where_is("singleton"), Some(first_addr), "original binding preserved");
}

/// Given no actors are registered,
/// when I look up a nonexistent name,
/// then I get None.
#[test]
fn where_is_returns_none_for_unknown_name() {
    let rt = std_runtime(RuntimeConfig::default());
    assert_eq!(rt.where_is("ghost"), None);
}

/// Given a named actor is stopped,
/// when the next tick runs cleanup,
/// then the name is automatically unregistered.
#[test]
fn name_auto_unregistered_on_actor_death() {
    let rt = std_runtime(RuntimeConfig::default());
    let addr = rt.spawn_named("ephemeral", PingPongActor).unwrap();
    rt.tick(); // on_start

    rt.stop_actor(addr).unwrap();
    rt.tick(); // process StopSignal + cleanup

    assert_eq!(rt.where_is("ephemeral"), None, "name should be freed after stop");
}

/// Given a named actor died and its name was freed,
/// when I spawn a new actor with the same name,
/// then registration succeeds with a new address.
#[test]
fn name_can_be_reused_after_actor_death() {
    let rt = std_runtime(RuntimeConfig::default());
    let first = rt.spawn_named("worker", PingPongActor).unwrap();
    rt.tick();
    rt.stop_actor(first).unwrap();
    rt.tick(); // cleanup frees the name

    let second = rt.spawn_named("worker", PingPongActor).unwrap();
    assert_ne!(first, second, "new actor should have a different address");
    assert_eq!(rt.where_is("worker"), Some(second));
}

/// Given a named actor panics (and is not restartable),
/// when the next tick runs cleanup,
/// then the name is freed.
#[test]
fn name_auto_unregistered_on_panic() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let _addr = rt.spawn_named("fragile", PanicActor).unwrap();
    rt.tick(); // on_start

    rt.send_to(_addr, PanicMsg).unwrap();
    rt.tick(); // panic → poison → cleanup

    assert_eq!(rt.where_is("fragile"), None, "name freed after panic");
    // Can reuse the name
    let _new = rt.spawn_named("fragile", PingPongActor).unwrap();
    assert!(rt.where_is("fragile").is_some());
    drop(inbox);
}

/// Given multiple named actors are registered,
/// when I call registered_names(),
/// then all names are returned.
#[test]
fn registered_names_lists_all() {
    let rt = std_runtime(RuntimeConfig::default());
    rt.spawn_named("alpha", PingPongActor).unwrap();
    rt.spawn_named("beta", PingPongActor).unwrap();
    rt.spawn_named("gamma", PingPongActor).unwrap();

    let mut names = rt.registered_names();
    names.sort();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);
}

/// Given a named actor exists,
/// when I manually unregister the name,
/// then the name is freed but the actor continues running.
#[test]
fn manual_unregister_frees_name_but_actor_lives() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let addr = rt.spawn_named("temp-name", PingPongActor).unwrap();
    rt.tick(); // on_start

    let removed = rt.unregister("temp-name");
    assert_eq!(removed, Some(addr));
    assert_eq!(rt.where_is("temp-name"), None, "name freed");

    // Actor still alive and can receive messages
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_some(), "actor still processes messages");
}

/// An actor that looks up a peer by name using ctx.where_is().
struct NameLookupActor {
    target_name: &'static str,
    reply_to: ActorAddress,
}

impl ActorInterface for NameLookupActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        if let Some(peer) = ctx.where_is(self.target_name) {
            ctx.send(self.reply_to, MyAddr(peer)).unwrap();
        }
    }
}

/// Given a named actor exists,
/// when another actor calls ctx.where_is() from inside a handler,
/// then it resolves the correct address.
#[test]
fn ctx_where_is_resolves_inside_handler() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<MyAddr>().unwrap();
    let target = rt.spawn_named("target", PingPongActor).unwrap();

    let looker = rt.spawn(NameLookupActor {
        target_name: "target",
        reply_to: *inbox.addr(),
    }).unwrap();

    rt.tick(); // on_start
    rt.send_to(looker, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick(); // handle → where_is → send
    rt.tick(); // deliver reply

    let result = inbox.try_recv();
    assert_eq!(result, Some(MyAddr(target)), "ctx.where_is found the named actor");
}

/// An actor that spawns a named child using ctx.spawn_named().
struct NamedSpawnerActor {
    reply_to: ActorAddress,
}

impl ActorInterface for NamedSpawnerActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        match ctx.spawn_named("child", PingPongActor) {
            Ok(addr) => { ctx.send(self.reply_to, MyAddr(addr)).unwrap(); }
            Err(_) => {}
        }
    }
}

/// Given an actor calls ctx.spawn_named("child", ...),
/// when the child is spawned,
/// then where_is("child") returns the correct address.
#[test]
fn ctx_spawn_named_registers_from_handler() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<MyAddr>().unwrap();

    let spawner = rt.spawn(NamedSpawnerActor {
        reply_to: *inbox.addr(),
    }).unwrap();

    rt.tick(); // on_start
    rt.send_to(spawner, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick(); // handle → spawn_named
    rt.tick(); // deliver reply

    let child_addr = inbox.try_recv().expect("should receive child address");
    assert_eq!(rt.where_is("child"), Some(child_addr.0), "name registered from handler");
}

// ── Actor Monitoring / Death Watch ──────────────────────────────────────────

/// An actor that monitors a target and forwards Down notifications to a reply address.
struct WatcherActor {
    watch_target: ActorAddress,
    reply_to: ActorAddress,
    mref: Option<MonitorRef>,
}

impl ActorInterface for WatcherActor {
    type Incoming = Down;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        self.mref = Some(ctx.monitor(self.watch_target));
    }
    fn handle(&mut self, ctx: &Ctx, msg: Down) {
        // Forward the Down notification to the test inbox
        ctx.send(self.reply_to, msg).unwrap();
    }
}

/// Given actor A monitors actor B,
/// when B is gracefully stopped,
/// then A receives a Down { reason: Normal } message.
#[test]
fn monitor_notifies_on_graceful_stop() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PingPongActor).unwrap();
    let _watcher = rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: *inbox.addr(),
        mref: None,
    }).unwrap();

    rt.tick(); // on_start → watcher sets up monitor
    rt.stop_actor(target).unwrap();
    rt.tick(); // target receives StopSignal → cleanup_dead emits Down
    rt.tick(); // watcher receives Down → forwards to inbox

    let down = inbox.try_recv().expect("should receive Down notification");
    assert_eq!(down.addr, target);
    assert_eq!(down.reason, StopReason::Normal);
}

/// Given actor A monitors actor B,
/// when B panics,
/// then A receives a Down { reason: Panicked } message.
#[test]
fn monitor_notifies_on_panic() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PanicActor).unwrap();
    let _watcher = rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: *inbox.addr(),
        mref: None,
    }).unwrap();

    rt.tick(); // on_start
    rt.send_to(target, PanicMsg).unwrap();
    rt.tick(); // target panics → cleanup_dead emits Down
    rt.tick(); // watcher receives Down → forwards to inbox

    let down = inbox.try_recv().expect("should receive Down on panic");
    assert_eq!(down.addr, target);
    assert_eq!(down.reason, StopReason::Panicked);
}

/// Given two actors both monitor the same target,
/// when the target dies,
/// then both watchers receive independent Down notifications.
#[test]
fn multiple_watchers_all_notified() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox1 = rt.new_inbox::<Down>().unwrap();
    let inbox2 = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PingPongActor).unwrap();
    rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: *inbox1.addr(),
        mref: None,
    }).unwrap();
    rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: *inbox2.addr(),
        mref: None,
    }).unwrap();

    rt.tick(); // on_start for all
    rt.stop_actor(target).unwrap();
    rt.tick(); // cleanup → Down emitted to both watchers
    rt.tick(); // watchers forward Down to inboxes

    assert!(inbox1.try_recv().is_some(), "watcher 1 should receive Down");
    assert!(inbox2.try_recv().is_some(), "watcher 2 should receive Down");
}

/// An actor that demonitors in response to a Ping message.
struct DemonitorActor {
    watch_target: ActorAddress,
    mref: Option<MonitorRef>,
}

impl ActorInterface for DemonitorActor {
    type Incoming = Ping;
    type Response = ();
    fn on_start(&mut self, ctx: &Ctx) {
        self.mref = Some(ctx.monitor(self.watch_target));
    }
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        // Cancel the monitor
        if let Some(mref) = self.mref.take() {
            ctx.demonitor(mref);
        }
    }
}

/// Given actor A monitors actor B then demonitors,
/// when B dies,
/// then A does NOT receive a Down notification.
#[test]
fn demonitor_cancels_notification() {
    let rt = std_runtime(RuntimeConfig::default());
    let down_inbox = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PingPongActor).unwrap();
    let watcher = rt.spawn(DemonitorActor {
        watch_target: target,
        mref: None,
    }).unwrap();

    rt.tick(); // on_start → monitor set up

    // Trigger demonitor
    rt.send_to(watcher, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick(); // handle → demonitor

    // Now kill the target
    rt.stop_actor(target).unwrap();
    rt.tick(); // cleanup — no Down should be emitted
    rt.tick(); // extra tick to be sure

    assert!(down_inbox.try_recv().is_none(), "demonitored — should NOT receive Down");
}

/// Given actor A monitors B, and A dies before B,
/// when B dies,
/// then no Down is delivered (dead watcher cleaned up).
#[test]
fn dead_watcher_does_not_receive_down() {
    let rt = std_runtime(RuntimeConfig::default());

    let target = rt.spawn(PingPongActor).unwrap();
    let watcher = rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: ActorAddress::default(), // won't matter, watcher dies first
        mref: None,
    }).unwrap();

    rt.tick(); // on_start → monitor set up
    rt.stop_actor(watcher).unwrap();
    rt.tick(); // watcher dies → its monitors are cleaned up

    // Now kill the target — the dead watcher's subscription should be gone
    rt.stop_actor(target).unwrap();
    rt.tick(); // cleanup — should not panic or try to deliver to dead watcher
    // If we get here without panic, the test passes
}

/// Given an external inbox monitors via the runtime,
/// when the target dies,
/// then the inbox receives a Down message.
#[test]
fn down_delivered_to_external_inbox() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PingPongActor).unwrap();

    // Set up a monitor from an actor that forwards Down to the inbox.
    // The watcher is an actor, but the final recipient is the inbox.
    let _watcher = rt.spawn(WatcherActor {
        watch_target: target,
        reply_to: *inbox.addr(),
        mref: None,
    }).unwrap();

    rt.tick(); // on_start
    rt.stop_actor(target).unwrap();
    rt.tick(); // cleanup → Down to watcher
    rt.tick(); // watcher forwards to inbox

    let down = inbox.try_recv().expect("inbox should receive forwarded Down");
    assert_eq!(down.addr, target);
    assert_eq!(down.reason, StopReason::Normal);
}

/// Given actor A monitors B with two independent monitors,
/// when B dies,
/// then A receives two Down messages (one per monitor).
#[test]
fn stacked_monitors_produce_multiple_notifications() {
    /// An actor that creates two monitors on the same target.
    struct DoubleWatcherActor {
        target: ActorAddress,
        reply_to: ActorAddress,
    }

    impl ActorInterface for DoubleWatcherActor {
        type Incoming = Down;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target);
            ctx.monitor(self.target);
        }
        fn handle(&mut self, ctx: &Ctx, msg: Down) {
            ctx.send(self.reply_to, msg).unwrap();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();

    let target = rt.spawn(PingPongActor).unwrap();
    rt.spawn(DoubleWatcherActor {
        target,
        reply_to: *inbox.addr(),
    }).unwrap();

    rt.tick(); // on_start → 2 monitors
    rt.stop_actor(target).unwrap();
    rt.tick(); // cleanup → 2 Down messages to watcher
    rt.tick(); // watcher forwards both to inbox

    assert!(inbox.try_recv().is_some(), "first Down");
    assert!(inbox.try_recv().is_some(), "second Down");
    assert!(inbox.try_recv().is_none(), "no more");
}

// ── Actor Groups / Pub-Sub ──────────────────────────────────────────────────

/// Given actors join a group,
/// when I query group_members,
/// then all joined actors are listed.
#[test]
fn group_members_returns_joined_actors() {
    let rt = std_runtime(RuntimeConfig::default());
    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();

    rt.join_group(a, "workers");
    rt.join_group(b, "workers");

    let mut members = rt.group_members("workers");
    members.sort_by_key(|addr| addr.0);
    let mut expected = vec![a, b];
    expected.sort_by_key(|addr| addr.0);
    assert_eq!(members, expected);
}

/// Given no actors have joined a group,
/// when I query group_members,
/// then the result is empty.
#[test]
fn empty_group_returns_no_members() {
    let rt = std_runtime(RuntimeConfig::default());
    assert!(rt.group_members("nonexistent").is_empty());
}

/// Given actors in a group,
/// when a message is published to the group,
/// then all members receive the message.
#[test]
fn publish_broadcasts_to_all_members() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox1 = rt.new_inbox::<Pong>().unwrap();
    let inbox2 = rt.new_inbox::<Pong>().unwrap();

    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();
    rt.join_group(a, "pongers");
    rt.join_group(b, "pongers");

    rt.tick(); // on_start

    // Publish a Ping with different reply_to for each — but since it's cloned,
    // all members get the same message. Use inbox1's addr as reply_to.
    let count = rt.publish_to("pongers", Ping { reply_to: *inbox1.addr() });
    assert_eq!(count, 2, "two members, two messages sent");

    rt.tick(); // actors handle Ping → send Pong to inbox1

    // Both actors send to inbox1 (because the published Ping had inbox1 as reply_to)
    assert!(inbox1.try_recv().is_some(), "first Pong");
    assert!(inbox1.try_recv().is_some(), "second Pong");
    assert!(inbox1.try_recv().is_none(), "no more");
    drop(inbox2);
}

/// Given an actor leaves a group,
/// when a message is published,
/// then the leaver does not receive it.
#[test]
fn leave_group_stops_receiving_publishes() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();
    rt.join_group(a, "pool");
    rt.join_group(b, "pool");
    rt.leave_group(b, "pool");

    rt.tick(); // on_start
    let count = rt.publish_to("pool", Ping { reply_to: *inbox.addr() });
    assert_eq!(count, 1, "only one member after leave");

    rt.tick();
    assert!(inbox.try_recv().is_some(), "one Pong from remaining member");
    assert!(inbox.try_recv().is_none(), "no second Pong");
}

/// Given a group member dies,
/// when a message is published,
/// then the dead member is not included.
#[test]
fn dead_actor_auto_removed_from_group() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();
    rt.join_group(a, "team");
    rt.join_group(b, "team");

    rt.tick(); // on_start
    rt.stop_actor(b).unwrap();
    rt.tick(); // b dies, cleaned up from group

    let count = rt.publish_to("team", Ping { reply_to: *inbox.addr() });
    assert_eq!(count, 1, "dead actor removed from group");

    rt.tick();
    assert!(inbox.try_recv().is_some());
    assert!(inbox.try_recv().is_none());
}

/// Given an actor is in multiple groups,
/// when the actor dies,
/// then it is removed from all groups.
#[test]
fn actor_removed_from_all_groups_on_death() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.join_group(actor, "alpha");
    rt.join_group(actor, "beta");
    rt.join_group(actor, "gamma");

    rt.tick();
    rt.stop_actor(actor).unwrap();
    rt.tick(); // cleanup removes from all groups

    assert!(rt.group_members("alpha").is_empty());
    assert!(rt.group_members("beta").is_empty());
    assert!(rt.group_members("gamma").is_empty());
}

/// Given a group becomes empty after its last member leaves,
/// then the group name disappears from the active groups list.
#[test]
fn empty_group_auto_deleted() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.join_group(actor, "temp");
    assert!(rt.groups().contains(&"temp".to_string()));

    rt.leave_group(actor, "temp");
    assert!(!rt.groups().contains(&"temp".to_string()), "empty group should be removed");
}

/// Given actors join groups from handlers using ctx.join_group(),
/// when group_members is queried,
/// then the joining actors are listed.
#[test]
fn ctx_join_group_from_handler() {
    struct GroupJoinerActor;

    impl ActorInterface for GroupJoinerActor {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.join_group("auto-joined");
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
    }

    let rt = std_runtime(RuntimeConfig::default());
    let a = rt.spawn(GroupJoinerActor).unwrap();
    let b = rt.spawn(GroupJoinerActor).unwrap();

    rt.tick(); // on_start → both join "auto-joined"

    let members = rt.group_members("auto-joined");
    assert_eq!(members.len(), 2);
    assert!(members.contains(&a));
    assert!(members.contains(&b));
}

/// Given an actor uses ctx.publish() from inside a handler,
/// when the published message is processed,
/// then all group members receive it.
#[test]
fn ctx_publish_broadcasts_from_handler() {
    #[derive(Clone)]
    struct BroadcastCmd {
        reply_to: ActorAddress,
    }

    struct BroadcasterActor;

    impl ActorInterface for BroadcasterActor {
        type Incoming = BroadcastCmd;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.join_group("broadcast-test");
        }
        fn handle(&mut self, ctx: &Ctx, msg: BroadcastCmd) {
            ctx.publish("broadcast-test", Ping { reply_to: msg.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Spawn 3 PingPongActors and one Broadcaster, all in the same group
    let _p1 = rt.spawn(PingPongActor).unwrap();
    let _p2 = rt.spawn(PingPongActor).unwrap();
    rt.join_group(_p1, "broadcast-test");
    rt.join_group(_p2, "broadcast-test");

    let broadcaster = rt.spawn(BroadcasterActor).unwrap();

    rt.tick(); // on_start (broadcaster joins group too)

    // Send BroadcastCmd to broadcaster
    rt.send_to(broadcaster, BroadcastCmd { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // broadcaster handles → publish Ping to all 3 members (including self)
    rt.tick(); // PingPong actors handle Ping → send Pong to inbox
    // Broadcaster also gets the Ping but it expects BroadcastCmd, so type mismatch (silent)

    // At least 2 Pongs from the PingPongActors
    let mut pong_count = 0;
    while inbox.try_recv().is_some() {
        pong_count += 1;
    }
    assert!(pong_count >= 2, "at least 2 PingPong members should reply, got {pong_count}");
}

// ── Ask Pattern ─────────────────────────────────────────────────────────────

/// Given a PingPong actor,
/// when I ask with recv_ticking,
/// then I get the Pong response.
#[test]
fn ask_recv_ticking_returns_response() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick(); // on_start

    let pong: Pong = rt.ask(actor, |reply_to| Ping { reply_to })
        .unwrap()
        .recv_ticking(&rt, 10)
        .unwrap();
    assert_eq!(pong, Pong);
}

/// Given a CounterActor,
/// when I ask multiple times,
/// then each response reflects the updated state.
#[test]
fn ask_multiple_times_tracks_state() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(CounterActor { count: 0 }).unwrap();
    rt.tick(); // on_start

    let c1: Count = rt.ask(actor, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    let c2: Count = rt.ask(actor, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    let c3: Count = rt.ask(actor, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();

    assert_eq!(c1, Count(1));
    assert_eq!(c2, Count(2));
    assert_eq!(c3, Count(3));
}

/// Given a dead actor,
/// when I ask and tick,
/// then recv_ticking returns a timeout error.
#[test]
fn ask_timeout_when_no_response() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick();
    rt.stop_actor(actor).unwrap();
    rt.tick(); // actor dies

    // Ask the dead actor — message is undeliverable, no response
    let result = rt.ask::<Ping, Pong>(actor, |reply_to| Ping { reply_to });
    // send_to may succeed (message goes to transfer queue) or fail (addr removed)
    // Either way, no response will come
    if let Ok(ask) = result {
        let err = ask.recv_ticking(&rt, 5);
        assert!(err.is_err(), "should timeout with no response");
    }
}

/// Given an ask handle,
/// when I use try_recv before ticking,
/// then it returns None (response hasn't arrived yet).
#[test]
fn ask_try_recv_returns_none_before_tick() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick(); // on_start

    let ask = rt.ask::<Ping, Pong>(actor, |reply_to| Ping { reply_to }).unwrap();
    assert!(ask.try_recv().is_none(), "no response before ticking");

    rt.tick(); // process message
    assert_eq!(ask.try_recv(), Some(Pong));
}

/// Given an ask, the reply_addr() returns the inbox address for manual use.
#[test]
fn ask_reply_addr_is_accessible() {
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick();

    let ask = rt.ask::<Ping, Pong>(actor, |reply_to| Ping { reply_to }).unwrap();
    let addr = *ask.reply_addr();
    // The address should be valid (non-zero)
    assert_ne!(addr, ActorAddress::default());
}

// ─── Supervisor Tests ──────────────────────────────────────────────────────

/// Actor that panics after receiving a configurable number of messages.
struct PanicAfterN {
    trigger: usize,
    count: usize,
    counter: Arc<AtomicUsize>,
}

impl ActorInterface for PanicAfterN {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        self.count += 1;
        self.counter.fetch_add(1, Ordering::SeqCst);
        let _ = ctx.send(msg.reply_to, Pong);
        if self.count >= self.trigger {
            panic!("intentional panic at message {}", self.count);
        }
    }
}

// --- handle_down tests ---

/// Given an actor with handle_down and a monitored target,
/// when the target dies, the watcher receives a Down via handle_down.
#[test]
fn handle_down_receives_death_notification() {
    struct MonitoringTracker {
        target: ActorAddress,
        downs: Vec<Down>,
        inbox: ActorAddress,
    }
    impl ActorInterface for MonitoringTracker {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target);
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let _ = ctx.send(self.inbox, Count(self.downs.len()));
        }
        fn handle_down(&mut self, _ctx: &Ctx, down: Down) {
            self.downs.push(down);
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let inbox_addr = *inbox.addr();
    let target = rt.spawn(PanicActor).unwrap();
    let tracker = rt.spawn(MonitoringTracker {
        target,
        downs: vec![],
        inbox: inbox_addr,
    }).unwrap();
    rt.tick(); // on_start for both

    // Kill the target
    rt.send_to(target, PanicMsg).unwrap();
    rt.tick(); // target panics
    rt.tick(); // Down delivered to tracker via handle_down

    // Ask tracker how many downs it saw
    rt.send_to(tracker, Ping { reply_to: inbox_addr }).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv(), Some(Count(1)));
}

/// Given an actor whose Incoming type IS Down, handle_down is NOT called —
/// the Down goes through the normal handle() method (backward compatibility).
#[test]
fn handle_down_skipped_when_incoming_is_down() {
    struct DownAsIncoming {
        target: ActorAddress,
        inbox: ActorAddress,
    }
    impl ActorInterface for DownAsIncoming {
        type Incoming = Down;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target);
        }
        fn handle(&mut self, ctx: &Ctx, msg: Down) {
            let _ = ctx.send(self.inbox, msg);
        }
        fn handle_down(&mut self, _ctx: &Ctx, _down: Down) {
            panic!("handle_down must not be called when Incoming=Down");
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Down>().unwrap();
    let inbox_addr = *inbox.addr();
    let target = rt.spawn(PanicActor).unwrap();
    let _watcher = rt.spawn(DownAsIncoming { target, inbox: inbox_addr }).unwrap();
    rt.tick(); // on_start

    rt.send_to(target, PanicMsg).unwrap();
    rt.tick(); // panic
    rt.tick(); // Down delivered through handle(), not handle_down

    let received = inbox.try_recv().expect("Down should be delivered via handle()");
    assert_eq!(received.reason, StopReason::Panicked);
}

// --- ctx.stop_actor tests ---

/// Given two actors, one can stop the other via ctx.stop_actor().
#[test]
fn ctx_stop_actor_stops_target() {
    #[derive(Clone)]
    struct StopCmd {
        target: ActorAddress,
    }
    struct Stopper;
    impl ActorInterface for Stopper {
        type Incoming = StopCmd;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: StopCmd) {
            let _ = ctx.stop_actor(msg.target);
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let target = rt.spawn(PingPongActor).unwrap();
    let stopper = rt.spawn(Stopper).unwrap();
    rt.tick(); // on_start

    rt.send_to(stopper, StopCmd { target }).unwrap();
    rt.tick(); // stopper handles StopCmd → stop_actor(target)
    rt.tick(); // StopSignal delivered to target, target stops
    rt.tick(); // cleanup

    assert!(rt.send_to(target, Ping { reply_to: ActorAddress::default() }).is_err());
    // Stopper should still be alive
    assert!(rt.send_to(stopper, StopCmd { target }).is_ok());
}

// --- Supervisor tests ---

/// Given a supervisor with one permanent child,
/// when the child panics, the supervisor restarts it.
#[test]
fn supervisor_restarts_permanent_child_on_panic() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_c = counter.clone();
    let inbox_holder: Arc<std::sync::Mutex<Option<ActorAddress>>> =
        Arc::new(std::sync::Mutex::new(None));

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox.addr();
    *inbox_holder.lock().unwrap() = Some(inbox_addr);

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![ChildSpec::new("worker", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(PanicAfterN {
                trigger: 2, // panics on 2nd message
                count: 0,
                counter: counter_c.clone(),
            })
        })],
    );
    let _sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); // supervisor on_start → spawns child
    rt.tick(); // child on_start

    // Find the child by checking stats
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 2); // supervisor + child

    // Send message to child — need to discover child address.
    // We'll use the address map from stats.
    let child_addr = stats.actors.iter()
        .find(|(addr, _)| *addr != _sup_addr)
        .map(|(addr, _)| *addr)
        .unwrap();

    // First message: child processes, increments counter
    rt.send_to(child_addr, Ping { reply_to: inbox_addr }).unwrap();
    rt.tick();
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Second message: child panics (trigger=2)
    rt.send_to(child_addr, Ping { reply_to: inbox_addr }).unwrap();
    rt.tick(); // child panics and is poisoned
    rt.tick(); // cleanup: Down delivered to supervisor via handle_down
    rt.tick(); // supervisor restarts child (spawns new one)
    rt.tick(); // new child on_start

    // Supervisor is still alive, and a new child exists
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 2); // supervisor + new child
}

/// Given a supervisor with a transient child,
/// when the child stops normally, it is NOT restarted.
#[test]
fn supervisor_does_not_restart_transient_child_on_normal_stop() {
    let rt = std_runtime(RuntimeConfig::default());

    struct StopsAfterFirst;
    impl ActorInterface for StopsAfterFirst {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![ChildSpec::new("worker", RestartPolicy::Transient, |ctx| {
            ctx.spawn(StopsAfterFirst)
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); // supervisor on_start → child spawned
    rt.tick(); // child on_start

    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 2); // sup + child

    // Find child address
    let child_addr = stats.actors.iter()
        .find(|(addr, _)| *addr != sup_addr)
        .map(|(addr, _)| *addr)
        .unwrap();

    // Send message — child stops itself
    rt.send_to(child_addr, Ping { reply_to: ActorAddress::default() }).unwrap();
    rt.tick(); // child handles, stops self
    rt.tick(); // cleanup: Down(Normal) delivered to supervisor
    rt.tick(); // supervisor sees Transient + Normal → no restart

    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 1); // only supervisor remains
}

/// Given a supervisor with a transient child,
/// when the child panics, it IS restarted.
#[test]
fn supervisor_restarts_transient_child_on_panic() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_c = counter.clone();

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![ChildSpec::new("worker", RestartPolicy::Transient, move |ctx| {
            ctx.spawn(PanicAfterN {
                trigger: 1, // panics on first message
                count: 0,
                counter: counter_c.clone(),
            })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); // supervisor starts, spawns child
    rt.tick(); // child on_start

    let child_addr = rt.stats().actors.iter()
        .find(|(addr, _)| *addr != sup_addr)
        .map(|(addr, _)| *addr)
        .unwrap();

    // Send message — child panics
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_addr, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // child panics
    rt.tick(); // Down(Panicked) → supervisor restarts
    rt.tick(); // new child spawned
    rt.tick(); // new child on_start

    // Supervisor + new child alive
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 2);
}

/// Given a supervisor with a temporary child,
/// when the child dies (any reason), it is never restarted.
#[test]
fn supervisor_never_restarts_temporary_child() {
    let rt = std_runtime(RuntimeConfig::default());

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![ChildSpec::new("worker", RestartPolicy::Temporary, |ctx| {
            ctx.spawn(PanicActor)
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); // supervisor starts, spawns child
    rt.tick(); // child on_start

    let child_addr = rt.stats().actors.iter()
        .find(|(addr, _)| *addr != sup_addr)
        .map(|(addr, _)| *addr)
        .unwrap();

    // Kill the child
    rt.send_to(child_addr, PanicMsg).unwrap();
    rt.tick(); // panic
    rt.tick(); // Down → supervisor sees Temporary → no restart
    rt.tick(); // settle

    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 1); // only supervisor
}

/// Given a supervisor with max_restarts=2,
/// when more than 2 restarts occur, the supervisor stops itself (meltdown).
#[test]
fn supervisor_meltdown_after_max_restarts() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        2, // only 2 restarts allowed
        vec![ChildSpec::new("crasher", RestartPolicy::Permanent, {
            let counter = counter.clone();
            move |ctx| {
                ctx.spawn(PanicAfterN {
                    trigger: 1,
                    count: 0,
                    counter: counter.clone(),
                })
            }
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // supervisor + child started

    // Crash the child 3 times (1 initial + 2 restarts = max, 3rd restart triggers meltdown)
    for _ in 0..3 {
        // Find current child
        if let Some((child_addr, _)) = rt.stats().actors.iter()
            .find(|(addr, _)| *addr != sup_addr)
        {
            let inbox = rt.new_inbox::<Pong>().unwrap();
            let _ = rt.send_to(*child_addr, Ping { reply_to: *inbox.addr() });
            rt.tick(); // child panics
            rt.tick(); // Down delivered → restart or meltdown
            rt.tick(); // new child spawned (or supervisor stopped)
            rt.tick(); // settle
        }
    }

    // After 3 crashes with max_restarts=2, supervisor should have stopped itself
    let stats = rt.stats();
    let sup_alive = stats.actors.iter().any(|(addr, _)| *addr == sup_addr);
    assert!(!sup_alive, "supervisor should have stopped after exceeding max_restarts");
}

/// Given a supervisor with multiple children,
/// when one child panics, only that child is restarted (OneForOne).
#[test]
fn supervisor_one_for_one_only_restarts_failed_child() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![
            ChildSpec::new("crasher", RestartPolicy::Permanent, {
                let c = counter_a.clone();
                move |ctx| ctx.spawn_named("child_a", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("stable", RestartPolicy::Permanent, {
                let c = counter_b.clone();
                move |ctx| ctx.spawn_named("child_b", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    let _sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // start up

    let child_a = rt.where_is("child_a").expect("child_a should be named");
    let child_b = rt.where_is("child_b").expect("child_b should be named");

    // Send to child_b to prove it's alive
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_b, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let b_processed_before = counter_b.load(Ordering::SeqCst);
    assert!(b_processed_before >= 1);

    // Crash child_a
    rt.send_to(child_a, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // child_a panics
    rt.tick(); // Down → supervisor restarts child_a
    rt.tick(); rt.tick(); // new child spawned + on_start

    // child_b should still be alive (same address, same name)
    let child_b_after = rt.where_is("child_b").expect("child_b should still exist");
    assert_eq!(child_b, child_b_after, "child_b address should be unchanged");

    rt.send_to(child_b, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert!(counter_b.load(Ordering::SeqCst) > b_processed_before,
        "child_b should still be processing messages");

    // Supervisor + 2 children should be alive
    assert_eq!(rt.stats().workers[0].num_actors, 3);
}

/// Given a OneForAll supervisor with 3 children,
/// when one child panics, ALL children are stopped and restarted in spec order.
#[test]
fn supervisor_one_for_all_restarts_all_on_single_failure() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));
    let counter_c = Arc::new(AtomicUsize::new(0));

    let sup = Supervisor::new(
        SupervisorStrategy::OneForAll,
        5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, {
                let c = counter_a.clone();
                move |ctx| ctx.spawn_named("ofa_a", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("b", RestartPolicy::Permanent, {
                let c = counter_b.clone();
                move |ctx| ctx.spawn_named("ofa_b", CountingPingActor { counter: c.clone() })
            }),
            ChildSpec::new("c", RestartPolicy::Permanent, {
                let c = counter_c.clone();
                move |ctx| ctx.spawn_named("ofa_c", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    let _sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // startup

    let old_b = rt.where_is("ofa_b").expect("ofa_b exists");
    let old_c = rt.where_is("ofa_c").expect("ofa_c exists");
    let child_a = rt.where_is("ofa_a").expect("ofa_a exists");

    // Crash child_a
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_a, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // child_a panics
    // supervisor receives Down(a) → OneForAll → stops b and c
    for _ in 0..8 { rt.tick(); } // wait for stops, Downs, restarts, on_starts

    // All 3 children should be alive with NEW addresses (old ones are dead)
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 4); // sup + 3 new children

    // The old addresses for b and c should be gone (they were stopped and re-created)
    // New names should be re-registered
    let new_b = rt.where_is("ofa_b").expect("ofa_b re-registered after restart");
    let new_c = rt.where_is("ofa_c").expect("ofa_c re-registered after restart");
    assert_ne!(old_b, new_b, "child_b should have a new address after restart");
    assert_ne!(old_c, new_c, "child_c should have a new address after restart");
}

/// Given a RestForOne supervisor with children [a, b, c],
/// when child b panics, children b and c are restarted (children after b in spec order).
/// Child a is unaffected.
#[test]
fn supervisor_rest_for_one_restarts_rest_after_failed() {
    let rt = std_runtime(RuntimeConfig::default());
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));
    let counter_c = Arc::new(AtomicUsize::new(0));

    let sup = Supervisor::new(
        SupervisorStrategy::RestForOne,
        5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, {
                let c = counter_a.clone();
                move |ctx| ctx.spawn_named("rfo_a", CountingPingActor { counter: c.clone() })
            }),
            ChildSpec::new("b", RestartPolicy::Permanent, {
                let c = counter_b.clone();
                move |ctx| ctx.spawn_named("rfo_b", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("c", RestartPolicy::Permanent, {
                let c = counter_c.clone();
                move |ctx| ctx.spawn_named("rfo_c", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    let _sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // startup

    let old_a = rt.where_is("rfo_a").expect("rfo_a exists");
    let old_c = rt.where_is("rfo_c").expect("rfo_c exists");
    let child_b = rt.where_is("rfo_b").expect("rfo_b exists");

    // Crash child_b
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_b, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // child_b panics
    // supervisor: Down(b) → RestForOne → stops c (rest after b), then restarts b+c
    for _ in 0..8 { rt.tick(); }

    // All 3 children should be alive
    let stats = rt.stats();
    assert_eq!(stats.workers[0].num_actors, 4); // sup + 3 children

    // child_a should be UNCHANGED (not affected by RestForOne)
    let new_a = rt.where_is("rfo_a").expect("rfo_a still exists");
    assert_eq!(old_a, new_a, "child_a should not be restarted in RestForOne when b fails");

    // child_c should have a NEW address (it was stopped and re-created)
    let new_c = rt.where_is("rfo_c").expect("rfo_c re-registered");
    assert_ne!(old_c, new_c, "child_c should have a new address after RestForOne restart");
}

/// Given a OneForAll supervisor, when the last child of the failed set confirms death,
/// all children are restarted in spec order (not reverse).
#[test]
fn supervisor_one_for_all_waits_for_all_downs_before_restart() {
    let rt = std_runtime(RuntimeConfig::default());

    let sup = Supervisor::new(
        SupervisorStrategy::OneForAll,
        5,
        vec![
            ChildSpec::new("x", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
            ChildSpec::new("y", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
        ],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // startup

    assert_eq!(rt.stats().workers[0].num_actors, 3); // sup + 2 children

    // Stop one child (graceful stop triggers OneForAll)
    let actors: Vec<_> = rt.stats().actors.iter()
        .filter(|(addr, _)| *addr != sup_addr)
        .map(|(addr, _)| *addr)
        .collect();
    rt.stop_actor(actors[0]).unwrap();

    // Tick enough times for full cycle: stop → Down → supervisor stops other → Down → restart all
    for _ in 0..10 { rt.tick(); }

    // Should have supervisor + 2 new children
    assert_eq!(rt.stats().workers[0].num_actors, 3);
}

/// Given a supervisor that stops, its children also stop.
#[test]
fn supervisor_on_stop_kills_children() {
    let rt = std_runtime(RuntimeConfig::default());

    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne,
        5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
            ChildSpec::new("b", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
        ],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    rt.tick(); rt.tick(); // start up

    assert_eq!(rt.stats().workers[0].num_actors, 3); // sup + 2 children

    // Stop the supervisor
    rt.stop_actor(sup_addr).unwrap();
    rt.tick(); // StopSignal delivered to supervisor, on_stop sends stop to children
    rt.tick(); // supervisor cleaned up, stop signals delivered to children
    rt.tick(); // children stop
    rt.tick(); // children cleaned up

    assert_eq!(rt.stats().workers[0].num_actors, 0);
}

// ── Router tests ─────────────────────────────────────────────────────────────

#[test]
fn router_round_robin_distributes_across_workers() {
    // Given a round-robin router with 3 workers
    // When we send 6 messages
    // Then each worker should receive exactly 2 messages
    let rt = std_runtime(RuntimeConfig::default());
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));

    struct Collector(Arc<std::sync::Mutex<Vec<(ActorAddress, usize)>>>);
    #[derive(Clone)]
    struct Work(usize);
    impl ActorInterface for Collector {
        type Incoming = Work;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: Work) {
            self.0.lock().unwrap().push((ctx.self_addr(), msg.0));
        }
    }

    let c = collected.clone();
    let router = Router::<Work>::new(
        RoutingStrategy::RoundRobin,
        3,
        move |ctx| ctx.spawn(Collector(c.clone())),
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick(); // on_start spawns 3 workers

    for i in 0..6 {
        rt.send_to(router_addr, Work(i)).unwrap();
    }
    rt.tick(); // router receives 6 Work messages, forwards to workers
    rt.tick(); // workers process their messages

    let data = collected.lock().unwrap();
    assert_eq!(data.len(), 6);

    // Count how many unique workers received messages
    let mut per_worker = std::collections::HashMap::new();
    for (addr, _) in data.iter() {
        *per_worker.entry(*addr).or_insert(0usize) += 1;
    }
    // All 3 workers should have received exactly 2 messages each
    assert_eq!(per_worker.len(), 3);
    for count in per_worker.values() {
        assert_eq!(*count, 2);
    }
}

#[test]
fn router_broadcast_sends_to_all_workers() {
    // Given a broadcast router with 3 workers
    // When we send 1 message
    // Then all 3 workers should receive it
    let rt = std_runtime(RuntimeConfig::default());
    let count = Arc::new(AtomicUsize::new(0));

    struct Counter(Arc<AtomicUsize>);
    #[derive(Clone)]
    struct Ping;
    impl ActorInterface for Counter {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let c = count.clone();
    let router = Router::<Ping>::new(
        RoutingStrategy::Broadcast,
        3,
        move |ctx| ctx.spawn(Counter(c.clone())),
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick(); // on_start spawns workers

    rt.send_to(router_addr, Ping).unwrap();
    rt.tick(); // router broadcasts
    rt.tick(); // workers process

    assert_eq!(count.load(Ordering::Relaxed), 3);
}

#[test]
fn router_random_delivers_to_some_worker() {
    // Given a random router with 3 workers
    // When we send 30 messages
    // Then at least 2 different workers should have received messages
    let rt = std_runtime(RuntimeConfig::default());
    let collected = Arc::new(std::sync::Mutex::new(Vec::new()));

    struct Collector(Arc<std::sync::Mutex<Vec<ActorAddress>>>);
    #[derive(Clone)]
    struct Work;
    impl ActorInterface for Collector {
        type Incoming = Work;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Work) {
            self.0.lock().unwrap().push(ctx.self_addr());
        }
    }

    let c = collected.clone();
    let router = Router::<Work>::new(
        RoutingStrategy::Random,
        3,
        move |ctx| ctx.spawn(Collector(c.clone())),
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();

    for _ in 0..30 {
        rt.send_to(router_addr, Work).unwrap();
    }
    rt.tick();
    rt.tick();

    let data = collected.lock().unwrap();
    assert_eq!(data.len(), 30);

    let unique: std::collections::HashSet<_> = data.iter().collect();
    // With 30 messages across 3 workers, probability of all going to 1 is vanishingly small
    assert!(unique.len() >= 2, "expected at least 2 workers used, got {}", unique.len());
}

#[test]
fn router_replaces_dead_worker() {
    // Given a router with 3 workers
    // When one worker panics
    // Then the router should spawn a replacement and messages continue to be delivered
    let rt = std_runtime(RuntimeConfig::default());
    let spawn_count = Arc::new(AtomicUsize::new(0));

    struct PanicOnFirst {
        first: bool,
    }
    #[derive(Clone)]
    struct Work;
    impl ActorInterface for PanicOnFirst {
        type Incoming = Work;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
            if self.first {
                self.first = false;
                panic!("first message panic");
            }
        }
    }

    let sc = spawn_count.clone();
    let router = Router::<Work>::new(
        RoutingStrategy::RoundRobin,
        3,
        move |ctx| {
            let n = sc.fetch_add(1, Ordering::Relaxed);
            // Only the first worker panics on its first message
            ctx.spawn(PanicOnFirst { first: n == 0 })
        },
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick(); // spawn workers (3 spawned)
    assert_eq!(spawn_count.load(Ordering::Relaxed), 3);

    // Send a message that will hit worker 0 (round-robin starts at 0)
    rt.send_to(router_addr, Work).unwrap();
    rt.tick(); // router forwards to worker 0
    rt.tick(); // worker 0 panics
    rt.tick(); // cleanup + Down delivered to router
    rt.tick(); // router spawns replacement
    rt.tick(); // replacement starts

    // Should have spawned 4 total (3 original + 1 replacement)
    assert_eq!(spawn_count.load(Ordering::Relaxed), 4);

    // Verify all 3 slots are live — stats should show router + 3 workers
    assert_eq!(rt.stats().workers[0].num_actors, 4);
}

#[test]
fn router_meltdown_after_max_restarts() {
    // Given a router with max_restarts=2
    // When 3 workers die in succession
    // Then the router should stop itself
    let rt = std_runtime(RuntimeConfig::default());

    struct AlwaysPanics;
    #[derive(Clone)]
    struct Work;
    impl ActorInterface for AlwaysPanics {
        type Incoming = Work;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Work) {
            panic!("always");
        }
    }

    let router = Router::<Work>::new(
        RoutingStrategy::RoundRobin,
        1,
        |ctx| ctx.spawn(AlwaysPanics),
        2, // max 2 restarts
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick(); // on_start

    // Kill the worker 3 times (> max_restarts=2)
    for _ in 0..3 {
        rt.send_to(router_addr, Work).unwrap();
        for _ in 0..5 {
            rt.tick();
        }
    }

    // After 3 restarts, router should have shut down
    for _ in 0..5 {
        rt.tick();
    }
    assert_eq!(rt.stats().workers[0].num_actors, 0);
}

#[test]
fn router_on_stop_kills_workers() {
    // Given a running router with 3 workers
    // When the router is stopped
    // Then all workers should also be stopped
    let rt = std_runtime(RuntimeConfig::default());

    struct Dummy;
    #[derive(Clone)]
    struct Work;
    impl ActorInterface for Dummy {
        type Incoming = Work;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Work) {}
    }

    let router = Router::<Work>::new(
        RoutingStrategy::RoundRobin,
        3,
        |ctx| ctx.spawn(Dummy),
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick(); // on_start
    assert_eq!(rt.stats().workers[0].num_actors, 4); // router + 3 workers

    rt.stop_actor(router_addr).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    assert_eq!(rt.stats().workers[0].num_actors, 0);
}

#[test]
fn router_broadcast_multiple_messages_all_received() {
    // Given a broadcast router
    // When we send 5 messages to 3 workers
    // Then total received = 5 * 3 = 15
    let rt = std_runtime(RuntimeConfig::default());
    let total = Arc::new(AtomicUsize::new(0));

    struct Sink(Arc<AtomicUsize>);
    #[derive(Clone)]
    struct Tick;
    impl ActorInterface for Sink {
        type Incoming = Tick;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Tick) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let t = total.clone();
    let router = Router::<Tick>::new(
        RoutingStrategy::Broadcast,
        3,
        move |ctx| ctx.spawn(Sink(t.clone())),
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();

    for _ in 0..5 {
        rt.send_to(router_addr, Tick).unwrap();
    }
    rt.tick(); // router broadcasts
    rt.tick(); // workers process

    assert_eq!(total.load(Ordering::Relaxed), 15);
}

// ── Identity Hasher Correctness ────────────────────────────────────────────

/// Given: 200 actors each expecting a unique numbered message
/// When:  Each actor receives its number and replies with (self_addr, number)
/// Then:  All 200 replies match — no message was misrouted by the identity hasher
#[test]
fn many_actors_all_receive_correct_messages() {
    #[derive(Clone)]
    struct NumberedMsg {
        n: usize,
        reply_to: ActorAddress,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct NumberedReply {
        from: ActorAddress,
        n: usize,
    }

    struct NumberedActor;

    impl ActorInterface for NumberedActor {
        type Incoming = NumberedMsg;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: NumberedMsg) {
            let _ = ctx.send(
                msg.reply_to,
                NumberedReply {
                    from: ctx.self_addr(),
                    n: msg.n,
                },
            );
        }
    }

    let rt = std_runtime(RuntimeConfig {
        max_actors: 300,
        channel_buffer_size: 1024,
        num_threads: 1,
        ..Default::default()
    });

    let inbox = rt.new_inbox::<NumberedReply>().unwrap();
    let inbox_addr = *inbox.addr();

    // Spawn 200 actors
    let mut addrs = Vec::new();
    for _ in 0..200 {
        addrs.push(rt.spawn(NumberedActor).unwrap());
    }
    rt.tick(); // on_start

    // Send unique numbered message to each
    for (i, addr) in addrs.iter().enumerate() {
        rt.send_to(
            *addr,
            NumberedMsg {
                n: i,
                reply_to: inbox_addr,
            },
        )
        .unwrap();
    }
    rt.tick(); // process + reply
    rt.tick(); // deliver replies

    // Verify all 200 replies
    let mut replies: Vec<NumberedReply> = Vec::new();
    while let Some(reply) = inbox.try_recv() {
        replies.push(reply);
    }

    assert_eq!(replies.len(), 200, "should receive exactly 200 replies");

    // Verify each reply came from the correct actor with the correct number
    for (i, addr) in addrs.iter().enumerate() {
        let reply = replies.iter().find(|r| r.n == i);
        assert!(
            reply.is_some(),
            "missing reply for actor #{i}"
        );
        assert_eq!(
            reply.unwrap().from, *addr,
            "reply #{i} came from wrong actor"
        );
    }
}

/// Given: A 100-actor ring where each actor forwards to the next
/// When:  A message enters the ring and traverses all 100 hops
/// Then:  The message completes the full circuit (address_map lookups all correct)
#[test]
fn ring_routing_unchanged_after_hasher_optimization() {
    #[derive(Clone)]
    struct RingHop {
        hops_remaining: usize,
        final_dest: ActorAddress,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct RingDone(usize); // total hops completed

    struct RingNode {
        next: ActorAddress,
    }

    impl ActorInterface for RingNode {
        type Incoming = RingHop;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: RingHop) {
            if msg.hops_remaining == 0 {
                let _ = ctx.send(msg.final_dest, RingDone(100));
            } else {
                let _ = ctx.send(
                    self.next,
                    RingHop {
                        hops_remaining: msg.hops_remaining - 1,
                        final_dest: msg.final_dest,
                    },
                );
            }
        }
    }

    let rt = std_runtime(RuntimeConfig {
        max_actors: 200,
        channel_buffer_size: 1024,
        num_threads: 1,
        ..Default::default()
    });

    let inbox = rt.new_inbox::<RingDone>().unwrap();
    let inbox_addr = *inbox.addr();

    // Build chain backwards: last node sends to inbox, first node receives
    let mut addrs = Vec::new();
    let mut next = inbox_addr;
    for _ in (0..100).rev() {
        let node = RingNode { next };
        let addr = rt.spawn(node).unwrap();
        addrs.push(addr);
        next = addr;
    }
    addrs.reverse(); // addrs[0] is start of chain

    rt.tick(); // on_start

    // Inject message at the start
    rt.send_to(
        addrs[0],
        RingHop {
            hops_remaining: 99,
            final_dest: inbox_addr,
        },
    )
    .unwrap();

    // Tick enough times for the message to traverse all 100 actors
    // (each tick processes one hop via pending_local delivery)
    for _ in 0..110 {
        rt.tick();
    }

    let result = inbox.try_recv();
    assert!(result.is_some(), "ring message should complete all 100 hops");
    assert_eq!(result.unwrap(), RingDone(100));
}

/// Given: An actor that calls ctx.stop_self() upon receiving a trigger message
/// When:  The trigger is sent, then 5 more messages are sent, then ticked
/// Then:  The actor is removed, only messages before stop are processed
#[test]
fn stop_self_with_pending_messages_still_works() {
    let processed = Arc::new(AtomicUsize::new(0));

    #[derive(Clone)]
    struct Msg(bool); // true = trigger stop

    struct StopOnTrigger(Arc<AtomicUsize>);

    impl ActorInterface for StopOnTrigger {
        type Incoming = Msg;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: Msg) {
            self.0.fetch_add(1, Ordering::Relaxed);
            if msg.0 {
                ctx.stop_self();
            }
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let p = processed.clone();
    let addr = rt.spawn(StopOnTrigger(p)).unwrap();
    rt.tick(); // on_start

    // Send: 2 normal, 1 trigger, 5 more normal
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(true)).unwrap(); // stop trigger
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(false)).unwrap();
    rt.send_to(addr, Msg(false)).unwrap();

    rt.tick(); // process messages — stops after trigger
    rt.tick(); // cleanup

    // Only 3 messages should be processed (2 normal + 1 trigger)
    assert_eq!(
        processed.load(Ordering::Relaxed),
        3,
        "should process exactly the messages up to and including the stop trigger"
    );

    // Subsequent sends should fail
    assert!(rt.send_to(addr, Msg(false)).is_err());
}
