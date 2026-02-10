use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Inbox, Runtime, RuntimeConfig};

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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
    let bogus = ActorAddress::new_random();

    // When I try to send to that address
    let result = rt.send_to(bogus, Pong);

    // Then I get an error
    assert!(result.is_err(), "sending to unknown address should fail");
}

#[test]
fn messages_sent_within_handler_are_delivered() {
    // Given a DelegatorActor (spawns child + sends in same handler call)
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig { num_threads: 4, ..Default::default() });
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
    let rt = Runtime::new(RuntimeConfig { num_threads: 2, ..Default::default() });
    let handle = rt.run().unwrap();

    // When I call shutdown + join
    handle.shutdown();
    handle.join();

    // Then join returns (threads have stopped) — test passes by not hanging
}

#[test]
fn cross_worker_delegation_delivers_reply() {
    // Given a 2-thread runtime with a DelegatorActor
    let rt = Runtime::new(RuntimeConfig { num_threads: 2, ..Default::default() });
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig {
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();
    for _ in 0..5 {
        rt.tick();
    }

    // When I send more messages to it
    let result = rt.send_to(panic_addr, PanicMsg);

    // Then send_to succeeds (address is still in address_map)
    assert!(
        result.is_ok(),
        "send_to poisoned actor should succeed from sender's POV"
    );

    // And ticking doesn't produce new panics — messages are discarded in tick_all
    for _ in 0..10 {
        rt.tick();
    }
    let s = rt.stats();
    let panics: u64 = s.workers.iter().map(|w| w.panics).sum();
    assert_eq!(panics, 1, "poisoned actor should not produce new panics");
}

#[test]
fn tiny_buffer_delivers_all_messages_in_order() {
    // Given a runtime with channel_buffer_size=1 (overflow on every 2nd message)
    let rt = Runtime::new(RuntimeConfig {
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
    let rt = Runtime::new(RuntimeConfig::default());

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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig {
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
    let rt = Runtime::new(RuntimeConfig {
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
    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(PingPongActor).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(addr, Ping { reply_to: *inbox.addr() }).unwrap();

    // Then inbox is empty — no processing without tick
    assert!(inbox.try_recv().is_none());
}

#[test]
fn interleaved_spawn_and_send_in_handler_all_complete() {
    // Given a FanOutActor that spawns 20 children with interleaved spawn+send
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    // Given a poisoned actor that then receives 10 more messages
    let rt = Runtime::new(RuntimeConfig::default());
    let panic_addr = rt.spawn(PanicActor).unwrap();
    rt.send_to(panic_addr, PanicMsg).unwrap();
    for _ in 0..5 {
        rt.tick();
    }
    let s1 = rt.stats();
    let processed_before: u64 = s1.workers.iter().map(|w| w.messages_processed).sum();

    // When I send 10 messages to the poisoned actor and tick
    for _ in 0..10 {
        rt.send_to(panic_addr, PanicMsg).unwrap();
    }
    for _ in 0..20 {
        rt.tick();
    }
    let s2 = rt.stats();
    let processed_after: u64 = s2.workers.iter().map(|w| w.messages_processed).sum();

    // Then the 10 discarded messages should NOT increase the processed count
    assert_eq!(
        processed_before, processed_after,
        "messages discarded by poisoned actors should not be counted as processed"
    );
}

#[test]
fn rapid_spawn_and_immediate_send() {
    // Given a runtime, spawn an actor and immediately send before any tick
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig::default());
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
    let rt = Runtime::new(RuntimeConfig { num_threads: 4, ..Default::default() });
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
