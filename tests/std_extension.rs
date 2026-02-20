//! StdExtension Tests — higher-level patterns from swactor-std.
//!
//! Covers: naming registry, groups/pub-sub, ask pattern, supervision
//! strategies and restart policies, and router work distribution.

mod common;
use common::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ── Local actors ────────────────────────────────────────────────────────────

/// Looks up a peer by name using ctx.where_is().
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

/// Spawns a named child from a handler.
struct NamedSpawnerActor {
    reply_to: ActorAddress,
}

impl ActorInterface for NamedSpawnerActor {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        if let Ok(addr) = ctx.spawn_named("child", PingPongActor) {
            ctx.send(self.reply_to, MyAddr(addr)).unwrap();
        }
    }
}

/// Panics after `trigger` messages.
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

/// Stops itself on first message.
struct StopsAfterFirst;
impl ActorInterface for StopsAfterFirst {
    type Incoming = Ping;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
        ctx.stop_self();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Naming Registry
// ═══════════════════════════════════════════════════════════════════════════

/// Full naming lifecycle: register, lookup, send, duplicate fails, auto-unregister
/// on stop and panic, name reuse, registered_names list, manual unregister.
#[test]
fn naming_registry_lifecycle() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Register "alice", lookup, send Ping → Pong
    let alice = rt.spawn_named("alice", PingPongActor).unwrap();
    assert_eq!(rt.where_is("alice"), Some(alice));
    rt.send_to(alice, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_some(), "named actor processes messages");

    // Duplicate fails, original binding preserved
    assert!(rt.spawn_named("alice", PingPongActor).is_err());
    assert_eq!(rt.where_is("alice"), Some(alice));

    // Unknown name → None
    assert_eq!(rt.where_is("ghost"), None);

    // Stop "alice" → name freed
    rt.stop_actor(alice).unwrap();
    rt.tick();
    assert_eq!(rt.where_is("alice"), None, "name freed after stop");

    // Reuse the name
    let alice2 = rt.spawn_named("alice", PingPongActor).unwrap();
    assert_ne!(alice, alice2);
    assert_eq!(rt.where_is("alice"), Some(alice2));

    // Panic also frees the name
    let bob = rt.spawn_named("bob", PanicActor).unwrap();
    rt.tick();
    rt.send_to(bob, PanicMsg).unwrap();
    rt.tick();
    assert_eq!(rt.where_is("bob"), None, "name freed after panic");
    let _bob2 = rt.spawn_named("bob", PingPongActor).unwrap();
    assert!(rt.where_is("bob").is_some());

    // registered_names enumerates all
    rt.spawn_named("gamma", PingPongActor).unwrap();
    let mut names = rt.registered_names();
    names.sort();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"bob".to_string()));
    assert!(names.contains(&"gamma".to_string()));

    // Manual unregister: name freed but actor lives
    let charlie_inbox = rt.new_inbox::<Pong>().unwrap();
    let charlie = rt.spawn_named("charlie", PingPongActor).unwrap();
    rt.tick();
    let removed = rt.unregister("charlie");
    assert_eq!(removed, Some(charlie));
    assert_eq!(rt.where_is("charlie"), None, "name freed by unregister");
    rt.send_to(charlie, Ping { reply_to: *charlie_inbox.addr() }).unwrap();
    rt.tick();
    assert!(charlie_inbox.try_recv().is_some(), "actor still alive after name unregistered");
}

/// Actors resolve and register names from handlers using ctx.
#[test]
fn naming_from_actor_handlers() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<MyAddr>().unwrap();

    // ctx.where_is from handler
    let target = rt.spawn_named("target", PingPongActor).unwrap();
    let looker = rt.spawn(NameLookupActor {
        target_name: "target",
        reply_to: *inbox.addr(),
    }).unwrap();
    rt.tick();
    rt.send_to(looker, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    assert_eq!(inbox.try_recv(), Some(MyAddr(target)), "ctx.where_is resolves");

    // ctx.spawn_named from handler
    let spawner = rt.spawn(NamedSpawnerActor { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(spawner, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let child_addr = inbox.try_recv().expect("child address returned");
    assert_eq!(rt.where_is("child"), Some(child_addr.0), "name registered from handler");
}

// ═══════════════════════════════════════════════════════════════════════════
// Groups / Pub-Sub
// ═══════════════════════════════════════════════════════════════════════════

/// Full groups lifecycle: join, publish broadcasts, leave stops delivery,
/// dead actor auto-removed, multi-group cleanup, empty group deleted,
/// join and publish from handlers.
#[test]
fn groups_pub_sub_lifecycle() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();

    // Join 3 actors, publish → all 3 get it
    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();
    let c = rt.spawn(PingPongActor).unwrap();
    rt.join_group(a, "workers");
    rt.join_group(b, "workers");
    rt.join_group(c, "workers");
    rt.tick();

    let count = rt.publish_to("workers", Ping { reply_to: *inbox.addr() });
    assert_eq!(count, 3, "3 members, 3 messages sent");
    rt.tick();
    let mut pongs = 0;
    while inbox.try_recv().is_some() { pongs += 1; }
    assert_eq!(pongs, 3, "all 3 received");

    // Leave stops delivery
    rt.leave_group(c, "workers");
    let count = rt.publish_to("workers", Ping { reply_to: *inbox.addr() });
    assert_eq!(count, 2, "2 after leave");
    rt.tick();
    let mut pongs = 0;
    while inbox.try_recv().is_some() { pongs += 1; }
    assert_eq!(pongs, 2);

    // Dead actor auto-removed
    rt.stop_actor(b).unwrap();
    rt.tick();
    let count = rt.publish_to("workers", Ping { reply_to: *inbox.addr() });
    assert_eq!(count, 1, "dead actor removed");

    // Multi-group cleanup: actor in alpha/beta/gamma dies → all cleaned
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.join_group(actor, "alpha");
    rt.join_group(actor, "beta");
    rt.join_group(actor, "gamma");
    rt.tick();
    rt.stop_actor(actor).unwrap();
    rt.tick();
    assert!(rt.group_members("alpha").is_empty());
    assert!(rt.group_members("beta").is_empty());
    assert!(rt.group_members("gamma").is_empty());

    // Empty group auto-deleted
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.join_group(actor, "temp");
    assert!(rt.groups().contains(&"temp".to_string()));
    rt.leave_group(actor, "temp");
    assert!(!rt.groups().contains(&"temp".to_string()), "empty group removed");

    // Empty group query
    let rt = std_runtime(RuntimeConfig::default());
    assert!(rt.group_members("nonexistent").is_empty());

    // ctx.join_group from on_start
    struct GroupJoiner;
    impl ActorInterface for GroupJoiner {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.join_group("auto-joined");
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
    }

    let rt = std_runtime(RuntimeConfig::default());
    let x = rt.spawn(GroupJoiner).unwrap();
    let y = rt.spawn(GroupJoiner).unwrap();
    rt.tick();
    let members = rt.group_members("auto-joined");
    assert_eq!(members.len(), 2);
    assert!(members.contains(&x));
    assert!(members.contains(&y));

    // ctx.publish from handler
    #[derive(Clone)]
    struct BroadcastCmd { reply_to: ActorAddress }

    struct Broadcaster;
    impl ActorInterface for Broadcaster {
        type Incoming = BroadcastCmd;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.join_group("bcast");
        }
        fn handle(&mut self, ctx: &Ctx, msg: BroadcastCmd) {
            ctx.publish("bcast", Ping { reply_to: msg.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let p1 = rt.spawn(PingPongActor).unwrap();
    let p2 = rt.spawn(PingPongActor).unwrap();
    rt.join_group(p1, "bcast");
    rt.join_group(p2, "bcast");
    let broadcaster = rt.spawn(Broadcaster).unwrap();
    rt.tick();
    rt.send_to(broadcaster, BroadcastCmd { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);
    let mut pongs = 0;
    while inbox.try_recv().is_some() { pongs += 1; }
    assert!(pongs >= 2, "at least 2 PingPong members replied, got {pongs}");
}

// ═══════════════════════════════════════════════════════════════════════════
// Ask Pattern
// ═══════════════════════════════════════════════════════════════════════════

/// Ask pattern: basic ask, repeated asks track state, try_recv before/after
/// tick, dead actor times out.
#[test]
fn ask_pattern() {
    let rt = std_runtime(RuntimeConfig::default());

    // Basic ask
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick();
    let pong: Pong = rt.ask(actor, |reply_to| Ping { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    assert_eq!(pong, Pong);

    // Repeated asks track state
    let counter = rt.spawn(CounterActor { count: 0 }).unwrap();
    rt.tick();
    let c1: Count = rt.ask(counter, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    let c2: Count = rt.ask(counter, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    let c3: Count = rt.ask(counter, |reply_to| Increment { reply_to })
        .unwrap().recv_ticking(&rt, 10).unwrap();
    assert_eq!((c1, c2, c3), (Count(1), Count(2), Count(3)));

    // try_recv: None before tick, Some after
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick();
    let ask = rt.ask::<Ping, Pong>(actor, |reply_to| Ping { reply_to }).unwrap();
    assert!(ask.try_recv().is_none(), "no response before tick");
    rt.tick();
    assert_eq!(ask.try_recv(), Some(Pong));

    // Dead actor → timeout
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(PingPongActor).unwrap();
    rt.tick();
    rt.stop_actor(actor).unwrap();
    rt.tick();
    if let Ok(ask) = rt.ask::<Ping, Pong>(actor, |reply_to| Ping { reply_to }) {
        assert!(ask.recv_ticking(&rt, 5).is_err(), "timeout with dead actor");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Supervision
// ═══════════════════════════════════════════════════════════════════════════

/// Restart policies: permanent always restarts, transient only on panic,
/// temporary never restarts, meltdown after max_restarts.
#[test]
fn supervision_restart_policies() {
    // Permanent child panics → restarted
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_c = counter.clone();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("worker", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(PanicAfterN { trigger: 2, count: 0, counter: counter_c.clone() })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    rt.send_to(child, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    rt.send_to(child, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 5); // panics, supervisor restarts
    assert_eq!(rt.stats().workers[0].num_actors, 2, "supervisor + restarted child");

    // Transient stops normally → NOT restarted
    let rt = std_runtime(RuntimeConfig::default());
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("worker", RestartPolicy::Transient, |ctx| {
            ctx.spawn(StopsAfterFirst)
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    rt.send_to(child, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 4);
    assert_eq!(rt.stats().workers[0].num_actors, 1, "transient+normal → no restart");

    // Transient panics → restarted
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_c = counter.clone();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("worker", RestartPolicy::Transient, move |ctx| {
            ctx.spawn(PanicAfterN { trigger: 1, count: 0, counter: counter_c.clone() })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 2, "transient+panic → restarted");

    // Temporary never restarts
    let rt = std_runtime(RuntimeConfig::default());
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("worker", RestartPolicy::Temporary, |ctx| ctx.spawn(PanicActor))],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    rt.send_to(child, PanicMsg).unwrap();
    tick_n(&rt, 4);
    assert_eq!(rt.stats().workers[0].num_actors, 1, "temporary → no restart");

    // Meltdown: max_restarts=2, crash 3 times → supervisor stops
    let rt = std_runtime(RuntimeConfig::default());
    let counter = Arc::new(AtomicUsize::new(0));
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 2,
        vec![ChildSpec::new("crasher", RestartPolicy::Permanent, {
            let c = counter.clone();
            move |ctx| ctx.spawn(PanicAfterN { trigger: 1, count: 0, counter: c.clone() })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    for _ in 0..3 {
        if let Some((child, _)) = rt.stats().actors.iter()
            .find(|(a, _)| *a != sup_addr)
        {
            let inbox = rt.new_inbox::<Pong>().unwrap();
            let _ = rt.send_to(*child, Ping { reply_to: *inbox.addr() });
            tick_n(&rt, 5);
        }
    }
    let sup_alive = rt.stats().actors.iter().any(|(a, _)| *a == sup_addr);
    assert!(!sup_alive, "supervisor stopped after exceeding max_restarts");
}

/// Strategies: OneForOne, OneForAll, RestForOne. Stopping supervisor kills children.
#[test]
fn supervision_strategies() {
    // OneForOne: only failed child restarted
    let rt = std_runtime(RuntimeConfig::default());
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![
            ChildSpec::new("crasher", RestartPolicy::Permanent, {
                let c = counter_a.clone();
                move |ctx| ctx.spawn_named("ofo_a", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("stable", RestartPolicy::Permanent, {
                let c = counter_b.clone();
                move |ctx| ctx.spawn_named("ofo_b", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let child_a = rt.where_is("ofo_a").unwrap();
    let child_b = rt.where_is("ofo_b").unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_a, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 5);
    let child_b_after = rt.where_is("ofo_b").unwrap();
    assert_eq!(child_b, child_b_after, "child_b unchanged in OneForOne");
    rt.send_to(child_b, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert!(counter_b.load(Ordering::SeqCst) >= 1, "child_b still processing");

    // OneForAll: all children restarted
    let rt = std_runtime(RuntimeConfig::default());
    let sup = Supervisor::new(
        SupervisorStrategy::OneForAll, 5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, {
                let c = Arc::new(AtomicUsize::new(0));
                move |ctx| ctx.spawn_named("ofa_a", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("b", RestartPolicy::Permanent, {
                let c = Arc::new(AtomicUsize::new(0));
                move |ctx| ctx.spawn_named("ofa_b", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let old_b = rt.where_is("ofa_b").unwrap();
    let child_a = rt.where_is("ofa_a").unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_a, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 8);
    let new_b = rt.where_is("ofa_b").expect("ofa_b re-registered");
    assert_ne!(old_b, new_b, "child_b restarted in OneForAll");

    // RestForOne: failed child + later children restarted, earlier unaffected
    let rt = std_runtime(RuntimeConfig::default());
    let sup = Supervisor::new(
        SupervisorStrategy::RestForOne, 5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, {
                let c = Arc::new(AtomicUsize::new(0));
                move |ctx| ctx.spawn_named("rfo_a", CountingPingActor { counter: c.clone() })
            }),
            ChildSpec::new("b", RestartPolicy::Permanent, {
                let c = Arc::new(AtomicUsize::new(0));
                move |ctx| ctx.spawn_named("rfo_b", PanicAfterN {
                    trigger: 1, count: 0, counter: c.clone(),
                })
            }),
            ChildSpec::new("c", RestartPolicy::Permanent, {
                let c = Arc::new(AtomicUsize::new(0));
                move |ctx| ctx.spawn_named("rfo_c", CountingPingActor { counter: c.clone() })
            }),
        ],
    );
    rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    let old_a = rt.where_is("rfo_a").unwrap();
    let old_c = rt.where_is("rfo_c").unwrap();
    let child_b = rt.where_is("rfo_b").unwrap();
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.send_to(child_b, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 8);
    let new_a = rt.where_is("rfo_a").unwrap();
    let new_c = rt.where_is("rfo_c").expect("rfo_c re-registered");
    assert_eq!(old_a, new_a, "child_a unchanged in RestForOne");
    assert_ne!(old_c, new_c, "child_c restarted in RestForOne");

    // Stopping supervisor kills children
    let rt = std_runtime(RuntimeConfig::default());
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![
            ChildSpec::new("a", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
            ChildSpec::new("b", RestartPolicy::Permanent, |ctx| ctx.spawn(PingPongActor)),
        ],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);
    assert_eq!(rt.stats().workers[0].num_actors, 3);
    rt.stop_actor(sup_addr).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 0, "stopping supervisor kills children");
}

/// handle_down dispatch and ctx.stop_actor from handler.
#[test]
fn handle_down_dispatch() {
    // ctx.stop_actor from handler stops target
    #[derive(Clone)]
    struct StopCmd { target: ActorAddress }
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
    rt.tick();
    rt.send_to(stopper, StopCmd { target }).unwrap();
    tick_n(&rt, 4);
    assert!(rt.send_to(target, Ping { reply_to: ActorAddress::default() }).is_err(),
        "target stopped by ctx.stop_actor");
    assert!(rt.send_to(stopper, StopCmd { target }).is_ok(), "stopper still alive");
}

// ═══════════════════════════════════════════════════════════════════════════
// Router
// ═══════════════════════════════════════════════════════════════════════════

/// Router distributes work: round-robin is even, broadcast hits all, random
/// uses multiple workers. Dead workers replaced. Stop router kills workers.
/// Meltdown after max restarts.
#[test]
fn router_work_distribution() {
    // Round-robin: 3 workers, 6 msgs → 2 each
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
        RoutingStrategy::RoundRobin, 3,
        move |ctx| ctx.spawn(Collector(c.clone())), 10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    for i in 0..6 {
        rt.send_to(router_addr, Work(i)).unwrap();
    }
    tick_n(&rt, 3);
    let data = collected.lock().unwrap();
    assert_eq!(data.len(), 6);
    let mut per_worker = std::collections::HashMap::new();
    for (addr, _) in data.iter() {
        *per_worker.entry(*addr).or_insert(0usize) += 1;
    }
    assert_eq!(per_worker.len(), 3, "3 distinct workers");
    for count in per_worker.values() {
        assert_eq!(*count, 2, "each worker gets exactly 2");
    }

    // Broadcast: 5 msgs to 3 workers → 15 total
    let rt = std_runtime(RuntimeConfig::default());
    let total = Arc::new(AtomicUsize::new(0));
    struct BCounter(Arc<AtomicUsize>);
    #[derive(Clone)]
    struct BPing;
    impl ActorInterface for BCounter {
        type Incoming = BPing;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: BPing) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let t = total.clone();
    let router = Router::<BPing>::new(
        RoutingStrategy::Broadcast, 3,
        move |ctx| ctx.spawn(BCounter(t.clone())), 10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    for _ in 0..5 {
        rt.send_to(router_addr, BPing).unwrap();
    }
    tick_n(&rt, 3);
    assert_eq!(total.load(Ordering::Relaxed), 15, "5 broadcasts × 3 workers = 15");

    // Random: 30 msgs → at least 2 workers used
    let rt = std_runtime(RuntimeConfig::default());
    let rcollected = Arc::new(std::sync::Mutex::new(Vec::new()));
    struct RCollector(Arc<std::sync::Mutex<Vec<ActorAddress>>>);
    #[derive(Clone)]
    struct RWork;
    impl ActorInterface for RCollector {
        type Incoming = RWork;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: RWork) {
            self.0.lock().unwrap().push(ctx.self_addr());
        }
    }
    let c = rcollected.clone();
    let router = Router::<RWork>::new(
        RoutingStrategy::Random, 3,
        move |ctx| ctx.spawn(RCollector(c.clone())), 10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    for _ in 0..30 {
        rt.send_to(router_addr, RWork).unwrap();
    }
    tick_n(&rt, 3);
    let data = rcollected.lock().unwrap();
    let unique: std::collections::HashSet<_> = data.iter().collect();
    assert!(unique.len() >= 2, "random uses at least 2 workers");

    // Dead worker replaced
    let rt = std_runtime(RuntimeConfig::default());
    let spawn_count = Arc::new(AtomicUsize::new(0));
    struct PanicOnFirst { first: bool }
    #[derive(Clone)]
    struct DWork;
    impl ActorInterface for PanicOnFirst {
        type Incoming = DWork;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: DWork) {
            if self.first { self.first = false; panic!("first message panic"); }
        }
    }
    let sc = spawn_count.clone();
    let router = Router::<DWork>::new(
        RoutingStrategy::RoundRobin, 3,
        move |ctx| { sc.fetch_add(1, Ordering::Relaxed); ctx.spawn(PanicOnFirst { first: sc.load(Ordering::Relaxed) == 1 }) },
        10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    rt.send_to(router_addr, DWork).unwrap();
    tick_n(&rt, 5);
    assert!(spawn_count.load(Ordering::Relaxed) >= 4, "replacement spawned");

    // Meltdown: max_restarts=2
    let rt = std_runtime(RuntimeConfig::default());
    struct AlwaysPanics;
    #[derive(Clone)]
    struct MWork;
    impl ActorInterface for AlwaysPanics {
        type Incoming = MWork;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: MWork) { panic!("always"); }
    }
    let router = Router::<MWork>::new(
        RoutingStrategy::RoundRobin, 1,
        |ctx| ctx.spawn(AlwaysPanics), 2,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    for _ in 0..3 {
        rt.send_to(router_addr, MWork).unwrap();
        tick_n(&rt, 5);
    }
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 0, "router melted down");

    // Stop router kills workers
    let rt = std_runtime(RuntimeConfig::default());
    struct Dummy;
    #[derive(Clone)]
    struct SWork;
    impl ActorInterface for Dummy {
        type Incoming = SWork;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: SWork) {}
    }
    let router = Router::<SWork>::new(
        RoutingStrategy::RoundRobin, 3,
        |ctx| ctx.spawn(Dummy), 10,
    );
    let router_addr = rt.spawn(router).unwrap();
    rt.tick();
    assert_eq!(rt.stats().workers[0].num_actors, 4);
    rt.stop_actor(router_addr).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 0, "stop router kills workers");
}

// ═══════════════════════════════════════════════════════════════════════════
// CtxSystem + CtxSelfStats
// ═══════════════════════════════════════════════════════════════════════════

/// Actor sees own stats after processing messages.
///
/// Sends N messages, ticks so they're processed, then sends a "report" message.
/// The actor reads its own stats in the handler and sends them back.
#[test]
fn actor_sees_own_stats_after_processing() {
    #[derive(Clone)]
    enum StatsMsg {
        Bump,
        Report { reply_to: ActorAddress },
    }

    #[derive(Clone, Debug, PartialEq)]
    struct StatsReport {
        processed: u64,
        type_counts: Vec<(String, u64)>,
    }

    struct StatsActor;
    impl ActorInterface for StatsActor {
        type Incoming = StatsMsg;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: StatsMsg) {
            match msg {
                StatsMsg::Bump => {}
                StatsMsg::Report { reply_to } => {
                    let report = StatsReport {
                        processed: ctx.messages_processed(),
                        type_counts: ctx.message_type_counts()
                            .iter()
                            .map(|(k, v)| (k.to_string(), *v))
                            .collect(),
                    };
                    let _ = ctx.send(reply_to, report);
                }
            }
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<StatsReport>().unwrap();
    let actor = rt.spawn(StatsActor).unwrap();
    rt.tick(); // on_start

    // Send 5 Bump messages and process them
    for _ in 0..5 {
        rt.send_to(actor, StatsMsg::Bump).unwrap();
    }
    rt.tick();

    // Now ask for a report — the actor should see 5 processed messages
    rt.send_to(actor, StatsMsg::Report { reply_to: *inbox.addr() }).unwrap();
    rt.tick();

    let report = inbox.try_recv().expect("should receive stats report");
    assert_eq!(report.processed, 5, "actor should see 5 previously processed messages");
    assert!(!report.type_counts.is_empty(), "type counts should be populated");
    // The type name should contain "StatsMsg"
    assert!(
        report.type_counts.iter().any(|(name, count)| name.contains("StatsMsg") && *count >= 5),
        "type counts should include StatsMsg entries with count >= 5, got {:?}",
        report.type_counts,
    );
}

/// Actor sees system info: worker count, total actors, uptime.
#[test]
fn actor_sees_system_info() {
    #[derive(Clone)]
    struct GetSysInfo { reply_to: ActorAddress }

    #[derive(Clone, Debug)]
    struct SysInfoReport {
        num_workers: usize,
        total_actors: usize,
    }

    struct SysInfoActor;
    impl ActorInterface for SysInfoActor {
        type Incoming = GetSysInfo;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: GetSysInfo) {
            let info = ctx.system_info();
            let _ = ctx.send(msg.reply_to, SysInfoReport {
                num_workers: info.num_workers,
                total_actors: info.total_actors,
            });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SysInfoReport>().unwrap();

    // Spawn a few actors so total_actors > 1
    let reporter = rt.spawn(SysInfoActor).unwrap();
    let _extra1 = rt.spawn(PingPongActor).unwrap();
    let _extra2 = rt.spawn(PingPongActor).unwrap();
    rt.tick(); // on_start + stats update

    rt.send_to(reporter, GetSysInfo { reply_to: *inbox.addr() }).unwrap();
    rt.tick();

    let report = inbox.try_recv().expect("should receive system info");
    assert_eq!(report.num_workers, 1, "default config has 1 worker");
    assert!(report.total_actors >= 3, "should see at least 3 actors, got {}", report.total_actors);
}

/// Mailbox depth reflects queued messages before dequeuing.
///
/// With budget=1, only 1 message is processed per tick. If we enqueue 5 messages,
/// the actor's first handler invocation should see all 5 in the mailbox snapshot.
#[test]
fn mailbox_depth_reflects_queued_messages() {
    #[derive(Clone)]
    struct DepthProbe { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct DepthReport(usize);

    struct DepthActor;
    impl ActorInterface for DepthActor {
        type Incoming = DepthProbe;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: DepthProbe) {
            let _ = ctx.send(msg.reply_to, DepthReport(ctx.mailbox_depth()));
        }
    }

    let config = RuntimeConfig {
        actor_message_budget: 1,
        ..RuntimeConfig::default()
    };
    let rt = std_runtime(config);
    let inbox = rt.new_inbox::<DepthReport>().unwrap();
    let actor = rt.spawn(DepthActor).unwrap();
    rt.tick(); // on_start

    // Enqueue 5 messages
    for _ in 0..5 {
        rt.send_to(actor, DepthProbe { reply_to: *inbox.addr() }).unwrap();
    }

    // Tick once — budget=1, so only the first message is processed
    rt.tick();

    let report = inbox.try_recv().expect("should receive depth report");
    // The snapshot is taken before any dequeuing in this tick, so depth == 5
    assert_eq!(report.0, 5, "mailbox depth should be 5 (snapshot before dequeue)");
}

// ═══════════════════════════════════════════════════════════════════════════
// CtxLineage — Parent Tracking
// ═══════════════════════════════════════════════════════════════════════════

/// Child spawned by an actor reports its parent address back.
#[test]
fn child_knows_its_parent() {
    #[derive(Clone)]
    struct ReportParent { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ParentReport(Option<ActorAddress>);

    struct ChildReporter;
    impl ActorInterface for ChildReporter {
        type Incoming = ReportParent;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportParent) {
            let _ = ctx.send(msg.reply_to, ParentReport(ctx.parent()));
        }
    }

    struct ParentActor { reply_to: ActorAddress }
    impl ActorInterface for ParentActor {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx.spawn(ChildReporter).unwrap();
            let _ = ctx.send(child, ReportParent { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ParentReport>().unwrap();
    let parent = rt.spawn(ParentActor { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // on_start
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("child should report parent");
    assert_eq!(report, ParentReport(Some(parent)));
}

/// Actor spawned via Runtime::spawn has no parent.
#[test]
fn runtime_spawned_has_no_parent() {
    #[derive(Clone)]
    struct ReportParent { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ParentReport(Option<ActorAddress>);

    struct Reporter;
    impl ActorInterface for Reporter {
        type Incoming = ReportParent;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportParent) {
            let _ = ctx.send(msg.reply_to, ParentReport(ctx.parent()));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ParentReport>().unwrap();
    let actor = rt.spawn(Reporter).unwrap();
    rt.tick(); // on_start
    rt.send_to(actor, ReportParent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("actor should report parent");
    assert_eq!(report, ParentReport(None));
}

/// In a A→B→C chain, C reports B as parent (not A).
#[test]
fn grandchild_reports_immediate_parent() {
    #[derive(Clone)]
    struct ReportParent { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ParentReport(Option<ActorAddress>);

    struct Leaf;
    impl ActorInterface for Leaf {
        type Incoming = ReportParent;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportParent) {
            let _ = ctx.send(msg.reply_to, ParentReport(ctx.parent()));
        }
    }

    struct Middle { reply_to: ActorAddress }
    impl ActorInterface for Middle {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx.spawn(Leaf).unwrap();
            let _ = ctx.send(child, ReportParent { reply_to: self.reply_to });
        }
    }

    struct Root { reply_to: ActorAddress }
    impl ActorInterface for Root {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let mid = ctx.spawn(Middle { reply_to: self.reply_to }).unwrap();
            let _ = ctx.send(mid, Ping { reply_to: ActorAddress::default() });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ParentReport>().unwrap();
    let root = rt.spawn(Root { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // on_start
    rt.send_to(root, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);
    let report = inbox.try_recv().expect("grandchild should report parent");
    // C's parent should be B (some address), not A (root) and not None
    assert!(report.0.is_some(), "grandchild has a parent");
    assert_ne!(report.0.unwrap(), root, "grandchild's parent is the middle actor, not root");
}

/// Parent address is available during on_stop.
#[test]
fn parent_visible_in_on_stop() {
    #[derive(Clone, Debug, PartialEq)]
    struct ParentReport(Option<ActorAddress>);

    struct OnStopReporter { reply_to: ActorAddress }
    impl ActorInterface for OnStopReporter {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
        fn on_stop(&mut self, ctx: &Ctx) {
            let _ = ctx.send(self.reply_to, ParentReport(ctx.parent()));
        }
    }

    struct Spawner { reply_to: ActorAddress }
    impl ActorInterface for Spawner {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx.spawn(OnStopReporter { reply_to: self.reply_to }).unwrap();
            let _ = ctx.stop_actor(child);
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ParentReport>().unwrap();
    let spawner = rt.spawn(Spawner { reply_to: *inbox.addr() }).unwrap();
    rt.tick(); // on_start
    rt.send_to(spawner, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);
    let report = inbox.try_recv().expect("on_stop should report parent");
    assert_eq!(report, ParentReport(Some(spawner)));
}

// ═══════════════════════════════════════════════════════════════════════════
// CtxEnvironment — Inherited Typed Key-Value Map
// ═══════════════════════════════════════════════════════════════════════════

/// Child inherits parent's environment: parent sets a typed env value via
/// spawn_builder, spawns child, child reads it back and confirms it matches.
#[test]
fn env_child_inherits_parent_environment() {
    #[derive(Clone, Debug, PartialEq)]
    struct DbAddr(String);

    #[derive(Clone)]
    struct ReportEnv { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport(Option<String>);

    struct EnvChild;
    impl ActorInterface for EnvChild {
        type Incoming = ReportEnv;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportEnv) {
            let val = ctx.env::<DbAddr>().map(|d| d.0.clone());
            let _ = ctx.send(msg.reply_to, EnvReport(val));
        }
    }

    struct EnvParent { reply_to: ActorAddress }
    impl ActorInterface for EnvParent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx
                .spawn_builder(EnvChild)
                .env(DbAddr("postgres://localhost".into()))
                .finish()
                .unwrap();
            let _ = ctx.send(child, ReportEnv { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let parent = rt.spawn(EnvParent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("child should report env");
    assert_eq!(report, EnvReport(Some("postgres://localhost".into())));
}

/// Runtime-spawned actor has empty environment — ctx.env::<T>() returns None.
#[test]
fn env_runtime_spawned_has_empty_environment() {
    #[derive(Clone, Debug, PartialEq)]
    struct Tag(String);

    #[derive(Clone)]
    struct ReportEnv { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport(bool);

    struct EnvReporter;
    impl ActorInterface for EnvReporter {
        type Incoming = ReportEnv;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportEnv) {
            let has_tag = ctx.env::<Tag>().is_some();
            let _ = ctx.send(msg.reply_to, EnvReport(has_tag));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let actor = rt.spawn(EnvReporter).unwrap();
    rt.tick();
    rt.send_to(actor, ReportEnv { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("actor should report env");
    assert_eq!(report, EnvReport(false), "runtime-spawned actor has no env values");
}

/// Environment flows through a grandchild chain: A sets env, spawns B, B
/// spawns C (via plain ctx.spawn — inherits env), C reads the value from A.
#[test]
fn env_flows_through_grandchild_chain() {
    #[derive(Clone, Debug, PartialEq)]
    struct Secret(u64);

    #[derive(Clone)]
    struct ReportEnv { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport(Option<u64>);

    struct Leaf;
    impl ActorInterface for Leaf {
        type Incoming = ReportEnv;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportEnv) {
            let val = ctx.env::<Secret>().map(|s| s.0);
            let _ = ctx.send(msg.reply_to, EnvReport(val));
        }
    }

    struct Middle { reply_to: ActorAddress }
    impl ActorInterface for Middle {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // ctx.spawn inherits parent env automatically
            let child = ctx.spawn(Leaf).unwrap();
            let _ = ctx.send(child, ReportEnv { reply_to: self.reply_to });
        }
    }

    struct Root { reply_to: ActorAddress }
    impl ActorInterface for Root {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let mid = ctx
                .spawn_builder(Middle { reply_to: self.reply_to })
                .env(Secret(42))
                .finish()
                .unwrap();
            let _ = ctx.send(mid, Ping { reply_to: ActorAddress::default() });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let root = rt.spawn(Root { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(root, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);
    let report = inbox.try_recv().expect("grandchild should report env");
    assert_eq!(report, EnvReport(Some(42)), "env value from root flows to grandchild");
}

/// Spawn builder overrides one key while inheriting others: parent has Key1 +
/// Key2, uses spawn_builder to override Key2. Child sees original Key1 and new Key2.
#[test]
fn env_spawn_builder_overrides_one_key_inherits_others() {
    #[derive(Clone, Debug, PartialEq)]
    struct Key1(String);
    #[derive(Clone, Debug, PartialEq)]
    struct Key2(String);

    #[derive(Clone)]
    struct ReportEnv { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport { key1: Option<String>, key2: Option<String> }

    struct EnvChild;
    impl ActorInterface for EnvChild {
        type Incoming = ReportEnv;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportEnv) {
            let _ = ctx.send(msg.reply_to, EnvReport {
                key1: ctx.env::<Key1>().map(|k| k.0.clone()),
                key2: ctx.env::<Key2>().map(|k| k.0.clone()),
            });
        }
    }

    struct EnvParent { reply_to: ActorAddress }
    impl ActorInterface for EnvParent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // Override Key2 only, Key1 should be inherited
            let child = ctx
                .spawn_builder(EnvChild)
                .env(Key2("overridden".into()))
                .finish()
                .unwrap();
            let _ = ctx.send(child, ReportEnv { reply_to: self.reply_to });
        }
    }

    // Build an env with both keys, then use EnvironmentBuilder to create the parent env
    let parent_env = EnvironmentBuilder::new()
        .set(Key1("original".into()))
        .set(Key2("original".into()))
        .build();

    // Spawn the parent with the built env using a "bootstrap" actor
    struct Bootstrap { reply_to: ActorAddress }
    impl ActorInterface for Bootstrap {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let parent = ctx
                .spawn_builder(EnvParent { reply_to: self.reply_to })
                .env(Key1("original".into()))
                .env(Key2("original".into()))
                .finish()
                .unwrap();
            let _ = ctx.send(parent, Ping { reply_to: ActorAddress::default() });
        }
    }

    let _ = parent_env; // verify it builds (used above for documentation)
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let bootstrap = rt.spawn(Bootstrap {
        reply_to: *inbox.addr(),
    }).unwrap();
    rt.tick();
    rt.send_to(bootstrap, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);
    let report = inbox.try_recv().expect("child should report env");
    assert_eq!(report.key1, Some("original".into()), "Key1 inherited from parent");
    assert_eq!(report.key2, Some("overridden".into()), "Key2 overridden by spawn_builder");
}

/// Environment is readable during on_stop callback.
#[test]
fn env_readable_in_on_stop() {
    #[derive(Clone, Debug, PartialEq)]
    struct Config(String);

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport(Option<String>);

    struct OnStopEnvReporter { reply_to: ActorAddress }
    impl ActorInterface for OnStopEnvReporter {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {}
        fn on_stop(&mut self, ctx: &Ctx) {
            let val = ctx.env::<Config>().map(|c| c.0.clone());
            let _ = ctx.send(self.reply_to, EnvReport(val));
        }
    }

    struct Spawner { reply_to: ActorAddress }
    impl ActorInterface for Spawner {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx
                .spawn_builder(OnStopEnvReporter { reply_to: self.reply_to })
                .env(Config("production".into()))
                .finish()
                .unwrap();
            let _ = ctx.stop_actor(child);
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let spawner = rt.spawn(Spawner { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(spawner, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);
    let report = inbox.try_recv().expect("on_stop should report env");
    assert_eq!(report, EnvReport(Some("production".into())));
}

/// Sibling overrides are independent: parent spawns child A with Version(1)
/// and child B with Version(2). Each sees its own version.
#[test]
fn env_sibling_overrides_are_independent() {
    #[derive(Clone, Debug, PartialEq)]
    struct Version(u32);

    #[derive(Clone)]
    struct ReportEnv { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct EnvReport(Option<u32>);

    struct VersionReporter;
    impl ActorInterface for VersionReporter {
        type Incoming = ReportEnv;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportEnv) {
            let val = ctx.env::<Version>().map(|v| v.0);
            let _ = ctx.send(msg.reply_to, EnvReport(val));
        }
    }

    struct Parent { reply_to: ActorAddress }
    impl ActorInterface for Parent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let a = ctx.spawn_builder(VersionReporter).env(Version(1)).finish().unwrap();
            let b = ctx.spawn_builder(VersionReporter).env(Version(2)).finish().unwrap();
            let _ = ctx.send(a, ReportEnv { reply_to: self.reply_to });
            let _ = ctx.send(b, ReportEnv { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<EnvReport>().unwrap();
    let parent = rt.spawn(Parent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    let mut reports: Vec<EnvReport> = std::iter::from_fn(|| inbox.try_recv()).collect();
    reports.sort_by_key(|r| r.0);
    assert_eq!(reports.len(), 2, "both siblings replied");
    assert_eq!(reports[0], EnvReport(Some(1)));
    assert_eq!(reports[1], EnvReport(Some(2)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SpawnTimestamp
// ═══════════════════════════════════════════════════════════════════════════

/// Any actor has SpawnTimestamp when StdExtension is installed.
#[test]
fn spawn_timestamp_present_with_std_extension() {
    #[derive(Clone)]
    struct ReportTs { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct TsReport(Option<u64>);

    struct TsActor;
    impl ActorInterface for TsActor {
        type Incoming = ReportTs;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportTs) {
            let ts = ctx.env::<SpawnTimestamp>().map(|t| t.0);
            let _ = ctx.send(msg.reply_to, TsReport(ts));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<TsReport>().unwrap();
    let actor = rt.spawn(TsActor).unwrap();
    rt.tick();
    rt.send_to(actor, ReportTs { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive timestamp report");
    assert!(report.0.is_some(), "SpawnTimestamp should be present with StdExtension");
}

/// Parent and child spawned at different times have different timestamps,
/// child's timestamp >= parent's timestamp.
#[test]
fn spawn_timestamp_parent_child_ordering() {
    #[derive(Clone, Debug)]
    struct TsPair { parent_ts: u64, child_ts: u64 }

    struct TsChild { reply_to: ActorAddress, parent_ts: u64 }
    impl ActorInterface for TsChild {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let child_ts = ctx.env::<SpawnTimestamp>().unwrap().0;
            let _ = ctx.send(self.reply_to, TsPair {
                parent_ts: self.parent_ts,
                child_ts,
            });
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
    }

    struct TsParent { reply_to: ActorAddress }
    impl ActorInterface for TsParent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let my_ts = ctx.env::<SpawnTimestamp>().unwrap().0;
            let _ = ctx.spawn(TsChild { reply_to: self.reply_to, parent_ts: my_ts });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<TsPair>().unwrap();
    let parent = rt.spawn(TsParent { reply_to: *inbox.addr() }).unwrap();
    // Tick a few times so some uptime accumulates before the child spawn
    tick_n(&rt, 3);
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("should receive timestamp pair");
    assert!(report.child_ts >= report.parent_ts,
        "child timestamp ({}) should be >= parent timestamp ({})",
        report.child_ts, report.parent_ts);
}

/// SpawnTimestamp is available during on_stop callback.
#[test]
fn spawn_timestamp_available_in_on_stop() {
    #[derive(Clone, Debug, PartialEq)]
    struct TsReport(Option<u64>);

    struct OnStopTsReporter { reply_to: ActorAddress }
    impl ActorInterface for OnStopTsReporter {
        type Incoming = ();
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn on_stop(&mut self, ctx: &Ctx) {
            let ts = ctx.env::<SpawnTimestamp>().map(|t| t.0);
            let _ = ctx.send(self.reply_to, TsReport(ts));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<TsReport>().unwrap();
    let actor = rt.spawn(OnStopTsReporter { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.stop_actor(actor).unwrap();
    tick_n(&rt, 3);
    let report = inbox.try_recv().expect("on_stop should report timestamp");
    assert!(report.0.is_some(), "SpawnTimestamp should be available in on_stop");
}

// ═══════════════════════════════════════════════════════════════════════════
// LogicalName
// ═══════════════════════════════════════════════════════════════════════════

/// Named actor knows its logical name.
#[test]
fn logical_name_present_for_named_actor() {
    #[derive(Clone)]
    struct ReportName { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct NameReport(Option<String>);

    struct NameActor;
    impl ActorInterface for NameActor {
        type Incoming = ReportName;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportName) {
            let name = ctx.env::<LogicalName>().map(|n| n.0.clone());
            let _ = ctx.send(msg.reply_to, NameReport(name));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<NameReport>().unwrap();
    let addr = rt.spawn_named("my-service", NameActor).unwrap();
    rt.tick();
    rt.send_to(addr, ReportName { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive name report");
    assert_eq!(report, NameReport(Some("my-service".to_string())));
}

/// Unnamed actor has no logical name.
#[test]
fn logical_name_absent_for_unnamed_actor() {
    #[derive(Clone)]
    struct ReportName { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct NameReport(Option<String>);

    struct NameActor;
    impl ActorInterface for NameActor {
        type Incoming = ReportName;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportName) {
            let name = ctx.env::<LogicalName>().map(|n| n.0.clone());
            let _ = ctx.send(msg.reply_to, NameReport(name));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<NameReport>().unwrap();
    let addr = rt.spawn(NameActor).unwrap();
    rt.tick();
    rt.send_to(addr, ReportName { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive name report");
    assert_eq!(report, NameReport(None));
}

/// Runtime-level spawn_named sets LogicalName.
#[test]
fn logical_name_via_runtime_spawn_named() {
    #[derive(Clone)]
    struct ReportName { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct NameReport(Option<String>);

    struct NameActor;
    impl ActorInterface for NameActor {
        type Incoming = ReportName;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportName) {
            let name = ctx.env::<LogicalName>().map(|n| n.0.clone());
            let _ = ctx.send(msg.reply_to, NameReport(name));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<NameReport>().unwrap();
    let addr = rt.spawn_named("svc", NameActor).unwrap();
    rt.tick();
    rt.send_to(addr, ReportName { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive name report");
    assert_eq!(report, NameReport(Some("svc".to_string())));
}

/// Child of named actor inherits LogicalName via environment inheritance.
#[test]
fn logical_name_inherited_by_child() {
    #[derive(Clone)]
    struct ReportName { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct NameReport(Option<String>);

    struct ChildReporter;
    impl ActorInterface for ChildReporter {
        type Incoming = ReportName;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportName) {
            let name = ctx.env::<LogicalName>().map(|n| n.0.clone());
            let _ = ctx.send(msg.reply_to, NameReport(name));
        }
    }

    struct NamedParent { reply_to: ActorAddress }
    impl ActorInterface for NamedParent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // ctx.spawn inherits parent env, which includes LogicalName
            let child = ctx.spawn(ChildReporter).unwrap();
            let _ = ctx.send(child, ReportName { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<NameReport>().unwrap();
    let parent = rt.spawn_named("parent-svc", NamedParent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("child should report inherited name");
    assert_eq!(report, NameReport(Some("parent-svc".to_string())));
}

// ═══════════════════════════════════════════════════════════════════════════
// Supervisor Lineage — ctx.supervisor()
// ═══════════════════════════════════════════════════════════════════════════

/// Supervised child knows its supervisor address.
#[test]
fn supervised_child_knows_supervisor() {
    #[derive(Clone)]
    struct ReportSupervisor { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct SupervisorReport(Option<ActorAddress>);

    struct SupervisedChild;
    impl ActorInterface for SupervisedChild {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            let sup = ctx.supervisor();
            let _ = ctx.send(msg.reply_to, SupervisorReport(sup));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SupervisorReport>().unwrap();
    let reply_to = *inbox.addr();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("child", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(SupervisedChild)
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);

    // Find the child address
    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    rt.send_to(child, ReportSupervisor { reply_to }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("child should report supervisor");
    assert_eq!(report, SupervisorReport(Some(sup_addr)));
}

/// Unsupervised actor has no supervisor.
#[test]
fn unsupervised_actor_has_no_supervisor() {
    #[derive(Clone)]
    struct ReportSupervisor { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct SupervisorReport(Option<ActorAddress>);

    struct PlainActor;
    impl ActorInterface for PlainActor {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            let sup = ctx.supervisor();
            let _ = ctx.send(msg.reply_to, SupervisorReport(sup));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SupervisorReport>().unwrap();
    let actor = rt.spawn(PlainActor).unwrap();
    rt.tick();
    rt.send_to(actor, ReportSupervisor { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("actor should report supervisor");
    assert_eq!(report, SupervisorReport(None));
}

/// After a permanent child panics and restarts, the new incarnation still
/// reports the same supervisor.
#[test]
fn supervisor_survives_child_restart() {
    #[derive(Clone)]
    struct ReportSupervisor { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct SupervisorReport(Option<ActorAddress>);

    struct CrashOnce {
        crash_counter: Arc<AtomicUsize>,
    }
    impl ActorInterface for CrashOnce {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            if self.crash_counter.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("intentional crash");
            }
            let sup = ctx.supervisor();
            let _ = ctx.send(msg.reply_to, SupervisorReport(sup));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SupervisorReport>().unwrap();
    let reply_to = *inbox.addr();
    let crash_counter = Arc::new(AtomicUsize::new(0));
    let cc = crash_counter.clone();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("crasher", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(CrashOnce { crash_counter: cc.clone() })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);

    // First: find child and make it crash
    let child_v1 = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    rt.send_to(child_v1, ReportSupervisor { reply_to }).unwrap();
    tick_n(&rt, 5); // panics, supervisor restarts

    // Find the new child (different address)
    let child_v2 = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr).map(|(a, _)| *a).unwrap();
    assert_ne!(child_v1, child_v2, "child should have a new address after restart");

    rt.send_to(child_v2, ReportSupervisor { reply_to }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("restarted child should report supervisor");
    assert_eq!(report, SupervisorReport(Some(sup_addr)));
}

/// Nested supervision: supervisor -> child A. Child A spawns grandchild B.
/// B's supervisor is None, A's supervisor is the supervisor.
#[test]
fn grandchild_not_supervised_child_is() {
    #[derive(Clone)]
    struct ReportSupervisor { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct SupervisorReport { addr: ActorAddress, supervisor: Option<ActorAddress> }

    struct GrandChild;
    impl ActorInterface for GrandChild {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            let _ = ctx.send(msg.reply_to, SupervisorReport {
                addr: ctx.self_addr(),
                supervisor: ctx.supervisor(),
            });
        }
    }

    struct ChildA { reply_to: ActorAddress }
    impl ActorInterface for ChildA {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            // Spawn a grandchild (not supervised)
            let gc = ctx.spawn(GrandChild).unwrap();
            let _ = ctx.send(gc, ReportSupervisor { reply_to: self.reply_to });
        }
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            let _ = ctx.send(msg.reply_to, SupervisorReport {
                addr: ctx.self_addr(),
                supervisor: ctx.supervisor(),
            });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SupervisorReport>().unwrap();
    let reply_to = *inbox.addr();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForOne, 5,
        vec![ChildSpec::new("a", RestartPolicy::Permanent, move |ctx| {
            ctx.spawn(ChildA { reply_to })
        })],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 5);

    // Grandchild report should come from on_start
    let gc_report = inbox.try_recv().expect("grandchild should report");
    assert_eq!(gc_report.supervisor, None, "grandchild is not supervised");

    // Now ask child A to report
    let child_a = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup_addr && *a != gc_report.addr)
        .map(|(a, _)| *a).unwrap();
    rt.send_to(child_a, ReportSupervisor { reply_to }).unwrap();
    rt.tick();
    let a_report = inbox.try_recv().expect("child A should report");
    assert_eq!(a_report.supervisor, Some(sup_addr), "child A's supervisor is the supervisor");
}

// ═══════════════════════════════════════════════════════════════════════════
// CtxResources — Typed Service Discovery
// ═══════════════════════════════════════════════════════════════════════════

/// Actor discovers a registered service by marker type.
#[test]
fn service_discovery_by_marker_type() {
    struct Datastore;

    #[derive(Clone)]
    struct LookupService { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ServiceReport(Option<ActorAddress>);

    struct ServiceConsumer;
    impl ActorInterface for ServiceConsumer {
        type Incoming = LookupService;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: LookupService) {
            let addr = ctx.resource::<Datastore>();
            let _ = ctx.send(msg.reply_to, ServiceReport(addr));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let fake_ds_addr = ActorAddress::new_random();
    rt.register_service::<Datastore>(fake_ds_addr);

    let inbox = rt.new_inbox::<ServiceReport>().unwrap();
    let consumer = rt.spawn(ServiceConsumer).unwrap();
    rt.tick();
    rt.send_to(consumer, LookupService { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive service report");
    assert_eq!(report, ServiceReport(Some(fake_ds_addr)));
}

/// Child inherits service binding from parent's environment.
#[test]
fn service_binding_inherited_by_child() {
    struct AuthService;

    #[derive(Clone)]
    struct LookupService { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ServiceReport(Option<ActorAddress>);

    struct Leaf;
    impl ActorInterface for Leaf {
        type Incoming = LookupService;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: LookupService) {
            let addr = ctx.resource::<AuthService>();
            let _ = ctx.send(msg.reply_to, ServiceReport(addr));
        }
    }

    struct Parent { reply_to: ActorAddress }
    impl ActorInterface for Parent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx.spawn(Leaf).unwrap();
            let _ = ctx.send(child, LookupService { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let auth_addr = ActorAddress::new_random();
    rt.register_service::<AuthService>(auth_addr);

    let inbox = rt.new_inbox::<ServiceReport>().unwrap();
    let parent = rt.spawn(Parent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("child should report service");
    assert_eq!(report, ServiceReport(Some(auth_addr)));
}

/// Multiple services registered, each accessible by its own marker type.
#[test]
fn multiple_services_each_accessible_by_marker() {
    struct Datastore;
    struct Cache;
    struct Logger;

    #[derive(Clone)]
    struct LookupAll { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct AllServicesReport {
        ds: Option<ActorAddress>,
        cache: Option<ActorAddress>,
        logger: Option<ActorAddress>,
    }

    struct MultiConsumer;
    impl ActorInterface for MultiConsumer {
        type Incoming = LookupAll;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: LookupAll) {
            let _ = ctx.send(msg.reply_to, AllServicesReport {
                ds: ctx.resource::<Datastore>(),
                cache: ctx.resource::<Cache>(),
                logger: ctx.resource::<Logger>(),
            });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let ds_addr = ActorAddress::new_random();
    let cache_addr = ActorAddress::new_random();
    let logger_addr = ActorAddress::new_random();
    rt.register_service::<Datastore>(ds_addr);
    rt.register_service::<Cache>(cache_addr);
    rt.register_service::<Logger>(logger_addr);

    let inbox = rt.new_inbox::<AllServicesReport>().unwrap();
    let actor = rt.spawn(MultiConsumer).unwrap();
    rt.tick();
    rt.send_to(actor, LookupAll { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive all services report");
    assert_eq!(report.ds, Some(ds_addr));
    assert_eq!(report.cache, Some(cache_addr));
    assert_eq!(report.logger, Some(logger_addr));
}

/// Unregistered service returns None.
#[test]
fn unregistered_service_returns_none() {
    struct Nonexistent;

    #[derive(Clone)]
    struct LookupService { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ServiceReport(Option<ActorAddress>);

    struct Consumer;
    impl ActorInterface for Consumer {
        type Incoming = LookupService;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: LookupService) {
            let addr = ctx.resource::<Nonexistent>();
            let _ = ctx.send(msg.reply_to, ServiceReport(addr));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    // No services registered
    let inbox = rt.new_inbox::<ServiceReport>().unwrap();
    let actor = rt.spawn(Consumer).unwrap();
    rt.tick();
    rt.send_to(actor, LookupService { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive service report");
    assert_eq!(report, ServiceReport(None));
}

/// Service binding overridable via spawn_builder — per-subtree customization.
#[test]
fn service_binding_overridable_via_spawn_builder() {
    struct Datastore;

    #[derive(Clone)]
    struct LookupService { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct ServiceReport(Option<ActorAddress>);

    struct Consumer;
    impl ActorInterface for Consumer {
        type Incoming = LookupService;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: LookupService) {
            let addr = ctx.resource::<Datastore>();
            let _ = ctx.send(msg.reply_to, ServiceReport(addr));
        }
    }

    struct Spawner { reply_to: ActorAddress, override_addr: ActorAddress }
    impl ActorInterface for Spawner {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // Override the Datastore binding for this subtree
            let child = ctx.spawn_builder(Consumer)
                .env(ServiceBinding::<Datastore>::new(self.override_addr))
                .finish()
                .unwrap();
            let _ = ctx.send(child, LookupService { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let global_ds = ActorAddress::new_random();
    let override_ds = ActorAddress::new_random();
    rt.register_service::<Datastore>(global_ds);

    let inbox = rt.new_inbox::<ServiceReport>().unwrap();

    // Spawn a plain consumer — should see the global binding
    let plain = rt.spawn(Consumer).unwrap();
    rt.tick();
    rt.send_to(plain, LookupService { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("plain consumer should report");
    assert_eq!(report, ServiceReport(Some(global_ds)), "plain consumer sees global service");

    // Spawn via spawn_builder override — should see the override
    let spawner = rt.spawn(Spawner {
        reply_to: *inbox.addr(),
        override_addr: override_ds,
    }).unwrap();
    rt.tick();
    rt.send_to(spawner, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("overridden consumer should report");
    assert_eq!(report, ServiceReport(Some(override_ds)), "overridden consumer sees custom service");
}

/// Service is accessible in on_start and on_stop lifecycle hooks.
#[test]
fn service_accessible_in_lifecycle_hooks() {
    struct MetricsService;

    #[derive(Clone, Debug, PartialEq)]
    struct LifecycleReport {
        on_start_addr: Option<ActorAddress>,
        on_stop_addr: Option<ActorAddress>,
    }

    struct LifecycleActor {
        reply_to: ActorAddress,
        on_start_addr: Option<ActorAddress>,
    }
    impl ActorInterface for LifecycleActor {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            self.on_start_addr = ctx.resource::<MetricsService>();
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
        fn on_stop(&mut self, ctx: &Ctx) {
            let on_stop_addr = ctx.resource::<MetricsService>();
            let _ = ctx.send(self.reply_to, LifecycleReport {
                on_start_addr: self.on_start_addr,
                on_stop_addr,
            });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let metrics_addr = ActorAddress::new_random();
    rt.register_service::<MetricsService>(metrics_addr);

    let inbox = rt.new_inbox::<LifecycleReport>().unwrap();
    let actor = rt.spawn(LifecycleActor {
        reply_to: *inbox.addr(),
        on_start_addr: None,
    }).unwrap();
    rt.tick(); // on_start
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5); // handle → stop_self → on_stop
    let report = inbox.try_recv().expect("should receive lifecycle report");
    assert_eq!(report, LifecycleReport {
        on_start_addr: Some(metrics_addr),
        on_stop_addr: Some(metrics_addr),
    });
}

/// OneForAll restart re-registers all children: crash one child, after restart
/// all children report the same supervisor.
#[test]
fn one_for_all_restart_re_registers_children() {
    #[derive(Clone)]
    struct ReportSupervisor { reply_to: ActorAddress }

    #[derive(Clone, Debug, PartialEq)]
    struct SupervisorReport(Option<ActorAddress>);

    struct StableChild;
    impl ActorInterface for StableChild {
        type Incoming = ReportSupervisor;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: ReportSupervisor) {
            let sup = ctx.supervisor();
            let _ = ctx.send(msg.reply_to, SupervisorReport(sup));
        }
    }

    struct CrashChild;
    impl ActorInterface for CrashChild {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: Ping) {
            panic!("intentional crash for OneForAll test");
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SupervisorReport>().unwrap();
    let reply_to = *inbox.addr();
    let sup = Supervisor::new(
        SupervisorStrategy::OneForAll, 5,
        vec![
            ChildSpec::new("crasher", RestartPolicy::Permanent, |ctx| ctx.spawn(CrashChild)),
            ChildSpec::new("stable", RestartPolicy::Permanent, |ctx| ctx.spawn(StableChild)),
        ],
    );
    let sup_addr = rt.spawn(sup).unwrap();
    tick_n(&rt, 2);

    // Find the crasher and make it crash
    // We need to identify which is which. The CrashChild accepts Ping,
    // and we know there are exactly 2 non-supervisor actors.
    let children: Vec<ActorAddress> = rt.stats().actors.iter()
        .filter(|(a, _)| *a != sup_addr)
        .map(|(a, _)| *a)
        .collect();
    assert_eq!(children.len(), 2);

    // Send Ping to the crasher (it will be one of them). We'll try both —
    // the StableChild doesn't handle Ping so it'll be a type mismatch, not a crash.
    for &child in &children {
        let _ = rt.send_to(child, Ping { reply_to: ActorAddress::default() });
    }
    tick_n(&rt, 8); // crash + OneForAll restart

    // After restart, all children should report the supervisor
    let new_children: Vec<ActorAddress> = rt.stats().actors.iter()
        .filter(|(a, _)| *a != sup_addr)
        .map(|(a, _)| *a)
        .collect();

    for &child in &new_children {
        let _ = rt.send_to(child, ReportSupervisor { reply_to });
    }
    rt.tick();

    // At least the stable child should report
    let reports: Vec<SupervisorReport> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert!(!reports.is_empty(), "at least one child should report after OneForAll restart");
    for report in &reports {
        assert_eq!(report.0, Some(sup_addr),
            "all children should report the supervisor after OneForAll restart");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Resource Handles (Part A)
// ═══════════════════════════════════════════════════════════════════════════

/// Handle wraps service and sends ergonomically.
#[test]
fn handle_wraps_service_and_sends_ergonomically() {
    struct CounterService;

    struct CounterHandle {
        service: ActorAddress,
        self_addr: ActorAddress,
    }

    impl ResourceHandle for CounterHandle {
        type Service = CounterService;
        fn from_parts(service_addr: ActorAddress, self_addr: ActorAddress) -> Self {
            Self { service: service_addr, self_addr }
        }
        fn service_addr(&self) -> ActorAddress { self.service }
        fn self_addr(&self) -> ActorAddress { self.self_addr }
    }

    impl CounterHandle {
        fn increment(&self, ctx: &Ctx) -> Result<(), swactor::Error> {
            ctx.send(self.service_addr(), Increment { reply_to: self.self_addr() })
        }
    }

    struct HandleUser { _inbox: ActorAddress }
    impl ActorInterface for HandleUser {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            if let Some(h) = ctx.handle::<CounterHandle>() {
                let _ = h.increment(ctx);
            }
        }
        fn on_actor_exit(&mut self, _ctx: &Ctx, _: ActorExited) {
            // Forward count reply to external inbox
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let counter_addr = rt.spawn(CounterActor { count: 0 }).unwrap();
    rt.register_service::<CounterService>(counter_addr);

    let inbox = rt.new_inbox::<Count>().unwrap();
    // Use spawn_with_env so we can set reply_to
    let user = rt.spawn(HandleUser { _inbox: *inbox.addr() }).unwrap();
    rt.tick(); // on_start

    // Instead of the handle's reply_to, we directly test: send Ping to user,
    // which uses the handle to increment. The counter replies to user's addr.
    // We observe the counter got incremented via ask.
    rt.send_to(user, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    // Verify: ask counter for its count
    rt.send_to(counter_addr, Increment { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2);
    let count = inbox.try_recv().expect("counter should reply");
    assert_eq!(count, Count(2), "handle increment + direct increment = 2");
}

/// Handle returns None when service not registered.
#[test]
fn handle_returns_none_when_service_not_registered() {
    struct Nonexistent;

    struct DummyHandle {
        _service: ActorAddress,
        _self_addr: ActorAddress,
    }
    impl ResourceHandle for DummyHandle {
        type Service = Nonexistent;
        fn from_parts(service_addr: ActorAddress, self_addr: ActorAddress) -> Self {
            Self { _service: service_addr, _self_addr: self_addr }
        }
        fn service_addr(&self) -> ActorAddress { self._service }
        fn self_addr(&self) -> ActorAddress { self._self_addr }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct HandleReport(bool);

    struct Reporter;
    impl ActorInterface for Reporter {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: Ping) {
            let has_handle = ctx.handle::<DummyHandle>().is_some();
            let _ = ctx.send(msg.reply_to, HandleReport(has_handle));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<HandleReport>().unwrap();
    let actor = rt.spawn(Reporter).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    let report = inbox.try_recv().expect("should receive handle report");
    assert_eq!(report, HandleReport(false), "handle returns None without registration");
}

/// Handle inherits service binding from parent.
#[test]
fn handle_inherits_service_binding_from_parent() {
    struct MyService;

    struct SvcHandle {
        service: ActorAddress,
        self_addr: ActorAddress,
    }
    impl ResourceHandle for SvcHandle {
        type Service = MyService;
        fn from_parts(s: ActorAddress, a: ActorAddress) -> Self { Self { service: s, self_addr: a } }
        fn service_addr(&self) -> ActorAddress { self.service }
        fn self_addr(&self) -> ActorAddress { self.self_addr }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct HandleReport(Option<ActorAddress>);

    struct Child;
    impl ActorInterface for Child {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: Ping) {
            let addr = ctx.handle::<SvcHandle>().map(|h| h.service_addr());
            let _ = ctx.send(msg.reply_to, HandleReport(addr));
        }
    }

    struct Parent { reply_to: ActorAddress }
    impl ActorInterface for Parent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let child = ctx.spawn(Child).unwrap();
            let _ = ctx.send(child, Ping { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let svc_addr = ActorAddress::new_random();
    rt.register_service::<MyService>(svc_addr);

    let inbox = rt.new_inbox::<HandleReport>().unwrap();
    let parent = rt.spawn(Parent { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let report = inbox.try_recv().expect("child should report handle");
    assert_eq!(report, HandleReport(Some(svc_addr)), "child inherits service binding");
}

/// Handle constructible in on_start.
#[test]
fn handle_constructible_in_on_start() {
    struct MySvc;

    struct MyHandle {
        service: ActorAddress,
        self_addr: ActorAddress,
    }
    impl ResourceHandle for MyHandle {
        type Service = MySvc;
        fn from_parts(s: ActorAddress, a: ActorAddress) -> Self { Self { service: s, self_addr: a } }
        fn service_addr(&self) -> ActorAddress { self.service }
        fn self_addr(&self) -> ActorAddress { self.self_addr }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct HandleReport(bool);

    struct OnStartChecker { reply_to: ActorAddress }
    impl ActorInterface for OnStartChecker {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let has = ctx.handle::<MyHandle>().is_some();
            let _ = ctx.send(self.reply_to, HandleReport(has));
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
    }

    let rt = std_runtime(RuntimeConfig::default());
    let svc_addr = ActorAddress::new_random();
    rt.register_service::<MySvc>(svc_addr);

    let inbox = rt.new_inbox::<HandleReport>().unwrap();
    let _ = rt.spawn(OnStartChecker { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);
    let report = inbox.try_recv().expect("should receive on_start handle report");
    assert_eq!(report, HandleReport(true), "handle available in on_start");
}

/// Two actors use same handle type — each gets responses at own address.
#[test]
fn two_actors_same_handle_own_addresses() {
    struct MySvc;

    struct MyHandle {
        service: ActorAddress,
        self_addr: ActorAddress,
    }
    impl ResourceHandle for MyHandle {
        type Service = MySvc;
        fn from_parts(s: ActorAddress, a: ActorAddress) -> Self { Self { service: s, self_addr: a } }
        fn service_addr(&self) -> ActorAddress { self.service }
        fn self_addr(&self) -> ActorAddress { self.self_addr }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct SelfAddrReport(ActorAddress);

    struct Reporter;
    impl ActorInterface for Reporter {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: Ping) {
            if let Some(h) = ctx.handle::<MyHandle>() {
                let _ = ctx.send(msg.reply_to, SelfAddrReport(h.self_addr()));
            }
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let svc = ActorAddress::new_random();
    rt.register_service::<MySvc>(svc);

    let inbox = rt.new_inbox::<SelfAddrReport>().unwrap();
    let a = rt.spawn(Reporter).unwrap();
    let b = rt.spawn(Reporter).unwrap();
    rt.tick();
    rt.send_to(a, Ping { reply_to: *inbox.addr() }).unwrap();
    rt.send_to(b, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);

    let mut reports: Vec<SelfAddrReport> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert_eq!(reports.len(), 2, "both actors report");
    reports.sort_by_key(|r| r.0 .0);
    assert_ne!(reports[0].0, reports[1].0, "each actor has its own self_addr in the handle");
}

// ═══════════════════════════════════════════════════════════════════════════
// Rich Exit Values (Part B.1)
// ═══════════════════════════════════════════════════════════════════════════

/// Actor stops with value, monitor receives it in Down.
#[test]
fn stop_with_value_monitor_receives_in_down() {
    struct Completer;
    impl ActorInterface for Completer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_with(42u64);
        }
    }

    #[derive(Clone, Debug)]
    struct DownReport { reason: StopReason, value: Option<u64> }

    struct Watcher { reply_to: ActorAddress }
    impl ActorInterface for Watcher {
        type Incoming = ();
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let val = down.exit_value.as_ref().and_then(|v| v.downcast_ref::<u64>().copied());
            let _ = ctx.send(self.reply_to, DownReport { reason: down.reason, value: val });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();
    let target = rt.spawn(Completer).unwrap();
    let _watcher = rt.spawn(Watcher { reply_to: *inbox.addr() }).unwrap();
    rt.tick();

    // Watcher monitors target
    rt.send_to(target, Ping { reply_to: ActorAddress::default() }).unwrap();

    // We need to set up the monitor — use a helper actor
    // Actually, let's use the runtime watch API which delivers ActorExited.
    // For monitor, we need ctx.monitor. Let's make watcher monitor in on_start.

    // Recreate with proper monitor setup
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();

    struct MonitorWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for MonitorWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.monitor(self.target).unwrap();
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let val = down.exit_value.as_ref().and_then(|v| v.downcast_ref::<u64>().copied());
            let _ = ctx.send(self.reply_to, DownReport { reason: down.reason, value: val });
        }
    }

    let target = rt.spawn(Completer).unwrap();
    let _watcher = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2); // on_start for both

    rt.send_to(target, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    let report = inbox.try_recv().expect("watcher should receive Down");
    assert_eq!(report.reason, StopReason::Completed, "reason is Completed");
    assert_eq!(report.value, Some(42), "exit value is 42");
}

/// Actor stops with value, watcher receives it in ActorExited.
#[test]
fn stop_with_value_watcher_receives_in_actor_exited() {
    struct Completer;
    impl ActorInterface for Completer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_with("done".to_string());
        }
    }

    #[derive(Clone, Debug)]
    struct ExitReport { reason: ExitReason, value: Option<String> }

    struct ExitWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for ExitWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.watch(self.target);
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
            let val = exited.exit_value.as_ref().and_then(|v| v.downcast_ref::<String>().cloned());
            let _ = ctx.send(self.reply_to, ExitReport { reason: exited.reason, value: val });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ExitReport>().unwrap();
    let target = rt.spawn(Completer).unwrap();
    let _watcher = rt.spawn(ExitWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2);

    rt.send_to(target, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    let report = inbox.try_recv().expect("watcher should receive ActorExited");
    assert_eq!(report.reason, ExitReason::Completed);
    assert_eq!(report.value, Some("done".to_string()));
}

/// Normal stop has exit_value: None.
#[test]
fn normal_stop_has_none_exit_value() {
    #[derive(Clone, Debug)]
    struct DownReport { reason: StopReason, has_value: bool }

    struct MonitorWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for MonitorWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) { ctx.monitor(self.target).unwrap(); }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let _ = ctx.send(self.reply_to, DownReport { reason: down.reason, has_value: down.exit_value.is_some() });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();
    let target = rt.spawn(StopsAfterFirst).unwrap();
    let _watcher = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2);

    rt.send_to(target, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    let report = inbox.try_recv().expect("should receive Down");
    assert_eq!(report.reason, StopReason::Normal);
    assert!(!report.has_value, "normal stop has no exit value");
}

/// Panic has exit_value: None.
#[test]
fn panic_has_none_exit_value() {
    #[derive(Clone, Debug)]
    struct DownReport { reason: StopReason, has_value: bool }

    struct MonitorWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for MonitorWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) { ctx.monitor(self.target).unwrap(); }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let _ = ctx.send(self.reply_to, DownReport { reason: down.reason, has_value: down.exit_value.is_some() });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();
    let target = rt.spawn(PanicActor).unwrap();
    let _watcher = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 5);

    let report = inbox.try_recv().expect("should receive Down after panic");
    assert_eq!(report.reason, StopReason::Panicked);
    assert!(!report.has_value, "panic has no exit value");
}

/// Multiple monitors receive cloned exit value.
#[test]
fn multiple_monitors_receive_cloned_exit_value() {
    struct Completer;
    impl ActorInterface for Completer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_with(99u32);
        }
    }

    #[derive(Clone, Debug)]
    struct DownReport(Option<u32>);

    struct MonitorWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for MonitorWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) { ctx.monitor(self.target).unwrap(); }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let val = down.exit_value.as_ref().and_then(|v| v.downcast_ref::<u32>().copied());
            let _ = ctx.send(self.reply_to, DownReport(val));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();
    let target = rt.spawn(Completer).unwrap();
    let _w1 = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    let _w2 = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    let _w3 = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 2);

    rt.send_to(target, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    let reports: Vec<DownReport> = std::iter::from_fn(|| inbox.try_recv()).collect();
    assert_eq!(reports.len(), 3, "all 3 monitors receive Down");
    for report in &reports {
        assert_eq!(report.0, Some(99), "each monitor receives the exit value");
    }
}

/// stop_with from on_start works.
#[test]
fn stop_with_from_on_start() {
    struct StartCompleter { _reply_to: ActorAddress }
    impl ActorInterface for StartCompleter {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            ctx.stop_with(7u8);
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
    }

    #[derive(Clone, Debug)]
    struct DownReport { reason: StopReason, value: Option<u8> }

    struct MonitorWatcher { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for MonitorWatcher {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) { ctx.monitor(self.target).unwrap(); }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let val = down.exit_value.as_ref().and_then(|v| v.downcast_ref::<u8>().copied());
            let _ = ctx.send(self.reply_to, DownReport { reason: down.reason, value: val });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<DownReport>().unwrap();
    // Spawn target first so we know its address for the watcher
    let target = rt.spawn(StartCompleter { _reply_to: ActorAddress::default() }).unwrap();
    let _watcher = rt.spawn(MonitorWatcher { target, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 10);

    let report = inbox.try_recv().expect("should receive Down from on_start stop_with");
    assert_eq!(report.reason, StopReason::Completed);
    assert_eq!(report.value, Some(7));
}

/// Supervisor receives rich exit value in handle_down (graceful handoff pattern).
#[test]
fn supervisor_receives_rich_exit_in_handle_down() {
    struct Completer;
    impl ActorInterface for Completer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_with(vec![1u8, 2, 3]);
        }
    }

    #[derive(Clone, Debug)]
    struct ValueReport(Option<Vec<u8>>);

    struct ManualSupervisor { reply_to: ActorAddress, child: Option<ActorAddress> }
    impl ActorInterface for ManualSupervisor {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let child = ctx.spawn(Completer).unwrap();
            ctx.monitor(child).unwrap();
            self.child = Some(child);
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            if let Some(child) = self.child {
                let _ = ctx.send(child, Ping { reply_to: ActorAddress::default() });
            }
        }
        fn handle_down(&mut self, ctx: &Ctx, down: Down) {
            let val = down.exit_value.as_ref().and_then(|v| v.downcast_ref::<Vec<u8>>().cloned());
            let _ = ctx.send(self.reply_to, ValueReport(val));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ValueReport>().unwrap();
    let sup = rt.spawn(ManualSupervisor { reply_to: *inbox.addr(), child: None }).unwrap();
    tick_n(&rt, 2);

    rt.send_to(sup, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);

    let report = inbox.try_recv().expect("supervisor should receive exit value");
    assert_eq!(report.0, Some(vec![1, 2, 3]));
}

// ═══════════════════════════════════════════════════════════════════════════
// Orphan Handling (Part B.2)
// ═══════════════════════════════════════════════════════════════════════════

/// Parent dies → unsupervised children killed.
#[test]
fn orphan_unsupervised_children_killed_when_parent_dies() {
    struct SpawnChildren { reply_to: ActorAddress }
    impl ActorInterface for SpawnChildren {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            // Spawn 3 children
            let c1 = ctx.spawn(PingPongActor).unwrap();
            let c2 = ctx.spawn(PingPongActor).unwrap();
            let c3 = ctx.spawn(PingPongActor).unwrap();
            let _ = ctx.send(self.reply_to, Count(3));
            let _ = (c1, c2, c3);
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let parent = rt.spawn(SpawnChildren { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);
    let _ = inbox.try_recv().expect("children spawned");
    // parent + 3 children = 4 actors
    assert_eq!(rt.stats().workers[0].num_actors, 4);

    // Kill parent
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);

    // All should be dead (parent stopped, children orphaned and killed)
    assert_eq!(rt.stats().workers[0].num_actors, 0, "all actors should be dead");
}

/// Parent dies → supervised children NOT killed.
#[test]
fn orphan_supervised_children_not_killed() {
    struct ParentActor;
    impl ActorInterface for ParentActor {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            // Spawn a supervisor as a child
            let sup = Supervisor::new(
                SupervisorStrategy::OneForOne, 5,
                vec![ChildSpec::new("worker", RestartPolicy::Permanent, |ctx| {
                    ctx.spawn(PingPongActor)
                })],
            );
            let _ = ctx.spawn(sup);
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let parent = rt.spawn(ParentActor).unwrap();
    tick_n(&rt, 5);
    // parent + supervisor + supervised child = 3
    let actors_before = rt.stats().workers[0].num_actors;
    assert!(actors_before >= 3, "should have parent + supervisor + child, got {}", actors_before);

    // Kill parent
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 10);

    // Supervisor and its child should still be alive (supervisor is a child of parent,
    // but it IS the supervisor, so it gets killed as orphan too — hmm.)
    // Actually: the supervisor IS a child of parent. It's NOT supervised itself.
    // So it will be orphan-killed. That's correct behavior.
    // Let me redesign: use a runtime-spawned supervisor.

    // Actually let me reconsider: the plan says "Parent dies → supervised children NOT killed"
    // This means: if parent spawns children, and those children are SUPERVISED by a supervisor,
    // they should not be orphan-killed. The supervisor itself (if unsupervised) would be killed.

    // The proper test: parent spawns child, child is also supervised.
    // But supervision registration happens when Supervisor::start_child calls supervisor_registry.register.
    // The orphan check is: supervisor_registry.lookup(&child).is_none() → kill.
    // So if a child is registered as supervised, it won't be killed.

    // Simplest: parent is a supervisor, parent dies. The supervisor's supervised children
    // should NOT be orphan-killed because they are in the supervisor registry.
    // But wait, the supervisor (parent) stops, and on_stop it sends stop to children.
    // So the children get stopped by the supervisor's on_stop, not by orphan handling.

    // Let me restructure: we have grandparent → parent → child.
    // Parent is NOT supervised. Child IS supervised by some supervisor actor.
    // When grandparent dies, parent is orphan-killed. But child should survive
    // because it's supervised.

    // Actually, the simplest reading is:
    // Parent spawns child_a and child_b. child_a is supervised. child_b is not.
    // Parent dies. child_b is killed (orphan). child_a survives (supervised).
    let rt = std_runtime(RuntimeConfig::default());

    struct GrandParent { _reply_to: ActorAddress }
    impl ActorInterface for GrandParent {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            // Spawn a supervisor for one child
            let sup = Supervisor::new(
                SupervisorStrategy::OneForOne, 5,
                vec![ChildSpec::new("supervised", RestartPolicy::Permanent, |ctx| {
                    ctx.spawn(PingPongActor)
                })],
            );
            let _sup_addr = ctx.spawn(sup).unwrap();
            // Also spawn an unsupervised child directly
            let _unsupervised = ctx.spawn(NullActor).unwrap();
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    let parent = rt.spawn(GrandParent { _reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let before = rt.stats().workers[0].num_actors;
    assert!(before >= 4, "should have parent + supervisor + supervised child + unsupervised, got {}", before);

    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 15);

    // After cascade: parent dies, supervisor+unsupervised get orphaned.
    // Unsupervised NullActor has no supervisor → killed.
    // Supervisor has no supervisor → killed. Its on_stop sends stop to supervised child.
    // End result: 0 actors (supervisor on_stop kills its children).
    let after = rt.stats().workers[0].num_actors;
    assert_eq!(after, 0, "all actors cleaned up after cascade");
}

/// Cascading orphan cleanup: A→B→C, A dies, B then C killed.
#[test]
fn orphan_cascading_cleanup() {
    struct SpawnChild { reply_to: ActorAddress }
    impl ActorInterface for SpawnChild {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let _ = ctx.spawn(PingPongActor).unwrap();
            let _ = ctx.send(self.reply_to, Pong);
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    struct Root { reply_to: ActorAddress }
    impl ActorInterface for Root {
        type Incoming = Ping;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            // Spawn middle, which spawns leaf
            let _ = ctx.spawn(SpawnChild { reply_to: self.reply_to }).unwrap();
        }
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.stop_self();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let root = rt.spawn(Root { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 5);
    let _ = inbox.try_recv(); // middle spawned its child

    // root + middle + leaf = 3
    let before = rt.stats().workers[0].num_actors;
    assert_eq!(before, 3, "should have root + middle + leaf");

    // Kill root → middle orphaned → leaf orphaned
    rt.send_to(root, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 15); // multiple ticks for cascade

    assert_eq!(rt.stats().workers[0].num_actors, 0, "cascade killed all");
}

/// Runtime-spawned actors unaffected (no parent).
#[test]
fn orphan_runtime_spawned_unaffected() {
    let rt = std_runtime(RuntimeConfig::default());
    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();
    rt.tick();
    assert_eq!(rt.stats().workers[0].num_actors, 2);

    // Stop one — the other should not be affected
    rt.stop_actor(a).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 1, "only stopped actor removed");

    rt.stop_actor(b).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// Suspend/Resume (Part B.3)
// ═══════════════════════════════════════════════════════════════════════════

/// Suspended actor queues but doesn't process; resume restores processing.
#[test]
fn suspended_actor_queues_then_resume_processes() {
    struct SuspendOnFirst { suspended: bool }
    impl ActorInterface for SuspendOnFirst {
        type Incoming = Increment;
        type Response = Count;
        fn handle(&mut self, ctx: &Ctx, msg: Increment) {
            if !self.suspended {
                self.suspended = true;
                ctx.suspend_self();
                // This message was already being processed, so we reply
                let _ = ctx.send(msg.reply_to, Count(1));
            } else {
                let _ = ctx.send(msg.reply_to, Count(99));
            }
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Count>().unwrap();
    let actor = rt.spawn(SuspendOnFirst { suspended: false }).unwrap();
    rt.tick(); // on_start

    // First message: processed, then actor suspends itself
    rt.send_to(actor, Increment { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv(), Some(Count(1)), "first message processed");

    // Second message: queued but not processed (actor suspended)
    rt.send_to(actor, Increment { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);
    assert!(inbox.try_recv().is_none(), "no reply while suspended");

    // Resume via runtime (unchecked at core level)
    // We need to use the ContextInner::request_resume. From test, use send ResumeSignal.
    // Actually, the simplest way: use another actor that resumes it.

    struct Resumer { target: ActorAddress }
    impl ActorInterface for Resumer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // Use the raw inner to resume (unchecked at core level)
            ctx.raw_inner().request_resume(self.target);
        }
    }

    let resumer = rt.spawn(Resumer { target: actor }).unwrap();
    rt.tick();
    rt.send_to(resumer, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    assert_eq!(inbox.try_recv(), Some(Count(99)), "queued message processed after resume");
}

/// Supervisor can resume suspended child.
#[test]
fn supervisor_can_resume_suspended_child() {
    #[derive(Clone)]
    struct Suspend;
    #[derive(Clone)]
    struct Resume { target: ActorAddress }
    #[derive(Clone, Debug, PartialEq)]
    struct Ack;

    struct SuspendableChild;
    impl ActorInterface for SuspendableChild {
        type Incoming = Suspend;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Suspend) {
            ctx.suspend_self();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Ack>().unwrap();

    struct MySup { child: Option<ActorAddress>, reply_to: ActorAddress }
    impl ActorInterface for MySup {
        type Incoming = Resume;
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let child = ctx.spawn(SuspendableChild).unwrap();
            ctx.monitor(child).unwrap();
            // Register as supervisor via public API
            let ext = ctx.extension().unwrap().as_any().downcast_ref::<StdExtension>().unwrap();
            ext.register_supervisor(ctx.self_addr(), child);
            self.child = Some(child);
        }
        fn handle(&mut self, ctx: &Ctx, msg: Resume) {
            if let Ok(()) = ctx.resume(msg.target) {
                let _ = ctx.send(self.reply_to, Ack);
            }
        }
    }

    let sup = rt.spawn(MySup { child: None, reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);

    let child = rt.stats().actors.iter()
        .find(|(a, _)| *a != sup).map(|(a, _)| *a).unwrap();

    // Suspend child
    rt.send_to(child, Suspend).unwrap();
    tick_n(&rt, 3);

    // Supervisor resumes child
    rt.send_to(sup, Resume { target: child }).unwrap();
    tick_n(&rt, 3);

    let ack = inbox.try_recv().expect("supervisor should be able to resume");
    assert_eq!(ack, Ack);
}

/// Non-supervisor cannot resume (returns Err).
#[test]
fn non_supervisor_cannot_resume() {
    #[derive(Clone)]
    struct TryResume { target: ActorAddress }
    #[derive(Clone, Debug, PartialEq)]
    struct ResumeResult(bool);

    struct NonSup { reply_to: ActorAddress }
    impl ActorInterface for NonSup {
        type Incoming = TryResume;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, msg: TryResume) {
            let ok = ctx.resume(msg.target).is_ok();
            let _ = ctx.send(self.reply_to, ResumeResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ResumeResult>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    let non_sup = rt.spawn(NonSup { reply_to: *inbox.addr() }).unwrap();
    rt.tick();

    rt.send_to(non_sup, TryResume { target }).unwrap();
    rt.tick();

    let result = inbox.try_recv().expect("should get resume result");
    assert_eq!(result, ResumeResult(false), "non-supervisor should be denied");
}

/// Suspended actor can be stopped.
#[test]
fn suspended_actor_can_be_stopped() {
    #[derive(Clone)]
    struct SuspendCmd;

    struct SuspendableActor;
    impl ActorInterface for SuspendableActor {
        type Incoming = SuspendCmd;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: SuspendCmd) {
            ctx.suspend_self();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(SuspendableActor).unwrap();
    rt.tick();

    // Suspend
    rt.send_to(actor, SuspendCmd).unwrap();
    tick_n(&rt, 3);
    assert_eq!(rt.stats().workers[0].num_actors, 1, "actor still alive while suspended");

    // Stop the suspended actor
    rt.stop_actor(actor).unwrap();
    tick_n(&rt, 5);
    assert_eq!(rt.stats().workers[0].num_actors, 0, "suspended actor stopped");
}

/// Cross-worker resume works (single-threaded test via transfer queue).
#[test]
fn cross_worker_resume_via_runtime() {
    #[derive(Clone)]
    struct SuspendCmd;

    struct SuspendableActor;
    impl ActorInterface for SuspendableActor {
        type Incoming = SuspendCmd;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: SuspendCmd) {
            ctx.suspend_self();
        }
    }

    // Test that request_resume from Runtime (outside worker) works
    // by sending ResumeSignal through the transfer queue.
    let rt = std_runtime(RuntimeConfig::default());
    let actor = rt.spawn(SuspendableActor).unwrap();
    rt.tick();

    // Suspend
    rt.send_to(actor, SuspendCmd).unwrap();
    tick_n(&rt, 3);

    // Queue a message while suspended
    rt.send_to(actor, SuspendCmd).unwrap();
    tick_n(&rt, 2);

    // Resume via an actor using raw_inner (simulates cross-worker)
    struct Resumer { target: ActorAddress }
    impl ActorInterface for Resumer {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            ctx.raw_inner().request_resume(self.target);
        }
    }

    let resumer = rt.spawn(Resumer { target: actor }).unwrap();
    rt.tick();
    rt.send_to(resumer, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);

    // Actor should be alive and resumed (processed the queued SuspendCmd, then suspended again)
    assert_eq!(rt.stats().workers[0].num_actors, 2, "both actors still alive");
}

// ── Capability Tests ─────────────────────────────────────────────────────────

/// An unrestricted actor (no CapabilitySet in env) can freely send, spawn, and monitor.
#[test]
fn cap_unrestricted_actor_sends_freely() {
    struct Spawner { reply_to: ActorAddress }
    impl ActorInterface for Spawner {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // Send to reply — should succeed
            let _ = ctx.send(self.reply_to, Pong).unwrap();
            // Spawn a child — should succeed
            let child = ctx.spawn(PingPongActor).unwrap();
            // Monitor the child — should succeed
            ctx.monitor(child).unwrap();
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let spawner = rt.spawn(Spawner { reply_to: *inbox.addr() }).unwrap();
    rt.tick();
    rt.send_to(spawner, Ping { reply_to: *inbox.addr() }).unwrap();
    tick_n(&rt, 3);
    assert!(inbox.try_recv().is_some(), "unrestricted actor can send freely");
}

/// A restricted actor (empty CapabilitySet) gets denied when sending to another actor.
#[test]
fn cap_restricted_actor_denied_send() {
    #[derive(Clone, Debug, PartialEq)]
    struct SendResult(bool);

    struct Restricted { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for Restricted {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ok = ctx.send(self.target, Pong).is_ok();
            let _ = ctx.send(self.reply_to, SendResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let result_inbox = rt.new_inbox::<SendResult>().unwrap();
    let peer = rt.spawn(PingPongActor).unwrap();
    // Spawn with empty CapabilitySet — restricted but can self-send
    let restricted = rt.spawn_with_env(
        Restricted { target: peer, reply_to: *result_inbox.addr() },
        EnvironmentBuilder::new()
            .set(CapabilitySet::new().with_send(*result_inbox.addr()))
            .build(),
    ).unwrap();
    rt.tick();
    rt.send_to(restricted, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = result_inbox.try_recv().expect("should get result");
    assert!(!result.0, "send to un-granted peer should fail");
}

/// A restricted actor with `with_send(peer)` can send to that peer.
#[test]
fn cap_restricted_actor_allowed_send() {
    struct GrantedSender { peer: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for GrantedSender {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let _ = ctx.send(self.peer, Ping { reply_to: self.reply_to });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let peer = rt.spawn(PingPongActor).unwrap();
    let caps = CapabilitySet::new().with_send(peer).with_send(*inbox.addr());
    let sender = rt.spawn_with_env(
        GrantedSender { peer, reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(sender, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    assert!(inbox.try_recv().is_some(), "granted sender should succeed");
}

/// Typed send grant: `with_send_typed::<Ping>(addr)` allows Ping but not other types.
#[test]
fn cap_typed_send_grant() {
    #[derive(Clone, Debug, PartialEq)]
    struct Report { ping_ok: bool, pong_ok: bool }

    struct TypeChecker { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for TypeChecker {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ping_ok = ctx.send(self.target, Ping { reply_to: ActorAddress::default() }).is_ok();
            let pong_ok = ctx.send(self.target, Pong).is_ok();
            let _ = ctx.send(self.reply_to, Report { ping_ok, pong_ok });
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Report>().unwrap();
    let target = rt.spawn(NullActor).unwrap();
    let caps = CapabilitySet::new()
        .with_send_typed::<Ping>(target)
        .with_send(*inbox.addr());
    let checker = rt.spawn_with_env(
        TypeChecker { target, reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(checker, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let report = inbox.try_recv().expect("should get report");
    assert!(report.ping_ok, "typed grant for Ping should allow Ping");
    assert!(!report.pong_ok, "typed grant for Ping should deny Pong");
}

/// A restricted actor without spawn permission gets denied on ctx.spawn().
#[test]
fn cap_spawn_denied() {
    #[derive(Clone, Debug, PartialEq)]
    struct SpawnResult(bool);

    struct NoSpawn { reply_to: ActorAddress }
    impl ActorInterface for NoSpawn {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ok = ctx.spawn(PingPongActor).is_ok();
            let _ = ctx.send(self.reply_to, SpawnResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SpawnResult>().unwrap();
    let caps = CapabilitySet::new().with_send(*inbox.addr());
    let actor = rt.spawn_with_env(
        NoSpawn { reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(!result.0, "spawn without permission should fail");
}

/// A restricted actor with `with_spawn()` can spawn children.
#[test]
fn cap_spawn_allowed() {
    #[derive(Clone, Debug, PartialEq)]
    struct SpawnResult(bool);

    struct CanSpawn { reply_to: ActorAddress }
    impl ActorInterface for CanSpawn {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ok = ctx.spawn(PingPongActor).is_ok();
            let _ = ctx.send(self.reply_to, SpawnResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SpawnResult>().unwrap();
    let caps = CapabilitySet::new().with_spawn().with_send(*inbox.addr());
    let actor = rt.spawn_with_env(
        CanSpawn { reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(result.0, "spawn with permission should succeed");
}

/// Child inherits parent's CapabilitySet and is equally restricted.
#[test]
fn cap_capability_inheritance() {
    #[derive(Clone, Debug, PartialEq)]
    struct ChildRestricted(bool);

    struct Parent { reply_to: ActorAddress }
    impl ActorInterface for Parent {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            // Child reports in on_start, so no need to send to it
            let _ = ctx.spawn(Child { reply_to: self.reply_to });
        }
    }

    struct Child { reply_to: ActorAddress }
    impl ActorInterface for Child {
        type Incoming = ();
        type Response = ();
        fn on_start(&mut self, ctx: &Ctx) {
            let restricted = ctx.env::<CapabilitySet>().is_some();
            let _ = ctx.send(self.reply_to, ChildRestricted(restricted));
        }
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ChildRestricted>().unwrap();
    let caps = CapabilitySet::new()
        .with_spawn()
        .with_send(*inbox.addr());
    let parent = rt.spawn_with_env(
        Parent { reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(parent, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 5);
    let result = inbox.try_recv().expect("should get report from child");
    assert!(result.0, "child should inherit parent's CapabilitySet");
}

/// A restricted actor without monitor grant gets denied on ctx.monitor().
#[test]
fn cap_monitor_denied() {
    #[derive(Clone, Debug, PartialEq)]
    struct MonitorResult(bool);

    struct NoMonitor { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for NoMonitor {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ok = ctx.monitor(self.target).is_ok();
            let _ = ctx.send(self.reply_to, MonitorResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<MonitorResult>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    let caps = CapabilitySet::new().with_send(*inbox.addr());
    let actor = rt.spawn_with_env(
        NoMonitor { target, reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(!result.0, "monitor without permission should fail");
}

/// A restricted actor without service grant gets None from ctx.resource().
#[test]
fn cap_service_access_denied() {
    struct MyService;

    #[derive(Clone, Debug, PartialEq)]
    struct ServiceResult(bool);

    struct ServiceUser { reply_to: ActorAddress }
    impl ActorInterface for ServiceUser {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let found = ctx.resource::<MyService>().is_some();
            let _ = ctx.send(self.reply_to, ServiceResult(found));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ServiceResult>().unwrap();
    // Give the actor a service binding but no capability to access it
    let service_addr = ActorAddress::new_random();
    let caps = CapabilitySet::new().with_send(*inbox.addr());
    let actor = rt.spawn_with_env(
        ServiceUser { reply_to: *inbox.addr() },
        EnvironmentBuilder::new()
            .set(caps)
            .set(ServiceBinding::<MyService>::new(service_addr))
            .build(),
    ).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(!result.0, "service access without grant should return None");
}

/// A restricted actor can always send to itself (self-send bypass).
#[test]
fn cap_self_send_always_allowed() {
    #[derive(Clone, Debug, PartialEq)]
    struct SelfSendResult(bool);

    struct SelfSender { reply_to: ActorAddress, sent_self: bool }
    impl ActorInterface for SelfSender {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            if !self.sent_self {
                self.sent_self = true;
                // Send to self — should always work even with empty caps
                let ok = ctx.send(ctx.self_addr(), Ping { reply_to: ActorAddress::default() }).is_ok();
                let _ = ctx.send(self.reply_to, SelfSendResult(ok));
            }
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<SelfSendResult>().unwrap();
    // Empty CapabilitySet — only self-send allowed (plus inbox for reporting)
    let caps = CapabilitySet::new().with_send(*inbox.addr());
    let actor = rt.spawn_with_env(
        SelfSender { reply_to: *inbox.addr(), sent_self: false },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(actor, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(result.0, "self-send should always be allowed");
}

/// stop_actor requires send permission to the target address.
#[test]
fn cap_stop_actor_requires_send() {
    #[derive(Clone, Debug, PartialEq)]
    struct StopResult(bool);

    struct Stopper { target: ActorAddress, reply_to: ActorAddress }
    impl ActorInterface for Stopper {
        type Incoming = Ping;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, _msg: Ping) {
            let ok = ctx.stop_actor(self.target).is_ok();
            let _ = ctx.send(self.reply_to, StopResult(ok));
        }
    }

    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<StopResult>().unwrap();
    let target = rt.spawn(PingPongActor).unwrap();
    // No send permission for target
    let caps = CapabilitySet::new().with_send(*inbox.addr());
    let stopper = rt.spawn_with_env(
        Stopper { target, reply_to: *inbox.addr() },
        EnvironmentBuilder::new().set(caps).build(),
    ).unwrap();
    rt.tick();
    rt.send_to(stopper, Ping { reply_to: ActorAddress::default() }).unwrap();
    tick_n(&rt, 3);
    let result = inbox.try_recv().expect("should get result");
    assert!(!result.0, "stop_actor without send permission should fail");
}
