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
