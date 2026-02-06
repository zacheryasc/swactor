use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Runtime, RuntimeConfig},
};

#[derive(Clone)]
struct Ping {
    reply_to: ActorAddress,
}

#[derive(Clone)]
struct Pong;

struct PingActor;

impl ActorInterface for PingActor {
    type Incoming = Ping;
    type Response = Pong;

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        let _ = ctx.send(msg.reply_to, Pong);
    }
}

/// Counter that just counts messages.
struct Counter(u64);

impl ActorInterface for Counter {
    type Incoming = u64;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: u64) {
        self.0 += 1;
    }
}

#[test]
fn stats_demo_single_thread() {
    let rt = Runtime::new(RuntimeConfig::default());

    // Spawn a few actors
    let ping1 = rt.spawn(PingActor).unwrap();
    let ping2 = rt.spawn(PingActor).unwrap();
    let counter = rt.spawn(Counter(0)).unwrap();

    // Send some messages (they queue up before we tick)
    for i in 0..20u64 {
        rt.send_to(counter, i).unwrap();
    }

    // Stats BEFORE ticking — messages are in the transfer queue, not yet in mailboxes
    let s = rt.stats();
    println!("=== Before any ticks ===");
    print_stats(&s);

    // Tick once — drains transfer queue into mailboxes, then processes messages
    rt.tick();

    let s = rt.stats();
    println!("\n=== After 1 tick ===");
    print_stats(&s);

    // Tick a few more times to drain remaining messages
    for _ in 0..5 {
        rt.tick();
    }

    let s = rt.stats();
    println!("\n=== After 6 ticks total ===");
    print_stats(&s);

    assert_eq!(s.num_workers, 1);
    assert_eq!(s.actors.len(), 3);
    assert_eq!(s.workers[0].num_actors, 3);
    // All 20 messages should be processed by now
    assert_eq!(s.workers[0].mailbox_depth, 0);
    assert!(s.workers[0].messages_processed >= 20);
}

#[test]
fn stats_demo_multi_thread() {
    let config = RuntimeConfig {
        num_threads: 3,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    // Spawn actors — round-robin will spread them across 3 workers
    let mut addrs = Vec::new();
    for _ in 0..6 {
        addrs.push(rt.spawn(Counter(0)).unwrap());
    }

    // Send messages to each actor
    for &addr in &addrs {
        for i in 0..10u64 {
            rt.send_to(addr, i).unwrap();
        }
    }

    let handle = rt.run().unwrap();

    // Let it process
    std::thread::sleep(std::time::Duration::from_millis(50));

    let s = handle.runtime.stats();
    println!("\n=== Multi-threaded (3 workers, 6 actors, 60 messages) ===");
    print_stats(&s);

    handle.shutdown();
    handle.join();

    assert_eq!(s.num_workers, 3);
    assert_eq!(s.actors.len(), 6);
    let total_processed: u64 = s.workers.iter().map(|w| w.messages_processed).sum();
    assert_eq!(total_processed, 60);
}

fn print_stats(s: &swactor::runtime::RuntimeStats) {
    println!(
        "RuntimeStats(actors={}, workers={})",
        s.actors.len(),
        s.num_workers
    );
    for w in &s.workers {
        println!(
            "  Worker {}: {} actors, {} queued, {} processed",
            w.id, w.num_actors, w.mailbox_depth, w.messages_processed
        );
        for (addr, wid) in &s.actors {
            if *wid == w.id {
                println!("    - {:x?}...", &addr.0[..4]);
            }
        }
    }
}
