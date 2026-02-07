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

struct Counter(u64);

impl ActorInterface for Counter {
    type Incoming = u64;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: u64) {
        self.0 += 1;
    }
}

#[test]
fn stats_reflect_actor_lifecycle() {
    let rt = Runtime::new(RuntimeConfig::default());

    let _ping1 = rt.spawn(PingActor).unwrap();
    let _ping2 = rt.spawn(PingActor).unwrap();
    let counter = rt.spawn(Counter(0)).unwrap();

    for i in 0..20u64 {
        rt.send_to(counter, i).unwrap();
    }

    // Tick enough to fully drain all messages
    for _ in 0..6 {
        rt.tick();
    }

    let s = rt.stats();
    assert_eq!(s.num_workers, 1);
    assert_eq!(s.actors.len(), 3);
    assert_eq!(s.workers[0].num_actors, 3);
    assert_eq!(s.workers[0].mailbox_depth, 0);
    assert_eq!(s.workers[0].messages_processed, 20);
}

#[test]
fn stats_reflect_multi_worker_distribution() {
    let config = RuntimeConfig {
        num_threads: 3,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    let mut addrs = Vec::new();
    for _ in 0..6 {
        addrs.push(rt.spawn(Counter(0)).unwrap());
    }

    for &addr in &addrs {
        for i in 0..10u64 {
            rt.send_to(addr, i).unwrap();
        }
    }

    let handle = rt.run().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let s = handle.runtime.stats();
    handle.shutdown();
    handle.join();

    assert_eq!(s.num_workers, 3);
    assert_eq!(s.actors.len(), 6);
    let total_processed: u64 = s.workers.iter().map(|w| w.messages_processed).sum();
    assert_eq!(total_processed, 60);
}
