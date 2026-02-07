use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Inbox, Runtime, RuntimeConfig},
};

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct EchoMessage {
    payload: usize,
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct EchoResponse(usize);

struct EchoActor;

impl ActorInterface for EchoActor {
    type Incoming = EchoMessage;
    type Response = EchoResponse;

    fn handle(&mut self, ctx: &Ctx, msg: EchoMessage) {
        let _ = ctx.send(msg.reply_to, EchoResponse(msg.payload));
    }
}

/// Child actor that doubles the payload and replies.
struct DoubleActor;

#[derive(Clone)]
struct DoubleRequest {
    value: usize,
    reply_to: ActorAddress,
}

#[derive(Clone, Debug, PartialEq)]
struct DoubleResponse(usize);

impl ActorInterface for DoubleActor {
    type Incoming = DoubleRequest;
    type Response = DoubleResponse;

    fn handle(&mut self, ctx: &Ctx, msg: DoubleRequest) {
        let _ = ctx.send(msg.reply_to, DoubleResponse(msg.value * 2));
    }
}

/// Parent actor that spawns a DoubleActor child and delegates work.
struct DelegateActor;

#[derive(Clone)]
struct DelegateRequest {
    value: usize,
    reply_to: ActorAddress,
}

impl ActorInterface for DelegateActor {
    type Incoming = DelegateRequest;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: DelegateRequest) {
        let child = ctx.spawn(DoubleActor).expect("spawn child");
        let _ = ctx.send(
            child,
            DoubleRequest {
                value: msg.value,
                reply_to: msg.reply_to,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_single_thread_spawn_actor_and_inbox() {
    let rt = Runtime::new(RuntimeConfig::default());

    let actor_addr = rt.spawn(EchoActor).expect("spawn echo actor");
    let inbox: Inbox<EchoResponse> = rt.new_inbox().unwrap();

    rt.send_to(
        actor_addr,
        EchoMessage {
            payload: 42,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    for _ in 0..10 {
        rt.tick();
        if let Some(response) = inbox.try_recv() {
            assert_eq!(response, EchoResponse(42));
            return;
        }
    }

    panic!("Did not receive EchoResponse");
}

#[test]
fn test_multi_thread_spawn_actor_and_inbox() {
    let config = RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    let actor_addr = rt.spawn(EchoActor).expect("spawn echo actor");
    let inbox: Inbox<EchoResponse> = rt.new_inbox().unwrap();

    rt.send_to(
        actor_addr,
        EchoMessage {
            payload: 99,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    let handle = rt.run().unwrap();

    let check = std::thread::spawn(move || {
        for _ in 0..100 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Some(response) = inbox.try_recv() {
                handle.shutdown();
                return Some(response);
            }
        }
        handle.shutdown();
        None
    });

    let result = check.join().unwrap();
    assert_eq!(result, Some(EchoResponse(99)));
}

// ---------------------------------------------------------------------------
// Behavioral story tests
// ---------------------------------------------------------------------------

#[test]
fn send_to_unknown_address_fails() {
    let rt = Runtime::new(RuntimeConfig::default());
    let bogus = ActorAddress::new_random();
    let result = rt.send_to(bogus, 42u64);
    assert!(result.is_err());
}

#[test]
fn actor_spawns_child_and_delegates() {
    // Uses 2 workers so parent and child land on different workers,
    // avoiding the single-worker timing issue where pending_local
    // delivery precedes spawn-queue draining.
    let config = RuntimeConfig {
        num_threads: 2,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    let parent = rt.spawn(DelegateActor).expect("spawn parent");
    let inbox: Inbox<DoubleResponse> = rt.new_inbox().unwrap();

    rt.send_to(
        parent,
        DelegateRequest {
            value: 7,
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    let handle = rt.run().unwrap();

    let check = std::thread::spawn(move || {
        for _ in 0..100 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Some(resp) = inbox.try_recv() {
                handle.shutdown();
                return Some(resp);
            }
        }
        handle.shutdown();
        None
    });

    let result = check.join().unwrap();
    assert_eq!(result, Some(DoubleResponse(14)));
}

#[test]
fn multiple_actors_independent_mailboxes() {
    let rt = Runtime::new(RuntimeConfig::default());

    let inbox_a: Inbox<EchoResponse> = rt.new_inbox().unwrap();
    let inbox_b: Inbox<EchoResponse> = rt.new_inbox().unwrap();
    let inbox_c: Inbox<EchoResponse> = rt.new_inbox().unwrap();

    let actor_a = rt.spawn(EchoActor).unwrap();
    let actor_b = rt.spawn(EchoActor).unwrap();
    let actor_c = rt.spawn(EchoActor).unwrap();

    rt.send_to(actor_a, EchoMessage { payload: 10, reply_to: *inbox_a.addr() }).unwrap();
    rt.send_to(actor_b, EchoMessage { payload: 20, reply_to: *inbox_b.addr() }).unwrap();
    rt.send_to(actor_c, EchoMessage { payload: 30, reply_to: *inbox_c.addr() }).unwrap();

    for _ in 0..10 {
        rt.tick();
    }

    assert_eq!(inbox_a.try_recv(), Some(EchoResponse(10)));
    assert_eq!(inbox_b.try_recv(), Some(EchoResponse(20)));
    assert_eq!(inbox_c.try_recv(), Some(EchoResponse(30)));
    // No cross-contamination
    assert_eq!(inbox_a.try_recv(), None);
    assert_eq!(inbox_b.try_recv(), None);
    assert_eq!(inbox_c.try_recv(), None);
}

#[test]
fn round_robin_distributes_across_workers() {
    let config = RuntimeConfig {
        num_threads: 3,
        ..Default::default()
    };
    let rt = Runtime::new(config);

    // Counter that just counts messages
    struct Noop;
    impl ActorInterface for Noop {
        type Incoming = ();
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}
    }

    for _ in 0..6 {
        rt.spawn(Noop).unwrap();
    }

    let s = rt.stats();
    assert_eq!(s.num_workers, 3);
    // Count actors per worker from the address map snapshot
    let mut per_worker = [0usize; 3];
    for (_addr, wid) in &s.actors {
        per_worker[*wid] += 1;
    }
    // Round-robin should place exactly 2 actors on each of the 3 workers
    for (wid, &count) in per_worker.iter().enumerate() {
        assert_eq!(count, 2, "worker {} should have 2 actors", wid);
    }
}
