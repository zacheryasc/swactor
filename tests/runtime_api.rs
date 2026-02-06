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
