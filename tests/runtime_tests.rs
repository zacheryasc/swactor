use swactor::{actor::{ActorAddress, ActorInterface}, runtime::{Inbox, Runtime, RuntimeConfig}};

#[derive(Clone)]
struct PingMessage {
    reply_to: ActorAddress,
}

#[derive(Clone)]
struct PongMessage;

struct PongActor;

impl ActorInterface for PongActor {
    type Incoming = PingMessage;
    type Response = PongMessage;

    fn handle(&mut self, ctx: &Runtime, msg: PingMessage) {
        let _ = ctx.send_to(msg.reply_to, PongMessage);
    }
}

/// An actor that forwards messages to another address
struct ForwarderActor {
    target: ActorAddress,
}

#[derive(Clone)]
struct ForwardMessage(usize);

impl ActorInterface for ForwarderActor {
    type Incoming = ForwardMessage;
    type Response = ();

    fn handle(&mut self, ctx: &Runtime, msg: ForwardMessage) {
        let _ = ctx.send_to(self.target, msg);
    }
}

#[test]
fn test_single_threaded_ping_pong() {
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox: Inbox<PongMessage> = rt.new_inbox().unwrap();

    let pong_addr = rt.spawn(PongActor).expect("spawn pong");

    // Send ping
    rt.send_to(
        pong_addr,
        PingMessage {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();

    // Tick until we get a response
    for _ in 0..10 {
        rt.tick();
        if inbox.try_recv().is_some() {
            return; // Success!
        }
    }

    panic!("Did not receive pong response");
}

#[test]
fn test_single_threaded_message_chain() {
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox: Inbox<ForwardMessage> = rt.new_inbox().unwrap();

    // Create a chain: A -> B -> C -> inbox
    let c_addr = rt
        .spawn(ForwarderActor {
            target: *inbox.addr(),
        })
        .unwrap();
    let b_addr = rt.spawn(ForwarderActor { target: c_addr }).unwrap();
    let a_addr = rt.spawn(ForwarderActor { target: b_addr }).unwrap();

    // Send message to start of chain
    rt.send_to(a_addr, ForwardMessage(42)).unwrap();

    // Tick until message arrives
    for _ in 0..20 {
        rt.tick();
        if let Some(ForwardMessage(val)) = inbox.try_recv() {
            assert_eq!(val, 42);
            return;
        }
    }

    panic!("Message did not traverse the chain");
}

#[test]
fn test_multithreaded_message_passing() {
    let config = RuntimeConfig {
        num_threads: 4,
        ..Default::default()
    };
    let rt = Runtime::new(config);
    let inbox: Inbox<ForwardMessage> = rt.new_inbox().unwrap();

    // Create a longer chain to exercise multi-threading
    let mut target = *inbox.addr();
    for _ in 0..20 {
        target = rt.spawn(ForwarderActor { target }).unwrap();
    }
    let start_addr = target;

    // Send message
    rt.send_to(start_addr, ForwardMessage(999)).unwrap();

    // Spawn thread to check for result and shutdown
    let ctx = rt.run().unwrap();
    let inbox_check = std::thread::spawn(move || {
        for _ in 0..100 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Some(ForwardMessage(val)) = inbox.try_recv() {
                ctx.shutdown();
                return Some(val);
            }
        }
        ctx.shutdown();
        None
    });

    let result = inbox_check.join().unwrap();
    assert_eq!(result, Some(999));
}
