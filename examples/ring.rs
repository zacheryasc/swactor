use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Inbox, Runtime, RuntimeConfig},
};

#[derive(Debug, Default, Clone)]
pub struct RingMessage {
    count: usize,
}

impl RingMessage {
    pub fn next(self) -> Self {
        Self {
            count: self.count + 1,
        }
    }
}

#[derive(Debug, Default)]
struct RingActor {
    next: ActorAddress,
}

impl RingActor {
    pub fn new(next: ActorAddress) -> Self {
        Self { next }
    }
}

impl ActorInterface for RingActor {
    type Incoming = RingMessage;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        if let Err(_) = ctx.send(self.next, msg.next()) {
            // do nothing
        }
    }
}

fn main() {
    let config = RuntimeConfig::default();
    let rt = Runtime::new(config);
    let inbox: Inbox<RingMessage> = rt.new_inbox().unwrap();

    let mut next = rt
        .spawn(RingActor::new(*inbox.addr()))
        .expect("failed to spawn");
    let num_passes = 500;
    for _ in 0..num_passes {
        let new = rt.spawn(RingActor::new(next)).expect("failed to spawn");
        next = new;
    }
    rt.send_to(next, RingMessage { count: 0 })
        .expect("failed to start message ring");

    let msg: RingMessage;
    loop {
        match inbox.try_recv() {
            Some(m) => {
                msg = m;
                break;
            }
            None => {
                rt.tick();
            }
        }
    }
    assert_eq!(msg.count, num_passes + 1); // count should equal the number of passes plus the return to main process inbox

    println!("{msg:?}");
}
