use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Runtime, RuntimeConfig},
};

#[derive(Debug, Default)]
struct Greeter {
    pub num_greeted: usize,
}

#[derive(Debug, Default, Clone)]
struct GreetMessage {
    /// who do we greet?
    who: String,

    /// who do we send out greeting back to?
    return_addr: ActorAddress,
}

#[derive(Debug, Default, Clone)]
struct GreetResponse(String);

impl ActorInterface for Greeter {
    type Incoming = GreetMessage;
    type Response = GreetResponse;

    fn handle(&mut self, ctx: &Ctx, msg: GreetMessage) {
        let res = GreetResponse(format!("Hello, {}!", msg.who));
        self.num_greeted += 1;
        if let Err(_) = ctx.send(msg.return_addr, res) {
            // no error handling
            self.num_greeted -= 1;
        }
    }
}

fn main() {
    let rt = Runtime::new(RuntimeConfig::default());

    // spawn a `Greeter` in the runtime, returning an address to contact it with
    let addr = rt
        .spawn(Greeter::default())
        .expect("failed to spawn greeter");

    // create an `Inbox` that allows us to receive messages from the runtime
    let inbox = rt.new_inbox::<GreetResponse>().unwrap();

    // send a message to the `Greeter` we spawned
    rt.send_to(
        addr,
        GreetMessage {
            who: "world".into(),
            return_addr: *inbox.addr(),
        },
    )
    .unwrap();

    // default runtime is single threaded, and requires the parent process to drive
    for _ in 0..3 {
        rt.tick();
    }
    let resp = inbox.try_recv().expect("greeter should have said hello");

    println!("{}", resp.0);
}
