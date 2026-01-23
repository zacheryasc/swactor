use swactor::{ActorAddress, ActorInterface, Message, Runtime, RuntimeFlavor};

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
impl Message for GreetMessage {}

impl ActorInterface for Greeter {
    type Incoming = GreetMessage;
    type Response = GreetResponse;

    fn handle(&mut self, ctx: &Runtime, msg: GreetMessage) {
        let res = GreetResponse(format!("Hello, {}!", msg.who));
        self.num_greeted += 1;
        if let Err(_) = ctx.send_to(msg.return_addr, res) {
            // no error handling
            self.num_greeted -= 1;
        }
    }
}

#[derive(Debug, Default, Clone)]
struct GreetResponse(String);
impl Message for GreetResponse {}

fn main() {
    let mut rt = Runtime::new(100, Some(RuntimeFlavor::SingleThreaded));
    let addr = rt
        .spawn(Greeter::default())
        .expect("failed to spawn greeter");
    let inbox = rt.new_inbox::<GreetResponse>();

    rt.send_to(
        addr,
        GreetMessage {
            who: "world".into(),
            return_addr: *inbox.addr(),
        },
    )
    .unwrap();
    for _ in 0..3 {
        rt.tick();
    }

    let resp = inbox.try_recv().expect("greeter should have said hello");

    println!("{}", resp.0);
}
