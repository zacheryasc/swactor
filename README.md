# swactor
(S)mall (W)ASM-compatible (actor) library

## Useful

View code dependency DAG

```bash
cargo run --manifest-path tools/depgraph/Cargo.toml -- --src-dir src/ --output deps
```

## Quick example

```rust
use swactor::{
    Ctx,
    actor::{ActorAddress, ActorInterface},
    runtime::{Runtime, RuntimeConfig},
};

#[derive(Debug, Default)]
struct Greeter { num_greeted: usize }

#[derive(Debug, Default, Clone)]
struct GreetMessage { who: String, return_addr: ActorAddress }

#[derive(Debug, Default, Clone)]
struct GreetResponse(String);

impl ActorInterface for Greeter {
    type Incoming = GreetMessage;
    type Response = GreetResponse;

    fn handle(&mut self, ctx: &Ctx, msg: GreetMessage) {
        let res = GreetResponse(format!("Hello, {}!", msg.who));
        self.num_greeted += 1;
        if let Err(_) = ctx.send(msg.return_addr, res) {
            self.num_greeted -= 1;
        }
    }
}

fn main() {
    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(Greeter::default()).expect("failed to spawn");

    let inbox = rt.new_inbox::<GreetResponse>().unwrap();
    rt.send_to(addr, GreetMessage {
        who: "world".into(),
        return_addr: *inbox.addr(),
    }).unwrap();

    for _ in 0..3 { rt.tick(); }
    let resp = inbox.try_recv().expect("should have response");
    println!("{}", resp.0); // "Hello, world!"
}
```

## Build & test

```sh
cargo build
cargo test
cargo test --features stress   # stress tests
cargo run --bin bench --release # benchmarks
cargo run --example hello
```
