use swactor::Actor;
use tokio::sync::oneshot;

pub enum GreeterMessage {
    Name(String),
}

pub enum GreeterResponse {
    Hello(String),
}

pub struct Greeter;

impl Actor for Greeter {
    type Message = GreeterMessage;
    type Response = GreeterResponse;

    fn handle_message(&self, msg: Self::Message, tx: oneshot::Sender<Self::Response>) {
        let rep = match msg {
            GreeterMessage::Name(name) => GreeterResponse::Hello(format!("Hello, {name}!")),
        };

        if let Err(_) = tx.send(rep) {
            // Greeter is not responsible for a dropped Receiver
        }
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("failed to build runtime");
    let greeter = Greeter.spawn(&rt);

    let response = rt
        .block_on(async move {
            greeter
                .send(GreeterMessage::Name("world".to_string()))
                .await
        })
        .expect("failed to get respose");

    match response {
        GreeterResponse::Hello(hello) => println!("{hello}"),
    }
}
