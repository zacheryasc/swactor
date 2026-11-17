use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
type Authority = TaskSpawner;
#[derive(Clone)]
struct Payload { authority: Option<std::sync::Arc<Authority>> }
#[derive(Clone)]
pub enum Message {
    Next(Box<Message>),
    Dispatch(Vec<Payload>),
}
pub struct Worker;
impl ActorInterface for Worker {
    type Incoming = Message;
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: Message) {}
}
