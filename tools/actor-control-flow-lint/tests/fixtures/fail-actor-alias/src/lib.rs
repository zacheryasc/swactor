use swactor::actor::{ActorInterface, Ctx};
use swactor_engine::TaskSpawner;
type Authority = TaskSpawner;
trait Resolve { type Value; }
struct Alias;
impl Resolve for Alias { type Value = Authority; }
pub struct Worker { authority: Option<<Alias as Resolve>::Value> }
impl ActorInterface for Worker {
    type Incoming = ();
    type Response = ();
    fn handle(&mut self, _: &Ctx, _: ()) {}
}
