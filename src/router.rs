use std::{collections::HashMap, sync::Arc};

use crate::{
    actor::{ActorAddress, ActorInterface, Message},
    channel::Sender,
    runtime::Runtime,
};

/// FIXME: Go over with a fine-toothed comb and reassure yourself this typing
/// makes sense, that we are not doing loads of indirection on a hot path.
///
/// A type erased `Message` to be routed between actor processes.
pub(crate) type Envelope = Arc<dyn std::any::Any + Send + Sync>;

pub(crate) trait SenderT: Send + Sync {
    fn try_send(&self, envelope: Envelope);
}

impl<M: Message> SenderT for Sender<M> {
    fn try_send(&self, envelope: Envelope) {
        if let Some(msg) = envelope.downcast_ref::<M>() {
            let _ = Sender::try_send(self, msg.clone());
        }
    }
}

/// Internal messages for the Router's own inbox
#[derive(Clone)]
pub(crate) enum RouterMessage {
    /// register addrs <addr> with sender <sender>
    AddAddr(ActorAddress, Arc<dyn SenderT>),

    /// FIXME: this will be active when we allow actors to shut themselves
    /// down. For now, disable the warning.
    #[allow(dead_code)]
    /// remove an actor from the address book
    RemoveAddr(ActorAddress),

    /// send <msg> to <addr>
    SendToAddr { addr: ActorAddress, msg: Envelope },
}

/// The `Router` is responsible for taking in and delivering all messages in the runtime.
pub(crate) struct Router {
    directory: HashMap<ActorAddress, Arc<dyn SenderT>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            directory: HashMap::new(),
        }
    }
}

impl ActorInterface for Router {
    type Incoming = RouterMessage;
    type Response = ();

    fn handle(&mut self, _ctx: &Runtime, msg: Self::Incoming) {
        match msg {
            RouterMessage::AddAddr(addr, sender) => {
                self.directory.insert(addr, sender);
            }
            RouterMessage::RemoveAddr(addr) => {
                self.directory.remove(&addr);
            }
            RouterMessage::SendToAddr { addr, msg } => {
                if let Some(sender) = self.directory.get(&addr) {
                    sender.try_send(msg);
                }
            }
        }
    }
}
