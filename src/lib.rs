pub mod error;

/// Public export as the oneshot channel is in the `Actor` trait signature
pub use tokio::sync::oneshot;

use tokio::{
    sync::{mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, convert_err};


const DEFAULT_CHANNEL_SIZE: usize = 100;

/// Combined 'JoinHandle' to await the actor process and 'Sender' for communication
pub struct Handle<A>
where
    A: Actor,
{
    cancel_token: CancellationToken,
    tx: mpsc::Sender<(A::Message, oneshot::Sender<A::Response>)>,
    _handle: JoinHandle<Result<()>>,
    // to prevent accidental swaps, strongly type the handle
    _type: std::marker::PhantomData<A>,
}

impl<A: Actor> Handle<A> {

    /// Send a message to the spawned `Actor` task and get a response corresponding to the `Actor::Response` type
    pub async fn send(&self, msg: A::Message) -> Result<A::Response> {
        let (tx, rx) = oneshot::channel::<A::Response>();
        self.tx.send((msg, tx)).await.map_err(convert_err)?;

        rx.await.map_err(convert_err)
    }
}

impl<A: Actor> Drop for Handle<A> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Primary trait defining an `Actor` capable of receiving, processing, and transmitting messages
pub trait Actor: Send + Sized + Unpin + 'static {
    /// The type for messages received by this `Actor`
    type Message: Send;
    /// The type for responses given by this actor when called from `Handle::send(..)`
    type Response: Send;

    /// Inner method that defines actor behavior
    fn handle_message(&self, msg: Self::Message, tx: oneshot::Sender<Self::Response>);

    /// Spawns the `Actor` utilizing the given runtime context
    /// Only `tokio` runtime is accepted for now
    fn spawn(self, ctx: &tokio::runtime::Runtime) -> Handle<Self> {
        let cancel_token = CancellationToken::new();
        let cancel = cancel_token.clone();

        let (tx, mut rx) =
            mpsc::channel::<(Self::Message, oneshot::Sender<Self::Response>)>(DEFAULT_CHANNEL_SIZE);
        let handle = ctx.spawn(async move {
            let mut res = Ok(());
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    },

                    msg = rx.recv() => {
                        match msg {
                            Some(m) => { self.handle_message(m.0, m.1); },
                            None => {res = Err(format!("Sender handle was dropped without calling cancel!").into()); break; },
                        }
                    }
                };
            }

            res
        });

        Handle {
            cancel_token,
            _handle: handle,
            tx,
            _type: std::marker::PhantomData::<Self>,
        }
    }
}
