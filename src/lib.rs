pub mod error;

/// Public export as the oneshot channel is in the `Actor` trait signature
pub use tokio::sync::oneshot;

use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::error::{Result, convert_err};

const DEFAULT_CHANNEL_SIZE: usize = 100;

type ActorRequest<A> = (
    <A as Actor>::Message,
    oneshot::Sender<<A as Actor>::Response>,
);

/// Wrapper defining the transmission end of a Request/Response channel with an `Actor`
pub struct ActorRequestSender<A: Actor>(mpsc::Sender<ActorRequest<A>>);

impl<A: Actor> ActorRequestSender<A> {
    pub async fn send(&self, request: A::Message) -> Result<A::Response> {
        let (tx, rx) = oneshot::channel::<A::Response>();
        self.0.send((request, tx)).await.map_err(convert_err)?;

        rx.await.map_err(convert_err)
    }
}

impl<A: Actor> From<mpsc::Sender<ActorRequest<A>>> for ActorRequestSender<A> {
    fn from(value: mpsc::Sender<ActorRequest<A>>) -> Self {
        Self(value)
    }
}

impl<A: Actor> Clone for ActorRequestSender<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Combined 'JoinHandle' to await the actor process and 'Sender' for communication
pub struct Handle<A>
where
    A: Actor,
{
    cancel_token: CancellationToken,
    tx: ActorRequestSender<A>,
    /// the task drops when the `JoinHandle` does, so be careful with the `Handle`
    _handle: JoinHandle<Result<()>>,
    // to prevent accidental swaps, strongly type the handle
    _type: std::marker::PhantomData<A>,
}

impl<A: Actor> Handle<A> {
    /// Send a message to the spawned `Actor` task and get a response corresponding to the `Actor::Response` type
    pub async fn send(&self, msg: A::Message) -> Result<A::Response> {
        self.tx.send(msg).await
    }

    /// Get a cloned sender for messaging the `Actor` this handle is for
    pub fn get_connection(&self) -> ActorRequestSender<A> {
        self.tx.clone()
    }
}

impl<A: Actor> Drop for Handle<A> {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Primary trait defining an `Actor` capable of receiving, processing, and transmitting messages
pub trait Actor: Send + Sized + 'static {
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
            tx: tx.into(),
            _type: std::marker::PhantomData::<Self>,
        }
    }
}
