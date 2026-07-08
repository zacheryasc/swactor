use crate::actor::{ActorAddress, ActorInterface, ActorTypeMetadata, AnyActor, Message};
use crate::runtime::{Inbox, Runtime};
use parking_lot::Mutex;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

pub type AdminResult<T> = Result<T, AdminError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    ActorNotFound {
        actor: ActorAddress,
    },
    AddressMismatch {
        requested: ActorAddress,
        snapshot: ActorAddress,
    },
    TypeMismatch {
        expected_actor_type: &'static str,
        expected_message_type: &'static str,
        actual_actor_type: &'static str,
        actual_message_type: &'static str,
    },
    Timeout,
}

pub struct RuntimeAdmin<'a> {
    pub(crate) runtime: &'a Runtime,
}

pub struct Admin<T: Message> {
    pub(crate) inbox: Inbox<AdminResult<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorStatus {
    pub started: bool,
    pub suspended: bool,
    pub stopping: bool,
    pub poisoned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorSummary {
    pub address: ActorAddress,
    pub actor_type: &'static str,
    pub message_type: &'static str,
    pub worker_id: usize,
    pub parent: Option<ActorAddress>,
    pub mailbox_depth: usize,
    pub status: ActorStatus,
    pub last_message_type: Option<&'static str>,
    pub messages_handled: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListActorsResponse {
    pub actors: Vec<ActorSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectActorResponse {
    pub summary: ActorSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorStateSnapshot<A> {
    pub actor: ActorAddress,
    pub actor_type: &'static str,
    pub message_type: &'static str,
    pub actor_instance: A,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetActorStateResponse<A> {
    pub state: ActorStateSnapshot<A>,
}

impl<A: ActorInterface> ActorStateSnapshot<A> {
    pub fn new(actor: ActorAddress, actor_instance: A) -> Self {
        Self {
            actor,
            actor_type: std::any::type_name::<A>(),
            message_type: std::any::type_name::<A::Incoming>(),
            actor_instance,
        }
    }
}

impl<T: Message> Admin<T> {
    pub(crate) fn new(inbox: Inbox<AdminResult<T>>) -> Self {
        Self { inbox }
    }

    pub fn try_recv(&self) -> Option<AdminResult<T>> {
        self.inbox.try_recv()
    }

    pub fn recv_ticking(&self, rt: &Runtime, max_ticks: usize) -> AdminResult<T> {
        for _ in 0..max_ticks {
            rt.tick();
            if let Some(resp) = self.inbox.try_recv() {
                return resp;
            }
        }
        Err(AdminError::Timeout)
    }

    pub fn reply_addr(&self) -> &ActorAddress {
        self.inbox.addr()
    }
}

pub(crate) type AdminBoxedReply = Box<dyn Any + Send>;

pub(crate) enum AdminCommand {
    ListActors {
        acc: Arc<ListActorsAccumulator>,
    },
    InspectActor {
        actor: ActorAddress,
        reply_to: ActorAddress,
    },
    GetActorState {
        actor: ActorAddress,
        reply_to: ActorAddress,
        get: Box<
            dyn FnOnce(ActorAddress, &dyn AnyActor, ActorTypeMetadata) -> AdminBoxedReply + Send,
        >,
        not_found: Box<dyn FnOnce(ActorAddress) -> AdminBoxedReply + Send>,
    },
    ReplaceActorState {
        actor: ActorAddress,
        reply_to: ActorAddress,
        replace: Box<
            dyn FnOnce(&mut dyn AnyActor, ActorTypeMetadata) -> AdminResult<OperationResult> + Send,
        >,
    },
    StopActor {
        actor: ActorAddress,
        reply_to: ActorAddress,
    },
    SuspendActor {
        actor: ActorAddress,
        reply_to: ActorAddress,
    },
    ResumeActor {
        actor: ActorAddress,
        reply_to: ActorAddress,
    },
}

pub(crate) struct ListActorsAccumulator {
    pub(crate) remaining: AtomicUsize,
    pub(crate) summaries: Mutex<Vec<ActorSummary>>,
    pub(crate) reply_to: ActorAddress,
}
