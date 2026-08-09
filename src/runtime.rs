use crate::Instant;
use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::actor::{
    Actor, ActorAddress, ActorInterface, ActorTypeMetadata, AnyActor, Environment, ExitValue,
    Message, ResumeSignal, SpawnRequest, StopSignal, StopWithSignal, SystemInfo,
};
use crate::admin::{
    ActorStateSnapshot, Admin, AdminCommand, AdminError, AdminResult, GetActorStateResponse,
    InspectActorResponse, ListActorsAccumulator, ListActorsResponse, OperationResult, RuntimeAdmin,
};
use crate::channel::{Receiver, Sender};
// Re-export config types so existing code using `runtime::RuntimeConfig` still works
pub use crate::config::RuntimeConfig;
use crate::delivery::{AddressMap, Envelope, InboxRegistry, TickContext};
use crate::extension::RuntimeExtension;
use crate::stats::{StatsHook, WorkerStats};
// Re-export stats types so existing code using `runtime::*` still works
use crate::Error;
pub use crate::stats::{RuntimeStats, WorkerInfo};
use crate::worker::Worker;

/// Generic message inbox for receiving messages outside of the runtime.
pub struct Inbox<M: Message> {
    addr: ActorAddress,
    inner: Receiver<M>,
}

impl<M: Message> Inbox<M> {
    pub fn addr(&self) -> &ActorAddress {
        &self.addr
    }

    pub fn try_recv(&self) -> Option<M> {
        self.inner.try_recv()
    }

    pub fn recv_ticking(&self, rt: &Runtime, max_ticks: usize) -> Option<M> {
        for _ in 0..max_ticks {
            rt.tick();
            if let Some(msg) = self.inner.try_recv() {
                return Some(msg);
            }
        }
        None
    }
}

/// Pending ask response — wraps an inbox with convenience recv methods.
///
/// Created by [`Runtime::ask`]. Provides `try_recv()` for polling and
/// `recv_ticking()` for automatic tick-until-response.
pub struct Ask<R: Message> {
    inbox: Inbox<R>,
}

impl<R: Message> Ask<R> {
    pub fn try_recv(&self) -> Option<R> {
        self.inbox.try_recv()
    }

    pub fn recv_ticking(&self, rt: &Runtime, max_ticks: usize) -> Option<R> {
        self.inbox.recv_ticking(rt, max_ticks)
    }
}

// Re-export Ctx for backwards compatibility
use crate::actor::ContextInner;
pub use crate::actor::Ctx;

// ─── Runtime ─────────────────────────────────────────────────────────────────

/// The `Runtime` struct is the primary gateway for interacting with the framework.
///
/// Owns a single `Worker` advanced by the caller via `tick()` / `try_tick()`.
///
/// Core is a transition-only state machine. Each call mutates state and
/// returns immediately, holding no control flow between calls. When to take
/// the next step is the engine's decision, not core's. Any driver that can
/// call `tick` (tokio, std-thread, a test stepper) can host it.
pub struct Runtime {
    config: RuntimeConfig,
    address_map: Arc<AddressMap>,
    inbox_registry: Arc<InboxRegistry>,
    extension: Option<Arc<dyn RuntimeExtension>>,
    transfer_tx: Sender<Envelope>,
    spawn_tx: Sender<SpawnRequest>,
    admin_tx: Sender<AdminCommand>,
    worker_stats: Arc<WorkerStats>,
    stats_hook: Option<Arc<dyn StatsHook>>,
    process_output_observer: OnceLock<Arc<dyn crate::process_observer::ProcessOutputObserver>>,
    worker: RefCell<Worker>,
    created_at: Instant,
    #[cfg(feature = "transport")]
    remote_sink: Option<Arc<dyn RemoteSink>>,
}

// Safety: `RefCell<Worker>` is only borrowed from the owning thread in
// `tick()` / `try_tick()` / `has_work()` / `with_extension()`. All `&self`
// methods callable through `Arc<Runtime>` from other threads (`send_to`,
// `spawn`, `deliver_raw`, `stats`, `create_sender`) access only `Sync` fields
// (Arcs, atomics, channels) — never the `RefCell`. No worker threads exist.
unsafe impl Sync for Runtime {}

/// Core's only hook for delivering a message to a **non-local** address.
///
/// Implemented outside core (e.g., `swactor-transport`'s `CodecRemoteSink`),
/// which owns all codec/transport concerns. Core stays codec-free: it hands the
/// sink a type-erased message and an address, and nothing more. `Send + Sync`
/// because the sink is stored in an `Arc` and shared across threads.
#[cfg(feature = "transport")]
pub trait RemoteSink: Send + Sync {
    fn send(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error>;
}

/// Globally unique identity of a swactor runtime instance.
/// Pure identity — no networking info. A runtime can exist on any device,
/// any protocol, or no network at all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuntimeAddress(pub [u8; 32]);

impl std::fmt::Display for RuntimeAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

impl RuntimeAddress {
    pub fn new_random() -> Self {
        let mut bytes = [0u8; 32];
        crate::get_random(&mut bytes);
        Self(bytes)
    }
}

/// A cloneable, `Send + Sync` handle for injecting messages into actor mailboxes
/// from any thread — including non-actor I/O threads.
///
/// Created via [`Runtime::create_sender`]. The primary use case is bridging
/// background I/O (e.g., pipe readers, network listeners) with the tick-based
/// actor system.
pub struct ExternalSender {
    address_map: Arc<AddressMap>,
    transfer_tx: Sender<Envelope>,
}

impl Clone for ExternalSender {
    fn clone(&self) -> Self {
        Self {
            address_map: self.address_map.clone(),
            transfer_tx: self.transfer_tx.clone(),
        }
    }
}

impl ExternalSender {
    /// Send a typed message to an actor address.
    ///
    /// Returns `Ok(())` if the message was accepted for routing. This does **not**
    /// guarantee delivery — the recipient may stop before processing it. If
    /// delivery confirmation is needed, implement an application-level ACK.
    ///
    /// Returns `Err` if the address is not found in the runtime's address map.
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        if self.address_map.contains(&addr) {
            self.transfer_tx.send(Envelope::new(addr, Box::new(msg)));
            Ok(())
        } else {
            Err(Error::from("Address not found"))
        }
    }
}

impl Runtime {
    /// Builds a new `Runtime` struct, but does not yet run anything.
    /// Drive via `tick()` / `try_tick()`.
    pub fn new(config: RuntimeConfig) -> Self {
        let address_map = Arc::new(AddressMap::with_capacity(config.max_actors));
        let inbox_registry = Arc::new(InboxRegistry::new());

        let transfer_rx = Receiver::<Envelope>::new(config.channel_buffer_size);
        let transfer_tx = transfer_rx.new_sender();

        let spawn_rx = Receiver::<SpawnRequest>::new(config.max_actors);
        let spawn_tx = spawn_rx.new_sender();

        let admin_rx = Receiver::<AdminCommand>::new(config.channel_buffer_size);
        let admin_tx = admin_rx.new_sender();

        let worker_stats = Arc::new(WorkerStats::new());

        let worker = Worker::new(transfer_rx, spawn_rx, admin_rx, worker_stats.clone());

        let rt = Self {
            config,
            address_map,
            inbox_registry,
            extension: None,
            transfer_tx,
            spawn_tx,
            admin_tx,
            worker_stats,
            stats_hook: None,
            process_output_observer: OnceLock::new(),
            worker: RefCell::new(worker),
            created_at: Instant::now(),
            #[cfg(feature = "transport")]
            remote_sink: None,
        };

        #[cfg(feature = "tracing")]
        tracing::info!(
            max_actors = rt.config.max_actors,
            "runtime.created"
        );

        rt
    }

    /// Spawn an actor, returns its address
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        self.address_map.insert(addr);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.spawn_tx.send(SpawnRequest {
            addr,
            actor: boxed,
            parent: None,
            env: Environment::new(),
        });

        #[cfg(feature = "tracing")]
        tracing::info!(
            actor_addr = %addr,
            "actor.spawned"
        );

        Ok(addr)
    }

    /// Spawn an actor with a pre-built environment, returns its address.
    pub fn spawn_with_env<A: ActorInterface>(
        &self,
        actor: A,
        env: Environment,
    ) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        self.address_map.insert(addr);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.spawn_tx.send(SpawnRequest {
            addr,
            actor: boxed,
            parent: None,
            env,
        });

        #[cfg(feature = "tracing")]
        tracing::info!(
            actor_addr = %addr,
            "actor.spawned"
        );

        Ok(addr)
    }

    /// Install a runtime extension. Extensions provide higher-level features
    /// (naming, monitoring, groups) via lifecycle hooks.
    ///
    /// Must be called before `tick()`.
    pub fn with_extension(mut self, ext: Arc<dyn RuntimeExtension>) -> Self {
        if let Some(wext) = ext.create_worker_extension() {
            self.worker.get_mut().worker_ext = Some(wext);
        }
        self.extension = Some(ext);
        self
    }

    /// Access the installed runtime extension (if any).
    pub fn extension(&self) -> Option<&dyn RuntimeExtension> {
        self.extension.as_deref()
    }

    pub fn admin(&self) -> RuntimeAdmin<'_> {
        RuntimeAdmin { runtime: self }
    }

    /// Send a request and get a handle for the response.
    ///
    /// Creates a temporary inbox, calls `msg_builder` with the inbox's address
    /// (so you can embed it as `reply_to`), sends the message, and returns an
    /// [`Ask`] handle for receiving the response.
    pub fn ask<Req: Message, Resp: Message>(
        &self,
        addr: ActorAddress,
        msg_builder: impl FnOnce(ActorAddress) -> Req,
    ) -> Result<Ask<Resp>, Error> {
        let inbox = self.new_inbox::<Resp>()?;
        let msg = msg_builder(*inbox.addr());
        self.send_to(addr, msg)?;
        Ok(Ask { inbox })
    }

    /// Send a message to an actor address.
    ///
    /// Returns `Ok(())` if the message was accepted for routing. This does **not**
    /// guarantee delivery — the recipient may stop before processing it. If
    /// delivery confirmation is needed, implement an application-level ACK.
    ///
    /// Returns `Err` if the address is unknown to the runtime.
    pub fn send_to<M: Message>(&self, addr: ActorAddress, msg: M) -> Result<(), Error> {
        let result = self.send_any(addr, Box::new(msg));

        #[cfg(feature = "tracing")]
        tracing::trace!(dest = %addr, "message.sent");

        result
    }

    /// Create an external inbox for receiving messages in the outer process containing the runtime
    pub fn new_inbox<M: Message>(&self) -> Result<Inbox<M>, Error> {
        let addr = ActorAddress::new_random();
        let receiver = Receiver::<M>::new(self.config.channel_buffer_size);
        let sender = receiver.new_sender();
        self.inbox_registry.register(addr, Arc::new(sender));
        Ok(Inbox {
            addr,
            inner: receiver,
        })
    }

    /// Create an [`ExternalSender`] handle for injecting messages from any thread.
    ///
    /// The returned handle is `Clone + Send + Sync` and can be moved into
    /// background I/O threads to bridge external events into the actor system.
    pub fn create_sender(&self) -> ExternalSender {
        ExternalSender {
            address_map: self.address_map.clone(),
            transfer_tx: self.transfer_tx.clone(),
        }
    }

    fn make_tick_context(&self) -> TickContext<'_> {
        TickContext {
            address_map: &self.address_map,
            spawn_tx: &self.spawn_tx,
            transfer_tx: &self.transfer_tx,
            inbox_registry: &self.inbox_registry,
            config: &self.config,
            extension: self.extension.as_deref(),
            process_output_observer: self.process_output_observer.get(),
            stats_hook: self.stats_hook.as_deref(),
            worker_stats: &self.worker_stats,
            created_at: self.created_at,
            #[cfg(feature = "transport")]
            remote_sink: self.remote_sink.as_deref(),
        }
    }

    /// Return whether the runtime currently has schedulable work.
    pub fn has_work(&self) -> bool {
        self.worker.borrow().has_work()
    }

    /// Try to drive one tick of the runtime.
    ///
    /// Returns `false` if no work was performed.
    /// Returns `true` if at least one actor was processed.
    pub fn try_tick(&self) -> bool {
        let tc = self.make_tick_context();
        self.worker.borrow_mut().tick_once(&tc)
    }

    /// Drive one tick of the runtime.
    pub fn tick(&self) {
        let _ = self.try_tick();
    }

    /// Returns a snapshot of runtime stats: actor placements and per-worker info.
    pub fn stats(&self) -> RuntimeStats {
        let workers = vec![self.worker_stats.snapshot(0)];

        let actors = self
            .address_map
            .addresses()
            .into_iter()
            .map(|addr| (addr, 0))
            .collect();

        let tick_timings = vec![self.worker_stats.drain_tick_timings()];

        let uptime_ms = self.created_at.elapsed().as_millis() as u64;

        RuntimeStats {
            num_workers: 1,
            uptime_ms,
            actors,
            workers,
            actor_details: Vec::new(),
            tick_timings,
        }
    }

    /// Request an actor to stop gracefully.
    ///
    /// The actor's `on_stop()` hook is called before removal. Pending messages
    /// in the mailbox are discarded. The stop takes effect on the next tick.
    ///
    /// Returns `Err` if the actor address is not found in the runtime.
    pub fn stop_actor(&self, addr: ActorAddress) -> Result<(), Error> {
        if self.address_map.contains(&addr) {
            self.transfer_tx
                .send(Envelope::new(addr, Box::new(StopSignal)));
            Ok(())
        } else {
            Err(Error::from("Actor not found"))
        }
    }

    /// Set a stats hook to receive per-actor snapshots from workers.
    ///
    /// Must be called before [`tick()`](Self::tick).
    pub fn set_stats_hook(&mut self, hook: Arc<dyn StatsHook>) {
        self.stats_hook = Some(hook);
    }

    /// Set the sink for non-local (remote) message delivery.
    ///
    /// The sink owns all codec/transport concerns; core only knows how to hand
    /// it a type-erased message destined for a non-local address.
    #[cfg(feature = "transport")]
    pub fn set_remote_sink(&mut self, sink: Arc<dyn RemoteSink>) {
        self.remote_sink = Some(sink);
    }

    /// Deliver a raw deserialized message into the runtime.
    ///
    /// Whoever owns the socket decodes the wire bytes outside core and calls
    /// this to inject the resulting message for a local actor or inbox.
    #[cfg(feature = "transport")]
    pub fn deliver_raw(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        if self.address_map.contains(&addr) {
            self.transfer_tx.send(Envelope::new(addr, msg));
            Ok(())
        } else {
            self.inbox_registry.try_deliver(addr, msg)
        }
    }
}

impl RuntimeAdmin<'_> {
    fn new_admin<T: Message>(&self) -> Result<(Admin<T>, ActorAddress), Error> {
        let inbox = self.runtime.new_inbox::<AdminResult<T>>()?;
        let reply_to = *inbox.addr();
        Ok((Admin::new(inbox), reply_to))
    }

    fn ready<T: Message>(&self, result: AdminResult<T>) -> Result<Admin<T>, Error> {
        let (admin, reply_to) = self.new_admin::<T>()?;
        let _ = self
            .runtime
            .inbox_registry
            .try_deliver(reply_to, Box::new(result));
        Ok(admin)
    }

    pub fn list_actors(&self) -> Result<Admin<ListActorsResponse>, Error> {
        let (admin, reply_to) = self.new_admin::<ListActorsResponse>()?;
        let acc = Arc::new(ListActorsAccumulator {
            remaining: AtomicUsize::new(1),
            summaries: parking_lot::Mutex::new(Vec::new()),
            reply_to,
        });

        self.runtime
            .admin_tx
            .send(AdminCommand::ListActors { acc: acc.clone() });

        Ok(admin)
    }

    pub fn inspect_actor(&self, actor: ActorAddress) -> Result<Admin<InspectActorResponse>, Error> {
        if !self.runtime.address_map.contains(&actor) {
            return self.ready::<InspectActorResponse>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<InspectActorResponse>()?;
        self.runtime
            .admin_tx
            .send(AdminCommand::InspectActor { actor, reply_to });
        Ok(admin)
    }

    pub fn get_actor_state<A>(
        &self,
        actor: ActorAddress,
    ) -> Result<Admin<GetActorStateResponse<A>>, Error>
    where
        A: ActorInterface + Clone + Sync,
    {
        if !self.runtime.address_map.contains(&actor) {
            return self
                .ready::<GetActorStateResponse<A>>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<GetActorStateResponse<A>>()?;

        let get = Box::new(
            |actor: ActorAddress,
             erased: &dyn AnyActor,
             metadata: ActorTypeMetadata|
             -> Box<dyn Any + Send> {
                let expected_actor_type = std::any::type_name::<A>();
                let expected_message_type = std::any::type_name::<A::Incoming>();
                if metadata.actor_type_id != TypeId::of::<A>()
                    || metadata.message_type_id != TypeId::of::<A::Incoming>()
                {
                    return Box::new(Err::<GetActorStateResponse<A>, AdminError>(
                        AdminError::TypeMismatch {
                            expected_actor_type,
                            expected_message_type,
                            actual_actor_type: metadata.actor_type_name,
                            actual_message_type: metadata.message_type_name,
                        },
                    ));
                }

                let Some(typed) = erased.as_any().downcast_ref::<Actor<A>>() else {
                    return Box::new(Err::<GetActorStateResponse<A>, AdminError>(
                        AdminError::TypeMismatch {
                            expected_actor_type,
                            expected_message_type,
                            actual_actor_type: metadata.actor_type_name,
                            actual_message_type: metadata.message_type_name,
                        },
                    ));
                };

                Box::new(Ok::<GetActorStateResponse<A>, AdminError>(
                    GetActorStateResponse {
                        state: ActorStateSnapshot {
                            actor,
                            actor_type: metadata.actor_type_name,
                            message_type: metadata.message_type_name,
                            actor_instance: typed.inner().clone(),
                        },
                    },
                ))
            },
        );
        let not_found = Box::new(|actor| {
            Box::new(Err::<GetActorStateResponse<A>, AdminError>(
                AdminError::ActorNotFound { actor },
            )) as Box<dyn Any + Send>
        });

        self.runtime.admin_tx.send(AdminCommand::GetActorState {
            actor,
            reply_to,
            get,
            not_found,
        });
        Ok(admin)
    }

    pub fn replace_actor_state<A>(
        &self,
        actor: ActorAddress,
        state: ActorStateSnapshot<A>,
    ) -> Result<Admin<OperationResult>, Error>
    where
        A: ActorInterface,
    {
        if state.actor != actor {
            return self.ready::<OperationResult>(Err(AdminError::AddressMismatch {
                requested: actor,
                snapshot: state.actor,
            }));
        }

        if !self.runtime.address_map.contains(&actor) {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;

        let actor_instance = state.actor_instance;
        let replace = Box::new(
            move |erased: &mut dyn AnyActor,
                  metadata: ActorTypeMetadata|
                  -> AdminResult<OperationResult> {
                let expected_actor_type = std::any::type_name::<A>();
                let expected_message_type = std::any::type_name::<A::Incoming>();
                if metadata.actor_type_id != TypeId::of::<A>()
                    || metadata.message_type_id != TypeId::of::<A::Incoming>()
                {
                    return Err(AdminError::TypeMismatch {
                        expected_actor_type,
                        expected_message_type,
                        actual_actor_type: metadata.actor_type_name,
                        actual_message_type: metadata.message_type_name,
                    });
                }

                let Some(typed) = erased.as_any_mut().downcast_mut::<Actor<A>>() else {
                    return Err(AdminError::TypeMismatch {
                        expected_actor_type,
                        expected_message_type,
                        actual_actor_type: metadata.actor_type_name,
                        actual_message_type: metadata.message_type_name,
                    });
                };

                typed.replace_inner(actor_instance);
                Ok(OperationResult { applied: true })
            },
        );

        self.runtime
            .admin_tx
            .send(AdminCommand::ReplaceActorState {
                actor,
                reply_to,
                replace,
            });
        Ok(admin)
    }

    pub fn stop_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        if !self.runtime.address_map.contains(&actor) {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        self.runtime
            .admin_tx
            .send(AdminCommand::StopActor { actor, reply_to });
        Ok(admin)
    }

    pub fn suspend_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        if !self.runtime.address_map.contains(&actor) {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        self.runtime
            .admin_tx
            .send(AdminCommand::SuspendActor { actor, reply_to });
        Ok(admin)
    }

    pub fn resume_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        if !self.runtime.address_map.contains(&actor) {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        }
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        self.runtime
            .admin_tx
            .send(AdminCommand::ResumeActor { actor, reply_to });
        Ok(admin)
    }
}

#[allow(private_interfaces)]
impl ContextInner for Runtime {
    fn send_any(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        if self.address_map.contains(&addr) {
            self.transfer_tx.send(Envelope::new(addr, msg));
            Ok(())
        } else {
            self.make_tick_context().route_nonlocal(addr, msg)
        }
    }

    fn spawn_any(&self, request: SpawnRequest) {
        self.address_map.insert(request.addr);
        self.spawn_tx.send(request);
    }

    fn request_stop(&self, addr: ActorAddress) {
        if self.address_map.contains(&addr) {
            self.transfer_tx
                .send(Envelope::new(addr, Box::new(StopSignal)));
        }
    }

    fn request_stop_with(&self, addr: ActorAddress, value: ExitValue) {
        if self.address_map.contains(&addr) {
            self.transfer_tx
                .send(Envelope::new(addr, Box::new(StopWithSignal(value))));
        }
    }

    fn request_suspend(&self, addr: ActorAddress) {
        // From spawn context (outside worker), not supported (suspend is per-actor, from handler)
        eprintln!("swactor: request_suspend called outside worker context for {addr} — ignored");
    }

    fn request_resume(&self, addr: ActorAddress) {
        if self.address_map.contains(&addr) {
            self.transfer_tx
                .send(Envelope::new(addr, Box::new(ResumeSignal)));
        }
    }

    fn post_worker_request(&self, _request: Box<dyn Any + Send>) {
        // Worker requests (e.g., timers) are per-worker; posting from outside
        // a worker context (e.g., rt.spawn() callback) is not supported.
        eprintln!("swactor: post_worker_request called outside worker context — ignored");
    }

    fn extension(&self) -> Option<&dyn RuntimeExtension> {
        self.extension.as_deref()
    }

    fn process_output_observer(
        &self,
    ) -> Option<Arc<dyn crate::process_observer::ProcessOutputObserver>> {
        self.process_output_observer.get().cloned()
    }

    fn system_info(&self) -> SystemInfo {
        SystemInfo {
            worker_id: 0,
            num_workers: 1,
            total_actors: self.worker_stats.num_actors.load(Ordering::Relaxed),
            uptime_ms: self.created_at.elapsed().as_millis() as u64,
        }
    }
}
