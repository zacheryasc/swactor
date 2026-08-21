use crate::Instant;
use std::any::{Any, TypeId};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};

use crate::actor::{
    Actor, ActorAddress, ActorInterface, ActorTypeMetadata, AnyActor, Environment, Message,
    SpawnRequest, StopSignal,
};
use crate::admin::{
    ActorStateSnapshot, Admin, AdminCommand, AdminError, AdminResult, GetActorStateResponse,
    InspectActorResponse, ListActorsAccumulator, ListActorsResponse, OperationResult, RuntimeAdmin,
};
use crate::channel::{AsyncReceiver, Receiver, Sender};
// Re-export config types so existing code using `runtime::RuntimeConfig` still works
pub use crate::config::RuntimeConfig;
use crate::delivery::{AddressMap, Envelope, InboxRegistry, WorkerId};
use crate::extension::RuntimeExtension;
use crate::stats::{RuntimeStats, StatsHook, WorkerInfo, WorkerStats};
// Re-export stats types so existing code using `runtime::*` still works
use crate::Error;
use crate::worker::Worker;

/// Generic message inbox for receiving messages outside of the runtime.
pub struct Inbox<M: Message> {
    addr: ActorAddress,
    inner: AsyncReceiver<M>,
    runtime: Weak<RuntimeShared>,
}

impl<M: Message> Inbox<M> {
    pub fn addr(&self) -> &ActorAddress {
        &self.addr
    }

    pub fn try_recv(&self) -> Option<M> {
        self.inner.try_recv()
    }

    /// Wait asynchronously for the next message delivered to this external
    /// inbox. The runtime must be driven independently.
    pub async fn recv(&self) -> M {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<M> {
        self.inner.poll_recv(cx)
    }

    /// Poll up to `max_ticks` times, driving `host` once per attempt, returning
    /// the first received message or `None` on timeout.
    pub fn recv_ticking(&self, host: &mut SingleThreadRuntime, max_ticks: usize) -> Option<M> {
        for _ in 0..max_ticks {
            host.try_tick();
            if let Some(msg) = self.inner.try_recv() {
                return Some(msg);
            }
        }
        None
    }
}

impl<M: Message> Drop for Inbox<M> {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.inbox_registry.unregister(&self.addr);
        }
    }
}

/// Pending ask response — an awaitable external inbox.
///
/// Created by [`Runtime::ask`]. The runtime must be driven independently while
/// this future is pending.
pub struct Ask<R: Message> {
    inbox: Inbox<R>,
}

impl<R: Message> Ask<R> {
    pub fn try_recv(&self) -> Option<R> {
        self.inbox.try_recv()
    }

    pub fn recv_ticking(&self, host: &mut SingleThreadRuntime, max_ticks: usize) -> Option<R> {
        self.inbox.recv_ticking(host, max_ticks)
    }
}

impl<R: Message> Future for Ask<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inbox.poll_recv(cx)
    }
}

// Re-export Ctx for backwards compatibility
pub use crate::actor::Ctx;

// ─── RuntimeShared ──────────────────────────────────────────────────────────

/// Runtime-wide shared state, owned via `Arc` by the [`Runtime`] handle and
/// every [`Worker`]. Contains only `Sync` data: routing tables, per-worker
/// producer handles, atomics, and the shared extension/observer hooks.
///
/// `Runtime` holds no worker state and needs no `unsafe Sync` justification.
pub(crate) struct RuntimeShared {
    pub(crate) config: RuntimeConfig,
    pub(crate) address_map: AddressMap,
    pub(crate) inbox_registry: InboxRegistry,
    pub(crate) extension: OnceLock<Arc<dyn RuntimeExtension>>,
    /// Per-worker transfer/spawn/admin producers, indexed by `WorkerId`.
    pub(crate) transfer_txs: Vec<Sender<Envelope>>,
    pub(crate) spawn_txs: Vec<Sender<SpawnRequest>>,
    pub(crate) admin_txs: Vec<Sender<AdminCommand>>,
    pub(crate) worker_stats: Vec<Arc<WorkerStats>>,
    /// Round-robin cursor for runtime-handle spawns.
    pub(crate) rr_worker: AtomicUsize,
    pub(crate) stats_hook: OnceLock<Arc<dyn StatsHook>>,
    pub(crate) process_output_observer:
        OnceLock<Arc<dyn crate::process_observer::ProcessOutputObserver>>,
    pub(crate) created_at: Instant,
    #[cfg(feature = "transport")]
    pub(crate) remote_sink: OnceLock<Arc<dyn RemoteSink>>,
}

impl RuntimeShared {
    /// Number of logical workers in this runtime.
    pub(crate) fn worker_count(&self) -> usize {
        self.worker_stats.len()
    }

    /// Pick the next worker for a runtime-handle spawn (round-robin).
    pub(crate) fn next_worker(&self) -> WorkerId {
        let n = self.worker_count();
        // fetch_add grows monotonically; wrap with modular arithmetic. `n >= 1`
        // is guaranteed at construction, so this never divides by zero.
        let idx = self.rr_worker.fetch_add(1, Ordering::Relaxed);
        WorkerId(idx % n)
    }

    /// Route a message whose destination is not a local actor address:
    /// process-local inbox registry first, then the remote transport seam.
    pub(crate) fn route_nonlocal(
        &self,
        addr: ActorAddress,
        msg: Box<dyn Any + Send>,
    ) -> Result<(), Error> {
        #[cfg(feature = "transport")]
        {
            if self.inbox_registry.contains(&addr) {
                return self.inbox_registry.try_deliver(addr, msg);
            }
            if let Some(sink) = self.remote_sink.get() {
                return sink.send(addr, msg);
            }
        }
        self.inbox_registry.try_deliver(addr, msg)
    }
}

// ─── Runtime ─────────────────────────────────────────────────────────────────

/// The cloneable shared handle for a swactor runtime.
///
/// `Runtime` routes and spawns; it owns no executable worker state and has no
/// `tick()` / `try_tick()`. Worker progression is owned by exactly one
/// execution host ([`SingleThreadRuntime`] or an engine that consumes
/// [`RuntimeParts`]).
///
/// Core is a transition-only state machine. Each call mutates shared routing
/// state and returns immediately, holding no control flow between calls.
#[derive(Clone)]
pub struct Runtime {
    pub(crate) shared: Arc<RuntimeShared>,
}

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
/// background I/O (e.g., pipe readers, network listeners) with the actor system.
pub struct ExternalSender {
    shared: Arc<RuntimeShared>,
}

impl Clone for ExternalSender {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
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
        if let Some(w) = self.shared.address_map.worker_of(&addr) {
            self.shared.transfer_txs[w.index()].send(Envelope::new(addr, Box::new(msg)));
            Ok(())
        } else {
            Err(Error::from("Address not found"))
        }
    }
}

// ─── RuntimeParts ────────────────────────────────────────────────────────────

/// The linear construction bundle: one [`Runtime`] handle and the [`Worker`]
/// values it routes to. Consumed by exactly one execution host.
///
/// Configuration that creates per-worker state — especially
/// [`RuntimeExtension::create_worker_extension`] — must be installed (via
/// [`RuntimeParts::with_extension`]) before the parts are consumed by a host.
pub struct RuntimeParts {
    runtime: Runtime,
    workers: Vec<Worker>,
}

impl RuntimeParts {
    /// Build a runtime with `config.worker_count` logical workers, each with its
    /// own transfer/spawn/admin queues and `WorkerStats`.
    ///
    /// Panics if `config.worker_count == 0`.
    pub fn new(config: RuntimeConfig) -> Self {
        assert!(
            config.worker_count >= 1,
            "swactor: RuntimeConfig.worker_count must be >= 1"
        );
        let n = config.worker_count;
        let max_actors = config.max_actors;
        let channel_buffer_size = config.channel_buffer_size;

        let mut transfer_rxs = Vec::with_capacity(n);
        let mut transfer_txs = Vec::with_capacity(n);
        let mut spawn_rxs = Vec::with_capacity(n);
        let mut spawn_txs = Vec::with_capacity(n);
        let mut admin_rxs = Vec::with_capacity(n);
        let mut admin_txs = Vec::with_capacity(n);
        let mut worker_stats = Vec::with_capacity(n);
        for _ in 0..n {
            let transfer_rx = Receiver::<Envelope>::new(channel_buffer_size);
            transfer_txs.push(transfer_rx.new_sender());
            transfer_rxs.push(transfer_rx);

            let spawn_rx = Receiver::<SpawnRequest>::new(max_actors);
            spawn_txs.push(spawn_rx.new_sender());
            spawn_rxs.push(spawn_rx);

            let admin_rx = Receiver::<AdminCommand>::new(channel_buffer_size);
            admin_txs.push(admin_rx.new_sender());
            admin_rxs.push(admin_rx);

            worker_stats.push(Arc::new(WorkerStats::new()));
        }

        let shared = Arc::new(RuntimeShared {
            config,
            address_map: AddressMap::with_capacity(max_actors),
            inbox_registry: InboxRegistry::new(),
            extension: OnceLock::new(),
            transfer_txs,
            spawn_txs,
            admin_txs,
            worker_stats,
            rr_worker: AtomicUsize::new(0),
            stats_hook: OnceLock::new(),
            process_output_observer: OnceLock::new(),
            created_at: Instant::now(),
            #[cfg(feature = "transport")]
            remote_sink: OnceLock::new(),
        });

        #[cfg(feature = "tracing")]
        tracing::info!(max_actors, worker_count = n, "runtime.created");

        let workers: Vec<Worker> = transfer_rxs
            .into_iter()
            .zip(spawn_rxs)
            .zip(admin_rxs)
            .enumerate()
            .map(|(i, ((trx, srx), arx))| {
                Worker::new(
                    WorkerId(i),
                    shared.clone(),
                    trx,
                    srx,
                    arx,
                    shared.worker_stats[i].clone(),
                )
            })
            .collect();

        Self {
            runtime: Runtime { shared },
            workers,
        }
    }

    /// Borrow the cloneable runtime handle.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Install a runtime extension and create one `WorkerExtension` per worker.
    ///
    /// Must be called before the parts are consumed by a host.
    pub fn with_extension(mut self, ext: Arc<dyn RuntimeExtension>) -> Self {
        for w in &mut self.workers {
            if let Some(wext) = ext.create_worker_extension() {
                w.worker_ext = Some(wext);
            }
        }
        let _ = self.runtime.shared.extension.set(ext);
        self
    }

    /// Install a stats hook across all workers. Must be called before the parts
    /// are consumed by a host.
    pub fn with_stats_hook(self, hook: Arc<dyn StatsHook>) -> Self {
        let _ = self.runtime.shared.stats_hook.set(hook);
        self
    }

    /// Consume the parts, returning the workers for an execution host to own.
    ///
    /// The runtime handle must be cloned beforehand via [`runtime`](Self::runtime).
    pub fn into_workers(self) -> Vec<Worker> {
        self.workers
    }
}

impl Runtime {
    /// Spawn an actor, returns its address.
    ///
    /// Runtime-handle spawns are assigned round-robin across workers.
    pub fn spawn<A: ActorInterface>(&self, actor: A) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let worker = self.shared.next_worker();
        self.shared.address_map.insert(addr, worker);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.shared.spawn_txs[worker.index()].send(SpawnRequest {
            addr,
            actor: boxed,
            parent: None,
            env: Environment::new(),
        });

        #[cfg(feature = "tracing")]
        tracing::info!(actor_addr = %addr, "actor.spawned");

        Ok(addr)
    }

    /// Spawn an actor with a pre-built environment, returns its address.
    pub fn spawn_with_env<A: ActorInterface>(
        &self,
        actor: A,
        env: Environment,
    ) -> Result<ActorAddress, Error> {
        let addr = ActorAddress::new_random();
        let worker = self.shared.next_worker();
        self.shared.address_map.insert(addr, worker);
        let boxed: Box<dyn AnyActor> = Box::new(Actor::new(actor));
        self.shared.spawn_txs[worker.index()].send(SpawnRequest {
            addr,
            actor: boxed,
            parent: None,
            env,
        });

        #[cfg(feature = "tracing")]
        tracing::info!(actor_addr = %addr, "actor.spawned");

        Ok(addr)
    }

    /// Access the installed runtime extension (if any).
    pub fn extension(&self) -> Option<&dyn RuntimeExtension> {
        self.shared.extension.get().map(|a| a.as_ref())
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
        let boxed: Box<dyn Any + Send> = Box::new(msg);
        let result = match self.shared.address_map.worker_of(&addr) {
            Some(w) => {
                self.shared.transfer_txs[w.index()].send(Envelope::new(addr, boxed));
                Ok(())
            }
            None => self.shared.route_nonlocal(addr, boxed),
        };

        #[cfg(feature = "tracing")]
        tracing::trace!(dest = %addr, "message.sent");

        result
    }

    /// Create an external inbox for receiving messages in the outer process containing the runtime
    pub fn new_inbox<M: Message>(&self) -> Result<Inbox<M>, Error> {
        let addr = ActorAddress::new_random();
        let receiver = AsyncReceiver::<M>::new(self.shared.config.channel_buffer_size);
        let sender = receiver.new_sender();
        self.shared.inbox_registry.register(addr, Arc::new(sender));
        Ok(Inbox {
            addr,
            inner: receiver,
            runtime: Arc::downgrade(&self.shared),
        })
    }

    /// Create an [`ExternalSender`] handle for injecting messages from any thread.
    ///
    /// The returned handle is `Clone + Send + Sync` and can be moved into
    /// background I/O threads to bridge external events into the actor system.
    pub fn create_sender(&self) -> ExternalSender {
        ExternalSender {
            shared: self.shared.clone(),
        }
    }

    /// Returns a snapshot of runtime stats: actor placements and per-worker info.
    pub fn stats(&self) -> RuntimeStats {
        let s = &self.shared;
        let num_workers = s.worker_count();
        let workers: Vec<WorkerInfo> = s
            .worker_stats
            .iter()
            .enumerate()
            .map(|(i, ws)| ws.snapshot(i))
            .collect();
        let actors = s
            .address_map
            .placements()
            .into_iter()
            .map(|(a, w)| (a, w.index()))
            .collect();
        let tick_timings: Vec<_> = s
            .worker_stats
            .iter()
            .map(|ws| ws.drain_tick_timings())
            .collect();
        let uptime_ms = s.created_at.elapsed().as_millis() as u64;

        RuntimeStats {
            num_workers,
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
    /// in the mailbox are discarded. The stop takes effect on the owning
    /// worker's next pass.
    ///
    /// Returns `Err` if the actor address is not found in the runtime.
    pub fn stop_actor(&self, addr: ActorAddress) -> Result<(), Error> {
        if let Some(w) = self.shared.address_map.worker_of(&addr) {
            self.shared.transfer_txs[w.index()].send(Envelope::new(addr, Box::new(StopSignal)));
            Ok(())
        } else {
            Err(Error::from("Actor not found"))
        }
    }

    /// Set a stats hook to receive per-actor snapshots from workers.
    ///
    /// Must be called before the runtime is driven.
    pub fn set_stats_hook(&self, hook: Arc<dyn StatsHook>) {
        let _ = self.shared.stats_hook.set(hook);
    }

    /// Set the sink for non-local (remote) message delivery.
    ///
    /// The sink owns all codec/transport concerns; core only knows how to hand
    /// it a type-erased message destined for a non-local address.
    #[cfg(feature = "transport")]
    pub fn set_remote_sink(&self, sink: Arc<dyn RemoteSink>) {
        let _ = self.shared.remote_sink.set(sink);
    }

    /// Deliver a raw deserialized message into the runtime.
    ///
    /// Whoever owns the socket decodes the wire bytes outside core and calls
    /// this to inject the resulting message for a local actor or inbox.
    #[cfg(feature = "transport")]
    pub fn deliver_raw(&self, addr: ActorAddress, msg: Box<dyn Any + Send>) -> Result<(), Error> {
        if let Some(w) = self.shared.address_map.worker_of(&addr) {
            self.shared.transfer_txs[w.index()].send(Envelope::new(addr, msg));
            Ok(())
        } else {
            self.shared.inbox_registry.try_deliver(addr, msg)
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
            .shared
            .inbox_registry
            .try_deliver(reply_to, Box::new(result));
        Ok(admin)
    }

    /// Borrow the producer that owns `actor`'s worker, or `None` if unknown.
    fn admin_tx_for(&self, actor: ActorAddress) -> Option<&Sender<AdminCommand>> {
        self.runtime
            .shared
            .address_map
            .worker_of(&actor)
            .map(|w| &self.runtime.shared.admin_txs[w.index()])
    }

    pub fn list_actors(&self) -> Result<Admin<ListActorsResponse>, Error> {
        let (admin, reply_to) = self.new_admin::<ListActorsResponse>()?;
        let n = self.runtime.shared.worker_count();
        let acc = Arc::new(ListActorsAccumulator {
            remaining: AtomicUsize::new(n),
            summaries: parking_lot::Mutex::new(Vec::new()),
            reply_to,
        });

        // Broadcast to every worker; the last to finish aggregates and replies.
        for tx in &self.runtime.shared.admin_txs {
            tx.send(AdminCommand::ListActors { acc: acc.clone() });
        }

        Ok(admin)
    }

    pub fn inspect_actor(&self, actor: ActorAddress) -> Result<Admin<InspectActorResponse>, Error> {
        let Some(tx) = self.admin_tx_for(actor) else {
            return self.ready::<InspectActorResponse>(Err(AdminError::ActorNotFound { actor }));
        };
        let (admin, reply_to) = self.new_admin::<InspectActorResponse>()?;
        tx.send(AdminCommand::InspectActor { actor, reply_to });
        Ok(admin)
    }

    pub fn get_actor_state<A>(
        &self,
        actor: ActorAddress,
    ) -> Result<Admin<GetActorStateResponse<A>>, Error>
    where
        A: ActorInterface + Clone + Sync,
    {
        let Some(tx) = self.admin_tx_for(actor) else {
            return self
                .ready::<GetActorStateResponse<A>>(Err(AdminError::ActorNotFound { actor }));
        };
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

        tx.send(AdminCommand::GetActorState {
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

        let Some(tx) = self.admin_tx_for(actor) else {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        };
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

        tx.send(AdminCommand::ReplaceActorState {
            actor,
            reply_to,
            replace,
        });
        Ok(admin)
    }

    pub fn stop_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        let Some(tx) = self.admin_tx_for(actor) else {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        };
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        tx.send(AdminCommand::StopActor { actor, reply_to });
        Ok(admin)
    }

    pub fn suspend_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        let Some(tx) = self.admin_tx_for(actor) else {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        };
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        tx.send(AdminCommand::SuspendActor { actor, reply_to });
        Ok(admin)
    }

    pub fn resume_actor(&self, actor: ActorAddress) -> Result<Admin<OperationResult>, Error> {
        let Some(tx) = self.admin_tx_for(actor) else {
            return self.ready::<OperationResult>(Err(AdminError::ActorNotFound { actor }));
        };
        let (admin, reply_to) = self.new_admin::<OperationResult>()?;
        tx.send(AdminCommand::ResumeActor { actor, reply_to });
        Ok(admin)
    }
}

// ─── SingleThreadRuntime ────────────────────────────────────────────────────

/// The explicit manual host: consumes [`RuntimeParts`] and sequentially advances
/// every worker once per [`try_tick`](Self::try_tick), without locks.
///
/// External handles retain only a cloned [`Runtime`]; the host owns the workers.
pub struct SingleThreadRuntime {
    runtime: Runtime,
    workers: Vec<Worker>,
}

impl SingleThreadRuntime {
    /// Consume `parts` and own its workers for manual progression.
    pub fn new(parts: RuntimeParts) -> Self {
        Self {
            runtime: parts.runtime,
            workers: parts.workers,
        }
    }

    /// Borrow the cloneable runtime handle.
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Return whether any owned worker currently has schedulable work.
    pub fn has_work(&self) -> bool {
        self.workers.iter().any(|w| w.has_work())
    }

    /// Advance every owned worker exactly once, combining their results.
    ///
    /// Does not short-circuit: later workers are still ticked after an earlier
    /// productive one, so cross-worker backlogs drain on the same call.
    pub fn try_tick(&mut self) -> bool {
        let mut did_work = false;
        for w in &mut self.workers {
            if w.try_tick() {
                did_work = true;
            }
        }
        did_work
    }

    /// Drive one pass of every worker (ignoring whether work was done).
    pub fn tick(&mut self) {
        let _ = self.try_tick();
    }
}
