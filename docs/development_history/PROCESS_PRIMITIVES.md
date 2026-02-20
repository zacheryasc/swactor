# Process Abstraction for Swactor — Development History

> Design and implementation record for the "process" abstraction layer built
> on top of swactor's actor primitives. This work ran across items 1–9 and
> added 9 extension traits, 3 registries, and ~70 scenario tests.

## Context

Swactor is a distributed actor runtime with local primitives (spawn, send, stop, monitor,
supervise) and distributed primitives (SWIM membership, Kademlia directory, cluster-wide naming,
content-addressed datastore). The goal was to design a "process" abstraction that sits on top of
these primitives, making the experience of running code on a swactor network feel closer to what
an OS process feels like -- with access to an API for requesting resources and querying system
state.

---
## Part 1: OS Process Mapping

### Already strong (direct OS equivalents exist)

OS Concept: PID
Swactor Equivalent: ActorAddress (32-byte random)
Where: src/actor.rs
────────────────────────────────────────
OS Concept: fork+exec
Swactor Equivalent: ctx.spawn(), Runtime::spawn()
Where: src/actor.rs, src/runtime.rs
────────────────────────────────────────
OS Concept: exit(0)
Swactor Equivalent: ctx.stop_self()
Where: src/actor.rs
────────────────────────────────────────
OS Concept: kill(pid, SIGTERM)
Swactor Equivalent: ctx.stop_actor(addr)
Where: src/actor.rs
────────────────────────────────────────
OS Concept: SIGCHLD / waitpid
Swactor Equivalent: ctx.monitor() -> Down, ctx.watch() -> ActorExited
Where: crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: IPC (message queues)
Swactor Equivalent: Typed message passing (local + cross-worker + cross-runtime)
Where: src/actor.rs, src/transport.rs
────────────────────────────────────────
OS Concept: Service names
Swactor Equivalent: NameRegistry (local), ClusterRegistry (cluster CRDT)
Where: crates/std/src/name_registry.rs, crates/distribution/src/registry.rs
────────────────────────────────────────
OS Concept: Process groups
Swactor Equivalent: GroupRegistry (join/leave/publish/members)
Where: crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: init/systemd
Swactor Equivalent: Supervisor with restart strategies
Where: crates/std/src/supervisor.rs
────────────────────────────────────────
OS Concept: Scheduler
Swactor Equivalent: Worker pool with load-aware placement + per-actor message budgets
Where: src/worker.rs, src/delivery.rs
────────────────────────────────────────
OS Concept: Machine identity
Swactor Equivalent: NodeId (ed25519 public key)
Where: crates/distribution/src/types.rs
────────────────────────────────────────
OS Concept: Cluster membership
Swactor Equivalent: SWIM protocol
Where: crates/distribution/src/swim/
────────────────────────────────────────
OS Concept: /proc, top, ps
Swactor Equivalent: RuntimeStats, StatsHook, Dashboard, Investigate protocol
Where: src/stats.rs, crates/dashboard/

### Implemented during this work

OS Concept: System introspection from inside
Swactor Equivalent: CtxSystem (worker_id, num_workers, total_actors, uptime_ms) + SystemInfo
Where: src/actor.rs, crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: Per-actor introspection
Swactor Equivalent: CtxSelfStats (messages_processed, mailbox_depth, message_type_counts)
Where: src/actor.rs, src/worker.rs, crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: Process lineage (getppid)
Swactor Equivalent: CtxLineage (ctx.parent(), ctx.supervisor())
Where: src/actor.rs, src/worker.rs, crates/std/src/ctx_ext.rs, crates/std/src/supervisor_registry.rs
────────────────────────────────────────
OS Concept: Process environment (environ/getenv)
Swactor Equivalent: CtxEnvironment (ctx.env::<T>(), ctx.environment(), SpawnBuilder for overrides)
Where: src/actor.rs, src/worker.rs, crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: Well-known environment keys (spawn metadata)
Swactor Equivalent: SpawnTimestamp(u64) injected by StdExtension on_spawn hook;
  LogicalName(String) injected by spawn_named (ctx and runtime level)
Where: src/actor.rs, src/extension.rs, src/worker.rs, crates/std/src/extension.rs,
  crates/std/src/ctx_ext.rs, crates/std/src/runtime_ext.rs, src/runtime.rs
────────────────────────────────────────
OS Concept: Service discovery
Swactor Equivalent: ServiceRegistry + CtxResources (ctx.resource::<S>() -> Option<ActorAddress>)
Where: src/actor.rs, crates/std/src/service_registry.rs, crates/std/src/ctx_ext.rs,
  crates/std/src/runtime_ext.rs, crates/std/src/extension.rs
────────────────────────────────────────
OS Concept: Resource request API (typed handles)
Swactor Equivalent: ResourceHandle trait + CtxHandles (ctx.handle::<H>() -> Option<H>)
Where: crates/std/src/resource_handle.rs, crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: Exit codes / rich exit values
Swactor Equivalent: ExitValue(Arc<dyn Any + Send + Sync>), ctx.stop_with(value),
  StopReason::Completed, ExitReason::Completed. Exit values propagated via Down/ActorExited.
Where: src/actor.rs, src/worker.rs, crates/std/src/extension.rs, crates/std/src/watch_registry.rs
────────────────────────────────────────
OS Concept: Parent-child hierarchy + orphan handling
Swactor Equivalent: ChildrenRegistry tracks parent->children. On parent death, unsupervised
  children are killed (StopSignal). Supervised children are left to their supervisor. Cascades
  naturally across generations via tick-based cleanup.
Where: crates/std/src/children_registry.rs, crates/std/src/extension.rs
────────────────────────────────────────
OS Concept: Suspend/resume (SIGSTOP/SIGCONT)
Swactor Equivalent: ctx.suspend_self(), ctx.resume(target) with auth (self or supervisor only).
  Suspended actors queue messages but don't process them. ResumeSignal via transfer queue for
  cross-worker resume.
Where: src/actor.rs, src/worker.rs, src/runtime.rs, crates/std/src/ctx_ext.rs
────────────────────────────────────────
OS Concept: Capability model / sandboxing
Swactor Equivalent: CapabilitySet stored in actor's Environment. Enforced at Ctx level (send,
  spawn, stop_actor, monitor, resource). Opt-in: actors without a CapabilitySet are unrestricted.
Where: src/actor.rs, crates/std/src/ctx_ext.rs

### Still partially there

OS Concept: Resource limits
What Exists: Mailbox capacity + message budget
What's Missing: No per-actor memory/CPU/fd limits
────────────────────────────────────────
OS Concept: Auth/permissions
What Exists: Datastore ACL + node-level peer auth + actor-level CapabilitySet
What's Missing: Cluster-level capability propagation (local-only today)

---
## Part 2: Design Primitives

The design followed the existing extension pattern: new capabilities were added as extension traits
on Ctx<'_>, backed by registries in the extension system. This preserved backwards compatibility
and kept the core minimal.

### 2.1 System Queries (CtxSystem)

What it enables: An actor can ask about the system it's running in.

Implemented queries (available via ctx.system_info() or the CtxSystem extension trait):
- ctx.worker_id() -> usize            -- which worker thread am I on?
- ctx.num_workers() -> usize           -- how many worker threads exist?
- ctx.total_actors() -> usize          -- live actors across all workers
- ctx.uptime_ms() -> u64              -- milliseconds since runtime creation

Implementation: SystemInfo struct in src/actor.rs. ContextInner::system_info() implemented on
both Runtime (for spawn-time context) and WorkerContext (for handler context). Data flows through
TickContext (worker_stats + created_at fields in src/delivery.rs). The CtxSystem extension trait
in crates/std/src/ctx_ext.rs provides ergonomic per-field accessors.

Future cluster-level queries (not yet implemented):
- What is my node's identity (NodeId)?
- How many cluster nodes are alive?
- Who are the cluster members?

These require the distribution crate's DistributedNode state to be exposed through the extension
system. The CtxSystem trait can be extended with these when the distribution integration is ready.

### 2.2 Process Environment (CtxEnvironment)

What it enables: Typed configuration that flows from parent to child at spawn time.

Properties:
- Inherited: When actor A spawns actor B via ctx.spawn(), B gets A's environment (Arc clone)
- Overridable: ctx.spawn_builder(actor).env(Key(val)).finish() lazily clones the parent's map
  on first override (copy-on-write), leaving the common case (no overrides) allocation-free
- Immutable after spawn: Set at creation, read-only thereafter. Mutable config goes through
  messages.
- Typed values: TypeId-keyed (like http::Extensions), not string-to-string
- Runtime-spawned actors start with an empty environment

Implementation: Environment is Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>> -- clone is an
Arc bump (zero allocation). EnvironmentBuilder provides from_env() for copy-on-write overrides
(cloning individual entries is cheap since values are also Arc-wrapped). The spawn channel was
replaced with a SpawnRequest struct (addr, actor, parent, env) to avoid further tuple growth.
ActorSlot stores env, and Ctx receives it at both construction sites (tick_all and cleanup_dead).
SpawnBuilder provides the ergonomic override API. The CtxEnvironment extension trait in
crates/std/src/ctx_ext.rs provides the import path, following the same pattern as CtxLineage
(no StdExtension dependency required). Python crate spawns with Environment::new(). 6 scenario
tests in tests/std_extension.rs cover: inheritance, empty for runtime-spawned, grandchild chain,
override-one-inherit-others, readable in on_stop, and sibling independence.

Well-known keys:
- SpawnTimestamp(u64): Injected by StdExtension's on_spawn hook. Milliseconds since runtime
  creation, same time base as SystemInfo::uptime_ms. Opt-in at runtime level (present when
  StdExtension is installed). Read via ctx.env::<SpawnTimestamp>().
- LogicalName(String): Injected by spawn_named() at both ctx and Runtime levels. Inherited by
  children via normal environment inheritance. Read via ctx.env::<LogicalName>().
- ServiceBinding<S>(ActorAddress): Injected by ServiceRegistry's inject_into() hook during
  on_spawn. Registered at runtime level via rt.register_service::<S>(addr). Read via
  ctx.resource::<S>() (CtxResources trait). Overridable per-subtree via spawn_builder.
- CapabilitySet: Granted at spawn time (via environment or spawn_builder). Inherited by children.
  Enforced at Ctx level. See section 2.7.

Analogy: Unix environ -- inherited by default, augmented at fork/exec time, readable via getenv().

### 2.3 Service Discovery (CtxResources)

What it enables: Actors can discover system services by type, not by knowing raw addresses.

How it differs from NameRegistry: NameRegistry maps strings to addresses. CtxResources maps
service marker types to addresses. Looking up "datastore" by name gives you a raw ActorAddress and
you must know what messages it accepts. ctx.resource::<Datastore>() gives you the address of the
service registered under that marker type.

Implementation: Three layers compose the feature:

1. Core type: ServiceBinding<S>(ActorAddress) in src/actor.rs -- a generic environment key
   parameterized by a zero-sized marker type. Any struct satisfying 'static + Send + Sync works
   as a marker (no special Service trait required, consistent with Environment's existing API).

2. Registry + injection: ServiceRegistry in crates/std/src/service_registry.rs stores registered
   bindings as RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>> (same thread-safety pattern
   as SupervisorRegistry). StdExtension's on_spawn hook calls inject_into() before adding
   SpawnTimestamp -- this merges all registered bindings into the actor's environment, skipping
   keys already present (preserves per-subtree overrides set via spawn_builder). Helper methods
   on Environment (contains_type_id) and EnvironmentBuilder (set_raw) support type-erased
   injection without knowing concrete types at compile time.

3. Read API: CtxResources trait in crates/std/src/ctx_ext.rs provides ctx.resource::<S>() ->
   Option<ActorAddress>, a thin wrapper around ctx.env::<ServiceBinding<S>>().map(|b| b.addr).
   Does NOT require StdExtension -- reads from core environment (same pattern as CtxEnvironment).
   When a CapabilitySet is present, resource() checks check_service::<S>() and returns None if
   denied. RuntimeResources trait in crates/std/src/runtime_ext.rs provides
   rt.register_service::<S>(addr) for startup-time registration.

Key design decisions:
- No Service marker trait: S: 'static + Send + Sync is sufficient. Any zero-size struct works.
- "Skip if present" injection: The registry doesn't overwrite env keys set by spawn_builder,
  enabling per-subtree service overrides (e.g., test doubles, staging vs production services).
- No cleanup on service actor death: A dead service's binding stays in the registry (stale
  address). Sends to it will fail. Service lifecycle management is a higher-level concern.

6 scenario tests in tests/std_extension.rs cover: discovery by marker type, child inherits
binding from parent, multiple services each accessible by marker, unregistered returns None,
overridable via spawn_builder, accessible in on_start and on_stop lifecycle hooks.

Well-known services that could be registered (when swactor-node is updated):
- Storage -- content-addressed datastore (currently wired manually in swactor-node)
- Directory -- actor location resolution (currently locked inside DistributedNode)
- Cluster -- membership/topology info (currently snapshot-only for dashboard)
- Metrics -- runtime stats (currently StatsHook push-only)

### 2.4 Resource Handles (CtxHandles)

What it enables: Domain-specific typed proxies that wrap service addresses and provide ergonomic
APIs.

The pattern: A handle wraps (service_address, self_address) and provides methods that construct
and send the right messages, embedding self_addr as reply_to. Responses arrive as normal messages
in the actor's handle().

Implementation: The ResourceHandle trait in crates/std/src/resource_handle.rs defines the contract:
- type Service: 'static + Send + Sync -- the marker type used for service discovery
- from_parts(service_addr, self_addr) -> Self -- construct from addresses
- service_addr() -> ActorAddress -- the underlying service address
- self_addr() -> ActorAddress -- the actor's own address (for reply_to)

The CtxHandles extension trait in crates/std/src/ctx_ext.rs provides ctx.handle::<H>() -> Option<H>,
which looks up ServiceBinding<H::Service> from the actor's environment and constructs the handle.
Returns None if the service is not registered (consistent with ctx.resource(), ctx.where_is()).

Handle methods take &self + &Ctx (not stored &Ctx -- avoids lifetime issues with &mut self in
handlers). Example:
  impl MyHandle {
      pub fn do_work(&self, ctx: &Ctx, data: Vec<u8>) -> Result<(), Error> {
          ctx.send(self.service_addr(), MyMsg::DoWork { data, reply_to: self.self_addr() })
      }
  }

Key design tension: Handles can't block (no await in swactor). The response arrives asynchronously
as a message. This is inherent to the actor model and not something to "fix" -- the handle just
makes the send side ergonomic.

5 scenario tests: handle wraps service and sends ergonomically, returns None when service not
registered, inherits service binding from parent, constructible in on_start, two actors with same
handle type each get responses at their own address.

### 2.5 Process Lineage (CtxLineage)

What it enables: Actors know their ancestry.

Implemented queries:
- ctx.parent() -> Option<ActorAddress> (who spawned me?)
  Returns Some(spawner_addr) for actor-spawned children, None for Runtime::spawn().
  Available in handle(), on_start(), and on_stop().
- ctx.supervisor() -> Option<ActorAddress> (who supervises me, if anyone?)
  Returns Some(supervisor_addr) for supervised children, None for unsupervised actors.
  Gracefully returns None when StdExtension is absent (no panic).

Implementation (parent): The spawn channel uses a SpawnRequest struct (addr, actor, parent, env) --
the original 3-tuple was replaced when CtxEnvironment was added. When Ctx::spawn is called, the
spawning actor's self_addr is passed as Some(parent). Runtime::spawn passes None. The parent is
stored in ActorSlot::parent_addr and threaded into Ctx::self_parent_addr at both construction sites
(tick_all and cleanup_dead). 4 scenario tests cover: child knows parent, runtime-spawned has no
parent, grandchild sees immediate parent (not grandparent), and parent is visible in on_stop.

Implementation (supervisor): SupervisorRegistry in crates/std/src/supervisor_registry.rs stores a
child_addr -> supervisor_addr map (RwLock<AddrMap<ActorAddress>>). Supervisor::start_child calls
register(self_addr, child_addr) after spawning and monitoring. cleanup() removes entries where the
dead address is either child or supervisor (O(n) scan for supervisor death, acceptable since
supervisor death is rare and the map is small). CtxLineage::supervisor() downcasts the extension
gracefully (returns None if StdExtension is absent). 5 scenario tests cover: supervised child knows
supervisor, unsupervised actor returns None, supervisor survives child restart, grandchild not
supervised but parent is, OneForAll restart re-registers all children.

The CtxLineage extension trait in crates/std/src/ctx_ext.rs provides the ergonomic import path.

Orphan handling was implemented as part of item 8 (Lifecycle Enrichment) -- see section 2.8.

### 2.6 Self-Introspection (CtxSelfStats)

What it enables: Actors can see their own operational metrics.

Implemented queries (available directly on Ctx or via the CtxSelfStats extension trait):
- ctx.messages_processed() -> u64     -- total successfully processed before current tick
- ctx.mailbox_depth() -> usize        -- messages queued at start of current tick (pre-dequeue)
- ctx.message_type_counts() -> &[(&str, u64)] -- per-type counts, sorted descending

Implementation: Stats are snapshotted from ActorSlot fields into Ctx before each tick_all
iteration (src/worker.rs). The snapshot captures the state before any messages are dequeued
in the current tick, giving actors a consistent view. The same snapshot is provided during
on_stop callbacks in cleanup_dead. The CtxSelfStats extension trait in crates/std/src/ctx_ext.rs
provides the ergonomic import path.

The Vec allocation for type counts is bounded (max 32 entries from ActorSlot's msg_type_counts
cap) and negligible relative to handle_any cost.

### 2.7 Capability Model (CapabilitySet + CtxCapabilities)

What it enables: Controlled access to system resources and other actors. Primarily important for
sandboxing untrusted code (wasm actors in crates/bin-runner/).

Approach: A single CapabilitySet stored in the actor's Environment. When present, enforcement is
active -- the actor can only perform operations granted by the set. When absent, the actor is
unrestricted (backward compatible). Capabilities inherit from parent to child via normal
environment inheritance.

Capability grants (all in CapabilitySet):
- with_send(addr) -- send any message type to a specific address
- with_send_typed::<M>(addr) -- send only messages of type M to a specific address
- with_spawn() -- permission to spawn new actors
- with_service::<S>() -- permission to access system service S via ctx.resource::<S>()
- with_monitor(addr) -- permission to monitor a specific actor

Enforcement points (all in src/actor.rs Ctx methods or crates/std/src/ctx_ext.rs):
- ctx.send::<M>(addr, msg) -- checks check_send::<M>(addr); self-send always allowed
- ctx.spawn() / SpawnBuilder::finish() -- checks check_spawn()
- ctx.stop_actor(addr) -- checks check_send_addr(addr) (stop is a send of StopSignal)
- ctx.monitor(addr) -- checks check_monitor(addr); returns Result<MonitorRef, Error>
- ctx.resource::<S>() -- checks check_service::<S>(); returns None if denied

Key design decisions:
- Opt-in: No CapabilitySet in environment means unrestricted. Zero behavioral change for existing
  actors. The only cost is an Option check (env.get::<CapabilitySet>()) at each enforcement point.
- Enforcement at Ctx level only: The core ContextInner::send_any is not gated. This means
  extension code (supervisors, timers, etc.) that calls send_any directly bypasses capability
  checks, which is intentional -- system infrastructure is trusted.
- Dual send granularity: with_send(addr) grants all message types to an address.
  with_send_typed::<M>(addr) grants only type M. The check tries address-only first, then typed.
  This allows coarse grants for trusted peers and fine-grained grants for untrusted actors.
- Self-send always allowed: A restricted actor can always send to its own address. This prevents
  capabilities from breaking actors that use self-messaging patterns (timers, state machines).
- monitor() returns Result: Changed from -> MonitorRef to -> Result<MonitorRef, Error>. This was
  a breaking change to all callers (supervisor.rs, router.rs, test files), fixed mechanically by
  adding ? or .unwrap().

Builder API: Fluent (CapabilitySet::new().with_send(addr).with_spawn()) and mutable
(caps.grant_send(addr)) variants. Mutable methods return &mut Self for chaining.

Introspection: CtxCapabilities extension trait in crates/std/src/ctx_ext.rs provides:
- ctx.capabilities() -> Option<&CapabilitySet> -- access the raw set
- ctx.is_restricted() -> bool -- quick check

Implementation locations:
- src/actor.rs: CapabilitySet struct, builder methods, check methods, Ctx::capabilities() helper,
  enforcement in send/spawn/stop_actor/SpawnBuilder::finish
- src/lib.rs: CapabilitySet re-export
- crates/std/src/ctx_ext.rs: CtxCapabilities trait, monitor() enforcement, resource() enforcement
- crates/std/src/lib.rs: CtxCapabilities re-export

11 scenario tests in tests/std_extension.rs cover: unrestricted actor sends freely (backward
compat), restricted actor denied send, restricted actor allowed send, typed send grant (Ping
allowed / Pong denied), spawn denied, spawn allowed, capability inheritance (child inherits
parent's CapabilitySet), monitor denied, service access denied, self-send always allowed, stop
requires send permission.

### 2.8 Lifecycle Enrichment

Rich exit values: ExitValue(Arc<dyn Any + Send + Sync>) is an opaque typed wrapper. Actors stop
with ctx.stop_with(value) which stores the value and triggers StopReason::Completed. The value
is propagated through Down (monitors) and ActorExited (watchers) via the exit_value: Option<ExitValue>
field. Manual PartialEq/Eq on ExitValue (always false -- opaque blob), so Down/ActorExited compare
by addr+reason only.

Implementation: StopWithSignal(ExitValue) is a sentinel message intercepted in tick_all (like
StopSignal). ActorSlot gains exit_value: Option<ExitValue>. cleanup_dead returns
Vec<(ActorAddress, StopReason, Option<ExitValue>)> with StopReason::Completed when exit_value is
present. The on_actor_death extension hook receives and propagates exit values to monitors/watchers.

7 scenario tests: stop_with value received in Down, received in ActorExited, normal stop has None,
panic has None, multiple monitors receive cloned value, stop_with from on_start, supervisor receives
rich exit in handle_down (graceful handoff pattern).

Orphan handling: ChildrenRegistry tracks parent -> set of children. Populated in on_spawn when a
parent is present. On parent death (on_actor_death), unsupervised children receive StopSignal.
Supervised children are left to their supervisor. Cascades naturally: parent dies -> children killed
next tick -> grandchildren killed the tick after that. StopSignal made pub (was pub(crate)) to
enable this -- it's not Message (not Clone) so can't be sent via ctx.send().

4 scenario tests: unsupervised children killed on parent death, supervised children not killed,
cascading cleanup across generations, runtime-spawned actors unaffected.

Suspend/resume: ActorSlot gains a suspended: bool flag. Suspended actors queue messages but don't
process them (tick_all skips them). ctx.suspend_self() sets the flag via a suspend_requests buffer.
ResumeSignal is intercepted in deliver() to clear the flag. StopSignal/StopWithSignal are also
intercepted for suspended actors (so stop_actor works on them). Cross-worker resume sends
ResumeSignal via the transfer queue.

Authorization: CtxLifecycle extension trait provides ctx.suspend_self() (always allowed) and
ctx.resume(target) which checks: target == self (self-resume) OR caller is the target's supervisor
via SupervisorRegistry. Returns Err if unauthorized.

5 scenario tests: suspended actor queues then resume processes, supervisor can resume, non-supervisor
cannot resume, suspended actor can be stopped, cross-worker resume via runtime.

Graceful handoff: Built on rich exit values. An outgoing actor stops with its state via
ctx.stop_with(state); the supervisor receives it in handle_down's Down message and can pass it
to the replacement's constructor. Enables zero-downtime upgrades. No additional mechanism needed --
the pattern composes from existing primitives.

---
## Part 3: How These Compose

The primitives form a layered system:

Layer 3: Integration (swactor-node wires services at startup)
Layer 2: Process (CapabilitySet, ProcessBuilder)
Layer 1: Std (CtxSystem, CtxEnvironment, CtxLineage, CtxSelfStats, Well-known env keys,
         SupervisorRegistry, CtxResources, CtxHandles, CtxLifecycle, ChildrenRegistry,
         CtxCapabilities)
Layer 0: Core (SystemInfo, Ctx self-stats, parent tracking, Environment + SpawnRequest,
         on_spawn hook, spawn_with_env, ServiceBinding, suspend flag, rich exit, orphan
         handling, CapabilitySet)

A "process" in swactor is an actor that has:
1. An identity (ActorAddress) and a name (NameRegistry)
2. A parent and supervisor it can query (CtxLineage)
3. An environment inherited from its spawner, with well-known keys (CtxEnvironment)
4. Access to system services through discovery (CtxResources)
5. The ability to query the system it lives in (CtxSystem)
6. Awareness of its own operational state (CtxSelfStats)
7. Typed resource handles for ergonomic service interaction (CtxHandles)
8. Rich lifecycle support including typed exit values, orphan handling, and suspend/resume
9. Controlled permissions for what it can access (CapabilitySet)

What stayed the same: The core actor model (message passing, mailboxes, workers, tick-based
execution) was unchanged. ActorInterface, Ctx, Runtime remained the foundation. The process
abstraction was additive -- existing actors continued to work exactly as before.

---
## Part 4: Implementation Sequence

Each item was implemented and merged in dependency order. Earlier items established the
infrastructure (Environment, extension hooks) that later items built on.

1. **CtxSystem + CtxSelfStats** -- Exposed existing internal data to actors. SystemInfo struct,
   ContextInner::system_info(), Ctx self-stats snapshot fields. Extension traits CtxSystem and
   CtxSelfStats in swactor-std. Covered by 3 scenario tests.

2. **CtxLineage (parent tracking)** -- Option<ActorAddress> threaded through the spawn path.
   ContextInner::spawn_any gained a parent parameter. ActorSlot stores parent_addr. Ctx exposes
   parent(). CtxLineage extension trait in swactor-std. 4 scenario tests.
   Python crate updated to pass parent on spawn.

3. **CtxEnvironment (process environment)** -- Typed key-value map inherited from parent to
   child at spawn time. Environment is Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>> -- clone
   is an Arc bump. EnvironmentBuilder supports copy-on-write overrides via from_env(). The spawn
   channel 3-tuple was replaced with a SpawnRequest struct (addr, actor, parent, env) to stop
   tuple growth. ActorSlot stores env. Ctx gains env::<T>(), environment(), and spawn_builder().
   SpawnBuilder lazily clones the parent's map on first .env() call. CtxEnvironment extension trait
   in swactor-std (no StdExtension dependency). Python crate spawns with Environment::new().
   6 scenario tests: inheritance, empty for runtime-spawned, grandchild chain,
   override-one-inherit-others, readable in on_stop, sibling independence.

4. **Well-known environment keys** -- SpawnTimestamp(u64) and LogicalName(String) types in
   src/actor.rs, exported from src/lib.rs. SpawnTimestamp is opt-in at runtime level: injected by
   StdExtension's on_spawn hook (new RuntimeExtension::on_spawn hook with default no-op in
   src/extension.rs). Worker::drain_spawns now takes &TickContext and calls on_spawn for each
   spawn request, passing uptime_ms to avoid exposing the pub(crate) Instant type. LogicalName is
   injected by spawn_named at both ctx level (via spawn_builder + env override) and runtime level
   (via new Runtime::spawn_with_env method). LogicalName inherits to children automatically via
   normal environment inheritance. 7 scenario tests.

5. **Supervisor lineage (ctx.supervisor())** -- SupervisorRegistry in
   crates/std/src/supervisor_registry.rs stores child_addr -> supervisor_addr as
   RwLock<AddrMap<ActorAddress>>. Supervisor::start_child calls register() after spawning and
   monitoring. cleanup() removes entries for dead actors (both as child and as supervisor).
   CtxLineage::supervisor() gracefully returns None when StdExtension is absent (downcasts via
   as_any, no panic). Distinct from parent() because not every parent is a supervisor. get_ext
   made pub(crate) so supervisor.rs can access it. 5 scenario tests.

6. **Service Registry + CtxResources** -- Actors discover system services by type
   (ctx.resource::<Datastore>()) rather than by raw address. ServiceBinding<S>(ActorAddress)
   is a generic environment key parameterized by a marker type. ServiceRegistry in StdExtension
   stores bindings and injects them into every actor's environment via on_spawn (skipping keys
   already present to preserve spawn_builder overrides). CtxResources trait provides
   ctx.resource::<S>() sugar. RuntimeResources trait provides rt.register_service::<S>(addr).
   6 scenario tests.

7. **Resource Handles (CtxHandles)** -- ResourceHandle trait + CtxHandles extension trait.
   ctx.handle::<H>() -> Option<H> constructs typed proxies from ServiceBinding<H::Service> in the
   actor's environment. Handle methods take &self + &Ctx for ergonomic domain-specific APIs.
   5 scenario tests.

8. **Lifecycle enrichment** -- Three sub-features:
   a) Rich exit values: ExitValue(Arc<dyn Any + Send + Sync>), ctx.stop_with(value),
      StopReason::Completed, ExitReason::Completed. Propagated through Down/ActorExited.
      7 scenario tests.
   b) Orphan handling: ChildrenRegistry tracks parent->children. Unsupervised children killed
      on parent death. Supervised children left to their supervisor. Natural cascade.
      4 scenario tests.
   c) Suspend/resume: ActorSlot::suspended flag, ctx.suspend_self(), ctx.resume(target) with
      auth (self or supervisor only). ResumeSignal for cross-worker resume.
      5 scenario tests.

9. **Capability model (CapabilitySet)** -- Per-actor permission set stored in the Environment.
   Grants: with_send(addr), with_send_typed::<M>(addr), with_spawn(), with_service::<S>(),
   with_monitor(addr). Enforced at Ctx level in send, spawn, stop_actor, monitor, and resource.
   Opt-in: actors without a CapabilitySet are unrestricted (zero behavioral change). Self-send
   always allowed. monitor() changed from -> MonitorRef to -> Result<MonitorRef, Error> (breaking
   change, fixed mechanically in supervisor.rs, router.rs, and all test files). CtxCapabilities
   extension trait for introspection. 11 scenario tests.
