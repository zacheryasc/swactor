use std::sync::Arc;

use wasm_bindgen::prelude::*;

use swactor::actor::{ActorAddress, ActorExited, ActorInterface};
use swactor::runtime::{Ctx, Inbox, Runtime, RuntimeConfig};
use swactor::std::{CtxGroups, CtxWatching, RuntimeNaming, RuntimeGroups, StdExtension};

// ─── Core JS-facing types ───────────────────────────────────────────────────

/// Opaque actor address handle for JavaScript.
///
/// Returned by spawn functions, passed to send functions. JS never sees
/// the raw 32-byte address — it just holds and forwards this handle.
#[wasm_bindgen]
#[derive(Clone)]
pub struct WasmAddr(ActorAddress);

#[wasm_bindgen]
impl WasmAddr {
    /// Debug representation of the address (first 8 hex bytes + ellipsis).
    #[wasm_bindgen(js_name = toString)]
    pub fn to_js_string(&self) -> String {
        format!("{}", self.0)
    }
}

impl WasmAddr {
    /// Access the inner address from Rust (not exposed to JS).
    pub fn inner(&self) -> ActorAddress {
        self.0
    }
}

/// Inbox that receives `u32` values from actors.
#[wasm_bindgen]
pub struct WasmInboxU32 {
    inner: Inbox<u32>,
}

#[wasm_bindgen]
impl WasmInboxU32 {
    /// The address actors should send results to.
    pub fn addr(&self) -> WasmAddr {
        WasmAddr(*self.inner.addr())
    }

    /// Poll for the next value. Returns `undefined` when empty.
    pub fn try_recv(&self) -> Option<u32> {
        self.inner.try_recv()
    }
}

/// Inbox that receives byte arrays from actors.
#[wasm_bindgen]
pub struct WasmInboxBytes {
    inner: Inbox<Vec<u8>>,
}

#[wasm_bindgen]
impl WasmInboxBytes {
    pub fn addr(&self) -> WasmAddr {
        WasmAddr(*self.inner.addr())
    }

    /// Poll for the next byte array. Returns `undefined` when empty.
    pub fn try_recv(&self) -> Option<Vec<u8>> {
        self.inner.try_recv()
    }
}

/// Inbox that receives string values (used for death notifications, etc.).
#[wasm_bindgen]
pub struct WasmInboxString {
    inner: Inbox<String>,
}

#[wasm_bindgen]
impl WasmInboxString {
    pub fn addr(&self) -> WasmAddr {
        WasmAddr(*self.inner.addr())
    }

    /// Poll for the next string. Returns `undefined` when empty.
    pub fn try_recv(&self) -> Option<String> {
        self.inner.try_recv()
    }
}

// ─── Runtime ────────────────────────────────────────────────────────────────

/// The browser-facing swactor runtime.
///
/// Wraps `swactor::Runtime` in single-threaded mode with StdExtension installed
/// (naming, monitoring, groups). Actors are spawned via dedicated spawn functions
/// (one per actor type). The runtime is driven by calling `tick()`.
#[wasm_bindgen]
pub struct WasmRuntime {
    rt: Runtime,
}

#[wasm_bindgen]
impl WasmRuntime {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        let rt = Runtime::new(RuntimeConfig {
            num_threads: 1,
            ..RuntimeConfig::default()
        })
        .with_extension(Arc::new(StdExtension::new()));
        Self { rt }
    }

    /// Drive one tick of the runtime.
    pub fn tick(&self) {
        self.rt.tick();
    }

    /// Number of actors currently alive.
    pub fn actor_count(&self) -> usize {
        self.rt.stats().actors.len()
    }

    /// Create an inbox that receives u32 values.
    pub fn new_inbox_u32(&self) -> WasmInboxU32 {
        WasmInboxU32 {
            inner: self.rt.new_inbox().expect("new_inbox_u32"),
        }
    }

    /// Create an inbox that receives byte arrays.
    pub fn new_inbox_bytes(&self) -> WasmInboxBytes {
        WasmInboxBytes {
            inner: self.rt.new_inbox().expect("new_inbox_bytes"),
        }
    }

    /// Create an inbox that receives strings.
    pub fn new_inbox_string(&self) -> WasmInboxString {
        WasmInboxString {
            inner: self.rt.new_inbox().expect("new_inbox_string"),
        }
    }

    /// Send a u32 to an actor. Returns false if the address is invalid.
    pub fn send_u32(&self, addr: &WasmAddr, value: u32) -> bool {
        self.rt.send_to(addr.0, value).is_ok()
    }

    /// Send a byte array to an actor. Returns false if the address is invalid.
    pub fn send_bytes(&self, addr: &WasmAddr, data: &[u8]) -> bool {
        self.rt.send_to(addr.0, data.to_vec()).is_ok()
    }

    /// Stop an actor gracefully.
    pub fn stop_actor(&self, addr: &WasmAddr) -> bool {
        self.rt.stop_actor(addr.0).is_ok()
    }

    /// Runtime uptime in milliseconds.
    pub fn uptime_ms(&self) -> f64 {
        self.rt.stats().uptime_ms as f64
    }

    // ─── Naming ─────────────────────────────────────────────────────────────

    /// Register a name for an actor address. Returns false if the name is taken.
    pub fn register_name(&self, name: &str, addr: &WasmAddr) -> bool {
        self.rt.register_name(name.to_string(), addr.0).is_ok()
    }

    /// Look up an actor address by name. Returns undefined if not found.
    pub fn where_is(&self, name: &str) -> Option<WasmAddr> {
        self.rt.where_is(name).map(WasmAddr)
    }

    /// Unregister a name. Returns the address it was bound to, or undefined.
    pub fn unregister_name(&self, name: &str) -> Option<WasmAddr> {
        self.rt.unregister(name).map(WasmAddr)
    }

    /// Return all registered actor names as a comma-separated string.
    pub fn registered_names(&self) -> String {
        self.rt.registered_names().join(",")
    }

    // ─── Groups ─────────────────────────────────────────────────────────────

    /// Add an actor to a named group.
    pub fn join_group(&self, addr: &WasmAddr, group: &str) {
        self.rt.join_group(addr.0, group.to_string());
    }

    /// Remove an actor from a named group.
    pub fn leave_group(&self, addr: &WasmAddr, group: &str) {
        self.rt.leave_group(addr.0, group);
    }

    /// Broadcast a u32 message to all members of a group. Returns count sent.
    pub fn publish_to_group_u32(&self, group: &str, msg: u32) -> usize {
        self.rt.publish_to(group, msg)
    }

    /// Number of actors in a group.
    pub fn group_member_count(&self, group: &str) -> usize {
        self.rt.group_members(group).len()
    }

    /// Return all group names as a comma-separated string.
    pub fn group_names(&self) -> String {
        self.rt.groups().join(",")
    }

    // ─── Stats ──────────────────────────────────────────────────────────────

    /// Total messages processed across all workers.
    pub fn total_messages(&self) -> f64 {
        self.rt.stats().workers.iter().map(|w| w.messages_processed).sum::<u64>() as f64
    }

    /// Total panics across all workers.
    pub fn total_panics(&self) -> f64 {
        self.rt.stats().workers.iter().map(|w| w.panics).sum::<u64>() as f64
    }
}

impl WasmRuntime {
    /// Access the inner Runtime from Rust (for custom spawn functions).
    pub fn runtime(&self) -> &Runtime {
        &self.rt
    }
}

// ─── Demo actors ────────────────────────────────────────────────────────────
//
// These demonstrate the pattern for exposing actors to JavaScript.
// Each actor type gets a `spawn_*` function that returns a WasmAddr.

struct Counter {
    total: u32,
    report_to: ActorAddress,
}

impl ActorInterface for Counter {
    type Incoming = u32;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: u32) {
        self.total += msg;
        let _ = ctx.send(self.report_to, self.total);
    }
}

struct Relay {
    target: ActorAddress,
}

impl ActorInterface for Relay {
    type Incoming = u32;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: u32) {
        let _ = ctx.send(self.target, msg);
    }
}

/// A sentinel actor that watches a target and reports its death to an inbox.
///
/// Uses the std monitoring extension (CtxMonitoring::monitor). When the target
/// dies, the sentinel receives a `Down` message and sends the dead actor's
/// string representation to the report inbox, then stops itself.
struct Sentinel {
    target: ActorAddress,
    report_to: ActorAddress,
}

impl ActorInterface for Sentinel {
    type Incoming = ();
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.watch(self.target);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        let msg = format!("{}:{:?}", exited.addr, exited.reason);
        let _ = ctx.send(self.report_to, msg);
        ctx.stop_self();
    }
}

/// A group member that joins a named group and forwards u32 messages to a report inbox.
struct GroupMember {
    group: String,
    report_to: ActorAddress,
}

impl ActorInterface for GroupMember {
    type Incoming = u32;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.join_group(self.group.clone());
    }

    fn handle(&mut self, ctx: &Ctx, msg: u32) {
        let _ = ctx.send(self.report_to, msg);
    }
}

/// Spawn a counter that accumulates u32 values and reports running totals
/// to the given inbox address.
#[wasm_bindgen]
pub fn spawn_counter(rt: &WasmRuntime, report_to: &WasmAddr) -> WasmAddr {
    let addr = rt
        .rt
        .spawn(Counter {
            total: 0,
            report_to: report_to.0,
        })
        .expect("spawn counter");
    WasmAddr(addr)
}

/// Spawn a relay that forwards every u32 message to the target actor.
#[wasm_bindgen]
pub fn spawn_relay(rt: &WasmRuntime, target: &WasmAddr) -> WasmAddr {
    let addr = rt
        .rt
        .spawn(Relay { target: target.0 })
        .expect("spawn relay");
    WasmAddr(addr)
}

/// Spawn a sentinel that watches a target actor and reports its death
/// to the given string inbox.
#[wasm_bindgen]
pub fn spawn_sentinel(rt: &WasmRuntime, target: &WasmAddr, report_to: &WasmInboxString) -> WasmAddr {
    let addr = rt
        .rt
        .spawn(Sentinel {
            target: target.0,
            report_to: *report_to.inner.addr(),
        })
        .expect("spawn sentinel");
    WasmAddr(addr)
}

/// Spawn a group member that joins the given group and forwards u32 messages
/// to the report inbox.
#[wasm_bindgen]
pub fn spawn_group_member(rt: &WasmRuntime, group: &str, report_to: &WasmAddr) -> WasmAddr {
    let addr = rt
        .rt
        .spawn(GroupMember {
            group: group.to_string(),
            report_to: report_to.0,
        })
        .expect("spawn group_member");
    WasmAddr(addr)
}
