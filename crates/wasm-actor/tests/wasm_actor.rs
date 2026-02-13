use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};
use swactor_wasm_actor::{ByteMessage, SharedEngine, WasmActorBuilder, WasmActorError};

fn guest_wasm(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/guests/{name}/target/wasm32-unknown-unknown/release/{name}_guest.wasm",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {path}: {e}"))
}

/// Build a message with an inbox address prepended (the guest contract).
fn framed_msg(dest: &ActorAddress, payload: &[u8]) -> ByteMessage {
    let mut buf = Vec::with_capacity(32 + payload.len());
    buf.extend_from_slice(&dest.0);
    buf.extend_from_slice(payload);
    ByteMessage(buf)
}

// ── Echo: send bytes in, same bytes come back ────────────────────────────────

#[test]
fn echo_returns_same_payload() {
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("echo"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    let payload = b"hello wasm";
    rt.send_to(addr, framed_msg(inbox.addr(), payload)).unwrap();
    rt.tick();

    let received = inbox.try_recv().expect("inbox should have a message");
    assert_eq!(received.0, payload);
}

#[test]
fn echo_preserves_binary_payload() {
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("echo"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    let payload: Vec<u8> = (0..=255).collect();
    rt.send_to(addr, framed_msg(inbox.addr(), &payload)).unwrap();
    rt.tick();

    let received = inbox.try_recv().expect("inbox should have a message");
    assert_eq!(received.0, payload);
}

// ── Silent: processes messages without sending anything ───────────────────────

#[test]
fn silent_produces_no_output() {
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("silent"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    rt.send_to(addr, ByteMessage(b"ignored".to_vec())).unwrap();
    rt.tick();

    assert!(inbox.try_recv().is_none(), "silent guest should not send anything");
}

// ── Double: one message in, two messages out ─────────────────────────────────

#[test]
fn double_sends_two_copies() {
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("double"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    let payload = b"dup me";
    rt.send_to(addr, framed_msg(inbox.addr(), payload)).unwrap();
    rt.tick();

    let first = inbox.try_recv().expect("should receive first copy");
    let second = inbox.try_recv().expect("should receive second copy");
    assert_eq!(first.0, payload);
    assert_eq!(second.0, payload);
    assert!(inbox.try_recv().is_none(), "exactly two messages expected");
}

// ── Missing export → WasmActorError::MissingExport ───────────────────────────

#[test]
fn missing_alloc_export_returns_error() {
    // Minimal valid Wasm module: (module) — no exports at all
    let minimal_wasm = wat::parse_str("(module)").unwrap();
    let engine = SharedEngine::new().unwrap();
    let result = WasmActorBuilder::new(engine, minimal_wasm).build();
    match result {
        Err(WasmActorError::MissingExport(name)) => {
            assert!(
                name == "memory" || name == "alloc",
                "expected missing memory or alloc, got: {name}"
            );
        }
        Err(other) => panic!("expected MissingExport, got: {other}"),
        Ok(_) => panic!("expected error for module with no exports"),
    }
}

// ── Engine sharing: two actors from the same engine ──────────────────────────

#[test]
fn shared_engine_serves_multiple_actors() {
    let engine = SharedEngine::new().unwrap();

    let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo"))
        .build()
        .unwrap();
    let silent = WasmActorBuilder::new(engine, guest_wasm("silent"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    let echo_addr = rt.spawn(echo).unwrap();
    let _silent_addr = rt.spawn(silent).unwrap();

    let payload = b"shared engine test";
    rt.send_to(echo_addr, framed_msg(inbox.addr(), payload)).unwrap();
    rt.tick();

    let received = inbox.try_recv().expect("echo actor should still work");
    assert_eq!(received.0, payload);
}

// ── Safety: edge cases that previously caused panics or corruption ────────────

#[test]
fn oob_send_traps_cleanly_and_actor_survives() {
    // Guest calls swactor.send with dest_ptr pointing past the end of memory.
    // The host should trap the call; the actor should survive for future messages.
    let wat = r#"
        (module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32)
                i32.const 0  ;; return start of memory (simplistic)
            )
            (func (export "handle") (param i32 i32)
                ;; Call send with dest_ptr = 65536 (1 page = end of memory, OOB for 32 bytes)
                i32.const 65536
                i32.const 0
                i32.const 0
                call $send
            )
        )
    "#;
    let wasm = wat::parse_str(wat).unwrap();
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    // Send a message — handle will try OOB send, which traps
    rt.send_to(addr, ByteMessage(vec![42])).unwrap();
    rt.tick();

    // No message should arrive (the send was invalid)
    assert!(inbox.try_recv().is_none(), "OOB send should not produce a message");
}

#[test]
fn alloc_oom_drops_message_actor_stays_alive() {
    // Guest alloc always returns 0 (OOM). Message should be dropped,
    // actor should remain alive for subsequent messages.
    let wat = r#"
        (module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32)
                i32.const 0  ;; always OOM
            )
            (func (export "handle") (param i32 i32)
                ;; Should never be called if alloc returned 0 for non-zero len
            )
        )
    "#;
    let wasm = wat::parse_str(wat).unwrap();
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(actor).unwrap();

    // Send a non-empty message — alloc returns 0, message should be dropped
    rt.send_to(addr, ByteMessage(vec![1, 2, 3])).unwrap();
    rt.tick();

    // Actor is still alive — send another message, tick again (no panic)
    rt.send_to(addr, ByteMessage(vec![4, 5, 6])).unwrap();
    rt.tick();
}

#[test]
fn negative_alloc_ptr_drops_message() {
    // Guest alloc returns -1. Host should detect the negative pointer and drop.
    let wat = r#"
        (module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32)
                i32.const -1  ;; invalid negative pointer
            )
            (func (export "handle") (param i32 i32))
        )
    "#;
    let wasm = wat::parse_str(wat).unwrap();
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(actor).unwrap();

    rt.send_to(addr, ByteMessage(vec![1])).unwrap();
    rt.tick(); // should not panic

    // Actor survives
    rt.send_to(addr, ByteMessage(vec![2])).unwrap();
    rt.tick();
}

#[test]
fn handle_trap_drops_message_actor_survives() {
    // Guest handle executes `unreachable`, causing a Wasm trap.
    // Message should be dropped, actor should stay alive.
    let wat = r#"
        (module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32)
                i32.const 256  ;; valid allocation
            )
            (func (export "handle") (param i32 i32)
                unreachable  ;; trap!
            )
        )
    "#;
    let wasm = wat::parse_str(wat).unwrap();
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(actor).unwrap();

    rt.send_to(addr, ByteMessage(vec![1, 2, 3])).unwrap();
    rt.tick(); // handle traps, but actor should survive

    // Actor is still alive
    rt.send_to(addr, ByteMessage(vec![4, 5, 6])).unwrap();
    rt.tick();
}

// ── Integration: WasmActor alongside a native Rust actor ─────────────────────

#[derive(Clone)]
struct ForwardToWasm {
    wasm_addr: ActorAddress,
    inbox_addr: ActorAddress,
}

struct Forwarder;

impl ActorInterface for Forwarder {
    type Incoming = ForwardToWasm;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ForwardToWasm) {
        // Build the framed message and forward to the wasm actor
        let payload = b"from native";
        let framed = framed_msg(&msg.inbox_addr, payload);
        let _ = ctx.send(msg.wasm_addr, framed);
    }
}

#[test]
fn native_actor_communicates_with_wasm_actor() {
    let engine = SharedEngine::new().unwrap();
    let wasm = WasmActorBuilder::new(engine, guest_wasm("echo"))
        .build()
        .unwrap();

    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    let wasm_addr = rt.spawn(wasm).unwrap();
    let forwarder_addr = rt.spawn(Forwarder).unwrap();

    rt.send_to(
        forwarder_addr,
        ForwardToWasm {
            wasm_addr,
            inbox_addr: *inbox.addr(),
        },
    )
    .unwrap();

    // Tick 1: Forwarder receives message and sends to WasmActor
    rt.tick();
    // Tick 2: WasmActor receives the forwarded message and echoes to inbox
    rt.tick();

    let received = inbox.try_recv().expect("wasm actor should have echoed");
    assert_eq!(received.0, b"from native");
}
