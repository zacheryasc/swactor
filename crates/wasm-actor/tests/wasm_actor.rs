use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};
use swactor_wasm_actor::{ByteMessage, SharedEngine, WasmActor, WasmActorBuilder, WasmActorError};

use proptest::prelude::*;

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

/// Build a WAT module where alloc returns a constant value.
fn alloc_returns_wat(alloc_val: i32) -> Vec<u8> {
    let wat = format!(
        r#"(module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32) i32.const {alloc_val})
            (func (export "handle") (param i32 i32))
        )"#
    );
    wat::parse_str(&wat).unwrap()
}

/// Build a WAT module where handle calls send with specific arguments.
fn send_args_wat(dest_ptr: i32, payload_ptr: i32, payload_len: i32) -> Vec<u8> {
    let wat = format!(
        r#"(module
            (import "swactor" "send" (func $send (param i32 i32 i32)))
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32) i32.const 256)
            (func (export "handle") (param i32 i32)
                i32.const {dest_ptr}
                i32.const {payload_ptr}
                i32.const {payload_len}
                call $send
            )
        )"#
    );
    wat::parse_str(&wat).unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 1: Builder contract
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn build_succeeds_for_valid_modules() {
    let engine = SharedEngine::new().unwrap();

    // Pre-compiled guests
    WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    WasmActorBuilder::new(engine.clone(), guest_wasm("double")).build().unwrap();
    WasmActorBuilder::new(engine.clone(), guest_wasm("silent")).build().unwrap();

    // Custom WAT with extra exports, data segments, funcref table
    let extras = wat::parse_str(r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (data (i32.const 0) "hello")
        (table 1 funcref)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param i32 i32))
        (func (export "custom_fn") (result i32) i32.const 42)
    )"#).unwrap();
    WasmActorBuilder::new(engine.clone(), extras).build().unwrap();

    // Module without send import (doesn't import swactor.send)
    let no_send = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param i32 i32))
    )"#).unwrap();
    WasmActorBuilder::new(engine, no_send).build().unwrap();
}

#[test]
fn build_rejects_missing_exports() {
    let engine = SharedEngine::new().unwrap();

    // Empty module — missing everything
    let result = WasmActorBuilder::new(engine.clone(), wat::parse_str("(module)").unwrap()).build();
    let err = result.err().expect("should fail");
    let msg = format!("{err}");
    assert!(msg.contains("memory") || msg.contains("alloc"), "got: {msg}");

    // Memory only — missing alloc
    let mem_only = wat::parse_str("(module (memory (export \"memory\") 1))").unwrap();
    let err = WasmActorBuilder::new(engine.clone(), mem_only).build().err().expect("should fail");
    assert!(format!("{err}").contains("alloc"));

    // Memory + alloc — missing handle
    let no_handle = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 0)
    )"#).unwrap();
    let err = WasmActorBuilder::new(engine.clone(), no_handle).build().err().expect("should fail");
    assert!(format!("{err}").contains("handle"));

    // Wrong memory export name
    let wrong_mem = wat::parse_str(r#"(module
        (memory (export "mem") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32))
    )"#).unwrap();
    assert!(WasmActorBuilder::new(engine.clone(), wrong_mem).build().is_err());

    // Wrong alloc signature (two params)
    let wrong_alloc = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32 i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32))
    )"#).unwrap();
    assert!(WasmActorBuilder::new(engine.clone(), wrong_alloc).build().is_err());

    // Wrong handle signature (returns i32)
    let wrong_handle = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32) (result i32) i32.const 0)
    )"#).unwrap();
    assert!(WasmActorBuilder::new(engine, wrong_handle).build().is_err());
}

#[test]
fn build_rejects_invalid_wasm_and_disabled_features() {
    let engine = SharedEngine::new().unwrap();

    // Invalid bytes
    assert!(WasmActorBuilder::new(engine.clone(), vec![0xDE, 0xAD]).build().is_err());

    // Empty bytes
    assert!(WasmActorBuilder::new(engine.clone(), vec![]).build().is_err());

    // Truncated valid wasm
    let valid = guest_wasm("echo");
    let truncated = valid[..valid.len() / 2].to_vec();
    assert!(WasmActorBuilder::new(engine.clone(), truncated).build().is_err());

    // SIMD (disabled in engine config)
    let simd = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32)
            v128.const i32x4 0 0 0 0
            drop
        )
    )"#);
    assert!(simd.is_err() || WasmActorBuilder::new(engine.clone(), simd.unwrap()).build().is_err());

    // Multi-value (disabled)
    let mv = wat::parse_str(r#"(module
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 0)
        (func (export "handle") (param i32 i32)
            (block (result i32 i32)
                i32.const 1
                i32.const 2
            )
            drop drop
        )
    )"#);
    assert!(mv.is_err() || WasmActorBuilder::new(engine, mv.unwrap()).build().is_err());
}

#[test]
fn error_display_and_from_impls() {
    // MissingExport
    let missing = WasmActorError::MissingExport("memory");
    let s = format!("{missing}");
    assert!(s.contains("memory"), "MissingExport display: {s}");

    // Wasmtime error via From
    let wt_err = wasmtime::Error::msg("test error");
    let converted: WasmActorError = wt_err.into();
    let s = format!("{converted}");
    assert!(s.contains("test error"), "Wasmtime display: {s}");

    // Distinct display
    let missing_s = format!("{}", WasmActorError::MissingExport("alloc"));
    let wt_s = format!("{}", WasmActorError::Wasmtime(wasmtime::Error::msg("x")));
    assert_ne!(missing_s, wt_s);

    // Send + Sync
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<WasmActorError>();
}

proptest! {
    #[test]
    fn prop_builder_never_panics(
        pages in 0u32..10,
        alloc_return in prop_oneof![-100i32..0, 0i32..70000, Just(i32::MIN), Just(i32::MAX)],
    ) {
        let wat = format!(
            r#"(module
                (memory (export "memory") {pages})
                (func (export "alloc") (param i32) (result i32) i32.const {alloc_return})
                (func (export "handle") (param i32 i32))
            )"#
        );
        if let Ok(wasm) = wat::parse_str(&wat) {
            let engine = SharedEngine::new().unwrap();
            // Should succeed or return clean error — never panic
            let _ = WasmActorBuilder::new(engine, wasm).build();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 2: Echo round-trip
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn echo_round_trip_story() {
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    // Text payload
    rt.send_to(addr, framed_msg(inbox.addr(), b"hello wasm")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"hello wasm");

    // Binary payload (all 256 byte values)
    let binary: Vec<u8> = (0..=255).collect();
    rt.send_to(addr, framed_msg(inbox.addr(), &binary)).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, binary);

    // Empty message — echo needs >= 32 bytes, so no reply
    rt.send_to(addr, ByteMessage(vec![])).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_none());

    // Address-only message (32 bytes, 0 payload) — echo sends empty payload
    rt.send_to(addr, framed_msg(inbox.addr(), b"")).unwrap();
    rt.tick();
    let resp = inbox.try_recv().unwrap();
    assert!(resp.0.is_empty(), "address-only should echo empty payload");

    // Single byte payload
    rt.send_to(addr, framed_msg(inbox.addr(), &[0x42])).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, vec![0x42]);

    // Actor still alive after all those messages
    rt.send_to(addr, framed_msg(inbox.addr(), b"fin")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"fin");
}

proptest! {
    #[test]
    fn prop_echo_preserves_arbitrary_payload(payload in proptest::collection::vec(any::<u8>(), 0..500)) {
        let engine = SharedEngine::new().unwrap();
        let actor = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<ByteMessage>().unwrap();
        let addr = rt.spawn(actor).unwrap();

        rt.send_to(addr, framed_msg(inbox.addr(), &payload)).unwrap();
        rt.tick();

        if payload.is_empty() {
            // echo guest: if len < 32, returns empty; if len == 32 (addr only), returns empty payload
            let resp = inbox.try_recv();
            // With 32-byte address + 0-byte payload, echo sends back empty
            if let Some(msg) = resp {
                prop_assert!(msg.0.is_empty());
            }
        } else {
            let received = inbox.try_recv().expect("echo should reply for non-empty payload");
            prop_assert_eq!(received.0, payload);
        }
    }
}

#[test]
fn double_and_silent_contracts() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // Double: 1 message in → exactly 2 out
    let double = WasmActorBuilder::new(engine.clone(), guest_wasm("double")).build().unwrap();
    let daddr = rt.spawn(double).unwrap();
    rt.send_to(daddr, framed_msg(inbox.addr(), b"dup")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"dup");
    assert_eq!(inbox.try_recv().unwrap().0, b"dup");
    assert!(inbox.try_recv().is_none(), "exactly 2");

    // Silent: messages in → 0 out
    let silent = WasmActorBuilder::new(engine, guest_wasm("silent")).build().unwrap();
    let saddr = rt.spawn(silent).unwrap();
    for _ in 0..10 {
        rt.send_to(saddr, framed_msg(inbox.addr(), b"ignored")).unwrap();
    }
    rt.tick();
    assert!(inbox.try_recv().is_none(), "silent never sends");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 3: Alloc failure resilience
// ═══════════════════════════════════════════════════════════════════════════════

proptest! {
    #[test]
    fn prop_any_alloc_return_never_kills_actor(
        alloc_val in prop_oneof![
            -100i32..0,
            0i32..70000,
            Just(i32::MIN),
            Just(i32::MAX),
            Just(0i32),
            Just(65500i32),
            Just(65536i32),
        ],
    ) {
        let wasm = alloc_returns_wat(alloc_val);
        let engine = SharedEngine::new().unwrap();
        let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let addr = rt.spawn(actor).unwrap();

        // Send two messages — actor must survive both regardless of alloc return
        rt.send_to(addr, ByteMessage(vec![1, 2, 3])).unwrap();
        rt.tick();
        rt.send_to(addr, ByteMessage(vec![4, 5, 6])).unwrap();
        rt.tick();
    }
}

#[test]
fn alloc_trap_recovery() {
    // Alloc traps on first call (counter == 0), succeeds thereafter
    let wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (global $calls (mut i32) (i32.const 0))
        (func (export "alloc") (param $len i32) (result i32)
            (global.set $calls (i32.add (global.get $calls) (i32.const 1)))
            (if (result i32) (i32.eq (global.get $calls) (i32.const 1))
                (then unreachable)
                (else i32.const 4096)
            )
        )
        (func (export "handle") (param $ptr i32) (param $len i32)
            (if (i32.ge_u (local.get $len) (i32.const 33))
                (then
                    (call $send
                        (local.get $ptr)
                        (i32.add (local.get $ptr) (i32.const 32))
                        (i32.sub (local.get $len) (i32.const 32))
                    )
                )
            )
        )
    )"#;
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wat::parse_str(wat).unwrap()).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    // First message — alloc traps, dropped
    rt.send_to(addr, framed_msg(inbox.addr(), b"first")).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_none(), "first msg dropped due to alloc trap");

    // Second message — alloc succeeds, echoed
    rt.send_to(addr, framed_msg(inbox.addr(), b"second")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"second");
}

#[test]
fn alloc_alternates_and_exhaustion() {
    // Part A: alternating alloc (even calls succeed, odd calls return 0)
    let wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (global $calls (mut i32) (i32.const 0))
        (func (export "alloc") (param $len i32) (result i32)
            (global.set $calls (i32.add (global.get $calls) (i32.const 1)))
            (if (result i32) (i32.rem_u (global.get $calls) (i32.const 2))
                (then i32.const 4096)
                (else i32.const 0)
            )
        )
        (func (export "handle") (param $ptr i32) (param $len i32)
            (if (i32.ge_u (local.get $len) (i32.const 33))
                (then
                    (call $send
                        (local.get $ptr)
                        (i32.add (local.get $ptr) (i32.const 32))
                        (i32.sub (local.get $len) (i32.const 32))
                    )
                )
            )
        )
    )"#;
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine.clone(), wat::parse_str(wat).unwrap()).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    let mut echoed = 0;
    for i in 0u8..6 {
        rt.send_to(addr, framed_msg(inbox.addr(), &[i])).unwrap();
        rt.tick();
        if inbox.try_recv().is_some() { echoed += 1; }
    }
    assert_eq!(echoed, 3, "should echo on odd-numbered alloc calls only");

    // Part B: echo guest under sustained load — bump allocator exhaustion
    let echo = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let rt2 = Runtime::new(RuntimeConfig::default());
    let inbox2 = rt2.new_inbox::<ByteMessage>().unwrap();
    let addr2 = rt2.spawn(echo).unwrap();

    let mut total_echoed = 0;
    for _ in 0..2000 {
        rt2.send_to(addr2, framed_msg(inbox2.addr(), b"ping")).unwrap();
        rt2.tick();
        if inbox2.try_recv().is_some() { total_echoed += 1; }
    }
    // Some succeed (before OOM), some fail (after OOM). Actor survives throughout.
    assert!(total_echoed > 0, "at least some messages should echo");
    assert!(total_echoed < 2000, "bump allocator should eventually exhaust");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 4: Handle trap & outbox semantics
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn handle_trap_actor_survives() {
    let engine = SharedEngine::new().unwrap();

    // unreachable trap
    let unreachable_wat = wat::parse_str(r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param i32 i32) unreachable)
    )"#).unwrap();
    let actor = WasmActorBuilder::new(engine.clone(), unreachable_wat).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(actor).unwrap();

    // Send 3 messages — all trap, all dropped
    for _ in 0..3 {
        rt.send_to(addr, ByteMessage(vec![1])).unwrap();
        rt.tick();
    }

    // Division by zero trap
    let divzero_wat = wat::parse_str(r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (drop (i32.div_u (local.get $len) (i32.const 0)))
        )
    )"#).unwrap();
    let actor2 = WasmActorBuilder::new(engine, divzero_wat).build().unwrap();
    let addr2 = rt.spawn(actor2).unwrap();
    rt.send_to(addr2, ByteMessage(vec![1])).unwrap();
    rt.tick(); // no panic
}

#[test]
fn outbox_cleared_on_trap() {
    // Guest sends once (valid), then traps. Outbox should be cleared.
    let wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param $ptr i32) (param $len i32)
            ;; Send a valid message
            (call $send (local.get $ptr) (local.get $ptr) (i32.const 1))
            ;; Then trap
            unreachable
        )
    )"#;
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wat::parse_str(wat).unwrap()).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    rt.send_to(addr, framed_msg(inbox.addr(), b"data")).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_none(), "outbox cleared on trap — no messages delivered");

    // Actor survives
    rt.send_to(addr, ByteMessage(vec![1])).unwrap();
    rt.tick();
}

proptest! {
    #[test]
    fn prop_any_send_args_never_crash_host(
        dest_ptr in any::<i32>(),
        payload_ptr in any::<i32>(),
        payload_len in any::<i32>(),
    ) {
        let wasm = send_args_wat(dest_ptr, payload_ptr, payload_len);
        let engine = SharedEngine::new().unwrap();
        let actor = WasmActorBuilder::new(engine, wasm).build().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let addr = rt.spawn(actor).unwrap();

        rt.send_to(addr, ByteMessage(vec![0u8; 64])).unwrap();
        rt.tick(); // never panic
    }
}

#[test]
fn send_boundary_conditions() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // Zero-length payload sends empty ByteMessage
    let zero_len = wat::parse_str(r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (call $send (local.get $ptr) (i32.const 0) (i32.const 0))
        )
    )"#).unwrap();
    let actor = WasmActorBuilder::new(engine.clone(), zero_len).build().unwrap();
    let addr = rt.spawn(actor).unwrap();
    rt.send_to(addr, framed_msg(inbox.addr(), b"x")).unwrap();
    rt.tick();
    let msg = inbox.try_recv().expect("zero-len payload should deliver");
    assert!(msg.0.is_empty());

    // Dest at exact memory boundary: ptr=65504, needs 32 bytes → end=65536 = memory size. Works.
    let exact_end = send_args_wat(65504, 0, 1);
    let actor2 = WasmActorBuilder::new(engine.clone(), exact_end).build().unwrap();
    let addr2 = rt.spawn(actor2).unwrap();
    rt.send_to(addr2, ByteMessage(vec![0u8; 64])).unwrap();
    rt.tick(); // should not trap (exact fit)

    // Dest one past boundary: ptr=65505 → end=65537 > 65536. Traps.
    let one_past = send_args_wat(65505, 0, 1);
    let actor3 = WasmActorBuilder::new(engine.clone(), one_past).build().unwrap();
    let addr3 = rt.spawn(actor3).unwrap();
    rt.send_to(addr3, ByteMessage(vec![0u8; 64])).unwrap();
    rt.tick(); // traps but actor survives

    // Send to garbage address — silently dropped (no matching inbox)
    let garbage = wat::parse_str(r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (data (i32.const 500) "\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff\ff")
        (func (export "alloc") (param i32) (result i32) i32.const 256)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (call $send (i32.const 500) (i32.const 0) (i32.const 1))
        )
    )"#).unwrap();
    let actor4 = WasmActorBuilder::new(engine, garbage).build().unwrap();
    let addr4 = rt.spawn(actor4).unwrap();
    rt.send_to(addr4, ByteMessage(vec![1])).unwrap();
    rt.tick();
    assert!(inbox.try_recv().is_none(), "garbage addr → no delivery to our inbox");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 5: Lifecycle & integration
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn full_lifecycle_story() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // Spawn echo, use it
    let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr1 = rt.spawn(echo).unwrap();
    rt.send_to(addr1, framed_msg(inbox.addr(), b"alive")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"alive");

    // Stop it
    let _ = rt.stop_actor(addr1);
    rt.tick();
    rt.tick();

    // Send to dead actor — silently dropped
    let _ = rt.send_to(addr1, framed_msg(inbox.addr(), b"dead"));
    rt.tick();
    assert!(inbox.try_recv().is_none());

    // Respawn — different address, still works
    let echo2 = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr2 = rt.spawn(echo2).unwrap();
    assert_ne!(addr1, addr2, "respawned actor gets different address");
    rt.send_to(addr2, framed_msg(inbox.addr(), b"new")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"new");

    // Double-stop is fine
    let _ = rt.stop_actor(addr2);
    let _ = rt.stop_actor(addr2);
    rt.tick();
}

struct Forwarder {
    engine: SharedEngine,
    wasm_bytes: Vec<u8>,
    inbox_addr: ActorAddress,
}

#[derive(Clone)]
struct ForwardMsg(Vec<u8>);

impl ActorInterface for Forwarder {
    type Incoming = ForwardMsg;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: ForwardMsg) {
        let wasm = WasmActorBuilder::new(self.engine.clone(), self.wasm_bytes.clone())
            .build()
            .unwrap();
        let wasm_addr = ctx.spawn(wasm).unwrap();
        let _ = ctx.send(wasm_addr, framed_msg(&self.inbox_addr, &msg.0));
    }
}

#[test]
fn wasm_native_interop() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // Native forwarder spawns a WASM echo actor and forwards a message to it
    let forwarder = Forwarder {
        engine: engine.clone(),
        wasm_bytes: guest_wasm("echo"),
        inbox_addr: inbox.addr().clone(),
    };
    let fwd_addr = rt.spawn(forwarder).unwrap();
    rt.send_to(fwd_addr, ForwardMsg(b"via-native".to_vec())).unwrap();
    for _ in 0..5 { rt.tick(); }
    assert_eq!(inbox.try_recv().unwrap().0, b"via-native");
}

#[test]
fn wasm_relay_chain() {
    // A→B→C→inbox: each echo actor strips 32 bytes of address and sends payload to that address
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    let a = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let b = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let c = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let addr_a = rt.spawn(a).unwrap();
    let addr_b = rt.spawn(b).unwrap();
    let addr_c = rt.spawn(c).unwrap();

    // Nested framed: [addr_b | addr_c | addr_inbox | "end"]
    let mut payload = Vec::new();
    payload.extend_from_slice(&addr_b.0);
    payload.extend_from_slice(&addr_c.0);
    payload.extend_from_slice(&inbox.addr().0);
    payload.extend_from_slice(b"end");

    rt.send_to(addr_a, ByteMessage(payload)).unwrap();
    for _ in 0..3 { rt.tick(); }

    // After 3 hops, inbox should have the final payload "end"
    // A sends [addr_c | addr_inbox | "end"] to B
    // B sends [addr_inbox | "end"] to C
    // C sends "end" to inbox
    let msg = inbox.try_recv().expect("relay chain should deliver");
    assert_eq!(msg.0, b"end");
}

struct DeathCounter {
    count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Clone)]
struct WatchAddr(ActorAddress);

impl ActorInterface for DeathCounter {
    type Incoming = WatchAddr;
    type Response = ();
    fn handle(&mut self, ctx: &Ctx, msg: WatchAddr) {
        ctx.watch(msg.0);
    }
    fn on_actor_exit(&mut self, _ctx: &Ctx, _exited: swactor::actor::ActorExited) {
        self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn watch_notification() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());

    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let target = WasmActorBuilder::new(engine, guest_wasm("silent")).build().unwrap();
    let target_addr = rt.spawn(target).unwrap();
    let watcher_addr = rt.spawn(DeathCounter { count: count.clone() }).unwrap();

    rt.send_to(watcher_addr, WatchAddr(target_addr)).unwrap();
    for _ in 0..3 { rt.tick(); }

    let _ = rt.stop_actor(target_addr);
    for _ in 0..5 { rt.tick(); }

    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

proptest! {
    #[test]
    fn prop_lifecycle_fuzz(
        guest_idx in 0usize..3,
        n_msgs in 0u8..20,
        ticks_before in 1usize..5,
        ticks_after in 1usize..5,
    ) {
        let names = ["echo", "double", "silent"];
        let engine = SharedEngine::new().unwrap();
        let actor = WasmActorBuilder::new(engine, guest_wasm(names[guest_idx])).build().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<ByteMessage>().unwrap();
        let addr = rt.spawn(actor).unwrap();

        for _ in 0..n_msgs {
            let _ = rt.send_to(addr, framed_msg(inbox.addr(), b"x"));
        }
        for _ in 0..ticks_before { rt.tick(); }
        let _ = rt.stop_actor(addr);
        for _ in 0..ticks_after { rt.tick(); }
        while inbox.try_recv().is_some() {}
    }
}

proptest! {
    #[test]
    fn prop_mixed_guest_response_counts(
        echo_n in 0usize..5,
        double_n in 0usize..5,
        silent_n in 0usize..5,
    ) {
        let engine = SharedEngine::new().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<ByteMessage>().unwrap();

        for _ in 0..echo_n {
            let a = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
            let addr = rt.spawn(a).unwrap();
            rt.send_to(addr, framed_msg(inbox.addr(), b"e")).unwrap();
        }
        for _ in 0..double_n {
            let a = WasmActorBuilder::new(engine.clone(), guest_wasm("double")).build().unwrap();
            let addr = rt.spawn(a).unwrap();
            rt.send_to(addr, framed_msg(inbox.addr(), b"d")).unwrap();
        }
        for _ in 0..silent_n {
            let a = WasmActorBuilder::new(engine.clone(), guest_wasm("silent")).build().unwrap();
            let addr = rt.spawn(a).unwrap();
            rt.send_to(addr, ByteMessage(b"s".to_vec())).unwrap();
        }

        rt.tick();
        let total: usize = std::iter::from_fn(|| inbox.try_recv()).count();
        prop_assert_eq!(total, echo_n + 2 * double_n);
    }
}

#[test]
fn non_byte_message_silently_ignored() {
    // Send a String (wrong type) to a WASM actor — should be silently dropped
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    // send_to with wrong type — shouldn't panic
    let _ = rt.send_to(addr, "wrong type".to_string());
    rt.tick();
    assert!(inbox.try_recv().is_none());

    // Actor still alive
    rt.send_to(addr, framed_msg(inbox.addr(), b"ok")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"ok");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 6: Engine & runtime config
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn shared_engine_traits_and_clone() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<SharedEngine>();
    assert_send::<WasmActor>();

    let engine = SharedEngine::new().unwrap();
    let _ = format!("{:?}", engine); // Debug doesn't panic

    // Clone produces working independent engine
    let clone = engine.clone();
    let a1 = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let a2 = WasmActorBuilder::new(clone, guest_wasm("echo")).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr1 = rt.spawn(a1).unwrap();
    let addr2 = rt.spawn(a2).unwrap();
    rt.send_to(addr1, framed_msg(inbox.addr(), b"c1")).unwrap();
    rt.send_to(addr2, framed_msg(inbox.addr(), b"c2")).unwrap();
    rt.tick();
    let mut msgs: Vec<Vec<u8>> = std::iter::from_fn(|| inbox.try_recv().map(|m| m.0)).collect();
    msgs.sort();
    assert_eq!(msgs, vec![b"c1".to_vec(), b"c2".to_vec()]);

    // ByteMessage traits
    let bm = ByteMessage(vec![1, 2, 3]);
    let bm2 = bm.clone();
    assert_eq!(bm, bm2);
    let _ = format!("{:?}", bm);
}

#[test]
fn multi_thread_runtime() {
    let engine = SharedEngine::new().unwrap();
    let config = RuntimeConfig { num_threads: 4, ..RuntimeConfig::default() };
    let rt = Runtime::new(config);
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    let mut addrs = Vec::new();
    for _ in 0..10 {
        let actor = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
        addrs.push(rt.spawn(actor).unwrap());
    }
    for (i, addr) in addrs.iter().enumerate() {
        rt.send_to(*addr, framed_msg(inbox.addr(), &[i as u8])).unwrap();
    }

    let handle = rt.run().unwrap();
    let mut received = Vec::new();
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(25));
        while let Some(msg) = inbox.try_recv() {
            received.push(msg.0[0]);
        }
        if received.len() == 10 { break; }
    }
    handle.shutdown();
    received.sort();
    assert_eq!(received, (0..10u8).collect::<Vec<_>>());
}

#[test]
fn budget_and_mailbox() {
    let engine = SharedEngine::new().unwrap();

    // Budget=1: one message processed per tick
    let mut cfg = RuntimeConfig::default();
    cfg.actor_message_budget = 1;
    let rt = Runtime::new(cfg);
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr = rt.spawn(echo).unwrap();
    rt.send_to(addr, framed_msg(inbox.addr(), b"a")).unwrap();
    rt.send_to(addr, framed_msg(inbox.addr(), b"b")).unwrap();
    rt.send_to(addr, framed_msg(inbox.addr(), b"c")).unwrap();
    rt.tick();
    let first_tick: usize = std::iter::from_fn(|| inbox.try_recv()).count();
    assert_eq!(first_tick, 1, "budget=1 processes exactly 1 per tick");
    rt.tick();
    rt.tick();
    let rest: usize = std::iter::from_fn(|| inbox.try_recv()).count();
    assert_eq!(rest, 2, "remaining 2 processed over next 2 ticks");

    // Self-send loop bounded by budget — no explosion
    let mut cfg2 = RuntimeConfig::default();
    cfg2.actor_message_budget = 2;
    let rt2 = Runtime::new(cfg2);
    let echo2 = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let self_addr = rt2.spawn(echo2).unwrap();
    rt2.send_to(self_addr, framed_msg(&self_addr, b"loop")).unwrap();
    for _ in 0..5 { rt2.tick(); } // no panic, bounded

    // DropOldest mailbox
    use swactor::runtime::MailboxOverflow;
    let mut cfg3 = RuntimeConfig::default();
    cfg3.default_mailbox_capacity = 3;
    cfg3.mailbox_overflow = MailboxOverflow::DropOldest;
    let rt3 = Runtime::new(cfg3);
    let inbox3 = rt3.new_inbox::<ByteMessage>().unwrap();
    let echo3 = WasmActorBuilder::new(engine, guest_wasm("echo")).build().unwrap();
    let addr3 = rt3.spawn(echo3).unwrap();
    for i in 0..10u8 {
        rt3.send_to(addr3, framed_msg(inbox3.addr(), &[i])).unwrap();
    }
    rt3.tick();
    let responses: Vec<_> = std::iter::from_fn(|| inbox3.try_recv()).collect();
    assert!(responses.len() <= 3, "mailbox capacity limits processing: got {}", responses.len());
}

#[test]
fn multiple_runtimes_and_scale() {
    let engine = SharedEngine::new().unwrap();

    // Two runtimes share engine
    let rt1 = Runtime::new(RuntimeConfig::default());
    let rt2 = Runtime::new(RuntimeConfig::default());
    let inbox1 = rt1.new_inbox::<ByteMessage>().unwrap();
    let inbox2 = rt2.new_inbox::<ByteMessage>().unwrap();
    let a1 = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let a2 = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr1 = rt1.spawn(a1).unwrap();
    let addr2 = rt2.spawn(a2).unwrap();
    rt1.send_to(addr1, framed_msg(inbox1.addr(), b"rt1")).unwrap();
    rt2.send_to(addr2, framed_msg(inbox2.addr(), b"rt2")).unwrap();
    rt1.tick();
    rt2.tick();
    assert_eq!(inbox1.try_recv().unwrap().0, b"rt1");
    assert_eq!(inbox2.try_recv().unwrap().0, b"rt2");

    // 50 actors from same engine
    let rt3 = Runtime::new(RuntimeConfig::default());
    let inbox3 = rt3.new_inbox::<ByteMessage>().unwrap();
    for i in 0..50u8 {
        let a = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
        let addr = rt3.spawn(a).unwrap();
        rt3.send_to(addr, framed_msg(inbox3.addr(), &[i])).unwrap();
    }
    rt3.tick();
    let count: usize = std::iter::from_fn(|| inbox3.try_recv()).count();
    assert_eq!(count, 50);

    // Build+drop 100 actors without spawning — no leak
    for _ in 0..100 {
        let _ = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    }
}

#[test]
fn ordering_and_determinism() {
    let engine = SharedEngine::new().unwrap();

    // FIFO ordering
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr = rt.spawn(echo).unwrap();
    for i in 0..20u8 {
        rt.send_to(addr, framed_msg(inbox.addr(), &[i])).unwrap();
    }
    rt.tick();
    let msgs: Vec<u8> = std::iter::from_fn(|| inbox.try_recv().map(|m| m.0[0])).collect();
    assert_eq!(msgs, (0..20u8).collect::<Vec<_>>(), "FIFO preserved");

    // Determinism: same input → same output across runs
    let mut results = Vec::new();
    for _ in 0..5 {
        let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
        let rt = Runtime::new(RuntimeConfig::default());
        let inbox = rt.new_inbox::<ByteMessage>().unwrap();
        let addr = rt.spawn(echo).unwrap();
        rt.send_to(addr, framed_msg(inbox.addr(), b"deterministic")).unwrap();
        rt.tick();
        results.push(inbox.try_recv().unwrap().0);
    }
    assert!(results.windows(2).all(|w| w[0] == w[1]), "deterministic across runs");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 7: WASM feature coverage & guest computation
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn kitchen_sink_wat_module() {
    // Single WAT module exercising as many supported WASM features as possible.
    // If this builds and doesn't trap, the engine config is correct.
    let wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 2)

        ;; Data segments
        (data (i32.const 0) "kitchen-sink")

        ;; Globals: mutable and immutable
        (global $counter (mut i32) (i32.const 0))
        (global $MAGIC i32 (i32.const 42))

        ;; Table for call_indirect
        (table 2 funcref)
        (elem (i32.const 0) $helper_add $helper_sub)

        ;; Internal helper functions
        (func $helper_add (param i32 i32) (result i32)
            (i32.add (local.get 0) (local.get 1)))
        (func $helper_sub (param i32 i32) (result i32)
            (i32.sub (local.get 0) (local.get 1)))
        (func $deep_call (param $x i32) (result i32)
            (i32.mul (local.get $x) (i32.const 2)))

        (func (export "alloc") (param $len i32) (result i32) i32.const 4096)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (local $a i32)
            (local $b i64)
            (local $c f32)
            (local $d f64)
            (local $i i32)
            (local $tmp i32)

            ;; Increment global counter
            (global.set $counter (i32.add (global.get $counter) (i32.const 1)))

            ;; Read immutable global
            (local.set $a (global.get $MAGIC))

            ;; i32 arithmetic
            (local.set $a (i32.add (local.get $a) (i32.const 10)))
            (local.set $a (i32.sub (local.get $a) (i32.const 5)))
            (local.set $a (i32.mul (local.get $a) (i32.const 3)))
            (local.set $a (i32.div_u (local.get $a) (i32.const 2)))
            (local.set $a (i32.rem_u (local.get $a) (i32.const 100)))

            ;; Bitwise operations
            (local.set $a (i32.and (local.get $a) (i32.const 0xFF)))
            (local.set $a (i32.or  (local.get $a) (i32.const 0x10)))
            (local.set $a (i32.xor (local.get $a) (i32.const 0x01)))
            (local.set $a (i32.shl (local.get $a) (i32.const 2)))
            (local.set $a (i32.shr_u (local.get $a) (i32.const 1)))
            (local.set $a (i32.shr_s (local.get $a) (i32.const 1)))
            (local.set $a (i32.rotl (local.get $a) (i32.const 3)))
            (local.set $a (i32.rotr (local.get $a) (i32.const 3)))

            ;; Bit counting
            (drop (i32.clz (local.get $a)))
            (drop (i32.ctz (local.get $a)))
            (drop (i32.popcnt (local.get $a)))
            (drop (i32.eqz (local.get $a)))

            ;; Comparisons
            (drop (i32.gt_s (local.get $a) (i32.const 0)))
            (drop (i32.lt_s (local.get $a) (i32.const 100)))
            (drop (i32.le_u (local.get $a) (i32.const 200)))
            (drop (i32.ge_s (local.get $a) (i32.const -1)))

            ;; i64 operations
            (local.set $b (i64.const 9999999999))
            (local.set $b (i64.add (local.get $b) (i64.const 1)))
            (i64.store (i32.const 800) (local.get $b))
            (drop (i64.load (i32.const 800)))

            ;; i64 ↔ i32 conversions
            (drop (i32.wrap_i64 (local.get $b)))
            (drop (i64.extend_i32_s (local.get $a)))

            ;; f32 operations
            (local.set $c (f32.const 3.14))
            (local.set $c (f32.add (local.get $c) (f32.const 1.0)))
            (local.set $c (f32.mul (local.get $c) (f32.const 2.0)))
            (f32.store (i32.const 900) (local.get $c))
            (drop (f32.load (i32.const 900)))

            ;; f64 operations
            (local.set $d (f64.promote_f32 (local.get $c)))
            (local.set $d (f64.mul (local.get $d) (f64.const 0.5)))
            (f64.store (i32.const 920) (local.get $d))

            ;; Sign extension
            (drop (i32.extend8_s (i32.const 0x80)))
            (drop (i32.extend16_s (i32.const 0x8000)))

            ;; Memory store/load variants
            (i32.store8 (i32.const 700) (i32.const 0xAB))
            (i32.store16 (i32.const 702) (i32.const 0xCDEF))
            (drop (i32.load8_u (i32.const 700)))
            (drop (i32.load16_u (i32.const 702)))

            ;; local.tee
            (local.set $tmp (local.tee $a (i32.const 77)))

            ;; Nested function calls
            (drop (call $deep_call (i32.const 5)))
            (drop (call $helper_add (i32.const 10) (i32.const 20)))

            ;; call_indirect via table
            (drop (call_indirect (type 0) (i32.const 7) (i32.const 3) (i32.const 0)))
            (drop (call_indirect (type 0) (i32.const 7) (i32.const 3) (i32.const 1)))

            ;; Control flow: if/else
            (if (i32.gt_s (local.get $len) (i32.const 0))
                (then nop)
                (else nop)
            )

            ;; select
            (drop (select (i32.const 10) (i32.const 20) (i32.const 1)))

            ;; block + br_if
            (block $skip
                (br_if $skip (i32.eqz (local.get $len)))
                nop
            )

            ;; loop with counter
            (local.set $i (i32.const 0))
            (block $exit
                (loop $loop
                    (br_if $exit (i32.ge_u (local.get $i) (i32.const 5)))
                    (local.set $i (i32.add (local.get $i) (i32.const 1)))
                    (br $loop)
                )
            )

            ;; br_table dispatch
            (block $b0 (block $b1 (block $b2
                (br_table $b0 $b1 $b2 (i32.const 1))
            ) nop) nop) ;; falls through

            ;; Bulk memory: fill and copy
            (memory.fill (i32.const 600) (i32.const 0xAA) (i32.const 32))
            (memory.copy (i32.const 650) (i32.const 600) (i32.const 32))

            ;; memory.size and memory.grow
            (drop (memory.size))
            (drop (memory.grow (i32.const 1)))

            ;; nop
            nop
        )

        ;; Type for call_indirect
        (type (func (param i32 i32) (result i32)))
    )"#;
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wat::parse_str(wat).unwrap()).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let addr = rt.spawn(actor).unwrap();

    // Send message, tick — no trap
    rt.send_to(addr, ByteMessage(vec![1, 2, 3, 4])).unwrap();
    rt.tick();

    // Send again — global counter increments, still no trap
    rt.send_to(addr, ByteMessage(vec![5, 6, 7, 8])).unwrap();
    rt.tick();
}

#[test]
fn guest_mutable_state_persists() {
    // Global counter increments on each handle call, sends count back
    let wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (global $count (mut i32) (i32.const 0))
        (func (export "alloc") (param $len i32) (result i32) i32.const 4096)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (if (i32.lt_u (local.get $len) (i32.const 32)) (then return))
            (global.set $count (i32.add (global.get $count) (i32.const 1)))
            (i32.store (i32.const 200) (global.get $count))
            (call $send (local.get $ptr) (i32.const 200) (i32.const 4))
        )
    )"#;
    let engine = SharedEngine::new().unwrap();
    let actor = WasmActorBuilder::new(engine, wat::parse_str(wat).unwrap()).build().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    for expected in 1..=5u32 {
        rt.send_to(addr, framed_msg(inbox.addr(), b"x")).unwrap();
        rt.tick();
        let resp = inbox.try_recv().expect("should get count back");
        let count = u32::from_le_bytes([resp.0[0], resp.0[1], resp.0[2], resp.0[3]]);
        assert_eq!(count, expected, "counter should persist across messages");
    }
}

#[test]
fn guest_transforms_payload() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // XOR each byte with 0xFF (bitwise NOT)
    let xor_wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param $len i32) (result i32) i32.const 4096)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (local $i i32)
            (if (i32.lt_u (local.get $len) (i32.const 33)) (then return))
            (local.set $i (i32.const 32))
            (block $exit (loop $loop
                (br_if $exit (i32.ge_u (local.get $i) (local.get $len)))
                (i32.store8
                    (i32.add (local.get $ptr) (local.get $i))
                    (i32.xor (i32.load8_u (i32.add (local.get $ptr) (local.get $i))) (i32.const 0xFF)))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $loop)
            ))
            (call $send (local.get $ptr)
                (i32.add (local.get $ptr) (i32.const 32))
                (i32.sub (local.get $len) (i32.const 32)))
        )
    )"#;
    let xor = WasmActorBuilder::new(engine.clone(), wat::parse_str(xor_wat).unwrap()).build().unwrap();
    let xaddr = rt.spawn(xor).unwrap();
    rt.send_to(xaddr, framed_msg(inbox.addr(), &[0x00, 0xFF, 0xAA])).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, vec![0xFF, 0x00, 0x55]);

    // Reverse payload
    let rev_wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param $len i32) (result i32) i32.const 4096)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (local $i i32) (local $plen i32) (local $pstart i32)
            (if (i32.lt_u (local.get $len) (i32.const 33)) (then return))
            (local.set $pstart (i32.add (local.get $ptr) (i32.const 32)))
            (local.set $plen (i32.sub (local.get $len) (i32.const 32)))
            (local.set $i (i32.const 0))
            (block $exit (loop $loop
                (br_if $exit (i32.ge_u (local.get $i) (local.get $plen)))
                (i32.store8
                    (i32.add (i32.const 900) (local.get $i))
                    (i32.load8_u (i32.add (local.get $pstart)
                        (i32.sub (i32.sub (local.get $plen) (i32.const 1)) (local.get $i)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $loop)
            ))
            (call $send (local.get $ptr) (i32.const 900) (local.get $plen))
        )
    )"#;
    let rev = WasmActorBuilder::new(engine, wat::parse_str(rev_wat).unwrap()).build().unwrap();
    let raddr = rt.spawn(rev).unwrap();
    rt.send_to(raddr, framed_msg(inbox.addr(), b"abcde")).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"edcba");
}

#[test]
fn guest_multi_send_patterns() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // Guest sends to 2 different destinations in one handle call
    let two_dest_wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param $len i32) (result i32) i32.const 1024)
        (func (export "handle") (param $ptr i32) (param $len i32)
            ;; Layout: [addr1:32][addr2:32][payload:rest]
            (if (i32.ge_u (local.get $len) (i32.const 65))
                (then
                    (call $send (local.get $ptr)
                        (i32.add (local.get $ptr) (i32.const 64))
                        (i32.sub (local.get $len) (i32.const 64)))
                    (call $send (i32.add (local.get $ptr) (i32.const 32))
                        (i32.add (local.get $ptr) (i32.const 64))
                        (i32.sub (local.get $len) (i32.const 64)))
                )
            )
        )
    )"#;
    let actor = WasmActorBuilder::new(engine.clone(), wat::parse_str(two_dest_wat).unwrap())
        .build().unwrap();
    let inbox2 = rt.new_inbox::<ByteMessage>().unwrap();
    let addr = rt.spawn(actor).unwrap();

    let mut msg = Vec::new();
    msg.extend_from_slice(&inbox.addr().0);   // addr1
    msg.extend_from_slice(&inbox2.addr().0);  // addr2
    msg.extend_from_slice(b"shared");         // payload
    rt.send_to(addr, ByteMessage(msg)).unwrap();
    rt.tick();
    assert_eq!(inbox.try_recv().unwrap().0, b"shared");
    assert_eq!(inbox2.try_recv().unwrap().0, b"shared");

    // Guest sends 100 messages in one handle call
    let many_sends_wat = r#"(module
        (import "swactor" "send" (func $send (param i32 i32 i32)))
        (memory (export "memory") 1)
        (func (export "alloc") (param $len i32) (result i32) i32.const 1024)
        (func (export "handle") (param $ptr i32) (param $len i32)
            (local $i i32)
            (if (i32.lt_u (local.get $len) (i32.const 32)) (then return))
            (local.set $i (i32.const 0))
            (block $exit (loop $loop
                (br_if $exit (i32.ge_u (local.get $i) (i32.const 100)))
                (i32.store8 (i32.const 900) (local.get $i))
                (call $send (local.get $ptr) (i32.const 900) (i32.const 1))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $loop)
            ))
        )
    )"#;
    let actor2 = WasmActorBuilder::new(engine, wat::parse_str(many_sends_wat).unwrap())
        .build().unwrap();
    let addr2 = rt.spawn(actor2).unwrap();
    rt.send_to(addr2, framed_msg(inbox.addr(), b"go")).unwrap();
    rt.tick();
    let count: usize = std::iter::from_fn(|| inbox.try_recv()).count();
    assert_eq!(count, 100, "all 100 sends should be delivered");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Group 8: Stress
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn echo_sustained_load() {
    let engine = SharedEngine::new().unwrap();
    let rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ByteMessage>().unwrap();

    // 500 messages to echo actor
    let echo = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
    let addr = rt.spawn(echo).unwrap();
    for i in 0..500u16 {
        rt.send_to(addr, framed_msg(inbox.addr(), &i.to_le_bytes())).unwrap();
    }
    for _ in 0..20 { rt.tick(); }
    let count: usize = std::iter::from_fn(|| inbox.try_recv()).count();
    assert_eq!(count, 500, "all 500 messages echoed");

    // Spawn and stop 100 actors — no panic, no leak
    for _ in 0..100 {
        let a = WasmActorBuilder::new(engine.clone(), guest_wasm("echo")).build().unwrap();
        let a_addr = rt.spawn(a).unwrap();
        rt.send_to(a_addr, framed_msg(inbox.addr(), b"x")).unwrap();
        rt.tick();
        let _ = rt.stop_actor(a_addr);
        rt.tick();
    }
    // Drain inbox
    while inbox.try_recv().is_some() {}
}
