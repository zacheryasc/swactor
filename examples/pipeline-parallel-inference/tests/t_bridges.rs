//! Tests for the three network-to-`StageMsg` adapter bridges in
//! `stage_actor.rs`: `RequestBridge`, `NextTokenBridge`, `ActivationBridge`.
//!
//! These actors are small (`handle` rewraps a single inbound type as the
//! corresponding `StageMsg::*` variant and forwards it), but they sit at
//! the seam between the network and the per-role actor: a wrong wrapper
//! is silently corrupted data. Tests assert the contract by spawning the
//! bridge with a stub target inbox and round-tripping a representative
//! payload of each type.
//!
//! Test names match TEST_SPEC §7 verbatim.

use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};

use pipeline_parallel_inference::messages::{InferenceRequest, NextToken, StageActivation};
use pipeline_parallel_inference::stage_actor::{
    ActivationBridge, NextTokenBridge, RequestBridge, StageMsg,
};

fn tick_until_recv<M: swactor::actor::Message>(
    rt: &Runtime,
    inbox: &Inbox<M>,
    timeout: Duration,
) -> Option<M> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        rt.tick();
        if let Some(msg) = inbox.try_recv() {
            return Some(msg);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    None
}

#[test]
fn request_bridge_forwards_inference_request_to_target() {
    let rt = Runtime::new(RuntimeConfig::default());
    let target = rt.new_inbox::<StageMsg>().unwrap();
    let reply_inbox = rt.new_inbox::<()>().unwrap();

    let bridge_addr = rt
        .spawn(RequestBridge {
            target: *target.addr(),
        })
        .unwrap();

    let req = InferenceRequest {
        reply_to: *reply_inbox.addr(),
        prompt: "hello".into(),
        max_tokens: 4,
    };
    rt.send_to(bridge_addr, req.clone()).unwrap();

    let received = tick_until_recv(&rt, &target, Duration::from_secs(2))
        .expect("RequestBridge must forward the InferenceRequest as StageMsg::Inference");
    match received {
        StageMsg::Inference(got) => assert_eq!(got, req),
        other => panic!("expected StageMsg::Inference, got {other:?}"),
    }
}

#[test]
fn next_token_bridge_forwards_next_token_to_target() {
    let rt = Runtime::new(RuntimeConfig::default());
    let target = rt.new_inbox::<StageMsg>().unwrap();

    let bridge_addr = rt
        .spawn(NextTokenBridge {
            target: *target.addr(),
        })
        .unwrap();

    let nt = NextToken {
        request_id: 0xCAFE,
        token_id: 17,
        position: 4,
        done: false,
    };
    rt.send_to(bridge_addr, nt.clone()).unwrap();

    let received = tick_until_recv(&rt, &target, Duration::from_secs(2))
        .expect("NextTokenBridge must forward the NextToken as StageMsg::NextToken");
    match received {
        StageMsg::NextToken(got) => assert_eq!(got, nt),
        other => panic!("expected StageMsg::NextToken, got {other:?}"),
    }
}

#[test]
fn activation_bridge_forwards_stage_activation_to_target() {
    let rt = Runtime::new(RuntimeConfig::default());
    let target = rt.new_inbox::<StageMsg>().unwrap();

    let bridge_addr = rt
        .spawn(ActivationBridge {
            target: *target.addr(),
        })
        .unwrap();

    let act = StageActivation {
        request_id: 7,
        position: 3,
        // Non-trivial bytes so a silent payload swap would show up as an
        // inequality on the asserted comparison.
        hidden: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04],
        seq_len: 2,
        is_prefill: true,
    };
    rt.send_to(bridge_addr, act.clone()).unwrap();

    let received = tick_until_recv(&rt, &target, Duration::from_secs(2))
        .expect("ActivationBridge must forward the StageActivation as StageMsg::Activation");
    match received {
        StageMsg::Activation(got) => assert_eq!(got, act),
        other => panic!("expected StageMsg::Activation, got {other:?}"),
    }
}

/// If a bridge's `target` does not point at a live inbox, the bridge's
/// internal `ctx.send` returns `Err`. The bridge handler discards that
/// error with `let _ = …`, so the runtime stays healthy and a subsequent
/// message to a separate, valid target arrives intact.
#[test]
fn bridges_drop_messages_when_target_address_invalid() {
    let rt = Runtime::new(RuntimeConfig::default());

    // Address that was never registered with the runtime.
    let bogus_target = ActorAddress::new_random();

    let req_bridge = rt
        .spawn(RequestBridge {
            target: bogus_target,
        })
        .unwrap();
    let nt_bridge = rt
        .spawn(NextTokenBridge {
            target: bogus_target,
        })
        .unwrap();
    let act_bridge = rt
        .spawn(ActivationBridge {
            target: bogus_target,
        })
        .unwrap();

    // None of these may panic or poison the runtime.
    let reply_to = rt.new_inbox::<()>().unwrap();
    rt.send_to(
        req_bridge,
        InferenceRequest {
            reply_to: *reply_to.addr(),
            prompt: "void".into(),
            max_tokens: 1,
        },
    )
    .unwrap();
    rt.send_to(
        nt_bridge,
        NextToken {
            request_id: 1,
            token_id: 0,
            position: 0,
            done: false,
        },
    )
    .unwrap();
    rt.send_to(
        act_bridge,
        StageActivation {
            request_id: 1,
            position: 0,
            hidden: vec![0u8; 16],
            seq_len: 1,
            is_prefill: true,
        },
    )
    .unwrap();

    // Pump a bit so each bridge has a chance to run its handler and
    // discard the send-to-invalid-target error.
    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        rt.tick();
        std::thread::sleep(Duration::from_millis(5));
    }

    // The runtime should still deliver to a valid target. Spawn a fresh
    // bridge with a real target and prove the path is intact.
    let target = rt.new_inbox::<StageMsg>().unwrap();
    let good_bridge = rt
        .spawn(NextTokenBridge {
            target: *target.addr(),
        })
        .unwrap();
    let nt = NextToken {
        request_id: 42,
        token_id: 5,
        position: 1,
        done: true,
    };
    rt.send_to(good_bridge, nt.clone()).unwrap();
    let received = tick_until_recv(&rt, &target, Duration::from_secs(2))
        .expect("runtime should still deliver after prior bridge sends to invalid target failed");
    match received {
        StageMsg::NextToken(got) => assert_eq!(got, nt),
        other => panic!("expected StageMsg::NextToken, got {other:?}"),
    }
}
