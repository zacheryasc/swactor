use std::collections::HashMap;

use proptest::prelude::*;
use swactor::actor::ActorAddress;
use swactor_process::*;

fn automated_spec() -> ProcessSpec {
    ProcessSpec {
        command: "test".into(),
        args: vec![],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

fn addr(n: u8) -> ActorAddress {
    let mut bytes = [0u8; 32];
    bytes[0] = n;
    ActorAddress(bytes)
}

fn arb_signal() -> impl Strategy<Value = Signal> {
    prop_oneof![
        Just(Signal::Terminate),
        Just(Signal::Kill),
        Just(Signal::Hangup),
        Just(Signal::Interrupt),
        (0..32i32).prop_map(Signal::Other),
    ]
}

fn arb_event() -> impl Strategy<Value = ProcessEvent> {
    prop_oneof![
        Just(ProcessEvent::Started),
        Just(ProcessEvent::KillTimeout),
        ".*".prop_map(|reason| ProcessEvent::SpawnFailed { reason }),
        proptest::collection::vec(any::<u8>(), 0..64).prop_map(|data| {
            ProcessEvent::OutputReceived {
                data,
                is_stderr: false,
            }
        }),
        proptest::collection::vec(any::<u8>(), 0..64).prop_map(|data| {
            ProcessEvent::OutputReceived {
                data,
                is_stderr: true,
            }
        }),
        prop_oneof![
            any::<i32>().prop_map(ExitStatus::Code),
            any::<i32>().prop_map(ExitStatus::Signal),
            Just(ExitStatus::Unknown),
        ]
        .prop_map(|status| ProcessEvent::Exited { status }),
        ".*".prop_map(|reason| ProcessEvent::ConnectionLost { reason }),
        (0..5usize).prop_map(|n| ProcessEvent::StdinWritten { byte_count: n * 10 }),
        Just(ProcessEvent::SignalSent),
        Just(ProcessEvent::PtyResized),
        proptest::collection::vec(any::<u8>(), 0..64)
            .prop_map(|data| ProcessEvent::WriteStdin { data }),
        arb_signal().prop_map(|signal| ProcessEvent::SendSignal { signal }),
        Just(ProcessEvent::ResizePty {
            size: PtySize { cols: 80, rows: 24 },
        }),
        Just(ProcessEvent::CloseStdin),
        Just(ProcessEvent::CloseRequested),
        (0..4u8).prop_map(|n| ProcessEvent::Subscribe { address: addr(n) }),
        (0..4u8).prop_map(|n| ProcessEvent::Unsubscribe { address: addr(n) }),
    ]
}

// ──────────────────────────────────────────────
// 1. No panics for arbitrary event sequences
// ──────────────────────────────────────────────

proptest! {
    #[test]
    fn no_panics_on_arbitrary_events(events in proptest::collection::vec(arb_event(), 0..50)) {
        let (mut session, _) = ProcessSession::new(automated_spec());
        for event in events {
            let _ = session.apply(event);
        }
    }
}

// ──────────────────────────────────────────────
// 2. Exited is terminal
// ──────────────────────────────────────────────

proptest! {
    #[test]
    fn exited_is_terminal(events in proptest::collection::vec(arb_event(), 0..50)) {
        let (mut session, _) = ProcessSession::new(automated_spec());
        let mut reached_exited = false;

        for event in events {
            let _ = session.apply(event);
            if session.state() == ProcessState::Exited {
                reached_exited = true;
            }
            if reached_exited {
                prop_assert_eq!(session.state(), ProcessState::Exited);
            }
        }
    }
}

// ──────────────────────────────────────────────
// 3. SelfTerminate always last action when entering Exited
// ──────────────────────────────────────────────

proptest! {
    #[test]
    fn self_terminate_is_last_when_entering_exited(events in proptest::collection::vec(arb_event(), 0..50)) {
        let (mut session, _) = ProcessSession::new(automated_spec());
        let mut was_exited = false;

        for event in events {
            let prev_state = session.state();
            let actions = session.apply(event);

            // If we just transitioned into Exited
            if session.state() == ProcessState::Exited && !was_exited && prev_state != ProcessState::Exited {
                prop_assert!(
                    matches!(actions.last(), Some(ProcessAction::SelfTerminate)),
                    "SelfTerminate must be last action when entering Exited, got: {:?}", actions
                );
            }

            if session.state() == ProcessState::Exited {
                was_exited = true;
            }
        }
    }
}

// ──────────────────────────────────────────────
// 4. Subscriber count matches add/remove operations
// ──────────────────────────────────────────────

proptest! {
    #[test]
    fn subscriber_count_is_consistent(
        ops in proptest::collection::vec(
            prop_oneof![
                (0..8u8).prop_map(|n| (true, n)),
                (0..8u8).prop_map(|n| (false, n)),
            ],
            0..30
        )
    ) {
        let (mut session, _) = ProcessSession::new(automated_spec());
        let mut expected: Vec<u8> = Vec::new();

        for (is_add, n) in ops {
            if is_add {
                session.apply(ProcessEvent::Subscribe { address: addr(n) });
                if !expected.contains(&n) {
                    expected.push(n);
                }
            } else {
                session.apply(ProcessEvent::Unsubscribe { address: addr(n) });
                expected.retain(|&x| x != n);
            }
            prop_assert_eq!(session.subscriber_count(), expected.len());
        }
    }
}

// ──────────────────────────────────────────────
// 5. State monotonicity (never goes backward)
// ──────────────────────────────────────────────

fn state_ordinal(s: ProcessState) -> u8 {
    match s {
        ProcessState::Starting => 0,
        ProcessState::Running => 1,
        ProcessState::Stopping => 2,
        ProcessState::Exited => 3,
    }
}

proptest! {
    #[test]
    fn state_never_goes_backward(events in proptest::collection::vec(arb_event(), 0..50)) {
        let (mut session, _) = ProcessSession::new(automated_spec());
        let mut max_ordinal = state_ordinal(session.state());

        for event in events {
            let _ = session.apply(event);
            let current = state_ordinal(session.state());
            prop_assert!(
                current >= max_ordinal,
                "State went backward: ordinal {} -> {}", max_ordinal, current
            );
            max_ordinal = current;
        }
    }
}
