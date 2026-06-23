use std::collections::HashMap;
use std::time::Duration;

use swactor::actor::ActorAddress;
use swactor_process::*;

fn automated_spec() -> ProcessSpec {
    ProcessSpec {
        command: "echo".into(),
        args: vec!["hello".into()],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

fn spec_with_kill_timeout(timeout: Duration) -> ProcessSpec {
    ProcessSpec {
        kill_timeout: Some(timeout),
        ..automated_spec()
    }
}

fn spec_with_stdin_limit(limit: usize) -> ProcessSpec {
    ProcessSpec {
        stdin_buffer_limit: Some(limit),
        ..automated_spec()
    }
}

fn interactive_spec() -> ProcessSpec {
    ProcessSpec {
        command: "/bin/bash".into(),
        args: vec![],
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Interactive,
        initial_pty_size: Some(PtySize { cols: 80, rows: 24 }),
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

fn addr(n: u8) -> ActorAddress {
    let mut bytes = [0u8; 32];
    bytes[0] = n;
    ActorAddress(bytes)
}

/// Verify that a SelfTerminate is present and is the last action.
fn assert_self_terminate_is_last(actions: &[ProcessAction]) {
    assert!(
        matches!(actions.last(), Some(ProcessAction::SelfTerminate)),
        "SelfTerminate must be the last action, got: {actions:?}"
    );
}

// ──────────────────────────────────────────────
// 1. Happy path — automated process
// ──────────────────────────────────────────────

#[test]
fn automated_process_runs_produces_output_and_exits_cleanly() {
    let (mut session, init) = ProcessSession::new(automated_spec());
    assert_eq!(session.state(), ProcessState::Starting);
    assert!(matches!(&init[0], ProcessAction::SpawnProcess { .. }));

    // Process starts
    let actions = session.apply(ProcessEvent::Started);
    assert_eq!(session.state(), ProcessState::Running);
    assert!(matches!(&actions[0], ProcessAction::NotifyStarted { .. }));

    // Some output arrives
    let actions = session.apply(ProcessEvent::OutputReceived {
        data: b"hello\n".to_vec(),
        is_stderr: false,
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyOutput {
            stream: OutputStream::Stdout,
            ..
        }
    ));

    // More output on stderr
    let actions = session.apply(ProcessEvent::OutputReceived {
        data: b"warn\n".to_vec(),
        is_stderr: true,
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyOutput {
            stream: OutputStream::Stderr,
            ..
        }
    ));

    // Process exits
    let actions = session.apply(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    assert_eq!(session.state(), ProcessState::Exited);
    assert_eq!(session.exit_status(), Some(ExitStatus::Code(0)));
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// 2. Interactive process with subscriber lifecycle
// ──────────────────────────────────────────────

#[test]
fn interactive_session_manages_subscribers_correctly() {
    let (mut session, _) = ProcessSession::new(interactive_spec());

    // Add two subscribers before start
    session.apply(ProcessEvent::Subscribe { address: addr(1) });
    session.apply(ProcessEvent::Subscribe { address: addr(2) });
    assert_eq!(session.subscriber_count(), 2);

    // Duplicate add is a no-op
    session.apply(ProcessEvent::Subscribe { address: addr(1) });
    assert_eq!(session.subscriber_count(), 2);

    // Start — both subscribers notified
    let actions = session.apply(ProcessEvent::Started);
    match &actions[0] {
        ProcessAction::NotifyStarted { subscribers } => {
            assert_eq!(subscribers.len(), 2);
        }
        other => panic!("expected NotifyStarted, got {other:?}"),
    }

    // Remove one subscriber
    session.apply(ProcessEvent::Unsubscribe { address: addr(1) });
    assert_eq!(session.subscriber_count(), 1);

    // Output only goes to remaining subscriber
    let actions = session.apply(ProcessEvent::OutputReceived {
        data: b"data".to_vec(),
        is_stderr: false,
    });
    match &actions[0] {
        ProcessAction::NotifyOutput { subscribers, .. } => {
            assert_eq!(subscribers, &vec![addr(2)]);
        }
        other => panic!("expected NotifyOutput, got {other:?}"),
    }

    // Exit
    let actions = session.apply(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    match &actions[0] {
        ProcessAction::NotifyExited { subscribers, .. } => {
            assert_eq!(subscribers, &vec![addr(2)]);
        }
        other => panic!("expected NotifyExited, got {other:?}"),
    }
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// 3. Spawn failure
// ──────────────────────────────────────────────

#[test]
fn spawn_failure_notifies_and_self_terminates() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Subscribe { address: addr(1) });

    let actions = session.apply(ProcessEvent::SpawnFailed {
        reason: "command not found".into(),
    });
    assert_eq!(session.state(), ProcessState::Exited);
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyError {
            error: ProcessError::SpawnFailed { .. },
            ..
        }
    ));
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// 4. Connection loss mid-run
// ──────────────────────────────────────────────

#[test]
fn connection_loss_during_running_transitions_to_exited() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);

    let actions = session.apply(ProcessEvent::ConnectionLost {
        reason: "pipe broken".into(),
    });
    assert_eq!(session.state(), ProcessState::Exited);
    assert_eq!(session.exit_status(), Some(ExitStatus::Unknown));
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyError {
            error: ProcessError::ConnectionLost { .. },
            ..
        }
    ));
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// 5. Close requested before start
// ──────────────────────────────────────────────

#[test]
fn close_before_start_sends_signal_on_belated_start() {
    let (mut session, _) = ProcessSession::new(automated_spec());

    // Close requested while still Starting
    let actions = session.apply(ProcessEvent::CloseRequested);
    assert!(actions.is_empty());
    assert_eq!(session.state(), ProcessState::Starting);

    // Process starts belatedly — should immediately get SIGTERM
    let actions = session.apply(ProcessEvent::Started);
    assert_eq!(session.state(), ProcessState::Stopping);
    assert!(matches!(&actions[0], ProcessAction::NotifyStarted { .. }));
    assert!(matches!(
        &actions[1],
        ProcessAction::SendSignal {
            signal: Signal::Terminate
        }
    ));
}

// ──────────────────────────────────────────────
// 6. Invalid operations produce errors, not panics
// ──────────────────────────────────────────────

#[test]
fn invalid_event_in_starting_produces_error() {
    let (mut session, _) = ProcessSession::new(automated_spec());

    let actions = session.apply(ProcessEvent::WriteStdin {
        data: b"hi".to_vec(),
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyError {
            error: ProcessError::InvalidState {
                attempted: "WriteStdin",
                current_state: "Starting"
            },
            ..
        }
    ));
    // State unchanged
    assert_eq!(session.state(), ProcessState::Starting);
}

#[test]
fn invalid_event_in_exited_produces_error() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::SpawnFailed {
        reason: "no".into(),
    });
    assert_eq!(session.state(), ProcessState::Exited);

    let actions = session.apply(ProcessEvent::WriteStdin {
        data: b"hi".to_vec(),
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyError {
            error: ProcessError::InvalidState {
                attempted: "WriteStdin",
                current_state: "Exited"
            },
            ..
        }
    ));
}

// ──────────────────────────────────────────────
// 7. Stdin closed then write → error
// ──────────────────────────────────────────────

#[test]
fn write_after_stdin_closed_produces_error() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);

    let actions = session.apply(ProcessEvent::CloseStdin);
    assert!(matches!(&actions[0], ProcessAction::CloseStdin));
    assert!(session.stdin_closed());

    // Duplicate close is a no-op
    let actions = session.apply(ProcessEvent::CloseStdin);
    assert!(actions.is_empty());

    // Write after close → error
    let actions = session.apply(ProcessEvent::WriteStdin {
        data: b"too late".to_vec(),
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::NotifyError {
            error: ProcessError::InvalidState { .. },
            ..
        }
    ));
}

// ──────────────────────────────────────────────
// 8. MockDriver round-trip (driver + session tick loop)
// ──────────────────────────────────────────────

#[test]
fn mock_driver_round_trip() {
    let (mut session, init_actions) = ProcessSession::new(automated_spec());
    let mut driver = MockDriver::new();

    // Execute initial actions (SpawnProcess)
    for action in init_actions {
        driver.execute(action);
    }
    assert!(matches!(
        &driver.executed_actions()[0],
        ProcessAction::SpawnProcess { .. }
    ));

    // Simulate: driver produces Started
    driver.inject(ProcessEvent::Started);

    // Tick loop: poll → apply → execute
    let events = driver.poll();
    for event in events {
        let actions = session.apply(event);
        for action in actions {
            driver.execute(action);
        }
    }
    assert_eq!(session.state(), ProcessState::Running);

    // Simulate output and exit
    driver.inject(ProcessEvent::OutputReceived {
        data: b"done".to_vec(),
        is_stderr: false,
    });
    driver.inject(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });

    let events = driver.poll();
    for event in events {
        let actions = session.apply(event);
        for action in actions {
            driver.execute(action);
        }
    }

    assert_eq!(session.state(), ProcessState::Exited);

    // Verify the driver saw the expected sequence
    let all_actions = driver.take_executed_actions();
    assert!(matches!(
        &all_actions[0],
        ProcessAction::SpawnProcess { .. }
    ));
    assert!(matches!(
        &all_actions[1],
        ProcessAction::NotifyStarted { .. }
    ));
    assert!(matches!(
        &all_actions[2],
        ProcessAction::NotifyOutput { .. }
    ));
    assert!(matches!(
        &all_actions[3],
        ProcessAction::NotifyExited { .. }
    ));
    assert!(matches!(&all_actions[4], ProcessAction::SelfTerminate));
}

// ──────────────────────────────────────────────
// 9. Signal escalation in Stopping
// ──────────────────────────────────────────────

#[test]
fn signal_escalation_allowed_in_stopping() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);

    // Escalate to Kill
    let actions = session.apply(ProcessEvent::SendSignal {
        signal: Signal::Kill,
    });
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Kill
        }
    ));

    // Can still receive output while stopping
    let actions = session.apply(ProcessEvent::OutputReceived {
        data: b"final".to_vec(),
        is_stderr: false,
    });
    assert!(matches!(&actions[0], ProcessAction::NotifyOutput { .. }));

    // Finally exits
    let actions = session.apply(ProcessEvent::Exited {
        status: ExitStatus::Signal(9),
    });
    assert_eq!(session.exit_status(), Some(ExitStatus::Signal(9)));
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// 10. Late acks in Exited silently consumed
// ──────────────────────────────────────────────

#[test]
fn late_acks_in_exited_are_silently_consumed() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    assert_eq!(session.state(), ProcessState::Exited);

    // Acks should produce no actions, no errors
    assert!(
        session
            .apply(ProcessEvent::StdinWritten { byte_count: 10 })
            .is_empty()
    );
    assert!(session.apply(ProcessEvent::SignalSent).is_empty());
    assert!(session.apply(ProcessEvent::PtyResized).is_empty());

    // Subscribe/Unsubscribe also still works in Exited
    assert!(
        session
            .apply(ProcessEvent::Subscribe { address: addr(1) })
            .is_empty()
    );
    assert_eq!(session.subscriber_count(), 1);
    assert!(
        session
            .apply(ProcessEvent::Unsubscribe { address: addr(1) })
            .is_empty()
    );
    assert_eq!(session.subscriber_count(), 0);
}

// ──────────────────────────────────────────────
// Flow control tracking
// ──────────────────────────────────────────────

#[test]
fn flow_control_tracks_pending_stdin_bytes() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);

    session.apply(ProcessEvent::WriteStdin {
        data: vec![0u8; 100],
    });
    assert_eq!(session.flow_control().pending_stdin_bytes, 100);

    session.apply(ProcessEvent::WriteStdin {
        data: vec![0u8; 50],
    });
    assert_eq!(session.flow_control().pending_stdin_bytes, 150);

    session.apply(ProcessEvent::StdinWritten { byte_count: 80 });
    assert_eq!(session.flow_control().pending_stdin_bytes, 70);

    // Ack more than pending → saturates at 0
    session.apply(ProcessEvent::StdinWritten { byte_count: 200 });
    assert_eq!(session.flow_control().pending_stdin_bytes, 0);
}

// ──────────────────────────────────────────────
// CloseStdin in Stopping
// ──────────────────────────────────────────────

#[test]
fn close_stdin_allowed_in_stopping() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);

    let actions = session.apply(ProcessEvent::CloseStdin);
    assert!(matches!(&actions[0], ProcessAction::CloseStdin));
    assert!(session.stdin_closed());
}

// ──────────────────────────────────────────────
// Connection loss in Stopping
// ──────────────────────────────────────────────

#[test]
fn connection_loss_in_stopping_transitions_to_exited() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);

    let actions = session.apply(ProcessEvent::ConnectionLost {
        reason: "gone".into(),
    });
    assert_eq!(session.state(), ProcessState::Exited);
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// Redundant CloseRequested in Stopping is no-op
// ──────────────────────────────────────────────

#[test]
fn duplicate_close_requested_in_stopping_is_noop() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);

    let actions = session.apply(ProcessEvent::CloseRequested);
    assert!(actions.is_empty());
    assert_eq!(session.state(), ProcessState::Stopping);
}

// ──────────────────────────────────────────────
// Kill timeout — A1–A6
// ──────────────────────────────────────────────

#[test]
fn close_requested_with_kill_timeout_schedules_timer() {
    let (mut session, _) = ProcessSession::new(spec_with_kill_timeout(Duration::from_secs(5)));
    session.apply(ProcessEvent::Started);

    let actions = session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Terminate
        }
    ));
    assert!(matches!(
        &actions[1],
        ProcessAction::ScheduleKillTimeout { duration } if *duration == Duration::from_secs(5)
    ));
}

#[test]
fn close_before_start_with_kill_timeout_schedules_timer_on_belated_start() {
    let (mut session, _) = ProcessSession::new(spec_with_kill_timeout(Duration::from_secs(3)));
    session.apply(ProcessEvent::CloseRequested);

    let actions = session.apply(ProcessEvent::Started);
    assert_eq!(session.state(), ProcessState::Stopping);
    assert!(matches!(&actions[0], ProcessAction::NotifyStarted { .. }));
    assert!(matches!(
        &actions[1],
        ProcessAction::SendSignal {
            signal: Signal::Terminate
        }
    ));
    assert!(matches!(
        &actions[2],
        ProcessAction::ScheduleKillTimeout { duration } if *duration == Duration::from_secs(3)
    ));
}

#[test]
fn kill_timeout_in_stopping_sends_sigkill() {
    let (mut session, _) = ProcessSession::new(spec_with_kill_timeout(Duration::from_secs(5)));
    session.apply(ProcessEvent::Started);
    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);

    let actions = session.apply(ProcessEvent::KillTimeout);
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Kill
        }
    ));
    assert_eq!(session.state(), ProcessState::Stopping);
}

#[test]
fn kill_timeout_silently_consumed_outside_stopping() {
    // Starting
    let (mut session, _) = ProcessSession::new(automated_spec());
    assert!(session.apply(ProcessEvent::KillTimeout).is_empty());
    assert_eq!(session.state(), ProcessState::Starting);

    // Running
    session.apply(ProcessEvent::Started);
    assert!(session.apply(ProcessEvent::KillTimeout).is_empty());
    assert_eq!(session.state(), ProcessState::Running);

    // Exited
    session.apply(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    assert!(session.apply(ProcessEvent::KillTimeout).is_empty());
    assert_eq!(session.state(), ProcessState::Exited);
}

#[test]
fn close_requested_without_kill_timeout_no_schedule_action() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);

    let actions = session.apply(ProcessEvent::CloseRequested);
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Terminate
        }
    ));
}

#[test]
fn kill_timeout_full_escalation_to_sigkill_then_exit() {
    let (mut session, _) = ProcessSession::new(spec_with_kill_timeout(Duration::from_secs(1)));
    session.apply(ProcessEvent::Started);

    // CloseRequested → SIGTERM + schedule
    let actions = session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.state(), ProcessState::Stopping);
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Terminate
        }
    ));
    assert!(matches!(
        &actions[1],
        ProcessAction::ScheduleKillTimeout { .. }
    ));

    // KillTimeout fires → SIGKILL
    let actions = session.apply(ProcessEvent::KillTimeout);
    assert!(matches!(
        &actions[0],
        ProcessAction::SendSignal {
            signal: Signal::Kill
        }
    ));

    // Process finally exits via signal 9
    let actions = session.apply(ProcessEvent::Exited {
        status: ExitStatus::Signal(9),
    });
    assert_eq!(session.state(), ProcessState::Exited);
    assert_eq!(session.exit_status(), Some(ExitStatus::Signal(9)));
    assert_self_terminate_is_last(&actions);
}

// ──────────────────────────────────────────────
// Backpressure — B1–B5
// ──────────────────────────────────────────────

#[test]
fn backpressure_buffers_when_over_limit() {
    let (mut session, _) = ProcessSession::new(spec_with_stdin_limit(100));
    session.apply(ProcessEvent::Started);

    // First write (50 bytes) — under limit, passes through
    let actions = session.apply(ProcessEvent::WriteStdin {
        data: vec![1u8; 50],
    });
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], ProcessAction::WriteStdin { .. }));
    assert_eq!(session.flow_control().pending_stdin_bytes, 50);

    // Second write (60 bytes) — still under limit (50 < 100), passes through
    let actions = session.apply(ProcessEvent::WriteStdin {
        data: vec![2u8; 60],
    });
    assert_eq!(actions.len(), 1);
    assert_eq!(session.flow_control().pending_stdin_bytes, 110);

    // Third write (30 bytes) — now at 110 >= 100, buffered
    let actions = session.apply(ProcessEvent::WriteStdin {
        data: vec![3u8; 30],
    });
    assert!(actions.is_empty());
    assert_eq!(session.stdin_buffer_bytes(), 30);
    // pending_stdin_bytes unchanged (buffered data not counted as pending)
    assert_eq!(session.flow_control().pending_stdin_bytes, 110);
}

#[test]
fn stdin_written_ack_drains_buffer() {
    let (mut session, _) = ProcessSession::new(spec_with_stdin_limit(100));
    session.apply(ProcessEvent::Started);

    // Fill up: 100 bytes pending
    session.apply(ProcessEvent::WriteStdin {
        data: vec![1u8; 100],
    });
    assert_eq!(session.flow_control().pending_stdin_bytes, 100);

    // Buffer two chunks
    session.apply(ProcessEvent::WriteStdin {
        data: vec![2u8; 40],
    });
    session.apply(ProcessEvent::WriteStdin {
        data: vec![3u8; 30],
    });
    assert_eq!(session.stdin_buffer_bytes(), 70);

    // Ack 80 bytes → pending drops to 20, buffer should drain in FIFO order
    let actions = session.apply(ProcessEvent::StdinWritten { byte_count: 80 });
    // pending was 100, now 20. Drain first chunk (40 bytes) → pending = 60.
    // 60 < 100, drain second chunk (30 bytes) → pending = 90.
    // 90 < 100, buffer empty.
    assert_eq!(actions.len(), 2);
    assert!(matches!(&actions[0], ProcessAction::WriteStdin { data } if data.len() == 40));
    assert!(matches!(&actions[1], ProcessAction::WriteStdin { data } if data.len() == 30));
    assert_eq!(session.flow_control().pending_stdin_bytes, 90);
    assert_eq!(session.stdin_buffer_bytes(), 0);
}

#[test]
fn close_requested_clears_stdin_buffer() {
    let (mut session, _) = ProcessSession::new(spec_with_stdin_limit(50));
    session.apply(ProcessEvent::Started);

    session.apply(ProcessEvent::WriteStdin {
        data: vec![1u8; 60],
    });
    session.apply(ProcessEvent::WriteStdin {
        data: vec![2u8; 30],
    });
    assert_eq!(session.stdin_buffer_bytes(), 30);

    session.apply(ProcessEvent::CloseRequested);
    assert_eq!(session.stdin_buffer_bytes(), 0);
}

#[test]
fn no_backpressure_when_limit_is_none() {
    let (mut session, _) = ProcessSession::new(automated_spec());
    session.apply(ProcessEvent::Started);

    // All writes pass through regardless of pending bytes
    for _ in 0..10 {
        let actions = session.apply(ProcessEvent::WriteStdin {
            data: vec![0u8; 1000],
        });
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], ProcessAction::WriteStdin { .. }));
    }
    assert_eq!(session.flow_control().pending_stdin_bytes, 10_000);
    assert_eq!(session.stdin_buffer_bytes(), 0);
}

#[test]
fn exit_clears_stdin_buffer() {
    let (mut session, _) = ProcessSession::new(spec_with_stdin_limit(50));
    session.apply(ProcessEvent::Started);

    session.apply(ProcessEvent::WriteStdin {
        data: vec![1u8; 60],
    });
    session.apply(ProcessEvent::WriteStdin {
        data: vec![2u8; 30],
    });
    assert_eq!(session.stdin_buffer_bytes(), 30);

    session.apply(ProcessEvent::Exited {
        status: ExitStatus::Code(0),
    });
    assert_eq!(session.stdin_buffer_bytes(), 0);
}
