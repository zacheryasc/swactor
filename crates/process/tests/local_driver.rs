//! Layer 4 — LocalDriver integration tests.
//!
//! Real OS processes, no actor layer. Tests LocalDriver in isolation.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use swactor_process::*;

fn automated_spec(cmd: &str, args: &[&str]) -> ProcessSpec {
    ProcessSpec {
        command: cmd.into(),
        args: args.iter().map(|s| s.to_string()).collect(),
        env: HashMap::new(),
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: None,
        stdin_buffer_limit: None,
    }
}

/// Poll the driver until `pred` matches at least one collected event, or timeout.
fn poll_until_match(
    driver: &mut LocalDriver,
    timeout: Duration,
    pred: impl Fn(&ProcessEvent) -> bool,
) -> Vec<ProcessEvent> {
    let start = std::time::Instant::now();
    let mut all_events = Vec::new();
    loop {
        let events = driver.poll();
        if events.is_empty() {
            if start.elapsed() >= timeout {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        all_events.extend(events);
        if all_events.iter().any(&pred) {
            break;
        }
    }
    all_events
}

fn has_event(events: &[ProcessEvent], pred: impl Fn(&ProcessEvent) -> bool) -> bool {
    events.iter().any(pred)
}

#[test]
fn echo_produces_started_output_and_exit_zero() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    let spec = automated_spec("echo", &["hello"]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    // Wait for Exited (which means Started + output + exit are all in)
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Exited { .. })
    });

    assert!(
        has_event(&events, |e| matches!(e, ProcessEvent::Started)),
        "should have Started event, got: {:?}",
        events
    );
    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::OutputReceived {
                is_stderr: false,
                ..
            }
        )),
        "should have stdout OutputReceived"
    );

    // Check the output contains "hello"
    let output: Vec<u8> = events
        .iter()
        .filter_map(|e| match e {
            ProcessEvent::OutputReceived {
                data,
                is_stderr: false,
            } => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains("hello"),
        "output should contain 'hello', got: {:?}",
        output_str
    );

    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::Exited {
                status: ExitStatus::Code(0)
            }
        )),
        "should have Exited(0)"
    );
}

#[test]
fn cat_stdin_echo_and_close() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    let spec = automated_spec("cat", &[]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    // Wait for Started
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Started)
    });
    assert!(has_event(&events, |e| matches!(e, ProcessEvent::Started)));

    // Write to stdin
    driver.execute(ProcessAction::WriteStdin {
        data: b"ping\n".to_vec(),
    });

    // Wait until we see actual output (not just the StdinWritten ack)
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::OutputReceived { .. })
    });
    let output: Vec<u8> = events
        .iter()
        .filter_map(|e| match e {
            ProcessEvent::OutputReceived { data, .. } => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    let output_str = String::from_utf8_lossy(&output);
    assert!(
        output_str.contains("ping"),
        "cat should echo back 'ping', got: {:?}",
        output_str
    );

    // Close stdin — cat should exit
    driver.execute(ProcessAction::CloseStdin);
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Exited { .. })
    });
    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::Exited {
                status: ExitStatus::Code(0)
            }
        )),
        "cat should exit cleanly after stdin close, got: {:?}",
        events
    );
}

#[test]
fn signal_terminates_long_running_process() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    let spec = automated_spec("sleep", &["60"]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    // Wait for Started
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Started)
    });
    assert!(has_event(&events, |e| matches!(e, ProcessEvent::Started)));

    // Send SIGTERM
    driver.execute(ProcessAction::SendSignal {
        signal: Signal::Terminate,
    });

    // Wait for Exited (may also see SignalSent ack first)
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Exited { .. })
    });
    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::Exited {
                status: ExitStatus::Signal(_)
            }
        )),
        "sleep should exit with signal status after SIGTERM, got: {:?}",
        events
    );
}

#[test]
fn bad_command_produces_spawn_failed() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    let spec = automated_spec("/nonexistent/binary/that/does/not/exist", &[]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::SpawnFailed { .. })
    });
    assert!(
        has_event(&events, |e| matches!(e, ProcessEvent::SpawnFailed { .. })),
        "nonexistent binary should produce SpawnFailed, got: {:?}",
        events
    );
}

#[test]
fn large_output_no_data_loss() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    // Generate a large amount of output: seq 1 10000
    let spec = automated_spec("seq", &["1", "10000"]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    // Collect all events until exit
    let events = poll_until_match(&mut driver, Duration::from_secs(10), |e| {
        matches!(e, ProcessEvent::Exited { .. })
    });

    // Gather all output
    let output: Vec<u8> = events
        .iter()
        .filter_map(|e| match e {
            ProcessEvent::OutputReceived { data, .. } => Some(data.clone()),
            _ => None,
        })
        .flatten()
        .collect();

    let output_str = String::from_utf8_lossy(&output);
    // seq 1 10000 should end with "10000\n"
    assert!(
        output_str.contains("10000"),
        "large output should contain '10000'"
    );
    // Check that it starts with "1\n"
    assert!(
        output_str.starts_with("1\n"),
        "large output should start with '1\\n'"
    );

    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::Exited {
                status: ExitStatus::Code(0)
            }
        )),
        "seq should exit cleanly"
    );
}

#[test]
fn kill_timeout_escalates_to_sigkill() {
    let queue = EventQueue::new();
    let waker_slot = Arc::new(OnceLock::new());
    let mut driver = LocalDriver::new(queue, waker_slot);

    // Spawn a process that traps SIGTERM. Use exec to replace the shell so
    // SIGTERM goes directly to the perl process (avoids shell vs child races).
    let spec = automated_spec("perl", &["-e", "$SIG{TERM} = 'IGNORE'; sleep 300"]);
    driver.execute(ProcessAction::SpawnProcess { spec });

    // Wait for Started
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Started)
    });
    assert!(has_event(&events, |e| matches!(e, ProcessEvent::Started)));

    // Give the process a moment to set up the trap
    thread::sleep(Duration::from_millis(100));

    // Send SIGTERM (the process ignores it)
    driver.execute(ProcessAction::SendSignal {
        signal: Signal::Terminate,
    });
    poll_until_match(&mut driver, Duration::from_secs(1), |e| {
        matches!(e, ProcessEvent::SignalSent)
    });

    // Verify the process is still alive after a short wait (SIGTERM was ignored)
    thread::sleep(Duration::from_millis(200));
    let events = driver.poll();
    assert!(
        !has_event(&events, |e| matches!(e, ProcessEvent::Exited { .. })),
        "process should still be alive after SIGTERM (trap should ignore it)"
    );

    // Schedule a short kill timeout
    driver.execute(ProcessAction::ScheduleKillTimeout {
        duration: Duration::from_millis(200),
    });

    // Wait for KillTimeout event
    let events = poll_until_match(&mut driver, Duration::from_secs(3), |e| {
        matches!(e, ProcessEvent::KillTimeout)
    });
    assert!(
        has_event(&events, |e| matches!(e, ProcessEvent::KillTimeout)),
        "should receive KillTimeout, got: {:?}",
        events
    );

    // Now send SIGKILL
    driver.execute(ProcessAction::SendSignal {
        signal: Signal::Kill,
    });

    // Wait for exit
    let events = poll_until_match(&mut driver, Duration::from_secs(5), |e| {
        matches!(e, ProcessEvent::Exited { .. })
    });
    assert!(
        has_event(&events, |e| matches!(
            e,
            ProcessEvent::Exited {
                status: ExitStatus::Signal(_)
            }
        )),
        "process should exit with signal after SIGKILL, got: {:?}",
        events
    );
}
