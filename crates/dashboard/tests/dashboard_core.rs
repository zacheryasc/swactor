use std::sync::Arc;

use dashboard::RuntimeTrace;
use dashboard::collector::StatsCollector;
use dashboard::layer::{DashboardEvent, EventStore};
use swactor::actor::ActorAddress;
use swactor::stats::{ActorSnapshot, StatsHook};

fn make_event(message: &str) -> DashboardEvent {
    DashboardEvent {
        seq: 0, // filled by EventStore::push
        timestamp_ms: 1000,
        level: "INFO".into(),
        message: message.into(),
        worker_id: None,
        actor_addr: None,
        fields: serde_json::Map::new(),
    }
}

// ── EventStore: Streaming cursor semantics ──────────────────────────────

/// Scenario: Two clients consume the same event stream at different rates.
/// A fast client reads every event; a slow client joins late and catches up.
/// Both eventually see the same final event.
#[test]
fn two_clients_consuming_at_different_rates() {
    let store = EventStore::new(100, false, 0);

    // Fast client starts at cursor 0
    let mut fast_cursor: u64 = 0;

    // Push 5 events
    for i in 0..5 {
        store.push(make_event(&format!("event-{i}")));
    }

    // Fast client reads all 5
    let (batch, new_cursor) = store.read_from(fast_cursor);
    assert_eq!(batch.len(), 5);
    assert_eq!(batch[0].message, "event-0");
    assert_eq!(batch[4].message, "event-4");
    fast_cursor = new_cursor;

    // Push 3 more
    for i in 5..8 {
        store.push(make_event(&format!("event-{i}")));
    }

    // Fast client sees only new 3
    let (batch, new_cursor) = store.read_from(fast_cursor);
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].message, "event-5");
    fast_cursor = new_cursor;

    // Slow client joins now at cursor 0 — sees all 8
    let (slow_batch, slow_cursor) = store.read_from(0);
    assert_eq!(slow_batch.len(), 8);
    assert_eq!(slow_batch[7].message, "event-7");

    // Both cursors now agree
    assert_eq!(fast_cursor, slow_cursor);
}

/// Scenario: The event stream overflows the ring buffer.
/// A client that fell behind loses old events but gets the most recent window.
#[test]
fn ring_buffer_overflow_caps_old_cursors() {
    let store = EventStore::new(10, false, 0);

    // Push 25 events into a 10-capacity ring
    for i in 0..25 {
        store.push(make_event(&format!("event-{i}")));
    }

    // A client at cursor 0 gets only the most recent 10
    let (batch, cursor) = store.read_from(0);
    assert_eq!(batch.len(), 10);
    assert_eq!(batch[0].message, "event-15");
    assert_eq!(batch[9].message, "event-24");
    assert_eq!(cursor, 25);

    // A client already caught up gets nothing
    let (batch, _) = store.read_from(cursor);
    assert!(batch.is_empty());
}

/// Scenario: Client has a cursor beyond the latest event (future cursor).
/// This can happen if events were trimmed. The client should get nothing, not panic.
#[test]
fn future_cursor_returns_empty() {
    let store = EventStore::new(10, false, 0);
    store.push(make_event("only-one"));

    let (batch, cursor) = store.read_from(999);
    assert!(batch.is_empty());
    assert_eq!(cursor, 999); // cursor unchanged
}

/// Scenario: Empty store — no events ever pushed.
#[test]
fn empty_store_returns_nothing() {
    let store = EventStore::new(10, false, 0);
    let (batch, cursor) = store.read_from(0);
    assert!(batch.is_empty());
    assert_eq!(cursor, 0);
}

// ── EventStore: Recording pipeline ──────────────────────────────────────

/// Scenario: A monitoring session records events and saves a valid trace file.
/// Given: recording enabled, events flowing through the store
/// When:  all_events() is called
/// Then:  every event is available and the data round-trips through JSON
#[test]
fn recording_session_produces_replayable_trace() {
    let store = EventStore::new(5, true, 100);

    // Simulate a burst of runtime events
    for i in 0..20 {
        let mut ev = make_event(&format!("tick-{i}"));
        ev.worker_id = Some(i % 3);
        store.push(ev);
    }

    // Drain the recording log
    let events = store.all_events().expect("recording should be enabled");
    assert_eq!(events.len(), 20, "all 20 events should be in the recording");

    // Build a trace and round-trip through JSON
    let trace = RuntimeTrace {
        events,
        stats_timeline: Vec::new(),
    };
    let json = serde_json::to_string(&trace).unwrap();
    let restored: RuntimeTrace = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.events.len(), 20);
    assert_eq!(restored.events[0].message, "tick-0");
    assert_eq!(restored.events[19].message, "tick-19");
    assert_eq!(restored.events[1].worker_id, Some(1));
}

/// Scenario: Recording disabled — all_events returns None.
#[test]
fn no_recording_means_no_full_log() {
    let store = EventStore::new(10, false, 0);
    store.push(make_event("hello"));
    assert!(store.all_events().is_none());
}

/// Scenario: all_events() is destructive — second call gets an empty vec.
#[test]
fn recording_drain_is_destructive() {
    let store = EventStore::new(5, true, 100);
    store.push(make_event("one"));
    store.push(make_event("two"));

    let first = store.all_events().unwrap();
    assert_eq!(first.len(), 2);

    let second = store.all_events().unwrap();
    assert!(second.is_empty(), "second drain should get nothing");
}

// ── StatsCollector: Multi-worker snapshot aggregation ───────────────────

/// Scenario: Three workers each report actor snapshots independently.
/// The dashboard merges all workers' data into a single view.
#[test]
fn three_workers_report_independently_merged_view_is_complete() {
    let collector = StatsCollector::new(3);

    let addr_a = ActorAddress::new_random();
    let addr_b = ActorAddress::new_random();
    let addr_c = ActorAddress::new_random();

    // Worker 0 reports 1 actor
    collector.on_tick(
        0,
        &[ActorSnapshot {
            address: addr_a,
            mailbox_depth: 5,
            last_msg_type: Some("Ping"),
            messages_processed: 100,
            poisoned: false,
            message_type_counts: vec![],
        }],
    );

    // Worker 1 reports 1 actor
    collector.on_tick(
        1,
        &[ActorSnapshot {
            address: addr_b,
            mailbox_depth: 0,
            last_msg_type: None,
            messages_processed: 50,
            poisoned: false,
            message_type_counts: vec![],
        }],
    );

    // Worker 2 reports 1 actor (poisoned)
    collector.on_tick(
        2,
        &[ActorSnapshot {
            address: addr_c,
            mailbox_depth: 3,
            last_msg_type: Some("BadMsg"),
            messages_processed: 10,
            poisoned: true,
            message_type_counts: vec![],
        }],
    );

    // Dashboard reads merged view
    let details = collector.actor_details();
    assert_eq!(details.len(), 3, "all 3 actors from 3 workers");

    let info_a = details.iter().find(|d| d.address == addr_a).unwrap();
    assert_eq!(info_a.worker_id, 0);
    assert_eq!(info_a.mailbox_depth, 5);
    assert_eq!(info_a.messages_processed, 100);

    let info_c = details.iter().find(|d| d.address == addr_c).unwrap();
    assert!(info_c.poisoned);
    assert_eq!(info_c.worker_id, 2);
}

/// Scenario: A worker updates its snapshots — old data is replaced, not accumulated.
#[test]
fn worker_update_replaces_stale_snapshot() {
    let collector = StatsCollector::new(1);
    let addr = ActorAddress::new_random();

    // First tick: 1 actor with 10 messages
    collector.on_tick(
        0,
        &[ActorSnapshot {
            address: addr,
            mailbox_depth: 5,
            last_msg_type: None,
            messages_processed: 10,
            poisoned: false,
            message_type_counts: vec![],
        }],
    );

    assert_eq!(collector.actor_details().len(), 1);
    assert_eq!(collector.actor_details()[0].messages_processed, 10);

    // Second tick: same actor now has 25 messages
    collector.on_tick(
        0,
        &[ActorSnapshot {
            address: addr,
            mailbox_depth: 2,
            last_msg_type: Some("Update"),
            messages_processed: 25,
            poisoned: false,
            message_type_counts: vec![],
        }],
    );

    let details = collector.actor_details();
    assert_eq!(details.len(), 1, "still 1 actor, not 2");
    assert_eq!(details[0].messages_processed, 25);
    assert_eq!(details[0].mailbox_depth, 2);
}

/// Scenario: A worker reports zero actors (all stopped). Dashboard reflects empty.
#[test]
fn worker_reports_empty_after_all_actors_stop() {
    let collector = StatsCollector::new(2);
    let addr = ActorAddress::new_random();

    // Worker 0 has actors
    collector.on_tick(
        0,
        &[ActorSnapshot {
            address: addr,
            mailbox_depth: 0,
            last_msg_type: None,
            messages_processed: 5,
            poisoned: false,
            message_type_counts: vec![],
        }],
    );
    assert_eq!(collector.actor_details().len(), 1);

    // Worker 0 reports empty (all actors stopped)
    collector.on_tick(0, &[]);
    assert!(collector.actor_details().is_empty());
}

// ── Integration: EventStore sequences are monotonically increasing ──────

/// Scenario: Push events from "multiple sources" — sequences never have gaps or duplicates.
#[test]
fn event_sequences_are_gap_free_and_monotonic() {
    let store = Arc::new(EventStore::new(50, false, 0));

    // Simulate interleaved pushes
    for i in 0..30 {
        let mut ev = make_event(&format!("source-{}-event", i % 3));
        ev.worker_id = Some(i % 3);
        store.push(ev);
    }

    let (batch, _) = store.read_from(0);
    assert_eq!(batch.len(), 30);

    // Verify monotonic sequences with no gaps
    for (i, ev) in batch.iter().enumerate() {
        assert_eq!(ev.seq, i as u64, "seq should be monotonically increasing");
    }
}
