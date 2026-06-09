//! Datastore metrics: scenario → readout / event-stream contracts.
//!
//! The datastore's metrics are a *source* the datastream frames. These tests
//! drive a realistic sequence of operations and check two contracts, each
//! against an independently-computed expectation — never the accumulator read
//! back against itself:
//!   * the steady `readout()` agrees with a plain tally over the op script; and
//!   * the op-event stream the observer emits is exactly the script's ops.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use swactor_datastore::metrics::{DatastoreEventObserver, DatastoreMetrics};

/// One operation in a scenario.
#[derive(Clone)]
enum Op {
    Put {
        hash: &'static str,
        name: Option<&'static str>,
        size: u64,
    },
    Get {
        hash: &'static str,
    },
    Delete {
        hash: &'static str,
        size: u64,
    },
}

/// A realistic mixed workload: puts, repeated reads, and a delete that retires
/// one object. No hash is put twice, so the object set is unambiguous.
fn scenario() -> Vec<Op> {
    vec![
        Op::Put { hash: "h-a", name: Some("alpha.bin"), size: 1024 },
        Op::Put { hash: "h-b", name: Some("beta.bin"), size: 2048 },
        Op::Get { hash: "h-a" },
        Op::Get { hash: "h-a" },
        Op::Put { hash: "h-c", name: None, size: 512 },
        Op::Delete { hash: "h-b", size: 2048 },
        Op::Get { hash: "h-c" },
    ]
}

fn apply(metrics: &DatastoreMetrics, script: &[Op]) {
    for op in script {
        match op {
            Op::Put { hash, name, size } => metrics.record_put(hash, *name, *size),
            Op::Get { hash } => metrics.record_get(hash),
            Op::Delete { hash, size } => metrics.record_delete(hash, *size),
        }
    }
}

/// Reference: the steady totals a correct accumulator must report after the
/// script — computed the obvious way, independent of the accumulator.
fn expected_totals(script: &[Op]) -> (u64, u64, u64, u64, u64) {
    let mut objects: BTreeMap<&str, u64> = BTreeMap::new();
    let (mut put, mut get, mut del) = (0u64, 0u64, 0u64);
    for op in script {
        match op {
            Op::Put { hash, size, .. } => {
                objects.insert(hash, *size);
                put += 1;
            }
            Op::Get { .. } => get += 1,
            Op::Delete { hash, .. } => {
                objects.remove(hash);
                del += 1;
            }
        }
    }
    let object_count = objects.len() as u64;
    let total_bytes: u64 = objects.values().sum();
    (object_count, total_bytes, put, get, del)
}

#[test]
fn readout_reports_the_scenarios_steady_totals() {
    let script = scenario();
    let metrics = DatastoreMetrics::new();
    apply(&metrics, &script);

    let got = metrics.readout();
    let (object_count, total_bytes, put_ops, get_ops, delete_ops) = expected_totals(&script);
    assert_eq!(got.object_count, object_count, "object count");
    assert_eq!(got.total_bytes, total_bytes, "total bytes of surviving objects");
    assert_eq!(got.put_ops, put_ops, "put tally");
    assert_eq!(got.get_ops, get_ops, "get tally");
    assert_eq!(got.delete_ops, delete_ops, "delete tally");
}

/// A spy observer recording every event it is handed, as `(kind, hash)`.
#[derive(Default)]
struct Spy {
    events: Mutex<Vec<(String, String)>>,
}

impl DatastoreEventObserver for Spy {
    fn on_event(&self, _ts: u64, kind: &str, hash: &str, _name: Option<&str>, _size: u64) {
        self.events
            .lock()
            .unwrap()
            .push((kind.to_string(), hash.to_string()));
    }
}

#[test]
fn op_events_stream_exactly_the_scenarios_operations() {
    let script = scenario();
    let metrics = DatastoreMetrics::new();
    let spy = Arc::new(Spy::default());
    metrics.set_event_observer(spy.clone());
    apply(&metrics, &script);

    // The event stream must be the script projected to (kind, hash), in order:
    // one event per recorded operation, none dropped, none invented.
    let want: Vec<(String, String)> = script
        .iter()
        .map(|op| match op {
            Op::Put { hash, .. } => ("put".to_string(), hash.to_string()),
            Op::Get { hash } => ("get".to_string(), hash.to_string()),
            Op::Delete { hash, .. } => ("delete".to_string(), hash.to_string()),
        })
        .collect();

    assert_eq!(*spy.events.lock().unwrap(), want);
}
