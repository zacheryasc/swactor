use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

pub const BENCHMARK_SCHEMA: u64 = 1;

static BENCHMARK_START: OnceLock<Instant> = OnceLock::new();
static BENCHMARK_SEQ: AtomicU64 = AtomicU64::new(1);

pub fn unix_ms_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub fn stamp(component: &'static str) -> Value {
    let start = BENCHMARK_START.get_or_init(Instant::now);
    let mono_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    let seq = BENCHMARK_SEQ.fetch_add(1, Ordering::Relaxed);
    json!({
        "schema": BENCHMARK_SCHEMA,
        "component": component,
        "pid": std::process::id(),
        "seq": seq,
        "wall_unix_ms": unix_ms_now(),
        "mono_ms": mono_ms,
    })
}
