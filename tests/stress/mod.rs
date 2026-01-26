//! Stress test utilities and result reporting.
//!
//! Provides a simple framework for stress tests with JSON + pretty output.

#![allow(dead_code)] // Utilities may not all be used in every test

pub mod concurrency;
pub mod saturation;

use std::time::{Duration, Instant};

/// Results from a stress test
#[derive(Debug)]
pub struct StressResult {
    pub name: String,
    pub duration: Duration,
    pub operations: u64,
    pub successes: u64,
    pub failures: u64,
    pub notes: Vec<String>,
}

impl StressResult {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            duration: Duration::ZERO,
            operations: 0,
            successes: 0,
            failures: 0,
            notes: Vec::new(),
        }
    }

    pub fn failure_rate(&self) -> f64 {
        if self.operations == 0 {
            0.0
        } else {
            (self.failures as f64 / self.operations as f64) * 100.0
        }
    }

    pub fn throughput(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.operations as f64 / secs
        } else {
            0.0
        }
    }

    pub fn note(&mut self, msg: impl Into<String>) {
        self.notes.push(msg.into());
    }

    pub fn print(&self) {
        println!("\n{}", "=".repeat(60));
        println!(" STRESS: {}", self.name);
        println!("{}", "=".repeat(60));
        println!("  Duration:     {:?}", self.duration);
        println!("  Operations:   {}", self.operations);
        println!("  Successes:    {}", self.successes);
        println!("  Failures:     {}", self.failures);
        println!("  Failure Rate: {:.2}%", self.failure_rate());
        println!("  Throughput:   {:.2} ops/sec", self.throughput());

        if !self.notes.is_empty() {
            println!();
            println!("  Notes:");
            for note in &self.notes {
                println!("    - {}", note);
            }
        }
        println!("{}", "=".repeat(60));
    }

    pub fn to_json(&self) -> String {
        format!(
            r#"{{"name":"{}","duration_ms":{},"operations":{},"successes":{},"failures":{},"failure_rate_pct":{:.2},"throughput":{:.2},"notes":{:?}}}"#,
            self.name,
            self.duration.as_millis(),
            self.operations,
            self.successes,
            self.failures,
            self.failure_rate(),
            self.throughput(),
            self.notes
        )
    }
}

/// A simple stress test runner
pub struct Stress {
    name: String,
    duration: Option<Duration>,
    iterations: Option<u64>,
}

impl Stress {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            duration: None,
            iterations: None,
        }
    }

    /// Run for a fixed duration
    pub fn for_duration(mut self, d: Duration) -> Self {
        self.duration = Some(d);
        self
    }

    /// Run for a fixed number of iterations
    pub fn for_iterations(mut self, n: u64) -> Self {
        self.iterations = Some(n);
        self
    }

    /// Run the stress test, counting successes and failures
    pub fn run<F>(self, mut f: F) -> StressResult
    where
        F: FnMut() -> bool, // returns true on success, false on failure
    {
        let mut result = StressResult::new(&self.name);
        let start = Instant::now();

        match (self.duration, self.iterations) {
            (Some(duration), _) => {
                while start.elapsed() < duration {
                    if f() {
                        result.successes += 1;
                    } else {
                        result.failures += 1;
                    }
                    result.operations += 1;
                }
            }
            (None, Some(iterations)) => {
                for _ in 0..iterations {
                    if f() {
                        result.successes += 1;
                    } else {
                        result.failures += 1;
                    }
                    result.operations += 1;
                }
            }
            (None, None) => {
                // Default: 1000 iterations
                for _ in 0..1000 {
                    if f() {
                        result.successes += 1;
                    } else {
                        result.failures += 1;
                    }
                    result.operations += 1;
                }
            }
        }

        result.duration = start.elapsed();
        result
    }
}

// Test actors used across stress tests
use swactor::{actor::ActorInterface, runtime::Runtime};

/// An actor that just absorbs messages
pub struct BlackHole;

#[derive(Clone)]
pub struct Msg;

impl ActorInterface for BlackHole {
    type Incoming = Msg;
    type Response = ();
    fn handle(&mut self, _ctx: &Runtime, _msg: Msg) {}
}

/// An actor that counts messages received
pub struct Counter {
    pub count: usize,
}

impl Counter {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl ActorInterface for Counter {
    type Incoming = Msg;
    type Response = ();
    fn handle(&mut self, _ctx: &Runtime, _msg: Msg) {
        self.count += 1;
    }
}
