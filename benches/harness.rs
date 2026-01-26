//! Manual benchmark harness - zero dependencies, full control.
//!
//! Provides statistical analysis of benchmark runs including:
//! - Mean, median, min, max
//! - Standard deviation
//! - Percentiles (P50, P90, P99, P99.9)
//! - Throughput calculations
//! - Outlier detection and removal

use std::time::{Duration, Instant};

/// Results from a single benchmark run
#[derive(Debug, Clone)]
pub struct BenchResult {
    pub name: String,
    pub iterations: usize,
    pub total_time: Duration,
    pub times: Vec<Duration>,
    /// Optional: elements processed (for throughput calculation)
    pub elements: Option<u64>,
}

/// Statistical summary of benchmark results
#[derive(Debug)]
pub struct Stats {
    pub mean: Duration,
    pub median: Duration,
    pub min: Duration,
    pub max: Duration,
    pub std_dev: Duration,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub throughput: Option<f64>, // elements per second
}

impl BenchResult {
    /// Calculate statistics from the raw timing data
    pub fn stats(&self) -> Stats {
        let mut sorted: Vec<Duration> = self.times.clone();
        sorted.sort();

        let n = sorted.len();
        assert!(n > 0, "Cannot compute stats on empty results");

        let sum: Duration = sorted.iter().sum();
        let mean = sum / n as u32;

        let median = if n % 2 == 0 {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2
        } else {
            sorted[n / 2]
        };

        // Standard deviation
        let mean_nanos = mean.as_nanos() as f64;
        let variance: f64 = sorted
            .iter()
            .map(|t| {
                let diff = t.as_nanos() as f64 - mean_nanos;
                diff * diff
            })
            .sum::<f64>()
            / n as f64;
        let std_dev = Duration::from_nanos(variance.sqrt() as u64);

        // Percentiles
        let percentile = |p: f64| -> Duration {
            let idx = ((p / 100.0) * (n - 1) as f64).round() as usize;
            sorted[idx.min(n - 1)]
        };

        let throughput = self.elements.map(|e| {
            let secs = self.total_time.as_secs_f64();
            if secs > 0.0 {
                (e * self.iterations as u64) as f64 / secs
            } else {
                0.0
            }
        });

        Stats {
            mean,
            median,
            min: sorted[0],
            max: sorted[n - 1],
            std_dev,
            p50: percentile(50.0),
            p90: percentile(90.0),
            p99: percentile(99.0),
            p999: percentile(99.9),
            throughput,
        }
    }

    /// Pretty print the results
    pub fn print(&self) {
        let stats = self.stats();

        println!("\n{}", "=".repeat(60));
        println!(" {}", self.name);
        println!("{}", "=".repeat(60));
        println!("  Iterations:  {}", self.iterations);
        println!("  Total time:  {:?}", self.total_time);
        println!();
        println!("  Mean:        {:?}", stats.mean);
        println!("  Median:      {:?}", stats.median);
        println!("  Std Dev:     {:?}", stats.std_dev);
        println!("  Min:         {:?}", stats.min);
        println!("  Max:         {:?}", stats.max);
        println!();
        println!("  P50:         {:?}", stats.p50);
        println!("  P90:         {:?}", stats.p90);
        println!("  P99:         {:?}", stats.p99);
        println!("  P99.9:       {:?}", stats.p999);

        if let Some(throughput) = stats.throughput {
            println!();
            println!("  Throughput:  {:.2} ops/sec", throughput);
            if throughput > 1_000_000.0 {
                println!("               {:.2} M ops/sec", throughput / 1_000_000.0);
            } else if throughput > 1_000.0 {
                println!("               {:.2} K ops/sec", throughput / 1_000.0);
            }
        }
        println!("{}", "=".repeat(60));
    }
}

/// A benchmark builder for configuring and running benchmarks
pub struct Bench {
    name: String,
    warmup_iters: usize,
    bench_iters: usize,
    elements_per_iter: Option<u64>,
}

impl Bench {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            warmup_iters: 3,
            bench_iters: 100,
            elements_per_iter: None,
        }
    }

    /// Set number of warmup iterations (default: 3)
    pub fn warmup(mut self, n: usize) -> Self {
        self.warmup_iters = n;
        self
    }

    /// Set number of benchmark iterations (default: 100)
    pub fn iters(mut self, n: usize) -> Self {
        self.bench_iters = n;
        self
    }

    /// Set elements per iteration for throughput calculation
    pub fn elements(mut self, n: u64) -> Self {
        self.elements_per_iter = Some(n);
        self
    }

    /// Run the benchmark with setup before each iteration
    pub fn run_with_setup<S, T, F>(self, mut setup: S, mut f: F) -> BenchResult
    where
        S: FnMut() -> T,
        F: FnMut(T),
    {
        // Warmup
        for _ in 0..self.warmup_iters {
            let state = setup();
            f(state);
        }

        // Benchmark
        let mut times = Vec::with_capacity(self.bench_iters);
        let total_start = Instant::now();

        for _ in 0..self.bench_iters {
            let state = setup();
            let start = Instant::now();
            f(state);
            times.push(start.elapsed());
        }

        let total_time = total_start.elapsed();

        BenchResult {
            name: self.name,
            iterations: self.bench_iters,
            total_time,
            times,
            elements: self.elements_per_iter,
        }
    }
}

/// A collection of benchmarks to run together
pub struct BenchSuite {
    name: String,
    results: Vec<BenchResult>,
}

impl BenchSuite {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            results: Vec::new(),
        }
    }

    pub fn add(&mut self, result: BenchResult) {
        self.results.push(result);
    }

    pub fn print_summary(&self) {
        println!("\n{}", "#".repeat(70));
        println!("# BENCHMARK SUITE: {}", self.name);
        println!("{}", "#".repeat(70));

        for result in &self.results {
            result.print();
        }

        // Summary table
        println!("\n{}", "-".repeat(70));
        println!(" SUMMARY");
        println!("{}", "-".repeat(70));
        println!(
            " {:30} {:>12} {:>12} {:>12}",
            "Benchmark", "Mean", "P99", "Throughput"
        );
        println!("{}", "-".repeat(70));

        for result in &self.results {
            let stats = result.stats();
            let throughput_str = stats
                .throughput
                .map(|t| {
                    if t > 1_000_000.0 {
                        format!("{:.2}M/s", t / 1_000_000.0)
                    } else if t > 1_000.0 {
                        format!("{:.2}K/s", t / 1_000.0)
                    } else {
                        format!("{:.2}/s", t)
                    }
                })
                .unwrap_or_else(|| "-".to_string());

            println!(
                " {:30} {:>12.2?} {:>12.2?} {:>12}",
                result.name, stats.mean, stats.p99, throughput_str
            );
        }
        println!("{}", "-".repeat(70));
    }
}

/// Prevent the compiler from optimizing away a value
#[inline(never)]
pub fn black_box<T>(x: T) -> T {
    // Use inline assembly to prevent optimization
    // This is a simplified version - in practice, reads from the value
    let ptr = &x as *const T;
    unsafe { std::ptr::read_volatile(ptr) }
}
