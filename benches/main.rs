//! Swactor Benchmark Suite
//!
//! A manual benchmark harness for measuring runtime performance.
//! Zero external dependencies - just std::time.
//!
//! Run with: cargo run --bin bench --release
//!
//! Options:
//!   --throughput    Run throughput benchmarks only
//!   --scaling       Run scaling benchmarks only
//!   --all           Run all benchmarks (default)

mod harness;
mod throughput;
mod scaling;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    
    println!("============================================================");
    println!("  SWACTOR BENCHMARK SUITE");
    println!("============================================================");
    println!();
    
    // Parse arguments
    let run_throughput = args.contains(&"--throughput".to_string()) 
        || args.contains(&"--all".to_string()) 
        || args.len() == 1;
    let run_scaling = args.contains(&"--scaling".to_string()) 
        || args.contains(&"--all".to_string()) 
        || args.len() == 1;
    
    if run_throughput {
        let suite = throughput::run_all();
        suite.print_summary();
    }
    
    if run_scaling {
        let suite = scaling::run_all();
        suite.print_summary();
    }
    
    println!("\nBenchmarks complete.");
}
