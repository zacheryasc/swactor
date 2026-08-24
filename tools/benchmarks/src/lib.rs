pub mod codec;
pub mod telemetry;
pub mod workload;

pub use workload::{SetupPolicy, WorkUnits, Workload, validate};

pub const BENCHMARK_COMMAND: &str = "cargo bench -p swactor-benchmarks --bench criterion";
pub const INITIAL_BASELINE_COMMAND: &str =
    "cargo bench -p swactor-benchmarks --bench criterion -- --save-baseline initial";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_workload_passes_its_correctness_check() {
        for workload in codec::workloads() {
            validate(&workload);
        }
        for workload in telemetry::workloads() {
            validate(&workload);
        }
    }

    #[test]
    fn benchmark_names_are_unique_and_stable() {
        let codec_names: Vec<String> = codec::workloads()
            .into_iter()
            .map(|workload| workload.name())
            .collect();
        let telemetry_names: Vec<String> = telemetry::workloads()
            .into_iter()
            .map(|workload| workload.name())
            .collect();
        assert_eq!(codec_names.len(), 53);
        assert_eq!(telemetry_names.len(), 31);
        assert!(codec_names.contains(&"codec/direct/fixed/encode/8b".to_owned()));
        assert!(codec_names.contains(&"codec/registry/json-nested/receive/65536b".to_owned()));
        assert!(
            telemetry_names.contains(&"telemetry/mux/submit-drain/65536b/batch-1024".to_owned())
        );
        assert!(
            telemetry_names.contains(&"telemetry/mux/concurrent/producers-8/total-4096".to_owned())
        );

        let mut names = codec_names;
        names.extend(telemetry_names);
        let total = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), total);
        assert!(names.iter().all(|name| !name.contains(char::is_whitespace)));
    }

    #[test]
    fn benchmark_commands_name_the_package() {
        assert!(BENCHMARK_COMMAND.contains("-p swactor-benchmarks"));
        assert!(INITIAL_BASELINE_COMMAND.contains("--save-baseline initial"));
    }
}
