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
        assert_eq!(telemetry_names.len(), 42);
        assert!(codec_names.contains(&"codec/direct/fixed/encode/8b".to_owned()));
        assert!(codec_names.contains(&"codec/registry/json-nested/receive/65536b".to_owned()));
        assert!(
            telemetry_names.contains(&"telemetry/mux/submit-drain/65536b/batch-1024".to_owned())
        );
        assert!(
            telemetry_names.contains(&"telemetry/mux/concurrent/producers-8/total-4096".to_owned())
        );
        assert!(telemetry_names.contains(
            &"telemetry/engine/multithread/channels-256/producers-8/total-16384".to_owned()
        ));
        assert!(
            telemetry_names.contains(
                &"telemetry/wire/subscription-batch/channels-256/frames-16384".to_owned()
            )
        );
        assert!(
            telemetry_names
                .contains(&"telemetry/payload-codec/messagepack/encode/records-4096".to_owned())
        );
        assert!(
            telemetry_names.contains(&"telemetry/wire/compression/zstd/frames-16384".to_owned())
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
    fn representative_telemetry_bandwidth_is_bounded() {
        let (payload_bytes, wire_bytes) = telemetry::representative_wire_sizes();
        println!("telemetry payload_bytes={payload_bytes} wire_bytes={wire_bytes}");
        assert!(wire_bytes < payload_bytes / 4);
    }

    #[test]
    fn binary_payload_codec_sizes_beat_json() {
        let sizes = telemetry::representative_payload_codec_sizes();
        println!("telemetry payload codec sizes: {sizes:?}");
        let size = |name| {
            sizes
                .iter()
                .find_map(|(codec, bytes)| (*codec == name).then_some(*bytes))
                .expect("codec size")
        };
        assert!(size("cbor") < size("json"));
        assert!(size("messagepack") < size("json"));
        assert!(size("messagepack") < size("cbor"));
    }

    #[test]
    fn binary_record_codecs_reduce_compressed_wire_bytes() {
        let sizes = telemetry::representative_wire_codec_sizes();
        println!("telemetry wire codec sizes: {sizes:?}");
        let wire_size = |name| {
            sizes
                .iter()
                .find_map(|(codec, _, wire_bytes)| (*codec == name).then_some(*wire_bytes))
                .expect("wire codec size")
        };
        assert!(wire_size("cbor") < wire_size("json"));
        assert!(wire_size("messagepack") < wire_size("json"));
        assert!(wire_size("messagepack") < wire_size("cbor"));
    }

    #[test]
    fn compression_candidates_preserve_bytes_and_report_size() {
        let sizes = telemetry::representative_compression_sizes();
        println!("telemetry compression sizes: {sizes:?}");
        assert!(sizes.iter().all(|(_, raw, compressed)| compressed < raw));
        let compressed_size = |name| {
            sizes
                .iter()
                .find_map(|(codec, _, bytes)| (*codec == name).then_some(*bytes))
                .expect("compression size")
        };
        assert!(compressed_size("zstd") < compressed_size("zstd-fast"));
        assert!(compressed_size("zstd-fast") < compressed_size("lz4"));
    }

    #[test]
    fn benchmark_commands_name_the_package() {
        assert!(BENCHMARK_COMMAND.contains("-p swactor-benchmarks"));
        assert!(INITIAL_BASELINE_COMMAND.contains("--save-baseline initial"));
    }
}
