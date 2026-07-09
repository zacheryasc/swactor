#![recursion_limit = "256"]

use std::path::Path;
use std::process::{Command, ExitCode};
use std::time::{Duration, Instant};

#[path = "support/local_e2e_cluster.rs"]
mod local_e2e_cluster;

const IMAGE: &str = "swactor-mvp-local-e2e-cluster:latest";
const SKIP_BUILD_ENV: &str = "MVP_LOCAL_E2E_CLUSTER_SKIP_BUILD";
const BUILD_ONLY_ENV: &str = "MVP_LOCAL_E2E_CLUSTER_BUILD_ONLY";

fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    match std::env::var("MVP_TEST_ROLE").ok().as_deref() {
        Some("cluster-supervisor" | "cluster-relay") => return local_e2e_cluster::run_main(),
        Some(role) => {
            eprintln!("unknown MVP_TEST_ROLE={role}");
            return ExitCode::from(2);
        }
        None => {}
    }

    if args.iter().any(|arg| arg == "--role=node") {
        return local_e2e_cluster::run_main();
    }

    local_e2e_cluster_docker_cpu_pipeline_prompt();
    ExitCode::SUCCESS
}

fn local_e2e_cluster_docker_cpu_pipeline_prompt() {
    if std::env::var_os("MVP_SYSTEM_LOCAL_E2E_CLUSTER").is_none() {
        eprintln!("skipping; set MVP_SYSTEM_LOCAL_E2E_CLUSTER=1 to run Docker CPU cluster e2e");
        return;
    }
    if !Path::new("/var/run/docker.sock").exists() {
        eprintln!(
            "skipping; /var/run/docker.sock is required for the relay-only Docker cluster e2e"
        );
        return;
    }

    build_docker_fixture();
    if std::env::var_os(BUILD_ONLY_ENV).is_some() {
        return;
    }

    let docker = DockerRelayFixture::start();
    let output = docker.run_supervisor("ping");

    assert!(
        output.status.success(),
        "mvp-local-e2e-cluster failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json stdout");
    assert_eq!(value["ok"], true);
    assert_eq!(value["actor_plane"], "iroh-swactor");
    assert_eq!(value["data_plane"], "iroh-quic-persistent-edge-streams");
    assert_eq!(
        value["edge_protocol"],
        "edge-id-preamble-mo01-object-records"
    );
    assert_eq!(
        value["node_local_data_plane"],
        "arena-backed-rings-json-metadata-only"
    );
    assert_eq!(
        value["worker_processes"],
        "docker-tinygrad-cpu-worker-per-node"
    );
    assert_eq!(value["tinygrad_device"], "CPU");
    assert_eq!(value["prompt_text"], "ping");
    assert_eq!(value["response_text"], "pong");
    assert_eq!(value["response_tokens"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        value["engine_builder_pattern"],
        "host-coordinator-static-topology-docker-workers"
    );
    assert_eq!(value["engine_builder_node_count"], 3);
    assert_eq!(value["engine_builder_stage_assignments"], 2);
    assert_eq!(value["injected_prompt_observed"], true);
    assert_eq!(value["token_received_observed"], true);
    assert_eq!(value["run_completed_observed"], true);
    assert_eq!(value["run_torn_down_observed"], true);
    assert_eq!(value["stop_sent_to_all_nodes"], true);
    assert_eq!(value["stage_ready_stdout_count"], 2);
    assert_eq!(value["provisioned_node_count"], 2);
    assert_eq!(value["provision_node_live_count"], 2);
    assert!(
        value["provision_stdout_line_count"]
            .as_u64()
            .is_some_and(|count| count >= 2),
        "{value}"
    );
    assert!(
        value["provision_stderr_line_count"]
            .as_u64()
            .is_some_and(|count| count >= 2),
        "{value}"
    );
    assert_eq!(value["provision_nodes_stopped"], true);
    assert_eq!(value["relay_only"], true, "{value}");
    assert_eq!(value["relay_url"], "http://relay:7843/");
    assert_eq!(value["orchestrator_endpoint_has_relay"], true, "{value}");
    assert_eq!(
        value["orchestrator_endpoint_relay_url"],
        "http://relay:7843/"
    );
    assert_eq!(value["node0_endpoint_has_relay"], true, "{value}");
    assert_eq!(value["node0_endpoint_relay_url"], "http://relay:7843/");
    assert_eq!(value["node1_endpoint_has_relay"], true, "{value}");
    assert_eq!(value["node1_endpoint_relay_url"], "http://relay:7843/");
    assert!(
        value["node0_endpoint"]["addrs"]
            .as_array()
            .is_some_and(|addrs| !addrs.is_empty()),
        "{value}"
    );
    assert!(
        value["node1_endpoint"]["addrs"]
            .as_array()
            .is_some_and(|addrs| !addrs.is_empty()),
        "{value}"
    );
}
struct DockerRelayFixture {
    relay_container: String,
    supervisor_network: String,
    node0_network: String,
    node1_network: String,
}

impl DockerRelayFixture {
    fn start() -> Self {
        let suffix = format!("{}-{}", std::process::id(), unique_nanos());
        let relay_container = format!("mvp-local-e2e-relay-{suffix}");
        let supervisor_network = format!("mvp-local-e2e-supervisor-{suffix}");
        let node0_network = format!("mvp-local-e2e-node0-{suffix}");
        let node1_network = format!("mvp-local-e2e-node1-{suffix}");
        for network in [&supervisor_network, &node0_network, &node1_network] {
            docker_status(
                ["network", "create", network],
                "create relay-only Docker network",
            );
        }
        docker_status(
            [
                "run",
                "-d",
                "--rm",
                "--name",
                &relay_container,
                "--network",
                &supervisor_network,
                "--network-alias",
                "relay",
                "-e",
                "MVP_TEST_ROLE=cluster-relay",
                "-e",
                "MVP_LOCAL_E2E_RELAY_LISTEN=0.0.0.0:7843",
                IMAGE,
            ],
            "start relay sidecar",
        );
        docker_status(
            [
                "network",
                "connect",
                "--alias",
                "relay",
                &node0_network,
                &relay_container,
            ],
            "attach relay to node0 network",
        );
        docker_status(
            [
                "network",
                "connect",
                "--alias",
                "relay",
                &node1_network,
                &relay_container,
            ],
            "attach relay to node1 network",
        );
        let fixture = Self {
            relay_container,
            supervisor_network,
            node0_network,
            node1_network,
        };
        fixture.wait_for_relay();
        fixture
    }

    fn run_supervisor(&self, prompt: &str) -> std::process::Output {
        Command::new("docker")
            .args([
                "run",
                "--rm",
                "--name",
                &format!("mvp-local-e2e-supervisor-{}", unique_nanos()),
                "--network",
                &self.supervisor_network,
                "--network-alias",
                "supervisor",
                "-v",
                "/var/run/docker.sock:/var/run/docker.sock",
                "-e",
                "MVP_TEST_ROLE=cluster-supervisor",
                "-e",
                &format!("MVP_LOCAL_E2E_CLUSTER_IMAGE={IMAGE}"),
                "-e",
                &format!("MVP_LOCAL_E2E_DOCKER_NETWORK_NODE0={}", self.node0_network),
                "-e",
                &format!("MVP_LOCAL_E2E_DOCKER_NETWORK_NODE1={}", self.node1_network),
                "-e",
                "MVP_IROH_RELAY_MODE=default",
                "-e",
                "MVP_IROH_RELAY_URL=http://relay:7843/",
                IMAGE,
                "--prompt",
                prompt,
            ])
            .output()
            .expect("run relay-only local e2e cluster supervisor")
    }

    fn wait_for_relay(&self) {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(20) {
            let logs = Command::new("docker")
                .args(["logs", &self.relay_container])
                .output()
                .expect("read relay sidecar logs");
            let stdout = String::from_utf8_lossy(&logs.stdout);
            let stderr = String::from_utf8_lossy(&logs.stderr);
            if stdout.contains("relay ready") || stderr.contains("relay ready") {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("relay sidecar did not report ready within 20s");
    }
}

impl Drop for DockerRelayFixture {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["stop", "-t", "2", &self.relay_container])
            .status();
        for network in [
            &self.node1_network,
            &self.node0_network,
            &self.supervisor_network,
        ] {
            let _ = Command::new("docker")
                .args(["network", "rm", network])
                .status();
        }
    }
}

fn docker_status<const N: usize>(args: [&str; N], action: &str) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .unwrap_or_else(|error| panic!("{action}: {error}"));
    assert!(status.success(), "{action} failed with status {status}");
}

fn unique_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos()
}

fn build_docker_fixture() {
    if std::env::var_os(SKIP_BUILD_ENV).is_some() {
        phase("using existing Docker CPU cluster fixture image");
        return;
    }

    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .canonicalize()
        .expect("canonical workspace root");
    let context = std::env::temp_dir().join(format!(
        "mvp-system-local-e2e-cluster-context-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&context);
    copy_workspace_context(&workspace, &context);
    let dockerfile = context.join("crates/mvp-system/tests/local_e2e_cluster/Dockerfile");

    phase("building Docker CPU cluster fixture image");
    let build = Command::new("docker")
        .args(["build", "-f"])
        .arg(&dockerfile)
        .args(["-t", IMAGE])
        .arg(&context)
        .status()
        .expect("run docker build");
    assert!(build.success(), "docker build failed with status {build}");
}

fn copy_workspace_context(source: &Path, dest: &Path) {
    std::fs::create_dir_all(dest).expect("create docker context");
    for entry in std::fs::read_dir(source).expect("read workspace") {
        let entry = entry.expect("read workspace entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if matches!(name.as_ref(), ".git" | "target" | ".dockerignore") {
            continue;
        }
        copy_context_entry(&entry.path(), &dest.join(name.as_ref()));
    }
}

fn copy_context_entry(source: &Path, dest: &Path) {
    let metadata = std::fs::symlink_metadata(source).expect("context metadata");
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(dest).expect("create context dir");
        for entry in std::fs::read_dir(source).expect("read context dir") {
            let entry = entry.expect("read context entry");
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(name.as_ref(), ".git" | "target" | ".dockerignore") {
                continue;
            }
            copy_context_entry(&entry.path(), &dest.join(name.as_ref()));
        }
    } else if metadata.is_file() {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).expect("create context parent");
        }
        std::fs::copy(source, dest).expect("copy context file");
    }
}

fn phase(message: &str) {
    eprintln!("local-e2e-cluster: {message}");
}
