use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

#[path = "support/local_e2e_cluster.rs"]
mod local_e2e_cluster;

const IMAGE: &str = "swactor-mvp-local-e2e-cluster:latest";

fn main() -> ExitCode {
    let args = std::env::args().collect::<Vec<_>>();
    match std::env::var("MVP_TEST_ROLE").ok().as_deref() {
        Some("cluster-supervisor") => return local_e2e_cluster::run_main(),
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

    build_docker_fixture();

    let output = Command::new(current_test_exe())
        .env("MVP_TEST_ROLE", "cluster-supervisor")
        .arg("--prompt")
        .arg("ping")
        .env("MVP_LOCAL_E2E_CLUSTER_IMAGE", IMAGE)
        .output()
        .expect("run local e2e cluster supervisor");

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

fn build_docker_fixture() {
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

fn current_test_exe() -> PathBuf {
    std::env::current_exe().expect("current test exe")
}
