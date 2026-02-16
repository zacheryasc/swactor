//! local-runner — single-machine CI runner for the Thinkpad.
//!
//! Receives Forgejo webhooks, queues pipelines, and executes jobs
//! one at a time for benchmark isolation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use clap::Parser;

use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use runtime_dashboard::ci_collector::{
    CiSnapshot, CiStatsProvider, JobSnapshot, PipelineSnapshot, ProvisionerStatus,
};

use swactor_ci::local_coordinator::{LocalCiSnapshot, LocalCoordinator, LocalCoordinatorMsg};
use swactor_ci::pipeline::PipelineExecution;
use swactor_ci::status_reporter::StatusReporter;
use swactor_ci::webhook_server;
use swactor_ci::yaml;
use swactor_ci::{CiConfig, LocalCiConfig};

#[derive(Parser)]
#[command(name = "local-runner", about = "Swactor local CI runner")]
struct Args {
    /// Webhook listen port.
    #[arg(long, default_value = "8787")]
    port: u16,

    /// Forgejo instance URL.
    #[arg(long, default_value = "")]
    forgejo_url: String,

    /// Forgejo API token.
    #[arg(long, default_value = "")]
    forgejo_token: String,

    /// Webhook secret for HMAC verification (empty to skip).
    #[arg(long, default_value = "")]
    secret: String,

    /// Path to .ci.yml file.
    #[arg(long, default_value = ".ci.yml")]
    yaml: String,

    /// Base directory for git checkouts.
    #[arg(long, default_value = "./ci-work")]
    work_dir: String,

    /// Git clone URL for the repository.
    #[arg(long, default_value = "")]
    repo_url: String,

    /// Dashboard HTTP port (omit to disable).
    #[arg(long)]
    dashboard_port: Option<u16>,

    /// Iroh Node ID of the ci-relay on the VPS (hex).
    /// When set, webhooks arrive via iroh instead of HTTP.
    #[arg(long)]
    relay_node_id: Option<String>,
}

/// Bridge from LocalCiSnapshot to CiSnapshot for the dashboard.
struct LocalCiSnapshotProvider {
    snapshot: Arc<Mutex<LocalCiSnapshot>>,
}

impl CiStatsProvider for LocalCiSnapshotProvider {
    fn snapshot(&self) -> CiSnapshot {
        let local = self.snapshot.lock().unwrap().clone();
        CiSnapshot {
            active_pipelines: local
                .active_pipelines
                .iter()
                .map(pipeline_to_dashboard)
                .collect(),
            recent_pipelines: local
                .recent_pipelines
                .iter()
                .map(pipeline_to_dashboard)
                .collect(),
            provisioner_status: ProvisionerStatus::Online,
            active_instances: if local.has_running_job { 1 } else { 0 },
        }
    }
}

fn pipeline_to_dashboard(p: &PipelineExecution) -> PipelineSnapshot {
    PipelineSnapshot {
        pipeline_id: p.pipeline_id,
        pipeline_name: p.pipeline_name.clone(),
        repo_owner: p.repo_owner.clone(),
        repo_name: p.repo_name.clone(),
        commit_sha: p.commit_sha.clone(),
        branch: p.branch.clone(),
        status: p.status.clone(),
        jobs: p
            .jobs
            .values()
            .map(|j| JobSnapshot {
                job_id: j.job_id.clone(),
                job_name: j.definition.name.clone(),
                status: j.status.clone(),
                output_line_count: j.output_lines.len(),
            })
            .collect(),
    }
}

fn main() {
    let args = Args::parse();
    let stop = Arc::new(AtomicBool::new(false));

    // Signal handler.
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || {
            stop.store(true, Ordering::Relaxed);
        })
        .expect("failed to set signal handler");
    }

    // Optionally start dashboard.
    let dash = args.dashboard_port.map(|port| {
        let d = runtime_dashboard::start_dashboard(runtime_dashboard::DashboardConfig {
            port,
            ..Default::default()
        });
        d.install_tracing();
        d
    });

    // Create 2-thread runtime.
    let num_threads = 2;
    let collector = runtime_dashboard::collector::StatsCollector::new(num_threads);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads,
        max_actors: 256,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    // Build config.
    let ci_config = CiConfig {
        webhook_port: args.port,
        webhook_secret: args.secret.clone(),
        forgejo_url: args.forgejo_url.clone(),
        forgejo_token: args.forgejo_token.clone(),
        data_dir: args.work_dir.clone(),
    };
    let local_config = LocalCiConfig {
        ci: ci_config,
        repo_url: args.repo_url.clone(),
        work_dir: args.work_dir.clone(),
        ci_yaml_path: args.yaml.clone(),
    };

    // Spawn StatusReporter.
    let reporter_addr = rt
        .spawn(StatusReporter::new())
        .expect("failed to spawn StatusReporter");

    // Spawn LocalCoordinator.
    let coordinator = LocalCoordinator::new(local_config).with_status_reporter(reporter_addr);
    let ci_snapshot = coordinator.ci_snapshot();
    let coordinator_addr = rt
        .spawn(coordinator)
        .expect("failed to spawn LocalCoordinator");

    // Load CI YAML from disk.
    let yaml_content = std::fs::read_to_string(&args.yaml)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", args.yaml));
    let ci_yaml = yaml::parse_ci_yaml(&yaml_content)
        .unwrap_or_else(|e| panic!("failed to parse CI YAML: {e}"));

    // Start runtime.
    let handle = rt.run().expect("failed to start runtime");

    // Send CiYaml to coordinator.
    let _ = handle
        .runtime
        .send_to(coordinator_addr, LocalCoordinatorMsg::SetCiYaml(ci_yaml));

    // Wire dashboard.
    if let Some(ref d) = dash {
        d.set_runtime(Arc::clone(&handle.runtime), collector);
        let provider = Arc::new(LocalCiSnapshotProvider {
            snapshot: ci_snapshot,
        });
        d.set_ci(provider);
    }

    // Start webhook source: iroh relay or HTTP listener.
    if let Some(ref relay_id_hex) = args.relay_node_id {
        start_iroh_receiver(
            relay_id_hex,
            Arc::clone(&handle.runtime),
            coordinator_addr,
            Arc::clone(&stop),
        );
    } else {
        let _webhook_handle = webhook_server::start_webhook_listener(
            args.port,
            args.secret,
            Arc::clone(&handle.runtime),
            coordinator_addr,
        );
    }

    eprintln!("Local CI runner started");
    if args.relay_node_id.is_some() {
        eprintln!("  Webhook: via iroh relay");
    } else {
        eprintln!("  Webhook: http://0.0.0.0:{}", args.port);
    }
    eprintln!("  YAML:    {}", args.yaml);
    eprintln!("  Workdir: {}", args.work_dir);
    if let Some(port) = args.dashboard_port {
        eprintln!("  Dashboard: http://0.0.0.0:{port}");
    }

    // Main loop — just wait for ctrlc.
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }

    eprintln!("\nShutting down...");
    handle.shutdown();
    if let Some(d) = dash {
        d.shutdown();
    }
    handle.join();
}

// ─── Iroh Webhook Receiver ──────────────────────────────────────────────────

/// ALPN protocol identifier — must match ci-relay.
const CI_ALPN: &[u8] = b"swactor/ci/1";

/// Connect to the VPS ci-relay via iroh and receive WebhookEvents.
///
/// Runs in a background thread with its own tokio runtime.
fn start_iroh_receiver(
    relay_id_hex: &str,
    swactor_rt: Arc<Runtime>,
    coordinator_addr: ActorAddress,
    stop: Arc<AtomicBool>,
) {
    let relay_key: iroh::PublicKey = relay_id_hex
        .parse()
        .unwrap_or_else(|e| panic!("invalid relay node ID '{relay_id_hex}': {e}"));

    thread::Builder::new()
        .name("iroh-receiver".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for iroh receiver");

            rt.block_on(async move {
                let endpoint = iroh::Endpoint::builder()
                    .alpns(vec![CI_ALPN.to_vec()])
                    .relay_mode(iroh::RelayMode::Default)
                    .bind()
                    .await
                    .expect("failed to bind iroh endpoint");

                eprintln!("  Iroh local ID: {}", endpoint.id());

                // Outer reconnection loop: reconnect when the connection drops.
                while !stop.load(Ordering::Relaxed) {
                    eprintln!("  Connecting to relay {relay_key}...");

                    let conn = match endpoint.connect(relay_key, CI_ALPN).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("iroh: connect failed: {e}, retrying in 5s...");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    };

                    eprintln!("  Connected to relay!");

                    // Receive loop: the relay opens uni streams to send us events.
                    while !stop.load(Ordering::Relaxed) {
                        match tokio::time::timeout(Duration::from_secs(1), conn.accept_uni()).await
                        {
                            Ok(Ok(mut recv)) => {
                                match read_tagged_message(&mut recv).await {
                                    Ok((tag, payload)) => {
                                        if tag == "ci::WebhookEvent" {
                                            match serde_json::from_slice::<
                                                swactor_ci::WebhookEvent,
                                            >(
                                                &payload
                                            ) {
                                                Ok(event) => {
                                                    eprintln!(
                                                        "iroh: received webhook {} on {}",
                                                        event
                                                            .commit_sha
                                                            .get(..8)
                                                            .unwrap_or(&event.commit_sha),
                                                        event.branch,
                                                    );
                                                    let _ = swactor_rt.send_to(
                                                        coordinator_addr,
                                                        LocalCoordinatorMsg::Webhook(event),
                                                    );
                                                }
                                                Err(e) => {
                                                    eprintln!(
                                                        "iroh: failed to deserialize event: {e}"
                                                    )
                                                }
                                            }
                                        } else {
                                            eprintln!("iroh: unknown tag '{tag}', ignoring");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("iroh: read error: {e}");
                                        break;
                                    }
                                }
                            }
                            Ok(Err(e)) => {
                                eprintln!("iroh: connection lost: {e}, reconnecting...");
                                break;
                            }
                            Err(_) => {
                                // 1s poll timeout — just loop and check stop flag.
                            }
                        }
                    }
                }

                endpoint.close().await;
            });
        })
        .expect("failed to spawn iroh-receiver thread");
}

/// Read a tagged message from a QUIC recv stream.
///
/// Frame format: `[4B tag_len][tag_bytes][payload_bytes]`
async fn read_tagged_message(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
    let mut tag_len_buf = [0u8; 4];
    recv.read_exact(&mut tag_len_buf).await?;
    let tag_len = u32::from_be_bytes(tag_len_buf) as usize;

    if tag_len > 1024 {
        return Err("tag too large".into());
    }

    let mut tag_buf = vec![0u8; tag_len];
    recv.read_exact(&mut tag_buf).await?;
    let tag = String::from_utf8(tag_buf)?;

    let payload = recv.read_to_end(64 * 1024).await?;

    Ok((tag, payload))
}
