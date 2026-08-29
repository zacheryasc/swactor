use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use crate::DEFAULT_PIPELINE_CACHED_MODEL_FILE;
use crate::codecs::register_myelin_actor_codecs;
use crate::observability::frame_collector::FrameCollector;
use crate::observability::orch_telemetry::{
    BootstrapEmission, DashboardSupport, MYELIN_SWIM_MEMBERSHIP, OrchTelemetry,
};
use crate::orchestration::actor::{
    ContextualArtifactCleanup, OrchestratorActor, OrchestratorMsg, OrchestratorReport,
};
use crate::orchestration::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
use crate::orchestration::control;
use crate::orchestration::daemon;
use crate::orchestration::manual_control::{
    CONTROL_REGISTRY_NAME, ConfigValidator, ManualActorControl, ManualActorControlConfig,
    ManualControl, ManualControlMsg, ManualControlReply, NodePhase, OfferDto, OfferSearchRequest,
    OfferSearcher, ProviderConfigurationRequest, ProviderFactory, ProviderReadiness, SpecBuilder,
};

use crate::node_provisioning::{ProviderKind, provider_kind};
use crate::orchestration::distribution_stack::{DistributionRuntimeStack, duration_ms_u64};
use crate::orchestration::provider_adapters::relay::{
    MYELIN_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV,
    relay_mode_env_value, relay_runtime_config_from_settings,
};
use crate::orchestration::provider_adapters::vastai::{
    BootstrapEnvSource, SshCommandBootstrapLauncher, ToolsVastAiLeaseClient,
    VastAiProvisioningConfig, VastAiProvisioningPlugin,
};
use crate::provisioning::{
    LocalDockerPlugin, LocalProcessPlugin, MockVastAiPlugin, NodeProvisionSpec, PluginObservation,
    PluginObservationSink, PluginSink, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use crate::run_fsm::{RunConfig, RunId};
use crate::run_plan::{GgufSource, TokenizerSource};
use distribution::node::DistributedNodeConfig;
use distribution::registry_actor::RegistryIn;
use distribution::swim::telemetry::ObservedTransition;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{EDGE_ALPN, IrohDriver, IrohDriverConfig, TELEMETRY_ALPN};
use iroh_driver::{EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Inbox, Runtime};
use swactor_engine::{ActorCompletion, Engine, EngineHandle, TokioBackend, TokioConfig};
const DEFAULT_IMAGE: &str = "myelin-node:latest";
const MYELIN_RUNTIME_CONFIG_ENV: &str = "MYELIN_RUNTIME_CONFIG";
const CACHED_MODEL_HOST_ENV: &str = "MYELIN_CACHED_MODEL_HOST_PATH";
const MYELIN_WORKER_BIN_ENV: &str = "MYELIN_WORKER_BIN";
const CACHED_MODEL_CONTAINER_DIR: &str = "/models/cached";
const DEFAULT_PIPELINE_MODEL_CACHE_DIR: &str = ".model-cache";
const DEFAULT_HF_REPO: &str = "bartowski/Llama-3.2-1B-Instruct-GGUF";
const DEFAULT_HF_FILE: &str = "Llama-3.2-1B-Instruct-Q4_K_M.gguf";
const DEFAULT_MODEL_ID: &str = "llama-3.2-1b-instruct-q4";
const DEFAULT_STATE_DIR: &str = "./.config";
const DEFAULT_MAX_TOKENS: u32 = 64;
const PUMP_INTERVAL: Duration = Duration::from_millis(10);
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);
const TELEMETRY_FRAME_LOG_ENV: &str = "MYELIN_TELEMETRY_FRAME_LOG";

fn sample_orchestrator_cpu(
    mut sampler: telemetry::hardware::cpu::CpuSampler,
    seq: u64,
) -> (
    telemetry::hardware::cpu::CpuSampler,
    telemetry::hardware::cpu::HostCpuSample,
) {
    let sample = sampler.sample(seq);
    (sampler, sample)
}

fn sample_orchestrator_gpu((): (), seq: u64) -> ((), telemetry::hardware::gpu::HostGpuSample) {
    ((), telemetry::hardware::gpu::sample(seq))
}

fn sample_orchestrator_memory(
    (): (),
    seq: u64,
) -> ((), telemetry::hardware::memory::HostMemorySample) {
    ((), telemetry::hardware::memory::sample(seq))
}

fn sample_orchestrator_net((): (), seq: u64) -> ((), telemetry::hardware::net::HostNetSample) {
    ((), telemetry::hardware::net::sample(seq))
}

fn sample_orchestrator_storage(
    (): (),
    seq: u64,
) -> ((), telemetry::hardware::storage::HostStorageSample) {
    ((), telemetry::hardware::storage::sample(seq))
}

fn spawn_orchestrator_hardware_samplers(engine: &EngineHandle, telemetry: &mut OrchTelemetry) {
    let cpu_channel = telemetry.record_channel::<telemetry::hardware::cpu::HostCpuSample>();
    let cpu_producer = telemetry.producer();
    telemetry::hardware::spawn_blocking_sampler(
        engine.clone(),
        telemetry::hardware::cpu::CPU_SAMPLE_INTERVAL,
        telemetry::hardware::cpu::CpuSampler::new([std::process::id()]),
        sample_orchestrator_cpu,
        || {},
        move |_, sample| {
            cpu_producer.submit_record(cpu_channel, &sample);
        },
    );

    let gpu_channel = telemetry.record_channel::<telemetry::hardware::gpu::HostGpuSample>();
    let gpu_producer = telemetry.producer();
    telemetry::hardware::spawn_blocking_sampler(
        engine.clone(),
        telemetry::hardware::gpu::GPU_SAMPLE_INTERVAL,
        (),
        sample_orchestrator_gpu,
        || {},
        move |_, sample| {
            gpu_producer.submit_record(gpu_channel, &sample);
        },
    );

    let memory_channel =
        telemetry.record_channel::<telemetry::hardware::memory::HostMemorySample>();
    let memory_producer = telemetry.producer();
    telemetry::hardware::spawn_blocking_sampler(
        engine.clone(),
        telemetry::hardware::memory::MEMORY_SAMPLE_INTERVAL,
        (),
        sample_orchestrator_memory,
        || {},
        move |_, sample| {
            memory_producer.submit_record(memory_channel, &sample);
        },
    );

    let net_channel = telemetry.record_channel::<telemetry::hardware::net::HostNetSample>();
    let net_producer = telemetry.producer();
    telemetry::hardware::spawn_blocking_sampler(
        engine.clone(),
        telemetry::hardware::net::HOST_NET_SAMPLE_INTERVAL,
        (),
        sample_orchestrator_net,
        || {},
        move |_, sample| {
            net_producer.submit_record(net_channel, &sample);
        },
    );

    let storage_channel =
        telemetry.record_channel::<telemetry::hardware::storage::HostStorageSample>();
    let storage_producer = telemetry.producer();
    telemetry::hardware::spawn_blocking_sampler(
        engine.clone(),
        telemetry::hardware::storage::STORAGE_SAMPLE_INTERVAL,
        (),
        sample_orchestrator_storage,
        || {},
        move |_, sample| {
            storage_producer.submit_record(storage_channel, &sample);
        },
    );
}

#[cfg(test)]
mod hardware_telemetry_tests {
    use std::time::{Duration, Instant};

    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;

    use super::*;

    #[test]
    fn orchestrator_emits_all_host_hardware_channels() {
        let frame_log = tempfile::NamedTempFile::new().expect("frame log");
        let mut telemetry =
            OrchTelemetry::new(1, Some(frame_log.path())).expect("orchestrator telemetry");
        let engine = Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
        )
        .expect("engine");
        spawn_orchestrator_hardware_samplers(&engine.handle(), &mut telemetry);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            telemetry.flush(None, "test");
            let frames = std::fs::read_to_string(frame_log.path()).expect("read frame log");
            if [
                "host.cpu",
                "host.gpu",
                "host.memory",
                "host.net",
                "host.storage",
            ]
            .iter()
            .all(|channel| frames.contains(&format!(r#""channel":"{channel}""#)))
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "orchestrator hardware channels were incomplete: {frames}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub(crate) fn run_with_options<I>(
    args: I,
    capture_stdio: bool,
    stop_rx: Option<mpsc::Receiver<()>>,
) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    let mut config_builder = ConfigBuilder::hardcoded_defaults();
    if let Some(overlay) = TomlConfigOverlay::load_optional(Path::new(DEFAULT_CONFIG_PATH))? {
        config_builder = config_builder.overlay_toml(overlay)?;
    }
    let mut config = config_builder
        .overlay_env()?
        .overlay_cli(args)?
        .finalize()?;
    let state_dir = daemon::StateDir::new(config.state_dir.clone());
    let data_namespace_path = config.state_dir.join("data-namespace.json");
    let contextual_upload_root = config.state_dir.join("contextual-uploads");
    if config.reset_state {
        state_dir.reset()?;
    }
    let identity = state_dir.load_or_create_identity()?;
    let mut snapshot = state_dir.load_snapshot()?;
    if snapshot.run_id == 0 {
        snapshot = daemon::ClusterSnapshot::fresh(config.run_id, daemon_label(&config));
    } else if snapshot.run_id != config.run_id {
        // Node container names and provider labels derive from run_id; the
        // persisted run owns them so adoption addresses the same resources.
        config.run_id = snapshot.run_id;
    }
    if let Some(url) = DashboardSupport::configured_url(config.dashboard)? {
        println!("Myelin dashboard: {url}");
        std::io::stdout()
            .flush()
            .map_err(|error| format!("flush dashboard URL to stdout: {error}"))?;
    }
    let orch_stdio_capture = if capture_stdio {
        OrchStdioCapture::install()?
    } else {
        None
    };
    let mut orch_telemetry =
        OrchTelemetry::new(config.run_id, config.telemetry_frame_log.as_deref())?;
    let run_id = config.run_id;
    let node_id = config.node_id;
    let bootstrap = |ds: &mut OrchTelemetry,
                     dash: Option<&DashboardSupport>,
                     phase: &str,
                     status: &str,
                     detail: Value| {
        ds.emit_bootstrap(dash, run_id, node_id, phase, status, detail);
    };
    bootstrap(
        &mut orch_telemetry,
        None,
        "config",
        "ready",
        json!({
            "config_profile":match config.config_profile {
                RuntimeConfigProfile::Local => "local",
                RuntimeConfigProfile::Deploy => "deploy",
            },
            "image":&config.image,
            "provider":config.provider.as_str(),
            "model_id":&config.model_id,
            "stage_index":config.stage_index,
            "legacy_layer_end_exclusive":config.layer_end_exclusive,
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "provider_config":config.provider_telemetry_detail(),
        }),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "telemetry_preflight",
        "configured",
        json!({
            "producer":"myelin-orchestrator",
            "telemetry_endpoint":{
                "role":"orchestrator-frame-archive",
                "transport":"telemetry-frame-log",
                "configured":config.telemetry_frame_log.is_some(),
                "archive_path":config.telemetry_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
            },
            "expected_worker_producers":["myelin-worker","tinygrad-worker"],
            "provider":config.provider.as_str(),
            "pipeline_stages":config.pipeline_stages,
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "provider_config":config.provider_telemetry_detail(),
        }),
    );
    let orch_synthetic_id = format!("myelin-orchestrator-{}-telemetry-preflight", config.run_id);
    for (phase, status) in [
        ("TelemetryProducerConfigured", "configured"),
        ("TelemetryProducerConnected", "ready"),
        ("TelemetrySyntheticEventSent", "sent"),
        ("TelemetrySyntheticEventObserved", "observed"),
    ] {
        bootstrap(
            &mut orch_telemetry,
            None,
            phase,
            status,
            json!({
                "producer":"myelin-orchestrator",
                "producer_class":"rust-orchestrator",
                "synthetic_id":orch_synthetic_id,
                "telemetry_endpoint":{
                    "role":"orchestrator-frame-archive",
                    "transport":"telemetry-frame-log",
                    "configured":config.telemetry_frame_log.is_some(),
                    "archive_path":config.telemetry_frame_log.as_ref().map(|path| path.to_string_lossy().to_string()),
                },
            }),
        );
    }
    let actors_channel = orch_telemetry.channel_by_name("runtime.actors");
    let orch_stats_hook = orch_telemetry.stats_hook_on(actors_channel);

    // Build the core swactor runtime parts, clone the routing handle needed by
    // integrations, then hand the workers to the engine. The engine owns both
    // core progression and the Tokio substrate (it schedules all background
    // work); components retain only cheap Runtime handles (ENGINE_SPEC.md).
    let (parts, runtime, codec, transport_router) = DistributionRuntimeStack::build_runtime(
        |registry| {
            register_myelin_actor_codecs(registry);
            telemetry::wire::register_telemetry_codec(registry);
        },
        Some(orch_stats_hook),
    );
    let engine = match TokioBackend::new(TokioConfig::default())
        .and_then(|backend| Engine::new(parts, backend))
    {
        Ok(engine) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "engine",
                "ready",
                json!({"backend":"tokio","owns":"core+substrate"}),
            );
            engine
        }
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "engine",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("create engine: {error}"));
        }
    };
    spawn_orchestrator_hardware_samplers(&engine.handle(), &mut orch_telemetry);
    let mut driver = match IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: Some(identity),
            relay_mode: config.relay.mode.clone(),
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec(), TELEMETRY_ALPN.to_vec()],
        },
    ) {
        Ok(driver) => driver,
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "iroh_driver",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("create iroh driver: {error}"));
        }
    };
    let remote_vast_relay_required = config.vastai.as_ref().is_some_and(|vastai| {
        vastai.provisioning_mode == VastAiProvisioningMode::Real
            && !matches!(config.relay.mode, RelayMode::Disabled)
    });
    if remote_vast_relay_required {
        bootstrap(
            &mut orch_telemetry,
            None,
            "iroh_relay",
            "waiting",
            json!({"relay_mode":format!("{:?}", config.relay.mode)}),
        );
        let endpoint = driver.wait_for_relay_endpoint()?;
        bootstrap(
            &mut orch_telemetry,
            None,
            "iroh_relay",
            "ready",
            json!({"relay_url":endpoint.relay_urls().next().map(ToString::to_string)}),
        );
    }
    let live_coordinator_endpoint = driver.endpoint_addr_supplier();
    let coordinator_endpoint =
        advertised_endpoint(driver.endpoint_addr(), config.endpoint_addr_mask)?;
    bootstrap(
        &mut orch_telemetry,
        None,
        "iroh_driver",
        "ready",
        json!({"endpoint":coordinator_endpoint.clone(),"has_relay":coordinator_endpoint.relay_urls().next().is_some(),"direct_addr_count":coordinator_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay.mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "endpoint_config_snapshot",
        "ready",
        json!({
            "producer":"myelin-orchestrator",
            "coordinator_endpoint":coordinator_endpoint.clone(),
            "has_relay":coordinator_endpoint.relay_urls().next().is_some(),
            "direct_addr_count":coordinator_endpoint.ip_addrs().count(),
            "relay_mode":format!("{:?}", config.relay.mode),
            "endpoint_addr_mask":config.endpoint_addr_mask.as_str(),
            "connectivity_preflight":"ready",
        }),
    );
    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime,
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );
    let orch_stdio_rx = orch_stdio_capture
        .map(|capture| capture.start(&stack.runtime))
        .transpose()?;
    bootstrap(
        &mut orch_telemetry,
        None,
        "distribution_stack",
        "ready",
        json!({"actors":"initialized","route_view":"initialized","swim":"initialized"}),
    );
    bootstrap(
        &mut orch_telemetry,
        None,
        "codecs",
        "ready",
        json!({"registered":["node_agent","orchestrator","provisioner","prompt_rpc","telemetry"]}),
    );
    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: stack.runtime.clone(),
        codec: stack.codec.clone(),
        routes: stack.actor_bridge_routes(),
        swim: stack.actors.swim,
        relay_mirror: stack.relay_mirror.clone(),
        route_view: stack.route_view.clone(),
        outbox: stack.outbox.clone(),
    });
    let data_namespace =
        crate::data_namespace::DataNamespaceAuthority::start(&stack, &driver, data_namespace_path)?;
    let tiny_linear_weights = std::env::var_os("MYELIN_TINY_LINEAR_WEIGHTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("apps/myelin/testdata/tiny_linear.weights"));
    futures_lite::future::block_on(
        data_namespace.control().ensure(
            data_plane::path::DataPath::parse("/models/tiny-linear/weights")
                .expect("static tiny-linear data path"),
            data_plane::blob::file(tiny_linear_weights),
        ),
    )
    .map_err(|error| format!("register tiny-linear weights: {error}"))?;
    let recovery_nodes = snapshot
        .nodes
        .iter()
        .filter(|node| !matches!(node.phase, NodePhase::Stopped))
        .filter_map(|node| {
            node.runtime
                .as_ref()
                .map(|runtime| (node.logical_node_id, runtime))
        })
        .map(|(node_id, runtime)| {
            serde_json::from_str::<EndpointAddr>(&runtime.endpoint)
                .map(|endpoint| (node_id, endpoint))
                .map_err(|error| format!("parse persisted endpoint for node {node_id}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !recovery_nodes.is_empty() {
        driver.join(
            &recovery_nodes
                .iter()
                .map(|(_, endpoint)| endpoint.clone())
                .collect::<Vec<_>>(),
        );
    }
    // Engine owns protocol tick injection and core progression; the application
    // loop only drains integration-owned queues (ENGINE_SPEC.md).
    stack.spawn_protocol_ticker(PUMP_INTERVAL);
    driver.install_actor_bridge_pump(PUMP_INTERVAL);
    bootstrap(
        &mut orch_telemetry,
        None,
        "actor_bridge",
        "ready",
        json!({"transport":"iroh","routes":"attached","protocol_ticker":"engine-hosted"}),
    );

    let collector = FrameCollector::new();
    for (node_id, endpoint) in &recovery_nodes {
        collector.subscribe_node(
            &engine.handle(),
            driver.endpoint(),
            endpoint.clone(),
            snapshot.run_id,
            *node_id,
        );
    }
    bootstrap(
        &mut orch_telemetry,
        None,
        "telemetry_collector",
        "ready",
        json!({"alpn":String::from_utf8_lossy(TELEMETRY_ALPN)}),
    );
    let dashboard: Option<DashboardSupport>;

    let orchestrator_reports = match stack.runtime.new_inbox::<OrchestratorReport>() {
        Ok(inbox) => inbox,
        Err(error) => {
            bootstrap(
                &mut orch_telemetry,
                None,
                "orchestrator_report_actor",
                "failed",
                json!({"error":error.to_string()}),
            );
            return Err(format!("orchestrator report inbox: {error}"));
        }
    };
    let orchestrator_report_actor = *orchestrator_reports.addr();
    stack.register_local_actor(driver.register_actor(orchestrator_report_actor, 1));

    let (obs_tx, obs_rx) = mpsc::channel::<PluginObservation>();
    let sink = PluginSink::new(Arc::new(ChannelObservationSink {
        tx: Mutex::new(obs_tx),
    }));
    let shared_config = Arc::new(Mutex::new(config.clone()));
    let live_orchestrator_actor = Arc::new(Mutex::new(None::<ActorAddress>));
    let bootstrap_route_config = Arc::clone(&shared_config);
    let bootstrap_route_actor = Arc::clone(&live_orchestrator_actor);
    let bootstrap_env_source: BootstrapEnvSource = Arc::new(move || {
        let config = bootstrap_route_config.lock();
        let endpoint = advertised_endpoint(live_coordinator_endpoint(), config.endpoint_addr_mask)?;
        let actor = (*bootstrap_route_actor.lock()).ok_or_else(|| {
            "current orchestrator actor is unavailable before SSH bootstrap".to_owned()
        })?;
        Ok(vec![
            (
                "MYELIN_COORDINATOR_ENDPOINT".to_owned(),
                serde_json::to_string(&endpoint)
                    .map_err(|error| format!("serialize live coordinator endpoint: {error}"))?,
            ),
            (
                "MYELIN_ORCHESTRATOR_ACTOR".to_owned(),
                serde_json::to_string(&actor)
                    .map_err(|error| format!("serialize live orchestrator actor: {error}"))?,
            ),
        ])
    });
    let provider_runtime = stack.runtime.clone();
    let provider_engine = engine.handle();
    let provider_config = Arc::clone(&shared_config);
    let provider_registry = state_dir.process_registry_path();
    let provider_bootstrap_env = Arc::clone(&bootstrap_env_source);
    let provider_factory: ProviderFactory = Arc::new(move || {
        provider_config.lock().clone().build_provisioner(
            provider_runtime.clone(),
            provider_engine.clone(),
            provider_registry.clone(),
            Arc::clone(&provider_bootstrap_env),
        )
    });
    let spec_config = Arc::clone(&shared_config);
    let spec_coordinator = coordinator_endpoint.clone();
    let spec_route_actor = Arc::clone(&live_orchestrator_actor);
    let spec_builder: SpecBuilder = Arc::new(move |node_id, orchestrator_actor| {
        *spec_route_actor.lock() = Some(orchestrator_actor);
        let config = spec_config.lock();
        let spec =
            config.node_spec_for_stage(spec_coordinator.clone(), orchestrator_actor, node_id, 0)?;
        Ok(spec)
    });
    let validation_config = Arc::clone(&shared_config);
    let validation_runtime = stack.runtime.clone();
    let validation_engine = engine.handle();
    let validation_registry = state_dir.process_registry_path();
    let validation_bootstrap_env = Arc::clone(&bootstrap_env_source);
    let config_validator: ConfigValidator = Arc::new(move |request| {
        let mut candidate = validation_config.lock().clone();
        if candidate.provider.as_str() != "vastai" {
            return Ok(());
        }
        let vastai = candidate
            .vastai
            .as_mut()
            .ok_or_else(|| "Vast.ai configuration is unavailable".to_owned())?;
        if let Some(api_key) = request.api_key {
            vastai.api_key = Some(api_key);
        }
        if let Some(path) = request.ssh_identity {
            vastai.ssh_identity = Some(expand_home_path(&path)?);
        }
        if let Some(command) = request.bootstrap_command {
            vastai.bootstrap_command = Some(command);
        }
        if vastai.provisioning_mode == VastAiProvisioningMode::Mock {
            let _ = candidate.build_provisioner(
                validation_runtime.clone(),
                validation_engine.clone(),
                validation_registry.clone(),
                Arc::clone(&validation_bootstrap_env),
            )?;
            *validation_config.lock() = candidate;
            return Ok(());
        }
        if vastai.api_key.as_deref().is_none_or(str::is_empty) {
            return Err("Vast.ai API key is required for live offer search".to_owned());
        }
        candidate.prepare_vastai_ssh_key()?;
        let _ = candidate.build_provisioner(
            validation_runtime.clone(),
            validation_engine.clone(),
            validation_registry.clone(),
            Arc::clone(&validation_bootstrap_env),
        )?;
        *validation_config.lock() = candidate;
        Ok(())
    });
    let offer_searcher: Option<OfferSearcher> = if config.provider.as_str() == "vastai" {
        let search_config = Arc::clone(&shared_config);
        Some(Arc::new(move |request: OfferSearchRequest| {
            request.validate()?;
            let criteria = request.browse_criteria();
            let config = search_config.lock().clone();
            let vastai = config
                .vastai
                .ok_or_else(|| "Vast.ai configuration is unavailable".to_owned())?;
            let api_key = vastai
                .api_key
                .ok_or_else(|| "Vast.ai API key is not configured".to_owned())?;
            let mut client = ToolsVastAiLeaseClient::from_api_key(api_key)?;
            Ok(client
                .browse_offers(&criteria)?
                .into_iter()
                .map(|offer| OfferDto {
                    offer_id: offer.id,
                    host_id: offer.host_id,
                    gpu_model: offer.gpu_name,
                    gpu_ram_mb: offer.gpu_ram,
                    compute_cap: offer.compute_cap,
                    verification: offer.verification,
                    reliability: offer.reliability2,
                    download_mbps: offer.inet_down,
                    upload_mbps: offer.inet_up,
                    location: offer.geolocation,
                    hourly_price: offer.dph_total,
                    download_cost_per_tb: offer.inet_down_cost_per_tb,
                    upload_cost_per_tb: offer.inet_up_cost_per_tb,
                })
                .collect())
        }) as OfferSearcher)
    } else {
        None
    };
    let vastai_provisioning_mode = config
        .vastai
        .as_ref()
        .map_or("real", |vastai| vastai.provisioning_mode.as_str());
    let readiness = if config.provider.as_str() == "vastai" && vastai_provisioning_mode != "mock" {
        ProviderReadiness::unconfigured_for(
            config.provider.as_str(),
            config.image.clone(),
            "submit a Vast.ai API key, SSH identity, and bootstrap command",
        )
        .with_provisioning_mode(vastai_provisioning_mode)
    } else {
        ProviderReadiness::ready_for(config.provider.as_str(), config.image.clone())
            .with_provisioning_mode(vastai_provisioning_mode)
    };
    let initial_configuration = config
        .vastai
        .as_ref()
        .map(|vastai| ProviderConfigurationRequest {
            api_key: vastai.api_key.clone(),
            ssh_identity: vastai
                .ssh_identity
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            bootstrap_command: vastai.bootstrap_command.clone(),
        });
    let manual = ManualActorControl::new(
        ManualControl::new(snapshot, readiness),
        stack.runtime.clone(),
        stack.engine.blocking_work_sender(),
        ManualActorControlConfig {
            state_dir: state_dir.clone(),
            sink,
            provider_factory,
            spec_builder,
            config_validator: Some(config_validator),
            offer_searcher,
            control_generation: daemon::unix_ms_now(),
        },
    );
    let orchestrator_actor = match stack.runtime.spawn(
        OrchestratorActor::new(
            RunConfig {
                run_id: RunId(config.run_id),
                max_tokens: u64::from(config.default_max_tokens),
                prompt: Vec::new(),
            },
            Some(orchestrator_report_actor),
        )
        .with_manual_control(manual)
        .with_contextual_artifact_cleanup(ContextualArtifactCleanup::new(
            data_namespace.directory(),
            contextual_upload_root.clone(),
        )),
    ) {
        Ok(actor) => actor,
        Err(error) => return Err(format!("spawn orchestrator actor: {error}")),
    };
    stack.register_local_actor(driver.register_actor(orchestrator_actor, 1));
    stack
        .runtime
        .send_to(
            stack.actors.registry,
            RegistryIn::RegisterName {
                name: CONTROL_REGISTRY_NAME.to_owned(),
                actor_addr: orchestrator_actor,
            },
        )
        .map_err(|error| format!("register manual control actor: {error}"))?;
    if let Some(request) = initial_configuration {
        stack
            .runtime
            .send_to(
                orchestrator_actor,
                OrchestratorMsg::Manual(ManualControlMsg::Configure {
                    request,
                    reply_to: None,
                }),
            )
            .map_err(|error| format!("route initial provider validation: {error}"))?;
    }

    let control_plugin = control::plugin(
        stack.runtime.clone(),
        engine.handle(),
        orchestrator_actor,
        data_namespace.control(),
        contextual_upload_root,
    );
    dashboard = DashboardSupport::start_with_plugins(
        config.dashboard,
        &engine.handle(),
        vec![control_plugin],
    )?;
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "fleet_control",
        "started",
        json!({
            "mode":"manual",
            "commands":["provision","kill"],
            "transport":"direct_actor_message",
        }),
    );
    bootstrap(
        &mut orch_telemetry,
        dashboard.as_ref(),
        "serve_cluster",
        "started",
        json!({"mode":"daemon","poll_interval_ms":PUMP_INTERVAL.as_millis()}),
    );

    let runtime = stack.runtime.clone();
    let completion = ActorCompletion::new();
    let serve_actor = runtime
        .spawn(ServeClusterActor {
            driver,
            stack,
            obs_rx,
            collector,
            orchestrator_reports,
            dashboard,
            orch_telemetry,
            orch_stdio_rx,
            run_id: config.run_id,
            orchestrator_node_id: config.node_id,
            provider: config.provider.clone(),
            orchestrator_actor,
            engine: engine.handle(),
            sender: runtime.create_sender(),
            lifecycle: ServeClusterLifecycle::new(),
            flush_reply_actor: None,
            completion: completion.clone(),
        })
        .map_err(|error| format!("spawn daemon lifecycle actor: {error}"))?;
    if let Err(error) = spawn_stop_listener(&runtime, stop_rx, serve_actor) {
        let _ = runtime.send_to(serve_actor, ServeClusterMsg::Abort(error));
    }
    let result = completion.wait();
    if result.is_ok() {
        let snapshot = state_dir.load_snapshot()?;
        state_dir.cleanup_recovery_checkpoint(&snapshot)?;
    }
    result
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VastAiProvisioningMode {
    Real,
    Mock,
}

impl VastAiProvisioningMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "real" => Ok(Self::Real),
            "mock" | "simulated" => Ok(Self::Mock),
            other => Err(format!(
                "unsupported Vast.ai provisioning mode {other:?}; use real or mock"
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Mock => "mock",
        }
    }
}

#[derive(Clone)]
struct VastAiRuntimeConfig {
    api_key: Option<String>,
    provisioning_mode: VastAiProvisioningMode,
    provisioning: VastAiProvisioningConfig,
    bootstrap_command: Option<String>,
    ssh_identity: Option<PathBuf>,
    ssh_public_fingerprint: Option<String>,
}

impl VastAiRuntimeConfig {
    fn from_builder(builder: &ConfigBuilder) -> Result<Self, String> {
        let mut provisioning = VastAiProvisioningConfig::default();
        macro_rules! raw_config {
            ($parser:ident, $env:literal, $raw:expr, $value:expr) => {
                $raw.as_ref()
                    .map(|value| ConfigBuilder::$parser($env, value))
                    .transpose()?
                    .or($value)
            };
        }
        if let Some(disk_gb) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_DISK_GB",
            builder.vastai_disk_gb_raw,
            builder.vastai_disk_gb
        ) {
            provisioning.disk_gb = disk_gb;
        }
        if let Some(ssh_user) = &builder.vastai_ssh_user {
            provisioning.ssh_user = ssh_user.clone();
        }
        if let Some(confirm_lease) = raw_config!(
            parse_bool,
            "MYELIN_VASTAI_CONFIRM_LEASE",
            builder.vastai_confirm_lease_raw,
            builder.vastai_confirm_lease
        ) {
            provisioning.confirm_lease = confirm_lease;
        }
        provisioning.onstart = builder.vastai_onstart.clone();
        provisioning.selection.gpu_name = builder.vastai_gpu_name.clone();
        if let Some(min_gpu_ram_mb) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_GPU_RAM_MB",
            builder.vastai_min_gpu_ram_mb_raw,
            builder.vastai_min_gpu_ram_mb
        ) {
            provisioning.selection.min_gpu_ram_mb = Some(min_gpu_ram_mb);
        }
        if let Some(min_down_mbps) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_DOWN_MBPS",
            builder.vastai_min_down_mbps_raw,
            builder.vastai_min_down_mbps
        ) {
            provisioning.selection.min_down_mbps = min_down_mbps;
        }
        if let Some(max_dph_total) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MAX_DPH_TOTAL",
            builder.vastai_max_dph_total_raw,
            builder.vastai_max_dph_total
        ) {
            provisioning.selection.max_dph_total = Some(max_dph_total);
        }
        if let Some(min_up_mbps) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_UP_MBPS",
            builder.vastai_min_up_mbps_raw,
            builder.vastai_min_up_mbps
        ) {
            provisioning.selection.min_up_mbps = Some(min_up_mbps);
        }
        if let Some(min_reliability) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_MIN_RELIABILITY",
            builder.vastai_min_reliability_raw,
            builder.vastai_min_reliability
        ) {
            provisioning.selection.min_reliability = min_reliability;
        }
        if let Some(require_verified) = raw_config!(
            parse_bool,
            "MYELIN_VASTAI_REQUIRE_VERIFIED",
            builder.vastai_require_verified_raw,
            builder.vastai_require_verified
        ) {
            provisioning.selection.require_verified = require_verified;
        }
        for host_id in &builder.vastai_blacklist_hosts {
            if !provisioning.selection.blacklist_hosts.contains(host_id) {
                provisioning.selection.blacklist_hosts.push(*host_id);
            }
        }
        if let Some(poll_interval_secs) = raw_config!(
            parse_value,
            "MYELIN_VASTAI_POLL_INTERVAL_SECS",
            builder.vastai_poll_interval_secs_raw,
            builder.vastai_poll_interval_secs
        ) {
            provisioning.lifecycle.poll_interval = Duration::from_secs(poll_interval_secs);
        }
        let ssh_identity = builder
            .vastai_ssh_identity_raw
            .as_ref()
            .map(|value| expand_home_path(value))
            .transpose()?;
        Ok(Self {
            api_key: builder.vastai_api_key.clone(),
            provisioning_mode: builder.vastai_provisioning_mode,
            provisioning,
            bootstrap_command: builder.vastai_bootstrap_command.clone(),
            ssh_identity,
            ssh_public_fingerprint: None,
        })
    }

    fn telemetry_detail(&self) -> Value {
        json!({
            "provisioning_mode": self.provisioning_mode.as_str(),
            "disk_gb": self.provisioning.disk_gb,
            "ssh_user": &self.provisioning.ssh_user,
            "gpu_name": &self.provisioning.selection.gpu_name,
            "min_gpu_ram_mb": self.provisioning.selection.min_gpu_ram_mb,
            "min_compute_cap": self.provisioning.selection.min_compute_cap,
            "min_down_mbps": self.provisioning.selection.min_down_mbps,
            "min_up_mbps": self.provisioning.selection.min_up_mbps,
            "max_dph_total": self.provisioning.selection.max_dph_total,
            "min_reliability": self.provisioning.selection.min_reliability,
            "require_verified": self.provisioning.selection.require_verified,
            "blacklist_hosts": &self.provisioning.selection.blacklist_hosts,
            "state_timeout_secs": self.provisioning.lifecycle.state_timeout.as_secs(),
            "confirm_lease": self.provisioning.confirm_lease,
            "has_api_key": self.api_key.is_some(),
            "has_onstart": self.provisioning.onstart.is_some(),
            "has_bootstrap_command": self.bootstrap_command.is_some(),
            "has_ssh_identity": self.ssh_identity.is_some(),
            "ssh_public_fingerprint": self.ssh_public_fingerprint.as_deref(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeConfigProfile {
    Local,
    Deploy,
}

impl RuntimeConfigProfile {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "deploy" => Ok(Self::Deploy),
            other => Err(format!(
                "unsupported {MYELIN_RUNTIME_CONFIG_ENV}={other:?}; use local or deploy"
            )),
        }
    }
}

#[derive(Clone)]
struct CachedModelConfig {
    host_path: PathBuf,
    container_path: String,
}

impl CachedModelConfig {
    fn from_host_path(provider: &str, requested: PathBuf) -> Result<Self, String> {
        if !matches!(provider, "process" | "docker" | "vastai") {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} is a host-local cache path and is only supported by provider=process, provider=docker, or vastai planning"
            ));
        }
        let host_path = requested.canonicalize().map_err(|e| {
            format!(
                "resolve {CACHED_MODEL_HOST_ENV} path {}: {e}",
                requested.display()
            )
        })?;
        if !host_path.is_file() {
            return Err(format!(
                "{CACHED_MODEL_HOST_ENV} must point at a file: {}",
                host_path.display()
            ));
        }
        let file_name = host_path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                format!(
                    "cached model path has no file name: {}",
                    host_path.display()
                )
            })?;
        let container_path = format!("{CACHED_MODEL_CONTAINER_DIR}/{file_name}");
        Ok(Self {
            host_path,
            container_path,
        })
    }

    fn telemetry_detail(&self) -> Value {
        json!({
            "host_path_present": true,
            "file": self.host_path.file_name().and_then(|name| name.to_str()),
            "container_path": &self.container_path,
        })
    }
}

fn default_pipeline_cached_model_path() -> PathBuf {
    let relative = PathBuf::from(".")
        .join(DEFAULT_PIPELINE_MODEL_CACHE_DIR)
        .join(DEFAULT_PIPELINE_CACHED_MODEL_FILE);
    if relative.is_file() {
        return relative;
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(DEFAULT_PIPELINE_MODEL_CACHE_DIR)
        .join(DEFAULT_PIPELINE_CACHED_MODEL_FILE)
}

#[derive(Clone)]
struct Config {
    config_profile: RuntimeConfigProfile,
    image: String,
    docker_gpus: String,
    provider: ProviderKind,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: Option<u32>,
    pipeline_stages: u32,
    model_id: String,
    default_max_tokens: u32,
    dashboard: bool,
    state_dir: PathBuf,
    reset_state: bool,
    relay: RelayRuntimeConfig,
    endpoint_addr_mask: EndpointAddrMask,
    vastai: Option<VastAiRuntimeConfig>,
    cached_model: Option<CachedModelConfig>,
    telemetry_frame_log: Option<PathBuf>,
    worker_bin: Option<PathBuf>,
}

#[derive(Clone)]
struct ConfigBuilder {
    config_profile: RuntimeConfigProfile,
    provider: Option<ProviderKind>,
    image: String,
    toml_vastai_image: Option<String>,
    image_overridden_after_toml: bool,
    docker_gpus: String,
    run_id: u64,
    node_id: u64,
    stage_index: u32,
    layer_end_exclusive: Option<u32>,
    pipeline_stages: u32,
    model_id: String,
    gguf_source: GgufSource,
    tokenizer: TokenizerSource,
    default_max_tokens: u32,
    dashboard: bool,
    state_dir: Option<PathBuf>,
    reset_state: bool,
    max_context: Option<u32>,
    relay_mode: Option<String>,
    relay_url: Option<String>,
    endpoint_addr_mask: Option<String>,
    vastai_api_key: Option<String>,
    vastai_provisioning_mode: VastAiProvisioningMode,
    vastai_bootstrap_command: Option<String>,
    vastai_disk_gb: Option<u32>,
    vastai_disk_gb_raw: Option<String>,
    vastai_ssh_user: Option<String>,
    vastai_confirm_lease: Option<bool>,
    vastai_confirm_lease_raw: Option<String>,
    vastai_onstart: Option<String>,
    vastai_ssh_identity_raw: Option<String>,
    vastai_gpu_name: Option<String>,
    vastai_min_gpu_ram_mb: Option<u64>,
    vastai_min_gpu_ram_mb_raw: Option<String>,
    vastai_min_down_mbps: Option<f64>,
    vastai_min_down_mbps_raw: Option<String>,
    vastai_min_up_mbps: Option<f64>,
    vastai_max_dph_total: Option<f64>,
    vastai_max_dph_total_raw: Option<String>,
    vastai_min_up_mbps_raw: Option<String>,
    vastai_min_reliability: Option<f64>,
    vastai_min_reliability_raw: Option<String>,
    vastai_require_verified: Option<bool>,
    vastai_require_verified_raw: Option<String>,
    vastai_poll_interval_secs: Option<u64>,
    vastai_blacklist_hosts: Vec<u64>,
    vastai_poll_interval_secs_raw: Option<String>,
    cached_model_host_path: Option<PathBuf>,
    telemetry_frame_log: Option<PathBuf>,
    worker_bin: Option<PathBuf>,
}

impl ConfigBuilder {
    fn hardcoded_defaults() -> Self {
        Self {
            config_profile: RuntimeConfigProfile::Local,
            provider: None,
            image: DEFAULT_IMAGE.to_owned(),
            toml_vastai_image: None,
            image_overridden_after_toml: false,
            docker_gpus: "all".to_owned(),
            run_id: 1,
            node_id: 1,
            stage_index: 0,
            layer_end_exclusive: None,
            pipeline_stages: 1,
            model_id: DEFAULT_MODEL_ID.to_owned(),
            gguf_source: GgufSource::HuggingFaceGguf {
                repo: DEFAULT_HF_REPO.to_owned(),
                file: DEFAULT_HF_FILE.to_owned(),
                revision: None,
            },
            tokenizer: TokenizerSource::EmbeddedGguf,
            default_max_tokens: DEFAULT_MAX_TOKENS,
            dashboard: true,
            state_dir: None,
            reset_state: false,
            max_context: None,
            relay_mode: None,
            relay_url: None,
            endpoint_addr_mask: None,
            vastai_api_key: None,
            vastai_provisioning_mode: VastAiProvisioningMode::Real,
            vastai_bootstrap_command: None,
            vastai_disk_gb: None,
            vastai_disk_gb_raw: None,
            vastai_ssh_user: None,
            vastai_confirm_lease: None,
            vastai_confirm_lease_raw: None,
            vastai_onstart: None,
            vastai_ssh_identity_raw: None,
            vastai_gpu_name: None,
            vastai_min_gpu_ram_mb: None,
            vastai_min_gpu_ram_mb_raw: None,
            vastai_min_down_mbps: None,
            vastai_min_down_mbps_raw: None,
            vastai_max_dph_total: None,
            vastai_max_dph_total_raw: None,
            vastai_min_up_mbps: None,
            vastai_min_up_mbps_raw: None,
            vastai_min_reliability: None,
            vastai_min_reliability_raw: None,
            vastai_require_verified: None,
            vastai_require_verified_raw: None,
            vastai_poll_interval_secs: None,
            vastai_blacklist_hosts: Vec::new(),
            vastai_poll_interval_secs_raw: None,
            cached_model_host_path: None,
            telemetry_frame_log: None,
            worker_bin: None,
        }
    }

    fn overlay_toml(mut self, overlay: TomlConfigOverlay) -> Result<Self, String> {
        macro_rules! apply {
            ($option:expr, |$value:ident| $body:block) => {
                if let Some($value) = $option $body
            };
        }

        apply!(overlay.runtime.profile, |profile| {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        });
        apply!(overlay.runtime.run_id, |run_id| { self.run_id = run_id });
        apply!(overlay.runtime.node_id, |node_id| {
            self.node_id = node_id
        });
        apply!(overlay.runtime.stage_index, |stage_index| {
            self.stage_index = stage_index
        });
        apply!(overlay.runtime.layer_end_exclusive, |layer_end_exclusive| {
            self.layer_end_exclusive = Some(layer_end_exclusive)
        });
        apply!(overlay.runtime.pipeline_stages, |pipeline_stages| {
            self.pipeline_stages = pipeline_stages
        });
        apply!(overlay.provider.kind, |provider| {
            self.provider = Some(provider_kind::parse_deploy(&provider)?);
        });
        apply!(overlay.image.node, |image| { self.image = image });
        apply!(overlay.relay.mode, |mode| { self.relay_mode = Some(mode) });
        apply!(overlay.relay.url, |url| { self.relay_url = Some(url) });
        apply!(overlay.prompt.max_tokens, |max_tokens| {
            self.default_max_tokens = max_tokens
        });
        apply!(overlay.prompt.dashboard, |dashboard| {
            self.dashboard = dashboard
        });
        apply!(overlay.model.id, |model_id| { self.model_id = model_id });
        apply!(overlay.model.gguf_local_path, |path| {
            self.gguf_source = GgufSource::LocalPath(path)
        });
        apply!(overlay.model.gguf_repo, |repo| {
            self.set_hf_source(Some(repo), None, None)
        });
        apply!(overlay.model.gguf_file, |file| {
            self.set_hf_source(None, Some(file), None)
        });
        apply!(overlay.model.gguf_revision, |revision| {
            self.set_hf_source(None, None, Some(Some(revision)))
        });
        apply!(overlay.model.tokenizer_local_path, |path| {
            self.tokenizer = TokenizerSource::LocalPath(path)
        });
        apply!(overlay.model.max_context, |max_context| {
            self.max_context = Some(max_context)
        });
        apply!(overlay.docker.gpus, |gpus| { self.docker_gpus = gpus });
        apply!(overlay.docker.cached_model_host_path, |path| {
            self.cached_model_host_path = Some(PathBuf::from(path))
        });
        apply!(overlay.observability.telemetry_frame_log, |path| {
            self.telemetry_frame_log = Some(PathBuf::from(path))
        });
        apply!(overlay.vastai.image, |image| {
            self.toml_vastai_image = Some(image)
        });
        apply!(overlay.vastai.provisioning_mode, |mode| {
            self.vastai_provisioning_mode = VastAiProvisioningMode::parse(&mode)?
        });
        apply!(overlay.vastai.api_key, |api_key| {
            self.vastai_api_key = Some(api_key)
        });
        apply!(overlay.vastai.bootstrap_command, |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        apply!(overlay.vastai.disk_gb, |disk_gb| {
            self.vastai_disk_gb = Some(disk_gb)
        });
        apply!(overlay.vastai.ssh_user, |ssh_user| {
            self.vastai_ssh_user = Some(ssh_user)
        });
        apply!(overlay.vastai.confirm_lease, |confirm_lease| {
            self.vastai_confirm_lease = Some(confirm_lease)
        });
        apply!(overlay.vastai.onstart, |onstart| {
            self.vastai_onstart = Some(onstart)
        });
        apply!(overlay.vastai.ssh_identity, |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });
        apply!(overlay.vastai.gpu_name, |gpu_name| {
            self.vastai_gpu_name = Some(gpu_name)
        });
        apply!(overlay.vastai.min_gpu_ram_mb, |min_gpu_ram_mb| {
            self.vastai_min_gpu_ram_mb = Some(min_gpu_ram_mb)
        });
        apply!(overlay.vastai.min_down_mbps, |min_down_mbps| {
            self.vastai_min_down_mbps = Some(min_down_mbps)
        });
        apply!(overlay.vastai.max_dph_total, |max_dph_total| {
            self.vastai_max_dph_total = Some(max_dph_total)
        });
        apply!(overlay.vastai.min_up_mbps, |min_up_mbps| {
            self.vastai_min_up_mbps = Some(min_up_mbps)
        });
        apply!(overlay.vastai.min_reliability, |min_reliability| {
            self.vastai_min_reliability = Some(min_reliability)
        });
        apply!(overlay.vastai.require_verified, |require_verified| {
            self.vastai_require_verified = Some(require_verified)
        });
        for host_id in overlay.vastai.blacklist_hosts {
            self.push_vastai_blacklist_host(host_id);
        }
        apply!(overlay.vastai.poll_interval_secs, |poll_interval_secs| {
            self.vastai_poll_interval_secs = Some(poll_interval_secs)
        });
        Ok(self)
    }

    fn overlay_env(mut self) -> Result<Self, String> {
        macro_rules! apply {
            ($option:expr, |$value:ident| $body:block) => {
                if let Some($value) = $option $body
            };
        }
        macro_rules! env_apply {
            ($name:expr, |$value:ident| $body:block) => {
                apply!(env_optional($name), |$value| $body)
            };
        }
        macro_rules! env_parse {
            ($name:expr, |$value:ident| $body:block) => {
                env_apply!($name, |raw| {
                    let $value = Self::parse_value($name, &raw)?;
                    $body
                })
            };
        }

        env_apply!(MYELIN_RUNTIME_CONFIG_ENV, |profile| {
            self.config_profile = RuntimeConfigProfile::parse(&profile)?;
        });
        env_parse!("MYELIN_RUN_ID", |run_id| { self.run_id = run_id });
        env_parse!("MYELIN_LOGICAL_NODE_ID", |node_id| {
            self.node_id = node_id
        });
        env_parse!("MYELIN_STAGE_INDEX", |stage_index| {
            self.stage_index = stage_index
        });
        env_parse!("MYELIN_LAYER_END_EXCLUSIVE", |layer_end_exclusive| {
            self.layer_end_exclusive = Some(layer_end_exclusive)
        });
        env_parse!("MYELIN_PIPELINE_STAGES", |pipeline_stages| {
            self.pipeline_stages = pipeline_stages
        });
        apply!(
            env_optional("MYELIN_NODE_PROVIDER").or_else(|| env_optional("MYELIN_PROVIDER")),
            |provider| {
                self.provider = Some(provider_kind::parse_deploy(&provider)?);
            }
        );
        env_apply!("MYELIN_NODE_IMAGE", |image| {
            self.image = image;
            self.image_overridden_after_toml = true;
        });
        env_apply!("MYELIN_DOCKER_GPUS", |gpus| { self.docker_gpus = gpus });
        env_apply!(CACHED_MODEL_HOST_ENV, |path| {
            self.cached_model_host_path = Some(PathBuf::from(path))
        });
        env_apply!(MYELIN_WORKER_BIN_ENV, |path| {
            self.worker_bin = Some(PathBuf::from(path))
        });
        env_parse!("MYELIN_PROMPT_MAX_TOKENS", |max_tokens| {
            self.default_max_tokens = max_tokens
        });
        env_apply!("MYELIN_DASHBOARD", |dashboard| {
            self.dashboard = Self::parse_bool("MYELIN_DASHBOARD", &dashboard)?;
        });
        env_apply!("MYELIN_STATE_DIR", |state_dir| {
            self.state_dir = Some(PathBuf::from(state_dir))
        });

        env_apply!(TELEMETRY_FRAME_LOG_ENV, |path| {
            self.telemetry_frame_log = Some(PathBuf::from(path))
        });
        env_apply!("MYELIN_MODEL_ID", |model_id| { self.model_id = model_id });
        env_apply!("MYELIN_GGUF_LOCAL_PATH", |path| {
            self.gguf_source = GgufSource::LocalPath(path)
        });
        env_apply!("MYELIN_GGUF_REPO", |repo| {
            self.set_hf_source(Some(repo), None, None)
        });
        env_apply!("MYELIN_GGUF_FILE", |file| {
            self.set_hf_source(None, Some(file), None)
        });
        env_apply!("MYELIN_GGUF_REVISION", |revision| {
            self.set_hf_source(None, None, Some(Some(revision)))
        });
        env_apply!("MYELIN_TOKENIZER_LOCAL_PATH", |path| {
            self.tokenizer = TokenizerSource::LocalPath(path)
        });
        env_parse!("MYELIN_MAX_CONTEXT", |max_context| {
            self.max_context = Some(max_context)
        });
        env_apply!("MYELIN_IROH_RELAY_MODE", |mode| {
            self.relay_mode = Some(mode.to_ascii_lowercase())
        });
        apply!(
            env_optional(MYELIN_IROH_RELAY_URL_ENV)
                .or_else(|| env_optional(SWACTOR_IROH_RELAY_URL_ENV)),
            |url| {
                self.relay_url = Some(url);
            }
        );
        env_apply!(MVP_IROH_ENDPOINT_ADDR_MASK_ENV, |mask| {
            self.endpoint_addr_mask = Some(mask)
        });
        apply!(
            env_optional("VAST_API_KEY")
                .or_else(|| env_optional("MYELIN_VASTAI_API_KEY"))
                .or_else(|| env_optional("VASTAI_API_KEY")),
            |api_key| {
                self.vastai_api_key = Some(api_key);
            }
        );
        env_apply!("MYELIN_VASTAI_PROVISIONING_MODE", |mode| {
            self.vastai_provisioning_mode = VastAiProvisioningMode::parse(&mode)?
        });
        env_apply!("MYELIN_VASTAI_BOOTSTRAP_COMMAND", |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        env_apply!("MYELIN_VASTAI_SSH_IDENTITY", |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });
        env_apply!("MYELIN_VASTAI_DISK_GB", |disk_gb| {
            self.vastai_disk_gb_raw = Some(disk_gb)
        });
        env_apply!("MYELIN_VASTAI_SSH_USER", |ssh_user| {
            self.vastai_ssh_user = Some(ssh_user)
        });
        env_apply!("MYELIN_VASTAI_CONFIRM_LEASE", |confirm_lease| {
            self.vastai_confirm_lease_raw = Some(confirm_lease)
        });
        env_apply!("MYELIN_VASTAI_ONSTART", |onstart| {
            self.vastai_onstart = Some(onstart)
        });
        env_apply!("MYELIN_VASTAI_GPU_NAME", |gpu_name| {
            self.vastai_gpu_name = Some(gpu_name)
        });
        env_apply!("MYELIN_VASTAI_MIN_GPU_RAM_MB", |min_gpu_ram_mb| {
            self.vastai_min_gpu_ram_mb_raw = Some(min_gpu_ram_mb)
        });
        env_apply!("MYELIN_VASTAI_MIN_DOWN_MBPS", |min_down_mbps| {
            self.vastai_min_down_mbps_raw = Some(min_down_mbps)
        });
        env_apply!("MYELIN_VASTAI_MAX_DPH_TOTAL", |max_dph_total| {
            self.vastai_max_dph_total_raw = Some(max_dph_total)
        });
        env_apply!("MYELIN_VASTAI_MIN_UP_MBPS", |min_up_mbps| {
            self.vastai_min_up_mbps_raw = Some(min_up_mbps)
        });
        env_apply!("MYELIN_VASTAI_MIN_RELIABILITY", |min_reliability| {
            self.vastai_min_reliability_raw = Some(min_reliability)
        });
        env_apply!("MYELIN_VASTAI_REQUIRE_VERIFIED", |require_verified| {
            self.vastai_require_verified_raw = Some(require_verified)
        });
        env_apply!("MYELIN_VASTAI_BLACKLIST_HOSTS", |blacklist_hosts| {
            for host_id in Self::parse_list("MYELIN_VASTAI_BLACKLIST_HOSTS", &blacklist_hosts)? {
                self.push_vastai_blacklist_host(host_id);
            }
        });
        env_apply!("MYELIN_VASTAI_POLL_INTERVAL_SECS", |poll_interval_secs| {
            self.vastai_poll_interval_secs_raw = Some(poll_interval_secs)
        });
        Ok(self)
    }

    fn overlay_cli(mut self, args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--runtime-config" => {
                    self.config_profile =
                        RuntimeConfigProfile::parse(&next_arg(&mut args, "--runtime-config")?)?
                }
                "--provider" => {
                    self.provider = Some(provider_kind::parse_deploy(&next_arg(
                        &mut args,
                        "--provider",
                    )?)?)
                }
                "--vastai-provisioning" => {
                    self.vastai_provisioning_mode = VastAiProvisioningMode::parse(&next_arg(
                        &mut args,
                        "--vastai-provisioning",
                    )?)?
                }
                "--vastai-provisioning-mock" => {
                    self.vastai_provisioning_mode = VastAiProvisioningMode::Mock
                }
                "--worker-bin" => {
                    self.worker_bin = Some(PathBuf::from(next_arg(&mut args, "--worker-bin")?))
                }
                "--image" => {
                    self.image = next_arg(&mut args, "--image")?;
                    self.image_overridden_after_toml = true;
                }
                "--gpus" => self.docker_gpus = next_arg(&mut args, "--gpus")?,
                "--run-id" => self.run_id = parse_next(&mut args, "--run-id")?,
                "--node-id" => self.node_id = parse_next(&mut args, "--node-id")?,
                "--stage-index" => self.stage_index = parse_next(&mut args, "--stage-index")?,
                "--layer-end-exclusive" => {
                    self.layer_end_exclusive = Some(parse_next(&mut args, "--layer-end-exclusive")?)
                }
                "-N" | "--pipeline-stages" => self.pipeline_stages = parse_next(&mut args, &arg)?,
                "--max-tokens" => self.default_max_tokens = parse_next(&mut args, "--max-tokens")?,
                "--dashboard" => self.dashboard = true,
                "--no-dashboard" => self.dashboard = false,
                "--state-dir" => {
                    self.state_dir = Some(PathBuf::from(next_arg(&mut args, "--state-dir")?))
                }
                "--reset-state" => self.reset_state = true,
                "--telemetry-frame-log" => {
                    self.telemetry_frame_log =
                        Some(PathBuf::from(next_arg(&mut args, "--telemetry-frame-log")?));
                }
                "--model-id" => self.model_id = next_arg(&mut args, "--model-id")?,
                "--gguf-local-path" => {
                    self.gguf_source =
                        GgufSource::LocalPath(next_arg(&mut args, "--gguf-local-path")?)
                }
                "--gguf-repo" => {
                    self.set_hf_source(Some(next_arg(&mut args, "--gguf-repo")?), None, None)
                }
                "--gguf-file" => {
                    self.set_hf_source(None, Some(next_arg(&mut args, "--gguf-file")?), None)
                }
                "--gguf-revision" => self.set_hf_source(
                    None,
                    None,
                    Some(Some(next_arg(&mut args, "--gguf-revision")?)),
                ),
                "--tokenizer-local-path" => {
                    self.tokenizer =
                        TokenizerSource::LocalPath(next_arg(&mut args, "--tokenizer-local-path")?)
                }
                "--max-context" => self.max_context = Some(parse_next(&mut args, "--max-context")?),
                "--cached-model-host-path" => {
                    self.cached_model_host_path = Some(PathBuf::from(next_arg(
                        &mut args,
                        "--cached-model-host-path",
                    )?));
                }
                "--relay-mode" => self.relay_mode = Some(next_arg(&mut args, "--relay-mode")?),
                "--relay-url" => self.relay_url = Some(next_arg(&mut args, "--relay-url")?),
                "--endpoint-addr-mask" => {
                    self.endpoint_addr_mask = Some(next_arg(&mut args, "--endpoint-addr-mask")?)
                }
                "--vastai-disk-gb" => {
                    self.vastai_disk_gb = Some(parse_next(&mut args, "--vastai-disk-gb")?);
                    self.vastai_disk_gb_raw = None;
                }
                "--vastai-min-gpu-ram-mb" => {
                    self.vastai_min_gpu_ram_mb =
                        Some(parse_next(&mut args, "--vastai-min-gpu-ram-mb")?);
                    self.vastai_min_gpu_ram_mb_raw = None;
                }
                "--vastai-min-down-mbps" => {
                    self.vastai_min_down_mbps =
                        Some(parse_next(&mut args, "--vastai-min-down-mbps")?);
                    self.vastai_min_down_mbps_raw = None;
                }
                "--vastai-max-dph-total" => {
                    self.vastai_max_dph_total =
                        Some(parse_next(&mut args, "--vastai-max-dph-total")?);
                    self.vastai_max_dph_total_raw = None;
                }
                "--vastai-min-up-mbps" => {
                    self.vastai_min_up_mbps = Some(parse_next(&mut args, "--vastai-min-up-mbps")?);
                    self.vastai_min_up_mbps_raw = None;
                }
                "--vastai-min-reliability" => {
                    self.vastai_min_reliability =
                        Some(parse_next(&mut args, "--vastai-min-reliability")?);
                    self.vastai_min_reliability_raw = None;
                }
                "--vastai-blacklist-host" => {
                    let host_id = parse_next(&mut args, "--vastai-blacklist-host")?;
                    self.push_vastai_blacklist_host(host_id);
                }
                "--vastai-poll-interval-secs" => {
                    self.vastai_poll_interval_secs =
                        Some(parse_next(&mut args, "--vastai-poll-interval-secs")?);
                    self.vastai_poll_interval_secs_raw = None;
                }
                "--vastai-api-key" => {
                    self.vastai_api_key = Some(next_arg(&mut args, "--vastai-api-key")?)
                }
                "--vastai-bootstrap-command" => {
                    self.vastai_bootstrap_command =
                        Some(next_arg(&mut args, "--vastai-bootstrap-command")?)
                }
                "--vastai-ssh-identity" => {
                    self.vastai_ssh_identity_raw =
                        Some(next_arg(&mut args, "--vastai-ssh-identity")?);
                }
                "--vastai-ssh-user" => {
                    self.vastai_ssh_user = Some(next_arg(&mut args, "--vastai-ssh-user")?)
                }
                "--vastai-onstart" => {
                    self.vastai_onstart = Some(next_arg(&mut args, "--vastai-onstart")?)
                }
                "--vastai-gpu-name" => {
                    self.vastai_gpu_name = Some(next_arg(&mut args, "--vastai-gpu-name")?)
                }
                "--vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(true);
                    self.vastai_confirm_lease_raw = None;
                }
                "--no-vastai-confirm-lease" => {
                    self.vastai_confirm_lease = Some(false);
                    self.vastai_confirm_lease_raw = None;
                }
                "--vastai-require-verified" => {
                    self.vastai_require_verified = Some(true);
                    self.vastai_require_verified_raw = None;
                }
                "--no-vastai-require-verified" => {
                    self.vastai_require_verified = Some(false);
                    self.vastai_require_verified_raw = None;
                }
                _ => return Err(format!("unknown argument {arg:?}")),
            }
        }
        Ok(self)
    }

    fn finalize(self) -> Result<Config, String> {
        let provider = self
            .provider
            .clone()
            .unwrap_or_else(|| match self.config_profile {
                RuntimeConfigProfile::Local => provider_kind::process(),
                RuntimeConfigProfile::Deploy => provider_kind::vastai(),
            });
        let provider_name = provider.as_str();
        let mut image = self.image.clone();
        if provider_name == "vastai"
            && !self.image_overridden_after_toml
            && let Some(vastai_image) = &self.toml_vastai_image
        {
            image = vastai_image.clone();
        }
        if self.pipeline_stages == 0 {
            return Err("--pipeline-stages must be greater than 0".to_owned());
        }
        let mut cached_model_host_path = self.cached_model_host_path.clone();
        if matches!(provider_name, "process" | "docker")
            && self.pipeline_stages > 1
            && cached_model_host_path.is_none()
            && (matches!(
                &self.gguf_source,
                GgufSource::HuggingFaceGguf {
                    repo,
                    file,
                    revision: None,
                } if repo == DEFAULT_HF_REPO && file == DEFAULT_HF_FILE
            ) || matches!(
                &self.gguf_source,
                GgufSource::HuggingFaceGguf {
                    file,
                    revision: None,
                    ..
                } if file == DEFAULT_PIPELINE_CACHED_MODEL_FILE
            ))
        {
            cached_model_host_path = Some(default_pipeline_cached_model_path());
        }
        let cached_model = cached_model_host_path
            .map(|path| CachedModelConfig::from_host_path(provider_name, path))
            .transpose()?;
        let relay = relay_runtime_config_from_settings(
            self.run_id,
            self.relay_mode.as_deref(),
            self.relay_url.as_deref(),
        )?;
        let endpoint_addr_mask = match self.endpoint_addr_mask.as_deref() {
            Some(mask) => EndpointAddrMask::parse(mask)?,
            None => EndpointAddrMask::Full,
        };
        let vastai = (provider_name == "vastai")
            .then(|| VastAiRuntimeConfig::from_builder(&self))
            .transpose()?;
        Ok(Config {
            config_profile: self.config_profile,
            image,
            docker_gpus: self.docker_gpus,
            provider,
            run_id: self.run_id,
            node_id: self.node_id,
            stage_index: self.stage_index,
            layer_end_exclusive: self.layer_end_exclusive,
            pipeline_stages: self.pipeline_stages,
            model_id: self.model_id,
            default_max_tokens: self.default_max_tokens,
            dashboard: self.dashboard,
            state_dir: self
                .state_dir
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR)),
            reset_state: self.reset_state,
            relay,
            endpoint_addr_mask,
            vastai,
            cached_model,
            worker_bin: self.worker_bin,
            telemetry_frame_log: self.telemetry_frame_log,
        })
    }

    fn set_hf_source(
        &mut self,
        repo: Option<String>,
        file: Option<String>,
        revision: Option<Option<String>>,
    ) {
        let (current_repo, current_file, current_revision) = match &self.gguf_source {
            GgufSource::HuggingFaceGguf {
                repo,
                file,
                revision,
            } => (repo.clone(), file.clone(), revision.clone()),
            GgufSource::LocalPath(_) => {
                (DEFAULT_HF_REPO.to_owned(), DEFAULT_HF_FILE.to_owned(), None)
            }
        };
        self.gguf_source = GgufSource::HuggingFaceGguf {
            repo: repo.unwrap_or(current_repo),
            file: file.unwrap_or(current_file),
            revision: revision.unwrap_or(current_revision),
        };
    }

    fn push_vastai_blacklist_host(&mut self, host_id: u64) {
        if !self.vastai_blacklist_hosts.contains(&host_id) {
            self.vastai_blacklist_hosts.push(host_id);
        }
    }

    fn parse_list<T>(name: &str, value: &str) -> Result<Vec<T>, String>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| Self::parse_value(name, part))
            .collect()
    }

    fn parse_value<T>(name: &str, value: &str) -> Result<T, String>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        value
            .parse::<T>()
            .map_err(|e| format!("invalid {name}={value:?}: {e}"))
    }

    fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
        match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!(
                "invalid {name}={value:?}; use 1/0, true/false, yes/no, or on/off"
            )),
        }
    }
}

impl Config {
    fn provider_telemetry_detail(&self) -> Value {
        match self.provider.as_str() {
            "process" => json!({
                "worker_bin": self.worker_bin.as_ref().map(|path| path.to_string_lossy().to_string()),
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::telemetry_detail),
            }),
            "docker" => json!({
                "docker_gpus": &self.docker_gpus,
                "cached_model": self.cached_model.as_ref().map(CachedModelConfig::telemetry_detail),
            }),
            "vastai" => self
                .vastai
                .as_ref()
                .map_or_else(|| json!({}), VastAiRuntimeConfig::telemetry_detail),
            _ => json!({}),
        }
    }

    fn prepare_vastai_ssh_key(&mut self) -> Result<(), String> {
        if self.provider.as_str() != "vastai" {
            return Ok(());
        }

        if self
            .vastai
            .as_ref()
            .is_some_and(|vastai| vastai.provisioning_mode == VastAiProvisioningMode::Mock)
        {
            return Ok(());
        }

        let api_key = self
            .vastai
            .as_ref()
            .and_then(|vastai| vastai.api_key.as_deref())
            .ok_or_else(|| {
                "VAST_API_KEY, MYELIN_VASTAI_API_KEY, or VASTAI_API_KEY is required when MYELIN_NODE_PROVIDER=vastai"
                    .to_owned()
            })?
            .to_owned();
        let identity = resolve_vastai_ssh_identity(
            self.vastai
                .as_ref()
                .and_then(|vastai| vastai.ssh_identity.clone()),
        )?;
        if !identity.is_file() {
            return Err(format!(
                "missing VastAI SSH identity {}; create/register one with vastai create ssh-key or set MYELIN_VASTAI_SSH_IDENTITY",
                identity.display()
            ));
        }
        let public_key = derive_ssh_public_key(&identity)?;
        let fingerprint = ssh_public_key_fingerprint(&public_key);
        ensure_vastai_account_ssh_key(&api_key, &public_key)?;

        eprintln!(
            "VastAI SSH identity {} fingerprint {} registered for account",
            identity.display(),
            fingerprint
        );

        let vastai = self
            .vastai
            .as_mut()
            .expect("VastAI config exists when provider is vastai");
        vastai.ssh_identity = Some(identity);
        vastai.provisioning.ssh_public_key = Some(public_key);
        vastai.ssh_public_fingerprint = Some(fingerprint);
        Ok(())
    }

    fn local_worker_bin(&self) -> Result<PathBuf, String> {
        let worker_bin = match &self.worker_bin {
            Some(worker_bin) => worker_bin.clone(),
            None => std::env::current_exe().map_err(|error| format!("current exe: {error}"))?,
        };
        if !worker_bin.is_file() {
            return Err(format!(
                "local process worker binary does not exist: {}",
                worker_bin.display()
            ));
        }
        Ok(worker_bin)
    }

    fn build_provisioner(
        &self,
        bootstrap_runtime: swactor::runtime::Runtime,
        bootstrap_engine: EngineHandle,
        process_registry_path: PathBuf,
        bootstrap_env: BootstrapEnvSource,
    ) -> Result<Box<dyn ProvisionPlugin>, String> {
        match self.provider.as_str() {
            "process" => Ok(Box::new(LocalProcessPlugin::with_registry(
                self.local_worker_bin()?,
                process_registry_path,
                "process",
                bootstrap_runtime.clone(),
            ))),
            "docker" => Ok(Box::new(LocalDockerPlugin::new(
                env_optional("MYELIN_DOCKER_CONTAINER_PREFIX")
                    .unwrap_or_else(|| "myelin-orchestrator".to_owned()),
                bootstrap_runtime.clone(),
            ))),
            "vastai" => {
                let vastai = self.vastai.as_ref().ok_or_else(|| {
                    "VastAI config was not resolved for provider vastai".to_owned()
                })?;
                if vastai.provisioning_mode == VastAiProvisioningMode::Mock {
                    return Ok(Box::new(MockVastAiPlugin::with_docker(
                        env_optional("MYELIN_MOCK_VASTAI_CONTAINER_PREFIX")
                            .unwrap_or_else(|| "myelin-mock-vastai".to_owned()),
                        bootstrap_runtime.clone(),
                    )));
                }
                if vastai.bootstrap_command.is_none() {
                    return Err(
                        "MYELIN_VASTAI_BOOTSTRAP_COMMAND is required when MYELIN_NODE_PROVIDER=vastai"
                            .to_owned(),
                    );
                }
                let api_key = vastai.api_key.clone().ok_or_else(|| {
                    "VAST_API_KEY, MYELIN_VASTAI_API_KEY, or VASTAI_API_KEY is required when MYELIN_NODE_PROVIDER=vastai"
                        .to_owned()
                })?;
                let ssh_identity = vastai
                    .ssh_identity
                    .clone()
                    .ok_or_else(|| "VastAI SSH identity was not prepared".to_owned())?;
                Ok(Box::new(VastAiProvisioningPlugin::new(
                    ToolsVastAiLeaseClient::from_api_key(api_key)?
                        .with_actor_host(bootstrap_runtime.clone(), bootstrap_engine.clone()),
                    SshCommandBootstrapLauncher::new(
                        Some(ssh_identity),
                        bootstrap_runtime,
                        bootstrap_env,
                        bootstrap_engine,
                    ),
                    vastai.provisioning.clone(),
                )))
            }
            _ => Err("mock provider cannot build a runtime provisioner".to_owned()),
        }
    }

    /// Provider-specific environment shared by filter and launch paths.
    fn extra_worker_env(&self) -> Vec<(String, String)> {
        let mut env = Vec::new();
        if let Some(url) = &self.relay.url {
            env.push((MYELIN_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
        }
        let docker_realized = self.provider.as_str() == "docker"
            || (self.provider.as_str() == "vastai"
                && self.vastai.as_ref().is_some_and(|vastai| {
                    vastai.provisioning_mode == VastAiProvisioningMode::Mock
                }));
        if docker_realized {
            env.push(("MYELIN_DOCKER_GPUS".to_owned(), self.docker_gpus.clone()));
        }
        env
    }

    fn node_spec_for_stage(
        &self,
        coordinator: EndpointAddr,
        orchestrator_actor: ActorAddress,
        logical_node_id: u64,
        stage_index: u32,
    ) -> Result<NodeProvisionSpec, String> {
        let provider_name = self.provider.as_str();
        let mut env = vec![
            ("MYELIN_AGENT_ONLY".to_owned(), "1".to_owned()),
            ("MYELIN_RUN_ID".to_owned(), self.run_id.to_string()),
            (
                "MYELIN_LOGICAL_NODE_ID".to_owned(),
                logical_node_id.to_string(),
            ),
            ("MYELIN_NODE_ATTEMPT_ID".to_owned(), "0".to_owned()),
            ("MYELIN_STAGE_INDEX".to_owned(), stage_index.to_string()),
            (
                MVP_IROH_ENDPOINT_ADDR_MASK_ENV.to_owned(),
                self.endpoint_addr_mask.as_str().to_owned(),
            ),
            ("MYELIN_NODE_PROVIDER".to_owned(), provider_name.to_owned()),
            (
                "MYELIN_COORDINATOR_ENDPOINT".to_owned(),
                serde_json::to_string(&coordinator)
                    .map_err(|e| format!("serialize coordinator endpoint: {e}"))?,
            ),
            (
                "MYELIN_ORCHESTRATOR_ACTOR".to_owned(),
                serde_json::to_string(&orchestrator_actor)
                    .map_err(|e| format!("serialize orchestrator actor: {e}"))?,
            ),
            (
                "MYELIN_IROH_RELAY_MODE".to_owned(),
                relay_mode_env_value(&self.relay.mode).to_owned(),
            ),
        ];
        if provider_name == "vastai"
            && self
                .vastai
                .as_ref()
                .is_some_and(|vastai| vastai.provisioning_mode == VastAiProvisioningMode::Real)
        {
            env.retain(|(key, _)| {
                key != "MYELIN_COORDINATOR_ENDPOINT" && key != "MYELIN_ORCHESTRATOR_ACTOR"
            });
        }
        env.extend(self.extra_worker_env());
        let args = match provider_name {
            "vastai"
                if self.vastai.as_ref().is_some_and(|vastai| {
                    vastai.provisioning_mode == VastAiProvisioningMode::Mock
                }) =>
            {
                self.worker_bin
                    .is_none()
                    .then(|| crate::ORCHESTRATOR_WORKER_MODE_ARG.to_owned())
                    .into_iter()
                    .collect()
            }
            "vastai" => self
                .vastai
                .as_ref()
                .and_then(|vastai| vastai.bootstrap_command.clone())
                .into_iter()
                .collect(),
            "process" if self.worker_bin.is_none() => {
                vec![crate::ORCHESTRATOR_WORKER_MODE_ARG.to_owned()]
            }
            "process" | "docker" => Vec::new(),
            _ => return Err("myelin-orchestrator does not support mock provider".to_owned()),
        };
        Ok(NodeProvisionSpec {
            run_id: self.run_id,
            node_id: logical_node_id,
            attempt_id: 0,
            stage_index: Some(stage_index),
            image: self.image.clone(),
            env,
            args,
            offer_criteria_json: None,
            mounts: Vec::new(),
        })
    }
}

#[derive(Clone)]
struct RuntimeReady {
    node_actor: ActorAddress,
    swim_node_id: DistNodeId,
}

fn runtime_ready_barrier_met(stack: &DistributionRuntimeStack, ready: &RuntimeReady) -> bool {
    stack.member_state(ready.swim_node_id) == Some(MemberState::Alive)
        && stack.route_owner(ready.node_actor) == Some(ready.swim_node_id)
}

#[cfg(target_os = "linux")]
struct OrchStdioCapture {
    stdout: File,
    stderr: File,
}

#[cfg(not(target_os = "linux"))]
struct OrchStdioCapture;

struct OrchStdioLine {
    stream: ProvisionLogStream,
    line: String,
}

struct OrchStdioRelay {
    tx: mpsc::Sender<OrchStdioLine>,
    closed: u8,
}

impl ActorInterface for OrchStdioRelay {
    type Incoming = swactor_process::ProcessStreamObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        match observation {
            swactor_process::ProcessStreamObservation::Line { stream, line } => {
                let stream = match stream {
                    swactor_process::ProcessStream::Stdout => ProvisionLogStream::Stdout,
                    swactor_process::ProcessStream::Stderr => ProvisionLogStream::Stderr,
                };
                if self.tx.send(OrchStdioLine { stream, line }).is_err() {
                    ctx.stop_self();
                }
            }
            swactor_process::ProcessStreamObservation::Error { .. } => {}
            swactor_process::ProcessStreamObservation::Closed { .. } => {
                self.closed = self.closed.saturating_add(1);
                if self.closed == 2 {
                    ctx.stop_self();
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl OrchStdioCapture {
    fn install() -> Result<Option<Self>, String> {
        Ok(Some(Self {
            stdout: Self::redirect_stream(libc::STDOUT_FILENO, "stdout")?,
            stderr: Self::redirect_stream(libc::STDERR_FILENO, "stderr")?,
        }))
    }

    fn start(self, runtime: &Runtime) -> Result<mpsc::Receiver<OrchStdioLine>, String> {
        let (tx, rx) = mpsc::channel();
        let actor = runtime
            .spawn(OrchStdioRelay { tx, closed: 0 })
            .map_err(|error| format!("spawn orchestrator stdio relay actor: {error}"))?;
        let sender = runtime.create_sender();
        swactor_process::spawn_line_reader(
            swactor_process::ProcessStream::Stdout,
            self.stdout,
            sender.clone(),
            actor,
        );
        swactor_process::spawn_line_reader(
            swactor_process::ProcessStream::Stderr,
            self.stderr,
            sender,
            actor,
        );
        Ok(rx)
    }

    fn redirect_stream(fd: libc::c_int, name: &str) -> Result<File, String> {
        let mut pipe_fds = [0; 2];
        let pipe_result = unsafe { libc::pipe(pipe_fds.as_mut_ptr()) };
        if pipe_result != 0 {
            return Err(format!(
                "create orchestrator {name} capture pipe: {}",
                std::io::Error::last_os_error()
            ));
        }

        let dup_result = unsafe { libc::dup2(pipe_fds[1], fd) };
        let close_write_result = unsafe { libc::close(pipe_fds[1]) };
        if dup_result < 0 {
            let error = std::io::Error::last_os_error();
            let _ = unsafe { libc::close(pipe_fds[0]) };
            return Err(format!("redirect orchestrator {name}: {error}"));
        }
        if close_write_result != 0 {
            let error = std::io::Error::last_os_error();
            let _ = unsafe { libc::close(pipe_fds[0]) };
            return Err(format!("close orchestrator {name} duplicate fd: {error}"));
        }

        Ok(unsafe { File::from_raw_fd(pipe_fds[0]) })
    }
}

#[cfg(not(target_os = "linux"))]
impl OrchStdioCapture {
    fn install() -> Result<Option<Self>, String> {
        Ok(None)
    }

    fn start(self, _runtime: &Runtime) -> Result<mpsc::Receiver<OrchStdioLine>, String> {
        unreachable!("stdio capture is unavailable on this target")
    }
}

fn drain_orch_stdio_capture(
    rx: Option<&mpsc::Receiver<OrchStdioLine>>,
    telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
) {
    let Some(rx) = rx else {
        return;
    };
    while let Ok(line) = rx.try_recv() {
        telemetry.emit_orchestrator_log(dashboard, line.stream, line.line);
    }
}

struct ChannelObservationSink {
    tx: Mutex<mpsc::Sender<PluginObservation>>,
}

impl PluginObservationSink for ChannelObservationSink {
    fn observe(&self, observation: PluginObservation) {
        let _ = self.tx.lock().send(observation);
    }
}

#[derive(Clone, Debug)]
enum ServeClusterMsg {
    Tick,
    Stop,
    Flushed,
    FlushFailed(String),
    FlushTimeout,
    Abort(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServeClusterState {
    Running,
    Flushing,
    Finished,
}

struct PendingRuntimeReady<T> {
    readiness_id: u64,
    ready: T,
}

struct ServeClusterLifecycle<T> {
    state: ServeClusterState,
    pending_readies: BTreeMap<u64, PendingRuntimeReady<T>>,
}

#[derive(Debug)]
enum ServeClusterEffect {
    None,
    Pump,
    BeginFlush,
    ProviderTerminalFailure { node_id: u64, error: String },
    Complete(Result<(), String>),
}

impl<T> ServeClusterLifecycle<T> {
    fn new() -> Self {
        Self {
            state: ServeClusterState::Running,
            pending_readies: BTreeMap::new(),
        }
    }

    fn apply_message(&mut self, message: ServeClusterMsg) -> ServeClusterEffect {
        match message {
            ServeClusterMsg::Tick if matches!(self.state, ServeClusterState::Running) => {
                ServeClusterEffect::Pump
            }
            ServeClusterMsg::Stop if matches!(self.state, ServeClusterState::Running) => {
                self.state = ServeClusterState::Flushing;
                self.pending_readies.clear();
                ServeClusterEffect::BeginFlush
            }
            ServeClusterMsg::Flushed if matches!(self.state, ServeClusterState::Flushing) => {
                self.complete(Ok(()))
            }
            ServeClusterMsg::FlushFailed(error)
                if matches!(self.state, ServeClusterState::Flushing) =>
            {
                self.complete(Err(error))
            }
            ServeClusterMsg::FlushTimeout if matches!(self.state, ServeClusterState::Flushing) => {
                self.complete(Err(
                    "timed out waiting for shutdown persistence flush".to_owned()
                ))
            }
            ServeClusterMsg::Abort(error) if !matches!(self.state, ServeClusterState::Finished) => {
                self.complete(Err(error))
            }
            ServeClusterMsg::Tick
            | ServeClusterMsg::Stop
            | ServeClusterMsg::Flushed
            | ServeClusterMsg::FlushFailed(_)
            | ServeClusterMsg::FlushTimeout
            | ServeClusterMsg::Abort(_) => ServeClusterEffect::None,
        }
    }

    fn track_runtime_ready(&mut self, node_id: u64, readiness_id: u64, ready: T) -> bool {
        if !matches!(self.state, ServeClusterState::Running) {
            return false;
        }
        if let Some(current) = self.pending_readies.get(&node_id)
            && current.readiness_id >= readiness_id
        {
            return false;
        }
        self.pending_readies.insert(
            node_id,
            PendingRuntimeReady {
                readiness_id,
                ready,
            },
        );
        true
    }

    fn acknowledge_runtime_ready(&mut self, node_id: u64, readiness_id: u64) -> bool {
        let matching = self
            .pending_readies
            .get(&node_id)
            .is_some_and(|ready| ready.readiness_id == readiness_id);
        if matching {
            self.pending_readies.remove(&node_id);
        }
        matching
    }

    fn remove_runtime_ready(&mut self, node_id: u64) {
        self.pending_readies.remove(&node_id);
    }

    fn plugin_observation(&self, terminal: Option<(u64, String)>) -> ServeClusterEffect {
        if !matches!(self.state, ServeClusterState::Running) {
            return ServeClusterEffect::None;
        }
        match terminal {
            Some((node_id, error)) => {
                ServeClusterEffect::ProviderTerminalFailure { node_id, error }
            }
            None => ServeClusterEffect::None,
        }
    }

    fn complete(&mut self, result: Result<(), String>) -> ServeClusterEffect {
        self.state = ServeClusterState::Finished;
        self.pending_readies.clear();
        ServeClusterEffect::Complete(result)
    }
}

struct StopSignalActor {
    sender: ExternalSender,
    serve_actor: ActorAddress,
}

impl ActorInterface for StopSignalActor {
    type Incoming = swactor_process::ProcessStopSignal;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _signal: Self::Incoming) {
        let _ = self.sender.send_to(self.serve_actor, ServeClusterMsg::Stop);
        ctx.stop_self();
    }
}

fn spawn_stop_listener(
    runtime: &Runtime,
    external: Option<mpsc::Receiver<()>>,
    serve_actor: ActorAddress,
) -> Result<(), String> {
    let actor = runtime
        .spawn(StopSignalActor {
            sender: runtime.create_sender(),
            serve_actor,
        })
        .map_err(|error| format!("spawn stop signal actor: {error}"))?;
    let sender = runtime.create_sender();
    if let Some(external) = external {
        swactor_process::spawn_stop_channel_wait(external, sender, actor);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    swactor_process::spawn_os_stop_signal_wait(sender, actor);
    Ok(())
}

struct ManualFlushForwarder {
    sender: ExternalSender,
    serve_actor: ActorAddress,
}

fn serve_cluster_flush_reply(reply: ManualControlReply) -> Option<ServeClusterMsg> {
    match reply {
        ManualControlReply::Flushed => Some(ServeClusterMsg::Flushed),
        ManualControlReply::Rejected(error) => Some(ServeClusterMsg::FlushFailed(format!(
            "shutdown persistence flush rejected: {error}"
        ))),
        ManualControlReply::TimedOut => Some(ServeClusterMsg::FlushTimeout),
        ManualControlReply::Accepted(_)
        | ManualControlReply::Provider(_)
        | ManualControlReply::Status(_)
        | ManualControlReply::FleetStatus(_)
        | ManualControlReply::Offers(_)
        | ManualControlReply::Rejoined(_) => None,
    }
}

impl ActorInterface for ManualFlushForwarder {
    type Incoming = ManualControlReply;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, reply: Self::Incoming) {
        if let Some(message) = serve_cluster_flush_reply(reply) {
            let _ = self.sender.send_to(self.serve_actor, message);
        }
        ctx.stop_self();
    }
}

struct ServeClusterActor {
    driver: IrohDriver,
    stack: DistributionRuntimeStack,
    obs_rx: mpsc::Receiver<PluginObservation>,
    collector: FrameCollector,
    orchestrator_reports: Inbox<OrchestratorReport>,
    dashboard: Option<DashboardSupport>,
    orch_telemetry: OrchTelemetry,
    orch_stdio_rx: Option<mpsc::Receiver<OrchStdioLine>>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: ProviderKind,
    orchestrator_actor: ActorAddress,
    engine: EngineHandle,
    sender: ExternalSender,
    lifecycle: ServeClusterLifecycle<RuntimeReady>,
    flush_reply_actor: Option<ActorAddress>,
    completion: ActorCompletion<Result<(), String>>,
}

fn daemon_label(config: &Config) -> String {
    match config.provider.as_str() {
        "docker" => env_optional("MYELIN_DOCKER_CONTAINER_PREFIX")
            .unwrap_or_else(|| "myelin-orchestrator".to_owned()),
        "vastai" => config
            .vastai
            .as_ref()
            .map(|vastai| vastai.provisioning.label_prefix.clone())
            .unwrap_or_else(|| "myelin".to_owned()),
        provider => format!("myelin-{provider}"),
    }
}

impl ServeClusterActor {
    fn observe_report(&mut self, report: OrchestratorReport) {
        match report {
            OrchestratorReport::NodeRuntimeReady {
                run_id,
                node_id,
                endpoint,
                node_actor,
                readiness_id,
                ..
            } if run_id == self.run_id => {
                let tracked = self.lifecycle.track_runtime_ready(
                    node_id,
                    readiness_id,
                    RuntimeReady {
                        node_actor,
                        swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                    },
                );
                if tracked {
                    self.driver.join(std::slice::from_ref(&endpoint));
                    self.collector.subscribe_node(
                        &self.engine,
                        self.driver.endpoint(),
                        endpoint,
                        run_id,
                        node_id,
                    );
                }
            }
            OrchestratorReport::NodeRuntimeReadyAck {
                run_id,
                node_id,
                readiness_id,
                ..
            } if run_id == self.run_id => {
                self.lifecycle
                    .acknowledge_runtime_ready(node_id, readiness_id);
            }
            _ => {}
        }
    }

    fn advance_join_barriers(&mut self) {
        let ready = self
            .lifecycle
            .pending_readies
            .iter()
            .filter(|(_, pending)| runtime_ready_barrier_met(&self.stack, &pending.ready))
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        for node_id in ready {
            if self
                .stack
                .runtime
                .send_to(
                    self.orchestrator_actor,
                    OrchestratorMsg::Manual(ManualControlMsg::JoinBarrierSatisfied { node_id }),
                )
                .is_ok()
            {
                self.lifecycle.remove_runtime_ready(node_id);
            }
        }
    }

    fn drain_observations(&mut self) {
        loop {
            let observation = match self.obs_rx.try_recv() {
                Ok(observation) => observation,
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    let _ = self.lifecycle.plugin_observation(None);
                    break;
                }
            };
            match &observation {
                PluginObservation::Exited {
                    run_id, node_id, ..
                }
                | PluginObservation::Failed {
                    run_id, node_id, ..
                } if *run_id == self.run_id => {
                    self.collector.unsubscribe_node(*run_id, *node_id);
                }
                _ => {}
            }
            emit_plugin_observation(
                &mut self.orch_telemetry,
                self.dashboard.as_ref(),
                &self.provider,
                &observation,
            );
            let terminal = match observation {
                PluginObservation::Exited {
                    node_id, status, ..
                } => Some((
                    node_id,
                    format!("provider process exited with status {status:?}"),
                )),
                PluginObservation::Failed {
                    node_id, reason, ..
                } => Some((node_id, reason)),
                _ => None,
            };
            if let ServeClusterEffect::ProviderTerminalFailure { node_id, error } =
                self.lifecycle.plugin_observation(terminal)
            {
                let _ = self.stack.runtime.send_to(
                    self.orchestrator_actor,
                    OrchestratorMsg::Manual(ManualControlMsg::ProviderTerminalFailure {
                        node_id,
                        error,
                    }),
                );
            }
        }
    }

    fn pump_once(&mut self) {
        self.collector.pump(&self.driver);
        let dashboard = self.dashboard.as_ref();
        let telemetry = &mut self.orch_telemetry;
        self.collector.drain(|stream, descriptor, channel, frame| {
            if let Some(dashboard) = dashboard {
                dashboard.publish_frame(stream, descriptor, channel, frame);
            }
            telemetry.archive_frame("node", stream, channel, frame);
        });
        self.orch_telemetry
            .flush(self.dashboard.as_ref(), "orchestrator");
        self.drain_observations();
        while let Some(report) = self.orchestrator_reports.try_recv() {
            self.observe_report(report);
        }
        self.advance_join_barriers();
        emit_swim_transitions(
            &mut self.orch_telemetry,
            self.dashboard.as_ref(),
            self.run_id,
            self.orchestrator_node_id,
            &self.stack,
        );
        emit_swim_probe_events(
            &mut self.orch_telemetry,
            self.dashboard.as_ref(),
            &self.stack,
            "daemon_monitor",
        );
        drain_orch_stdio_capture(
            self.orch_stdio_rx.as_ref(),
            &mut self.orch_telemetry,
            self.dashboard.as_ref(),
        );
        self.orch_telemetry
            .flush(self.dashboard.as_ref(), "orchestrator");
    }

    fn schedule(&self, ctx: &Ctx, delay: Duration, message: ServeClusterMsg) {
        self.engine
            .send_after(delay, self.sender.clone(), ctx.self_addr(), message);
    }

    fn begin_flush(&mut self, ctx: &Ctx) -> Result<(), String> {
        let reply_actor = self
            .stack
            .runtime
            .spawn(ManualFlushForwarder {
                sender: self.sender.clone(),
                serve_actor: ctx.self_addr(),
            })
            .map_err(|error| format!("spawn shutdown flush reply actor: {error}"))?;
        if let Err(error) = self.stack.runtime.send_to(
            self.orchestrator_actor,
            OrchestratorMsg::Manual(ManualControlMsg::Shutdown {
                reply_to: reply_actor,
            }),
        ) {
            let _ = self.stack.runtime.stop_actor(reply_actor);
            return Err(format!("request shutdown persistence flush: {error}"));
        }
        self.flush_reply_actor = Some(reply_actor);
        self.schedule(ctx, SHUTDOWN_FLUSH_TIMEOUT, ServeClusterMsg::FlushTimeout);
        Ok(())
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), String>) {
        if let Some(reply_actor) = self.flush_reply_actor.take() {
            let _ = self.stack.runtime.stop_actor(reply_actor);
        }
        if let Err(error) = &result {
            self.orch_telemetry.emit_bootstrap(
                self.dashboard.as_ref(),
                self.run_id,
                self.orchestrator_node_id,
                "serve_cluster",
                "failed",
                json!({"error":error}),
            );
        }
        assert!(
            self.completion.complete(result).is_ok(),
            "daemon lifecycle completed twice"
        );
        ctx.stop_self();
    }
}

impl ActorInterface for ServeClusterActor {
    type Incoming = ServeClusterMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), ServeClusterMsg::Tick);
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        let effect = self.lifecycle.apply_message(message);
        match effect {
            ServeClusterEffect::None => {}
            ServeClusterEffect::Pump => {
                self.pump_once();
                self.schedule(ctx, PUMP_INTERVAL, ServeClusterMsg::Tick);
            }
            ServeClusterEffect::BeginFlush => {
                if let Err(error) = self.begin_flush(ctx) {
                    let failure = self.lifecycle.apply_message(ServeClusterMsg::Abort(error));
                    if let ServeClusterEffect::Complete(result) = failure {
                        self.finish(ctx, result);
                    }
                }
            }
            ServeClusterEffect::ProviderTerminalFailure { node_id, error } => {
                let _ = self.stack.runtime.send_to(
                    self.orchestrator_actor,
                    OrchestratorMsg::Manual(ManualControlMsg::ProviderTerminalFailure {
                        node_id,
                        error,
                    }),
                );
            }
            ServeClusterEffect::Complete(result) => self.finish(ctx, result),
        }
    }
}

fn emit_plugin_observation(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    provider: &ProviderKind,
    observation: &PluginObservation,
) {
    match observation {
        PluginObservation::StdoutLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Stdout,
                line: line.clone(),
            },
        ),
        PluginObservation::StderrLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Stderr,
                line: line.clone(),
            },
        ),
        PluginObservation::ProviderLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(
            dashboard,
            ProvisionLogLine {
                run_id: *run_id,
                node_id: *node_id,
                stream: ProvisionLogStream::Provider,
                line: line.clone(),
            },
        ),
        PluginObservation::PhaseChanged {
            run_id,
            node_id,
            phase,
        } => orch_telemetry.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: *phase,
                provider: Some(provider.as_str().to_owned()),
                message: None,
            },
        ),
        PluginObservation::TelemetryFrame {
            channel, payload, ..
        } => orch_telemetry.emit_bytes_from(
            dashboard,
            channel,
            payload.as_bytes().to_vec(),
            "node_bootstrap_stdio",
        ),
        PluginObservation::Exited {
            run_id,
            node_id,
            status,
        } => orch_telemetry.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: ProvisionEventKind::NodeStopped,
                provider: Some(provider.as_str().to_owned()),
                message: Some(format!("node process exited with {status:?}")),
            },
        ),
        PluginObservation::Failed {
            run_id,
            node_id,
            reason,
        } => orch_telemetry.emit_event(
            dashboard,
            ProvisionEvent {
                run_id: *run_id,
                node_id: *node_id,
                kind: ProvisionEventKind::ProvisionFailed,
                provider: Some(provider.as_str().to_owned()),
                message: Some(reason.clone()),
            },
        ),
    }
}

fn emit_swim_transitions(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    run_id: u64,
    node_id: u64,
    stack: &DistributionRuntimeStack,
) -> Vec<ObservedTransition> {
    let transitions = stack.drain_swim_transitions();
    for transition in &transitions {
        let peer = format!("{:?}", transition.peer);
        let from = transition.from.map(|state| format!("{:?}", state));
        let to = format!("{:?}", transition.to);
        let member_state = stack
            .member_state(transition.peer)
            .map(|state| format!("{:?}", state));
        let last_ack_age_ms = transition.last_ack_age.map(duration_ms_u64);
        let consecutive_timeouts = transition.consecutive_timeouts;
        let recent_probe_targets = stack.swim_recent_probe_targets();
        orch_telemetry.emit_bootstrap_to_channel(BootstrapEmission {
            dashboard,
            channel: MYELIN_SWIM_MEMBERSHIP,
            run_id,
            node_id,
            phase: "membership_transition",
            status: "observed",
            detail: json!({
                "peer":peer.clone(),
                "from":from.clone(),
                "to":to.clone(),
                "reason":transition.reason,
                "last_ack_age_ms":last_ack_age_ms,
                "consecutive_timeouts":consecutive_timeouts,
                "recent_probe_targets":recent_probe_targets.clone(),
                "member_state":member_state.clone(),
            }),
        });
        orch_telemetry.emit_record(dashboard, &stack.membership_transition(transition));
    }
    transitions
}

fn emit_swim_probe_events(
    orch_telemetry: &mut OrchTelemetry,
    dashboard: Option<&DashboardSupport>,
    stack: &DistributionRuntimeStack,
    local_phase: &str,
) {
    for event in stack.drain_swim_probe_events() {
        let record = stack.swim_probe_event_record(event, local_phase);
        orch_telemetry.emit_record(dashboard, &record);
    }
    if let Some(summary) = stack.drain_swim_probe_summary() {
        let record = stack.swim_probe_summary_record(summary, local_phase);
        orch_telemetry.emit_record(dashboard, &record);
    }
}

pub(crate) fn env_optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub(crate) fn resolve_vastai_ssh_identity(explicit: Option<PathBuf>) -> Result<PathBuf, String> {
    match explicit {
        Some(path) => Ok(path),
        None => {
            let home = std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    "MYELIN_VASTAI_SSH_IDENTITY is required because HOME is unset".to_owned()
                })?;
            Ok(PathBuf::from(home).join(".ssh").join("id_ed25519"))
        }
    }
}

pub(crate) fn expand_home_path(value: &str) -> Result<PathBuf, String> {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "MYELIN_VASTAI_SSH_IDENTITY uses ~/ but HOME is unset".to_owned())?;
        return Ok(PathBuf::from(home).join(rest));
    }
    Ok(PathBuf::from(trimmed))
}

pub(crate) fn derive_ssh_public_key(identity: &Path) -> Result<String, String> {
    let output = swactor_process::command_output(
        Command::new("ssh-keygen").arg("-y").arg("-f").arg(identity),
    )
    .map_err(|e| {
        format!(
            "derive VastAI SSH public key from {}: {e}",
            identity.display()
        )
    })?;
    let public_key = String::from_utf8_lossy(&output.stdout)
        .trim_end_matches(['\r', '\n'])
        .to_owned();
    if !output.status.success() || public_key.trim().is_empty() {
        return Err(format!(
            "derive VastAI SSH public key from {}: {}",
            identity.display(),
            command_output_failure_detail(&output, None)
        ));
    }
    Ok(public_key)
}

pub(crate) fn ssh_public_key_fingerprint(public_key: &str) -> String {
    const UNAVAILABLE: &str = "unavailable";

    let path =
        std::env::temp_dir().join(format!("myelin-vastai-ssh-key-{}.pub", std::process::id()));
    if std::fs::write(&path, format!("{public_key}\n")).is_err() {
        return UNAVAILABLE.to_owned();
    }
    let output =
        swactor_process::command_output(Command::new("ssh-keygen").arg("-l").arg("-f").arg(&path))
            .ok();
    let _ = std::fs::remove_file(&path);
    let Some(output) = output.filter(|output| output.status.success()) else {
        return UNAVAILABLE.to_owned();
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut fields = stdout.split_whitespace();
    fields
        .next()
        .zip(fields.next())
        .map(|(bits, fingerprint)| format!("{bits} {fingerprint}"))
        .unwrap_or_else(|| UNAVAILABLE.to_owned())
}

fn vastai_account_has_ssh_key(api_key: &str, public_key: &str) -> Result<bool, String> {
    let output = swactor_process::command_output(Command::new("vastai").args([
        "show",
        "ssh-keys",
        "--raw",
        "--api-key",
        api_key,
    ]))
    .map_err(vastai_cli_error)?;
    if !output.status.success() {
        return Err(format!(
            "vastai show ssh-keys failed: {}",
            command_output_failure_detail(&output, Some(api_key))
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(account_ssh_keys_output_contains_public_key(
        &stdout, public_key,
    ))
}

pub(crate) fn ensure_vastai_account_ssh_key(api_key: &str, public_key: &str) -> Result<(), String> {
    if vastai_account_has_ssh_key(api_key, public_key)? {
        return Ok(());
    }

    let output = swactor_process::command_output(
        Command::new("vastai")
            .args(["create", "ssh-key"])
            .arg(public_key)
            .args(["-y", "--api-key", api_key]),
    )
    .map_err(vastai_cli_error)?;
    if !output.status.success() {
        return Err(format!(
            "vastai create ssh-key failed: {}",
            command_output_failure_detail(&output, Some(api_key))
        ));
    }

    if vastai_account_has_ssh_key(api_key, public_key)? {
        Ok(())
    } else {
        Err("VastAI SSH key registration did not make the selected key visible in vastai show ssh-keys".to_owned())
    }
}

fn account_ssh_keys_output_contains_public_key(output: &str, public_key: &str) -> bool {
    let public_key = public_key.trim();
    !public_key.is_empty()
        && (output.contains(public_key)
            || public_key
                .split_whitespace()
                .nth(1)
                .is_some_and(|body| !body.is_empty() && output.contains(body)))
}

fn vastai_cli_error(error: std::io::Error) -> String {
    if error.kind() == std::io::ErrorKind::NotFound {
        "vastai CLI is required to verify/register MYELIN_VASTAI_SSH_IDENTITY; install with pip install vastai".to_owned()
    } else {
        format!("run vastai CLI: {error}")
    }
}

fn command_output_failure_detail(output: &std::process::Output, secret: Option<&str>) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut detail = match stderr.trim() {
        "" => output.status.to_string(),
        detail => detail.to_owned(),
    };
    if let Some(secret) = secret.filter(|secret| !secret.is_empty()) {
        detail = detail.replace(secret, "<redacted>");
    }
    detail
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value after {name}"))
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = next_arg(args, name)?;
    value
        .parse::<T>()
        .map_err(|e| format!("invalid {name}={value:?}: {e}"))
}

#[cfg(test)]
mod serve_cluster_properties {
    use proptest::prelude::*;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{ActorCompletion, Engine, SteppingBackend};

    use super::*;
    use crate::tests::fuzz_support::{actor_census, drive_steps};

    const READY_NODE_DOMAIN: u8 = 8;
    const STEPS_PER_ACTION: usize = 16;
    const FINAL_STEPS: usize = 32;

    #[derive(Clone, Debug)]
    enum LifecycleAction {
        Tick,
        RuntimeReady { node_id: u8, readiness_id: u8 },
        RuntimeReadyAck { node_id: u8, readiness_id: u8 },
        PluginObserved { node_id: u8 },
        PluginFailed { node_id: u8, code: u8 },
        PluginDisconnected,
        Stop,
        ManualFlushed,
        ManualRejected(u8),
        ManualTimedOut,
        FlushTimeout,
        Abort(u8),
    }

    fn lifecycle_actions() -> impl Strategy<Value = Vec<LifecycleAction>> {
        prop::collection::vec(
            prop_oneof![
                4 => Just(LifecycleAction::Tick),
                4 => (0_u8..READY_NODE_DOMAIN, any::<u8>()).prop_map(
                    |(node_id, readiness_id)| LifecycleAction::RuntimeReady {
                        node_id,
                        readiness_id,
                    }
                ),
                3 => (0_u8..READY_NODE_DOMAIN, any::<u8>()).prop_map(
                    |(node_id, readiness_id)| LifecycleAction::RuntimeReadyAck {
                        node_id,
                        readiness_id,
                    }
                ),
                3 => (0_u8..READY_NODE_DOMAIN)
                    .prop_map(|node_id| LifecycleAction::PluginObserved { node_id }),
                3 => (0_u8..READY_NODE_DOMAIN, any::<u8>()).prop_map(
                    |(node_id, code)| LifecycleAction::PluginFailed { node_id, code }
                ),
                2 => Just(LifecycleAction::PluginDisconnected),
                3 => Just(LifecycleAction::Stop),
                2 => Just(LifecycleAction::ManualFlushed),
                2 => any::<u8>().prop_map(LifecycleAction::ManualRejected),
                2 => Just(LifecycleAction::ManualTimedOut),
                2 => Just(LifecycleAction::FlushTimeout),
                2 => any::<u8>().prop_map(LifecycleAction::Abort),
            ],
            0..=32,
        )
    }

    #[derive(Clone, Debug)]
    enum LifecycleHarnessMsg {
        Action(LifecycleAction),
        ScheduledFlushTimeout,
        Finalize,
    }

    #[derive(Clone, Debug)]
    struct LifecycleTrace {
        state: ServeClusterState,
        pending_readies: Vec<(u64, u64)>,
        max_pending_readies: usize,
        completion_results: Vec<Result<(), String>>,
        completion_publication_failures: usize,
        pumps: usize,
        readies_tracked: usize,
        readies_acknowledged: usize,
        provider_failures: Vec<(u64, String)>,
        disconnected_observations: usize,
        manual_replies: Vec<&'static str>,
        flushes_started: usize,
        flush_timers_scheduled: usize,
    }

    impl LifecycleTrace {
        fn new() -> Self {
            Self {
                state: ServeClusterState::Running,
                pending_readies: Vec::new(),
                max_pending_readies: 0,
                completion_results: Vec::new(),
                completion_publication_failures: 0,
                pumps: 0,
                readies_tracked: 0,
                readies_acknowledged: 0,
                provider_failures: Vec::new(),
                disconnected_observations: 0,
                manual_replies: Vec::new(),
                flushes_started: 0,
                flush_timers_scheduled: 0,
            }
        }
    }

    struct ServeClusterLifecycleHarness {
        lifecycle: ServeClusterLifecycle<()>,
        completion: ActorCompletion<Result<(), String>>,
        trace: Arc<Mutex<LifecycleTrace>>,
        engine: EngineHandle,
        sender: ExternalSender,
    }

    impl ServeClusterLifecycleHarness {
        fn record_state(&self) {
            let pending_readies = self
                .lifecycle
                .pending_readies
                .iter()
                .map(|(node_id, pending)| (*node_id, pending.readiness_id))
                .collect::<Vec<_>>();
            let mut trace = self.trace.lock();
            trace.state = self.lifecycle.state;
            trace.max_pending_readies = trace.max_pending_readies.max(pending_readies.len());
            trace.pending_readies = pending_readies;
        }

        fn apply_effect(&mut self, ctx: &Ctx, effect: ServeClusterEffect) {
            match effect {
                ServeClusterEffect::None => {}
                ServeClusterEffect::Pump => {
                    self.trace.lock().pumps += 1;
                }
                ServeClusterEffect::BeginFlush => {
                    {
                        let mut trace = self.trace.lock();
                        trace.flushes_started += 1;
                        trace.flush_timers_scheduled += 1;
                    }
                    self.engine.send_after(
                        SHUTDOWN_FLUSH_TIMEOUT,
                        self.sender.clone(),
                        ctx.self_addr(),
                        LifecycleHarnessMsg::ScheduledFlushTimeout,
                    );
                }
                ServeClusterEffect::ProviderTerminalFailure { node_id, error } => {
                    self.trace.lock().provider_failures.push((node_id, error));
                }
                ServeClusterEffect::Complete(result) => {
                    self.trace.lock().completion_results.push(result.clone());
                    if self.completion.complete(result).is_err() {
                        self.trace.lock().completion_publication_failures += 1;
                    }
                }
            }
        }

        fn apply_action(&mut self, ctx: &Ctx, action: LifecycleAction) {
            let effect = match action {
                LifecycleAction::Tick => self.lifecycle.apply_message(ServeClusterMsg::Tick),
                LifecycleAction::RuntimeReady {
                    node_id,
                    readiness_id,
                } => {
                    if self.lifecycle.track_runtime_ready(
                        u64::from(node_id),
                        u64::from(readiness_id),
                        (),
                    ) {
                        self.trace.lock().readies_tracked += 1;
                    }
                    ServeClusterEffect::None
                }
                LifecycleAction::RuntimeReadyAck {
                    node_id,
                    readiness_id,
                } => {
                    if self
                        .lifecycle
                        .acknowledge_runtime_ready(u64::from(node_id), u64::from(readiness_id))
                    {
                        self.trace.lock().readies_acknowledged += 1;
                    }
                    ServeClusterEffect::None
                }
                LifecycleAction::PluginObserved { node_id } => {
                    let _ = node_id;
                    self.lifecycle.plugin_observation(None)
                }
                LifecycleAction::PluginFailed { node_id, code } => {
                    self.lifecycle.plugin_observation(Some((
                        u64::from(node_id),
                        format!("plugin-failed-{code}"),
                    )))
                }
                LifecycleAction::PluginDisconnected => {
                    self.trace.lock().disconnected_observations += 1;
                    self.lifecycle.plugin_observation(None)
                }
                LifecycleAction::Stop => self.lifecycle.apply_message(ServeClusterMsg::Stop),
                LifecycleAction::ManualFlushed => {
                    self.trace.lock().manual_replies.push("flushed");
                    let message = serve_cluster_flush_reply(ManualControlReply::Flushed)
                        .expect("flush reply must be forwarded");
                    self.lifecycle.apply_message(message)
                }
                LifecycleAction::ManualRejected(code) => {
                    self.trace.lock().manual_replies.push("rejected");
                    let message = serve_cluster_flush_reply(ManualControlReply::Rejected(format!(
                        "manual-rejected-{code}"
                    )))
                    .expect("rejected flush reply must be forwarded");
                    self.lifecycle.apply_message(message)
                }
                LifecycleAction::ManualTimedOut => {
                    self.trace.lock().manual_replies.push("timed-out");
                    let message = serve_cluster_flush_reply(ManualControlReply::TimedOut)
                        .expect("timed-out flush reply must be forwarded");
                    self.lifecycle.apply_message(message)
                }
                LifecycleAction::FlushTimeout => {
                    self.lifecycle.apply_message(ServeClusterMsg::FlushTimeout)
                }
                LifecycleAction::Abort(code) => self
                    .lifecycle
                    .apply_message(ServeClusterMsg::Abort(format!("abort-{code}"))),
            };
            self.apply_effect(ctx, effect);
            self.record_state();
        }
    }

    impl ActorInterface for ServeClusterLifecycleHarness {
        type Incoming = LifecycleHarnessMsg;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
            match message {
                LifecycleHarnessMsg::Action(action) => self.apply_action(ctx, action),
                LifecycleHarnessMsg::ScheduledFlushTimeout => {
                    let effect = self.lifecycle.apply_message(ServeClusterMsg::FlushTimeout);
                    self.apply_effect(ctx, effect);
                    self.record_state();
                }
                LifecycleHarnessMsg::Finalize => {
                    if !matches!(self.lifecycle.state, ServeClusterState::Finished) {
                        let effect = self.lifecycle.apply_message(ServeClusterMsg::Abort(
                            "generated sequence exhausted".to_owned(),
                        ));
                        self.apply_effect(ctx, effect);
                    }
                    self.record_state();
                    ctx.stop_self();
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    struct ExpectedLifecycle {
        success: bool,
        readies_tracked: usize,
        readies_acknowledged: usize,
        provider_failures: usize,
        manual_replies: usize,
        flushes_started: usize,
    }

    fn expected_lifecycle(actions: &[LifecycleAction]) -> ExpectedLifecycle {
        let mut state = ServeClusterState::Running;
        let mut pending_readies = BTreeMap::<u64, u64>::new();
        let mut result = None;
        let mut readies_tracked = 0;
        let mut readies_acknowledged = 0;
        let mut provider_failures = 0;
        let mut manual_replies = 0;
        let mut flushes_started = 0;

        for action in actions {
            match action {
                LifecycleAction::Tick | LifecycleAction::PluginObserved { .. } => {}
                LifecycleAction::RuntimeReady {
                    node_id,
                    readiness_id,
                } if matches!(state, ServeClusterState::Running) => {
                    let node_id = u64::from(*node_id);
                    let readiness_id = u64::from(*readiness_id);
                    let newer = pending_readies
                        .get(&node_id)
                        .is_none_or(|current| *current < readiness_id);
                    if newer {
                        pending_readies.insert(node_id, readiness_id);
                        readies_tracked += 1;
                    }
                }
                LifecycleAction::RuntimeReadyAck {
                    node_id,
                    readiness_id,
                } => {
                    let node_id = u64::from(*node_id);
                    let readiness_id = u64::from(*readiness_id);
                    if pending_readies.get(&node_id) == Some(&readiness_id) {
                        pending_readies.remove(&node_id);
                        readies_acknowledged += 1;
                    }
                }
                LifecycleAction::PluginFailed { .. }
                    if matches!(state, ServeClusterState::Running) =>
                {
                    provider_failures += 1;
                }
                LifecycleAction::PluginDisconnected
                | LifecycleAction::PluginFailed { .. }
                | LifecycleAction::RuntimeReady { .. } => {}
                LifecycleAction::Stop if matches!(state, ServeClusterState::Running) => {
                    state = ServeClusterState::Flushing;
                    pending_readies.clear();
                    flushes_started += 1;
                }
                LifecycleAction::ManualFlushed => {
                    manual_replies += 1;
                    if matches!(state, ServeClusterState::Flushing) {
                        state = ServeClusterState::Finished;
                        pending_readies.clear();
                        result = Some(true);
                    }
                }
                LifecycleAction::ManualRejected(_) | LifecycleAction::ManualTimedOut => {
                    manual_replies += 1;
                    if matches!(state, ServeClusterState::Flushing) {
                        state = ServeClusterState::Finished;
                        pending_readies.clear();
                        result = Some(false);
                    }
                }
                LifecycleAction::FlushTimeout if matches!(state, ServeClusterState::Flushing) => {
                    state = ServeClusterState::Finished;
                    pending_readies.clear();
                    result = Some(false);
                }
                LifecycleAction::Abort(_) if !matches!(state, ServeClusterState::Finished) => {
                    state = ServeClusterState::Finished;
                    pending_readies.clear();
                    result = Some(false);
                }
                LifecycleAction::Stop
                | LifecycleAction::FlushTimeout
                | LifecycleAction::Abort(_) => {}
            }
        }

        if matches!(state, ServeClusterState::Flushing) {
            result = Some(false);
        }
        ExpectedLifecycle {
            success: result.unwrap_or(false),
            readies_tracked,
            readies_acknowledged,
            provider_failures,
            manual_replies,
            flushes_started,
        }
    }

    #[derive(Clone, Debug)]
    struct LifecycleResources {
        baseline_actors: usize,
        peak_actors: usize,
        final_actors: usize,
        baseline_tasks: usize,
        peak_tasks: usize,
        final_tasks: usize,
        poisoned_actors: usize,
        worker_panics: u64,
        mailbox_depth: usize,
    }

    impl LifecycleResources {
        fn new(baseline_actors: usize, baseline_tasks: usize) -> Self {
            Self {
                baseline_actors,
                peak_actors: baseline_actors,
                final_actors: baseline_actors,
                baseline_tasks,
                peak_tasks: baseline_tasks,
                final_tasks: baseline_tasks,
                poisoned_actors: 0,
                worker_panics: 0,
                mailbox_depth: 0,
            }
        }

        fn observe(&mut self, runtime: &Runtime, backend: &SteppingBackend) {
            let stats = runtime.stats();
            self.peak_actors = self.peak_actors.max(stats.actors.len());
            self.final_actors = stats.actors.len();
            self.peak_tasks = self.peak_tasks.max(backend.pending_task_count());
            self.final_tasks = backend.pending_task_count();
            self.poisoned_actors = stats
                .actor_details
                .iter()
                .filter(|actor| actor.poisoned)
                .count();
            self.worker_panics = stats.workers.iter().map(|worker| worker.panics).sum();
            self.mailbox_depth = stats
                .workers
                .iter()
                .map(|worker| worker.mailbox_depth)
                .sum::<usize>()
                + stats
                    .actor_details
                    .iter()
                    .map(|actor| actor.mailbox_depth)
                    .sum::<usize>();
        }
    }

    fn lifecycle_invariant_errors(
        trace: &LifecycleTrace,
        resources: &LifecycleResources,
        expected: &ExpectedLifecycle,
        published_result: &Option<Result<(), String>>,
    ) -> Vec<String> {
        let mut errors = Vec::new();
        if trace.state != ServeClusterState::Finished {
            errors.push(format!(
                "lifecycle did not converge: state={:?}",
                trace.state
            ));
        }
        if !trace.pending_readies.is_empty() {
            errors.push(format!(
                "pending readiness did not drain: {:?}",
                trace.pending_readies
            ));
        }
        if trace.max_pending_readies > usize::from(READY_NODE_DOMAIN) {
            errors.push(format!(
                "pending readiness exceeded node domain: max={} domain={}",
                trace.max_pending_readies, READY_NODE_DOMAIN
            ));
        }
        if trace.completion_results.len() != 1 {
            errors.push(format!(
                "expected one completion, observed {:?}",
                trace.completion_results
            ));
        }
        if trace.completion_publication_failures != 0 {
            errors.push(format!(
                "completion publication failed {} time(s)",
                trace.completion_publication_failures
            ));
        }
        if trace.completion_results.first().map(Result::is_ok) != Some(expected.success) {
            errors.push(format!(
                "completion/model mismatch: completion={:?} expected_success={}",
                trace.completion_results, expected.success
            ));
        }
        if published_result.as_ref() != trace.completion_results.first() {
            errors.push(format!(
                "published completion mismatch: published={published_result:?} trace={:?}",
                trace.completion_results
            ));
        }
        if trace.readies_tracked != expected.readies_tracked
            || trace.readies_acknowledged != expected.readies_acknowledged
        {
            errors.push(format!(
                "readiness model mismatch: tracked={}/{} acknowledged={}/{}",
                trace.readies_tracked,
                expected.readies_tracked,
                trace.readies_acknowledged,
                expected.readies_acknowledged
            ));
        }
        if trace.provider_failures.len() != expected.provider_failures {
            errors.push(format!(
                "manual provider-failure forwarding mismatch: observed={:?} expected_count={}",
                trace.provider_failures, expected.provider_failures
            ));
        }
        if trace.manual_replies.len() != expected.manual_replies {
            errors.push(format!(
                "manual reply evidence mismatch: observed={:?} expected_count={}",
                trace.manual_replies, expected.manual_replies
            ));
        }
        if trace.flushes_started != expected.flushes_started
            || trace.flush_timers_scheduled != trace.flushes_started
            || trace.flushes_started > 1
        {
            errors.push(format!(
                "flush state was not bounded: starts={} timers={} expected_starts={}",
                trace.flushes_started, trace.flush_timers_scheduled, expected.flushes_started
            ));
        }
        if resources.peak_actors > resources.baseline_actors.saturating_add(1) {
            errors.push(format!(
                "per-event actor growth: baseline={} peak={}",
                resources.baseline_actors, resources.peak_actors
            ));
        }
        if resources.final_actors != resources.baseline_actors {
            errors.push(format!(
                "actor census did not return to baseline: baseline={} final={}",
                resources.baseline_actors, resources.final_actors
            ));
        }
        if resources.peak_tasks > resources.baseline_tasks.saturating_add(1) {
            errors.push(format!(
                "pending tasks were unbounded: baseline={} peak={}",
                resources.baseline_tasks, resources.peak_tasks
            ));
        }
        if resources.final_tasks != resources.baseline_tasks {
            errors.push(format!(
                "pending tasks did not drain: baseline={} final={}",
                resources.baseline_tasks, resources.final_tasks
            ));
        }
        if resources.poisoned_actors != 0 || resources.worker_panics != 0 {
            errors.push(format!(
                "actor poison detected: poisoned={} worker_panics={}",
                resources.poisoned_actors, resources.worker_panics
            ));
        }
        if resources.mailbox_depth != 0 {
            errors.push(format!(
                "mailboxes did not drain: depth={}",
                resources.mailbox_depth
            ));
        }
        errors
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn serve_cluster_production_transitions_converge_once_without_growth(
            actions in lifecycle_actions()
        ) {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).expect("one-worker stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let completion = ActorCompletion::new();
            let trace = Arc::new(Mutex::new(LifecycleTrace::new()));
            let actor = runtime
                .spawn(ServeClusterLifecycleHarness {
                    lifecycle: ServeClusterLifecycle::new(),
                    completion: completion.clone(),
                    trace: Arc::clone(&trace),
                    engine: engine.handle(),
                    sender: runtime.create_sender(),
                })
                .expect("spawn production lifecycle transition harness");
            let mut resources = LifecycleResources::new(baseline_actors, baseline_tasks);
            resources.observe(&runtime, &backend);

            for action in &actions {
                runtime
                    .send_to(actor, LifecycleHarnessMsg::Action(action.clone()))
                    .expect("queue generated serve-cluster action");
                drive_steps(&backend, STEPS_PER_ACTION);
                resources.observe(&runtime, &backend);
            }

            backend.advance_time(SHUTDOWN_FLUSH_TIMEOUT);
            drive_steps(&backend, FINAL_STEPS);
            resources.observe(&runtime, &backend);
            runtime
                .send_to(actor, LifecycleHarnessMsg::Finalize)
                .expect("queue serve-cluster lifecycle finalizer");
            drive_steps(&backend, FINAL_STEPS);
            resources.observe(&runtime, &backend);

            let trace = trace.lock().clone();
            let published_result = if trace.completion_results.len() == 1
                && trace.completion_publication_failures == 0
            {
                Some(completion.wait())
            } else {
                None
            };
            let expected = expected_lifecycle(&actions);
            let errors =
                lifecycle_invariant_errors(&trace, &resources, &expected, &published_result);
            prop_assert!(
                errors.is_empty(),
                "serve-cluster lifecycle invariant failure\nerrors={errors:#?}\nactions={actions:#?}\nstate/replies={trace:#?}\nresources={resources:#?}\ncensus=\n{}",
                actor_census(&runtime),
            );
        }
    }

    #[test]
    fn serve_cluster_lifecycle_invariants_reject_injected_duplicate_and_growth() {
        let mut trace = LifecycleTrace::new();
        trace.state = ServeClusterState::Finished;
        trace.completion_results = vec![
            Err("injected-first".to_owned()),
            Err("injected-duplicate".to_owned()),
        ];
        trace.completion_publication_failures = 1;
        let expected = ExpectedLifecycle {
            success: false,
            readies_tracked: 0,
            readies_acknowledged: 0,
            provider_failures: 0,
            manual_replies: 0,
            flushes_started: 0,
        };
        let resources = LifecycleResources {
            baseline_actors: 3,
            peak_actors: 5,
            final_actors: 3,
            baseline_tasks: 0,
            peak_tasks: 0,
            final_tasks: 0,
            poisoned_actors: 0,
            worker_panics: 0,
            mailbox_depth: 0,
        };
        let published_result = Some(Err("injected-first".to_owned()));

        let errors = lifecycle_invariant_errors(&trace, &resources, &expected, &published_result);
        assert!(
            errors.iter().any(|error| error.contains("one completion"))
                && errors
                    .iter()
                    .any(|error| error.contains("per-event actor growth")),
            "controlled defects were not rejected by property invariants: {errors:#?}"
        );
    }
}

#[cfg(test)]
mod lifecycle_policy_tests {
    use super::{ActorAddress, ConfigBuilder, EndpointAddr, VastAiProvisioningMode};
    #[test]
    fn vastai_mock_mode_needs_no_ssh_or_bootstrap_configuration() {
        let mut config = ConfigBuilder::hardcoded_defaults()
            .overlay_cli([
                "--provider".to_owned(),
                "vastai".to_owned(),
                "--vastai-provisioning".to_owned(),
                "mock".to_owned(),
            ])
            .unwrap()
            .finalize()
            .unwrap();

        config.prepare_vastai_ssh_key().unwrap();
        let vastai = config.vastai.as_ref().unwrap();
        assert_eq!(vastai.provisioning_mode, VastAiProvisioningMode::Mock);
        assert!(vastai.ssh_identity.is_none());
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[7; 32]).public());
        let spec = config
            .node_spec_for_stage(coordinator, ActorAddress::default(), 1, 0)
            .unwrap();
        assert_eq!(spec.args, [crate::ORCHESTRATOR_WORKER_MODE_ARG]);
        let attempt_id = spec.attempt_id.to_string();
        assert_eq!(
            spec.env.iter().find_map(
                |(key, value)| (key == "MYELIN_NODE_ATTEMPT_ID").then_some(value.as_str())
            ),
            Some(attempt_id.as_str()),
        );
        assert_eq!(
            spec.env
                .iter()
                .find_map(|(key, value)| (key == "MYELIN_DOCKER_GPUS").then_some(value.as_str())),
            Some("all"),
        );
        assert!(vastai.bootstrap_command.is_none());
    }

    #[test]
    fn vastai_real_specs_do_not_persist_orchestrator_routing() {
        let mut config = ConfigBuilder::hardcoded_defaults()
            .overlay_cli([
                "--provider".to_owned(),
                "vastai".to_owned(),
                "--vastai-provisioning".to_owned(),
                "mock".to_owned(),
            ])
            .unwrap()
            .finalize()
            .unwrap();
        let vastai = config.vastai.as_mut().unwrap();
        vastai.provisioning_mode = VastAiProvisioningMode::Real;
        vastai.bootstrap_command = Some("exec /usr/local/bin/myelin-node".to_owned());
        let coordinator = EndpointAddr::new(iroh::SecretKey::from_bytes(&[8; 32]).public());

        let spec = config
            .node_spec_for_stage(coordinator, ActorAddress::default(), 1, 0)
            .unwrap();

        assert!(spec.env.iter().all(|(key, _)| {
            key != "MYELIN_COORDINATOR_ENDPOINT" && key != "MYELIN_ORCHESTRATOR_ACTOR"
        }));
    }

    #[test]
    fn vastai_mock_alias_selects_docker_backed_mock_mode() {
        let config = ConfigBuilder::hardcoded_defaults()
            .overlay_cli([
                "--provider".to_owned(),
                "vastai".to_owned(),
                "--vastai-provisioning-mock".to_owned(),
                "--gpus".to_owned(),
                "device=0".to_owned(),
            ])
            .unwrap()
            .finalize()
            .unwrap();

        assert_eq!(
            config.vastai.as_ref().unwrap().provisioning_mode,
            VastAiProvisioningMode::Mock
        );
        assert_eq!(
            config.extra_worker_env().iter().find_map(|(key, value)| {
                (key == "MYELIN_DOCKER_GPUS").then_some(value.as_str())
            }),
            Some("device=0"),
        );
    }

    #[test]
    fn vastai_provisioning_mode_rejects_unknown_values() {
        let error = ConfigBuilder::hardcoded_defaults()
            .overlay_cli([
                "--provider".to_owned(),
                "vastai".to_owned(),
                "--vastai-provisioning".to_owned(),
                "imaginary".to_owned(),
            ])
            .err()
            .unwrap();
        assert!(error.contains("imaginary"));
        assert!(error.contains("mock"));
    }
}
