use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, mpsc};

use crate::DEFAULT_PIPELINE_CACHED_MODEL_FILE;
use crate::codecs::register_myelin_actor_codecs;
use crate::orchestration::actor::{
    ContextualArtifactCleanup, OrchestratorActor, OrchestratorMsg, OrchestratorReport,
};
use crate::orchestration::config::{DEFAULT_CONFIG_PATH, TomlConfigOverlay};
use crate::orchestration::control;
use crate::orchestration::daemon;
use crate::orchestration::manual_control::{
    CONTROL_REGISTRY_NAME, ConfigValidator, ManualActorControl, ManualActorControlConfig,
    ManualControl, ManualControlMsg, ManualControlReply, NodePhase, OfferDto, OfferSearchRequest,
    OfferSearchRequestExt, OfferSearcher, ProviderConfigurationRequest, ProviderFactory,
    ProviderReadiness, SpecBuilder,
};
use crate::orchestration::telemetry::{
    BootstrapEmission, DashboardSupport, MYELIN_MEMBERSHIP, OrchTelemetry,
};

use crate::node_actor::NodeAgentMsg;
use crate::node_provisioning::{ProviderKind, provider_kind};
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::orchestration::provider_adapters::relay::{
    MYELIN_IROH_RELAY_URL_ENV, RelayRuntimeConfig, SWACTOR_IROH_RELAY_URL_ENV,
    relay_mode_env_value, relay_runtime_config_from_settings,
};
use crate::orchestration::provider_adapters::ssh_bootstrap::{
    BootstrapEnvSource, DEPLOYED_WORKER_BIN_PATH, SshArtifactBootstrapLauncher,
    SshCommandBootstrapLauncher, read_bundle_identity,
};
use crate::orchestration::provider_adapters::vastai::webhooks::{
    VastAiWebhookConfig, VastAiWebhooks,
};
use crate::orchestration::provider_adapters::vastai::{
    ToolsVastAiLeaseClient, VastAiProvisioningConfig, VastAiProvisioningPlugin, VastAiTaskPort,
};
use crate::provisioning::{
    LocalDockerPlugin, LocalProcessPlugin, NodeProvisionSpec, PluginObservation,
    PluginObservationSink, PluginSink, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use crate::run_fsm::{RunConfig, RunId};
use crate::run_plan::{GgufSource, TokenizerSource};
use distribution::directory_actor::DirectoryIn;
use distribution::node::DistributedNodeConfig;
use distribution::registry_actor::RegistryIn;
use distribution::transport_bridge::RouteBinder;
use distribution::types::{MemberState, NodeId as DistNodeId};
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{EDGE_ALPN, IrohDriver, IrohDriverConfig, TELEMETRY_ALPN};
use iroh_driver::{EndpointAddrMask, MVP_IROH_ENDPOINT_ADDR_MASK_ENV, advertised_endpoint};
use parking_lot::Mutex;
use serde_json::{Value, json};
use swactor::actor::{ActorAddress, ActorExited, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime, RuntimeParts};
use swactor::std::CtxWatching;
use swactor_engine::{ActorCompletion, BlockingWorkSender, Engine, EngineHandle, TaskSpawner};
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

fn spawn_orchestrator_hardware_samplers(
    tasks: &TaskSpawner,
    engine: &EngineHandle,
    telemetry: &mut OrchTelemetry,
) -> tokio::sync::mpsc::Sender<()> {
    let cpu_channel = telemetry.record_channel::<telemetry::hardware::cpu::HostCpuSample>();
    let gpu_channel = telemetry.record_channel::<telemetry::hardware::gpu::HostGpuSample>();
    let memory_channel =
        telemetry.record_channel::<telemetry::hardware::memory::HostMemorySample>();
    let net_channel = telemetry.record_channel::<telemetry::hardware::net::HostNetSample>();
    let storage_channel =
        telemetry.record_channel::<telemetry::hardware::storage::HostStorageSample>();
    let producer = telemetry.producer();
    let endpoint = telemetry.endpoint();
    let blocking = engine.blocking_work_sender();
    let (requests, mut incoming) = tokio::sync::mpsc::channel(1);
    let _ = requests.try_send(());
    tasks.spawn(async move {
        let mut cpu = telemetry::hardware::cpu::CpuSampler::new([std::process::id()]);
        let mut seq = 0_u64;
        while incoming.recv().await.is_some() {
            let (complete, result) = tokio::sync::oneshot::channel();
            if blocking
                .submit(Box::new(move || {
                    let samples = (
                        cpu.sample(seq),
                        telemetry::hardware::gpu::sample(seq),
                        telemetry::hardware::memory::sample(seq),
                        telemetry::hardware::net::sample(seq),
                        telemetry::hardware::storage::sample(seq),
                    );
                    let _ = complete.send((cpu, samples));
                }))
                .is_err()
            {
                return;
            }
            let Ok((sampler, (cpu_sample, gpu, memory, net, storage))) = result.await else {
                return;
            };
            cpu = sampler;
            producer.submit_record(cpu_channel, &cpu_sample);
            producer.submit_record(gpu_channel, &gpu);
            producer.submit_record(memory_channel, &memory);
            producer.submit_record(net_channel, &net);
            producer.submit_record(storage_channel, &storage);
            endpoint.tick();
            let Some(next_seq) = seq.checked_add(1) else {
                return;
            };
            seq = next_seq;
        }
    });
    requests
}

/// Prepare configuration, telemetry, and core workers without creating task authority.
///
/// Binary/test roots construct the engine from these parts, mint its task
/// capability, and pass both to the continuation for the complete application run.
/// Runtime capabilities supplied to an ordinary provisioning adapter.
#[derive(Clone)]
pub struct ProviderContext {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub tasks: VastAiTaskPort,
    pub process_registry_path: PathBuf,
    pub bootstrap_env: BootstrapEnvSource,
}

/// An application composition entry; fixture policy belongs to its caller.
#[derive(Clone)]
pub struct ProviderComposition {
    pub kind: ProviderKind,
    pub readiness: myelin_control_contract::ProviderReadiness,
    pub remote: bool,
    pub http_plugin:
        Option<Arc<dyn Fn(BlockingWorkSender) -> dashboard::DashboardPlugin + Send + Sync>>,
    pub build:
        Arc<dyn Fn(ProviderContext) -> Result<Box<dyn ProvisionPlugin>, String> + Send + Sync>,
    pub prepare_spec: Arc<dyn Fn(&mut NodeProvisionSpec) -> Result<(), String> + Send + Sync>,
    pub configure: Option<
        Arc<
            dyn Fn(myelin_control_contract::ProviderConfigurationRequest) -> Result<(), String>
                + Send
                + Sync,
        >,
    >,
    pub search_offers: Option<
        Arc<
            dyn Fn(
                    myelin_control_contract::OfferSearchRequest,
                ) -> Result<Vec<myelin_control_contract::Offer>, String>
                + Send
                + Sync,
        >,
    >,
}

pub fn prepare_with_options<I>(
    args: I,
    capture_stdio: bool,
    stop_rx: Option<mpsc::Receiver<()>>,
) -> Result<
    (
        RuntimeParts,
        impl FnOnce(Engine, TaskSpawner) -> Result<(), String>,
    ),
    String,
>
where
    I: IntoIterator<Item = String>,
{
    prepare_with_providers(args, capture_stdio, Vec::new(), stop_rx)
}

pub fn prepare_with_providers<I>(
    args: I,
    capture_stdio: bool,
    providers: Vec<ProviderComposition>,
    stop_rx: Option<mpsc::Receiver<()>>,
) -> Result<
    (
        RuntimeParts,
        impl FnOnce(Engine, TaskSpawner) -> Result<(), String>,
    ),
    String,
>
where
    I: IntoIterator<Item = String>,
{
    let mut config_builder = ConfigBuilder::hardcoded_defaults();
    config_builder.providers = providers;
    if let Some(overlay) = TomlConfigOverlay::load_optional(Path::new(DEFAULT_CONFIG_PATH))? {
        config_builder = config_builder.overlay_toml(overlay)?;
    }
    let mut config = config_builder
        .overlay_env()?
        .overlay_cli(args)?
        .finalize()?;
    let state_dir = daemon::StateDir::new(config.state_dir.clone());
    let data_namespace_path = config.state_dir.join("data-namespace.json");
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
    if config
        .provider_composition
        .as_ref()
        .is_some_and(|provider| provider.http_plugin.is_some())
    {
        if !config.dashboard {
            return Err("provider HTTP routes require MYELIN_DASHBOARD".to_owned());
        }
        DashboardSupport::configured_url(config.dashboard)?;
    }
    if config.provider.as_str() == "vastai" && config.provider_composition.is_none() {
        if !config.dashboard {
            return Err(
                "Vast.ai webhooks require MYELIN_DASHBOARD to serve their receiver route"
                    .to_owned(),
            );
        }
        DashboardSupport::configured_url(config.dashboard)?;
        let webhook_config =
            VastAiWebhookConfig::from_env(config.state_dir.join("vastai-webhooks"))?;
        let vastai = config
            .vastai
            .as_mut()
            .expect("built-in Vast.ai configuration");
        vastai.webhooks = Some(VastAiWebhooks::open(
            webhook_config,
            vastai.api_key.as_deref().unwrap_or(""),
        )?);
    }
    let readiness = if let Some(provider) = &config.provider_composition {
        let mut readiness = provider.readiness.clone();
        if provider.http_plugin.is_some() {
            readiness.kind = myelin_control_contract::ProviderReadinessKind::Validating;
        }
        readiness
    } else if config.provider.as_str() == "vastai" {
        ProviderReadiness::unconfigured_for(
            config.provider.as_str(),
            config.image.clone(),
            "submit a Vast.ai API key, SSH identity, and bootstrap command",
        )
    } else {
        ProviderReadiness::ready_for(config.provider.as_str(), config.image.clone())
    };
    let mut manual_core = ManualControl::new(snapshot, readiness);
    let deployment_refresh_nodes = manual_core.reconcile_active_deployment_intent(
        config.deployment.as_ref().map(|bundle| &bundle.identity),
    );
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
    let mut orch_telemetry = OrchTelemetry::new(config.run_id, &config.state_dir)?;
    let run_id = config.run_id;
    let node_id = config.node_id;
    let bootstrap = move |ds: &mut OrchTelemetry, phase: &str, status: &str, detail: Value| {
        ds.emit_bootstrap(run_id, node_id, phase, status, detail);
    };
    bootstrap(
        &mut orch_telemetry,
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
            "deployment_refresh_nodes":&deployment_refresh_nodes,
        }),
    );
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
    Ok((parts, move |engine: Engine, tasks: TaskSpawner| {
        bootstrap(
            &mut orch_telemetry,
            "engine",
            "ready",
            json!({"backend":"tokio","owns":"core+substrate"}),
        );
        let hardware_samples =
            spawn_orchestrator_hardware_samplers(&tasks, &engine.handle(), &mut orch_telemetry);
        let mut driver = match IrohDriver::with_engine(
            tasks.clone(),
            engine.handle(),
            IrohDriverConfig {
                secret_key: Some(identity),
                relay_mode: config.relay.mode.clone(),
                bind_port: config.endpoint_bind_port,
                node: DistributedNodeConfig::default(),
                peer_auth: None,
                additional_alpns: vec![EDGE_ALPN.to_vec(), TELEMETRY_ALPN.to_vec()],
            },
        ) {
            Ok(driver) => driver,
            Err(error) => {
                bootstrap(
                    &mut orch_telemetry,
                    "iroh_driver",
                    "failed",
                    json!({"error":error.to_string()}),
                );
                return Err(format!("create iroh driver: {error}"));
            }
        };
        driver.retain_telemetry_connections();
        let remote_vast_relay_required = config
            .provider_composition
            .as_ref()
            .map_or(config.provider.as_str() == "vastai", |provider| {
                provider.remote
            })
            && !matches!(config.relay.mode, RelayMode::Disabled);
        if remote_vast_relay_required {
            bootstrap(
                &mut orch_telemetry,
                "iroh_relay",
                "waiting",
                json!({"relay_mode":format!("{:?}", config.relay.mode)}),
            );
            let endpoint = driver.wait_for_relay_endpoint()?;
            bootstrap(
                &mut orch_telemetry,
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
            "iroh_driver",
            "ready",
            json!({"endpoint":coordinator_endpoint.clone(),"has_relay":coordinator_endpoint.relay_urls().next().is_some(),"direct_addr_count":coordinator_endpoint.ip_addrs().count(),"relay_mode":format!("{:?}", config.relay.mode),"endpoint_addr_mask":config.endpoint_addr_mask.as_str()}),
        );
        bootstrap(
            &mut orch_telemetry,
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
        let ingress = ServeClusterIngress::new(stack.runtime.create_sender());
        let _ingress_lifetime = ServeClusterIngressLifetime(&ingress);
        let orch_stdio_actor = orch_stdio_capture
            .map(|capture| {
                capture.start(
                    &engine.handle().blocking_work_sender(),
                    &stack.runtime,
                    ingress.clone(),
                )
            })
            .transpose()?;
        bootstrap(
            &mut orch_telemetry,
            "distribution_stack",
            "ready",
            json!({"actors":"initialized","route_view":"initialized","membership":"transport-events"}),
        );
        bootstrap(
            &mut orch_telemetry,
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
        let snapshot = manual_core.snapshot();
        let recovery_run_id = snapshot.run_id;
        let recovery_swim_peers = snapshot
            .nodes
            .iter()
            .filter(|node| !matches!(node.phase, NodePhase::Stopped))
            .filter_map(|node| node.runtime.as_ref().map(|runtime| runtime.swim_node_id))
            .collect::<Vec<_>>();
        let data_namespace = crate::data_namespace::DataNamespaceAuthority::start(
            &stack,
            &driver,
            data_namespace_path,
            recovery_swim_peers,
        )?;
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
                    .map(|endpoint| {
                        (
                            node_id,
                            endpoint,
                            (runtime.node_actor, runtime.swim_node_id),
                        )
                    })
                    .map_err(|error| {
                        format!("parse persisted endpoint for node {node_id}: {error}")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (_, _, (node_actor, swim_node_id)) in &recovery_nodes {
            if let Ok(mut pinned) = stack.pinned_routes.write() {
                pinned.insert(*node_actor, *swim_node_id);
            }
            if let Ok(mut routes) = stack.route_view.write() {
                routes.insert(*node_actor, *swim_node_id);
            }
            stack.route_binder.ensure_routable(*node_actor);
        }
        if !recovery_nodes.is_empty() {
            driver.join(
                &recovery_nodes
                    .iter()
                    .map(|(_, endpoint, _)| endpoint.clone())
                    .collect::<Vec<_>>(),
            );
            for (node_id, _, (node_actor, _)) in &recovery_nodes {
                // Dial each worker directly (fresh endpoint) in addition to the
                // join above: a restarted orchestrator has new direct addresses,
                // while every worker still caches the pre-restart endpoint for
                // redials. Without this push, worker egress to the orchestrator
                // black-holes until endpoint gossip rediscovers the fresh
                // addresses, stranding namespace clients during recovery.
                let mut dial_targets = vec![coordinator_endpoint.clone()];
                dial_targets.extend(
                    recovery_nodes
                        .iter()
                        .filter(|(peer_id, _, _)| peer_id != node_id)
                        .map(|(_, endpoint, _)| endpoint.clone()),
                );
                let _ = stack.runtime.send_to(
                    *node_actor,
                    NodeAgentMsg::JoinPeers {
                        endpoints: dial_targets,
                    },
                );
                let _ = stack
                    .runtime
                    .send_to(*node_actor, NodeAgentMsg::ResyncRoutes);
            }
        }
        driver.install_actor_bridge();
        bootstrap(
            &mut orch_telemetry,
            "actor_bridge",
            "ready",
            json!({"transport":"iroh","routes":"attached","actor_bridge":"event-driven"}),
        );

        let dashboard: Option<DashboardSupport>;

        let orchestrator_report_actor = stack
            .runtime
            .spawn(OrchestratorReportForwarder {
                ingress: ingress.clone(),
            })
            .map_err(|error| format!("spawn orchestrator report actor: {error}"))?;
        stack.register_local_actor(driver.register_actor(orchestrator_report_actor, 1));

        let sink = PluginSink::new(Arc::new(ChannelObservationSink {
            ingress: ingress.clone(),
        }));
        let shared_config = Arc::new(Mutex::new(config.clone()));
        let live_orchestrator_actor = Arc::new(Mutex::new(None::<ActorAddress>));
        let bootstrap_route_config = Arc::clone(&shared_config);
        let bootstrap_route_actor = Arc::clone(&live_orchestrator_actor);
        let bootstrap_env_source: BootstrapEnvSource = Arc::new(move || {
            let config = bootstrap_route_config.lock();
            let endpoint =
                advertised_endpoint(live_coordinator_endpoint(), config.endpoint_addr_mask)?;
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
        let provider_tasks = VastAiTaskPort::new(&tasks, engine.handle().blocking_work_sender());
        let validation_tasks = provider_tasks.clone();
        let provider_factory: ProviderFactory = Arc::new(move || {
            provider_config.lock().clone().build_provisioner(
                provider_runtime.clone(),
                provider_engine.clone(),
                provider_tasks.clone(),
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
            let spec = config.node_spec_for_stage(
                spec_coordinator.clone(),
                orchestrator_actor,
                node_id,
                0,
            )?;
            Ok(spec)
        });
        let validation_config = Arc::clone(&shared_config);
        let validation_runtime = stack.runtime.clone();
        let validation_engine = engine.handle();
        let validation_registry = state_dir.process_registry_path();
        let validation_bootstrap_env = Arc::clone(&bootstrap_env_source);
        let config_validator: ConfigValidator = Arc::new(move |request| {
            let mut candidate = validation_config.lock().clone();
            if let Some(provider) = &candidate.provider_composition {
                if let Some(configure) = &provider.configure {
                    configure(request)?;
                }
                let _ = candidate.build_provisioner(
                    validation_runtime.clone(),
                    validation_engine.clone(),
                    validation_tasks.clone(),
                    validation_registry.clone(),
                    Arc::clone(&validation_bootstrap_env),
                )?;
                return Ok(());
            }
            if candidate.provider.as_str() != "vastai" {
                return Ok(());
            }
            let vastai = candidate
                .vastai
                .as_mut()
                .ok_or_else(|| "Vast.ai configuration is unavailable".to_owned())?;
            if let Some(api_key) = request.api_key
                && vastai.api_key.as_deref() != Some(api_key.as_str())
            {
                if vastai.webhooks.is_some() {
                    return Err("changing the Vast.ai API key requires restarting with matching webhook credentials".to_owned());
                }
                vastai.client = Some(swactor_vastai::VastClient::new(&api_key));
                vastai.api_key = Some(api_key);
            }
            if let Some(path) = request.ssh_identity {
                vastai.ssh_identity = Some(expand_home_path(&path)?);
            }
            if let Some(command) = request.bootstrap_command {
                vastai.bootstrap_command = Some(command);
            }

            if vastai.api_key.as_deref().is_none_or(str::is_empty) {
                return Err("Vast.ai API key is required for live offer search".to_owned());
            }
            candidate.prepare_vastai_ssh_key()?;
            let _ = candidate.build_provisioner(
                validation_runtime.clone(),
                validation_engine.clone(),
                validation_tasks.clone(),
                validation_registry.clone(),
                Arc::clone(&validation_bootstrap_env),
            )?;
            *validation_config.lock() = candidate;
            Ok(())
        });
        let offer_searcher: Option<OfferSearcher> =
            if let Some(provider) = &config.provider_composition {
                provider.search_offers.clone()
            } else if config.provider.as_str() == "vastai" {
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
            })
            .or_else(|| {
                config
                    .provider_composition
                    .as_ref()
                    .filter(|provider| provider.http_plugin.is_some())
                    .map(|_| ProviderConfigurationRequest {
                        api_key: None,
                        ssh_identity: None,
                        bootstrap_command: None,
                    })
            });
        let manual = ManualActorControl::new(
            manual_core,
            stack.runtime.clone(),
            stack.engine.blocking_work_sender(),
            ManualActorControlConfig {
                state_dir: state_dir.clone(),
                sink,
                provider_factory,
                spec_builder,
                config_validator: Some(config_validator),
                offer_searcher,
                control_generation: state_dir.allocate_control_generation()?,
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
            )),
        ) {
            Ok(actor) => actor,
            Err(error) => return Err(format!("spawn orchestrator actor: {error}")),
        };
        stack.register_local_actor(driver.register_actor(orchestrator_actor, 1));
        // The live actor handle is a static fact from spawn time; recovery
        // tooling (the control endpoint route) needs it before any SSH
        // bootstrap has driven the spec builder.
        *live_orchestrator_actor.lock() = Some(orchestrator_actor);
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

        let control_plugin = control::plugin(
            stack.runtime.clone(),
            orchestrator_actor,
            data_namespace.control(),
            Arc::clone(&bootstrap_env_source),
        );
        let mut plugins = vec![control_plugin];
        if let Some(webhooks) = config
            .vastai
            .as_ref()
            .and_then(|vastai| vastai.webhooks.as_ref())
        {
            plugins.push(dashboard::DashboardPlugin::new(
                webhooks.router(engine.handle().blocking_work_sender()),
            ));
        }
        if let Some(plugin) = config
            .provider_composition
            .as_ref()
            .and_then(|provider| provider.http_plugin.as_ref())
        {
            plugins.push(plugin(engine.handle().blocking_work_sender()));
        }
        dashboard = DashboardSupport::start_with_plugins(
            config.dashboard,
            &tasks,
            &engine.handle(),
            driver.endpoint(),
            plugins,
        )?;
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
        if let Some(dashboard) = &dashboard {
            dashboard.observe_local(orch_telemetry.endpoint());
            for (node_id, endpoint, _) in &recovery_nodes {
                dashboard.subscribe_node(endpoint.clone(), recovery_run_id, *node_id);
            }
        }
        let peers = driver.peer_connector();
        let route_ingress = ingress.clone();
        let distribution_wake: Arc<dyn Fn() + Send + Sync> =
            Arc::new(move || route_ingress.send(ServeClusterMsg::DistributionChanged));
        stack.watch_membership(Arc::downgrade(&distribution_wake));
        stack
            .runtime
            .send_to(
                stack.actors.directory,
                DirectoryIn::WatchRoutes {
                    changed: Arc::downgrade(&distribution_wake),
                },
            )
            .map_err(|error| format!("subscribe daemon route changes: {error}"))?;
        let connection_watch = driver
            .connection_observer()
            .watch_connections(Arc::clone(&distribution_wake));
        orch_telemetry.serve(&tasks, driver);
        bootstrap(
            &mut orch_telemetry,
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
            "serve_cluster",
            "started",
            json!({"mode":"daemon","ingress":"actor-events"}),
        );

        let runtime = stack.runtime.clone();
        let completion = ActorCompletion::new();
        let serve_actor = runtime
            .spawn(ServeClusterActor {
                peers,
                stack,
                ingress: ingress.clone(),
                _distribution_wake: distribution_wake,
                _connection_watch: connection_watch,
                orchestrator_report_actor,
                dashboard,
                orch_telemetry,
                orch_stdio_actor,
                hardware_samples,
                run_id: config.run_id,
                orchestrator_node_id: config.node_id,
                provider: config.provider.clone(),
                orchestrator_actor,
                sender: runtime.create_sender(),
                lifecycle: ServeClusterLifecycle::new(),
                ready_peers: BTreeMap::new(),
                dispatched_deployments: BTreeMap::new(),
                requires_deployment_receipt: config.deployment.is_some(),
                pending_undispatched_readies: BTreeMap::new(),
                flush_reply_actor: None,
                completion: Some(completion.clone()),
            })
            .map_err(|error| format!("spawn daemon lifecycle actor: {error}"))?;
        ingress.bind(serve_actor);
        ingress.send(ServeClusterMsg::DistributionChanged);
        if let Err(error) = spawn_stop_listener(
            &engine.handle().blocking_work_sender(),
            &runtime,
            stop_rx,
            serve_actor,
        ) {
            let _ = runtime.send_to(serve_actor, ServeClusterMsg::Abort(error));
        }
        let result = completion.wait();
        if result.is_ok() {
            let snapshot = state_dir.load_snapshot()?;
            state_dir.cleanup_recovery_checkpoint(&snapshot)?;
        }
        result
    }))
}

#[derive(Clone)]
struct VastAiRuntimeConfig {
    api_key: Option<String>,
    client: Option<swactor_vastai::VastClient>,
    webhooks: Option<Arc<VastAiWebhooks>>,

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
        let ssh_identity = builder
            .vastai_ssh_identity_raw
            .as_ref()
            .map(|value| expand_home_path(value))
            .transpose()?;
        Ok(Self {
            api_key: builder.vastai_api_key.clone(),
            client: builder
                .vastai_api_key
                .as_ref()
                .map(swactor_vastai::VastClient::new),
            webhooks: None,

            provisioning,
            bootstrap_command: builder.vastai_bootstrap_command.clone(),
            ssh_identity,
            ssh_public_fingerprint: None,
        })
    }

    fn telemetry_detail(&self) -> Value {
        json!({
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
    provider_composition: Option<ProviderComposition>,
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
    endpoint_bind_port: Option<u16>,
    endpoint_addr_mask: EndpointAddrMask,
    vastai: Option<VastAiRuntimeConfig>,

    deployment: Option<DeploymentBundle>,
    cached_model: Option<CachedModelConfig>,
    worker_bin: Option<PathBuf>,
}

#[derive(Clone)]
struct ConfigBuilder {
    providers: Vec<ProviderComposition>,
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
    endpoint_bind_port: Option<u16>,
    endpoint_addr_mask: Option<String>,
    vastai_api_key: Option<String>,

    vastai_bootstrap_command: Option<String>,
    vastai_disk_gb: Option<u32>,
    vastai_disk_gb_raw: Option<String>,
    vastai_ssh_user: Option<String>,
    vastai_confirm_lease: Option<bool>,
    vastai_confirm_lease_raw: Option<String>,
    vastai_onstart: Option<String>,
    vastai_ssh_identity_raw: Option<String>,

    deployment_bundle_raw: Option<String>,
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
    vastai_blacklist_hosts: Vec<u64>,
    cached_model_host_path: Option<PathBuf>,
    worker_bin: Option<PathBuf>,
}

impl ConfigBuilder {
    fn parse_provider(&self, name: &str) -> Result<ProviderKind, String> {
        self.providers
            .iter()
            .find(|entry| entry.kind.as_str() == name)
            .map(|entry| Ok(entry.kind.clone()))
            .unwrap_or_else(|| provider_kind::parse_deploy(name))
    }

    fn hardcoded_defaults() -> Self {
        Self {
            providers: Vec::new(),
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
            endpoint_bind_port: None,
            endpoint_addr_mask: None,
            vastai_api_key: None,

            vastai_bootstrap_command: None,
            vastai_disk_gb: None,
            vastai_disk_gb_raw: None,
            vastai_ssh_user: None,
            vastai_confirm_lease: None,
            vastai_confirm_lease_raw: None,
            vastai_onstart: None,
            vastai_ssh_identity_raw: None,

            deployment_bundle_raw: None,
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
            vastai_blacklist_hosts: Vec::new(),
            cached_model_host_path: None,
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
            self.provider = Some(self.parse_provider(&provider)?);
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
        apply!(overlay.vastai.image, |image| {
            self.toml_vastai_image = Some(image)
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
                self.provider = Some(self.parse_provider(&provider)?);
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
        env_parse!("MYELIN_IROH_BIND_PORT", |port| {
            self.endpoint_bind_port = Some(port)
        });
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

        env_apply!("MYELIN_VASTAI_BOOTSTRAP_COMMAND", |command| {
            self.vastai_bootstrap_command = Some(command)
        });
        env_apply!("MYELIN_VASTAI_SSH_IDENTITY", |identity| {
            self.vastai_ssh_identity_raw = Some(identity)
        });

        env_apply!("MYELIN_DEPLOYMENT_BUNDLE", |bundle| {
            self.deployment_bundle_raw = Some(bundle)
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
                    self.provider = Some(self.parse_provider(&next_arg(&mut args, "--provider")?)?)
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

                "--deployment-bundle" => {
                    self.deployment_bundle_raw = Some(next_arg(&mut args, "--deployment-bundle")?);
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
        let deployment = self
            .deployment_bundle_raw
            .as_deref()
            .map(|raw| {
                let path = PathBuf::from(raw);
                Ok::<_, String>(DeploymentBundle {
                    identity: read_bundle_identity(&path)?,
                    path,
                })
            })
            .transpose()?;
        let provider_composition = self
            .providers
            .into_iter()
            .find(|entry| entry.kind == provider);
        Ok(Config {
            config_profile: self.config_profile,
            image,
            docker_gpus: self.docker_gpus,
            provider,
            provider_composition,
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
            endpoint_bind_port: self.endpoint_bind_port,
            endpoint_addr_mask,
            vastai,

            deployment,
            cached_model,
            worker_bin: self.worker_bin,
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

/// A verified deployment bundle: live host-local path plus the identity it
/// carries. The path never enters durable state; only the identity does.
#[derive(Clone, Debug)]
struct DeploymentBundle {
    path: PathBuf,
    identity: crate::provisioning::DeploymentIdentity,
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
        tasks: VastAiTaskPort,
        process_registry_path: PathBuf,
        bootstrap_env: BootstrapEnvSource,
    ) -> Result<Box<dyn ProvisionPlugin>, String> {
        if let Some(provider) = &self.provider_composition {
            return (provider.build)(ProviderContext {
                runtime: bootstrap_runtime,
                engine: bootstrap_engine,
                tasks,
                process_registry_path,
                bootstrap_env,
            });
        }
        match self.provider.as_str() {
            "process" => Ok(Box::new(LocalProcessPlugin::with_registry(
                bootstrap_engine.blocking_work_sender(),
                self.local_worker_bin()?,
                process_registry_path,
                "process",
                bootstrap_runtime.clone(),
            ))),
            "docker" => Ok(Box::new(LocalDockerPlugin::new(
                bootstrap_engine.blocking_work_sender(),
                env_optional("MYELIN_DOCKER_CONTAINER_PREFIX")
                    .unwrap_or_else(|| "myelin-orchestrator".to_owned()),
                bootstrap_runtime.clone(),
            ))),
            "vastai" => {
                let vastai = self.vastai.as_ref().ok_or_else(|| {
                    "VastAI config was not resolved for provider vastai".to_owned()
                })?;

                if vastai.bootstrap_command.is_none() {
                    return Err(
                        "MYELIN_VASTAI_BOOTSTRAP_COMMAND is required when MYELIN_NODE_PROVIDER=vastai"
                            .to_owned(),
                    );
                }
                let client = vastai.client.clone().ok_or_else(|| {
                    "VAST_API_KEY, MYELIN_VASTAI_API_KEY, or VASTAI_API_KEY is required when MYELIN_NODE_PROVIDER=vastai"
                        .to_owned()
                })?;
                let ssh_identity = vastai
                    .ssh_identity
                    .clone()
                    .ok_or_else(|| "VastAI SSH identity was not prepared".to_owned())?;
                let client =
                    ToolsVastAiLeaseClient::new(client)?.with_actor_host(
                        bootstrap_runtime.clone(),
                        tasks,
                        Arc::clone(vastai.webhooks.as_ref().ok_or_else(|| {
                            "Vast.ai webhook receiver is not initialized".to_owned()
                        })?),
                    );
                if let Some(deployment) = self.deployment.as_ref() {
                    let launcher = SshArtifactBootstrapLauncher::new(
                        ssh_identity,
                        deployment.path.clone(),
                        bootstrap_runtime,
                        bootstrap_env,
                        bootstrap_engine,
                    );
                    return Ok(Box::new(VastAiProvisioningPlugin::new(
                        client,
                        launcher,
                        vastai.provisioning.clone(),
                    )));
                }
                let launcher = SshCommandBootstrapLauncher::new(
                    Some(ssh_identity),
                    bootstrap_runtime,
                    bootstrap_env,
                    bootstrap_engine,
                );
                Ok(Box::new(VastAiProvisioningPlugin::new(
                    client,
                    launcher,
                    vastai.provisioning.clone(),
                )))
            }

            _ => Err("provider is not registered for runtime provisioning".to_owned()),
        }
    }

    /// Provider-specific environment shared by filter and launch paths.
    fn extra_worker_env(&self) -> Vec<(String, String)> {
        let mut env = Vec::new();
        if let Some(url) = &self.relay.url {
            env.push((MYELIN_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
        }
        let docker_realized = self.provider.as_str() == "docker";
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
        if self
            .provider_composition
            .as_ref()
            .map_or(provider_name == "vastai", |provider| provider.remote)
        {
            env.retain(|(key, _)| {
                key != "MYELIN_COORDINATOR_ENDPOINT" && key != "MYELIN_ORCHESTRATOR_ACTOR"
            });
        }
        env.extend(self.extra_worker_env());
        let args = if self.provider_composition.is_some() {
            Vec::new()
        } else {
            match provider_name {
                "vastai" if self.deployment.is_some() => {
                    vec![format!("exec {DEPLOYED_WORKER_BIN_PATH}")]
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
                _ => return Err("provider is not registered for node specifications".to_owned()),
            }
        };
        if let Some(deployment) = &self.deployment {
            env.push((
                "MYELIN_ARTIFACT_DIGEST".to_owned(),
                deployment.identity.artifact_digest.clone(),
            ));
            env.push((
                "MYELIN_DEPLOYMENT_GENERATION".to_owned(),
                deployment.identity.deployment_generation.clone(),
            ));
        }
        let deployment = self
            .deployment
            .as_ref()
            .map(|bundle| bundle.identity.clone());
        let mut spec = NodeProvisionSpec {
            deployment,
            run_id: self.run_id,
            node_id: logical_node_id,
            attempt_id: 0,
            stage_index: Some(stage_index),
            image: self.image.clone(),
            env,
            args,
            offer_criteria_json: None,
            mounts: Vec::new(),
        };
        if let Some(provider) = &self.provider_composition {
            (provider.prepare_spec)(&mut spec)?;
        }
        Ok(spec)
    }
}

#[derive(Clone)]
struct RuntimeReady {
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    swim_node_id: DistNodeId,
    facts: daemon::RuntimeFacts,
}

fn runtime_ready_barrier_met(stack: &DistributionRuntimeStack, ready: &RuntimeReady) -> bool {
    stack.member_state(ready.swim_node_id) == Some(MemberState::Alive)
        && stack.route_owner(ready.node_actor) == Some(ready.swim_node_id)
}

fn deployment_dispatch_identity(line: &str, node_id: u64) -> Option<(String, String, u64)> {
    let value = serde_json::from_str::<serde_json::Value>(line).ok()?;
    if value.pointer("/type")?.as_str()? != "MyelinBootstrapDispatched"
        || value.pointer("/node_id")?.as_u64()? != node_id
    {
        return None;
    }
    let artifact = value.pointer("/artifact_digest")?.as_str()?;
    let generation = value.pointer("/deployment_generation")?.as_str()?;
    let receipt = value.pointer("/receipt")?;
    let executable = receipt.pointer("/executable_digest")?.as_str()?;
    if receipt.pointer("/type")?.as_str()? != "MyelinBootstrapReceipt"
        || receipt.pointer("/artifact_digest")?.as_str()? != artifact
        || receipt.pointer("/deployment_generation")?.as_str()? != generation
        || !executable.starts_with("sha256:")
        || receipt.pointer("/pid")?.as_u64()? <= 1
        || receipt.pointer("/process_start_ticks")?.as_u64()? == 0
        || receipt.pointer("/worker_count")?.as_u64()? != 1
        || receipt.pointer("/incarnation")?.as_str()?.is_empty()
    {
        return None;
    }
    let incarnation = format!(
        "{}:{}:{generation}",
        receipt.pointer("/pid")?.as_u64()?,
        receipt.pointer("/process_start_ticks")?.as_u64()?
    );
    if receipt.pointer("/incarnation")?.as_str()? != incarnation {
        return None;
    }
    Some((
        artifact.to_owned(),
        generation.to_owned(),
        daemon::deployment_readiness_id(&incarnation),
    ))
}

fn runtime_ready_matches_dispatch(
    dispatches: &BTreeMap<u64, (String, String, u64)>,
    node_id: u64,
    readiness_id: u64,
    artifact_digest: &Option<String>,
    deployment_generation: &Option<String>,
) -> bool {
    match (artifact_digest, deployment_generation) {
        (None, None) => true,
        (Some(artifact), Some(generation)) => dispatches.get(&node_id).is_some_and(|identity| {
            &identity.0 == artifact && &identity.1 == generation && identity.2 == readiness_id
        }),
        _ => false,
    }
}

#[cfg(target_os = "linux")]
struct OrchStdioCapture {
    stdout: File,
    stderr: File,
}

#[cfg(not(target_os = "linux"))]
struct OrchStdioCapture;

#[derive(Clone, Debug)]
struct OrchStdioLine {
    stream: ProvisionLogStream,
    line: String,
}

struct OrchStdioRelay {
    ingress: ServeClusterIngress,
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
                self.ingress
                    .send(ServeClusterMsg::Stdio(OrchStdioLine { stream, line }));
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

    fn start(
        self,
        blocking: &BlockingWorkSender,
        runtime: &Runtime,
        ingress: ServeClusterIngress,
    ) -> Result<ActorAddress, String> {
        let actor = runtime
            .spawn(OrchStdioRelay { ingress, closed: 0 })
            .map_err(|error| format!("spawn orchestrator stdio relay actor: {error}"))?;
        let sender = runtime.create_sender();
        swactor_process::spawn_line_reader(
            blocking,
            swactor_process::ProcessStream::Stdout,
            self.stdout,
            sender.clone(),
            actor,
        );
        swactor_process::spawn_line_reader(
            blocking,
            swactor_process::ProcessStream::Stderr,
            self.stderr,
            sender,
            actor,
        );
        Ok(actor)
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

    fn start(
        self,
        _blocking: &BlockingWorkSender,
        _runtime: &Runtime,
        _ingress: ServeClusterIngress,
    ) -> Result<ActorAddress, String> {
        unreachable!("stdio capture is unavailable on this target")
    }
}

#[derive(Clone)]
struct ServeClusterIngress {
    sender: ExternalSender,
    state: Arc<Mutex<ServeClusterIngressState>>,
}

struct ServeClusterIngressLifetime<'a>(&'a ServeClusterIngress);

impl Drop for ServeClusterIngressLifetime<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct ServeClusterIngressState {
    actor: Option<ActorAddress>,
    pending: VecDeque<ServeClusterMsg>,
    closed: bool,
}

impl ServeClusterIngress {
    fn new(sender: ExternalSender) -> Self {
        Self {
            sender,
            state: Arc::new(Mutex::new(ServeClusterIngressState {
                actor: None,
                pending: VecDeque::new(),
                closed: false,
            })),
        }
    }

    fn send(&self, message: ServeClusterMsg) {
        let mut state = self.state.lock();
        if state.closed {
            return;
        }
        if let Some(actor) = state.actor {
            let _ = self.sender.send_to(actor, message);
        } else {
            state.pending.push_back(message);
        }
    }

    fn bind(&self, actor: ActorAddress) {
        let mut state = self.state.lock();
        state.actor = Some(actor);
        for message in state.pending.drain(..) {
            let _ = self.sender.send_to(actor, message);
        }
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.pending.clear();
    }
}

struct OrchestratorReportForwarder {
    ingress: ServeClusterIngress,
}

impl ActorInterface for OrchestratorReportForwarder {
    type Incoming = OrchestratorReport;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, report: Self::Incoming) {
        self.ingress.send(ServeClusterMsg::Report(report));
    }
}

struct ChannelObservationSink {
    ingress: ServeClusterIngress,
}

impl PluginObservationSink for ChannelObservationSink {
    fn observe(&self, observation: PluginObservation) {
        self.ingress.send(ServeClusterMsg::Observation(observation));
    }
}

#[derive(Clone, Debug)]
enum ServeClusterMsg {
    DistributionChanged,
    Report(OrchestratorReport),
    Observation(PluginObservation),
    Stdio(OrchStdioLine),
    Stop,
    Flushed,
    FlushFailed(String),
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
    ObserveDistribution,
    Report(OrchestratorReport),
    Observation(PluginObservation),
    Stdio(OrchStdioLine),
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
            ServeClusterMsg::DistributionChanged
                if !matches!(self.state, ServeClusterState::Finished) =>
            {
                ServeClusterEffect::ObserveDistribution
            }
            ServeClusterMsg::Report(report) if matches!(self.state, ServeClusterState::Running) => {
                ServeClusterEffect::Report(report)
            }
            ServeClusterMsg::Observation(observation)
                if !matches!(self.state, ServeClusterState::Finished) =>
            {
                ServeClusterEffect::Observation(observation)
            }
            ServeClusterMsg::Stdio(line) if !matches!(self.state, ServeClusterState::Finished) => {
                ServeClusterEffect::Stdio(line)
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
            ServeClusterMsg::Abort(error) if !matches!(self.state, ServeClusterState::Finished) => {
                self.complete(Err(error))
            }
            ServeClusterMsg::DistributionChanged
            | ServeClusterMsg::Report(_)
            | ServeClusterMsg::Observation(_)
            | ServeClusterMsg::Stdio(_)
            | ServeClusterMsg::Stop
            | ServeClusterMsg::Flushed
            | ServeClusterMsg::FlushFailed(_)
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

impl ServeClusterLifecycle<RuntimeReady> {
    fn advance_join_barriers(
        &self,
        stack: &DistributionRuntimeStack,
        orchestrator_actor: ActorAddress,
    ) {
        for (node_id, pending) in &self.pending_readies {
            if !runtime_ready_barrier_met(stack, &pending.ready) {
                continue;
            }
            // Enqueueing admission is not completion: the manual actor may not
            // yet be Joining, and the worker's final ACK can be lost after it
            // stops announcing readiness. Keep this retry owner until that ACK
            // returns with the matching identity. Manual control coalesces
            // duplicate admission behind its durable ACK barrier.
            let _ = stack.runtime.send_to(
                orchestrator_actor,
                OrchestratorMsg::Manual(ManualControlMsg::JoinBarrierSatisfied {
                    node_id: *node_id,
                    facts: pending.ready.facts.clone(),
                }),
            );
        }
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
    blocking: &BlockingWorkSender,
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
        swactor_process::spawn_stop_channel_wait(blocking, external, sender, actor);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    swactor_process::spawn_os_stop_signal_wait(blocking, sender, actor)
        .map_err(|error| format!("install orchestrator stop signal handler: {error}"))?;
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
        ManualControlReply::Accepted(_)
        | ManualControlReply::Provider(_)
        | ManualControlReply::Status(_)
        | ManualControlReply::FleetStatus(_)
        | ManualControlReply::Offers(_)
        | ManualControlReply::RejoinResolved { .. } => None,
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
    peers: iroh_driver::PeerConnector,
    stack: DistributionRuntimeStack,
    ingress: ServeClusterIngress,
    _distribution_wake: Arc<dyn Fn() + Send + Sync>,
    _connection_watch: iroh_driver::ConnectionWatch,
    orchestrator_report_actor: ActorAddress,
    dashboard: Option<DashboardSupport>,
    orch_telemetry: OrchTelemetry,
    orch_stdio_actor: Option<ActorAddress>,
    hardware_samples: tokio::sync::mpsc::Sender<()>,
    run_id: u64,
    orchestrator_node_id: u64,
    provider: ProviderKind,
    orchestrator_actor: ActorAddress,
    sender: ExternalSender,
    lifecycle: ServeClusterLifecycle<RuntimeReady>,
    flush_reply_actor: Option<ActorAddress>,
    ready_peers: BTreeMap<u64, RuntimeReady>,
    dispatched_deployments: BTreeMap<u64, (String, String, u64)>,
    requires_deployment_receipt: bool,
    pending_undispatched_readies:
        BTreeMap<(u64, Option<String>, Option<String>, u64), OrchestratorReport>,
    completion: Option<ActorCompletion<Result<(), String>>>,
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
        let deferred_report = report.clone();
        match report {
            OrchestratorReport::NodeRuntimeReady {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                readiness_id,
                artifact_digest,
                deployment_generation,
                ..
            } if run_id == self.run_id => {
                if self.requires_deployment_receipt
                    && (artifact_digest.is_none() || deployment_generation.is_none())
                {
                    return;
                }
                if artifact_digest.is_some()
                    && deployment_generation.is_some()
                    && !self.dispatched_deployments.contains_key(&node_id)
                {
                    self.pending_undispatched_readies.insert(
                        (
                            node_id,
                            artifact_digest.clone(),
                            deployment_generation.clone(),
                            readiness_id,
                        ),
                        deferred_report,
                    );
                    self.orch_telemetry.emit_bootstrap(
                        self.run_id,
                        node_id,
                        "runtime_ready",
                        "deferred",
                        json!({"reason":"awaiting matching validated deployment receipt"}),
                    );
                    return;
                }
                if !runtime_ready_matches_dispatch(
                    &self.dispatched_deployments,
                    node_id,
                    readiness_id,
                    &artifact_digest,
                    &deployment_generation,
                ) {
                    self.orch_telemetry.emit_bootstrap(self.run_id, node_id, "runtime_ready", "rejected", json!({"reason":"runtime identity does not match validated deployment receipt"}));
                    return;
                }
                let ready = RuntimeReady {
                    endpoint: endpoint.clone(),
                    node_actor,
                    swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                    facts: daemon::RuntimeFacts {
                        run_id,
                        attempt_id: 0,
                        endpoint: serde_json::to_string(&endpoint).expect("serialize endpoint"),
                        node_actor,
                        swim_node_id: DistNodeId(*endpoint.id.as_bytes()),
                        stage_index,
                        readiness_id,
                        artifact_digest,
                        deployment_generation,
                    },
                };
                if !self.lifecycle.pending_readies.contains_key(&node_id)
                    && self
                        .ready_peers
                        .get(&node_id)
                        .is_some_and(|accepted| accepted.facts == ready.facts)
                {
                    // The matching final ACK retired admission. A readiness
                    // announcement already in transit must not re-arm it.
                    // Deployment/terminal transitions clear both peer and
                    // pending state before admitting a replacement.
                    return;
                }
                let tracked =
                    self.lifecycle
                        .track_runtime_ready(node_id, readiness_id, ready.clone());
                if tracked {
                    self.peers.connect(endpoint.clone());
                    self.connect_ready_peer_mesh(node_id, &ready);
                    if let Some(dashboard) = &self.dashboard {
                        dashboard.subscribe_node(endpoint, run_id, node_id);
                    }
                }
            }
            OrchestratorReport::NodeRuntimeReadyAck {
                run_id,
                node_id,
                stage_index,
                readiness_id,
                ..
            } if run_id == self.run_id => {
                if self
                    .lifecycle
                    .pending_readies
                    .get(&node_id)
                    .is_some_and(|pending| pending.ready.facts.stage_index == stage_index)
                {
                    self.lifecycle
                        .acknowledge_runtime_ready(node_id, readiness_id);
                }
            }
            _ => {}
        }
    }

    /// Connect every ready worker directly to its peers.
    ///
    /// Workers initially know only the orchestrator endpoint. Actor routing
    /// maps a source address to its worker node, but a direct-only deployment
    /// cannot dial that node from a bare public key; without the explicit peer
    /// addresses the first cross-worker transfer can exhaust its bounded route
    /// budget while a background dial has nothing to dial.
    fn connect_ready_peer_mesh(&mut self, node_id: u64, ready: &RuntimeReady) {
        let existing = self.ready_peers.clone();
        for (peer_id, peer) in existing.iter() {
            if *peer_id == node_id {
                continue;
            }
            let _ = self.stack.runtime.send_to(
                peer.node_actor,
                NodeAgentMsg::JoinPeers {
                    endpoints: vec![ready.endpoint.clone()],
                },
            );
        }
        let peer_endpoints = existing
            .iter()
            .filter(|(peer_id, _)| **peer_id != node_id)
            .map(|(_, peer)| peer.endpoint.clone())
            .collect::<Vec<_>>();
        if !peer_endpoints.is_empty() {
            let _ = self.stack.runtime.send_to(
                ready.node_actor,
                NodeAgentMsg::JoinPeers {
                    endpoints: peer_endpoints,
                },
            );
        }
        self.ready_peers.insert(node_id, ready.clone());
    }

    fn observe_plugin(&mut self, observation: PluginObservation) {
        let mut deferred_ready = None;
        match &observation {
            PluginObservation::PhaseChanged {
                run_id,
                node_id,
                phase: ProvisionEventKind::NodeStopped,
            }
            | PluginObservation::Exited {
                run_id, node_id, ..
            }
            | PluginObservation::Failed {
                run_id, node_id, ..
            } if *run_id == self.run_id => {
                if let Some(dashboard) = &self.dashboard {
                    dashboard.unsubscribe_node(*run_id, *node_id);
                }
                self.lifecycle.remove_runtime_ready(*node_id);
                self.ready_peers.remove(node_id);
                self.dispatched_deployments.remove(node_id);
                self.pending_undispatched_readies
                    .retain(|(pending_node, _, _, _), _| pending_node != node_id);
            }
            PluginObservation::ProviderLine {
                run_id,
                node_id,
                line,
            } if *run_id == self.run_id => {
                if let Some(identity) = deployment_dispatch_identity(line, *node_id) {
                    let key = (
                        *node_id,
                        Some(identity.0.clone()),
                        Some(identity.1.clone()),
                        identity.2,
                    );
                    self.lifecycle.remove_runtime_ready(*node_id);
                    self.ready_peers.remove(node_id);
                    if let Some(dashboard) = &self.dashboard {
                        dashboard.unsubscribe_node(*run_id, *node_id);
                    }
                    self.dispatched_deployments.insert(*node_id, identity);
                    deferred_ready = self.pending_undispatched_readies.remove(&key);
                    self.pending_undispatched_readies
                        .retain(|(pending_node, _, _, _), _| pending_node != node_id);
                }
            }
            _ => {}
        }
        emit_plugin_observation(&mut self.orch_telemetry, &self.provider, &observation);
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
        if let Some(report) = deferred_ready {
            self.observe_report(report);
        }
    }

    fn observe_distribution(&mut self) {
        emit_membership_transitions(
            &mut self.orch_telemetry,
            self.run_id,
            self.orchestrator_node_id,
            &self.stack,
        );
        self.orch_telemetry.flush();
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
        Ok(())
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), String>) {
        self.ingress.close();
        let _ = self
            .stack
            .runtime
            .stop_actor(self.orchestrator_report_actor);
        if let Some(actor) = self.orch_stdio_actor.take() {
            let _ = self.stack.runtime.stop_actor(actor);
        }
        if let Some(reply_actor) = self.flush_reply_actor.take() {
            let _ = self.stack.runtime.stop_actor(reply_actor);
        }
        if let Err(error) = &result {
            self.orch_telemetry.emit_bootstrap(
                self.run_id,
                self.orchestrator_node_id,
                "serve_cluster",
                "failed",
                json!({"error":error}),
            );
        }
        assert!(
            self.completion
                .take()
                .expect("daemon lifecycle completion owner")
                .complete(result)
                .is_ok(),
            "daemon lifecycle completed twice"
        );
        ctx.stop_self();
    }
}

impl ActorInterface for ServeClusterActor {
    type Incoming = ServeClusterMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.watch(self.orchestrator_actor);
        if !self.stack.runtime.is_local_actor(self.orchestrator_actor) {
            self.handle(
                ctx,
                ServeClusterMsg::Abort("orchestrator actor is unavailable".to_owned()),
            );
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        if exited.addr == self.orchestrator_actor {
            self.handle(
                ctx,
                ServeClusterMsg::Abort(format!("orchestrator actor exited: {:?}", exited.reason)),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
        if matches!(
            &message,
            ServeClusterMsg::Report(_)
                | ServeClusterMsg::Observation(
                    PluginObservation::PhaseChanged { .. }
                        | PluginObservation::Exited { .. }
                        | PluginObservation::Failed { .. }
                )
        ) {
            let _ = self.hardware_samples.try_send(());
        }
        let effect = self.lifecycle.apply_message(message);
        match effect {
            ServeClusterEffect::None => {}
            ServeClusterEffect::ObserveDistribution => self.observe_distribution(),
            ServeClusterEffect::Report(report) => self.observe_report(report),
            ServeClusterEffect::Observation(observation) => self.observe_plugin(observation),
            ServeClusterEffect::Stdio(line) => {
                self.orch_telemetry
                    .emit_orchestrator_log(line.stream, line.line);
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
        if matches!(self.lifecycle.state, ServeClusterState::Running) {
            self.lifecycle
                .advance_join_barriers(&self.stack, self.orchestrator_actor);
        }
    }
}

impl Drop for ServeClusterActor {
    fn drop(&mut self) {
        self.ingress.close();
        if let Some(completion) = self.completion.take() {
            let _ = self
                .stack
                .runtime
                .stop_actor(self.orchestrator_report_actor);
            if let Some(actor) = self.orch_stdio_actor.take() {
                let _ = self.stack.runtime.stop_actor(actor);
            }
            if let Some(actor) = self.flush_reply_actor.take() {
                let _ = self.stack.runtime.stop_actor(actor);
            }
            let _ = completion.complete(Err(
                "daemon lifecycle actor stopped before shutdown completed".to_owned(),
            ));
        }
    }
}

fn emit_plugin_observation(
    orch_telemetry: &mut OrchTelemetry,
    provider: &ProviderKind,
    observation: &PluginObservation,
) {
    match observation {
        PluginObservation::StdoutLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(ProvisionLogLine {
            run_id: *run_id,
            node_id: *node_id,
            stream: ProvisionLogStream::Stdout,
            line: line.clone(),
        }),
        PluginObservation::StderrLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(ProvisionLogLine {
            run_id: *run_id,
            node_id: *node_id,
            stream: ProvisionLogStream::Stderr,
            line: line.clone(),
        }),
        PluginObservation::ProviderLine {
            run_id,
            node_id,
            line,
        } => orch_telemetry.emit_log(ProvisionLogLine {
            run_id: *run_id,
            node_id: *node_id,
            stream: ProvisionLogStream::Provider,
            line: line.clone(),
        }),
        PluginObservation::PhaseChanged {
            run_id,
            node_id,
            phase,
        } => orch_telemetry.emit_event(ProvisionEvent {
            run_id: *run_id,
            node_id: *node_id,
            kind: *phase,
            provider: Some(provider.as_str().to_owned()),
            message: None,
        }),
        PluginObservation::TelemetryFrame {
            channel, payload, ..
        } => orch_telemetry.emit_bytes(channel, payload.as_bytes().to_vec()),
        PluginObservation::Exited {
            run_id,
            node_id,
            status,
        } => orch_telemetry.emit_event(ProvisionEvent {
            run_id: *run_id,
            node_id: *node_id,
            kind: ProvisionEventKind::NodeStopped,
            provider: Some(provider.as_str().to_owned()),
            message: Some(format!("node process exited with {status:?}")),
        }),
        PluginObservation::Failed {
            run_id,
            node_id,
            reason,
        } => orch_telemetry.emit_event(ProvisionEvent {
            run_id: *run_id,
            node_id: *node_id,
            kind: ProvisionEventKind::ProvisionFailed,
            provider: Some(provider.as_str().to_owned()),
            message: Some(reason.clone()),
        }),
    }
}

fn emit_membership_transitions(
    orch_telemetry: &mut OrchTelemetry,
    run_id: u64,
    node_id: u64,
    stack: &DistributionRuntimeStack,
) {
    for transition in stack.drain_membership_transitions() {
        orch_telemetry.emit_bootstrap_to_channel(BootstrapEmission {
            channel: MYELIN_MEMBERSHIP,
            run_id,
            node_id,
            phase: "membership_transition",
            status: "observed",
            detail: json!({
                "peer":format!("{:?}", transition.peer),
                "from":transition.from.map(|state| format!("{:?}", state)),
                "to":format!("{:?}", transition.to),
                "reason":transition.reason,
                "member_state":stack.member_state(transition.peer).map(|state| format!("{:?}", state)),
            }),
        });
        orch_telemetry.emit_record(&stack.membership_transition(&transition));
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

pub fn derive_ssh_public_key(identity: &Path) -> Result<String, String> {
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

pub fn ensure_vastai_account_ssh_key(api_key: &str, public_key: &str) -> Result<(), String> {
    let client = swactor_vastai::BlockingVastClient::new(swactor_vastai::VastClient::new(api_key))?;
    if account_ssh_keys_output_contains_public_key(&client.account_ssh_keys()?, public_key) {
        return Ok(());
    }

    client.register_account_ssh_key(public_key)?;
    if account_ssh_keys_output_contains_public_key(&client.account_ssh_keys()?, public_key) {
        Ok(())
    } else {
        Err("VastAI SSH key registration did not make the selected key visible in the account listing".to_owned())
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

    #[test]
    fn admission_retries_lost_final_ack_without_duplicate_provider_completion() {
        use crate::node_actor::{NodeAgentActor, NodeAgentReport};
        use crate::orchestration::manual_control::{EffectKind, EffectOutcome, ManualAction};

        fn settle_persistence(core: &mut ManualControl) -> Vec<ManualAction> {
            let mut effects = Vec::new();
            loop {
                let actions = core.take_actions().collect::<Vec<_>>();
                if actions.is_empty() {
                    return effects;
                }
                for action in actions {
                    match action {
                        ManualAction::Persist { generation, .. } => {
                            core.persisted(generation, Ok(())).unwrap();
                        }
                        effect => effects.push(effect),
                    }
                }
            }
        }

        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let stack = DistributionRuntimeStack::new_from_runtime(
            runtime.clone(),
            Arc::new(distribution::messages::actor_codec_registry()),
            Arc::new(swactor_transport::TransportRouter::new()),
            DistNodeId([1; 32]),
            DistributedNodeConfig::default(),
            engine.handle(),
        );
        let messages = runtime.new_inbox::<OrchestratorMsg>().unwrap();
        let reports = runtime.new_inbox::<NodeAgentReport>().unwrap();
        let node_actor = runtime
            .spawn(NodeAgentActor::new(
                myelin::staging::NodeId(2),
                *messages.addr(),
                Some(*reports.addr()),
            ))
            .unwrap();
        let endpoint = EndpointAddr::new(iroh::SecretKey::from_bytes(&[2; 32]).public());
        let worker = DistNodeId(*endpoint.id.as_bytes());
        let facts = daemon::RuntimeFacts {
            run_id: 4_611_686_018_447_648_814,
            attempt_id: 0,
            endpoint: serde_json::to_string(&endpoint).unwrap(),
            node_actor,
            swim_node_id: worker,
            stage_index: 3,
            readiness_id: 684_671_332_900_197_836,
            artifact_digest: Some("sha256:current".to_owned()),
            deployment_generation: Some("current-deployment".to_owned()),
        };
        let mut snapshot = daemon::ClusterSnapshot::fresh(facts.run_id, "ack-loss");
        snapshot.upsert_node(daemon::SnapshotNode {
            logical_node_id: 2,
            spec: Some(NodeProvisionSpec {
                run_id: facts.run_id,
                node_id: 2,
                attempt_id: facts.attempt_id,
                stage_index: Some(facts.stage_index),
                deployment: facts.deployment_identity(),
                image: "worker".to_owned(),
                env: Vec::new(),
                args: Vec::new(),
                offer_criteria_json: None,
                mounts: Vec::new(),
            }),
            selected_offer_id: None,
            provider_ref: Some("retained-worker".to_owned()),
            phase: NodePhase::Joining,
            runtime: None,
            last_error: None,
        });
        let mut core = ManualControl::new(snapshot, ProviderReadiness::ready());
        let mut lifecycle = ServeClusterLifecycle::new();
        assert!(lifecycle.track_runtime_ready(
            2,
            facts.readiness_id,
            RuntimeReady {
                endpoint,
                node_actor,
                swim_node_id: worker,
                facts: facts.clone(),
            },
        ));
        drive_steps(&backend, STEPS_PER_ACTION);

        // Neither membership alone nor a missing route grants admission.
        lifecycle.advance_join_barriers(&stack, *messages.addr());
        drive_steps(&backend, STEPS_PER_ACTION);
        assert!(messages.try_recv().is_none());
        stack
            .membership_mirror
            .lock()
            .unwrap()
            .apply(worker, MemberState::Alive, 1);
        lifecycle.advance_join_barriers(&stack, *messages.addr());
        drive_steps(&backend, STEPS_PER_ACTION);
        assert!(messages.try_recv().is_none());
        stack.route_view.write().unwrap().insert(node_actor, worker);

        // The worker accepts the first grant and stops announcing readiness.
        // Lose only its final reply; the orchestrator must still own recovery.
        for round in 0..2 {
            for _ in 0..2 {
                lifecycle.advance_join_barriers(&stack, *messages.addr());
                drive_steps(&backend, STEPS_PER_ACTION);
                let Some(OrchestratorMsg::Manual(ManualControlMsg::JoinBarrierSatisfied {
                    node_id,
                    facts: admitted,
                })) = messages.try_recv()
                else {
                    panic!("admission was abandoned before the worker's final ACK");
                };
                assert_eq!(admitted, facts);
                core.runtime_ready(node_id, admitted).unwrap();
                if round == 0 && core.snapshot().node(2).unwrap().phase == NodePhase::Joining {
                    core.join_barrier_satisfied(node_id).unwrap();
                }
            }
            assert_eq!(
                core.snapshot().node(2).unwrap().phase,
                NodePhase::Acknowledging
            );
            let effects = settle_persistence(&mut core);
            let [
                ManualAction::SendRuntimeReadyAck {
                    node_id,
                    facts: grant,
                },
            ] = effects.as_slice()
            else {
                panic!("one durable grant must coalesce repeated admission: {effects:?}");
            };
            runtime
                .send_to(
                    grant.node_actor,
                    NodeAgentMsg::RuntimeReadyAck {
                        run_id: grant.run_id,
                        node_id: *node_id,
                        stage_index: grant.stage_index,
                        readiness_id: grant.readiness_id,
                    },
                )
                .unwrap();
            drive_steps(&backend, STEPS_PER_ACTION);
            assert!(matches!(
                reports.try_recv(),
                Some(NodeAgentReport::RuntimeReadyAck { readiness_id, .. })
                    if readiness_id == facts.readiness_id
            ));
            let Some(OrchestratorMsg::ObserveNodeRuntimeReadyAck { readiness_id, .. }) =
                messages.try_recv()
            else {
                panic!("worker did not reply to the durable readiness grant");
            };
            if round == 0 {
                let mut stale = facts.clone();
                stale.node_actor = ActorAddress::default();
                assert!(core.runtime_ready(2, stale).is_err());
                assert!(!lifecycle.acknowledge_runtime_ready(2, readiness_id + 1));
                continue;
            }
            assert!(core.node_ack(2, readiness_id + 1).is_err());
            core.node_ack(2, readiness_id).unwrap();
            core.node_ack(2, readiness_id).unwrap();
            assert!(lifecycle.acknowledge_runtime_ready(2, readiness_id));
        }
        assert_eq!(
            core.snapshot().node(2).unwrap().phase,
            NodePhase::Acknowledging
        );
        assert!(matches!(
            settle_persistence(&mut core).as_slice(),
            [ManualAction::CompleteBootstrap { node_id: 2 }]
        ));
        core.node_ack(2, facts.readiness_id).unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        core.effect_finished(
            2,
            EffectKind::CompleteBootstrap,
            Ok(EffectOutcome::BootstrapCompleted),
        )
        .unwrap();
        assert!(settle_persistence(&mut core).is_empty());
        assert_eq!(core.snapshot().node(2).unwrap().phase, NodePhase::Running);
        assert_eq!(
            core.snapshot().node(2).unwrap().runtime.as_ref(),
            Some(&facts)
        );
        lifecycle.advance_join_barriers(&stack, *messages.addr());
        drive_steps(&backend, STEPS_PER_ACTION);
        assert!(messages.try_recv().is_none());
    }

    #[derive(Clone, Debug)]
    enum LifecycleAction {
        RuntimeReady { node_id: u8, readiness_id: u8 },
        RuntimeReadyAck { node_id: u8, readiness_id: u8 },
        PluginFailed { node_id: u8, code: u8 },
        Stop,
        ManualFlushed,
        ManualRejected(u8),
        Abort(u8),
    }

    fn lifecycle_actions() -> impl Strategy<Value = Vec<LifecycleAction>> {
        prop::collection::vec(
            prop_oneof![
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
                3 => (0_u8..READY_NODE_DOMAIN, any::<u8>()).prop_map(
                    |(node_id, code)| LifecycleAction::PluginFailed { node_id, code }
                ),
                3 => Just(LifecycleAction::Stop),
                2 => Just(LifecycleAction::ManualFlushed),
                2 => any::<u8>().prop_map(LifecycleAction::ManualRejected),
                2 => any::<u8>().prop_map(LifecycleAction::Abort),
            ],
            0..=32,
        )
    }

    #[derive(Clone, Debug)]
    enum LifecycleHarnessMsg {
        Action(LifecycleAction),
        Finalize,
    }

    #[derive(Clone, Debug)]
    struct LifecycleTrace {
        state: ServeClusterState,
        pending_readies: Vec<(u64, u64)>,
        max_pending_readies: usize,
        completion_results: Vec<Result<(), String>>,
        completion_publication_failures: usize,
        readies_tracked: usize,
        readies_acknowledged: usize,
        provider_failures: Vec<(u64, String)>,
        flushes_started: usize,
    }

    impl LifecycleTrace {
        fn new() -> Self {
            Self {
                state: ServeClusterState::Running,
                pending_readies: Vec::new(),
                max_pending_readies: 0,
                completion_results: Vec::new(),
                completion_publication_failures: 0,
                readies_tracked: 0,
                readies_acknowledged: 0,
                provider_failures: Vec::new(),
                flushes_started: 0,
            }
        }
    }

    struct ServeClusterLifecycleHarness {
        lifecycle: ServeClusterLifecycle<()>,
        completion: ActorCompletion<Result<(), String>>,
        trace: Arc<Mutex<LifecycleTrace>>,
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

        fn apply_effect(&mut self, effect: ServeClusterEffect) {
            match effect {
                ServeClusterEffect::None
                | ServeClusterEffect::ObserveDistribution
                | ServeClusterEffect::Report(_)
                | ServeClusterEffect::Observation(_)
                | ServeClusterEffect::Stdio(_) => {}
                ServeClusterEffect::BeginFlush => {
                    self.trace.lock().flushes_started += 1;
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

        fn apply_action(&mut self, action: LifecycleAction) {
            let effect = match action {
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
                LifecycleAction::PluginFailed { node_id, code } => {
                    self.lifecycle.plugin_observation(Some((
                        u64::from(node_id),
                        format!("plugin-failed-{code}"),
                    )))
                }
                LifecycleAction::Stop => self.lifecycle.apply_message(ServeClusterMsg::Stop),
                LifecycleAction::ManualFlushed => {
                    let message = serve_cluster_flush_reply(ManualControlReply::Flushed)
                        .expect("flush reply must be forwarded");
                    self.lifecycle.apply_message(message)
                }
                LifecycleAction::ManualRejected(code) => {
                    let message = serve_cluster_flush_reply(ManualControlReply::Rejected(format!(
                        "manual-rejected-{code}"
                    )))
                    .expect("rejected flush reply must be forwarded");
                    self.lifecycle.apply_message(message)
                }
                LifecycleAction::Abort(code) => self
                    .lifecycle
                    .apply_message(ServeClusterMsg::Abort(format!("abort-{code}"))),
            };
            self.apply_effect(effect);
            self.record_state();
        }
    }

    impl ActorInterface for ServeClusterLifecycleHarness {
        type Incoming = LifecycleHarnessMsg;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
            match message {
                LifecycleHarnessMsg::Action(action) => self.apply_action(action),
                LifecycleHarnessMsg::Finalize => {
                    if !matches!(self.lifecycle.state, ServeClusterState::Finished) {
                        let effect = self.lifecycle.apply_message(ServeClusterMsg::Abort(
                            "generated sequence exhausted".to_owned(),
                        ));
                        self.apply_effect(effect);
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
        terminal_observed: bool,
        readies_tracked: usize,
        readies_acknowledged: usize,
        provider_failures: usize,
        flushes_started: usize,
    }

    fn expected_lifecycle(actions: &[LifecycleAction]) -> ExpectedLifecycle {
        let mut state = ServeClusterState::Running;
        let mut pending_readies = BTreeMap::<u64, u64>::new();
        let mut result = None;
        let mut readies_tracked = 0;
        let mut readies_acknowledged = 0;
        let mut provider_failures = 0;
        let mut flushes_started = 0;

        for action in actions {
            match action {
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
                LifecycleAction::PluginFailed { .. } | LifecycleAction::RuntimeReady { .. } => {}
                LifecycleAction::Stop if matches!(state, ServeClusterState::Running) => {
                    state = ServeClusterState::Flushing;
                    pending_readies.clear();
                    flushes_started += 1;
                }
                LifecycleAction::ManualFlushed => {
                    if matches!(state, ServeClusterState::Flushing) {
                        state = ServeClusterState::Finished;
                        pending_readies.clear();
                        result = Some(true);
                    }
                }
                LifecycleAction::ManualRejected(_) => {
                    if matches!(state, ServeClusterState::Flushing) {
                        state = ServeClusterState::Finished;
                        pending_readies.clear();
                        result = Some(false);
                    }
                }
                LifecycleAction::Abort(_) if !matches!(state, ServeClusterState::Finished) => {
                    state = ServeClusterState::Finished;
                    pending_readies.clear();
                    result = Some(false);
                }
                LifecycleAction::Stop | LifecycleAction::Abort(_) => {}
            }
        }

        ExpectedLifecycle {
            success: result.unwrap_or(false),
            terminal_observed: result.is_some(),
            readies_tracked,
            readies_acknowledged,
            provider_failures,
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
        if trace.flushes_started != expected.flushes_started || trace.flushes_started > 1 {
            errors.push(format!(
                "flush state was not bounded: starts={} expected_starts={}",
                trace.flushes_started, expected.flushes_started
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
        if resources.peak_tasks > resources.baseline_tasks {
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
            let _engine = Engine::new(parts, backend.clone()).expect("one-worker stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let completion = ActorCompletion::new();
            let trace = Arc::new(Mutex::new(LifecycleTrace::new()));
            let actor = runtime
                .spawn(ServeClusterLifecycleHarness {
                    lifecycle: ServeClusterLifecycle::new(),
                    completion: completion.clone(),
                    trace: Arc::clone(&trace),
                })
                .expect("spawn production lifecycle transition harness");
            let mut resources = LifecycleResources::new(baseline_actors, baseline_tasks);
            resources.observe(&runtime, &backend);

            for (index, action) in actions.iter().enumerate() {
                runtime
                    .send_to(actor, LifecycleHarnessMsg::Action(action.clone()))
                    .expect("queue generated serve-cluster action");
                drive_steps(&backend, STEPS_PER_ACTION);
                resources.observe(&runtime, &backend);
                let expected = expected_lifecycle(&actions[..=index]);
                prop_assert_eq!(
                    !trace.lock().completion_results.is_empty(),
                    expected.terminal_observed,
                    "completion occurred without a terminal ingress event",
                );
            }

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
}

#[cfg(test)]
mod lifecycle_policy_tests {

    #[test]
    fn deployment_receipt_rejects_readiness_from_an_earlier_launch() {
        let artifact = Some("sha256:payload".to_owned());
        let generation = Some("same-deployment".to_owned());
        let current = "123:456:same-deployment";
        let receipt = serde_json::json!({
            "type": "MyelinBootstrapDispatched",
            "node_id": 1,
            "artifact_digest": artifact,
            "deployment_generation": generation,
            "receipt": {
                "type": "MyelinBootstrapReceipt",
                "artifact_digest": artifact,
                "deployment_generation": generation,
                "executable_digest": "sha256:executable",
                "pid": 123,
                "process_start_ticks": 456,
                "incarnation": current,
                "worker_count": 1
            }
        });
        let dispatch = super::deployment_dispatch_identity(&receipt.to_string(), 1).unwrap();
        let dispatches = std::collections::BTreeMap::from([(1, dispatch)]);
        assert!(super::runtime_ready_matches_dispatch(
            &dispatches,
            1,
            super::daemon::deployment_readiness_id(current),
            &artifact,
            &generation,
        ));
        // Reusing the provision attempt and deployment does not make a dead
        // launch's readiness eligible for the replacement's receipt.
        assert!(!super::runtime_ready_matches_dispatch(
            &dispatches,
            1,
            super::daemon::deployment_readiness_id("122:455:same-deployment"),
            &artifact,
            &generation,
        ));
        assert!(!super::runtime_ready_matches_dispatch(
            &dispatches,
            1,
            0,
            &artifact,
            &generation,
        ));
    }
}
