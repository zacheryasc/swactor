//! Fixture providers composed into the same Myelin application as production.
mod mock_vastai;
mod paid_vastai;
mod static_ssh;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use myelin::providers::{
    DEPLOYED_WORKER_BIN_PATH, OfferSearchRequestExt, SshArtifactBootstrapLauncher,
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin, VastAiWebhookConfig, VastAiWebhooks, derive_ssh_public_key,
    ensure_vastai_account_ssh_key,
};
use myelin::{ProviderComposition, ProviderContext};
use myelin_control_contract::{Offer, ProviderReadiness};
use provisioning::ProviderKind;
use provisioning::plugin::{NodeProvisionSpec, ProvisionPlugin};
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TaskSpawner};

use mock_vastai::MockVastAiPlugin;
use paid_vastai::{PaidProvisioningPlugin, PaidVastAiLeaseClient};
use static_ssh::{StaticFleetManifest, StaticSshFleetPlugin};

fn option(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .rev()
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

fn take_option(args: &mut Vec<String>, flag: &str) -> Result<Option<String>, String> {
    let mut value = None;
    while let Some(index) = args.iter().position(|arg| arg == flag) {
        if index + 1 == args.len() {
            return Err(format!("{flag} requires a value"));
        }
        args.remove(index);
        value = Some(args.remove(index));
    }
    Ok(value)
}

fn path_option(args: &[String], flag: &str, env: &str) -> Option<PathBuf> {
    option(args, flag)
        .map(PathBuf::from)
        .or_else(|| std::env::var_os(env).map(PathBuf::from))
}

/// Resolve harness fixture policy before entering ordinary application composition.
/// The production executable never parses these fixture-only switches or records.
pub fn prepare_orchestrator(
    mut args: Vec<String>,
) -> Result<
    (
        RuntimeParts,
        impl FnOnce(Engine, TaskSpawner) -> Result<(), String>,
    ),
    String,
> {
    let mock_switch = args.iter().any(|arg| arg == "--vastai-provisioning-mock");
    args.retain(|arg| arg != "--vastai-provisioning-mock");
    let mode = take_option(&mut args, "--vastai-provisioning")?
        .or_else(|| std::env::var("MYELIN_VASTAI_PROVISIONING_MODE").ok())
        .unwrap_or_else(|| if mock_switch { "mock" } else { "real" }.to_owned());
    if !matches!(mode.as_str(), "mock" | "real") {
        return Err(format!("unknown fixture provisioning mode {mode:?}"));
    }
    let manifest = take_option(&mut args, "--static-ssh-manifest")?
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("MYELIN_STATIC_SSH_MANIFEST").map(PathBuf::from));
    let identity = take_option(&mut args, "--ssh-identity")?
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("MYELIN_STATIC_SSH_IDENTITY").map(PathBuf::from));
    let provider = option(&args, "--provider")
        .or_else(|| std::env::var("MYELIN_NODE_PROVIDER").ok())
        .unwrap_or_else(|| "vastai".to_owned());
    let image = option(&args, "--image").unwrap_or_else(|| "myelin-node:latest".to_owned());
    let bundle = path_option(&args, "--deployment-bundle", "MYELIN_DEPLOYMENT_BUNDLE");
    let composition = match provider.as_str() {
        "static-ssh" => {
            let manifest =
                StaticFleetManifest::load(&manifest.ok_or("static fixture manifest is required")?)?;
            let identity = identity.ok_or("static fixture SSH identity is required")?;
            if !identity.is_file() {
                return Err(format!(
                    "static fixture SSH identity {} is missing",
                    identity.display()
                ));
            }
            let bundle = bundle.ok_or("static fixture deployment bundle is required")?;
            ProviderComposition {
                http_plugin: None,
                kind: ProviderKind::new("static-ssh"),
                readiness: ProviderReadiness::ready_for("static-ssh", image),
                remote: true,
                build: Arc::new(move |context| {
                    let launcher = SshArtifactBootstrapLauncher::new(
                        identity.clone(),
                        bundle.clone(),
                        context.runtime,
                        context.bootstrap_env,
                        context.engine,
                    );
                    Ok(Box::new(StaticSshFleetPlugin::new(
                        manifest.clone(),
                        launcher,
                    )))
                }),
                prepare_spec: Arc::new(|spec| {
                    spec.args = vec![format!("exec {DEPLOYED_WORKER_BIN_PATH}")];
                    Ok(())
                }),
                configure: None,
                search_offers: None,
            }
        }
        "vastai" if mode == "mock" => {
            if bundle.is_some() {
                return Err("mock VastAI provisioning does not deploy bundles".to_owned());
            }
            let prefix = std::env::var("MYELIN_MOCK_VASTAI_CONTAINER_PREFIX")
                .unwrap_or_else(|_| "myelin-mock-vastai".to_owned());
            let selected: Vec<u64> = serde_json::from_str(
                &std::env::var("MYELIN_MOCK_VASTAI_OFFERS").unwrap_or_else(|_| "[]".to_owned()),
            )
            .map_err(|error| format!("decode mock fixture offer IDs: {error}"))?;
            ProviderComposition {
                http_plugin: None,
                kind: ProviderKind::new("vastai"),
                readiness: ProviderReadiness::ready_for("vastai", image)
                    .with_provisioning_mode("mock"),
                remote: false,
                build: Arc::new(move |context| {
                    Ok(Box::new(MockVastAiPlugin::with_docker(
                        context.engine.blocking_work_sender(),
                        prefix.clone(),
                        context.runtime,
                    )))
                }),
                prepare_spec: Arc::new(|spec| {
                    spec.args = vec![myelin::ORCHESTRATOR_WORKER_MODE_ARG.to_owned()];
                    spec.env.push((
                        "MYELIN_DOCKER_GPUS".to_owned(),
                        std::env::var("MYELIN_DOCKER_GPUS").unwrap_or_default(),
                    ));
                    Ok(())
                }),
                configure: None,
                search_offers: Some(Arc::new(move |request| {
                    request.validate()?;
                    Ok(selected
                        .iter()
                        .map(|&id| Offer {
                            offer_id: id,
                            host_id: Some(id),
                            gpu_model: "fixture".to_owned(),
                            gpu_ram_mb: Some(24_576.0),
                            compute_cap: 800,
                            verification: Some("verified".to_owned()),
                            reliability: Some(1.0),
                            download_mbps: Some(1_000.0),
                            upload_mbps: Some(1_000.0),
                            location: Some("fixture".to_owned()),
                            hourly_price: 0.0,
                            download_cost_per_tb: 0.0,
                            upload_cost_per_tb: 0.0,
                        })
                        .collect())
                })),
            }
        }
        "vastai" => paid_composition(&args, image, bundle)?,
        other => return Err(format!("unsupported harness fixture provider {other:?}")),
    };
    myelin::prepare_orchestrator_with_providers(args, true, vec![composition], None)
}

struct PaidSettings {
    api_key: String,
    identity: PathBuf,
    command: String,
    config: VastAiProvisioningConfig,
}

fn paid_composition(
    args: &[String],
    image: String,
    bundle: Option<PathBuf>,
) -> Result<ProviderComposition, String> {
    let admission = std::env::var_os("MYELIN_PAID_FIXTURE_ADMISSION")
        .map(PathBuf::from)
        .ok_or("paid fixture admission path is required")?;
    let api_key = ["VAST_API_KEY", "MYELIN_VASTAI_API_KEY", "VASTAI_API_KEY"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok())
        .ok_or("paid fixture API key is required")?;
    let identity = path_option(args, "--vastai-ssh-identity", "MYELIN_VASTAI_SSH_IDENTITY")
        .ok_or("paid fixture SSH identity is required")?;
    let command = option(args, "--vastai-bootstrap-command")
        .or_else(|| std::env::var("MYELIN_VASTAI_BOOTSTRAP_COMMAND").ok())
        .unwrap_or_else(|| "exec /usr/local/bin/myelin-node".to_owned());
    let webhooks = VastAiWebhooks::open(
        VastAiWebhookConfig::from_env(admission.with_extension("webhooks"))?,
        &api_key,
    )?;
    let receiver = Arc::clone(&webhooks);
    let settings = Arc::new(Mutex::new(PaidSettings {
        api_key,
        identity,
        command,
        config: VastAiProvisioningConfig::default(),
    }));
    let configure_settings = settings.clone();
    let build_settings = settings.clone();
    let spec_settings = settings.clone();
    let deployed = bundle.is_some();
    Ok(ProviderComposition {
        http_plugin: Some(Arc::new(move |blocking| {
            dashboard::DashboardPlugin::new(receiver.router(blocking))
        })),
        kind: ProviderKind::new("vastai"),
        readiness: ProviderReadiness::unconfigured_for(
            "vastai",
            image,
            "validate paid fixture SSH configuration",
        ),
        remote: true,
        configure: Some(Arc::new(move |request| {
            let mut settings = configure_settings
                .lock()
                .map_err(|error| error.to_string())?;
            if let Some(key) = request.api_key
                && key != settings.api_key
            {
                return Err(
                    "restart required to rebind paid fixture webhook credentials".to_owned(),
                );
            }
            if let Some(identity) = request.ssh_identity {
                settings.identity = PathBuf::from(identity);
            }
            if let Some(command) = request.bootstrap_command {
                settings.command = command;
            }
            if settings.api_key.trim().is_empty() {
                return Err("paid fixture API key is empty".to_owned());
            }
            let public_key = derive_ssh_public_key(&settings.identity)?;
            ensure_vastai_account_ssh_key(&settings.api_key, &public_key)?;
            settings.config.ssh_public_key = Some(public_key);
            Ok(())
        })),
        build: Arc::new(move |context: ProviderContext| {
            let settings = build_settings.lock().map_err(|error| error.to_string())?;
            let api = swactor_vastai::VastClient::new(&settings.api_key);
            let monitor = ToolsVastAiLeaseClient::new(api.clone())?.with_actor_host(
                context.runtime.clone(),
                context.tasks,
                Arc::clone(&webhooks),
            );
            let lease = PaidVastAiLeaseClient::new(api, monitor, admission.clone())?;
            let inner: Box<dyn ProvisionPlugin> = if let Some(bundle) = &bundle {
                Box::new(VastAiProvisioningPlugin::new(
                    lease,
                    SshArtifactBootstrapLauncher::new(
                        settings.identity.clone(),
                        bundle.clone(),
                        context.runtime,
                        context.bootstrap_env,
                        context.engine,
                    ),
                    settings.config.clone(),
                ))
            } else {
                Box::new(VastAiProvisioningPlugin::new(
                    lease,
                    SshCommandBootstrapLauncher::new(
                        Some(settings.identity.clone()),
                        context.runtime,
                        context.bootstrap_env,
                        context.engine,
                    ),
                    settings.config.clone(),
                ))
            };
            Ok(Box::new(PaidProvisioningPlugin::new(
                inner,
                admission.clone(),
            )))
        }),
        prepare_spec: Arc::new(move |spec: &mut NodeProvisionSpec| {
            spec.args = vec![if deployed {
                format!("exec {DEPLOYED_WORKER_BIN_PATH}")
            } else {
                spec_settings
                    .lock()
                    .map_err(|error| error.to_string())?
                    .command
                    .clone()
            }];
            Ok(())
        }),
        search_offers: Some(Arc::new(move |request| {
            request.validate()?;
            let settings = settings.lock().map_err(|error| error.to_string())?;
            let mut client = ToolsVastAiLeaseClient::from_api_key(&settings.api_key)?;
            Ok(client
                .browse_offers(&request.browse_criteria())?
                .into_iter()
                .map(|offer| Offer {
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
        })),
    })
}
