//! Demo bootstrap logic #2: docker container as foreign node.
//!
//! Implements [`BootstrapLogic`] for `"docker"`-kind specs: the node is a
//! `scratch` container running the statically-linked xtask binary in node
//! role, launched attached (`docker run --rm`) so the supervised `docker`
//! CLI child's lifetime tracks the container's — its exit *is* the node
//! exit. Readiness is the same control-plane announce as the process kind.
//!
//! Foreign-node masking: every container runs on a per-run user-defined
//! bridge network, so each node gets its own bridge IP and the supervisor is
//! reached through the bridge gateway — no localhost shortcuts, UDP
//! hole-punching over a real (if virtual) network.
//!
//! Termination always goes through `docker rm -f` (force-remove): signals to
//! the attached CLI are unreliable proxies, and a force-remove both kills
//! the container and satisfies `--rm` cleanup. Zero wastage by construction:
//! no volumes, no mounts, `--rm` containers, and label-filtered startup +
//! exit sweeps that remove anything a SIGKILLed supervisor left behind.
//! Images persist across runs (rebuilds are content-addressed by run token).

use std::path::Path;
use std::time::SystemTime;

use swactor::actor::{ActorAddress, Ctx};
use swactor::runtime::ExternalSender;
use swactor_process::{ProcessOutputConfig, ProcessSpec, spawn_local_process};

use provisioning::bootstrap::{BootstrapLogic, LogicProbe, NodeLaunchSpec};

use crate::demo::provider::{NodeManager, NodeRelayActor, NodeRuntime};

/// Generic label present on every demo container/network (sweep key).
pub const SWEEP_LABEL: &str = "swactor-demo";
/// Per-run label value: only this run's resources.
pub const RUN_LABEL: &str = "swactor-demo-run";
/// Image repository (tagged per run token).
pub const IMAGE_REPO: &str = "swactor-demo-node";
/// Static-musl target the node image is built from.
pub const IMAGE_TARGET: &str = "x86_64-unknown-linux-musl";

const DOCKERFILE: &str = include_str!("docker/Dockerfile");

/// Everything the docker launch style needs, produced by [`preflight`].
#[derive(Clone)]
pub struct DockerLaunch {
    /// Fully-qualified image ref (`swactor-demo-node:<token>`).
    pub image: String,
    /// Per-run user-defined bridge network name.
    pub network: String,
    /// Per-run label value used by the exit sweep.
    pub run_token: String,
    /// Serde `iroh::EndpointAddr` of the supervisor, rebuilt to advertise
    /// the bridge gateway address (containers cannot use the host's
    /// localhost or LAN addrs meaningfully).
    pub supervisor_addr_json: String,
}

/// Container name for one provision attempt (unique per attempt).
pub fn container_name(attempt: u64) -> String {
    format!("{SWEEP_LABEL}-node-{attempt}")
}

/// Docker bootstrap logic. Identical lifecycle shape to the local process
/// kind — the supervised child is the attached `docker run` CLI; the
/// container is force-removed on every terminal path (terminate, exit,
/// spawn failure, deregistration) so it can never outlive the attempt.
pub struct DockerProcessLogic {
    spec: NodeLaunchSpec,
    manager: NodeManager,
    process_actor: Option<ActorAddress>,
    /// The attempt's container has been force-removed (or removal was
    /// spawned); guards idempotency across probe/terminate paths.
    removed: bool,
}

impl DockerProcessLogic {
    pub fn new(spec: NodeLaunchSpec, manager: NodeManager) -> Self {
        Self {
            spec,
            manager,
            process_actor: None,
            removed: false,
        }
    }

    /// Idempotent force-remove of this attempt's container (kills it if
    /// running, satisfies --rm, releases the name; no-op if already gone).
    fn force_remove(&mut self) {
        if self.removed {
            return;
        }
        self.removed = true;
        let name = container_name(self.spec.attempt);
        std::thread::spawn(move || {
            let status = std::process::Command::new("docker")
                .args(["rm", "-f", "--", &name])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            if let Err(error) = status {
                eprintln!("demo: docker rm -f {name} failed: {error}");
            }
        });
    }
}

impl BootstrapLogic for DockerProcessLogic {
    fn start(
        &mut self,
        ctx: &Ctx,
        owner: ActorAddress,
        sender: &ExternalSender,
    ) -> Result<(), String> {
        let attempt = self.spec.attempt;
        let relay = ctx
            .spawn(NodeRelayActor::new(self.manager.clone(), attempt))
            .map_err(|error| format!("spawn relay actor: {error}"))?;

        let (command, args) = self
            .spec
            .argv
            .split_first()
            .map(|(head, tail)| (head.clone(), tail.to_vec()))
            .ok_or_else(|| "docker spec missing argv".to_owned())?;
        let spec = ProcessSpec {
            command,
            args,
            env: self.spec.env.clone().into_iter().collect(),
            working_dir: self.spec.workdir.clone(),
            label: self.spec.label.clone(),
        };
        let output = ProcessOutputConfig::disabled(relay);
        let process_actor = spawn_local_process(ctx, sender, spec, output)
            .map_err(|error| format!("spawn docker process actor: {error}"))?;
        self.process_actor = Some(process_actor);

        self.manager.register(NodeRuntime {
            attempt,
            logical_node: self.spec.logical_node.clone(),
            bootstrap: owner,
            pid: None,
            exited: None,
            spawn_failed: None,
            last_announce_ms: None,
            endpoint_addr: None,
        });
        Ok(())
    }

    fn probe(&mut self, _now: SystemTime) -> LogicProbe {
        let Some(runtime) = self.manager.get(self.spec.attempt) else {
            self.force_remove();
            return LogicProbe::Exited("deregistered (lease destroyed)".to_owned());
        };
        if let Some(error) = &runtime.spawn_failed {
            self.force_remove();
            return LogicProbe::Failed(format!("docker run failed to spawn: {error}"));
        }
        if let Some(status) = &runtime.exited {
            // The attached CLI's exit mirrors the container's (or is the
            // launch failure itself); the actor's phase machine classifies
            // Failed-vs-Exited against the announce. Either way the
            // container must not outlive the attempt: a SIGKILLed CLI
            // orphans a *running* container (signal-proxy never fired), so
            // the exit path force-removes too — not just terminate().
            self.force_remove();
            return LogicProbe::Exited(format!("docker run {status:?}"));
        }
        LogicProbe::Pending
    }

    fn terminate(&mut self, sender: &ExternalSender, kill_after: Option<std::time::Duration>) {
        // Force-remove the container: kills it regardless of signal-proxy
        // semantics, satisfies --rm, and releases the name. The attached
        // CLI child then exits on its own; the explicit Stop below is
        // actor-tree hygiene (the kill_after escalation applies to the
        // CLI, not the container).
        self.force_remove();
        if let Some(process_actor) = self.process_actor {
            let _ = swactor_process::send_process_command(
                sender,
                process_actor,
                swactor_process::ProcessCommand::Stop { kill_after },
            );
        }
    }
}

// ─── Preflight: image + network ─────────────────────────────────────────────

/// Build the node image and the per-run network: sweep stale demo resources,
/// compile the static-musl xtask binary, `docker build` it from a staging
/// dir, create the labeled bridge network, and resolve the gateway address
/// the containers will dial the supervisor on.
pub fn preflight(
    root: &Path,
    supervisor_pubkey: iroh::PublicKey,
    port: u16,
) -> Result<DockerLaunch, String> {
    docker_version()?;
    let run_token = format!("{}-{}", std::process::id(), unix_ms());
    sweep_stale();
    let image = format!("{IMAGE_REPO}:{run_token}");
    build_image(root, &image)?;
    let network = format!("{SWEEP_LABEL}-net-{run_token}");
    docker_ok(
        &[
            "network",
            "create",
            "--label",
            &format!("{SWEEP_LABEL}=1"),
            "--label",
            &format!("{RUN_LABEL}={run_token}"),
            "--",
            &network,
        ],
        "create demo network",
    )?;
    let gateway = docker_output(
        &[
            "network",
            "inspect",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
            "--",
            &network,
        ],
        "read demo network gateway",
    )?;
    let gateway_ip: std::net::IpAddr = gateway
        .trim()
        .parse()
        .map_err(|error| format!("network gateway {gateway:?}: {error}"))?;
    let supervisor_addr = iroh::EndpointAddr::new(supervisor_pubkey)
        .with_ip_addr(std::net::SocketAddr::new(gateway_ip, port));
    let supervisor_addr_json = serde_json::to_string(&supervisor_addr)
        .map_err(|error| format!("serialize supervisor addr: {error}"))?;
    Ok(DockerLaunch {
        image,
        network,
        run_token,
        supervisor_addr_json,
    })
}

/// Remove every container and network carrying the generic demo label. All
/// such resources found here are stale: this runs before the current run
/// creates anything, so anything matched is a leftover (e.g. of a SIGKILLed
/// supervisor). One demo instance at a time.
fn sweep_stale() {
    let containers = docker_output(
        &["ps", "-aq", "--filter", &format!("label={SWEEP_LABEL}=1")],
        "list stale demo containers",
    )
    .unwrap_or_default();
    let ids: Vec<String> = containers
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    if !ids.is_empty() {
        let mut args = vec!["rm".to_owned(), "-f".to_owned()];
        args.extend(ids.iter().map(|id| id.to_string()));
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let _ = docker_ok(&refs, "remove stale demo containers");
    }
    let networks = docker_output(
        &[
            "network",
            "ls",
            "-q",
            "--filter",
            &format!("label={SWEEP_LABEL}=1"),
        ],
        "list stale demo networks",
    )
    .unwrap_or_default();
    let nets: Vec<String> = networks
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    if !nets.is_empty() {
        let mut args = vec!["network".to_owned(), "rm".to_owned()];
        args.extend(nets.iter().map(|id| id.to_string()));
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let _ = docker_ok(&refs, "remove stale demo networks");
    }
}

/// Exit-path sweep: remove this run's containers (by run token) and its
/// network. The image persists (per-run tokens make old images trivially
/// identifiable; they are tiny scratch images).
pub fn sweep_run(launch: &DockerLaunch) {
    let containers = docker_output(
        &[
            "ps",
            "-aq",
            "--filter",
            &format!("label={RUN_LABEL}={}", launch.run_token),
        ],
        "list run containers",
    )
    .unwrap_or_default();
    let ids: Vec<&str> = containers
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    if !ids.is_empty() {
        let mut args = vec!["rm".to_owned(), "-f".to_owned()];
        args.extend(ids.iter().map(|id| id.to_string()));
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let _ = docker_ok(&refs, "remove run containers");
    }
    let _ = docker_ok(
        &["network", "rm", "--", &launch.network],
        "remove run network",
    );
}

/// Build the scratch image: stage the static binary + Dockerfile in a temp
/// dir (keeps the build context to one file), then `docker build`.
fn build_image(root: &Path, image: &str) -> Result<(), String> {
    println!("demo --docker: building static node binary…");
    let bin = root
        .join("target")
        .join(IMAGE_TARGET)
        .join("release")
        .join("xtask");
    // Build when the static binary is missing; DEMO_DOCKER_REBUILD=1 forces
    // a rebuild (e.g. after source changes).
    let force_rebuild = std::env::var("DEMO_DOCKER_REBUILD").as_deref() == Ok("1");
    if force_rebuild || !bin.exists() {
        run_cargo_build(root)?;
    }
    if !bin.exists() {
        return Err(format!(
            "node binary missing after build: {}",
            bin.display()
        ));
    }
    let staging = std::env::temp_dir().join(format!("{SWEEP_LABEL}-image-{}", unix_ms()));
    std::fs::create_dir_all(&staging).map_err(|e| format!("staging dir: {e}"))?;
    let result = (|| {
        std::fs::copy(&bin, staging.join("xtask")).map_err(|e| format!("stage binary: {e}"))?;
        std::fs::write(staging.join("Dockerfile"), DOCKERFILE)
            .map_err(|e| format!("stage Dockerfile: {e}"))?;
        docker_ok(
            &["build", "-q", "-t", image, "--", &staging.to_string_lossy()],
            "build demo node image",
        )
    })();
    let _ = std::fs::remove_dir_all(&staging);
    result.map(|_| {
        println!("demo --docker: image {image} ready");
    })
}

fn run_cargo_build(root: &Path) -> Result<(), String> {
    let output = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "--target",
            IMAGE_TARGET,
            "--package",
            "xtask",
        ])
        .current_dir(root)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("run cargo: {e}"))?;
    if !output.status.success() {
        let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if stderr.len() > 4000 {
            stderr.truncate(4000);
        }
        return Err(format!("cargo build for {IMAGE_TARGET} failed:\n{stderr}"));
    }
    Ok(())
}

/// Docker CLI + daemon reachable?
fn docker_version() -> Result<(), String> {
    docker_output(
        &["version", "--format", "{{.Server.Version}}"],
        "docker daemon",
    )
    .map(|_| ())
}

fn docker_ok(args: &[&str], label: &str) -> Result<(), String> {
    let output = docker_raw(args)?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("{label} failed: {stderr}"))
    }
}

fn docker_output(args: &[&str], label: &str) -> Result<String, String> {
    let output = docker_raw(args)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("{label} failed: {stderr}"))
    }
}

fn docker_raw(args: &[&str]) -> Result<std::process::Output, String> {
    std::process::Command::new("docker")
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| format!("run docker {args:?}: {e}"))
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
