//! Boot-phase provisioning telemetry over SSH (via the `process` crate).
//!
//! While a rented node boots — docker pull, image load, the worker's first
//! seconds before its swactor datastream is live — the orchestrator streams the
//! node's boot log back over an SSH channel and folds it onto its *own*
//! datastream as `proc.boot.<stage>.*`. That is the "talk directly to the
//! swactor process that spawned it" half: boot status reaches the very same
//! [`DatastreamSink`](datastream::DatastreamSink) the running-phase
//! cluster telemetry uses, with no dedicated channel. Once the node is running
//! swactor it ships its own structured telemetry over the cluster transport and
//! the SSH boot log is just history.
//!
//! This is **best-effort and opt-in**: it activates only when a deploy SSH key
//! is configured ([`deploy_key_path`]) and the fleet sink is live. The container
//! entrypoint still launches `pp-worker`, so a run never depends on SSH working.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::process_observer::ProcessOutputObserver;
use swactor::runtime::{Ctx, ExternalSender, Runtime};

use dashboard::telemetry::{process_output, IdentityRecord, ProcStream, IDENTITY};
use datastream::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use datastream::wire::{encode_delivery, DatastreamFrame};
use datastream::Record;

use swactor_process::ssh::SshConfig;
use swactor_process::{spawn_ssh_process, ProcessMode, ProcessSpec};

/// Path to the orchestrator's deploy SSH private key, from `PP_DEPLOY_KEY`.
/// `None` (key unset or missing) disables SSH boot telemetry entirely.
pub fn deploy_key_path() -> Option<PathBuf> {
    let p = std::env::var("PP_DEPLOY_KEY")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    let path = PathBuf::from(p);
    path.exists().then_some(path)
}

/// Where to reach a rented node over SSH.
#[derive(Debug, Clone)]
pub struct SshTarget {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub key_file: PathBuf,
}

/// Folds boot-phase SSH output onto the orchestrator's datastream. Each tapped
/// chunk is shipped straight to the in-process `datastream-sink` as a
/// `DatastreamFrame` under the orchestrator's stream — no tick loop, no UDP. The
/// frame's position is a per-channel monotonic counter (text channels tolerate
/// gaps, so an in-process send that the sink drops merely advances the count).
struct BootTelemetry {
    rt: Arc<Runtime>,
    sink: ActorAddress,
    stream: StreamId,
    positions: Mutex<HashMap<String, u64>>,
}

impl BootTelemetry {
    fn ship(&self, channel: ChannelId, payload: Vec<u8>) {
        let position = {
            let mut pos = self.positions.lock().unwrap();
            let n = pos.entry(channel.as_str().to_string()).or_insert(0);
            let p = *n;
            *n += 1;
            Position(p)
        };
        let frame = Frame::new(channel, position, payload);
        let _ = self.rt.send_to(
            self.sink,
            DatastreamFrame {
                payload: encode_delivery(&self.stream, &frame),
            },
        );
    }
}

impl ProcessOutputObserver for BootTelemetry {
    fn on_output(&self, label: &str, is_stderr: bool, data: &[u8]) {
        let stream = if is_stderr {
            ProcStream::Stderr
        } else {
            ProcStream::Stdout
        };
        self.ship(process_output(label, stream), data.to_vec());
    }
}

/// Install boot telemetry on the orchestrator runtime: ship one labelled
/// identity frame so the Fleet table shows the orchestrator's own row, then tap
/// every SSH child's output onto `proc.boot.<stage>.*` of that row, all routed
/// in-process to `sink`. Idempotent — the observer slot is set once.
pub fn install_boot_telemetry(rt: &Arc<Runtime>, orch_hex: &str, life: u64, sink: ActorAddress) {
    let boot = Arc::new(BootTelemetry {
        rt: Arc::clone(rt),
        sink,
        stream: StreamId::new(NodeId::new(orch_hex), Lifetime(life)),
        positions: Mutex::new(HashMap::new()),
    });
    // Label the orchestrator's own row before any process output flows.
    boot.ship(
        ChannelId::new(IDENTITY),
        IdentityRecord {
            node: orch_hex.to_string(),
            life,
            node_name: "pp-orchestrator".to_string(),
            ..Default::default()
        }
        .encode(),
    );
    rt.set_process_output_observer(boot);
}

/// What the [`ProvisionActor`] receives.
#[derive(Clone)]
pub enum ProvisionMsg {
    /// SSH into a rented node and stream `pp-worker`'s boot log back. The output
    /// is tapped onto `proc.boot.<stage>.*` by the installed [`BootTelemetry`].
    TailStage { stage: u32, ssh: SshTarget },
}

/// Drives boot-phase SSH connections. It exists so the SSH spawn has a `Ctx`
/// (the `process` facility's spawn helpers require one); the orchestrator sends
/// it one [`ProvisionMsg::TailStage`] per leased stage.
pub struct ProvisionActor {
    sender: ExternalSender,
    tokio_handle: tokio::runtime::Handle,
}

impl ProvisionActor {
    pub fn new(sender: ExternalSender, tokio_handle: tokio::runtime::Handle) -> Self {
        Self {
            sender,
            tokio_handle,
        }
    }
}

impl ActorInterface for ProvisionActor {
    type Incoming = ProvisionMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: ProvisionMsg) {
        match msg {
            ProvisionMsg::TailStage { stage, ssh } => {
                let cfg = SshConfig {
                    host: ssh.host,
                    port: ssh.port,
                    username: ssh.username,
                    key_file: ssh.key_file,
                    key_passphrase: None,
                };
                // `tail -F` follows the worker log across the boot→running
                // handoff. Automated mode runs it via `exec` (no PTY); the
                // remote env is empty (nothing to pass for a tail).
                let spec = ProcessSpec {
                    command: "tail".to_string(),
                    args: vec![
                        "-n".to_string(),
                        "+1".to_string(),
                        "-F".to_string(),
                        "/var/log/pp-worker.log".to_string(),
                    ],
                    env: HashMap::new(),
                    working_dir: None,
                    mode: ProcessMode::Automated,
                    initial_pty_size: None,
                    kill_timeout: Some(Duration::from_secs(5)),
                    stdin_buffer_limit: None,
                };
                match spawn_ssh_process(
                    ctx,
                    &self.sender,
                    spec,
                    self.tokio_handle.clone(),
                    cfg,
                    Some(format!("boot.{stage}")),
                ) {
                    Ok(addr) => {
                        eprintln!("provision: stage {stage} boot-log tail -> {addr:?}")
                    }
                    Err(e) => {
                        eprintln!("provision: stage {stage} boot-log tail failed: {e}")
                    }
                }
            }
        }
    }
}
