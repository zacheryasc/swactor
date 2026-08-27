#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_plane::bootstrap::channel::{SessionBootstrap, bootstrap_channel};
use data_plane::path::SessionAccess;
use data_plane::protocol::{HostSessionIn, SessionCapability};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime, RuntimeConfig, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_process::{ProcessOutput, ProcessSpec};
use swactor_process_context::{
    ContextProvisioner, ContextualProcessOutput, ContextualProcessOutputConfig,
    ContextualProcessSpawner, ContextualProcessSpec, ExecutionIdentity, ProvisionedContext,
};

struct FakeHostSession;

impl ActorInterface for FakeHostSession {
    type Incoming = HostSessionIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostSessionIn) {
        if let HostSessionIn::Close { reply_to } = message {
            if let Some(reply_to) = reply_to {
                let _ = ctx.send(reply_to, Ok::<(), data_plane::protocol::DataPlaneError>(()));
            }
            ctx.stop_self();
        }
    }
}

struct FakeProvisioner {
    runtime: Runtime,
}

impl ContextProvisioner for FakeProvisioner {
    fn provision(
        &self,
        _identity: ExecutionIdentity,
        _access: SessionAccess,
    ) -> Result<ProvisionedContext, String> {
        let host_session = self
            .runtime
            .spawn(FakeHostSession)
            .map_err(|error| error.to_string())?;
        let (bootstrap_host, child_bootstrap) =
            bootstrap_channel().map_err(|error| error.to_string())?;
        let bootstrap_cancellation = bootstrap_host
            .cancellation_handle()
            .map_err(|error| error.to_string())?;
        let name = CString::new("contextual-actor-test").expect("memfd name");
        // SAFETY: name is NUL-terminated and memfd_create returns a fresh fd.
        let arena = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
        if arena < 0 {
            return Err(io::Error::last_os_error().to_string());
        }
        // SAFETY: memfd_create returned a fresh uniquely owned descriptor.
        let arena_fd = unsafe { OwnedFd::from_raw_fd(arena) };
        Ok(ProvisionedContext {
            host_session,
            arena_fd,
            child_bootstrap,
            bootstrap_host,
            bootstrap_cancellation,
            material: SessionBootstrap {
                host_session,
                session_capability: SessionCapability::new([7; 32]),
                routing: b"test-routing".to_vec(),
            },
        })
    }
}

struct LaunchActor {
    spawner: Arc<ContextualProcessSpawner>,
    sender: ExternalSender,
    spec: Option<ContextualProcessSpec>,
    output: Option<ContextualProcessOutputConfig>,
}

impl ActorInterface for LaunchActor {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.spawner
            .spawn(
                ctx,
                &self.sender,
                self.spec.take().expect("contextual spec"),
                self.output.take().expect("contextual output"),
            )
            .expect("spawn contextual actor");
        ctx.stop_self();
    }

    fn handle(&mut self, _ctx: &Ctx<'_>, _message: ()) {}
}

fn run_probe(arguments: Vec<String>) -> Vec<ContextualProcessOutput> {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    let output = runtime
        .new_inbox::<ContextualProcessOutput>()
        .expect("output inbox");
    let provisioner: Arc<dyn ContextProvisioner> = Arc::new(FakeProvisioner {
        runtime: runtime.clone(),
    });
    let spawner = Arc::new(ContextualProcessSpawner::new(engine.handle(), provisioner));
    runtime
        .spawn(LaunchActor {
            spawner,
            sender: runtime.create_sender(),
            spec: Some(ContextualProcessSpec {
                process: ProcessSpec {
                    command: env!("CARGO_BIN_EXE_context_guest_probe").to_owned(),
                    args: arguments,
                    env: HashMap::new(),
                    working_dir: None,
                    label: Some(format!(
                        "context-probe-{}",
                        ActorAddress::new_random().to_full_hex()
                    )),
                },
                access: SessionAccess {
                    execution_id: "actor-test".to_owned(),
                    read_prefixes: vec![],
                    write_prefixes: vec![],
                },
                attach_deadline: Duration::from_secs(5),
            }),
            output: Some(ContextualProcessOutputConfig::disabled(*output.addr())),
        })
        .expect("launch actor");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed = Vec::new();
    while Instant::now() < deadline {
        while let Some(event) = output.try_recv() {
            let terminal = matches!(
                event,
                ContextualProcessOutput::Process(
                    ProcessOutput::SpawnFailed { .. }
                        | ProcessOutput::Exited { .. }
                        | ProcessOutput::Error { .. }
                )
            );
            observed.push(event);
            if terminal {
                return observed;
            }
        }
        std::thread::yield_now();
    }
    panic!("contextual process did not terminate: {observed:?}");
}

fn position(
    outputs: &[ContextualProcessOutput],
    predicate: impl Fn(&ContextualProcessOutput) -> bool,
) -> usize {
    outputs.iter().position(predicate).expect("expected output")
}

#[test]
fn contextual_actor_reports_ready_before_native_exit() {
    let outputs = run_probe(Vec::new());
    let started = position(&outputs, |output| {
        matches!(
            output,
            ContextualProcessOutput::Process(ProcessOutput::Started { .. })
        )
    });
    let ready = position(&outputs, |output| {
        matches!(output, ContextualProcessOutput::ContextReady)
    });
    let exited = position(&outputs, |output| {
        matches!(
            output,
            ContextualProcessOutput::Process(ProcessOutput::Exited { .. })
        )
    });
    assert!(started < ready && ready < exited, "outputs: {outputs:?}");
}

#[test]
fn attachment_failure_is_resolved_before_native_exit() {
    let outputs = run_probe(vec!["--fail-attachment".to_owned()]);
    let failed = position(&outputs, |output| {
        matches!(output, ContextualProcessOutput::BootstrapFailed { .. })
    });
    let exited = position(&outputs, |output| {
        matches!(
            output,
            ContextualProcessOutput::Process(ProcessOutput::Exited { .. })
        )
    });
    assert!(failed < exited, "outputs: {outputs:?}");
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, ContextualProcessOutput::ContextReady))
    );
}
