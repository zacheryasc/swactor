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
use swactor_engine::{Engine, EngineHandle, TokioBackend, TokioConfig};
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

fn run_probe(
    runtime: &Runtime,
    engine: EngineHandle,
    arguments: Vec<String>,
) -> Vec<ContextualProcessOutput> {
    let output = runtime
        .new_inbox::<ContextualProcessOutput>()
        .expect("output inbox");
    let provisioner: Arc<dyn ContextProvisioner> = Arc::new(FakeProvisioner {
        runtime: runtime.clone(),
    });
    let spawner = Arc::new(ContextualProcessSpawner::new(engine, provisioner));
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
                attach_deadline: Some(Duration::from_secs(5)),
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
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let outputs = run_probe(&runtime, engine.handle(), Vec::new());
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
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let outputs = run_probe(
        &runtime,
        engine.handle(),
        vec!["--fail-attachment".to_owned()],
    );
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

struct OutputChannel(std::sync::mpsc::Sender<ContextualProcessOutput>);

impl ActorInterface for OutputChannel {
    type Incoming = ContextualProcessOutput;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, output: Self::Incoming) {
        let terminal = matches!(
            &output,
            ContextualProcessOutput::Process(
                ProcessOutput::Exited { .. }
                    | ProcessOutput::SpawnFailed { .. }
                    | ProcessOutput::Error { .. }
            )
        );
        self.0.send(output).unwrap();
        if terminal {
            ctx.stop_self();
        }
    }
}

fn launch_event_probe(
    runtime: &Runtime,
    engine: EngineHandle,
    argument: &str,
) -> std::sync::mpsc::Receiver<ContextualProcessOutput> {
    let (sender, output) = std::sync::mpsc::channel();
    let upstream = runtime.spawn(OutputChannel(sender)).unwrap();
    let spawner = Arc::new(ContextualProcessSpawner::new(
        engine,
        Arc::new(FakeProvisioner {
            runtime: runtime.clone(),
        }),
    ));
    runtime
        .spawn(LaunchActor {
            spawner,
            sender: runtime.create_sender(),
            spec: Some(ContextualProcessSpec::event_driven(
                ProcessSpec {
                    command: env!("CARGO_BIN_EXE_context_guest_probe").to_owned(),
                    args: vec![argument.to_owned()],
                    env: HashMap::new(),
                    working_dir: None,
                    label: None,
                },
                SessionAccess {
                    execution_id: "event-probe".to_owned(),
                    read_prefixes: vec![],
                    write_prefixes: vec![],
                },
            )),
            output: Some(ContextualProcessOutputConfig::disabled(upstream)),
        })
        .unwrap();
    output
}

fn assert_native_reaped(pid: u32) {
    let mut status = 0;
    assert_eq!(
        unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}
struct CensusReply(swactor_engine::ActorCompletion<swactor::admin::ListActorsResponse>);

impl ActorInterface for CensusReply {
    type Incoming = swactor::admin::ListActorsResponse;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, response: Self::Incoming) {
        let _ = self.0.complete(response);
        ctx.stop_self();
    }
}

#[test]
fn output_relay_loss_cancels_live_native_process_and_waits_for_real_reap() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let output = launch_event_probe(&runtime, engine.handle(), "--hold-context");
    let mut pid = None;
    loop {
        match output.recv().unwrap() {
            ContextualProcessOutput::Process(ProcessOutput::Started { pid: native }) => {
                pid = Some(native)
            }
            ContextualProcessOutput::ContextReady => break,
            other => panic!("unexpected output before ready: {other:?}"),
        }
    }
    let pid = pid.expect("native process started");
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
    let census = swactor_engine::ActorCompletion::new();
    let reply = runtime.spawn(CensusReply(census.clone())).unwrap();
    let _request = runtime
        .admin()
        .list_actors_to(reply, |response| response)
        .unwrap();
    let actors = census.wait();
    let relay = actors
        .actors
        .iter()
        .find(|actor| actor.actor_type.ends_with("::ProcessOutputRelay"))
        .expect("live native output relay")
        .address;
    runtime.stop_actor(relay).unwrap();
    loop {
        match output.recv().unwrap() {
            ContextualProcessOutput::Process(
                ProcessOutput::Stdout(_) | ProcessOutput::Stderr(_),
            ) => {}
            ContextualProcessOutput::Process(ProcessOutput::Exited { status }) => {
                // Immediate cancellation first offers TERM, then sends KILL if
                // the owned child is still live. Either real signal can win.
                assert!(matches!(
                    status,
                    swactor_process::ExitStatus::Signal(signal)
                        if signal == libc::SIGTERM || signal == libc::SIGKILL
                ));
                break;
            }
            other => panic!("observer loss must not fabricate native terminal output: {other:?}"),
        }
    }
    assert_native_reaped(pid);
}

#[test]
fn closed_bootstrap_peer_cancels_child_that_remains_live() {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let output = launch_event_probe(&runtime, engine.handle(), "--close-bootstrap-and-wait");
    let mut pid = None;
    let mut failed = false;
    loop {
        match output.recv().unwrap() {
            ContextualProcessOutput::Process(ProcessOutput::Started { pid: native }) => {
                pid = Some(native)
            }
            ContextualProcessOutput::BootstrapFailed { reason } => {
                assert_eq!(
                    reason,
                    swactor_process_context::BootstrapFailure::ChannelClosed
                );
                failed = true;
            }
            ContextualProcessOutput::Process(ProcessOutput::Exited { status }) => {
                assert!(
                    failed,
                    "bootstrap EOF must be observed before native termination"
                );
                assert_eq!(status, swactor_process::ExitStatus::Signal(libc::SIGTERM));
                break;
            }
            other => panic!("unexpected bootstrap-close output: {other:?}"),
        }
    }
    assert_native_reaped(pid.expect("native process started"));
}
