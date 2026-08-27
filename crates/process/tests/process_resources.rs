#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::{Duration, Instant};

use swactor::actor::{ActorInterface, Ctx};
use swactor::runtime::{Inbox, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime};
use swactor_process::{
    ExitStatus, ProcessOutput, ProcessOutputConfig, ProcessResourceError, ProcessSpawnResources,
    ProcessSpec, spawn_local_process, spawn_local_process_with_resources,
};

const CHILD_BOOTSTRAP_FD: RawFd = 198;

struct SpawnOnce {
    spec: Option<ProcessSpec>,
    resources: Option<ProcessSpawnResources>,
    output: Option<ProcessOutputConfig>,
    sender: swactor::runtime::ExternalSender,
}

impl ActorInterface for SpawnOnce {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        spawn_local_process_with_resources(
            ctx,
            &self.sender,
            self.spec.take().expect("process spec"),
            self.resources.take().expect("process resources"),
            self.output.take().expect("process output"),
        )
        .expect("spawn process actor");
        ctx.stop_self();
    }

    fn handle(&mut self, _ctx: &Ctx<'_>, _message: ()) {}
}
struct SpawnNativeOnce {
    spec: Option<ProcessSpec>,
    output: Option<ProcessOutputConfig>,
    sender: swactor::runtime::ExternalSender,
}

impl ActorInterface for SpawnNativeOnce {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        spawn_local_process(
            ctx,
            &self.sender,
            self.spec.take().expect("process spec"),
            self.output.take().expect("process output"),
        )
        .expect("spawn native process actor");
        ctx.stop_self();
    }

    fn handle(&mut self, _ctx: &Ctx<'_>, _message: ()) {}
}

fn seqpacket_pair() -> (OwnedFd, OwnedFd) {
    let mut descriptors = [-1; 2];
    // SAFETY: storage contains two descriptor slots and successful socketpair
    // initializes both with uniquely owned descriptors.
    let result = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            descriptors.as_mut_ptr(),
        )
    };
    assert_eq!(result, 0, "socketpair: {}", io::Error::last_os_error());
    // SAFETY: successful socketpair returned two fresh descriptors.
    unsafe {
        (
            OwnedFd::from_raw_fd(descriptors[0]),
            OwnedFd::from_raw_fd(descriptors[1]),
        )
    }
}

fn recv_packet(fd: RawFd) -> Vec<u8> {
    let mut bytes = [0_u8; 64];
    // SAFETY: bytes is writable and fd is a connected seqpacket endpoint.
    let length = unsafe {
        libc::recv(
            fd,
            bytes.as_mut_ptr().cast(),
            bytes.len(),
            libc::MSG_CMSG_CLOEXEC,
        )
    };
    assert!(length >= 0, "recv: {}", io::Error::last_os_error());
    bytes[..length as usize].to_vec()
}

fn runtime_host() -> (Runtime, SingleThreadRuntime) {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let host = SingleThreadRuntime::new(parts);
    (runtime, host)
}

fn python_spec(script: &str, args: Vec<String>, label: &str) -> ProcessSpec {
    let mut command_args = vec!["-c".to_owned(), script.to_owned()];
    command_args.extend(args);
    ProcessSpec {
        command: "python3".to_owned(),
        args: command_args,
        env: HashMap::new(),
        working_dir: None,
        label: Some(label.to_owned()),
    }
}

fn spawn_once(
    runtime: &Runtime,
    spec: ProcessSpec,
    resources: ProcessSpawnResources,
    output: &Inbox<ProcessOutput>,
) {
    runtime
        .spawn(SpawnOnce {
            spec: Some(spec),
            resources: Some(resources),
            output: Some(ProcessOutputConfig::disabled(*output.addr())),
            sender: runtime.create_sender(),
        })
        .expect("spawn resource test actor");
}

fn drive_until_exits(
    host: &mut SingleThreadRuntime,
    output: &Inbox<ProcessOutput>,
    expected: usize,
) -> Vec<ProcessOutput> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed = Vec::new();
    while Instant::now() < deadline {
        host.tick();
        while let Some(event) = output.try_recv() {
            observed.push(event);
        }
        if observed
            .iter()
            .filter(|event| matches!(event, ProcessOutput::Exited { .. }))
            .count()
            == expected
        {
            return observed;
        }
        std::thread::yield_now();
    }
    panic!("processes did not exit: {observed:?}");
}

#[test]
fn descriptor_mapping_survives_exec_and_source_stays_cloexec() {
    let (host_endpoint, child_endpoint) = seqpacket_pair();
    let source_fd = child_endpoint.as_raw_fd();
    let mut resources = ProcessSpawnResources::new();
    resources
        .add_descriptor(child_endpoint, CHILD_BOOTSTRAP_FD)
        .expect("resource mapping");

    // SAFETY: F_GETFD only reads flags for a live descriptor.
    let flags = unsafe { libc::fcntl(source_fd, libc::F_GETFD) };
    assert_ne!(flags & libc::FD_CLOEXEC, 0);

    let (runtime, mut runtime_host) = runtime_host();
    let output = runtime.new_inbox::<ProcessOutput>().expect("output inbox");
    spawn_once(
        &runtime,
        python_spec(
            "import os,sys\nsource=int(sys.argv[1])\ntry:\n os.fstat(source)\nexcept OSError:\n os.write(198,b'mapped')\nelse:\n raise SystemExit(3)",
            vec![source_fd.to_string()],
            "resource-owned",
        ),
        resources,
        &output,
    );
    let observed = drive_until_exits(&mut runtime_host, &output, 1);

    assert!(
        observed
            .iter()
            .any(|event| matches!(event, ProcessOutput::Started { .. }))
    );
    assert_eq!(recv_packet(host_endpoint.as_raw_fd()), b"mapped");
}

#[test]
fn concurrent_children_receive_only_their_own_descriptor() {
    let (host_a, child_a) = seqpacket_pair();
    let (host_b, child_b) = seqpacket_pair();
    let source_a = child_a.as_raw_fd();
    let source_b = child_b.as_raw_fd();

    let mut resources_a = ProcessSpawnResources::new();
    resources_a
        .add_descriptor(child_a, CHILD_BOOTSTRAP_FD)
        .expect("resource A");
    let mut resources_b = ProcessSpawnResources::new();
    resources_b
        .add_descriptor(child_b, CHILD_BOOTSTRAP_FD)
        .expect("resource B");

    let script = "import os,sys\nfor value in sys.argv[1:3]:\n try:\n  os.fstat(int(value))\n except OSError:\n  pass\n else:\n  raise SystemExit(3)\nos.write(198,sys.argv[3].encode())";
    let (runtime, mut runtime_host) = runtime_host();
    let output = runtime.new_inbox::<ProcessOutput>().expect("output inbox");
    spawn_once(
        &runtime,
        python_spec(
            script,
            vec![source_a.to_string(), source_b.to_string(), "A".to_owned()],
            "resource-a",
        ),
        resources_a,
        &output,
    );
    spawn_once(
        &runtime,
        python_spec(
            script,
            vec![source_a.to_string(), source_b.to_string(), "B".to_owned()],
            "resource-b",
        ),
        resources_b,
        &output,
    );
    let _ = drive_until_exits(&mut runtime_host, &output, 2);

    assert_eq!(recv_packet(host_a.as_raw_fd()), b"A");
    assert_eq!(recv_packet(host_b.as_raw_fd()), b"B");
}

#[test]
fn native_spawn_receives_no_contextual_descriptor() {
    let (runtime, mut runtime_host) = runtime_host();
    let output = runtime.new_inbox::<ProcessOutput>().expect("output inbox");
    runtime
        .spawn(SpawnNativeOnce {
            spec: Some(python_spec(
                "import os\ntry:\n os.fstat(198)\nexcept OSError:\n raise SystemExit(0)\nraise SystemExit(3)",
                Vec::new(),
                "native-no-context",
            )),
            output: Some(ProcessOutputConfig::disabled(*output.addr())),
            sender: runtime.create_sender(),
        })
        .expect("spawn native test actor");
    let observed = drive_until_exits(&mut runtime_host, &output, 1);
    assert!(observed.iter().any(|event| {
        matches!(
            event,
            ProcessOutput::Exited {
                status: ExitStatus::Code(0)
            }
        )
    }));
}

#[test]
fn duplicate_and_standard_child_targets_are_rejected() {
    let (_, first) = seqpacket_pair();
    let (_, second) = seqpacket_pair();
    let (_, standard) = seqpacket_pair();
    let mut resources = ProcessSpawnResources::new();
    resources
        .add_descriptor(first, CHILD_BOOTSTRAP_FD)
        .expect("first target");
    assert!(matches!(
        resources.add_descriptor(second, CHILD_BOOTSTRAP_FD),
        Err(ProcessResourceError::DuplicateChildDescriptor(
            CHILD_BOOTSTRAP_FD
        ))
    ));
    assert!(matches!(
        resources.add_descriptor(standard, 2),
        Err(ProcessResourceError::InvalidChildDescriptor(2))
    ));
}
