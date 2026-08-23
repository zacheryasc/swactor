#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use data_plane::bootstrap::{
    ENV_ARENA_FD, ENV_DATA_PLANE_ACTOR, ENV_DATA_PLANE_ENDPOINT, ENV_JOB_CAPABILITY,
};
use data_plane::data_plane::StreamConsumer;
use data_plane::namespace::{
    DataDirectoryActor, NamespaceClient, NamespaceClientActor, NamespaceDiscovery,
};
use data_plane::path::{DataPath, JobContext};
use data_plane::protocol::JobCapability;
use data_plane::source::BlobSourcePublisher;
use data_plane::stream_transport::{LocalStreamTransport, StreamTransport};
use futures_lite::future;
use parking_lot::Mutex;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use crate::job_data_plane::ActorJobDataPlane;

const ARENA_BYTES: u64 = 4096;
const CAPABILITY: JobCapability = JobCapability::new([3; 32]);

fn path(value: &str) -> DataPath {
    DataPath::parse(value).expect("test path")
}

fn plane() -> ActorJobDataPlane {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    ActorJobDataPlane::new(
        &runtime,
        crate::job_data_plane::ActorJobDataPlaneConfig {
            arena_bytes: ARENA_BYTES,
            arena_generation: 11,
            session_generation: 13,
            capability: CAPABILITY,
            job_context: JobContext {
                run_id: "run-1".to_owned(),
                read_prefixes: vec![path("/models")],
                write_prefixes: vec![path("/runs/run-1/results")],
            },
            namespace: None,
            transfer_receiver: None,
            source_sender: None,
            source_publisher: None,
            route_registrar: None,
            stream_transport: None,
        },
    )
    .expect("actor data-plane")
}

#[test]
fn handoff_contains_one_descriptor_and_private_actor_metadata() {
    let plane = plane();
    let env = plane.handoff_env(r#"{"id":"host"}"#);

    assert_eq!(
        env.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ENV_ARENA_FD,
            ENV_DATA_PLANE_ACTOR,
            ENV_DATA_PLANE_ENDPOINT,
            ENV_JOB_CAPABILITY,
        ])
    );
    assert_eq!(env[ENV_ARENA_FD], plane.arena_fd().to_string());
    assert_eq!(env[ENV_DATA_PLANE_ACTOR].len(), 64);
    assert_eq!(env[ENV_JOB_CAPABILITY], CAPABILITY.to_hex());
    assert_eq!(env[ENV_DATA_PLANE_ENDPOINT], r#"{"id":"host"}"#);

    // The only inherited descriptor is the arena and it remains inheritable.
    // SAFETY: F_GETFD only inspects the live descriptor retained by `plane`.
    let flags = unsafe { libc::fcntl(plane.arena_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(flags & libc::FD_CLOEXEC, 0);
}

struct StaticDiscovery(ActorAddress);

impl NamespaceDiscovery for StaticDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        Some(self.0)
    }
}

struct LocalPublisher;

impl BlobSourcePublisher for LocalPublisher {
    fn publish_source(&self, _source: ActorAddress) -> Result<(), String> {
        Ok(())
    }
}

struct BytesConsumer(Arc<Mutex<Vec<u8>>>);

impl StreamConsumer for BytesConsumer {
    fn consume(&self, bytes: &[u8]) -> Result<(), String> {
        self.0.lock().extend_from_slice(bytes);
        Ok(())
    }
}

#[test]
fn canonical_inference_result_uses_native_stream_end_to_end() {
    let state_root = std::env::temp_dir().join(format!(
        "myelin-stream-test-{}",
        ActorAddress::new_random().to_full_hex()
    ));
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    let directory = runtime
        .spawn(
            DataDirectoryActor::recover(state_root.join("namespace.json"), |_recovery, _length| {
                Err(data_plane::namespace::NamespaceError::SourceRecovery(
                    "no recovered blob sources".to_owned(),
                ))
            })
            .expect("directory"),
        )
        .expect("spawn directory");
    let proxy = runtime
        .spawn(NamespaceClientActor::new(
            engine.handle(),
            runtime.create_sender(),
            Arc::new(StaticDiscovery(directory)),
            Duration::from_millis(5),
        ))
        .expect("namespace proxy");
    let namespace = NamespaceClient::new(runtime.clone(), proxy);
    let transport: Arc<dyn StreamTransport> = Arc::new(LocalStreamTransport::new());
    let publisher: Arc<dyn BlobSourcePublisher> = Arc::new(LocalPublisher);

    let make_plane = |read: bool| {
        ActorJobDataPlane::new(
            &runtime,
            crate::job_data_plane::ActorJobDataPlaneConfig {
                arena_bytes: 1 << 20,
                arena_generation: if read { 21 } else { 22 },
                session_generation: if read { 31 } else { 32 },
                capability: CAPABILITY,
                job_context: JobContext {
                    run_id: "0".to_owned(),
                    read_prefixes: if read {
                        vec![path("/runs/0/results")]
                    } else {
                        Vec::new()
                    },
                    write_prefixes: if read {
                        Vec::new()
                    } else {
                        vec![path("/runs/0/results")]
                    },
                },
                namespace: Some(namespace.clone()),
                transfer_receiver: None,
                source_sender: None,
                source_publisher: Some(Arc::clone(&publisher)),
                route_registrar: None,
                stream_transport: Some(Arc::clone(&transport)),
            },
        )
        .expect("actor plane")
    };
    let reader_plane = make_plane(true);
    let writer_plane = make_plane(false);
    let reader = reader_plane.attach_local().expect("reader attachment");
    let writer = writer_plane.attach_local().expect("writer attachment");
    let logical = path("/runs/0/results/inference");
    let observed = Arc::new(Mutex::new(Vec::new()));
    let consumer: Arc<dyn StreamConsumer> = Arc::new(BytesConsumer(Arc::clone(&observed)));
    let completed = reader
        .collect_stream(logical.clone(), consumer)
        .expect("register result sink");

    future::block_on(async {
        let mut writer = writer.write_stream(&logical).await.expect("open writer");
        writer
            .write(br#"{"device":"CUDA:0","output":[2.75,-8.75]}"#)
            .await
            .expect("write result");
        writer.close().await.expect("close writer");
    });
    completed.wait().expect("result sink completed");
    assert_eq!(
        &*observed.lock(),
        br#"{"device":"CUDA:0","output":[2.75,-8.75]}"#
    );
    let _ = std::fs::remove_dir_all(state_root);
}
