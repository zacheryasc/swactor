#![cfg(target_os = "linux")]

use std::collections::BTreeSet;

use data_plane::bootstrap::{
    ENV_ARENA_FD, ENV_DATA_PLANE_ACTOR, ENV_DATA_PLANE_ENDPOINT, ENV_JOB_CAPABILITY,
};
use data_plane::path::{DataPath, JobContext};
use data_plane::protocol::JobCapability;
use swactor::config::RuntimeConfig;
use swactor::runtime::RuntimeParts;

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
