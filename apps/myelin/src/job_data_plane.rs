use std::collections::BTreeMap;
use std::sync::Arc;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId as ArenaNodeId};
use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::bootstrap::{self, BootstrapSpec, ENV_DATA_PLANE_ENDPOINT, JobHandoff};
use data_plane::host::{
    HostDataPlaneConfig, HostDataPlaneSessionActor, HostRouteRegistrar, install_session_env,
};
use data_plane::namespace::NamespaceClient;
use data_plane::path::JobContext;
use data_plane::protocol::JobCapability;
use data_plane::source::BlobSourcePublisher;
use distribution::transport_bridge::{OutboxRouteBinder, RouteBinder, RouteView};
use distribution::types::NodeId;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;

pub(crate) struct MyelinChildRouteRegistrar {
    route_view: RouteView,
    pinned_routes: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
}

impl MyelinChildRouteRegistrar {
    pub(crate) fn new(
        route_view: RouteView,
        pinned_routes: RouteView,
        route_binder: Arc<OutboxRouteBinder>,
    ) -> Self {
        Self {
            route_view,
            pinned_routes,
            route_binder,
        }
    }

    fn register_route(&self, actor: ActorAddress, node_bytes: [u8; 32]) -> Result<(), String> {
        let node = NodeId(node_bytes);
        self.pinned_routes
            .write()
            .map_err(|_| "pinned route view is poisoned".to_owned())?
            .insert(actor, node);
        self.route_view
            .write()
            .map_err(|_| "route view is poisoned".to_owned())?
            .insert(actor, node);
        self.route_binder.ensure_routable(actor);
        Ok(())
    }
}

impl HostRouteRegistrar for MyelinChildRouteRegistrar {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String> {
        self.register_route(child_session, child_node)
    }
}

impl swactor_job_runner::JobRouteRegistrar for MyelinChildRouteRegistrar {
    fn register(&self, actor: ActorAddress, node: [u8; 32]) -> Result<(), String> {
        self.register_route(actor, node)
    }
}

const ARENA_ALIGNMENT: u64 = 64;

/// Actor-owned host half of one exec-child data-plane session.
///
/// Protocol state lives in `HostDataPlaneSessionActor`, its transient binding
/// actors, and the arena allocator actor. This handle only retains the
/// inheritable arena descriptor and immutable bootstrap identities.
pub(crate) struct ActorJobDataPlane {
    handoff: JobHandoff,
    host_session: ActorAddress,
    runtime: Runtime,
}

pub(crate) struct ActorJobDataPlaneConfig {
    pub(crate) arena_bytes: u64,
    pub(crate) arena_generation: u64,
    pub(crate) session_generation: u64,
    pub(crate) capability: JobCapability,
    pub(crate) job_context: JobContext,
    pub(crate) namespace: Option<NamespaceClient>,
    pub(crate) transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    pub(crate) source_sender: Option<Arc<dyn BlobTransferSender>>,
    pub(crate) source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    pub(crate) route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
}

impl ActorJobDataPlane {
    pub(crate) fn new(runtime: &Runtime, config: ActorJobDataPlaneConfig) -> Result<Self, String> {
        let ActorJobDataPlaneConfig {
            arena_bytes,
            arena_generation,
            session_generation,
            capability,
            job_context,
            namespace,
            transfer_receiver,
            source_sender,
            source_publisher,
            route_registrar,
        } = config;
        let mut arena = ArenaManager::boot(ArenaConfig {
            node_id: ArenaNodeId(1),
            reservation_ceiling: arena_bytes,
            base_alignment: ARENA_ALIGNMENT,
        })
        .map_err(|error| format!("boot job data-plane arena: {error:?}"))?;
        let mut handoff = bootstrap::write_bootstrap(
            &mut arena,
            BootstrapSpec {
                arena_generation,
                alignment: ARENA_ALIGNMENT,
            },
        )
        .map_err(|error| format!("write job data-plane bootstrap: {error}"))?;
        let host_session = runtime
            .spawn(
                HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                    runtime: runtime.clone(),
                    arena,
                    arena_generation,
                    session_generation,
                    capability,
                    job_context,
                    namespace,
                    transfer_receiver,
                    source_sender,
                    source_publisher,
                    route_registrar,
                })
                .map_err(|error| format!("configure host data-plane session: {error}"))?,
            )
            .map_err(|error| format!("spawn host data-plane session: {error}"))?;
        install_session_env(&mut handoff, host_session, capability);
        Ok(Self {
            handoff,
            runtime: runtime.clone(),
            host_session,
        })
    }

    pub(crate) fn configure_run(&self, run_id: String) -> Result<(), String> {
        futures_lite::future::block_on(async {
            self.runtime
                .ask::<data_plane::protocol::HostSessionIn, Result<(), data_plane::protocol::DataPlaneError>>(
                    self.host_session,
                    |reply_to| data_plane::protocol::HostSessionIn::ConfigureRun {
                        run_id,
                        reply_to,
                    },
                )
                .map_err(|error| format!("configure data-plane run: {error}"))?
                .await
                .map_err(|error| format!("configure data-plane run: {error}"))
        })
    }

    pub(crate) fn close(&self) {
        let _ = self.runtime.send_to(
            self.host_session,
            data_plane::protocol::HostSessionIn::Close,
        );
    }

    pub(crate) fn handoff_env(&self, host_endpoint_json: &str) -> BTreeMap<String, String> {
        let mut env = self.handoff.env.clone();
        env.insert(
            ENV_DATA_PLANE_ENDPOINT.to_owned(),
            host_endpoint_json.to_owned(),
        );
        env
    }

    #[cfg(test)]
    pub(crate) fn arena_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.handoff.arena_fd.as_raw_fd()
    }
}
