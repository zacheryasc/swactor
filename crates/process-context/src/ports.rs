use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId as ArenaNodeId};
use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::bootstrap::channel::{
    BootstrapCancellation, BootstrapHost, SessionBootstrap, bootstrap_channel,
};
use data_plane::bootstrap::{BootstrapSpec, prepare_arena};
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor, HostRouteRegistrar};
use data_plane::namespace::NamespaceClient;
use data_plane::path::SessionAccess;
use data_plane::protocol::{HostSessionIn, SessionCapability};
use data_plane::source::BlobSourcePublisher;
use data_plane::stream_transport::StreamTransport;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_engine::EngineHandle;

use crate::model::ExecutionIdentity;

pub trait RoutingMaterialProvider: Send + Sync + 'static {
    fn routing_material(&self, identity: ExecutionIdentity) -> Result<Vec<u8>, String>;
}

impl<F> RoutingMaterialProvider for F
where
    F: Fn(ExecutionIdentity) -> Result<Vec<u8>, String> + Send + Sync + 'static,
{
    fn routing_material(&self, identity: ExecutionIdentity) -> Result<Vec<u8>, String> {
        self(identity)
    }
}

pub trait ContextProvisioner: Send + Sync + 'static {
    fn provision(
        &self,
        identity: ExecutionIdentity,
        access: SessionAccess,
    ) -> Result<ProvisionedContext, String>;
}

pub struct ProvisionedContext {
    pub host_session: ActorAddress,
    pub arena_fd: OwnedFd,
    pub child_bootstrap: OwnedFd,
    pub bootstrap_host: BootstrapHost,
    pub bootstrap_cancellation: BootstrapCancellation,
    pub material: SessionBootstrap,
}

pub struct DataPlaneProvisionerConfig {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub arena_bytes: u64,
    pub arena_alignment: u64,
    pub namespace: Option<NamespaceClient>,
    pub transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    pub source_sender: Option<Arc<dyn BlobTransferSender>>,
    pub source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    pub route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    pub stream_transport: Option<Arc<dyn StreamTransport>>,
    pub routing: Arc<dyn RoutingMaterialProvider>,
}

pub struct DataPlaneProvisioner {
    config: DataPlaneProvisionerConfig,
    next_generation: AtomicU64,
}

impl DataPlaneProvisioner {
    pub fn new(config: DataPlaneProvisionerConfig) -> Result<Self, String> {
        if config.arena_bytes == 0 || config.arena_alignment == 0 {
            return Err("context arena bytes and alignment must be nonzero".to_owned());
        }
        Ok(Self {
            config,
            next_generation: AtomicU64::new(1),
        })
    }
}

impl ContextProvisioner for DataPlaneProvisioner {
    fn provision(
        &self,
        identity: ExecutionIdentity,
        access: SessionAccess,
    ) -> Result<ProvisionedContext, String> {
        access
            .validate()
            .map_err(|error| format!("validate session access: {error}"))?;
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        if generation == 0 {
            return Err("context generation space exhausted".to_owned());
        }
        let routing = self.config.routing.routing_material(identity)?;
        let capability = SessionCapability::new(ActorAddress::new_random().0);
        let mut arena = ArenaManager::boot(ArenaConfig {
            node_id: ArenaNodeId(generation),
            reservation_ceiling: self.config.arena_bytes,
            base_alignment: self.config.arena_alignment,
        })
        .map_err(|error| format!("boot contextual arena: {error:?}"))?;
        let prepared = prepare_arena(
            &mut arena,
            BootstrapSpec {
                arena_generation: generation,
                alignment: self.config.arena_alignment,
            },
        )
        .map_err(|error| format!("prepare contextual arena: {error}"))?;
        let host_session = self
            .config
            .runtime
            .spawn(
                HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                    runtime: self.config.runtime.clone(),
                    engine: self.config.engine.clone(),
                    arena,
                    arena_generation: generation,
                    session_generation: generation,
                    capability,
                    session_access: access,
                    namespace: self.config.namespace.clone(),
                    transfer_receiver: self.config.transfer_receiver.clone(),
                    source_sender: self.config.source_sender.clone(),
                    source_publisher: self.config.source_publisher.clone(),
                    route_registrar: self.config.route_registrar.clone(),
                    stream_transport: self.config.stream_transport.clone(),
                })
                .map_err(|error| format!("configure contextual host session: {error}"))?,
            )
            .map_err(|error| format!("spawn contextual host session: {error}"))?;
        let (bootstrap_host, child_bootstrap) = match bootstrap_channel() {
            Ok(channel) => channel,
            Err(error) => {
                let _ = self
                    .config
                    .runtime
                    .send_to(host_session, HostSessionIn::Close { reply_to: None });
                return Err(format!("create contextual bootstrap channel: {error}"));
            }
        };
        let bootstrap_cancellation = match bootstrap_host.cancellation_handle() {
            Ok(cancellation) => cancellation,
            Err(error) => {
                let _ = self
                    .config
                    .runtime
                    .send_to(host_session, HostSessionIn::Close { reply_to: None });
                return Err(format!("create bootstrap cancellation handle: {error}"));
            }
        };
        Ok(ProvisionedContext {
            host_session,
            arena_fd: prepared.arena_fd,
            child_bootstrap,
            bootstrap_host,
            bootstrap_cancellation,
            material: SessionBootstrap {
                host_session,
                session_capability: capability,
                routing,
            },
        })
    }
}
