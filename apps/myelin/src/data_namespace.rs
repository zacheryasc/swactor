use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use data_plane::blob_transfer::BlobTransferSender;
use data_plane::control::{DataNamespaceService, DataPlaneControl};
use data_plane::namespace::{NamespaceClient, NamespaceClientActor, NamespaceDiscovery};
use data_plane::source::BlobSourcePublisher;
use distribution::directory_actor::DirectoryIn;
use distribution::registry_actor::{RegistryIn, RegistryView};
use iroh_driver::{ActorRegistrar, IrohBlobTransferSender, IrohDriver};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

pub(crate) const DATA_DIRECTORY_SERVICE: &str = "swactor.data-directory";
const NAMESPACE_RETRY: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct LocalActorPublisher {
    runtime: Runtime,
    distribution_directory: ActorAddress,
    registrar: ActorRegistrar,
}

impl LocalActorPublisher {
    fn publish(&self, actor: ActorAddress) -> Result<(), String> {
        let claim = self.registrar.register_actor(actor, 1);
        self.runtime
            .send_to(self.distribution_directory, DirectoryIn::Register(claim))
            .map_err(|error| error.to_string())
    }
}

impl BlobSourcePublisher for LocalActorPublisher {
    fn publish_source(&self, source: ActorAddress) -> Result<(), String> {
        self.publish(source)
    }
}

struct RegistryNamespaceDiscovery {
    view: RegistryView,
}

impl NamespaceDiscovery for RegistryNamespaceDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        self.view
            .read()
            .expect("registry view poisoned")
            .entries
            .iter()
            .find(|entry| entry.name == DATA_DIRECTORY_SERVICE && !entry.tombstone)
            .map(|entry| entry.actor_addr)
    }
}

#[derive(Clone)]
enum NamespaceServicePublisherIn {
    Tick,
}

struct NamespaceServicePublisher {
    registry: ActorAddress,
    view: RegistryView,
    directory: ActorAddress,
}

impl ActorInterface for NamespaceServicePublisher {
    type Incoming = NamespaceServicePublisherIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: NamespaceServicePublisherIn) {
        match message {
            NamespaceServicePublisherIn::Tick => {
                let published = self
                    .view
                    .read()
                    .expect("registry view poisoned")
                    .entries
                    .iter()
                    .any(|entry| {
                        entry.name == DATA_DIRECTORY_SERVICE
                            && !entry.tombstone
                            && entry.actor_addr == self.directory
                    });
                if !published {
                    let _ = ctx.send(
                        self.registry,
                        RegistryIn::RegisterName {
                            name: DATA_DIRECTORY_SERVICE.to_owned(),
                            actor_addr: self.directory,
                        },
                    );
                }
            }
        }
    }
}

pub(crate) struct DataNamespaceAuthority {
    service: DataNamespaceService,
}

impl DataNamespaceAuthority {
    pub(crate) fn start(
        stack: &DistributionRuntimeStack,
        driver: &IrohDriver,
        state_path: impl AsRef<Path>,
    ) -> Result<Self, String> {
        let runtime = stack.runtime.clone();
        let source_sender: Arc<dyn BlobTransferSender> = Arc::new(IrohBlobTransferSender::new(
            driver.edge_connector(),
            &stack.engine,
        ));
        let publisher = Arc::new(LocalActorPublisher {
            runtime: runtime.clone(),
            distribution_directory: stack.actors.directory,
            registrar: driver.actor_registrar(),
        });
        let source_publisher: Arc<dyn BlobSourcePublisher> = publisher.clone();
        let service = DataNamespaceService::recover(
            runtime.clone(),
            state_path,
            source_sender,
            source_publisher,
        )
        .map_err(|error| format!("recover data namespace: {error}"))?;
        let directory = service.directory();
        publisher.publish(directory)?;
        runtime
            .send_to(
                stack.actors.registry,
                RegistryIn::RegisterName {
                    name: DATA_DIRECTORY_SERVICE.to_owned(),
                    actor_addr: directory,
                },
            )
            .map_err(|error| format!("publish data directory service: {error}"))?;
        let service_publisher = runtime
            .spawn(NamespaceServicePublisher {
                registry: stack.actors.registry,
                view: Arc::clone(&stack.registry_view),
                directory,
            })
            .map_err(|error| format!("spawn namespace service publisher: {error}"))?;
        stack.engine.send_every(
            Duration::from_secs(1),
            runtime.create_sender(),
            service_publisher,
            NamespaceServicePublisherIn::Tick,
        );
        Ok(Self { service })
    }

    #[cfg(test)]
    pub(crate) fn directory(&self) -> ActorAddress {
        self.service.directory()
    }

    pub(crate) fn control(&self) -> DataPlaneControl {
        self.service.control()
    }
}

pub(crate) struct InstalledNamespaceClient {
    pub(crate) client: NamespaceClient,
    pub(crate) source_publisher: Arc<dyn BlobSourcePublisher>,
}

pub(crate) fn install_namespace_client(
    stack: &DistributionRuntimeStack,
    driver: &IrohDriver,
) -> Result<InstalledNamespaceClient, String> {
    let discovery: Arc<dyn NamespaceDiscovery> = Arc::new(RegistryNamespaceDiscovery {
        view: Arc::clone(&stack.registry_view),
    });
    let proxy = stack
        .runtime
        .spawn(NamespaceClientActor::new(
            stack.engine.clone(),
            stack.runtime.create_sender(),
            discovery,
            NAMESPACE_RETRY,
        ))
        .map_err(|error| format!("spawn namespace client: {error}"))?;
    stack.register_local_actor(driver.register_actor(proxy, 1));
    let source_publisher: Arc<dyn BlobSourcePublisher> = Arc::new(LocalActorPublisher {
        runtime: stack.runtime.clone(),
        distribution_directory: stack.actors.directory,
        registrar: driver.actor_registrar(),
    });
    Ok(InstalledNamespaceClient {
        client: NamespaceClient::new(stack.runtime.clone(), proxy),
        source_publisher,
    })
}
