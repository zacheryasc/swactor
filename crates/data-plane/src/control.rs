//! Reusable namespace service lifecycle and public file-registration control.

use std::path::Path;
use std::sync::Arc;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;

use crate::blob::FileRegistration;
use crate::blob_transfer::BlobTransferSender;
use crate::namespace::{DataDirectoryActor, DirectoryClient, NamespaceError, OperationId};
use crate::namespace_store::SourceRecovery;
use crate::path::DataPath;
use crate::source::{BlobSourceIn, BlobSourcePublisher, FileBlobSourceActor};

pub struct DataNamespaceService {
    directory: ActorAddress,
    control: DataPlaneControl,
}

impl DataNamespaceService {
    pub fn recover(
        runtime: Runtime,
        store_path: impl AsRef<Path>,
        source_sender: Arc<dyn BlobTransferSender>,
        source_publisher: Arc<dyn BlobSourcePublisher>,
    ) -> Result<Self, NamespaceError> {
        let recovery_runtime = runtime.clone();
        let recovery_sender = Arc::clone(&source_sender);
        let recovery_publisher = Arc::clone(&source_publisher);
        let directory = DataDirectoryActor::recover(
            store_path,
            move |recovery, expected_length| match recovery {
                SourceRecovery::File { path } => {
                    let source = FileBlobSourceActor::recover(
                        recovery_runtime.clone(),
                        Arc::clone(&recovery_sender),
                        path,
                        expected_length,
                    )?;
                    let source = recovery_runtime.spawn(source).map_err(|error| {
                        NamespaceError::SourceRecovery(format!(
                            "spawn recovered file source {}: {error}",
                            path.display()
                        ))
                    })?;
                    if let Err(error) = recovery_publisher.publish_source(source) {
                        let _ = recovery_runtime.send_to(source, BlobSourceIn::Retire);
                        return Err(NamespaceError::SourceRecovery(error));
                    }
                    Ok(source)
                }
                SourceRecovery::Actor { actor } => Ok(*actor),
            },
        )?;
        let directory = runtime.spawn(directory).map_err(|error| {
            NamespaceError::SourceRecovery(format!("spawn data directory: {error}"))
        })?;
        let control = DataPlaneControl {
            runtime: runtime.clone(),
            directory: DirectoryClient::new(runtime, directory),
            source_sender,
            source_publisher,
        };
        Ok(Self { directory, control })
    }

    pub fn directory(&self) -> ActorAddress {
        self.directory
    }

    pub fn control(&self) -> DataPlaneControl {
        self.control.clone()
    }
}

#[derive(Clone)]
pub struct DataPlaneControl {
    runtime: Runtime,
    directory: DirectoryClient,
    source_sender: Arc<dyn BlobTransferSender>,
    source_publisher: Arc<dyn BlobSourcePublisher>,
}

impl DataPlaneControl {
    pub async fn ensure(
        &self,
        path: DataPath,
        registration: FileRegistration,
    ) -> Result<(), NamespaceError> {
        match self.directory.resolve(path.clone()).await {
            Ok(_) => Ok(()),
            Err(NamespaceError::PathNotFound(_)) | Err(NamespaceError::SourceRecovery(_)) => {
                self.register(path, registration).await
            }
            Err(error) => Err(error),
        }
    }

    pub async fn register(
        &self,
        path: DataPath,
        registration: FileRegistration,
    ) -> Result<(), NamespaceError> {
        let source = FileBlobSourceActor::open(
            self.runtime.clone(),
            Arc::clone(&self.source_sender),
            registration.path(),
        )?;
        let length = source.length();
        let recovery = source.recovery();
        let source = self
            .runtime
            .spawn(source)
            .map_err(|error| NamespaceError::SourceRecovery(error.to_string()))?;
        let mut cleanup = PendingSourceRegistration {
            runtime: self.runtime.clone(),
            source,
            armed: true,
        };
        self.source_publisher
            .publish_source(source)
            .map_err(NamespaceError::SourceRecovery)?;
        self.directory
            .register(path, source, length, recovery, random_operation_id())
            .await?;
        cleanup.armed = false;
        Ok(())
    }

    pub async fn unregister(&self, path: DataPath) -> Result<(), NamespaceError> {
        self.directory
            .unregister(path, random_operation_id())
            .await?;
        Ok(())
    }
}

struct PendingSourceRegistration {
    runtime: Runtime,
    source: ActorAddress,
    armed: bool,
}

impl Drop for PendingSourceRegistration {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.send_to(self.source, BlobSourceIn::Retire);
        }
    }
}

fn random_operation_id() -> OperationId {
    let actor = ActorAddress::new_random();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&actor.0[..16]);
    OperationId::from_u128(u128::from_be_bytes(bytes))
}
