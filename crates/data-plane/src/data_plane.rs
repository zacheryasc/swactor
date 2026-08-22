//! Child-side data-plane session, per-operation actors, and native API.

use std::collections::{HashMap, HashSet};
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::EngineHandle;

use crate::blob::{
    Blob, BlobLease, BlobMetadata, LeaseReleaser, WritableArenaView, WritableBlobLease,
};
use crate::mapped_arena::MappedArena;
use crate::path::DataPath;
use crate::protocol::{ChildSessionIn, DataPlaneError, HostSessionIn, JobCapability};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildSessionState {
    Attaching,
    Running,
    Closing,
    Closed,
}

pub struct DataPlaneBootstrap {
    pub arena: Arc<MappedArena>,
    pub data_plane: DataPlane,
}

pub struct AttachDeadline {
    pub engine: EngineHandle,
    pub timeout: Duration,
}

impl DataPlaneBootstrap {
    pub async fn attach(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
    ) -> Result<Self, DataPlaneError> {
        Self::attach_routed(arena_fd, runtime, host_session, job_capability, None).await
    }

    pub fn map_arena(
        arena_fd: OwnedFd,
    ) -> Result<(Arc<MappedArena>, crate::bootstrap::ResolvedBootstrap), DataPlaneError> {
        let (arena, resolved) = MappedArena::map(arena_fd)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        Ok((Arc::new(arena), resolved))
    }

    pub async fn attach_routed(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
    ) -> Result<Self, DataPlaneError> {
        let (arena, resolved) = Self::map_arena(arena_fd)?;
        Self::attach_mapped(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
        )
        .await
    }

    pub async fn attach_mapped(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
    ) -> Result<Self, DataPlaneError> {
        Self::attach_mapped_inner(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
            None,
        )
        .await
    }

    pub async fn attach_mapped_with_deadline(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
        deadline: AttachDeadline,
    ) -> Result<Self, DataPlaneError> {
        let sender = runtime.create_sender();
        Self::attach_mapped_inner(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
            Some((deadline.engine, sender, deadline.timeout)),
        )
        .await
    }

    async fn attach_mapped_inner(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
        deadline: Option<(EngineHandle, ExternalSender, Duration)>,
    ) -> Result<Self, DataPlaneError> {
        let attached = runtime
            .new_inbox::<Result<u64, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let attach_reply = *attached.addr();
        let child = runtime
            .spawn(ChildDataPlaneSessionActor {
                runtime: runtime.clone(),
                host_session,
                arena: arena.clone(),
                arena_generation: resolved.arena_generation,
                job_capability,
                child_node,
                session_generation: None,
                attach_reply: Some(attach_reply),
                operations: HashSet::new(),
                read_operations: HashMap::new(),
                state: ChildSessionState::Attaching,
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        if let Some((engine, sender, timeout)) = deadline {
            engine.send_after(timeout, sender, child, ChildSessionIn::AttachmentDeadline);
        }

        attached.recv().await?;
        Ok(Self {
            arena: arena.clone(),
            data_plane: DataPlane {
                runtime,
                child_session: child,
                arena,
            },
        })
    }
}

struct ReadCancellation {
    runtime: Runtime,
    child_session: ActorAddress,
    reply_to: ActorAddress,
    armed: bool,
}

impl Drop for ReadCancellation {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.send_to(
                self.child_session,
                ChildSessionIn::CancelRead {
                    reply_to: self.reply_to,
                },
            );
        }
    }
}

#[derive(Clone)]
pub struct DataPlane {
    runtime: Runtime,
    child_session: ActorAddress,
    arena: Arc<MappedArena>,
}

impl DataPlane {
    pub fn child_session(&self) -> ActorAddress {
        self.child_session
    }

    pub fn arena(&self) -> &Arc<MappedArena> {
        &self.arena
    }

    pub async fn read_blob(&self, path: &DataPath) -> Result<Blob, DataPlaneError> {
        let inbox = self
            .runtime
            .new_inbox::<Result<Blob, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let reply_to = *inbox.addr();
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::ReadBlob {
                    path: path.clone(),
                    reply_to,
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let mut cancellation = ReadCancellation {
            runtime: self.runtime.clone(),
            child_session: self.child_session,
            reply_to,
            armed: true,
        };
        let result = inbox.recv().await;
        cancellation.armed = false;
        result
    }

    pub async fn read_blob_path(&self, path: &str) -> Result<Blob, DataPlaneError> {
        let path = DataPath::parse(path)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        self.read_blob(&path).await
    }

    pub async fn write_blob(
        &self,
        path: &DataPath,
        length: u64,
    ) -> Result<BlobWriter, DataPlaneError> {
        let ask = self
            .runtime
            .ask::<ChildSessionIn, Result<WriteBlobGrant, DataPlaneError>>(
                self.child_session,
                |reply_to| ChildSessionIn::OpenWriteBlob {
                    path: path.clone(),
                    length,
                    reply_to,
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let grant = ask.await?;
        let writable =
            WritableBlobLease::from_grant(self.arena.clone(), grant.lease, grant.metadata.clone())?;
        grant.cancellation.disarm();
        Ok(BlobWriter {
            runtime: self.runtime.clone(),
            operation: grant.operation,
            writable,
            finalized: false,
        })
    }

    pub async fn write_blob_path(
        &self,
        path: &str,
        length: u64,
    ) -> Result<BlobWriter, DataPlaneError> {
        let path = DataPath::parse(path)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        self.write_blob(&path, length).await
    }

    pub async fn read_stream(&self, _path: &DataPath) -> Result<(), DataPlaneError> {
        Err(DataPlaneError::StreamsDeferred)
    }

    pub async fn write_stream(&self, _path: &DataPath) -> Result<(), DataPlaneError> {
        Err(DataPlaneError::StreamsDeferred)
    }

    pub fn close(&self) -> Result<(), DataPlaneError> {
        self.runtime
            .send_to(self.child_session, ChildSessionIn::Close)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
    }
}

pub struct BlobWriter {
    runtime: Runtime,
    operation: ActorAddress,
    writable: Arc<WritableBlobLease>,
    finalized: bool,
}

impl BlobWriter {
    pub fn length(&self) -> u64 {
        self.writable.metadata().length
    }

    pub fn map(&self) -> Result<WritableArenaView, DataPlaneError> {
        self.writable.map().map_err(Into::into)
    }

    pub async fn seal(&mut self) -> Result<(), DataPlaneError> {
        let metadata = self.writable.seal()?;
        let lease = self.writable.lease();
        let ask = self
            .runtime
            .ask::<ChildOperationIn, Result<(), DataPlaneError>>(self.operation, |reply_to| {
                ChildOperationIn::SealRequested {
                    reply_to,
                    lease,
                    metadata,
                }
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let result = ask.await;
        if result.is_ok() {
            self.finalized = true;
        }
        result
    }

    pub async fn abort(&mut self) -> Result<(), DataPlaneError> {
        self.writable.abort()?;
        self.finalized = true;
        let lease = self.writable.lease();
        let ask = self
            .runtime
            .ask::<ChildOperationIn, Result<(), DataPlaneError>>(self.operation, |reply_to| {
                ChildOperationIn::AbortRequested {
                    reply_to: Some(reply_to),
                    lease,
                }
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        ask.await
    }
}

impl Drop for BlobWriter {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        let should_abort = self.writable.is_finished() || self.writable.abort().is_ok();
        if should_abort {
            let _ = self.runtime.send_to(
                self.operation,
                ChildOperationIn::AbortRequested {
                    reply_to: None,
                    lease: self.writable.lease(),
                },
            );
        }
    }
}

pub struct ChildDataPlaneSessionActor {
    runtime: Runtime,
    host_session: ActorAddress,
    arena: Arc<MappedArena>,
    arena_generation: u64,
    job_capability: JobCapability,
    child_node: Option<[u8; 32]>,
    session_generation: Option<u64>,
    attach_reply: Option<ActorAddress>,
    operations: HashSet<ActorAddress>,
    read_operations: HashMap<ActorAddress, ActorAddress>,
    state: ChildSessionState,
}

impl ChildDataPlaneSessionActor {
    pub fn state(&self) -> ChildSessionState {
        self.state
    }

    fn fail_local_open(&self, ctx: &Ctx<'_>, reply_to: ActorAddress, error: DataPlaneError) {
        let _ = ctx.send(reply_to, Err::<Blob, _>(error));
    }
}

impl ActorInterface for ChildDataPlaneSessionActor {
    type Incoming = ChildSessionIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.host_session,
                HostSessionIn::Attach {
                    child_session: ctx.self_addr(),
                    arena_generation: self.arena_generation,
                    job_capability: self.job_capability,
                    child_node: self.child_node,
                },
            )
            .is_err()
        {
            self.state = ChildSessionState::Closed;
            if let Some(reply_to) = self.attach_reply.take() {
                let _ = ctx.send(
                    reply_to,
                    Err::<u64, _>(DataPlaneError::SessionFailed(
                        "route to host data-plane session is unavailable".to_owned(),
                    )),
                );
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildSessionIn) {
        match message {
            ChildSessionIn::Attached { session_generation }
                if self.state == ChildSessionState::Attaching =>
            {
                self.session_generation = Some(session_generation);
                self.state = ChildSessionState::Running;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(reply_to, Ok::<_, DataPlaneError>(session_generation));
                }
            }
            ChildSessionIn::AttachmentFailed { error }
                if self.state == ChildSessionState::Attaching =>
            {
                self.state = ChildSessionState::Closed;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(reply_to, Err::<u64, _>(error));
                }
            }
            ChildSessionIn::AttachmentDeadline if self.state == ChildSessionState::Attaching => {
                self.state = ChildSessionState::Closed;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(
                        reply_to,
                        Err::<u64, _>(DataPlaneError::SessionFailed(
                            "data-plane attachment deadline elapsed".to_owned(),
                        )),
                    );
                }
                ctx.stop_self();
            }
            ChildSessionIn::ReadBlob { path, reply_to } => {
                if self.state != ChildSessionState::Running {
                    self.fail_local_open(ctx, reply_to, DataPlaneError::SessionNotRunning);
                    return;
                }
                let operation = ReadBlobOperationActor {
                    runtime: self.runtime.clone(),
                    arena: self.arena.clone(),
                    host_session: self.host_session,
                    child_session: ctx.self_addr(),
                    path,
                    reply_to,
                    replied: false,
                };
                match ctx.spawn(operation) {
                    Ok(operation) => {
                        self.operations.insert(operation);
                        self.read_operations.insert(reply_to, operation);
                    }
                    Err(error) => self.fail_local_open(
                        ctx,
                        reply_to,
                        DataPlaneError::SessionFailed(error.to_string()),
                    ),
                }
            }
            ChildSessionIn::CancelRead { reply_to } => {
                if let Some(operation) = self.read_operations.remove(&reply_to) {
                    self.operations.remove(&operation);
                    let _ = ctx.stop_actor(operation);
                }
            }
            ChildSessionIn::OpenWriteBlob {
                path,
                length,
                reply_to,
            } => {
                if self.state != ChildSessionState::Running {
                    let _ = ctx.send(
                        reply_to,
                        Err::<WriteBlobGrant, _>(DataPlaneError::SessionNotRunning),
                    );
                    return;
                }
                let operation = WriteBlobOperationActor {
                    runtime: self.runtime.clone(),
                    host_session: self.host_session,
                    child_session: ctx.self_addr(),
                    path,
                    length,
                    open_reply: Some(reply_to),
                    finish_reply: None,
                    grant: None,
                    state: WriteOperationState::Opening,
                };
                match ctx.spawn(operation) {
                    Ok(operation) => {
                        self.operations.insert(operation);
                    }
                    Err(error) => {
                        let _ = ctx.send(
                            reply_to,
                            Err::<WriteBlobGrant, _>(DataPlaneError::SessionFailed(
                                error.to_string(),
                            )),
                        );
                    }
                }
            }
            ChildSessionIn::BlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(
                        operation,
                        ChildOperationIn::ReadOpened {
                            host_binding,
                            lease,
                            metadata,
                        },
                    );
                }
            }
            ChildSessionIn::WriteBlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(
                        operation,
                        ChildOperationIn::WriteOpened {
                            host_binding,
                            lease,
                            metadata,
                        },
                    );
                }
            }
            ChildSessionIn::OperationFailed { operation, error } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::Failed(error));
                }
            }
            ChildSessionIn::WritePublished { operation } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::WritePublished);
                }
            }
            ChildSessionIn::WriteAborted { operation } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::WriteAborted);
                }
            }
            ChildSessionIn::OperationDone { operation } => {
                self.operations.remove(&operation);
                self.read_operations
                    .retain(|_, read_operation| *read_operation != operation);
            }
            ChildSessionIn::Close => {
                if matches!(
                    self.state,
                    ChildSessionState::Closing | ChildSessionState::Closed
                ) {
                    return;
                }
                self.state = ChildSessionState::Closing;
                for operation in self.operations.iter().copied() {
                    let _ = ctx.stop_actor(operation);
                }
                self.read_operations.clear();
                let _ = ctx.send(self.host_session, HostSessionIn::Close);
                self.state = ChildSessionState::Closed;
            }
            _ => {}
        }
    }
}

#[derive(Clone)]
enum ChildOperationIn {
    ReadOpened {
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    WriteOpened {
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    Failed(DataPlaneError),
    SealRequested {
        reply_to: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    AbortRequested {
        reply_to: Option<ActorAddress>,
        lease: BlobLease,
    },
    WritePublished,
    WriteAborted,
}

struct RuntimeLeaseReleaser {
    runtime: Runtime,
    host_session: ActorAddress,
    host_binding: ActorAddress,
}

impl LeaseReleaser for RuntimeLeaseReleaser {
    fn release(&self, lease: BlobLease) {
        let _ = self.runtime.send_to(
            self.host_session,
            HostSessionIn::ReleaseBlob {
                binding: self.host_binding,
                lease_id: lease.lease_id,
                generation: lease.generation,
            },
        );
    }
}

struct ReadBlobOperationActor {
    runtime: Runtime,
    arena: Arc<MappedArena>,
    host_session: ActorAddress,
    child_session: ActorAddress,
    path: DataPath,
    reply_to: ActorAddress,
    replied: bool,
}

impl ReadBlobOperationActor {
    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<Blob, DataPlaneError>) {
        self.replied = true;
        let _ = ctx.send(self.reply_to, result);
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }
}

impl ActorInterface for ReadBlobOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.host_session,
                HostSessionIn::OpenReadBlob {
                    path: self.path.clone(),
                    child_session: self.child_session,
                    operation: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.finish(
                ctx,
                Err(DataPlaneError::SessionFailed(
                    "send read-blob open to host session".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildOperationIn) {
        match message {
            ChildOperationIn::ReadOpened {
                host_binding,
                lease,
                metadata,
            } => {
                let releaser: Arc<dyn LeaseReleaser> = Arc::new(RuntimeLeaseReleaser {
                    runtime: self.runtime.clone(),
                    host_session: self.host_session,
                    host_binding,
                });
                let result = Blob::from_sealed_lease(self.arena.clone(), lease, metadata, releaser)
                    .map_err(Into::into);
                self.finish(ctx, result);
            }
            ChildOperationIn::Failed(error) => self.finish(ctx, Err(error)),
            _ => {}
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.replied {
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelReadBlob {
                    operation: ctx.self_addr(),
                },
            );
            let _ = ctx.send(
                self.reply_to,
                Err::<Blob, _>(DataPlaneError::OperationCancelled),
            );
        }
    }
}

#[derive(Clone)]
struct WriteBlobGrant {
    operation: ActorAddress,
    lease: BlobLease,
    metadata: BlobMetadata,
    cancellation: Arc<WriteGrantCancellation>,
}

struct WriteGrantCancellation {
    runtime: Runtime,
    operation: ActorAddress,
    lease: BlobLease,
    armed: AtomicBool,
}

impl WriteGrantCancellation {
    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }
}

impl Drop for WriteGrantCancellation {
    fn drop(&mut self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let _ = self.runtime.send_to(
                self.operation,
                ChildOperationIn::AbortRequested {
                    reply_to: None,
                    lease: self.lease,
                },
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteOperationState {
    Opening,
    Filling,
    Sealing,
    Aborting,
    Finished,
}

struct WriteBlobOperationActor {
    runtime: Runtime,
    host_session: ActorAddress,
    child_session: ActorAddress,
    path: DataPath,
    length: u64,
    open_reply: Option<ActorAddress>,
    finish_reply: Option<ActorAddress>,
    grant: Option<(ActorAddress, BlobLease, BlobMetadata)>,
    state: WriteOperationState,
}

impl WriteBlobOperationActor {
    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        self.state = WriteOperationState::Finished;
        if let Some(reply_to) = self.finish_reply.take() {
            let _ = ctx.send(reply_to, result);
        }
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }

    fn fail(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if let Some(reply_to) = self.open_reply.take() {
            let _ = ctx.send(reply_to, Err::<WriteBlobGrant, _>(error));
            self.state = WriteOperationState::Finished;
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::OperationDone {
                    operation: ctx.self_addr(),
                },
            );
            ctx.stop_self();
        } else {
            self.finish(ctx, Err(error));
        }
    }
}

impl ActorInterface for WriteBlobOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.host_session,
                HostSessionIn::OpenWriteBlob {
                    path: self.path.clone(),
                    length: self.length,
                    child_session: self.child_session,
                    operation: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.fail(
                ctx,
                DataPlaneError::SessionFailed("send write-blob open to host session".to_owned()),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildOperationIn) {
        match message {
            ChildOperationIn::WriteOpened {
                host_binding,
                lease,
                metadata,
            } if self.state == WriteOperationState::Opening => {
                self.state = WriteOperationState::Filling;
                self.grant = Some((host_binding, lease, metadata.clone()));
                if let Some(reply_to) = self.open_reply.take()
                    && ctx
                        .send(
                            reply_to,
                            Ok::<_, DataPlaneError>(WriteBlobGrant {
                                operation: ctx.self_addr(),
                                lease,
                                metadata,
                                cancellation: Arc::new(WriteGrantCancellation {
                                    runtime: self.runtime.clone(),
                                    operation: ctx.self_addr(),
                                    lease,
                                    armed: AtomicBool::new(true),
                                }),
                            }),
                        )
                        .is_err()
                {
                    self.state = WriteOperationState::Aborting;
                    if ctx
                        .send(
                            self.host_session,
                            HostSessionIn::AbortWriteBlob {
                                binding: host_binding,
                                operation: ctx.self_addr(),
                                lease_id: lease.lease_id,
                                generation: lease.generation,
                            },
                        )
                        .is_err()
                    {
                        self.fail(
                            ctx,
                            DataPlaneError::SessionFailed(
                                "abort cancelled write-blob open".to_owned(),
                            ),
                        );
                    }
                }
            }
            ChildOperationIn::SealRequested {
                reply_to,
                lease,
                metadata,
            } if self.state == WriteOperationState::Filling => {
                let Some((host_binding, expected_lease, expected_metadata)) = self.grant.clone()
                else {
                    self.fail(ctx, DataPlaneError::OperationCancelled);
                    return;
                };
                if lease != expected_lease || metadata != expected_metadata {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.state = WriteOperationState::Sealing;
                self.finish_reply = Some(reply_to);
                if ctx
                    .send(
                        self.host_session,
                        HostSessionIn::SealWriteBlob {
                            binding: host_binding,
                            operation: ctx.self_addr(),
                            lease,
                            metadata,
                        },
                    )
                    .is_err()
                {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(
                            "send write-blob seal to host session".to_owned(),
                        ),
                    );
                }
            }
            ChildOperationIn::AbortRequested { reply_to, lease }
                if matches!(
                    self.state,
                    WriteOperationState::Filling | WriteOperationState::Sealing
                ) =>
            {
                let Some((host_binding, expected_lease, _)) = self.grant.clone() else {
                    self.fail(ctx, DataPlaneError::OperationCancelled);
                    return;
                };
                if lease != expected_lease {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.state = WriteOperationState::Aborting;
                self.finish_reply = reply_to;
                if ctx
                    .send(
                        self.host_session,
                        HostSessionIn::AbortWriteBlob {
                            binding: host_binding,
                            operation: ctx.self_addr(),
                            lease_id: lease.lease_id,
                            generation: lease.generation,
                        },
                    )
                    .is_err()
                {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(
                            "send write-blob abort to host session".to_owned(),
                        ),
                    );
                }
            }
            ChildOperationIn::WritePublished if self.state == WriteOperationState::Sealing => {
                self.finish(ctx, Ok(()));
            }
            ChildOperationIn::WriteAborted if self.state == WriteOperationState::Aborting => {
                self.finish(ctx, Ok(()));
            }
            ChildOperationIn::Failed(error) => self.fail(ctx, error),
            _ => {}
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if let Some(reply_to) = self.open_reply.take() {
            let _ = ctx.send(
                reply_to,
                Err::<WriteBlobGrant, _>(DataPlaneError::OperationCancelled),
            );
        }
        if let Some(reply_to) = self.finish_reply.take() {
            let _ = ctx.send(reply_to, Err::<(), _>(DataPlaneError::OperationCancelled));
        }
    }
}

pub fn parse_actor_address(encoded: &str) -> Result<ActorAddress, DataPlaneError> {
    if encoded.len() != 64 {
        return Err(DataPlaneError::SessionFailed(
            "data-plane actor address must contain 64 hexadecimal digits".to_owned(),
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| {
            DataPlaneError::SessionFailed("data-plane actor address is not hexadecimal".to_owned())
        })?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| {
            DataPlaneError::SessionFailed("data-plane actor address is not hexadecimal".to_owned())
        })?;
    }
    Ok(ActorAddress(bytes))
}
