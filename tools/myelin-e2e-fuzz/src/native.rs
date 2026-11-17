//! Native contextual workloads for the binding-independent behavioral protocol.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use data_plane::data_plane::{
    DataPlane, Descriptor, MapRequest, MapTarget, Protection, RegionSlice, Sharing, StreamReader,
    StreamWriter,
};
use data_plane::namespace::{EntryKind, NamespaceNode};
use data_plane::path::DataPath;
use data_plane::protocol::{AccessMode, BlobAllocation, DataPlaneError, Errno, OpenOptions};
use futures_util::future::try_join;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use swactor::runtime::Runtime;
use swactor_engine::EngineHandle;

use crate::ir::{
    Action, ActionObservation, ActionOp, BARRIER_LAP_COMPLETED, BARRIER_TOKEN_FORWARDED,
    BARRIER_TOKEN_RECEIVED, BarrierObservation, BehaviorCase, DescriptorFinish,
    DescriptorObservation, DescriptorReadMethod, DescriptorTerminalResult, DescriptorWriteMethod,
    EVIDENCE_HINT_BARRIER, EVIDENCE_HINT_EDGE_INDEX, EVIDENCE_HINT_LAP, EVIDENCE_HINT_TOKEN,
    ExpectedOutcome, ProcessProgram, TopologyFamily, TransferObservation,
};
use crate::oracle::digest;

mod actions;
mod bootstrap;
mod deadline;
mod routes;
mod streams;
pub use bootstrap::run_contextual_program;

/// The complete, replayable input to one contextual child process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeProgram {
    pub case: BehaviorCase,
    pub process_id: String,
    pub budget_ms: u64,
}

/// Serialize the shared input contract without cloning the case's payloads.
pub fn serialize_program(
    case: &BehaviorCase,
    process_id: &str,
    budget_ms: u64,
) -> Result<String, String> {
    #[derive(Serialize)]
    struct BorrowedProgram<'a> {
        case: &'a BehaviorCase,
        process_id: &'a str,
        budget_ms: u64,
    }
    serde_json::to_string(&BorrowedProgram {
        case,
        process_id,
        budget_ms,
    })
    .map_err(|error| format!("serialize native program: {error}"))
}

/// Normalized native failure. Diagnostic text is not an oracle classification.
#[derive(Clone, Debug)]
pub struct NativeError {
    pub errno: Option<i32>,
    pub message: String,
}

impl NativeError {
    fn protocol(message: impl Into<String>) -> Self {
        Self {
            errno: Some(libc::EIO),
            message: message.into(),
        }
    }

    fn deadline_exhausted() -> Self {
        Self {
            errno: Some(libc::ETIMEDOUT),
            message: "native action budget expired".into(),
        }
    }

    fn error_type(&self) -> &str {
        if self.errno.is_some() {
            "errno"
        } else {
            "protocol_error"
        }
    }
}

impl fmt::Display for NativeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for NativeError {}

impl From<DataPlaneError> for NativeError {
    fn from(error: DataPlaneError) -> Self {
        let errno = match error.errno() {
            Errno::Eacces => libc::EACCES,
            Errno::Eagain => libc::EAGAIN,
            Errno::Ebadf => libc::EBADF,
            Errno::Ebusy => libc::EBUSY,
            Errno::Ecanceled => libc::ECANCELED,
            Errno::Econnreset => libc::ECONNRESET,
            Errno::Eexist => libc::EEXIST,
            Errno::Einval => libc::EINVAL,
            Errno::Eio => libc::EIO,
            Errno::Enodev => libc::ENODEV,
            Errno::Enoent => libc::ENOENT,
            Errno::Enomem => libc::ENOMEM,
            Errno::Enospc => libc::ENOSPC,
            Errno::Enotsup => libc::ENOTSUP,
            Errno::Enxio => libc::ENXIO,
            Errno::Epipe => libc::EPIPE,
            Errno::Estale => libc::ESTALE,
        };
        Self {
            errno: Some(errno),
            message: error.to_string(),
        }
    }
}

impl From<io::Error> for NativeError {
    fn from(error: io::Error) -> Self {
        Self {
            errno: error.raw_os_error(),
            message: error.to_string(),
        }
    }
}

type Result<T, E = NativeError> = std::result::Result<T, E>;
type Emitter<'a> = dyn Fn(&ActionObservation) -> io::Result<()> + Send + Sync + 'a;

#[derive(Clone, Copy)]
struct Budget<'a> {
    runtime: &'a Runtime,
    engine: &'a EngineHandle,
    deadline: Instant,
}

impl Budget<'_> {
    fn remaining(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(self.engine.now().to_instant())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(NativeError::deadline_exhausted)
    }

    async fn bound<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        deadline::bounded(*self, operation).await
    }

    async fn pace(&self, delay: &mut Duration, cap: Duration) -> Result<()> {
        deadline::pace(*self, *delay).await?;
        *delay = (*delay * 2).min(cap);
        Ok(())
    }
}

fn path(value: &str) -> Result<DataPath> {
    DataPath::parse(value).map_err(|error| DataPlaneError::InvalidPath(error.to_string()).into())
}

fn observation(process: &str, step: usize, action: &Action) -> ActionObservation {
    ActionObservation {
        process: process.to_owned(),
        step,
        action: action.operation.class().as_str().to_owned(),
        path: action.operation.path().to_owned(),
        outcome: "ok".into(),
        length: None,
        digest: None,
        kind: None,
        revision: None,
        active: None,
        errno: None,
        error_type: None,
        error: None,
        descriptor: None,
        transfer: None,
        barrier: None,
        incarnation: None,
        token: action.evidence_hints.get(EVIDENCE_HINT_TOKEN).cloned(),
        lap: action
            .evidence_hints
            .get(EVIDENCE_HINT_LAP)
            .and_then(|value| value.parse().ok()),
    }
}

fn barrier(emit: &Emitter<'_>, base: &ActionObservation, value: BarrierObservation) -> Result<()> {
    let mut record = base.clone();
    record.outcome = "barrier".into();
    record.barrier = Some(value);
    record.length = None;
    record.digest = None;
    record.kind = None;
    record.revision = None;
    record.active = None;
    record.errno = None;
    record.error_type = None;
    record.error = None;
    record.descriptor = None;
    record.transfer = None;
    record.incarnation = None;
    emit(&record).map_err(Into::into)
}

fn hint_barrier(emit: &Emitter<'_>, record: &ActionObservation, action: &Action) -> Result<()> {
    let token = record.token.clone().unwrap_or_default();
    let lap = record.lap.unwrap_or(0);
    let edge_index = action
        .evidence_hints
        .get(EVIDENCE_HINT_EDGE_INDEX)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let value = match action
        .evidence_hints
        .get(EVIDENCE_HINT_BARRIER)
        .map(String::as_str)
    {
        Some(BARRIER_TOKEN_RECEIVED) => BarrierObservation::TokenReceived {
            token,
            lap,
            edge_index,
        },
        Some(BARRIER_TOKEN_FORWARDED) => BarrierObservation::TokenForwarded {
            token,
            lap,
            edge_index,
        },
        Some(BARRIER_LAP_COMPLETED) => BarrierObservation::LapCompleted { token, lap },
        _ => return Ok(()),
    };
    barrier(emit, record, value)
}

#[derive(Default)]
struct Transfer {
    length: usize,
    hasher: Sha256,
    complete: bool,
}

#[derive(Default)]
struct Evidence {
    transfer: Option<Transfer>,
    incarnation: Option<u64>,
    descriptor: Option<DescriptorObservation>,
}

impl Evidence {
    fn transferred(&mut self, bytes: &[u8], complete: bool) {
        let transfer = self.transfer.get_or_insert_with(Transfer::default);
        transfer.length += bytes.len();
        transfer.hasher.update(bytes);
        transfer.complete = complete;
    }

    fn apply(self, record: &mut ActionObservation) {
        record.incarnation = self.incarnation;
        record.descriptor = self.descriptor;
        if let Some(transfer) = self.transfer {
            let digest = format!("{:x}", transfer.hasher.finalize());
            record.length = Some(transfer.length);
            record.digest = Some(digest.clone());
            record.transfer = Some(TransferObservation {
                length: transfer.length,
                digest,
                complete: transfer.complete,
            });
        }
    }
}

async fn wait_entry(
    data: &DataPlane,
    value: &DataPath,
    budget: Budget<'_>,
    quiescent: bool,
) -> Result<NamespaceNode> {
    let mut delay = Duration::from_millis(1);
    loop {
        match data.lookup(value).await {
            Ok(entry) if !quiescent => return Ok(entry),
            Ok(entry) => {
                if entry.kind != EntryKind::Stream {
                    return Err(NativeError::protocol(
                        "quiescence requires a stream namespace node",
                    ));
                }
                if !entry.active {
                    return Ok(entry);
                }
            }
            Err(error) if !quiescent && error.errno() == Errno::Enoent => {}
            Err(error) => return Err(error.into()),
        }
        budget
            .pace(
                &mut delay,
                Duration::from_millis(if quiescent { 100 } else { 10 }),
            )
            .await?;
    }
}

async fn mutation_origin(data: &DataPlane, value: &DataPath) -> Option<u64> {
    data.lookup(value).await.ok().map(|entry| entry.revision)
}

async fn publish(
    data: &DataPlane,
    value: &DataPath,
    bytes: &[u8],
    evidence: &mut Evidence,
) -> Result<()> {
    let mut writer = data.write_blob(value, bytes.len() as u64).await?;
    {
        let mut view = writer.map()?;
        view.as_mut().copy_from_slice(bytes);
        evidence.transferred(bytes, true);
    }
    writer.seal().await?;
    Ok(())
}

fn open_options(flags: i32, length: Option<u64>) -> Result<OpenOptions> {
    let known = libc::O_ACCMODE | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC | libc::O_NONBLOCK;
    if flags & !known != 0 {
        return Err(DataPlaneError::Unsupported("unknown descriptor open flags".into()).into());
    }
    let access = match flags & libc::O_ACCMODE {
        libc::O_RDONLY => AccessMode::ReadOnly,
        libc::O_WRONLY => AccessMode::WriteOnly,
        libc::O_RDWR => AccessMode::ReadWrite,
        _ => {
            return Err(
                DataPlaneError::InvalidArgument("invalid descriptor access mode".into()).into(),
            );
        }
    };
    Ok(OpenOptions {
        access,
        create: flags & libc::O_CREAT != 0,
        exclusive: flags & libc::O_EXCL != 0,
        truncate: flags & libc::O_TRUNC != 0,
        nonblocking: flags & libc::O_NONBLOCK != 0,
        allocation: length.map(|length| BlobAllocation {
            length,
            digest: None,
        }),
    })
}

fn map_request(offset: u64, length: usize, writable: bool) -> MapRequest {
    MapRequest {
        protection: if writable {
            Protection::ReadWrite
        } else {
            Protection::Read
        },
        sharing: Sharing::Shared,
        target: MapTarget::Host,
        offset,
        length: length as u64,
    }
}

async fn finish_descriptor(
    mut descriptor: Descriptor,
    finish: DescriptorFinish,
    evidence: &mut Evidence,
) -> Result<()> {
    if finish == DescriptorFinish::Drop {
        drop(descriptor);
        if let Some(DescriptorObservation::Write { dropped, .. }) = evidence.descriptor.as_mut() {
            *dropped = true;
        }
        return Ok(());
    }
    let count = if matches!(
        finish,
        DescriptorFinish::CloseTwice | DescriptorFinish::AbortTwice
    ) {
        2
    } else {
        1
    };
    for _ in 0..count {
        let result = if matches!(
            finish,
            DescriptorFinish::Abort | DescriptorFinish::AbortTwice
        ) {
            descriptor.abort().await
        } else {
            descriptor.close().await
        }
        .map_err(NativeError::from);
        let terminal = match &result {
            Ok(()) => DescriptorTerminalResult::Ok,
            Err(error) => DescriptorTerminalResult::Error {
                errno: error.errno,
                error_type: error.error_type().into(),
            },
        };
        match evidence
            .descriptor
            .as_mut()
            .expect("descriptor evidence initialized before finish")
        {
            DescriptorObservation::Write {
                terminal_results, ..
            }
            | DescriptorObservation::Read {
                terminal_results, ..
            } => terminal_results.push(terminal),
        }
        result?;
    }
    Ok(())
}

async fn observe_drop_release(
    data: &DataPlane,
    value: &DataPath,
    budget: Budget<'_>,
) -> Result<()> {
    let mut delay = Duration::from_millis(1);
    loop {
        match data.lookup(value).await {
            Err(error) if error.errno() == Errno::Eexist => {
                budget.pace(&mut delay, Duration::from_millis(10)).await?;
                continue;
            }
            Err(error) if error.errno() == Errno::Enoent => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(NativeError::protocol("dropped writer published a blob")),
        }
        let options = OpenOptions {
            exclusive: true,
            ..OpenOptions::staged_blob(0)
        };
        match data.open(value, options).await {
            Ok(mut probe) => {
                probe.abort().await?;
                return Ok(());
            }
            Err(error) if error.errno() == Errno::Eexist => {}
            Err(error) => return Err(error.into()),
        }
        budget.pace(&mut delay, Duration::from_millis(10)).await?;
    }
}

/// Execute one role against its actual authorized data-plane session.
///
/// The caller owns the engine and session. Run this future on that engine's
/// task capability so its timers use the same execution substrate. Records are
/// delivered synchronously as milestones occur; no stdout parsing is required
/// for in-process consumers.
pub async fn run_native_program(
    program: &NativeProgram,
    data: &DataPlane,
    runtime: &Runtime,
    engine: &EngineHandle,
    emit: &(dyn Fn(&ActionObservation) -> io::Result<()> + Send + Sync),
) -> Result<()> {
    let process = program
        .case
        .processes
        .iter()
        .find(|process| process.id == program.process_id)
        .ok_or_else(|| {
            NativeError::protocol(format!(
                "unknown native process role: {}",
                program.process_id
            ))
        })?;
    let plan = routes::plan(&program.case, process)?;
    let prepare_at = routes::first_route(&program.case, process, &plan);
    let deadline = engine
        .now()
        .to_instant()
        .checked_add(Duration::from_millis(program.budget_ms))
        .ok_or_else(|| NativeError::protocol("native process budget overflows clock"))?;
    let mut routes = routes::PreparedRoutes::default();
    for (step, action) in process.actions.iter().enumerate() {
        let budget = Budget {
            runtime,
            engine,
            deadline: deadline.min(engine.now().to_instant() + Duration::from_secs(30)),
        };
        let mut record = observation(&process.id, step, action);
        let mut evidence = Evidence::default();
        let result = budget
            .bound(async {
                if prepare_at == Some(step) {
                    routes.prepare(data, process, &plan, budget, emit).await?;
                }
                actions::execute(
                    data,
                    action,
                    &mut record,
                    &mut evidence,
                    plan[step].as_ref(),
                    &mut routes,
                    budget,
                    program.case.stream_frame_limit(),
                    emit,
                )
                .await
            })
            .await;
        if evidence.incarnation.is_none() {
            evidence.incarnation = routes
                .endpoints
                .get(&step)
                .map(routes::Endpoint::incarnation);
        }
        evidence.apply(&mut record);
        match result {
            Ok(()) => {
                emit(&record)?;
                hint_barrier(emit, &record, action)?;
            }
            Err(error) => {
                let expected = match action.expected {
                    ExpectedOutcome::Error(errno)
                    | ExpectedOutcome::Linearized { error: errno, .. } => {
                        error.errno == Some(errno)
                    }
                    ExpectedOutcome::Ok => false,
                };
                record.outcome = if expected { "expected_error" } else { "error" }.into();
                record.errno = error.errno;
                record.error_type = Some(error.error_type().into());
                record.error = Some(error.message.clone());
                // Error records retain partial transfer evidence, not successful
                // terminal payload fields.
                record.length = None;
                record.digest = None;
                record.token = None;
                record.lap = None;
                emit(&record)?;
                if !expected {
                    return Err(error);
                }
            }
        }
    }
    // Any prepared endpoint not consumed because of an expected failure is
    // dropped here; native endpoint Drop performs the protocol abort.
    drop(routes);
    Ok(())
}
