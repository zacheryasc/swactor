//! Iroh-specific implementation of the data-plane SPSC stream transport port.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use data_plane::byte_ring::{Endpoint as RingEndpoint, FlowError, RecordKind, RingProbe};
use data_plane::namespace::StreamIncarnation;
use data_plane::stream_transport::{
    StreamPeerDescriptor, StreamSinkRequest, StreamSourceRequest, StreamTransport,
    StreamTransportEvent, StreamTransportNotifier,
};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint as IrohEndpoint, EndpointAddr};
use parking_lot::Mutex;
use swactor_engine::EngineHandle;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

pub const STREAM_ALPN: &[u8] = b"swactor/data-plane-spsc/1";
const PREAMBLE_LEN: usize = 16;
const RECORD_HEADER_LEN: usize = 5;

#[derive(Clone)]
struct TaskControl {
    wake: mpsc::Sender<()>,
    progress_pending: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl TaskControl {
    fn pair() -> (Self, mpsc::Receiver<()>) {
        let (wake, receiver) = mpsc::channel(1);
        (
            Self {
                wake,
                progress_pending: Arc::new(AtomicBool::new(false)),
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            receiver,
        )
    }

    fn progress(&self) {
        if !self.progress_pending.swap(true, Ordering::AcqRel) {
            let _ = self.wake.try_send(());
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.progress();
    }

    async fn wait(&self, receiver: &mut mpsc::Receiver<()>) -> bool {
        if self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        if receiver.recv().await.is_none() {
            return false;
        }
        self.progress_pending.store(false, Ordering::Release);
        !self.cancelled.load(Ordering::Acquire)
    }
}

struct PendingSink {
    endpoint: RingEndpoint,
    notifier: Arc<dyn StreamTransportNotifier>,
}

#[derive(Default)]
struct TransportState {
    pending_sinks: BTreeMap<StreamIncarnation, PendingSink>,
    controls: BTreeMap<StreamIncarnation, Vec<TaskControl>>,
    source_probes: BTreeMap<StreamIncarnation, RingProbe>,
    sink_probes: BTreeMap<StreamIncarnation, RingProbe>,
}

struct Inner {
    engine: EngineHandle,
    endpoint: IrohEndpoint,
    state: Mutex<TransportState>,
}

/// Cloneable adapter capability installed into `HostDataPlaneConfig`.
#[derive(Clone)]
pub struct IrohStreamTransport {
    inner: Arc<Inner>,
}

impl IrohStreamTransport {
    pub fn new(engine: EngineHandle, endpoint: IrohEndpoint) -> Self {
        Self {
            inner: Arc::new(Inner {
                engine,
                endpoint,
                state: Mutex::new(TransportState::default()),
            }),
        }
    }

    pub(crate) fn accept_connection(&self, connection: Connection) {
        let transport = self.clone();
        self.inner.engine.spawn(async move {
            while let Ok(mut recv) = connection.accept_uni().await {
                let mut preamble = [0_u8; PREAMBLE_LEN];
                if recv.read_exact(&mut preamble).await.is_err() {
                    continue;
                }
                let incarnation = decode_incarnation(preamble);
                let pending = transport
                    .inner
                    .state
                    .lock()
                    .pending_sinks
                    .remove(&incarnation);
                let Some(pending) = pending else {
                    continue;
                };
                let (control, receiver) = TaskControl::pair();
                transport
                    .inner
                    .state
                    .lock()
                    .controls
                    .entry(incarnation)
                    .or_default()
                    .push(control.clone());
                transport.inner.engine.spawn(run_sink(
                    incarnation,
                    recv,
                    pending.endpoint,
                    pending.notifier,
                    control,
                    receiver,
                ));
            }
        });
    }

    fn register_control(&self, incarnation: StreamIncarnation, control: TaskControl) {
        self.inner
            .state
            .lock()
            .controls
            .entry(incarnation)
            .or_default()
            .push(control);
    }

    fn progress_controls(&self, incarnation: StreamIncarnation) {
        if let Some(controls) = self.inner.state.lock().controls.get(&incarnation) {
            for control in controls {
                control.progress();
            }
        }
    }
}

impl StreamTransport for IrohStreamTransport {
    fn descriptor(&self) -> Result<StreamPeerDescriptor, String> {
        serde_json::to_vec(&self.inner.endpoint.addr())
            .map(StreamPeerDescriptor)
            .map_err(|error| format!("encode iroh stream endpoint: {error}"))
    }

    fn install_source(&self, request: StreamSourceRequest) -> Result<(), String> {
        let peer: EndpointAddr = serde_json::from_slice(&request.peer.0)
            .map_err(|error| format!("decode iroh stream endpoint: {error}"))?;
        let (control, receiver) = TaskControl::pair();
        self.register_control(request.incarnation, control.clone());
        let endpoint = self.inner.endpoint.clone();
        let probe = request.endpoint.probe();
        self.inner
            .state
            .lock()
            .source_probes
            .insert(request.incarnation, probe);
        self.inner.engine.spawn(run_source(
            request.incarnation,
            endpoint,
            peer,
            request.endpoint,
            request.notifier,
            control,
            receiver,
        ));
        Ok(())
    }

    fn install_sink(&self, request: StreamSinkRequest) -> Result<(), String> {
        let mut state = self.inner.state.lock();
        if state.pending_sinks.contains_key(&request.incarnation) {
            return Err("iroh stream sink is already installed".to_owned());
        }
        let probe = request.endpoint.probe();
        state.sink_probes.insert(request.incarnation, probe);
        state.pending_sinks.insert(
            request.incarnation,
            PendingSink {
                endpoint: request.endpoint,
                notifier: request.notifier,
            },
        );
        Ok(())
    }

    fn source_progress(&self, incarnation: StreamIncarnation) {
        self.progress_controls(incarnation);
    }

    fn sink_progress(&self, incarnation: StreamIncarnation) {
        self.progress_controls(incarnation);
    }

    fn source_has_capacity(&self, incarnation: StreamIncarnation) -> bool {
        self.inner
            .state
            .lock()
            .source_probes
            .get(&incarnation)
            .is_some_and(RingProbe::has_capacity)
    }

    fn sink_has_data(&self, incarnation: StreamIncarnation) -> bool {
        self.inner
            .state
            .lock()
            .sink_probes
            .get(&incarnation)
            .is_some_and(RingProbe::has_data)
    }

    fn terminate(&self, incarnation: StreamIncarnation) {
        let (pending, controls) = {
            let mut state = self.inner.state.lock();
            let pending = state.pending_sinks.remove(&incarnation);
            let controls = state.controls.remove(&incarnation).unwrap_or_default();
            state.source_probes.remove(&incarnation);
            state.sink_probes.remove(&incarnation);
            (pending, controls)
        };
        if let Some(pending) = pending {
            pending.notifier.notify(StreamTransportEvent::Quiesced);
        }
        for control in controls {
            control.cancel();
        }
    }
}

async fn run_source(
    incarnation: StreamIncarnation,
    endpoint: IrohEndpoint,
    peer: EndpointAddr,
    mut source: RingEndpoint,
    notifier: Arc<dyn StreamTransportNotifier>,
    control: TaskControl,
    mut receiver: mpsc::Receiver<()>,
) {
    let result = async {
        let connection = endpoint
            .connect(peer, STREAM_ALPN)
            .await
            .map_err(|error| format!("connect stream incarnation: {error}"))?;
        let mut send = connection
            .open_uni()
            .await
            .map_err(|error| format!("open stream incarnation: {error}"))?;
        send.write_all(&encode_incarnation(incarnation))
            .await
            .map_err(|error| format!("write stream preamble: {error}"))?;
        send.flush()
            .await
            .map_err(|error| format!("flush stream preamble: {error}"))?;
        notifier.notify(StreamTransportEvent::Ready);

        loop {
            if control.cancelled.load(Ordering::Acquire) {
                return Ok(());
            }
            let mut moved = false;
            while let Some(meta) = source
                .next_record_meta()
                .map_err(|error| format!("inspect source ring: {error:?}"))?
            {
                let view = source
                    .peek_record()
                    .map_err(|error| format!("pin source ring: {error:?}"))?
                    .ok_or_else(|| "source record disappeared after inspection".to_owned())?;
                write_record(&mut send, meta.kind, view.spans())
                    .await
                    .map_err(|error| format!("write stream record: {error}"))?;
                view.release()
                    .map_err(|error| format!("release source ring: {error:?}"))?;
                notifier.notify(StreamTransportEvent::CapacityAvailable);
                moved = true;
                if matches!(meta.kind, RecordKind::Eof | RecordKind::Fault) {
                    send.finish()
                        .map_err(|error| format!("finish stream incarnation: {error}"))?;
                    match send
                        .stopped()
                        .await
                        .map_err(|error| format!("await stream finish: {error}"))?
                    {
                        Some(code) => {
                            return Err(format!("peer stopped stream incarnation: {code}"));
                        }
                        None => return Ok(()),
                    }
                }
            }
            if !moved && !control.wait(&mut receiver).await {
                return Ok(());
            }
        }
    }
    .await;
    if let Err(reason) = result {
        notifier.notify(StreamTransportEvent::Fault(reason));
    }
    notifier.notify(StreamTransportEvent::Quiesced);
}

async fn write_record(
    send: &mut SendStream,
    kind: RecordKind,
    spans: (&[u8], &[u8]),
) -> Result<(), String> {
    let len = spans.0.len() + spans.1.len();
    let len = u32::try_from(len).map_err(|_| "stream record exceeds u32 framing".to_owned())?;
    let mut header = [0_u8; RECORD_HEADER_LEN];
    header[0] = kind.to_byte();
    header[1..].copy_from_slice(&len.to_le_bytes());
    send.write_all(&header)
        .await
        .map_err(|error| error.to_string())?;
    if !spans.0.is_empty() {
        send.write_all(spans.0)
            .await
            .map_err(|error| error.to_string())?;
    }
    if !spans.1.is_empty() {
        send.write_all(spans.1)
            .await
            .map_err(|error| error.to_string())?;
    }
    send.flush().await.map_err(|error| error.to_string())
}

async fn run_sink(
    _incarnation: StreamIncarnation,
    mut recv: RecvStream,
    mut sink: RingEndpoint,
    notifier: Arc<dyn StreamTransportNotifier>,
    control: TaskControl,
    mut receiver: mpsc::Receiver<()>,
) {
    notifier.notify(StreamTransportEvent::Ready);
    let result = async {
        loop {
            if control.cancelled.load(Ordering::Acquire) {
                return Ok(());
            }
            let mut header = [0_u8; RECORD_HEADER_LEN];
            recv.read_exact(&mut header)
                .await
                .map_err(|error| format!("read stream record header: {error}"))?;
            let kind = RecordKind::from_byte(header[0])
                .ok_or_else(|| format!("invalid stream record kind {}", header[0]))?;
            let len = u64::from(u32::from_le_bytes(header[1..].try_into().unwrap()));
            let mut reservation = loop {
                match sink.reserve_record(kind, len) {
                    Ok(reservation) => break reservation,
                    Err(FlowError::InsufficientSpace { .. }) => {
                        if !control.wait(&mut receiver).await {
                            return Ok(());
                        }
                    }
                    Err(error) => return Err(format!("reserve sink ring: {error:?}")),
                }
            };
            let (first, second) = reservation.spans_mut();
            if !first.is_empty() {
                recv.read_exact(first)
                    .await
                    .map_err(|error| format!("read first stream span: {error}"))?;
            }
            if !second.is_empty() {
                recv.read_exact(second)
                    .await
                    .map_err(|error| format!("read second stream span: {error}"))?;
            }
            reservation
                .commit()
                .map_err(|error| format!("commit sink ring: {error:?}"))?;
            notifier.notify(StreamTransportEvent::DataAvailable);
            if matches!(kind, RecordKind::Eof | RecordKind::Fault) {
                let mut trailing = [0_u8; 1];
                match recv
                    .read(&mut trailing)
                    .await
                    .map_err(|error| format!("read stream finish: {error}"))?
                {
                    None | Some(0) => return Ok(()),
                    Some(_) => return Err("bytes followed terminal stream record".to_owned()),
                }
            }
        }
    }
    .await;
    if let Err(reason) = result {
        notifier.notify(StreamTransportEvent::Fault(reason));
    }
    notifier.notify(StreamTransportEvent::Quiesced);
}

fn encode_incarnation(incarnation: StreamIncarnation) -> [u8; PREAMBLE_LEN] {
    let mut encoded = [0_u8; PREAMBLE_LEN];
    encoded[..8].copy_from_slice(&incarnation.authority_epoch.to_le_bytes());
    encoded[8..].copy_from_slice(&incarnation.revision.to_le_bytes());
    encoded
}

fn decode_incarnation(encoded: [u8; PREAMBLE_LEN]) -> StreamIncarnation {
    StreamIncarnation {
        authority_epoch: u64::from_le_bytes(encoded[..8].try_into().unwrap()),
        revision: u64::from_le_bytes(encoded[8..].try_into().unwrap()),
    }
}
