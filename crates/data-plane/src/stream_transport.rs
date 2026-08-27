//! Replaceable bounded transport port for SPSC stream incarnations.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::byte_ring::{Endpoint, FlowError};
use crate::namespace::StreamIncarnation;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPeerDescriptor(pub Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamTransportEvent {
    Ready,
    DataAvailable,
    CapacityAvailable,
    Quiesced,
    Fault(String),
}

pub trait StreamTransportNotifier: Send + Sync + 'static {
    fn notify(&self, event: StreamTransportEvent);
}

pub struct StreamSourceRequest {
    pub incarnation: StreamIncarnation,
    pub peer: StreamPeerDescriptor,
    pub endpoint: Endpoint,
    pub notifier: Arc<dyn StreamTransportNotifier>,
}

pub struct StreamSinkRequest {
    pub incarnation: StreamIncarnation,
    pub endpoint: Endpoint,
    pub notifier: Arc<dyn StreamTransportNotifier>,
}

/// Transport effect boundary. Namespace, terminal, reconnection, and lease
/// policy stay in the data-plane actors that invoke this port.
pub trait StreamTransport: Send + Sync + 'static {
    fn descriptor(&self) -> Result<StreamPeerDescriptor, String>;
    fn install_source(&self, request: StreamSourceRequest) -> Result<(), String>;
    fn install_sink(&self, request: StreamSinkRequest) -> Result<(), String>;
    fn source_progress(&self, incarnation: StreamIncarnation);
    fn sink_progress(&self, incarnation: StreamIncarnation);
    fn source_has_capacity(&self, _incarnation: StreamIncarnation) -> bool {
        false
    }
    fn sink_has_data(&self, _incarnation: StreamIncarnation) -> bool {
        false
    }
    fn terminate(&self, incarnation: StreamIncarnation);
}

struct LocalSource {
    endpoint: Endpoint,
    notifier: Arc<dyn StreamTransportNotifier>,
}

struct LocalSink {
    endpoint: Endpoint,
    notifier: Arc<dyn StreamTransportNotifier>,
}

#[derive(Default)]
struct LocalTransfer {
    source: Option<LocalSource>,
    sink: Option<LocalSink>,
    ready: bool,
}

#[derive(Default)]
struct LocalState {
    transfers: BTreeMap<StreamIncarnation, LocalTransfer>,
}

static NEXT_LOCAL_TRANSPORT: AtomicU64 = AtomicU64::new(1);

/// Direct in-process adapter. Host composition shares one instance between
/// sessions on a node. Payload bytes copy once from the source ring spans to
/// the destination ring spans; no payload-sized staging allocation exists.
pub struct LocalStreamTransport {
    id: u64,
    state: Mutex<LocalState>,
}

impl Default for LocalStreamTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalStreamTransport {
    pub fn new() -> Self {
        Self {
            id: NEXT_LOCAL_TRANSPORT.fetch_add(1, Ordering::Relaxed),
            state: Mutex::new(LocalState::default()),
        }
    }

    fn drive(&self, incarnation: StreamIncarnation) {
        let mut notifications = Vec::new();
        let mut fault = None;
        let mut terminal_moved = false;
        {
            let mut state = self.state.lock();
            let Some(transfer) = state.transfers.get_mut(&incarnation) else {
                return;
            };
            let (Some(source), Some(sink)) = (&mut transfer.source, &mut transfer.sink) else {
                return;
            };
            if !transfer.ready {
                transfer.ready = true;
                notifications.push((Arc::clone(&source.notifier), StreamTransportEvent::Ready));
                notifications.push((Arc::clone(&sink.notifier), StreamTransportEvent::Ready));
            }

            let mut moved = false;
            loop {
                let meta = match source.endpoint.next_record_meta() {
                    Ok(Some(meta)) => meta,
                    Ok(None) => break,
                    Err(error) => {
                        fault = Some(format_flow_error(error));
                        break;
                    }
                };
                let mut destination = match sink.endpoint.reserve_record(meta.kind, meta.len) {
                    Ok(destination) => destination,
                    Err(FlowError::InsufficientSpace { .. }) => break,
                    Err(error) => {
                        fault = Some(format_flow_error(error));
                        break;
                    }
                };
                let source_view = match source.endpoint.peek_record() {
                    Ok(Some(view)) => view,
                    Ok(None) => {
                        fault = Some("source record disappeared after inspection".to_owned());
                        break;
                    }
                    Err(error) => {
                        fault = Some(format_flow_error(error));
                        break;
                    }
                };
                let (source_first, source_second) = source_view.spans();
                let (destination_first, destination_second) = destination.spans_mut();
                copy_spans(
                    source_first,
                    source_second,
                    destination_first,
                    destination_second,
                );
                if let Err(error) = destination.commit() {
                    fault = Some(format_flow_error(error));
                    break;
                }
                if let Err(error) = source_view.release() {
                    fault = Some(format_flow_error(error));
                    break;
                }
                moved = true;
                if matches!(
                    meta.kind,
                    crate::byte_ring::RecordKind::Eof | crate::byte_ring::RecordKind::Fault
                ) {
                    terminal_moved = true;
                    break;
                }
            }

            if moved {
                notifications.push((
                    Arc::clone(&source.notifier),
                    StreamTransportEvent::CapacityAvailable,
                ));
                notifications.push((
                    Arc::clone(&sink.notifier),
                    StreamTransportEvent::DataAvailable,
                ));
            }
            if terminal_moved {
                notifications.push((Arc::clone(&source.notifier), StreamTransportEvent::Quiesced));
                notifications.push((Arc::clone(&sink.notifier), StreamTransportEvent::Quiesced));
            }
            if let Some(reason) = &fault {
                notifications.push((
                    Arc::clone(&source.notifier),
                    StreamTransportEvent::Fault(reason.clone()),
                ));
                notifications.push((
                    Arc::clone(&sink.notifier),
                    StreamTransportEvent::Fault(reason.clone()),
                ));
            }
        }
        for (notifier, event) in notifications {
            notifier.notify(event);
        }
        if fault.is_some() {
            self.state.lock().transfers.remove(&incarnation);
        }
    }
}

impl StreamTransport for LocalStreamTransport {
    fn descriptor(&self) -> Result<StreamPeerDescriptor, String> {
        Ok(StreamPeerDescriptor(self.id.to_le_bytes().to_vec()))
    }

    fn install_source(&self, request: StreamSourceRequest) -> Result<(), String> {
        if request.peer != self.descriptor()? {
            return Err("local stream peer belongs to a different transport instance".to_owned());
        }
        let incarnation = request.incarnation;
        let mut state = self.state.lock();
        let transfer = state.transfers.entry(incarnation).or_default();
        if transfer.source.is_some() {
            return Err("stream source is already installed".to_owned());
        }
        transfer.source = Some(LocalSource {
            endpoint: request.endpoint,
            notifier: request.notifier,
        });
        drop(state);
        self.drive(incarnation);
        Ok(())
    }

    fn install_sink(&self, request: StreamSinkRequest) -> Result<(), String> {
        let incarnation = request.incarnation;
        let mut state = self.state.lock();
        let transfer = state.transfers.entry(incarnation).or_default();
        if transfer.sink.is_some() {
            return Err("stream sink is already installed".to_owned());
        }
        transfer.sink = Some(LocalSink {
            endpoint: request.endpoint,
            notifier: request.notifier,
        });
        drop(state);
        self.drive(incarnation);
        Ok(())
    }

    fn source_progress(&self, incarnation: StreamIncarnation) {
        self.drive(incarnation);
    }

    fn sink_progress(&self, incarnation: StreamIncarnation) {
        self.drive(incarnation);
    }

    fn source_has_capacity(&self, incarnation: StreamIncarnation) -> bool {
        self.state
            .lock()
            .transfers
            .get(&incarnation)
            .and_then(|transfer| transfer.source.as_ref())
            .is_some_and(|source| source.endpoint.probe().has_capacity())
    }

    fn sink_has_data(&self, incarnation: StreamIncarnation) -> bool {
        self.state
            .lock()
            .transfers
            .get(&incarnation)
            .and_then(|transfer| transfer.sink.as_ref())
            .is_some_and(|sink| sink.endpoint.probe().has_data())
    }

    fn terminate(&self, incarnation: StreamIncarnation) {
        let transfer = self.state.lock().transfers.remove(&incarnation);
        if let Some(transfer) = transfer {
            if let Some(source) = transfer.source {
                source.notifier.notify(StreamTransportEvent::Quiesced);
            }
            if let Some(sink) = transfer.sink {
                sink.notifier.notify(StreamTransportEvent::Quiesced);
            }
        }
    }
}

fn copy_spans(
    source_first: &[u8],
    source_second: &[u8],
    destination_first: &mut [u8],
    destination_second: &mut [u8],
) {
    debug_assert_eq!(
        source_first.len() + source_second.len(),
        destination_first.len() + destination_second.len()
    );
    let sources = [source_first, source_second];
    let mut source_index = 0;
    let mut source_offset = 0;
    for destination in [destination_first, destination_second] {
        let mut destination_offset = 0;
        while destination_offset < destination.len() {
            while source_index < sources.len() && source_offset == sources[source_index].len() {
                source_index += 1;
                source_offset = 0;
            }
            let source = sources[source_index];
            let take = (destination.len() - destination_offset).min(source.len() - source_offset);
            destination[destination_offset..destination_offset + take]
                .copy_from_slice(&source[source_offset..source_offset + take]);
            destination_offset += take;
            source_offset += take;
        }
    }
}

fn format_flow_error(error: FlowError) -> String {
    format!("byte ring transport fault: {error:?}")
}
