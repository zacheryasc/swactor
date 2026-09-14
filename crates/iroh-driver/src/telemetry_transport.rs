//! Iroh/QUIC transport adapter for telemetry subscriptions.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::TryRecvError;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
use swactor::actor::ActorAddress;
use swactor::runtime::ExternalSender;
use swactor_engine::EngineHandle;
use telemetry::frame::{
    ChannelDescriptor, ChannelId, ChannelRef, FrameDelivery, Position, StreamDescriptor, StreamId,
    TelemetryEvent,
};
use telemetry::{TelemetrySnapshot, TelemetrySubscription};

pub const TELEMETRY_ALPN: &[u8] = b"swactor/telemetry/1";

const MAGIC: &[u8; 4] = b"DSQ2";
const TAG_CHANNEL_DECLARED: u8 = 0x01;
const TAG_FRAME_BATCH: u8 = 0x02;
const TAG_STREAM_ENDED: u8 = 0x03;
const TAG_LZ4_FRAME_BATCH: u8 = 0x04;
const TAG_ZSTD_FRAME_BATCH: u8 = 0x05;
const WRITE_BATCH_TARGET_BYTES: usize = 256 * 1024;
const WRITE_BATCH_MAX_EVENTS: usize = 1024;

// ─── Pull model: collector-initiated subscriptions ───────────────────────────
//
// A supervising node dials a freshly-bootstrapped node on `TELEMETRY_ALPN`,
// sends one subscription request on the first uni stream, and the node answers
// by writing the existing header+events stream shape on a uni stream of the
// same connection. Subscription lifetime = connection lifetime.

/// Write a pull request (magic + flow id + token + `SubscriptionRequest`) and
/// finish the stream so the serving side's read completes.
const REQUEST_MAGIC: &[u8; 4] = b"DSQR";
pub async fn write_pull_request(
    send: &mut SendStream,
    flow_id: [u8; 16],
    token: &[u8],
    request: &telemetry::SubscriptionRequest,
) -> Result<(), BoxError> {
    if token.len() > u16::MAX as usize {
        return Err("telemetry pull token exceeds u16 length prefix".into());
    }
    send.write_all(REQUEST_MAGIC).await?;
    send.write_all(&flow_id).await?;
    send.write_all(&(token.len() as u16).to_le_bytes()).await?;
    send.write_all(token).await?;
    write_json(send, request).await?;
    send.finish()?;
    Ok(())
}

/// Read a pull request written by [`write_pull_request`].
pub async fn read_pull_request(
    recv: &mut RecvStream,
) -> Result<([u8; 16], Vec<u8>, telemetry::SubscriptionRequest), BoxError> {
    let mut magic = [0u8; 4];
    recv.read_exact(&mut magic).await?;
    if &magic != REQUEST_MAGIC {
        return Err("invalid telemetry pull request magic".into());
    }
    let mut flow_id = [0u8; 16];
    recv.read_exact(&mut flow_id).await?;
    let mut token_len = [0u8; 2];
    recv.read_exact(&mut token_len).await?;
    let token_len = u16::from_le_bytes(token_len) as usize;
    let mut token = vec![0u8; token_len];
    recv.read_exact(&mut token).await?;
    let request = read_json(recv).await?;
    Ok((flow_id, token, request))
}

/// Node side: serve one accepted `TELEMETRY_ALPN` connection. Reads the pull
/// request from the first uni stream, subscribes the local endpoint, and
/// writes the answering subscription stream (header + events, until the
/// subscription ends or the connection drops) on a uni stream of the same
/// connection. One request per connection; the task exits when the writer
/// ends or the connection fails.
pub fn spawn_pull_server(
    engine: &EngineHandle,
    conn: Connection,
    endpoint: std::sync::Arc<telemetry::TelemetryEndpoint>,
    idle_sleep: Duration,
) {
    let engine_handle = engine.clone();
    engine.spawn(async move {
        let Ok(mut recv) = conn.accept_uni().await else {
            return;
        };
        let Ok((_flow_id, token, request)) = read_pull_request(&mut recv).await else {
            return;
        };
        let subscription = endpoint.subscribe_retained("supervisor-pull", request);
        let header =
            match TelemetryQuicHeader::from_snapshot(_flow_id, token, subscription.snapshot()) {
                Ok(header) => header,
                Err(_) => return,
            };
        let Ok(send) = conn.open_uni().await else {
            return;
        };
        tokio::select! {
            _ = conn.closed() => {}
            _ = write_subscription_until_closed(
                &engine_handle, send, header, subscription, idle_sleep,
            ) => {}
        }
    });
}

/// Cancellation handle for one collector-initiated telemetry subscription.
///
/// Cancellation is idempotent and immediately interrupts network I/O,
/// reconnect backoff, and all subsequent reconnect attempts.
#[derive(Debug)]
pub struct PullCollectorHandle {
    cancellation: tokio::sync::watch::Sender<bool>,
    completion: tokio::sync::watch::Receiver<bool>,
}

impl PullCollectorHandle {
    pub fn cancel(&self) {
        self.cancellation.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancellation.borrow()
    }

    pub fn is_finished(&self) -> bool {
        *self.completion.borrow()
    }
}

impl Drop for PullCollectorHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

struct PullCollectorCompletion(tokio::sync::watch::Sender<bool>);

impl Drop for PullCollectorCompletion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

pub struct PullCollectorConfig {
    pub endpoint: Endpoint,
    pub peer: EndpointAddr,
    pub flow_id: [u8; 16],
    pub token: Vec<u8>,
    pub request: telemetry::SubscriptionRequest,
    pub fanout: std::sync::Arc<telemetry::DeliveryFanout>,
}

/// Supervisor side: retain a pull subscription to a node on `TELEMETRY_ALPN`.
///
/// A transport interruption reconnects with bounded backoff. Returning after
/// the first EOF leaves a healthy node permanently stale, which is especially
/// easy to trigger while several freshly-bootstrapped nodes answer at once.
pub fn spawn_pull_collector(
    engine: &EngineHandle,
    config: PullCollectorConfig,
    on_header: std::sync::mpsc::Sender<TelemetryQuicHeader>,
) -> PullCollectorHandle {
    spawn_pull_collector_with_sink(engine, config, PullHeaderSink::Channel(on_header))
}

/// Supervisor side variant that delivers each connection header directly to
/// an actor. Transport owns the subscription task; the actor owns how the
/// stream identity changes domain state.
pub fn spawn_pull_collector_to_actor(
    engine: &EngineHandle,
    config: PullCollectorConfig,
    sender: ExternalSender,
    actor: ActorAddress,
) -> PullCollectorHandle {
    spawn_pull_collector_with_sink(engine, config, PullHeaderSink::Actor { sender, actor })
}

enum PullHeaderSink {
    Channel(std::sync::mpsc::Sender<TelemetryQuicHeader>),
    Actor {
        sender: ExternalSender,
        actor: ActorAddress,
    },
}

impl PullHeaderSink {
    fn deliver(&self, header: TelemetryQuicHeader) -> bool {
        match self {
            Self::Channel(sender) => sender.send(header).is_ok(),
            Self::Actor { sender, actor } => sender.send_to(*actor, header).is_ok(),
        }
    }
}

fn spawn_pull_collector_with_sink(
    engine: &EngineHandle,
    config: PullCollectorConfig,
    on_header: PullHeaderSink,
) -> PullCollectorHandle {
    let PullCollectorConfig {
        endpoint,
        peer,
        flow_id,
        token,
        request,
        fanout,
    } = config;
    let (cancellation, mut cancellation_rx) = tokio::sync::watch::channel(false);
    let (completion, completion_rx) = tokio::sync::watch::channel(false);
    let engine_handle = engine.clone();
    engine.spawn(async move {
        let _completion = PullCollectorCompletion(completion);
        let peer_id = peer.id.to_string();
        let mut retry_delay = Duration::from_millis(250);
        let mut cursor = None;
        loop {
            if *cancellation_rx.borrow() {
                return;
            }
            let result = tokio::select! {
                _ = cancellation_rx.changed() => return,
                result = collect_pull_once(
                    &endpoint, &peer, flow_id, &token, &request, &fanout, &on_header,
                    &mut cursor,
                ) => result,
            };
            match result {
                Ok(()) => return,
                Err(error) => {
                    eprintln!(
                        "telemetry-pull: {peer_id}: {error}; retrying in {} ms",
                        retry_delay.as_millis()
                    );
                }
            }
            tokio::select! {
                _ = cancellation_rx.changed() => return,
                _ = engine_handle.timer(retry_delay) => {}
            }
            retry_delay = retry_delay
                .checked_mul(2)
                .unwrap_or(Duration::from_secs(5))
                .min(Duration::from_secs(5));
        }
    });
    PullCollectorHandle {
        cancellation,
        completion: completion_rx,
    }
}

async fn collect_pull_once(
    endpoint: &Endpoint,
    peer: &EndpointAddr,
    flow_id: [u8; 16],
    token: &[u8],
    request: &telemetry::SubscriptionRequest,
    fanout: &telemetry::DeliveryFanout,
    on_header: &PullHeaderSink,
    cursor: &mut Option<(StreamId, Position)>,
) -> Result<(), String> {
    let conn = endpoint
        .connect(peer.clone(), TELEMETRY_ALPN)
        .await
        .map_err(|error| format!("connect failed: {error}"))?;
    let mut req = conn
        .open_uni()
        .await
        .map_err(|error| format!("open request stream failed: {error}"))?;
    write_pull_request(&mut req, flow_id, token, request)
        .await
        .map_err(|error| format!("write request failed: {error}"))?;
    let mut recv = conn
        .accept_uni()
        .await
        .map_err(|error| format!("no answer stream: {error}"))?;
    let header = read_header(&mut recv)
        .await
        .map_err(|error| format!("answer header unreadable: {error}"))?;
    if !on_header.deliver(header.clone()) {
        return Ok(());
    }
    let stream = header.stream;
    if cursor
        .as_ref()
        .is_some_and(|(previous, _)| previous != &stream.stream)
    {
        *cursor = None;
    }
    loop {
        match read_next_events(&mut recv, &stream).await {
            Ok(Some(mut events)) => {
                // The endpoint replays its retained suffix on every connection.
                // A cursor belongs to this producer stream, not a channel, and
                // survives reconnects so a real frame is delivered only once.
                // Do not commit it past a locally dropped delivery batch: the
                // reconnect must replay that batch from its previous boundary.
                let cursor_before_batch = cursor.clone();
                events.retain(|event| {
                    let TelemetryEvent::Frame(frame) = event else {
                        return true;
                    };
                    if cursor
                        .as_ref()
                        .is_some_and(|(_, position)| frame.position <= *position)
                    {
                        return false;
                    }
                    *cursor = Some((frame.channel.stream.clone(), frame.position));
                    true
                });
                let ended = events
                    .iter()
                    .any(|event| matches!(event, TelemetryEvent::StreamEnded(_)));
                let delivery = fanout.publish_batch(events);
                if delivery.dropped_for_subscribers != 0 {
                    *cursor = cursor_before_batch;
                    return Err(format!(
                        "collector fanout dropped {} telemetry events",
                        delivery.dropped_for_subscribers
                    ));
                }
                if ended {
                    return Ok(());
                }
            }
            Ok(None) => return Err("answer stream closed".to_owned()),
            Err(error) => return Err(format!("read answer stream failed: {error}")),
        }
    }
}
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryQuicHeader {
    pub flow_id: [u8; 16],
    pub token: Vec<u8>,
    pub stream: StreamDescriptor,
    pub channels: Vec<ChannelDescriptor>,
}

impl TelemetryQuicHeader {
    pub fn new(
        flow_id: [u8; 16],
        token: impl Into<Vec<u8>>,
        stream: StreamDescriptor,
        channels: Vec<ChannelDescriptor>,
    ) -> Self {
        Self {
            flow_id,
            token: token.into(),
            stream,
            channels,
        }
    }

    pub fn from_snapshot(
        flow_id: [u8; 16],
        token: impl Into<Vec<u8>>,
        snapshot: &TelemetrySnapshot,
    ) -> Result<Self, BoxError> {
        let stream = snapshot
            .streams
            .first()
            .cloned()
            .ok_or("telemetry subscription snapshot has no stream")?;
        Ok(Self::new(flow_id, token, stream, snapshot.channels.clone()))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TelemetryQuicWriteStats {
    pub events: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryQuicRead {
    pub header: TelemetryQuicHeader,
    pub events: Vec<TelemetryEvent>,
}

pub fn spawn_subscription_writer(
    engine: &EngineHandle,
    endpoint: Endpoint,
    peer: EndpointAddr,
    header: TelemetryQuicHeader,
    subscription: TelemetrySubscription,
    idle_sleep: Duration,
) {
    let engine_handle = engine.clone();
    engine.spawn(async move {
        let Ok(conn) = endpoint.connect(peer, TELEMETRY_ALPN).await else {
            return;
        };
        let Ok(send) = conn.open_uni().await else {
            return;
        };
        let _ =
            write_subscription_until_closed(&engine_handle, send, header, subscription, idle_sleep)
                .await;
    });
}

pub async fn write_available_subscription(
    engine: &EngineHandle,
    send: SendStream,
    header: &TelemetryQuicHeader,
    subscription: &TelemetrySubscription,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    write_subscription_inner(engine, send, header, subscription, None).await
}

pub async fn write_subscription_until_closed(
    engine: &EngineHandle,
    mut send: SendStream,
    header: TelemetryQuicHeader,
    subscription: TelemetrySubscription,
    idle_sleep: Duration,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    write_header(&mut send, &header).await?;
    let mut stats = TelemetryQuicWriteStats::default();
    let mut compressor = zstd::bulk::Compressor::new(1)?;
    let mut raw = Vec::with_capacity(WRITE_BATCH_TARGET_BYTES);
    let mut bytes = Vec::with_capacity(WRITE_BATCH_TARGET_BYTES);
    let mut events = Vec::with_capacity(WRITE_BATCH_MAX_EVENTS);
    loop {
        let dropped = subscription.dropped();
        if dropped != 0 {
            return Err(format!("telemetry subscription dropped {dropped} events").into());
        }
        match subscription.try_recv() {
            Ok(first) => {
                take_event_batch(first, &subscription, &mut events);
                let written =
                    write_event_batch(&mut send, &events, &mut compressor, &mut raw, &mut bytes)
                        .await?;
                stats.events += written.events;
                stats.bytes += written.bytes;
            }
            Err(TryRecvError::Empty) => engine.timer(idle_sleep).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    send.finish()?;
    Ok(stats)
}

async fn write_subscription_inner(
    engine: &EngineHandle,
    mut send: SendStream,
    header: &TelemetryQuicHeader,
    subscription: &TelemetrySubscription,
    idle_sleep: Option<Duration>,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    write_header(&mut send, header).await?;
    let mut stats = TelemetryQuicWriteStats::default();
    let mut compressor = zstd::bulk::Compressor::new(1)?;
    let mut raw = Vec::with_capacity(WRITE_BATCH_TARGET_BYTES);
    let mut bytes = Vec::with_capacity(WRITE_BATCH_TARGET_BYTES);
    let mut events = Vec::with_capacity(WRITE_BATCH_MAX_EVENTS);
    loop {
        let dropped = subscription.dropped();
        if dropped != 0 {
            return Err(format!("telemetry subscription dropped {dropped} events").into());
        }
        match subscription.try_recv() {
            Ok(first) => {
                take_event_batch(first, subscription, &mut events);
                let written =
                    write_event_batch(&mut send, &events, &mut compressor, &mut raw, &mut bytes)
                        .await?;
                stats.events += written.events;
                stats.bytes += written.bytes;
            }
            Err(TryRecvError::Empty) => match idle_sleep {
                Some(delay) => engine.timer(delay).await,
                None => break,
            },
            Err(TryRecvError::Disconnected) => break,
        }
    }
    send.finish()?;
    Ok(stats)
}

fn take_event_batch(
    first: TelemetryEvent,
    subscription: &TelemetrySubscription,
    events: &mut Vec<TelemetryEvent>,
) {
    let mut estimated_bytes = event_size_hint(&first);
    events.clear();
    events.push(first);
    while estimated_bytes < WRITE_BATCH_TARGET_BYTES && events.len() < WRITE_BATCH_MAX_EVENTS {
        let Ok(event) = subscription.try_recv() else {
            break;
        };
        estimated_bytes = estimated_bytes.saturating_add(event_size_hint(&event));
        events.push(event);
    }
}

fn event_size_hint(event: &TelemetryEvent) -> usize {
    match event {
        TelemetryEvent::StreamDeclared(_) => 0,
        TelemetryEvent::ChannelDeclared(descriptor) => descriptor.name.len() + 64,
        TelemetryEvent::Frame(delivery) => delivery.payload.len() + 16,
        TelemetryEvent::StreamEnded(_) => 1,
    }
}

async fn write_event_batch(
    send: &mut SendStream,
    events: &[TelemetryEvent],
    compressor: &mut zstd::bulk::Compressor<'_>,
    raw: &mut Vec<u8>,
    bytes: &mut Vec<u8>,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    bytes.clear();
    bytes.reserve(
        events
            .iter()
            .map(event_size_hint)
            .sum::<usize>()
            .min(WRITE_BATCH_TARGET_BYTES * 2),
    );
    let stats = encode_event_batch_with(events, bytes, compressor, raw)?;
    if !bytes.is_empty() {
        send.write_all(bytes).await?;
    }
    Ok(stats)
}

/// Append telemetry events in the compact batched subscription wire format.
pub fn encode_event_batch(
    events: &[TelemetryEvent],
    out: &mut Vec<u8>,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    let mut compressor = zstd::bulk::Compressor::new(1)?;
    let mut raw = Vec::new();
    encode_event_batch_with(events, out, &mut compressor, &mut raw)
}

fn encode_event_batch_with(
    events: &[TelemetryEvent],
    out: &mut Vec<u8>,
    compressor: &mut zstd::bulk::Compressor<'_>,
    raw: &mut Vec<u8>,
) -> Result<TelemetryQuicWriteStats, BoxError> {
    let start = out.len();
    let mut encoded_events = 0;
    let mut index = 0;
    while index < events.len() {
        match &events[index] {
            TelemetryEvent::StreamDeclared(_) => {
                index += 1;
            }
            TelemetryEvent::ChannelDeclared(descriptor) => {
                let bytes = postcard::to_allocvec(descriptor)?;
                if bytes.len() > MAX_RECORD_BYTES {
                    return Err("telemetry channel declaration exceeds max size".into());
                }
                out.push(TAG_CHANNEL_DECLARED);
                put_varint(out, bytes.len() as u64);
                out.extend_from_slice(&bytes);
                encoded_events += 1;
                index += 1;
            }
            TelemetryEvent::Frame(_) => {
                let start_index = index;
                let mut estimated_bytes = 0_usize;
                while index < events.len()
                    && matches!(events[index], TelemetryEvent::Frame(_))
                    && index - start_index < WRITE_BATCH_MAX_EVENTS
                {
                    let next_bytes = event_size_hint(&events[index]);
                    if index > start_index
                        && estimated_bytes.saturating_add(next_bytes) > WRITE_BATCH_TARGET_BYTES
                    {
                        break;
                    }
                    estimated_bytes = estimated_bytes.saturating_add(next_bytes);
                    index += 1;
                }
                encode_frame_batch(&events[start_index..index], out, raw, compressor)?;
                encoded_events += index - start_index;
            }
            TelemetryEvent::StreamEnded(_) => {
                out.push(TAG_STREAM_ENDED);
                encoded_events += 1;
                index += 1;
            }
        }
    }
    Ok(TelemetryQuicWriteStats {
        events: encoded_events,
        bytes: out.len() - start,
    })
}

fn encode_frame_batch(
    events: &[TelemetryEvent],
    out: &mut Vec<u8>,
    raw: &mut Vec<u8>,
    compressor: &mut zstd::bulk::Compressor<'_>,
) -> Result<(), BoxError> {
    let raw_capacity = events.iter().fold(0_usize, |total, event| {
        let TelemetryEvent::Frame(frame) = event else {
            unreachable!("frame batch contains only frames");
        };
        total.saturating_add(frame.payload.len() + 16)
    });
    if raw_capacity > MAX_RECORD_BYTES {
        return Err("telemetry frame batch exceeds max size".into());
    }
    raw.clear();
    raw.reserve(raw_capacity);
    let mut previous_position = None;
    for event in events {
        let TelemetryEvent::Frame(frame) = event else {
            unreachable!("frame batch contains only frames");
        };
        put_varint(raw, u64::from(frame.channel.channel.0));
        let position = frame.position.0;
        let encoded_position = match previous_position {
            Some(previous) => position
                .checked_sub(previous)
                .ok_or("telemetry frame positions are not monotonic")?,
            None => position,
        };
        put_varint(raw, encoded_position);
        put_varint(raw, frame.payload.len() as u64);
        raw.extend_from_slice(&frame.payload);
        previous_position = Some(position);
    }

    let compressed = compressor.compress(raw)?;
    let compressed_size =
        1 + varint_size(raw.len() as u64) + varint_size(compressed.len() as u64) + compressed.len();
    let raw_size = 1 + varint_size(raw.len() as u64) + raw.len();
    if compressed_size < raw_size {
        out.push(TAG_ZSTD_FRAME_BATCH);
        put_varint(out, raw.len() as u64);
        put_varint(out, compressed.len() as u64);
        out.extend_from_slice(&compressed);
    } else {
        out.push(TAG_FRAME_BATCH);
        put_varint(out, raw.len() as u64);
        out.extend_from_slice(&raw);
    }
    Ok(())
}

/// Append one telemetry event in the compact subscription wire format.
pub fn encode_event_record(event: &TelemetryEvent, out: &mut Vec<u8>) -> Result<usize, BoxError> {
    Ok(encode_event_batch(std::slice::from_ref(event), out)?.bytes)
}

pub async fn write_event(send: &mut SendStream, event: &TelemetryEvent) -> Result<usize, BoxError> {
    let mut compressor = zstd::bulk::Compressor::new(1)?;
    let mut raw = Vec::new();
    let mut bytes = Vec::new();
    Ok(write_event_batch(
        send,
        std::slice::from_ref(event),
        &mut compressor,
        &mut raw,
        &mut bytes,
    )
    .await?
    .bytes)
}

pub async fn read_stream_header(recv: &mut RecvStream) -> Result<TelemetryQuicHeader, BoxError> {
    read_header(recv).await
}

pub async fn read_events_from_stream(mut recv: RecvStream) -> Result<TelemetryQuicRead, BoxError> {
    let header = read_header(&mut recv).await?;
    let mut events = Vec::new();
    while let Some(batch) = read_next_events(&mut recv, &header.stream).await? {
        events.extend(batch);
    }
    Ok(TelemetryQuicRead { header, events })
}

pub async fn read_next_uni_from_connection(
    conn: &Connection,
) -> Result<TelemetryQuicRead, BoxError> {
    let recv = conn.accept_uni().await?;
    read_events_from_stream(recv).await
}

pub fn spawn_connection_reader(
    engine: &EngineHandle,
    conn: Connection,
    sink: std::sync::mpsc::Sender<TelemetryEvent>,
) {
    engine.spawn(async move {
        loop {
            let recv = match conn.accept_uni().await {
                Ok(recv) => recv,
                Err(_) => return,
            };
            let Ok(read) = read_events_from_stream(recv).await else {
                continue;
            };
            for event in read.events {
                if sink.send(event).is_err() {
                    return;
                }
            }
        }
    });
}

async fn write_header(send: &mut SendStream, header: &TelemetryQuicHeader) -> Result<(), BoxError> {
    if header.token.len() > u16::MAX as usize {
        return Err("telemetry token exceeds u16 length prefix".into());
    }
    send.write_all(MAGIC).await?;
    send.write_all(&header.flow_id).await?;
    send.write_all(&(header.token.len() as u16).to_le_bytes())
        .await?;
    send.write_all(&header.token).await?;
    write_postcard(send, &header.stream).await?;
    write_postcard(send, &header.channels).await?;
    Ok(())
}

async fn read_header(recv: &mut RecvStream) -> Result<TelemetryQuicHeader, BoxError> {
    let mut magic = [0u8; 4];
    recv.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err("invalid telemetry QUIC magic".into());
    }
    let mut flow_id = [0u8; 16];
    recv.read_exact(&mut flow_id).await?;
    let mut token_len = [0u8; 2];
    recv.read_exact(&mut token_len).await?;
    let token_len = u16::from_le_bytes(token_len) as usize;
    let mut token = vec![0u8; token_len];
    recv.read_exact(&mut token).await?;
    let stream = read_postcard(recv).await?;
    let channels = read_postcard(recv).await?;
    Ok(TelemetryQuicHeader {
        flow_id,
        token,
        stream,
        channels,
    })
}

/// Read the next compact event batch from a telemetry subscription stream.
pub async fn read_next_events(
    recv: &mut RecvStream,
    stream: &StreamDescriptor,
) -> Result<Option<Vec<TelemetryEvent>>, BoxError> {
    let mut tag = [0_u8; 1];
    if recv.read_exact(&mut tag).await.is_err() {
        return Ok(None);
    }
    match tag[0] {
        TAG_CHANNEL_DECLARED => {
            let len = read_varint(recv).await?;
            let bytes = read_sized(recv, len, "telemetry channel declaration").await?;
            let descriptor = postcard::from_bytes(&bytes)?;
            Ok(Some(vec![TelemetryEvent::ChannelDeclared(descriptor)]))
        }
        TAG_FRAME_BATCH => {
            let len = read_varint(recv).await?;
            let raw = read_sized(recv, len, "telemetry frame batch").await?;
            decode_frame_batch(&raw, stream).map(Some)
        }
        TAG_LZ4_FRAME_BATCH | TAG_ZSTD_FRAME_BATCH => {
            let raw_len = checked_record_len(read_varint(recv).await?, "telemetry frame batch")?;
            let compressed_len = read_varint(recv).await?;
            let compressed =
                read_sized(recv, compressed_len, "compressed telemetry frame batch").await?;
            let raw = if tag[0] == TAG_LZ4_FRAME_BATCH {
                lz4_flex::block::decompress(&compressed, raw_len).map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid LZ4 telemetry frame batch: {error}"),
                    )
                })?
            } else {
                decompress_zstd_batch(&compressed, raw_len)?
            };
            decode_frame_batch(&raw, stream).map(Some)
        }
        TAG_STREAM_ENDED => Ok(Some(vec![TelemetryEvent::StreamEnded(
            stream.stream.clone(),
        )])),
        _ => Err("unknown telemetry QUIC record tag".into()),
    }
}

/// Decode complete compact event batches from one subscription stream.
pub fn decode_event_records(
    bytes: &[u8],
    stream: &StreamDescriptor,
) -> Result<Vec<TelemetryEvent>, BoxError> {
    let mut cursor = RecordCursor::new(bytes);
    let mut events = Vec::new();
    while !cursor.is_empty() {
        match cursor.take_u8()? {
            TAG_CHANNEL_DECLARED => {
                let len = cursor.take_record_len("telemetry channel declaration")?;
                let descriptor: ChannelDescriptor = postcard::from_bytes(cursor.take(len)?)?;
                events.push(TelemetryEvent::ChannelDeclared(descriptor));
            }
            TAG_FRAME_BATCH => {
                let len = cursor.take_record_len("telemetry frame batch")?;
                events.extend(decode_frame_batch(cursor.take(len)?, stream)?);
            }
            tag @ (TAG_LZ4_FRAME_BATCH | TAG_ZSTD_FRAME_BATCH) => {
                let raw_len = cursor.take_record_len("telemetry frame batch")?;
                let compressed_len = cursor.take_record_len("compressed telemetry frame batch")?;
                let compressed = cursor.take(compressed_len)?;
                let raw = if tag == TAG_LZ4_FRAME_BATCH {
                    lz4_flex::block::decompress(compressed, raw_len).map_err(|error| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid LZ4 telemetry frame batch: {error}"),
                        )
                    })?
                } else {
                    decompress_zstd_batch(compressed, raw_len)?
                };
                events.extend(decode_frame_batch(&raw, stream)?);
            }
            TAG_STREAM_ENDED => {
                events.push(TelemetryEvent::StreamEnded(stream.stream.clone()));
            }
            _ => return Err("unknown telemetry QUIC record tag".into()),
        }
    }
    Ok(events)
}

fn decompress_zstd_batch(compressed: &[u8], raw_len: usize) -> Result<Vec<u8>, BoxError> {
    let raw = zstd::bulk::decompress(compressed, raw_len).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid zstd telemetry frame batch: {error}"),
        )
    })?;
    if raw.len() != raw_len {
        return Err("zstd telemetry frame batch length mismatch".into());
    }
    Ok(raw)
}

fn decode_frame_batch(
    bytes: &[u8],
    stream: &StreamDescriptor,
) -> Result<Vec<TelemetryEvent>, BoxError> {
    let mut cursor = RecordCursor::new(bytes);
    let mut events = Vec::new();
    let mut previous_position: Option<u64> = None;
    while !cursor.is_empty() {
        let channel =
            u32::try_from(cursor.take_varint()?).map_err(|_| "telemetry channel id exceeds u32")?;
        let encoded_position = cursor.take_varint()?;
        let position = match previous_position {
            Some(previous) => previous
                .checked_add(encoded_position)
                .ok_or("telemetry frame position overflow")?,
            None => encoded_position,
        };
        let payload_len = cursor.take_record_len("telemetry frame payload")?;
        let payload = cursor.take(payload_len)?.to_vec();
        events.push(TelemetryEvent::Frame(FrameDelivery {
            channel: ChannelRef {
                stream: stream.stream.clone(),
                channel: ChannelId(channel),
            },
            position: Position(position),
            payload,
        }));
        previous_position = Some(position);
    }
    Ok(events)
}

pub async fn read_stream_into_fanout(
    recv: RecvStream,
    fanout: Arc<telemetry::DeliveryFanout>,
) -> Result<TelemetryQuicHeader, BoxError> {
    let read = read_events_from_stream(recv).await?;
    fanout.publish_batch(read.events);
    Ok(read.header)
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn varint_size(value: u64) -> usize {
    ((u64::BITS - value.leading_zeros()).max(1) as usize).div_ceil(7)
}

fn checked_record_len(value: u64, label: &'static str) -> Result<usize, BoxError> {
    let len = usize::try_from(value).map_err(|_| "telemetry record length exceeds usize")?;
    if len > MAX_RECORD_BYTES {
        return Err(format!("{label} exceeds max size").into());
    }
    Ok(len)
}

async fn read_varint(recv: &mut RecvStream) -> Result<u64, BoxError> {
    let mut value = 0_u64;
    for shift in (0..u64::BITS).step_by(7) {
        let mut byte = [0_u8; 1];
        recv.read_exact(&mut byte).await?;
        if shift == 63 && byte[0] & 0x7e != 0 {
            return Err("telemetry varint exceeds u64".into());
        }
        value |= u64::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err("telemetry varint exceeds u64".into())
}

async fn read_sized(
    recv: &mut RecvStream,
    len: u64,
    label: &'static str,
) -> Result<Vec<u8>, BoxError> {
    let len = checked_record_len(len, label)?;
    let mut bytes = vec![0_u8; len];
    recv.read_exact(&mut bytes).await?;
    Ok(bytes)
}

struct RecordCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> RecordCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take_u8(&mut self) -> Result<u8, BoxError> {
        Ok(self.take(1)?[0])
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BoxError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or("telemetry record length overflow")?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or("telemetry record truncated")?;
        self.position = end;
        Ok(bytes)
    }

    fn take_varint(&mut self) -> Result<u64, BoxError> {
        let mut value = 0_u64;
        for shift in (0..u64::BITS).step_by(7) {
            let byte = self.take_u8()?;
            if shift == 63 && byte & 0x7e != 0 {
                return Err("telemetry varint exceeds u64".into());
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err("telemetry varint exceeds u64".into())
    }

    fn take_record_len(&mut self, label: &'static str) -> Result<usize, BoxError> {
        checked_record_len(self.take_varint()?, label)
    }
}

async fn write_postcard<T: serde::Serialize>(
    send: &mut SendStream,
    value: &T,
) -> Result<(), BoxError> {
    let bytes = postcard::to_allocvec(value)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err("telemetry header exceeds max size".into());
    }
    let mut len = Vec::with_capacity(10);
    put_varint(&mut len, bytes.len() as u64);
    send.write_all(&len).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

async fn read_postcard<T: serde::de::DeserializeOwned>(
    recv: &mut RecvStream,
) -> Result<T, BoxError> {
    let len = read_varint(recv).await?;
    let bytes = read_sized(recv, len, "telemetry header").await?;
    Ok(postcard::from_bytes(&bytes)?)
}

async fn write_json<T: serde::Serialize>(send: &mut SendStream, value: &T) -> Result<(), BoxError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > u32::MAX as usize {
        return Err("telemetry header JSON exceeds u32 length prefix".into());
    }
    send.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

async fn read_json<T: serde::de::DeserializeOwned>(recv: &mut RecvStream) -> Result<T, BoxError> {
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_RECORD_BYTES {
        return Err("telemetry QUIC record exceeds max size".into());
    }
    let mut bytes = vec![0u8; len];
    recv.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
