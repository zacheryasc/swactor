//! Iroh/QUIC transport adapter for telemetry subscriptions.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::TryRecvError;
use telemetry::{TelemetrySnapshot, TelemetrySubscription};
use telemetry::frame::{ChannelDescriptor, ChannelId, ChannelRef, TelemetryEvent, FrameDelivery, Position,
StreamDescriptor,};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
use swactor_engine::EngineHandle;

pub const TELEMETRY_ALPN: &[u8] = b"swactor/telemetry/0";

const MAGIC: &[u8; 4] = b"DSQ1";
const TAG_CHANNEL_DECLARED: u8 = 0x01;
const TAG_FRAME: u8 = 0x02;
const TAG_STREAM_ENDED: u8 = 0x03;
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
        let _ = write_subscription_until_closed(&engine_handle, send, header, subscription, idle_sleep).await;
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
    loop {
        match subscription.try_recv() {
            Ok(event) => {
                let bytes = write_event(&mut send, &event).await?;
                if bytes > 0 {
                    stats.bytes += bytes;
                    stats.events += 1;
                }
            }
            Err(TryRecvError::Empty) => {
                engine.timer(idle_sleep).await;
            }
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
    loop {
        match subscription.try_recv() {
            Ok(event) => {
                let bytes = write_event(&mut send, &event).await?;
                if bytes > 0 {
                    stats.bytes += bytes;
                    stats.events += 1;
                }
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

pub async fn write_event(
    send: &mut SendStream,
    event: &TelemetryEvent,
) -> Result<usize, BoxError> {
    let mut bytes = Vec::new();
    match event {
        TelemetryEvent::StreamDeclared(_) => return Ok(0),
        TelemetryEvent::ChannelDeclared(descriptor) => {
            bytes.push(TAG_CHANNEL_DECLARED);
            put_json(&mut bytes, descriptor)?;
        }
        TelemetryEvent::Frame(delivery) => {
            bytes.push(TAG_FRAME);
            bytes.extend_from_slice(&delivery.channel.channel.0.to_le_bytes());
            bytes.extend_from_slice(&delivery.position.0.to_le_bytes());
            put_bytes(&mut bytes, &delivery.payload)?;
        }
        TelemetryEvent::StreamEnded(_) => {
            bytes.push(TAG_STREAM_ENDED);
        }
    }
    if bytes.len() > MAX_RECORD_BYTES {
        return Err("telemetry QUIC record exceeds max size".into());
    }
    send.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(4 + bytes.len())
}

pub async fn read_stream_header(recv: &mut RecvStream) -> Result<TelemetryQuicHeader, BoxError> {
    read_header(recv).await
}

pub async fn read_events_from_stream(mut recv: RecvStream) -> Result<TelemetryQuicRead, BoxError> {
    let header = read_header(&mut recv).await?;
    let mut events = Vec::new();
    loop {
        match read_next_event(&mut recv, &header.stream).await? {
            Some(event) => events.push(event),
            None => break,
        }
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

async fn write_header(
    send: &mut SendStream,
    header: &TelemetryQuicHeader,
) -> Result<(), BoxError> {
    if header.token.len() > u16::MAX as usize {
        return Err("telemetry token exceeds u16 length prefix".into());
    }
    send.write_all(MAGIC).await?;
    send.write_all(&header.flow_id).await?;
    send.write_all(&(header.token.len() as u16).to_le_bytes())
        .await?;
    send.write_all(&header.token).await?;
    write_json(send, &header.stream).await?;
    write_json(send, &header.channels).await?;
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
    let stream = read_json(recv).await?;
    let channels = read_json(recv).await?;
    Ok(TelemetryQuicHeader {
        flow_id,
        token,
        stream,
        channels,
    })
}

pub async fn read_next_event(
    recv: &mut RecvStream,
    stream: &StreamDescriptor,
) -> Result<Option<TelemetryEvent>, BoxError> {
    let mut len = [0u8; 4];
    if recv.read_exact(&mut len).await.is_err() {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_RECORD_BYTES {
        return Err("telemetry QUIC record exceeds max size".into());
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    decode_record(&buf, stream).map(Some)
}

fn decode_record(buf: &[u8], stream: &StreamDescriptor) -> Result<TelemetryEvent, BoxError> {
    if buf.is_empty() {
        return Err("empty telemetry QUIC record".into());
    }
    match buf[0] {
        TAG_CHANNEL_DECLARED => {
            let descriptor: ChannelDescriptor = serde_json::from_slice(&buf[1..])?;
            Ok(TelemetryEvent::ChannelDeclared(descriptor))
        }
        TAG_FRAME => {
            if buf.len() < 1 + 4 + 8 + 4 {
                return Err("telemetry QUIC frame record truncated".into());
            }
            let channel = ChannelId(u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]));
            let position = Position(u64::from_le_bytes([
                buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12],
            ]));
            let mut len = [0u8; 4];
            len.copy_from_slice(&buf[13..17]);
            let payload_len = u32::from_le_bytes(len) as usize;
            let payload = buf
                .get(17..17 + payload_len)
                .ok_or("telemetry QUIC frame payload truncated")?;
            if 17 + payload_len != buf.len() {
                return Err("bytes remain after telemetry QUIC frame record".into());
            }
            Ok(TelemetryEvent::Frame(FrameDelivery {
                channel: ChannelRef {
                    stream: stream.stream.clone(),
                    channel,
                },
                position,
                payload: payload.to_vec(),
            }))
        }
        TAG_STREAM_ENDED => Ok(TelemetryEvent::StreamEnded(stream.stream.clone())),
        _ => Err("unknown telemetry QUIC record tag".into()),
    }
}

pub async fn read_stream_into_fanout(
    recv: RecvStream,
    fanout: Arc<telemetry::DeliveryFanout>,
) -> Result<TelemetryQuicHeader, BoxError> {
    let read = read_events_from_stream(recv).await?;
    fanout.publish_batch(read.events);
    Ok(read.header)
}

fn put_json<T: serde::Serialize>(out: &mut Vec<u8>, value: &T) -> Result<(), BoxError> {
    out.extend_from_slice(&serde_json::to_vec(value)?);
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), BoxError> {
    if bytes.len() > u32::MAX as usize {
        return Err("telemetry delivery exceeds u32 length prefix".into());
    }
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
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
