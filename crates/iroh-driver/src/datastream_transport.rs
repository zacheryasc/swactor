//! Iroh/QUIC transport adapter for datastream subscriptions.
//!
//! Swactor actors negotiate whether a subscription should exist; this module is
//! the byte plane. It uses Iroh's endpoint/connection machinery and an ALPN
//! separate from actor traffic, so NAT traversal and relay fallback stay owned
//! by Iroh while datastream frames avoid per-frame actor messages.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use datastream::DatastreamSubscription;
use datastream::transport::Delivery;
use datastream::wire::{decode_delivery, encode_delivery};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
use tokio::runtime::Handle;

pub const DATASTREAM_ALPN: &[u8] = b"swactor/datastream/0";

const MAGIC: &[u8; 4] = b"DSQ0";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastreamQuicHeader {
    pub flow_id: [u8; 16],
    pub token: Vec<u8>,
}

impl DatastreamQuicHeader {
    pub fn new(flow_id: [u8; 16], token: impl Into<Vec<u8>>) -> Self {
        Self {
            flow_id,
            token: token.into(),
        }
    }
}

impl Default for DatastreamQuicHeader {
    fn default() -> Self {
        Self {
            flow_id: [0; 16],
            token: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DatastreamQuicWriteStats {
    pub deliveries: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastreamQuicRead {
    pub header: DatastreamQuicHeader,
    pub deliveries: Vec<Delivery>,
}

/// Connect to `peer` with [`DATASTREAM_ALPN`] and stream a subscription until
/// the endpoint side is dropped.
pub fn spawn_subscription_writer(
    handle: &Handle,
    endpoint: Endpoint,
    peer: EndpointAddr,
    header: DatastreamQuicHeader,
    subscription: DatastreamSubscription,
    idle_sleep: Duration,
) -> tokio::task::JoinHandle<Result<DatastreamQuicWriteStats, String>> {
    handle.spawn(async move {
        let conn = endpoint
            .connect(peer, DATASTREAM_ALPN)
            .await
            .map_err(|error| error.to_string())?;
        let send = conn.open_uni().await.map_err(|error| error.to_string())?;
        write_subscription_until_closed(send, header, subscription, idle_sleep)
            .await
            .map_err(|error| error.to_string())
    })
}

/// Drain whatever is currently queued for `subscription`, write it, and finish
/// the QUIC stream. This is useful for deterministic tests and one-shot tools.
pub async fn write_available_subscription(
    send: SendStream,
    header: &DatastreamQuicHeader,
    subscription: &DatastreamSubscription,
) -> Result<DatastreamQuicWriteStats, BoxError> {
    write_subscription_inner(send, header, subscription, None).await
}

/// Write subscription deliveries until the sender side disappears.
pub async fn write_subscription_until_closed(
    mut send: SendStream,
    header: DatastreamQuicHeader,
    subscription: DatastreamSubscription,
    idle_sleep: Duration,
) -> Result<DatastreamQuicWriteStats, BoxError> {
    write_header(&mut send, &header).await?;
    let mut stats = DatastreamQuicWriteStats::default();
    loop {
        match subscription.try_recv() {
            Ok(delivery) => {
                stats.bytes += write_delivery(&mut send, &delivery).await?;
                stats.deliveries += 1;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                tokio::time::sleep(idle_sleep).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }
    send.finish()?;
    Ok(stats)
}

async fn write_subscription_inner(
    mut send: SendStream,
    header: &DatastreamQuicHeader,
    subscription: &DatastreamSubscription,
    idle_sleep: Option<Duration>,
) -> Result<DatastreamQuicWriteStats, BoxError> {
    write_header(&mut send, header).await?;
    let mut stats = DatastreamQuicWriteStats::default();
    loop {
        match subscription.try_recv() {
            Ok(delivery) => {
                stats.bytes += write_delivery(&mut send, &delivery).await?;
                stats.deliveries += 1;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => match idle_sleep {
                Some(delay) => tokio::time::sleep(delay).await,
                None => break,
            },
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }
    send.finish()?;
    Ok(stats)
}

pub async fn write_delivery(send: &mut SendStream, delivery: &Delivery) -> Result<usize, BoxError> {
    let bytes = encode_delivery(&delivery.stream, &delivery.frame);
    if bytes.len() > u32::MAX as usize {
        return Err("datastream delivery exceeds u32 length prefix".into());
    }
    send.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(4 + bytes.len())
}

/// Read one unidirectional datastream QUIC stream to completion.
pub async fn read_deliveries_from_stream(
    mut recv: RecvStream,
) -> Result<DatastreamQuicRead, BoxError> {
    let header = read_header(&mut recv).await?;
    let mut deliveries = Vec::new();
    loop {
        match read_next_delivery(&mut recv).await? {
            Some(delivery) => deliveries.push(delivery),
            None => break,
        }
    }
    Ok(DatastreamQuicRead { header, deliveries })
}

/// Read the next accepted unidirectional stream from a datastream connection.
pub async fn read_next_uni_from_connection(
    conn: &Connection,
) -> Result<DatastreamQuicRead, BoxError> {
    let recv = conn.accept_uni().await?;
    read_deliveries_from_stream(recv).await
}

/// Spawn readers for every unidirectional stream on an accepted datastream
/// connection, forwarding decoded deliveries to `sink`.
pub fn spawn_connection_reader(
    handle: &Handle,
    conn: Connection,
    sink: std::sync::mpsc::Sender<Delivery>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    handle.spawn(async move {
        loop {
            let recv = match conn.accept_uni().await {
                Ok(recv) => recv,
                Err(error) => return Err(error.to_string()),
            };
            let read = read_deliveries_from_stream(recv)
                .await
                .map_err(|error| error.to_string())?;
            for delivery in read.deliveries {
                if sink.send(delivery).is_err() {
                    return Ok(());
                }
            }
        }
    })
}

async fn write_header(
    send: &mut SendStream,
    header: &DatastreamQuicHeader,
) -> Result<(), BoxError> {
    if header.token.len() > u16::MAX as usize {
        return Err("datastream token exceeds u16 length prefix".into());
    }
    send.write_all(MAGIC).await?;
    send.write_all(&header.flow_id).await?;
    send.write_all(&(header.token.len() as u16).to_le_bytes())
        .await?;
    send.write_all(&header.token).await?;
    Ok(())
}

async fn read_header(recv: &mut RecvStream) -> Result<DatastreamQuicHeader, BoxError> {
    let mut magic = [0u8; 4];
    recv.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err("invalid datastream QUIC magic".into());
    }
    let mut flow_id = [0u8; 16];
    recv.read_exact(&mut flow_id).await?;
    let mut token_len = [0u8; 2];
    recv.read_exact(&mut token_len).await?;
    let token_len = u16::from_le_bytes(token_len) as usize;
    let mut token = vec![0u8; token_len];
    recv.read_exact(&mut token).await?;
    Ok(DatastreamQuicHeader { flow_id, token })
}

async fn read_next_delivery(recv: &mut RecvStream) -> Result<Option<Delivery>, BoxError> {
    let mut len = [0u8; 4];
    if recv.read_exact(&mut len).await.is_err() {
        return Ok(None);
    }
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_RECORD_BYTES {
        return Err("datastream QUIC record exceeds max size".into());
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await?;
    let (stream, frame) = decode_delivery(&buf)?;
    Ok(Some(Delivery::new(stream, frame)))
}

/// Share an accepted delivery stream with several local consumers without
/// making those consumers know about Iroh.
pub async fn read_stream_into_fanout(
    recv: RecvStream,
    fanout: Arc<datastream::DeliveryFanout>,
) -> Result<DatastreamQuicHeader, BoxError> {
    let read = read_deliveries_from_stream(recv).await?;
    fanout.publish_batch(read.deliveries);
    Ok(read.header)
}
