//! Shared iroh transport infrastructure for actor-level QUIC messaging.
//!
//! Provides `IrohActorTransport` (sends `WireEnvelope`s over iroh QUIC
//! uni-directional streams), wire encoding/decoding, and a drain helper
//! that feeds incoming messages into a swactor `Runtime`.

use std::time::Duration;

use distribution::iroh_driver::IrohDriver;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_transport::{CodecRegistry, Transport, WireEnvelope};
use swactor::Error;

/// ALPN protocol for actor-level messages (distinct from SWIM protocol).
pub const ACTOR_ALPN: &[u8] = b"swactor/actor/1";

/// Sends WireEnvelopes over iroh QUIC uni-directional streams.
///
/// Caches the QUIC connection so it stays alive between sends — dropping
/// a connection before the receiver reads its streams causes "closed by
/// peer" errors.
///
/// Wire format: [32B dest_addr][4B tag_len BE][tag bytes][payload bytes]
pub struct IrohActorTransport {
    endpoint: iroh::Endpoint,
    target_addr: iroh::EndpointAddr,
    handle: tokio::runtime::Handle,
    conn: std::sync::Mutex<Option<iroh::endpoint::Connection>>,
}

impl IrohActorTransport {
    pub fn new(
        endpoint: iroh::Endpoint,
        target_addr: iroh::EndpointAddr,
        handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            endpoint,
            target_addr,
            handle,
            conn: std::sync::Mutex::new(None),
        }
    }
}

impl Transport for IrohActorTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        let data = encode_wire(&envelope);
        let ep = self.endpoint.clone();
        let target = self.target_addr.clone();
        let cached = self.conn.lock().unwrap().clone();

        let conn = self.handle.block_on(async move {
            if let Some(c) = cached {
                if c.close_reason().is_none() {
                    return Ok(c);
                }
            }
            ep.connect(target, ACTOR_ALPN)
                .await
                .map_err(|e| Error::from(format!("iroh connect: {e}")))
        })?;

        *self.conn.lock().unwrap() = Some(conn.clone());

        self.handle.block_on(async move {
            let mut stream = conn
                .open_uni()
                .await
                .map_err(|e| Error::from(format!("iroh open_uni: {e}")))?;
            stream
                .write_all(&data)
                .await
                .map_err(|e| Error::from(format!("iroh write: {e}")))?;
            stream
                .finish()
                .map_err(|e| Error::from(format!("iroh finish: {e}")))?;
            Ok(())
        })
    }
}

pub fn encode_wire(env: &WireEnvelope) -> Vec<u8> {
    let tag = env.type_tag.as_bytes();
    let mut buf = Vec::with_capacity(32 + 4 + tag.len() + env.payload.len());
    buf.extend_from_slice(&env.dest.0);
    buf.extend_from_slice(&(tag.len() as u32).to_be_bytes());
    buf.extend_from_slice(tag);
    buf.extend_from_slice(&env.payload);
    buf
}

pub fn decode_wire(data: &[u8]) -> Option<WireEnvelope> {
    if data.len() < 36 {
        return None;
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&data[..32]);
    let tag_len = u32::from_be_bytes(data[32..36].try_into().ok()?) as usize;
    if data.len() < 36 + tag_len {
        return None;
    }
    let type_tag = String::from_utf8(data[36..36 + tag_len].to_vec()).ok()?;
    let payload = data[36 + tag_len..].to_vec();
    Some(WireEnvelope {
        dest: ActorAddress(addr),
        type_tag,
        payload,
    })
}

/// Drain actor messages arriving via iroh into a swactor runtime.
///
/// Spawns tokio tasks (on the driver's runtime) to read uni-directional
/// streams from pending actor-ALPN connections. Results are collected
/// via a channel and delivered to the swactor runtime.
///
/// `drain_sleep` controls how long to wait for spawned tasks to read
/// streams before collecting results. Use shorter durations (e.g. 100ms)
/// for localhost tests, longer (e.g. 500ms) for cross-node scenarios.
pub fn drain_actor_messages(
    driver: &IrohDriver,
    codecs: &CodecRegistry,
    rt: &Runtime,
    drain_sleep: Duration,
) {
    let conns = driver.drain_other_connections();
    if conns.is_empty() {
        return;
    }
    let handle = driver.tokio_handle();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();

    for (_node_id, conn) in conns {
        let tx = tx.clone();
        handle.spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_secs(1), conn.accept_uni()).await {
                    Ok(Ok(mut recv)) => {
                        if let Ok(data) = recv.read_to_end(256 * 1024).await {
                            let _ = tx.send(data);
                        }
                    }
                    _ => break,
                }
            }
        });
    }
    drop(tx);

    std::thread::sleep(drain_sleep);

    for data in rx.try_iter() {
        if let Some(envelope) = decode_wire(&data) {
            if let Ok((addr, msg)) = codecs.receive(envelope) {
                let _ = rt.deliver_raw(addr, msg);
            }
        }
    }
}
