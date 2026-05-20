//! Iroh actor transport for the pipeline-parallel example.
//!
//! Mirrors `single-gpu-inference::iroh_transport` so the pipeline-parallel
//! example can stand alone (binaries do not have to reach across the sibling
//! crate). `IrohActorTransport` sends `WireEnvelope`s over iroh QUIC
//! uni-directional streams, and `drain_actor_messages` feeds inbound
//! envelopes into a swactor `Runtime` via its codec registry.

use std::sync::mpsc;
use std::time::Duration;

use distribution::iroh_driver::IrohDriver;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor::transport::{CodecRegistry, Transport, WireEnvelope};
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

        // The actor transport shares a fate with the SWIM driver: on a
        // canary-relay WAN, a single iroh dial occasionally takes
        // multiple seconds and is the difference between a token round-
        // tripping or the pipeline silently stalling. Retry the dial a
        // few times before giving up; the cached connection short-
        // circuits when it's still open.
        let conn = self.handle.block_on(async move {
            if let Some(c) = cached {
                if c.close_reason().is_none() {
                    return Ok::<_, Error>(c);
                }
            }
            const ATTEMPTS: u32 = 3;
            const PER_ATTEMPT: Duration = Duration::from_secs(10);
            let mut last_err: Option<Error> = None;
            for attempt in 1..=ATTEMPTS {
                let target = target.clone();
                match tokio::time::timeout(PER_ATTEMPT, ep.connect(target, ACTOR_ALPN)).await {
                    Ok(Ok(c)) => return Ok(c),
                    Ok(Err(e)) => {
                        if attempt < ATTEMPTS {
                            eprintln!(
                                "iroh actor transport: connect attempt {attempt}/{ATTEMPTS} failed: {e}"
                            );
                        }
                        last_err = Some(Error::from(format!("iroh connect: {e}")));
                    }
                    Err(_) => {
                        if attempt < ATTEMPTS {
                            eprintln!(
                                "iroh actor transport: connect attempt {attempt}/{ATTEMPTS} timed out"
                            );
                        }
                        last_err = Some(Error::from("iroh connect: timeout"));
                    }
                }
                if attempt == 1 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                } else if attempt == 2 {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                }
            }
            Err(last_err.unwrap_or_else(|| Error::from("iroh connect: failed")))
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
///
/// Note: this drains a single bounded burst — every spawned task exits when
/// `accept_uni` is idle for one second, and the per-call channel is dropped
/// at function return. That is appropriate for short request/response tests
/// (single-GPU example, t_cluster tests). For long-lived flows that send
/// multiple streams over the same cached connection across many seconds,
/// use [`ActorMessagePump`] instead — it keeps one persistent task per
/// connection and never drops the channel.
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

/// Persistent actor-message pump: keeps one tokio task per accepted ALPN
/// connection running for the lifetime of the pump, reading uni-streams into
/// a shared channel that the caller drains every tick.
///
/// This exists because pipeline-parallel inference runs many round-trips over
/// a single cached QUIC connection across many seconds; the per-call drain
/// above gives each connection only a ~1s window before its task exits and
/// further streams are silently dropped. The pump's channel sender is owned
/// by the pump (never dropped between drains), so its background tasks can
/// keep delivering streams as long as the underlying connection stays open.
pub struct ActorMessagePump {
    tx: mpsc::Sender<Vec<u8>>,
    rx: mpsc::Receiver<Vec<u8>>,
}

impl ActorMessagePump {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Vec<u8>>();
        Self { tx, rx }
    }

    /// Accept any newly-arrived ALPN connections from the driver and spawn a
    /// persistent task per connection. Then deliver any messages already
    /// queued on this pump's channel into `rt` using `codecs`.
    pub fn pump(&self, driver: &IrohDriver, codecs: &CodecRegistry, rt: &Runtime) {
        let conns = driver.drain_other_connections();
        if !conns.is_empty() {
            let handle = driver.tokio_handle();
            for (_node_id, conn) in conns {
                let tx = self.tx.clone();
                handle.spawn(async move {
                    loop {
                        match conn.accept_uni().await {
                            Ok(mut recv) => match recv.read_to_end(256 * 1024).await {
                                Ok(data) => {
                                    if tx.send(data).is_err() {
                                        break;
                                    }
                                }
                                Err(_) => continue,
                            },
                            Err(_) => break,
                        }
                    }
                });
            }
        }
        while let Ok(data) = self.rx.try_recv() {
            if let Some(envelope) = decode_wire(&data) {
                if let Ok((addr, msg)) = codecs.receive(envelope) {
                    let _ = rt.deliver_raw(addr, msg);
                }
            }
        }
    }
}

impl Default for ActorMessagePump {
    fn default() -> Self {
        Self::new()
    }
}
