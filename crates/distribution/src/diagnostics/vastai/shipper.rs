//! Standalone transport for the vastai monitoring layer.
//!
//! This deliberately does **not** reuse [`HttpSink`](crate::diagnostics::HttpSink):
//! that sink is typed to the swactor `EventRecord` end to end. The vastai layer
//! owns its full transport so the two layers stay independent — vastai monitoring
//! works with swactor diagnostics off, and vice versa. The duplicated HTTP/retry
//! plumbing here is the accepted cost of that independence.
//!
//! Shape: the typed `instance`/`sample`/`logs`/`lifecycle` methods serialize a
//! [`VastaiRecord`] and drop it on an unbounded channel, returning immediately. A
//! background drainer POSTs each record to the collector under its matching
//! `vastai_*` `RecordKind`, spooling to disk on failure and draining the spool on
//! recovery — the same resilience the swactor sink has, kept separate.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::diagnostics::collector::protocol::{PostAck, RecordKind};
use crate::diagnostics::collector::wall_ms_now;
use crate::diagnostics::spool::Spool;

use super::record::{
    HostSample, InstanceObservation, LifecycleEvent, LogBatch, Source, VastaiBody, VastaiNodeRef,
    VastaiRecord, VASTAI_SCHEMA_VERSION,
};

/// Configuration for a [`VastaiShipper`]. Independent of `SinkConfig` so the
/// vastai layer can be enabled/tuned on its own.
#[derive(Debug, Clone)]
pub struct VastaiShipperConfig {
    /// Base URL of the collector, `http://host[:port]` (no TLS — same constraint
    /// as the swactor sink; the collector is an inside-VPC service).
    pub collector_url: String,
    pub run_id: String,
    /// Synthetic collector node id for this producer — e.g. `vastai-external`
    /// (orchestrator) or `vastai-stage-{i}` (container). Chosen so vastai records
    /// never collide with the swactor node's 64-hex directory.
    pub node_id: String,
    /// Spool root; failed POSTs land under `{spool_dir}/{run_id}/`.
    pub spool_dir: PathBuf,
    /// How often the drainer retries the spool when idle.
    pub drain_interval: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub request_timeout: Duration,
}

impl VastaiShipperConfig {
    pub fn new(
        collector_url: impl Into<String>,
        run_id: impl Into<String>,
        node_id: impl Into<String>,
        spool_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            collector_url: collector_url.into(),
            run_id: run_id.into(),
            node_id: node_id.into(),
            spool_dir: spool_dir.into(),
            drain_interval: Duration::from_secs(2),
            retry_initial: Duration::from_millis(250),
            retry_max: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_drain_interval(mut self, d: Duration) -> Self {
        self.drain_interval = d;
        self
    }

    pub fn with_request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }
}

/// Out-of-band handle for graceful shutdown and test inspection. Cheap to clone.
#[derive(Clone)]
pub struct VastaiShipperHandle {
    spool_dir: PathBuf,
    run_id: String,
    delivered: Arc<AtomicU64>,
    shutdown_tx: Arc<tokio::sync::Mutex<Option<mpsc::Sender<tokio::sync::oneshot::Sender<()>>>>>,
}

impl VastaiShipperHandle {
    /// Directory under which spool files for this run live.
    pub fn spool_run_dir(&self) -> PathBuf {
        self.spool_dir.join(&self.run_id)
    }

    /// Number of records the drainer has successfully delivered to the collector.
    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Flush pending records and drain the spool once, then stop the drainer.
    pub async fn shutdown(&self) {
        let mut guard = self.shutdown_tx.lock().await;
        let Some(tx) = guard.take() else { return };
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(done_tx).await;
        let _ = done_rx.await;
    }
}

/// The vastai telemetry shipper. Construct inside a tokio runtime (spawns a
/// background drainer). Cheap to clone — clones share the same drainer.
#[derive(Clone)]
pub struct VastaiShipper {
    tx: mpsc::UnboundedSender<Cmd>,
    handle: VastaiShipperHandle,
    node: VastaiNodeRef,
    source: Source,
    seq: Arc<AtomicU64>,
}

impl VastaiShipper {
    /// Build the shipper and spawn its drainer. `node`/`source` are stamped onto
    /// every record this shipper emits.
    pub fn spawn(
        config: VastaiShipperConfig,
        node: VastaiNodeRef,
        source: Source,
    ) -> std::io::Result<Self> {
        let endpoint = Endpoint::parse(&config.collector_url).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid collector_url {:?}: {e}", config.collector_url),
            )
        })?;
        let spool = Spool::open_sync(&config.spool_dir, &config.run_id)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = VastaiShipperHandle {
            spool_dir: config.spool_dir.clone(),
            run_id: config.run_id.clone(),
            delivered: Arc::clone(&delivered),
            shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
        };
        let drainer = Drainer {
            config,
            endpoint,
            spool,
            rx,
            shutdown_rx,
            delivered,
        };
        tokio::spawn(drainer.run());
        Ok(Self {
            tx,
            handle,
            node,
            source,
            seq: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn handle(&self) -> VastaiShipperHandle {
        self.handle.clone()
    }

    /// Override the node reference for records emitted through a clone — used when
    /// one orchestrator-side shipper reports many contracts and wants to stamp the
    /// contract under observation per record.
    pub fn with_node(&self, node: VastaiNodeRef) -> Self {
        let mut c = self.clone();
        c.node = node;
        c
    }

    fn enqueue(&self, body: VastaiBody) {
        let rec = VastaiRecord {
            v: VASTAI_SCHEMA_VERSION,
            vastai_node: self.node.clone(),
            source: self.source,
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            wall_ms: wall_ms_now(),
            body,
        };
        let kind = rec.kind();
        let bytes = match serde_json::to_vec(&rec) {
            Ok(b) => b,
            Err(_) => return,
        };
        let _ = self.tx.send(Cmd::Record { kind, body: bytes });
    }

    /// Ship a full external instance observation.
    pub fn instance(&self, obs: InstanceObservation) {
        self.enqueue(VastaiBody::Instance(obs));
    }

    /// Ship one in-VM host metrics sample.
    pub fn sample(&self, sample: HostSample) {
        self.enqueue(VastaiBody::HostSample(sample));
    }

    /// Ship a batch of log lines.
    pub fn logs(&self, batch: LogBatch) {
        self.enqueue(VastaiBody::Logs(batch));
    }

    /// Ship a lifecycle marker.
    pub fn lifecycle(&self, event: LifecycleEvent) {
        self.enqueue(VastaiBody::Lifecycle(event));
    }
}

enum Cmd {
    Record { kind: RecordKind, body: Vec<u8> },
}

struct Drainer {
    config: VastaiShipperConfig,
    endpoint: Endpoint,
    spool: Spool,
    rx: mpsc::UnboundedReceiver<Cmd>,
    shutdown_rx: mpsc::Receiver<tokio::sync::oneshot::Sender<()>>,
    delivered: Arc<AtomicU64>,
}

impl Drainer {
    async fn run(mut self) {
        let mut backoff = self.config.retry_initial;
        let mut interval = tokio::time::interval(self.config.drain_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await; // consume the immediate first tick

        let mut shutdown_ack: Option<tokio::sync::oneshot::Sender<()>> = None;
        loop {
            tokio::select! {
                biased;
                ack = self.shutdown_rx.recv() => {
                    shutdown_ack = ack;
                    break;
                }
                msg = self.rx.recv() => {
                    match msg {
                        Some(Cmd::Record { kind, body }) => {
                            self.send_one(kind, &body, &mut backoff).await;
                        }
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    self.drain_spool(&mut backoff).await;
                }
            }
        }

        // Drain anything still queued before exiting so records emitted moments
        // before shutdown (e.g. a teardown marker) are not dropped.
        while let Ok(Cmd::Record { kind, body }) = self.rx.try_recv() {
            self.send_one(kind, &body, &mut backoff).await;
        }
        self.drain_spool(&mut backoff).await;
        if let Some(ack) = shutdown_ack {
            let _ = ack.send(());
        }
    }

    async fn send_one(&mut self, kind: RecordKind, body: &[u8], backoff: &mut Duration) {
        match self.post_with_timeout(kind, body).await {
            Ok(_) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
                *backoff = self.config.retry_initial;
                self.drain_spool(backoff).await;
            }
            Err(_) => {
                let _ = self.spool.append(kind, body).await;
                let doubled = backoff.saturating_mul(2);
                *backoff = doubled.min(self.config.retry_max);
            }
        }
    }

    /// Push every spooled record up in FIFO order; stop at the first failure to
    /// preserve ordering.
    async fn drain_spool(&mut self, backoff: &mut Duration) {
        let entries = match self.spool.list().await {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries {
            let body = match tokio::fs::read(&entry.path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            match self.post_with_timeout(entry.kind, &body).await {
                Ok(_) => {
                    self.delivered.fetch_add(1, Ordering::Relaxed);
                    *backoff = self.config.retry_initial;
                    let _ = tokio::fs::remove_file(&entry.path).await;
                }
                Err(_) => {
                    let doubled = backoff.saturating_mul(2);
                    *backoff = doubled.min(self.config.retry_max);
                    return;
                }
            }
        }
    }

    async fn post_with_timeout(
        &self,
        kind: RecordKind,
        body: &[u8],
    ) -> Result<PostAck<Value>, PostError> {
        let fut = post(&self.endpoint, &self.config, kind, body);
        match tokio::time::timeout(self.config.request_timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(PostError::Timeout),
        }
    }
}

/// Parsed `http://host[:port][/base]`. http-only by design (collector is an
/// inside-VPC service without TLS termination).
#[derive(Debug, Clone)]
struct Endpoint {
    host: String,
    port: u16,
    base: String,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| "must start with http://".to_string())?;
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return Err("missing host".into());
        }
        let (host, port) = match authority.rfind(':') {
            Some(i) => {
                let port: u16 = authority[i + 1..]
                    .parse()
                    .map_err(|e| format!("invalid port: {e}"))?;
                (authority[..i].to_string(), port)
            }
            None => (authority.to_string(), 80u16),
        };
        Ok(Self {
            host,
            port,
            base: path.trim_end_matches('/').to_string(),
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)] // read via Debug in log lines
enum PostError {
    Connect(std::io::Error),
    Io(std::io::Error),
    Status(u16),
    BadResponse,
    Timeout,
}

async fn post(
    endpoint: &Endpoint,
    config: &VastaiShipperConfig,
    kind: RecordKind,
    body: &[u8],
) -> Result<PostAck<Value>, PostError> {
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(PostError::Connect)?;
    let send_ms = wall_ms_now();
    let path = format!("{}/diag/{}", endpoint.base, kind.as_str());
    let mut header = String::with_capacity(256);
    use std::fmt::Write as _;
    let _ = write!(
        header,
        "POST {path} HTTP/1.1\r\n\
         host: {host}:{port}\r\n\
         connection: close\r\n\
         content-type: application/json\r\n\
         content-length: {len}\r\n\
         x-run-id: {run}\r\n\
         x-node-id: {node}\r\n\
         x-node-send-ms: {send_ms}\r\n\
         \r\n",
        host = endpoint.host,
        port = endpoint.port,
        len = body.len(),
        run = config.run_id,
        node = config.node_id,
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(PostError::Io)?;
    stream.write_all(body).await.map_err(PostError::Io)?;
    stream.flush().await.ok();

    let mut buf = Vec::with_capacity(512);
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(PostError::Io)?;
    parse_response(&buf)
}

fn parse_response(bytes: &[u8]) -> Result<PostAck<Value>, PostError> {
    let split = find_double_crlf(bytes).ok_or(PostError::BadResponse)?;
    let head = std::str::from_utf8(&bytes[..split]).map_err(|_| PostError::BadResponse)?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().ok_or(PostError::BadResponse)?;
    let mut parts = status_line.split_whitespace();
    let _proto = parts.next();
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or(PostError::BadResponse)?;
    if !(200..300).contains(&status) {
        return Err(PostError::Status(status));
    }
    let body = &bytes[split + 4..];
    serde_json::from_slice(body).map_err(|_| PostError::BadResponse)
}

fn find_double_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|w| w == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_parses_host_port_and_base() {
        let e = Endpoint::parse("http://127.0.0.1:9080/api/").unwrap();
        assert_eq!(e.host, "127.0.0.1");
        assert_eq!(e.port, 9080);
        assert_eq!(e.base, "/api");
    }

    #[test]
    fn endpoint_rejects_non_http() {
        assert!(Endpoint::parse("https://x").is_err());
        assert!(Endpoint::parse("x").is_err());
    }
}
