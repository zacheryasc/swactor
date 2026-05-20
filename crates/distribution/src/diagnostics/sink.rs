//! Sinks: where the aggregator hands off records and snapshots.
//!
//! [`Sink`] is the seam between the aggregator and "what to do with
//! the data." Production wires up [`HttpSink`] (feature `collector`)
//! which POSTs to the collector and spools to disk when the collector
//! is unreachable. Tests use [`InMemorySink`]. Default call sites in
//! existing distribution code take [`NoopSink`] so that adding the
//! diagnostics parameter does not perturb behavior.

use std::sync::{Arc, Mutex};

use crate::diagnostics::event::{Event, EventRecord};
use crate::diagnostics::identity::Identity;
use crate::diagnostics::snapshot::Snapshot;

/// Type alias for the trait-object form threaded through observed
/// crates. `Arc<dyn Sink>` is the canonical handle subsystems hold
/// (rather than a generic `S: Sink`) so constructor signatures stay
/// simple and call sites can swap implementations at runtime.
pub type DynSink = Arc<dyn Sink + Send + Sync + 'static>;

/// Anything that can receive records and snapshots from the
/// aggregator. Implementations must be cheap and non-blocking — back-
/// pressure in delivery must never block the actor runtime
/// (`DIAGNOSTICS_PLAN.md` A.1).
///
/// `boot` and `finalize` carry per-process metadata that doesn't fit
/// the event/snapshot streams. Both default to no-ops; only [`HttpSink`]
/// implements them.
pub trait Sink: Send + Sync {
    fn emit(&self, record: EventRecord);
    fn snapshot(&self, snap: Snapshot);
    fn boot(&self, _identity: &Identity) {}
    fn finalize(&self, _body: serde_json::Value) {}
}

/// The thin interface observed subsystems hold to publish diagnostic
/// events without knowing about sequencing, identity, or sinks.
///
/// Production wires this through [`Aggregator`](crate::diagnostics::Aggregator),
/// which assigns the monotonic sequence, stamps the wall clock, updates
/// reachability, and forwards an [`EventRecord`] to its [`Sink`]. The
/// emitter handle threaded through `SwimNode` / `IrohDriver` defaults
/// to [`NoopEmitter`] so call sites that do not opt in are unaffected.
pub trait EventEmitter: Send + Sync {
    fn emit_event(&self, event: Event);
}

/// Trait-object form held by instrumented subsystems. Cheap to clone.
pub type DynEmitter = Arc<dyn EventEmitter + Send + Sync + 'static>;

/// Drop every diagnostic event on the floor. The default for code that
/// has not opted in to diagnostics yet.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopEmitter;

impl EventEmitter for NoopEmitter {
    fn emit_event(&self, _event: Event) {}
}

/// Convenience: an Arc-wrapped [`NoopEmitter`] for use as a default
/// field value where a [`DynEmitter`] is expected.
pub fn noop_emitter() -> DynEmitter {
    Arc::new(NoopEmitter)
}

impl<E: EventEmitter + ?Sized> EventEmitter for Arc<E> {
    fn emit_event(&self, event: Event) {
        (**self).emit_event(event);
    }
}

/// Drop everything on the floor. The default for tests and for the
/// many existing call sites that have no aggregator wired in yet.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl Sink for NoopSink {
    fn emit(&self, _record: EventRecord) {}
    fn snapshot(&self, _snap: Snapshot) {}
}

/// Blanket forward through `Arc`. Lets call sites hold an
/// `Arc<dyn Sink>` (or `Arc<InMemorySink>` for tests) and pass it as a
/// `Sink` without an explicit deref.
impl<S: Sink + ?Sized> Sink for Arc<S> {
    fn emit(&self, record: EventRecord) {
        (**self).emit(record);
    }
    fn snapshot(&self, snap: Snapshot) {
        (**self).snapshot(snap);
    }
    fn boot(&self, identity: &Identity) {
        (**self).boot(identity);
    }
    fn finalize(&self, body: serde_json::Value) {
        (**self).finalize(body);
    }
}

/// In-process buffer of everything seen. Used by tests that want to
/// assert on the event sequence. Not for production — unbounded.
#[derive(Debug, Default)]
pub struct InMemorySink {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    records: Vec<EventRecord>,
    snapshots: Vec<Snapshot>,
    boots: Vec<Identity>,
    finalizes: Vec<serde_json::Value>,
}

impl InMemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the records recorded so far. Allocates.
    pub fn records(&self) -> Vec<EventRecord> {
        self.inner.lock().expect("sink mutex poisoned").records.clone()
    }

    /// Snapshot the snapshots (sic) recorded so far. Allocates.
    pub fn snapshots(&self) -> Vec<Snapshot> {
        self.inner.lock().expect("sink mutex poisoned").snapshots.clone()
    }

    /// Snapshot the boot records observed.
    pub fn boots(&self) -> Vec<Identity> {
        self.inner.lock().expect("sink mutex poisoned").boots.clone()
    }

    /// Snapshot the finalize bodies observed.
    pub fn finalizes(&self) -> Vec<serde_json::Value> {
        self.inner.lock().expect("sink mutex poisoned").finalizes.clone()
    }

    pub fn record_count(&self) -> usize {
        self.inner.lock().expect("sink mutex poisoned").records.len()
    }

    pub fn snapshot_count(&self) -> usize {
        self.inner.lock().expect("sink mutex poisoned").snapshots.len()
    }
}

impl Sink for InMemorySink {
    fn emit(&self, record: EventRecord) {
        self.inner.lock().expect("sink mutex poisoned").records.push(record);
    }

    fn snapshot(&self, snap: Snapshot) {
        self.inner.lock().expect("sink mutex poisoned").snapshots.push(snap);
    }

    fn boot(&self, identity: &Identity) {
        self.inner.lock().expect("sink mutex poisoned").boots.push(identity.clone());
    }

    fn finalize(&self, body: serde_json::Value) {
        self.inner.lock().expect("sink mutex poisoned").finalizes.push(body);
    }
}

#[cfg(feature = "collector")]
pub use http::{HttpSink, SinkConfig, SinkHandle};

#[cfg(feature = "collector")]
mod http {
    //! HTTP-backed sink that POSTs to a [`crate::diagnostics::collector`]
    //! and spools to disk when the collector is unreachable.
    //!
    //! Architecture: emit/snapshot/boot/finalize calls drop a message
    //! onto an unbounded tokio mpsc channel and return immediately.
    //! A background drainer task batches events, posts them with
    //! exponential backoff, and spools failed batches to disk under
    //! `{spool_dir}/{run_id}/`. The spool drains on every successful
    //! POST so a recovered collector catches up automatically.
    //!
    //! Every successful POST's clock echo (T1.5) gets recorded as a
    //! `Custom { kind: "clock_sample", ... }` event in the next outbound
    //! batch, so post-hoc clock alignment has the data it needs.

    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use serde::Serialize;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    use super::Sink;
    use crate::diagnostics::collector::protocol::{ClockEcho, Hints, PostAck, RecordKind};
    use crate::diagnostics::event::{Event, EventRecord};
    use crate::diagnostics::identity::Identity;
    use crate::diagnostics::signal::SnapshotSignal;
    use crate::diagnostics::snapshot::Snapshot;
    use crate::diagnostics::spool::Spool;

    /// Configuration for an [`HttpSink`].
    ///
    /// `collector_url` is the base URL of the collector — only
    /// `http://host[:port]` is supported (the collector is intended for
    /// VPS deployment alongside the run, not behind TLS terminators).
    #[derive(Debug, Clone)]
    pub struct SinkConfig {
        pub collector_url: String,
        pub run_id: String,
        pub node_id_hex: String,
        pub spool_dir: PathBuf,
        /// Soft cap on the number of events buffered between flushes.
        /// A batch is also flushed every `batch_interval`.
        pub batch_max_events: usize,
        pub batch_interval: Duration,
        pub retry_initial: Duration,
        pub retry_max: Duration,
        /// Per-request total budget (connect + write + read).
        pub request_timeout: Duration,
        /// Optional cross-task signal fired when the collector returns
        /// `hints.snapshot_now == true`. Pair with
        /// [`crate::diagnostics::aggregator::spawn_periodic_snapshots`]
        /// so the aggregator's snapshot task receives the hint.
        pub snapshot_signal: Option<SnapshotSignal>,
    }

    impl SinkConfig {
        /// Build a config with sane defaults. The four mandatory
        /// fields (`collector_url`, `run_id`, `node_id_hex`,
        /// `spool_dir`) are required because the aggregator can't
        /// guess them.
        pub fn new(
            collector_url: impl Into<String>,
            run_id: impl Into<String>,
            node_id_hex: impl Into<String>,
            spool_dir: impl Into<PathBuf>,
        ) -> Self {
            Self {
                collector_url: collector_url.into(),
                run_id: run_id.into(),
                node_id_hex: node_id_hex.into(),
                spool_dir: spool_dir.into(),
                batch_max_events: 512,
                batch_interval: Duration::from_millis(1_000),
                retry_initial: Duration::from_millis(250),
                retry_max: Duration::from_secs(30),
                request_timeout: Duration::from_secs(10),
                snapshot_signal: None,
            }
        }

        /// Attach a [`SnapshotSignal`] so the drainer can relay
        /// `snapshot_now` hints to the aggregator.
        pub fn with_snapshot_signal(mut self, signal: SnapshotSignal) -> Self {
            self.snapshot_signal = Some(signal);
            self
        }

        pub fn with_batch_interval(mut self, d: Duration) -> Self {
            self.batch_interval = d;
            self
        }

        pub fn with_retry_initial(mut self, d: Duration) -> Self {
            self.retry_initial = d;
            self
        }

        pub fn with_retry_max(mut self, d: Duration) -> Self {
            self.retry_max = d;
            self
        }

        pub fn with_request_timeout(mut self, d: Duration) -> Self {
            self.request_timeout = d;
            self
        }
    }

    /// Out-of-band handle to an [`HttpSink`] for graceful shutdown and
    /// for tests that want to peek at the spool. Cheap to clone.
    #[derive(Debug, Clone)]
    pub struct SinkHandle {
        spool_dir: PathBuf,
        run_id: String,
        shutdown_tx: Arc<
            tokio::sync::Mutex<
                Option<mpsc::Sender<tokio::sync::oneshot::Sender<()>>>,
            >,
        >,
        delivered_count: Arc<AtomicU64>,
    }

    impl SinkHandle {
        /// Directory under which spool files for this run live. Used
        /// by tests to assert spool-fills-and-drains behavior.
        pub fn spool_run_dir(&self) -> PathBuf {
            self.spool_dir.join(&self.run_id)
        }

        /// Number of POSTs the drainer has successfully completed
        /// against the collector (events batches, snapshots, boot,
        /// finalize all counted). Useful for tests that need to wait
        /// for a flush.
        pub fn delivered_count(&self) -> u64 {
            self.delivered_count.load(Ordering::Relaxed)
        }

        /// Trigger a graceful shutdown: flush any pending events and
        /// drain the spool one last time, then exit the drainer task.
        /// Returns after the drainer task has completed.
        pub async fn shutdown(&self) {
            let mut guard = self.shutdown_tx.lock().await;
            let Some(tx) = guard.take() else { return };
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            // tx is a tokio::sync::mpsc::Sender<oneshot>; we send the
            // completion oneshot through it so the drainer can ack.
            let _ = tx.send(done_tx).await;
            let _ = done_rx.await;
        }
    }

    /// Production sink. Constructed inside a tokio runtime (it spawns
    /// a background drainer task on construction).
    pub struct HttpSink {
        tx: mpsc::UnboundedSender<Command>,
        handle: SinkHandle,
    }

    impl HttpSink {
        /// Build the sink and spawn its drainer.
        ///
        /// Must be called from within a tokio runtime — the drainer is
        /// spawned with [`tokio::spawn`]. Returns an error if the
        /// `collector_url` is malformed or the spool directory cannot
        /// be created.
        pub fn new(config: SinkConfig) -> std::io::Result<Self> {
            let endpoint = Endpoint::parse(&config.collector_url).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid collector_url {:?}: {e}", config.collector_url),
                )
            })?;
            let node_id = decode_hex_node_id(&config.node_id_hex);
            let spool = Spool::open_sync(&config.spool_dir, &config.run_id)?;
            let (tx, rx) = mpsc::unbounded_channel();
            let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
            let delivered = Arc::new(AtomicU64::new(0));
            let snapshot_signal = config.snapshot_signal.clone();
            let handle = SinkHandle {
                spool_dir: config.spool_dir.clone(),
                run_id: config.run_id.clone(),
                shutdown_tx: Arc::new(tokio::sync::Mutex::new(Some(shutdown_tx))),
                delivered_count: Arc::clone(&delivered),
            };
            let drainer = Drainer {
                config,
                endpoint,
                spool,
                rx,
                shutdown_rx,
                pending: Vec::new(),
                clock_sample_seq: 0,
                node_id,
                delivered,
                snapshot_signal,
            };
            tokio::spawn(drainer.run());
            Ok(Self { tx, handle })
        }

        /// Out-of-band handle for shutdown and test inspection.
        pub fn handle(&self) -> SinkHandle {
            self.handle.clone()
        }
    }

    fn decode_hex_node_id(hex: &str) -> crate::types::NodeId {
        let mut out = [0u8; 32];
        if hex.len() == 64 {
            for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
                if let (Some(hi), Some(lo)) = (hex_nibble(chunk[0]), hex_nibble(chunk[1])) {
                    out[i] = (hi << 4) | lo;
                }
            }
        }
        crate::types::NodeId(out)
    }

    fn hex_nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    fn is_clock_sample(record: &EventRecord) -> bool {
        matches!(&record.event, Event::Custom { kind, .. } if kind == "clock_sample")
    }

    impl Sink for HttpSink {
        fn emit(&self, record: EventRecord) {
            let _ = self.tx.send(Command::Event(record));
        }
        fn snapshot(&self, snap: Snapshot) {
            let _ = self.tx.send(Command::Snapshot(Box::new(snap)));
        }
        fn boot(&self, identity: &Identity) {
            let _ = self.tx.send(Command::Boot(Box::new(identity.clone())));
        }
        fn finalize(&self, body: serde_json::Value) {
            let _ = self.tx.send(Command::Finalize(body));
        }
    }

    enum Command {
        Event(EventRecord),
        Snapshot(Box<Snapshot>),
        Boot(Box<Identity>),
        Finalize(Value),
    }

    struct Drainer {
        config: SinkConfig,
        endpoint: Endpoint,
        spool: Spool,
        rx: mpsc::UnboundedReceiver<Command>,
        shutdown_rx: mpsc::Receiver<tokio::sync::oneshot::Sender<()>>,
        pending: Vec<EventRecord>,
        clock_sample_seq: u64,
        node_id: crate::types::NodeId,
        delivered: Arc<AtomicU64>,
        snapshot_signal: Option<SnapshotSignal>,
    }

    impl Drainer {
        async fn run(mut self) {
            let mut backoff = self.config.retry_initial;
            let mut interval = tokio::time::interval(self.config.batch_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately; consume it so we don't
            // flush an empty batch at startup.
            interval.tick().await;

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
                            Some(Command::Event(rec)) => {
                                self.pending.push(rec);
                                if self.pending.len() >= self.config.batch_max_events {
                                    self.flush_events(&mut backoff).await;
                                }
                            }
                            Some(Command::Snapshot(snap)) => {
                                self.send_one(RecordKind::Snapshot, &*snap, &mut backoff).await;
                            }
                            Some(Command::Boot(id)) => {
                                self.send_one(RecordKind::Boot, &*id, &mut backoff).await;
                            }
                            Some(Command::Finalize(body)) => {
                                // Make sure the event tail goes out
                                // before the finalize record.
                                if !self.pending.is_empty() {
                                    self.flush_events(&mut backoff).await;
                                }
                                self.send_one(RecordKind::Finalize, &body, &mut backoff).await;
                            }
                            None => break,
                        }
                    }
                    _ = interval.tick() => {
                        if !self.pending.is_empty() {
                            self.flush_events(&mut backoff).await;
                        } else {
                            // Empty pending — still try to drain spool
                            // periodically. If the spool has nothing,
                            // this is a no-op.
                            self.drain_spool(&mut backoff).await;
                        }
                    }
                }
            }

            // Shutdown cleanup. We need to drain anything still queued
            // on `rx` before exiting, otherwise commands the caller
            // sent moments before `shutdown()` (e.g. the orchestrator's
            // finalize record) get silently dropped. Use `try_recv` so
            // the drain terminates even if some other producer is still
            // emitting — the unbounded sender is held by the public
            // `HttpSink`, which is itself about to be dropped.
            loop {
                match self.rx.try_recv() {
                    Ok(Command::Event(rec)) => self.pending.push(rec),
                    Ok(Command::Snapshot(snap)) => {
                        self.send_one(RecordKind::Snapshot, &*snap, &mut backoff).await;
                    }
                    Ok(Command::Boot(id)) => {
                        self.send_one(RecordKind::Boot, &*id, &mut backoff).await;
                    }
                    Ok(Command::Finalize(body)) => {
                        if !self.pending.is_empty() {
                            self.flush_events(&mut backoff).await;
                        }
                        self.send_one(RecordKind::Finalize, &body, &mut backoff).await;
                    }
                    Err(_) => break,
                }
            }
            // Best-effort final flush — anything still in pending lands
            // on the collector or in the spool so a later run picks it
            // up. Then one last spool drain in case earlier sends
            // pushed items there.
            if !self.pending.is_empty() {
                self.flush_events(&mut backoff).await;
            }
            let _ = self.drain_spool(&mut backoff).await;
            if let Some(ack) = shutdown_ack {
                let _ = ack.send(());
            }
        }

        async fn flush_events(&mut self, backoff: &mut Duration) {
            if self.pending.is_empty() {
                return;
            }
            let batch = std::mem::take(&mut self.pending);
            // Only record a clock_sample if this batch carried at
            // least one non-sample event — otherwise the clock_sample
            // we record after sending would itself trigger the next
            // batch ad infinitum.
            let carries_real_events = batch.iter().any(|r| !is_clock_sample(r));
            let bytes = match serde_json::to_vec(&batch) {
                Ok(b) => b,
                Err(_) => return,
            };
            match self.post_with_timeout(RecordKind::Events, &bytes).await {
                Ok(ack) => {
                    self.note_success(backoff, &ack.clock, carries_real_events);
                    self.relay_hints(ack.hints.as_ref());
                    // Try to flush the spool while we're connected.
                    self.drain_spool(backoff).await;
                }
                Err(_) => {
                    let _ = self.spool.append(RecordKind::Events, &bytes).await;
                    self.bump_backoff(backoff);
                }
            }
        }

        async fn send_one<T: Serialize + ?Sized>(
            &mut self,
            kind: RecordKind,
            body: &T,
            backoff: &mut Duration,
        ) {
            let bytes = match serde_json::to_vec(body) {
                Ok(b) => b,
                Err(_) => return,
            };
            match self.post_with_timeout(kind, &bytes).await {
                Ok(ack) => {
                    // Boot/snapshot/finalize POSTs are infrequent;
                    // their clock samples are always useful.
                    self.note_success(backoff, &ack.clock, true);
                    self.relay_hints(ack.hints.as_ref());
                    self.drain_spool(backoff).await;
                }
                Err(_) => {
                    let _ = self.spool.append(kind, &bytes).await;
                    self.bump_backoff(backoff);
                }
            }
        }

        /// Try to push every spooled record up to the collector, in
        /// order. Stops at the first failure to preserve FIFO delivery.
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
                    Ok(ack) => {
                        // Don't pile up clock_samples while we're
                        // catching up — one per drain pass is plenty.
                        self.note_success(backoff, &ack.clock, false);
                        self.relay_hints(ack.hints.as_ref());
                        let _ = tokio::fs::remove_file(&entry.path).await;
                    }
                    Err(_) => {
                        self.bump_backoff(backoff);
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
                Ok(Ok(ack)) => Ok(ack),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(PostError::Timeout),
            }
        }

        fn note_success(
            &mut self,
            backoff: &mut Duration,
            clock: &ClockEcho,
            record_sample: bool,
        ) {
            self.delivered.fetch_add(1, Ordering::Relaxed);
            *backoff = self.config.retry_initial;
            if !record_sample {
                return;
            }
            // Record the clock sample as a Custom event in the next
            // outbound batch. The sink owns a private seq counter for
            // these (the aggregator's seq is independent — clock
            // samples are sink-generated, not user emissions).
            self.clock_sample_seq += 1;
            self.pending.push(EventRecord {
                node_id: self.node_id,
                monotonic_seq: self.clock_sample_seq,
                wall_ms: super::super::collector::wall_ms_now(),
                event: Event::Custom {
                    kind: "clock_sample".into(),
                    fields: serde_json::json!({
                        "node_send_ms_echoed": clock.node_send_ms_echoed,
                        "collector_recv_ms": clock.collector_recv_ms,
                        "collector_send_ms": clock.collector_send_ms,
                    }),
                },
            });
        }

        fn bump_backoff(&self, backoff: &mut Duration) {
            let doubled = backoff.saturating_mul(2);
            *backoff = doubled.min(self.config.retry_max);
        }

        /// Pass on any collector-issued hints to whatever is listening.
        /// Currently only `snapshot_now` is wired (T1.4 pull-trigger);
        /// new hints can be relayed here without touching the four POST
        /// call sites.
        fn relay_hints(&self, hints: Option<&Hints>) {
            let Some(hints) = hints else { return };
            if hints.snapshot_now {
                if let Some(signal) = &self.snapshot_signal {
                    signal.request();
                }
            }
        }
    }

    /// Parsed `http://host[:port][/base]` URL. Only the http scheme is
    /// supported; the collector is meant for inside-VPS or inside-VPC
    /// deployment without TLS termination.
    #[derive(Debug, Clone)]
    pub(crate) struct Endpoint {
        host: String,
        port: u16,
        base: String,
    }

    impl Endpoint {
        pub(crate) fn parse(url: &str) -> Result<Self, String> {
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
            let base = path.trim_end_matches('/').to_string();
            Ok(Self { host, port, base })
        }
    }

    #[derive(Debug)]
    #[allow(dead_code)] // Variants are read via Debug for log lines.
    enum PostError {
        Connect(std::io::Error),
        Io(std::io::Error),
        Status(u16),
        BadResponse,
        Timeout,
    }

    async fn post(
        endpoint: &Endpoint,
        config: &SinkConfig,
        kind: RecordKind,
        body: &[u8],
    ) -> Result<PostAck<Value>, PostError> {
        let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .map_err(PostError::Connect)?;
        let send_ms = super::super::collector::wall_ms_now();
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
            node = config.node_id_hex,
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
        fn endpoint_parses_host_and_port() {
            let e = Endpoint::parse("http://collector.example:9080").unwrap();
            assert_eq!(e.host, "collector.example");
            assert_eq!(e.port, 9080);
            assert_eq!(e.base, "");
        }

        #[test]
        fn endpoint_parses_base_path() {
            let e = Endpoint::parse("http://127.0.0.1:9080/api/").unwrap();
            assert_eq!(e.host, "127.0.0.1");
            assert_eq!(e.port, 9080);
            assert_eq!(e.base, "/api");
        }

        #[test]
        fn endpoint_defaults_port_to_80() {
            let e = Endpoint::parse("http://example.com").unwrap();
            assert_eq!(e.port, 80);
        }

        #[test]
        fn endpoint_rejects_non_http_scheme() {
            assert!(Endpoint::parse("https://example.com").is_err());
            assert!(Endpoint::parse("ftp://example.com").is_err());
            assert!(Endpoint::parse("example.com").is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::event::Event;
    use crate::types::NodeId;

    fn dummy_record(seq: u64) -> EventRecord {
        EventRecord {
            node_id: NodeId([0u8; 32]),
            monotonic_seq: seq,
            wall_ms: 0,
            event: Event::MessageSent {
                peer: NodeId([0u8; 32]),
                kind: "ping".into(),
                size: 64,
            },
        }
    }

    #[test]
    fn noop_sink_swallows_records() {
        let s = NoopSink;
        s.emit(dummy_record(1));
        // No observable behavior — just a smoke test that it doesn't panic.
    }

    #[test]
    fn in_memory_sink_records_records_and_snapshots() {
        let sink = InMemorySink::new();
        sink.emit(dummy_record(1));
        sink.emit(dummy_record(2));
        assert_eq!(sink.record_count(), 2);
        assert_eq!(sink.snapshot_count(), 0);
        assert_eq!(sink.records().len(), 2);
    }

    #[test]
    fn in_memory_sink_records_boot_and_finalize() {
        let sink = InMemorySink::new();
        let id = crate::diagnostics::identity::Identity::new(
            NodeId([1u8; 32]),
            crate::diagnostics::identity::Role::stage(),
            "run-a",
        );
        sink.boot(&id);
        sink.finalize(serde_json::json!({"exit_reason": "ok"}));
        assert_eq!(sink.boots().len(), 1);
        assert_eq!(sink.finalizes().len(), 1);
        assert_eq!(sink.finalizes()[0]["exit_reason"], "ok");
    }
}
