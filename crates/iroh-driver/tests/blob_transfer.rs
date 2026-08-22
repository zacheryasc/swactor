use std::collections::HashMap;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use data_plane::blob_transfer::{
    BlobTransferCompletion, BlobTransferEvent, BlobTransferId, BlobTransferReceiver,
    BlobTransferSender, FileTransferRequest,
};
use iroh::RelayMode;
use iroh_driver::{
    EDGE_ALPN, IrohBlobTransferReceiver, IrohBlobTransferSender, IrohDriver, IrohDriverConfig,
};
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct Completion(std::sync::mpsc::Sender<Result<(), String>>);

impl BlobTransferCompletion for Completion {
    fn complete(self: Box<Self>, result: Result<(), String>) {
        let _ = self.0.send(result);
    }
}

struct Node {
    engine: Engine,
    runtime: Runtime,
    driver: IrohDriver,
}

fn node() -> Node {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: distribution::node::DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec()],
        },
    )
    .expect("Iroh driver");
    let codecs = Arc::new(CodecRegistry::new());
    let router = Arc::new(TransportRouter::new());
    runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(Arc::clone(&codecs), router)));
    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: runtime.clone(),
        codec: codecs,
        routes: HashMap::new(),
        swim: ActorAddress::default(),
        relay_mirror: Arc::new(RwLock::new(HashMap::new())),
        route_view: Arc::new(RwLock::new(HashMap::new())),
        outbox: Arc::new(Mutex::new(Vec::new())),
    });
    driver.install_actor_bridge_pump(Duration::from_millis(5));
    Node {
        engine,
        runtime,
        driver,
    }
}

#[test]
fn real_iroh_transfer_delivers_exact_file_bytes() {
    let source = node();
    let destination = node();
    let receiver = Arc::new(IrohBlobTransferReceiver::new(
        destination.driver.endpoint_addr(),
        destination.driver.edge_events_handle(),
    ));
    receiver.install_pump(
        &destination.engine.handle(),
        destination.runtime.clone(),
        Duration::from_millis(5),
    );
    let sender =
        IrohBlobTransferSender::new(source.driver.edge_connector(), &source.engine.handle());
    let inbox = destination
        .runtime
        .new_inbox::<BlobTransferEvent>()
        .expect("destination inbox");
    let transfer_id = BlobTransferId(77);
    let offer = receiver
        .open(*inbox.addr(), transfer_id)
        .expect("open transfer receiver");

    let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "swactor-iroh-blob-{}-{sequence}",
        std::process::id()
    ));
    let expected = b"iroh-file-blob";
    std::fs::write(&path, expected).expect("write fixture");
    let (completion_tx, completion_rx) = std::sync::mpsc::channel();
    sender
        .start_file(FileTransferRequest {
            offer,
            file: File::open(&path).expect("open fixture"),
            offset: 0,
            length: expected.len() as u64,
            completion: Box::new(Completion(completion_tx)),
        })
        .expect("start Iroh transfer");
    completion_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("source completion")
        .expect("source transfer");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut received = Vec::new();
    loop {
        if let Some(event) = inbox.try_recv() {
            match event {
                BlobTransferEvent::Chunk {
                    transfer_id: found,
                    bytes,
                } => {
                    assert_eq!(found, transfer_id);
                    received.extend_from_slice(&bytes);
                }
                BlobTransferEvent::Finished { transfer_id: found } => {
                    assert_eq!(found, transfer_id);
                    break;
                }
                BlobTransferEvent::Failed { reason, .. } => {
                    panic!("Iroh blob transfer failed: {reason}")
                }
                event => panic!("unexpected local blob transfer event: {event:?}"),
            }
        } else {
            assert!(
                Instant::now() < deadline,
                "destination transfer deadline elapsed"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(received, expected);
    let _ = std::fs::remove_file(path);
}
