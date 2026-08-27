use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use data_plane::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferSender, FileTransferRequest,
};
use data_plane::namespace::NamespaceError;
use data_plane::source::{BlobSourceIn, FileBlobSourceActor};
use futures_lite::future;
use parking_lot::Mutex;
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct TestFile {
    path: PathBuf,
}

impl TestFile {
    fn new(bytes: &[u8]) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "swactor-file-source-{}-{sequence}",
            std::process::id()
        ));
        std::fs::write(&path, bytes).expect("write source fixture");
        Self { path }
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn runtime() -> (Engine, Runtime) {
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    (engine, runtime)
}

struct LoopbackSender {
    runtime: Runtime,
}

impl BlobTransferSender for LoopbackSender {
    fn start_file(&self, mut request: FileTransferRequest) -> Result<(), String> {
        let mut bytes = Vec::new();
        request
            .file
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        if bytes.len() as u64 != request.length {
            return Err("source length changed".to_owned());
        }
        self.runtime
            .send_to(
                request.offer.destination,
                BlobTransferEvent::Chunk {
                    transfer_id: request.offer.transfer_id,
                    bytes,
                },
            )
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                BlobTransferEvent::Finished {
                    transfer_id: request.offer.transfer_id,
                },
            )
            .map_err(|error| error.to_string())?;
        request.completion.complete(Ok(()));
        Ok(())
    }
}

#[test]
fn file_source_owns_fixed_length_and_transfers_opened_file() {
    let fixture = TestFile::new(b"fixed-source");
    let (_engine, runtime) = runtime();
    let sender = Arc::new(LoopbackSender {
        runtime: runtime.clone(),
    });
    let source = FileBlobSourceActor::open(runtime.clone(), sender, &fixture.path)
        .expect("open file source");
    assert_eq!(source.length(), 12);
    let destination = runtime
        .new_inbox::<BlobTransferEvent>()
        .expect("destination inbox");
    let source = runtime.spawn(source).expect("spawn file source");
    let transfer_id = BlobTransferId(41);

    runtime
        .send_to(
            source,
            BlobSourceIn::BeginTransfer {
                offer: BlobTransferOffer {
                    transfer_id,
                    destination: *destination.addr(),
                    failure_proxy: None,
                    transport: Vec::new(),
                },
            },
        )
        .expect("begin source transfer");

    future::block_on(async {
        assert_eq!(
            destination.recv().await,
            BlobTransferEvent::Chunk {
                transfer_id,
                bytes: b"fixed-source".to_vec(),
            }
        );
        assert_eq!(
            destination.recv().await,
            BlobTransferEvent::Finished { transfer_id }
        );
    });
}

struct HeldSender {
    request: Mutex<Option<FileTransferRequest>>,
    starts: AtomicU64,
}

impl BlobTransferSender for HeldSender {
    fn start_file(&self, request: FileTransferRequest) -> Result<(), String> {
        self.starts.fetch_add(1, Ordering::Relaxed);
        *self.request.lock() = Some(request);
        Ok(())
    }
}

#[test]
fn retirement_does_not_cancel_an_accepted_transfer() {
    let fixture = TestFile::new(b"retained");
    let (_engine, runtime) = runtime();
    let sender = Arc::new(HeldSender {
        request: Mutex::new(None),
        starts: AtomicU64::new(0),
    });
    let source_actor = FileBlobSourceActor::open(runtime.clone(), sender.clone(), &fixture.path)
        .expect("open file source");
    let destination = runtime
        .new_inbox::<BlobTransferEvent>()
        .expect("destination inbox");
    let source = runtime.spawn(source_actor).expect("spawn source");
    let transfer_id = BlobTransferId(9);
    runtime
        .send_to(
            source,
            BlobSourceIn::BeginTransfer {
                offer: BlobTransferOffer {
                    transfer_id,
                    destination: *destination.addr(),
                    failure_proxy: None,
                    transport: Vec::new(),
                },
            },
        )
        .unwrap();
    runtime
        .send_to(source, BlobSourceIn::Retire { reply_to: None })
        .unwrap();

    let mut request = loop {
        if let Some(request) = sender.request.lock().take() {
            break request;
        }
        std::thread::yield_now();
    };
    let mut bytes = Vec::new();
    request.file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"retained");
    request.completion.complete(Ok(()));
}

#[test]
fn repeated_begin_transfer_is_idempotent_while_rendezvous_is_in_flight() {
    let fixture = TestFile::new(b"retry-safe");
    let (_engine, runtime) = runtime();
    let sender = Arc::new(HeldSender {
        request: Mutex::new(None),
        starts: AtomicU64::new(0),
    });
    let source = runtime
        .spawn(
            FileBlobSourceActor::open(runtime.clone(), sender.clone(), &fixture.path)
                .expect("open file source"),
        )
        .unwrap();
    let destination = runtime.new_inbox::<BlobTransferEvent>().unwrap();
    let offer = BlobTransferOffer {
        transfer_id: BlobTransferId(77),
        destination: *destination.addr(),
        failure_proxy: None,
        transport: Vec::new(),
    };
    runtime
        .send_to(
            source,
            BlobSourceIn::BeginTransfer {
                offer: offer.clone(),
            },
        )
        .unwrap();
    runtime
        .send_to(
            source,
            BlobSourceIn::BeginTransfer {
                offer: offer.clone(),
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let request = loop {
        if let Some(request) = sender.request.lock().take() {
            break request;
        }
        assert!(Instant::now() < deadline, "source transfer did not start");
        std::thread::yield_now();
    };
    request.completion.complete(Ok(()));
    runtime
        .send_to(source, BlobSourceIn::BeginTransfer { offer })
        .unwrap();
    runtime
        .send_to(source, BlobSourceIn::Retire { reply_to: None })
        .unwrap();
    loop {
        if runtime
            .stats()
            .actors
            .iter()
            .all(|(actor, _)| *actor != source)
        {
            break;
        }
        assert!(Instant::now() < deadline, "retired source did not stop");
        std::thread::yield_now();
    }
    assert_eq!(sender.starts.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_transfer_attempt_can_retry_the_same_rendezvous() {
    let fixture = TestFile::new(b"retry-after-failure");
    let (_engine, runtime) = runtime();
    let sender = Arc::new(HeldSender {
        request: Mutex::new(None),
        starts: AtomicU64::new(0),
    });
    let source = runtime
        .spawn(
            FileBlobSourceActor::open(runtime.clone(), sender.clone(), &fixture.path)
                .expect("open file source"),
        )
        .unwrap();
    let destination = runtime.new_inbox::<BlobTransferEvent>().unwrap();
    let offer = BlobTransferOffer {
        transfer_id: BlobTransferId(78),
        destination: *destination.addr(),
        failure_proxy: None,
        transport: Vec::new(),
    };
    runtime
        .send_to(
            source,
            BlobSourceIn::BeginTransfer {
                offer: offer.clone(),
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let first = loop {
        if let Some(request) = sender.request.lock().take() {
            break request;
        }
        assert!(Instant::now() < deadline, "first transfer did not start");
        std::thread::yield_now();
    };
    first.completion.complete(Err("transient".to_owned()));
    loop {
        if matches!(
            destination.try_recv(),
            Some(BlobTransferEvent::Failed { transfer_id, .. })
                if transfer_id == BlobTransferId(78)
        ) {
            break;
        }
        assert!(Instant::now() < deadline, "failure was not delivered");
        std::thread::yield_now();
    }
    runtime
        .send_to(source, BlobSourceIn::BeginTransfer { offer })
        .unwrap();
    loop {
        if sender.starts.load(Ordering::Relaxed) == 2 && sender.request.lock().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "failed transfer did not retry");
        std::thread::yield_now();
    }
}

#[test]
fn recovery_rejects_a_changed_file_length() {
    let fixture = TestFile::new(b"changed");
    let (_engine, runtime) = runtime();
    let sender = Arc::new(LoopbackSender {
        runtime: runtime.clone(),
    });
    let recovered = FileBlobSourceActor::recover(runtime, sender, &fixture.path, 99);
    assert!(
        matches!(recovered, Err(NamespaceError::SourceRecovery(reason)) if reason.contains("length"))
    );
}
