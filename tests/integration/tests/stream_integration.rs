//! End-to-end test: store a blob on node A, download via QUIC stream on node B.

use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;

use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor_datastore::bridge::{DatastoreGroup, DatastoreGroupConfig};
use swactor_datastore::messages::{DatastoreNodeMsg, DatastoreResponse};
use swactor::std::RuntimeNaming;

fn make_driver_with_streams() -> IrohDriver {
    IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: DistributedNodeConfig::default(),
        peer_auth: None,
        additional_alpns: vec![swactor_datastore::streams::ALPN.to_vec()],
    })
    .expect("create iroh driver")
}

fn make_runtime() -> Arc<Runtime> {
    let rt = Runtime::new(RuntimeConfig {
        num_threads: 2,
        max_actors: 256,
        channel_buffer_size: 2000,
        ..Default::default()
    })
    .with_extension(Arc::new(swactor::std::StdExtension::new()));

    let handle = rt.run().expect("start runtime");
    handle.runtime
}

fn spawn_stream_manager(
    runtime: &Arc<Runtime>,
    driver: &IrohDriver,
) -> swactor::actor::ActorAddress {
    let mgr = swactor_datastore::streams::StreamManager::new(
        driver.endpoint().clone(),
        driver.tokio_handle(),
        Arc::clone(runtime),
    );
    let addr = runtime.spawn(mgr).expect("spawn StreamManager");
    runtime
        .register_name(swactor_datastore::streams::STREAM_MANAGER_NAME, addr)
        .expect("register StreamManager");
    addr
}

fn spawn_datastore(
    runtime: &Arc<Runtime>,
    driver: &IrohDriver,
    mgr_addr: swactor::actor::ActorAddress,
) -> DatastoreGroup {
    let node_id = driver.node_id();
    let group = DatastoreGroup::spawn(
        Arc::clone(runtime),
        DatastoreGroupConfig {
            node_id,
            node_id_hex: format!("{:?}", node_id),
            chunk_size: 256,
            storage_path: None, // in-memory
            auth: None,
            gc_interval: u64::MAX,
            disseminate_interval: u64::MAX,
        },
    )
    .expect("spawn datastore");
    group.configure_streams(mgr_addr, driver.tokio_handle());
    group
}

/// Poll for a response from the inbox, routing stream connections between
/// the two nodes. Actor message processing is handled by worker threads.
fn pump_until_response(
    rt_a: &Arc<Runtime>,
    rt_b: &Arc<Runtime>,
    driver_a: &mut IrohDriver,
    driver_b: &mut IrohDriver,
    mgr_a: swactor::actor::ActorAddress,
    mgr_b: swactor::actor::ActorAddress,
    inbox: &swactor::runtime::Inbox<DatastoreResponse>,
    timeout: Duration,
) -> Option<DatastoreResponse> {
    let deadline = Instant::now() + timeout;
    let tokio_handle = driver_a.tokio_handle();

    loop {
        // SWIM protocol ticks
        driver_a.recv();
        driver_a.tick();
        driver_b.recv();
        driver_b.tick();

        // Route incoming stream connections on node A
        for (node_id, conn) in driver_a.drain_other_connections() {
            let rt = Arc::clone(rt_a);
            let mgr = mgr_a;
            let node_bytes = node_id.0;
            tokio_handle.spawn(async move {
                let _ = swactor_datastore::streams::accept::handle_incoming(node_bytes, conn, &rt, mgr).await;
            });
        }

        // Route incoming stream connections on node B
        for (node_id, conn) in driver_b.drain_other_connections() {
            let rt = Arc::clone(rt_b);
            let mgr = mgr_b;
            let node_bytes = node_id.0;
            tokio_handle.spawn(async move {
                let _ = swactor_datastore::streams::accept::handle_incoming(node_bytes, conn, &rt, mgr).await;
            });
        }

        // Check for completion
        if let Some(resp) = inbox.try_recv() {
            return Some(resp);
        }

        if Instant::now() >= deadline {
            return None;
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn small_blob_transfers_between_two_nodes_via_stream() {
    let mut driver_a = make_driver_with_streams();
    let mut driver_b = make_driver_with_streams();

    let rt_a = make_runtime();
    let rt_b = make_runtime();

    let mgr_a = spawn_stream_manager(&rt_a, &driver_a);
    let mgr_b = spawn_stream_manager(&rt_b, &driver_b);

    let _ds_a = spawn_datastore(&rt_a, &driver_a, mgr_a);
    let ds_b = spawn_datastore(&rt_b, &driver_b, mgr_b);
    let _ = &ds_b; // keep alive

    // Have both nodes discover each other via SWIM
    let addr_a = driver_a.endpoint_addr();
    let addr_b = driver_b.endpoint_addr();
    driver_a.join(&[addr_b]);
    driver_b.join(&[addr_a]);

    // Pump until SWIM membership converges
    let swim_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        driver_a.recv();
        driver_a.tick();
        driver_b.recv();
        driver_b.tick();

        let snap_a = driver_a.snapshot();
        let snap_b = driver_b.snapshot();
        if snap_a.alive_count >= 1 && snap_b.alive_count >= 1 {
            break;
        }
        if Instant::now() >= swim_deadline {
            panic!("SWIM convergence timed out");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Give worker threads time to process spawned actors
    std::thread::sleep(Duration::from_millis(100));

    // Store a small blob on Node A
    let test_data = b"Hello from node A! This is a stream integration test.";
    let put_inbox = rt_a
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let ds_a_addr = rt_a.where_is("Datastore").expect("Datastore registered on A");
    let _ = rt_a.send_to(
        ds_a_addr,
        DatastoreNodeMsg::Put {
            data: test_data.to_vec(),
            name: Some("test-blob".into()),
            tags: Default::default(),
            reply_to: *put_inbox.addr(),
        },
    );

    // Wait for PutOk (worker threads process messages)
    let content_hash = loop {
        if let Some(resp) = put_inbox.try_recv() {
            match resp {
                DatastoreResponse::PutOk { content_hash } => break content_hash,
                other => panic!("expected PutOk, got: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    // PutOk comes from MetadataActor; BlobStore writes are fire-and-forget.
    // Give BlobStore time to finish writing chunks + manifest.
    std::thread::sleep(Duration::from_millis(200));

    // Download the blob on Node B via stream
    let download_inbox = rt_b
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let ds_b_addr = rt_b.where_is("Datastore").expect("Datastore registered on B");
    let _ = rt_b.send_to(
        ds_b_addr,
        DatastoreNodeMsg::DownloadViaStream {
            content_hash,
            source_node: driver_a.node_id().0,
            reply_to: *download_inbox.addr(),
        },
    );

    // Pump loop until we get a response
    let resp = pump_until_response(
        &rt_a,
        &rt_b,
        &mut driver_a,
        &mut driver_b,
        mgr_a,
        mgr_b,
        &download_inbox,
        Duration::from_secs(15),
    );

    match resp {
        Some(DatastoreResponse::PutOk { content_hash: h }) => {
            assert_eq!(h, content_hash, "downloaded blob hash should match");
        }
        other => panic!("expected PutOk from download, got: {other:?}"),
    }

    // Verify: read the blob back from Node B's datastore
    let verify_inbox = rt_b
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let _ = rt_b.send_to(
        ds_b_addr,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: *verify_inbox.addr(),
        },
    );

    let verify_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(resp) = verify_inbox.try_recv() {
            match resp {
                DatastoreResponse::GetOk { entry, manifest } => {
                    assert_eq!(entry.content_hash, content_hash);
                    assert_eq!(manifest.total_size, test_data.len() as u64);
                    break;
                }
                other => panic!("expected GetOk, got: {other:?}"),
            }
        }
        if Instant::now() >= verify_deadline {
            panic!("verify timed out — blob not found on Node B");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    driver_a.shutdown();
    driver_b.shutdown();
    rt_a.shutdown();
    rt_b.shutdown();
}

#[test]
fn multi_chunk_blob_transfers_between_two_nodes_via_stream() {
    let mut driver_a = make_driver_with_streams();
    let mut driver_b = make_driver_with_streams();

    let rt_a = make_runtime();
    let rt_b = make_runtime();

    let mgr_a = spawn_stream_manager(&rt_a, &driver_a);
    let mgr_b = spawn_stream_manager(&rt_b, &driver_b);

    let _ds_a = spawn_datastore(&rt_a, &driver_a, mgr_a);
    let ds_b = spawn_datastore(&rt_b, &driver_b, mgr_b);
    let _ = &ds_b;

    // Discover each other via SWIM
    let addr_a = driver_a.endpoint_addr();
    let addr_b = driver_b.endpoint_addr();
    driver_a.join(&[addr_b]);
    driver_b.join(&[addr_a]);

    let swim_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        driver_a.recv();
        driver_a.tick();
        driver_b.recv();
        driver_b.tick();

        let snap_a = driver_a.snapshot();
        let snap_b = driver_b.snapshot();
        if snap_a.alive_count >= 1 && snap_b.alive_count >= 1 {
            break;
        }
        if Instant::now() >= swim_deadline {
            panic!("SWIM convergence timed out");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Give worker threads time to process spawned actors
    std::thread::sleep(Duration::from_millis(100));

    // 4 chunks at 256 bytes each = 1024 bytes
    let test_data: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
    let put_inbox = rt_a
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let ds_a_addr = rt_a.where_is("Datastore").expect("Datastore registered on A");
    let _ = rt_a.send_to(
        ds_a_addr,
        DatastoreNodeMsg::Put {
            data: test_data.clone(),
            name: Some("multi-chunk".into()),
            tags: Default::default(),
            reply_to: *put_inbox.addr(),
        },
    );

    let content_hash = loop {
        if let Some(resp) = put_inbox.try_recv() {
            match resp {
                DatastoreResponse::PutOk { content_hash } => break content_hash,
                other => panic!("expected PutOk, got: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    // PutOk comes from MetadataActor; BlobStore writes are fire-and-forget.
    std::thread::sleep(Duration::from_millis(200));

    // Download on Node B
    let download_inbox = rt_b
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let ds_b_addr = rt_b.where_is("Datastore").expect("Datastore registered on B");
    let _ = rt_b.send_to(
        ds_b_addr,
        DatastoreNodeMsg::DownloadViaStream {
            content_hash,
            source_node: driver_a.node_id().0,
            reply_to: *download_inbox.addr(),
        },
    );

    let resp = pump_until_response(
        &rt_a,
        &rt_b,
        &mut driver_a,
        &mut driver_b,
        mgr_a,
        mgr_b,
        &download_inbox,
        Duration::from_secs(15),
    );

    match resp {
        Some(DatastoreResponse::PutOk { content_hash: h }) => {
            assert_eq!(h, content_hash);
        }
        other => panic!("expected PutOk from download, got: {other:?}"),
    }

    // Verify the data on Node B by reading each chunk
    let verify_inbox = rt_b
        .new_inbox::<DatastoreResponse>()
        .expect("create inbox");
    let _ = rt_b.send_to(
        ds_b_addr,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: *verify_inbox.addr(),
        },
    );

    let verify_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(resp) = verify_inbox.try_recv() {
            match resp {
                DatastoreResponse::GetOk { entry, manifest } => {
                    assert_eq!(entry.content_hash, content_hash);
                    assert_eq!(manifest.total_size, test_data.len() as u64);
                    assert!(manifest.chunks.len() > 1, "should be multi-chunk");
                    break;
                }
                other => panic!("expected GetOk, got: {other:?}"),
            }
        }
        if Instant::now() >= verify_deadline {
            panic!("verify timed out — blob not found on Node B");
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    driver_a.shutdown();
    driver_b.shutdown();
    rt_a.shutdown();
    rt_b.shutdown();
}
