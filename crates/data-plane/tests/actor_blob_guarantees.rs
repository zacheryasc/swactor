#![cfg(target_os = "linux")]

include!("data_plane_test_support.inc");

#[test]
fn attachment_without_a_host_reply_fails_on_actor_deadline() {
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(8),
        reservation_ceiling: 4096,
        base_alignment: 64,
    })
    .unwrap();
    let handoff = bootstrap::write_bootstrap(
        &mut arena,
        BootstrapSpec {
            arena_generation: ARENA_GENERATION,
            alignment: 64,
        },
    )
    .unwrap();
    let (parts, runtime) = runtime_parts();
    runtime.set_remote_sink(Arc::new(BlackHoleSink));
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .unwrap(),
    )
    .unwrap();
    let (mapped, resolved) = DataPlaneBootstrap::map_arena(handoff.arena_fd).unwrap();
    let result = future::block_on(DataPlaneBootstrap::attach_mapped_with_deadline(
        mapped,
        resolved,
        runtime.clone(),
        ActorAddress::new_random(),
        CAPABILITY,
        None,
        data_plane::data_plane::AttachDeadline {
            engine: engine.handle(),
            timeout: Duration::from_millis(20),
        },
    ));
    assert!(matches!(
        result,
        Err(DataPlaneError::SessionFailed(reason)) if reason.contains("deadline")
    ));
}

#[test]
fn routed_read_blob_maps_final_sealed_lease_without_copying() {
    let harness = harness(4096);
    let blob = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("read blob");

    assert_eq!(blob.length(), 24);
    assert!(blob.digest().is_none());
    let lease = blob.lease();
    let view = blob.map().expect("map sealed blob");
    assert_eq!(view.as_ref(), WEIGHTS);
    assert_eq!(
        view.as_ptr(),
        // SAFETY: the lease is validated and the expected payload offset lies
        // inside the mapping retained by the test harness.
        unsafe {
            harness
                .bootstrap
                .arena
                .base_ptr()
                .add((lease.offset + BLOB_HEADER_LEN) as usize)
        }
    );

    let mut stale = lease;
    stale.generation += 1;
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        stale,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("stale generation must fail");
    assert!(matches!(error, BlobError::StaleGeneration { .. }));

    let mut outside = lease;
    outside.offset = u64::MAX - 32;
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        outside,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("out-of-bounds descriptor must fail");
    assert!(matches!(error, BlobError::RangeOutOfBounds { .. }));

    // Simulate a descriptor granted before its producer release-publishes the
    // sealed state. Validation must acquire and reject it before exposing bytes.
    let state_offset = lease.offset as usize + 24;
    // SAFETY: blob headers are 64-byte aligned and the state field is an
    // aligned AtomicU64 at the stable ABI offset 24.
    let state = unsafe {
        &*harness
            .bootstrap
            .arena
            .base_ptr()
            .add(state_offset)
            .cast::<AtomicU64>()
    };
    state.store(BlobSharedState::Filling as u64, Ordering::Release);
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        lease,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("early grant must fail");
    assert!(matches!(error, BlobError::InvalidState { .. }));
}

#[test]
fn thirty_two_concurrent_remote_opens_complete_without_cross_wiring() {
    fn reads(
        data_plane: data_plane::data_plane::DataPlane,
        first: usize,
        count: usize,
    ) -> future::Boxed<Vec<(usize, Blob)>> {
        if count == 1 {
            return async move {
                let path = if first.is_multiple_of(2) {
                    "/models/tiny-linear/weights"
                } else {
                    "/models/second"
                };
                vec![(
                    first,
                    data_plane
                        .read_blob_path(path)
                        .await
                        .expect("concurrent open"),
                )]
            }
            .boxed();
        }
        let left_count = count / 2;
        let left = reads(data_plane.clone(), first, left_count);
        let right = reads(data_plane, first + left_count, count - left_count);
        async move {
            let (mut left, right) = future::zip(left, right).await;
            left.extend(right);
            left
        }
        .boxed()
    }

    let harness = harness(16 * 1024);
    let blobs = future::block_on(reads(harness.bootstrap.data_plane.clone(), 0, 32));
    for (index, blob) in blobs {
        let expected = if index % 2 == 0 {
            WEIGHTS
        } else {
            b"second-blob"
        };
        assert_eq!(blob.map().unwrap().as_ref(), expected);
    }
}

#[test]
fn concurrent_remote_opens_keep_actor_identity_correlation() {
    let harness = harness(4096);
    let (weights, second) = future::block_on(future::zip(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/second"),
    ));
    assert_eq!(weights.unwrap().map().unwrap().as_ref(), WEIGHTS);
    assert_eq!(second.unwrap().map().unwrap().as_ref(), b"second-blob");
}

#[test]
fn live_view_prevents_reclaim_until_last_guard_drops() {
    let harness = harness(256);
    let blob = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("first read");
    let view = blob.map().expect("view");
    drop(blob);

    let blocked = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    );
    assert!(matches!(blocked, Err(DataPlaneError::ArenaExhausted)));

    drop(view);
    std::thread::sleep(Duration::from_millis(10));
    let reopened = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("lease is reclaimable after the final view drops");
    assert_eq!(reopened.map().unwrap().as_ref(), WEIGHTS);
}

#[test]
fn path_absence_and_authorization_fail_before_blob_success() {
    let harness = harness(4096);
    let missing = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/missing"),
    );
    assert!(matches!(missing, Err(DataPlaneError::PathNotFound(_))));

    let unauthorized = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/runs/self/private"),
    );
    assert!(matches!(
        unauthorized,
        Err(DataPlaneError::Unauthorized { .. })
    ));
}

#[test]
fn cancelled_write_open_releases_queued_grant() {
    let harness = harness(256);
    let mut cancelled = Box::pin(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/cancelled", 8),
    );
    assert!(
        future::block_on(future::poll_once(cancelled.as_mut())).is_none(),
        "first poll only submits the actor operation"
    );
    std::thread::sleep(Duration::from_millis(10));
    drop(cancelled);
    std::thread::sleep(Duration::from_millis(10));

    let mut retry = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/cancelled", 8),
    )
    .expect("cancelled grant was reclaimed");
    future::block_on(retry.abort()).unwrap();
}

#[test]
fn write_blob_seals_once_and_abort_publishes_nothing() {
    let harness = harness(4096);
    let mut writer = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/blob", 6),
    )
    .expect("write grant");
    let mut view = writer.map().expect("writable view");
    view.copy_from_slice(b"result");
    assert!(matches!(
        future::block_on(writer.seal()),
        Err(DataPlaneError::Blob(
            data_plane::protocol::BlobFailure::ActiveWritableView
        ))
    ));
    drop(view);
    future::block_on(writer.seal()).expect("seal and publish");

    let published = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/runs/self/results/blob"),
    )
    .expect("clean exit publishes exactly once");
    assert_eq!(published.length(), 6);
    assert_eq!(published.map().unwrap().as_ref(), b"result");
    assert!(future::block_on(writer.seal()).is_err());

    let mut aborted = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/aborted", 4),
    )
    .expect("abort grant");
    let active = aborted.map().expect("active writable view");
    assert!(future::block_on(aborted.abort()).is_err());
    drop(active);
    future::block_on(aborted.abort()).expect("abort after view closes");
    assert!(matches!(
        future::block_on(
            harness
                .bootstrap
                .data_plane
                .read_blob_path("/runs/self/results/aborted")
        ),
        Err(DataPlaneError::PathNotFound(_))
    ));
}

#[test]
fn stream_endpoints_open_only_after_match_and_deliver_eof_in_order() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/inference");

    future::block_on(async {
        let mut reader_open = Box::pin(data_plane.read_stream(&logical));
        assert!(
            future::poll_once(reader_open.as_mut()).await.is_none(),
            "reader open waits for its source"
        );
        let mut writer_open = Box::pin(data_plane.write_stream(&logical));
        let mut writer = writer_open.as_mut().await.expect("writer opens");
        let mut reader = reader_open.await.expect("reader opens");

        writer.write(b"first").await.expect("write first");
        writer.write(b"second").await.expect("write second");
        assert_eq!(
            reader.read().await.expect("read first"),
            Some(b"first".to_vec())
        );
        assert_eq!(
            reader.read().await.expect("read second"),
            Some(b"second".to_vec())
        );
        writer.close().await.expect("clean writer close");
        assert!(matches!(
            writer.write(b"late").await,
            Err(DataPlaneError::StreamClosed)
        ));
        assert_eq!(reader.read().await.expect("read eof"), None);
        assert_eq!(reader.read().await.expect("sticky eof"), None);
    });
}

#[test]
fn stream_writer_suspends_until_reader_releases_bounded_capacity() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/backpressure");

    future::block_on(async {
        let mut reader_open = Box::pin(data_plane.read_stream(&logical));
        assert!(future::poll_once(reader_open.as_mut()).await.is_none());
        let mut writer = data_plane
            .write_stream(&logical)
            .await
            .expect("writer opens");
        let mut reader = reader_open.await.expect("reader opens");

        let capacity = writer.capacity() as usize;
        let payload: Vec<u8> = (0..(capacity * 2 + 97))
            .map(|index| (index % 251) as u8)
            .collect();
        let mut writing = Box::pin(writer.write(&payload));
        assert!(
            future::poll_once(writing.as_mut()).await.is_none(),
            "bounded source and destination rings must eventually suspend the writer"
        );

        let mut observed = reader
            .read()
            .await
            .expect("read releases destination capacity")
            .expect("first data");
        writing.await.expect("writer resumes");
        while observed.len() < payload.len() {
            observed.extend(
                reader
                    .read()
                    .await
                    .expect("read remaining")
                    .expect("remaining data"),
            );
        }
        assert_eq!(observed, payload);
        writer.close().await.expect("close");
        assert_eq!(reader.read().await.expect("eof"), None);
    });
}
#[test]
fn transport_startup_failure_faults_both_pending_opens() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness_with_transport(2 << 20, Arc::new(RejectingStreamTransport));
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/faulted");

    future::block_on(async {
        let mut reader_open = Box::pin(data_plane.read_stream(&logical));
        assert!(future::poll_once(reader_open.as_mut()).await.is_none());
        let writer_error = match data_plane.write_stream(&logical).await {
            Ok(_) => panic!("writer must not open when transport setup fails"),
            Err(error) => error,
        };
        let reader_error = match reader_open.await {
            Ok(_) => panic!("reader must not open when transport setup fails"),
            Err(error) => error,
        };
        assert!(matches!(
            writer_error,
            DataPlaneError::PeerLost | DataPlaneError::StreamFault(_)
        ));
        assert!(matches!(
            reader_error,
            DataPlaneError::PeerLost | DataPlaneError::StreamFault(_)
        ));
    });
}

#[test]
fn peer_replacement_requires_and_supports_a_fresh_incarnation() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/failover");

    future::block_on(async {
        let mut first_reader_open = Box::pin(data_plane.read_stream(&logical));
        assert!(
            future::poll_once(first_reader_open.as_mut())
                .await
                .is_none()
        );
        let mut first_writer = data_plane
            .write_stream(&logical)
            .await
            .expect("first writer");
        let mut first_reader = first_reader_open.await.expect("first reader");
        first_writer.write(b"old").await.expect("old write");
        assert_eq!(
            first_reader.read().await.expect("old read"),
            Some(b"old".to_vec())
        );
        first_writer.abort().expect("abort first incarnation");
        assert!(matches!(
            first_reader.read().await,
            Err(DataPlaneError::PeerLost)
        ));

        let mut replacement_writer_open = Box::pin(data_plane.write_stream_replacing(&logical));
        assert!(
            future::poll_once(replacement_writer_open.as_mut())
                .await
                .is_none(),
            "replacement writer waits for an explicit new reader"
        );
        let mut replacement_reader = data_plane
            .read_stream(&logical)
            .await
            .expect("replacement reader");
        let mut replacement_writer = replacement_writer_open.await.expect("replacement writer");
        replacement_writer.write(b"new").await.expect("new write");
        assert_eq!(
            replacement_reader.read().await.expect("new read"),
            Some(b"new".to_vec())
        );
        replacement_writer.close().await.expect("new close");
        assert_eq!(replacement_reader.read().await.expect("new eof"), None);
    });
}

#[test]
fn actor_stream_consumer_registers_before_writer_and_collects_to_eof() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/collector");
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let consumer: Arc<dyn data_plane::data_plane::StreamConsumer> =
        Arc::new(CollectBytes(Arc::clone(&observed)));
    let completion = data_plane
        .collect_stream(logical.clone(), consumer)
        .expect("spawn collector");

    future::block_on(async {
        let mut writer = data_plane
            .write_stream(&logical)
            .await
            .expect("writer matches collector");
        writer.write(b"actor-").await.expect("first write");
        writer.write(b"consumer").await.expect("second write");
        writer.close().await.expect("close");
    });

    completion.wait().expect("collector completes");
    assert_eq!(&*observed.lock(), b"actor-consumer");
}

#[test]
fn raw_blob_descriptor_enforces_offsets_rights_and_terminal_state() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/raw-blob");

    future::block_on(async {
        let mut writer = data_plane
            .open(&logical, OpenOptions::staged_blob(8))
            .await
            .expect("open staged descriptor");
        assert_eq!(writer.kind(), DescriptorKind::Blob);
        assert!(
            writer
                .capabilities()
                .contains(DescriptorCapabilities::WRITE)
        );
        assert_eq!(writer.write(b"abc").await.expect("first write"), 3);
        assert_eq!(writer.write(b"defgh").await.expect("second write"), 5);
        assert_eq!(writer.last_route(), Some(TransferRoute::Staged));
        assert_eq!(
            writer
                .write(b"!")
                .await
                .expect_err("growth rejected")
                .errno(),
            Errno::Enotsup
        );
        writer.close().await.expect("publish");
        assert_eq!(
            writer.close().await.expect_err("double close").errno(),
            Errno::Ebadf
        );

        let mut reader = data_plane
            .open(&logical, OpenOptions::read_only())
            .await
            .expect("open published descriptor");
        assert_eq!(reader.kind(), DescriptorKind::Blob);
        let mut first = [0xa5; 5];
        reader
            .read_exact(&mut first[..3])
            .await
            .expect("exact prefix read");
        assert_eq!(&first, b"abc\xa5\xa5");
        let mut rest = [0_u8; 8];
        assert_eq!(reader.read(&mut rest).await.expect("remaining read"), 5);
        assert_eq!(reader.read(&mut rest).await.expect("eof"), 0);
        assert_eq!(
            reader.write(b"x").await.expect_err("wrong access").errno(),
            Errno::Ebadf
        );
        assert_eq!(reader.read(&mut []).await.expect("zero length"), 0);
        reader.close().await.expect("close reader");

        let mut source = data_plane
            .open(&logical, OpenOptions::read_only())
            .await
            .expect("open arena-region source");
        let target_path = path("/runs/self/results/raw-region-target");
        let mut target = data_plane
            .open(&target_path, OpenOptions::staged_blob(8))
            .await
            .expect("open arena-region target");
        let mut target_mapping = target
            .map(MapRequest {
                protection: Protection::ReadWrite,
                sharing: Sharing::Shared,
                target: MapTarget::Host,
                offset: 0,
                length: 8,
            })
            .expect("map arena target");
        assert_eq!(target_mapping.route(), TransferRoute::Direct);
        let count = source
            .read_into(RegionSlice::arena(
                target_mapping.as_mut().expect("writable arena region"),
            ))
            .await
            .expect("read into arena region");
        assert_eq!(count, 8);
        assert_eq!(target_mapping.as_ref(), b"abcdefgh");
        drop(target_mapping);
        target.abort().await.expect("abort arena target");
        source.close().await.expect("close arena source");
        assert_eq!(
            reader
                .read(&mut rest)
                .await
                .expect_err("read after close")
                .errno(),
            Errno::Ebadf
        );
    });
}

#[test]
fn raw_stream_descriptor_hides_record_boundaries_and_preserves_eof() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/raw-stream");

    future::block_on(async {
        let typed_reader = data_plane.read_stream(&logical);
        let typed_writer = data_plane.write_stream(&logical);
        let (typed_reader, typed_writer) = future::zip(typed_reader, typed_writer).await;
        let mut typed_reader = typed_reader.expect("seed stream reader");
        let mut typed_writer = typed_writer.expect("seed stream writer");
        typed_writer.close().await.expect("seed close");
        assert_eq!(typed_reader.read().await.expect("seed eof"), None);

        let raw_reader = data_plane.open(&logical, OpenOptions::read_only());
        let raw_writer = data_plane.open(
            &logical,
            OpenOptions {
                access: AccessMode::WriteOnly,
                ..OpenOptions::default()
            },
        );
        let (raw_reader, raw_writer) = future::zip(raw_reader, raw_writer).await;
        let mut reader = raw_reader.expect("raw stream reader");
        let mut writer = raw_writer.expect("raw stream writer");
        assert_eq!(reader.kind(), DescriptorKind::Stream);
        writer
            .write_all(b"abcdefgh")
            .await
            .expect("stream write all");
        writer.close().await.expect("writer close");

        let mut chunk = [0_u8; 3];
        assert_eq!(reader.read(&mut chunk).await.expect("chunk one"), 3);
        assert_eq!(&chunk, b"abc");
        assert_eq!(reader.read(&mut chunk).await.expect("chunk two"), 3);
        assert_eq!(&chunk, b"def");
        assert_eq!(reader.read(&mut chunk).await.expect("chunk three"), 2);
        assert_eq!(&chunk[..2], b"gh");
        assert_eq!(reader.read(&mut chunk).await.expect("stream eof"), 0);
        assert_eq!(reader.read(&mut chunk).await.expect("sticky eof"), 0);
        reader.close().await.expect("reader close");
    });
}

#[test]
fn raw_blob_mapping_is_bounded_and_can_outlive_descriptor_close() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/raw-mapped-blob");

    future::block_on(async {
        let mut writer = data_plane
            .open(&logical, OpenOptions::staged_blob(6))
            .await
            .expect("open mapped writer");
        let mut mapping = writer
            .map(MapRequest {
                protection: Protection::ReadWrite,
                sharing: Sharing::Shared,
                target: MapTarget::Host,
                offset: 1,
                length: 4,
            })
            .expect("bounded writable mapping");
        assert_eq!(mapping.route(), TransferRoute::Direct);
        mapping
            .as_mut()
            .expect("writable mapping")
            .copy_from_slice(b"data");
        writer.close().await.expect("deferred close intent");
        assert_eq!(mapping.as_ref(), b"data");
        drop(mapping);

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let blob = loop {
            match data_plane.read_blob(&logical).await {
                Ok(blob) => break blob,
                Err(DataPlaneError::PathNotFound(_)) if std::time::Instant::now() < deadline => {
                    future::yield_now().await;
                }
                Err(error) => panic!("deferred publication failed: {error}"),
            }
        };
        let view = blob.map().expect("published mapping");
        assert_eq!(&view[..], b"\0data\0");
    });
}
