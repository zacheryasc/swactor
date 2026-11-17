use super::routes::{Endpoint, PreparedRoutes, RouteAction};
use super::streams::{
    FramedReader, open_reader, open_writer, read_frames, retry_first_frame, write_frames,
};
use super::*;

pub(super) async fn execute(
    data: &DataPlane,
    action: &Action,
    record: &mut ActionObservation,
    evidence: &mut Evidence,
    route: Option<&RouteAction>,
    routes: &mut PreparedRoutes,
    budget: Budget<'_>,
    frame_limit: usize,
    emit: &Emitter<'_>,
) -> Result<()> {
    let value = path(action.operation.path())?;
    let step = record.step;
    match &action.operation {
        ActionOp::PublishBlob { bytes, .. } => {
            let payload = match route {
                Some(route) => routes.output(route, bytes)?,
                None => Cow::Borrowed(bytes.as_slice()),
            };
            publish(data, &value, &payload, evidence).await?;
        }
        ActionOp::ReadBlob { expected, .. } => {
            let blob = loop {
                let attempt = Budget {
                    deadline: budget
                        .deadline
                        .min(budget.engine.now().to_instant() + Duration::from_secs(3)),
                    ..budget
                };
                match attempt
                    .bound(async { data.read_blob(&value).await.map_err(Into::into) })
                    .await
                {
                    Err(error) if error.errno == Some(libc::ETIMEDOUT) => {
                        budget.remaining()?;
                    }
                    result => break result?,
                }
            };
            let view = blob.map().map_err(DataPlaneError::from)?;
            evidence.transferred(view.as_ref(), true);
            if route.is_some() {
                routes.retain(step, view.as_ref().to_vec(), expected)?;
            }
        }
        ActionOp::StreamWrite {
            chunks, replace, ..
        }
        | ActionOp::GatedStreamWrite {
            frames: chunks,
            replace,
            ..
        } => {
            let endpoint = routes.endpoints.remove(&step);
            let (mut writer, origin) = match endpoint {
                Some(Endpoint::Writer { writer, origin }) => (writer, origin),
                Some(Endpoint::Reader(_)) => {
                    return Err(NativeError::protocol(
                        "route writer received reader endpoint",
                    ));
                }
                None => {
                    let origin = if *replace {
                        mutation_origin(data, &value).await
                    } else {
                        None
                    };
                    let writer = open_writer(data, &value, *replace).await?;
                    barrier(
                        emit,
                        record,
                        BarrierObservation::StreamOpened {
                            incarnation: writer.incarnation().revision,
                        },
                    )?;
                    (writer, origin)
                }
            };
            let incarnation = writer.incarnation().revision;
            evidence.incarnation = Some(incarnation);
            let payload = route
                .map(|route| routes.frames(route, chunks))
                .transpose()?
                .flatten();
            let gate = match &action.operation {
                ActionOp::GatedStreamWrite { release_path, .. } => {
                    Some((data, release_path.as_str(), budget))
                }
                _ => None,
            };
            write_frames(
                &mut writer,
                chunks,
                payload.as_deref(),
                frame_limit,
                gate,
                record,
                evidence,
                emit,
            )
            .await?;
            if *replace {
                barrier(
                    emit,
                    record,
                    BarrierObservation::MutationApplied {
                        from_revision: origin,
                        to_revision: Some(incarnation),
                    },
                )?;
            }
        }
        ActionOp::StreamRoundTrip { chunks, .. } => {
            let (mut writer, reader) = try_join(
                open_writer(data, &value, false),
                open_reader(data, &value, false, budget),
            )
            .await?;
            let incarnation = writer.incarnation();
            if incarnation != reader.incarnation() {
                return Err(NativeError::protocol(
                    "stream round trip incarnation mismatch",
                ));
            }
            evidence.incarnation = Some(incarnation.revision);
            barrier(
                emit,
                record,
                BarrierObservation::StreamOpened {
                    incarnation: incarnation.revision,
                },
            )?;
            let length = chunks.iter().map(Vec::len).sum();
            let mut written = Evidence::default();
            try_join(
                write_frames(
                    &mut writer,
                    chunks,
                    None,
                    frame_limit,
                    None,
                    record,
                    &mut written,
                    emit,
                ),
                read_frames(
                    FramedReader::new(reader, length, frame_limit, &[]),
                    None,
                    None,
                    false,
                    record,
                    evidence,
                    emit,
                ),
            )
            .await?;
            let mut expected = Sha256::new();
            for chunk in chunks {
                expected.update(chunk);
            }
            let transfer = evidence
                .transfer
                .as_ref()
                .ok_or_else(|| NativeError::protocol("round trip yielded no transfer"))?;
            if transfer.length != length
                || transfer.hasher.clone().finalize() != expected.finalize()
            {
                return Err(NativeError::protocol("stream round trip payload mismatch"));
            }
        }
        ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. } => {
            let buffers = match &action.operation {
                ActionOp::StreamReadInto { buffer_sizes, .. } => buffer_sizes.as_slice(),
                _ => &[],
            };
            let (framed, first) = match routes.endpoints.remove(&step) {
                Some(Endpoint::Reader(reader)) => (
                    FramedReader::new(reader, expected.len(), frame_limit, buffers),
                    None,
                ),
                Some(Endpoint::Writer { .. }) => {
                    return Err(NativeError::protocol(
                        "route reader received writer endpoint",
                    ));
                }
                None => {
                    let (framed, first) = if matches!(
                        action.operation,
                        ActionOp::GatedStreamRead {
                            retry_attach: true,
                            ..
                        }
                    ) {
                        let (framed, first) =
                            retry_first_frame(data, &value, expected, frame_limit, budget).await?;
                        (framed, Some(first))
                    } else {
                        let retry =
                            matches!(action.operation, ActionOp::StreamReadWithRetry { .. });
                        let reader = open_reader(data, &value, retry, budget).await?;
                        (
                            FramedReader::new(reader, expected.len(), frame_limit, buffers),
                            None,
                        )
                    };
                    barrier(
                        emit,
                        record,
                        BarrierObservation::StreamOpened {
                            incarnation: framed.reader.incarnation().revision,
                        },
                    )?;
                    (framed, first)
                }
            };
            evidence.incarnation = Some(framed.reader.incarnation().revision);
            let gated = match &action.operation {
                ActionOp::GatedStreamRead {
                    observed_path,
                    park_after_first_frame,
                    ..
                } => Some((data, observed_path.as_str(), *park_after_first_frame)),
                _ => None,
            };
            let payload = read_frames(
                framed,
                first,
                gated,
                route.is_some(),
                record,
                evidence,
                emit,
            )
            .await?;
            if route.is_some() {
                routes.retain(step, payload, expected)?;
            }
        }
        ActionOp::Lookup { .. }
        | ActionOp::AwaitEntry { .. }
        | ActionOp::WaitForQuiescent { .. } => {
            let entry = match &action.operation {
                ActionOp::Lookup { .. } => data.lookup(&value).await?,
                ActionOp::AwaitEntry { .. } => wait_entry(data, &value, budget, false).await?,
                _ => wait_entry(data, &value, budget, true).await?,
            };
            record.kind = Some(
                match entry.kind {
                    EntryKind::Blob => "blob",
                    EntryKind::Stream => "stream",
                }
                .into(),
            );
            record.revision = Some(entry.revision);
            record.active = Some(entry.active);
        }
        ActionOp::Rename {
            destination,
            replace,
            ..
        } => {
            let origin = mutation_origin(data, &value).await;
            let revision = data.rename(&value, &path(destination)?, *replace).await?;
            barrier(
                emit,
                record,
                BarrierObservation::MutationApplied {
                    from_revision: origin,
                    to_revision: Some(revision),
                },
            )?;
            record.revision = Some(revision);
        }
        ActionOp::Unlink { .. } => {
            let origin = mutation_origin(data, &value).await;
            let revision = data.unlink(&value).await?;
            barrier(
                emit,
                record,
                BarrierObservation::MutationApplied {
                    from_revision: origin,
                    to_revision: Some(revision),
                },
            )?;
            record.revision = Some(revision);
        }
        ActionOp::DescriptorWrite {
            flags,
            length,
            bytes,
            method,
            finish,
            ..
        } => {
            let mut descriptor = data.open(&value, open_options(*flags, *length)?).await?;
            match method {
                DescriptorWriteMethod::Write => {
                    let count = descriptor.write(bytes).await?;
                    evidence.transferred(&bytes[..count], true);
                }
                DescriptorWriteMethod::WriteFrom => {
                    // RegionSlice owns writable access even for a read-only source;
                    // retain the immutable IR input by providing one host buffer.
                    let mut source = bytes.clone();
                    let count = descriptor
                        .write_from(RegionSlice::host(&mut source))
                        .await?;
                    evidence.transferred(&source[..count], true);
                }
                DescriptorWriteMethod::Mapping => {
                    let mut mapping = descriptor.map(map_request(0, bytes.len(), true))?;
                    mapping.as_mut()?.copy_from_slice(bytes);
                    evidence.transferred(mapping.as_ref(), true);
                }
            }
            evidence.descriptor = Some(DescriptorObservation::Write {
                method: *method,
                finish: *finish,
                terminal_results: Vec::new(),
                dropped: false,
                reservation_released: false,
            });
            finish_descriptor(descriptor, *finish, evidence).await?;
            if *finish == DescriptorFinish::Drop {
                observe_drop_release(data, &value, budget).await?;
                if let Some(DescriptorObservation::Write {
                    reservation_released,
                    ..
                }) = evidence.descriptor.as_mut()
                {
                    *reservation_released = true;
                }
            }
        }
        ActionOp::DescriptorRead {
            flags,
            expected,
            method,
            offset,
            finish,
            ..
        } => {
            let mut descriptor = data.open(&value, open_options(*flags, None)?).await?;
            match method {
                DescriptorReadMethod::Read | DescriptorReadMethod::ReadInto => {
                    let mut target = vec![0; expected.len()];
                    let count = if *method == DescriptorReadMethod::Read {
                        descriptor.read(&mut target).await?
                    } else {
                        descriptor.read_into(RegionSlice::host(&mut target)).await?
                    };
                    evidence.transferred(&target[..count], true);
                }
                DescriptorReadMethod::Mapping => {
                    let mapping = descriptor.map(map_request(*offset, expected.len(), false))?;
                    evidence.transferred(mapping.as_ref(), true);
                }
            }
            evidence.descriptor = Some(DescriptorObservation::Read {
                method: *method,
                finish: *finish,
                terminal_results: Vec::new(),
            });
            finish_descriptor(descriptor, *finish, evidence).await?;
        }
    }
    Ok(())
}
