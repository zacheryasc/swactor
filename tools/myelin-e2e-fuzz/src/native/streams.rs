use super::*;

pub(super) struct FramedReader<'a> {
    pub reader: StreamReader,
    remaining: usize,
    frame_limit: usize,
    pub index: u64,
    buffers: &'a [usize],
    read_index: usize,
    pending: Vec<u8>,
    position: usize,
}

impl<'a> FramedReader<'a> {
    pub fn new(
        reader: StreamReader,
        remaining: usize,
        frame_limit: usize,
        buffers: &'a [usize],
    ) -> Self {
        Self {
            reader,
            remaining,
            frame_limit,
            index: 0,
            buffers,
            read_index: 0,
            pending: Vec::new(),
            position: 0,
        }
    }

    async fn fill(&mut self, destination: &mut [u8]) -> Result<usize> {
        let mut filled = 0;
        while filled < destination.len() {
            if !self.buffers.is_empty() {
                let capacity = (destination.len() - filled)
                    .min(self.buffers[self.read_index % self.buffers.len()]);
                self.read_index += 1;
                if capacity == 0 {
                    return Err(NativeError::protocol("zero stream readinto buffer"));
                }
                let count = self
                    .reader
                    .read_into(&mut destination[filled..filled + capacity])
                    .await?;
                if count == 0 {
                    break;
                }
                filled += count;
            } else {
                if self.position == self.pending.len() {
                    match self.reader.read().await? {
                        Some(bytes) => {
                            self.pending = bytes;
                            self.position = 0;
                        }
                        None => break,
                    }
                }
                let count = (destination.len() - filled).min(self.pending.len() - self.position);
                destination[filled..filled + count]
                    .copy_from_slice(&self.pending[self.position..self.position + count]);
                self.position += count;
                filled += count;
            }
        }
        Ok(filled)
    }

    pub async fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let mut header = [0; 16];
        let count = self.fill(&mut header).await?;
        if count == 0 {
            return Ok(None);
        }
        if count != header.len() {
            return Err(NativeError::protocol(
                "truncated logical stream frame header",
            ));
        }
        let index = u64::from_le_bytes(header[..8].try_into().expect("fixed header"));
        let length = u64::from_le_bytes(header[8..].try_into().expect("fixed header"));
        if index != self.index {
            return Err(NativeError::protocol(
                "duplicate, reordered, or missing logical stream frame",
            ));
        }
        if index >= self.frame_limit as u64 || length > self.remaining as u64 {
            return Err(NativeError::protocol(
                "logical stream frame exceeds generated bounds",
            ));
        }
        let mut payload = vec![0; length as usize];
        if self.fill(&mut payload).await? != payload.len() {
            return Err(NativeError::protocol(
                "truncated logical stream frame payload",
            ));
        }
        self.index += 1;
        self.remaining -= payload.len();
        Ok(Some(payload))
    }
}

pub(super) async fn open_writer(
    data: &DataPlane,
    value: &DataPath,
    replace: bool,
) -> Result<StreamWriter> {
    Ok(if replace {
        data.write_stream_replacing(value).await?
    } else {
        data.write_stream(value).await?
    })
}

pub(super) async fn open_reader(
    data: &DataPlane,
    value: &DataPath,
    retry: bool,
    budget: Budget<'_>,
) -> Result<StreamReader> {
    let mut delay = Duration::from_millis(1);
    loop {
        match data.read_stream(value).await {
            Ok(reader) => return Ok(reader),
            Err(_) if retry => budget.pace(&mut delay, Duration::from_millis(10)).await?,
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) async fn write_frames(
    writer: &mut StreamWriter,
    frames: &[Vec<u8>],
    relayed: Option<&[u8]>,
    frame_limit: usize,
    gate: Option<(&DataPlane, &str, Budget<'_>)>,
    record: &ActionObservation,
    evidence: &mut Evidence,
    emit: &Emitter<'_>,
) -> Result<()> {
    let incarnation = writer.incarnation().revision;
    let mut offset = 0;
    for (index, frame) in frames.iter().enumerate() {
        if index >= frame_limit {
            return Err(NativeError::protocol(
                "logical stream frame exceeds generated bounds",
            ));
        }
        let payload = match relayed {
            Some(bytes) => &bytes[offset..offset + frame.len()],
            None => frame.as_slice(),
        };
        offset += payload.len();
        let mut header = [0; 16];
        header[..8].copy_from_slice(&(index as u64).to_le_bytes());
        header[8..].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        writer.write(&header).await?;
        writer.write(payload).await?;
        let mut frame_record = record.clone();
        frame_record.action = "stream_write".into();
        barrier(
            emit,
            &frame_record,
            BarrierObservation::StreamFrame {
                incarnation,
                index: index as u64,
                length: payload.len() as u64,
                digest: digest(payload),
            },
        )?;
        evidence.transferred(payload, false);
        if index == 0
            && let Some((data, release_path, budget)) = gate
        {
            wait_entry(data, &path(release_path)?, budget, false).await?;
            barrier(
                emit,
                record,
                BarrierObservation::ReleaseObserved {
                    path: release_path.into(),
                },
            )?;
        }
    }
    evidence.transferred(&[], true);
    writer.close().await?;
    Ok(())
}

pub(super) async fn read_frames(
    mut framed: FramedReader<'_>,
    first: Option<Vec<u8>>,
    gated: Option<(&DataPlane, &str, bool)>,
    retain: bool,
    record: &ActionObservation,
    evidence: &mut Evidence,
    emit: &Emitter<'_>,
) -> Result<Vec<u8>> {
    let incarnation = framed.reader.incarnation().revision;
    let mut retained = if retain {
        Vec::with_capacity(framed.remaining + first.as_ref().map_or(0, Vec::len))
    } else {
        Vec::new()
    };
    let mut first = first;
    loop {
        let chunk = if first.is_some() {
            first.take()
        } else {
            framed.read().await?
        };
        let Some(chunk) = chunk else {
            if gated.is_some() && framed.index == 0 {
                return Err(NativeError::protocol(
                    "hot stream ended before its first frame",
                ));
            }
            evidence.transferred(&[], true);
            barrier(emit, record, BarrierObservation::StreamEof { incarnation })?;
            break;
        };
        let mut frame_record = record.clone();
        frame_record.action = "stream_read".into();
        barrier(
            emit,
            &frame_record,
            BarrierObservation::StreamFrame {
                incarnation,
                index: framed.index - 1,
                length: chunk.len() as u64,
                digest: digest(&chunk),
            },
        )?;
        evidence.transferred(&chunk, false);
        if framed.index == 1 {
            if let Some((data, observed_path, _)) = gated {
                publish(data, &path(observed_path)?, &[], &mut Evidence::default()).await?;
            }
            barrier(
                emit,
                record,
                BarrierObservation::StreamFirstFrame { incarnation },
            )?;
            if gated.is_some_and(|(_, _, park)| park) {
                std::future::pending::<()>().await;
            }
        }
        if retain {
            retained.extend_from_slice(&chunk);
        }
    }
    Ok(retained)
}

pub(super) async fn retry_first_frame<'a>(
    data: &DataPlane,
    value: &DataPath,
    expected: &[u8],
    limit: usize,
    budget: Budget<'_>,
) -> Result<(FramedReader<'a>, Vec<u8>)> {
    let mut delay = Duration::from_millis(1);
    loop {
        // Discard stale generations before emitting any accepted-incarnation evidence.
        let attempt = async {
            let reader = data.read_stream(value).await?;
            let mut framed = FramedReader::new(reader, expected.len(), limit, &[]);
            match framed.read().await? {
                Some(first) if expected.starts_with(&first) => Ok((framed, first)),
                _ => Err(NativeError::protocol(
                    "replacement first frame did not match expected prefix",
                )),
            }
        }
        .await;
        if let Ok(accepted) = attempt {
            return Ok(accepted);
        }
        budget.pace(&mut delay, Duration::from_millis(10)).await?;
    }
}
