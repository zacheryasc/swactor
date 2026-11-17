use super::streams::{open_reader, open_writer};
use super::*;

pub(super) struct RouteAction {
    inputs: Vec<usize>,
    rotation: usize,
}

pub(super) enum Endpoint {
    Writer {
        writer: StreamWriter,
        origin: Option<u64>,
    },
    Reader(StreamReader),
}

impl Endpoint {
    pub fn incarnation(&self) -> u64 {
        match self {
            Self::Writer { writer, .. } => writer.incarnation().revision,
            Self::Reader(reader) => reader.incarnation().revision,
        }
    }
}

pub(super) fn read_expected(operation: &ActionOp) -> Option<&[u8]> {
    match operation {
        ActionOp::ReadBlob { expected, .. }
        | ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. } => Some(expected),
        _ => None,
    }
}

pub(super) fn plan(
    case: &BehaviorCase,
    program: &ProcessProgram,
) -> Result<Vec<Option<RouteAction>>> {
    program
        .actions
        .iter()
        .enumerate()
        .map(|(step, action)| {
            let reading = read_expected(&action.operation).is_some();
            if !reading
                && !matches!(
                    action.operation,
                    ActionOp::PublishBlob { .. }
                        | ActionOp::StreamWrite { .. }
                        | ActionOp::GatedStreamWrite { .. }
                )
            {
                return Ok(None);
            }
            let Some((route, edge_index)) = case.routes.iter().find_map(|route| {
                route
                    .edges
                    .iter()
                    .position(|edge| {
                        edge.path == action.operation.path()
                            && if reading {
                                edge.destination_role == program.id
                            } else {
                                edge.source_role == program.id
                            }
                    })
                    .map(|index| (route, index))
            }) else {
                return Ok(None);
            };
            let mut inputs = Vec::new();
            let mut rotation = 0;
            if !reading {
                if case.topology == TopologyFamily::RingWalk && route.edges.len() > 1 {
                    if edge_index != 0 {
                        inputs.push(
                            program.actions[..step]
                                .iter()
                                .rposition(|prior| {
                                    read_expected(&prior.operation).is_some()
                                        && route
                                            .edges
                                            .iter()
                                            .any(|edge| edge.path == prior.operation.path())
                                })
                                .ok_or_else(|| {
                                    NativeError::protocol(
                                        "ring relay requires a preceding route read",
                                    )
                                })?,
                        );
                    }
                } else {
                    let source = &route.edges[edge_index].source_role;
                    for incoming in route
                        .edges
                        .iter()
                        .filter(|edge| &edge.destination_role == source)
                    {
                        inputs.push(
                            program.actions[..step]
                                .iter()
                                .rposition(|prior| {
                                    read_expected(&prior.operation).is_some()
                                        && prior.operation.path() == incoming.path
                                })
                                .ok_or_else(|| {
                                    NativeError::protocol(
                                        "DAG relay requires every preceding input read",
                                    )
                                })?,
                        );
                    }
                    if !inputs.is_empty() {
                        rotation = edge_index + 1;
                    }
                }
            }
            Ok(Some(RouteAction { inputs, rotation }))
        })
        .collect()
}

pub(super) fn first_route(
    case: &BehaviorCase,
    program: &ProcessProgram,
    routes: &[Option<RouteAction>],
) -> Option<usize> {
    program.actions.iter().zip(routes).position(|(action, route)| {
        route.is_some() || matches!(&action.operation, ActionOp::AwaitEntry { path, .. }
            if case.routes.iter().any(|route| route.edges.iter().any(|edge| edge.destination_role == program.id && edge.path == *path)))
    })
}

#[derive(Default)]
pub(super) struct PreparedRoutes {
    pub endpoints: BTreeMap<usize, Endpoint>,
    received: BTreeMap<usize, Vec<u8>>,
}

impl PreparedRoutes {
    pub async fn prepare(
        &mut self,
        data: &DataPlane,
        program: &ProcessProgram,
        routes: &[Option<RouteAction>],
        budget: Budget<'_>,
        emit: &Emitter<'_>,
    ) -> Result<()> {
        let mut opens = FuturesUnordered::new();
        for (step, (action, route)) in program.actions.iter().zip(routes).enumerate() {
            if route.is_none() {
                continue;
            }
            let (write, replace, retry) = match &action.operation {
                ActionOp::StreamWrite { replace, .. }
                | ActionOp::GatedStreamWrite { replace, .. } => (true, *replace, false),
                ActionOp::StreamReadWithRetry { .. } => (false, false, true),
                ActionOp::GatedStreamRead { retry_attach, .. } => (false, false, *retry_attach),
                ActionOp::StreamRead { .. } | ActionOp::StreamReadInto { .. } => {
                    (false, false, false)
                }
                _ => continue,
            };
            opens.push(async move {
                let value = path(action.operation.path())?;
                let endpoint = if write {
                    let origin = if replace {
                        mutation_origin(data, &value).await
                    } else {
                        None
                    };
                    Endpoint::Writer {
                        writer: open_writer(data, &value, replace).await?,
                        origin,
                    }
                } else {
                    Endpoint::Reader(open_reader(data, &value, retry, budget).await?)
                };
                barrier(
                    emit,
                    &observation(&program.id, step, action),
                    BarrierObservation::StreamOpened {
                        incarnation: endpoint.incarnation(),
                    },
                )?;
                Ok::<_, NativeError>((step, endpoint))
            });
        }
        // Poll every open concurrently. Sequential attachment deadlocks rings and
        // fan-in/out because stream opening itself is a peer rendezvous.
        while let Some(opened) = opens.next().await {
            let (step, endpoint) = opened?;
            self.endpoints.insert(step, endpoint);
        }
        Ok(())
    }

    pub fn output<'a>(&'a self, route: &RouteAction, original: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        if route.inputs.is_empty() {
            return Ok(Cow::Borrowed(original));
        }
        self.relay(route, original.len())
    }

    pub fn frames<'a>(
        &'a self,
        route: &RouteAction,
        frames: &[Vec<u8>],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if route.inputs.is_empty() {
            return Ok(None);
        }
        self.relay(route, frames.iter().map(Vec::len).sum())
            .map(Some)
    }

    fn relay(&self, route: &RouteAction, length: usize) -> Result<Cow<'_, [u8]>> {
        let actual = route.inputs.iter().try_fold(0usize, |length, step| {
            self.received
                .get(step)
                .map(|payload| length + payload.len())
                .ok_or_else(|| NativeError::protocol("route input has not completed"))
        })?;
        if actual != length {
            return Err(NativeError::protocol("route relay length mismatch"));
        }
        if route.inputs.len() == 1 && route.rotation == 0 {
            return Ok(Cow::Borrowed(&self.received[&route.inputs[0]]));
        }
        let mut output = Vec::with_capacity(length);
        for step in &route.inputs {
            output.extend_from_slice(&self.received[step]);
        }
        if length != 0 {
            output.rotate_left(route.rotation % length);
        }
        Ok(Cow::Owned(output))
    }

    pub fn retain(&mut self, step: usize, payload: Vec<u8>, expected: &[u8]) -> Result<()> {
        if payload.len() != expected.len() || digest(&payload) != digest(expected) {
            return Err(NativeError::protocol(
                "route read length or digest mismatch",
            ));
        }
        self.received.insert(step, payload);
        Ok(())
    }
}
