//! Rendering of typed action IR into executable Python programs.

use std::fmt::Write as _;
use std::time::Duration;

use sha2::{Digest as _, Sha256};

use crate::ir::{
    Action, ActionClass, ActionOp, BARRIER_LAP_COMPLETED, BARRIER_TOKEN_FORWARDED,
    BARRIER_TOKEN_RECEIVED, BehaviorCase, DescriptorFinish, DescriptorReadMethod,
    DescriptorWriteMethod, EVIDENCE_HINT_BARRIER, EVIDENCE_HINT_EDGE_INDEX, EVIDENCE_HINT_LAP,
    EVIDENCE_HINT_TOKEN, ExpectedOutcome, FailureInjection, LaunchFailureKind, ProcessProgram,
    TopologyFamily,
};

/// Per-action ceiling, subordinate to the process owner's remaining budget.
const NAMESPACE_BARRIER_DEADLINE_SECONDS: f64 = 30.0;

// The public binding has no predicate subscription yet. Poll immediately, then
// pace adaptively; cancelling the actual binding future propagates into its Rust
// request owner. The harness also stops/reaps the contextual process on expiry,
// including a misbehaving coroutine that refuses cancellation.
const BUDGET_PYTHON: &str = r#"
def remaining(deadline, predicate):
    value = deadline - asyncio.get_running_loop().time()
    if value <= 0:
        raise TimeoutError(f'budget expired: {predicate}')
    return value

def consume_cancelled(task):
    if not task.cancelled():
        task.exception()

async def bounded(operation, deadline, predicate):
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=remaining(deadline, predicate))
        if not done:
            raise TimeoutError(f'budget expired: {predicate}')
        remaining(deadline, predicate)
        return task.result()
    finally:
        if not task.done():
            task.cancel()
            task.add_done_callback(consume_cancelled)
        else:
            consume_cancelled(task)

async def pace(deadline, predicate, delay, cap=0.01):
    await asyncio.sleep(min(delay, remaining(deadline, predicate)))
    remaining(deadline, predicate)
    return min(delay * 2, cap)

def timing(predicate, started, requests, wakes, pending, kind='generated_wait'):
    # Successful action output already carries the typed behavioral record.
    # Emit a second stderr record only for waits or a failed/timed-out action.
    if kind == 'generated_action' and not pending:
        return
    record = {'type': kind, 'predicate': predicate,
              'elapsed_seconds': asyncio.get_running_loop().time() - started,
              'pending': pending}
    if requests is not None:
        record.update(request_count=requests, wake_count=wakes)
    print(json.dumps(record, separators=(',', ':')), file=sys.stderr)
async def wait_entry(data, path, deadline, quiescent=False):
    predicate = f'{"quiescence" if quiescent else "publication"}: {path}'
    started = asyncio.get_running_loop().time()
    delay, requests, wakes, pending = 0.001, 0, 0, True
    try:
        while True:
            requests += 1
            try:
                entry = await bounded(data.lookup(path), deadline, predicate)
            except OSError as error:
                if quiescent or error.errno != errno.ENOENT:
                    raise
            else:
                if not quiescent:
                    pending = False
                    return entry
                if entry.kind != 'stream':
                    raise RuntimeError(f'expected stream namespace node, observed {entry.kind}')
                if not entry.active:
                    pending = False
                    return entry
            delay = await pace(deadline, predicate, delay, 0.1 if quiescent else 0.01)
            wakes += 1
    finally:
        timing(predicate, started, requests, wakes, pending)
"#;

const TRANSFER_PYTHON: &str = r#"
class Transfer:
    def __init__(self):
        self.length = 0
        self.hasher = hashlib.sha256()
        self.complete = False

    def facts(self):
        return {'length': self.length, 'digest': self.hasher.hexdigest(), 'complete': self.complete}

def transferred(previous, payload, complete=False):
    evidence = previous if previous is not None else Transfer()
    evidence.length += len(payload)
    evidence.hasher.update(payload)
    evidence.complete = complete
    return evidence

def transfer_facts(evidence):
    return None if evidence is None else evidence.facts()

async def read_blob_retry(data, path, deadline):
    predicate = f'blob transfer: {path}'
    while True:
        attempt_deadline = min(deadline, asyncio.get_running_loop().time() + 3)
        try:
            return await bounded(data.read_blob(path), attempt_deadline, predicate)
        except TimeoutError:
            remaining(deadline, predicate)

async def observe_drop_release(data, path, deadline):
    delay = 0.001
    while True:
        try:
            await bounded(data.lookup(path), deadline, 'dropped writer nonpublication')
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
        else:
            raise RuntimeError('dropped writer published a blob')
        try:
            probe = await bounded(data.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC, length=0), deadline, 'dropped writer reservation release')
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
            delay = await pace(deadline, 'dropped writer reservation release', delay)
        else:
            await bounded(probe.abort(), deadline, 'release reservation probe')
            return
"#;

// The decoder owns only a fixed header and one validated logical payload.
// Transport reads may split or coalesce either; their boundaries are immaterial.
const FRAMING_PYTHON: &str = r#"
class FramedWriter:
    def __init__(self, writer, limit, step, path, **hints):
        self.writer, self.limit = writer, limit
        self.step, self.path, self.hints = step, path, hints
        self.index = 0

    async def write(self, payload):
        length = len(payload)
        if length > self.limit or self.index >= STREAM_FRAME_LIMIT:
            raise RuntimeError('logical stream frame exceeds generated bounds')
        await self.writer.write(struct.pack('<QQ', self.index, length))
        if length:
            await self.writer.write(payload)
        emit(self.step, 'stream_write', self.path, 'barrier',
             barrier={'type': 'stream_frame', 'incarnation': self.writer.incarnation,
                      'index': self.index, 'length': length, 'digest': digest(payload)},
             **self.hints)
        self.index += 1

class FramedReader:
    def __init__(self, reader, limit, step, path, buffer_sizes=None, **hints):
        self.reader, self.remaining = reader, limit
        self.step, self.path, self.hints = step, path, hints
        self.buffer_sizes, self.read_index = buffer_sizes, 0
        self.pending = memoryview(b'')
        self.header = bytearray(16)
        self.index = 0

    async def fill(self, target):
        position = 0
        with memoryview(target) as view:
            while position < len(view):
                if self.buffer_sizes is not None:
                    capacity = min(len(view) - position,
                                   self.buffer_sizes[self.read_index % len(self.buffer_sizes)])
                    with view[position:position + capacity] as destination:
                        count = await self.reader.readinto(destination)
                    self.read_index += 1
                    if count < 0 or count > capacity:
                        raise RuntimeError('invalid stream readinto count')
                    if count == 0:
                        return position
                else:
                    while not self.pending:
                        self.pending.release()
                        chunk = await self.reader.read()
                        if chunk is None:
                            self.pending = memoryview(b'')
                            return position
                        self.pending = memoryview(chunk)
                        del chunk
                    count = min(len(view) - position, len(self.pending))
                    view[position:position + count] = self.pending[:count]
                    previous = self.pending
                    self.pending = previous[count:]
                    previous.release()
                position += count
        return position

    async def read(self, record=True):
        count = await self.fill(self.header)
        if count == 0:
            return None
        if count != len(self.header):
            raise RuntimeError('truncated logical stream frame header')
        index, length = struct.unpack('<QQ', self.header)
        if index != self.index:
            raise RuntimeError('duplicate, reordered, or missing logical stream frame')
        if index >= STREAM_FRAME_LIMIT or length > self.remaining:
            raise RuntimeError('logical stream frame exceeds generated bounds')
        payload = bytearray(length)
        if await self.fill(payload) != length:
            raise RuntimeError('truncated logical stream frame payload')
        if record:
            emit(self.step, 'stream_read', self.path, 'barrier',
                 barrier={'type': 'stream_frame', 'incarnation': self.reader.incarnation,
                          'index': index, 'length': length, 'digest': digest(payload)},
                 **self.hints)
        self.index += 1
        self.remaining -= length
        return payload
"#;

// Endpoint-owner tasks keep the public write context alive across preparation
// and transfer. Reader ownership is released by dropping the public reader;
// the binding deliberately does not expose a reader close method.
const ROUTE_PYTHON: &str = r#"
async def route_bounded(operation, deadline, predicate):
    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait((task,), timeout=remaining(deadline, predicate))
        if not done:
            raise TimeoutError(f'budget expired: {predicate}')
        remaining(deadline, predicate)
        return task.result()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

class RouteEndpoint:
    def __init__(self, spec):
        self.spec = spec
        loop = asyncio.get_running_loop()
        self.opened = loop.create_future()
        self.finished = loop.create_future()
        self.handle = None
        self.task = None
        self.origin = None
        self.incarnation = None

    async def opened_handle(self, handle):
        try:
            self.handle = handle
            self.incarnation = handle.incarnation
            spec = self.spec
            emit(spec['step'], spec['action'], spec['path'], 'barrier',
                 barrier={'type': 'stream_opened', 'incarnation': handle.incarnation},
                 **spec['hints'])
            self.opened.set_result(None)
            await self.finished
        finally:
            handle = None

    async def own(self, data, deadline):
        spec = self.spec
        try:
            if spec['write']:
                if spec['replace']:
                    self.origin = await mutation_origin(data, spec['path'], deadline)
                async with data.write_stream(spec['path'], replace=spec['replace']) as writer:
                    await self.opened_handle(writer)
            else:
                delay = 0.001
                while True:
                    try:
                        reader = await data.read_stream(spec['path'])
                        break
                    except (OSError, swactor.SessionError, swactor.StreamError):
                        if not spec['retry']:
                            raise
                        delay = await pace(deadline, 'stream reattachment', delay)
                try:
                    await self.opened_handle(reader)
                finally:
                    del reader
        except BaseException as error:
            if not self.opened.done():
                self.opened.set_exception(error)
            raise
        finally:
            self.handle = None

class PreparedRoutes:
    def __init__(self, specs):
        self.endpoints = {spec['step']: RouteEndpoint(spec) for spec in specs}
        self.received = {}

    async def prepare(self, data, deadline):
        for endpoint in self.endpoints.values():
            endpoint.task = asyncio.create_task(endpoint.own(data, deadline))
        try:
            await route_bounded(
                asyncio.gather(*(endpoint.opened for endpoint in self.endpoints.values())),
                deadline, 'route endpoint preparation')
        except BaseException:
            await self.close()
            raise

    async def finish(self, step):
        endpoint = self.endpoints[step]
        endpoint.finished.set_result(None)
        await endpoint.task
        endpoint.task = None

    async def close(self):
        tasks = []
        for endpoint in self.endpoints.values():
            if endpoint.task is not None:
                if not endpoint.task.done() and not endpoint.task.cancelling():
                    endpoint.task.cancel()
                tasks.append(endpoint.task)
        await asyncio.gather(*tasks, return_exceptions=True)
        for endpoint in self.endpoints.values():
            endpoint.handle = None
            endpoint.task = None
        self.received.clear()
        self.endpoints.clear()

    def retain(self, step, payload, transfer, expected_length, expected_digest):
        if (transfer.length != expected_length
                or transfer.hasher.hexdigest() != expected_digest
                or len(payload) != expected_length):
            raise RuntimeError('route read length or digest mismatch')
        self.received[step] = payload

    def output(self, steps, rotation, expected_length):
        length = sum(len(self.received[step]) for step in steps)
        if length != expected_length:
            raise RuntimeError('route relay length mismatch')
        if len(steps) == 1 and rotation == 0:
            return self.received[steps[0]]
        payload = bytearray(length)
        position = (-rotation) % length if length else 0
        for step in steps:
            value = memoryview(self.received[step])
            split = min(len(value), length - position)
            payload[position:position + split] = value[:split]
            payload[:len(value) - split] = value[split:]
            position = (position + len(value)) % length if length else 0
            value.release()
        return payload
"#;

struct RouteAction {
    inputs: Vec<usize>,
    rotation: usize,
}

fn route_read_expected(operation: &ActionOp) -> Option<&[u8]> {
    match operation {
        ActionOp::ReadBlob { expected, .. }
        | ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. } => Some(expected),
        _ => None,
    }
}

fn route_actions(case: &BehaviorCase, program: &ProcessProgram) -> Vec<Option<RouteAction>> {
    program
        .actions
        .iter()
        .enumerate()
        .map(|(step, action)| {
            let is_read = route_read_expected(&action.operation).is_some();
            if !is_read
                && !matches!(
                    &action.operation,
                    ActionOp::PublishBlob { .. }
                        | ActionOp::StreamWrite { .. }
                        | ActionOp::GatedStreamWrite { .. }
                )
            {
                return None;
            }
            let (route, edge_index) = case.routes.iter().find_map(|route| {
                route
                    .edges
                    .iter()
                    .position(|edge| {
                        edge.path == action.operation.path()
                            && if is_read {
                                edge.destination_role == program.id
                            } else {
                                edge.source_role == program.id
                            }
                    })
                    .map(|edge_index| (route, edge_index))
            })?;
            let mut inputs = Vec::new();
            let mut rotation = 0;
            if !is_read {
                if case.topology == TopologyFamily::RingWalk && route.edges.len() > 1 {
                    if edge_index != 0 {
                        inputs.push(
                            program.actions[..step]
                                .iter()
                                .rposition(|prior| {
                                    route_read_expected(&prior.operation).is_some()
                                        && route
                                            .edges
                                            .iter()
                                            .any(|edge| edge.path == prior.operation.path())
                                })
                                .expect("ring relay requires a preceding same-process route read"),
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
                                    route_read_expected(&prior.operation).is_some()
                                        && prior.operation.path() == incoming.path
                                })
                                .expect(
                                    "DAG relay requires every preceding same-process input read",
                                ),
                        );
                    }
                    if !inputs.is_empty() {
                        rotation = edge_index + 1;
                    }
                }
            }
            Some(RouteAction { inputs, rotation })
        })
        .collect()
}

fn render_route_specs(
    source: &mut String,
    program: &ProcessProgram,
    routes: &[Option<RouteAction>],
) {
    let specs = program
        .actions
        .iter()
        .zip(routes)
        .enumerate()
        .filter_map(|(step, (action, route))| {
            route.as_ref()?;
            let (write, replace, retry) = match &action.operation {
                ActionOp::StreamWrite { replace, .. } => (true, *replace, false),
                ActionOp::GatedStreamWrite { .. } => (true, false, false),
                ActionOp::StreamReadWithRetry { .. } => (false, false, true),
                ActionOp::StreamRead { .. }
                | ActionOp::GatedStreamRead { .. }
                | ActionOp::StreamReadInto { .. } => (false, false, false),
                _ => return None,
            };
            let plan = EvidencePlan::resolve(action);
            let mut hints = serde_json::Map::new();
            if let Some(token) = plan.token {
                hints.insert("token".to_owned(), token.into());
            }
            if let Some(lap) = plan.lap {
                hints.insert("lap".to_owned(), lap.into());
            }
            Some(serde_json::json!({
                "step": step, "action": action.operation.class().as_str(),
                "path": action.operation.path(), "write": write, "replace": replace,
                "retry": retry, "hints": hints,
            }))
        })
        .collect::<Vec<_>>();
    let encoded = serde_json::to_string(&specs).expect("serialize route endpoint specifications");
    let _ = writeln!(
        source,
        "\nROUTE_ENDPOINTS = json.loads({})",
        python_string(&encoded)
    );
}

pub fn render_python(program: &ProcessProgram) -> String {
    render_program(program, Duration::from_secs(120), None)
}

fn render_program(
    program: &ProcessProgram,
    budget: Duration,
    case: Option<&BehaviorCase>,
) -> String {
    let routes = case.map(|case| route_actions(case, program));
    // Route-local blob waits must not prevent this role from opening its
    // independent stream endpoints. Keep unrelated primitive prefixes ahead
    // of preparation, and leave every transfer and rendezvous barrier intact.
    let first_route = routes.as_ref().and_then(|routes| {
        program
            .actions
            .iter()
            .zip(routes)
            .position(|(action, route)| {
                route.is_some()
                    || matches!(&action.operation, ActionOp::AwaitEntry { path, .. }
                    if case.is_some_and(|case| case.routes.iter().any(|route|
                        route.edges.iter().any(|edge|
                            edge.destination_role == program.id && edge.path == *path))))
            })
    });
    let route_arguments = if first_route.is_some() {
        ", routes"
    } else {
        ""
    };
    let mut source = String::new();
    source.push_str(
        "import asyncio\nimport errno\nimport faulthandler\nimport gc\nimport hashlib\nimport json\nimport os\nimport struct\nimport sys\n\nfaulthandler.enable(all_threads=True)\n\nimport swactor\n\n",
    );
    let _ = writeln!(source, "PROCESS = {}", python_string(&program.id));
    source.push_str(BUDGET_PYTHON);
    source.push_str(TRANSFER_PYTHON);
    source.push_str(FRAMING_PYTHON);
    let _ = writeln!(
        source,
        "\nSTREAM_FRAME_LIMIT = {}",
        case.map_or(
            crate::ir::MAX_STREAM_FRAMES,
            BehaviorCase::stream_frame_limit
        )
    );
    if first_route.is_some() {
        source.push_str(ROUTE_PYTHON);
        render_route_specs(
            &mut source,
            program,
            routes.as_ref().expect("route context"),
        );
    }
    source.push_str(
        "\ndef emit(step, action, path, outcome, **facts):\n    record = {'process': PROCESS, 'step': step, 'action': action, 'path': path, 'outcome': outcome}\n    record.update(facts)\n    # The controller releases stream rendezvous at open and injects stops after\n    # first writer/reader frames. Flush those milestones; terminal pipe draining\n    # preserves every other record.\n    barrier = facts.get('barrier') or {}\n    barrier_type = barrier.get('type')\n    control_milestone = (outcome == 'barrier' and\n                         (barrier_type in ('stream_opened', 'stream_first_frame') or\n                          (barrier_type == 'stream_frame' and barrier.get('index') == 0)))\n    print(json.dumps(record, separators=(',', ':')), flush=control_milestone)\n\ndef digest(data):\n    return hashlib.sha256(data).hexdigest()\n\nasync def mutation_origin(data, path, deadline):\n    try:\n        entry = await bounded(data.lookup(path), deadline, f'mutation origin: {path}')\n    except OSError:\n        return None\n    return entry.revision\n",
    );
    for (step, action) in program.actions.iter().enumerate() {
        let _ = writeln!(
            source,
            "\nasync def action_{step}(data, deadline{route_arguments}):"
        );
        render_action(
            &mut source,
            step,
            action,
            routes.as_ref().and_then(|routes| routes[step].as_ref()),
            first_route == Some(step),
        );
    }
    source.push_str(
        "\nasync def main(ctx):\n    data = ctx.data\n    loop = asyncio.get_running_loop()\n",
    );
    let _ = writeln!(
        source,
        "    process_deadline = loop.time() + {:?}",
        budget.as_secs_f64()
    );
    if first_route.is_some() {
        source.push_str("    routes = PreparedRoutes(ROUTE_ENDPOINTS)\n    try:\n");
    }
    let indent = if first_route.is_some() { "    " } else { "" };
    let bound = if first_route.is_some() {
        "route_bounded"
    } else {
        "bounded"
    };
    for (step, action) in program.actions.iter().enumerate() {
        let _ = writeln!(
            source,
            "{indent}    deadline = min(process_deadline, loop.time() + {NAMESPACE_BARRIER_DEADLINE_SECONDS})"
        );
        let predicate = python_string(&format!(
            "{} step {step} {}: {}",
            program.id,
            action.operation.class().as_str(),
            action.operation.path()
        ));
        let _ = writeln!(
            source,
            "{indent}    started = loop.time()\n{indent}    completed = False\n{indent}    try:\n{indent}        await {bound}(action_{step}(data, deadline{route_arguments}), deadline, {predicate})\n{indent}        completed = True\n{indent}    finally:\n{indent}        timing({predicate}, started, None, None, not completed, kind='generated_action')"
        );
    }
    if first_route.is_some() {
        source.push_str("    finally:\n        await routes.close()\n        gc.collect()\n");
    }
    source.push_str("\nswactor.run(main)\n");
    source
}

fn render_frames(source: &mut String, frames: &[Vec<u8>]) {
    source.push_str("        frames = [\n");
    for frame in frames {
        let _ = writeln!(
            source,
            "            bytes.fromhex({}),",
            python_string(&hex(frame))
        );
    }
    source.push_str("        ]\n");
}

pub fn render_case_python(
    case: &BehaviorCase,
    program: &ProcessProgram,
    budget: Duration,
) -> String {
    match &case.failure {
        FailureInjection::LaunchFailure {
            process,
            kind: LaunchFailureKind::PythonSyntax,
        } if process == &program.id => "def invalid(:\n".to_owned(),
        FailureInjection::LaunchFailure {
            process,
            kind: LaunchFailureKind::PythonRuntime,
        } if process == &program.id => "import swactor\n\nasync def main(ctx):\n    raise RuntimeError('injected Python runtime failure')\n\nswactor.run(main)\n".to_owned(),
        _ => render_program(program, budget, Some(case)),
    }
}

#[cfg(test)]
pub(crate) fn render_absence_probe_python(
    process: &str,
    paths: &[String],
    budget: Duration,
) -> String {
    let mut source = String::from("import asyncio\nimport errno\nimport json\nimport swactor\n");
    let _ = writeln!(
        source,
        "\nPROCESS = {}\nPATHS = {}\nPROBE_SECONDS = {:?}",
        python_string(process),
        serde_json::to_string(paths).expect("serialize absence-probe paths"),
        budget.as_secs_f64()
    );
    source.push_str(BUDGET_PYTHON);
    source.push_str(
        r#"
async def main(ctx):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + PROBE_SECONDS

    async def probe(step, path):
        try:
            await bounded(ctx.data.lookup(path), deadline, f'absence lookup: {path}')
        except BaseException as error:
            if isinstance(error, OSError) and error.errno == errno.ENOENT:
                return {'process': PROCESS, 'step': step, 'action': 'lookup',
                        'path': path, 'outcome': 'expected_error',
                        'errno': error.errno, 'error_type': type(error).__name__,
                        'error': f'{type(error).__name__}: {error}'}
            if isinstance(error, asyncio.CancelledError):
                error = TimeoutError('absence probe budget expired or owner cancelled')
            return {'process': PROCESS, 'step': step, 'action': 'lookup',
                    'path': path, 'outcome': 'error',
                    'errno': getattr(error, 'errno', None),
                    'error_type': type(error).__name__,
                    'error': f'{type(error).__name__}: {error}'}
        return {'process': PROCESS, 'step': step, 'action': 'lookup',
                'path': path, 'outcome': 'error', 'errno': None,
                'error_type': 'PathExists',
                'error': f'PathExists: expected absence for {path}'}

    records = await asyncio.gather(*(probe(step, path) for step, path in enumerate(PATHS)))
    for record in records:
        print(json.dumps(record, separators=(',', ':')))
    failed = [record['path'] for record in records if record['outcome'] != 'expected_error']
    if failed:
        raise RuntimeError(f'absence unproved: {failed}')

swactor.run(main)
"#,
    );
    source
}

/// Evidence hint resolution for one rendered action.
///
/// Hints are opaque corpus-authored labels; codegen threads the known keys
/// into emit calls without interpreting their scenario semantics.
struct EvidencePlan {
    token: Option<String>,
    lap: Option<u32>,
    edge_index: Option<u32>,
    barrier: Option<&'static str>,
}

impl EvidencePlan {
    fn resolve(action: &Action) -> Self {
        let hints = &action.evidence_hints;
        let barrier = hints
            .get(EVIDENCE_HINT_BARRIER)
            .and_then(|value| match value.as_str() {
                BARRIER_TOKEN_RECEIVED => Some(BARRIER_TOKEN_RECEIVED),
                BARRIER_TOKEN_FORWARDED => Some(BARRIER_TOKEN_FORWARDED),
                BARRIER_LAP_COMPLETED => Some(BARRIER_LAP_COMPLETED),
                _ => None,
            });
        Self {
            token: hints.get(EVIDENCE_HINT_TOKEN).cloned(),
            lap: hints
                .get(EVIDENCE_HINT_LAP)
                .and_then(|value| value.parse().ok()),
            edge_index: hints
                .get(EVIDENCE_HINT_EDGE_INDEX)
                .and_then(|value| value.parse().ok()),
            barrier,
        }
    }

    /// Top-level emit facts (`token=`/`lap=`) applied to ok records and
    /// barrier records of this action.
    fn facts(&self) -> String {
        let mut facts = String::new();
        if let Some(token) = &self.token {
            facts.push_str(", token=");
            facts.push_str(&python_string(token));
        }
        if let Some(lap) = self.lap {
            let _ = write!(facts, ", lap={lap}");
        }
        facts
    }
}

fn render_route_payload(source: &mut String, route: &RouteAction, bytes: &[u8]) {
    if route.inputs.is_empty() {
        let _ = writeln!(
            source,
            "        payload = bytes.fromhex({})",
            python_string(&hex(bytes))
        );
    } else {
        let _ = writeln!(
            source,
            "        payload = routes.output({:?}, {}, {})",
            route.inputs,
            route.rotation,
            bytes.len()
        );
    }
}

fn render_route_frames(source: &mut String, route: &RouteAction, frames: &[Vec<u8>]) {
    if route.inputs.is_empty() {
        render_frames(source, frames);
    } else {
        let length = frames.iter().map(Vec::len).sum::<usize>();
        let _ = writeln!(
            source,
            "        payload = routes.output({:?}, {}, {length})",
            route.inputs, route.rotation
        );
        let mut offset = 0;
        source.push_str("        frame_ranges = [\n");
        for frame in frames {
            let end = offset + frame.len();
            let _ = writeln!(source, "            ({offset}, {end}),");
            offset = end;
        }
        source.push_str(
            "        ]\n        frames = (memoryview(payload)[start:end] for start, end in frame_ranges)\n",
        );
    }
}

fn render_route_retain(source: &mut String, step: usize, expected: &[u8]) {
    let _ = writeln!(
        source,
        "        routes.retain({step}, payload, transfer, {}, {})",
        expected.len(),
        python_string(&digest(expected))
    );
}

fn render_stream_read_loop(
    source: &mut String,
    step: usize,
    action: &Action,
    length: usize,
    hint_facts: &str,
    indent: &str,
    retain: bool,
    first_already_read: bool,
) {
    let class = action.operation.class().as_str();
    let path = python_string(action.operation.path());
    let buffers = match &action.operation {
        ActionOp::StreamReadInto { buffer_sizes, .. } => {
            serde_json::to_string(buffer_sizes).expect("serialize readinto sizes")
        }
        _ => "None".to_owned(),
    };
    if first_already_read {
        let _ = writeln!(
            source,
            "{indent}emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_frame', 'incarnation': incarnation_{step}, 'index': 0, 'length': len(first), 'digest': digest(first)}}{hint_facts})\n{indent}transfer = transferred(transfer, first)"
        );
        if let ActionOp::GatedStreamRead { observed_path, .. } = &action.operation {
            let observed_path = python_string(observed_path);
            let _ = writeln!(
                source,
                "{indent}async with data.write_blob({observed_path}, length=0) as observed_writer:\n{indent}    with observed_writer.map() as mapped:\n{indent}        view = memoryview(mapped)\n{indent}        view.release()"
            );
        }
        let _ = writeln!(
            source,
            "{indent}emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_first_frame', 'incarnation': incarnation_{step}}}{hint_facts})"
        );
        if matches!(
            action.operation,
            ActionOp::GatedStreamRead {
                park_after_first_frame: true,
                ..
            }
        ) {
            let _ = writeln!(source, "{indent}await asyncio.Future()");
        }
        let _ = writeln!(source, "{indent}del first");
    } else {
        let _ = writeln!(
            source,
            "{indent}framed = FramedReader(reader, {length}, {step}, {path}, buffer_sizes={buffers}{hint_facts})"
        );
    }
    let _ = writeln!(
        source,
        "{indent}while True:\n{indent}    chunk = await framed.read()\n{indent}    if chunk is None:"
    );
    if matches!(&action.operation, ActionOp::GatedStreamRead { .. }) {
        let _ = writeln!(
            source,
            "{indent}        if framed.index == 0:\n{indent}            raise RuntimeError('hot stream ended before its first frame')"
        );
    }
    let _ = writeln!(
        source,
        "{indent}        transfer = transferred(transfer, b'', complete=True)\n{indent}        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_eof', 'incarnation': incarnation_{step}}}{hint_facts})\n{indent}        break\n{indent}    transfer = transferred(transfer, chunk)\n{indent}    if framed.index == 1:"
    );
    if let ActionOp::GatedStreamRead { observed_path, .. } = &action.operation {
        let observed_path = python_string(observed_path);
        let _ = writeln!(
            source,
            "{indent}        async with data.write_blob({observed_path}, length=0) as observed_writer:\n{indent}            with observed_writer.map() as mapped:\n{indent}                view = memoryview(mapped)\n{indent}                view.release()"
        );
    }
    let _ = writeln!(
        source,
        "{indent}        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_first_frame', 'incarnation': incarnation_{step}}}{hint_facts})"
    );
    if matches!(
        action.operation,
        ActionOp::GatedStreamRead {
            park_after_first_frame: true,
            ..
        }
    ) {
        // Keep the public endpoint attached until the causally triggered
        // stop cancels this task; never race EOF against controller relay.
        let _ = writeln!(source, "{indent}        await asyncio.Future()");
    }
    if retain {
        let _ = writeln!(
            source,
            "{indent}    payload[transfer.length - len(chunk):transfer.length] = chunk"
        );
    }
    let _ = writeln!(source, "{indent}    del chunk\n{indent}del framed");
}

fn render_route_operation(
    source: &mut String,
    step: usize,
    action: &Action,
    route: &RouteAction,
    hint_facts: &str,
    ok_facts: &str,
) {
    let class = action.operation.class().as_str();
    let path = python_string(action.operation.path());
    match &action.operation {
        ActionOp::PublishBlob { bytes, .. } => {
            render_route_payload(source, route, bytes);
            let _ = writeln!(
                source,
                "        async with data.write_blob({path}, length=len(payload)) as writer:"
            );
            source.push_str(
                "            with writer.map() as mapped:\n                view = memoryview(mapped)\n                view[:] = payload\n                view.release()\n                transfer = transferred(transfer, payload, complete=True)\n",
            );
        }
        ActionOp::ReadBlob { expected, .. } => {
            let _ = writeln!(
                source,
                "        blob = await read_blob_retry(data, {path}, deadline)\n        with blob.map() as mapped:\n            with memoryview(mapped) as view:\n                if len(view) != {}:\n                    raise RuntimeError('route blob length mismatch')\n                payload = bytes(view)\n                transfer = transferred(transfer, payload, complete=True)",
                expected.len()
            );
            render_route_retain(source, step, expected);
        }
        // Stream adapters are shared with non-route actions; prepared handles
        // change endpoint ownership, never the wire or payload semantics.
        ActionOp::StreamWrite {
            chunks, replace, ..
        } => {
            render_route_frames(source, route, chunks);
            let _ = writeln!(
                source,
                "        writer = routes.endpoints[{step}].handle\n        incarnation_{step} = writer.incarnation\n        framed = FramedWriter(writer, {}, {step}, {path}{hint_facts})\n        try:\n            for frame in frames:\n                await framed.write(frame)\n                transfer = transferred(transfer, frame)\n            transfer = transferred(transfer, b'', complete=True)\n            del framed\n            await routes.finish({step})\n        finally:\n            writer = None",
                chunks.iter().map(Vec::len).max().unwrap_or(0)
            );
            if *replace {
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'mutation_applied', 'from_revision': routes.endpoints[{step}].origin, 'to_revision': incarnation_{step}}}{hint_facts})"
                );
            }
        }
        ActionOp::GatedStreamWrite {
            frames,
            release_path,
            ..
        } => {
            render_route_frames(source, route, frames);
            let release_path = python_string(release_path);
            let _ = writeln!(
                source,
                "        writer = routes.endpoints[{step}].handle\n        incarnation_{step} = writer.incarnation\n        framed = FramedWriter(writer, {}, {step}, {path}{hint_facts})\n        try:",
                frames.iter().map(Vec::len).max().unwrap_or(0)
            );
            let _ = writeln!(
                source,
                "            frames = iter(frames)\n            first = next(frames)\n            await framed.write(first)\n            transfer = transferred(transfer, first)\n            await wait_entry(data, {release_path}, deadline)\n            emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'release_observed', 'path': {release_path}}}{hint_facts})\n            for frame in frames:\n                await framed.write(frame)\n                transfer = transferred(transfer, frame)\n            transfer = transferred(transfer, b'', complete=True)\n            del framed\n            await routes.finish({step})\n        finally:\n            writer = None"
            );
        }
        ActionOp::StreamRead { expected, .. }
        | ActionOp::StreamReadWithRetry { expected, .. }
        | ActionOp::GatedStreamRead { expected, .. }
        | ActionOp::StreamReadInto { expected, .. } => {
            let _ = writeln!(
                source,
                "        reader = routes.endpoints[{step}].handle\n        incarnation_{step} = reader.incarnation\n        payload = bytearray({})\n        try:",
                expected.len()
            );
            render_stream_read_loop(
                source,
                step,
                action,
                expected.len(),
                hint_facts,
                "            ",
                true,
                false,
            );
            let _ = writeln!(
                source,
                "            await routes.finish({step})\n        finally:\n            reader = None"
            );
            render_route_retain(source, step, expected);
        }
        _ => unreachable!("route plan contains only route transfer actions"),
    }
    let _ = writeln!(
        source,
        "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
    );
}

fn render_action(
    source: &mut String,
    step: usize,
    action: &Action,
    route: Option<&RouteAction>,
    prepare_routes: bool,
) {
    let class = action.operation.class().as_str();
    let path = python_string(action.operation.path());
    let plan = EvidencePlan::resolve(action);
    let hint_facts = plan.facts();
    let mut ok_facts = hint_facts.clone();
    let incarnation_facts = if matches!(
        action.operation.class(),
        ActionClass::StreamWrite | ActionClass::StreamRead
    ) {
        let _ = writeln!(source, "    incarnation_{step} = None");
        format!(", incarnation=incarnation_{step}")
    } else {
        String::new()
    };
    ok_facts.push_str(&incarnation_facts);
    source.push_str("    transfer = None\n");
    ok_facts.push_str(", transfer=transfer_facts(transfer)");
    let descriptor_facts = if matches!(
        &action.operation,
        ActionOp::DescriptorWrite { .. } | ActionOp::DescriptorRead { .. }
    ) {
        source.push_str("    descriptor_evidence = None\n");
        ", descriptor=descriptor_evidence"
    } else {
        ""
    };
    let _ = writeln!(source, "    try:");
    if prepare_routes {
        source.push_str("        await routes.prepare(data, deadline)\n");
    }
    if let Some(route) = route {
        render_route_operation(source, step, action, route, &hint_facts, &ok_facts);
    } else {
        match &action.operation {
            ActionOp::PublishBlob { bytes, .. } => {
                let _ = writeln!(
                    source,
                    "        payload = bytes.fromhex({})",
                    python_string(&hex(bytes))
                );
                let _ = writeln!(
                    source,
                    "        async with data.write_blob({path}, length=len(payload)) as writer:"
                );
                source.push_str(
                "            with writer.map() as mapped:\n                view = memoryview(mapped)\n                view[:] = payload\n                view.release()\n                transfer = transferred(transfer, payload, complete=True)\n",
            );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::ReadBlob { .. } => {
                let _ = writeln!(
                    source,
                    "        blob = await read_blob_retry(data, {path}, deadline)"
                );
                source.push_str(
                "        with blob.map() as mapped:\n            payload = bytes(mapped)\n            transfer = transferred(transfer, payload, complete=True)\n",
            );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::StreamWrite {
                chunks, replace, ..
            } => {
                if *replace {
                    let _ = writeln!(
                        source,
                        "        from_revision_{step} = await mutation_origin(data, {path}, deadline)"
                    );
                }
                render_frames(source, chunks);
                let _ = writeln!(
                    source,
                    "        async with data.write_stream({path}, replace={}) as writer:",
                    if *replace { "True" } else { "False" }
                );
                let _ = writeln!(
                    source,
                    "            incarnation_{step} = writer.incarnation"
                );
                let _ = writeln!(
                    source,
                    "            emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_opened', 'incarnation': incarnation_{step}}}{hint_facts})"
                );
                let _ = writeln!(
                    source,
                    "            framed = FramedWriter(writer, {}, {step}, {path}{hint_facts})\n            for frame in frames:\n                await framed.write(frame)\n                transfer = transferred(transfer, frame)\n            transfer = transferred(transfer, b'', complete=True)\n            del framed",
                    chunks.iter().map(Vec::len).max().unwrap_or(0)
                );
                if *replace {
                    let _ = writeln!(
                        source,
                        "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'mutation_applied', 'from_revision': from_revision_{step}, 'to_revision': incarnation_{step}}}{hint_facts})"
                    );
                }
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::StreamRoundTrip { chunks, .. } => {
                let _ = writeln!(
                    source,
                    "        def roundtrip_opened_{step}(stream):\n            nonlocal incarnation_{step}\n            attached_incarnation = stream.incarnation\n            if incarnation_{step} is None:\n                emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_opened', 'incarnation': attached_incarnation}}{hint_facts})\n            elif incarnation_{step} != attached_incarnation:\n                raise RuntimeError('stream round trip incarnation mismatch')\n            incarnation_{step} = attached_incarnation\n            return attached_incarnation"
                );
                render_frames(source, chunks);
                let length = chunks.iter().map(Vec::len).sum::<usize>();
                let mut expected_digest = Sha256::new();
                for chunk in chunks {
                    expected_digest.update(chunk);
                }
                let expected_digest = format!("{:x}", expected_digest.finalize());
                let _ = writeln!(source, "        async def roundtrip_writer_{step}():");
                let _ = writeln!(
                    source,
                    "            async with data.write_stream({path}, replace=False) as writer:"
                );
                let _ = writeln!(source, "                roundtrip_opened_{step}(writer)");
                let _ = writeln!(
                    source,
                    "                framed = FramedWriter(writer, {}, {step}, {path}{hint_facts})\n                for frame in frames:\n                    await framed.write(frame)\n                del framed",
                    chunks.iter().map(Vec::len).max().unwrap_or(0)
                );
                let _ = writeln!(source, "        async def roundtrip_reader_{step}():");
                source.push_str("            nonlocal transfer\n");
                let _ = writeln!(
                    source,
                    "            reader = await data.read_stream({path})"
                );
                let _ = writeln!(
                    source,
                    "            incarnation_{step} = roundtrip_opened_{step}(reader)"
                );
                render_stream_read_loop(
                    source,
                    step,
                    action,
                    length,
                    &hint_facts,
                    "            ",
                    false,
                    false,
                );
                source.push_str("            del reader\n");
                let _ = writeln!(
                    source,
                    "        await asyncio.gather(roundtrip_writer_{step}(), roundtrip_reader_{step}())"
                );
                let _ = writeln!(
                    source,
                    "        if transfer.length != {length} or transfer.hasher.hexdigest() != '{expected_digest}':\n            raise RuntimeError('stream round trip payload mismatch')"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::GatedStreamWrite {
                frames,
                release_path,
                replace,
                ..
            } => {
                let release_path = python_string(release_path);
                if *replace {
                    let _ = writeln!(
                        source,
                        "        from_revision_{step} = await mutation_origin(data, {path}, deadline)"
                    );
                }
                render_frames(source, frames);
                let _ = writeln!(
                    source,
                    "        async with data.write_stream({path}, replace={}) as writer:",
                    if *replace { "True" } else { "False" }
                );
                let _ = writeln!(
                    source,
                    "            incarnation_{step} = writer.incarnation"
                );
                let _ = writeln!(
                    source,
                    "            emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_opened', 'incarnation': incarnation_{step}}}{hint_facts})"
                );
                let _ = writeln!(
                    source,
                    "            framed = FramedWriter(writer, {}, {step}, {path}{hint_facts})\n            first = frames[0]\n            await framed.write(first)\n            transfer = transferred(transfer, first)",
                    frames.iter().map(Vec::len).max().unwrap_or(0)
                );
                let _ = writeln!(
                    source,
                    "            await wait_entry(data, {release_path}, deadline)\n            emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'release_observed', 'path': {release_path}}}{hint_facts})"
                );
                source.push_str(
                    "            for index in range(1, len(frames)):\n                await framed.write(frames[index])\n                transfer = transferred(transfer, frames[index])\n            transfer = transferred(transfer, b'', complete=True)\n            del framed\n",
                );
                if *replace {
                    let _ = writeln!(
                        source,
                        "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'mutation_applied', 'from_revision': from_revision_{step}, 'to_revision': incarnation_{step}}}{hint_facts})"
                    );
                }
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::StreamRead { expected, .. }
            | ActionOp::StreamReadWithRetry { expected, .. }
            | ActionOp::GatedStreamRead { expected, .. }
            | ActionOp::StreamReadInto { expected, .. } => {
                let retry_gated = matches!(
                    &action.operation,
                    ActionOp::GatedStreamRead {
                        retry_attach: true,
                        ..
                    }
                );
                if retry_gated {
                    let expected_payload = python_string(&hex(expected));
                    let _ = writeln!(
                        source,
                        "        expected_payload = bytes.fromhex({expected_payload})\n        delay = 0.001\n        while True:\n            reader = None\n            framed = None\n            first = None\n            try:\n                reader = await bounded(data.read_stream({path}), deadline, 'stream reattachment')\n                framed = FramedReader(reader, {}, {step}, {path}{hint_facts})\n                first = await bounded(framed.read(record=False), deadline, 'replacement first frame')\n                if first is not None and expected_payload.startswith(bytes(first)):\n                    break\n            except (OSError, RuntimeError, swactor.SessionError, swactor.StreamError):\n                pass\n            framed = None\n            reader = None\n            delay = await pace(deadline, 'stream reattachment', delay)",
                        expected.len()
                    );
                } else if matches!(&action.operation, ActionOp::StreamReadWithRetry { .. }) {
                    let _ = writeln!(
                        source,
                        "        delay = 0.001\n        while True:\n            try:\n                reader = await bounded(data.read_stream({path}), deadline, 'stream reattachment')\n                break\n            except (OSError, swactor.SessionError, swactor.StreamError):\n                delay = await pace(deadline, 'stream reattachment', delay)"
                    );
                } else {
                    let _ = writeln!(source, "        reader = await data.read_stream({path})");
                }
                let _ = writeln!(source, "        incarnation_{step} = reader.incarnation");
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'stream_opened', 'incarnation': incarnation_{step}}}{hint_facts})"
                );
                render_stream_read_loop(
                    source,
                    step,
                    action,
                    expected.len(),
                    &hint_facts,
                    "        ",
                    false,
                    retry_gated,
                );
                source.push_str("        del reader\n");
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(){ok_facts})"
                );
            }
            ActionOp::Lookup { .. } => {
                let _ = writeln!(
                    source,
                    "        entry = await bounded(data.lookup({path}), deadline, 'namespace lookup')"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active{ok_facts})"
                );
            }
            ActionOp::AwaitEntry { .. } => {
                let _ = writeln!(
                    source,
                    "        entry = await wait_entry(data, {path}, deadline)"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active{ok_facts})"
                );
            }
            ActionOp::WaitForQuiescent { .. } => {
                let _ = writeln!(
                    source,
                    "        entry = await wait_entry(data, {path}, deadline, quiescent=True)"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active{ok_facts})"
                );
            }
            ActionOp::Rename {
                destination,
                replace,
                ..
            } => {
                let destination = python_string(destination);
                let _ = writeln!(
                    source,
                    "        from_revision_{step} = await mutation_origin(data, {path}, deadline)"
                );
                let _ = writeln!(
                    source,
                    "        revision = await data.rename({path}, {destination}, replace={})",
                    if *replace { "True" } else { "False" }
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'mutation_applied', 'from_revision': from_revision_{step}, 'to_revision': revision}}{hint_facts})"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', revision=revision{ok_facts})"
                );
            }
            ActionOp::Unlink { .. } => {
                let _ = writeln!(
                    source,
                    "        from_revision_{step} = await mutation_origin(data, {path}, deadline)"
                );
                let _ = writeln!(source, "        revision = await data.unlink({path})");
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': 'mutation_applied', 'from_revision': from_revision_{step}, 'to_revision': revision}}{hint_facts})"
                );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', revision=revision{ok_facts})"
                );
            }
            ActionOp::DescriptorWrite {
                flags,
                length,
                bytes,
                method,
                finish,
                ..
            } => {
                match length {
                    Some(length) => {
                        let _ = writeln!(
                            source,
                            "        descriptor = await data.open({path}, {flags}, length={length})"
                        );
                    }
                    None => {
                        let _ = writeln!(
                            source,
                            "        descriptor = await data.open({path}, {flags})"
                        );
                    }
                }
                let _ = writeln!(
                    source,
                    "        payload = bytes.fromhex({})",
                    python_string(&hex(bytes))
                );
                match method {
                    DescriptorWriteMethod::Write => {
                        source.push_str("        count = await descriptor.write(payload)\n        transfer = transferred(transfer, memoryview(payload)[:count], complete=True)\n");
                    }
                    DescriptorWriteMethod::WriteFrom => {
                        source.push_str("        count = await descriptor.writefrom(payload)\n        transfer = transferred(transfer, memoryview(payload)[:count], complete=True)\n");
                    }
                    DescriptorWriteMethod::Mapping => {
                        source.push_str(
                        "        mapping = await descriptor.map(offset=0, length=len(payload), writable=True)\n        with mapping as mapped:\n            view = memoryview(mapped)\n            view[:] = payload\n            transfer = transferred(transfer, view, complete=True)\n            view.release()\n",
                    );
                    }
                }
                let descriptor = serde_json::to_string(&crate::ir::DescriptorObservation::Write {
                    method: *method,
                    finish: *finish,
                    terminal_results: Vec::new(),
                    dropped: false,
                    reservation_released: false,
                })
                .expect("descriptor evidence is serializable");
                let _ = writeln!(
                    source,
                    "        descriptor_evidence = json.loads({})",
                    python_string(&descriptor)
                );
                render_descriptor_finish(source, *finish);
                if *finish == DescriptorFinish::Drop {
                    let _ = writeln!(
                        source,
                        "        descriptor_evidence['dropped'] = True\n        await observe_drop_release(data, {path}, deadline)\n        descriptor_evidence['reservation_released'] = True"
                    );
                }
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(), descriptor=descriptor_evidence{ok_facts})"
                );
            }
            ActionOp::DescriptorRead {
                flags,
                expected,
                method,
                offset,
                finish,
                ..
            } => {
                let _ = writeln!(
                    source,
                    "        descriptor = await data.open({path}, {flags})"
                );
                match method {
                    DescriptorReadMethod::Read => {
                        let _ = writeln!(
                            source,
                            "        payload = await descriptor.read({})\n        transfer = transferred(transfer, payload, complete=True)",
                            expected.len()
                        );
                    }
                    DescriptorReadMethod::ReadInto => {
                        let _ = writeln!(
                            source,
                            "        target = bytearray({})\n        count = await descriptor.readinto(target)\n        with memoryview(target)[:count] as view:\n            transfer = transferred(transfer, view, complete=True)",
                            expected.len()
                        );
                    }
                    DescriptorReadMethod::Mapping => {
                        let _ = writeln!(
                            source,
                            "        mapping = await descriptor.map(offset={offset}, length={}, writable=False)",
                            expected.len()
                        );
                        source.push_str(
                        "        with mapping as mapped:\n            with memoryview(mapped) as view:\n                transfer = transferred(transfer, view, complete=True)\n",
                    );
                    }
                }
                let descriptor = serde_json::to_string(&crate::ir::DescriptorObservation::Read {
                    method: *method,
                    finish: *finish,
                    terminal_results: Vec::new(),
                })
                .expect("descriptor evidence is serializable");
                let _ = writeln!(
                    source,
                    "        descriptor_evidence = json.loads({})",
                    python_string(&descriptor)
                );
                render_descriptor_finish(source, *finish);
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=transfer.length, digest=transfer.hasher.hexdigest(), descriptor=descriptor_evidence{ok_facts})"
                );
            }
            ActionOp::MappingExportClose { length, .. } => {
                let _ = writeln!(
                    source,
                    "        descriptor = await data.open({path}, 0)\n        mapping = await descriptor.map(offset=0, length={length}, writable=False)"
                );
                source.push_str(
                "        view = memoryview(mapping)\n        try:\n            mapping.close()\n        except BaseException as close_error:\n            view.release()\n            mapping.close()\n            await descriptor.close()\n            raise close_error\n        view.release()\n        await descriptor.close()\n        payload = b''\n",
            );
                let _ = writeln!(
                    source,
                    "        emit({step}, {class:?}, {path}, 'ok', length=0, digest=digest(payload){ok_facts})"
                );
            }
        }
    }
    if let Some(kind) = plan.barrier {
        let token = python_string(plan.token.as_deref().unwrap_or(""));
        let lap = plan.lap.unwrap_or(0);
        let edge_index = if kind == BARRIER_LAP_COMPLETED {
            String::new()
        } else {
            format!(", 'edge_index': {}", plan.edge_index.unwrap_or(0))
        };
        let _ = writeln!(
            source,
            "        emit({step}, {class:?}, {path}, 'barrier', barrier={{'type': '{kind}', 'token': {token}, 'lap': {lap}{edge_index}}}{hint_facts})"
        );
    }
    source.push_str("    except BaseException as error:\n        if isinstance(error, asyncio.CancelledError):\n            error = TimeoutError('action budget expired or owner cancelled')\n        error_number = getattr(error, 'errno', None)\n        error_type = type(error).__name__\n");
    if route.is_some()
        && matches!(
            action.operation.class(),
            ActionClass::StreamWrite | ActionClass::StreamRead
        )
    {
        let _ = writeln!(
            source,
            "        incarnation_{step} = routes.endpoints[{step}].incarnation"
        );
    }
    match action.expected {
        ExpectedOutcome::Ok => {
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}', transfer=transfer_facts(transfer){descriptor_facts}{incarnation_facts})"
            );
            source.push_str("        raise\n");
        }
        ExpectedOutcome::Error(expected)
        | ExpectedOutcome::Linearized {
            error: expected, ..
        } => {
            let _ = writeln!(source, "        if error_number != {expected}:");
            let _ = writeln!(
                source,
                "            emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}', transfer=transfer_facts(transfer){descriptor_facts}{incarnation_facts})"
            );
            source.push_str("            raise\n");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'expected_error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}', transfer=transfer_facts(transfer){descriptor_facts}{incarnation_facts})"
            );
        }
        ExpectedOutcome::Exception(expected) => {
            let expected = expected.as_str();
            let _ = writeln!(source, "        if error_type != {expected:?}:");
            let _ = writeln!(
                source,
                "            emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}', transfer=transfer_facts(transfer){descriptor_facts}{incarnation_facts})"
            );
            source.push_str("            raise\n");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'expected_error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}', transfer=transfer_facts(transfer){descriptor_facts}{incarnation_facts})"
            );
        }
    }
}

fn render_descriptor_finish(source: &mut String, finish: DescriptorFinish) {
    let (operation, count) = match finish {
        DescriptorFinish::Close => ("await descriptor.close()", 1),
        DescriptorFinish::Abort => ("await descriptor.abort()", 1),
        DescriptorFinish::Drop => {
            source.push_str("        del descriptor\n        gc.collect()\n");
            return;
        }
        DescriptorFinish::CloseTwice => ("await descriptor.close()", 2),
        DescriptorFinish::AbortTwice => ("await descriptor.abort()", 2),
    };
    for _ in 0..count {
        let _ = writeln!(source, "        try:\n            {operation}");
        source.push_str(
            "        except BaseException as terminal_error:\n            descriptor_evidence['terminal_results'].append({'outcome': 'error', 'errno': getattr(terminal_error, 'errno', None), 'error_type': type(terminal_error).__name__})\n            raise\n        descriptor_evidence['terminal_results'].append({'outcome': 'ok'})\n",
        );
    }
}

fn python_string(value: &str) -> String {
    serde_json::to_string(value).expect("serialize Python string")
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::AccessSpec;
    use std::process::{Command, Stdio};

    fn execute_generated(sources: &[String], scenario: &str) -> String {
        let runner = format!(
            "import asyncio,contextlib,errno,io,json,signal,sys,types\nsignal.alarm(5)\nsources = json.load(sys.stdin)\nswactor = types.ModuleType('swactor')\nswactor.run = lambda main: None\nswactor.SessionError = type('SessionError', (Exception,), {{}})\nswactor.StreamError = type('StreamError', (Exception,), {{}})\nsys.modules['swactor'] = swactor\n{scenario}"
        );
        let mut child = Command::new("python3")
            .args(["-c", &runner])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn generated execution regression");
        serde_json::to_writer(child.stdin.as_mut().unwrap(), sources).unwrap();
        drop(child.stdin.take());
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "generated execution failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("generated output is UTF-8")
    }

    fn generated_observation(
        case: &BehaviorCase,
        records: Vec<Vec<crate::ir::ActionObservation>>,
    ) -> crate::ir::CaseObservation {
        assert_eq!(records.len(), case.processes.len());
        crate::ir::CaseObservation {
            case_id: case.id.clone(),
            executions: case
                .processes
                .iter()
                .zip(records)
                .map(|(program, results)| crate::ir::ExecutionObservation {
                    process: program.id.clone(),
                    request_id: case.execution_request_id(program),
                    logical_node_id: program.logical_node_id,
                    lifecycle: [
                        "spawned",
                        "process_started",
                        "context_ready",
                        "user_result",
                        "exited",
                    ]
                    .map(str::to_owned)
                    .to_vec(),
                    results,
                    terminal: true,
                    exit_success: true,
                    exit_status: Some(r#"{"kind":"code","value":0}"#.to_owned()),
                    stdout: String::new(),
                    stderr: String::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn generated_descriptor_read_retains_corruption_before_ebadf() {
        let mut case = crate::corpus::stable_corpus(2, 17).remove(0);
        case.processes = vec![ProcessProgram {
            id: "descriptor-reader".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("descriptor-reader"),
            depends_on: Vec::new(),
            actions: vec![Action::error(
                ActionOp::DescriptorRead {
                    path: "/models/fixture".to_owned(),
                    flags: libc::O_RDONLY,
                    expected: b"a".to_vec(),
                    method: DescriptorReadMethod::Read,
                    offset: 0,
                    finish: DescriptorFinish::CloseTwice,
                },
                libc::EBADF,
            )],
        }];
        case.read_only_fixture_paths = ["/models/fixture".to_owned()].into();
        let output = execute_generated(
            &[render_python(&case.processes[0])],
            r#"
class Descriptor:
    def __init__(self, payload, fail_read):
        self.payload, self.fail_read, self.closed = payload, fail_read, False
    async def read(self, count):
        if self.fail_read:
            raise OSError(errno.EBADF, 'read failed before transfer')
        return self.payload
    async def close(self):
        if self.closed:
            raise OSError(errno.EBADF, 'already closed')
        self.closed = True

class Data:
    def __init__(self, payload, stage):
        self.payload, self.stage = payload, stage
    async def open(self, *args, **kwargs):
        if self.stage == 'open':
            raise OSError(errno.EBADF, 'open failed before transfer')
        return Descriptor(self.payload, self.stage == 'read')

async def scenario():
    all_records = []
    for payload, stage in [(b'a', None), (b'X', None), (b'a', 'open'), (b'a', 'read')]:
        namespace, output = {}, io.StringIO()
        exec(sources[0], namespace)
        with contextlib.redirect_stdout(output):
            await namespace['main'](types.SimpleNamespace(data=Data(payload, stage)))
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        assert records[0]['outcome'] == 'expected_error'
        assert records[0]['errno'] == errno.EBADF
        if stage:
            assert records[0]['transfer'] is None, records
        all_records.append(records)
    print(json.dumps(all_records))

asyncio.run(scenario())
"#,
        );
        let records: Vec<Vec<crate::ir::ActionObservation>> =
            serde_json::from_str(&output).unwrap();
        let correct = generated_observation(&case, vec![records[0].clone()]);
        crate::oracle::BehaviorOracle::verify(&case, &correct).unwrap();
        let corrupt = generated_observation(&case, vec![records[1].clone()]);
        assert!(crate::oracle::BehaviorOracle::verify(&case, &corrupt).is_err());
        assert_eq!(
            records[1][0].transfer.as_ref().unwrap().digest,
            digest(b"X")
        );
        assert!(records[2][0].transfer.is_none());
        assert!(records[3][0].transfer.is_none());
    }

    #[test]
    fn generated_writer_drop_proves_nonpublication_release_and_reuse() {
        let case = crate::corpus::stable_corpus(2, 17)
            .into_iter()
            .find(|case| case.id == "descriptor-drop-17")
            .unwrap();
        let sources = case.processes.iter().map(render_python).collect::<Vec<_>>();
        let output = execute_generated(
            &sources,
            r#"
class Descriptor:
    def __init__(self, data, path):
        self.data, self.path, self.payload, self.finished = data, path, b'', False
    async def write(self, payload):
        self.payload = payload
        return len(payload)
    async def abort(self):
        self.finished = True
        self.data.reserved.remove(self.path)
    async def close(self):
        self.data.blobs[self.path] = self.payload
        await self.abort()
    def __del__(self):
        if self.finished:
            return
        if self.data.mode == 'publication':
            self.data.blobs[self.path] = self.payload
        if self.data.mode != 'leak':
            self.data.reserved.remove(self.path)

class Blob:
    def __init__(self, payload):
        self.payload = payload
    def map(self):
        return contextlib.nullcontext(self.payload)

class Data:
    def __init__(self, mode):
        self.mode, self.reserved, self.blobs = mode, set(), {}
    async def open(self, path, flags, length=None):
        assert flags & 128, 'reuse must be exclusive'
        if path in self.reserved or path in self.blobs:
            raise OSError(errno.EEXIST, 'reserved or committed')
        self.reserved.add(path)
        return Descriptor(self, path)
    async def lookup(self, path):
        if path not in self.blobs:
            raise OSError(errno.ENOENT, 'not committed')
        return types.SimpleNamespace(kind='blob', revision=1, active=False)
    async def read_blob(self, path):
        return Blob(self.blobs[path])

async def run(mode):
    data, records = Data(mode), []
    for source in sources:
        namespace, output = {}, io.StringIO()
        exec(source, namespace)
        with contextlib.redirect_stdout(output):
            try:
                await namespace['main'](types.SimpleNamespace(data=data))
            finally:
                for _ in range(8):
                    await asyncio.sleep(0)
        records.append([json.loads(line) for line in output.getvalue().splitlines()])
    assert not data.reserved
    assert list(data.blobs.values()) == [b'reused']
    return records

async def scenario():
    valid = await run('release')
    for mode in ['leak', 'publication']:
        try:
            await asyncio.wait_for(run(mode), timeout=0.1)
        except (RuntimeError, TimeoutError):
            pass
        else:
            raise AssertionError(f'{mode} received drop credit')
    print(json.dumps(valid))

asyncio.run(scenario())
"#,
        );
        let records = serde_json::from_str(&output).unwrap();
        let observation = generated_observation(&case, records);
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
    }

    #[test]
    fn generated_authorization_scopes_allowed_work_and_independent_rename_denials() {
        let case = crate::corpus::stable_corpus(3, 17)
            .into_iter()
            .find(|case| case.id == "authorization-17")
            .unwrap()
            .for_attempt(7, true);
        case.validate().unwrap();
        let sources = case.processes.iter().map(render_python).collect::<Vec<_>>();
        let grants = serde_json::to_string(
            &case
                .processes
                .iter()
                .map(|program| &program.access)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let scenario = format!(
            "grants = json.loads({})\nfixture_path = {}\nfixture = bytes.fromhex({})\n{}",
            python_string(&grants),
            python_string(crate::corpus::SEEDED_MODEL_PATH),
            python_string(&hex(crate::corpus::SEEDED_MODEL_BYTES)),
            r#"
def permits(prefixes, path):
    return any(prefix == '/' or path == prefix or path.startswith(prefix.rstrip('/') + '/') for prefix in prefixes)

class Blob:
    def __init__(self, payload):
        self.payload = payload
    def map(self):
        return contextlib.nullcontext(self.payload)

class Writer:
    def __init__(self, data, path, length):
        self.data, self.path, self.payload = data, path, bytearray(length)
    async def __aenter__(self):
        self.data.require('write_prefixes', self.path)
        return self
    def map(self):
        return contextlib.nullcontext(self.payload)
    async def __aexit__(self, error_type, error, traceback):
        if error is None:
            self.data.store[self.path] = bytes(self.payload)

class Data:
    def __init__(self, grant, store, denials):
        self.grant, self.store, self.denials = grant, store, denials
    def require(self, kind, path):
        if not permits(self.grant[kind], path):
            raise OSError(errno.EACCES, 'outside scoped grant')
    def write_blob(self, path, length):
        return Writer(self, path, length)
    async def read_blob(self, path):
        self.require('read_prefixes', path)
        return Blob(self.store[path])
    async def lookup(self, path):
        self.require('read_prefixes', path)
        if path not in self.store:
            raise OSError(errno.ENOENT, 'absent')
        return types.SimpleNamespace(kind='blob', revision=1, active=False)
    async def rename(self, source, destination, replace):
        assert source in self.store, 'rename must not fail because its source is absent'
        source_allowed = permits(self.grant['write_prefixes'], source)
        destination_allowed = permits(self.grant['write_prefixes'], destination)
        assert source_allowed != destination_allowed, 'exactly one rename direction must be denied'
        self.denials.append('destination' if source_allowed else 'source')
        self.require('write_prefixes', source)
        self.require('write_prefixes', destination)
        raise AssertionError('unauthorized rename succeeded')

async def scenario():
    store, denials, all_records = {fixture_path: fixture}, [], []
    for source, grant in zip(sources, grants):
        namespace, output = {}, io.StringIO()
        exec(source, namespace)
        with contextlib.redirect_stdout(output):
            await namespace['main'](types.SimpleNamespace(data=Data(grant, store, denials)))
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        assert any(record['outcome'] == 'ok' for record in records), records
        assert any(record['outcome'] == 'expected_error' and record['errno'] == errno.EACCES for record in records), records
        all_records.append(records)
    assert denials == ['source', 'destination'], denials
    print(json.dumps(all_records))

asyncio.run(scenario())
"#
        );
        let records = serde_json::from_str(&execute_generated(&sources, &scenario)).unwrap();
        let observation = generated_observation(&case, records);
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
        let mut ledger =
            crate::coverage::CoverageLedger::survivor(case.live_nodes.clone()).unwrap();
        ledger.plan_case(&case).unwrap();
        ledger
            .bind_identity(crate::coverage::EvidenceIdentity {
                plan_digest: digest(b"authorization-plan"),
                artifacts_digest: digest(b"generated-public-binding-test"),
                deployment_generation: "authorization-test".to_owned(),
                fixture_mapping_digest: digest(b"three-logical-nodes"),
            })
            .unwrap();
        ledger.observe_case(&case, &observation).unwrap();
    }

    #[test]
    fn generated_fresh_reader_first_waits_for_original_attachment() {
        let case = crate::corpus::stable_corpus(2, 17)
            .into_iter()
            .find(|case| case.id == "active-stream-replacement-17")
            .unwrap();
        let sources = case.processes.iter().map(render_python).collect::<Vec<_>>();
        let output = execute_generated(
            &sources,
            r#"
class World:
    def __init__(self):
        self.marker = asyncio.Event()
        self.fresh_release, self.final_release = asyncio.Event(), asyncio.Event()
        self.old_attached, self.stale_attached = asyncio.Event(), asyncio.Event()
        self.first_written, self.replaced = asyncio.Event(), asyncio.Event()
        self.fresh_attached, self.fresh_waiting = asyncio.Event(), asyncio.Event()
        self.fresh_attempts = 0
        self.open_order = []

class Writer:
    def __init__(self, world, replace):
        self.world, self.replace = world, replace
        self.incarnation = 3 if replace else 1
        self.wire = bytearray()
    async def __aenter__(self):
        if self.replace:
            assert self.world.marker.is_set()
            await self.world.stale_attached.wait()
            self.world.replaced.set()
            await self.world.fresh_attached.wait()
        else:
            await self.world.old_attached.wait()
        return self
    async def __aexit__(self, *error):
        return False
    async def write(self, payload):
        if not self.replace:
            if self.world.replaced.is_set():
                raise OSError(errno.ESTALE, 'original writer displaced')
            expected = b'active-first'
        else:
            expected = b'replacement'
        self.wire.extend(payload)
        wire = bytes(8) + len(expected).to_bytes(8, 'little') + expected
        assert wire.startswith(self.wire)
        if not self.replace and self.wire == wire:
            self.world.first_written.set()

class Reader:
    def __init__(self, world, fresh):
        self.world, self.fresh, self.reads = world, fresh, 0
        self.incarnation = 3 if fresh else 1
    async def read(self):
        self.reads += 1
        if self.fresh:
            return bytes(8) + (11).to_bytes(8, 'little') + b'replacement' if self.reads == 1 else None
        if self.reads == 1:
            await self.world.first_written.wait()
            return bytes(8) + (12).to_bytes(8, 'little') + b'active-first'
        await self.world.replaced.wait()
        raise OSError(errno.ESTALE, 'original reader displaced')

class MarkerWriter:
    def __init__(self, marker):
        self.marker = marker
    async def __aenter__(self):
        return self
    def map(self):
        return contextlib.nullcontext(bytearray())
    async def __aexit__(self, error_type, error, traceback):
        if error is None:
            self.marker.set()

class Data:
    def __init__(self, world, role):
        self.world, self.role = world, role
    async def lookup(self, path):
        if path.endswith('/reader-attached'):
            if self.role == 3:
                self.world.fresh_waiting.set()
            marker, revision = self.world.marker, 2
        elif path.endswith('/fresh-reader-attached'):
            marker, revision = self.world.fresh_release, 4
        elif path.endswith('/release'):
            marker, revision = self.world.final_release, 5
        else:
            return types.SimpleNamespace(kind='stream', revision=3 if self.world.replaced.is_set() else 1, active=True)
        if not marker.is_set():
            raise OSError(errno.ENOENT, 'marker not published')
        return types.SimpleNamespace(kind='blob', revision=revision, active=False)
    def write_blob(self, path, length):
        assert length == 0
        if path.endswith('/reader-attached'):
            marker = self.world.marker
        elif path.endswith('/fresh-reader-attached'):
            marker = self.world.fresh_release
        else:
            marker = self.world.final_release
        return MarkerWriter(marker)
    def write_stream(self, path, replace):
        return Writer(self.world, replace)
    async def read_stream(self, path):
        if self.role == 3:
            self.world.fresh_attempts += 1
            if self.world.fresh_attempts == 1:
                self.world.open_order.append('stale')
                self.world.stale_attached.set()
                return Reader(self.world, False)
            await self.world.replaced.wait()
            self.world.open_order.append('replacement')
            self.world.fresh_attached.set()
            return Reader(self.world, True)
        self.world.open_order.append('original')
        self.world.old_attached.set()
        return Reader(self.world, False)

async def scenario():
    world, output, namespaces = World(), io.StringIO(), []
    for source in sources:
        namespace = {}
        exec(source, namespace)
        namespaces.append(namespace)
    with contextlib.redirect_stdout(output):
        fresh = asyncio.create_task(namespaces[3]['main'](types.SimpleNamespace(data=Data(world, 3))))
        await world.fresh_waiting.wait()
        assert world.open_order == [], 'fresh reader attempted attachment before original marker'
        others = [asyncio.create_task(namespaces[index]['main'](types.SimpleNamespace(data=Data(world, index))))
                  for index in range(3)]
        await asyncio.wait_for(asyncio.gather(fresh, *others), timeout=1)
    assert world.open_order == ['original', 'stale', 'replacement'], world.open_order
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    print(json.dumps([[record for record in records if record['process'] == namespace['PROCESS']]
                      for namespace in namespaces]))

asyncio.run(scenario())
"#,
        );
        let records: Vec<Vec<crate::ir::ActionObservation>> =
            serde_json::from_str(&output).unwrap();
        for index in [0, 1] {
            let final_record = records[index]
                .iter()
                .find(|record| record.outcome == "expected_error")
                .unwrap();
            assert_eq!(final_record.errno, Some(libc::ESTALE));
            let prefix = final_record.transfer.as_ref().unwrap();
            assert!(!prefix.complete);
            assert_eq!(prefix.digest, digest(b"active-first"));
        }
        let observation = generated_observation(&case, records);
        crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap();
    }

    #[test]
    fn generated_topology_routes_finish_under_adverse_open_orders() {
        use crate::coverage::{CoverageKey, CoverageLedger, EvidenceIdentity};
        use crate::ir::{CoverageScenario, DataKind, TopologyFamily};
        for nodes in 3..=5 {
            let cases = crate::corpus::generated_campaign_cases(17, 32, nodes).unwrap();
            for family in [
                TopologyFamily::Chain,
                TopologyFamily::RingWalk,
                TopologyFamily::FanIn,
                TopologyFamily::FanOut,
                TopologyFamily::Diamond,
                TopologyFamily::RandomDag,
            ] {
                if nodes == 3 && family == TopologyFamily::Diamond {
                    continue;
                }
                let mut case = cases
                    .iter()
                    .find(|case| {
                        case.topology == family
                            && case.routes.iter().any(|route| {
                                route.id.contains("-topology-") && route.kind == DataKind::Stream
                            })
                    })
                    .unwrap()
                    .clone();
                case.routes.retain(|route| route.id.contains("-topology-"));
                let paths = case
                    .routes
                    .iter()
                    .flat_map(|route| route.edges.iter().map(|edge| edge.path.clone()))
                    .collect::<std::collections::BTreeSet<_>>();
                for process in &mut case.processes {
                    process
                        .actions
                        .retain(|action| paths.contains(action.operation.path()));
                }
                case.processes.retain(|process| !process.actions.is_empty());
                case.scenarios =
                    std::collections::BTreeSet::from([CoverageScenario::ConcurrentStartup]);
                if family == TopologyFamily::RingWalk {
                    case.scenarios.insert(CoverageScenario::RingCompletion);
                }
                case.failure = FailureInjection::None;
                case.validate().unwrap();
                let sources = case
                    .processes
                    .iter()
                    .map(|process| render_case_python(&case, process, Duration::from_secs(120)))
                    .collect::<Vec<_>>();
                let readers_first = case
                    .processes
                    .iter()
                    .enumerate()
                    .filter(|(_, process)| {
                        process.actions[0].operation.class() == ActionClass::StreamRead
                    })
                    .map(|(index, _)| index)
                    .chain(
                        case.processes
                            .iter()
                            .enumerate()
                            .filter(|(_, process)| {
                                process.actions[0].operation.class() != ActionClass::StreamRead
                            })
                            .map(|(index, _)| index),
                    )
                    .collect::<Vec<_>>();
                let scenario = format!(
                    "readers_first = {}\n{}",
                    serde_json::to_string(&readers_first).unwrap(),
                    r#"
class Stream:
    def __init__(self, incarnation):
        self.incarnation = incarnation
        self.writer, self.reader = asyncio.Event(), asyncio.Event()
        self.frames = asyncio.Queue(maxsize=4)
        self.pending = b''
        self.eof = False
        self.read_index = 0

class Writer:
    def __init__(self, stream, started):
        self.stream, self.started = stream, started
        self.incarnation = stream.incarnation
    async def __aenter__(self):
        assert not self.stream.writer.is_set(), 'duplicate writer'
        self.stream.writer.set()
        self.started.set()
        await self.stream.reader.wait()
        return self
    async def __aexit__(self, error_type, error, traceback):
        if error is None:
            await self.stream.frames.put(None)
    async def write(self, frame):
        await self.stream.frames.put(bytes(frame))

class Reader:
    def __init__(self, stream):
        self.stream, self.incarnation = stream, stream.incarnation
    async def take(self, capacity):
        while not self.stream.pending and not self.stream.eof:
            chunk = await self.stream.frames.get()
            if chunk is None:
                self.stream.eof = True
            else:
                self.stream.pending = chunk
        while not self.stream.frames.empty() and not self.stream.eof:
            chunk = self.stream.frames.get_nowait()
            if chunk is None:
                self.stream.eof = True
            else:
                self.stream.pending += chunk
        count = min(capacity, len(self.stream.pending))
        chunk, self.stream.pending = self.stream.pending[:count], self.stream.pending[count:]
        return chunk
    async def read(self):
        capacity = [1, 7, 19, 4093][self.stream.read_index % 4]
        self.stream.read_index += 1
        return await self.take(capacity) or None
    async def readinto(self, target):
        chunk = await self.take(len(target))
        target[:len(chunk)] = chunk
        return len(chunk)

class Blob:
    def __init__(self, body, revision):
        self.body, self.revision = body, revision
    @contextlib.contextmanager
    def map(self):
        yield self.body

class BlobWriter(Blob):
    def __init__(self, world, path, length):
        super().__init__(bytearray(length), None)
        self.world, self.path = world, path
    async def __aenter__(self):
        return self
    async def __aexit__(self, error_type, error, traceback):
        if error is None:
            self.revision = self.world.next_revision()
            self.world.blobs[self.path] = self

class World:
    def __init__(self):
        self.streams = {}
        self.blobs = {}
        self.revision = 0
    def next_revision(self):
        self.revision += 1
        return self.revision
    def stream(self, path):
        if path not in self.streams:
            self.streams[path] = Stream(self.next_revision())
        return self.streams[path]

class Data:
    def __init__(self, world):
        self.world, self.started = world, asyncio.Event()
    def write_stream(self, path, replace):
        assert not replace
        return Writer(self.world.stream(path), self.started)
    async def read_stream(self, path):
        stream = self.world.stream(path)
        assert not stream.reader.is_set(), 'duplicate reader'
        stream.reader.set()
        self.started.set()
        await stream.writer.wait()
        return Reader(stream)
    def write_blob(self, path, length):
        self.started.set()
        return BlobWriter(self.world, path, length)
    async def read_blob(self, path):
        self.started.set()
        return self.world.blobs[path]
    async def lookup(self, path):
        self.started.set()
        if path not in self.world.blobs:
            raise OSError(errno.ENOENT, 'unpublished blob')
        blob = self.world.blobs[path]
        return types.SimpleNamespace(kind='blob', revision=blob.revision, active=False)

async def run(order):
    world, output, namespaces = World(), io.StringIO(), []
    for source in sources:
        namespace = {}
        exec(source, namespace)
        namespaces.append(namespace)
    tasks = []
    with contextlib.redirect_stdout(output):
        for index in order:
            data = Data(world)
            tasks.append(asyncio.create_task(namespaces[index]['main'](types.SimpleNamespace(data=data))))
            # Admit participants in an adverse order, including blob-only DAG
            # vertices whose initial namespace wait precedes any transfer.
            await data.started.wait()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert all(stream.writer.is_set() and stream.reader.is_set() and stream.frames.empty()
               for stream in world.streams.values()), 'unfinished rendezvous'
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    return [[record for record in records if record['process'] == namespace['PROCESS']]
            for namespace in namespaces]

async def scenario():
    print(json.dumps([await run(list(reversed(range(len(sources))))),
                      await run(readers_first)]))

asyncio.run(scenario())
"#
                );
                let observations: Vec<Vec<Vec<crate::ir::ActionObservation>>> =
                    serde_json::from_str(&execute_generated(&sources, &scenario)).unwrap();
                for records in observations {
                    let observation = generated_observation(&case, records);
                    crate::oracle::BehaviorOracle::verify(&case, &observation).unwrap_or_else(
                        |error| panic!("nodes={nodes} family={family:?}: {error:?}"),
                    );
                    let mut ledger = CoverageLedger::survivor(case.live_nodes.clone()).unwrap();
                    ledger.plan_case(&case).unwrap();
                    ledger
                        .bind_identity(EvidenceIdentity {
                            plan_digest: digest(b"generated topology plan"),
                            artifacts_digest: digest(b"generated topology binding adapter"),
                            deployment_generation: "generated-topology".to_owned(),
                            fixture_mapping_digest: digest(b"generated topology nodes"),
                        })
                        .unwrap();
                    ledger.observe_case(&case, &observation).unwrap();
                    assert_eq!(
                        ledger.observed.get(&CoverageKey::Topology { family }),
                        Some(&1)
                    );
                    assert_eq!(
                        ledger.observed.get(&CoverageKey::Scenario {
                            scenario: CoverageScenario::ConcurrentStartup,
                        }),
                        Some(&1)
                    );
                }
            }
        }
    }

    #[test]
    fn generated_predicates_cancel_withheld_public_binding_replies() {
        let operations = [
            ActionOp::AwaitEntry {
                path: "/cases/withheld/entry".to_owned(),
                expected_kind: "blob".to_owned(),
            },
            ActionOp::WaitForQuiescent {
                path: "/cases/withheld/stream".to_owned(),
            },
            ActionOp::GatedStreamWrite {
                path: "/cases/withheld/stream".to_owned(),
                replace: false,
                frames: vec![b"first".to_vec(), b"after-release".to_vec()],
                release_path: "/cases/withheld/release".to_owned(),
            },
        ];
        let sources = operations
            .into_iter()
            .map(|operation| {
                render_program(
                    &ProcessProgram {
                        id: "withheld".to_owned(),
                        logical_node_id: 1,
                        access: AccessSpec::unrestricted("withheld"),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(operation)],
                    },
                    Duration::from_millis(50),
                    None,
                )
            })
            .collect::<Vec<_>>();
        execute_generated(
            &sources,
            r#"
class Writer:
    incarnation = 1
    def __init__(self):
        self.frames = []
    async def __aenter__(self):
        return self
    async def __aexit__(self, *error):
        return False
    async def write(self, frame):
        self.frames.append(frame)

class Data:
    def __init__(self):
        self.cancelled = []
        self.writer = Writer()
    async def lookup(self, path):
        try:
            await asyncio.Future()
        finally:
            self.cancelled.append(path)
    def write_stream(self, *args, **kwargs):
        return self.writer

async def scenario():
    for source in sources:
        namespace, output, data = {}, io.StringIO(), Data()
        exec(source, namespace)
        with contextlib.redirect_stdout(output):
            try:
                await namespace['main'](types.SimpleNamespace(data=data))
            except TimeoutError:
                pass
            else:
                raise AssertionError('withheld reply passed')
            for _ in range(8):
                await asyncio.sleep(0)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(data.cancelled) == 1, data.cancelled
        assert b'after-release' not in b''.join(data.writer.frames), 'writer crossed withheld release'
        results = [record for record in records if record['outcome'] != 'barrier']
        assert len(results) == 1, records
        assert results[0]['outcome'] == 'error', records
        assert results[0]['error_type'] == 'TimeoutError', records
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()], 'leaked awaited request'

asyncio.run(scenario())
"#,
        );
    }

    #[test]
    fn absence_probe_looks_up_all_paths_concurrently_and_reports_in_order() {
        let paths = (0..12)
            .map(|index| format!("/cases/absence/{index}"))
            .collect::<Vec<_>>();
        let source =
            render_absence_probe_python("typed-absence-probe", &paths, Duration::from_millis(150));
        execute_generated(
            &[source],
            r#"
class Data:
    def __init__(self):
        self.started = 0
        self.all_started = asyncio.Event()
    async def lookup(self, path):
        self.started += 1
        if self.started == 12:
            self.all_started.set()
        await asyncio.wait_for(self.all_started.wait(), 0.05)
        raise OSError(errno.ENOENT, 'absent')

async def scenario():
    namespace, output, data = {}, io.StringIO(), Data()
    exec(sources[0], namespace)
    with contextlib.redirect_stdout(output):
        await namespace['main'](types.SimpleNamespace(data=data))
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert data.started == 12, data.started
    assert [record['step'] for record in records] == list(range(12)), records
    assert all(record['outcome'] == 'expected_error' for record in records), records
    assert all(record['errno'] == errno.ENOENT for record in records), records

asyncio.run(scenario())
"#,
        );
    }
}
