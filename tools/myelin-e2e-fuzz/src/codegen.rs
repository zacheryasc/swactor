//! Rendering of typed action IR into executable Python programs.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

use crate::ir::{
    Action, ActionOp, BehaviorCase, DescriptorFinish, DescriptorReadMethod, DescriptorWriteMethod,
    ExpectedOutcome, FailureInjection, LaunchFailureKind, ProcessProgram,
};

/// Wall-clock budget for generated in-process namespace barriers.
///
/// Generated Python waits for externally observable state (entry publication,
/// stream quiescence, unlink quiescence) by polling the namespace predicate
/// until this deadline; expiry fails the action instead of spinning a fixed
/// attempt count.
const NAMESPACE_BARRIER_DEADLINE_SECONDS: f64 = 30.0;

pub fn render_python(program: &ProcessProgram) -> String {
    let mut source = String::new();
    source
        .push_str("import asyncio\nimport errno\nimport hashlib\nimport json\nimport swactor\n\n");
    source.push_str(&format!("PROCESS = {}\n", python_string(&program.id)));
    source.push_str(
        "def emit(step, action, path, outcome, **facts):\n    record = {'process': PROCESS, 'step': step, 'action': action, 'path': path, 'outcome': outcome}\n    record.update(facts)\n    print(json.dumps(record, sort_keys=True), flush=True)\n\ndef digest(data):\n    return hashlib.sha256(data).hexdigest()\n\nasync def main(ctx):\n    data = ctx.data\n",
    );
    for (step, action) in program.actions.iter().enumerate() {
        render_action(&mut source, step, action);
    }
    source.push_str("\nswactor.run(main)\n");
    source
}

pub(crate) fn case_cleanup_paths(case: &BehaviorCase) -> Vec<String> {
    let mut paths = BTreeSet::new();
    for process in &case.processes {
        for action in &process.actions {
            let mut retain = |path: &str| {
                let path = if let Some(suffix) = path.strip_prefix("/runs/self") {
                    format!("/runs/{}{}", process.access.execution_id, suffix)
                } else {
                    path.to_owned()
                };
                if path.starts_with("/cases/") || path.starts_with("/runs/") {
                    paths.insert(path);
                }
            };
            retain(action.operation.path());
            if let ActionOp::Rename { destination, .. } = &action.operation {
                retain(destination);
            }
        }
    }
    paths.into_iter().collect()
}
pub fn render_case_python(case: &BehaviorCase, program: &ProcessProgram) -> String {
    match &case.failure {
        FailureInjection::LaunchFailure {
            process,
            kind: LaunchFailureKind::PythonSyntax,
        } if process == &program.id => "def invalid(:\n".to_owned(),
        FailureInjection::LaunchFailure {
            process,
            kind: LaunchFailureKind::PythonRuntime,
        } if process == &program.id => "import swactor\n\nasync def main(ctx):\n    raise RuntimeError('injected Python runtime failure')\n\nswactor.run(main)\n".to_owned(),
        _ => render_python(program),
    }
}

pub(crate) fn render_cleanup_python(paths: &[String]) -> String {
    let paths = serde_json::to_string(paths).expect("serialize cleanup paths");
    format!(
        "import asyncio\nimport errno\nimport swactor\n\nPATHS = {paths}\n\nasync def main(ctx):\n    loop = asyncio.get_running_loop()\n    for path in PATHS:\n        deadline = loop.time() + {NAMESPACE_BARRIER_DEADLINE_SECONDS}\n        while True:\n            try:\n                await ctx.data.unlink(path)\n            except OSError as error:\n                if error.errno == errno.ENOENT:\n                    break\n                if error.errno != errno.ENXIO:\n                    raise\n                if loop.time() >= deadline:\n                    raise RuntimeError(f'namespace path did not become quiescent: {{path}}')\n                await asyncio.sleep(0)\n            else:\n                break\n        try:\n            await ctx.data.lookup(path)\n        except OSError as error:\n            if error.errno != errno.ENOENT:\n                raise\n        else:\n            raise AssertionError(f'cleanup left namespace path visible: {{path}}')\n\nswactor.run(main)\n"
    )
}

fn render_action(source: &mut String, step: usize, action: &Action) {
    let class = action.operation.class().as_str();
    let path = python_string(action.operation.path());
    let _ = writeln!(source, "    try:");
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
                "            with writer.map() as mapped:\n                view = memoryview(mapped)\n                view[:] = payload\n                view.release()\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
            );
        }
        ActionOp::ReadBlob { .. } => {
            let _ = writeln!(source, "        blob = await data.read_blob({path})");
            source.push_str(
                "        with blob.map() as mapped:\n            payload = bytes(mapped)\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
            );
        }
        ActionOp::StreamWrite {
            chunks, replace, ..
        } => {
            let _ = writeln!(
                source,
                "        async with data.write_stream({path}, replace={}) as writer:",
                if *replace { "True" } else { "False" }
            );
            for chunk in chunks {
                let _ = writeln!(
                    source,
                    "            await writer.write(bytes.fromhex({}))",
                    python_string(&hex(chunk))
                );
            }
            let payload = chunks.concat();
            let _ = writeln!(
                source,
                "        payload = bytes.fromhex({})",
                python_string(&hex(&payload))
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
            );
        }
        ActionOp::StreamRead { .. } => {
            let _ = writeln!(source, "        reader = await data.read_stream({path})");
            source.push_str(
                "        chunks = []\n        while True:\n            chunk = await reader.read()\n            if chunk is None:\n                break\n            chunks.append(bytes(chunk))\n        payload = b''.join(chunks)\n        del reader\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
            );
        }
        ActionOp::StreamReadInto { buffer_sizes, .. } => {
            let sizes = serde_json::to_string(buffer_sizes).expect("serialize readinto sizes");
            let _ = writeln!(source, "        reader = await data.read_stream({path})");
            let _ = writeln!(source, "        buffer_sizes = {sizes}");
            source.push_str(
                "        chunks = []\n        read_index = 0\n        while True:\n            target = bytearray(buffer_sizes[read_index % len(buffer_sizes)])\n            count = await reader.readinto(target)\n            if count == 0:\n                break\n            chunks.append(bytes(target[:count]))\n            read_index += 1\n        payload = b''.join(chunks)\n        del reader\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
            );
        }
        ActionOp::Lookup { .. } => {
            let _ = writeln!(source, "        entry = await data.lookup({path})");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active)"
            );
        }
        ActionOp::AwaitEntry { .. } => {
            source.push_str(
                "        loop = asyncio.get_running_loop()\n        deadline = loop.time() + ",
            );
            let _ = writeln!(source, "{NAMESPACE_BARRIER_DEADLINE_SECONDS}");
            source.push_str(
                "        while True:\n            try:\n                entry = await data.lookup(",
            );
            let _ = writeln!(source, "{path})");
            source.push_str(
                "            except OSError as error:\n                if error.errno != errno.ENOENT:\n                    raise\n                if loop.time() >= deadline:\n                    raise TimeoutError('namespace barrier did not resolve') from error\n                await asyncio.sleep(0)\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active)"
            );
        }
        ActionOp::WaitForQuiescent { .. } => {
            source.push_str(
                "        loop = asyncio.get_running_loop()\n        deadline = loop.time() + ",
            );
            let _ = writeln!(source, "{NAMESPACE_BARRIER_DEADLINE_SECONDS}");
            source.push_str("        while True:\n            entry = await data.lookup(");
            let _ = writeln!(source, "{path})");
            source.push_str(
                "            if entry.kind != 'stream':\n                raise RuntimeError(f'expected stream namespace node, observed {entry.kind}')\n            if not entry.active:\n                break\n            if loop.time() >= deadline:\n                raise TimeoutError('stream namespace node did not become quiescent')\n            await asyncio.sleep(0)\n",
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', kind=entry.kind, revision=entry.revision, active=entry.active)"
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
                "        revision = await data.rename({path}, {destination}, replace={})",
                if *replace { "True" } else { "False" }
            );
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', revision=revision)"
            );
        }
        ActionOp::Unlink { .. } => {
            let _ = writeln!(source, "        revision = await data.unlink({path})");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', revision=revision)"
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
                    source.push_str("        await descriptor.write(payload)\n");
                }
                DescriptorWriteMethod::WriteFrom => {
                    source.push_str("        await descriptor.writefrom(payload)\n");
                }
                DescriptorWriteMethod::Mapping => {
                    source.push_str(
                        "        mapping = await descriptor.map(offset=0, length=len(payload), writable=True)\n        with mapping as mapped:\n            view = memoryview(mapped)\n            view[:] = payload\n            view.release()\n",
                    );
                }
            }
            render_descriptor_finish(source, *finish);
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
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
                        "        payload = await descriptor.read({})",
                        expected.len()
                    );
                }
                DescriptorReadMethod::ReadInto => {
                    let _ = writeln!(
                        source,
                        "        target = bytearray({})\n        count = await descriptor.readinto(target)\n        payload = bytes(target[:count])",
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
                        "        with mapping as mapped:\n            payload = bytes(mapped)\n",
                    );
                }
            }
            render_descriptor_finish(source, *finish);
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'ok', length=len(payload), digest=digest(payload))"
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
                "        emit({step}, {class:?}, {path}, 'ok', length=0, digest=digest(payload))"
            );
        }
    }
    source.push_str("    except BaseException as error:\n        error_number = getattr(error, 'errno', None)\n        error_type = type(error).__name__\n");
    match action.expected {
        ExpectedOutcome::Ok => {
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}')"
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
                "            emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}')"
            );
            source.push_str("            raise\n");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'expected_error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}')"
            );
        }
        ExpectedOutcome::Exception(expected) => {
            let expected = expected.as_str();
            let _ = writeln!(source, "        if error_type != {expected:?}:");
            let _ = writeln!(
                source,
                "            emit({step}, {class:?}, {path}, 'error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}')"
            );
            source.push_str("            raise\n");
            let _ = writeln!(
                source,
                "        emit({step}, {class:?}, {path}, 'expected_error', errno=error_number, error_type=error_type, error=f'{{error_type}}: {{error}}')"
            );
        }
    }
}

fn render_descriptor_finish(source: &mut String, finish: DescriptorFinish) {
    match finish {
        DescriptorFinish::Close => source.push_str("        await descriptor.close()\n"),
        DescriptorFinish::Abort => source.push_str("        await descriptor.abort()\n"),
        DescriptorFinish::Drop => {
            source.push_str("        del descriptor\n");
        }
        DescriptorFinish::CloseTwice => {
            source.push_str("        await descriptor.close()\n        await descriptor.close()\n");
        }
        DescriptorFinish::AbortTwice => {
            source.push_str("        await descriptor.abort()\n        await descriptor.abort()\n");
        }
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
