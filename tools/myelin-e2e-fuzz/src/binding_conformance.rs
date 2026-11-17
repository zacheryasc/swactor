//! Deterministic native/Python conformance, separate from the protocol campaign.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use crate::binding_cases::{
    PythonBindingCase, PythonBindingExpectation, PythonBindingOp, PythonException,
};

use crate::ir::{
    AccessSpec, Action, ActionObservation, ActionOp, BarrierObservation, BehaviorCase,
    CaseObservation, DescriptorFinish, DescriptorObservation, DescriptorReadMethod,
    DescriptorTerminalResult, DescriptorWriteMethod, FailureInjection, ProcessProgram,
};
use crate::oracle::BehaviorOracle;

/// A bounded, deterministic cross-binding corpus. No race winner, process stop,
/// Python-only exception, or generated topology is part of this gate.
pub fn binding_conformance_cases(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    if node_count == 0 {
        return Vec::new();
    }
    let mut cases = Vec::new();
    let root = format!("/cases/{seed}/binding-conformance");
    let blob = format!("{root}/blob");
    let renamed = format!("{root}/renamed");
    let payload: Vec<u8> = (0..4097)
        .map(|index| (index as u8).wrapping_add(seed as u8))
        .collect();
    cases.push(conformance_case(
        "binding-namespace",
        node_count,
        seed,
        vec![program(
            "namespace",
            1,
            vec![
                missing(&blob),
                Action::ok(ActionOp::PublishBlob {
                    path: blob.clone(),
                    bytes: payload.clone(),
                }),
                Action::ok(ActionOp::Lookup {
                    path: blob.clone(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::error(
                    ActionOp::StreamWrite {
                        path: blob.clone(),
                        chunks: vec![b"wrong-kind".to_vec()],
                        replace: false,
                    },
                    libc::ENXIO,
                ),
                Action::error(
                    ActionOp::StreamRead {
                        path: blob.clone(),
                        expected: Vec::new(),
                    },
                    libc::ENXIO,
                ),
                Action::ok(ActionOp::Rename {
                    source: blob.clone(),
                    destination: renamed.clone(),
                    replace: false,
                }),
                missing(&blob),
                Action::ok(ActionOp::ReadBlob {
                    path: renamed.clone(),
                    expected: payload.clone(),
                }),
                Action::ok(ActionOp::Unlink {
                    path: renamed.clone(),
                }),
                missing(&renamed),
            ],
        )],
    ));
    cases[0]
        .read_only_fixture_paths
        .insert(crate::corpus::SEEDED_MODEL_PATH.to_owned());

    let mut writes = Vec::new();
    let mut reads = Vec::new();
    for (index, (write_method, read_method)) in [
        (DescriptorWriteMethod::Write, DescriptorReadMethod::Read),
        (
            DescriptorWriteMethod::WriteFrom,
            DescriptorReadMethod::ReadInto,
        ),
        (
            DescriptorWriteMethod::Mapping,
            DescriptorReadMethod::Mapping,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let path = format!("{root}/descriptor-{index}");
        writes.push(descriptor_write(
            &path,
            &payload,
            write_method,
            DescriptorFinish::Close,
        ));
        reads.push(Action::ok(ActionOp::DescriptorRead {
            path,
            flags: libc::O_RDONLY,
            expected: payload.clone(),
            method: read_method,
            offset: 0,
            finish: DescriptorFinish::Close,
        }));
    }
    reads.push(Action::error(
        ActionOp::DescriptorRead {
            path: format!("{root}/descriptor-0"),
            flags: libc::O_RDONLY | libc::O_NONBLOCK,
            expected: Vec::new(),
            method: DescriptorReadMethod::Read,
            offset: 0,
            finish: DescriptorFinish::Close,
        },
        libc::ENOTSUP,
    ));
    reads.push(Action::error(
        ActionOp::DescriptorRead {
            path: format!("{root}/descriptor-0"),
            flags: libc::O_RDONLY,
            expected: vec![0; 2],
            method: DescriptorReadMethod::Mapping,
            offset: payload.len() as u64 - 1,
            finish: DescriptorFinish::Close,
        },
        libc::EINVAL,
    ));
    let mut reader = program("descriptor-reader", u64::from(node_count), reads);
    reader.depends_on.push("descriptor-writer".to_owned());
    cases.push(conformance_case(
        "binding-descriptors",
        node_count,
        seed,
        vec![program("descriptor-writer", 1, writes), reader],
    ));

    let mut lifecycle = Vec::new();
    for (label, finish) in [
        ("abort", DescriptorFinish::Abort),
        ("drop", DescriptorFinish::Drop),
    ] {
        let path = format!("{root}/{label}");
        lifecycle.push(descriptor_write(
            &path,
            b"discarded",
            DescriptorWriteMethod::Write,
            finish,
        ));
        lifecycle.push(missing(&path));
        lifecycle.push(descriptor_write(
            &path,
            b"reused",
            DescriptorWriteMethod::WriteFrom,
            DescriptorFinish::Close,
        ));
        lifecycle.push(Action::ok(ActionOp::ReadBlob {
            path,
            expected: b"reused".to_vec(),
        }));
    }
    for (label, finish) in [
        ("close-twice", DescriptorFinish::CloseTwice),
        ("abort-twice", DescriptorFinish::AbortTwice),
    ] {
        let path = format!("{root}/{label}");
        let mut action = descriptor_write(&path, b"terminal", DescriptorWriteMethod::Write, finish);
        action.expected = crate::ir::ExpectedOutcome::Error(libc::EBADF);
        lifecycle.push(action);
        lifecycle.push(if finish == DescriptorFinish::CloseTwice {
            Action::ok(ActionOp::ReadBlob {
                path,
                expected: b"terminal".to_vec(),
            })
        } else {
            missing(&path)
        });
    }
    cases.push(conformance_case(
        "binding-descriptor-lifecycle",
        node_count,
        seed,
        vec![program("lifecycle", 1, lifecycle)],
    ));

    let stream = format!("{root}/stream");
    let chunks = vec![Vec::new(), b"binary\0payload".to_vec(), payload];
    let expected = chunks.concat();
    cases.push(conformance_case(
        "binding-stream-framing",
        node_count,
        seed,
        vec![
            program(
                "stream-writer",
                1,
                vec![Action::ok(ActionOp::StreamWrite {
                    path: stream.clone(),
                    chunks,
                    replace: false,
                })],
            ),
            program(
                "stream-reader",
                u64::from(node_count),
                vec![
                    Action::ok(ActionOp::StreamReadInto {
                        path: stream.clone(),
                        expected,
                        buffer_sizes: vec![1, 7, 1024],
                    }),
                    Action::ok(ActionOp::WaitForQuiescent { path: stream }),
                ],
            ),
        ],
    ));

    let denied = format!("{root}/denied");
    let mut restricted = program(
        "restricted",
        1,
        vec![
            Action::error(
                ActionOp::Lookup {
                    path: denied.clone(),
                    expected_kind: "blob".to_owned(),
                },
                libc::EACCES,
            ),
            Action::error(
                ActionOp::PublishBlob {
                    path: denied,
                    bytes: b"denied".to_vec(),
                },
                libc::EACCES,
            ),
        ],
    );
    restricted.access.read_prefixes.clear();
    restricted.access.write_prefixes.clear();
    cases.push(conformance_case(
        "binding-access-errors",
        node_count,
        seed,
        vec![restricted],
    ));
    cases
}

fn program(id: &str, logical_node_id: u64, actions: Vec<Action>) -> ProcessProgram {
    ProcessProgram {
        id: id.to_owned(),
        logical_node_id,
        access: AccessSpec::unrestricted(id),
        depends_on: Vec::new(),
        actions,
    }
}

fn conformance_case(
    id: &str,
    node_count: u8,
    seed: u64,
    processes: Vec<ProcessProgram>,
) -> BehaviorCase {
    BehaviorCase {
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        id: id.to_owned(),
        seed,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes,
        failure: FailureInjection::None,
    }
}

fn missing(path: &str) -> Action {
    Action::error(
        ActionOp::Lookup {
            path: path.to_owned(),
            expected_kind: "blob".to_owned(),
        },
        libc::ENOENT,
    )
}

fn descriptor_write(
    path: &str,
    bytes: &[u8],
    method: DescriptorWriteMethod,
    finish: DescriptorFinish,
) -> Action {
    Action::ok(ActionOp::DescriptorWrite {
        path: path.to_owned(),
        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
        length: Some(bytes.len() as u64),
        bytes: bytes.to_vec(),
        method,
        finish,
    })
}

/// Verify each run independently, then compare the observable contract of the
/// same deterministic case under distinct attempt namespaces. Request IDs,
/// diagnostic text, lifecycle bookkeeping and absolute revision numbers are not
/// cross-binding outcomes. Revision equality/order, frame identities, payloads,
/// terminal errno, descriptor finalization and all causal barriers are retained.
pub fn compare_conformance(
    native_case: &BehaviorCase,
    native: &CaseObservation,
    python_case: &BehaviorCase,
    python: &CaseObservation,
) -> Result<(), String> {
    BehaviorOracle::verify(native_case, native)
        .map_err(|error| format!("native protocol verdict: {error}"))?;
    BehaviorOracle::verify(python_case, python)
        .map_err(|error| format!("Python conformance verdict: {error}"))?;
    let native_paths = canonical_paths(native_case);
    let python_paths = canonical_paths(python_case);
    if canonical_programs(native_case, &native_paths)
        != canonical_programs(python_case, &python_paths)
    {
        return Err("conformance runs do not describe the same normalized program".to_owned());
    }
    let native_results = normalized_results(native, &native_paths)?;
    let python_results = normalized_results(python, &python_paths)?;
    for (process, expected) in &native_results {
        let observed = python_results
            .get(process)
            .ok_or_else(|| format!("Python conformance is missing process {process}"))?;
        if expected != observed {
            let index = expected
                .iter()
                .zip(observed)
                .position(|(left, right)| left != right)
                .unwrap_or_else(|| expected.len().min(observed.len()));
            return Err(format!(
                "binding conformance mismatch for {process} record {index}: native={:?}, Python={:?}",
                expected.get(index),
                observed.get(index)
            ));
        }
    }
    if native_results.len() != python_results.len() {
        return Err("conformance process sets differ".to_owned());
    }
    Ok(())
}

fn canonical_paths(case: &BehaviorCase) -> BTreeMap<String, String> {
    let mut paths = BTreeMap::new();
    for process in &case.processes {
        for action in &process.actions {
            for path in action.operation.paths().into_iter().flatten() {
                let next = paths.len();
                paths
                    .entry(path.to_owned())
                    .or_insert_with(|| format!("path-{next}"));
            }
        }
    }
    paths
}

fn canonical_programs(
    case: &BehaviorCase,
    paths: &BTreeMap<String, String>,
) -> Vec<(String, u64, Vec<String>, Vec<Action>)> {
    case.processes
        .iter()
        .map(|program| {
            let mut actions = program.actions.clone();
            for action in &mut actions {
                action
                    .operation
                    .map_paths(|path| *path = paths[path].clone());
            }
            (
                program.id.clone(),
                program.logical_node_id,
                program.depends_on.clone(),
                actions,
            )
        })
        .collect()
}

fn normalized_results(
    observation: &CaseObservation,
    paths: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Vec<ActionObservation>>, String> {
    let mut revisions = BTreeSet::new();
    for result in observation
        .executions
        .iter()
        .flat_map(|execution| &execution.results)
    {
        revisions.extend(result.revision);
        revisions.extend(result.incarnation);
        match &result.barrier {
            Some(
                BarrierObservation::StreamOpened { incarnation }
                | BarrierObservation::StreamFrame { incarnation, .. }
                | BarrierObservation::StreamFirstFrame { incarnation }
                | BarrierObservation::StreamEof { incarnation },
            ) => {
                revisions.insert(*incarnation);
            }
            Some(BarrierObservation::MutationApplied {
                from_revision,
                to_revision,
            }) => {
                revisions.extend(from_revision);
                revisions.extend(to_revision);
            }
            _ => {}
        }
    }
    let revisions: BTreeMap<_, _> = revisions
        .into_iter()
        .enumerate()
        .map(|(index, revision)| (revision, index as u64 + 1))
        .collect();
    let mut executions = BTreeMap::new();
    for execution in &observation.executions {
        let mut records = Vec::with_capacity(execution.results.len());
        for result in &execution.results {
            let mut record = result.clone();
            record.path = paths
                .get(&record.path)
                .ok_or_else(|| format!("observation has unmodeled path {}", record.path))?
                .clone();
            record.error = None;
            record.error_type = None;
            record.revision = record.revision.map(|revision| revisions[&revision]);
            record.incarnation = record.incarnation.map(|revision| revisions[&revision]);
            if let Some(descriptor) = &mut record.descriptor {
                let (DescriptorObservation::Write {
                    terminal_results, ..
                }
                | DescriptorObservation::Read {
                    terminal_results, ..
                }) = descriptor;
                for terminal in terminal_results {
                    if let DescriptorTerminalResult::Error { errno, error_type } = terminal {
                        *error_type = if errno.is_some() {
                            "errno"
                        } else {
                            "protocol_error"
                        }
                        .to_owned();
                    }
                }
            }
            match &mut record.barrier {
                Some(
                    BarrierObservation::StreamOpened { incarnation }
                    | BarrierObservation::StreamFrame { incarnation, .. }
                    | BarrierObservation::StreamFirstFrame { incarnation }
                    | BarrierObservation::StreamEof { incarnation },
                ) => *incarnation = revisions[incarnation],
                Some(BarrierObservation::MutationApplied {
                    from_revision,
                    to_revision,
                }) => {
                    *from_revision = from_revision.map(|revision| revisions[&revision]);
                    *to_revision = to_revision.map(|revision| revisions[&revision]);
                }
                Some(BarrierObservation::ReleaseObserved { path }) => {
                    *path = paths
                        .get(path)
                        .ok_or_else(|| format!("barrier has unmodeled release path {path}"))?
                        .clone();
                }
                _ => {}
            }
            records.push((
                serde_json::to_string(&record).map_err(|error| error.to_string())?,
                record,
            ));
        }
        // Receipt order of independent milestones is not causal. Frame indices
        // and revision aliases retain the semantic ordering within each action.
        records.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        executions.insert(
            execution.process.clone(),
            records.into_iter().map(|(_, record)| record).collect(),
        );
    }
    Ok(executions)
}

/// Render one binding-only workload. Argument/buffer cases assert their exact
/// Python contract and return successfully. Syntax/runtime cases deliberately
/// fail at their respective interpreter phases.
pub fn render_python_binding_case(case: &PythonBindingCase) -> String {
    match &case.operation {
        PythonBindingOp::SyntaxFailure => "def invalid(:\n".to_owned(),
        PythonBindingOp::RuntimeFailure => {
            "async def main(ctx):\n    raise RuntimeError('injected Python runtime failure')\n"
                .to_owned()
        }
        PythonBindingOp::InvalidOpenAccess { path, flags } => {
            let path = serde_json::to_string(path).expect("binding path is serializable");
            format!(
                "async def main(ctx):\n    try:\n        descriptor = await ctx.data.open({path}, {flags})\n    except ValueError:\n        return\n    await descriptor.close()\n    raise AssertionError('invalid access mode accepted by Python binding')\n"
            )
        }
        PythonBindingOp::MappingExportClose { path, length } => {
            let path = serde_json::to_string(path).expect("binding path is serializable");
            format!(
                r#"async def main(ctx):
    descriptor = await ctx.data.open({path}, 0)
    mapping = None
    try:
        mapping = await descriptor.map(offset=0, length={length}, writable=False)
        view = memoryview(mapping)
        before = bytes(view)
        try:
            try:
                mapping.close()
            except BufferError:
                if bytes(view) != before:
                    raise AssertionError('failed close invalidated the exported buffer')
            else:
                raise AssertionError('mapping closed with an active buffer export')
        finally:
            view.release()
        mapping.close()
        try:
            leaked = memoryview(mapping)
        except BufferError:
            pass
        else:
            leaked.release()
            raise AssertionError('closed mapping accepted another buffer export')
    finally:
        if mapping is not None:
            mapping.close()
        await descriptor.close()
"#
            )
        }
    }
}

/// Define `async binding_checks(ctx)` for an explicitly selected Python gate
/// workload. The checks use real descriptor/mapping objects from its bootstrap
/// context; there are no synthetic binding objects. Results go to stderr so the
/// neutral action transport cannot mistake them for protocol observations.
pub fn render_python_binding_checks(cases: &[PythonBindingCase], budget: Duration) -> String {
    let mut source = String::from(
        "\nimport asyncio\nimport json\nimport sys\n\nasync def binding_checks(ctx):\n    loop = asyncio.get_running_loop()\n",
    );
    let _ = writeln!(
        source,
        "    deadline = loop.time() + {:?}",
        budget.as_secs_f64()
    );
    for case in cases {
        let expected = match (&case.operation, case.expected) {
            (
                PythonBindingOp::InvalidOpenAccess { .. },
                PythonBindingExpectation::Exception(PythonException::ValueError),
            ) => "ValueError",
            (
                PythonBindingOp::MappingExportClose { .. },
                PythonBindingExpectation::Exception(PythonException::BufferError),
            ) => "BufferError",
            (PythonBindingOp::SyntaxFailure, PythonBindingExpectation::BootstrapFailure) => {
                "SyntaxError"
            }
            (PythonBindingOp::RuntimeFailure, PythonBindingExpectation::RuntimeFailure) => {
                "RuntimeError"
            }
            _ => panic!("binding case {} has an incompatible expectation", case.id),
        };
        let workload = serde_json::to_string(&render_python_binding_case(case))
            .expect("binding source is serializable");
        let name = serde_json::to_string(&case.id).expect("binding identity is serializable");
        let _ = writeln!(
            source,
            "    if loop.time() >= deadline:\n        raise TimeoutError('Python binding conformance budget expired')\n    namespace = {{}}"
        );
        match &case.operation {
            PythonBindingOp::SyntaxFailure => {
                let _ = writeln!(
                    source,
                    "    try:\n        compile({workload}, {name}, 'exec')\n    except SyntaxError:\n        pass\n    else:\n        raise AssertionError('invalid Python source passed compilation')"
                );
            }
            PythonBindingOp::RuntimeFailure => {
                let _ = writeln!(
                    source,
                    "    exec(compile({workload}, {name}, 'exec'), namespace)\n    try:\n        await asyncio.wait_for(namespace['main'](ctx), timeout=deadline - loop.time())\n    except RuntimeError:\n        pass\n    else:\n        raise AssertionError('injected Python runtime failure disappeared')"
                );
            }
            _ => {
                let _ = writeln!(
                    source,
                    "    exec(compile({workload}, {name}, 'exec'), namespace)\n    await asyncio.wait_for(namespace['main'](ctx), timeout=deadline - loop.time())"
                );
            }
        }
        let _ = writeln!(
            source,
            "    print(json.dumps({{'type': 'python_binding_conformance', 'case_id': {name}, 'outcome': 'expected_error', 'error_type': '{expected}'}}, separators=(',', ':')), file=sys.stderr, flush=True)"
        );
    }
    source
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{ExecutionObservation, TransferObservation};
    use std::process::{Command, Stdio};

    fn result(case: &BehaviorCase, step: usize) -> ActionObservation {
        let program = &case.processes[0];
        let action = &program.actions[step];
        ActionObservation {
            process: program.id.clone(),
            step,
            action: action.operation.class().as_str().to_owned(),
            path: action.operation.path().to_owned(),
            outcome: "ok".to_owned(),
            length: None,
            digest: None,
            kind: None,
            revision: None,
            active: None,
            errno: None,
            error_type: None,
            error: None,
            descriptor: None,
            transfer: None,
            barrier: None,
            incarnation: None,
            token: None,
            lap: None,
        }
    }

    fn observation(case: &BehaviorCase, results: Vec<ActionObservation>) -> CaseObservation {
        let program = &case.processes[0];
        CaseObservation {
            case_id: case.id.clone(),
            executions: vec![ExecutionObservation {
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
            }],
        }
    }

    #[test]
    fn comparison_accepts_scoped_paths_and_revision_renumbering() {
        let fixture = crate::corpus::SEEDED_MODEL_PATH;
        let mut case = conformance_case(
            "comparison",
            2,
            17,
            vec![program(
                "probe",
                1,
                vec![
                    Action::ok(ActionOp::Lookup {
                        path: fixture.to_owned(),
                        expected_kind: "blob".to_owned(),
                    }),
                    missing("/cases/comparison/missing"),
                ],
            )],
        );
        case.read_only_fixture_paths.insert(fixture.to_owned());
        let native_case = case.for_attempt(1, true);
        let python_case = case.for_attempt(2, true);
        let observed = |case: &BehaviorCase, revision, error_type: &str| {
            let mut lookup = result(case, 0);
            lookup.kind = Some("blob".to_owned());
            lookup.revision = Some(revision);
            lookup.active = Some(false);
            let mut missing = result(case, 1);
            missing.outcome = "expected_error".to_owned();
            missing.errno = Some(libc::ENOENT);
            missing.error_type = Some(error_type.to_owned());
            missing.error = Some(format!("diagnostic from {error_type}"));
            observation(case, vec![lookup, missing])
        };
        let native = observed(&native_case, 7, "errno");
        let python = observed(&python_case, 91, "FileNotFoundError");
        compare_conformance(&native_case, &native, &python_case, &python).unwrap();
    }

    #[test]
    fn matching_corruption_is_not_conformance() {
        let case = conformance_case(
            "comparison",
            2,
            17,
            vec![program(
                "writer",
                1,
                vec![Action::ok(ActionOp::PublishBlob {
                    path: "/cases/comparison/blob".to_owned(),
                    bytes: b"expected".to_vec(),
                })],
            )],
        );
        case.validate().unwrap();
        let mut corrupted = result(&case, 0);
        corrupted.length = Some(7);
        corrupted.digest = Some(crate::oracle::digest(b"corrupt"));
        corrupted.transfer = Some(TransferObservation {
            length: 7,
            digest: crate::oracle::digest(b"corrupt"),
            complete: true,
        });
        let observed = observation(&case, vec![corrupted]);
        assert!(compare_conformance(&case, &observed, &case, &observed).is_err());
    }

    #[test]
    fn interpreter_failure_checks_execute_without_protocol_records() {
        let cases: Vec<_> = crate::binding_cases::python_binding_corpus(17)
            .into_iter()
            .filter(|case| {
                matches!(
                    case.operation,
                    PythonBindingOp::SyntaxFailure | PythonBindingOp::RuntimeFailure
                )
            })
            .collect();
        let mut source = render_python_binding_checks(&cases, Duration::from_secs(2));
        source.push_str("\nasyncio.run(binding_checks(None))\n");
        let child = swactor_process::command_spawn(
            Command::new("python3")
                .args(["-c", &source])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped()),
        )
        .expect("spawn binding interpreter checks");
        let output = swactor_process::child_wait_with_output(child).unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout.is_empty(),
            "binding evidence leaked into protocol transport"
        );
        let records: Vec<serde_json::Value> = String::from_utf8(output.stderr)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .map(|record| record["error_type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["SyntaxError", "RuntimeError"]
        );
        assert!(
            records
                .iter()
                .all(|record| record["outcome"] == "expected_error")
        );
    }
}
