//! Deterministic stable/failure corpus and random short-DAG generation.

use std::collections::{BTreeMap, BTreeSet};

use crate::budget::Budget;
use crate::ir::{
    AccessSpec, Action, ActionClass, ActionOp, BehaviorCase, CoverageScenario, DataEdge, DataKind,
    DataRoute, DescriptorFinish, DescriptorReadMethod, DescriptorWriteMethod, FailureInjection,
    LaunchFailureKind, ProcessProgram, ProcessStopPhase, PythonException, TopologyFamily,
};
use crate::ir::{
    BARRIER_LAP_COMPLETED, BARRIER_TOKEN_FORWARDED, BARRIER_TOKEN_RECEIVED, EVIDENCE_HINT_BARRIER,
    EVIDENCE_HINT_EDGE_INDEX, EVIDENCE_HINT_LAP, EVIDENCE_HINT_TOKEN,
};

pub(crate) const SEEDED_MODEL_PATH: &str = "/models/tiny-linear/weights";
pub(crate) const SEEDED_MODEL_BYTES: &[u8] =
    include_bytes!("../../../apps/myelin/testdata/tiny_linear.weights");

pub fn stable_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let mut cases = Vec::new();
    for source_node in 1..=node_count {
        for destination_node in 1..=node_count {
            if source_node == destination_node {
                continue;
            }
            let blob_id = format!("blob-{source_node}-{destination_node}");
            let blob_path = format!("/cases/{seed}/{blob_id}");
            let payload = boundary_payload(seed ^ u64::from(source_node), 65_537);
            cases.push(BehaviorCase {
                id: blob_id.clone(),
                seed,
                schema_version: crate::ir::CASE_SCHEMA_VERSION,
                generator_version: crate::ir::GENERATOR_VERSION,
                live_nodes: crate::ir::contiguous_nodes(node_count),
                topology: Default::default(),
                scenarios: Default::default(),
                routes: Vec::new(),
                read_only_fixture_paths: Default::default(),
                resource_bounds: Default::default(),
                processes: vec![
                    ProcessProgram {
                        id: format!("publisher-{source_node}"),
                        logical_node_id: u64::from(source_node),
                        access: AccessSpec::unrestricted(format!("{blob_id}-publisher")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::PublishBlob {
                            path: blob_path.clone(),
                            bytes: payload.clone(),
                        })],
                    },
                    ProcessProgram {
                        id: format!("consumer-{destination_node}"),
                        logical_node_id: u64::from(destination_node),
                        access: AccessSpec::unrestricted(format!("{blob_id}-consumer")),
                        depends_on: vec![format!("publisher-{source_node}")],
                        actions: vec![Action::ok(ActionOp::ReadBlob {
                            path: blob_path,
                            expected: payload,
                        })],
                    },
                ],
                failure: FailureInjection::None,
            });

            let stream_id = format!("stream-{source_node}-{destination_node}");
            let stream_path = format!("/cases/{seed}/{stream_id}");
            let chunks = vec![
                boundary_payload(seed ^ u64::from(destination_node), 4095),
                boundary_payload(seed.rotate_left(7), 1),
                boundary_payload(seed.rotate_right(11), 4097),
            ];
            cases.push(BehaviorCase {
                id: stream_id.clone(),
                seed,
                schema_version: crate::ir::CASE_SCHEMA_VERSION,
                generator_version: crate::ir::GENERATOR_VERSION,
                live_nodes: crate::ir::contiguous_nodes(node_count),
                topology: Default::default(),
                scenarios: Default::default(),
                routes: Vec::new(),
                read_only_fixture_paths: Default::default(),
                resource_bounds: Default::default(),
                processes: vec![
                    ProcessProgram {
                        id: format!("stream-source-{source_node}"),
                        logical_node_id: u64::from(source_node),
                        access: AccessSpec::unrestricted(format!("{stream_id}-source")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::StreamWrite {
                            path: stream_path.clone(),
                            chunks: chunks.clone(),
                            replace: false,
                        })],
                    },
                    ProcessProgram {
                        id: format!("stream-sink-{destination_node}"),
                        logical_node_id: u64::from(destination_node),
                        access: AccessSpec::unrestricted(format!("{stream_id}-sink")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::StreamRead {
                            path: stream_path,
                            expected: chunks.concat(),
                        })],
                    },
                ],
                failure: FailureInjection::None,
            });
        }
    }
    cases.push(BehaviorCase {
        id: format!("seeded-model-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "model-reader".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("seeded-model-reader"),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::ReadBlob {
                path: SEEDED_MODEL_PATH.to_owned(),
                expected: SEEDED_MODEL_BYTES.to_vec(),
            })],
        }],
        failure: FailureInjection::None,
    });
    let namespace_source = format!("/cases/{seed}/namespace/source");
    let namespace_destination = format!("/cases/{seed}/namespace/destination");
    cases.push(BehaviorCase {
        id: format!("namespace-mutations-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "namespace-mutator".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("namespace-mutator"),
            depends_on: Vec::new(),
            actions: vec![
                Action::ok(ActionOp::PublishBlob {
                    path: namespace_source.clone(),
                    bytes: b"renamed-value".to_vec(),
                }),
                Action::ok(ActionOp::Lookup {
                    path: namespace_source.clone(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(ActionOp::PublishBlob {
                    path: namespace_destination.clone(),
                    bytes: b"displaced-value".to_vec(),
                }),
                Action::ok(ActionOp::Rename {
                    source: namespace_source,
                    destination: namespace_destination.clone(),
                    replace: true,
                }),
                Action::ok(ActionOp::ReadBlob {
                    path: namespace_destination.clone(),
                    expected: b"renamed-value".to_vec(),
                }),
                Action::ok(ActionOp::Unlink {
                    path: namespace_destination.clone(),
                }),
                Action::error(
                    ActionOp::Lookup {
                        path: namespace_destination,
                        expected_kind: "blob".to_owned(),
                    },
                    libc::ENOENT,
                ),
            ],
        }],
        failure: FailureInjection::None,
    });
    let replacement_path = format!("/cases/{seed}/stream-replacement");
    cases.push(BehaviorCase {
        id: format!("stream-replacement-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "replacement-first-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("replacement-first-writer"),
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::StreamWrite {
                        path: replacement_path.clone(),
                        chunks: vec![b"first".to_vec()],
                        replace: false,
                    }),
                    Action::ok(ActionOp::PublishBlob {
                        path: format!("{replacement_path}-first-complete"),
                        bytes: Vec::new(),
                    }),
                ],
            },
            ProcessProgram {
                id: "replacement-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("replacement-reader"),
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::StreamRead {
                        path: replacement_path.clone(),
                        expected: b"first".to_vec(),
                    }),
                    Action::ok(ActionOp::AwaitEntry {
                        path: format!("{replacement_path}-first-complete"),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::WaitForQuiescent {
                        path: replacement_path.clone(),
                    }),
                    Action::ok(ActionOp::PublishBlob {
                        path: format!("{replacement_path}-quiescent"),
                        bytes: Vec::new(),
                    }),
                    // Either endpoint may create the next incarnation. This
                    // reader's quiescence-before-open evidence proves the
                    // transition even when the replacement writer joins it.
                    Action::ok(ActionOp::StreamRead {
                        path: replacement_path.clone(),
                        expected: b"second".to_vec(),
                    }),
                ],
            },
            ProcessProgram {
                id: "replacement-second-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("replacement-second-writer"),
                depends_on: vec!["replacement-first-writer".to_owned()],
                actions: vec![
                    Action::ok(ActionOp::AwaitEntry {
                        path: format!("{replacement_path}-quiescent"),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::StreamWrite {
                        path: replacement_path,
                        chunks: vec![b"second".to_vec()],
                        replace: true,
                    }),
                ],
            },
        ],
        failure: FailureInjection::None,
    });
    // An active stream is displaced mid-flight: the stale writer and a
    // consumer attached to the displaced incarnation observe ESTALE, and a
    // fresh consumer of the replacement incarnation completes cleanly.
    let active_root = format!("/cases/{seed}/active-stream-replacement");
    let active_stream = format!("{active_root}/stream");
    let active_marker = format!("{active_root}/reader-attached");
    let active_fresh_marker = format!("{active_root}/fresh-reader-attached");
    let active_release = format!("{active_root}/release");
    cases.push(BehaviorCase {
        id: format!("active-stream-replacement-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: [CoverageScenario::ActiveMutation].into(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "active-replacement-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("active-replacement-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::error(
                    ActionOp::GatedStreamWrite {
                        path: active_stream.clone(),
                        replace: false,
                        frames: vec![b"active-first".to_vec(), b"active-second".to_vec()],
                        release_path: active_release.clone(),
                    },
                    libc::ESTALE,
                )],
            },
            ProcessProgram {
                id: "active-replacement-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("active-replacement-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::error(
                    ActionOp::GatedStreamRead {
                        expected: b"active-firstactive-second".to_vec(),
                        path: active_stream.clone(),
                        observed_path: active_marker.clone(),
                        retry_attach: false,
                        park_after_first_frame: false,
                    },
                    libc::ESTALE,
                )],
            },
            ProcessProgram {
                id: "active-replacement-replacer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("active-replacement-replacer"),
                // The replacer must not depend on the writer's completion:
                // the writer's gate waits for the release published
                // immediately after the fenced replacement completes.
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::AwaitEntry {
                        path: active_marker.clone(),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::GatedStreamWrite {
                        path: active_stream.clone(),
                        replace: true,
                        frames: vec![b"replacement".to_vec()],
                        release_path: active_fresh_marker.clone(),
                    }),
                    Action::ok(ActionOp::PublishBlob {
                        path: active_release,
                        bytes: Vec::new(),
                    }),
                ],
            },
            ProcessProgram {
                // The displaced reader's terminal event proves that the
                // replacement incarnation exists. Starting the fresh reader
                // earlier can bind it to the old incarnation, whose fencing
                // correctly terminates the entire contextual process.
                id: "active-replacement-fresh-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("active-replacement-fresh-reader"),
                depends_on: vec!["active-replacement-reader".to_owned()],
                actions: vec![
                    Action::ok(ActionOp::AwaitEntry {
                        path: active_marker,
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::GatedStreamRead {
                        path: active_stream,
                        expected: b"replacement".to_vec(),
                        observed_path: active_fresh_marker,
                        retry_attach: true,
                        park_after_first_frame: false,
                    }),
                ],
            },
        ],
        failure: FailureInjection::None,
    });
    cases.push(BehaviorCase {
        id: format!("authorization-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: [CoverageScenario::Authorization].into(),
        routes: Vec::new(),
        read_only_fixture_paths: [SEEDED_MODEL_PATH.to_owned()].into(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "read-only".to_owned(),
                logical_node_id: 1,
                access: AccessSpec {
                    execution_id: "authorization-read-only".to_owned(),
                    read_prefixes: vec!["/models".to_owned()],
                    write_prefixes: Vec::new(),
                },
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::ReadBlob {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        expected: SEEDED_MODEL_BYTES.to_vec(),
                    }),
                    Action::error(
                        ActionOp::PublishBlob {
                            path: format!("/cases/{seed}/unauthorized-publication"),
                            bytes: vec![1],
                        },
                        libc::EACCES,
                    ),
                ],
            },
            ProcessProgram {
                id: "write-only".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec {
                    execution_id: "authorization-write-only".to_owned(),
                    read_prefixes: Vec::new(),
                    write_prefixes: vec!["/cases".to_owned()],
                },
                depends_on: Vec::new(),
                actions: vec![
                    Action::ok(ActionOp::PublishBlob {
                        path: format!("/cases/{seed}/denied/source"),
                        bytes: b"denied-source".to_vec(),
                    }),
                    Action::error(
                        ActionOp::ReadBlob {
                            path: SEEDED_MODEL_PATH.to_owned(),
                            expected: SEEDED_MODEL_BYTES.to_vec(),
                        },
                        libc::EACCES,
                    ),
                ],
            },
            ProcessProgram {
                id: "rename-guard".to_owned(),
                logical_node_id: 1,
                access: AccessSpec {
                    execution_id: "authorization-rename".to_owned(),
                    read_prefixes: Vec::new(),
                    write_prefixes: vec![format!("/cases/{seed}/allowed")],
                },
                depends_on: vec!["write-only".to_owned()],
                actions: vec![
                    Action::ok(ActionOp::PublishBlob {
                        path: format!("/cases/{seed}/allowed/source"),
                        bytes: b"allowed-source".to_vec(),
                    }),
                    Action::error(
                        ActionOp::Rename {
                            source: format!("/cases/{seed}/denied/source"),
                            destination: format!("/cases/{seed}/allowed/destination"),
                            replace: false,
                        },
                        libc::EACCES,
                    ),
                    Action::error(
                        ActionOp::Rename {
                            source: format!("/cases/{seed}/allowed/source"),
                            destination: format!("/cases/{seed}/denied/destination"),
                            replace: false,
                        },
                        libc::EACCES,
                    ),
                ],
            },
        ],
        failure: FailureInjection::None,
    });
    let isolation_path = format!("/runs/self/isolation/{seed}");
    cases.push(BehaviorCase {
        id: format!("self-isolation-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "self-a".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("self-a"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::PublishBlob {
                    path: isolation_path.clone(),
                    bytes: vec![0x5a],
                })],
            },
            ProcessProgram {
                id: "self-b".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("self-b"),
                depends_on: vec!["self-a".to_owned()],
                actions: vec![Action::error(
                    ActionOp::Lookup {
                        path: isolation_path,
                        expected_kind: "blob".to_owned(),
                    },
                    libc::ENOENT,
                )],
            },
        ],
        failure: FailureInjection::None,
    });
    for length in [0_usize, 1, 4095, 4096, 4097] {
        let id = format!("boundary-{length}-{seed}");
        let path = format!("/cases/{seed}/boundary/{length}");
        let bytes = boundary_payload(seed ^ length as u64, length);
        cases.push(BehaviorCase {
            id: id.clone(),
            seed,
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            live_nodes: crate::ir::contiguous_nodes(node_count),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: Default::default(),
            processes: vec![
                ProcessProgram {
                    id: "boundary-publisher".to_owned(),
                    logical_node_id: 1,
                    access: AccessSpec::unrestricted(format!("{id}-publisher")),
                    depends_on: Vec::new(),
                    actions: vec![Action::ok(ActionOp::PublishBlob {
                        path: path.clone(),
                        bytes: bytes.clone(),
                    })],
                },
                ProcessProgram {
                    id: "boundary-reader".to_owned(),
                    logical_node_id: u64::from(node_count),
                    access: AccessSpec::unrestricted(format!("{id}-reader")),
                    depends_on: vec!["boundary-publisher".to_owned()],
                    actions: vec![Action::ok(ActionOp::ReadBlob {
                        path,
                        expected: bytes,
                    })],
                },
            ],
            failure: FailureInjection::None,
        });
    }
    let backpressure_path = format!("/cases/{seed}/stream-backpressure");
    let backpressure_chunks = (0..9)
        .map(|chunk| boundary_payload(seed.rotate_left(chunk + 1), 65_537))
        .collect::<Vec<_>>();
    cases.push(BehaviorCase {
        id: format!("stream-backpressure-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "backpressure-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("backpressure-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: backpressure_path.clone(),
                    chunks: backpressure_chunks.clone(),
                    replace: false,
                })],
            },
            ProcessProgram {
                id: "backpressure-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("backpressure-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamReadInto {
                    path: backpressure_path,
                    expected: backpressure_chunks.concat(),
                    buffer_sizes: vec![1, 4095, 65_536, 17],
                })],
            },
        ],
        failure: FailureInjection::None,
    });
    cases.extend(descriptor_corpus(node_count, seed));
    cases.extend(namespace_edge_corpus(node_count, seed));
    cases.extend(concurrent_namespace_corpus(node_count, seed));
    cases.extend(ordered_pair_corpus(node_count, seed));
    cases
}

fn descriptor_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let mut cases = Vec::new();
    let write_methods = [
        DescriptorWriteMethod::Write,
        DescriptorWriteMethod::WriteFrom,
        DescriptorWriteMethod::Mapping,
    ];
    let read_methods = [
        DescriptorReadMethod::Read,
        DescriptorReadMethod::ReadInto,
        DescriptorReadMethod::Mapping,
    ];
    for (index, (write_method, read_method)) in
        write_methods.into_iter().zip(read_methods).enumerate()
    {
        let id = format!("descriptor-method-{index}-{seed}");
        let path = format!("/cases/{seed}/descriptor/method-{index}");
        let bytes = boundary_payload(seed.rotate_left(index as u32 + 1), 4097 + index);
        cases.push(BehaviorCase {
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            id: id.clone(),
            seed: seed ^ index as u64,
            live_nodes: crate::ir::contiguous_nodes(node_count),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: Default::default(),
            processes: vec![
                ProcessProgram {
                    id: "descriptor-writer".to_owned(),
                    logical_node_id: 1,
                    access: AccessSpec::unrestricted(format!("{id}-writer")),
                    depends_on: Vec::new(),
                    actions: vec![Action::ok(ActionOp::DescriptorWrite {
                        path: path.clone(),
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(bytes.len() as u64),
                        bytes: bytes.clone(),
                        method: write_method,
                        finish: DescriptorFinish::Close,
                    })],
                },
                ProcessProgram {
                    id: "descriptor-reader".to_owned(),
                    logical_node_id: u64::from(node_count),
                    access: AccessSpec::unrestricted(format!("{id}-reader")),
                    depends_on: vec!["descriptor-writer".to_owned()],
                    actions: vec![Action::ok(ActionOp::DescriptorRead {
                        path,
                        flags: libc::O_RDONLY,
                        expected: bytes,
                        method: read_method,
                        offset: 0,
                        finish: DescriptorFinish::Close,
                    })],
                },
            ],
            failure: FailureInjection::None,
        });
    }

    for finish in [DescriptorFinish::Abort, DescriptorFinish::Drop] {
        let finish_name = match finish {
            DescriptorFinish::Abort => "abort",
            DescriptorFinish::Drop => "drop",
            _ => unreachable!("fixed unfinished-writer corpus"),
        };
        let id = format!("descriptor-{finish_name}-{seed}");
        let path = format!("/cases/{seed}/descriptor/{finish_name}");
        cases.push(BehaviorCase {
            id: id.clone(),
            seed,
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            live_nodes: crate::ir::contiguous_nodes(node_count),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: Default::default(),
            processes: vec![
                ProcessProgram {
                    id: "unfinished-writer".to_owned(),
                    logical_node_id: 1,
                    access: AccessSpec::unrestricted(format!("{id}-writer")),
                    depends_on: Vec::new(),
                    actions: vec![Action::ok(ActionOp::DescriptorWrite {
                        path: path.clone(),
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(3),
                        bytes: b"raw".to_vec(),
                        method: DescriptorWriteMethod::Write,
                        finish,
                    })],
                },
                ProcessProgram {
                    id: "unfinished-observer".to_owned(),
                    logical_node_id: u64::from(node_count),
                    access: AccessSpec::unrestricted(format!("{id}-observer")),
                    depends_on: vec!["unfinished-writer".to_owned()],
                    actions: vec![
                        Action::error(
                            ActionOp::Lookup {
                                path: path.clone(),
                                expected_kind: "blob".to_owned(),
                            },
                            libc::ENOENT,
                        ),
                        Action::ok(ActionOp::DescriptorWrite {
                            path: path.clone(),
                            flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                            length: Some(6),
                            bytes: b"reused".to_vec(),
                            method: DescriptorWriteMethod::Write,
                            finish: DescriptorFinish::Close,
                        }),
                        Action::ok(ActionOp::ReadBlob {
                            path,
                            expected: b"reused".to_vec(),
                        }),
                    ],
                },
            ],
            failure: FailureInjection::None,
        });
    }

    let repeated_path = format!("/cases/{seed}/descriptor/repeated-close");
    cases.push(BehaviorCase {
        id: format!("descriptor-repeated-terminal-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "descriptor-repeater".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("descriptor-repeater"),
            depends_on: Vec::new(),
            actions: vec![
                Action::error(
                    ActionOp::DescriptorWrite {
                        path: repeated_path,
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(1),
                        bytes: vec![0xa5],
                        method: DescriptorWriteMethod::WriteFrom,
                        finish: DescriptorFinish::CloseTwice,
                    },
                    libc::EBADF,
                ),
                Action::error(
                    ActionOp::DescriptorWrite {
                        path: format!("/cases/{seed}/descriptor/repeated-abort"),
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(1),
                        bytes: vec![0x5a],
                        method: DescriptorWriteMethod::Write,
                        finish: DescriptorFinish::AbortTwice,
                    },
                    libc::EBADF,
                ),
            ],
        }],
        failure: FailureInjection::None,
    });

    cases.push(BehaviorCase {
        id: format!("descriptor-invalid-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "descriptor-invalid".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("descriptor-invalid"),
            depends_on: Vec::new(),
            actions: vec![
                Action::exception(
                    ActionOp::DescriptorRead {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        flags: libc::O_ACCMODE,
                        expected: Vec::new(),
                        method: DescriptorReadMethod::Read,
                        offset: 0,
                        finish: DescriptorFinish::Close,
                    },
                    PythonException::ValueError,
                ),
                Action::error(
                    ActionOp::DescriptorRead {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        flags: libc::O_RDONLY | libc::O_NONBLOCK,
                        expected: Vec::new(),
                        method: DescriptorReadMethod::Read,
                        offset: 0,
                        finish: DescriptorFinish::Close,
                    },
                    libc::ENOTSUP,
                ),
                Action::error(
                    ActionOp::DescriptorRead {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        flags: libc::O_RDONLY,
                        expected: vec![0; 2],
                        method: DescriptorReadMethod::Mapping,
                        offset: SEEDED_MODEL_BYTES.len() as u64 - 1,
                        finish: DescriptorFinish::Close,
                    },
                    libc::EINVAL,
                ),
                Action::exception(
                    ActionOp::MappingExportClose {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        length: SEEDED_MODEL_BYTES.len() as u64,
                    },
                    PythonException::BufferError,
                ),
            ],
        }],
        failure: FailureInjection::None,
    });
    cases
}

fn namespace_edge_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let mut cases = Vec::new();

    let occupied_source = format!("/cases/{seed}/namespace/no-replace-source");
    let occupied_destination = format!("/cases/{seed}/namespace/no-replace-destination");
    cases.push(BehaviorCase {
        id: format!("namespace-no-replace-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "no-replace-mutator".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted("no-replace-mutator"),
            depends_on: Vec::new(),
            actions: vec![
                Action::ok(ActionOp::PublishBlob {
                    path: occupied_source.clone(),
                    bytes: b"source".to_vec(),
                }),
                Action::ok(ActionOp::PublishBlob {
                    path: occupied_destination.clone(),
                    bytes: b"destination".to_vec(),
                }),
                Action::error(
                    ActionOp::Rename {
                        source: occupied_source.clone(),
                        destination: occupied_destination.clone(),
                        replace: false,
                    },
                    libc::EEXIST,
                ),
                Action::ok(ActionOp::ReadBlob {
                    path: occupied_source,
                    expected: b"source".to_vec(),
                }),
                Action::ok(ActionOp::ReadBlob {
                    path: occupied_destination,
                    expected: b"destination".to_vec(),
                }),
            ],
        }],
        failure: FailureInjection::None,
    });

    let quiescent_source = format!("/cases/{seed}/namespace/quiescent-stream");
    let quiescent_destination = format!("/cases/{seed}/namespace/quiescent-stream-renamed");
    cases.push(BehaviorCase {
        id: format!("namespace-quiescent-stream-rename-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "quiescent-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("quiescent-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: quiescent_source.clone(),
                    chunks: vec![b"quiescent".to_vec()],
                    replace: false,
                })],
            },
            ProcessProgram {
                id: "quiescent-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("quiescent-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamRead {
                    path: quiescent_source.clone(),
                    expected: b"quiescent".to_vec(),
                })],
            },
            ProcessProgram {
                id: "quiescent-mutator".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("quiescent-mutator"),
                depends_on: vec!["quiescent-writer".to_owned(), "quiescent-reader".to_owned()],
                actions: vec![
                    Action::ok(ActionOp::Rename {
                        source: quiescent_source,
                        destination: quiescent_destination.clone(),
                        replace: false,
                    }),
                    Action::ok(ActionOp::Lookup {
                        path: quiescent_destination.clone(),
                        expected_kind: "stream".to_owned(),
                    }),
                    Action::ok(ActionOp::Unlink {
                        path: quiescent_destination,
                    }),
                ],
            },
        ],
        failure: FailureInjection::None,
    });

    let blob_collision = format!("/cases/{seed}/namespace/blob-stream-collision");
    cases.push(BehaviorCase {
        id: format!("namespace-blob-stream-collision-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "blob-owner".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("blob-owner"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::PublishBlob {
                    path: blob_collision.clone(),
                    bytes: b"blob".to_vec(),
                })],
            },
            ProcessProgram {
                id: "stream-collider".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("stream-collider"),
                depends_on: vec!["blob-owner".to_owned()],
                actions: vec![Action::exception(
                    ActionOp::StreamWrite {
                        path: blob_collision,
                        chunks: vec![b"stream".to_vec()],
                        replace: false,
                    },
                    PythonException::StreamError,
                )],
            },
        ],
        failure: FailureInjection::None,
    });

    let stream_collision = format!("/cases/{seed}/namespace/stream-blob-collision");
    cases.push(BehaviorCase {
        id: format!("namespace-stream-blob-collision-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "stream-owner-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("stream-owner-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: stream_collision.clone(),
                    chunks: vec![b"stream".to_vec()],
                    replace: false,
                })],
            },
            ProcessProgram {
                id: "stream-owner-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("stream-owner-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamRead {
                    path: stream_collision.clone(),
                    expected: b"stream".to_vec(),
                })],
            },
            ProcessProgram {
                id: "blob-collider".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("blob-collider"),
                depends_on: vec![
                    "stream-owner-writer".to_owned(),
                    "stream-owner-reader".to_owned(),
                ],
                actions: vec![Action::exception(
                    ActionOp::PublishBlob {
                        path: stream_collision,
                        bytes: b"blob".to_vec(),
                    },
                    PythonException::StreamError,
                )],
            },
        ],
        failure: FailureInjection::None,
    });

    let blob_over_stream = format!("/cases/{seed}/namespace/blob-over-stream");
    let blob_over_stream_source = format!("/cases/{seed}/namespace/blob-over-stream-source");
    cases.push(BehaviorCase {
        id: format!("namespace-blob-over-stream-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "replace-stream-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("replace-stream-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: blob_over_stream.clone(),
                    chunks: vec![b"old-stream".to_vec()],
                    replace: false,
                })],
            },
            ProcessProgram {
                id: "replace-stream-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("replace-stream-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamRead {
                    path: blob_over_stream.clone(),
                    expected: b"old-stream".to_vec(),
                })],
            },
            ProcessProgram {
                id: "blob-over-stream-mutator".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("blob-over-stream-mutator"),
                depends_on: vec![
                    "replace-stream-writer".to_owned(),
                    "replace-stream-reader".to_owned(),
                ],
                actions: vec![
                    Action::ok(ActionOp::PublishBlob {
                        path: blob_over_stream_source.clone(),
                        bytes: b"new-blob".to_vec(),
                    }),
                    Action::ok(ActionOp::Rename {
                        source: blob_over_stream_source,
                        destination: blob_over_stream.clone(),
                        replace: true,
                    }),
                    Action::ok(ActionOp::Lookup {
                        path: blob_over_stream.clone(),
                        expected_kind: "blob".to_owned(),
                    }),
                    Action::ok(ActionOp::ReadBlob {
                        path: blob_over_stream,
                        expected: b"new-blob".to_vec(),
                    }),
                ],
            },
        ],
        failure: FailureInjection::None,
    });

    let stream_over_blob = format!("/cases/{seed}/namespace/stream-over-blob-source");
    let stream_over_blob_destination =
        format!("/cases/{seed}/namespace/stream-over-blob-destination");
    cases.push(BehaviorCase {
        id: format!("namespace-stream-over-blob-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "replacement-stream-writer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("replacement-stream-writer"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: stream_over_blob.clone(),
                    chunks: vec![b"new-stream".to_vec()],
                    replace: false,
                })],
            },
            ProcessProgram {
                id: "replacement-stream-reader".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("replacement-stream-reader"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamRead {
                    path: stream_over_blob.clone(),
                    expected: b"new-stream".to_vec(),
                })],
            },
            ProcessProgram {
                id: "stream-over-blob-mutator".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("stream-over-blob-mutator"),
                depends_on: vec![
                    "replacement-stream-writer".to_owned(),
                    "replacement-stream-reader".to_owned(),
                ],
                actions: vec![
                    Action::ok(ActionOp::PublishBlob {
                        path: stream_over_blob_destination.clone(),
                        bytes: b"old-blob".to_vec(),
                    }),
                    Action::ok(ActionOp::Rename {
                        source: stream_over_blob,
                        destination: stream_over_blob_destination.clone(),
                        replace: true,
                    }),
                    Action::ok(ActionOp::Lookup {
                        path: stream_over_blob_destination.clone(),
                        expected_kind: "stream".to_owned(),
                    }),
                    Action::ok(ActionOp::Unlink {
                        path: stream_over_blob_destination,
                    }),
                ],
            },
        ],
        failure: FailureInjection::None,
    });

    cases
}

fn concurrent_namespace_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let publish_path = format!("/cases/{seed}/concurrent/publish");
    let publish_case = BehaviorCase {
        id: format!("concurrent-publish-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "publisher-a".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-publisher-a"),
                depends_on: Vec::new(),
                actions: vec![Action::linearized(
                    ActionOp::DescriptorWrite {
                        path: publish_path.clone(),
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(b"publisher-a".len() as u64),
                        bytes: b"publisher-a".to_vec(),
                        method: DescriptorWriteMethod::WriteFrom,
                        finish: DescriptorFinish::Close,
                    },
                    1,
                    1,
                    libc::EEXIST,
                )],
            },
            ProcessProgram {
                id: "publisher-b".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("concurrent-publisher-b"),
                depends_on: Vec::new(),
                actions: vec![Action::linearized(
                    ActionOp::DescriptorWrite {
                        path: publish_path.clone(),
                        flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                        length: Some(b"publisher-b".len() as u64),
                        bytes: b"publisher-b".to_vec(),
                        method: DescriptorWriteMethod::WriteFrom,
                        finish: DescriptorFinish::Close,
                    },
                    1,
                    1,
                    libc::EEXIST,
                )],
            },
            ProcessProgram {
                id: "publish-observer".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-publish-observer"),
                depends_on: vec!["publisher-a".to_owned(), "publisher-b".to_owned()],
                actions: vec![Action::ok(ActionOp::Lookup {
                    path: publish_path,
                    expected_kind: "blob".to_owned(),
                })],
            },
        ],
        failure: FailureInjection::None,
    };

    let rename_source = format!("/cases/{seed}/concurrent/rename-source");
    let rename_case = BehaviorCase {
        id: format!("concurrent-rename-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "rename-setup".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-rename-setup"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::PublishBlob {
                    path: rename_source.clone(),
                    bytes: b"rename".to_vec(),
                })],
            },
            ProcessProgram {
                id: "renamer-a".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-renamer-a"),
                depends_on: vec!["rename-setup".to_owned()],
                actions: vec![Action::linearized(
                    ActionOp::Rename {
                        source: rename_source.clone(),
                        destination: format!("/cases/{seed}/concurrent/rename-a"),
                        replace: false,
                    },
                    2,
                    1,
                    libc::ENOENT,
                )],
            },
            ProcessProgram {
                id: "renamer-b".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("concurrent-renamer-b"),
                depends_on: vec!["rename-setup".to_owned()],
                actions: vec![Action::linearized(
                    ActionOp::Rename {
                        source: rename_source,
                        destination: format!("/cases/{seed}/concurrent/rename-b"),
                        replace: false,
                    },
                    2,
                    1,
                    libc::ENOENT,
                )],
            },
        ],
        failure: FailureInjection::None,
    };

    let unlink_path = format!("/cases/{seed}/concurrent/unlink");
    let unlink_case = BehaviorCase {
        id: format!("concurrent-unlink-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![
            ProcessProgram {
                id: "unlink-setup".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-unlink-setup"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::PublishBlob {
                    path: unlink_path.clone(),
                    bytes: b"unlink".to_vec(),
                })],
            },
            ProcessProgram {
                id: "unlinker-a".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("concurrent-unlinker-a"),
                depends_on: vec!["unlink-setup".to_owned()],
                actions: vec![Action::linearized(
                    ActionOp::Unlink {
                        path: unlink_path.clone(),
                    },
                    3,
                    1,
                    libc::ENOENT,
                )],
            },
            ProcessProgram {
                id: "unlinker-b".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("concurrent-unlinker-b"),
                depends_on: vec!["unlink-setup".to_owned()],
                actions: vec![Action::linearized(
                    ActionOp::Unlink { path: unlink_path },
                    3,
                    1,
                    libc::ENOENT,
                )],
            },
        ],
        failure: FailureInjection::None,
    };

    vec![publish_case, rename_case, unlink_case]
}

pub fn ordered_pair_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let mut cases = Vec::new();
    for (left_index, left) in ActionClass::ALL.into_iter().enumerate() {
        for (right_index, right) in ActionClass::ALL.into_iter().enumerate() {
            let id = format!("pair-{left_index}-{right_index}");
            let mut primary = ProcessProgram {
                id: "primary".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted(format!("{id}-primary")),
                depends_on: Vec::new(),
                actions: Vec::new(),
            };
            let mut companions = Vec::new();
            primary
                .actions
                .push(canonical_action(left, &id, 0, node_count, &mut companions));
            primary
                .actions
                .push(canonical_action(right, &id, 1, node_count, &mut companions));
            let mut processes = vec![primary];
            processes.extend(companions.into_iter().take(2));
            cases.push(BehaviorCase {
                id,
                seed: seed ^ ((left_index as u64) << 32) ^ right_index as u64,
                schema_version: crate::ir::CASE_SCHEMA_VERSION,
                generator_version: crate::ir::GENERATOR_VERSION,
                live_nodes: crate::ir::contiguous_nodes(node_count),
                topology: Default::default(),
                scenarios: Default::default(),
                routes: Vec::new(),
                read_only_fixture_paths: Default::default(),
                resource_bounds: Default::default(),
                processes,
                failure: FailureInjection::None,
            });
        }
    }
    cases
}

fn canonical_action(
    class: ActionClass,
    case_id: &str,
    step: usize,
    node_count: u8,
    companions: &mut Vec<ProcessProgram>,
) -> Action {
    let path = format!("/cases/pairs/{case_id}/{step}");
    match class {
        ActionClass::PublishBlob => Action::ok(ActionOp::PublishBlob {
            path,
            bytes: vec![step as u8, 0xa5],
        }),
        ActionClass::ReadBlob => Action::ok(ActionOp::ReadBlob {
            path: SEEDED_MODEL_PATH.to_owned(),
            expected: SEEDED_MODEL_BYTES.to_vec(),
        }),
        ActionClass::Lookup => Action::ok(ActionOp::Lookup {
            path: SEEDED_MODEL_PATH.to_owned(),
            expected_kind: "blob".to_owned(),
        }),
        ActionClass::Rename => Action::error(
            ActionOp::Rename {
                source: path,
                destination: format!("/cases/pairs/{case_id}/{step}-renamed"),
                replace: false,
            },
            libc::ENOENT,
        ),
        ActionClass::Unlink => Action::error(ActionOp::Unlink { path }, libc::ENOENT),
        ActionClass::Descriptor => Action::ok(ActionOp::DescriptorRead {
            path: SEEDED_MODEL_PATH.to_owned(),
            flags: 0,
            expected: SEEDED_MODEL_BYTES.to_vec(),
            method: DescriptorReadMethod::Read,
            offset: 0,
            finish: DescriptorFinish::Close,
        }),
        ActionClass::StreamWrite => {
            let expected = vec![step as u8, 0x5a, 0xff];
            companions.push(ProcessProgram {
                id: format!("companion-reader-{step}"),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted(format!("{case_id}-reader-{step}")),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamRead {
                    path: path.clone(),
                    expected: expected.clone(),
                })],
            });
            Action::ok(ActionOp::StreamWrite {
                path,
                chunks: vec![expected],
                replace: false,
            })
        }
        ActionClass::StreamRead => {
            let expected = vec![step as u8, 0xc3, 0x3c];
            companions.push(ProcessProgram {
                id: format!("companion-writer-{step}"),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted(format!("{case_id}-writer-{step}")),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::StreamWrite {
                    path: path.clone(),
                    chunks: vec![expected.clone()],
                    replace: false,
                })],
            });
            Action::ok(ActionOp::StreamRead { path, expected })
        }
    }
}

fn boundary_payload(seed: u64, length: usize) -> Vec<u8> {
    let offset = (seed % 251) as usize;
    (0..length)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(offset)) % 251) as u8)
        .collect()
}

pub fn failure_corpus(node_count: u8, seed: u64) -> Vec<BehaviorCase> {
    let stopped_case = |phase: ProcessStopPhase, label: &str| BehaviorCase {
        id: format!("failure-process-stop-{label}-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "stopped-process".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(format!("failure-process-stop-{label}")),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::StreamRead {
                path: format!("/cases/failure/{seed}/process-stop-{label}"),
                expected: Vec::new(),
            })],
        }],
        failure: FailureInjection::StopProcess {
            process: "stopped-process".to_owned(),
            phase,
            kill_after_ms: Some(100),
        },
    };
    vec![
        stopped_case(ProcessStopPhase::AfterSpawn, "after-spawn"),
        stopped_case(ProcessStopPhase::DuringBootstrap, "during-bootstrap"),
        stopped_case(ProcessStopPhase::AfterContextReady, "after-context-ready"),
        {
            let launch_case = |kind: LaunchFailureKind, label: &str| BehaviorCase {
                schema_version: crate::ir::CASE_SCHEMA_VERSION,
                generator_version: crate::ir::GENERATOR_VERSION,
                id: format!("failure-launch-{label}-{seed}"),
                seed,
                live_nodes: crate::ir::contiguous_nodes(node_count),
                topology: Default::default(),
                scenarios: Default::default(),
                routes: Vec::new(),
                read_only_fixture_paths: Default::default(),
                resource_bounds: Default::default(),
                processes: vec![ProcessProgram {
                    id: "failed-launch".to_owned(),
                    logical_node_id: 1,
                    access: AccessSpec::unrestricted(format!("failure-launch-{label}")),
                    depends_on: Vec::new(),
                    actions: vec![Action::ok(ActionOp::Lookup {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        expected_kind: "blob".to_owned(),
                    })],
                }],
                failure: FailureInjection::LaunchFailure {
                    process: "failed-launch".to_owned(),
                    kind,
                },
            };
            launch_case(LaunchFailureKind::EmptyCommand, "empty-command")
        },
        {
            let mut case = failure_corpus_launch_case(
                node_count,
                seed,
                LaunchFailureKind::MissingExecutable,
                "missing-executable",
            );
            case.seed = seed.rotate_left(1);
            case
        },
        failure_corpus_launch_case(
            node_count,
            seed,
            LaunchFailureKind::MalformedExecutionIdentity,
            "malformed-execution-identity",
        ),
        failure_corpus_launch_case(
            node_count,
            seed,
            LaunchFailureKind::PythonSyntax,
            "python-syntax",
        ),
        failure_corpus_launch_case(
            node_count,
            seed,
            LaunchFailureKind::PythonRuntime,
            "python-runtime",
        ),
    ]
}

fn failure_corpus_launch_case(
    node_count: u8,
    seed: u64,
    kind: LaunchFailureKind,
    label: &str,
) -> BehaviorCase {
    BehaviorCase {
        id: format!("failure-launch-{label}-{seed}"),
        seed,
        schema_version: crate::ir::CASE_SCHEMA_VERSION,
        generator_version: crate::ir::GENERATOR_VERSION,
        live_nodes: crate::ir::contiguous_nodes(node_count),
        topology: Default::default(),
        scenarios: Default::default(),
        routes: Vec::new(),
        read_only_fixture_paths: Default::default(),
        resource_bounds: Default::default(),
        processes: vec![ProcessProgram {
            id: "failed-launch".to_owned(),
            logical_node_id: 1,
            access: AccessSpec::unrestricted(format!("failure-launch-{label}")),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::Lookup {
                path: SEEDED_MODEL_PATH.to_owned(),
                expected_kind: "blob".to_owned(),
            })],
        }],
        failure: FailureInjection::LaunchFailure {
            process: "failed-launch".to_owned(),
            kind,
        },
    }
}

pub fn generated_campaign_cases(
    seed: u64,
    count: usize,
    node_count: u8,
) -> Result<Vec<BehaviorCase>, String> {
    generated_campaign_cases_inner(seed, count, node_count, None)
}

pub(crate) fn generated_campaign_cases_budgeted(
    seed: u64,
    count: usize,
    node_count: u8,
    budget: &Budget,
) -> Result<Vec<BehaviorCase>, String> {
    generated_campaign_cases_inner(seed, count, node_count, Some(budget))
}

fn generated_campaign_cases_inner(
    seed: u64,
    count: usize,
    node_count: u8,
    budget: Option<&Budget>,
) -> Result<Vec<BehaviorCase>, String> {
    if let Some(budget) = budget {
        budget.check("generate deterministic campaign base cases")?;
    }
    if !(3..=5).contains(&node_count) {
        return Err("campaign generation requires three to five nodes".to_owned());
    }
    let mut cases = if node_count == 5 && count == 128 {
        let mut stable = stable_corpus(node_count, seed);
        if stable.len() < count {
            return Err(format!(
                "stable coverage corpus has {} cases, expected at least {count}",
                stable.len()
            ));
        }
        stable.drain(0..stable.len() - count);
        stable
    } else {
        match budget {
            Some(budget) => random_short_dags_budgeted(seed, count, node_count, budget)?,
            None => random_short_dags(seed, count, node_count),
        }
    };
    for (index, case) in cases.iter_mut().enumerate() {
        if let Some(budget) = budget {
            budget.check("decorate complete deterministic campaign case")?;
        }
        decorate_campaign_case(case, index, count, node_count, seed)?;
    }
    if let Some(budget) = budget {
        budget.check("complete deterministic campaign generation")?;
    }
    Ok(cases)
}

fn decorate_campaign_case(
    case: &mut BehaviorCase,
    index: usize,
    count: usize,
    node_count: u8,
    seed: u64,
) -> Result<(), String> {
    let nodes = (1..=u64::from(node_count)).collect::<Vec<_>>();
    let family = campaign_family(index, count, node_count);
    case.topology = family;
    if family == TopologyFamily::RingWalk {
        case.scenarios.insert(CoverageScenario::RingCompletion);
    }
    let churn = index == 0;
    if case.id.contains("authorization") {
        case.scenarios.insert(CoverageScenario::Authorization);
    }
    // Each replacement variant proves exactly its own mutation scenario:
    // the quiescent variant replaces a stream after an observed quiescence,
    // the active variant displaces one mid-flight. Requiring the other
    // scenario would make the case unprovable by construction.
    if case.id.starts_with("active-stream-replacement-") {
        case.scenarios.insert(CoverageScenario::ActiveMutation);
    } else if case.id.starts_with("stream-replacement-") {
        case.scenarios.insert(CoverageScenario::QuiescentMutation);
    }
    let topology_occurrence = if node_count < 5 || count < 128 {
        index / 6
    } else {
        let first = match family {
            TopologyFamily::Chain => 0,
            TopologyFamily::RingWalk => 26,
            TopologyFamily::FanOut => 45,
            TopologyFamily::FanIn => 64,
            TopologyFamily::Diamond => 83,
            TopologyFamily::RandomDag => 96,
            TopologyFamily::Fixed => unreachable!("fixed is not a campaign topology"),
        };
        index - first
    };
    let supplemental_survivor_kind = count == 32 && node_count == 4 && matches!(index, 3 | 9);
    let kinds: &[DataKind] = if supplemental_survivor_kind {
        &[DataKind::Blob, DataKind::Stream]
    } else if topology_occurrence % 2 == 0 {
        &[DataKind::Blob]
    } else {
        // Blob and stream topology are sampled independently across the fixed
        // campaign. Their planned ledgers still require every survivor edge
        // and node role for both kinds before provider access.
        &[DataKind::Stream]
    };
    if kinds.contains(&DataKind::Stream) {
        case.scenarios.insert(CoverageScenario::ConcurrentStartup);
    }
    if family == TopologyFamily::RandomDag {
        let action_limit = if count == 16 { 60 } else { 56 };
        append_random_dag_routes(case, &nodes, seed, index, action_limit, kinds)?;
    } else {
        let edges = topology_edges(family, &nodes, index);
        for &kind in kinds {
            let label = if kind == DataKind::Blob {
                "topology-blob"
            } else {
                "topology-stream"
            };
            append_executable_route(case, kind, label, &edges, family)?;
        }
    }

    // Topology rotation already covers every ordered node pair for both data
    // kinds across each phase. Do not duplicate those edges with two extra
    // fixed routes in every case; retain the required 16-action floor below.
    let hot_cold = family != TopologyFamily::RandomDag && index % 16 == 5;
    if churn {
        append_executable_route(
            case,
            DataKind::Stream,
            "pair-stream",
            &[(nodes[0], nodes[1])],
            TopologyFamily::Fixed,
        )?;
    }
    if churn {
        append_churn_target(case, nodes[index % nodes.len()])?;
    }
    if matches!(index, 8 | 9) {
        append_active_stream_fault(case, &nodes, index == 8);
    }
    if index < 4 {
        append_descriptor_terminal(case, index);
    }
    if index == 4 && family != TopologyFamily::RandomDag {
        case.scenarios.insert(CoverageScenario::FailureIsolation);
        case.processes.push(ProcessProgram {
            id: "expected-failed-branch".to_owned(),
            logical_node_id: nodes[0],
            access: AccessSpec::unrestricted(format!("{}-failed-branch", case.id)),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::Lookup {
                path: SEEDED_MODEL_PATH.to_owned(),
                expected_kind: "blob".to_owned(),
            })],
        });
        case.failure = FailureInjection::LaunchFailure {
            process: "expected-failed-branch".to_owned(),
            kind: LaunchFailureKind::MissingExecutable,
        };
    }
    if hot_cold {
        append_hot_cold_scenario(case, &nodes);
    }
    if index == 6 {
        append_slow_branch(case, &nodes);
    }

    if case
        .processes
        .iter()
        .flat_map(|process| &process.actions)
        .any(|action| action.operation.path().starts_with("/models/"))
    {
        case.read_only_fixture_paths
            .insert(SEEDED_MODEL_PATH.to_owned());
    }
    let mut padding_cursor = 0;
    while case
        .processes
        .iter()
        .map(|process| process.actions.len())
        .sum::<usize>()
        < 16
    {
        let process_count = case.processes.len();
        if process_count == 0 {
            return Err(format!("case {} has no process to pad", case.id));
        }
        let process = &mut case.processes[padding_cursor % process_count];
        padding_cursor += 1;
        process.actions.push(Action::ok(ActionOp::Lookup {
            path: SEEDED_MODEL_PATH.to_owned(),
            expected_kind: "blob".to_owned(),
        }));
        case.read_only_fixture_paths
            .insert(SEEDED_MODEL_PATH.to_owned());
    }
    let action_count = case
        .processes
        .iter()
        .map(|process| process.actions.len())
        .sum::<usize>();
    if !(2..=20).contains(&case.processes.len()) || !(16..=64).contains(&action_count) {
        return Err(format!(
            "decorated case {} has {} processes and {action_count} actions",
            case.id,
            case.processes.len()
        ));
    }
    case.validate()
}
fn append_descriptor_terminal(case: &mut BehaviorCase, index: usize) {
    let (finish, expected_error) = match index {
        0 => (DescriptorFinish::Abort, None),
        1 => (DescriptorFinish::Drop, None),
        2 => (DescriptorFinish::CloseTwice, Some(libc::EBADF)),
        _ => (DescriptorFinish::AbortTwice, Some(libc::EBADF)),
    };
    let operation = ActionOp::DescriptorRead {
        path: SEEDED_MODEL_PATH.to_owned(),
        flags: libc::O_RDONLY,
        expected: SEEDED_MODEL_BYTES.to_vec(),
        method: DescriptorReadMethod::Read,
        offset: 0,
        finish,
    };
    let action = match expected_error {
        Some(error) => Action::error(operation, error),
        None => Action::ok(operation),
    };
    case.processes[0].actions.push(action);
    case.read_only_fixture_paths
        .insert(SEEDED_MODEL_PATH.to_owned());
}

fn append_slow_branch(case: &mut BehaviorCase, nodes: &[u64]) {
    let parked_path = format!("/cases/{}/slow-branch/parked", case.id);
    let release_path = format!("/cases/{}/slow-branch/release", case.id);
    let completed_path = format!("/cases/{}/slow-branch/completed", case.id);
    let healthy = case
        .processes
        .iter()
        .map(|program| program.id.clone())
        .collect();
    for program in &mut case.processes {
        if program.depends_on.is_empty() {
            program.actions.insert(
                0,
                Action::ok(ActionOp::AwaitEntry {
                    path: parked_path.clone(),
                    expected_kind: "blob".to_owned(),
                }),
            );
        }
    }
    case.processes.push(ProcessProgram {
        id: "slow-branch".to_owned(),
        logical_node_id: nodes[0],
        access: AccessSpec::unrestricted(format!("{}-slow-branch", case.id)),
        depends_on: Vec::new(),
        actions: vec![
            Action::ok(ActionOp::PublishBlob {
                path: parked_path.clone(),
                bytes: Vec::new(),
            }),
            Action::ok(ActionOp::AwaitEntry {
                path: release_path.clone(),
                expected_kind: "blob".to_owned(),
            }),
            Action::ok(ActionOp::PublishBlob {
                path: completed_path,
                bytes: b"released".to_vec(),
            }),
        ],
    });
    case.processes.push(ProcessProgram {
        id: "slow-branch-release".to_owned(),
        logical_node_id: nodes[1],
        access: AccessSpec::unrestricted(format!("{}-slow-branch-release", case.id)),
        depends_on: healthy,
        actions: vec![
            Action::ok(ActionOp::AwaitEntry {
                path: parked_path.clone(),
                expected_kind: "blob".to_owned(),
            }),
            Action::ok(ActionOp::PublishBlob {
                path: release_path.clone(),
                bytes: Vec::new(),
            }),
        ],
    });
    case.failure = FailureInjection::SlowProcess {
        process: "slow-branch".to_owned(),
        parked_path,
        release_path,
    };
    case.scenarios.insert(CoverageScenario::FailureIsolation);
}

fn append_hot_cold_scenario(case: &mut BehaviorCase, nodes: &[u64]) {
    // Causal order required by the plan (10.3): the hot stream is written and
    // parked, its first frame is observed, both cold flows do observable work
    // and terminate, only then is the release published, and the hot flow
    // terminates on clean EOF. Ordering uses the first-frame observation
    // barrier, the release-path lookup barrier, and process-completion
    // dependencies for the release publisher, never a schedule.
    case.scenarios.insert(CoverageScenario::HotColdFairness);
    let stream_path = format!("/cases/{}/hot-cold/hot", case.id);
    let observed_path = format!("/cases/{}/hot-cold/observed", case.id);
    let release_path = format!("/cases/{}/hot-cold/release", case.id);
    let cold_blob_path = format!("/cases/{}/hot-cold/cold-blob", case.id);
    let cold_stream_path = format!("/cases/{}/hot-cold/cold-stream", case.id);
    // The first logical frame exceeds the 256 KiB endpoint ring, so reaching
    // the cold-work gate proves a multi-chunk hot transfer, not a tiny marker.
    // The writer retains its active endpoint and remaining payload until release.
    let frames = vec![
        boundary_payload(case.seed.rotate_left(7), 262_145),
        boundary_payload(case.seed.rotate_left(11), 65_537),
    ];
    let cold_blob_payload = boundary_payload(case.seed.rotate_left(3), 4_097);
    let cold_stream_payload = boundary_payload(case.seed.rotate_left(5), 8_193);
    let cold_stream_split = cold_stream_payload.len() / 2;
    let cold_stream_chunks = vec![
        cold_stream_payload[..cold_stream_split].to_vec(),
        cold_stream_payload[cold_stream_split..].to_vec(),
    ];
    case.routes.push(DataRoute {
        id: format!("{}-hot-cold", case.id),
        kind: DataKind::Stream,
        edges: vec![DataEdge {
            source: nodes[0],
            destination: nodes[1],
            source_role: "hot-writer".to_owned(),
            destination_role: "hot-reader".to_owned(),
            path: stream_path.clone(),
        }],
        join_inputs: Vec::new(),
    });
    let cold_blob_node = nodes[2];
    let cold_stream_node = nodes[nodes.len() - 1];
    case.processes.extend([
        ProcessProgram {
            id: "hot-writer".to_owned(),
            logical_node_id: nodes[0],
            access: AccessSpec::unrestricted(format!("{}-hot-writer", case.id)),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::GatedStreamWrite {
                path: stream_path.clone(),
                replace: false,
                frames: frames.clone(),
                release_path: release_path.clone(),
            })],
        },
        ProcessProgram {
            id: "hot-reader".to_owned(),
            logical_node_id: nodes[1],
            access: AccessSpec::unrestricted(format!("{}-hot-reader", case.id)),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::GatedStreamRead {
                path: stream_path,
                expected: frames.concat(),
                observed_path: observed_path.clone(),
                retry_attach: false,
                park_after_first_frame: false,
            })],
        },
        ProcessProgram {
            id: "cold-blob-flow".to_owned(),
            logical_node_id: cold_blob_node,
            access: AccessSpec::unrestricted(format!("{}-cold-blob-flow", case.id)),
            depends_on: Vec::new(),
            actions: vec![
                Action::ok(ActionOp::AwaitEntry {
                    path: observed_path.clone(),
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(ActionOp::PublishBlob {
                    path: cold_blob_path.clone(),
                    bytes: cold_blob_payload.clone(),
                }),
                Action::ok(ActionOp::ReadBlob {
                    path: cold_blob_path,
                    expected: cold_blob_payload,
                }),
            ],
        },
        ProcessProgram {
            id: "cold-stream-flow".to_owned(),
            logical_node_id: cold_stream_node,
            access: AccessSpec::unrestricted(format!("{}-cold-stream-flow", case.id)),
            depends_on: Vec::new(),
            actions: vec![
                Action::ok(ActionOp::AwaitEntry {
                    path: observed_path,
                    expected_kind: "blob".to_owned(),
                }),
                Action::ok(ActionOp::StreamRoundTrip {
                    path: cold_stream_path,
                    chunks: cold_stream_chunks,
                }),
            ],
        },
        ProcessProgram {
            id: "hot-release-publisher".to_owned(),
            logical_node_id: cold_blob_node,
            access: AccessSpec::unrestricted(format!("{}-hot-release-publisher", case.id)),
            depends_on: vec!["cold-blob-flow".to_owned(), "cold-stream-flow".to_owned()],
            actions: vec![Action::ok(ActionOp::PublishBlob {
                path: release_path,
                bytes: Vec::new(),
            })],
        },
    ]);
}

fn append_churn_target(case: &mut BehaviorCase, node: u64) -> Result<(), String> {
    // Keep an independent healthy stream live across the stop. Its first frame
    // causes the stop, while a completion-dependent publisher releases it only
    // after the target's terminal event. No elapsed-time overlap is assumed.
    let edge = case
        .routes
        .iter()
        .find(|route| route.kind == DataKind::Stream && route.id.ends_with("-pair-stream"))
        .and_then(|route| route.edges.first())
        .cloned()
        .ok_or("churn requires its independent pair stream")?;
    let observed_path = format!("/cases/{}/churn/observed", case.id);
    let release_path = format!("/cases/{}/churn/release", case.id);
    for process in &mut case.processes {
        for action in &mut process.actions {
            if action.operation.path() != edge.path {
                continue;
            }
            action.operation = match &action.operation {
                ActionOp::StreamWrite { path, chunks, .. } => ActionOp::GatedStreamWrite {
                    path: path.clone(),
                    replace: false,
                    frames: chunks.clone(),
                    release_path: release_path.clone(),
                },
                ActionOp::StreamRead { path, expected }
                | ActionOp::StreamReadInto { path, expected, .. } => ActionOp::GatedStreamRead {
                    path: path.clone(),
                    expected: expected.clone(),
                    observed_path: observed_path.clone(),
                    retry_attach: false,
                    park_after_first_frame: false,
                },
                _ => return Err("churn pair has an unsupported stream endpoint".to_owned()),
            };
        }
    }
    case.scenarios.insert(CoverageScenario::ProcessChurn);
    case.processes.extend([
        ProcessProgram {
            id: "churn-target".to_owned(),
            logical_node_id: node,
            access: AccessSpec::unrestricted(format!("{}-churn-target", case.id)),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::StreamRead {
                path: format!("/cases/{}/churn/parked", case.id),
                expected: Vec::new(),
            })],
        },
        ProcessProgram {
            id: "churn-release-publisher".to_owned(),
            logical_node_id: node,
            access: AccessSpec::unrestricted(format!("{}-churn-release", case.id)),
            depends_on: vec!["churn-target".to_owned()],
            actions: vec![Action::ok(ActionOp::PublishBlob {
                path: release_path,
                bytes: Vec::new(),
            })],
        },
    ]);
    case.failure = FailureInjection::StopProcess {
        process: "churn-target".to_owned(),
        phase: ProcessStopPhase::AfterSiblingStreamFirstFrame,
        kill_after_ms: Some(100),
    };
    Ok(())
}

fn append_active_stream_fault(case: &mut BehaviorCase, nodes: &[u64], abort_writer: bool) {
    let label = if abort_writer {
        "active-stream-writer-abort"
    } else {
        "active-stream-reader-stop"
    };
    let path = format!("/cases/{}/{label}/stream", case.id);
    let observed_path = format!("/cases/{}/{label}/observed", case.id);
    let release_path = format!("/cases/{}/{label}/reader-stopped", case.id);
    let writer_id = format!("{label}-writer");
    let reader_id = format!("{label}-reader");
    let frames = if abort_writer {
        vec![b"writer-abort-first".to_vec(), vec![0xa5; 4_097]]
    } else {
        vec![b"reader-stop-first".to_vec(), vec![0x5a; 524_288]]
    };
    let expected = frames.concat();
    let write = if abort_writer {
        ActionOp::GatedStreamWrite {
            path: path.clone(),
            replace: false,
            frames,
            release_path: format!("/cases/{}/{label}/never-release", case.id),
        }
    } else {
        ActionOp::GatedStreamWrite {
            path: path.clone(),
            replace: false,
            frames,
            release_path: release_path.clone(),
        }
    };
    let read = ActionOp::GatedStreamRead {
        path: path.clone(),
        expected,
        observed_path,
        retry_attach: false,
        park_after_first_frame: !abort_writer,
    };
    let (writer_actions, reader_actions) = if abort_writer {
        (
            vec![Action::ok(write)],
            vec![
                Action::exception(read, PythonException::StreamError),
                Action::ok(ActionOp::WaitForQuiescent { path }),
            ],
        )
    } else {
        (
            vec![
                Action::exception(write, PythonException::StreamError),
                Action::ok(ActionOp::WaitForQuiescent { path }),
            ],
            vec![Action::ok(read)],
        )
    };
    case.processes.extend([
        ProcessProgram {
            id: writer_id.clone(),
            logical_node_id: nodes[0],
            access: AccessSpec::unrestricted(format!("{}-{writer_id}", case.id)),
            depends_on: Vec::new(),
            actions: writer_actions,
        },
        ProcessProgram {
            id: reader_id.clone(),
            logical_node_id: nodes[1],
            access: AccessSpec::unrestricted(format!("{}-{reader_id}", case.id)),
            depends_on: Vec::new(),
            actions: reader_actions,
        },
    ]);
    if !abort_writer {
        case.processes.push(ProcessProgram {
            id: format!("{label}-release"),
            logical_node_id: nodes[1],
            access: AccessSpec::unrestricted(format!("{}-{label}-release", case.id)),
            depends_on: vec![reader_id.clone()],
            actions: vec![Action::ok(ActionOp::PublishBlob {
                path: release_path,
                bytes: Vec::new(),
            })],
        });
    }
    case.scenarios.insert(if abort_writer {
        CoverageScenario::ActiveStreamWriterAbort
    } else {
        CoverageScenario::ActiveStreamReaderStop
    });
    case.failure = FailureInjection::StopProcess {
        process: if abort_writer { writer_id } else { reader_id },
        phase: ProcessStopPhase::AfterStreamFirstFrame,
        kill_after_ms: Some(100),
    };
}

fn campaign_family(index: usize, count: usize, node_count: u8) -> TopologyFamily {
    if node_count < 5 || count < 128 {
        // Survivor and recovery campaigns rotate every realizable family.
        // A diamond needs four distinct corners, not a relabeled chain.
        return match index % 6 {
            0 => TopologyFamily::Chain,
            1 => TopologyFamily::RingWalk,
            2 => TopologyFamily::FanOut,
            3 => TopologyFamily::FanIn,
            4 if node_count >= 4 => TopologyFamily::Diamond,
            _ => TopologyFamily::RandomDag,
        };
    }
    match index {
        0..=25 => TopologyFamily::Chain,
        26..=44 => TopologyFamily::RingWalk,
        45..=63 => TopologyFamily::FanOut,
        64..=82 => TopologyFamily::FanIn,
        83..=95 => TopologyFamily::Diamond,
        _ => TopologyFamily::RandomDag,
    }
}

/// The graph vertices are process roles, not physical nodes. Preserve the
/// primitive-pair fixtures in independent vertex programs: their prefix work
/// uses only fixture/base-case paths and finishes before any route preparation.
/// Dependent base programs stay outside the graph and consume reserved slots.
fn append_random_dag_routes(
    case: &mut BehaviorCase,
    nodes: &[u64],
    seed: u64,
    index: usize,
    action_limit: usize,
    kinds: &[DataKind],
) -> Result<(), String> {
    let mut roles = case
        .processes
        .iter()
        .enumerate()
        .filter_map(|(index, process)| process.depends_on.is_empty().then_some(index))
        .collect::<Vec<_>>();
    let reserved = case.processes.len() - roles.len();
    // The 128-case campaign covers every 3..=20 vertex boundary once. Its
    // remaining random-DAG cases stay at the cheapest nontrivial size; repeating
    // intermediate vertex counts adds load without adding a boundary.
    // Short recovery and survivor campaigns still vary their DAGs.
    let requested = if (96..114).contains(&index) {
        3 + (index - 96)
    } else if index >= 114 {
        3
    } else {
        3 + ((seed % 10) as usize + index % 10) % 10
    };
    let vertices = requested.min(20 - reserved).max(roles.len());
    let selected_kind = *kinds.last().expect("campaign topology kind");
    let large_kind = if vertices > 17 {
        DataKind::Stream
    } else {
        selected_kind
    };
    let small_kind = if large_kind == DataKind::Blob {
        DataKind::Stream
    } else {
        DataKind::Blob
    };
    let action_cost = |kind| if kind == DataKind::Blob { 3 } else { 2 };
    let base_actions = case
        .processes
        .iter()
        .map(|process| process.actions.len())
        .sum::<usize>();
    let max_edges =
        (action_limit - base_actions - 2 * action_cost(small_kind)) / action_cost(large_kind);
    if !(3..=20).contains(&vertices) || vertices - 1 > max_edges {
        return Err(format!("case {} cannot fit its random DAG", case.id));
    }
    let mut random = XorShift64(
        seed.rotate_left(23) ^ case.seed ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
    );
    let mut edges = Vec::with_capacity(max_edges.min(vertices + 3));
    let mut incoming = vec![0_usize; vertices];
    let mut outgoing = vec![0_usize; vertices];
    for vertex in 0..vertices {
        if vertex >= roles.len() {
            let available = (0..vertex)
                .filter(|&source| outgoing[source] < 4)
                .collect::<Vec<_>>();
            let parent = available[(random.next_u64() as usize) % available.len()];
            let parent_node = case.processes[roles[parent]].logical_node_id;
            let choices = nodes
                .iter()
                .copied()
                .filter(|&node| node != parent_node)
                .collect::<Vec<_>>();
            let node = choices[(random.next_u64() as usize) % choices.len()];
            let id = format!("topology-role-{vertex}");
            roles.push(case.processes.len());
            case.processes.push(ProcessProgram {
                id: id.clone(),
                logical_node_id: node,
                access: AccessSpec::unrestricted(format!("{}-{id}", case.id)),
                depends_on: Vec::new(),
                actions: Vec::new(),
            });
        }
        if vertex == 0 {
            continue;
        }
        let node = case.processes[roles[vertex]].logical_node_id;
        let parents = (0..vertex)
            .filter(|&source| {
                outgoing[source] < 4 && case.processes[roles[source]].logical_node_id != node
            })
            .collect::<Vec<_>>();
        if parents.is_empty() {
            return Err(format!("case {} has no live-node DAG parent", case.id));
        }
        let source = parents[(random.next_u64() as usize) % parents.len()];
        edges.push((source, vertex));
        outgoing[source] += 1;
        incoming[vertex] += 1;
    }
    let small_edges = edges[..2].to_vec();
    // Every role belongs to the random spanning tree; extra independently
    // selected forward edges introduce joins without changing acyclicity.
    let mut candidates = (0..vertices)
        .flat_map(|source| (source + 1..vertices).map(move |destination| (source, destination)))
        .filter(|&(source, destination)| {
            case.processes[roles[source]].logical_node_id
                != case.processes[roles[destination]].logical_node_id
                && !edges.contains(&(source, destination))
        })
        .collect::<Vec<_>>();
    for end in (1..candidates.len()).rev() {
        let other = (random.next_u64() as usize) % (end + 1);
        candidates.swap(end, other);
    }
    let extra_limit = (max_edges - edges.len()).min(4).min(candidates.len());
    let target = edges.len() + (random.next_u64() as usize) % (extra_limit + 1);
    let mut copies = [0_usize; 20];
    for (source, destination) in candidates {
        if edges.len() == target {
            break;
        }
        if outgoing[source] == 4 || incoming[destination] == 4 {
            continue;
        }
        edges.push((source, destination));
        // Bound concatenation growth by independently counting source payload
        // contributions. Eight copies per edge keeps even large framed DAGs
        // well below the existing per-case allocation/payload ceiling.
        for vertex in 0..vertices {
            copies[vertex] = edges
                .iter()
                .filter(|&&(_, destination)| destination == vertex)
                .map(|&(source, _)| copies[source])
                .sum::<usize>()
                .max(1);
        }
        if copies.iter().any(|&copies| copies > 8) {
            edges.pop();
            continue;
        }
        outgoing[source] += 1;
        incoming[destination] += 1;
    }
    edges.sort_unstable();
    if kinds.len() > 1 {
        append_role_route(case, small_kind, &roles[..3], &small_edges);
    }
    append_role_route(case, large_kind, &roles, &edges);
    Ok(())
}

fn append_role_route(
    case: &mut BehaviorCase,
    kind: DataKind,
    roles: &[usize],
    edges: &[(usize, usize)],
) {
    let label = if kind == DataKind::Blob {
        "topology-blob"
    } else {
        "topology-stream"
    };
    let payload = boundary_payload(
        case.seed ^ label.bytes().map(u64::from).sum::<u64>(),
        if kind == DataKind::Blob { 257 } else { 4_097 },
    );
    let paths = edges
        .iter()
        .enumerate()
        .map(|(edge, (source, destination))| {
            format!(
                "/cases/{}/routes/{label}/{edge}-role-{source}-{destination}",
                case.id
            )
        })
        .collect::<Vec<_>>();
    let mut edge_payloads = vec![Vec::new(); edges.len()];
    for vertex in 0..roles.len() {
        if !edges.iter().any(|&(source, _)| source == vertex) {
            continue;
        }
        let inputs = edges
            .iter()
            .enumerate()
            .filter(|&(_, &(_, destination))| destination == vertex)
            .map(|(edge, _)| edge_payloads[edge].as_slice())
            .collect::<Vec<_>>();
        let relay = !inputs.is_empty();
        let joined = if relay {
            inputs.concat()
        } else {
            payload.clone()
        };
        for (edge, &(source, _)) in edges.iter().enumerate() {
            if source == vertex {
                let mut output = joined.clone();
                if relay && !output.is_empty() {
                    let rotation = (edge + 1) % output.len();
                    output.rotate_left(rotation);
                }
                edge_payloads[edge] = output;
            }
        }
    }
    let route_edges = edges
        .iter()
        .zip(&paths)
        .map(|(&(source, destination), path)| {
            let source = &case.processes[roles[source]];
            let destination = &case.processes[roles[destination]];
            DataEdge {
                source: source.logical_node_id,
                destination: destination.logical_node_id,
                source_role: source.id.clone(),
                destination_role: destination.id.clone(),
                path: path.clone(),
            }
        })
        .collect();
    let mut join_inputs = Vec::new();
    for (vertex, &process) in roles.iter().enumerate() {
        let incoming = edges
            .iter()
            .enumerate()
            .filter(|&(_, &(_, destination))| destination == vertex)
            .map(|(edge, _)| edge)
            .collect::<Vec<_>>();
        if incoming.len() > 1 {
            join_inputs.extend(incoming.iter().map(|&edge| paths[edge].clone()));
        }
        let actions = &mut case.processes[process].actions;
        for edge in incoming {
            append_read_actions(actions, kind, &paths[edge], &edge_payloads[edge], edge);
        }
        for (edge, &(source, _)) in edges.iter().enumerate() {
            if source == vertex {
                append_write_action(actions, kind, &paths[edge], &edge_payloads[edge], edge);
            }
        }
    }
    case.routes.push(DataRoute {
        id: format!("{}-{label}", case.id),
        kind,
        edges: route_edges,
        join_inputs,
    });
}

fn topology_edges(family: TopologyFamily, nodes: &[u64], index: usize) -> Vec<(u64, u64)> {
    let mut rotated = nodes.to_vec();
    let length = rotated.len();
    rotated.rotate_left(index % length);
    match family {
        TopologyFamily::Fixed | TopologyFamily::Chain => {
            rotated.windows(2).map(|pair| (pair[0], pair[1])).collect()
        }
        TopologyFamily::RingWalk => {
            // Rings traverse multiple laps with a fresh data path per lap, so
            // generation keeps a compact rotating ring: action bounds hold for
            // every live-set size while rotation still spreads relay duty
            // across all nodes over the campaign.
            let ring = &rotated[..length.min(3)];
            ring.iter()
                .copied()
                .zip(ring.iter().copied().cycle().skip(1))
                .take(ring.len())
                .collect()
        }
        TopologyFamily::FanOut => rotated[1..]
            .iter()
            .map(|destination| (rotated[0], *destination))
            .collect(),
        TopologyFamily::FanIn => rotated[..rotated.len() - 1]
            .iter()
            .map(|source| (*source, rotated[rotated.len() - 1]))
            .collect(),
        TopologyFamily::Diamond => vec![
            (rotated[0], rotated[1]),
            (rotated[0], rotated[2]),
            (rotated[1], rotated[3]),
            (rotated[2], rotated[3]),
        ],
        TopologyFamily::RandomDag => unreachable!("random DAGs use independent process vertices"),
    }
}

fn append_executable_route(
    case: &mut BehaviorCase,
    kind: DataKind,
    label: &str,
    edges: &[(u64, u64)],
    family: TopologyFamily,
) -> Result<(), String> {
    if edges.is_empty() {
        return Err(format!("case {} route {label} has no edges", case.id));
    }
    if family == TopologyFamily::RingWalk && edges.len() > 1 {
        append_ring_processes(case, kind, label, edges);
        return Ok(());
    }
    let payload_seed = case.seed ^ label.bytes().map(u64::from).sum::<u64>();
    let length = if family == TopologyFamily::Fixed {
        let mut random =
            XorShift64(payload_seed ^ edges[0].0.rotate_left(13) ^ edges[0].1.rotate_left(29));
        258 + (random.next_u64() % (4_095 - 258)) as usize
    } else if kind == DataKind::Blob {
        257
    } else {
        4_097
    };
    let payload = boundary_payload(payload_seed, length);
    let paths = edges
        .iter()
        .enumerate()
        .map(|(edge, (source, destination))| {
            format!(
                "/cases/{}/routes/{label}/{edge}-{source}-{destination}",
                case.id
            )
        })
        .collect::<Vec<_>>();
    let roles = append_dag_processes(case, kind, label, edges, &paths, &payload)?;
    let route_id = format!("{}-{label}", case.id);
    case.routes.push(DataRoute {
        id: route_id,
        kind,
        edges: edges
            .iter()
            .zip(&paths)
            .map(|((source, destination), path)| DataEdge {
                source: *source,
                destination: *destination,
                source_role: roles[source].clone(),
                destination_role: roles[destination].clone(),
                path: path.clone(),
            })
            .collect(),
        join_inputs: join_inputs(edges, &paths),
    });
    Ok(())
}

fn join_inputs(edges: &[(u64, u64)], paths: &[String]) -> Vec<String> {
    let mut incoming = BTreeMap::<u64, Vec<String>>::new();
    for ((_, destination), path) in edges.iter().zip(paths) {
        incoming.entry(*destination).or_default().push(path.clone());
    }
    incoming
        .into_values()
        .filter(|paths| paths.len() > 1)
        .flatten()
        .collect()
}

fn append_dag_processes(
    case: &mut BehaviorCase,
    kind: DataKind,
    label: &str,
    edges: &[(u64, u64)],
    paths: &[String],
    payload: &[u8],
) -> Result<BTreeMap<u64, String>, String> {
    let involved = edges
        .iter()
        .flat_map(|(source, destination)| [*source, *destination])
        .collect::<BTreeSet<_>>();
    // Derive expectations from the graph, separately from the generated
    // binding program's actual received-byte transformations.
    let mut edge_payloads = vec![None::<Vec<u8>>; edges.len()];
    let mut pending = involved.clone();
    while !pending.is_empty() {
        let node = pending
            .iter()
            .copied()
            .find(|node| {
                edges.iter().enumerate().all(|(edge, (_, destination))| {
                    destination != node || edge_payloads[edge].is_some()
                })
            })
            .ok_or_else(|| format!("case {} route {label} is cyclic", case.id))?;
        let incoming = edges
            .iter()
            .enumerate()
            .filter(|(_, (_, destination))| *destination == node)
            .map(|(edge, _)| edge_payloads[edge].as_deref().expect("ready incoming edge"))
            .collect::<Vec<_>>();
        let joined = if incoming.is_empty() {
            payload.to_vec()
        } else {
            incoming.concat()
        };
        let relay = !incoming.is_empty();
        for (edge, (source, _)) in edges.iter().enumerate() {
            if *source == node {
                let mut output = joined.clone();
                if relay && !output.is_empty() {
                    let rotation = (edge + 1) % output.len();
                    output.rotate_left(rotation);
                }
                edge_payloads[edge] = Some(output);
            }
        }
        pending.remove(&node);
    }
    let mut roles = BTreeMap::new();
    for node in involved {
        let mut actions = Vec::new();
        for (((_, destination), path), edge_index) in edges.iter().zip(paths).zip(0_usize..) {
            if *destination == node {
                append_read_actions(
                    &mut actions,
                    kind,
                    path,
                    edge_payloads[edge_index]
                        .as_deref()
                        .expect("derived route payload"),
                    edge_index,
                );
            }
        }
        for (((source, _), path), edge_index) in edges.iter().zip(paths).zip(0_usize..) {
            if *source == node {
                append_write_action(
                    &mut actions,
                    kind,
                    path,
                    edge_payloads[edge_index]
                        .as_deref()
                        .expect("derived route payload"),
                    edge_index,
                );
            }
        }
        let id = format!("{label}-node-{node}");
        if label == "topology-stream" {
            let blob_id = format!("topology-blob-node-{node}");
            if let Some(program) = case
                .processes
                .iter_mut()
                .find(|program| program.id == blob_id)
            {
                program.actions.extend(actions);
                roles.insert(node, blob_id);
                continue;
            }
        }
        case.processes.push(ProcessProgram {
            id: id.clone(),
            logical_node_id: node,
            access: AccessSpec::unrestricted(format!("{}-{label}-node-{node}", case.id)),
            depends_on: Vec::new(),
            actions,
        });
        roles.insert(node, id);
    }
    Ok(roles)
}

/// Lap count for generated rings; the plan requires at least two laps so a
/// ring proves repeated traversal rather than a single cycle.
const RING_LAPS: usize = 2;

/// Labels a ring action with its lap token identity, lap, edge, and barrier
/// kind using the canonical evidence-hint keys shared with codegen.
fn tag_ring_action(action: &mut Action, token: &str, lap: usize, edge_index: usize, barrier: &str) {
    action
        .evidence_hints
        .insert(EVIDENCE_HINT_TOKEN.to_owned(), token.to_owned());
    action
        .evidence_hints
        .insert(EVIDENCE_HINT_LAP.to_owned(), lap.to_string());
    action
        .evidence_hints
        .insert(EVIDENCE_HINT_EDGE_INDEX.to_owned(), edge_index.to_string());
    action
        .evidence_hints
        .insert(EVIDENCE_HINT_BARRIER.to_owned(), barrier.to_owned());
}

/// Appends a ring edge read and labels the consuming action with the lap's
/// token identity so observations prove the per-edge receive milestone.
fn append_ring_read(
    actions: &mut Vec<Action>,
    kind: DataKind,
    path: &str,
    token: &[u8],
    edge_index: usize,
    lap: usize,
    barrier: &str,
) {
    append_read_actions(actions, kind, path, token, edge_index);
    tag_ring_action(
        actions
            .last_mut()
            .expect("ring read appends a consuming action"),
        &crate::codegen::digest(token),
        lap,
        edge_index,
        barrier,
    );
}

/// Appends a ring edge write and labels the forwarding action with the lap's
/// token identity so observations prove the per-edge forward milestone.
fn append_ring_write(
    actions: &mut Vec<Action>,
    kind: DataKind,
    path: &str,
    token: &[u8],
    edge_index: usize,
    lap: usize,
    barrier: &str,
) {
    append_write_action(actions, kind, path, token, edge_index);
    tag_ring_action(
        actions
            .last_mut()
            .expect("ring write appends a forwarding action"),
        &crate::codegen::digest(token),
        lap,
        edge_index,
        barrier,
    );
}

fn append_ring_processes(
    case: &mut BehaviorCase,
    kind: DataKind,
    label: &str,
    edges: &[(u64, u64)],
) {
    let role_label = if label == "topology-stream"
        && case
            .processes
            .iter()
            .any(|process| process.id == "topology-blob-origin-relay")
    {
        "topology-blob"
    } else {
        label
    };
    // Ring execution (plan 10.2): every participant starts concurrently, each
    // lap carries an explicit token identity, every non-origin relay reads
    // before it forwards, the origin forwards its outgoing-edge token while a
    // separate reader on the same node concurrently awaits the final lap's
    // arrival, and the ring ends on clean EOF once the final token is
    // accounted for. Each lap uses its own data path per edge, so no data
    // edge is ever reused across laps.
    let laps = RING_LAPS.max(2);
    let hops = edges.len();
    let case_id = case.id.clone();
    let case_seed = case.seed;
    let hop_path = |lap: usize, hop: usize| -> String {
        let (source, destination) = edges[hop];
        format!("/cases/{case_id}/routes/{label}/lap-{lap}/hop-{hop}-{source}-{destination}")
    };
    let token = |_lap: usize| -> Vec<u8> {
        let length = if kind == DataKind::Blob { 257 } else { 4_097 };
        let body = boundary_payload(
            case_seed ^ label.bytes().map(u64::from).sum::<u64>(),
            length,
        );
        let header = format!("ring-token/{label}");
        [header.as_bytes(), body.as_slice()].concat()
    };
    case.routes.push(DataRoute {
        id: format!("{case_id}-{label}"),
        kind,
        edges: (0..laps)
            .flat_map(|lap| (0..hops).map(move |hop| (lap, hop)))
            .map(|(lap, hop)| {
                let (source, destination) = edges[hop];
                DataEdge {
                    source,
                    destination,
                    source_role: if hop == 0 {
                        format!("{role_label}-origin-relay")
                    } else {
                        format!("{role_label}-relay-{source}")
                    },
                    destination_role: if hop + 1 < hops {
                        format!("{role_label}-relay-{destination}")
                    } else if lap + 1 < laps {
                        format!("{role_label}-origin-relay")
                    } else {
                        format!("{role_label}-origin-reader")
                    },
                    path: hop_path(lap, hop),
                }
            })
            .collect(),
        join_inputs: Vec::new(),
    });
    let origin = edges[0].0;
    let arrival = hops - 1;
    let mut origin_actions = Vec::new();
    append_ring_write(
        &mut origin_actions,
        kind,
        &hop_path(0, 0),
        &token(0),
        0,
        0,
        BARRIER_TOKEN_FORWARDED,
    );
    for lap in 0..laps - 1 {
        append_ring_read(
            &mut origin_actions,
            kind,
            &hop_path(lap, arrival),
            &token(lap),
            lap * hops + arrival,
            lap,
            BARRIER_TOKEN_RECEIVED,
        );
        append_ring_write(
            &mut origin_actions,
            kind,
            &hop_path(lap + 1, 0),
            &token(lap + 1),
            (lap + 1) * hops,
            lap + 1,
            BARRIER_TOKEN_FORWARDED,
        );
    }
    append_ring_role(
        case,
        format!("{role_label}-origin-relay"),
        origin,
        origin_actions,
    );
    let mut reader_actions = Vec::new();
    append_ring_read(
        &mut reader_actions,
        kind,
        &hop_path(laps - 1, arrival),
        &token(laps - 1),
        (laps - 1) * hops + arrival,
        laps - 1,
        BARRIER_LAP_COMPLETED,
    );
    append_ring_role(
        case,
        format!("{role_label}-origin-reader"),
        origin,
        reader_actions,
    );
    for hop in 1..hops {
        let node = edges[hop].0;
        let mut actions = Vec::new();
        for lap in 0..laps {
            append_ring_read(
                &mut actions,
                kind,
                &hop_path(lap, hop - 1),
                &token(lap),
                lap * hops + hop - 1,
                lap,
                BARRIER_TOKEN_RECEIVED,
            );
            append_ring_write(
                &mut actions,
                kind,
                &hop_path(lap, hop),
                &token(lap),
                lap * hops + hop,
                lap,
                BARRIER_TOKEN_FORWARDED,
            );
        }
        append_ring_role(case, format!("{role_label}-relay-{node}"), node, actions);
    }
}

fn append_ring_role(
    case: &mut BehaviorCase,
    id: String,
    logical_node_id: u64,
    actions: Vec<Action>,
) {
    if let Some(process) = case.processes.iter_mut().find(|process| process.id == id) {
        process.actions.extend(actions);
        return;
    }
    case.processes.push(ProcessProgram {
        access: AccessSpec::unrestricted(format!("{}-{id}", case.id)),
        id,
        logical_node_id,
        depends_on: Vec::new(),
        actions,
    });
}

fn append_read_actions(
    actions: &mut Vec<Action>,
    kind: DataKind,
    path: &str,
    payload: &[u8],
    edge_index: usize,
) {
    match kind {
        DataKind::Blob => {
            actions.push(Action::ok(ActionOp::AwaitEntry {
                path: path.to_owned(),
                expected_kind: "blob".to_owned(),
            }));
            actions.push(Action::ok(ActionOp::ReadBlob {
                path: path.to_owned(),
                expected: payload.to_vec(),
            }));
        }
        DataKind::Stream => {
            let operation = if edge_index % 2 == 0 {
                ActionOp::StreamRead {
                    path: path.to_owned(),
                    expected: payload.to_vec(),
                }
            } else {
                ActionOp::StreamReadInto {
                    path: path.to_owned(),
                    expected: payload.to_vec(),
                    buffer_sizes: vec![1, 255, 4_096, 17],
                }
            };
            actions.push(Action::ok(operation));
        }
    }
}

fn append_write_action(
    actions: &mut Vec<Action>,
    kind: DataKind,
    path: &str,
    payload: &[u8],
    edge_index: usize,
) {
    match kind {
        DataKind::Blob => actions.push(Action::ok(ActionOp::PublishBlob {
            path: path.to_owned(),
            bytes: payload.to_vec(),
        })),
        DataKind::Stream => {
            let split = (edge_index % payload.len().max(1))
                .max(1)
                .min(payload.len());
            actions.push(Action::ok(ActionOp::StreamWrite {
                path: path.to_owned(),
                chunks: vec![payload[..split].to_vec(), payload[split..].to_vec()],
                replace: false,
            }));
        }
    }
}

pub fn random_short_dags(seed: u64, count: usize, node_count: u8) -> Vec<BehaviorCase> {
    random_short_dags_iter(seed, count, node_count).collect()
}

pub fn random_short_dags_budgeted(
    seed: u64,
    count: usize,
    node_count: u8,
    budget: &Budget,
) -> Result<Vec<BehaviorCase>, String> {
    let mut cases = Vec::new();
    let mut generated = random_short_dags_iter(seed, count, node_count);
    for _ in 0..count {
        budget.check("generate deterministic random DAG and boundary payload")?;
        cases.push(
            generated
                .next()
                .expect("generator emits one case per requested index"),
        );
    }
    Ok(cases)
}

fn random_short_dags_iter(
    seed: u64,
    count: usize,
    node_count: u8,
) -> impl Iterator<Item = BehaviorCase> {
    let mut random = XorShift64(seed);
    (0..count).map(move |index| {
        let source_node = 1 + random.next_u64() % u64::from(node_count);
        let mut destination_node = 1 + random.next_u64() % u64::from(node_count);
        if destination_node == source_node {
            destination_node = destination_node % u64::from(node_count) + 1;
        }
        let length = [0, 1, 4095, 4096, 4097, 65_537][(random.next_u64() as usize) % 6];
        let shape = random.next_u64() % 5;
        let id = format!("random-{seed}-{index}");
        let path = format!("/cases/random/{seed}/{index}");
        let bytes = boundary_payload(random.next_u64(), length);
        let processes = match shape {
            0 => vec![
                ProcessProgram {
                    id: "producer".to_owned(),
                    logical_node_id: source_node,
                    access: AccessSpec::unrestricted(format!("{id}-producer")),
                    depends_on: Vec::new(),
                    actions: vec![
                        Action::ok(ActionOp::PublishBlob {
                            path: path.clone(),
                            bytes: bytes.clone(),
                        }),
                        Action::ok(ActionOp::Lookup {
                            path: path.clone(),
                            expected_kind: "blob".to_owned(),
                        }),
                    ],
                },
                ProcessProgram {
                    id: "consumer".to_owned(),
                    logical_node_id: destination_node,
                    access: AccessSpec::unrestricted(format!("{id}-consumer")),
                    depends_on: vec!["producer".to_owned()],
                    actions: vec![Action::ok(ActionOp::ReadBlob {
                        path,
                        expected: bytes,
                    })],
                },
            ],
            1 => {
                let destination = format!("{path}-renamed");
                vec![
                    ProcessProgram {
                        id: "mutator".to_owned(),
                        logical_node_id: source_node,
                        access: AccessSpec::unrestricted(format!("{id}-mutator")),
                        depends_on: Vec::new(),
                        actions: vec![
                            Action::ok(ActionOp::PublishBlob {
                                path: path.clone(),
                                bytes: bytes.clone(),
                            }),
                            Action::ok(ActionOp::Rename {
                                source: path,
                                destination: destination.clone(),
                                replace: false,
                            }),
                            Action::ok(ActionOp::Lookup {
                                path: destination.clone(),
                                expected_kind: "blob".to_owned(),
                            }),
                        ],
                    },
                    ProcessProgram {
                        id: "consumer".to_owned(),
                        logical_node_id: destination_node,
                        access: AccessSpec::unrestricted(format!("{id}-consumer")),
                        depends_on: vec!["mutator".to_owned()],
                        actions: vec![Action::ok(ActionOp::ReadBlob {
                            path: destination,
                            expected: bytes,
                        })],
                    },
                ]
            }
            2 => {
                let payload = if bytes.is_empty() { vec![0] } else { bytes };
                let write_method = match random.next_u64() % 3 {
                    0 => DescriptorWriteMethod::Write,
                    1 => DescriptorWriteMethod::WriteFrom,
                    _ => DescriptorWriteMethod::Mapping,
                };
                let read_method = match random.next_u64() % 3 {
                    0 => DescriptorReadMethod::Read,
                    1 => DescriptorReadMethod::ReadInto,
                    _ => DescriptorReadMethod::Mapping,
                };
                vec![
                    ProcessProgram {
                        id: "descriptor-writer".to_owned(),
                        logical_node_id: source_node,
                        access: AccessSpec::unrestricted(format!("{id}-writer")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::DescriptorWrite {
                            path: path.clone(),
                            flags: libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC,
                            length: Some(payload.len() as u64),
                            bytes: payload.clone(),
                            method: write_method,
                            finish: DescriptorFinish::Close,
                        })],
                    },
                    ProcessProgram {
                        id: "descriptor-reader".to_owned(),
                        logical_node_id: destination_node,
                        access: AccessSpec::unrestricted(format!("{id}-reader")),
                        depends_on: vec!["descriptor-writer".to_owned()],
                        actions: vec![Action::ok(ActionOp::DescriptorRead {
                            path,
                            flags: libc::O_RDONLY,
                            expected: payload,
                            method: read_method,
                            offset: 0,
                            finish: DescriptorFinish::Close,
                        })],
                    },
                ]
            }
            3 => {
                let payload = if bytes.is_empty() { vec![0] } else { bytes };
                let split = payload.len() / 2;
                let chunks = vec![payload[..split].to_vec(), payload[split..].to_vec()];
                vec![
                    ProcessProgram {
                        id: "stream-writer".to_owned(),
                        logical_node_id: source_node,
                        access: AccessSpec::unrestricted(format!("{id}-writer")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::StreamWrite {
                            path: path.clone(),
                            chunks,
                            replace: false,
                        })],
                    },
                    ProcessProgram {
                        id: "stream-reader".to_owned(),
                        logical_node_id: destination_node,
                        access: AccessSpec::unrestricted(format!("{id}-reader")),
                        depends_on: Vec::new(),
                        actions: vec![Action::ok(ActionOp::StreamRead {
                            path,
                            expected: payload,
                        })],
                    },
                ]
            }
            _ => {
                let replacement = boundary_payload(random.next_u64(), length.saturating_add(1));
                vec![
                    ProcessProgram {
                        id: "replacer".to_owned(),
                        logical_node_id: source_node,
                        access: AccessSpec::unrestricted(format!("{id}-replacer")),
                        depends_on: Vec::new(),
                        actions: vec![
                            Action::ok(ActionOp::PublishBlob {
                                path: path.clone(),
                                bytes,
                            }),
                            Action::ok(ActionOp::DescriptorWrite {
                                path: path.clone(),
                                flags: libc::O_WRONLY | libc::O_TRUNC,
                                length: Some(replacement.len() as u64),
                                bytes: replacement.clone(),
                                method: DescriptorWriteMethod::WriteFrom,
                                finish: DescriptorFinish::Close,
                            }),
                        ],
                    },
                    ProcessProgram {
                        id: "replacement-reader".to_owned(),
                        logical_node_id: destination_node,
                        access: AccessSpec::unrestricted(format!("{id}-reader")),
                        depends_on: vec!["replacer".to_owned()],
                        actions: vec![Action::ok(ActionOp::ReadBlob {
                            path,
                            expected: replacement,
                        })],
                    },
                ]
            }
        };
        BehaviorCase {
            id,
            seed: random.next_u64(),
            schema_version: crate::ir::CASE_SCHEMA_VERSION,
            generator_version: crate::ir::GENERATOR_VERSION,
            live_nodes: crate::ir::contiguous_nodes(node_count),
            topology: Default::default(),
            scenarios: Default::default(),
            routes: Vec::new(),
            read_only_fixture_paths: Default::default(),
            resource_bounds: Default::default(),
            processes,
            failure: FailureInjection::None,
        }
    })
}

struct XorShift64(u64);

impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        if self.0 == 0 {
            self.0 = 0x9e37_79b9_7f4a_7c15;
        }
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_campaign_topologies_preserve_survivor_cases_and_all_pairs() {
        for (nodes, count) in [(3, 32), (4, 32), (5, 128)] {
            let cases = generated_campaign_cases(17, count, nodes).unwrap();
            assert_eq!(cases.len(), count);
            let mut observed_pairs = BTreeSet::new();
            for case in &cases {
                case.validate().unwrap();
                let resources = case.resource_summary().unwrap();
                assert!((16..=64).contains(&resources.action_count));
                assert!((2..=20).contains(&resources.process_count));
                if nodes == 3 {
                    assert_ne!(case.topology, TopologyFamily::Diamond);
                }
                for route in &case.routes {
                    observed_pairs.extend(
                        route
                            .edges
                            .iter()
                            .map(|edge| (edge.source, edge.destination, route.kind)),
                    );
                }
            }
            for case in &cases {
                for kind in [DataKind::Blob, DataKind::Stream] {
                    assert!(
                        cases.iter().any(|candidate| {
                            candidate.topology == case.topology
                                && candidate.routes.iter().any(|route| {
                                    route.kind == kind
                                        && crate::coverage::route_has_topology(
                                            route,
                                            candidate.topology,
                                        )
                                })
                        }),
                        "{nodes}-node campaign lacks {:?} {kind:?}",
                        case.topology
                    );
                }
            }
            for source in 1..=u64::from(nodes) {
                for destination in 1..=u64::from(nodes) {
                    if source != destination {
                        for kind in [DataKind::Blob, DataKind::Stream] {
                            assert!(
                                observed_pairs.contains(&(source, destination, kind)),
                                "{nodes}-node campaign omitted {source}->{destination} {kind:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn random_role_dags_cover_vertex_bounds_with_seed_varied_branching() {
        let first = generated_campaign_cases(17, 128, 5).unwrap();
        let repeated = generated_campaign_cases(17, 128, 5).unwrap();
        let different = generated_campaign_cases(35, 128, 5).unwrap();
        assert_eq!(first, repeated);
        let mut vertex_counts = BTreeSet::new();
        let mut changed_topologies = 0;
        for (left, right) in first.iter().zip(&different) {
            if left.topology != TopologyFamily::RandomDag {
                continue;
            }
            let signature = |case: &BehaviorCase| {
                case.routes
                    .iter()
                    .map(|route| {
                        (
                            route.kind,
                            route
                                .edges
                                .iter()
                                .map(|edge| {
                                    (edge.source_role.clone(), edge.destination_role.clone())
                                })
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            changed_topologies += usize::from(signature(left) != signature(right));
            for route in &left.routes {
                let mut degrees = BTreeMap::<&str, (usize, usize)>::new();
                for edge in &route.edges {
                    degrees.entry(&edge.source_role).or_default().1 += 1;
                    degrees.entry(&edge.destination_role).or_default().0 += 1;
                }
                vertex_counts.insert(degrees.len());
                assert!(
                    degrees
                        .values()
                        .all(|&(input, output)| input <= 4 && output <= 4)
                );
                assert!(crate::coverage::route_has_topology(
                    route,
                    TopologyFamily::RandomDag
                ));
            }
        }
        assert_eq!(vertex_counts, (3..=20).collect());
        // The seeds select identical vertex-count slots. Changes therefore
        // prove actual graph variation, not relabeling or a size-only switch.
        assert!(changed_topologies >= 8);
    }
}
