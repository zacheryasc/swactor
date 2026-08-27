//! Deterministic stable/failure corpus and random short-DAG generation.

use crate::ir::{
    AccessSpec, Action, ActionClass, ActionOp, BehaviorCase, DescriptorFinish,
    DescriptorReadMethod, DescriptorWriteMethod, FailureInjection, LaunchFailureKind,
    ProcessProgram, ProcessStopPhase, PythonException,
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
                node_count,
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
                node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
    cases.push(BehaviorCase {
        id: format!("authorization-{seed}"),
        seed,
        node_count,
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
                actions: vec![Action::error(
                    ActionOp::PublishBlob {
                        path: format!("/cases/{seed}/unauthorized-publication"),
                        bytes: vec![1],
                    },
                    libc::EACCES,
                )],
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
                actions: vec![Action::error(
                    ActionOp::ReadBlob {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        expected: SEEDED_MODEL_BYTES.to_vec(),
                    },
                    libc::EACCES,
                )],
            },
            ProcessProgram {
                id: "rename-guard".to_owned(),
                logical_node_id: 1,
                access: AccessSpec {
                    execution_id: "authorization-rename".to_owned(),
                    read_prefixes: Vec::new(),
                    write_prefixes: vec![format!("/cases/{seed}/allowed")],
                },
                depends_on: Vec::new(),
                actions: vec![
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
        node_count,
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
            node_count,
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
        node_count,
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
            id: id.clone(),
            seed: seed ^ index as u64,
            node_count,
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
            node_count,
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
                    actions: vec![Action::error(
                        ActionOp::Lookup {
                            path,
                            expected_kind: "blob".to_owned(),
                        },
                        libc::ENOENT,
                    )],
                },
            ],
            failure: FailureInjection::None,
        });
    }

    let repeated_path = format!("/cases/{seed}/descriptor/repeated-close");
    cases.push(BehaviorCase {
        id: format!("descriptor-repeated-terminal-{seed}"),
        seed,
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
        node_count,
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
                node_count,
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
        node_count,
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
                id: format!("failure-launch-{label}-{seed}"),
                seed,
                node_count,
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
        BehaviorCase {
            id: format!("failure-node-loss-{seed}"),
            seed: seed.rotate_left(9),
            node_count,
            processes: vec![
                ProcessProgram {
                    id: "lost-node-process".to_owned(),
                    logical_node_id: 1,
                    access: AccessSpec::unrestricted("failure-node-loss"),
                    depends_on: Vec::new(),
                    actions: vec![Action::ok(ActionOp::StreamWrite {
                        path: format!("/cases/failure/{seed}/node-loss"),
                        chunks: vec![vec![1, 2, 3]],
                        replace: false,
                    })],
                },
                ProcessProgram {
                    id: "surviving-node-process".to_owned(),
                    logical_node_id: u64::from(node_count),
                    access: AccessSpec::unrestricted("failure-node-survivor"),
                    depends_on: vec!["lost-node-process".to_owned()],
                    actions: vec![Action::ok(ActionOp::Lookup {
                        path: SEEDED_MODEL_PATH.to_owned(),
                        expected_kind: "blob".to_owned(),
                    })],
                },
            ],
            failure: FailureInjection::KillNode { logical_node_id: 1 },
        },
        BehaviorCase {
            id: format!("failure-orchestrator-stop-{seed}"),
            seed: seed.rotate_right(7),
            node_count,
            processes: vec![ProcessProgram {
                id: "orchestrator-stop-process".to_owned(),
                logical_node_id: u64::from(node_count),
                access: AccessSpec::unrestricted("failure-orchestrator-stop"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(ActionOp::Lookup {
                    path: SEEDED_MODEL_PATH.to_owned(),
                    expected_kind: "blob".to_owned(),
                })],
            }],
            failure: FailureInjection::StopOrchestrator,
        },
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
        node_count,
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

pub fn random_short_dags(seed: u64, count: usize, node_count: u8) -> Vec<BehaviorCase> {
    let mut random = XorShift64(seed);
    (0..count)
        .map(|index| {
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
                                flags: libc::O_WRONLY
                                    | libc::O_CREAT
                                    | libc::O_EXCL
                                    | libc::O_TRUNC,
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
                node_count,
                processes,
                failure: FailureInjection::None,
            }
        })
        .collect()
}

struct XorShift64(u64);

impl XorShift64 {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
