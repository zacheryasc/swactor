#![feature(rustc_private)]

extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_middle;
extern crate rustc_span;

use std::collections::HashSet;
use std::env;
use std::process::ExitCode;

use rustc_driver::{Callbacks, Compilation};
use rustc_hir as hir;
use rustc_hir::def::{DefKind, Res};
use rustc_hir::def_id::{DefId, LocalDefId};
use rustc_hir::intravisit::{self, Visitor};
use rustc_interface::interface::Compiler;
use rustc_middle::ty::{self, Ty, TyCtxt, TypeckResults};
use rustc_span::{Span, symbol::{Symbol, sym}};

/// Existing timer, process-I/O, and runtime-driving boundaries. These roles
/// never authorize task/thread creation or task-capability minting. Their
/// dependency direction remains an independent architectural restriction.
const EXECUTION_OWNERS: &[&str] = &[
    "dashboard",
    "iroh-driver",
    "swactor",
    "swactor-engine",
    "swactor-process",
    "swactor-process-context",
    "swactor-transport",
    "swactor-vastai",
    "telemetry",
];

/// Policy-bearing crates that must never enter an execution owner's dependency
/// closure.
const DOMAIN_CONTROL_CRATES: &[&str] = &["myelin", "provisioning", "xtask"];

#[derive(Clone, Copy)]
struct Capability {
    label: &'static str,
    resolution: &'static str,
    test_wait: bool,
    substrate: bool,
    paths: &'static [&'static str],
}

const USE_TASK_CAPABILITY: &str =
    "receive TaskSpawner explicitly from a binary entrypoint or test composition root; raw scheduling belongs only to the engine substrate";
const USE_ACTOR_TIMER: &str = "schedule a typed actor message through the engine; the receiving actor owns the deadline decision";

/// Stable resolved item paths. These are deliberately compiler identities, not
/// spellings found in source, so re-exports, renamed imports, and local wrappers
/// cannot evade the boundary.
const CAPABILITIES: &[Capability] = &[
    Capability {
        label: "asynchronous task spawning",
        resolution: USE_TASK_CAPABILITY,
        test_wait: false,
        substrate: true,
        paths: &[
            "tokio::runtime::Handle::spawn",
            "tokio::runtime::Runtime::spawn",
            "tokio::spawn",
            "tokio::task::spawn",
            "tokio::task::spawn_local",
            "tokio::task::JoinSet::spawn",
            "tokio::task::JoinSet::spawn_on",
            "tokio::task::JoinSet::spawn_local",
            "tokio::task::JoinSet::spawn_local_on",
            "tokio::task::LocalSet::spawn_local",
            "tokio::task::Builder::spawn",
            "tokio::task::Builder::spawn_on",
            "tokio::task::Builder::spawn_local",
            "tokio::task::Builder::spawn_local_on",
        ],
    },
    Capability {
        label: "blocking task spawning",
        resolution: USE_TASK_CAPABILITY,
        test_wait: false,
        substrate: true,
        paths: &[
            "tokio::runtime::Handle::spawn_blocking",
            "tokio::runtime::Runtime::spawn_blocking",
            "tokio::task::spawn_blocking",
            "tokio::task::JoinSet::spawn_blocking",
            "tokio::task::JoinSet::spawn_blocking_on",
        ],
    },
    // These helpers hide runtime spawning (including stream backpressure) or
    // expose/install its global authority. Context-only helpers and the local
    // TaskSpawner-based bridge do not mint execution capability.
    Capability {
        label: "implicit Python runtime task scheduling",
        resolution: USE_TASK_CAPABILITY,
        test_wait: false,
        substrate: true,
        paths: &[
            "pyo3_async_runtimes::tokio::future_into_py",
            "pyo3_async_runtimes::tokio::future_into_py_with_locals",
            "pyo3_async_runtimes::tokio::local_future_into_py",
            "pyo3_async_runtimes::tokio::local_future_into_py_with_locals",
            "pyo3_async_runtimes::tokio::get_runtime",
            "pyo3_async_runtimes::tokio::init",
            "pyo3_async_runtimes::tokio::init_with_runtime",
            "pyo3_async_runtimes::tokio::run",
            "pyo3_async_runtimes::tokio::run_until_complete",
            "pyo3_async_runtimes::tokio::into_stream_v1",
            "pyo3_async_runtimes::tokio::into_stream_with_locals_v1",
            "pyo3_async_runtimes::tokio::into_stream_v2",
            "pyo3_async_runtimes::tokio::into_stream_with_locals_v2",
            "pyo3_async_runtimes::async_std::future_into_py",
            "pyo3_async_runtimes::async_std::future_into_py_with_locals",
            "pyo3_async_runtimes::async_std::local_future_into_py",
            "pyo3_async_runtimes::async_std::local_future_into_py_with_locals",
            "pyo3_async_runtimes::async_std::run",
            "pyo3_async_runtimes::async_std::run_until_complete",
            "pyo3_async_runtimes::async_std::into_stream_v1",
            "pyo3_async_runtimes::async_std::into_stream_with_locals_v1",
            "pyo3_async_runtimes::async_std::into_stream_v2",
            "pyo3_async_runtimes::async_std::into_stream_with_locals_v2",
            "pyo3_async_runtimes::generic::future_into_py",
            "pyo3_async_runtimes::generic::future_into_py_with_locals",
            "pyo3_async_runtimes::generic::local_future_into_py",
            "pyo3_async_runtimes::generic::local_future_into_py_with_locals",
            "pyo3_async_runtimes::generic::run",
            "pyo3_async_runtimes::generic::run_until_complete",
            "pyo3_async_runtimes::generic::into_stream_v1",
            "pyo3_async_runtimes::generic::into_stream_with_locals_v1",
            "pyo3_async_runtimes::generic::into_stream_v2",
            "pyo3_async_runtimes::generic::into_stream_with_locals_v2",
        ],
    },
    Capability {
        label: "engine task scheduling",
        resolution: USE_TASK_CAPABILITY,
        test_wait: false,
        substrate: true,
        paths: &[
            "swactor_engine::ExecutionBackend::spawn",
            "swactor_engine::ExecutionBackend::spawn_blocking",
        ],
    },
    Capability {
        label: "OS thread creation",
        resolution: USE_TASK_CAPABILITY,
        test_wait: false,
        substrate: true,
        paths: &[
            "std::thread::Builder::spawn",
            "std::thread::Builder::spawn_unchecked",
            "std::thread::spawn",
            "std::thread::Scope::spawn",
            "std::thread::Builder::spawn_scoped",
        ],
    },
    Capability {
        label: "thread sleeping",
        resolution: USE_ACTOR_TIMER,
        test_wait: true,
        substrate: false,
        paths: &[
            "std::thread::park",
            "std::thread::park_timeout",
            "std::thread::sleep",
        ],
    },
    Capability {
        label: "direct timer driving",
        resolution: USE_ACTOR_TIMER,
        test_wait: false,
        substrate: false,
        paths: &[
            "swactor_engine::EngineHandle::interval",
            "swactor_engine::EngineHandle::timer",
            "swactor_engine::EngineHandle::timeout",
            "tokio::time::interval",
            "tokio::time::interval_at",
            "tokio::time::sleep",
            "tokio::time::sleep_until",
            "tokio::time::timeout",
            "tokio::time::timeout_at",
        ],
    },
    Capability {
        label: "runtime construction or driving",
        resolution: "the actor engine owns runtime construction and progression",
        test_wait: false,
        substrate: false,
        paths: &[
            "swactor::runtime::SingleThreadRuntime::tick",
            "swactor::runtime::SingleThreadRuntime::try_tick",
            "swactor::runtime::SingleThreadRuntime::has_work",
            "tokio::runtime::Builder::new_current_thread",
            "tokio::runtime::Builder::new_multi_thread",
            "tokio::runtime::Handle::block_on",
            "tokio::runtime::Runtime::block_on",
            "tokio::runtime::Runtime::new",
        ],
    },
    Capability {
        label: "blocking receive used as a controller",
        resolution: "receive observations in an actor; tests may use a bounded observation wait",
        test_wait: true,
        substrate: false,
        paths: &[
            "crossbeam_channel::channel::Receiver::recv",
            "crossbeam_channel::channel::Receiver::recv_deadline",
            "crossbeam_channel::channel::Receiver::recv_timeout",
            "std::sync::mpsc::Receiver::recv",
            "std::sync::mpsc::Receiver::recv_deadline",
            "std::sync::mpsc::Receiver::recv_timeout",
            "tokio::sync::mpsc::bounded::Receiver::blocking_recv",
            "tokio::sync::oneshot::Receiver::blocking_recv",
        ],
    },
    Capability {
        label: "process creation",
        resolution: "send a command to the process I/O owner and return exit/output observations to an actor",
        test_wait: false,
        substrate: false,
        paths: &[
            "std::process::Child::kill",
            "std::process::Child::try_wait",
            "std::process::Child::wait",
            "std::process::Child::wait_with_output",
            "std::process::Command::output",
            "std::process::Command::spawn",
            "std::process::Command::status",
            "tokio::process::Child::kill",
            "tokio::process::Child::start_kill",
            "tokio::process::Child::try_wait",
            "tokio::process::Child::wait",
            "tokio::process::Child::wait_with_output",
            "tokio::process::Command::output",
            "tokio::process::Command::spawn",
            "tokio::process::Command::status",
        ],
    },
];

include!("timing_policy.rs");
include!("execution_policy.rs");
include!("myelin_time_policy.rs");
struct ActorControlFlowCallbacks {
    package: String,
    test_build: bool,
    trace: bool,
}

impl Callbacks for ActorControlFlowCallbacks {
    fn after_analysis<'tcx>(&mut self, _compiler: &Compiler, tcx: TyCtxt<'tcx>) -> Compilation {
        if self.package == "myelin" {
            check_myelin_time_declarations(tcx);
        }
        let execution_policy = ExecutionPolicy::collect(tcx, self.test_build);
        if EXECUTION_OWNERS.contains(&self.package.as_str()) {
            check_owner_dependencies(tcx, &self.package);
        }

        for owner in tcx.hir_body_owners() {
            let typeck = tcx.typeck(owner);
            let body = tcx.hir_body_owned_by(owner);
            let mut visitor = CapabilityVisitor {
                tcx,
                typeck,
                package: &self.package,
                caller: tcx.def_path_str(owner.to_def_id()),
                owner,
                nonexecution_owner: EXECUTION_OWNERS.contains(&self.package.as_str()),
                test_root: execution_policy.is_test_root(tcx, owner),
                minting_root: execution_policy.is_composition_root(tcx, owner),
                actor_handler: execution_policy.is_actor_handler(tcx, owner),
                execution_policy: &execution_policy,
                reported_actor_capability: false,
                trace: self.trace,
                timing_counts: Vec::new(),
            };
            visitor.visit_body(body);
        }

        Compilation::Continue
    }
}

fn check_owner_dependencies(tcx: TyCtxt<'_>, package: &str) {
    for &crate_num in tcx.crates(()) {
        let dependency_symbol = tcx.crate_name(crate_num);
        let dependency = dependency_symbol.as_str();
        if DOMAIN_CONTROL_CRATES.contains(&dependency) {
            tcx.dcx().err(format!(
                "actor control-flow policy: execution owner `{package}` depends on domain-control crate `{dependency}`; execution owners must remain below domain policy in the dependency graph"
            ));
        }
    }
}

struct CapabilityVisitor<'a, 'tcx> {
    tcx: TyCtxt<'tcx>,
    typeck: &'tcx TypeckResults<'tcx>,
    package: &'a str,
    caller: String,
    owner: LocalDefId,
    nonexecution_owner: bool,
    test_root: bool,
    minting_root: bool,
    actor_handler: bool,
    execution_policy: &'a ExecutionPolicy,
    reported_actor_capability: bool,
    trace: bool,
    timing_counts: Vec<(String, usize)>,
}

impl<'tcx> Visitor<'tcx> for CapabilityVisitor<'_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx hir::Expr<'tcx>) {
        if let Some(ty) = self.typeck.expr_ty_opt(expr) {
            if self.package == "myelin" {
                check_myelin_time_type(self.tcx, ty, expr.span);
            }
            if self.actor_handler && !self.reported_actor_capability
                && contains_task_spawner(self.tcx, self.owner, ty)
            {
                self.reported_actor_capability = true;
                self.tcx.dcx().span_err(
                    expr.span,
                    "actor task-capability violation: TaskSpawner must not be introduced inside an actor handler",
                );
            }
            self.execution_policy.check_actor_instance(self.tcx, self.owner, ty, expr.span);
        }
        match &expr.kind {
            hir::ExprKind::MethodCall(..) => {
                if let Some(def_id) = self.typeck.type_dependent_def_id(expr.hir_id) {
                    self.check(def_id, expr.span);
                }
            }
            hir::ExprKind::Path(qpath) => {
                if let Res::Def(_, def_id) = self.typeck.qpath_res(qpath, expr.hir_id) {
                    if is_engine_method(self.tcx, def_id, "Engine", "task_spawner")
                        && !matches!(
                            self.tcx.parent_hir_node(expr.hir_id),
                            hir::Node::Expr(hir::Expr { kind: hir::ExprKind::Call(callee, _), .. })
                                if callee.hir_id == expr.hir_id
                        )
                    {
                        self.tcx.dcx().span_err(
                            expr.span,
                            "task-capability minting violation: Engine::task_spawner must be called directly at a composition root, not exported as a function value",
                        );
                        return;
                    }
                    self.check(def_id, expr.span);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}

impl CapabilityVisitor<'_, '_> {
    fn check(&mut self, def_id: rustc_hir::def_id::DefId, span: Span) {
        let path = self.tcx.def_path_str(def_id);
        let normalized_path = normalize_def_path(&path);
        let item_name = self.tcx.item_name(def_id);
        if self.package == "myelin" && myelin_timing_api(&normalized_path) {
            self.tcx.dcx().span_err(span, format!(
                "Myelin no-time policy: timing API `{normalized_path}` is forbidden; consume readiness/completion events or explicit cancellation instead"));
            return;
        }
        if self.trace && is_candidate_name(item_name) {
            eprintln!(
                "actor-control-flow trace: package=`{}` caller=`{}` callee=`{path}`",
                self.package, self.caller
            );
        }

        if is_engine_method(self.tcx, def_id, "Engine", "task_spawner")
            && !self.minting_root
        {
            self.tcx.dcx().span_err(
                span,
                "task-capability minting violation: Engine::task_spawner is restricted to a compiler-identified binary entrypoint or test composition root; pass TaskSpawner explicitly to helpers",
            );
            return;
        }
        if GUARDED_TIMING_ITEMS.contains(&item_name.as_str()) {
            let occurrence = self.next_timing_occurrence(&normalized_path);
            let allowance = TIMING_ALLOWANCES.iter().find(|allowance| {
                allowance.package == self.package
                    && allowance.caller == self.caller
                    && allowance.callee == normalized_path
            });
            match allowance {
                Some(allowance) if occurrence <= allowance.calls => return,
                Some(allowance) => {
                    self.tcx.dcx().span_err(
                        span,
                        format!(
                            "unapproved timing primitive: `{normalized_path}` occurrence {occurrence} exceeds the {} audited call(s) in `{}` (`{}`); remove the timer or update the central timing registry after architectural review",
                            allowance.calls, self.caller, allowance.purpose
                        ),
                    );
                }
                None => {
                    self.tcx.dcx().span_err(
                        span,
                        format!(
                            "unapproved timing primitive: `{normalized_path}` is forbidden in workspace package `{}` at `{}`; use typed engine scheduling or register an exact external-resource boundary",
                            self.package, self.caller
                        ),
                    );
                }
            }
            return;
        }

        let backend_scheduling = is_backend_scheduling(self.tcx, def_id);
        let python_runtime_scheduling = is_python_runtime_scheduling(self.tcx, def_id);
        let Some(capability) = CAPABILITIES
            .iter()
            .find(|capability| {
                capability.paths.contains(&normalized_path.as_str())
                    || (backend_scheduling && capability.label == "engine task scheduling")
                    || (python_runtime_scheduling
                        && capability.label == "implicit Python runtime task scheduling")
            })
        else {
            return;
        };

        if capability.substrate {
            if trusted_substrate_call(self.tcx, self.owner, def_id, &normalized_path) {
                return;
            }
        } else if self.nonexecution_owner
            || (self.test_root && (capability.test_wait || capability.label == "process creation"))
        {
            return;
        }

        self.tcx.dcx().span_err(
            span,
            format!(
                "actor control-flow violation: `{}` is forbidden in workspace crate `{}`; {}",
                capability.label, self.package, capability.resolution
            ),
        );
    }

    fn next_timing_occurrence(&mut self, callee: &str) -> usize {
        if let Some((_, count)) = self
            .timing_counts
            .iter_mut()
            .find(|(known, _)| known == callee)
        {
            *count += 1;
            return *count;
        }
        self.timing_counts.push((callee.to_owned(), 1));
        1
    }
}

fn normalize_def_path(path: &str) -> String {
    let mut normalized = String::with_capacity(path.len());
    let mut cursor = 0;
    while let Some(relative_start) = path[cursor..].find("::<") {
        let start = cursor + relative_start;
        normalized.push_str(&path[cursor..start]);
        let generic_start = start + 3;
        let mut depth = 1_usize;
        let mut end = path.len();
        for (offset, character) in path[generic_start..].char_indices() {
            match character {
                '<' => depth += 1,
                '>' => {
                    depth -= 1;
                    if depth == 0 {
                        end = generic_start + offset + character.len_utf8();
                        break;
                    }
                }
                _ => {}
            }
        }
        cursor = end;
    }
    normalized.push_str(&path[cursor..]);
    normalized
}

fn is_candidate_name(name: Symbol) -> bool {
    GUARDED_TIMING_ITEMS.contains(&name.as_str())
        || matches!(
            name.as_str(),
            "block_on"
                | "blocking_recv"
                | "future_into_py"
                | "future_into_py_with_locals"
                | "local_future_into_py"
                | "local_future_into_py_with_locals"
                | "get_runtime"
                | "init"
                | "init_with_runtime"
                | "run"
                | "run_until_complete"
                | "into_stream_v1"
                | "into_stream_with_locals_v1"
                | "into_stream_v2"
                | "into_stream_with_locals_v2"
                | "has_work"
                | "interval"
                | "interval_at"
                | "new_current_thread"
                | "new_multi_thread"
                | "recv"
                | "spawn"
                | "spawn_blocking"
                | "spawn_local"
                | "task_spawner"
                | "tick"
                | "timer"
                | "try_tick"
        )
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let package = env::var("MYELIN_ACTOR_LINT_PACKAGE").unwrap_or_else(|_| "unknown".to_owned());
    let test_build = env::var_os("MYELIN_ACTOR_LINT_TEST_BUILD").is_some();
    let trace = env::var_os("MYELIN_ACTOR_LINT_TRACE").is_some();
    let mut callbacks = ActorControlFlowCallbacks {
        package,
        test_build,
        trace,
    };
    rustc_driver::catch_with_exit_code(|| rustc_driver::run_compiler(&args, &mut callbacks))
}
