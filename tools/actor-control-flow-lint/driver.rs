#![feature(rustc_private)]

extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_middle;
extern crate rustc_span;

use std::env;
use std::process::ExitCode;

use rustc_driver::{Callbacks, Compilation};
use rustc_hir as hir;
use rustc_hir::def::Res;
use rustc_hir::intravisit::{self, Visitor};
use rustc_interface::interface::Compiler;
use rustc_middle::ty::{TyCtxt, TypeckResults};
use rustc_span::{Span, symbol::Symbol};

/// Crates whose purpose is to run the actor engine or turn external I/O,
/// process, telemetry, and provider API streams into actor observations.
/// Adding an entry changes the architecture.
const EXECUTION_OWNERS: &[&str] = &[
    "dashboard",
    "iroh-driver",
    "swactor",
    "swactor-engine",
    "swactor-process",
    "swactor-transport",
    "swactor-vastai",
    "telemetry",
];

/// This package owns only the compile-contract subprocess harness. It cannot be
/// used as a workspace dependency.
const TEST_SUPPORT_OWNERS: &[&str] = &["actor-control-flow-lint-tests"];

/// Policy-bearing crates that must never enter an execution owner's dependency
/// closure.
const DOMAIN_CONTROL_CRATES: &[&str] = &[
    "myelin",
    "provisioning",
    "swactor-job-runner",
    "xtask",
];

#[derive(Clone, Copy)]
struct Capability {
    label: &'static str,
    resolution: &'static str,
    test_wait: bool,
    paths: &'static [&'static str],
}

const MOVE_TO_OWNER: &str =
    "move stream mechanics into an approved execution owner or move the decision into an actor";
const USE_ACTOR_TIMER: &str =
    "schedule a typed actor message through the engine; the receiving actor owns the deadline decision";

/// Stable resolved item paths. These are deliberately compiler identities, not
/// spellings found in source, so re-exports, renamed imports, and local wrappers
/// cannot evade the boundary.
const CAPABILITIES: &[Capability] = &[
    Capability {
        label: "asynchronous task spawning",
        resolution: MOVE_TO_OWNER,
        test_wait: false,
        paths: &[
            "tokio::runtime::Handle::spawn",
            "tokio::runtime::Runtime::spawn",
            "tokio::spawn",
            "tokio::task::spawn",
            "tokio::task::spawn_local",
        ],
    },
    Capability {
        label: "blocking task spawning",
        resolution: MOVE_TO_OWNER,
        test_wait: false,
        paths: &[
            "tokio::runtime::Handle::spawn_blocking",
            "tokio::runtime::Runtime::spawn_blocking",
            "tokio::task::spawn_blocking",
        ],
    },
    Capability {
        label: "engine task scheduling",
        resolution: MOVE_TO_OWNER,
        test_wait: false,
        paths: &["swactor_engine::EngineHandle::spawn"],
    },
    Capability {
        label: "OS thread creation",
        resolution: MOVE_TO_OWNER,
        test_wait: false,
        paths: &[
            "std::thread::Builder::spawn",
            "std::thread::Builder::spawn_unchecked",
            "std::thread::spawn",
        ],
    },
    Capability {
        label: "thread sleeping",
        resolution: USE_ACTOR_TIMER,
        test_wait: true,
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

struct ActorControlFlowCallbacks {
    package: String,
    test_build: bool,
    trace: bool,
}

impl Callbacks for ActorControlFlowCallbacks {
    fn after_analysis<'tcx>(
        &mut self,
        _compiler: &Compiler,
        tcx: TyCtxt<'tcx>,
    ) -> Compilation {
        if TEST_SUPPORT_OWNERS.contains(&self.package.as_str()) {
            return Compilation::Continue;
        }
        if EXECUTION_OWNERS.contains(&self.package.as_str()) {
            check_owner_dependencies(tcx, &self.package);
            return Compilation::Continue;
        }

        for owner in tcx.hir_body_owners() {
            let typeck = tcx.typeck(owner);
            let body = tcx.hir_body_owned_by(owner);
            let mut visitor = CapabilityVisitor {
                tcx,
                typeck,
                package: &self.package,
                test_build: self.test_build,
                trace: self.trace,
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
    test_build: bool,
    trace: bool,
}

impl<'tcx> Visitor<'tcx> for CapabilityVisitor<'_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx hir::Expr<'tcx>) {
        match &expr.kind {
            hir::ExprKind::MethodCall(..) => {
                if let Some(def_id) = self.typeck.type_dependent_def_id(expr.hir_id) {
                    self.check(def_id, expr.span);
                }
            }
            hir::ExprKind::Path(qpath) => {
                if let Res::Def(_, def_id) = self.typeck.qpath_res(qpath, expr.hir_id) {
                    self.check(def_id, expr.span);
                }
            }
            _ => {}
        }
        intravisit::walk_expr(self, expr);
    }
}

impl CapabilityVisitor<'_, '_> {
    fn check(&self, def_id: rustc_hir::def_id::DefId, span: Span) {
        let path = self.tcx.def_path_str(def_id);
        let normalized_path = normalize_def_path(&path);
        if self.trace && is_candidate_name(self.tcx.item_name(def_id)) {
            eprintln!("actor-control-flow trace: {path}");
        }

        let Some(capability) = CAPABILITIES
            .iter()
            .find(|capability| capability.paths.contains(&normalized_path.as_str()))
        else {
            return;
        };


        if self.test_build && capability.test_wait {
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
    matches!(
        name.as_str(),
        "block_on"
            | "blocking_recv"
            | "has_work"
            | "interval"
            | "interval_at"
            | "new_current_thread"
            | "new_multi_thread"
            | "recv"
            | "recv_deadline"
            | "recv_timeout"
            | "sleep"
            | "sleep_until"
            | "spawn"
            | "spawn_blocking"
            | "spawn_local"
            | "tick"
            | "timeout"
            | "timeout_at"
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
