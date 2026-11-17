/// Resolved actor and composition-root identities, collected once per crate.
struct ExecutionPolicy {
    entrypoint: Option<DefId>,
    test_roots: HashSet<LocalDefId>,
    actor_handlers: HashSet<LocalDefId>,
    actor_types: HashSet<DefId>,
}

impl ExecutionPolicy {
    fn collect(tcx: TyCtxt<'_>, test_build: bool) -> Self {
        let mut policy = Self {
            entrypoint: tcx.entry_fn(()).map(|(def_id, _)| def_id),
            test_roots: HashSet::new(),
            actor_handlers: HashSet::new(),
            actor_types: HashSet::new(),
        };
        if test_build {
            // #[test] expands to a compiler-marked TestDescAndFn constant.
            // Follow its resolved function reference instead of matching names,
            // cfg(test), crate roles, or arbitrary user-written attributes.
            for owner in tcx.hir_body_owners() {
                let descriptor = lexical_body_root(tcx, owner);
                if tcx.def_kind(descriptor) != DefKind::Const
                    || !tcx.hir_attrs(tcx.local_def_id_to_hir_id(descriptor))
                        .iter().any(|attr| attr.has_name(sym::rustc_test_marker))
                {
                    continue;
                }
                let mut visitor = TestRootVisitor {
                    typeck: tcx.typeck(owner),
                    roots: &mut policy.test_roots,
                };
                visitor.visit_body(tcx.hir_body_owned_by(owner));
            }
        }

        for item_id in tcx.hir_free_items() {
            let item = tcx.hir_item(item_id);
            if !matches!(item.kind, hir::ItemKind::Impl(implementation) if implementation.of_trait.is_some()) {
                continue;
            }
            let impl_id = item.owner_id.def_id;
            let trait_ref = tcx.impl_trait_ref(impl_id);
            if !is_named_item(tcx, trait_ref.instantiate_identity().def_id,
                "swactor", "actor", "ActorInterface")
            {
                continue;
            }
            let actor_ty = tcx.type_of(impl_id).instantiate_identity();
            if let ty::Adt(adt, _) = actor_ty.kind() {
                policy.actor_types.insert(adt.did());
            }
            if contains_task_spawner(tcx, impl_id, actor_ty) {
                tcx.dcx().span_err(item.span,
                    "actor task-capability violation: a type implementing ActorInterface must not store TaskSpawner, including nested fields or aliases");
            }
            for associated in tcx.associated_items(impl_id).in_definition_order() {
                if associated.is_fn() {
                    if let Some(local) = associated.def_id.as_local() {
                        policy.actor_handlers.insert(local);
                    }
                } else if associated.is_type() && associated.name().as_str() == "Incoming" {
                    let incoming = tcx.type_of(associated.def_id).instantiate_identity();
                    if contains_task_spawner(tcx, impl_id, incoming) {
                        tcx.dcx().span_err(tcx.def_span(associated.def_id),
                            "actor task-capability violation: an actor incoming-message type must not carry TaskSpawner, including nested fields or aliases");
                    }
                }
            }
        }
        policy
    }

    fn is_test_root(&self, tcx: TyCtxt<'_>, owner: LocalDefId) -> bool {
        self.test_roots.contains(&lexical_body_root(tcx, owner))
    }

    fn is_composition_root(&self, tcx: TyCtxt<'_>, owner: LocalDefId) -> bool {
        let root = lexical_body_root(tcx, owner);
        self.entrypoint == Some(root.to_def_id()) || self.test_roots.contains(&root)
    }

    fn is_actor_handler(&self, tcx: TyCtxt<'_>, owner: LocalDefId) -> bool {
        self.actor_handlers.contains(&lexical_body_root(tcx, owner))
    }

    fn check_actor_instance<'tcx>(&self, tcx: TyCtxt<'tcx>, owner: LocalDefId, ty: Ty<'tcx>, span: Span) {
        // Generic actor definitions may not yet name their concrete fields.
        // Check instantiated actors too, before a constructor erases their type.
        if let ty::Adt(adt, _) = ty.kind()
            && self.actor_types.contains(&adt.did())
            && contains_task_spawner(tcx, owner, ty)
        {
            tcx.dcx().span_err(span,
                "actor task-capability violation: an instantiated ActorInterface type must not store TaskSpawner");
        }
    }
}

struct TestRootVisitor<'a, 'tcx> {
    typeck: &'tcx TypeckResults<'tcx>,
    roots: &'a mut HashSet<LocalDefId>,
}

impl<'tcx> Visitor<'tcx> for TestRootVisitor<'_, 'tcx> {
    fn visit_expr(&mut self, expr: &'tcx hir::Expr<'tcx>) {
        if let hir::ExprKind::Path(path) = &expr.kind
            && let Res::Def(DefKind::Fn, def_id) = self.typeck.qpath_res(path, expr.hir_id)
            && let Some(local) = def_id.as_local()
        {
            self.roots.insert(local);
        }
        intravisit::walk_expr(self, expr);
    }
}

/// Closures and async bodies belong to their lexical root; a nested helper
/// function is a new boundary and never inherits composition-root permission.
fn lexical_body_root(tcx: TyCtxt<'_>, mut owner: LocalDefId) -> LocalDefId {
    while matches!(tcx.def_kind(owner), DefKind::Closure) {
        let Some(parent) = tcx.opt_parent(owner.to_def_id()).and_then(DefId::as_local) else { break };
        owner = parent;
    }
    owner
}

fn is_named_item(tcx: TyCtxt<'_>, def_id: DefId, krate: &str, module: &str, name: &str) -> bool {
    tcx.crate_name(def_id.krate).as_str() == krate
        && tcx.def_kind(tcx.parent(def_id)) == DefKind::Mod
        && tcx.item_name(def_id).as_str() == name
        && tcx.item_name(tcx.parent(def_id)).as_str() == module
}

fn is_engine_method(tcx: TyCtxt<'_>, def_id: DefId, type_name: &str, method: &str) -> bool {
    if tcx.def_kind(def_id) != DefKind::AssocFn || tcx.item_name(def_id).as_str() != method {
        return false;
    }
    let Some(impl_id) = tcx.associated_item(def_id).impl_container(tcx) else { return false };
    let ty::Adt(adt, _) = tcx.type_of(impl_id).instantiate_identity().kind() else { return false };
    is_named_item(tcx, adt.did(), "swactor_engine", "engine", type_name)
}

fn backend_method(tcx: TyCtxt<'_>, def_id: DefId) -> Option<Symbol> {
    if tcx.def_kind(def_id) != DefKind::AssocFn {
        return None;
    }
    let item = tcx.associated_item(def_id);
    let trait_item = item.trait_item_def_id().unwrap_or(def_id);
    let trait_id = tcx.associated_item(trait_item).trait_container(tcx)?;
    is_named_item(tcx, trait_id, "swactor_engine", "backend", "ExecutionBackend")
        .then(|| tcx.item_name(trait_item))
}

fn is_backend_scheduling(tcx: TyCtxt<'_>, def_id: DefId) -> bool {
    backend_method(tcx, def_id)
        .is_some_and(|name| matches!(name.as_str(), "spawn" | "spawn_blocking"))
}

fn is_python_runtime_scheduling(tcx: TyCtxt<'_>, def_id: DefId) -> bool {
    if tcx.def_kind(def_id) != DefKind::AssocFn {
        return false;
    }
    let item = tcx.associated_item(def_id);
    let trait_item = item.trait_item_def_id().unwrap_or(def_id);
    let Some(trait_id) = tcx.associated_item(trait_item).trait_container(tcx) else { return false };
    let trait_name = match tcx.item_name(trait_item).as_str() {
        "spawn" => "Runtime",
        "spawn_local" => "SpawnLocalExt",
        _ => return false,
    };
    is_named_item(tcx, trait_id, "pyo3_async_runtimes", "generic", trait_name)
}

fn is_backend_implementation(tcx: TyCtxt<'_>, owner: DefId, module: &str, name: &str, method: &str) -> bool {
    if backend_method(tcx, owner).is_none_or(|item| item.as_str() != method) {
        return false;
    }
    let Some(impl_id) = tcx.associated_item(owner).impl_container(tcx) else { return false };
    let ty::Adt(adt, _) = tcx.type_of(impl_id).instantiate_identity().kind() else { return false };
    is_named_item(tcx, adt.did(), "swactor_engine", module, name)
}

/// The entire raw execution substrate. Both the caller and callee must match;
/// even another method in the same impl or a closure does not gain permission.
fn trusted_substrate_call(tcx: TyCtxt<'_>, owner: LocalDefId, callee: DefId, path: &str) -> bool {
    let caller = owner.to_def_id();
    if let Some(method) = backend_method(tcx, callee) {
        return match method.as_str() {
            "spawn" => is_engine_method(tcx, caller, "TaskSpawner", "spawn")
                || is_engine_method(tcx, caller, "EngineHandle", "send_after")
                || is_engine_method(tcx, caller, "EngineHandle", "send_every")
                || is_named_item(tcx, caller, "swactor_engine", "core_driver", "install"),
            "spawn_blocking" => is_engine_method(tcx, caller, "BlockingWorkSender", "submit")
                || is_engine_method(tcx, caller, "BlockingWorkSender", "submit_owned")
                || is_engine_method(tcx, caller, "BlockingWorkSender", "submit_joinable"),
            _ => false,
        };
    }
    match path {
        "tokio::runtime::Handle::spawn" =>
            is_backend_implementation(tcx, caller, "tokio", "TokioBackend", "spawn"),
        "tokio::runtime::Handle::spawn_blocking" =>
            is_backend_implementation(tcx, caller, "tokio", "TokioBackend", "spawn_blocking"),
        "std::thread::spawn" =>
            is_backend_implementation(tcx, caller, "stepping", "SteppingBackend", "spawn_blocking"),
        // Joined resource I/O must not depend on progress in the caller's
        // bounded blocking pool. Only this accounted engine capability owns it.
        "std::thread::Builder::spawn" =>
            is_engine_method(tcx, caller, "BlockingWorkSender", "spawn_owned_thread"),
        _ => false,
    }
}

fn contains_task_spawner<'tcx>(tcx: TyCtxt<'tcx>, owner: LocalDefId, ty: Ty<'tcx>) -> bool {
    fn visit<'tcx>(tcx: TyCtxt<'tcx>, env: ty::TypingEnv<'tcx>, value: Ty<'tcx>,
        seen: &mut HashSet<Ty<'tcx>>, adts: &mut HashSet<DefId>) -> bool
    {
        let value = tcx.try_normalize_erasing_regions(env, value).unwrap_or(value);
        if !seen.insert(value) { return false; }
        match value.kind() {
            ty::Adt(adt, args) => {
                if is_named_item(tcx, adt.did(), "swactor_engine", "engine", "TaskSpawner") {
                    return true;
                }
                if args.types().any(|arg| visit(tcx, env, arg, seen, adts)) { return true; }
                if !adts.insert(adt.did()) { return false; }
                let found = adt.all_fields().any(|field| visit(tcx, env, field.ty(tcx, args), seen, adts));
                adts.remove(&adt.did());
                found
            }
            ty::Ref(_, inner, _) | ty::RawPtr(inner, _) | ty::Array(inner, _) | ty::Slice(inner) =>
                visit(tcx, env, *inner, seen, adts),
            ty::Tuple(fields) => fields.iter().any(|field| visit(tcx, env, field, seen, adts)),
            ty::Closure(_, args) => args.as_closure().upvar_tys().iter()
                .any(|field| visit(tcx, env, field, seen, adts)),
            ty::Coroutine(_, args) => args.as_coroutine().upvar_tys().iter()
                .any(|field| visit(tcx, env, field, seen, adts)),
            ty::CoroutineClosure(_, args) => args.as_coroutine_closure().upvar_tys().iter()
                .any(|field| visit(tcx, env, field, seen, adts)),
            _ => false,
        }
    }
    visit(tcx, ty::TypingEnv::post_analysis(tcx, owner), ty, &mut HashSet::new(), &mut HashSet::new())
}
