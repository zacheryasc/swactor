// Myelin owns event-driven policy. Dependency implementations are not traversed:
// an opaque client may use time internally without exposing time to Myelin.
fn myelin_temporal_path(path: &str) -> bool {
    ["std::time::", "core::time::", "tokio::time::", "web_time::"]
        .iter()
        .any(|prefix| path.starts_with(prefix))
        || (path.starts_with("swactor_engine::")
            && path.split("::").any(|part| {
                matches!(
                    part,
                    "EngineInstant" | "Timer" | "Interval" | "Timeout" | "ActorTimer"
                )
            }))
}

fn check_myelin_time_type(tcx: TyCtxt<'_>, value: Ty<'_>, span: Span) {
    // Walk explicit type arguments, not the private fields of external clients.
    for argument in value.walk() {
        if let Some(ty) = argument.as_type()
            && let ty::Adt(adt, _) = ty.kind()
        {
            let path = tcx.def_path_str(adt.did());
            if myelin_temporal_path(&path) {
                tcx.dcx().span_err(span, format!(
                    "Myelin no-time policy: temporal type `{path}` is forbidden; use events, operation identities, and explicit cancellation"));
                return;
            }
        }
    }
}

fn check_myelin_time_declarations(tcx: TyCtxt<'_>) {
    for id in tcx.iter_local_def_id() {
        match tcx.def_kind(id) {
            DefKind::Fn | DefKind::AssocFn => {
                let signature = tcx.fn_sig(id).instantiate_identity().skip_binder();
                for ty in signature.inputs_and_output {
                    check_myelin_time_type(tcx, ty, tcx.def_span(id));
                }
            }
            DefKind::Field
            | DefKind::TyAlias
            | DefKind::Const
            | DefKind::AssocConst
            | DefKind::Static { .. } => {
                check_myelin_time_type(
                    tcx,
                    tcx.type_of(id).instantiate_identity(),
                    tcx.def_span(id),
                );
            }
            DefKind::AssocTy if tcx.defaultness(id).has_value() => {
                check_myelin_time_type(
                    tcx,
                    tcx.type_of(id).instantiate_identity(),
                    tcx.def_span(id),
                );
            }
            _ => {}
        }
    }
}

fn myelin_timing_api(path: &str) -> bool {
    if myelin_temporal_path(path) {
        return true;
    }
    let name = path.rsplit("::").next().unwrap_or("");
    let standard = path.starts_with("std::")
        || path.starts_with("tokio::")
        || path.starts_with("parking_lot::")
        || path.starts_with("parking_lot_core::")
        || path.starts_with("crossbeam_channel::");
    if standard
        && matches!(
            name,
            "sleep"
                | "park_timeout"
                | "recv_timeout"
                | "recv_deadline"
                | "wait_timeout"
                | "wait_timeout_while"
                | "wait_for"
                | "wait_until"
                | "set_read_timeout"
                | "set_write_timeout"
        )
    {
        return true;
    }
    if path.starts_with("libc::")
        && matches!(
            name,
            "clock_gettime"
                | "gettimeofday"
                | "time"
                | "nanosleep"
                | "clock_nanosleep"
                | "sleep"
                | "usleep"
                | "alarm"
                | "setitimer"
                | "timer_create"
                | "timer_settime"
                | "timerfd_create"
                | "timerfd_settime"
                | "poll"
                | "ppoll"
                | "select"
                | "pselect"
                | "epoll_wait"
                | "epoll_pwait"
                | "epoll_pwait2"
        )
    {
        return true;
    }
    if path.starts_with("swactor_engine::")
        && matches!(
            name,
            "now"
                | "virtual_now"
                | "advance_time"
                | "with_clock"
                | "timer"
                | "interval"
                | "timeout"
                | "send_after"
                | "send_every"
        )
    {
        return true;
    }
    // Resolved external entry points whose effect is installing/configuring time.
    matches!(
        path,
        "iroh_driver::iroh_driver::IrohDriver::install_actor_bridge_pump"
            | "telemetry::hardware::spawn_blocking_sampler"
    ) || ((path.starts_with("reqwest::") || path.starts_with("ureq::"))
        && matches!(
            name,
            "timeout"
                | "connect_timeout"
                | "read_timeout"
                | "timeout_connect"
                | "timeout_read"
                | "timeout_write"
        ))
}
