use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use swactor_vastai::VastClient;

use crate::orchestration::actor::{ContextualReplyObserverMsg, OrchestratorMsg};
use crate::orchestration::manual_control::{
    KillRequest, ManualControlMsg, ManualControlReply, OfferSearchRequest,
    ProviderConfigurationRequest, ProvisionRequest,
};
use myelin_control_contract::{
    ContextualControlReply, ContextualEventsAckRequest, ContextualEventsRequest,
    ContextualProcessEventKind, ContextualProcessSpec, ContextualProgramFile,
    ContextualSpawnRequest, ContextualStopRequest, Versioned,
};

const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_secs(2);
const RETAINED_BLOBS_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const NAMESPACE_CLEANUP_REPLY_TIMEOUT: Duration = Duration::from_secs(5);
const OFFER_SEARCH_REPLY_MARGIN: Duration = Duration::from_secs(5);
const OFFER_SEARCH_REPLY_TIMEOUT: Duration =
    VastClient::REQUEST_TIMEOUT.saturating_add(OFFER_SEARCH_REPLY_MARGIN);
const PROGRAM_UPLOAD_LIMIT: usize = 256 * 1024;
const PROGRAM_ATTACH_TIMEOUT: Duration = Duration::from_secs(80);
const PROGRAM_SPAWN_REPLY_TIMEOUT: Duration = Duration::from_secs(85);
struct ControlReplyObserver {
    reply: Arc<Mutex<Option<tokio::sync::oneshot::Sender<ManualControlReply>>>>,
    engine: EngineHandle,
    sender: ExternalSender,
    timeout: Duration,
}

impl ActorInterface for ControlReplyObserver {
    type Incoming = ManualControlReply;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.engine.send_after(
            self.timeout,
            self.sender.clone(),
            ctx.self_addr(),
            ManualControlReply::TimedOut,
        );
    }

    fn handle(&mut self, ctx: &Ctx, reply: Self::Incoming) {
        if let Some(response) = self
            .reply
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = response.send(reply);
        }
        ctx.stop_self();
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ContextualResponse {
    Reply(ContextualControlReply),
    TimedOut,
}

struct ContextualReplyObserver {
    reply: Arc<Mutex<Option<tokio::sync::oneshot::Sender<ContextualResponse>>>>,
    engine: EngineHandle,
    sender: ExternalSender,
    timeout: Duration,
    orchestrator: ActorAddress,
    cancel_key: Option<String>,
}

impl ActorInterface for ContextualReplyObserver {
    type Incoming = ContextualReplyObserverMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: Self::Incoming) {
        let response = match message {
            ContextualReplyObserverMsg::ArmTimeout => {
                self.engine.send_after(
                    self.timeout,
                    self.sender.clone(),
                    ctx.self_addr(),
                    ContextualReplyObserverMsg::TimedOut,
                );
                return;
            }
            ContextualReplyObserverMsg::TimedOut => {
                // The orchestrator never answers now; drop the parked request so
                // a later reply cannot be swallowed by a stale entry.
                if let Some(control_request_id) = self.cancel_key.take() {
                    let _ = ctx.send(
                        self.orchestrator,
                        OrchestratorMsg::ContextualControlCancel { control_request_id },
                    );
                }
                ContextualResponse::TimedOut
            }
            ContextualReplyObserverMsg::Reply(reply) => ContextualResponse::Reply(reply),
        };
        if let Some(reply) = self
            .reply
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = reply.send(response);
        }
        ctx.stop_self();
    }
}
const PROVISION_PAGE: &str = include_str!("provision_page.html");
const FLEET_CONTROL_SCRIPT: &str = include_str!("fleet_control.js");
pub(crate) const FLEET_CONTROL_SCRIPT_URL: &str = "/assets/myelin-fleet-control.js";

#[derive(Clone)]
struct ControlHttpState {
    runtime: Runtime,
    engine: EngineHandle,
    orchestrator: ActorAddress,
    namespace: Option<data_plane::control::DataPlaneControl>,
    upload_root: Arc<PathBuf>,
    live_env: crate::orchestration::provider_adapters::ssh_bootstrap::BootstrapEnvSource,
}

pub(crate) fn plugin(
    runtime: Runtime,
    engine: EngineHandle,
    orchestrator: ActorAddress,
    namespace: data_plane::control::DataPlaneControl,
    upload_root: PathBuf,
    live_env: crate::orchestration::provider_adapters::ssh_bootstrap::BootstrapEnvSource,
) -> dashboard::DashboardPlugin {
    let state = ControlHttpState {
        runtime,
        engine,
        orchestrator,
        namespace: Some(namespace),
        upload_root: Arc::new(upload_root),
        live_env,
    };
    let routes = Router::new()
        .route(FLEET_CONTROL_SCRIPT_URL, get(fleet_control_script))
        .route("/api/control/status", get(status))
        .route("/api/control/endpoint", get(live_endpoint))
        .route("/api/control/fleet", get(fleet_status))
        .route("/api/control/actors", get(actor_stats))
        .route("/api/control/actors/snapshot", get(actor_snapshot))
        .route("/api/control/changes", post(control_changes))
        .route("/api/control/provision", post(provision))
        .route("/api/control/kill", post(kill))
        .route("/api/control/provider", post(configure_provider))
        .route("/api/control/offers", post(search_offers))
        .route("/api/control/flush", post(flush))
        .route("/api/control/nodes/{logical_node_id}/kill", post(kill_path))
        .route("/api/control/contextual/spawn", post(contextual_spawn))
        .route(
            "/api/control/contextual/nodes/{logical_node_id}/python",
            post(contextual_python),
        )
        .route(
            "/api/control/contextual/{request_id}/events",
            get(contextual_events),
        )
        .route(
            "/api/control/contextual/events",
            post(contextual_events_batch),
        )
        .route(
            "/api/control/contextual/{request_id}/ack",
            post(contextual_events_ack),
        )
        .route(
            "/api/control/contextual/{request_id}/stop",
            post(contextual_stop),
        )
        .route(
            "/api/control/contextual/nodes/{logical_node_id}",
            get(contextual_query_node),
        )
        .route(
            "/api/control/contextual/namespace-cleanup",
            post(namespace_cleanup),
        )
        .route(
            "/api/control/contextual/retained-blobs",
            post(retained_blobs),
        )
        .layer(DefaultBodyLimit::max(PROGRAM_UPLOAD_LIMIT))
        .with_state(state);
    dashboard::DashboardPlugin::new(routes).with_page(dashboard::PluginPage::new(
        "provision",
        "Provision",
        "/provision",
        PROVISION_PAGE,
    ))
}

async fn fleet_control_script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        FLEET_CONTROL_SCRIPT,
    )
}
/// The orchestrator's CURRENT transport endpoint and actor address.
///
/// A restarted orchestrator binds a fresh port; remote workers that were
/// still booting when the previous process died never persisted an endpoint
/// the recovery join can dial, and they keep targeting the dead address.
/// Operators (and the e2e harness) read this and push the live endpoint to
/// those workers through each node's debug-join socket.
async fn live_endpoint(State(state): State<ControlHttpState>) -> Response {
    match (state.live_env)() {
        Ok(env) => Json(serde_json::Map::from_iter(
            env.into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value))),
        ))
        .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse { error }),
        )
            .into_response(),
    }
}

async fn provision(
    State(state): State<ControlHttpState>,
    Json(request): Json<ProvisionRequest>,
) -> Response {
    request_reply(&state, CONTROL_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::Provision {
            request,
            reply_to: Some(reply_to),
        }
    })
    .await
}

async fn kill(State(state): State<ControlHttpState>, Json(request): Json<KillRequest>) -> Response {
    route_mutation(
        &state,
        ManualControlMsg::Kill {
            request,
            reply_to: None,
        },
    )
}

#[derive(serde::Deserialize)]
struct KillPathRequest {
    command_id: String,
}

async fn kill_path(
    Path(logical_node_id): Path<u64>,
    State(state): State<ControlHttpState>,
    Json(request): Json<KillPathRequest>,
) -> Response {
    route_mutation(
        &state,
        ManualControlMsg::Kill {
            request: KillRequest {
                command_id: request.command_id,
                logical_node_id,
            },
            reply_to: None,
        },
    )
}

async fn configure_provider(
    State(state): State<ControlHttpState>,
    Json(request): Json<ProviderConfigurationRequest>,
) -> Response {
    route_mutation(
        &state,
        ManualControlMsg::Configure {
            request,
            reply_to: None,
        },
    )
}

async fn status(State(state): State<ControlHttpState>) -> Response {
    request_reply(&state, CONTROL_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::Query { reply_to }
    })
    .await
}

async fn fleet_status(State(state): State<ControlHttpState>) -> Response {
    let response_rx = match begin_request_reply(&state, CONTROL_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::QueryFleet { reply_to }
    }) {
        Ok(response_rx) => response_rx,
        Err(response) => return *response,
    };
    match response_rx.await {
        Ok(ManualControlReply::FleetStatus(model)) => {
            Json(ManualControlReply::FleetStatus(model)).into_response()
        }
        Ok(ManualControlReply::Rejected(error)) => {
            (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response()
        }
        Ok(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "manual control returned an unexpected Fleet status reply".to_owned(),
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("control reply observer stopped: {error}"),
            }),
        )
            .into_response(),
    }
}

async fn actor_stats(State(state): State<ControlHttpState>) -> impl IntoResponse {
    Json(state.runtime.stats())
}

async fn search_offers(
    State(state): State<ControlHttpState>,
    Json(request): Json<OfferSearchRequest>,
) -> Response {
    request_reply(&state, OFFER_SEARCH_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::SearchOffers { request, reply_to }
    })
    .await
}

async fn flush(State(state): State<ControlHttpState>) -> Response {
    request_reply(&state, CONTROL_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::Flush { reply_to }
    })
    .await
}

async fn contextual_spawn(
    State(state): State<ControlHttpState>,
    Json(request): Json<ContextualSpawnRequest>,
) -> Response {
    contextual_request_reply(&state, None, |reply_to| OrchestratorMsg::ContextualSpawn {
        logical_node_id: request.logical_node_id,
        request_id: request.request_id,
        spec: request.spec,
        reply_to,
    })
    .await
}
#[derive(serde::Deserialize)]
struct ContextualPythonQuery {
    filename: String,
}

async fn contextual_python(
    Path(logical_node_id): Path<u64>,
    Query(query): Query<ContextualPythonQuery>,
    State(state): State<ControlHttpState>,
    body: Bytes,
) -> Response {
    let Some(namespace) = state.namespace.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "uploaded program control is unavailable".to_owned(),
            }),
        )
            .into_response();
    };
    let filename = query.filename.trim();
    if filename.is_empty()
        || filename
            != PathBuf::from(filename)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
        || !filename.to_ascii_lowercase().ends_with(".py")
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "select one Python file with a .py extension".to_owned(),
            }),
        )
            .into_response();
    }
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "selected Python file is empty".to_owned(),
            }),
        )
            .into_response();
    }
    if body.len() > PROGRAM_UPLOAD_LIMIT {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorResponse {
                error: format!(
                    "selected Python file exceeds the {} byte limit",
                    PROGRAM_UPLOAD_LIMIT
                ),
            }),
        )
            .into_response();
    }

    let request_id = format!("ui-{}", ActorAddress::new_random().to_full_hex());
    let host_path = state.upload_root.join(format!("{request_id}.py"));
    if let Err(error) = write_uploaded_program(&state, host_path.clone(), body).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error }),
        )
            .into_response();
    }
    let namespace_path = format!("/myelin/contextual/{request_id}/program.py");
    let data_path = data_plane::path::DataPath::parse(namespace_path.clone())
        .expect("generated contextual program path is valid");
    if let Err(error) = namespace
        .register(data_path.clone(), data_plane::blob::file(&host_path))
        .await
    {
        remove_uploaded_program(&state, host_path);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("register uploaded program: {error}"),
            }),
        )
            .into_response();
    }

    let spec = python_contextual_spec(&request_id, filename, namespace_path.clone());
    let response_rx = match begin_contextual_request_reply(
        &state,
        None,
        PROGRAM_SPAWN_REPLY_TIMEOUT,
        |reply_to| OrchestratorMsg::ContextualSpawn {
            logical_node_id,
            request_id,
            spec,
            reply_to,
        },
    ) {
        Ok(response_rx) => response_rx,
        Err(response) => {
            cleanup_uploaded_program(&state, data_path, host_path).await;
            return *response;
        }
    };
    contextual_response(response_rx).await
}

fn python_contextual_spec(
    request_id: &str,
    filename: &str,
    namespace_path: String,
) -> ContextualProcessSpec {
    ContextualProcessSpec {
        command: "python3".to_owned(),
        args: Vec::new(),
        env: BTreeMap::new(),
        working_dir: None,
        label: Some(filename.to_owned()),
        execution_id: request_id.to_owned(),
        read_prefixes: vec!["/models".to_owned(), format!("/runs/{request_id}/results")],
        write_prefixes: vec![format!("/runs/{request_id}/results")],
        attach_timeout_ms: PROGRAM_ATTACH_TIMEOUT.as_millis() as u64,
        staged_program: Some(ContextualProgramFile { namespace_path }),
    }
}

async fn write_uploaded_program(
    state: &ControlHttpState,
    path: PathBuf,
    body: Bytes,
) -> Result<(), String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let root = Arc::clone(&state.upload_root);
    let work = Box::new(move || {
        let result = std::fs::create_dir_all(root.as_ref())
            .and_then(|()| std::fs::write(&path, &body))
            .map_err(|error| format!("store uploaded program: {error}"));
        let _ = tx.send(result);
    });
    state
        .engine
        .blocking_work_sender()
        .submit(work)
        .map_err(|_| "execution engine stopped before storing uploaded program".to_owned())?;
    rx.await
        .map_err(|error| format!("uploaded program writer stopped: {error}"))?
}

fn remove_uploaded_program(state: &ControlHttpState, path: PathBuf) {
    let _ = state
        .engine
        .blocking_work_sender()
        .submit(Box::new(move || {
            let _ = std::fs::remove_file(path);
        }));
}

async fn cleanup_uploaded_program(
    state: &ControlHttpState,
    data_path: data_plane::path::DataPath,
    host_path: PathBuf,
) {
    if let Some(namespace) = &state.namespace {
        let _ = namespace.unregister(data_path).await;
    }
    remove_uploaded_program(state, host_path);
}

#[derive(Default, serde::Deserialize)]
struct ContextualEventsQuery {
    #[serde(default)]
    after_sequence: u64,
}

async fn contextual_events(
    Path(request_id): Path<String>,
    Query(query): Query<ContextualEventsQuery>,
    State(state): State<ControlHttpState>,
) -> Response {
    contextual_request_reply(&state, None, |reply_to| OrchestratorMsg::ContextualEvents {
        request_id,
        after_sequence: query.after_sequence,
        reply_to,
    })
    .await
}

#[derive(serde::Deserialize)]
struct ActorSnapshotQuery {
    request_id: String,
}

async fn actor_snapshot(
    State(state): State<ControlHttpState>,
    Query(query): Query<ActorSnapshotQuery>,
) -> Response {
    if query.request_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "actor snapshot requires a nonempty request identity".to_owned(),
            }),
        )
            .into_response();
    }
    let request = match state.runtime.admin().list_actors() {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("request fresh actor census: {error}"),
                }),
            )
                .into_response();
        }
    };
    let actors = match tokio::time::timeout(CONTROL_REPLY_TIMEOUT, request.recv()).await {
        Ok(Ok(snapshot)) => snapshot.actors,
        result => {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(ErrorResponse {
                    error: format!("fresh actor census incomplete: {result:?}"),
                }),
            )
                .into_response();
        }
    };
    Json(serde_json::json!({
        "schema_version": myelin_control_contract::SCHEMA_VERSION,
        "request_id": query.request_id,
        "actors": actors.into_iter().map(|actor| serde_json::json!({
            "address": actor.address.to_full_hex(),
            "actor_type": actor.actor_type,
            "worker_id": actor.worker_id,
            "mailbox_depth": actor.mailbox_depth,
            "poisoned": actor.status.poisoned,
            "stopping": actor.status.stopping,
        })).collect::<Vec<_>>(),
        "runtime_stats": state.runtime.stats(),
        "namespace_commits": data_plane::namespace_store::commit_metrics(),
    }))
    .into_response()
}

async fn control_changes(
    State(state): State<ControlHttpState>,
    Json(request): Json<Versioned<myelin_control_contract::ControlChangesRequest>>,
) -> Response {
    let request = match request.into_payload() {
        Ok(request) => request,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response();
        }
    };
    let wait_ms = request.wait_ms.min(1_000);
    let wait_key = (wait_ms != 0).then(|| format!("control-{}", ActorAddress::new_random()));
    let timeout = if wait_ms == 0 {
        CONTROL_REPLY_TIMEOUT
    } else {
        Duration::from_millis(wait_ms)
    };
    let receiver =
        match begin_contextual_request_reply(&state, wait_key.clone(), timeout, |reply_to| {
            OrchestratorMsg::ControlChanges {
                generation: request.generation,
                after_revision: request.after_revision,
                wait_key,
                reply_to,
            }
        }) {
            Ok(receiver) => receiver,
            Err(response) => return *response,
        };
    contextual_response(receiver).await
}

async fn contextual_events_batch(
    State(state): State<ControlHttpState>,
    Json(request): Json<Versioned<ContextualEventsRequest>>,
) -> Response {
    let request = match request.into_payload() {
        Ok(request) => request,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response();
        }
    };
    let wait_ms = request.wait_ms.min(1_000);
    let wait_key = (wait_ms != 0).then(|| format!("events-{}", ActorAddress::new_random()));
    let timeout = if wait_ms == 0 {
        CONTROL_REPLY_TIMEOUT
    } else {
        Duration::from_millis(wait_ms)
    };
    let cursors = request.cursors;
    let receiver =
        match begin_contextual_request_reply(&state, wait_key.clone(), timeout, |reply_to| {
            OrchestratorMsg::ContextualEventsBatch {
                cursors: cursors.clone(),
                wait_key,
                reply_to,
            }
        }) {
            Ok(receiver) => receiver,
            Err(response) => return *response,
        };
    match receiver.await {
        Ok(ContextualResponse::TimedOut) if wait_ms != 0 => {
            let receiver = match begin_contextual_request_reply(
                &state,
                None,
                CONTROL_REPLY_TIMEOUT,
                |reply_to| OrchestratorMsg::ContextualEventsBatch {
                    cursors,
                    wait_key: None,
                    reply_to,
                },
            ) {
                Ok(receiver) => receiver,
                Err(response) => return *response,
            };
            match receiver.await {
                Ok(ContextualResponse::Reply(ContextualControlReply::EventsBatch(batch)))
                    if batch.missing.is_empty()
                        && batch
                            .executions
                            .iter()
                            .all(|execution| execution.events.is_empty()) =>
                {
                    contextual_response_result(Ok(ContextualResponse::TimedOut))
                }
                response => contextual_response_result(response),
            }
        }
        response => contextual_response_result(response),
    }
}

async fn contextual_events_ack(
    Path(request_id): Path<String>,
    State(state): State<ControlHttpState>,
    Json(request): Json<Versioned<ContextualEventsAckRequest>>,
) -> Response {
    let request = match request.into_payload() {
        Ok(request) => request,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response();
        }
    };
    contextual_request_reply(&state, None, |reply_to| {
        OrchestratorMsg::ContextualEventsAck {
            request_id,
            through_sequence: request.through_sequence,
            execution_incarnation: request.execution_incarnation,
            reply_to,
        }
    })
    .await
}

async fn contextual_stop(
    Path(request_id): Path<String>,
    State(state): State<ControlHttpState>,
    Json(request): Json<ContextualStopRequest>,
) -> Response {
    contextual_request_reply(
        &state,
        Some(request.control_request_id.clone()),
        |reply_to| OrchestratorMsg::ContextualStop {
            request_id,
            control_request_id: request.control_request_id,
            kill_after_ms: request.kill_after_ms,
            reply_to,
        },
    )
    .await
}

#[derive(serde::Deserialize)]
struct ContextualNodeQuery {
    control_request_id: String,
}

async fn contextual_query_node(
    Path(logical_node_id): Path<u64>,
    Query(query): Query<ContextualNodeQuery>,
    State(state): State<ControlHttpState>,
) -> Response {
    use myelin_control_contract::{
        ContextualHealthEvent, ContextualHealthObservation, ContextualHealthReply,
        ContextualHealthReplyType, HealthExecution,
    };
    let response = match begin_contextual_request_reply(
        &state,
        Some(query.control_request_id.clone()),
        CONTROL_REPLY_TIMEOUT,
        |reply_to| OrchestratorMsg::ContextualQuery {
            logical_node_id,
            control_request_id: query.control_request_id.clone(),
            reply_to,
        },
    ) {
        Ok(response) => response,
        Err(response) => return *response,
    };
    let reply = match response.await {
        Ok(ContextualResponse::Reply(ContextualControlReply::Event { observation })) => observation,
        other => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!(
                        "contextual health did not return a successful observation: {other:?}"
                    ),
                }),
            )
                .into_response();
        }
    };
    let ContextualProcessEventKind::LiveExecutions {
        executions,
        resources: Some(resources),
    } = reply.event
    else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "contextual health omitted live executions or fresh resources".to_owned(),
            }),
        )
            .into_response();
    };
    let reply = ContextualHealthReply {
        schema_version: myelin_control_contract::SCHEMA_VERSION,
        reply_type: ContextualHealthReplyType::Event,
        observation: ContextualHealthObservation {
            logical_node_id: reply.logical_node_id,
            request_id: reply.request_id,
            event: ContextualHealthEvent::LiveExecutions {
                executions: executions
                    .into_iter()
                    .map(|execution| HealthExecution {
                        request_id: execution.request_id,
                        process: execution.process,
                        execution_id: execution.identity.execution_id,
                        generation: execution.identity.generation,
                        started_pid: execution.started_pid,
                        context_ready: execution.context_ready,
                    })
                    .collect(),
                resources,
            },
        },
    };
    if let Err(error) = reply.validate(logical_node_id, &query.control_request_id) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse { error }),
        )
            .into_response();
    }
    Json(reply).into_response()
}

async fn namespace_cleanup(
    State(state): State<ControlHttpState>,
    Json(request): Json<myelin_control_contract::NamespaceCleanupRequest>,
) -> Response {
    use data_plane::control::NamespaceCleanupStatus;
    use myelin_control_contract::{
        NamespaceCleanupPath, NamespaceCleanupQuiescence, NamespaceCleanupReply, SCHEMA_VERSION,
    };
    if request.schema_version != SCHEMA_VERSION
        || request.request_id.is_empty()
        || request.paths.is_empty()
        || request
            .paths
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != request.paths.len()
        || request
            .paths
            .iter()
            .any(|path| !path.starts_with("/cases/") && !path.starts_with("/runs/"))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid owned namespace cleanup request".to_owned(),
            }),
        )
            .into_response();
    }
    let Some(namespace) = &state.namespace else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "namespace is unavailable".to_owned(),
            }),
        )
            .into_response();
    };
    let mut parsed = Vec::with_capacity(request.paths.len());
    for path in &request.paths {
        match data_plane::path::DataPath::parse(path) {
            Ok(data_path) => parsed.push((path.clone(), data_path)),
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid owned cleanup path: {error}"),
                    }),
                )
                    .into_response();
            }
        }
    }

    let deadline = tokio::time::Instant::now() + NAMESPACE_CLEANUP_REPLY_TIMEOUT;
    let mut cleanups = tokio::task::JoinSet::new();
    for (path, data_path) in parsed {
        let namespace = namespace.clone();
        cleanups.spawn(async move {
            let mut quiescence = None;
            let error = loop {
                match tokio::time::timeout_at(
                    deadline,
                    namespace.try_unregister_quiescent(data_path.clone()),
                )
                .await
                {
                    Ok(Ok(NamespaceCleanupStatus::ActiveStream)) => {
                        tokio::task::yield_now().await;
                    }
                    Ok(Ok(NamespaceCleanupStatus::Absent)) => break None,
                    Ok(Ok(NamespaceCleanupStatus::Removed {
                        quiescent_stream_revision,
                    })) => {
                        quiescence =
                            quiescent_stream_revision.map(|revision| NamespaceCleanupQuiescence {
                                revision,
                                active: false,
                            });
                        break None;
                    }
                    Ok(Err(error)) => break Some(error.to_string()),
                    Err(error) => break Some(format!("namespace cleanup deadline: {error}")),
                }
            };
            NamespaceCleanupPath {
                record_type: "path_cleanup".to_owned(),
                path,
                attempted: true,
                absent: error.is_none(),
                quiescence,
                error,
            }
        });
    }
    let mut paths = Vec::with_capacity(request.paths.len());
    while let Some(result) = cleanups.join_next().await {
        match result {
            Ok(path) => paths.push(path),
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("namespace cleanup task failed: {error}"),
                    }),
                )
                    .into_response();
            }
        }
    }
    paths.sort_by(|left, right| left.path.cmp(&right.path));
    Json(NamespaceCleanupReply {
        schema_version: SCHEMA_VERSION,
        request_id: request.request_id,
        paths,
    })
    .into_response()
}

async fn retained_blobs(
    State(state): State<ControlHttpState>,
    Json(request): Json<myelin_control_contract::RetainedBlobsRequest>,
) -> Response {
    use data_plane::control::RetainedNamespaceResources;
    use myelin_control_contract::RetainedNonBlob;
    use myelin_control_contract::{RetainedBlob, RetainedBlobsReply, SCHEMA_VERSION};
    if request.schema_version != SCHEMA_VERSION
        || request.request_id.is_empty()
        || request
            .paths
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != request.paths.len()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid retained-blob ownership query".to_owned(),
            }),
        )
            .into_response();
    }
    let Some(namespace) = &state.namespace else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "namespace is unavailable".to_owned(),
            }),
        )
            .into_response();
    };
    let mut blobs = std::collections::BTreeMap::new();
    let mut non_blobs = std::collections::BTreeMap::new();
    let mut pending = request
        .paths
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let deadline = tokio::time::Instant::now() + RETAINED_BLOBS_REPLY_TIMEOUT;
    let mut queries = tokio::task::JoinSet::new();
    for path in request.paths {
        let parsed = match data_plane::path::DataPath::parse(&path) {
            Ok(path) => path,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("invalid retained path: {error}"),
                    }),
                )
                    .into_response();
            }
        };
        let namespace = namespace.clone();
        queries.spawn(async move {
            let resources = namespace.retained_blob_resources(parsed).await;
            (path, resources)
        });
    }
    while !queries.is_empty() {
        let joined = match tokio::time::timeout_at(deadline, queries.join_next()).await {
            Ok(Some(joined)) => joined,
            Ok(None) => break,
            Err(error) => {
                return (
                    StatusCode::GATEWAY_TIMEOUT,
                    Json(ErrorResponse {
                        error: format!(
                            "retained blob ownership query deadline: {error}; pending={pending:?}"
                        ),
                    }),
                )
                    .into_response();
            }
        };
        let (path, binding) = match joined {
            Ok(result) => result,
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("retained blob ownership query task failed: {error}"),
                    }),
                )
                    .into_response();
            }
        };
        pending.remove(&path);
        match binding {
            Ok(RetainedNamespaceResources::Blob(binding)) => {
                blobs.insert(
                    path,
                    RetainedBlob {
                        source: binding.source.to_full_hex(),
                        binding: binding.owner.map(|owner| owner.to_full_hex()),
                        source_node: binding.source_node,
                        length: binding.length,
                        revision: binding.revision,
                    },
                );
            }
            Ok(RetainedNamespaceResources::Absent) => {
                non_blobs.insert(path, RetainedNonBlob::Absent);
            }
            Ok(RetainedNamespaceResources::QuiescentStream { revision }) => {
                non_blobs.insert(path, RetainedNonBlob::QuiescentStream { revision });
            }
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("retained blob could not be resolved: {error}"),
                    }),
                )
                    .into_response();
            }
        }
    }
    Json(RetainedBlobsReply {
        schema_version: SCHEMA_VERSION,
        request_id: request.request_id,
        blobs,
        non_blobs,
    })
    .into_response()
}

fn route_mutation(state: &ControlHttpState, msg: ManualControlMsg) -> Response {
    match state
        .runtime
        .send_to(state.orchestrator, OrchestratorMsg::Manual(msg))
    {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("orchestrator control actor unavailable: {error}"),
            }),
        )
            .into_response(),
    }
}

async fn request_reply(
    state: &ControlHttpState,
    timeout: Duration,
    build: impl FnOnce(ActorAddress) -> ManualControlMsg,
) -> Response {
    let response_rx = match begin_request_reply(state, timeout, build) {
        Ok(response_rx) => response_rx,
        Err(response) => return *response,
    };

    match response_rx.await {
        Ok(ManualControlReply::Rejected(error)) => {
            (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response()
        }
        Ok(ManualControlReply::TimedOut) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(ErrorResponse {
                error: "orchestrator control reply timed out".to_owned(),
            }),
        )
            .into_response(),
        Ok(reply) => Json(reply).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("control reply observer stopped: {error}"),
            }),
        )
            .into_response(),
    }
}

async fn contextual_request_reply(
    state: &ControlHttpState,
    cancel_key: Option<String>,
    build: impl FnOnce(ActorAddress) -> OrchestratorMsg,
) -> Response {
    let response_rx =
        match begin_contextual_request_reply(state, cancel_key, CONTROL_REPLY_TIMEOUT, build) {
            Ok(response_rx) => response_rx,
            Err(response) => return *response,
        };
    contextual_response(response_rx).await
}

async fn contextual_response(
    response_rx: tokio::sync::oneshot::Receiver<ContextualResponse>,
) -> Response {
    contextual_response_result(response_rx.await)
}

fn contextual_response_result(
    response: Result<ContextualResponse, tokio::sync::oneshot::error::RecvError>,
) -> Response {
    match response {
        Ok(ContextualResponse::TimedOut) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(ErrorResponse {
                error: "contextual control reply timed out".to_owned(),
            }),
        )
            .into_response(),
        Ok(ContextualResponse::Reply(ContextualControlReply::Rejected { error })) => {
            (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response()
        }
        Ok(ContextualResponse::Reply(
            reply @ (ContextualControlReply::EventsBatch(_)
            | ContextualControlReply::Acknowledged(_)
            | ContextualControlReply::ControlRevision(_)),
        )) => Json(Versioned::new(reply)).into_response(),
        Ok(ContextualResponse::Reply(reply)) => Json(reply).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("contextual reply observer stopped: {error}"),
            }),
        )
            .into_response(),
    }
}

fn begin_contextual_request_reply(
    state: &ControlHttpState,
    cancel_key: Option<String>,
    timeout: Duration,
    build: impl FnOnce(ActorAddress) -> OrchestratorMsg,
) -> Result<tokio::sync::oneshot::Receiver<ContextualResponse>, Box<Response>> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let response_tx = Arc::new(Mutex::new(Some(response_tx)));
    let reply_to = state
        .runtime
        .spawn(ContextualReplyObserver {
            reply: response_tx,
            engine: state.engine.clone(),
            sender: state.runtime.create_sender(),
            timeout,
            orchestrator: state.orchestrator,
            cancel_key,
        })
        .map_err(|error| {
            Box::new(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("create contextual reply observer: {error}"),
                    }),
                )
                    .into_response(),
            )
        })?;
    if let Err(error) = state.runtime.send_to(state.orchestrator, build(reply_to)) {
        let _ = state.runtime.stop_actor(reply_to);
        return Err(Box::new(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("orchestrator control actor unavailable: {error}"),
                }),
            )
                .into_response(),
        ));
    }
    // The subscription is already queued before its timeout can enqueue
    // cancellation. Otherwise a short timeout can cancel a not-yet-registered
    // waiter and strand it after the observer has stopped.
    let _ = state
        .runtime
        .send_to(reply_to, ContextualReplyObserverMsg::ArmTimeout);
    Ok(response_rx)
}

fn begin_request_reply(
    state: &ControlHttpState,
    timeout: Duration,
    build: impl FnOnce(ActorAddress) -> ManualControlMsg,
) -> Result<tokio::sync::oneshot::Receiver<ManualControlReply>, Box<Response>> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let response_tx = Arc::new(Mutex::new(Some(response_tx)));
    let reply_to = state
        .runtime
        .spawn(ControlReplyObserver {
            reply: response_tx,
            engine: state.engine.clone(),
            sender: state.runtime.create_sender(),
            timeout,
        })
        .map_err(|error| {
            Box::new(
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse {
                        error: format!("create control reply observer: {error}"),
                    }),
                )
                    .into_response(),
            )
        })?;
    if let Err(error) = state
        .runtime
        .send_to(state.orchestrator, OrchestratorMsg::Manual(build(reply_to)))
    {
        let _ = state.runtime.stop_actor(reply_to);
        return Err(Box::new(
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("orchestrator control actor unavailable: {error}"),
                }),
            )
                .into_response(),
        ));
    }
    Ok(response_rx)
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    use super::*;
    use crate::tests::fuzz_support::{
        actor_census, assert_actor_delta_at_most, assert_mailboxes_drained, assert_no_poison,
        drive_steps,
    };

    const STEP_BUDGET: usize = 64;

    fn reply(code: u8) -> ManualControlReply {
        match code % 3 {
            0 => ManualControlReply::Flushed,
            1 => ManualControlReply::Rejected(format!("rejected-{code}")),
            _ => ManualControlReply::TimedOut,
        }
    }

    #[derive(Clone, Debug)]
    enum HttpAction {
        Provision { command_slot: u8, count: u8 },
        Kill { command_slot: u8, node: u8 },
        KillPath { command_slot: u8, node: u8 },
        Configure { value: u8 },
        Status,
        ActorStats,
        Flush,
        SearchOffers,
    }

    impl HttpAction {
        fn from_raw(kind: u8, command_slot: u8, value: u8) -> Self {
            match kind % 8 {
                0 => Self::Provision {
                    command_slot,
                    count: value,
                },
                1 => Self::Kill {
                    command_slot,
                    node: value,
                },
                2 => Self::KillPath {
                    command_slot,
                    node: value,
                },
                3 => Self::Configure { value },
                4 => Self::Status,
                5 => Self::ActorStats,
                6 => Self::Flush,
                _ => Self::SearchOffers,
            }
        }

        fn command_id(command_slot: u8) -> String {
            format!("repeated-{}", command_slot % 4)
        }

        fn expected_bridge_observation(&self) -> Option<String> {
            match self {
                Self::Provision { command_slot, .. } => {
                    Some(format!("Provision:{}", Self::command_id(*command_slot)))
                }
                Self::Kill { command_slot, .. } | Self::KillPath { command_slot, .. } => {
                    Some(format!("Kill:{}", Self::command_id(*command_slot)))
                }
                Self::Configure { .. } => Some("Configure".to_owned()),
                Self::Status => Some("Status".to_owned()),
                Self::ActorStats => None,
                Self::Flush => Some("Flush".to_owned()),
                Self::SearchOffers => Some("SearchOffers".to_owned()),
            }
        }

        fn is_mutation(&self) -> bool {
            matches!(
                self,
                Self::Provision { .. }
                    | Self::Kill { .. }
                    | Self::KillPath { .. }
                    | Self::Configure { .. }
            )
        }

        fn uses_reply_observer(&self) -> bool {
            matches!(self, Self::Status | Self::Flush | Self::SearchOffers)
        }
    }

    #[derive(Clone, Debug)]
    struct HttpObservation {
        index: usize,
        action: HttpAction,
        status: StatusCode,
    }

    struct HttpBridgeProbe {
        observations: Arc<Mutex<Vec<String>>>,
        disappear_reply_observers: bool,
    }

    impl HttpBridgeProbe {
        fn finish_reply(&self, ctx: &Ctx, reply_to: ActorAddress, reply: ManualControlReply) {
            if self.disappear_reply_observers {
                let _ = ctx.stop_actor(reply_to);
            } else {
                let _ = ctx.send(reply_to, reply);
            }
        }
    }

    impl ActorInterface for HttpBridgeProbe {
        type Incoming = OrchestratorMsg;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
            let OrchestratorMsg::Manual(message) = message else {
                return;
            };
            match message {
                ManualControlMsg::Provision { request, .. } => self
                    .observations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(format!("Provision:{}", request.command_id)),
                ManualControlMsg::Kill { request, .. } => self
                    .observations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(format!("Kill:{}", request.command_id)),
                ManualControlMsg::Configure { .. } => self
                    .observations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push("Configure".to_owned()),
                ManualControlMsg::Query { reply_to } => {
                    self.observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push("Status".to_owned());
                    self.finish_reply(ctx, reply_to, ManualControlReply::Flushed);
                }
                ManualControlMsg::Flush { reply_to } => {
                    self.observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push("Flush".to_owned());
                    self.finish_reply(ctx, reply_to, ManualControlReply::Flushed);
                }
                ManualControlMsg::SearchOffers { reply_to, .. } => {
                    self.observations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push("SearchOffers".to_owned());
                    self.finish_reply(
                        ctx,
                        reply_to,
                        ManualControlReply::Offers(
                            crate::orchestration::manual_control::OfferSearchResults {
                                search_id: 0,
                                offers: Vec::new(),
                            },
                        ),
                    );
                }
                _ => {}
            }
        }
    }

    enum PendingHttpObservation {
        Ready(HttpObservation),
        Reply {
            index: usize,
            action: HttpAction,
            receiver: tokio::sync::oneshot::Receiver<ManualControlReply>,
        },
    }

    fn begin_http_action(
        state: &ControlHttpState,
        index: usize,
        action: HttpAction,
    ) -> PendingHttpObservation {
        let immediate = |status| {
            PendingHttpObservation::Ready(HttpObservation {
                index,
                action: action.clone(),
                status,
            })
        };
        let reply = |result: Result<_, Box<Response>>| match result {
            Ok(receiver) => PendingHttpObservation::Reply {
                index,
                action: action.clone(),
                receiver,
            },
            Err(response) => immediate(response.status()),
        };

        match &action {
            HttpAction::Provision {
                command_slot,
                count,
            } => immediate(
                route_mutation(
                    state,
                    ManualControlMsg::Provision {
                        request: ProvisionRequest {
                            command_id: HttpAction::command_id(*command_slot),
                            count: u32::from(*count),
                            selected_offer_ids: Vec::new(),
                            search_id: None,
                            image: None,
                        },
                        reply_to: None,
                    },
                )
                .status(),
            ),
            HttpAction::Kill { command_slot, node }
            | HttpAction::KillPath { command_slot, node } => immediate(
                route_mutation(
                    state,
                    ManualControlMsg::Kill {
                        request: KillRequest {
                            command_id: HttpAction::command_id(*command_slot),
                            logical_node_id: u64::from(*node),
                        },
                        reply_to: None,
                    },
                )
                .status(),
            ),
            HttpAction::Configure { value } => immediate(
                route_mutation(
                    state,
                    ManualControlMsg::Configure {
                        request: ProviderConfigurationRequest {
                            api_key: Some(format!("generated-key-{value}")),
                            ssh_identity: None,
                            bootstrap_command: None,
                        },
                        reply_to: None,
                    },
                )
                .status(),
            ),
            HttpAction::Status => reply(begin_request_reply(
                state,
                CONTROL_REPLY_TIMEOUT,
                |reply_to| ManualControlMsg::Query { reply_to },
            )),
            HttpAction::ActorStats => immediate(StatusCode::OK),
            HttpAction::Flush => reply(begin_request_reply(
                state,
                CONTROL_REPLY_TIMEOUT,
                |reply_to| ManualControlMsg::Flush { reply_to },
            )),
            HttpAction::SearchOffers => reply(begin_request_reply(
                state,
                OFFER_SEARCH_REPLY_TIMEOUT,
                |reply_to| ManualControlMsg::SearchOffers {
                    request: OfferSearchRequest::default(),
                    reply_to,
                },
            )),
        }
    }

    fn finish_http_action(observation: PendingHttpObservation) -> Result<HttpObservation, String> {
        let PendingHttpObservation::Reply {
            index,
            action,
            mut receiver,
        } = observation
        else {
            let PendingHttpObservation::Ready(observation) = observation else {
                unreachable!("pending HTTP observation variant changed")
            };
            return Ok(observation);
        };
        let status = match receiver.try_recv() {
            Ok(ManualControlReply::Rejected(_)) => StatusCode::CONFLICT,
            Ok(ManualControlReply::TimedOut) => StatusCode::GATEWAY_TIMEOUT,
            Ok(_) => StatusCode::OK,
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                return Err(format!(
                    "control reply remained pending after fixed step budget; \
                     index={index}, action={action:?}"
                ));
            }
        };
        Ok(HttpObservation {
            index,
            action,
            status,
        })
    }

    fn http_bridge_invariant_failure(
        actions: &[HttpAction],
        responses: &[HttpObservation],
        observed: &[String],
        state: &ControlHttpState,
        baseline_actors: usize,
        disappear_reply_observers: bool,
    ) -> Option<String> {
        let mut expected_bridge = actions
            .iter()
            .filter_map(HttpAction::expected_bridge_observation)
            .collect::<Vec<_>>();
        expected_bridge.sort();
        let mut actual_bridge = observed.to_vec();
        actual_bridge.sort();
        let statuses_valid = responses.iter().all(|response| {
            let expected = if response.action.is_mutation() {
                StatusCode::ACCEPTED
            } else if disappear_reply_observers && response.action.uses_reply_observer() {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            };
            response.status == expected
        });
        let stats = state.runtime.stats();
        let panics = stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        let mailbox_depth = stats
            .workers
            .iter()
            .map(|worker| worker.mailbox_depth)
            .sum::<usize>()
            + stats
                .actor_details
                .iter()
                .map(|actor| actor.mailbox_depth)
                .sum::<usize>();
        if responses.len() == actions.len()
            && statuses_valid
            && actual_bridge == expected_bridge
            && stats.actors.len() == baseline_actors
            && stats.actor_details.iter().all(|actor| !actor.poisoned)
            && panics == 0
            && mailbox_depth == 0
        {
            None
        } else {
            Some(format!(
                "responses={responses:?}, expected_bridge={expected_bridge:?}, \
                 observed_bridge={actual_bridge:?}, expected_actor_count={baseline_actors}, \
                 mailbox_depth={mailbox_depth}, actor_census=\n{}",
                actor_census(&state.runtime),
            ))
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_http_bridge_sequences_terminate_without_control_actor_growth(
            raw_actions in prop::collection::vec(
                (any::<u8>(), any::<u8>(), any::<u8>()),
                0..=32,
            ),
            concurrent in any::<bool>(),
            disappear_reply_observers in any::<bool>(),
        ) {
            let actions = raw_actions
                .into_iter()
                .map(|(kind, command_slot, value)| {
                    HttpAction::from_raw(kind, command_slot, value)
                })
                .collect::<Vec<_>>();
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine =
                Engine::new(parts, backend.clone()).expect("control HTTP stepping engine");
            let observations = Arc::new(Mutex::new(Vec::new()));
            let orchestrator = runtime
                .spawn(HttpBridgeProbe {
                    observations: Arc::clone(&observations),
                    disappear_reply_observers,
                })
                .expect("spawn control HTTP bridge probe");
            drive_steps(&backend, STEP_BUDGET);
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let state = ControlHttpState {
                runtime: runtime.clone(),
                engine: engine.handle(),
                orchestrator,
                namespace: None,
                upload_root: Arc::new(PathBuf::new()),
                live_env: Arc::new(|| Ok(Vec::new())),
            };
            let outcome = (|| {
                let mut responses = Vec::with_capacity(actions.len());
                if concurrent {
                    let pending = actions
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(index, action)| begin_http_action(&state, index, action))
                        .collect::<Vec<_>>();
                    drive_steps(&backend, STEP_BUDGET);
                    drive_steps(&backend, STEP_BUDGET);
                    for observation in pending {
                        responses.push(finish_http_action(observation)?);
                    }
                } else {
                    for (index, action) in actions.iter().cloned().enumerate() {
                        let pending = begin_http_action(&state, index, action);
                        drive_steps(&backend, STEP_BUDGET);
                        drive_steps(&backend, STEP_BUDGET);
                        responses.push(finish_http_action(pending)?);
                    }
                }
                responses.sort_by_key(|response| response.index);
                Ok::<_, String>(responses)
            })();
            prop_assert!(
                outcome.is_ok(),
                "control HTTP request did not terminate; actions={:?}; error={:?}; \
                 responses=[]; actor_census=\n{}",
                actions,
                outcome.as_ref().err(),
                actor_census(&runtime),
            );
            let responses = outcome.expect("outcome checked above");
            drive_steps(&backend, STEP_BUDGET);
            let observed = observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let failure = http_bridge_invariant_failure(
                &actions,
                &responses,
                &observed,
                &state,
                baseline_actors,
                disappear_reply_observers,
            );
            prop_assert!(
                failure.is_none(),
                "control HTTP/bridge invariant failed; actions={:?}; responses={:?}; failure={}",
                actions,
                responses,
                failure.unwrap_or_default(),
            );

            backend.advance_time(OFFER_SEARCH_REPLY_TIMEOUT);
            drive_steps(&backend, STEP_BUDGET);
            prop_assert_eq!(
                backend.pending_task_count(),
                baseline_tasks,
                "control reply timers survived their fixed drain budget; actions={:?}; \
                 responses={:?}; actor_census=\n{}",
                actions,
                responses,
                actor_census(&runtime),
            );
            runtime
                .stop_actor(orchestrator)
                .expect("stop control HTTP bridge probe");
            drive_steps(&backend, STEP_BUDGET);
            assert_no_poison(&runtime);
            assert_actor_delta_at_most(&runtime, 0, 0);
            assert_mailboxes_drained(&runtime);
        }

        #[test]
        fn generated_duplicate_control_replies_deliver_first_once_and_remove_observer(
            replies in prop::collection::vec(any::<u8>(), 0..=16)
        ) {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine =
                Engine::new(parts, backend.clone()).expect("control reply stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let baseline_tasks = backend.pending_task_count();
            let (response_tx, mut response_rx) = tokio::sync::oneshot::channel();
            let observer = runtime
                .spawn(ControlReplyObserver {
                    reply: Arc::new(Mutex::new(Some(response_tx))),
                    engine: engine.handle(),
                    sender: runtime.create_sender(),
                    timeout: Duration::from_millis(1),
                })
                .expect("spawn control reply observer");
            for code in &replies {
                runtime
                    .send_to(observer, reply(*code))
                    .expect("send generated control reply");
            }
            drive_steps(&backend, 32);
            if replies.is_empty() {
                backend.advance_time(Duration::from_millis(1));
                drive_steps(&backend, 32);
            }

            let observed = response_rx
                .try_recv()
                .expect("control reply observer produced a terminal reply");
            let expected = replies
                .first()
                .map(|code| reply(*code))
                .unwrap_or(ManualControlReply::TimedOut);
            prop_assert_eq!(
                observed,
                expected,
                "control reply observer accepted a stale duplicate; replies={:?}; \
                 actor_census=\n{}",
                replies,
                actor_census(&runtime),
            );

            backend.advance_time(Duration::from_millis(1));
            drive_steps(&backend, 32);
            prop_assert_eq!(backend.pending_task_count(), baseline_tasks);
            assert_no_poison(&runtime);
            assert_actor_delta_at_most(&runtime, baseline_actors, 0);
            assert_mailboxes_drained(&runtime);
        }
    }

    #[test]
    fn disappeared_orchestrator_removes_control_reply_actor() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("control failure stepping engine");
        let baseline_tasks = backend.pending_task_count();
        let state = ControlHttpState {
            runtime: runtime.clone(),
            engine: engine.handle(),
            orchestrator: ActorAddress::default(),
            namespace: None,
            upload_root: Arc::new(PathBuf::new()),
            live_env: Arc::new(|| Ok(Vec::new())),
        };
        let response = match begin_request_reply(&state, Duration::from_millis(1), |reply_to| {
            ManualControlMsg::Query { reply_to }
        }) {
            Ok(_) => panic!("missing orchestrator unexpectedly accepted a control query"),
            Err(response) => response,
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        drive_steps(&backend, STEP_BUDGET);
        backend.advance_time(Duration::from_millis(1));
        drive_steps(&backend, STEP_BUDGET);
        assert_eq!(backend.pending_task_count(), baseline_tasks);
        assert_no_poison(&runtime);
        assert_actor_delta_at_most(&runtime, 0, 0);
        assert_mailboxes_drained(&runtime);
    }

    #[test]
    fn reply_observer_disappearance_returns_a_bounded_terminal_http_response() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine =
            Engine::new(parts, backend.clone()).expect("reply disappearance stepping engine");
        let observations = Arc::new(Mutex::new(Vec::new()));
        let orchestrator = runtime
            .spawn(HttpBridgeProbe {
                observations,
                disappear_reply_observers: true,
            })
            .expect("spawn disappearing-reply bridge");
        drive_steps(&backend, STEP_BUDGET);
        let baseline_actors = runtime.stats().actors.len();
        let baseline_tasks = backend.pending_task_count();
        let state = ControlHttpState {
            runtime: runtime.clone(),
            engine: engine.handle(),
            orchestrator,
            namespace: None,
            upload_root: Arc::new(PathBuf::new()),
            live_env: Arc::new(|| Ok(Vec::new())),
        };
        let pending = begin_http_action(&state, 0, HttpAction::Status);
        drive_steps(&backend, STEP_BUDGET);
        drive_steps(&backend, STEP_BUDGET);
        let response = finish_http_action(pending).unwrap_or_else(|error| {
            panic!(
                "{error}; responses=[]; actor_census=\n{}",
                actor_census(&runtime),
            )
        });
        assert_eq!(
            response.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "reply observer disappearance returned {}; actor_census=\n{}",
            response.status,
            actor_census(&runtime),
        );
        drive_steps(&backend, STEP_BUDGET);
        assert_actor_delta_at_most(&runtime, baseline_actors, 0);
        backend.advance_time(CONTROL_REPLY_TIMEOUT);
        drive_steps(&backend, STEP_BUDGET);
        assert_eq!(backend.pending_task_count(), baseline_tasks);
        runtime
            .stop_actor(orchestrator)
            .expect("stop disappearing-reply bridge");
        drive_steps(&backend, STEP_BUDGET);
        assert_no_poison(&runtime);
        assert_actor_delta_at_most(&runtime, 0, 0);
        assert_mailboxes_drained(&runtime);
    }

    #[test]
    fn http_bridge_invariant_rejects_a_controlled_duplicate_forward() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine =
            Engine::new(parts, backend.clone()).expect("control invariant stepping engine");
        let state = ControlHttpState {
            runtime,
            engine: engine.handle(),
            orchestrator: ActorAddress::default(),
            namespace: None,
            upload_root: Arc::new(PathBuf::new()),
            live_env: Arc::new(|| Ok(Vec::new())),
        };
        let actions = vec![HttpAction::Status];
        let responses = vec![HttpObservation {
            index: 0,
            action: HttpAction::Status,
            status: StatusCode::OK,
        }];
        let duplicated = vec!["Status".to_owned(), "Status".to_owned()];

        assert!(
            http_bridge_invariant_failure(&actions, &responses, &duplicated, &state, 0, false,)
                .is_some(),
            "control HTTP/bridge invariant accepted a controlled duplicate forward"
        );
    }
    #[test]
    fn uploaded_python_receives_model_read_and_execution_scoped_result_access() {
        let spec = python_contextual_spec(
            "ui-scenario",
            "tiny_linear_inference.py",
            "/myelin/contextual/ui-scenario/program.py".to_owned(),
        );

        assert_eq!(
            spec.read_prefixes,
            vec!["/models", "/runs/ui-scenario/results"],
        );
        assert_eq!(spec.write_prefixes, vec!["/runs/ui-scenario/results"],);
        assert_eq!(spec.execution_id, "ui-scenario");
        assert_eq!(spec.command, "python3");
        assert!(spec.args.is_empty());
    }

    #[test]
    fn subscription_timeout_starts_only_after_request_is_enqueued() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        let observer = runtime
            .spawn(ContextualReplyObserver {
                reply: Arc::new(Mutex::new(Some(sender))),
                engine: engine.handle(),
                sender: runtime.create_sender(),
                timeout: Duration::from_millis(1),
                orchestrator: ActorAddress::default(),
                cancel_key: Some("withheld-registration".to_owned()),
            })
            .unwrap();
        drive_steps(&backend, STEP_BUDGET);
        backend.advance_time(Duration::from_secs(1));
        drive_steps(&backend, STEP_BUDGET);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        runtime
            .send_to(observer, ContextualReplyObserverMsg::ArmTimeout)
            .unwrap();
        drive_steps(&backend, STEP_BUDGET);
        backend.advance_time(Duration::from_millis(1));
        drive_steps(&backend, STEP_BUDGET);
        assert_eq!(receiver.try_recv().unwrap(), ContextualResponse::TimedOut);
    }
}
