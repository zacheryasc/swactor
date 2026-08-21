use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_lite::future;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::{BlockingWorkSender, EngineHandle};
use swactor_job_runner::{Job, JobDone, JobState, OrchestratorJobMsg};
use swactor_vastai::VastClient;

use distribution::transport_bridge::{OutboxRouteBinder, RouteBinder, RouteView};
use iroh::EndpointAddr;
use swactor_transport::NodeId;

use crate::orchestration::actor::OrchestratorMsg;
use crate::orchestration::manual_control::{
    KillRequest, ManualControlMsg, ManualControlReply, NodePhase, OfferSearchRequest,
    ProviderConfigurationRequest, ProvisionRequest,
};

const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_secs(2);
const OFFER_SEARCH_REPLY_MARGIN: Duration = Duration::from_secs(5);
const OFFER_SEARCH_REPLY_TIMEOUT: Duration =
    VastClient::REQUEST_TIMEOUT.saturating_add(OFFER_SEARCH_REPLY_MARGIN);
const MAX_JOB_FILE_BYTES: usize = 64 * 1024;

#[derive(Deserialize)]
struct UploadedJobFile {
    job: Job,
}

fn parse_uploaded_job_file(body: &str) -> Result<Job, String> {
    if body.len() > MAX_JOB_FILE_BYTES {
        return Err(format!(
            "job file exceeds the {MAX_JOB_FILE_BYTES}-byte limit"
        ));
    }
    let file = toml::from_str::<UploadedJobFile>(body)
        .map_err(|error| format!("invalid job TOML: {error}"))?;
    if file.job.name.trim().is_empty() {
        return Err("job.name must not be empty".to_owned());
    }
    if file.job.run.trim().is_empty() {
        return Err("job.run must not be empty".to_owned());
    }
    Ok(file.job)
}

#[derive(Clone)]
struct FleetJobRoutes {
    route_view: RouteView,
    pinned_routes: RouteView,
    route_binder: Arc<OutboxRouteBinder>,
}

#[derive(Clone)]
pub(crate) struct FleetJobController {
    runtime: Runtime,
    engine: Option<EngineHandle>,
    actor: ActorAddress,
    done: Arc<swactor::runtime::Inbox<JobDone>>,
    busy: Arc<AtomicBool>,
    routes: Option<FleetJobRoutes>,
}

impl FleetJobController {
    pub(crate) fn new(
        runtime: Runtime,
        engine: EngineHandle,
        actor: ActorAddress,
        done: swactor::runtime::Inbox<JobDone>,
        route_view: RouteView,
        pinned_routes: RouteView,
        route_binder: Arc<OutboxRouteBinder>,
    ) -> Self {
        Self {
            runtime,
            engine: Some(engine),
            actor,
            done: Arc::new(done),
            busy: Arc::new(AtomicBool::new(false)),
            routes: Some(FleetJobRoutes {
                route_view,
                pinned_routes,
                route_binder,
            }),
        }
    }

    #[cfg(test)]
    fn inert() -> Self {
        let parts = swactor::runtime::RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let done = runtime.new_inbox::<JobDone>().expect("inert job inbox");
        Self {
            runtime,
            engine: None,
            actor: ActorAddress::new_random(),
            done: Arc::new(done),
            busy: Arc::new(AtomicBool::new(false)),
            routes: None,
        }
    }

    fn run(
        &self,
        node_actor: ActorAddress,
        node_endpoint: &EndpointAddr,
        job: Job,
    ) -> Result<JobDone, String> {
        if self.busy.swap(true, Ordering::AcqRel) {
            return Err("another uploaded job is still active".to_owned());
        }
        struct BusyReset(Arc<AtomicBool>);
        impl Drop for BusyReset {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _busy = BusyReset(self.busy.clone());
        while self.done.try_recv().is_some() {}

        let routes = self
            .routes
            .as_ref()
            .ok_or_else(|| "job controller routes are unavailable".to_owned())?;
        let node = NodeId(*node_endpoint.id.as_bytes());
        routes
            .pinned_routes
            .write()
            .map_err(|_| "job pinned-route registry is poisoned".to_owned())?
            .insert(node_actor, node);
        routes
            .route_view
            .write()
            .map_err(|_| "job route view is poisoned".to_owned())?
            .insert(node_actor, node);
        routes.route_binder.ensure_routable(node_actor);

        self.runtime
            .send_to(self.actor, OrchestratorJobMsg::Submit { job, node_actor })
            .map_err(|error| format!("submit uploaded job: {error}"))?;

        let deadline = self
            .runtime
            .new_inbox::<()>()
            .map_err(|error| format!("job deadline inbox: {error}"))?;
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| "job controller is unavailable".to_owned())?;
        engine.send_after(
            Duration::from_secs(30),
            self.runtime.create_sender(),
            *deadline.addr(),
            (),
        );
        future::block_on(future::race(async { Ok(self.done.recv().await) }, async {
            deadline.recv().await;
            Err("uploaded job did not complete within 30 seconds".to_owned())
        }))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum FleetJobState {
    Idle,
    Started,
    Running,
    Completed,
    Failed,
}

impl FleetJobState {
    fn is_active(&self) -> bool {
        matches!(self, Self::Started | Self::Running)
    }
}

#[derive(Clone, Debug, Serialize)]
struct FleetJobStatus {
    state: FleetJobState,
    message: String,
}

impl FleetJobStatus {
    fn idle() -> Self {
        Self {
            state: FleetJobState::Idle,
            message: "No job submitted".to_owned(),
        }
    }

    fn started() -> Self {
        Self {
            state: FleetJobState::Started,
            message: "Job started".to_owned(),
        }
    }

    fn running() -> Self {
        Self {
            state: FleetJobState::Running,
            message: "Job running".to_owned(),
        }
    }

    fn completed() -> Self {
        Self {
            state: FleetJobState::Completed,
            message: "Job completed".to_owned(),
        }
    }

    fn failed(error: impl Into<String>) -> Self {
        Self {
            state: FleetJobState::Failed,
            message: format!("Job failed: {}", error.into()),
        }
    }
}

struct FleetJobRecord {
    generation: u64,
    status: FleetJobStatus,
}

#[derive(Clone)]
struct FleetJobManager {
    jobs: Arc<Mutex<BTreeMap<u64, FleetJobRecord>>>,
    next_generation: Arc<AtomicU64>,
    controller: FleetJobController,
}

impl FleetJobManager {
    fn new(controller: FleetJobController) -> Self {
        Self {
            jobs: Arc::new(Mutex::new(BTreeMap::new())),
            next_generation: Arc::new(AtomicU64::new(0)),
            controller,
        }
    }

    fn status(&self, node_id: u64) -> FleetJobStatus {
        self.jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&node_id)
            .map(|record| record.status.clone())
            .unwrap_or_else(FleetJobStatus::idle)
    }

    fn submit(
        &self,
        blocking: &BlockingWorkSender,
        node_id: u64,
        job_actor: ActorAddress,
        endpoint: EndpointAddr,
        job: Job,
    ) -> Result<FleetJobStatus, String> {
        let generation = self.next_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let started = FleetJobStatus::started();
        {
            let mut jobs = self
                .jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if jobs.values().any(|record| record.status.state.is_active()) {
                return Err("another uploaded job is already active".to_owned());
            }
            jobs.insert(
                node_id,
                FleetJobRecord {
                    generation,
                    status: started.clone(),
                },
            );
        }

        let manager = self.clone();
        let controller = self.controller.clone();
        let work = Box::new(move || {
            manager.mark_running(node_id, generation);
            let status = match controller.run(job_actor, &endpoint, job) {
                Ok(done) if done.state == JobState::Completed => FleetJobStatus::completed(),
                Ok(done) => FleetJobStatus::failed(format!(
                    "remote state {:?}, exit code {:?}",
                    done.state, done.exit_code
                )),
                Err(error) => FleetJobStatus::failed(error),
            };
            manager.complete_generation(node_id, generation, status);
        });
        if blocking.submit(work).is_err() {
            let error = "job execution backend is unavailable".to_owned();
            self.fail_generation(node_id, generation, error.clone());
            return Err(error);
        }
        Ok(started)
    }

    fn mark_running(&self, node_id: u64, generation: u64) {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = jobs.get_mut(&node_id)
            && record.generation == generation
            && matches!(record.status.state, FleetJobState::Started)
        {
            record.status = FleetJobStatus::running();
        }
    }

    fn complete_generation(&self, node_id: u64, generation: u64, status: FleetJobStatus) {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(record) = jobs.get_mut(&node_id)
            && record.generation == generation
            && record.status.state.is_active()
        {
            record.status = status;
        }
    }

    fn fail_running(&self, node_id: u64, error: impl Into<String>) {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = jobs.get_mut(&node_id) else {
            return;
        };
        if record.status.state.is_active() {
            record.status = FleetJobStatus::failed(error);
        }
    }

    fn fail_generation(&self, node_id: u64, generation: u64, error: String) {
        let mut jobs = self
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(record) = jobs.get_mut(&node_id) else {
            return;
        };
        if record.generation == generation {
            record.status = FleetJobStatus::failed(error);
        }
    }
}

#[cfg(test)]
impl Default for FleetJobManager {
    fn default() -> Self {
        Self::new(FleetJobController::inert())
    }
}
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
const PROVISION_PAGE: &str = include_str!("provision_page.html");
const FLEET_CONTROL_SCRIPT: &str = include_str!("fleet_control.js");
pub(crate) const FLEET_CONTROL_SCRIPT_URL: &str = "/assets/myelin-fleet-control.js";

#[derive(Clone)]
struct ControlHttpState {
    runtime: Runtime,
    engine: EngineHandle,
    orchestrator: ActorAddress,
    blocking: BlockingWorkSender,
    jobs: FleetJobManager,
}

pub(crate) fn plugin(
    runtime: Runtime,
    engine: EngineHandle,
    orchestrator: ActorAddress,
    job_controller: FleetJobController,
) -> dashboard::DashboardPlugin {
    let blocking = engine.blocking_work_sender();
    let state = ControlHttpState {
        runtime,
        engine,
        orchestrator,
        blocking,
        jobs: FleetJobManager::new(job_controller),
    };
    let routes = Router::new()
        .route(FLEET_CONTROL_SCRIPT_URL, get(fleet_control_script))
        .route("/api/control/status", get(status))
        .route("/api/control/actors", get(actor_stats))
        .route("/api/control/provision", post(provision))
        .route("/api/control/kill", post(kill))
        .route("/api/control/provider", post(configure_provider))
        .route("/api/control/offers", post(search_offers))
        .route("/api/control/flush", post(flush))
        .route("/api/control/nodes/{logical_node_id}/kill", post(kill_path))
        .route(
            "/api/control/nodes/{logical_node_id}/job",
            get(node_job_status).post(submit_node_job),
        )
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

async fn provision(
    State(state): State<ControlHttpState>,
    Json(request): Json<ProvisionRequest>,
) -> Response {
    route_mutation(
        &state,
        ManualControlMsg::Provision {
            request,
            reply_to: None,
        },
    )
}

async fn kill(State(state): State<ControlHttpState>, Json(request): Json<KillRequest>) -> Response {
    let logical_node_id = request.logical_node_id;
    let response = route_mutation(
        &state,
        ManualControlMsg::Kill {
            request,
            reply_to: None,
        },
    );
    if response.status().is_success() {
        state.jobs.fail_running(
            logical_node_id,
            "managed node was killed while the GPU job was running",
        );
    }
    response
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
    let response = route_mutation(
        &state,
        ManualControlMsg::Kill {
            request: KillRequest {
                command_id: request.command_id,
                logical_node_id,
            },
            reply_to: None,
        },
    );
    if response.status().is_success() {
        state.jobs.fail_running(
            logical_node_id,
            "managed node was killed while the job was running",
        );
    }
    response
}

async fn node_job_status(
    Path(logical_node_id): Path<u64>,
    State(state): State<ControlHttpState>,
) -> Response {
    Json(state.jobs.status(logical_node_id)).into_response()
}

async fn submit_node_job(
    Path(logical_node_id): Path<u64>,
    State(state): State<ControlHttpState>,
    body: String,
) -> Response {
    let job = match parse_uploaded_job_file(&body) {
        Ok(job) => job,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error })).into_response();
        }
    };
    let response_rx = match begin_request_reply(&state, CONTROL_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::Query { reply_to }
    }) {
        Ok(response_rx) => response_rx,
        Err(response) => return response,
    };
    let model = match response_rx.await {
        Ok(ManualControlReply::Status(model)) => model,
        Ok(ManualControlReply::Rejected(error)) => {
            return (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response();
        }
        Ok(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "manual control returned an unexpected job-selection reply".to_owned(),
                }),
            )
                .into_response();
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("control reply observer stopped: {error}"),
                }),
            )
                .into_response();
        }
    };
    let Some(node) = model
        .nodes
        .into_iter()
        .find(|node| node.logical_node_id == logical_node_id)
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("managed node {logical_node_id} does not exist"),
            }),
        )
            .into_response();
    };
    if node.phase != NodePhase::Running {
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("managed node {logical_node_id} is not running"),
            }),
        )
            .into_response();
    }
    let Some(runtime) = node.runtime else {
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("managed node {logical_node_id} has no runtime identity"),
            }),
        )
            .into_response();
    };
    let Some(job_actor) = runtime.job_actor else {
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("managed node {logical_node_id} has no job runner"),
            }),
        )
            .into_response();
    };
    let endpoint = match serde_json::from_str::<EndpointAddr>(&runtime.endpoint) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            return (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: format!("managed node {logical_node_id} endpoint is invalid: {error}"),
                }),
            )
                .into_response();
        }
    };
    match state
        .jobs
        .submit(&state.blocking, logical_node_id, job_actor, endpoint, job)
    {
        Ok(status) => (StatusCode::ACCEPTED, Json(status)).into_response(),
        Err(error) => (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response(),
    }
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
        Err(response) => return response,
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

fn begin_request_reply(
    state: &ControlHttpState,
    timeout: Duration,
    build: impl FnOnce(ActorAddress) -> ManualControlMsg,
) -> Result<tokio::sync::oneshot::Receiver<ManualControlReply>, Response> {
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
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("create control reply observer: {error}"),
                }),
            )
                .into_response()
        })?;
    if let Err(error) = state
        .runtime
        .send_to(state.orchestrator, OrchestratorMsg::Manual(build(reply_to)))
    {
        let _ = state.runtime.stop_actor(reply_to);
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("orchestrator control actor unavailable: {error}"),
            }),
        )
            .into_response());
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

    fn running_job(generation: u64) -> FleetJobRecord {
        FleetJobRecord {
            generation,
            status: FleetJobStatus::running(),
        }
    }

    #[test]
    fn worker_kill_fails_job_and_late_completion_cannot_overwrite_failure() {
        let manager = FleetJobManager::default();
        manager
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(7, running_job(1));

        manager.fail_running(7, "worker stopped");
        let record = manager
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(matches!(record[&7].status.state, FleetJobState::Failed));
        drop(record);

        manager.complete_generation(7, 1, FleetJobStatus::completed());
        assert!(matches!(manager.status(7).state, FleetJobState::Failed));

        manager
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(7, running_job(2));
        manager.complete_generation(7, 1, FleetJobStatus::failed("stale generation"));
        assert!(matches!(manager.status(7).state, FleetJobState::Running));
        manager.complete_generation(7, 2, FleetJobStatus::completed());
        assert!(matches!(manager.status(7).state, FleetJobState::Completed));
    }

    #[test]
    fn uploaded_job_file_parses_existing_toml_contract() {
        let job = parse_uploaded_job_file(include_str!("../../jobs/ui_smoke.toml")).unwrap();
        assert_eq!(job.name, "ui_smoke");
        assert!(job.run.contains("myelin ui job completed"));
    }

    #[test]
    fn uploaded_job_file_rejects_malformed_or_empty_jobs() {
        assert!(parse_uploaded_job_file("not toml = [").is_err());
        assert!(parse_uploaded_job_file("[job]\nname = \"\"\nrun = \"echo ok\"").is_err());
        assert!(parse_uploaded_job_file("[job]\nname = \"empty\"\nrun = \"\"").is_err());
        assert!(parse_uploaded_job_file(&"x".repeat(MAX_JOB_FILE_BYTES + 1)).is_err());
    }

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
                    self.finish_reply(ctx, reply_to, ManualControlReply::Offers(Vec::new()));
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
        let reply = |result: Result<_, Response>| match result {
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
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
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
                blocking: engine.handle().blocking_work_sender(),
                jobs: FleetJobManager::default(),
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
            let mut config = RuntimeConfig::default();
            config.worker_count = 1;
            let parts = RuntimeParts::new(config);
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
            blocking: engine.handle().blocking_work_sender(),
            jobs: FleetJobManager::default(),
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
        let mut config = RuntimeConfig::default();
        config.worker_count = 1;
        let parts = RuntimeParts::new(config);
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
            blocking: engine.handle().blocking_work_sender(),
            jobs: FleetJobManager::default(),
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
            blocking: engine.handle().blocking_work_sender(),
            jobs: FleetJobManager::default(),
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
}
