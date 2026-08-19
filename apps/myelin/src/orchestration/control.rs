use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_vastai::VastClient;

use crate::orchestration::actor::OrchestratorMsg;
use crate::orchestration::manual_control::{
    KillRequest, ManualControlMsg, ManualControlReply, OfferSearchRequest,
    ProviderConfigurationRequest, ProvisionRequest,
};

const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_secs(2);
const OFFER_SEARCH_REPLY_MARGIN: Duration = Duration::from_secs(5);
const OFFER_SEARCH_REPLY_TIMEOUT: Duration =
    VastClient::REQUEST_TIMEOUT.saturating_add(OFFER_SEARCH_REPLY_MARGIN);
const CONTROL_REPLY_POLL: Duration = Duration::from_millis(5);
const PROVISION_PAGE: &str = include_str!("provision_page.html");
const FLEET_CONTROL_SCRIPT: &str = include_str!("fleet_control.js");
pub(crate) const FLEET_CONTROL_SCRIPT_URL: &str = "/assets/myelin-fleet-control.js";

#[derive(Clone)]
struct ControlHttpState {
    runtime: Runtime,
    orchestrator: ActorAddress,
}

pub(crate) fn plugin(runtime: Runtime, orchestrator: ActorAddress) -> dashboard::DashboardPlugin {
    let state = ControlHttpState {
        runtime,
        orchestrator,
    };
    let routes = Router::new()
        .route(FLEET_CONTROL_SCRIPT_URL, get(fleet_control_script))
        .route("/api/control/status", get(status))
        .route("/api/control/provision", post(provision))
        .route("/api/control/kill", post(kill))
        .route("/api/control/provider", post(configure_provider))
        .route("/api/control/offers", post(search_offers))
        .route("/api/control/nodes/{logical_node_id}/kill", post(kill_path))
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

async fn search_offers(
    State(state): State<ControlHttpState>,
    Json(request): Json<OfferSearchRequest>,
) -> Response {
    request_reply(&state, OFFER_SEARCH_REPLY_TIMEOUT, |reply_to| {
        ManualControlMsg::SearchOffers { request, reply_to }
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
    let inbox = match state.runtime.new_inbox::<ManualControlReply>() {
        Ok(inbox) => inbox,
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("create control reply inbox: {error}"),
                }),
            )
                .into_response();
        }
    };
    if let Err(error) = state.runtime.send_to(
        state.orchestrator,
        OrchestratorMsg::Manual(build(*inbox.addr())),
    ) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: format!("orchestrator control actor unavailable: {error}"),
            }),
        )
            .into_response();
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(reply) = inbox.try_recv() {
            return match reply {
                ManualControlReply::Rejected(error) => {
                    (StatusCode::CONFLICT, Json(ErrorResponse { error })).into_response()
                }
                reply => Json(reply).into_response(),
            };
        }
        if tokio::time::Instant::now() >= deadline {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(ErrorResponse {
                    error: "orchestrator control reply timed out".to_owned(),
                }),
            )
                .into_response();
        }
        tokio::time::sleep(CONTROL_REPLY_POLL).await;
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}
