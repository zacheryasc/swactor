use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::store::DashboardStore;
use crate::view::ViewRegistry;
use crate::{FrameEvent, PluginPage};

#[derive(Clone)]
pub(crate) struct AppState {
    pub frames: broadcast::Sender<FrameEvent>,
    pub store: Arc<DashboardStore>,
    pub views: Arc<ViewRegistry>,
    pub plugin_pages: Arc<Vec<PluginPage>>,
    pub page_script_urls: Arc<Vec<String>>,
    pub shutdown_notify: Arc<tokio::sync::Notify>,
}

pub(crate) async fn run_server(state: AppState, port: u16) {
    run_server_with_routes(state, port, Router::new()).await;
}

pub(crate) async fn run_server_with_routes(state: AppState, port: u16, extra: Router) {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind HTTP server");
    let shutdown = Arc::clone(&state.shutdown_notify);
    axum::serve(listener, router(state).merge(extra))
        .with_graceful_shutdown(async move { shutdown.notified().await })
        .await
        .expect("HTTP server error");
}

fn router(state: AppState) -> Router {
    let mut router = Router::new()
        .route("/", get(root_page))
        .route("/events", get(frame_stream))
        .route("/api/frames", get(recent_frames))
        .route("/api/views", get(views_json))
        .route("/api/view/{*path}", get(view_snapshot))
        .route("/view/{*path}", get(view_page));
    for page in state.plugin_pages.iter().cloned() {
        router = router.route(
            page.path,
            get(move |State(state): State<AppState>| {
                let page = page.clone();
                async move { Html(inject_nav(page.html, page.id, &state)) }
            }),
        );
    }
    #[cfg(feature = "demo-control")]
    let router = router
        .route("/control/kill", axum::routing::post(control_kill))
        .route("/control/provision", axum::routing::post(control_provision))
        .route("/control/remove", axum::routing::post(control_remove))
        .route("/control/edge", axum::routing::post(control_edge));
    router.with_state(state)
}

#[cfg(feature = "demo-control")]
async fn control_kill(Json(command): Json<crate::control::ControlCommand>) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Kill { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_provision(
    Json(command): Json<crate::control::ControlCommand>,
) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Provision { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_remove(Json(command): Json<crate::control::ControlCommand>) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Remove { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_edge(Json(command): Json<crate::control::ControlCommand>) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::EstablishEdge { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

async fn root_page(State(state): State<AppState>) -> Response {
    // Home is the fleet control-plane view.
    match state.views.html("fleet") {
        Some(html) => Html(inject_nav(html, "fleet", &state)).into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "fleet control-plane view missing",
        )
            .into_response(),
    }
}

/// Placeholder marker pages include to receive the shared navbar.
const NAV_PLACEHOLDER: &str = "<!--swactor:nav-->";

/// Build the unified top navbar from the view registry (registration order,
/// active by served path). Fleet links to `/` — it is the home page.
fn inject_nav(html: &str, active_path: &str, state: &AppState) -> String {
    let mut links = Vec::new();
    for view in state.views.descriptors() {
        if !view.show_in_nav {
            continue;
        }
        let href = if view.path == "fleet" {
            "/".to_owned()
        } else {
            view.page.clone()
        };
        let active = view.path == active_path;
        links.push(format!(
            r#"<a href="{}"{}>{}</a>"#,
            escape_html(&href),
            if active {
                r#" aria-current="page" data-active="true""#
            } else {
                ""
            },
            escape_html(view.title),
        ));
    }
    for page in state.plugin_pages.iter() {
        links.push(format!(
            r#"<a href="{}"{}>{}</a>"#,
            escape_html(page.path),
            if page.id == active_path {
                r#" aria-current="page" data-active="true""#
            } else {
                ""
            },
            escape_html(page.title),
        ));
    }
    let nav = format!(
        concat!(
            r#"<script>try{{var t=localStorage.getItem('swactor-theme');document.documentElement.dataset.theme=t||(matchMedia('(prefers-color-scheme: light)').matches?'light':'dark')}}catch(e){{}}</script>"#,
            r#"<style>"#,
            r#".sw-nav{{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin:0 0 16px;padding:8px;"#,
            r#"background:#001220;border:1px solid #14406a;border-radius:2px;}}"#,
            r#".sw-nav a{{padding:6px 10px;color:#c7d3de;border:1px solid transparent;border-bottom:2px solid transparent;"#,
            r#"border-radius:2px;text-decoration:none;font:600 13px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;"#,
            r#"transition:color .12s,background .12s,border-color .12s;}}"#,
            r#".sw-nav a:hover{{color:#ffffff;background:#001c38;}}"#,
            r#".sw-nav a:focus-visible{{outline:2px solid #5cd5ff;outline-offset:2px;}}"#,
            r#".sw-nav a[data-active="true"]{{color:#ff9900;border-bottom-color:#ff9900;}}"#,
            r#".sw-nav .sw-theme{{margin-left:auto;cursor:pointer;background:transparent;border:1px solid #14406a;"#,
            r#"color:#c7d3de;border-radius:2px;font:600 13px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:6px 10px;}}"#,
            r#".sw-nav .sw-theme:hover{{color:#ffffff;background:#001c38;border-color:#5cd5ff;}}"#,
            r#":root[data-theme="light"] .sw-nav{{background:#ffffff;border-color:#dcdce0;}}"#,
            r#":root[data-theme="light"] .sw-nav a{{color:#696c77;}}"#,
            r#":root[data-theme="light"] .sw-nav a:hover{{color:#383a42;background:#f0f0f0;}}"#,
            r#":root[data-theme="light"] .sw-nav a[data-active="true"]{{color:#986801;border-bottom-color:#986801;}}"#,
            r#":root[data-theme="light"] .sw-nav .sw-theme{{border-color:#dcdce0;color:#696c77;}}"#,
            r#":root[data-theme="light"] .sw-nav .sw-theme:hover{{color:#383a42;background:#f0f0f0;border-color:#0184bc;}}"#,
            r#"</style>"#,
            r#"<nav class="sw-nav" aria-label="Dashboard views">{}"#,
            r#"<button class="sw-theme" aria-label="Toggle light or dark theme" title="Toggle light or dark theme""#,
            r#" onclick="var d=document.documentElement;d.dataset.theme=d.dataset.theme==='light'?'dark':'light';try{{localStorage.setItem('swactor-theme',d.dataset.theme)}}catch(e){{}}">◐</button></nav>"#,
        ),
        links.join("")
    );
    let rendered = html.replace(NAV_PLACEHOLDER, &nav);
    if state.page_script_urls.is_empty() {
        return rendered;
    }
    let scripts = state
        .page_script_urls
        .iter()
        .map(|url| format!(r#"<script src="{}"></script>"#, escape_html(url)))
        .collect::<String>();
    rendered.replacen("</body>", &format!("{scripts}</body>"), 1)
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn recent_frames(State(state): State<AppState>) -> Json<Vec<FrameEvent>> {
    Json(state.store.recent_frames())
}

async fn views_json(State(state): State<AppState>) -> Json<serde_json::Value> {
    let pages = state
        .plugin_pages
        .iter()
        .map(|page| {
            serde_json::json!({
                "id": page.id,
                "title": page.title,
                "path": page.path,
            })
        })
        .collect::<Vec<_>>();
    Json(serde_json::json!({
        "views": state.views.descriptors(),
        "plugin_pages": pages,
    }))
}

async fn view_snapshot(
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    State(state): State<AppState>,
) -> Response {
    // `/api/view/<path>/detail?<query>` serves bounded per-entity detail.
    if let Some(base) = path.strip_suffix("/detail") {
        let query = query.unwrap_or_default();
        return match state.views.detail(base, &query) {
            Some(snapshot) => Json(snapshot).into_response(),
            None => (StatusCode::NOT_FOUND, "unknown dashboard detail").into_response(),
        };
    }
    match state.views.snapshot(&path) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown dashboard view").into_response(),
    }
}

async fn view_page(Path(path): Path<String>, State(state): State<AppState>) -> Response {
    match state.views.html(&path) {
        Some(html) => Html(inject_nav(html, &path, &state)).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown dashboard view").into_response(),
    }
}

async fn frame_stream(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.frames.subscribe();
    let (tx, out) = mpsc::channel::<Event>(32);

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(frame) => {
                    let Ok(json) = serde_json::to_string(&frame) else {
                        continue;
                    };
                    if tx
                        .send(Event::default().event("frame").data(json))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let json = serde_json::json!({ "skipped": skipped }).to_string();
                    if tx
                        .send(Event::default().event("lagged").data(json))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    Sse::new(ReceiverStream::new(out).map(Ok)).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "demo-control")]
    use std::sync::{LazyLock, Mutex};
    #[cfg(feature = "demo-control")]
    use std::time::Duration;

    use super::*;
    #[cfg(feature = "demo-control")]
    use axum::body::Body;
    #[cfg(feature = "demo-control")]
    use axum::http::Request;
    #[cfg(feature = "demo-control")]
    use proptest::prelude::*;
    #[cfg(feature = "demo-control")]
    use tower::util::ServiceExt;

    fn state_with_plugin(page: PluginPage) -> AppState {
        let views = Arc::new(ViewRegistry::new());
        let store = Arc::new(DashboardStore::new(1, Arc::clone(&views)));
        let (frames, _) = broadcast::channel(1);
        AppState {
            frames,
            store,
            views,
            plugin_pages: Arc::new(vec![page]),
            page_script_urls: Arc::new(Vec::new()),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[test]
    fn registered_plugin_page_appears_in_shared_navigation() {
        let state = state_with_plugin(PluginPage::new(
            "provision",
            "Provision",
            "/provision",
            "<!--swactor:nav-->",
        ));
        let rendered = inject_nav("<!--swactor:nav-->", "provision", &state);
        assert!(rendered.contains(r#"<a href="/provision""#));
        assert!(rendered.contains(r#"aria-current="page" data-active="true">Provision</a>"#));
    }

    #[cfg(feature = "demo-control")]
    const HTTP_RESPONSE_BUDGET: Duration = Duration::from_secs(1);

    #[cfg(feature = "demo-control")]
    static TEST_CONTROL_RECEIVER: LazyLock<
        Mutex<std::sync::mpsc::Receiver<crate::control::ControlCommand>>,
    > = LazyLock::new(|| {
        let (sender, receiver) = std::sync::mpsc::channel();
        crate::control::set_control_sender(sender);
        Mutex::new(receiver)
    });

    #[cfg(feature = "demo-control")]
    fn test_control_receiver()
    -> &'static Mutex<std::sync::mpsc::Receiver<crate::control::ControlCommand>> {
        &TEST_CONTROL_RECEIVER
    }

    #[cfg(feature = "demo-control")]
    #[derive(Clone, Debug)]
    struct HttpAction {
        route: u8,
        payload: u8,
        value: u8,
    }

    #[cfg(feature = "demo-control")]
    impl HttpAction {
        fn uri(&self) -> &'static str {
            match self.route % 4 {
                0 => "/control/kill",
                1 => "/control/provision",
                2 => "/control/remove",
                _ => "/control/edge",
            }
        }

        fn command_json(&self, route: u8) -> String {
            let command_id = format!("repeated-{}", self.value % 4);
            match route % 4 {
                0 => serde_json::json!({
                    "Kill": {
                        "command_id": command_id,
                        "node": format!("node-{}", self.value),
                    }
                })
                .to_string(),
                1 => serde_json::json!({
                    "Provision": {
                        "command_id": command_id,
                        "count": self.value,
                    }
                })
                .to_string(),
                2 => serde_json::json!({
                    "Remove": {
                        "command_id": command_id,
                        "count": self.value,
                    }
                })
                .to_string(),
                _ => serde_json::json!({
                    "EstablishEdge": {
                        "command_id": command_id,
                        "node": format!("node-{}", self.value),
                    }
                })
                .to_string(),
            }
        }

        fn body(&self) -> String {
            match self.payload % 5 {
                0 => self.command_json(self.route),
                1 => match self.value % 6 {
                    0 => String::new(),
                    1 => "{".to_owned(),
                    2 => "[".to_owned(),
                    3 => "{\"".to_owned(),
                    4 => "{\"command_id\":".to_owned(),
                    _ => "not-json".to_owned(),
                },
                2 => {
                    let command_id = format!("repeated-{}", self.value % 4);
                    match self.route % 4 {
                        0 => serde_json::json!({
                            "Kill": {"command_id": command_id}
                        })
                        .to_string(),
                        1 => serde_json::json!({
                            "Provision": {"count": self.value}
                        })
                        .to_string(),
                        2 => serde_json::json!({
                            "Remove": {"command_id": command_id}
                        })
                        .to_string(),
                        _ => serde_json::json!({
                            "EstablishEdge": {"node": format!("node-{}", self.value)}
                        })
                        .to_string(),
                    }
                }
                3 => self.command_json(self.route.wrapping_add(1)),
                _ => match self.route % 4 {
                    0 => r#"{"Kill":{"command_id":7,"node":[]}}"#.to_owned(),
                    1 => r#"{"Provision":{"command_id":7,"count":"one"}}"#.to_owned(),
                    2 => r#"{"Remove":{"command_id":[],"count":-1}}"#.to_owned(),
                    _ => r#"{"EstablishEdge":{"command_id":false,"node":7}}"#.to_owned(),
                },
            }
        }

        fn is_valid_for_route(&self) -> bool {
            self.payload % 5 == 0
        }
    }

    #[cfg(feature = "demo-control")]
    #[derive(Clone, Debug)]
    struct HttpObservation {
        index: usize,
        uri: &'static str,
        body: String,
        expected_valid: bool,
        status: StatusCode,
    }

    #[cfg(feature = "demo-control")]
    async fn send_control_request(
        app: Router,
        index: usize,
        action: HttpAction,
    ) -> Result<HttpObservation, String> {
        let uri = action.uri();
        let body = action.body();
        let expected_valid = action.is_valid_for_route();
        let request = Request::post(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.clone()))
            .map_err(|error| format!("build dashboard request: {error}"))?;
        let response = tokio::time::timeout(HTTP_RESPONSE_BUDGET, app.oneshot(request))
            .await
            .map_err(|_| format!("request {index} {uri} exceeded {HTTP_RESPONSE_BUDGET:?}"))?
            .map_err(|error| format!("route dashboard request: {error}"))?;
        Ok(HttpObservation {
            index,
            uri,
            body,
            expected_valid,
            status: response.status(),
        })
    }

    #[cfg(feature = "demo-control")]
    fn http_invariant_failure(
        actions: &[HttpAction],
        responses: &[HttpObservation],
        forwarded: usize,
    ) -> Option<String> {
        let expected_forwarded = actions
            .iter()
            .filter(|action| action.is_valid_for_route())
            .count();
        let terminal = responses.len() == actions.len();
        let response_log = responses
            .iter()
            .map(|response| {
                format!(
                    "#{} {} body={:?} -> {}",
                    response.index, response.uri, response.body, response.status,
                )
            })
            .collect::<Vec<_>>();
        let statuses_valid = responses.iter().all(|response| {
            !response.status.is_server_error()
                && if response.expected_valid {
                    response.status == StatusCode::ACCEPTED
                } else {
                    response.status.is_client_error()
                }
        });
        if terminal && statuses_valid && forwarded == expected_forwarded {
            None
        } else {
            Some(format!(
                "terminal={terminal}, expected_forwarded={expected_forwarded}, \
                 forwarded={forwarded}, responses={response_log:?}, \
                 actor_census=dashboard HTTP routes own no actors"
            ))
        }
    }

    #[cfg(feature = "demo-control")]
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_control_http_sequences_are_bounded_and_typed(
            raw_actions in prop::collection::vec(
                (any::<u8>(), any::<u8>(), any::<u8>()),
                0..=32,
            ),
            concurrent in any::<bool>(),
        ) {
            let actions = raw_actions
                .into_iter()
                .map(|(route, payload, value)| HttpAction {
                    route,
                    payload,
                    value,
                })
                .collect::<Vec<_>>();
            let receiver = test_control_receiver();
            while receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .try_recv()
                .is_ok()
            {}
            let state = state_with_plugin(PluginPage::new(
                "control-test",
                "Control test",
                "/control-test",
                "",
            ));
            let current_thread = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread dashboard HTTP runtime");
            let outcome = current_thread.block_on(async {
                let mut responses = Vec::with_capacity(actions.len());
                if concurrent {
                    let mut requests = tokio::task::JoinSet::new();
                    for (index, action) in actions.iter().cloned().enumerate() {
                        requests.spawn(send_control_request(router(state.clone()), index, action));
                    }
                    while let Some(result) = requests.join_next().await {
                        responses.push(
                            result
                                .map_err(|error| format!("dashboard request task failed: {error}"))??,
                        );
                    }
                } else {
                    for (index, action) in actions.iter().cloned().enumerate() {
                        responses.push(
                            send_control_request(router(state.clone()), index, action).await?,
                        );
                    }
                }
                responses.sort_by_key(|response| response.index);
                Ok::<_, String>(responses)
            });
            prop_assert!(
                outcome.is_ok(),
                "dashboard HTTP request did not terminate; actions={:?}; error={:?}; \
                 responses=[]; actor_census=dashboard HTTP routes own no actors",
                actions,
                outcome.as_ref().err(),
            );
            let responses = outcome.expect("outcome checked above");
            let forwarded = {
                let receiver = receiver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                receiver.try_iter().count()
            };
            let failure = http_invariant_failure(&actions, &responses, forwarded);
            prop_assert!(
                failure.is_none(),
                "dashboard HTTP invariant failed; actions={:?}; responses={:?}; failure={}",
                actions,
                responses,
                failure.unwrap_or_default(),
            );
        }
    }

    #[cfg(feature = "demo-control")]
    #[test]
    fn control_http_invariant_rejects_a_controlled_server_error() {
        let actions = vec![HttpAction {
            route: 0,
            payload: 1,
            value: 0,
        }];
        let responses = vec![HttpObservation {
            index: 0,
            uri: actions[0].uri(),
            body: actions[0].body(),
            expected_valid: false,
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }];
        assert!(
            http_invariant_failure(&actions, &responses, 0).is_some(),
            "HTTP invariant accepted a controlled 5xx response for invalid JSON"
        );
    }
}
