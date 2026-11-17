use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use data_plane::bootstrap::channel::{GuestAttachment, GuestBootstrap};
use data_plane::bootstrap::host_route::BootstrapHostRoute;
use data_plane::data_plane::DataPlaneBootstrap;
use data_plane::protocol::register_data_plane_codecs;
use distribution::node::DistributedNodeConfig;
use distribution::transport_bridge::{
    Outbox, OutboxRouteBinder, RelayMirror, RouteBinder, RouteView, RouteViewTransport,
};
use iroh::{EndpointAddr, RelayMode};
use iroh_driver::{IrohDriver, IrohDriverConfig};
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_engine::{ActorCompletion, Engine, TaskSpawner};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};

use super::*;

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);

struct Routing(Arc<IrohDriver>);

impl Drop for Routing {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

fn child_routing(
    runtime: &Runtime,
    engine: &Engine,
    tasks: TaskSpawner,
    host: ActorAddress,
    endpoint: EndpointAddr,
) -> Result<Routing> {
    let mut codecs = CodecRegistry::new();
    register_data_plane_codecs(&mut codecs);
    let codecs = Arc::new(codecs);
    let router = Arc::new(TransportRouter::new());
    runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
        codecs.clone(),
        router.clone(),
    )));
    let mut driver = IrohDriver::with_engine(
        tasks,
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: if endpoint.relay_urls().next().is_some() {
                RelayMode::Custom(endpoint.relay_urls().cloned().collect())
            } else {
                RelayMode::Disabled
            },
            bind_port: None,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: Vec::new(),
        },
    )
    .map_err(|error| NativeError::protocol(format!("start native actor transport: {error}")))?;
    let node = swactor_transport::NodeId(*endpoint.id.as_bytes());
    let route_view: RouteView = Arc::new(RwLock::new(HashMap::from([(host, node)])));
    let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
    if let Some(relay) = endpoint.relay_urls().next() {
        relay_mirror
            .write()
            .expect("relay mirror")
            .insert(node, relay.to_string());
    }
    let outbox: Outbox = Arc::new(Default::default());
    let transport = Arc::new(RouteViewTransport::new(route_view.clone(), outbox.clone()));
    OutboxRouteBinder::new(router, transport).ensure_routable(host);
    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: runtime.clone(),
        codec: codecs,
        routes: HashMap::new(),
        swim: ActorAddress::default(),
        relay_mirror,
        route_view,
        outbox,
    });
    driver.install_actor_bridge();
    driver.connect_peer(endpoint);
    let routing = Routing(Arc::new(driver));
    let completion = ActorCompletion::new();
    let ready = completion.clone();
    let weak = Arc::downgrade(&routing.0);
    let watch = routing
        .0
        .connection_observer()
        .watch_connections(Arc::new(move || {
            if weak
                .upgrade()
                .is_some_and(|driver| driver.has_active_connection(&node))
            {
                let _ = ready.complete(());
            }
        }));
    completion
        .wait_deadline(BOOTSTRAP_TIMEOUT)
        .ok_or_else(|| NativeError::protocol("native route to host did not become ready"))?;
    drop(watch);
    Ok(routing)
}

fn attachment_failed(attachment: GuestAttachment, error: &NativeError) {
    if let Ok(notification) = attachment.begin_attachment_failed(&error.to_string()) {
        let _ = notification.wait_deadline(BOOTSTRAP_TIMEOUT);
    }
}

fn bootstrap_budget<'a>(runtime: &'a Runtime, engine: &'a EngineHandle) -> Budget<'a> {
    Budget {
        runtime,
        engine,
        deadline: engine.now().to_instant() + BOOTSTRAP_TIMEOUT,
    }
}

fn close_session(
    data: DataPlane,
    runtime: Runtime,
    engine: &Engine,
    tasks: &TaskSpawner,
) -> Result<()> {
    let completion = ActorCompletion::new();
    let finished = completion.clone();
    let clock = engine.handle();
    tasks.spawn(async move {
        let result = bootstrap_budget(&runtime, &clock)
            .bound(async { data.close_acknowledged().await.map_err(Into::into) })
            .await;
        if result.is_err() {
            let _ = data.close();
        }
        drop(data);
        let _ = finished.complete(result);
    });
    completion.wait()
}

/// Attach an inherited contextual session and execute it on caller-owned engine
/// capabilities. Session revocation is acknowledged before transport teardown,
/// including workload and attachment-notification failure paths.
pub fn run_contextual_program(
    program: NativeProgram,
    runtime: Runtime,
    engine: &Engine,
    tasks: TaskSpawner,
    emit: Arc<dyn Fn(&ActionObservation) -> io::Result<()> + Send + Sync>,
) -> Result<()> {
    if std::env::var_os("MYELIN_E2E_HOLD_BEFORE_BOOTSTRAP").is_some_and(|value| value == "1") {
        // The host has observed process-start, but no bootstrap descriptor has
        // been claimed. Its causal stop therefore cannot race attachment.
        let completion = ActorCompletion::new();
        let finished = completion.clone();
        let clock = engine.handle();
        let deadline = clock
            .now()
            .to_instant()
            .checked_add(Duration::from_millis(program.budget_ms))
            .ok_or_else(|| NativeError::protocol("native bootstrap hold budget overflows clock"))?;
        tasks.spawn(async move {
            let budget = Budget {
                runtime: &runtime,
                engine: &clock,
                deadline,
            };
            let result = budget.bound(std::future::pending::<Result<()>>()).await;
            let _ = finished.complete(result);
        });
        return completion.wait();
    }
    let host_route = BootstrapHostRoute::inherited(engine.handle().blocking_work_sender())?;
    let guest =
        GuestBootstrap::claim().map_err(|error| NativeError::protocol(error.to_string()))?;
    let (material, arena_fd, attachment) = guest
        .into_parts()
        .map_err(|error| NativeError::protocol(error.to_string()))?;
    let endpoint = match serde_json::from_slice::<EndpointAddr>(&material.routing) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let error = NativeError::protocol(format!("parse native host routing: {error}"));
            attachment_failed(attachment, &error);
            return Err(error);
        }
    };
    let routing = match child_routing(
        &runtime,
        engine,
        tasks.clone(),
        material.host_session,
        endpoint,
    ) {
        Ok(routing) => routing,
        Err(error) => {
            attachment_failed(attachment, &error);
            return Err(error);
        }
    };
    let child_node = routing.0.node_id().0;
    let attached = ActorCompletion::new();
    let ready = attached.clone();
    let clock = engine.handle();
    let child_runtime = runtime.clone();
    tasks.spawn(async move {
        let result = bootstrap_budget(&child_runtime, &clock)
            .bound(async {
                DataPlaneBootstrap::attach_routed(
                    arena_fd,
                    child_runtime.clone(),
                    material.host_session,
                    material.session_capability,
                    Some(child_node),
                    None,
                    Arc::new(host_route),
                )
                .await
                .map_err(Into::into)
            })
            .await;
        let _ = ready.complete(result.map(|bootstrap| bootstrap.data_plane));
    });
    let data = match attached.wait() {
        Ok(data) => data,
        Err(error) => {
            attachment_failed(attachment, &error);
            return Err(error);
        }
    };
    if let Err(error) = attachment
        .begin_attachment_succeeded()
        .and_then(|notification| notification.wait_deadline(BOOTSTRAP_TIMEOUT))
    {
        let _ = close_session(data, runtime, engine, &tasks);
        return Err(NativeError::protocol(error.to_string()));
    }
    let completion = ActorCompletion::new();
    let finished = completion.clone();
    let clock = engine.handle();
    tasks.spawn(async move {
        let result = run_native_program(&program, &data, &runtime, &clock, emit.as_ref()).await;
        let close = bootstrap_budget(&runtime, &clock)
            .bound(async { data.close_acknowledged().await.map_err(Into::into) })
            .await;
        if close.is_err() {
            let _ = data.close();
        }
        drop(data);
        let _ = finished.complete(result.and(close));
    });
    let result = completion.wait();
    drop(routing);
    result
}
