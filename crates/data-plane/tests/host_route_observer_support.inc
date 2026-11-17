struct RuntimeHostRouteObserver {
    runtime: Runtime,
}

struct RuntimeHostRouteSubscription {
    runtime: Runtime,
    watcher: ActorAddress,
}

impl Drop for RuntimeHostRouteSubscription {
    fn drop(&mut self) {
        let _ = self.runtime.stop_actor(self.watcher);
    }
}

struct RuntimeHostRouteWatcher {
    runtime: Runtime,
    host: ActorAddress,
    terminal: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl RuntimeHostRouteWatcher {
    fn host_terminated(&mut self, ctx: &swactor::actor::Ctx<'_>) {
        if let Some(terminal) = self.terminal.take() {
            terminal();
        }
        ctx.stop_self();
    }
}

impl swactor::actor::ActorInterface for RuntimeHostRouteWatcher {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &swactor::actor::Ctx<'_>) {
        swactor::std::CtxWatching::watch(ctx, self.host);
        // A host may have exited between synchronous registration and this
        // actor's first turn. Rechecking after watch installation closes that gap.
        if !self.runtime.is_local_actor(self.host) {
            self.host_terminated(ctx);
        }
    }

    fn handle(&mut self, _ctx: &swactor::actor::Ctx<'_>, _message: ()) {}

    fn on_actor_exit(&mut self, ctx: &swactor::actor::Ctx<'_>, exited: swactor::actor::ActorExited) {
        if exited.addr == self.host {
            self.host_terminated(ctx);
        }
    }
}

impl data_plane::data_plane::HostSessionRouteObserver for RuntimeHostRouteObserver {
    fn watch_terminal(
        &self,
        host: ActorAddress,
        terminal: Arc<dyn Fn() + Send + Sync>,
    ) -> data_plane::data_plane::HostSessionRouteWatch {
        if !self.runtime.is_local_actor(host) {
            terminal();
            return data_plane::data_plane::HostSessionRouteWatch::new(());
        }
        match self.runtime.spawn(RuntimeHostRouteWatcher {
            runtime: self.runtime.clone(), host, terminal: Some(terminal.clone()),
        }) {
            Ok(watcher) => data_plane::data_plane::HostSessionRouteWatch::new(RuntimeHostRouteSubscription {
                runtime: self.runtime.clone(), watcher,
            }),
            Err(_) => {
                terminal();
                data_plane::data_plane::HostSessionRouteWatch::new(())
            }
        }
    }
}
