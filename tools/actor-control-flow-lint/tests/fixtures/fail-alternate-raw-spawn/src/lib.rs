pub fn adapter(tasks: swactor_engine::TaskSpawner) {
    tasks.spawn(async {});
    let mut group = tokio::task::JoinSet::new();
    group.spawn(async {});
    let local = tokio::task::LocalSet::new();
    local.spawn_local(async {});
    std::thread::scope(|scope| { scope.spawn(|| {}); });
}
