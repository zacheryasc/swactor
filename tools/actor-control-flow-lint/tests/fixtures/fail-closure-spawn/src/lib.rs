pub fn adapter(tasks: swactor_engine::TaskSpawner) {
    tasks.spawn(async {
        tokio::spawn(async {});
        tokio::task::spawn_blocking(|| {});
        std::thread::spawn(|| {});
    });
}
