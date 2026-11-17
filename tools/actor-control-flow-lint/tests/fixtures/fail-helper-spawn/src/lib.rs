pub fn adapter(tasks: swactor_engine::TaskSpawner) {
    tasks.spawn(async {});
    hidden_helper();
}
fn hidden_helper() {
    tokio::spawn(async {});
}
