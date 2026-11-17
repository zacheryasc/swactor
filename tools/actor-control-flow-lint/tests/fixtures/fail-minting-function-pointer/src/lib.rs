pub fn factory() -> fn(&swactor_engine::Engine) -> swactor_engine::TaskSpawner {
    swactor_engine::Engine::task_spawner
}
