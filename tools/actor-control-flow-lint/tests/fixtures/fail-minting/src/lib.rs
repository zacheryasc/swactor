type EngineAlias = swactor_engine::Engine;
pub fn main(engine: &EngineAlias) -> swactor_engine::TaskSpawner {
    engine.task_spawner()
}
