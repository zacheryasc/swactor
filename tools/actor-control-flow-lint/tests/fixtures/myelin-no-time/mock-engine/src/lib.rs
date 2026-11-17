pub struct EngineHandle;
impl EngineHandle {
    pub fn send_after(&self, cycles: u64) {
        let _ = cycles;
    }
    pub fn send_every(&self, cycles: u64) {
        let _ = cycles;
    }
    pub fn now(&self) -> u64 {
        0
    }
}
