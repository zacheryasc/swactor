pub fn external_outcome() -> bool {
    external_clock::Client::new().completed()
}
// Identical local names do not imply temporal semantics.
pub fn timeout(sequence: u64) -> u64 {
    sequence
}
#[cfg(feature = "types")]
pub type Budget = Option<std::time::Duration>;
#[cfg(feature = "types")]
pub struct Pending {
    pub deadline: std::time::Instant,
}
#[cfg(feature = "types")]
pub trait Contract {
    fn deadline(&self) -> std::time::SystemTime;
}
#[cfg(feature = "types")]
impl Pending {
    pub fn unused(&self, _: Budget) {}
}
#[cfg(feature = "calls")]
pub fn timing() {
    use std::time::Instant as Alias;
    let _clock = Alias::now;
    let _sleep = std::thread::sleep;
    macro_rules! clock {
        () => {
            std::time::SystemTime::now()
        };
    }
    let _ = clock!();
}
#[cfg(feature = "inferred")]
pub fn inferred() {
    let _ = external_clock::exposed_clock();
}
#[cfg(test)]
#[test]
fn test_code_is_not_exempt() {
    let _ = std::time::Instant::now();
}

#[cfg(feature = "scheduling")]
pub fn scheduling() {
    let engine = swactor_engine::EngineHandle;
    engine.send_after(1);
    let _ = swactor_engine::EngineHandle::send_every;
    let _ = engine.now();
}
