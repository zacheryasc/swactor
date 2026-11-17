pub struct Client {
    started: std::time::Instant,
}
impl Client {
    pub fn new() -> Self {
        Self {
            started: std::time::Instant::now(),
        }
    }
    pub fn completed(&self) -> bool {
        self.started.elapsed().as_nanos() > 0
    }
}
pub fn exposed_clock() -> std::time::Instant {
    std::time::Instant::now()
}
