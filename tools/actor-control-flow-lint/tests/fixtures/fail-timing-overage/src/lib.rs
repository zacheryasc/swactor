use std::time::Duration;

pub fn exceeds_registered_timing_budget() {
    let (_sender, receiver) = std::sync::mpsc::channel::<()>();
    let _ = receiver.recv_timeout(Duration::from_millis(1));
    let _ = receiver.recv_timeout(Duration::from_millis(1));
}
