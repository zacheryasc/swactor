mod bridge {
    pub use tokio::spawn as launch;
    pub use tokio::task::spawn_blocking as dispatch;
}
pub fn adapter() {
    bridge::launch(async {});
    bridge::dispatch(|| {});
}
