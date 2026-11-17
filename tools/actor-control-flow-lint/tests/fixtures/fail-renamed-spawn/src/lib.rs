use tokio::spawn as launch;
use tokio::task::spawn_blocking as dispatch;
pub fn adapter() {
    launch(async {});
    dispatch(|| {});
}
