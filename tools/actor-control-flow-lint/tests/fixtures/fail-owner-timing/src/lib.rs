use std::time::Duration;
use tokio::time::sleep as pause;

pub async fn unapproved_owner_wait() {
    pause(Duration::from_millis(1)).await;
}
