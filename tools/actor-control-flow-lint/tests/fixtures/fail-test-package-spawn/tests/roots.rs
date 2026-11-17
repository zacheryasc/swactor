#[test]
fn coordination_does_not_grant_raw_execution() {
    tokio::spawn(async {});
    tokio::task::spawn_blocking(|| {});
    std::thread::spawn(|| {});
}
