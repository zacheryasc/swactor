pub fn adapter() {
    tokio::spawn(async {});
    tokio::task::spawn_blocking(|| {});
    std::thread::spawn(|| {});
}
