mod engine {
    pub fn unrelated_engine_function() {
        tokio::spawn(async {});
        tokio::task::spawn_blocking(|| {});
        std::thread::spawn(|| {});
    }

    struct BlockingWorkSender;

    impl BlockingWorkSender {
        fn spawn_owned_thread(&self) {
            let _ = std::thread::Builder::new().spawn(|| {});
        }
    }
}
