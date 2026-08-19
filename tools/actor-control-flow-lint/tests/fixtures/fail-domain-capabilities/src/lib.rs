#![allow(clippy::disallowed_methods, dead_code, unused_must_use)]

use std::thread::sleep as renamed_sleep;
use std::time::Duration;
use swactor_engine::EngineHandle;

fn local_sleep_wrapper() {
    renamed_sleep(Duration::from_millis(1));
}

fn forbidden_engine_controls(handle: &EngineHandle) {
    handle.spawn(async {});
    handle.timer(Duration::from_millis(1));
}

fn forbidden_thread_and_receive() {
    std::thread::spawn(|| {});
    local_sleep_wrapper();
    let (_sender, receiver) = std::sync::mpsc::channel::<()>();
    let _ = receiver.recv();
    let _ = std::process::Command::new("true").output();
}

fn forbidden_runtime_driver() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(async {});
}

async fn forbidden_tokio_controls() {
    tokio::spawn(async {});
    tokio::task::spawn_blocking(|| {});
    tokio::time::sleep(Duration::from_millis(1)).await;
}
