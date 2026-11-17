fn helper() {
    let (_, receiver) = std::sync::mpsc::channel::<()>();
    let _ = receiver.recv();
    let _ = std::process::Command::new("true").status();
}
#[test]
fn composition() { helper(); }
