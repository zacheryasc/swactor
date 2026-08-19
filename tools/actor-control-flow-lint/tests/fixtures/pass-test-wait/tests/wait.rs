use std::time::Duration;

#[test]
fn bounded_observation_wait_is_allowed() {
    let (sender, receiver) = std::sync::mpsc::channel();
    sender.send(7_u8).expect("send observation");
    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(10)),
        Ok(7)
    );
    std::thread::sleep(Duration::from_millis(1));
}
