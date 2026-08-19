use std::time::Duration;

pub fn start_transport_pump() {
    tokio::spawn(async {
        let _io_type: Option<tokio::net::TcpStream> = None;
        tokio::time::sleep(Duration::from_millis(1)).await;
    });
}
