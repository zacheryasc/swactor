use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use dashboard::{DashboardConfig, start_dashboard};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn wait_for_http(port: u16) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("dashboard HTTP server did not start on port {port}");
}

fn read_sse_until(port: u16, needle: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect dashboard");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("set read timeout");
    write!(
        stream,
        "GET /events HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    )
    .expect("write request");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut out = String::new();
    let mut buf = [0_u8; 4096];
    while Instant::now() < deadline {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                out.push_str(&String::from_utf8_lossy(&buf[..n]));
                if out.contains(needle) {
                    return out;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => panic!("read SSE response: {e}"),
        }
    }
    panic!("SSE response did not contain {needle:?}; response was: {out}");
}

fn activity_json_from_sse(response: &str) -> serde_json::Value {
    let mut in_activity = false;
    for line in response.lines() {
        if line == "event: activity" {
            in_activity = true;
            continue;
        }
        if in_activity && let Some(json) = line.strip_prefix("data: ") {
            return serde_json::from_str(json).expect("activity JSON");
        }
    }
    panic!("no activity event in SSE response: {response}");
}

#[test]
fn activity_events_are_streamed_over_public_http_api() {
    let port = free_port();
    let dashboard = start_dashboard(DashboardConfig {
        port,
        event_capacity: 10,
        ..DashboardConfig::default()
    });
    dashboard.start_http_standalone();
    wait_for_http(port);

    dashboard.push_activity(false, "datastream connected");
    dashboard.push_activity(true, "mailbox pressure rising");

    let response = read_sse_until(port, "mailbox pressure rising");
    dashboard.shutdown();

    let events = activity_json_from_sse(&response);
    let rows = events.as_array().expect("activity array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["message"], "datastream connected");
    assert_eq!(rows[0]["level"], "INFO");
    assert_eq!(rows[1]["message"], "mailbox pressure rising");
    assert_eq!(rows[1]["level"], "WARN");
}

#[test]
fn activity_stream_honors_event_capacity_for_late_clients() {
    let port = free_port();
    let dashboard = start_dashboard(DashboardConfig {
        port,
        event_capacity: 2,
        ..DashboardConfig::default()
    });
    dashboard.start_http_standalone();
    wait_for_http(port);

    dashboard.push_activity(false, "oldest");
    dashboard.push_activity(false, "middle");
    dashboard.push_activity(false, "newest");

    let response = read_sse_until(port, "newest");
    dashboard.shutdown();

    let events = activity_json_from_sse(&response);
    let rows = events.as_array().expect("activity array");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["message"], "middle");
    assert_eq!(rows[1]["message"], "newest");
    assert!(
        !response.contains("oldest"),
        "late client should only receive the retained activity window"
    );
}

#[test]
fn activity_sequences_are_gap_free_for_retained_window() {
    let port = free_port();
    let dashboard = start_dashboard(DashboardConfig {
        port,
        event_capacity: 10,
        ..DashboardConfig::default()
    });
    dashboard.start_http_standalone();
    wait_for_http(port);

    for i in 0..5 {
        dashboard.push_activity(false, format!("source-{}-event", i % 2));
    }

    let response = read_sse_until(port, "source-0-event");
    dashboard.shutdown();

    let events = activity_json_from_sse(&response);
    let rows = events.as_array().expect("activity array");
    assert_eq!(rows.len(), 5);
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(row["seq"], idx as u64);
    }
}
