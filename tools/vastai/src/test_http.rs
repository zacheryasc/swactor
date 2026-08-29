//! Synchronous HTTP fixture owned by the provider-I/O substrate.
//!
//! The fixture accepts only declarative method/path/JSON routes. Domain tests
//! cannot supply futures, callbacks, or other independently scheduled work.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct TestHttpRoute {
    method: String,
    path: String,
    status: u16,
    body: Vec<u8>,
    keep_open: bool,
}

impl TestHttpRoute {
    pub fn json(method: &str, path: &str, status: u16, body: serde_json::Value) -> Self {
        Self {
            method: method.to_owned(),
            path: path.to_owned(),
            status,
            body: serde_json::to_vec(&body).expect("test HTTP JSON serializes"),
            keep_open: false,
        }
    }

    pub fn raw(method: &str, path: &str, status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            method: method.to_owned(),
            path: path.to_owned(),
            status,
            body: body.into(),
            keep_open: false,
        }
    }

    pub fn open_raw(method: &str, path: &str, status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            method: method.to_owned(),
            path: path.to_owned(),
            status,
            body: body.into(),
            keep_open: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestHttpRequest {
    pub method: String,
    pub path: String,
}

pub struct TestHttpServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<TestHttpRequest>>>,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl TestHttpServer {
    pub fn start(routes: Vec<TestHttpRoute>) -> Result<Self, String> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|error| format!("bind test HTTP server: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("read test HTTP address: {error}"))?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_requests = Arc::clone(&requests);
        let thread_stop = Arc::clone(&stop);
        let join = std::thread::spawn(move || {
            while let Ok((stream, _)) = listener.accept() {
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                serve(stream, &routes, &thread_requests);
            }
        });
        Ok(Self {
            address,
            requests,
            stop,
            join: Some(join),
        })
    }

    pub fn uri(&self) -> String {
        format!("http://{}", self.address)
    }

    pub fn requests(&self) -> Vec<TestHttpRequest> {
        self.requests.lock().expect("test HTTP requests").clone()
    }
}

impl Drop for TestHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn serve(mut stream: TcpStream, routes: &[TestHttpRoute], requests: &Mutex<Vec<TestHttpRequest>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let Ok(request) = read_request(&mut stream) else {
        return;
    };
    requests
        .lock()
        .expect("test HTTP requests")
        .push(request.clone());
    let route = routes.iter().find(|route| {
        route.method == request.method
            && request
                .path
                .split_once('?')
                .map_or(request.path.as_str(), |(path, _)| path)
                == route.path
    });
    let route = route.cloned().unwrap_or_else(|| TestHttpRoute {
        method: request.method,
        path: request.path,
        status: 404,
        body: b"{}".to_vec(),
        keep_open: false,
    });
    let status = route.status;
    let body = route.body.as_slice();
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Response",
    };
    let header = if route.keep_open {
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/octet-stream\r\nConnection: keep-alive\r\n\r\n"
        )
    } else {
        format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
    };
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
    if route.keep_open {
        let mut buffer = [0_u8; 1];
        while stream.read(&mut buffer).is_ok_and(|read| read > 0) {}
    }
}

fn read_request(stream: &mut TcpStream) -> Result<TestHttpRequest, String> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream
            .read(&mut buffer)
            .map_err(|error| format!("read test HTTP request: {error}"))?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if bytes.len() > 64 * 1024 {
            return Err("test HTTP request headers too large".to_owned());
        }
    }
    let line_end = bytes
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| "test HTTP request has no request line".to_owned())?;
    let line = std::str::from_utf8(&bytes[..line_end])
        .map_err(|error| format!("test HTTP request line is not UTF-8: {error}"))?;
    let mut fields = line.split_whitespace();
    let method = fields
        .next()
        .ok_or_else(|| "test HTTP request has no method".to_owned())?;
    let path = fields
        .next()
        .ok_or_else(|| "test HTTP request has no path".to_owned())?;
    Ok(TestHttpRequest {
        method: method.to_owned(),
        path: path
            .split_once('?')
            .map_or(path, |(path, _)| path)
            .to_owned(),
    })
}
