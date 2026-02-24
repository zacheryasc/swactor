use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};

use swactor::stats::RuntimeStats;

use super::event::AppEvent;
use super::types::RuntimeEndpoint;

/// Debug log file — visible after TUI exits (stderr is swallowed by raw mode).
const DEBUG_LOG: &str = "/tmp/swactor_sse_debug.log";

fn debug_log(msg: &str) {
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(DEBUG_LOG) {
        let _ = writeln!(f, "[sse] {}", msg);
    }
}

/// Spawn a thread that connects to a runtime's `/events` SSE endpoint
/// and sends parsed `RuntimeStats` through the provided channel.
///
/// Only handles `http://` endpoints. Returns an error if the endpoint
/// cannot be parsed or the initial TCP connection fails.
pub fn spawn_sse_reader(
    endpoint: RuntimeEndpoint,
    tx: mpsc::Sender<AppEvent>,
) -> io::Result<JoinHandle<()>> {
    let (host, port) = endpoint.parse_http().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unsupported endpoint (expected http://host:port): {}",
                endpoint.endpoint
            ),
        )
    })?;

    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(&addr)?;
    let host_header = addr.clone();

    Ok(thread::spawn(move || {
        debug_log(&format!("connecting to {}", endpoint));
        if let Err(e) = sse_read_loop(stream, &host_header, &endpoint, &tx) {
            debug_log(&format!("error for {}: {}", endpoint, e));
            eprintln!("SSE reader error for {}: {}", endpoint, e);
        }
    }))
}

fn sse_read_loop(
    mut stream: TcpStream,
    host: &str,
    endpoint: &RuntimeEndpoint,
    tx: &mpsc::Sender<AppEvent>,
) -> io::Result<()> {
    // HTTP/1.1 so tiny_http will use chunked transfer encoding for streaming.
    // (HTTP/1.0 causes tiny_http to buffer the entire response.)
    let request = format!(
        "GET /events HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\n\r\n",
        host
    );
    stream.write_all(request.as_bytes())?;

    let mut reader = BufReader::new(stream);

    // Parse response headers — detect chunked encoding
    let mut chunked = false;
    let mut header_line = String::new();
    loop {
        header_line.clear();
        let n = reader.read_line(&mut header_line)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during headers",
            ));
        }
        let trimmed = header_line.trim();
        if trimmed.is_empty() {
            break;
        }
        debug_log(&format!("header: {}", trimmed));
        if trimmed.to_ascii_lowercase().starts_with("transfer-encoding:")
            && trimmed.to_ascii_lowercase().contains("chunked")
        {
            chunked = true;
        }
    }

    debug_log(&format!("headers done, chunked={}", chunked));

    if chunked {
        let dechunked = DechunkedReader::new(reader);
        let line_reader = BufReader::new(dechunked);
        parse_sse_events(line_reader, endpoint, tx)
    } else {
        parse_sse_events(reader, endpoint, tx)
    }
}

fn parse_sse_events<R: BufRead>(
    mut reader: R,
    endpoint: &RuntimeEndpoint,
    tx: &mpsc::Sender<AppEvent>,
) -> io::Result<()> {
    let mut current_event = String::new();
    let mut data_buf = String::new();
    let mut line = String::new();
    let mut event_count: u64 = 0;

    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            debug_log("EOF");
            break;
        }

        let trimmed = line.trim_end();

        if trimmed.is_empty() {
            // Empty line = end of SSE event, dispatch
            if current_event == "stats" && !data_buf.is_empty() {
                match serde_json::from_str::<RuntimeStats>(&data_buf) {
                    Ok(stats) => {
                        event_count += 1;
                        if event_count <= 3 || event_count % 100 == 0 {
                            debug_log(&format!("stats event #{}", event_count));
                        }
                        let event = AppEvent::StatsUpdate {
                            source: endpoint.clone(),
                            stats: Box::new(stats),
                        };
                        if tx.send(event).is_err() {
                            debug_log("channel closed, exiting");
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        debug_log(&format!(
                            "JSON parse error: {} data={}",
                            e,
                            &data_buf[..data_buf.len().min(200)]
                        ));
                    }
                }
            } else if current_event == "done" {
                debug_log("received done event");
                return Ok(());
            }
            current_event.clear();
            data_buf.clear();
        } else if let Some(event_type) = trimmed.strip_prefix("event: ") {
            current_event = event_type.to_string();
        } else if let Some(data) = trimmed.strip_prefix("data: ") {
            data_buf.push_str(data);
        }
    }

    Ok(())
}

// ── Chunked transfer encoding decoder ────────────────────────────────

/// Transparently decodes HTTP chunked transfer encoding.
///
/// Wraps a `BufReader<R>` (which already consumed the response headers)
/// and exposes a plain `Read` that yields the dechunked body bytes.
struct DechunkedReader<R> {
    inner: BufReader<R>,
    remaining: usize,
    finished: bool,
}

impl<R: Read> DechunkedReader<R> {
    fn new(inner: BufReader<R>) -> Self {
        Self {
            inner,
            remaining: 0,
            finished: false,
        }
    }
}

impl<R: Read> Read for DechunkedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }

        // Start of a new chunk — read the hex size line
        if self.remaining == 0 {
            let mut size_line = String::new();
            let n = self.inner.read_line(&mut size_line)?;
            if n == 0 {
                self.finished = true;
                return Ok(0);
            }
            let hex = size_line.trim();
            if hex.is_empty() {
                self.finished = true;
                return Ok(0);
            }
            let size = usize::from_str_radix(hex, 16).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid chunk size: {:?}", hex),
                )
            })?;
            if size == 0 {
                // Terminal chunk
                self.finished = true;
                return Ok(0);
            }
            self.remaining = size;
        }

        // Read up to `remaining` bytes from the current chunk
        let to_read = buf.len().min(self.remaining);
        let n = self.inner.read(&mut buf[..to_read])?;
        if n == 0 {
            self.finished = true;
            return Ok(0);
        }
        self.remaining -= n;

        // After consuming all bytes in a chunk, skip the trailing \r\n
        if self.remaining == 0 {
            let mut crlf = [0u8; 2];
            self.inner.read_exact(&mut crlf)?;
        }

        Ok(n)
    }
}
