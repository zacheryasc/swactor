//! The datastream consumer, standalone: bind a UDP socket, decode every
//! delivery that arrives, and print each frame to stdout the instant it lands.
//!
//! This is the "raw stream to stdout" end of the per-node telemetry datastream
//! (see `distribution/DATASTREAM_SPEC.md`). It is deliberately the dumbest
//! possible consumer: one datagram carries one frame
//! ([`encode_delivery`](datastream::wire::encode_delivery)), so
//! we decode and print in arrival order — loss and reorder show up as they
//! happen on the wire, which is exactly what you want when watching a live
//! cluster. No store, no views, no dashboard.
//!
//! Usage: `swactor-datastream-collector [--bind HOST:PORT]`
//! (defaults to `$SWACTOR_DATASTREAM_BIND` or `0.0.0.0:7700`).

use std::io::Write;
use std::net::UdpSocket;

use datastream::views::decode_body;
use datastream::wire::decode_delivery;

fn main() {
    let bind = resolve_bind();
    let sock = match UdpSocket::bind(&bind) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("datastream collector: failed to bind {bind}: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("datastream collector listening on {bind} (one frame per datagram)");

    // 64 KiB comfortably exceeds a UDP datagram; a frame never spans datagrams.
    let mut buf = vec![0u8; 64 * 1024];
    let stdout = std::io::stdout();
    loop {
        match sock.recv_from(&mut buf) {
            Ok((n, _src)) => match decode_delivery(&buf[..n]) {
                Ok((stream, frame)) => {
                    let body = decode_body(&frame.channel, &frame.payload);
                    let node = stream.node.as_str();
                    let short = &node[..node.len().min(8)];
                    let mut out = stdout.lock();
                    // arrival order — no grouping, no buffering.
                    let _ = writeln!(
                        out,
                        "{short}#{life} #{pos:<5} [{chan}] {body}",
                        life = stream.life.0,
                        pos = frame.position.0,
                        chan = frame.channel,
                    );
                    let _ = out.flush();
                }
                Err(e) => eprintln!("datastream collector: dropped malformed datagram ({n} B): {e:?}"),
            },
            Err(e) => eprintln!("datastream collector: recv error: {e}"),
        }
    }
}

/// Resolve the bind address: `--bind HOST:PORT`, else `$SWACTOR_DATASTREAM_BIND`,
/// else `0.0.0.0:7700`.
fn resolve_bind() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => {
                if let Some(v) = args.next() {
                    return v;
                }
            }
            other if other.starts_with("--bind=") => {
                return other["--bind=".len()..].to_string();
            }
            _ => {}
        }
    }
    std::env::var("SWACTOR_DATASTREAM_BIND").unwrap_or_else(|_| "0.0.0.0:7700".to_string())
}
