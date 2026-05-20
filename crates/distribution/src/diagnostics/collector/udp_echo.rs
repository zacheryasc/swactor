//! Tiny UDP echo service co-located with the collector
//! (`DIAGNOSTICS_PLAN.md` T3.3 + open decision 6).
//!
//! The probe layer (`crate::diagnostics::probes`) on each node sends a
//! small datagram here and waits for the same bytes back. RTT and
//! reachability of this endpoint give us a "network is up at all"
//! signal that's independent of iroh — when iroh reports a peer as
//! Dead and these probes still round-trip, the bug is iroh-side; when
//! both fail, the bug is the environment.
//!
//! Bind explicitly on a UDP socket, then [`spawn_udp_echo`] hands back
//! the bound `SocketAddr` and a join handle. The service runs on the
//! current tokio runtime until the handle is aborted or the listener
//! errors. Packet size is capped at 1500 bytes (typical Ethernet MTU)
//! to keep memory pressure bounded.

use std::net::SocketAddr;

use tokio::net::UdpSocket;

/// Per-packet buffer size — one Ethernet-MTU frame is plenty for our
/// 5-byte probe payload. Bigger payloads get silently truncated.
pub const MAX_PACKET_BYTES: usize = 1500;

/// Handle returned by [`spawn_udp_echo`]. Owns the spawned tokio task;
/// dropping the handle drops the task. Use [`UdpEchoHandle::abort`] for
/// an explicit shutdown.
pub struct UdpEchoHandle {
    /// The socket address the echo service is actually bound on.
    /// Useful when the caller passed `"127.0.0.1:0"` and needs to know
    /// the ephemeral port.
    pub local_addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl UdpEchoHandle {
    /// Abort the background task. Idempotent; safe to call multiple
    /// times.
    pub fn abort(&self) {
        self.handle.abort();
    }
}

impl Drop for UdpEchoHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Bind a UDP socket on `bind` and spawn the echo loop on the current
/// tokio runtime. Returns the bound address (so callers know the port
/// even if they passed `:0`) plus a handle.
pub async fn spawn_udp_echo(bind: SocketAddr) -> std::io::Result<UdpEchoHandle> {
    let socket = UdpSocket::bind(bind).await?;
    let local_addr = socket.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_PACKET_BYTES];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((n, peer)) => {
                    // Best-effort echo. If the send fails (e.g. peer
                    // closed) just move on — the next probe from any
                    // sender will retry.
                    let _ = socket.send_to(&buf[..n], peer).await;
                }
                Err(e) => {
                    // recv_from on a UDP socket only errors on
                    // unrecoverable conditions (e.g. socket closed).
                    // Log and exit so the supervisor restarts us.
                    eprintln!("swactor-diag-collector: udp echo recv error: {e}");
                    break;
                }
            }
        }
    });
    Ok(UdpEchoHandle { local_addr, handle })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn echoes_a_payload_back_to_the_sender() {
        let echo = spawn_udp_echo("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind echo");
        let probe = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind probe");
        probe.connect(echo.local_addr).await.expect("connect echo");
        probe.send(b"hello").await.expect("send");
        let mut buf = [0u8; 16];
        let n = tokio::time::timeout(Duration::from_secs(1), probe.recv(&mut buf))
            .await
            .expect("recv timed out")
            .expect("recv error");
        assert_eq!(&buf[..n], b"hello");
    }

    #[tokio::test]
    async fn truncates_oversized_payload_silently() {
        let echo = spawn_udp_echo("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        probe.connect(echo.local_addr).await.unwrap();
        // Send something just under MTU — full echo expected.
        let payload = vec![0xab; 1200];
        probe.send(&payload).await.unwrap();
        let mut buf = vec![0u8; 2048];
        let n = tokio::time::timeout(Duration::from_secs(1), probe.recv(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, payload.len());
        assert_eq!(&buf[..n], &payload[..]);
    }
}
