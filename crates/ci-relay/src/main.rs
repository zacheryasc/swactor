//! ci-relay — webhook relay for VPS side.
//!
//! Receives Forgejo webhook POSTs over HTTP, then forwards the parsed
//! `WebhookEvent` payloads to the Thinkpad local-runner over iroh.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use iroh::{Endpoint, RelayMode};
use tokio::sync::Mutex as TokioMutex;

use swactor_ci::webhook_server::parse_webhook_json;
use swactor_ci::{EventType, WebhookEvent};

/// ALPN protocol identifier for CI relay traffic over iroh.
const ALPN: &[u8] = b"swactor/ci/1";

/// Wire tag for WebhookEvent messages.
const WEBHOOK_TAG: &str = "ci::WebhookEvent";

#[derive(Parser)]
#[command(name = "ci-relay", about = "Webhook relay: Forgejo → iroh → local-runner")]
struct Args {
    /// HTTP port for receiving Forgejo webhooks.
    #[arg(long, default_value = "8787")]
    port: u16,

    /// Webhook secret for HMAC-SHA256 verification (empty to skip).
    #[arg(long, default_value = "")]
    secret: String,
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let endpoint = rt.block_on(async {
        Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .relay_mode(RelayMode::Default)
            .bind()
            .await
            .expect("failed to bind iroh endpoint")
    });

    let node_id = endpoint.id();
    eprintln!("ci-relay started");
    eprintln!("  Iroh Node ID: {node_id}");
    eprintln!("  Webhook HTTP: http://0.0.0.0:{}", args.port);
    eprintln!();
    eprintln!("Waiting for runner to connect...");

    // Shared state: the active connection from the Thinkpad runner.
    let connection: Arc<TokioMutex<Option<iroh::endpoint::Connection>>> =
        Arc::new(TokioMutex::new(None));

    // Spawn a task that accepts inbound iroh connections from the runner.
    {
        let endpoint = endpoint.clone();
        let connection = Arc::clone(&connection);
        rt.spawn(async move {
            loop {
                match endpoint.accept().await {
                    Some(incoming) => match incoming.await {
                        Ok(conn) => {
                            let remote = conn.remote_id();
                            eprintln!("Runner connected: {remote}");
                            *connection.lock().await = Some(conn);
                        }
                        Err(e) => {
                            eprintln!("iroh accept error: {e}");
                        }
                    },
                    None => {
                        eprintln!("iroh endpoint closed");
                        break;
                    }
                }
            }
        });
    }

    // Run the HTTP webhook listener on a standard thread (blocking).
    let secret = args.secret.clone();
    let server = tiny_http::Server::http(format!("0.0.0.0:{}", args.port))
        .expect("failed to start HTTP server");

    eprintln!("Listening for webhooks...");

    for mut request in server.incoming_requests() {
        let response = handle_webhook(&mut request, &secret, &connection, &rt);
        let _ = request.respond(response);
    }
}

/// Handle an incoming webhook HTTP request.
///
/// Parses and verifies the webhook, then forwards the event over iroh.
fn handle_webhook(
    request: &mut tiny_http::Request,
    secret: &str,
    connection: &Arc<TokioMutex<Option<iroh::endpoint::Connection>>>,
    rt: &tokio::runtime::Runtime,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    if request.method() != &tiny_http::Method::Post {
        return tiny_http::Response::from_string("method not allowed").with_status_code(405);
    }

    // Read body.
    let mut body = String::new();
    if let Err(e) = std::io::Read::read_to_string(&mut request.as_reader(), &mut body) {
        eprintln!("webhook: failed to read body: {e}");
        return tiny_http::Response::from_string("bad request").with_status_code(400);
    }

    // Verify HMAC-SHA256 signature if secret is non-empty.
    if !secret.is_empty() {
        let sig_header = request
            .headers()
            .iter()
            .find(|h| h.field.equiv("X-Forgejo-Signature"))
            .map(|h| h.value.as_str().to_string());

        match sig_header {
            Some(sig_hex) => {
                type HmacSha256 = Hmac<Sha256>;
                let mut mac =
                    HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key creation");
                hmac::Mac::update(&mut mac, body.as_bytes());
                let expected = hex::encode(mac.finalize().into_bytes());
                if sig_hex != expected {
                    eprintln!("webhook: signature mismatch");
                    return tiny_http::Response::from_string("unauthorized").with_status_code(401);
                }
            }
            None => {
                eprintln!("webhook: missing signature header");
                return tiny_http::Response::from_string("unauthorized").with_status_code(401);
            }
        }
    }

    // Determine event type from Forgejo header.
    let event_header = request
        .headers()
        .iter()
        .find(|h| h.field.equiv("X-Forgejo-Event"))
        .map(|h| h.value.as_str().to_string())
        .unwrap_or_default();

    let event_type = match event_header.as_str() {
        "push" => EventType::Push,
        "create" => EventType::Tag,
        "pull_request" => EventType::Merge,
        other => {
            eprintln!("webhook: ignoring event type '{other}'");
            return tiny_http::Response::from_string("ignored").with_status_code(200);
        }
    };

    // Parse JSON body.
    let json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("webhook: failed to parse JSON: {e}");
            return tiny_http::Response::from_string("bad json").with_status_code(400);
        }
    };

    let webhook_event = match parse_webhook_json(&json, event_type) {
        Some(e) => e,
        None => {
            eprintln!("webhook: could not extract fields from JSON");
            return tiny_http::Response::from_string("bad payload").with_status_code(400);
        }
    };

    eprintln!(
        "webhook: {} {} on {}/{}",
        webhook_event.commit_sha.get(..8).unwrap_or(&webhook_event.commit_sha),
        webhook_event.branch,
        webhook_event.repo_owner,
        webhook_event.repo_name,
    );

    // Forward over iroh.
    match forward_event(&webhook_event, connection, rt) {
        Ok(()) => {
            eprintln!("  → forwarded to runner");
            tiny_http::Response::from_string("ok").with_status_code(200)
        }
        Err(e) => {
            eprintln!("  → forward failed: {e}");
            tiny_http::Response::from_string("relay error").with_status_code(502)
        }
    }
}

/// Serialize and send a WebhookEvent over the iroh connection.
fn forward_event(
    event: &WebhookEvent,
    connection: &Arc<TokioMutex<Option<iroh::endpoint::Connection>>>,
    rt: &tokio::runtime::Runtime,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(event)?;

    rt.block_on(async {
        let guard = connection.lock().await;
        let conn = guard.as_ref().ok_or("no runner connected")?;

        let mut send = conn.open_uni().await?;
        write_tagged_message(&mut send, WEBHOOK_TAG.as_bytes(), &payload).await?;
        send.finish()?;

        // Wait briefly for the stream to flush.
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(())
    })
}

/// Write a tagged message to a QUIC send stream.
///
/// Frame format: `[4B tag_len][tag_bytes][payload_bytes]`
async fn write_tagged_message(
    send: &mut iroh::endpoint::SendStream,
    tag: &[u8],
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let tag_len = (tag.len() as u32).to_be_bytes();
    send.write_all(&tag_len).await?;
    send.write_all(tag).await?;
    send.write_all(payload).await?;
    Ok(())
}
