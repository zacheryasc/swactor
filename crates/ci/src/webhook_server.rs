//! Webhook HTTP listener: receives Forgejo webhook POSTs and forwards
//! them to the LocalCoordinator actor.
//!
//! Runs as a standard thread (not an actor) using `tiny_http`.

use crate::{EventType, WebhookEvent};

/// Start the webhook listener in a new thread.
///
/// Returns a join handle for the listener thread.
#[cfg(feature = "local")]
pub fn start_webhook_listener(
    port: u16,
    secret: String,
    runtime: std::sync::Arc<swactor::runtime::Runtime>,
    coordinator_addr: swactor::actor::ActorAddress,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("webhook-listener".into())
        .spawn(move || {
            let server = tiny_http::Server::http(format!("0.0.0.0:{port}"))
                .expect("failed to start webhook server");

            eprintln!("Webhook listener on http://0.0.0.0:{port}");

            for mut request in server.incoming_requests() {
                let response = handle_request(&mut request, &secret, &runtime, coordinator_addr);
                let _ = request.respond(response);
            }
        })
        .expect("failed to spawn webhook listener thread")
}

#[cfg(feature = "local")]
fn handle_request(
    request: &mut tiny_http::Request,
    secret: &str,
    runtime: &std::sync::Arc<swactor::runtime::Runtime>,
    coordinator_addr: swactor::actor::ActorAddress,
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use crate::local_coordinator::LocalCoordinatorMsg;

    // Only accept POST.
    if request.method() != &tiny_http::Method::Post {
        return tiny_http::Response::from_string("method not allowed")
            .with_status_code(405);
    }

    // Read body.
    let mut body = String::new();
    if let Err(e) = std::io::Read::read_to_string(&mut request.as_reader(), &mut body) {
        eprintln!("webhook: failed to read body: {e}");
        return tiny_http::Response::from_string("bad request")
            .with_status_code(400);
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
                let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                    .expect("HMAC key creation");
                hmac::Mac::update(&mut mac, body.as_bytes());
                let expected = hex::encode(mac.finalize().into_bytes());
                if sig_hex != expected {
                    eprintln!("webhook: signature mismatch");
                    return tiny_http::Response::from_string("unauthorized")
                        .with_status_code(401);
                }
            }
            None => {
                eprintln!("webhook: missing signature header");
                return tiny_http::Response::from_string("unauthorized")
                    .with_status_code(401);
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

    // Parse JSON body to extract fields.
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
            eprintln!("webhook: could not extract webhook fields from JSON");
            return tiny_http::Response::from_string("bad payload").with_status_code(400);
        }
    };

    // Send to coordinator.
    let _ = runtime.send_to(coordinator_addr, LocalCoordinatorMsg::Webhook(webhook_event));

    tiny_http::Response::from_string("ok").with_status_code(200)
}

/// Parse a Forgejo webhook JSON payload into a WebhookEvent.
pub fn parse_webhook_json(json: &serde_json::Value, event_type: EventType) -> Option<WebhookEvent> {
    let repo = json.get("repository")?;
    let repo_owner = repo
        .get("owner")
        .and_then(|o| o.get("login"))
        .or_else(|| repo.get("owner").and_then(|o| o.get("username")))
        .and_then(|v| v.as_str())?
        .to_string();
    let repo_name = repo.get("name").and_then(|v| v.as_str())?.to_string();

    let (branch, commit_sha, tag) = match event_type {
        EventType::Push => {
            let reference = json.get("ref").and_then(|v| v.as_str()).unwrap_or("");
            let branch = reference.strip_prefix("refs/heads/").unwrap_or(reference);
            let sha = json
                .get("after")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            (branch.to_string(), sha, None)
        }
        EventType::Tag => {
            let reference = json.get("ref").and_then(|v| v.as_str()).unwrap_or("");
            let tag_name = reference.strip_prefix("refs/tags/").unwrap_or(reference);
            let sha = json
                .get("sha")
                .or_else(|| json.get("after"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            (String::new(), sha, Some(tag_name.to_string()))
        }
        EventType::Merge => {
            let pr = json.get("pull_request")?;
            let branch = pr
                .get("head")
                .and_then(|h| h.get("ref"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let sha = pr
                .get("head")
                .and_then(|h| h.get("sha"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            (branch, sha, None)
        }
    };

    Some(WebhookEvent {
        event_type,
        repo_owner,
        repo_name,
        branch,
        commit_sha,
        tag,
    })
}
