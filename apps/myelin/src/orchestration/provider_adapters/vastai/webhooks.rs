//! Provider-specific authenticated snapshot invalidations, with durable acceptance.
//! Signed timestamp bytes are authenticated, never used as a clock. Replays can
//! only invalidate snapshots; durable event IDs suppress duplicate delivery.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::post;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use swactor_engine::BlockingWorkSender;

const WEBHOOK_PATH: &str = "/api/providers/vastai/webhook";
const READY_PATH: &str = "/api/providers/vastai/ssh-ready";
const MAX_BODY: usize = 64 * 1024;
const MAX_PENDING: usize = 16;
const API: &str = "https://console.vast.ai/api/v0/webhooks";
type Invalidation = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone)]
pub struct VastAiWebhookConfig {
    pub public_url: String,
    pub event_types: Vec<String>,
    pub state_dir: PathBuf,
    pub signing_secret: Option<String>,
    pub user_id: Option<u64>,
    pub register: bool,
}

impl VastAiWebhookConfig {
    pub fn from_env(state_dir: PathBuf) -> Result<Self, String> {
        if std::env::var_os("MYELIN_VASTAI_POLL_INTERVAL_SECS").is_some() {
            return Err("MYELIN_VASTAI_POLL_INTERVAL_SECS is obsolete; provider progress requires webhook events".to_owned());
        }
        let value = |name: &str| -> Result<Option<String>, String> {
            match std::env::var(name) {
                Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
                Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
                Err(_) => Err(format!("{name} must contain UTF-8")),
            }
        };
        let public_url = value("MYELIN_VASTAI_WEBHOOK_URL")?
            .ok_or("MYELIN_VASTAI_WEBHOOK_URL must name the public HTTPS webhook endpoint")?;
        let event_types = value("MYELIN_VASTAI_WEBHOOK_EVENTS")?
            .ok_or("MYELIN_VASTAI_WEBHOOK_EVENTS must name official client notification keys")?
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect();
        let register = match value("MYELIN_VASTAI_WEBHOOK_REGISTER")?.as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err("MYELIN_VASTAI_WEBHOOK_REGISTER must be true or false".to_owned()),
        };
        let user_id = value("MYELIN_VASTAI_WEBHOOK_USER_ID")?
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| "invalid MYELIN_VASTAI_WEBHOOK_USER_ID".to_owned())
            })
            .transpose()?;
        Ok(Self {
            public_url,
            event_types,
            state_dir,
            signing_secret: value("MYELIN_VASTAI_WEBHOOK_SECRET")?,
            user_id,
            register,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    public_url: String,
    event_types: BTreeSet<String>,
    user_id: u64,
    signing_secret: String,
    api_key_fingerprint: String,
    webhook_id: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyCredential {
    label: String,
    token: String,
    ready: bool,
    notified: bool,
    revoked: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedEvent {
    event_id: String,
    body_digest: String,
    notified: bool,
}

#[derive(Deserialize)]
struct ProviderEvent {
    event_id: String,
    user_id: u64,
    notif_type: String,
}

struct Subscriber {
    label: String,
    callback: Invalidation,
}

#[derive(Default)]
struct Subscribers {
    next_id: u64,
    entries: BTreeMap<u64, Subscriber>,
}

pub struct VastAiWebhooks {
    registration: Registration,
    state_dir: PathBuf,
    ready_url: String,
    durable: Mutex<()>,
    subscribers: Mutex<Subscribers>,
    pending: Arc<tokio::sync::Semaphore>,
    _directory_owner: File,
}

impl std::fmt::Debug for VastAiWebhooks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VastAiWebhooks")
            .field("public_url", &self.registration.public_url)
            .field("user_id", &self.registration.user_id)
            .finish_non_exhaustive()
    }
}

pub struct VastAiWebhookSubscription {
    owner: Weak<VastAiWebhooks>,
    id: u64,
}

impl Drop for VastAiWebhookSubscription {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.subscribers.lock().entries.remove(&self.id);
        }
    }
}

impl VastAiWebhooks {
    pub fn open(mut config: VastAiWebhookConfig, api_key: &str) -> Result<Arc<Self>, String> {
        let uri: Uri = config
            .public_url
            .parse()
            .map_err(|_| "invalid VastAI webhook URL")?;
        if uri.scheme_str() != Some("https")
            || uri.host().is_none()
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || uri.path() != WEBHOOK_PATH
            || uri.query().is_some()
            || config.public_url.len() > 2048
        {
            return Err(format!(
                "VastAI webhook URL must be public HTTPS with path {WEBHOOK_PATH}, without credentials or query"
            ));
        }
        config.event_types.sort();
        config.event_types.dedup();
        if config.event_types.is_empty()
            || config.event_types.iter().any(|event| {
                !event.starts_with("client:")
                    || event.len() <= 7
                    || event.len() > 128
                    || !event
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_:-".contains(&byte))
            })
        {
            return Err("configure explicit official client: notification keys; host events cannot share this receiver".to_owned());
        }
        private_directory(&config.state_dir)?;
        let directory_owner = private_file(&config.state_dir.join("receiver.lock"), false)?;
        directory_owner.try_lock().map_err(|error| {
            format!("VastAI webhook state is already owned or cannot be locked: {error}")
        })?;
        let registration_path = config.state_dir.join("registration.json");
        let fingerprint = blake3::hash(api_key.as_bytes()).to_hex().to_string();
        let events: BTreeSet<_> = config.event_types.iter().cloned().collect();
        let registration = match load_json::<Registration>(&registration_path)? {
            Some(stored) => {
                if stored.public_url != config.public_url
                    || stored.event_types != events
                    || stored.api_key_fingerprint != fingerprint
                    || config.user_id.is_some_and(|id| id != stored.user_id)
                    || config
                        .signing_secret
                        .as_ref()
                        .is_some_and(|secret| secret != &stored.signing_secret)
                {
                    return Err("VastAI webhook state is bound to different endpoint, events, account credentials, or secret; explicitly recover matching registration before enabling this provider".to_owned());
                }
                stored
            }
            None => {
                let registration = if let Some(secret) = config.signing_secret.as_ref() {
                    let user_id = config
                        .user_id
                        .filter(|id| *id != 0)
                        .ok_or("manual webhook secret requires MYELIN_VASTAI_WEBHOOK_USER_ID")?;
                    if secret.is_empty() {
                        return Err("empty VastAI webhook signing secret".to_owned());
                    }
                    Registration {
                        public_url: config.public_url.clone(),
                        event_types: events,
                        user_id,
                        signing_secret: secret.clone(),
                        api_key_fingerprint: fingerprint,
                        webhook_id: None,
                    }
                } else if config.register {
                    if api_key.trim().is_empty() {
                        return Err("webhook registration requires a VastAI API key".to_owned());
                    }
                    let intent = config.state_dir.join("registration.pending.json");
                    if intent.exists() {
                        return Err("previous webhook registration outcome is unresolved; recover its signing secret and user ID instead of creating another webhook".to_owned());
                    }
                    persist_json(
                        &intent,
                        &serde_json::json!({
                            "public_url": config.public_url, "event_types": config.event_types,
                            "api_key_fingerprint": fingerprint,
                        }),
                    )?;
                    register_webhook(&config, api_key, fingerprint)?
                } else {
                    return Err("VastAI lifecycle requires a persisted webhook registration, manual signing secret + user ID, or explicit MYELIN_VASTAI_WEBHOOK_REGISTER=true".to_owned());
                };
                persist_json(&registration_path, &registration)?;
                remove_durable(&config.state_dir.join("registration.pending.json"))?;
                registration
            }
        };
        if registration.user_id == 0 || registration.signing_secret.is_empty() {
            return Err("invalid persisted VastAI webhook registration".to_owned());
        }
        for directory in ["events", "labels", "tokens"] {
            private_directory(&config.state_dir.join(directory))?;
        }
        let ready_url = format!(
            "https://{}{READY_PATH}",
            uri.authority().ok_or("webhook URL has no authority")?
        );
        Ok(Arc::new(Self {
            registration,
            state_dir: config.state_dir,
            ready_url,
            durable: Mutex::new(()),
            subscribers: Mutex::new(Subscribers::default()),
            pending: Arc::new(tokio::sync::Semaphore::new(MAX_PENDING)),
            _directory_owner: directory_owner,
        }))
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn subscribe(
        self: &Arc<Self>,
        label: &str,
        callback: Invalidation,
    ) -> VastAiWebhookSubscription {
        let id = {
            let mut subscribers = self.subscribers.lock();
            let id = subscribers.next_id;
            subscribers.next_id = id
                .checked_add(1)
                .expect("VastAI subscription identity exhausted");
            subscribers.entries.insert(
                id,
                Subscriber {
                    label: label.to_owned(),
                    callback: Arc::clone(&callback),
                },
            );
            id
        };
        // One authoritative reconciliation closes the restart/pre-subscription gap.
        // A callback only queues work; it never asserts provider state itself.
        if !callback() {
            self.subscribers.lock().entries.remove(&id);
        }
        VastAiWebhookSubscription {
            owner: Arc::downgrade(self),
            id,
        }
    }

    pub fn bootstrap_environment(&self, label: &str) -> Result<Vec<(String, String)>, String> {
        if label.is_empty() || label.len() > 1024 {
            return Err("invalid VastAI readiness label".to_owned());
        }
        let _guard = self.durable.lock();
        let label_path = self.label_path(label);
        let credential = match load_json::<ReadyCredential>(&label_path)? {
            Some(credential) => {
                if credential.label != label || credential.revoked {
                    return Err(
                        "VastAI readiness credential identity is revoked or inconsistent"
                            .to_owned(),
                    );
                }
                credential
            }
            None => {
                let mut random = [0u8; 32];
                File::open("/dev/urandom")
                    .and_then(|mut source| source.read_exact(&mut random))
                    .map_err(|error| format!("generate VastAI readiness credential: {error}"))?;
                let token = encode_hex(&random);
                let credential = ReadyCredential {
                    label: label.to_owned(),
                    token,
                    ready: false,
                    notified: false,
                    revoked: false,
                };
                persist_json(&label_path, &credential)?;
                credential
            }
        };
        // Repair an interrupted two-file commit without rotating a credential
        // already supplied to the owned container.
        persist_json(&self.token_path(&credential.token), &credential.label)?;
        Ok(vec![
            ("MYELIN_VASTAI_READY_URL".to_owned(), self.ready_url.clone()),
            ("MYELIN_VASTAI_READY_TOKEN".to_owned(), credential.token),
        ])
    }

    pub fn revoke_bootstrap(&self, label: &str) -> Result<(), String> {
        let _guard = self.durable.lock();
        if let Some(mut credential) = load_json::<ReadyCredential>(&self.label_path(label))? {
            if credential.label != label {
                return Err("VastAI readiness identity mismatch".to_owned());
            }
            credential.revoked = true;
            persist_json(&self.label_path(label), &credential)?;
            remove_durable(&self.token_path(&credential.token))?;
        }
        Ok(())
    }

    pub fn router(self: &Arc<Self>, blocking: BlockingWorkSender) -> Router {
        Router::new()
            .route(WEBHOOK_PATH, post(receive_webhook))
            .route(READY_PATH, post(receive_ready))
            .layer(DefaultBodyLimit::max(MAX_BODY))
            .with_state(Receiver {
                leaf: Arc::clone(self),
                blocking,
            })
    }

    fn label_path(&self, label: &str) -> PathBuf {
        self.state_dir
            .join("labels")
            .join(format!("{}.json", blake3::hash(label.as_bytes()).to_hex()))
    }

    fn token_path(&self, token: &str) -> PathBuf {
        self.state_dir
            .join("tokens")
            .join(format!("{}.json", blake3::hash(token.as_bytes()).to_hex()))
    }

    fn notify(&self, label: Option<&str>) {
        let callbacks: Vec<_> = self
            .subscribers
            .lock()
            .entries
            .iter()
            .filter(|(_, subscriber)| label.is_none_or(|label| label == subscriber.label))
            .map(|(&id, subscriber)| (id, Arc::clone(&subscriber.callback)))
            .collect();
        for (id, callback) in callbacks {
            if !callback() {
                self.subscribers.lock().entries.remove(&id);
            }
        }
    }

    fn accept_webhook(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), StatusCode> {
        if body.len() > MAX_BODY {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        let timestamp = header(headers, "x-vast-timestamp")?;
        if timestamp.is_empty()
            || timestamp.len() > 32
            || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let signature = header(headers, "x-vast-signature-256")?;
        let signature = signature
            .strip_prefix("sha256=")
            .and_then(decode_signature)
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(self.registration.signing_secret.as_bytes())
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        mac.update(timestamp.as_bytes());
        mac.update(b".");
        mac.update(body);
        mac.verify_slice(&signature)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        let event: ProviderEvent =
            serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
        if event.user_id != self.registration.user_id {
            return Err(StatusCode::FORBIDDEN);
        }
        if event.event_id.is_empty()
            || event.event_id.len() > 128
            || header(headers, "x-vast-event-id")? != event.event_id
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        if event.notif_type != "webhook_test"
            && !self
                .registration
                .event_types
                .contains(&format!("client:{}", event.notif_type))
        {
            return Err(StatusCode::FORBIDDEN);
        }
        let path = self.state_dir.join("events").join(format!(
            "{}.json",
            blake3::hash(event.event_id.as_bytes()).to_hex()
        ));
        let digest = blake3::hash(body).to_hex().to_string();
        let _guard = self.durable.lock();
        let mut accepted = match load_json::<AcceptedEvent>(&path).map_err(unavailable)? {
            Some(accepted) => {
                if accepted.event_id != event.event_id || accepted.body_digest != digest {
                    return Err(StatusCode::CONFLICT);
                }
                // A prior directory sync may have failed after the atomic rename.
                sync_existing(&path).map_err(unavailable)?;
                accepted
            }
            None => {
                let accepted = AcceptedEvent {
                    event_id: event.event_id,
                    body_digest: digest,
                    notified: false,
                };
                persist_json(&path, &accepted).map_err(unavailable)?;
                accepted
            }
        };
        if !accepted.notified {
            self.notify(None);
            accepted.notified = true;
            persist_json(&path, &accepted).map_err(unavailable)?;
        }
        Ok(())
    }

    fn accept_ready(&self, headers: &HeaderMap, body: &[u8]) -> Result<(), StatusCode> {
        let value: serde_json::Value =
            serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
        if !value.as_object().is_some_and(|object| object.is_empty()) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let token = header(headers, "authorization")?
            .strip_prefix("Bearer ")
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let _guard = self.durable.lock();
        let label = load_json::<String>(&self.token_path(token))
            .map_err(unavailable)?
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let path = self.label_path(&label);
        let mut credential = load_json::<ReadyCredential>(&path)
            .map_err(unavailable)?
            .ok_or(StatusCode::UNAUTHORIZED)?;
        if credential.label != label || credential.revoked || credential.token != token {
            return Err(StatusCode::UNAUTHORIZED);
        }
        if !credential.ready {
            credential.ready = true;
            persist_json(&path, &credential).map_err(unavailable)?;
        } else {
            sync_existing(&path).map_err(unavailable)?;
        }
        if !credential.notified {
            self.notify(Some(&label));
            credential.notified = true;
            persist_json(&path, &credential).map_err(unavailable)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Receiver {
    leaf: Arc<VastAiWebhooks>,
    blocking: BlockingWorkSender,
}

async fn receive_webhook(
    State(receiver): State<Receiver>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    receive(receiver, headers, body, false).await
}

async fn receive_ready(
    State(receiver): State<Receiver>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    receive(receiver, headers, body, true).await
}

async fn receive(receiver: Receiver, headers: HeaderMap, body: Bytes, ready: bool) -> StatusCode {
    let Ok(permit) = Arc::clone(&receiver.leaf.pending).try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let (sender, completion) = tokio::sync::oneshot::channel();
    let leaf = receiver.leaf;
    if receiver
        .blocking
        .submit(Box::new(move || {
            let _permit = permit;
            let result = if ready {
                leaf.accept_ready(&headers, &body)
            } else {
                leaf.accept_webhook(&headers, &body)
            };
            let _ = sender.send(result);
        }))
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match completion.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT,
        Ok(Err(status)) => status,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, StatusCode> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or(StatusCode::UNAUTHORIZED)?;
    if values.next().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)
}

fn unavailable(_: String) -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}

fn decode_signature(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte: u8| char::from(byte).to_digit(16).map(|digit| digit as u8);
        bytes[index] = nibble(pair[0])? << 4 | nibble(pair[1])?;
    }
    Some(bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 15)]));
    }
    value
}

fn private_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path).map_err(|error| format!("create VastAI private state: {error}"))?;
    if !fs::symlink_metadata(path)
        .map_err(|error| error.to_string())?
        .is_dir()
    {
        return Err("VastAI state must be a real directory, not a symbolic link".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| error.to_string())?;
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn private_file(path: &Path, truncate: bool) -> Result<File, String> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if !metadata.is_file() {
            return Err("VastAI state file must be regular".to_owned());
        }
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map_err(|error| format!("open VastAI private state: {error}"))
}

fn load_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("inspect VastAI state: {error}")),
    };
    if !metadata.is_file() || metadata.len() > MAX_BODY as u64 {
        return Err("invalid VastAI state file".to_owned());
    }
    let bytes = fs::read(path).map_err(|error| format!("read VastAI state: {error}"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("decode VastAI state: {error}"))
}

fn persist_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let pending = path.with_extension("pending");
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    let mut file = private_file(&pending, true)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("commit VastAI state: {error}"))?;
    fs::rename(&pending, path).map_err(|error| format!("publish VastAI state: {error}"))?;
    sync_existing(path)
}

fn sync_existing(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| error.to_string())?;
    #[cfg(test)]
    if FAIL_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err("injected directory sync failure after publication".to_owned());
    }
    File::open(path.parent().ok_or("VastAI state has no parent")?)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync VastAI state directory: {error}"))
}

fn remove_durable(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => File::open(path.parent().ok_or("VastAI state has no parent")?)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove VastAI state: {error}")),
    }
}

fn register_webhook(
    config: &VastAiWebhookConfig,
    api_key: &str,
    fingerprint: String,
) -> Result<Registration, String> {
    let body = serde_json::to_string(&serde_json::json!({
        "name": "Myelin provider events", "webhook_url": config.public_url,
        "event_types": config.event_types,
    }))
    .map_err(|error| error.to_string())?;
    let response = ureq::post(API).set("Authorization", &format!("Bearer {api_key}"))
        .set("Content-Type", "application/json").send_string(&body)
        .map_err(|_| "VastAI webhook registration failed or has unknown outcome; recover registration before retrying".to_owned())?;
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take((MAX_BODY + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read webhook registration response: {error}"))?;
    if bytes.len() > MAX_BODY {
        return Err("oversized VastAI webhook registration response".to_owned());
    }
    let response: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid webhook registration response")?;
    if response.get("success").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err("VastAI did not confirm webhook registration".to_owned());
    }
    let webhook = response
        .get("webhook")
        .ok_or("registration response has no webhook")?;
    let user_id = webhook
        .get("user_id")
        .and_then(serde_json::Value::as_u64)
        .filter(|id| *id != 0)
        .ok_or("registration response has no account user ID")?;
    if config.user_id.is_some_and(|expected| expected != user_id) {
        return Err("registered webhook belongs to an unexpected account".to_owned());
    }
    let public_url = webhook
        .get("webhook_url")
        .and_then(serde_json::Value::as_str)
        .ok_or("registration response has no URL")?;
    let event_types: BTreeSet<String> = serde_json::from_value(
        webhook
            .get("event_types")
            .cloned()
            .ok_or("registration response has no event keys")?,
    )
    .map_err(|_| "invalid registered event keys")?;
    if public_url != config.public_url
        || event_types != config.event_types.iter().cloned().collect()
    {
        return Err(
            "registered webhook endpoint or events differ from requested configuration".to_owned(),
        );
    }
    let signing_secret = webhook
        .get("webhook_secret")
        .and_then(serde_json::Value::as_str)
        .filter(|secret| !secret.is_empty())
        .ok_or("registration response has no signing secret")?
        .to_owned();
    let webhook_id = webhook
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .filter(|id| *id != 0)
        .ok_or("registration response has no webhook ID")?;
    Ok(Registration {
        public_url: public_url.to_owned(),
        event_types,
        user_id,
        signing_secret,
        api_key_fingerprint: fingerprint,
        webhook_id: Some(webhook_id),
    })
}

#[cfg(test)]
thread_local! {
    static FAIL_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn config(directory: &Path) -> VastAiWebhookConfig {
        VastAiWebhookConfig {
            public_url: format!("https://receiver.example{WEBHOOK_PATH}"),
            event_types: vec!["client:instance_started".to_owned()],
            state_dir: directory.to_owned(),
            signing_secret: Some("fixture-secret".to_owned()),
            user_id: Some(17),
            register: false,
        }
    }

    fn signed(body: &[u8], event: &str) -> HeaderMap {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"fixture-secret").unwrap();
        mac.update(b"1.");
        mac.update(body);
        let mut headers = HeaderMap::new();
        headers.insert("x-vast-timestamp", "1".parse().unwrap());
        headers.insert("x-vast-event-id", event.parse().unwrap());
        headers.insert(
            "x-vast-signature-256",
            format!("sha256={}", encode_hex(&mac.finalize().into_bytes()))
                .parse()
                .unwrap(),
        );
        headers
    }

    fn counted(
        leaf: &Arc<VastAiWebhooks>,
        label: &str,
    ) -> (Arc<AtomicUsize>, VastAiWebhookSubscription) {
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let subscription = leaf.subscribe(
            label,
            Arc::new(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                true
            }),
        );
        (count, subscription)
    }

    fn bearer(leaf: &VastAiWebhooks, label: &str) -> HeaderMap {
        let environment = leaf.bootstrap_environment(label).unwrap();
        let token = environment
            .iter()
            .find(|(key, _)| key == "MYELIN_VASTAI_READY_TOKEN")
            .unwrap()
            .1
            .clone();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
        headers
    }

    #[test]
    fn webhook_failed_directory_sync_retries_the_unnotified_event_and_deduplicates_restart() {
        let directory = tempfile::tempdir().unwrap();
        let leaf = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        let (count, subscription) = counted(&leaf, "lease-a");
        let body = br#"{"event_id":"one","user_id":17,"notif_type":"instance_started"}"#;
        let headers = signed(body, "one");
        FAIL_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert_eq!(
            leaf.accept_webhook(&headers, body),
            Err(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        leaf.accept_webhook(&headers, body).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        leaf.accept_webhook(&headers, body).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        drop(subscription);
        drop(leaf);
        let restored = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        let (count, _subscription) = counted(&restored, "lease-a");
        restored.accept_webhook(&headers, body).unwrap();
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "restart reconciliation must not turn a duplicate into another event"
        );
    }

    #[test]
    fn readiness_failed_directory_sync_preserves_the_only_invalidation() {
        let directory = tempfile::tempdir().unwrap();
        let leaf = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        let headers = bearer(&leaf, "lease-a");
        let (count, _subscription) = counted(&leaf, "lease-a");
        FAIL_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert_eq!(
            leaf.accept_ready(&headers, b"{}"),
            Err(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        leaf.accept_ready(&headers, b"{}").unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        leaf.accept_ready(&headers, b"{}").unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn readiness_credentials_are_label_scoped_and_remain_revoked_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let leaf = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        let a = bearer(&leaf, "lease-a");
        let b = bearer(&leaf, "lease-b");
        let (count_a, subscription_a) = counted(&leaf, "lease-a");
        let (count_b, subscription_b) = counted(&leaf, "lease-b");
        assert_eq!(
            leaf.accept_ready(&a, br#"{"label":"lease-b"}"#),
            Err(StatusCode::BAD_REQUEST)
        );
        leaf.accept_ready(&a, b"{}").unwrap();
        assert_eq!(count_a.load(Ordering::SeqCst), 2);
        assert_eq!(count_b.load(Ordering::SeqCst), 1);
        leaf.revoke_bootstrap("lease-a").unwrap();
        assert_eq!(leaf.accept_ready(&a, b"{}"), Err(StatusCode::UNAUTHORIZED));
        leaf.accept_ready(&b, b"{}").unwrap();
        assert_eq!(count_b.load(Ordering::SeqCst), 2);
        drop((subscription_a, subscription_b, leaf));
        let restored = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        assert_eq!(
            restored.accept_ready(&a, b"{}"),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert!(restored.bootstrap_environment("lease-a").is_err());
    }

    #[test]
    fn webhook_authentication_and_account_scope_precede_durable_acceptance() {
        let directory = tempfile::tempdir().unwrap();
        let leaf = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        let (count, _subscription) = counted(&leaf, "lease");
        let body = br#"{"event_id":"one","user_id":17,"notif_type":"instance_started"}"#;
        let headers = signed(body, "one");
        assert_eq!(
            leaf.accept_webhook(&headers, b"{}"),
            Err(StatusCode::UNAUTHORIZED)
        );
        let other_account = br#"{"event_id":"one","user_id":18,"notif_type":"instance_started"}"#;
        assert_eq!(
            leaf.accept_webhook(&signed(other_account, "one"), other_account),
            Err(StatusCode::FORBIDDEN)
        );
        assert_eq!(
            leaf.accept_webhook(&signed(body, "different"), body),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        leaf.accept_webhook(&headers, body).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let changed = br#"{"event_id":"one","user_id":17,"notif_type":"instance_started","message":"different"}"#;
        assert_eq!(
            leaf.accept_webhook(&signed(changed, "one"), changed),
            Err(StatusCode::CONFLICT)
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn persisted_receiver_rejects_changed_account_credentials_and_concurrent_owners() {
        let directory = tempfile::tempdir().unwrap();
        let leaf = VastAiWebhooks::open(config(directory.path()), "account-key").unwrap();
        assert!(VastAiWebhooks::open(config(directory.path()), "account-key").is_err());
        drop(leaf);
        assert!(VastAiWebhooks::open(config(directory.path()), "another-key").is_err());
        let mut wrong_account = config(directory.path());
        wrong_account.user_id = Some(18);
        assert!(VastAiWebhooks::open(wrong_account, "account-key").is_err());
        assert!(VastAiWebhooks::open(config(directory.path()), "account-key").is_ok());
    }
}
