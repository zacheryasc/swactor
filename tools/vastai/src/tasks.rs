use swactor_engine::EngineHandle;

use crate::{ProviderInstanceStatus, VastClient};

/// Cancels one engine-owned Vast.ai I/O task. Dropping the handle also cancels it.
pub struct VastTaskCancellation {
    sender: Option<tokio::sync::watch::Sender<bool>>,
}

impl VastTaskCancellation {
    pub fn cancel(mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(true);
        }
    }
}

impl Drop for VastTaskCancellation {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(true);
        }
    }
}

#[derive(Debug)]
pub enum VastLogStreamEvent {
    Chunk(Vec<u8>),
    Finished {
        accepted_bytes: usize,
        truncated: bool,
        error: Option<String>,
    },
}

pub fn spawn_instance_status_task<F>(
    engine: &EngineHandle,
    client: VastClient,
    contract_id: u64,
    complete: F,
) -> VastTaskCancellation
where
    F: FnOnce(Result<ProviderInstanceStatus, String>) + Send + 'static,
{
    let (sender, mut cancelled) = tokio::sync::watch::channel(false);
    engine.spawn(async move {
        let result = tokio::select! {
            biased;
            _ = cancelled.changed() => return,
            result = client.instance_status(contract_id) => result,
        };
        complete(result);
    });
    VastTaskCancellation {
        sender: Some(sender),
    }
}

pub fn spawn_log_stream_task<F>(
    engine: &EngineHandle,
    client: VastClient,
    contract_id: u64,
    max_response_bytes: usize,
    max_chunk_bytes: usize,
    mut observe: F,
) -> VastTaskCancellation
where
    F: FnMut(VastLogStreamEvent) -> bool + Send + 'static,
{
    let (sender, mut cancelled) = tokio::sync::watch::channel(false);
    engine.spawn(async move {
        let result_url = tokio::select! {
            biased;
            _ = cancelled.changed() => return,
            result = client.request_logs(contract_id) => result,
        };
        let result_url = match result_url {
            Ok(url) => url,
            Err(error) => {
                observe(VastLogStreamEvent::Finished {
                    accepted_bytes: 0,
                    truncated: false,
                    error: Some(error),
                });
                return;
            }
        };
        let stream = tokio::select! {
            biased;
            _ = cancelled.changed() => return,
            result = client.open_log_stream(&result_url) => result,
        };
        let mut stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                observe(VastLogStreamEvent::Finished {
                    accepted_bytes: 0,
                    truncated: false,
                    error: Some(error),
                });
                return;
            }
        };
        let mut accepted_bytes = 0;
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancelled.changed() => return,
                result = stream.next_chunk() => result,
            };
            match chunk {
                Ok(Some(mut bytes)) => {
                    let remaining = max_response_bytes.saturating_sub(accepted_bytes);
                    let truncated = bytes.len() > remaining;
                    bytes.truncate(remaining);
                    accepted_bytes = accepted_bytes.saturating_add(bytes.len());
                    for chunk in bytes.chunks(max_chunk_bytes.max(1)) {
                        if !observe(VastLogStreamEvent::Chunk(chunk.to_vec())) {
                            return;
                        }
                    }
                    if truncated || accepted_bytes == max_response_bytes {
                        observe(VastLogStreamEvent::Finished {
                            accepted_bytes,
                            truncated: true,
                            error: None,
                        });
                        return;
                    }
                }
                Ok(None) => {
                    observe(VastLogStreamEvent::Finished {
                        accepted_bytes,
                        truncated: false,
                        error: None,
                    });
                    return;
                }
                Err(error) => {
                    observe(VastLogStreamEvent::Finished {
                        accepted_bytes,
                        truncated: false,
                        error: Some(error),
                    });
                    return;
                }
            }
        }
    });
    VastTaskCancellation {
        sender: Some(sender),
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use std::sync::mpsc;

    use serde_json::json;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, TokioBackend, TokioConfig};

    use crate::test_http::{TestHttpRoute, TestHttpServer};

    use super::*;

    fn test_engine() -> Engine {
        Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            TokioBackend::new(TokioConfig::default()).expect("Tokio backend"),
        )
        .expect("actor engine")
    }

    fn api_server(log_url: &str) -> TestHttpServer {
        TestHttpServer::start(vec![
            TestHttpRoute::json(
                "PUT",
                "/api/v0/instances/request_logs/73/",
                200,
                json!({"result_url": log_url}),
            ),
            TestHttpRoute::json(
                "GET",
                "/api/v0/instances/73/",
                200,
                json!({"instances": {
                    "actual_status": "running",
                    "intended_status": "running"
                }}),
            ),
        ])
        .expect("API server")
    }

    #[test]
    fn open_log_stream_does_not_block_status_and_is_cancellable() {
        let log_server = TestHttpServer::start(vec![TestHttpRoute::open_raw(
            "GET",
            "/logs",
            200,
            b"first line\n".to_vec(),
        )])
        .expect("log server");
        let api_server = api_server(&format!("{}/logs", log_server.uri()));
        let client = VastClient::with_base_url(api_server.uri(), "secret");
        let engine = test_engine();
        let (log_tx, log_rx) = mpsc::channel();
        let log_cancel = spawn_log_stream_task(
            &engine.handle(),
            client.clone(),
            73,
            1024,
            1024,
            move |event| log_tx.send(event).is_ok(),
        );
        assert!(matches!(
            log_rx.recv(),
            Ok(VastLogStreamEvent::Chunk(bytes)) if bytes == b"first line\n"
        ));

        let (status_tx, status_rx) = mpsc::channel();
        let _status_cancel =
            spawn_instance_status_task(&engine.handle(), client, 73, move |result| {
                let _ = status_tx.send(result);
            });
        let status = status_rx
            .recv()
            .expect("status completes while log response remains open")
            .expect("status succeeds");
        assert_eq!(status.actual_status, "running");
        log_cancel.cancel();
    }

    #[test]
    fn log_stream_enforces_response_and_message_caps() {
        let log_server = TestHttpServer::start(vec![TestHttpRoute::open_raw(
            "GET",
            "/logs",
            200,
            b"0123456789".to_vec(),
        )])
        .expect("log server");
        let api_server = api_server(&format!("{}/logs", log_server.uri()));
        let client = VastClient::with_base_url(api_server.uri(), "secret");
        let engine = test_engine();
        let (event_tx, event_rx) = mpsc::channel();
        let _cancel = spawn_log_stream_task(&engine.handle(), client, 73, 5, 3, move |event| {
            event_tx.send(event).is_ok()
        });
        let mut bytes = Vec::new();
        loop {
            match event_rx.recv().expect("bounded log event") {
                VastLogStreamEvent::Chunk(chunk) => {
                    assert!(chunk.len() <= 3);
                    bytes.extend_from_slice(&chunk);
                }
                VastLogStreamEvent::Finished {
                    accepted_bytes,
                    truncated,
                    error,
                } => {
                    assert_eq!(accepted_bytes, 5);
                    assert!(truncated);
                    assert_eq!(error, None);
                    break;
                }
            }
        }
        assert_eq!(bytes, b"01234");
    }
}
