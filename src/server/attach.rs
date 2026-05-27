//! Raw stream handler for interactive attach sessions.
//!
//! After an HTTP upgrade handshake, the connection becomes a raw bidirectional
//! byte stream — stdin bytes flow in, PTY output bytes flow out, with zero
//! framing overhead. Resize events arrive via a separate POST endpoint.

use super::ApiState;
use crate::runner::RunnerCommand;
use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

/// Query parameters for the attach upgrade request.
#[derive(Deserialize)]
pub(crate) struct AttachParams {
    pid: u32,
    cols: u16,
    rows: u16,
}

/// Body for the resize endpoint.
#[derive(Deserialize)]
pub(crate) struct ResizeBody {
    cols: u16,
    rows: u16,
}

/// `GET /attach/{name}?pid=N&cols=C&rows=R` — upgrade to raw stream.
///
/// Validates the `Upgrade: don-attach` header, requests an attach session
/// from the runner (blocking until the service/task is ready), then responds
/// with `101 Switching Protocols`. After the upgrade, the connection carries
/// raw stdin/stdout bytes with zero framing.
pub(crate) async fn attach_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(params): Query<AttachParams>,
    request: axum::http::Request<axum::body::Body>,
) -> Response {
    // Validate upgrade header.
    let upgrade_ok = request
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("don-attach"));
    if !upgrade_ok {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": "expected Upgrade: don-attach"})),
        )
            .into_response();
    }

    // Request attach session from runner.
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Attach {
            name: name.clone(),
            pid: params.pid,
            reply: tx,
        })
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({"error": "runner is shutting down"})),
        )
            .into_response();
    }

    let session = match rx.await {
        Ok(Ok(session)) => session,
        Ok(Err(e)) => {
            let status = match e {
                crate::runner::CommandError::UnknownService { .. } => StatusCode::NOT_FOUND,
                crate::runner::CommandError::InvalidState { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return (
                status,
                axum::Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({"error": "runner is shutting down"})),
            )
                .into_response();
        }
    };

    let pty_write = session.pty_write;
    let output_rx = session.output_rx;

    // Apply initial resize.
    let _ = pty_write.resize(pty_process::Size::new(params.rows, params.cols));

    // Register resize channel.
    let (resize_tx, resize_rx) = mpsc::channel::<(u16, u16)>(4);
    {
        let mut map = state.attach_resize_txs.lock().await;
        map.insert(name.clone(), resize_tx);
    }

    // Spawn background task to handle the upgraded connection.
    let state_clone = state.clone();
    let name_clone = name.clone();
    tokio::spawn(async move {
        let upgraded = match hyper::upgrade::on(request).await {
            Ok(upgraded) => upgraded,
            Err(_) => {
                // Upgrade failed — clean up.
                let mut map = state_clone.attach_resize_txs.lock().await;
                map.remove(&name_clone);
                let _ = state_clone.cmd_tx.send(RunnerCommand::Detach {
                    name: name_clone,
                    pty_write: Some(pty_write),
                });
                return;
            }
        };

        let io = hyper_util::rt::TokioIo::new(upgraded);
        let pty_back = bridge_raw(io, pty_write, output_rx, resize_rx).await;

        // Clean up resize channel.
        {
            let mut map = state_clone.attach_resize_txs.lock().await;
            map.remove(&name_clone);
        }

        // Return PTY write handle to runner.
        let _ = state_clone.cmd_tx.send(RunnerCommand::Detach {
            name: name_clone,
            pty_write: pty_back,
        });
    });

    // Return 101 Switching Protocols.
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "Upgrade")
        .header("upgrade", "don-attach")
        .body(axum::body::Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `POST /attach/{name}/resize` — resize the attached PTY.
pub(crate) async fn resize_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Json(body): Json<ResizeBody>,
) -> Response {
    let map = state.attach_resize_txs.lock().await;
    match map.get(&name) {
        Some(tx) => {
            let _ = tx.try_send((body.cols, body.rows));
            StatusCode::NO_CONTENT.into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(
                serde_json::json!({"error": format!("no active attach session for '{name}'")}),
            ),
        )
            .into_response(),
    }
}

/// Bridge a raw bidirectional stream to the PTY. Returns the PTY write
/// handle (Some if still valid, None if dropped).
async fn bridge_raw<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    io: S,
    mut pty_write: pty_process::OwnedWritePty,
    mut output_rx: mpsc::Receiver<crate::output::SinkLine>,
    mut resize_rx: mpsc::Receiver<(u16, u16)>,
) -> Option<pty_process::OwnedWritePty> {
    let (mut io_read, mut io_write) = tokio::io::split(io);

    // Channel for OSC responses detected in the output stream.
    let (osc_tx, mut osc_rx) = mpsc::channel::<bytes::Bytes>(8);

    // Task: output_rx → client (raw bytes) + OSC detection.
    let output_to_client = async move {
        while let Some(sink_line) = output_rx.recv().await {
            for response in crate::output::osc::find_responses(&sink_line.line) {
                let _ = osc_tx.try_send(bytes::Bytes::from_static(response));
            }
            if io_write.write_all(&sink_line.line).await.is_err() {
                break;
            }
        }
    };

    // Task: client → PTY (raw bytes) + OSC responses + resize.
    let client_to_pty = async move {
        let mut buf = [0u8; 4096];
        loop {
            tokio::select! {
                result = io_read.read(&mut buf) => {
                    match result {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if pty_write.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Some(data) = osc_rx.recv() => {
                    let _ = pty_write.write_all(&data).await;
                }
                Some((cols, rows)) = resize_rx.recv() => {
                    let _ = pty_write.resize(pty_process::Size::new(rows, cols));
                }
            }
        }
        pty_write
    };

    tokio::select! {
        _ = output_to_client => None,
        pty = client_to_pty => Some(pty),
    }
}
