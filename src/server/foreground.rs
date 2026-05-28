//! `GET /foreground/{name}` — bridge a foreground task's PTY to a client.
//!
//! A foreground task (`terminal = "foreground"`) runs on its own PTY inside the
//! daemon, which has no controlling terminal of its own. The runner parks the
//! PTY master in the [`super::ForegroundRegistry`]; this handler claims it on
//! an HTTP upgrade and copies bytes both ways — exactly like `don attach`, but
//! the far side is the task's PTY rather than a reclaimed service PTY.
//!
//! `don run <task>` connects here for a foreground task; an attached `don tui`
//! connects here after a `ForegroundWaiting` event (releasing its own terminal
//! first). Resize uses the shared `/attach/{name}/resize` endpoint.

use super::ApiState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

/// Query params for the foreground bridge: the client's current terminal size.
#[derive(Deserialize)]
pub(crate) struct ForegroundParams {
    cols: u16,
    rows: u16,
}

/// `GET /foreground/{name}?cols=C&rows=R` — upgrade to a raw PTY bridge.
pub(crate) async fn foreground_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    Query(params): Query<ForegroundParams>,
    request: axum::http::Request<axum::body::Body>,
) -> Response {
    let upgrade_ok = request
        .headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("don-foreground"));
    if !upgrade_ok {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": "expected Upgrade: don-foreground"})),
        )
            .into_response();
    }

    // Claim the parked PTY session. A 404 means the task hasn't reached the
    // foreground-spawn point yet (or already exited) — the client retries.
    let session = {
        let mut map = state.foreground.lock().await;
        map.remove(&name)
    };
    let Some(session) = session else {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({
                "error": format!("no foreground task '{name}' waiting for a terminal")
            })),
        )
            .into_response();
    };

    let write = session.write;
    let read = session.read;
    let _ = write.resize(pty_process::Size::new(params.rows, params.cols));

    // Reuse the attach resize map + `/attach/{name}/resize` endpoint.
    let (resize_tx, resize_rx) = mpsc::channel::<(u16, u16)>(4);
    {
        let mut map = state.attach_resize_txs.lock().await;
        map.insert(name.clone(), resize_tx);
    }

    let state_clone = state.clone();
    let name_clone = name.clone();
    tokio::spawn(async move {
        let upgraded = match hyper::upgrade::on(request).await {
            Ok(upgraded) => upgraded,
            Err(_) => {
                state_clone.attach_resize_txs.lock().await.remove(&name_clone);
                return;
            }
        };
        let io = hyper_util::rt::TokioIo::new(upgraded);
        bridge_pty(io, read, write, resize_rx).await;
        state_clone.attach_resize_txs.lock().await.remove(&name_clone);
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("connection", "Upgrade")
        .header("upgrade", "don-foreground")
        .body(axum::body::Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Copy bytes both ways between the client connection and the task PTY until
/// either side closes (task exits → PTY EOF; client detaches → read 0).
async fn bridge_pty<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    io: S,
    mut read_pty: pty_process::OwnedReadPty,
    mut write_pty: pty_process::OwnedWritePty,
    mut resize_rx: mpsc::Receiver<(u16, u16)>,
) {
    let (mut io_read, mut io_write) = tokio::io::split(io);

    // PTY output → client.
    let pty_to_client = async move {
        let mut buf = [0u8; 8192];
        loop {
            match read_pty.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if io_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    };

    // Client input + resize → PTY.
    let client_to_pty = async move {
        let mut buf = [0u8; 8192];
        loop {
            tokio::select! {
                result = io_read.read(&mut buf) => match result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if write_pty.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                },
                Some((cols, rows)) = resize_rx.recv() => {
                    let _ = write_pty.resize(pty_process::Size::new(rows, cols));
                }
            }
        }
    };

    tokio::select! {
        _ = pty_to_client => {}
        _ = client_to_pty => {}
    }
}
