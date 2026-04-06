//! WebSocket handler for interactive attach sessions.
//!
//! Bridges a WebSocket connection to a service's PTY: binary frames carry
//! raw stdin/stdout bytes, text frames carry JSON control messages (init,
//! resize). The handler holds the PTY write half for the duration of the
//! session and returns it to the runner via a `Detach` command on disconnect.

use super::ApiState;
use crate::runner::RunnerCommand;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, State, WebSocketUpgrade};
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

/// `GET /attach/:name` — upgrade to a WebSocket for interactive PTY access.
pub(crate) async fn ws_attach(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Response {
    ws.on_upgrade(move |socket| handle_attach(socket, state, name))
}

/// JSON control messages sent by the CLI over text frames.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlMessage {
    /// Initial handshake — CLI sends its PID.
    Init { pid: u32 },
    /// Terminal resize event.
    Resize { cols: u16, rows: u16 },
}

/// Drive the attach session after the WebSocket upgrade completes.
async fn handle_attach(mut socket: WebSocket, state: Arc<ApiState>, name: String) {
    // Step 1: Wait for the init message with the client PID.
    let pid = match wait_for_init(&mut socket).await {
        Some(pid) => pid,
        None => return,
    };

    // Step 2: Request an attach session from the runner.
    let (tx, rx) = oneshot::channel();
    if state
        .cmd_tx
        .send(RunnerCommand::Attach {
            name: name.clone(),
            pid,
            reply: tx,
        })
        .await
        .is_err()
    {
        let _ = send_error(&mut socket, "runner is shutting down").await;
        return;
    }

    let session = match rx.await {
        Ok(Ok(session)) => session,
        Ok(Err(e)) => {
            let _ = send_error(&mut socket, &e.to_string()).await;
            return;
        }
        Err(_) => {
            let _ = send_error(&mut socket, "runner is shutting down").await;
            return;
        }
    };

    // Step 3: Bridge the WebSocket to the PTY.
    let pty_write = bridge(socket, session.pty_write, session.output_rx).await;

    // Step 4: Detach — return the PTY write handle to the runner.
    let _ = state
        .cmd_tx
        .send(RunnerCommand::Detach {
            name,
            pty_write,
        })
        .await;
}

/// Wait for the client's init message. Returns the PID, or None on error.
async fn wait_for_init(socket: &mut WebSocket) -> Option<u32> {
    // Give the client 5 seconds to send the init message.
    let msg = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.recv(),
    )
    .await
    {
        Ok(Some(Ok(Message::Text(text)))) => text,
        _ => {
            let _ = send_error(socket, "expected init message within 5 seconds").await;
            return None;
        }
    };

    match serde_json::from_str::<ControlMessage>(&msg) {
        Ok(ControlMessage::Init { pid }) => Some(pid),
        _ => {
            let _ = send_error(socket, "expected {\"type\":\"init\",\"pid\":N} as first message").await;
            None
        }
    }
}

/// Bridge WebSocket ↔ PTY until one side closes. Returns the PTY write
/// handle (Some if still valid, None if the PTY was dropped).
async fn bridge(
    socket: WebSocket,
    mut pty_write: pty_process::OwnedWritePty,
    mut output_rx: tokio::sync::mpsc::Receiver<crate::output::SinkLine>,
) -> Option<pty_process::OwnedWritePty> {
    let (ws_tx, ws_rx) = socket.split();
    let mut ws_tx: futures_util::stream::SplitSink<WebSocket, Message> = ws_tx;
    let mut ws_rx: futures_util::stream::SplitStream<WebSocket> = ws_rx;

    // Task: PTY output → WebSocket (as binary frames with \r\n appended).
    let output_to_ws = async move {
        while let Some(sink_line) = output_rx.recv().await {
            let mut data = sink_line.line.to_vec();
            data.extend_from_slice(b"\r\n");
            if ws_tx.send(Message::Binary(data.into())).await.is_err() {
                break;
            }
        }
    };

    // Task: WebSocket → PTY stdin (binary) + control messages (text).
    let ws_to_pty = async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(data) => {
                    if pty_write.write_all(&data).await.is_err() {
                        break;
                    }
                }
                Message::Text(text) => {
                    if let Ok(ctrl) = serde_json::from_str::<ControlMessage>(&text) {
                        match ctrl {
                            ControlMessage::Resize { cols, rows } => {
                                let _ = pty_write.resize(pty_process::Size::new(rows, cols));
                            }
                            ControlMessage::Init { .. } => {
                                // Ignore duplicate init messages.
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        // Return the pty_write so it can be given back to the runner.
        pty_write
    };

    // Run both tasks. The first to complete cancels the other.
    tokio::select! {
        _ = output_to_ws => {
            // Output stream ended (service died or sinks cleared).
            // We don't have pty_write here — it's in ws_to_pty. Drop it.
            None
        }
        pty = ws_to_pty => {
            // Client disconnected or PTY write failed.
            Some(pty)
        }
    }
}

/// Send a JSON error message and close the WebSocket.
async fn send_error(socket: &mut WebSocket, message: &str) -> Result<(), axum::Error> {
    let json = serde_json::json!({ "type": "error", "message": message });
    let text = serde_json::to_string(&json).unwrap_or_default();
    socket.send(Message::Text(text.into())).await
}
