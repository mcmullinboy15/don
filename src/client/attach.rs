//! Client-side interactive attach — connects to the daemon's WebSocket
//! endpoint, puts the terminal in raw mode, and bridges stdin/stdout.
//!
//! Ctrl+C or Ctrl+D detaches without killing the service. A `Drop` guard
//! ensures raw mode is always restored. If the server disconnects (e.g.
//! task rerun), the client automatically reconnects.

use super::ClientError;
use futures_util::{SinkExt, StreamExt};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// RAII guard that restores the terminal from raw mode on drop.
struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> Result<Self, ClientError> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|e| ClientError::Io(std::io::Error::other(format!("enable raw mode: {e}"))))?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Send a JSON text message over the WebSocket.
fn text_msg(json: &serde_json::Value) -> Message {
    Message::Text(json.to_string().into())
}

/// Why the bridge loop exited.
enum DisconnectReason {
    /// User pressed Ctrl+C or Ctrl+D.
    UserDetach,
    /// Server closed the connection (rerun, restart, shutdown).
    ServerDisconnect,
    /// An error occurred.
    Error(ClientError),
}

/// Connect to the daemon and run an interactive attach session.
///
/// Puts the terminal in raw mode, bridges stdin↔WebSocket↔PTY and
/// PTY↔WebSocket↔stdout. Auto-reconnects when the server disconnects
/// (e.g. task rerun or service restart). Returns when the user detaches
/// with Ctrl+C/Ctrl+D.
pub async fn run_attach(socket_path: &Path, name: &str) -> Result<(), ClientError> {
    // Enter raw mode once, keep it for the entire session including reconnects.
    let _guard = RawModeGuard::enable()?;

    loop {
        match attach_once(socket_path, name).await {
            DisconnectReason::UserDetach => return Ok(()),
            DisconnectReason::Error(e) => return Err(e),
            DisconnectReason::ServerDisconnect => {
                // Write a notice — the next attach_once will block on the
                // server side until the process starts again.
                let mut stdout = tokio::io::stdout();
                let _ = stdout.write_all(b"\r\n[waiting for process...]\r\n").await;
                let _ = stdout.flush().await;
            }
        }
    }
}

/// Run a single attach session. Returns the reason for disconnection.
async fn attach_once(socket_path: &Path, name: &str) -> DisconnectReason {
    let stream = match UnixStream::connect(socket_path).await {
        Ok(s) => s,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return DisconnectReason::Error(ClientError::NotRunning {
                path: socket_path.to_path_buf(),
            });
        }
        Err(e) => return DisconnectReason::Error(ClientError::Io(e)),
    };

    // Perform WebSocket handshake over the Unix stream.
    let url_path = format!("/attach/{}", super::urlencode(name));
    let ws_url = format!("ws://localhost{url_path}");
    let req = match tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(&ws_url)
        .header("Host", "localhost")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(())
    {
        Ok(r) => r,
        Err(e) => return DisconnectReason::Error(ClientError::Invalid(format!("build ws request: {e}"))),
    };

    let (mut ws, _response) = match tokio_tungstenite::client_async(req, stream).await {
        Ok(r) => r,
        Err(e) => return DisconnectReason::Error(ClientError::Invalid(format!("websocket handshake failed: {e}"))),
    };

    // Send init message with our PID.
    let pid = std::process::id();
    let init = serde_json::json!({"type": "init", "pid": pid});
    if let Err(e) = ws.send(text_msg(&init)).await {
        return DisconnectReason::Error(ClientError::Io(std::io::Error::other(format!("send init: {e}"))));
    }

    // Check for an immediate error response from the server.
    if let Ok(Some(Ok(msg))) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        ws.next(),
    )
    .await
    {
        if let Message::Text(ref text) = msg
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(text)
            && v.get("type").and_then(|t| t.as_str()) == Some("error")
        {
            let error_msg = v
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("attach failed");
            return DisconnectReason::Error(ClientError::Server {
                status: 0,
                message: error_msg.to_string(),
            });
        }
        // Not an error — it's a data frame (ring buffer replay). Write to stdout.
        if let Message::Binary(ref data) = msg {
            let mut stdout = tokio::io::stdout();
            let _ = stdout.write_all(data).await;
            let _ = stdout.flush().await;
        }
    }

    // Send initial terminal size.
    if let Ok((cols, rows)) = crossterm::terminal::size() {
        let resize = serde_json::json!({"type": "resize", "cols": cols, "rows": rows});
        let _ = ws.send(text_msg(&resize)).await;
    }

    // Bridge stdin/stdout with the WebSocket.
    let reason = bridge_terminal(&mut ws).await;

    // Clean close.
    let _ = ws.close(None).await;
    reason
}

/// Check data for detach triggers: Ctrl+C (\x03) or Ctrl+D (\x04).
/// Returns true if a detach should occur.
fn should_detach(data: &[u8]) -> bool {
    data.iter().any(|b| matches!(b, 0x03 | 0x04))
}

/// Bridge stdin↔WebSocket↔stdout. Returns the reason for exiting.
async fn bridge_terminal(
    ws: &mut WebSocketStream<UnixStream>,
) -> DisconnectReason {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    // Resize event channel.
    let (resize_tx, mut resize_rx) = tokio::sync::mpsc::channel::<(u16, u16)>(4);

    // Spawn a task to watch for terminal resize events.
    let resize_handle = tokio::spawn(async move {
        let mut reader = crossterm::event::EventStream::new();
        while let Some(Ok(event)) = reader.next().await {
            if let crossterm::event::Event::Resize(cols, rows) = event
                && resize_tx.send((cols, rows)).await.is_err()
            {
                break;
            }
        }
    });

    let mut buf = [0u8; 4096];
    let reason = loop {
        tokio::select! {
            // stdin → WebSocket
            read_result = stdin.read(&mut buf) => {
                match read_result {
                    Ok(0) => break DisconnectReason::UserDetach,
                    Ok(n) => {
                        let data = &buf[..n];
                        if should_detach(data) {
                            break DisconnectReason::UserDetach;
                        }
                        let bytes: bytes::Bytes = bytes::Bytes::copy_from_slice(data);
                        if ws_tx.send(Message::Binary(bytes)).await.is_err() {
                            break DisconnectReason::ServerDisconnect;
                        }
                    }
                    Err(e) => break DisconnectReason::Error(ClientError::Io(e)),
                }
            }
            // WebSocket → stdout
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        if stdout.write_all(&data).await.is_err() {
                            break DisconnectReason::UserDetach;
                        }
                        let _ = stdout.flush().await;
                    }
                    Some(Ok(Message::Text(ref text))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(text)
                            && v.get("type").and_then(|t| t.as_str()) == Some("error")
                        {
                            let err_msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("error");
                            break DisconnectReason::Error(ClientError::Server {
                                status: 0,
                                message: err_msg.to_string(),
                            });
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break DisconnectReason::ServerDisconnect,
                    Some(Err(_)) => break DisconnectReason::ServerDisconnect,
                    _ => {}
                }
            }
            // Terminal resize → WebSocket
            Some((cols, rows)) = resize_rx.recv() => {
                let resize = serde_json::json!({"type": "resize", "cols": cols, "rows": rows});
                let _ = ws_tx.send(text_msg(&resize)).await;
            }
        }
    };

    resize_handle.abort();
    reason
}
