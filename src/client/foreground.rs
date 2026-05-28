//! Client side of foreground-task forwarding.
//!
//! A `terminal = "foreground"` task runs on a PTY inside the daemon; this
//! bridges the caller's real terminal to that PTY over the socket. Unlike
//! `don attach`, Ctrl+C is *not* a detach gesture — it passes through to the
//! interactive task. The session ends when the task exits (the daemon closes
//! the stream), restoring the terminal via the raw-mode guard.

use super::ClientError;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

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

/// Bridge the caller's terminal to a foreground task's PTY.
///
/// Triggering the task (`POST /run/:name`) is the caller's job; this connects
/// to `GET /foreground/:name` and forwards bytes until the task exits. The
/// task spawns a moment after the run command is accepted, so a not-found
/// response is retried briefly.
pub async fn run_foreground(socket_path: &Path, name: &str) -> Result<(), ClientError> {
    let (mut stream, leftover) = connect_with_retry(socket_path, name).await?;

    let _guard = RawModeGuard::enable()?;
    if !leftover.is_empty() {
        let mut stdout = tokio::io::stdout();
        let _ = stdout.write_all(&leftover).await;
        let _ = stdout.flush().await;
    }
    bridge_terminal(&mut stream, socket_path, name).await
}

/// Connect + upgrade, retrying while the daemon reports the task isn't waiting
/// yet (it spawns just after the run command is accepted).
async fn connect_with_retry(
    socket_path: &Path,
    name: &str,
) -> Result<(UnixStream, Vec<u8>), ClientError> {
    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let path = format!(
        "/foreground/{}?cols={cols}&rows={rows}",
        super::urlencode(name),
    );
    let mut attempt = 0;
    loop {
        let mut stream = match UnixStream::connect(socket_path).await {
            Ok(s) => s,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                return Err(ClientError::NotRunning {
                    path: socket_path.to_path_buf(),
                });
            }
            Err(e) => return Err(ClientError::Io(e)),
        };

        let req = format!(
            "GET {path} HTTP/1.1\r\n\
             Host: localhost\r\n\
             Connection: Upgrade\r\n\
             Upgrade: don-foreground\r\n\
             \r\n"
        );
        stream.write_all(req.as_bytes()).await?;
        let (status, headers, leftover) = super::read_head(&mut stream).await?;
        if status == 101 {
            return Ok((stream, leftover));
        }
        let body = super::drain_body(&mut stream, &headers, leftover).await?;
        // 404 = task hasn't reached its foreground spawn yet; retry briefly.
        if status == 404 && attempt < 50 {
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return Err(super::classify_error(status, &body));
    }
}

/// Bridge stdin↔stream↔stdout until the task exits or stdin closes. Resize
/// events go through the shared `/attach/:name/resize` endpoint.
async fn bridge_terminal(
    stream: &mut UnixStream,
    socket_path: &Path,
    name: &str,
) -> Result<(), ClientError> {
    let (mut stream_read, mut stream_write) = stream.split();
    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    let (resize_tx, mut resize_rx) = tokio::sync::mpsc::channel::<(u16, u16)>(4);
    let resize_handle = tokio::spawn({
        use futures_util::StreamExt;
        async move {
            let mut reader = crossterm::event::EventStream::new();
            while let Some(Ok(event)) = reader.next().await {
                if let crossterm::event::Event::Resize(cols, rows) = event
                    && resize_tx.send((cols, rows)).await.is_err()
                {
                    break;
                }
            }
        }
    });

    let socket_path = socket_path.to_path_buf();
    let name = name.to_string();
    let mut stdin_buf = [0u8; 4096];
    let mut stream_buf = [0u8; 8192];
    let result = loop {
        tokio::select! {
            // stdin → task PTY (Ctrl+C included — it's the task's to handle)
            read_result = stdin.read(&mut stdin_buf) => {
                match read_result {
                    Ok(0) => break Ok(()),
                    Ok(n) => {
                        if stream_write.write_all(&stdin_buf[..n]).await.is_err() {
                            break Ok(()); // task exited
                        }
                    }
                    Err(e) => break Err(ClientError::Io(e)),
                }
            }
            // task PTY → stdout; EOF means the task exited.
            read_result = stream_read.read(&mut stream_buf) => {
                match read_result {
                    Ok(0) => break Ok(()),
                    Ok(n) => {
                        if stdout.write_all(&stream_buf[..n]).await.is_err() {
                            break Ok(());
                        }
                        let _ = stdout.flush().await;
                    }
                    Err(_) => break Ok(()),
                }
            }
            Some((cols, rows)) = resize_rx.recv() => {
                let sp = socket_path.clone();
                let n = name.clone();
                tokio::spawn(async move {
                    let _ = send_resize(&sp, &n, cols, rows).await;
                });
            }
        }
    };

    resize_handle.abort();
    result
}

/// Send a resize via the shared attach resize endpoint.
async fn send_resize(socket_path: &Path, name: &str, cols: u16, rows: u16) -> Result<(), ClientError> {
    let mut stream = UnixStream::connect(socket_path).await?;
    let body = serde_json::json!({ "cols": cols, "rows": rows });
    let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
    let path = format!("/attach/{}/resize", super::urlencode(name));
    let req = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body_bytes.len(),
    );
    stream.write_all(req.as_bytes()).await?;
    stream.write_all(&body_bytes).await?;
    let mut buf = [0u8; 256];
    let _ = stream.read(&mut buf).await;
    Ok(())
}
