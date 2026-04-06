#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::Runner;
use futures_util::{SinkExt, StreamExt};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// Wait for the socket file to exist.
async fn wait_for_socket(path: &Path, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    while !path.exists() {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

/// Spawn a runner. Returns (socket_path, shutdown_tx, join_handle).
async fn spawn_runner(
    toml: &str,
    base_dir: &Path,
) -> (
    std::path::PathBuf,
    mpsc::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let config: Config = toml.parse().unwrap();
    config.validate(PLATFORM).unwrap();

    let service_configs: Vec<(&str, &LogConfig)> = config
        .services
        .iter()
        .map(|(n, s)| (n.as_str(), &s.log))
        .collect();
    let task_configs: Vec<(&str, &LogConfig)> = config
        .tasks
        .iter()
        .map(|(n, t)| (n.as_str(), &t.log))
        .collect();
    let all_configs: Vec<(&str, &LogConfig)> =
        service_configs.into_iter().chain(task_configs).collect();

    let output_manager = OutputManager::new(&all_configs, tokio::io::sink())
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(2);
    let runner = Runner::new(
        config,
        base_dir.join("don.toml"),
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
    )
    .await
    .unwrap();
    let socket_path = base_dir.join(".don").join("don.sock");
    let handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });
    (socket_path, shutdown_tx, handle)
}

/// Connect a WebSocket client to the daemon over the Unix socket.
/// Sends the init message and returns the WebSocket stream.
async fn ws_connect(
    socket_path: &Path,
    name: &str,
    pid: u32,
) -> tokio_tungstenite::WebSocketStream<UnixStream> {
    let stream = UnixStream::connect(socket_path).await.unwrap();
    let url = format!("ws://localhost/attach/{name}");
    let req = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(&url)
        .header("Host", "localhost")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(())
        .unwrap();

    let (mut ws, _resp) = tokio_tungstenite::client_async(req, stream)
        .await
        .unwrap();

    // Send init message with PID.
    let init = serde_json::json!({"type": "init", "pid": pid});
    ws.send(Message::Text(init.to_string().into()))
        .await
        .unwrap();
    ws
}

/// Read the next text or binary message, skipping pings/pongs. Returns None on close/timeout.
async fn next_msg(
    ws: &mut tokio_tungstenite::WebSocketStream<UnixStream>,
    timeout: Duration,
) -> Option<Message> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg @ (Message::Text(_) | Message::Binary(_))))) => return Some(msg),
            Ok(Some(Ok(Message::Ping(_)))) => continue,
            Ok(Some(Ok(Message::Pong(_)))) => continue,
            _ => return None,
        }
    }
}

/// Collect binary frames until timeout, returning concatenated bytes.
async fn collect_binary(
    ws: &mut tokio_tungstenite::WebSocketStream<UnixStream>,
    timeout: Duration,
) -> Vec<u8> {
    let mut all = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Binary(data)))) => all.extend_from_slice(&data),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            _ => break,
        }
    }
    all
}

// --- Integration tests ---

#[test]
fn integration_attach_send_input_and_receive_output() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-io");
        // Use `cat` as the service — it echoes stdin to stdout.
        let toml = ConfigBuilder::new()
            .add_custom_service("echoer", "cat", &[])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut ws = ws_connect(&socket, "echoer", 12345).await;

        // Drain any ring buffer replay lines (brief pause).
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send some input.
        ws.send(Message::Binary(b"hello\n".to_vec().into()))
            .await
            .unwrap();

        // Wait for the echo back.
        let output = collect_binary(&mut ws, Duration::from_secs(2)).await;
        let text = String::from_utf8_lossy(&output);
        assert!(
            text.contains("hello"),
            "expected echoed input in output; got: {text:?}"
        );

        // Close the WebSocket.
        let _ = ws.close(None).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_second_attach_rejected_with_pid() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-lock");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // First attach should succeed.
        let _ws1 = ws_connect(&socket, "keeper", 11111).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second attach should be rejected.
        let mut ws2 = ws_connect(&socket, "keeper", 22222).await;
        let msg = next_msg(&mut ws2, Duration::from_secs(2)).await;
        let text = match msg {
            Some(Message::Text(t)) => t.to_string(),
            other => panic!("expected text error message, got: {other:?}"),
        };
        assert!(
            text.contains("11111"),
            "error should mention first PID; got: {text}"
        );
        assert!(
            text.contains("attached"),
            "error should mention 'attached'; got: {text}"
        );

        let _ = ws2.close(None).await;
        // ws1 is still connected — don't close it yet.
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_second_attach_succeeds_after_first_disconnects() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-release");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // First attach.
        let mut ws1 = ws_connect(&socket, "keeper", 11111).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Disconnect first.
        let _ = ws1.close(None).await;
        drop(ws1);
        // Give the server time to process the disconnect and release the lock.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Second attach should succeed (no error message).
        let mut ws2 = ws_connect(&socket, "keeper", 22222).await;
        // If we get a binary frame (output) or timeout, it's a success.
        // If we get an error text frame, it's a failure.
        let msg = next_msg(&mut ws2, Duration::from_secs(1)).await;
        match msg {
            Some(Message::Text(t)) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap_or_default();
                assert_ne!(
                    v.get("type").and_then(|t| t.as_str()),
                    Some("error"),
                    "second attach should not get error: {t}"
                );
            }
            Some(Message::Binary(_)) | None => {
                // Binary frame (output) or timeout = success (connected, no error).
            }
            other => panic!("unexpected message: {other:?}"),
        }

        let _ = ws2.close(None).await;
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_service_keeps_running_after_detach() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-detach-alive");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Attach and then disconnect.
        let mut ws = ws_connect(&socket, "keeper", 12345).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = ws.close(None).await;
        drop(ws);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check that the service is still running via the status endpoint.
        let stream = UnixStream::connect(&socket).await.unwrap();
        let mut stream = stream;
        let req = "GET /status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        tokio::io::AsyncWriteExt::write_all(&mut stream, req.as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&response);
        assert!(
            body.contains("\"state\":\"running\"") || body.contains("\"state\":\"ready\""),
            "service should still be running after detach; got: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_attach_to_nonexistent_service_returns_error() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("attach-unknown");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut ws = ws_connect(&socket, "ghost", 12345).await;
        let msg = next_msg(&mut ws, Duration::from_secs(2)).await;
        let text = match msg {
            Some(Message::Text(t)) => t.to_string(),
            other => panic!("expected text error, got: {other:?}"),
        };
        assert!(
            text.contains("ghost"),
            "error should mention service name; got: {text}"
        );

        let _ = ws.close(None).await;
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_resize_propagates_to_subprocess_pty() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("attach-resize");
        // The script traps SIGWINCH (sent by the kernel when the PTY is
        // resized) and prints the new terminal size via `stty size`.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "resizer",
                "bash",
                &["-c", "trap 'stty size' WINCH; while true; do sleep 1; done"],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut ws = ws_connect(&socket, "resizer", 12345).await;
        // Drain any initial output from the ring buffer replay.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = collect_binary(&mut ws, Duration::from_millis(100)).await;

        // Send a resize control message: 42 rows × 133 cols.
        let resize = serde_json::json!({"type": "resize", "cols": 133, "rows": 42});
        ws.send(Message::Text(resize.to_string().into()))
            .await
            .unwrap();

        // Wait for the SIGWINCH handler to fire and `stty size` to output.
        let output = collect_binary(&mut ws, Duration::from_secs(3)).await;
        let text = String::from_utf8_lossy(&output);

        // `stty size` prints "ROWS COLS\n", so we expect "42 133".
        assert!(
            text.contains("42 133"),
            "expected '42 133' in output after resize; got: {text:?}"
        );

        let _ = ws.close(None).await;
        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
