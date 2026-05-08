#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod helpers;

use don::config::{Config, LogConfig, Platform};
use don::output::OutputManager;
use don::runner::{Runner, TerminalCoordinator};
use helpers::config::ConfigBuilder;
use helpers::tempdir::TempDir;
use helpers::timeout::run_with_timeout;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

const PLATFORM: Platform = Platform::LinuxX86_64;

/// Minimal HTTP-over-unix-socket client for tests. Returns (status, body).
async fn request(socket_path: &Path, method: &str, path: &str) -> (u16, String) {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let text = String::from_utf8_lossy(&response);
    let first_line = text.lines().next().unwrap_or("");
    let status: u16 = first_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("failed to parse response: {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

/// Wait for the socket file to exist (server finished binding).
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

/// Make a runner + background task. Returns the socket path, shutdown tx,
/// and join handle.
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
        PLATFORM,
        output_manager,
        base_dir.to_path_buf(),
        None,
        shutdown_rx,
        TerminalCoordinator::detached(),
    )
    .await
    .unwrap();
    let socket_path = base_dir.join(".don").join("don.sock");
    let handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });
    (socket_path, shutdown_tx, handle)
}

// --- Integration tests ---

#[test]
fn integration_status_endpoint_returns_items() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-status");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "GET", "/status").await;
        assert_eq!(status, 200, "body: {body}");
        assert!(body.contains("\"items\""), "body: {body}");
        assert!(body.contains("\"name\":\"keeper\""), "body: {body}");
        assert!(body.contains("\"kind\":\"service\""), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_status_endpoint_includes_task_last_run() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-task-last-run");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("prep", "true", &[])
            .log("ignore")
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let mut body = String::new();
        let start = tokio::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let (status, next_body) = request(&socket, "GET", "/status").await;
            assert_eq!(status, 200, "body: {next_body}");
            body = next_body;
            if body.contains("\"name\":\"prep\"") && body.contains("\"state\":\"completed\"") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(body.contains("\"name\":\"prep\""), "body: {body}");
        assert!(body.contains("\"state\":\"completed\""), "body: {body}");
        assert!(body.contains("\"last_run\""), "body: {body}");
        assert!(body.contains("\"success\":true"), "body: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_stop_endpoint_stops_service() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-stop");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Give the service time to start.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (status, _) = request(&socket, "POST", "/stop/keeper").await;
        assert_eq!(status, 204);

        // Confirm state flipped to Stopped via /status.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (_, body) = request(&socket, "GET", "/status").await;
        assert!(
            body.contains("\"state\":\"stopped\""),
            "service should be stopped; body: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_restart_endpoint_restarts_service() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-restart");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (status, _) = request(&socket, "POST", "/restart/keeper").await;
        assert_eq!(status, 204);

        // After restart, service should still be ready/running.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (_, body) = request(&socket, "GET", "/status").await;
        assert!(
            body.contains("\"state\":\"running\"") || body.contains("\"state\":\"ready\""),
            "service should be running after restart; body: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_stop_on_task_returns_400() {
    // Control commands only apply to services — a task name should return
    // 400 (bad request) with a clear message, not a confusing 404.
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-task-control");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .add_task("prep", "true", &[])
            .log("ignore")
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "POST", "/stop/prep").await;
        assert_eq!(status, 400, "body: {body}");
        assert!(
            body.contains("task") && body.contains("services"),
            "body should explain task vs service: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_socket_permissions_are_0600() {
    use std::os::unix::fs::PermissionsExt;
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-perms");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let mode = std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "socket should be owner-only");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_unknown_name_returns_404() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-404");
        let toml = ConfigBuilder::new()
            .add_custom_service("keeper", "sleep", &["60"])
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);

        let (status, body) = request(&socket, "POST", "/stop/ghost").await;
        assert_eq!(status, 404, "body: {body}");
        assert!(body.contains("ghost"), "body should mention name: {body}");

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

/// Stream an NDJSON-body request until `want_lines` lines have been seen
/// or `timeout` elapses. Returns the collected line texts.
async fn follow_lines(
    socket_path: &Path,
    path: &str,
    want_lines: usize,
    timeout: Duration,
) -> Vec<String> {
    let mut stream = UnixStream::connect(socket_path).await.unwrap();
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Length: 0\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut buffer = Vec::new();
    let mut headers_consumed = false;
    let mut lines: Vec<String> = Vec::new();
    loop {
        if tokio::time::Instant::now() > deadline {
            break;
        }
        let read_fut = async {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                None
            } else {
                Some(chunk[..n].to_vec())
            }
        };
        let timeout_left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let chunk = match tokio::time::timeout(timeout_left, read_fut).await {
            Ok(Some(c)) => c,
            _ => break,
        };
        buffer.extend_from_slice(&chunk);

        if !headers_consumed && let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            // Drain the header portion.
            buffer.drain(..pos + 4);
            headers_consumed = true;
        }
        if !headers_consumed {
            continue;
        }
        // Parse chunked transfer encoding: <size hex>\r\n<data>\r\n...
        while let Some(rn) = buffer.windows(2).position(|w| w == b"\r\n") {
            let size_str = std::str::from_utf8(&buffer[..rn]).unwrap_or("").trim();
            let size = usize::from_str_radix(size_str, 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            if buffer.len() < rn + 2 + size + 2 {
                break; // not enough data yet
            }
            let data = buffer[rn + 2..rn + 2 + size].to_vec();
            buffer.drain(..rn + 2 + size + 2);
            for line_bytes in data.split(|b| *b == b'\n') {
                if line_bytes.is_empty() {
                    continue;
                }
                let text = String::from_utf8_lossy(line_bytes).into_owned();
                lines.push(text);
            }
        }
        if lines.len() >= want_lines {
            break;
        }
    }
    lines
}

#[test]
fn integration_logs_follow_streams_lines() {
    run_with_timeout(Duration::from_secs(15), async {
        let dir = TempDir::new("server-follow");
        // Emit 2 lines immediately (pre), pause 2s to let the subscriber
        // connect, then emit 3 more (live). Subscriber asks for last=2 so
        // it should see pre1/pre2 from the snapshot + live1..3 from the stream.
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "echo pre1; echo pre2; sleep 2; for i in 1 2 3; do echo live$i; sleep 0.2; done; sleep 60",
                ],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Give the service time to emit pre1/pre2 into the ring buffer.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let lines = follow_lines(
            &socket,
            "/logs/chatty?last=2&follow=true",
            5,
            Duration::from_secs(5),
        )
        .await;

        // Should include preloaded tail (last=2) + live lines as they're emitted.
        let joined: String = lines.join(" | ");
        assert!(
            joined.contains("pre1"),
            "expected pre1 in snapshot; got: {joined}"
        );
        assert!(
            joined.contains("pre2"),
            "expected pre2 in snapshot; got: {joined}"
        );
        assert!(
            joined.contains("live1"),
            "expected live1 from stream; got: {joined}"
        );
        assert!(
            joined.contains("live3"),
            "expected live3 from stream; got: {joined}"
        );
        // Each line should be valid NDJSON.
        for line in &lines {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not NDJSON: {line:?} ({e})"));
            assert!(v.get("line").is_some(), "missing 'line' field: {line}");
        }

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}

#[test]
fn integration_logs_endpoint_returns_ring_buffer() {
    run_with_timeout(Duration::from_secs(10), async {
        let dir = TempDir::new("server-logs");
        let toml = ConfigBuilder::new()
            .add_custom_service(
                "chatty",
                "bash",
                &[
                    "-c",
                    "echo line1; echo line2; echo line3; echo line4; echo line5; sleep 60",
                ],
            )
            .log("ignore")
            .ready_exec("true", &[])
            .done()
            .build();

        let (socket, shutdown_tx, handle) = spawn_runner(&toml, dir.path()).await;
        assert!(wait_for_socket(&socket, Duration::from_secs(3)).await);
        // Wait for the service to emit its lines.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let (status, body) = request(&socket, "GET", "/logs/chatty?last=3").await;
        assert_eq!(status, 200);
        assert!(body.contains("line5"), "body: {body}");
        assert!(body.contains("line3"), "body: {body}");
        // last=3 → should NOT include line1 (oldest, evicted).
        assert!(
            !body.contains("line1"),
            "body should not include line1: {body}"
        );

        // Logs for a service with log=ignore should still be accessible
        // (ring buffer is fed regardless of log routing).
        let (status, body) = request(&socket, "GET", "/logs/chatty?last=10").await;
        assert_eq!(status, 200);
        assert!(
            body.contains("line1"),
            "ring buffer should have all lines: {body}"
        );

        let _ = shutdown_tx.send(()).await;
        handle.await.unwrap();
    });
}
